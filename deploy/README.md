# Sealed console deployment

Start this topology only on the console host. Docker publishes no service ports:
Cloudflare Tunnel is the sole ingress to the controller. The controller accepts
`CF-Connecting-IP` only from the fixed `cloudflared` container address
`172.30.0.2`; do not attach another service to the ingress network.

Create a remotely managed Cloudflare Tunnel and configure its public hostname
to route to `http://controller:8000`. Store its rotated tunnel token only in
the root-readable `deploy/.env`:

```dotenv
ABT_CLOUDFLARE_TUNNEL_TOKEN=<rotated-token>
```

Compose supplies it as `TUNNEL_TOKEN` to cloudflared. Never put the token in a
Compose command, Git, terminal history, or support transcript; rotate it
immediately if exposed.

Set `OPENBAO_PKCS11_PIN` and `SOFTHSM_SO_PIN` in the host environment before
running `docker compose up`. They initialize and unlock the SoftHSM token; keep
them out of source control and shell history. The controller reports unhealthy
until its `ABT_OPENBAO_HEALTH_TOKEN` can read `abt/data/health`. Bootstrap the
token only after initializing OpenBao: create the `abt` KV-v2 mount, write the
health sentinel, grant a policy limited to `read` on `abt/data/health`, then
set the resulting token in the host environment before starting the controller.

Set `ABT_OPENBAO_CONTROLLER_TOKEN` to a separate controller policy token. It
may create, read and delete only MT5 credential secrets and sign device
certificates through the configured Transit key; it must not be the OpenBao
root token or the health token. Pending registration credentials are deleted
when the registration is rejected or expires.

## First-time OpenBao bootstrap

Create `deploy/.env` with high-entropy `OPENBAO_PKCS11_PIN` and
`SOFTHSM_SO_PIN`, then run:

```bash
sudo ./deploy/bootstrap-openbao.sh
```

The script waits for SoftHSM to create the `abt-root` seal key, initializes
only an uninitialized OpenBao instance, creates the required KV-v2/Transit
mounts and least-privilege policies, writes health/controller tokens to
`deploy/.env`, creates the one-time administrator before the controller owns
the DuckDB ledger, and starts the controller. It prints recovery keys, the
initial root token, and administrator credentials once; store them offline
and never add them to `.env`, Git, or shell history. It refuses to run against
initialized OpenBao state.

## First administrator

`bootstrap-openbao.sh` creates the one-time administrator before the
controller starts and prints its credentials once. Do not run `abt-console`
against an active controller ledger: DuckDB has a single-writer boundary and
the controller holds its lock.

Store the generated credentials in a password manager; the password is not
shown again.

OpenBao alone joins the `plugin-registry` network so it can fetch the pinned
PKCS#11 plugin from GHCR; it has no published host port. The controller and
SoftHSM remain isolated from that egress network.

Persistent volumes have separate ownership and backup boundaries:

| Volume | Owner | Contents |
| --- | --- | --- |
| `controller_ledger` | controller | DuckDB control-plane ledger |
| `openbao_raft` | OpenBao | encrypted OpenBao Raft data |
| `softhsm_tokens` | SoftHSM/OpenBao | PKCS#11 token material |
| `controller_backups` | controller | 24 local complete restore sets |

Back up each volume as a distinct encrypted artifact. A restore must restore
`openbao_raft` and `softhsm_tokens` as a matching pair; never copy either into
the controller or worker environment.

## Release-gate backups and restore verification

The controller creates a complete restore set every hour and retains the newest
24 sets in `controller_backups`. Each set contains a DuckDB export, OpenBao
Raft archive, SoftHSM token archive and a SHA-256 manifest. Approval and
revocation create an immediate `pki-change` restore set. The controller is the
only process that snapshots the live DuckDB ledger.

Once a week, an operator copies a verified complete set to encrypted offsite
storage. Keep the offsite encryption key separately from the console host.
Before copying, and during the weekly restore drill, run:

```powershell
abt-console --ledger <ledger-path> verify-restore-set `
  --backup-directory <allowed-backup-directory> <backup-set>
```

Only a set directly beneath the configured backup directory is accepted. For a
restore drill, stop the Compose stack, verify the selected set, restore its
three artifacts together into an isolated replacement of the ledger, Raft and
SoftHSM volumes, then start the replacement stack and verify `/health`.
Never restore a partial set or a set from an arbitrary path.

Audit events and reconciliation snapshots/deltas remain in the control-plane
ledger for at least one year. The Web UI has no deletion endpoint; any future
host-local retention cleanup must preserve that minimum and emit an audit
event.

## Two-worker read-only release exercise

Run the exercise with the console host and each native worker on three separate
networks. Enroll and approve two distinct test accounts, prove each worker can
obtain only its own credential through WSS, then collect a full reconciliation
snapshot from each. Revoke one certificate and confirm its reconnect is denied;
recover the other worker's WSS session and reconciliation cursor. Introduce a
known external test-account order/position change and confirm the immutable
event, high-priority alert, and `needs_human` state.

The automated scenario uses a broker double whose write entry points fail and
asserts no write invocation. For the live exercise, record broker orders and
positions before and after every step and retain the API/terminal logs with the
release evidence. The counts and tickets must be unchanged: this slice must
not place, modify, cancel, close, or otherwise write any MT5 order or
position. Stop the exercise on any discrepancy.
