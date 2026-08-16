# Sealed console deployment

Start this topology only on the console host. Docker publishes no service ports:
Cloudflare Tunnel is the sole ingress to the controller. The controller accepts
`CF-Connecting-IP` only from the fixed `cloudflared` container address
`172.30.0.2`; do not attach another service to the ingress network.

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

OpenBao alone joins the `plugin-registry` network so it can fetch the pinned
PKCS#11 plugin from GHCR; it has no published host port. The controller and
SoftHSM remain isolated from that egress network.

Persistent volumes have separate ownership and backup boundaries:

| Volume | Owner | Contents |
| --- | --- | --- |
| `controller_ledger` | controller | DuckDB control-plane ledger |
| `openbao_raft` | OpenBao | encrypted OpenBao Raft data |
| `softhsm_tokens` | SoftHSM/OpenBao | PKCS#11 token material |
| `cloudflared_credentials` | cloudflared | Tunnel credentials |
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
