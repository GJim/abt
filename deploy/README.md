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

Back up each volume as a distinct encrypted artifact. A restore must restore
`openbao_raft` and `softhsm_tokens` as a matching pair; never copy either into
the controller or worker environment.
