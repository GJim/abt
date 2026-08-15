# 03 — Deliver sealed console runtime

**What to build:** Make the console deployable as a sealed Docker Compose control plane whose services have no direct host ingress, whose state survives restart in separated volumes, and whose OpenBao secret control plane proves it is unsealed before the controller becomes healthy.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] The Compose deployment allows cloudflared to reach the controller but publishes no controller, OpenBao, SoftHSM or database host port.
- [ ] DuckDB ledger, OpenBao Raft data, SoftHSM token material and Tunnel credentials use distinct persistent volumes with documented ownership boundaries.
- [ ] OpenBao uses the selected version-locked PKCS#11 KMS plugin and SoftHSM auto-unseal path; startup health proves a least-privilege secret can be read.
- [ ] The controller runs as exactly one ASGI process and exposes a testable secret-control interface without exposing OpenBao to workers.
