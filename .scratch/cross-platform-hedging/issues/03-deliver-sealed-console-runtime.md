# 03 — Deliver sealed console runtime

**What to build:** Make the console deployable as a sealed Docker Compose control plane whose services have no direct host ingress, whose state survives restart in separated volumes, and whose OpenBao secret control plane proves it is unsealed before the controller becomes healthy.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] The Compose deployment allows cloudflared to reach the controller but publishes no controller, OpenBao, SoftHSM or database host port.
- [x] DuckDB ledger, OpenBao Raft data, SoftHSM token material and Tunnel credentials use distinct persistent volumes with documented ownership boundaries.
- [x] OpenBao uses the selected version-locked PKCS#11 KMS plugin and SoftHSM auto-unseal path; startup health proves a least-privilege secret can be read.
- [x] The controller runs as exactly one ASGI process and exposes a testable secret-control interface without exposing OpenBao to workers.

## Answer

Delivered the sealed Compose topology, including persisted and isolated state
volumes, a standard OpenBao image with SoftHSM support, the digest-pinned
PKCS#11 plugin, and a single-worker controller. The controller health endpoint
requires its dedicated token to read the `abt/data/health` sentinel, rather
than trusting OpenBao's public health status alone.

Validated with Docker Compose on 2026-08-15: initialized SoftHSM/OpenBao,
created a read-only sentinel token, verified controller health, restarted
OpenBao, then verified automatic unseal, sentinel read access, and controller
health recovery. The isolated validation project and volumes were removed.
