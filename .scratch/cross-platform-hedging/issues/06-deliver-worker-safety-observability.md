# 06 — Deliver worker safety observability

**What to build:** Make read-only worker operation safely observable: detect connection loss, terminal failure, account mismatch and external broker changes; expose actionable alerts; and make certificate revocation immediately stop a worker from reconnecting.

**Blocked by:** 05 — Deliver authenticated worker reconciliation.

**Status:** ready-for-human

- [x] Workers exchange 30-second heartbeats and enter the documented five-minute lost-link safety state without issuing broker writes.
- [x] Terminal/API failure retries use bounded backoff and stop after three failures; account mismatch immediately requires human handling.
- [x] A broker order/position change not attributed to the control plane creates an immutable event, a high-priority alert and a worker state requiring human handling.
- [x] Revoking a 裝置憑證 terminates/rejects WSS access and is visible in the management UI and audit stream.
- [x] Tests cover loss, recovery, mismatch, revocation and external-change behavior entirely through observable control-plane results.

## Answer

Implemented the public WSS heartbeat/safety-state contract, a read-only native safety adapter with 30-second heartbeats, five-minute lost-link safety, bounded terminal/API recovery, and immediate account-mismatch escalation. The management REST/UI now exposes immutable high-priority alerts, worker safety state, and audited certificate revocation that closes live WSS sessions and blocks reconnects. No broker write API is present or invoked.
