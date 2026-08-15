# 06 — Deliver worker safety observability

**What to build:** Make read-only worker operation safely observable: detect connection loss, terminal failure, account mismatch and external broker changes; expose actionable alerts; and make certificate revocation immediately stop a worker from reconnecting.

**Blocked by:** 05 — Deliver authenticated worker reconciliation.

**Status:** ready-for-agent

- [ ] Workers exchange 30-second heartbeats and enter the documented five-minute lost-link safety state without issuing broker writes.
- [ ] Terminal/API failure retries use bounded backoff and stop after three failures; account mismatch immediately requires human handling.
- [ ] A broker order/position change not attributed to the control plane creates an immutable event, a high-priority alert and a worker state requiring human handling.
- [ ] Revoking a 裝置憑證 terminates/rejects WSS access and is visible in the management UI and audit stream.
- [ ] Tests cover loss, recovery, mismatch, revocation and external-change behavior entirely through observable control-plane results.
