# 07 — Deliver the read-only release gate

**What to build:** Make the 安全通訊與唯讀對帳切片 operationally releasable by retaining and restoring its evidence, checking its sealed deployment boundary, and demonstrating the full flow with two workers in separate networks while proving it writes nothing to either broker.

**Blocked by:** 03 — Deliver sealed console runtime; 04 — Deliver approved worker enrollment; 05 — Deliver authenticated worker reconciliation; 06 — Deliver worker safety observability.

**Status:** ready-for-agent

- [ ] DuckDB/OpenBao backups run hourly with 24 local rotations and immediate PKI-change backup; controlled retention keeps audit/reconciliation evidence for at least one year.
- [ ] The operator workflow documents and verifies weekly encrypted offsite backup plus restoration from the allowed backup set.
- [ ] Deployment tests prove the Compose ingress, persistent-volume and single-ASGI-process constraints remain intact.
- [ ] A two-worker, three-network release exercise proves enrollment, approval, credential mediation, reconciliation, revocation, recovery and external-change alerting.
- [ ] Automated and live verification prove the slice did not place, modify, cancel, close or otherwise write any broker order or position.
