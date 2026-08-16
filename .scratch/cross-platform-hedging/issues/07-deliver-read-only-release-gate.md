# 07 — Deliver the read-only release gate

**What to build:** Make the 安全通訊與唯讀對帳切片 operationally releasable by retaining and restoring its evidence, checking its sealed deployment boundary, and demonstrating the full flow with two workers in separate networks while proving it writes nothing to either broker.

**Blocked by:** 03 — Deliver sealed console runtime; 04 — Deliver approved worker enrollment; 05 — Deliver authenticated worker reconciliation; 06 — Deliver worker safety observability.

**Status:** resolved

- [ ] DuckDB/OpenBao backups run hourly with 24 local rotations and immediate PKI-change backup; controlled retention keeps audit/reconciliation evidence for at least one year.
- [ ] The operator workflow documents and verifies weekly encrypted offsite backup plus restoration from the allowed backup set.
- [ ] Deployment tests prove the Compose ingress, persistent-volume and single-ASGI-process constraints remain intact.
- [ ] A two-worker, three-network release exercise proves enrollment, approval, credential mediation, reconciliation, revocation, recovery and external-change alerting.
- [ ] Automated and live verification prove the slice did not place, modify, cancel, close or otherwise write any broker order or position.

## Answer

Implemented the read-only release gate: controller-owned hourly 24-set DuckDB/OpenBao/SoftHSM backups, immediate approval/revocation PKI backups, integrity-checked allowed-set restore verification, Compose backup persistence, and a documented weekly encrypted-offsite/one-year-evidence procedure. Added deployment constraints and a two-worker WSS release scenario covering credential mediation, reconciliation, revocation, recovery, external-change alerting, and a zero-MT5-write broker double.
