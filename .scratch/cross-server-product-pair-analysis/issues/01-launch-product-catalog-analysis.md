# 01 — Launch product catalog analysis

**What to build:** Let a 主控台管理員 select two approved, healthy, connected 帳戶工作者 on different exact MT5 servers, create or select an immutable 分析政策快照, and launch a read-only 商品配對分析. The Console receives complete catalog/specification evidence from both workers over the authenticated session, displays eligible FX candidates and non-FOREX exceptions, and records an auditable success or failure without broker writes.

**Blocked by:** None — can start immediately.

**Status:** done

- [ ] The Console rejects same-server, unhealthy, disconnected, unapproved, or revoked worker selections before dispatch.
- [ ] A submitted analysis saves an immutable policy snapshot and source-worker/server identities.
- [ ] Both workers return complete timestamped catalog/specification evidence or the analysis fails without candidates.
- [ ] FX candidate identity requires equal base/profit currencies and FOREX calculation mode; exceptions are visible but not buildable.
- [ ] The end-to-end REST/WSS flow is authenticated, auditable, and proves zero broker writes.
