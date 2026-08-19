# 04 — Deliver live worker and account health overview

**What to build:** Give an operator a scan-friendly fleet overview of workers, showing idle and unhealthy workers, concise account/reconciliation health, and data freshness. The operator can open details for a worker without loading raw reconciliation evidence into the primary view.

**Blocked by:** 01 — Create operations dashboard read model and intervention taxonomy; 02 — Adopt Astryx operations shell and progressive-disclosure primitives.

**Status:** done

- [x] The overview distinguishes idle, healthy, stale, and human-action worker states without relying on color alone.
- [x] Each visible health signal includes a meaningful freshness indicator and leads to relevant details on demand.
- [x] Browser tests cover realistic fleet sizes, empty results, stale data, and narrow-screen operation.
