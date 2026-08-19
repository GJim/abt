# 06 — Make operational overview refresh safely in real time

**What to build:** Keep the intervention queue, worker/account health, and hedge/product-pair overview current through bounded live refresh. Operators see freshness and connection feedback, and updates do not interrupt a safe action, discard a detail view, or obscure an error that needs attention.

**Blocked by:** 03 — Deliver actionable intervention queue; 04 — Deliver live worker and account health overview; 05 — Deliver active hedge and product-pair overview.

**Status:** done

- [x] Critical operational changes appear within the agreed refresh bound and visibly communicate data freshness.
- [x] Refresh failures and stale data remain intelligible and recoverable without presenting old data as current.
- [x] Browser tests cover live updates, stale connection feedback, failed refresh, and in-progress operator actions.
