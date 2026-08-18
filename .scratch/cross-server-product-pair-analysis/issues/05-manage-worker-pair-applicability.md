# 05 — Manage worker-pair applicability

**What to build:** Make an active 跨伺服器商品配對 server-wide by default while allowing an administrator to manually inspect a specific worker's live specification against the immutable reference snapshot and explicitly exclude that worker from that one pair.

**Blocked by:** 04 — Build and replace active product pairs.

**Status:** done

- [x] A newly eligible worker on either endpoint server is uninspected-but-applicable by default.
- [x] The Console provides an explicit compatibility check that reports hard-block and warning differences against the reference snapshot.
- [x] A detected difference never changes applicability automatically.
- [x] Only explicit admin approval can exclude a worker, and exclusion affects exactly one worker/product-pair relationship.
- [x] Exclusion, compatibility evidence, and authorization decisions are visible in audit history and perform no broker writes.
