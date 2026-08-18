# 04 — Build and replace active product pairs

**What to build:** Let an administrator explicitly Build a final passing candidate into an active 跨伺服器商品配對, retire it when appropriate, or atomically Replace an existing active pair with the same unordered server/symbol endpoints. Each built FX v1 pair expresses a 1:1 lot relationship and preserves immutable approval evidence.

**Blocked by:** 03 — Verify FX candidates with M1.

**Status:** done

- [ ] Build requires explicit admin confirmation showing the candidate evidence, policy snapshot, reference specifications, source workers, and UTC analysis period.
- [ ] A built pair is unordered, uses exact MT5 server identities, has immutable reference specifications and policy evidence, and has a 1:1 lot relationship.
- [ ] The ledger permits at most one active pair for one unordered server/symbol endpoint pair.
- [ ] Replace retires the old pair and creates the new active pair in one auditable transaction; ordinary Build conflicts instead of duplicating.
- [ ] Retirement prevents future use while retaining at least one year of evidence; no UI deletion is available.
