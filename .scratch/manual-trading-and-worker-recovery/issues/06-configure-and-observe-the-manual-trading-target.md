# 06 — Configure and observe the manual-trading target

**What to build:** Deliver the read and configuration path of the
**管理員手動交易頁**. Administrators configure one shared, persistent active
product pair, its applicable connected endpoint workers, and the directional
leg plan while reviewing their live quotes, connectivity, and all open
orders/positions.

**Blocked by:** 01 — Publish live worker market state; 02 — Expand worker-level trading isolation.

**Status:** ready-for-agent

- [x] Only active product pairs and their applicable, connected, non-frozen
  endpoint workers can form the shared current target.
- [x] The target persists its two workers, pair, Buy/Sell dispatch order, and
  non-negative integral-second interval across management sessions.
- [x] The page presents live quote timestamps and all worker orders/positions,
  visibly identifies target-related records, and rejects target replacement
  while a manual trade is active.
