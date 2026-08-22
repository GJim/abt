# 03 — Deliver worker cleanup and independent release

**What to build:** Deliver the **人工復歸工作區** for frozen workers. An
administrator can filter by freeze source, review all broker orders and
positions, explicitly preview account cleanup, cancel pending orders before
closing positions, verify an empty broker account, and independently release
one worker.

**Blocked by:** 01 — Publish live worker market state; 02 — Expand worker-level trading isolation.

**Status:** ready-for-human

- [x] The workspace lists frozen workers, their source, and their current
  broker-visible orders/positions, with filterable operational state.
- [x] Cleanup requires preview and confirmation, confirms cancellation before
  closing positions, and remains frozen after any failed or uncertain step.
- [x] Release is idempotent and available only after broker reconciliation
  proves zero pending orders and zero positions; audit records identity, time,
  and operation ID.
