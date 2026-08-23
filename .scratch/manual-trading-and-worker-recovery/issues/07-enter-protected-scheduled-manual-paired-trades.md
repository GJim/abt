# 07 — Enter protected scheduled manual paired trades

**What to build:** Let an administrator create one confirmed,
idempotent **管理員手動配對交易** from the configured target. The controller
derives the counter-leg amount, dispatches the selected directional order and
interval, and protects the first filled leg immediately.

**Blocked by:** 06 — Configure and observe the manual-trading target.

**Status:** ready-for-human

- [x] Entry preview and confirmation show worker directions, derived lots,
  dispatch order/interval, live quote estimates, and required common TP/SL
  pip distances.
- [x] Entry accepts one valid base lots value, derives the fixed-ratio counter
  amount without rounding, and is durable/idempotent across retries.
- [x] If the first leg's protection exits before second-leg dispatch, or any
  execution result is anomalous, the second leg is suppressed as appropriate
  and both participating workers freeze.

## Comments

- Completed in `54d7ca7` and `862280f`. Validated with 217 Python tests and
  36 Playwright tests.
