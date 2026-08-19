# 03 — Model intent submissions and dual-broker preflight

**What to build:** Persist idempotent intent submissions and perform exact
two-worker broker `order_check` preflight before creating an executable
共同交易意圖.

**Blocked by:** 02 — Deliver Trader identity and reconnecting WSS.

**Status:** ready-for-agent

- [ ] Submission deduplication uses originator identity, command ID, and
  payload hash; replay returns the prior result and hash conflicts reject.
- [ ] The contract accepts only an active pair, direction, one decimal lots,
  shared entry price, positive SL/TP pips, FOK/IOC, and absolute expiry.
- [ ] Static validation records specific rejection reasons for unsupported
  filling mode, invalid pair state, price/protection constraints, and any
  volume that cannot exactly satisfy both endpoints.
- [ ] Both selected workers receive the exact eventual broker order parameters
  through `order_check`.
- [ ] An intent is created only after both checks succeed before the
  controller-monotonic expiry. Failure, timeout, malformed evidence, or expiry
  writes `rejected_preflight` with both outcomes and creates no intent.
- [ ] The implementation makes explicit that check success does not reserve
  liquidity or guarantee later broker execution.

