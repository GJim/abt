# 08 — Operate active manual paired trades

**What to build:** Complete the active-trade path of the
**管理員手動交易頁**. Administrators can review, confirm, and idempotently
fully exit the unique active manual trade or update its shared TP/SL pip
distances across both legs.

**Blocked by:** 07 — Enter protected scheduled manual paired trades.

**Status:** ready-for-agent

- [ ] A target has at most one active manual paired trade and exposes one
  preview-confirmed full-exit action that operates both legs.
- [ ] A protection update previews the mapped broker TP/SL values for both
  actual fills, then updates both legs from one shared positive pip-distance
  request.
- [ ] Full-exit and protection-update retries are idempotent; failed or
  uncertain broker outcomes freeze both participating workers.
