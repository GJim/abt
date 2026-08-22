# 05 — Contract legacy pair-level recovery

**What to build:** Remove the superseded pair-freeze, broker-state adoption,
and abandoned-pair recovery behavior after all execution paths use
**工作者凍結** and account-empty release.

**Blocked by:** 04 — Migrate automatic execution safety to worker isolation.

**Status:** ready-for-agent

- [ ] No production execution or recovery path can create or resolve a
  pair-level freeze.
- [ ] Recovery exposes only account cleanup, broker-empty reconciliation, and
  independent worker release; it never adopts or recreates an old trade.
- [ ] Regression tests prove the control plane has one worker-level safety
  model and existing non-anomalous trading behavior remains available.

