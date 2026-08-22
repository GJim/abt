# 04 — Migrate automatic execution safety to worker isolation

**What to build:** Move every existing automated execution anomaly to the
worker-level isolation model. Intent dispatch, protection, reconciliation,
external broker change, credential containment, and connectivity failures
freeze the affected workers and route recovery through the new workspace.

**Blocked by:** 02 — Expand worker-level trading isolation; 03 — Deliver worker cleanup and independent release.

**Status:** ready-for-human

- [x] Every pre-existing trading and reconciliation failure path freezes its
  participating workers instead of making a pair-level recovery decision.
- [x] A worker loss of connection freezes that worker and every counterpart
  with which it has an active trade.
- [x] Contract tests cover each migrated failure class and prove no frozen
  worker can resume trading before cleanup and explicit release.
