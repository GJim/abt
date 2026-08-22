# 02 — Expand worker-level trading isolation

**What to build:** Add durable **工作者凍結** as an administrator-visible,
account-level safety state without yet removing legacy pair-recovery behavior.
Frozen workers are excluded from all new trading selection, and freeze events
are recorded with their source and affected workers.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] A worker can enter and expose a durable frozen state with immutable
  source, affected-worker, and audit information.
- [ ] A frozen worker cannot be selected for a new management or automated
  trade, while non-frozen workers retain existing behavior.
- [ ] Existing pair-level recovery remains compatible during the migration,
  with service and browser tests proving the new isolation visibility and
  selection exclusion.

