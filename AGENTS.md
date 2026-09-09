## Agent skills

### Issue tracker

Issues and specs are tracked as local Markdown under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the default five-role vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: root `CONTEXT.md` and `docs/adr/`. See `docs/agents/domain.md`.

## Current Objective (2026-09-08)

Modify pair strategy so that for any pair, one leg maximizes profit while the
other leg only needs to respect daily max loss (`daily_loss_fraction` of frozen
budget) and single-trade max loss (`min(trade_loss_fraction, maximum_loss_per_trade_usd)`).
Direct code/log changes allowed until the goal is met. Follower restart needs user help.

Role map: controller = identity/auth + opaque relay + route arbitration only
(Docker on this host); leader worker = sole lifecycle owner, edge detection,
canonical policy, immediate dispatch (this host); follower worker = safety-checked
executor, no independent edge (remote host).
