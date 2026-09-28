## Agent skills

### Issue tracker

Issues and specs are tracked as local Markdown under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Uses the default five-role vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`, `ready-for-human`, and `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context layout: root `CONTEXT.md` and `docs/adr/`. See `docs/agents/domain.md`.

## Current Objective (2026-09-26, supersedes 2026-09-08)

Edge-only pair strategy: the pair cell admits a single entry semantic
(cross-broker net-edge arbitrage) with symmetric mirrored-box protection.
The 2026-09-08 asymmetric objective (one leg maximizes profit while the
other only respects loss caps) is explicitly removed, along with the
`donchian`/`momentum` solo modes and all single-leg continuation flow.
See `.scratch/edge-only-worker/spec.md`.
Direct code/log changes allowed until the goal is met. Follower restart needs user help.

Role map: controller = identity/auth + opaque relay + route arbitration only
(Docker on this host); leader worker = sole lifecycle owner, edge detection,
canonical policy, immediate dispatch (this host); follower worker = safety-checked
executor, no independent edge (remote host).
