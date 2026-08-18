# 12 — Validate historical evidence completeness

**Status:** ready-for-agent

**Blocked by:** None — requires a broker-session completeness decision before implementation.

## Problem

The worker returns any non-empty M15/M1 rate sequence for the controller-requested UTC week. The controller calculates common coverage only relative to the two returned sequences, so identical but partial histories can appear to have 100% common coverage and potentially pass analysis.

## What to build

Define a broker-session-aware completeness contract for requested M15/M1 historical evidence, enforce it before candidates can pass, and surface an auditable failure when either worker returns incomplete data.

## Acceptance criteria

- [ ] The domain rule states how expected trading windows, weekend closures, broker daily maintenance windows, holidays, and DST are handled.
- [ ] The worker or controller validates that each returned series covers the requested UTC period according to that rule.
- [ ] Matching partial series cannot satisfy `minimum_common_coverage` merely because both omit the same interval.
- [ ] Incomplete evidence fails the applicable stage atomically and records a clear analysis failure reason.
- [ ] Tests cover a complete FX week, a missing leading/trailing segment, and a matching internal gap on both workers.
- [ ] Validation does not introduce broker-write operations or persist raw M15/M1 bars.

## Relevant code

- `abt/worker/session.py`
- `abt/controlplane/service.py`
- `.scratch/cross-server-product-pair-analysis/spec.md`
- `tests/test_controlplane_service.py`
- `tests/test_worker_reconciliation.py`

## Comments

- 2026-08-18: Identified during review of ticket 10. Implementation must not assume a fixed number of bars without first defining the broker-session calendar semantics.
