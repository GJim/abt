# 12 — Validate historical evidence completeness

**Status:** done

**Blocked by:** None — requires a broker-session completeness decision before implementation.

## Problem

The worker returns any non-empty M15/M1 rate sequence for the controller-requested UTC week. The controller calculates common coverage only relative to the two returned sequences, so identical but partial histories can appear to have 100% common coverage and potentially pass analysis.

## What to build

Define a broker-session-aware completeness contract for requested M15/M1 historical evidence, enforce it before candidates can pass, and surface an auditable failure when either worker returns incomplete data.

## Acceptance criteria

- [x] The domain rule states how expected trading windows, weekend closures, broker daily maintenance windows, holidays, and DST are handled.
- [x] The worker or controller validates that each returned series covers the requested UTC period according to that rule.
- [x] Matching partial series cannot satisfy `minimum_common_coverage` merely because both omit the same interval.
- [x] Incomplete evidence fails the applicable stage atomically and records a clear analysis failure reason.
- [x] Tests cover a complete FX week, a missing leading/trailing segment, and a matching internal gap on both workers.
- [x] Validation does not introduce broker-write operations or persist raw M15/M1 bars.

## Relevant code

- `abt/worker/session.py`
- `abt/controlplane/service.py`
- `.scratch/cross-server-product-pair-analysis/spec.md`
- `tests/test_controlplane_service.py`
- `tests/test_worker_reconciliation.py`

## Comments

- 2026-08-18: Identified during review of ticket 10. Implementation must not assume a fixed number of bars without first defining the broker-session calendar semantics.
- 2026-08-18: Policy selected by the operator: each requested UTC Monday-Friday must contain at least one returned bar for every requested symbol and timeframe. Weekend closure is expected; broker maintenance, holidays, DST shifts, and intraday gaps are allowed because no fixed intraday bar count is inferred. The controller rejects missing leading, trailing, or internal weekdays before coverage calculation.
- 2026-08-18: Coverage includes direct leading/trailing/internal weekday validation and a live two-worker M15 stage test where both workers omit the same internal weekday; the controller fails the stage atomically before coverage evaluation.
