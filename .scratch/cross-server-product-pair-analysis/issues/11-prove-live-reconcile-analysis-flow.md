# 11 — Prove live reconcile analysis flow

**Status:** ready-for-agent

**Blocked by:** 10 — worker analysis dispatch must remain implemented.

## Problem

Ticket 10 added unit coverage for the reconciliation runtime with fake sessions, but the feature still lacks an integration test that proves two real authenticated worker WSS sessions running the `reconcile` runtime can complete catalog, M15, and M1 analysis stages.

## What to build

Add a two-worker live WSS integration test that runs the real reconciliation runtime against the control-plane analysis API rather than using hand-written websocket responders.

## Acceptance criteria

- [ ] The test authenticates two independent worker sessions through the same WSS protocol used by `abt-worker reconcile`.
- [ ] Each worker runs the real reconciliation runtime with a read-only fake MT5 adapter.
- [ ] A launched analysis receives catalog, M15, and M1 evidence from both workers and reaches the expected terminal result.
- [ ] The test proves controller-pushed requests do not interfere with snapshot acknowledgements or heartbeats.
- [ ] The test proves all analysis and reconciliation operations make zero broker-write calls.

## Relevant code

- `abt/worker/reconciliation.py`
- `abt/worker/session.py`
- `abt/controlplane/service.py`
- `tests/test_controlplane_service.py`
- `tests/test_worker_reconciliation.py`

## Comments

- 2026-08-18: Identified during review of ticket 10. Existing tests cover the real reconciler with fake sessions and controller WSS with hand-written responders, but not their combined public behavior.
