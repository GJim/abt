# 10 — Dispatch worker analysis requests during reconciliation

**Status:** done

**Blocked by:** None — controller-side analysis orchestration and worker evidence collectors already exist.

## Problem

The completed catalog/M15/M1 tickets implemented the controller request/response contract and the worker-side evidence helpers, but `abt-worker reconcile` only performs snapshot/delta reconciliation and heartbeats. It never receives a `product_catalog_analysis_request`, calls `collect_product_catalog_evidence()` or `collect_market_data_evidence()`, or returns a `product_catalog_analysis_response`.

As a result, live analyses fail at `catalog_failed` after the controller's five-second response timeout without collecting symbol or historical-rate evidence.

## What to build

Wire the authenticated worker session into the reconciliation runtime so a connected worker can receive, execute, and reply to controller-initiated catalog, M15-screening, and M1-verification requests while preserving reconciliation and read-only broker boundaries.

## Acceptance criteria

- [ ] `abt-worker reconcile` dispatches each authenticated `product_catalog_analysis_request` by stage: `catalog`, `m15_screening`, or `m1_verification`.
- [ ] Catalog requests call `collect_product_catalog_evidence()` and return correlated, timestamped catalog evidence using `send_product_catalog_analysis()`.
- [ ] M15 and M1 requests call `collect_market_data_evidence()` only for controller-requested symbols and exact UTC period/timeframe, then return correlated, timestamped market-data evidence.
- [ ] The worker handles controller-pushed analysis messages without confusing them with acknowledgements for reconciliation, heartbeat, password, or safety-state requests.
- [ ] A worker processes at most one analysis request at a time; reconciliation remains responsive and has priority over queued analysis work.
- [ ] Invalid request shape, unknown stage, incomplete MT5 evidence, timeout, disconnect, or response-send failure fails the affected analysis safely without broker writes or password logging.
- [ ] A live two-worker integration test proves catalog, M15, and M1 evidence are collected through the real `reconcile` runtime, not only through hand-written WSS test responders.
- [ ] Tests prove zero broker-write calls for every analysis stage and retain existing reconciliation/heartbeat behavior.

## Relevant code

- `abt/worker/reconciliation.py`
- `abt/worker/session.py`
- `abt/worker/cli.py`
- `abt/controlplane/service.py`
- `tests/test_worker_reconciliation.py`
- `tests/test_worker_enrollment.py`
- `tests/test_controlplane_service.py`

## Comments

- 2026-08-18: Live analysis `ef8fb3ff-5c08-41cb-854b-9866c093b786` failed at catalog collection after approximately five seconds with zero candidates. This exposed that the `done` controller/UI tickets did not wire the worker dispatcher into `reconcile`.
- 2026-08-18: Implemented catalog, M15, and M1 dispatch in the authenticated reconciliation runtime, including interleaved controller-push buffering and read-only worker adapter methods.
