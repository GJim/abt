# Manual Trading and Worker Recovery

Status: ready-for-agent

## Problem Statement

Administrators can review workers, active product pairs, intents, and
reconciliation snapshots, but cannot operate a selected active product pair
manually from the management station. They also lack a single, account-level
recovery workflow after a trading or connectivity anomaly.

An administrator needs current two-market quotes and the complete open
order/position state of the two selected workers before deliberately opening,
protecting, modifying, or fully exiting a manual paired trade. When an
execution anomaly occurs, the system must prevent unsafe reuse of affected
accounts until an administrator has cleared and verified each account through
the broker.

## Solution

Add two management-station pages:

1. A **管理員手動交易頁** for one shared, persistent **目前選取的商品配對**.
   It selects one active 跨伺服器商品配對 and an applicable, connected
   帳戶工作者 for each endpoint; presents live quotes, connection state, and
   every open order/position for both workers; and lets an administrator
   perform a confirmed, idempotent 管理員手動配對交易.
2. An **人工復歸工作區** for every **工作者凍結**. It shows the frozen
   worker's outstanding broker state, cancels all pending orders, closes all
   positions after cancellation is confirmed, verifies that the account is
   empty, and then permits an explicit independent worker release.

MT5's Python API is polling-only for the required state. Workers therefore
poll their locally attached terminals, publish changed state to the controller
over authenticated WSS, and the management station renders the controller's
latest state.

## User Stories

1. As a 主控台管理員, I want to open the 管理員手動交易頁, so that I can
   operate the one shared manual-trading target without entering a CLI.
2. As a 主控台管理員, I want to select only an active 跨伺服器商品配對, so
   that manual trading cannot target an unapproved or retired symbol mapping.
3. As a 主控台管理員, I want to choose one applicable, connected 帳戶工作者
   for each endpoint of the selected product pair, so that each leg uses a
   worker known to support its endpoint.
4. As a 主控台管理員, I want the currently selected product pair and workers
   to be shared and durable, so that every administrator sees the same
   operating target after refresh or controller restart.
5. As a 主控台管理員, I want the selected target to identify its active pair,
   endpoint symbols, workers, leg order, and interval, so that I can inspect
   the exact future execution plan.
6. As a 主控台管理員, I want to be unable to change the selected target while
   its manual trade is active, so that an exit cannot be directed at a
   different pair.
7. As a 主控台管理員, I want to see each selected worker's connection status,
   so that I can make the required human trading judgment from current
   availability.
8. As a 主控台管理員, I want to see each endpoint's latest bid, ask, broker
   quote time, and controller receipt time, so that I can judge price
   freshness and spread.
9. As a 主控台管理員, I want to see all open orders and positions on both
   selected workers, so that unrelated account exposure is not hidden from
   my decision.
10. As a 主控台管理員, I want the current product-pair and active manual-trade
    records visually identified within those order and position lists, so
    that I can distinguish the operation I am about to affect.
11. As a 主控台管理員, I want to set which worker is the Buy leg and which is
    the Sell leg, so that the intended direction is explicit rather than
    inferred from worker ordering.
12. As a 主控台管理員, I want to enter one positive base lots value, so that
    the controller derives the counter-leg volume from the active product
    pair's fixed lot relationship.
13. As a 主控台管理員, I want an invalid derived broker volume to be rejected
    rather than rounded, so that the intended hedge ratio is never silently
    changed.
14. As a 主控台管理員, I want to choose a shared Buy-to-Sell or Sell-to-Buy
    leg dispatch order, so that my deliberate directional execution strategy
    is explicit.
15. As a 主控台管理員, I want to configure an integer-second interval without
    an artificial upper limit between manual-trade legs, so that the shared
    plan can match my judgment of market conditions.
16. As a 主控台管理員, I want the leg order and interval to be shared,
    persistent settings of the current target, so that every later trade
    uses a reviewed plan.
17. As a 主控台管理員, I want to supply positive common TP and SL pip
    distances for every entry, so that neither leg can be opened without
    broker-managed protection.
18. As a 主控台管理員, I want the confirmation preview to estimate protection
    prices from current bid/ask while final TP/SL prices use each actual fill,
    so that I see useful estimates without misrepresenting fill-dependent
    protection.
19. As a 主控台管理員, I want to review the target, worker directions, lots,
    leg sequence, interval, quotes, TP/SL, and current exposure before
    submitting an entry, so that a market-order action is never an accidental
    click.
20. As a 主控台管理員, I want the first filled leg to receive its TP/SL
    immediately, so that the configured interval does not leave it
    unprotected while the second leg waits.
21. As a 主控台管理員, I want the second leg to be suppressed when the first
    leg's protection exits before dispatch, so that the controller does not
    create a new unpaired exposure.
22. As a 主控台管理員, I want only one active manual paired trade per current
    target, so that full exit has one unambiguous pair of legs.
23. As a 主控台管理員, I want to fully exit that active manual trade through
    a preview and explicit confirmation, so that both legs are deliberately
    closed together.
24. As a 主控台管理員, I want to modify an active manual trade's shared TP/SL
    pip distances, so that the controller can map and update both legs'
    broker protections together.
25. As a 主控台管理員, I want an entry, full exit, or TP/SL modification to
    be idempotent across double-clicks, refreshes, and retries, so that a
    repeated management request cannot create a second trade.
26. As a 主控台管理員, I want an anomalous execution to freeze both
    participating workers, so that neither account can be selected for a new
    trade while its broker state is unsafe.
27. As a 主控台管理員, I want a worker loss of connection to freeze that worker
    and every worker participating in its active trades, so that all
    potentially coupled accounts are isolated.
28. As a 主控台管理員, I want every execution origin to use the same
    worker-level isolation rule, so that a manual trade, a management intent,
    and a Trader intent never receive inconsistent safety treatment.
29. As a 主控台管理員, I want a frozen worker excluded from manual-trading
    selection, so that no new paired trade can include an account awaiting
    recovery.
30. As a 主控台管理員, I want to open the 人工復歸工作區 and filter frozen
    workers by their freeze source, so that I can prioritize operational
    recovery.
31. As a 主控台管理員, I want to see each frozen worker's complete current
    orders and positions, so that I can review the broker account before
    requesting destructive cleanup.
32. As a 主控台管理員, I want the cleanup preview to show every pending order
    and position that will be affected, so that account-wide cleanup is an
    informed action.
33. As a 主控台管理員, I want cleanup to cancel all pending orders and confirm
    cancellation before closing positions, so that a pending order cannot
    fill while positions are being flattened.
34. As a 主控台管理員, I want cleanup to keep the worker frozen whenever
    cancellation, closing, or reconciliation fails or is uncertain, so that
    the empty-account requirement is never assumed.
35. As a 主控台管理員, I want to release each frozen worker independently
    only after the broker verifies zero pending orders and zero positions, so
    that a recovered account becomes available without waiting for a former
    counterpart.
36. As a 主控台管理員, I want cleanup and release records to include my
    identity, time, and operation ID, so that the account's return to service
    is auditable without requiring a free-text justification.
37. As a 主控台管理員, I want prior anomalous trades to remain immutable
    records after recovery, so that releasing an account never adopts,
    repairs, or recreates an unsafe prior trade.

## Implementation Decisions

- Extend the existing management control-plane boundary rather than creating
  a second trading service. The controller remains the authority for the
  currently selected product pair, command idempotency, worker eligibility,
  trading lifecycle, audit events, and worker safety state.
- Add persistent control-plane state for the singleton currently selected
  product pair: active product-pair ID, one worker for each endpoint, the
  configured Buy/Sell dispatch order, an unbounded non-negative integer
  interval in seconds, revision/version data for compare-and-confirm, and
  the current manual-trade reference when present.
- The configuration must reject retired/non-active pairs, endpoint-worker
  mismatches, workers excluded by pair applicability, disconnected workers,
  frozen workers, duplicate workers, and a replacement while an active
  manual trade exists.
- Manual-trading admission intentionally verifies only that both eligible
  workers are connected. Margin, quote freshness, volume capability, and
  other trading judgment remain administrator responsibilities; failures
  remain explicit broker/controller outcomes and are never silently ignored.
- The manual entry contract contains a persistent administrator command ID,
  selected-target revision, Buy worker ID, Sell worker ID, one positive
  decimal base-lots value, and positive common TP/SL pip distances. The
  controller derives the other lots value from the product pair ratio and
  rejects non-exact broker volume steps.
- The controller deduplicates every manual entry, full exit, protection
  update, cleanup, and release by originator identity, command ID, and
  payload hash. The same ID and payload returns the recorded result; the
  same ID with a different payload is rejected.
- Manual entry dispatches the configured first directional leg, waits the
  configured integral seconds, then dispatches the opposite leg. The first
  filled leg gets its TP/SL from its actual fill price immediately. If that
  protection exits before the second leg dispatches, the second dispatch is
  suppressed and both workers freeze.
- A successful two-leg manual entry creates one 管理員手動配對交易. The
  management station exposes a full-exit command only for that unique active
  trade; it targets both legs and requires preview confirmation.
- TP/SL modification accepts new positive common pip distances. It maps them
  from both actual fill prices and directions, previews the two resulting
  broker values, then updates both legs. A failed or uncertain update freezes
  both participating workers.
- Replace pair-level freeze recovery with worker-level isolation in all
  execution paths. Any execution anomaly freezes the two participating
  workers; an unavailable worker freezes itself and each worker participating
  in any of its active trades. Frozen workers cannot participate in new
  selection or dispatch.
- A frozen worker's recovery is account-wide and destructive: cancel every
  pending order, wait for confirmed cancellation, close every position, and
  reconcile directly with the broker. Only an observed empty account may be
  released. Release is independent per worker and does not recreate, adopt,
  or otherwise resume an old trade.
- Record each freeze, cleanup request/result, empty-account reconciliation,
  and release as immutable events. The requested audit surface records
  administrator identity, timestamp, and operation ID; no reason field or
  before/after snapshot retention is introduced for release.
- Extend the worker WSS protocol with diff messages for watched-symbol quotes,
  open orders, positions, and connectivity, plus a complete state snapshot
  after worker/WSS reconnection. The worker serializes terminal access and
  polls latest quotes every 500 ms, orders/positions every 1 second, and
  terminal connection status every 5 seconds; it pushes only changed state.
  Quote payloads retain broker timestamp and controller receipt time.
- Use `symbol_info_tick` for bid/ask/time, `orders_get` and `positions_get`
  for live account state, `terminal_info` for connectivity, and
  `history_deals_get` as the recovery safety net. Do not use
  `symbol_select` or `market_book_add` as a UI push mechanism; the Python
  bridge receives no MQL5 event callbacks.
- Add routes in the existing management SPA for manual trading and recovery.
  The manual page embeds current-target configuration, live quote/status
  table, all order/position rows, preview-confirm dialogs, and active-trade
  controls. The recovery page lists frozen workers, source filters, destructive
  cleanup previews, reconciliation status, and independent release controls.
- Continue to use the existing secure cookie session, CSRF checks, same-origin
  admin API, and immutable control-plane ledger. No new administrator
  identity, browser credential, or direct worker browser connection is added.

## Testing Decisions

- The primary feature seam is the authenticated management control-plane
  contract: administrative HTTP commands and queries, coupled to simulated
  authenticated worker WSS messages. This is the highest existing boundary
  that observes persistent selection, command idempotency, worker state,
  execution dispatch, freezing, cleanup, reconciliation, and audit events
  without asserting internal storage or polling implementation details.
- Service/API contract tests should drive connected, disconnected, applicable,
  excluded, frozen, and mismatched worker states; active and retired product
  pairs; target persistence and revision conflicts; all manual entry,
  confirmation, exit, TP/SL update, and idempotent-retry outcomes; ordered
  dispatch and interval behavior; first-leg protection; every anomalous
  outcome; counterpart expansion on worker loss; cleanup order; uncertain
  broker result; empty-account verification; independent release; and audit
  visibility.
- Worker-session tests should simulate MT5 poll results and assert externally
  visible WSS snapshots/diffs, unchanged-value suppression, broker timestamp
  preservation, reconnect snapshots, and the selected 500 ms/1 s/5 s
  schedules. They should not assert private loop structure.
- Browser tests should use the existing Playwright management test patterns
  and mock the admin API. They should prove discoverable live state, target
  configuration, eligible-worker restriction, preview-confirm actions,
  disabled frozen selections, active-target locking, all-orders/positions
  visibility, recovery filtering, cleanup preview, and release availability
  only after an empty reconciliation.
- Extend the existing FastAPI control-plane tests and worker session/
  reconciliation tests rather than introducing a separate test framework.
  A good test observes an administrator-visible result or protocol contract,
  not SQL layout, React local state, or a particular poll-loop implementation.

## Out of Scope

- Candlestick charts, historical chart data, market-depth UI, and broker
  depth-of-market trading.
- A resident MQL5 EA or direct broker callback architecture.
- Automatic margin, exposure, freshness, spread, or other aggregate risk
  limits beyond connected-worker eligibility.
- Arbitrary worker/symbol pairings, inactive/retired product pairs, and
  separate manually entered leg volumes.
- Multiple simultaneous manual trades for the selected target, partial manual
  exit, and per-leg absolute TP/SL editing.
- Automatic recovery, adoption of broker state, resurrection of prior
  trades, or a free-text release-reason requirement.
- Any change to management authentication, worker key/certificate trust,
  direct browser-to-worker connectivity, or MT5 credential exposure.

## Further Notes

- This specification applies ADR 0007: worker-level trading isolation is a
  deliberate account-safety trade-off. It replaces pair-level recovery
  behavior throughout the relevant execution paths.
- MT5's Python API requires local polling; `market_book_add` subscribes the
  terminal to DOM changes but does not expose MQL5 `OnBookEvent` to Python.
  The worker-to-controller WSS channel, not the broker API, supplies the
  asynchronous update path to the management station.
- The authoritative API details and source links are retained in the MT5 live
  updates research note. The domain glossary defines the canonical terms used
  by this specification.
