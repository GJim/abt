# Strategy-Owned Realtime Pair Lifecycle Refactor

Status: ready-for-human

## Problem Statement

Realtime arbitrage currently splits one protected pair lifecycle across the
strategy script, Trader process, controller ledger, controller recovery tasks,
and two Workers. Each layer retains only part of the same fact:

- the strategy retains the signal, requested prices, requested volume, and a
  local `Pair`;
- the controller retains command, pair, recovery, and observed-leg state;
- each Worker retains broker effects and direct MT5 observations.

The split was introduced before the realtime strategy lifecycle had stabilized.
It creates duplicated states, one-way handoffs, and information lag. In
particular, the strategy can receive `CATCHING_UP` without receiving the later
authoritative tickets, fills, positions, orders, or `ACTIVE_VERIFIED`
transition. It therefore cannot make shutdown, reversal, risk, or integrity
decisions from one coherent state.

Adding more synchronization events to the existing split would enlarge the
interface without establishing one lifecycle owner. The refactor must instead
replace the split implementation with one durable Strategy Runtime and remove
the obsolete controller-owned pair implementation.

## Decision

Combine realtime strategy policy and Trader execution orchestration in one
durable **Strategy Runtime**. Do not introduce a separate Trader Execution
module or interface during this refactor.

The Strategy Runtime owns the complete cross-Worker pair lifecycle:

- market-data consumption and signal decisions;
- sizing and risk policy;
- protected paired dispatch coordination;
- requested and broker-observed leg facts;
- entry, active, close, containment, and terminal state;
- reconnect, replay, restart recovery, and idempotency;
- every decision that depends on current positions or orders.

The controller remains the authenticated external-network relay and audit
plane. It authenticates Strategy Runtimes and Workers, authorizes routes,
maintains connected-session routing and replayable delivery, and records an
immutable copy of relayed envelopes. It does not interpret broker facts,
derive pair state, decide compensation, own protected-pair lifecycle, or
provide management pair and trade operations.

Each Worker remains the exclusive owner of one MT5 terminal and account. It
serializes broker access, enforces account-local safety and command admission,
durably journals broker effects, deduplicates effect IDs, and emits
sequence-numbered broker facts directly to the owning Strategy Runtime through
the controller relay.

## Design Principle

This refactor deliberately chooses a deep in-process module before introducing
another seam. Strategy policy and pair execution change together, share the
same authoritative state, and currently have only one production
implementation. A separate Trader Execution interface may be considered only
after:

1. the combined lifecycle has operated without ownership defects;
2. at least two real strategy implementations need the same execution
   behaviour; and
3. the stable shared behaviour can be identified from working code rather than
   predicted in advance.

Tests may use internal fake relay and Worker adapters. Those test adapters do
not make Trader Execution a public module.

## Target Ownership

### Strategy Runtime

The Strategy Runtime is the only protected-pair lifecycle owner. Its local
SQLite/WAL store contains:

- runtime identity and schema version;
- monotonically increasing local command sequence;
- immutable command payload and payload hash;
- one active pair aggregate;
- desired pair state;
- per-leg Worker, symbol, direction, requested volume, and protection;
- Worker effect ID and delivery state;
- broker receipt, ticket, fill, order, and position evidence;
- last acknowledged Worker event cursor per Worker;
- lifecycle transition history;
- unresolved recovery work.

The runtime must persist the next transition before emitting any command whose
delivery may create or reduce broker exposure.

### Controller

The controller owns:

- Trader and Worker registration, identity, certificate, and revocation;
- route authorization between an authenticated Trader identity and selected
  Workers;
- live connection registry and heartbeats;
- bounded message transport;
- at-least-once envelope delivery and cursor acknowledgement;
- immutable audit copies of commands, facts, delivery acknowledgements, and
  connection events;
- identity, connectivity, and opaque relay-audit visibility.

The controller must treat trading payloads as versioned opaque envelopes. It
may validate envelope shape, identity, route, size, and protocol version, but
must not validate symbol, volume, pair state, order state, position state, or
recovery transitions.

The management site must not create, select, replace, retest, cancel, close,
modify, or emergency-flatten a product pair, trading intent, order, position,
or protected pair. Existing management trading pages and mutation endpoints
are removed rather than redirected through the Strategy Runtime.

### Worker

Each Worker owns:

- direct MT5 session and broker truth for its account;
- serialized read and write scheduling;
- deadline checks at the last safe pre-send point;
- durable effect preparation, `send_started`, receipt, and observation;
- same-effect-ID/same-payload replay and conflicting reuse rejection;
- account-local protection and recovery rules;
- sequence-numbered snapshots, deltas, receipts, and operation outcomes.

A Worker never infers the state of the opposite leg. It reports its own facts
and executes only commands authorized for its account.

## Strategy Runtime Lifecycle

Use one internal pair aggregate. State names are local implementation details
and are not controller states, but the implementation must distinguish at
least:

1. `EMPTY` - current Worker facts prove no owned pair exposure.
2. `DISPATCHING` - at least one entry command may reach a Worker.
3. `ENTRY_UNCERTAIN` - one or more broker outcomes require fresh observation.
4. `VERIFYING` - both entry receipts exist; current Worker facts must bind the
   actual tickets, fills, orders, positions, and protection.
5. `ACTIVE` - both current Worker facts prove the expected protected pair.
6. `CLOSING` - desired state is empty; pending orders are cancelled and
   positions are closed from current evidence.
7. `CLOSE_UNCERTAIN` - a close effect crossed `send_started` without conclusive
   receipt.
8. `NEEDS_HUMAN` - identity, journal integrity, or persistently unobservable
   broker state prevents automatic recovery.

The runtime may refine these states internally. Callers and tests should
observe pair facts and allowed actions rather than depend on every internal
state name.

## Required Behaviour

### Startup and Restart

- Opening the SQLite/WAL store and recovering unresolved work happens before
  live signal evaluation.
- The runtime reconnects to both Workers through the controller and resumes
  each Worker event stream from its last committed cursor.
- It requests a fresh full orders/positions snapshot from both Workers before
  deciding the current pair state.
- Stored desired state plus current broker facts determine recovery. Stored
  pre-crash assumptions never override current broker facts.
- No new entry is admitted until both accounts are freshly observed and the
  previous pair is proven active, empty, or terminal.

### Entry

- One durable local command ID identifies the complete pair entry.
- The runtime records the planned pair and both deterministic Worker effect IDs
  before dispatch.
- Both account snapshots are fetched concurrently before admission.
- Cached symbol and margin facts prepare exact protected orders before the
  signal; MT5 validates each order when it is submitted.
- Dispatch begins only while the shared expiry still permits both commands.
- Both Worker sends are issued concurrently.
- A failed or unknown leg never triggers a blind resend.
- Any uncertain or asymmetric outcome immediately changes desired state to
  empty and begins evidence-driven convergence.
- Broker receipts are not active-position proof. Fresh Worker facts must bind
  actual tickets, volumes, sides, symbols, orders, positions, SL, and TP.

### Active Pair

- Positions, orders, fills, protection, and tickets used by strategy decisions
  come from current Worker facts in the same pair aggregate.
- Risk, reversal, cutoff, and integrity decisions read one immutable aggregate
  snapshot per decision.
- A missing leg, unexpected order, volume change, side change, protection
  change, or unknown ticket changes desired state to empty.
- Price-only movement does not create integrity divergence.

### Close

- Reversal, risk limit, daily cutoff, Ctrl+C, shutdown, and recovery all set the
  same durable desired state: empty.
- Close is a lifecycle transition, not a retry of a physical broker request.
- The runtime observes orders, cancels current orders, observes again, closes
  current positions, and requires a final empty snapshot from both Workers.
- A close whose response is lost is resolved from fresh Worker facts and a new
  compensating effect only when those facts justify it.
- Process shutdown may exit only after broker-verified empty, explicit
  `NEEDS_HUMAN`, or a configured operator-approved detach that leaves the
  durable runtime supervisor running.

### Connectivity

- Controller relay disconnection pauses new exposure but does not erase pair
  state.
- Worker-specific availability is represented separately for each leg.
- Reconnect resumes by Worker cursor and is followed by a fresh full snapshot.
- Duplicate Worker events are deduplicated by Worker identity and immutable
  event ID.
- A cursor gap forces a full snapshot before further lifecycle decisions.

## Relay Protocol

Define versioned end-to-end envelopes between Strategy Runtime and Worker:

- `worker_read_requested` / `worker_read_completed`;
- `worker_effect_requested` / `worker_effect_outcome`;
- `worker_snapshot`;
- `worker_broker_delta`;
- `worker_stream_resumed`;
- `worker_stream_gap`.

Every request contains Trader identity, Worker identity, runtime command ID,
request or effect ID, protocol version, expiry where applicable, payload hash,
and correlation metadata. Every Worker fact contains Worker identity, recovery
epoch, event ID or snapshot baseline, observed time, and the originating effect
ID when known.

The controller adds transport metadata in a separate envelope section. It must
not rewrite the end-to-end payload.

## Audit and Management

- Audit is a copy of end-to-end commands, outcomes, facts, identity decisions,
  and delivery evidence; it is not the source used by the Strategy Runtime for
  trading decisions.
- The management site may retain identity, certificate, Worker health,
  connectivity, and opaque audit inspection. It must not expose pair or trade
  controls.
- Existing pair, analysis, intent, manual-trade, protection, cancellation, and
  emergency-flatten management pages and endpoints are removed.
- Historical trading records may remain read-only for retention and incident
  evidence, but no historical record may be resumed as live controller state.
- If the Strategy Runtime is unavailable, Workers retain account-local safety.
  A separate break-glass Worker command may be retained, but it must be
  local to the Worker operator, explicit, and outside the controller and
  ordinary pair orchestration.

## Migration Plan

### Phase 1 - Establish the Combined Runtime

- Move `RealtimeArbitrage`, Trader JSONL session handling, message correlation,
  and local command handling into one process.
- Introduce the Strategy Runtime SQLite/WAL and recover it before signal
  processing.
- Replace raw dictionaries at the in-process lifecycle seam with typed values.
- Keep the existing controller transport temporarily as an adapter.
- Do not add a Trader Execution interface.

Exit condition: strategy policy and all pair decisions use one in-process pair
aggregate, and restart tests recover it from local storage.

### Phase 2 - Reduce the Controller to a Relay

- Relay Worker receipts, snapshots, orders, positions, and deltas directly to
  the authenticated Strategy Runtime.
- Add Worker cursors, replay, gap detection, and full-snapshot recovery.
- Bind actual broker tickets and fills into the local pair aggregate.
- Make risk, integrity, reverse, and shutdown paths consume only that aggregate.
- Stop creating controller trading commands, intents, protected pairs, pair
  recoveries, execution identities, or semantic lifecycle events.

Exit condition: the runtime never queries a controller projection to determine
broker or pair state, and the controller can route the protocol without
deserializing its trading payload.

### Phase 3 - Move Pair Orchestration

- Move paired preflight, dispatch, compensation, and close convergence from
  controller functions into the Strategy Runtime.
- Preserve deterministic Worker effect IDs and Worker no-blind-retry
  guarantees.
- Route paired commands as ordinary authenticated end-to-end Worker envelopes.
- Keep old controller pair execution disabled behind a temporary migration
  switch for rollback.

Exit condition: no new pair lifecycle record is created in the controller
ledger, and all paired execution scenarios pass through the Strategy Runtime.

### Phase 4 - Remove the Old Ownership

- Delete controller hedged-entry execution, protected-pair state, pair recovery
  reducers, strategy-facing protected-pair commands, and duplicated tests.
- Delete controller intent execution, cancellation, paired preflight,
  management emergency flatten, manual pair trading, and product-pair trading
  selection.
- Delete management pair, intent, analysis, manual-trade, protection,
  cancellation, and emergency-flatten pages, routes, and mutation endpoints.
- Remove the Trader subprocess/JSONL gateway from realtime strategy startup.
- Replace controller semantic lifecycle events with relay delivery and audit
  events.
- Migrate or archive historical controller pair records for read-only audit;
  never import them as live Strategy Runtime state.

Exit condition: deleting the Strategy Runtime would force pair complexity back
into callers, while deleting controller pair code removes complexity without
redistributing it. This is the deletion test for the new seam.

### Phase 5 - Operational Cutover

- Start only with broker-verified empty accounts.
- Run dry-run and shadow observation before enabling Worker effects.
- Enable one Strategy Runtime and one approved Worker pair.
- Prove restart recovery at every durable boundary before broader rollout.
- Remove the migration switch only after the old controller path has remained
  unused through the agreed observation period.

## Test Strategy

Test through the combined Strategy Runtime interface with in-memory relay and
scripted Worker adapters plus a temporary SQLite/WAL store.

Required scenario matrix:

- crash before either send;
- crash after first delivery and before second delivery;
- one accepted send and one rejected send;
- send crossed `send_started` with lost receipt;
- both receipts received before either broker delta;
- broker delta received before its command outcome;
- duplicate and out-of-order Worker events;
- Worker reconnect with a new recovery epoch;
- Strategy Runtime restart in every lifecycle state;
- controller relay outage during entry, active monitoring, and close;
- broker SL/TP closes one leg;
- external order, position, volume, side, ticket, SL, or TP change;
- Ctrl+C and host shutdown while active or uncertain;
- final empty proof followed by a new entry.

Tests assert observable Worker effects, persisted desired state, emitted
strategy decisions, restart outcome, and final broker facts. They must not
assert private reducer structure or SQLite table layout.

## Acceptance Criteria

- Exactly one durable module owns protected-pair lifecycle.
- Strategy decisions use broker facts delivered directly from Workers, never a
  controller-derived pair projection.
- The controller cannot transition pair state or choose a compensating broker
  effect.
- No effect that crossed Worker `send_started` is blindly resent.
- A Strategy Runtime restart deterministically reaches verified active,
  broker-verified empty, or explicit `NEEDS_HUMAN`.
- Ctrl+C and shutdown use the same durable desired-empty path as every other
  exit.
- Controller audit delay or projection failure cannot block or change a
  trading decision.
- The management site and management API expose no pair or trade operation.
- Existing identity, route authorization, certificate revocation, Worker MT5
  exclusivity, Worker serialization, and Worker effect-journal guarantees are
  preserved.
- Obsolete controller pair lifecycle and Trader subprocess code are deleted,
  not retained as a second fallback implementation.

## Out of Scope

- Extracting a reusable Trader Execution module.
- Supporting more than two legs or multiple Strategy Runtimes sharing one
  active pair.
- Changing signal selection, sizing formula, loss thresholds, or market-data
  edge calculation.
- Replacing controller identity, certificate, Worker administration, or
  external-network deployment.
- Replacing Worker MT5 ownership, scheduler, or effect journal.
- High-availability replication of the Strategy Runtime WAL.

## Superseded Specifications

This specification supersedes the trading ownership and management-trading
decisions in:

- `.scratch/trader-intent-execution/spec.md`;
- `.scratch/lifecycle-owned-pair-management/spec.md`;
- management manual-pair and product-pair trading requirements elsewhere in
  `.scratch/`.

Identity enrollment, authenticated relay, Worker administration, and immutable
audit requirements remain applicable where they do not require the controller
or management site to understand or operate a trade or pair.
