# Pair Execution Cell Owns Lifecycle for Opted-In Pairs

Supersedes ADR-0009's lifecycle-owner placement, but only for Worker pairs
that explicitly opt into the Pair Execution Cell execution mode. ADR-0009's
Strategy Runtime remains the lifecycle owner for every pair that has not
opted in, and the two owners never run live for the same Worker pair at the
same time (see "Execution-mode exclusivity" below).

## Context

ADR-0009 kept the complete cross-Worker pair lifecycle in one durable
Strategy Runtime process to avoid splitting a single-owner invariant across
an unstable interface. That runtime performs its edge decision, admission
checks, and dispatch coordination centrally, then relays commands to each
Worker over the controller. The `.scratch/low-latency-pair-execution-cell`
specification measured that this centralized round trip is the largest
controllable contributor to entry latency: signal-time cross-network reads
and relay round trips, not MT5 execution itself, consumed most of the time
between `entry_signal` and the durable command. Colocating the edge decision
and MT5 execution on the same account Worker removes that round trip, but
only if exactly one lifecycle owner still exists per pair.

## Decision

For a Worker pair whose operator has opted it into the Pair Execution Cell
execution mode, the **leased leader Worker** is the pair's lifecycle owner
instead of the Strategy Runtime. The same `abt.pair_cell.PairExecutionCell`
module runs on both paired Workers; an immutable, epoch-fenced lease
(`abt.pair_cell.PairLease`) selects leader or follower behavior. Only the
leader creates entry attempts; the follower validates and executes an
assigned leg but never derives an independent edge or terminal pair state.

The controller keeps ADR-0009's opaque-relay-plane role. It issues and
fences pair leases (`ControlLedger.issue_pair_lease`), and it relays
`abt.trader_protocol.PairCellRelayEnvelope` messages between the two leased
Workers (`ControlLedger.record_pair_relay`, wired in
`abt.controlplane.service`). It validates identity, route, lease epoch and
expiry, protocol version, and payload size only; it never inspects the
opaque `payload` (quotes, arm/commit, leg status) and never calculates an
edge or derives pair-lifecycle state. Controller audit for attempt-scoped
envelopes is a durable, replay-deduplicated log for operator investigation,
not an authoritative pair projection.

### Execution-mode exclusivity

`ControlLedger.claim_pair_execution_mode` enforces that a Worker pair has at
most one **live** lifecycle owner: `strategy_runtime` and
`pair_execution_cell` are mutually exclusive for the same pair, and issuing a
Pair Execution Cell lease is rejected while the legacy Strategy Runtime still
holds the pair. A `shadow` claim may run alongside either live owner so
quote coherence, readiness, and edge decisions can be compared before
cutover without any effect-journal mutation or broker write. Cutover from
one live mode to the other is an explicit operator action (release the old
mode's claim, then claim the new one); the two production lifecycle owners
are never simultaneously enabled for the same pair.

### What is retained from ADR-0008 and ADR-0009

- Broker observation remains the authoritative fact; a durable ledger
  (SQLite/WAL in the Worker's `WorkerEffectJournal`, plus the Pair Execution
  Cell's own SQLite/WAL state for lease, contract, readiness, quotes,
  attempts, per-leg effects, desired state, and recovery) is the recovery
  target, not memory.
- `prepared` before every MT5 write, irreversible `send_started` immediately
  before the call, and no resend of an effect ID that crossed that boundary.
- One pair, one lifecycle owner at a time; the follower never becomes a
  second owner, and the controller never becomes a hidden third one.
- MT5 `None`/malformed/unavailable records are read failures, never an empty
  or ready fact, in every readiness, verification, containment, and final
  -empty check.
- Every close path (risk, cutoff, timed exit, shutdown, asymmetric
  containment, integrity divergence) converges through one owned
  desired-`EMPTY` implementation, exactly as ADR-0008 requires for a single
  account.

### What changes for opted-in pairs

- The lifecycle owner is the leased leader Worker, not the Strategy Runtime.
  Readiness is computed continuously from local facts already pushed to the
  Worker (reconciliation-style snapshots/deltas and a locally maintained
  protection calibration); no new remote admission read is issued at signal
  time.
- Quote evidence carries broker tick time, recovery epoch, and a monotonic
  sequence; edge decisions use only broker-time freshness and cross-Worker
  skew bounds, never transport-observed time.
- Entry uses a local arm/commit protocol between exactly two Workers instead
  of controller-mediated dispatch: the follower arms from local checks only,
  and the leader commits with one common execution time and a short
  execution expiry re-checked immediately before every `order_send`.
- Arming is not committing. Each Worker persists a per-attempt commit
  authorization (the leader when it issues the commit, the follower when it
  validates that exact commit) and refuses `order_send` without it, so a
  dropped `arm_ack` or `commit` can never produce a unilateral entry. Lease
  authority, account admissibility, quote freshness, and the absolute expiry
  are all re-checked adjacent to the send.
- An attempt is terminal only when both sides are proven safe: each Worker
  produces fresh broker-observed empty facts, or the leader never authorized
  a commit and the follower therefore provably could not have sent. Arm
  timeout, execution expiry, lease loss, stale quotes, and lost admissibility
  notify the peer and converge through the same desired-`EMPTY` path instead
  of resetting an attempt the peer may already have executed.
- Every item dequeued from the Worker's shared, serialized Trader RPC
  scheduler is terminalized exactly once, including durable-journal failures,
  MT5 exceptions, and missing receipts, so emergency containment and ordinary
  Worker RPC work stay runnable after any abnormal broker write.

## Consequences

Two Worker processes now contain lifecycle logic (the Strategy Runtime and
the Pair Execution Cell module), but never for the same pair at the same
time, and never with the controller deriving pair state in either case. The
legacy Strategy Runtime path is retained only for the bounded migration
period described in the specification and is removed after acceptance,
leaving one lifecycle-owner implementation.
