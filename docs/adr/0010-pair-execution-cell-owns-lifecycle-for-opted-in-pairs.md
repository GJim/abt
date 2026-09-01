# Pair Execution Cell Owns Lifecycle for Opted-In Pairs

Supersedes ADR-0009's lifecycle-owner placement, but only for Worker pairs
running the Pair Execution Cell execution mode. ADR-0009's Strategy Runtime
remains the lifecycle owner for every pair that is not paired into a cell, and
the two owners never run live for the same Worker pair at the same time (see
"Execution-mode exclusivity" below).

This ADR was revised after the `.scratch/low-latency-pair-execution-cell`
specification replaced the originally shipped controller-leased,
arm/commit-based cell with an immediate-dispatch design, and revised again
after that specification replaced administrator-created, Trader-bound routes
and pre-approved Built product universes with Worker-initiated pairing and
Worker-performed product discovery. It records the **specified** design,
including the two-phase pairing workflow, `route_id` and `universe_generation`,
the `UNPAIRING` route state, role-specific configuration authority, and the
Pairing Acceptance payload. The pairing, discovery, policy-construction,
configuration-default, and `trader_id`-removal parts of that design are not yet
implemented in `abt.pair_cell`, `abt.worker.pair_cell_adapter`,
`abt.controlplane.ledger`, or `abt.controlplane.service`; see "What changed
since the original decision" and the specification's Migration and Removal
section. Because this ADR already describes the target, implementation
verifies it against the shipped code rather than rewriting it afterwards.

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

The first Pair Execution Cell shipped with controller-issued Pair Leases and
Pair Contracts, fixed per-Worker volumes, and a local arm/commit round trip
with a common future execution time. Measurement showed this still delayed
the first broker send and made the controller part of the execution
protocol, and fixed volumes were unsafe across products with different
margin and volume constraints. The revised specification removes all of
that: leader/follower is a durable route property instead of a lease,
volume is computed locally per product per hour instead of fixed, and entry
is one immediate leader dispatch plus one immutable follower attempt instead
of an arm/commit handshake.

That revision still left the pair unable to form itself. A route had to be
created by an administrator through the controller, was bound to a Trader
identity the cell never used for any trading decision, and could only trade a
pre-analyzed, explicitly Built cross-server product universe distributed to
each Worker as configuration. None of that can exist before the pair does: a
Built universe for a Worker pair presupposes the pair, the `trader_id` made
the controller look like a trading identity holder, and every deployment
needed an out-of-band admin step before any Worker could trade. The cell also
refused to start at all without a configuration file naming the route, the
built universe, and both budgets, which is an unreasonable prerequisite for an
unattended Worker.

## Decision

For a Worker pair running the Pair Execution Cell execution mode, the
**leader Worker named by that pair's durable route** is the pair's lifecycle
owner instead of the Strategy Runtime. The same
`abt.pair_cell.PairExecutionCell` module runs on both paired Workers; leader
or follower behavior comes from the durable route, not from a lease, epoch, or
trading TTL.

That route is **established by the Workers themselves, not by an
administrator**. A Worker declares a desired role on its first unpaired
connection, and an omitted role means it is an available follower. A leader
asks the controller for the currently connected, available, unpaired
followers, selects one interactively or by an explicit
`--follower-worker-id`, and proposes the pairing over the authenticated
controller. A leader on a non-TTY process without that flag remains
authenticated and unpaired with an actionable diagnostic rather than blocking
on input, and an interactive leader shown an empty list behaves the same way.
The selected follower accepts automatically, with no human step, only when it
can prove locally that it is broker-verified empty, has no unresolved attempt
or effect, is entry-ready enough to pair, can read a strictly positive balance
in a USD account, and is still unpaired.

Pairing runs as a **two-phase control workflow**, not as a single
compare-and-swap, because the follower's acceptance decision sits between the
two steps and belongs to the follower. One atomic controller transaction
generates a unique `proposal_id` and reserves **both** Workers under a
30-second **pairing-reservation timeout**; the controller then forwards the
proposal; the follower accepts or refuses; a second atomic transaction
CAS-creates the route only if that reservation is still valid and both Workers
remain eligible. Follower refusal, reservation timeout, a leader or follower
disconnect, or a failed final commit releases the reservation entirely,
leaving no route, no role assignment, no retained execution-mode claim, and no
durable local pairing state. Two leaders selecting the same follower therefore
produce exactly one route and one deterministic conflict, detected at phase
one. Both transactions enforce per-Worker uniqueness: neither Worker may appear
in another route, another live reservation, or a conflicting live
strategy-runtime ownership. The pairing-reservation timeout is control-plane
metadata only — never a trading lease, a route TTL, or a fencing token.

The route carries **no Trader identity**. Durable route identity is the ordered
leader/follower Worker pair plus a controller-generated, globally unique
`route_id`, and `trader_id` is removed from the Pair Execution Cell route, its
envelopes, its ownership records, its configuration, and its controller APIs.
This removal is scoped to the Pair Execution Cell; the legacy Strategy Runtime
and Trader RPC surfaces keep their Trader identity unchanged.

`route_id` is not a lease epoch. Every new pairing receives a new `route_id`
and a `route_id` is never reused, but it has no TTL, is never renewed, never
fences a broker effect, and confers no trading authorization. Discovery has its
own separate identifier, `universe_generation`, unique within a route. The
Worker-local `recovery_epoch` remains a third, unrelated concept: market
evidence about one Worker's own broker session, carried on quote evidence and
never compared with `route_id`.

The **controller's route record is authoritative** for relay permission and
for which Worker holds which role; each Worker's durable route copy is
recovery evidence about its own participation. When the two disagree, the
Worker immediately removes entry readiness and raises a metadata
reconciliation. It never discards, closes, hides, or infers local broker
exposure because of a metadata disagreement, and it never adopts a role the
controller does not assign.

The route is durable across reconnect and Worker restart until an explicit
safe unpair. A durable route always wins over startup role defaults and role
flags, so a reconnecting Worker resumes its assigned role and never re-enters
follower selection. A safe unpair runs through an explicit **`UNPAIRING`**
route state that either authenticated Worker may enter by command. Entering it
immediately removes entry readiness on both sides and forbids starting any new
attempt, while never blocking protection or containment of exposure already
owned. Each Worker then asserts broker-verified emptiness and terminal
attempts/effects, and each assertion binds to `route_id` **plus** that Worker's
current local attempt/effect state version; any new or changed local effect
advances that version and invalidates the assertion. The controller removes the
route only on two fresh, currently-bound assertions, and never derives those
facts itself. Either Worker may cancel `UNPAIRING` and return the route to
normal operation. Emergency identity revocation stays a separate mechanism: it
removes authentication, relay, and entry readiness, but never implies broker
`EMPTY` and is never a safe unpair.

Only the leader creates entry attempts; the follower validates and executes an
assigned leg but never derives an independent edge or terminal pair state. The
leader holds and publishes the one canonical strategy policy, but it does not
author the follower's risk numbers. The follower's acceptance carries a
**Pairing Acceptance payload** through the opaque relay, containing its
`proposal_id`, `worker_id`, its **frozen startup balance** with account
currency, and its own authored risk tunables — `maximum_margin_fraction`,
`daily_loss_fraction`, `trade_loss_fraction`, and
`maximum_loss_per_trade_usd`. The leader copies those **verbatim** into the
canonical policy's `follower_risk` block and may not recompute, clamp,
rescale, or substitute them; a missing, malformed, non-USD, or non-positive
payload makes the leader fail closed without publishing a policy. Shared policy
(`mode`, `entry_edge_points`, `quote_max_age_seconds`,
`quote_max_skew_seconds`, `follower_confirmation_timeout_seconds`,
`flatten_at_ny`, `maximum_holding_seconds`) is leader-authored alone.

Before quote processing begins the leader sends the follower that canonical,
versioned policy and its hash over the opaque relay. The follower verifies its
own block byte-for-byte against what it sent and durably accepts that exact
policy only while its own account is broker-verified empty. **Both Workers
persist the full canonical policy content, not only its hash**, so a restarted
Worker never has to re-request the policy to know what it agreed to.

Each Worker also continuously publishes a **versioned
remaining-allowed-leg-loss summary** to its peer. The leader must hold a
current peer summary before creating an attempt and may assign a leg no more
loss than its owner's published allowance; the follower independently rejects
an attempt whose assigned loss exceeds its **current local** allowance, which
is local risk arithmetic rather than market evaluation.

No Pair Execution Cell configuration file is required; when none is present the
runtime synthesizes its defaults, the default execution mode is `live`, and
each Worker's default `strategy_budget_usd` is its own startup broker account
**balance** frozen into that pairing's policy. A supplied file has
**role-specific authority**: a follower file may set only its own four risk
tunables and its own `allow_live` safety guard, never shared edge, quote,
timeout, flatten, or `mode` values; a leader file may set shared policy and its
own risk, never the follower's. `mode` is pair-level and leader-authored, and a
follower with `allow_live = false` **fails closed** against a `live` policy —
it refuses with an explicit reason and stays paired, safe, and idle rather than
rewriting policy or silently running shadow against a live leader. The leader
treats that refusal as peer-not-ready and originates nothing. The leader
continuously evaluates the discovered common product universe and freezes the
selected product and mirror direction into each immutable attempt.

The controller keeps ADR-0009's opaque-relay-plane role, plus exactly one
trading-adjacent authority: **arbitration of exclusive route ownership**. It
authenticates Workers, lists available followers to a proposing leader, runs
the two-phase reservation and route-creation transactions, records the two
bound Worker assertions that authorize a safe unpair, and relays
`abt.trader_protocol.PairCellRelayEnvelope` messages between the two Workers on
that route. It validates identity, `route_id`, the sending Worker's role on
that route record, protocol version, and payload size only; it never inspects
the opaque `payload` (policy, Pairing Acceptance payloads, catalog and
specification summaries, discovered universe content, sizing-plan and
remaining-allowance summaries, quotes, attempts, leg status), never calculates
an edge, never derives pair-lifecycle state, and never infers broker emptiness
or attempt terminality. It is an identity and authentication authority, a
route-ownership arbiter, and an opaque control-and-data relay -- not a trading
coordinator. A reconnect starts a new ordered live relay session; the
controller does not replay envelopes from a previous session, and it stores no
durable relay row or attempt projection to replay from.

### Execution-mode exclusivity

`ControlLedger.claim_pair_execution_mode` enforces that a Worker pair has at
most one **live** lifecycle owner: `strategy_runtime` and
`pair_execution_cell` are mutually exclusive for the same pair, and a Pair
Execution Cell pairing is rejected while the legacy Strategy Runtime still
holds the pair. That exclusivity is claimed for **both** Workers inside the
pairing reservation transaction and confirmed inside the final route-creation
transaction, so a pairing can never half-claim one Worker. The Pair Execution
Cell claims it with a fixed owner kind of `pair_execution_cell` and **no**
`trader_id`; the legacy Strategy Runtime keeps its own Trader identity on its
own claims. Route removal after a safe unpair releases both claims. A `shadow`
claim may run alongside either live owner so quote coherence, readiness, and
candidate-ranking decisions can be compared before cutover, using the same
immediate-entry decision path but a simulated fill instead of any
effect-journal mutation or broker write. Cutover from one live mode to the
other is an explicit operator action (release the old claim, safe-unpair or
pair as appropriate); the two production lifecycle owners are never
simultaneously enabled for the same pair.

### What is retained from ADR-0008 and ADR-0009

- Broker observation remains the authoritative fact; a durable ledger
  (SQLite/WAL in the Worker's `WorkerEffectJournal`, plus the Pair Execution
  Cell's own SQLite/WAL state for the durable route and `route_id`, role,
  `universe_generation`, frozen discovered universe, full canonical policy
  content and hash, sizing-plan versions including retained superseded ones,
  the local attempt/effect state version, readiness, quotes, attempts, per-leg
  effects, desired state, quarantine, and recovery) is the recovery target,
  not memory.
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

- The lifecycle owner is the Worker named leader by the pair's durable,
  Worker-established route, not the Strategy Runtime, not a lease, and not an
  administrator authorization. Readiness is computed continuously from local
  facts already pushed to the Worker (reconciliation-style snapshots/deltas);
  no new remote admission read is issued at signal time.
- A pair forms itself: role declaration, controller listing of available
  followers, leader proposal, automatic evidence-gated follower acceptance
  carrying a Pairing Acceptance payload, and a two-phase controller workflow —
  one atomic transaction reserving both Workers under a unique `proposal_id`
  and a 30-second pairing-reservation timeout, then a second atomic
  compare-and-swap creating the route. Refusal, timeout, either disconnect, or
  a failed commit releases the reservation completely. There is no
  administrator route-creation surface and no `trader_id` anywhere in that
  route, its envelopes, its ownership records, its configuration, or its
  controller APIs. Safe unpair and rediscovery both have conceptual Worker-side
  operator surfaces usable from either role.
- Durable route identity is the ordered Worker pair plus a
  controller-generated, globally unique, never-reused `route_id`. It is not a
  lease epoch: no TTL, no renewal, no fencing, no trading authorization.
  Discovery carries a separate `universe_generation`, unique within the route.
  Envelopes carry `route_id`; policy, attempt, product, sizing-plan, and
  quarantine state carry `universe_generation` where product identity applies.
  The controller validates `route_id` and sending role, never universe
  content. The Worker-local `recovery_epoch` stays separate market evidence.
- The controller's route record is authoritative for relay permission and
  assigned role; each Worker's durable copy is recovery evidence. A
  disagreement removes entry readiness and triggers metadata reconciliation,
  and never discards, closes, or infers local exposure.
- The route survives reconnect and restart and overrides any startup role
  default or flag. Removing it runs through an explicit `UNPAIRING` state that
  either authenticated Worker may enter by command; entering it immediately
  removes entry readiness on both sides and forbids new attempts while leaving
  containment fully available. Each assertion of broker-verified emptiness and
  terminal attempts/effects binds to `route_id` plus that Worker's current
  local attempt/effect state version, and any new or changed local effect
  invalidates it. The controller removes the route only on two fresh,
  currently-bound assertions; identity revocation is separate and never
  implies `EMPTY`.
- No configuration file is required. When absent, the runtime synthesizes
  defaults: `live` mode, this Worker's startup broker **balance** as its
  `strategy_budget_usd` frozen into the pairing's policy hash,
  `entry_edge_points` `4`, `maximum_margin_fraction` `0.10`,
  `daily_loss_fraction` `0.03`, `trade_loss_fraction` `0.02`,
  `maximum_loss_per_trade_usd` `40`, `quote_max_age_seconds` `1`,
  `quote_max_skew_seconds` `1`, `follower_confirmation_timeout_seconds` `5`,
  `flatten_at_ny` `16:00`, and no `maximum_holding_seconds`. Live by default
  is an intentional operator decision and does not relax any admission rule:
  pairing, discovery, clock calibration, sizing, protection, emptiness, and
  peer readiness must all pass first.
- Startup **balance** is deliberately chosen over legacy
  `realtime_arbitrage`'s startup **equity** because it excludes floating P&L
  from unrelated positions, giving a stable base for a frozen policy. It
  requires a USD account currency and a strictly positive balance and
  otherwise fails closed; no FX conversion of budgets or loss figures is
  performed. Legacy's extra 20% margin headroom is deliberately **not** carried
  forward — `maximum_margin_fraction` is the whole margin budget.
- `maximum_loss_per_trade_usd = 40` reuses the number from legacy
  `--emergency-stop-loss-usd` but is a deliberate **reinterpretation, not the
  same semantic**: here it is a hard per-trade admission cap, one of three
  ceilings bounding `allowed_leg_loss_usd` before any order is sent, rather
  than an emergency stop-out trigger. Rough emergency protection targets the
  current computed `allowed_leg_loss_usd`, not a fixed constant.
- Configuration authority is role-specific. A follower file may set only its
  own four risk tunables and its own `allow_live` guard; a leader file may set
  shared policy and its own risk, never the follower's. `mode` is pair-level
  and leader-authored; a follower with `allow_live = false` fails closed
  against a `live` policy, refusing explicitly and staying paired, safe, and
  idle rather than rewriting policy or silently running shadow.
- Product eligibility comes from Initial Compatible Product Discovery
  performed by the two Workers over the opaque relay, after the route exists
  and before quote processing, replacing the pre-approved Built universe and
  `ProductCatalog.active_built_products`. Compatibility uses the legacy
  `strategy/realtime_arbitrage.py` rules verbatim: a `trade_mode == 4` (full
  trading) precondition applied to each catalog **before** the intersection,
  exact symbol-name intersection with no suffix normalization, hard equality of
  `trade_calc_mode`, `contract_size`, `volume_min`, `volume_step`, and
  `allowed_directions`, both `LONG` and `SHORT` required, and a shared `FOK`
  preferred over a shared `IOC`. A missing, non-integer, or boolean
  `trade_mode` makes the catalog invalid rather than skipping one symbol.
  Base/profit currency, digits, point, and trade tick size are deliberately not
  required to be equal. The canonical execution point is the larger of the two
  points, the common `volume_max` is the smaller, and product identity is
  derived deterministically from the exact symbol plus the canonical
  compatibility summary hash. Equality of contract size, minimum volume, and
  volume step is therefore still an admission precondition, so an admitted FX
  product still has a 1:1 lot relationship. Discovery fails closed when either
  catalog is unavailable or invalid or when no compatible symbol survives, the
  universe is frozen for its `universe_generation`, and an hourly refresh may
  revalidate or suspend but never silently admit a newly appeared symbol and
  never changes `universe_generation`; an explicit rediscovery requires both
  accounts empty and installs a new `universe_generation` on the same route
  without producing a new `route_id`.
- Sizing and protection plans cannot exist before discovery. The ordering is
  route, discovery, policy acceptance, quote collection, then initial plans
  built from a current local quote and a current MT5 margin calculation, and
  only then candidate evaluation; no candidate is processed while any valid
  positive plan is missing. Each Worker then refreshes its own local plan for
  every discovered product and direction once per hour
  (`PairExecutionCell._refresh_sizing_plans`), from its
  `strategy_budget_usd`, `maximum_margin_fraction`, and current MT5
  margin-per-lot; a failed product refresh suspends only that product until
  the next hourly refresh. The two Workers exchange versioned sizing-plan
  summaries, and the common attempt volume is the lower of the two local
  capacities, snapped to the shared volume step; a product is ineligible
  when either side has no valid positive plan. No signal-time margin RPC is
  added.
- Each superseded plan version is retained and remains valid for validating an
  already-dispatched attempt for at least
  `follower_confirmation_timeout_seconds` plus the bounded relay-handling
  window, so an attempt in flight across a refresh boundary validates against
  its named immutable version instead of being guaranteed to fail. A retained
  version is never eligible for a new candidate.
- Each Worker separately enforces a `America/New_York` calendar-day realized
  -loss budget: `daily_loss_fraction` (default `0.03`) of its own
  `strategy_budget_usd`, decremented only by realized P&L from closed
  strategy-owned legs (floating P&L excluded). Each leg's allowed loss is
  the lower of `trade_loss_fraction` (default `0.02`) of budget,
  `maximum_loss_per_trade_usd`, and the Worker's own remaining daily
  allowance; take-profit targets the same USD amount 1:1. No entry is
  admitted when either Worker's allowance is exhausted. Each Worker
  continuously publishes a versioned remaining-allowed-leg-loss summary to its
  peer; the leader must hold a current peer summary before creating an attempt
  and may assign no leg more than its owner's published allowance, and the
  follower independently rejects an attempt whose assigned loss exceeds its
  current local allowance.
- Quote evidence carries broker tick time, recovery epoch, and a monotonic
  sequence; edge decisions use only broker-time freshness
  (`quote_max_age_seconds`) and cross-Worker skew bounds
  (`quote_max_skew_seconds`), never transport-observed time. These are
  leader-side candidate-admission checks only; the follower repeats neither
  after receiving an attempt.
- There is no controller-mediated dispatch, arm phase, commit phase, common
  future execution time, execution window, Pair Lease, or lease TTL. When
  the leader selects the best candidate
  (`PairExecutionCell._maybe_create_attempt`), it durably persists one
  immutable attempt and its own prepared broker effect, starts its own local
  monotonic confirmation timer, submits its own protected market order with
  cached rough emergency SL/TP, and concurrently relays that same immutable
  attempt to the follower -- all from the same durable decision, with no
  handshake in between. The follower performs non-market safety checks only
  (authenticated route with the current `route_id` and a route not in
  `UNPAIRING`, current `universe_generation`, exact accepted policy hash,
  duplicate/changed-content attempt IDs, broker-verified-empty and a locally
  eligible sizing plan version that is either current or a retained superseded
  one, matching lots, assigned leg loss within its current local allowance,
  quarantine, constructible rough protection) and, once they
  pass, immediately starts its own local monotonic confirmation timer and
  submits its own protected market order without waiting for a second
  leader message or re-checking quotes or edge. Same-ID/same-content
  delivery returns the durable outcome and never sends twice. Because there
  is no attempt-delivery-age check, a delayed attempt may still enter after
  the leader's timer has already expired and the leader has already
  contained its leg; that follower exposure remains bounded by broker
  -native rough protection and the follower's own receive-relative timer.
- `follower_confirmation_timeout_seconds` (default `5`) is the only
  entry-protocol timeout duration, and it is never a shared deadline: the
  leader starts one local monotonic timer at its own decision, and the
  follower starts a separate local monotonic timer at its own receipt of
  the attempt. The 30-second pairing-reservation timeout is control-plane
  metadata and is not an entry-protocol timeout. Before its timer expires, the
  leader must obtain exact
  broker-observed evidence (ticket, symbol, side, volume, fill price,
  attached rough SL/TP -- an order receipt alone is never sufficient) for
  both its own leg and the follower leg. Only once both exact positions are
  confirmed does each Worker compute precise, actual-fill-based 1:1 SL/TP
  and the pair become `ACTIVE`.
- An attempt is terminal only when both sides are proven safe: either both
  Workers produce fresh broker-observed empty facts, or both exact
  positions are broker-observed with precise protection applied. Missed
  confirmation, entry rejection, unknown-after-send outcomes, inconsistent
  evidence, stale quotes, or lost peer-session readiness never abandon an
  attempt outright; each Worker instead converges through its own local
  desired-`EMPTY` containment (cancel attempt-owned pending orders, close
  the exact attempt-owned position, require fresh broker-verified-empty
  facts) using only its own durable effects and exact broker facts. A
  best-effort `pair_entry_failed` notification accelerates the peer's
  containment but is never required for safety.
- Every item dequeued from the Worker's shared, serialized Trader RPC
  scheduler is terminalized exactly once, including durable-journal failures,
  MT5 exceptions, and missing receipts, so emergency containment and ordinary
  Worker RPC work stay runnable after any abnormal broker write.
- A confirmed entry retcode `10021` (`TRADE_RETCODE_PRICE_OFF`) durably
  quarantines that derived product identity for the Worker pair before
  ordinary asymmetric containment continues. Quarantine survives process
  restart, does not block unrelated eligible products, and can only be
  released by an explicit authenticated, audited operator action while no
  attempt is unresolved.

### What changed since the original decision

**First revision (shipped).** The originally shipped cell (`PairLease`,
`PairContract`, `arm_ack`/`commit`, `ttl_seconds`, `arm_timeout_seconds`,
`commit_lead_seconds`, `execution_expiry_seconds`, fixed
`leader_volume`/`follower_volume`) was replaced end to end by the
immediate-dispatch design above; none of that machinery remains in
`abt.pair_cell`, `abt.controlplane.ledger`, or `abt.controlplane.service`.
The controller's role shrank from issuing and fencing leases to one durable
route plus opaque relay; it stores no durable relay row or attempt
projection, so a reconnected session cannot be replayed into. Role changes
now require both accounts to be broker-verified empty and all prior attempts
terminal, exactly like any other policy change, instead of a lease handoff.

**Second revision (specified, not yet implemented).** Administrator-created,
Trader-bound routes and the pre-approved Built product universe are replaced
by Worker-initiated pairing and Initial Compatible Product Discovery. This ADR
describes that **specified target design**, so implementation must verify this
ADR and the domain glossary against the shipped code and update them only if a
name changes; there is no deferred "write the ADR afterwards" step. The
following is outstanding implementation work, enumerated in the
specification's Migration and Removal section:

- replace `ControlLedger.authorize_pair_route`, `revoke_pair_route`, and the
  `/api/admin/pairs/route` and `/api/admin/pairs/route/revoke` endpoints with
  Worker-facing listing, pairing-proposal, safe-unpair, and rediscovery
  surfaces, and implement the two-phase workflow: an atomic reservation
  transaction over both Workers under a unique `proposal_id` and a 30-second
  pairing-reservation timeout, then a separate final route-creation
  compare-and-swap, with per-Worker uniqueness enforced in both and full
  reservation release on refusal, timeout, disconnect, or failed commit;
- remove `trader_id` from the `pair_routes` schema, the Pair Execution Cell
  path of `claim_pair_execution_mode`, `abt.trader_protocol.PairRouteClaim`
  and `PairCellRelayEnvelope`, `abt.worker.pair_cell_adapter.PairRoute`, and
  every Pair Cell controller API, while leaving the legacy Strategy Runtime
  and Trader RPC surfaces untouched, and claim `pair_execution_cell`
  exclusivity for both Workers with the fixed owner kind inside the pairing
  transactions;
- add a globally unique, never-reused `route_id` and a separate
  `universe_generation`, durable-route precedence over startup role flags, the
  controller route record as the authority for relay and role, and the
  `UNPAIRING` state with state-version-bound two-sided assertions;
- add the Pairing Acceptance payload with verbatim `follower_risk` copying,
  the continuous versioned remaining-allowed-leg-loss summary with the
  follower's local re-check, and full canonical policy content persistence on
  both Workers;
- make `load_pair_cell_config` absence mean "synthesize defaults and run",
  implement role-specific configuration authority with the follower's
  `allow_live` fail-closed guard, reject
  route/`route_id`/`trader_id`/`built_products`/`eligible_products` in any
  supplied file, and add `--pair-cell-role` and `--follower-worker-id` to
  `abt.worker.cli` with non-blocking non-TTY leader behavior; and
- replace `BuiltProduct`, `ProductCatalog.active_built_products`,
  `WorkerProductCatalog`, and `StrategyPolicy.eligible_products` with the
  discovery protocol (including the `trade_mode == 4` catalog precondition)
  and derived product identity, re-keying quarantine accordingly, and
  implement the post-discovery plan construction order with superseded plan
  retention.

## Consequences

Two Worker processes now contain lifecycle logic (the Strategy Runtime and
the Pair Execution Cell module), but never for the same pair at the same
time, and never with the controller deriving pair state in either case. The
legacy Strategy Runtime path is retained only for the bounded migration
period described in the specification and is removed after acceptance,
leaving one lifecycle-owner implementation.

The second revision buys convenience and autonomous startup, and pays for it
in two places.

What is gained: a fresh pair of Workers can be deployed with no controller
admin action, no hand-authored configuration file, and no pre-analysis step.
An omitted role makes a Worker an available follower, a leader picks one and
pairs, defaults are synthesized from each Worker's own broker balance, and the
pair discovers its own tradable universe. Operating cost and the number of
ways a deployment can be half-configured both drop sharply.

What is lost: the pre-approved Built universe and the explicit administrator
authorization of each pair. Products are no longer vetted before they can be
traded — a symbol is admitted because two live broker catalogs both mark it
fully tradable and agree on calculation mode, contract size, minimum volume,
volume step, directions, and a filling mode, not because a human previously
analyzed the cross-server pair.
Likewise, no administrator now decides which two Workers may trade together;
any two authenticated Workers that are unpaired and safe can pair themselves.
The mitigations are that discovery uses the same `trade_mode == 4` precondition
and hard-equality rules the already-operated `strategy/realtime_arbitrage.py`
uses, that it fails closed on any missing catalog or empty intersection, that
the universe is frozen per `universe_generation` so it cannot silently grow,
and that both Workers remain individually enrolled and revocable.

What is deliberately kept central: the controller still arbitrates exclusive
route ownership, so two leaders can never claim the same follower and a Worker
can never be on two routes at once. That is the one fact a single arbiter must
own. Because the follower's decision sits between reservation and route
creation, that arbitration is honestly two transactions with a short-lived,
releasable reservation rather than one impossible compare-and-swap, and the
30-second timeout that bounds it is control-plane bookkeeping, not a trading
lease. The controller still owns no trading policy, no lifecycle state, no
product universe, and no emptiness or terminality determination — those stay
with the Workers and their broker observations — but its route record is the
authority on relay permission and assigned role, with each Worker's durable
copy serving only as recovery evidence.

Splitting policy authorship is the other deliberate cost. The leader still
publishes exactly one canonical policy, but the follower authors its own budget
and its own risk limits and the leader may only transcribe them, and the
follower's `allow_live = false` guard can stop the pair rather than diverge
from it. That is more moving parts than a single-author policy, and it buys the
guarantee that no Worker can quietly size or risk another Worker's account.

Live-by-default is an explicit operator decision that accepts autonomous
production trading in exchange for unattended deployment; it is bounded by the
unchanged fail-closed admission chain (pairing, discovery, clock calibration,
quote collection, sizing and protection plans, protection constructibility,
emptiness, peer readiness) rather than by a manual gate, and by the follower's
ability to refuse live outright.
