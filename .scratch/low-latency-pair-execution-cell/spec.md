# Low-Latency Pair Execution Cell

Status: ready-for-agent

## Problem Statement

The realtime arbitrage strategy currently detects an entry signal in the
Strategy Runtime and then performs synchronous cross-network work before either
account reaches its broker. In the observed run, the median time from
`entry_signal` to the FTMO broker position was 1.586 seconds, while the median
time to the Runtime's durable receipt was 2.970 seconds and the median time to
broker-verified `ACTIVE` was about 4.467 seconds.

The largest controllable entry delay was not MT5 execution. About 1.364 seconds
were spent preparing the durable command and fetching entry-admission snapshots
from both Workers after the signal already existed. The actual median interval
from prepared command to FTMO broker position was about 0.206 seconds.

This architecture makes a short-lived cross-broker edge pay for remote reads,
relay round trips, and centralized orchestration after detection. By the time
both legs are submitted, the quotes that created the opportunity may already
be stale. The operator wants strategy decisions and account-local MT5
execution placed closer together so that continuously maintained account
readiness can replace signal-time admission reads.

Simply running the same strategy independently on both Workers is unsafe.
Workers can receive different quote revisions, observe different connection or
account states, and make different decisions at the same nominal time. Two
independent lifecycle owners would permit split-brain entry, duplicate
attempts, stale execution, and contradictory compensation.

Likewise, opening an unprotected market position and calculating its first
SL/TP only after fill creates a crash and disconnect window in which the
position has no broker-native protection. Cross-broker execution cannot be
atomic, so the design must reduce latency without weakening durable effect
journaling, authoritative broker observation, asymmetric-entry containment,
or broker-verified empty convergence.

## Solution

Introduce a low-latency **Pair Execution Cell** that runs the same module on
both paired account Workers while preserving exactly one protected-pair
lifecycle owner.

The controller issues a short-lived, epoch-based pair lease that assigns one
Worker as leader and the other as follower. Both Workers receive the same
immutable pair contract, including role assignment, eligible symbols,
directions, volume plans, risk limits, quote freshness policy, execution
deadlines, and conservative emergency protection. The leader alone may create
a pair entry attempt. The follower validates and executes a leader-created
attempt but never independently creates a competing attempt.

Both Workers continuously publish versioned local quote evidence through the
controller. The controller authenticates, relays, and audits these messages
without calculating an edge or deriving pair state. Each Pair Execution Cell
maintains its local quote, the latest admissible peer quote, current
account-local broker facts, margin headroom, terminal health, effect-journal
health, and lease state. A Worker is `PAIR_READY` only while all entry
invariants remain continuously true.

When the leader detects an edge from a coherent pair of fresh quote revisions,
it durably creates one immutable attempt and asks the follower to arm it. The
follower performs only local, memory-resident and journal-backed admission
checks. After both sides are armed, the leader emits one commit carrying a
near-future common execution time and a short absolute expiry. Each Worker
checks its lease, attempt, readiness, quote age, and deadline immediately
before its local MT5 call. A late or invalid commit is rejected without broker
send.

The initial market order includes conservative emergency SL/TP derived before
the signal from current symbol constraints and risk policy. A position is
therefore never intentionally opened naked. After fill, each Worker uses its
actual broker-observed entry price to calculate and journal refined SL/TP.
Both refined protections must be proven on the exact owned tickets before the
leader declares the pair `ACTIVE`.

Every leg reports durable effect state and broker observation through the
controller. A peer acknowledgement or successful order receipt is not pair
proof. If either leg rejects, expires, becomes uncertain, fails protection
refinement, or cannot be observed, the leader changes desired state to
`EMPTY` and performs evidence-driven convergence. Owned orders are cancelled,
owned positions are closed, and both Workers must produce fresh authoritative
empty facts before the attempt is terminal.

The Pair Execution Cell replaces the Strategy Runtime as the deployment
location of realtime entry and protected-pair orchestration for opted-in
pairs. It preserves the architectural invariant behind ADR-0009—one pair has
one lifecycle owner—but moves that owner to the leased leader Worker. The
controller remains the identity, lease, opaque relay, and immutable audit
plane. A new ADR records this ownership migration and supersedes ADR-0009 only
for Pair Execution Cell pairs.

## User Stories

1. As a strategy operator, I want entry decisions colocated with Worker execution, so that a short-lived edge does not wait for signal-time cross-network broker reads.
2. As a strategy operator, I want both Workers continuously prepared before an edge appears, so that readiness work is removed from the critical entry path.
3. As a strategy operator, I want one Worker designated as pair leader, so that only one module can create an entry attempt.
4. As a strategy operator, I want the other Worker designated as follower, so that it can execute its assigned leg without becoming a competing lifecycle owner.
5. As a strategy operator, I want leadership protected by an epoch-based lease, so that a restarted or partitioned Worker cannot create attempts under stale authority.
6. As a strategy operator, I want each lease to identify the exact Worker pair, so that authority cannot be reused with another account.
7. As a strategy operator, I want each pair contract immutable within a lease epoch, so that both Workers execute the same roles and risk policy.
8. As a strategy operator, I want pair role assignment completed before market observation begins, so that no entry-time negotiation is required.
9. As a strategy operator, I want the leader to be the only edge decision maker, so that different quote arrival order cannot produce split-brain attempts.
10. As a strategy operator, I want the follower to reject self-originated entry attempts, so that identical deployed code cannot accidentally create a second lifecycle.
11. As a strategy operator, I want local and peer quotes identified by Worker, symbol, recovery epoch, and monotonic sequence, so that every edge can be tied to exact evidence.
12. As a strategy operator, I want broker tick time carried with every quote, so that transport observation time cannot make an old broker quote appear fresh.
13. As a strategy operator, I want out-of-order peer quotes ignored, so that network reordering cannot recreate an expired edge.
14. As a strategy operator, I want a recovery-epoch change to invalidate prior quotes, so that reconnect cannot mix evidence from two terminal sessions.
15. As a strategy operator, I want an upper bound on local quote age, so that a Worker never trades from its terminal's stale tick cache.
16. As a strategy operator, I want an upper bound on peer quote age, so that delayed relay delivery cannot create a false edge.
17. As a strategy operator, I want an upper bound on cross-Worker quote skew, so that an edge compares approximately contemporaneous markets.
18. As a strategy operator, I want quote freshness measured from broker evidence and calibrated clocks, so that machine clock drift is not mistaken for opportunity.
19. As a strategy operator, I want the controller to relay quote evidence without deriving edges, so that pair policy remains outside the control plane.
20. As a strategy operator, I want quote relay interruption to remove `PAIR_READY`, so that a Worker cannot keep opening from frozen peer prices.
21. As a strategy operator, I want account facts maintained continuously, so that the entry path does not fetch a new remote admission snapshot after every signal.
22. As a strategy operator, I want any local order or position to remove entry readiness immediately, so that new exposure cannot overlap unresolved exposure.
23. As a strategy operator, I want terminal or account identity changes to remove readiness immediately, so that a pair cannot trade through the wrong MT5 session.
24. As a strategy operator, I want insufficient margin headroom to remove readiness before an edge, so that margin calculation is not deferred to the critical path.
25. As a strategy operator, I want effect-journal integrity required for readiness, so that a Worker cannot trade without durable no-blind-retry guarantees.
26. As a strategy operator, I want readiness represented separately for each Worker, so that one healthy leg cannot conceal an unavailable peer.
27. As a strategy operator, I want one immutable attempt ID for both legs, so that receipts, observations, protection, and compensation remain correlated.
28. As a strategy operator, I want the attempt to record both quote revisions, so that the precise entry decision can be audited and replayed.
29. As a strategy operator, I want the attempt persisted before the follower is armed, so that a leader crash cannot erase a possible execution.
30. As a strategy operator, I want the follower to durably deduplicate attempts, so that controller replay cannot produce a duplicate market order.
31. As a strategy operator, I want attempt-ID reuse with changed content rejected, so that deduplication cannot hide a modified order.
32. As a strategy operator, I want follower arming to perform only local checks, so that it remains fast without trusting the leader's account assumptions.
33. As a strategy operator, I want both Workers armed before commit, so that a known-unready leg prevents either market send.
34. As a strategy operator, I want an armed attempt to expire quickly, so that a delayed commit cannot execute a vanished edge.
35. As a strategy operator, I want a common near-future execution time, so that both MT5 sends begin within a bounded cross-Worker skew.
36. As a strategy operator, I want a late commit rejected before broker send, so that a Worker never chases an expired opportunity.
37. As a strategy operator, I want the final expiry check adjacent to `order_send`, so that queue and scheduling delay cannot bypass the deadline.
38. As a strategy operator, I want MT5 access to remain serialized per Worker, so that low latency does not introduce unsafe terminal concurrency.
39. As a strategy operator, I want entry work prioritized over background reconciliation and sizing work, so that a prepared opportunity is not blocked by non-critical reads.
40. As a strategy operator, I want emergency close and recovery work prioritized above new entry, so that latency optimization never delays risk reduction.
41. As a strategy operator, I want every market order to include conservative emergency SL/TP, so that a crash immediately after fill does not leave a naked position.
42. As a strategy operator, I want emergency protection computed before the signal, so that it adds no remote calculation to the critical path.
43. As a strategy operator, I want emergency protection to respect current stop distance, freeze distance, digits, and tick size, so that the initial protected order is broker-valid.
44. As a strategy operator, I want entry rejected when valid emergency protection cannot be constructed, so that speed never requires intentional naked exposure.
45. As a strategy operator, I want refined protection calculated from the actual broker-observed fill, so that final risk targets are based on real entry price.
46. As a strategy operator, I want refined protection journaled as a new broker effect, so that a lost response cannot cause blind modification retry.
47. As a strategy operator, I want both exact tickets observed with refined protection before `ACTIVE`, so that receipts alone cannot prove a protected pair.
48. As a strategy operator, I want refinement failure to change desired state to `EMPTY`, so that a pair cannot remain active under incomplete protection.
49. As a strategy operator, I want a rejected leg to trigger immediate asymmetric-entry containment, so that the surviving leg's directional exposure is minimized.
50. As a strategy operator, I want an expired-not-started leg distinguished from an unknown-after-send leg, so that recovery does not guess whether a position exists.
51. As a strategy operator, I want an unknown broker outcome resolved from fresh local observation, so that an effect that may have executed is never blindly resent.
52. As a strategy operator, I want owned pending orders cancelled before positions are closed, so that a later fill cannot recreate exposure during containment.
53. As a strategy operator, I want only exact attempt-owned tickets mutated, so that manual or unrelated strategy exposure is never closed by shape matching.
54. As a strategy operator, I want both Workers freshly observed empty before the attempt is terminal, so that accepted close receipts cannot create false `EMPTY`.
55. As a strategy operator, I want MT5 `None` record results treated as read failures, so that unavailable broker facts cannot masquerade as an empty account.
56. As a strategy operator, I want a surviving protected leg closed when its peer cannot be proven, so that emergency protection is a fallback rather than permission for indefinite one-sided exposure.
57. As a strategy operator, I want a leader restart to recover its durable attempt before evaluating new quotes, so that restart cannot overlap an unresolved pair.
58. As a strategy operator, I want a follower restart to replay its durable leg outcome without re-executing it, so that recovery preserves exactly-once broker-effect semantics.
59. As a strategy operator, I want lease loss to stop new attempts but preserve active-pair recovery, so that authority loss does not abandon existing exposure.
60. As a strategy operator, I want a new leader lease blocked while an earlier epoch has unresolved broker effects, so that leadership transfer cannot create concurrent lifecycles.
61. As a strategy operator, I want controller audit copies sufficient to locate unresolved attempt evidence, so that operator investigation does not depend on transient logs.
62. As a strategy operator, I want active pair management to continue from broker facts during temporary quote-relay loss, so that communication loss does not erase desired state.
63. As a strategy operator, I want no new entry while either Worker is recovering, so that containment and opportunity execution cannot interleave.
64. As a strategy operator, I want active-pair integrity checks to compare exact tickets, volume, side, symbol, and protection, so that external changes trigger safe convergence.
65. As a strategy operator, I want risk, cutoff, timed exit, shutdown, and integrity divergence to use the same desired-empty path, so that every close has one recovery model.
66. As a strategy operator, I want pair lifecycle state emitted with the attempt and lease epoch, so that operational logs cannot mix different generations.
67. As a strategy operator, I want separate timing for quote observation, arm, commit, MT5 send start, broker receipt, position observation, protection verification, and terminal state, so that latency can be optimized from evidence.
68. As a strategy operator, I want broker server time explicitly calibrated, so that FTMO GMT+3 history can be compared correctly with UTC runtime events.
69. As a strategy operator, I want system-controlled signal-to-send latency measured separately from broker execution latency, so that release decisions target controllable delay.
70. As a strategy operator, I want cross-leg broker-send skew measured, so that the one-sided exposure window is visible.
71. As a controller operator, I want pair leases authenticated and auditable, so that only approved Worker pairs can coordinate execution.
72. As a controller operator, I want the controller to validate identity, route, lease envelope, size, and protocol version only, so that it does not become a second lifecycle owner.
73. As a controller operator, I want opaque relay messages durably associated with attempt IDs, so that delivery and replay remain inspectable without interpreting trades.
74. As a Worker operator, I want local account recovery to remain authoritative for my terminal, so that peer coordination cannot bypass account safety.
75. As a Worker operator, I want malformed or unauthorized pair messages rejected before journal mutation, so that the execution cell cannot be driven by an invalid peer.
76. As a maintainer, I want leader and follower behavior implemented by the same deep module, so that safety logic does not drift across two code paths.
77. As a maintainer, I want one high-level execution interface for callers and tests, so that pair behavior can be verified without mocking internal helper calls.
78. As a maintainer, I want deterministic clocks, relay, and MT5 adapters, so that timing races can be reproduced without sleeping or live brokers.
79. As a maintainer, I want crash tests at every durable effect boundary, so that restart behavior is demonstrated rather than inferred.
80. As a maintainer, I want the existing Strategy Runtime path retained behind an explicit migration switch during rollout, so that low-risk comparison is possible before cutover.
81. As a strategy operator, I want shadow mode to compare old and new edge decisions without broker writes, so that quote coherence and readiness can be validated first.
82. As a strategy operator, I want a controlled test-account rollout with small exposure, so that latency and asymmetric-entry behavior are measured before broad activation.
83. As a strategy operator, I want the legacy path removed after successful cutover, so that two production lifecycle owners cannot remain enabled indefinitely.

## Implementation Decisions

- Introduce one deep **Pair Execution Cell** module. The same implementation
  runs on both paired Workers; an immutable leased role selects leader or
  follower behavior.
- The module's external interface has three lifecycle entry points:
  activation with an immutable pair contract and lease, acceptance of typed
  local/peer/broker/clock events, and restart recovery. Callers do not manage
  arming, effect IDs, retries, protection refinement, or compensation.
- Typed events include local quote evidence, relayed peer quote evidence,
  readiness changes, leader arm and commit envelopes, Worker effect outcomes,
  broker snapshots and deltas, lease changes, and monotonic clock advancement.
- The interface returns the current observable pair result while injected
  adapters carry authenticated relay messages and serialized MT5 effects.
  Internal state names, scheduling mechanics, and journal tables remain hidden
  behind the seam.
- The primary test seam is this same Pair Execution Cell interface. Pair tests
  run one leader and one follower module connected by a deterministic relay
  adapter; no second public orchestration interface is introduced for tests.
- Exactly one Worker holds the lifecycle-owner role for a lease epoch. The
  follower owns only its account-local leg facts and effects; it does not
  calculate an independent edge or terminal pair state.
- The controller owns pair lease issuance, renewal, revocation, fencing epoch,
  route authorization, message delivery, replay, and immutable audit copies.
  It does not inspect quote values, calculate edges, assign directions after
  activation, or choose compensation.
- Pair leases bind leader Worker, follower Worker, trader identity, pair
  contract hash, issue time, expiry, and strictly increasing fencing epoch.
  Both Workers persist the highest accepted epoch and reject older envelopes.
- Lease expiry removes authority to create or commit new entry attempts. It
  does not cancel broker effects already past `send_started` and does not
  prevent emergency reduction of owned exposure.
- Leadership transfer is prohibited while the prior epoch has an unresolved
  attempt or active pair. Initial delivery may require the existing leader to
  recover; automatic distributed consensus or mid-attempt failover is not
  introduced.
- The pair contract snapshots eligible symbols, role-to-direction mapping,
  volume plans, filling modes, edge threshold, risk budget, emergency
  protection policy, refined protection policy, quote-age and skew limits,
  arm timeout, commit lead time, execution expiry, and lifecycle exit policy.
- Contract changes require a new lease epoch and broker-verified empty facts
  from both Workers. Live attempts and active pairs retain their original
  contract.
- Both Workers continuously maintain local account readiness from serialized
  terminal reads, reconciliation events, Worker effect journal state, account
  identity, terminal permissions, current exposure, symbol constraints, and
  margin headroom.
- `PAIR_READY` is revoked immediately on current exposure, unobservable
  orders or positions, terminal disconnect, account mismatch, journal error,
  insufficient margin, stale local constraints, lease loss, relay loss,
  recovery, or unresolved effects.
- A genuinely empty MT5 query is represented only by an empty tuple/list.
  `None`, malformed records, and read exceptions are unavailable facts and
  cannot establish `PAIR_READY`, `ACTIVE`, or `EMPTY`.
- Margin sizing and protection calibration run before a signal and refresh on
  bounded schedules. A stale plan removes readiness rather than triggering a
  signal-time refresh.
- Worker quote evidence contains Worker identity, symbol, bid, ask, last,
  broker tick time, local receive time, recovery epoch, and monotonic quote
  sequence. Broker tick time is preserved end to end.
- The leader evaluates an edge only from its latest local quote and one peer
  quote satisfying configured age, skew, epoch, sequence, and symbol checks.
  Transport `observed_at` is not a substitute for broker quote freshness.
- Quotes are relayed through authenticated controller Worker streams. The
  controller may fan out and replay opaque envelopes but does not normalize or
  merge quote values.
- Every attempt is immutable and contains lease epoch, contract hash, attempt
  ID, symbol, assigned directions, volumes, both quote revisions, decision
  time, arm deadline, common execution time, execution expiry, and emergency
  protection payloads.
- Attempt and per-leg deterministic effect IDs are persisted before the first
  arm message. Same-ID/same-payload replay returns durable state; changed
  payload reuse is rejected.
- Entry uses an arm/commit protocol. The follower locally validates an arm and
  durably records `ARMED` without touching MT5. The leader commits only after
  its own local arm and the follower's matching arm acknowledgement.
- Commit includes a common execution time far enough in the future to absorb
  measured relay jitter but still within the attempt's short expiry. Initial
  values are operational configuration, not correctness constants.
- Both Workers reject commit unless the exact attempt remains armed, lease and
  contract match, account remains ready, relevant quotes remain fresh, and
  sufficient deadline remains immediately before broker send.
- Worker scheduler priority remains `emergency`, `execution`, `protection`,
  `normal`, then `background`. Ready execution must be offered the serialized
  MT5 seam before live quote polling, margin refresh, or periodic
  reconciliation.
- Each Worker's effect journal preserves `prepared`, irreversible
  `send_started`, receipt, and observed evidence. An effect that crossed
  `send_started` is never resent with the same effect ID.
- Initial entry orders carry conservative emergency SL/TP. The emergency
  levels are calculated from current quote side, maximum calibrated broker
  stop/freeze constraints, tick size, and a bounded risk amount.
- If the broker cannot accept attached emergency protection for the current
  symbol, that symbol is not eligible for Pair Execution Cell entry. The first
  production version does not permit intentional post-fill naked positions.
- Actual position evidence—not only an order receipt—provides ticket, side,
  volume, symbol, fill price, and emergency protection for each leg.
- Each Worker calculates refined SL/TP from its observed fill and the pair
  contract. Refinement is a distinct journaled effect with deterministic
  identity and no blind retry.
- The leader declares `ACTIVE` only after fresh broker facts show both exact
  attempt-owned positions with their expected refined protection.
- Any reject, expiry, unknown-after-send outcome, inconsistent observation,
  protection failure, or contradictory peer evidence changes desired state to
  `EMPTY`. No new attempt is admitted during convergence.
- Empty convergence observes both Workers, cancels attempt-owned pending
  orders, observes again, closes exact attempt-owned positions at current
  volume, and requires final fresh empty facts from both Workers.
- A follower may immediately apply account-local emergency recovery when the
  leader or relay is unavailable, but it cannot infer peer success or declare
  pair terminal. Its durable facts are reconciled when coordination returns.
- Active-pair exit policy, timed synchronized exit, cutoff, loss limits,
  integrity divergence, shutdown, and operator-approved local break-glass all
  converge through the same owned desired-empty implementation.
- Runtime state persists lease, contract, quote revisions used for each
  attempt, attempts, per-leg effects, broker evidence, protection generation,
  desired state, lifecycle result, deadlines, and transition history.
- Controller audit persists opaque lease, arm, commit, effect-outcome, and
  broker-fact envelopes with delivery timing. It is not an authoritative pair
  projection.
- Structured timing records include local quote observation, peer quote
  observation, edge decision, attempt persistence, arm send/receive/ack,
  commit send/receive, scheduler dequeue, MT5 `send_started`, broker response,
  position observation, refined protection verification, containment start,
  and broker-verified terminal time.
- Broker timestamps retain their native server-time metadata and calibrated
  UTC representation. Reports never compare raw FTMO GMT+3 timestamps with
  UTC strategy timestamps.
- The performance acceptance metric separates system-controlled
  signal-to-`send_started`, broker execution, receipt transport, verification,
  and cross-leg send skew. Safety outcomes never depend on meeting a latency
  target.
- The initial target is system-controlled median signal-to-`send_started` no
  greater than 250 milliseconds, p95 no greater than 500 milliseconds, and
  p95 cross-leg `send_started` skew no greater than 250 milliseconds in the
  controlled deployment topology.
- Deploy behind an explicit per-pair execution-mode switch. Shadow mode runs
  quote coherence, readiness, edge decisions, arm simulation, and telemetry
  without effect-journal mutation or broker writes.
- Cutover requires broker-verified empty accounts, one approved Worker pair,
  test-sized exposure, restart exercises at every durable boundary, and a
  comparison against the current latency baseline.
- The legacy Strategy Runtime entry path remains available only during a
  bounded migration period and cannot be enabled simultaneously for the same
  Worker pair. It is removed after acceptance rather than retained as a second
  fallback lifecycle owner.
- Record a new architecture decision that supersedes ADR-0009's lifecycle
  owner placement for opted-in Pair Execution Cell pairs while retaining its
  one-owner, durable-state, opaque-controller, and broker-observation
  invariants.

## Testing Decisions

- The single primary test seam is the Pair Execution Cell interface used by
  production activation, event delivery, and recovery.
- Pair-level tests instantiate one leader and one follower with deterministic
  relay, clock, scheduler, journal, and MT5 adapters. Tests drive typed events
  and assert observable lifecycle results, emitted relay envelopes, broker
  effects, and durable recovery.
- Tests assert external behavior and safety invariants, not private reducers,
  helper calls, queue internals, table layout, or incidental state names.
- Existing Strategy Runtime scripted-relay tests are prior art for
  asymmetric entry, unknown outcomes, owned-ticket verification, close
  convergence, cursor gaps, and restart recovery. Scenarios move to the new
  higher seam rather than being duplicated indefinitely.
- Existing Worker scheduler tests are prior art for priority, deadline,
  expiry-before-send, serialized MT5 access, and command deduplication.
- Existing Worker reconciliation and effect-journal tests are prior art for
  broker record validation, `None` read failures, `send_started`, receipt
  replay, and observation-driven recovery.
- Existing realtime strategy tests are prior art for quote freshness, edge
  selection, sizing plans, risk limits, timed exits, and operational logging.
- Lease tests cover valid activation, stale epoch, wrong pair, wrong leader,
  changed contract hash, expiry before arm, expiry after arm, revoked lease,
  restart with the highest epoch, and attempted transfer during unresolved
  exposure.
- Quote tests cover valid coherent pairs, stale local tick, stale peer tick,
  transport-fresh but broker-stale evidence, sequence regression, duplicate
  quote, recovery-epoch change, symbol mismatch, clock skew, relay gap, and
  delayed replay.
- Decision tests prove that only the leader creates attempts and that leader
  and follower running identical code cannot both originate one.
- Readiness tests cover every revocation cause and prove that restored
  readiness requires fresh authoritative evidence rather than elapsed time.
- Arm/commit tests cover follower rejection, leader readiness loss, late arm
  acknowledgement, duplicate arm, duplicate commit, changed commit, commit
  before both arms, commit after expiry, and commit under a stale lease.
- Deterministic timing tests vary relay delay and clock offset to prove the
  common execution time bounds send skew without permitting late execution.
- Entry tests cover both accepted, either leg rejected, both rejected, either
  leg expired-not-started, either leg unknown-after-send, receipt loss,
  broker delta before receipt, receipt before broker delta, partial fill, and
  position evidence inconsistent with the attempt.
- Emergency-protection tests prove every submitted entry contains valid
  protection and that missing or broker-invalid protection prevents send.
- Refinement tests cover both successful, either rejected, either uncertain,
  one leg naturally closed during refinement, changed volume, changed ticket,
  external SL/TP mutation, and restart at each refinement boundary.
- Containment tests prove the exact surviving owned leg is closed immediately
  after asymmetric observation and that no unrelated exposure is touched.
- False-empty tests inject `None`, malformed results, timeouts, and terminal
  disconnects into every orders/positions observation used for readiness,
  verification, containment, and final empty proof.
- Journal tests crash before prepare, after prepare, at `send_started`, after
  broker execution, after receipt, after observation, during protection, and
  during close. Recovery never blindly resends a crossed effect.
- Controller relay tests prove opaque routing, identity authorization,
  attempt-scoped replay, delivery deduplication, payload-size enforcement, and
  absence of edge or pair-state derivation.
- Active lifecycle tests cover integrity checks, timed synchronized exit,
  reverse signal, risk limit, daily cutoff, shutdown, relay loss, leader loss,
  follower loss, and account-local recovery.
- Performance tests use injected deterministic delays to verify timing
  telemetry arithmetic and expiry semantics. They do not assert wall-clock
  speed in unit tests.
- A controlled live acceptance exercise records both Workers' calibrated
  broker times and every structured timing point. It compares median and p95
  system-controlled latency, broker latency, verification latency, and
  cross-leg skew against the current baseline.
- Shadow acceptance requires zero divergent leader/follower role decisions,
  zero stale quote admissions, no attempt under non-ready state, and complete
  explainability of every old-path versus new-path edge difference.
- Live test-account acceptance includes normal paired entry, either-leg broker
  rejection, worker restart before arm, restart after one send, receipt loss,
  protection-refinement failure, close failure, and final broker-verified
  empty recovery.

## Out of Scope

- Atomic transactions across two brokers or guaranteed simultaneous fills.
- Guaranteed equal fill prices, slippage, or execution latency.
- Two Workers independently originating attempts from their own edge
  decisions.
- Direct Worker-to-Worker network connections that bypass controller identity,
  authorization, relay audit, or revocation.
- Controller calculation of edges, broker protection, compensation, or pair
  lifecycle state.
- Intentional naked market entry followed by first-time protection after fill.
- Blind retry of any effect that may have crossed MT5 `order_send`.
- Automatic leadership transfer while an attempt or active pair is unresolved.
- A distributed consensus system, shared cross-Worker database, or
  multi-controller high availability.
- More than two legs or more than one active protected pair per initial
  execution cell.
- Multiple strategies sharing the same account Worker concurrently.
- Changes to the initial edge formula, exposure cap, loss budget, or product
  pair eligibility beyond the freshness and protection requirements needed by
  this architecture.
- Removal of account-local Worker recovery, effect journals, terminal
  exclusivity, authenticated commands, or broker verification.
- Management-site trade controls or controller-owned emergency flatten.
- Production activation before shadow and controlled test-account acceptance.

## Further Notes

- “Integrated strategy and Worker” means the Pair Execution Cell implementation
  runs beside the Worker-owned MT5 seam. It does not mean both Workers become
  independent strategy authorities.
- The leader/follower distinction is an execution authority rule, not a code
  fork. Both Workers deploy the same module and can occupy either role in a
  future broker-verified-empty lease epoch.
- The arm/commit exchange deliberately spends one short relay round trip to
  avoid known asymmetric dispatch. It removes the much more expensive
  signal-time remote broker snapshots while preserving a common attempt.
- Emergency protection and refined protection serve different purposes.
  Emergency levels bound the post-fill crash window; refined levels implement
  the strategy's intended risk/reward from actual fills.
- `ACTIVE` remains a broker-observed statement, not a peer agreement. `EMPTY`
  remains a fresh broker-observed statement, not a successful close receipt.
- The current measured baseline is 1.586 seconds median from signal to FTMO
  position, 2.970 seconds median to Runtime receipt, and about 4.467 seconds
  median to verified active pair. These values are incident evidence, not
  permanent constants.
- The architecture should first optimize signal-to-`send_started`. Broker
  execution and server behavior remain outside system control and must be
  reported separately.
- Any implementation that requires callers to coordinate arm states, effect
  retries, refined protection, or close convergence has made the Pair
  Execution Cell interface too shallow and should be redesigned before
  rollout.
