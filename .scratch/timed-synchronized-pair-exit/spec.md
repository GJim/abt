# Timed Synchronized Pair Exit

Status: ready-for-human

## Problem Statement

A verified protected pair can remain open indefinitely when no reverse signal,
risk limit, cutoff, or shutdown occurs. Long holding periods increase exposure
to basis drift, connectivity failures, broker-specific price divergence, and
the chance that one leg exits through its existing protection while the other
remains open.

The operator wants a configurable maximum holding age. After that age, the
Strategy Runtime should move both legs' SL/TP close to the current market so a
normal short-term price movement is likely to close both legs around the same
market event. If only one leg exits, the remaining owned leg may wait briefly
for the same movement, but it must be market-closed after 15 seconds.

The two Workers and brokers cannot apply protection atomically. Quotes, spread,
minimum stop distance, fill price, and trigger side also differ between the
two accounts. The feature therefore cannot promise identical fills. It must
provide durable, evidence-driven convergence without reintroducing lifecycle
ownership into the controller or allowing a partial protection update to
leave unmanaged exposure.

## Solution

Add an optional timed synchronized exit policy to each protected-pair plan.
The policy is snapshotted when the pair is created and does not change when
process configuration changes.

The Strategy Runtime records the broker-verified activation time. Once the
configured maximum holding age elapses, it durably changes the pair's desired
state to `EMPTY`, obtains fresh facts and market evidence from both Workers,
and computes a narrow, broker-valid exit corridor around a shared market
reference.

The long leg receives its lower boundary as SL and upper boundary as TP. The
short leg receives its upper boundary as SL and lower boundary as TP. The
actual per-Worker prices are adjusted for current spread and cross-broker
basis so the two sets of protection target the same normalized market
movement rather than merely using identical numeric prices.

Both protection changes are persisted as deterministic Worker effects before
being dispatched concurrently. Broker receipts are not sufficient proof that
the corridor is armed; fresh position facts must show the expected owned
tickets and exact new protection.

When one owned leg is first authoritatively observed absent, the Strategy
Runtime persists a 15-second orphan deadline. If the other leg exits before
that deadline, the Runtime verifies both accounts empty. Otherwise it
market-closes only the remaining owned ticket and current observable volume.

An armed corridor also has a bounded waiting deadline. If neither leg exits
within 30 seconds after both protection updates are verified, the Runtime
market-closes both owned legs concurrently and verifies both accounts empty.
This prevents the maximum-hold feature from becoming another indefinite hold.

All timers, effects, observations, and transitions are durable and recoverable.
The controller remains an authenticated opaque relay and audit plane. Workers
remain the sole owners of MT5 access, broker effect journaling, and local
command admission.

## User Stories

1. As a strategy operator, I want to configure a maximum pair holding age, so that a pair cannot remain open indefinitely.
2. As a strategy operator, I want the timed exit policy disabled unless explicitly configured, so that existing deployments retain their current behavior.
3. As a strategy operator, I want the holding age measured from broker-verified pair activation, so that entry latency does not consume the holding window.
4. As a strategy operator, I want an active pair's exit policy snapshotted at entry, so that a configuration reload cannot silently change a live lifecycle.
5. As a strategy operator, I want both legs to target the same normalized market movement, so that they are likely to exit during the same natural fluctuation.
6. As a strategy operator, I want spread and cross-broker basis considered when calculating protection, so that nominally equal prices do not create predictably staggered triggers.
7. As a strategy operator, I want recent market movement considered when sizing the exit corridor, so that the targets are reachable without being arbitrary.
8. As a strategy operator, I want broker stop and freeze distances respected, so that timed protection is not knowingly submitted with invalid prices.
9. As a strategy operator, I want price levels aligned to each symbol's tick size and digits, so that both Workers receive representable values.
10. As a strategy operator, I want timeout protection never to widen either leg's existing loss protection, so that timed exit cannot increase the pair's configured downside.
11. As a strategy operator, I want both protection updates dispatched concurrently, so that cross-broker arming skew is minimized.
12. As a strategy operator, I want the exact arming intent persisted before either Worker is called, so that a crash cannot erase knowledge of a possible broker write.
13. As a strategy operator, I want fresh broker facts to verify both protection updates, so that receipts alone cannot incorrectly declare the corridor armed.
14. As a strategy operator, I want an unknown protection outcome reconciled from observation, so that the same broker effect is never blindly resent.
15. As a strategy operator, I want a partial arming result contained immediately, so that one leg is not left under a materially different exit policy.
16. As a strategy operator, I want an invalid or unavailable corridor to fall back to owned-pair close convergence, so that calculation failure cannot extend the hold indefinitely.
17. As a strategy operator, I want the first authoritative missing-leg observation to start the orphan timer, so that the deadline is based on evidence the Runtime actually possesses.
18. As a strategy operator, I want the remaining leg to retain its armed protection during the orphan grace period, so that it can still exit naturally.
19. As a strategy operator, I want the remaining leg market-closed after 15 seconds, so that one-sided exposure has a strict bound.
20. As a strategy operator, I want the surviving ticket and volume re-observed immediately before market close, so that stale assumptions are not used for a broker write.
21. As a strategy operator, I want only Runtime-owned tickets mutated, so that manual or unrelated strategy exposure is never closed by shape matching.
22. As a strategy operator, I want both legs market-closed if neither corridor boundary is reached within 30 seconds, so that timed exit always has a terminal path.
23. As a strategy operator, I want final fresh snapshots from both Workers, so that `EMPTY` means broker-verified absence rather than accepted close requests.
24. As a strategy operator, I want restart recovery to preserve the original hold, arming, corridor, and orphan deadlines, so that restarting cannot reset safety timers.
25. As a strategy operator, I want restart recovery to inspect current protection and positions before emitting effects, so that already-applied work is not repeated.
26. As a strategy operator, I want stream gaps or Worker epoch changes to require fresh snapshots, so that missing events cannot drive an orphan close decision.
27. As a strategy operator, I want Worker disconnection to preserve desired state and deadlines, so that lifecycle intent is not forgotten during an outage.
28. As a strategy operator, I want a recovered Worker to provide fresh evidence before overdue close work proceeds, so that reconnect never operates from stale tickets.
29. As a strategy operator, I want unrelated exposure or contradictory tickets to produce `NEEDS_HUMAN`, so that the Runtime fails closed rather than guessing ownership.
30. As a strategy operator, I want risk, cutoff, reverse-signal, and shutdown closes to take precedence over the timed corridor, so that urgent close reasons are not delayed.
31. As a strategy operator, I want new entries disabled while a timed exit is active, so that exit convergence cannot overlap another pair lifecycle.
32. As a strategy operator, I want trade completion counted only after broker-verified empty, so that a timed exit cannot be recorded prematurely.
33. As a strategy operator, I want lifecycle logs for timeout due, corridor calculation, arming outcomes, orphan deadline, fallback close, and final empty, so that live behavior is observable.
34. As an account Worker operator, I want each protection or close effect to retain normal journal semantics, so that `send_started` effects are never resent after uncertainty.
35. As a controller operator, I want timed-exit payloads to remain opaque, so that the controller does not regain pair lifecycle responsibility.
36. As a maintainer, I want deterministic clock-driven tests for every timeout transition, so that the feature can be verified without waiting in real time.
37. As a maintainer, I want crash tests at every durable transition, so that recovery behavior is proven rather than inferred.
38. As a maintainer, I want all two-leg result orderings tested, so that cross-broker non-atomicity cannot hide an asymmetric failure.

## Implementation Decisions

- The Strategy Runtime remains the only protected-pair lifecycle owner. Timed
  exit is added to that deep module rather than implemented as Strategy
  callbacks, controller reducers, or Worker-to-Worker coordination.
- The primary interface becomes a periodic Runtime maintenance operation that
  accepts the current UTC time and optional fresh pair market observation, and
  returns the current pair execution result. The interface hides timeout
  evaluation, protection effects, orphan detection, market-close fallback,
  and recovery.
- The realtime Strategy calls Runtime maintenance on every loop iteration,
  including quiet one-second market-data timeouts. Timer progress must not
  depend on receiving another quote message.
- Existing immediate empty convergence remains available for reverse signals,
  risk limits, daily cutoff, Ctrl+C, and shutdown. Those reasons bypass the
  natural-movement waiting period.
- The timed exit policy contains maximum holding age, a 15-second orphan grace
  period, a 30-second armed-corridor deadline, market-evidence freshness, and
  the natural-movement calculation settings. The feature is disabled when no
  positive maximum holding age is configured.
- The pair plan persists the policy snapshot. The pair aggregate additionally
  persists broker-verified activation time, exit phase, due time, normalized
  corridor, per-leg translated protection, arming generation and effect
  outcomes, armed deadline, first missing-leg evidence, orphan deadline,
  surviving owned ticket, and current desired state.
- Holding age begins when fresh facts first verify both entry legs active with
  their authoritative tickets and protection. Recovery never replaces this
  timestamp with process startup time.
- Corridor calculation uses fresh bid/ask from both Workers and recent
  one-second midpoint movement over a 60-second window. The natural movement
  distance is the maximum of broker minimum stop/freeze distance plus tick
  safety, three times the largest current spread, and a robust recent movement
  estimate.
- The initial implementation uses one fixed calculation policy. It does not
  introduce a generic exit-policy hierarchy or pluggable strategy framework.
- A normalized lower and upper boundary describe the shared market event.
  Each Worker receives translated SL/TP values that account for its current
  midpoint, spread, and basis relative to the shared reference.
- Protection prices are rounded outward to the Worker's tick size. The final
  prices must leave both executable quote sides inside the corridor and satisfy
  current broker stop/freeze constraints.
- A timed exit may move protection closer to the market but must not move an
  existing SL farther into loss. If a valid corridor cannot satisfy that
  invariant, the Runtime skips arming and begins normal owned-pair close
  convergence.
- Reaching the maximum holding age durably changes desired state to `EMPTY`
  before any protection effect is emitted.
- Both `modify_sl_tp` effects receive deterministic IDs and stable payload
  hashes and are persisted before concurrent dispatch.
- Cross-Worker arming is explicitly non-atomic. The Runtime never describes
  the pair as armed until fresh facts show both exact owned tickets with the
  expected protection generation.
- Rejected or uncertain arming does not trigger blind rollback or reuse of an
  effect ID. Fresh facts decide whether the update applied. A materially
  partial or unverifiable result proceeds to ordinary close convergence.
- A leg is considered naturally exited only when current Worker facts prove
  its owned ticket absent. Receipt timestamps and requested prices do not
  establish this fact.
- The 15-second orphan grace starts at the first durable authoritative
  observation of exactly one missing owned leg. It does not attempt to infer
  the broker's earlier execution time.
- During orphan grace, the remaining leg keeps its verified timed-exit
  protection. At deadline, the Runtime obtains another fresh snapshot and
  market-closes only the exact remaining owned ticket and current volume.
- If both legs remain through the 30-second armed deadline, the Runtime begins
  ordinary concurrent owned-pair close convergence.
- If both legs disappear at any point, the Runtime still requires final fresh
  empty snapshots from both Workers before deleting the pair aggregate.
- Refined internal states are `TIMED_EXIT_ARMING`, `TIMED_EXIT_ARMED`, and
  `ORPHAN_GRACE`. Existing `CLOSING`, `CLOSE_UNCERTAIN`, `EMPTY`, and
  `NEEDS_HUMAN` semantics remain authoritative.
- Startup recovery recognizes all timed-exit phases and resumes from durable
  desired state, deadlines, effects, and fresh Worker facts. It does not
  collapse every timed-exit restart directly into a new physical close
  request.
- Worker protocol already supports journaled `modify_sl_tp` and `close`
  operations. No semantic controller endpoint or management operation is
  added.
- Worker symbol evidence is extended or retained as needed to expose current
  stop level, freeze level, digits, and tick size to the Strategy Runtime.
  Workers remain responsible for final local broker validation.
- Logs are emitted only for meaningful timed-exit transitions and periodic
  verified state, not for every quote or polling iteration.
- Configuration exposes the maximum holding age. The 15-second orphan grace
  and 30-second corridor deadline initially have defaults but remain explicit
  policy fields for deterministic testing and future operational tuning.

## Testing Decisions

- The single primary test seam is the Strategy Runtime maintenance interface.
  Tests use the same interface as the realtime Strategy caller.
- Tests inject a fake clock and a scripted relay adapter. Advancing time is
  deterministic; tests never sleep for maximum-hold, 15-second orphan, or
  30-second corridor deadlines.
- Tests assert externally observable pair results, emitted Worker effects,
  durable recovery behavior, and broker facts. They do not assert private
  helper calls or every internal state transition.
- Runtime tests cover disabled policy, not-yet-due maintenance, exact due time,
  stale market evidence, insufficient history, broker constraint failure,
  tick rounding, spread/basis translation, and the no-widening-loss invariant.
- Runtime tests cover successful concurrent arming and every meaningful
  ordering of two Worker outcomes: both accepted, first accepted, second
  accepted, rejection, timeout, disconnect, and `unknown_after_send`.
- Fresh-fact tests prove that receipts cannot establish armed state and that
  exact SL/TP on both owned tickets can.
- Orphan tests cover first leg missing, second leg missing, both missing
  together, second natural exit before 15 seconds, surviving leg at exactly
  15 seconds, and changed ticket or volume at the deadline.
- Corridor-deadline tests prove that two still-open legs begin ordinary close
  convergence at 30 seconds.
- Ownership tests include unrelated orders, unrelated positions, shape-matched
  foreign tickets, and external protection changes. No such exposure may be
  mutated.
- Crash-recovery tests reopen the durable store after each persisted
  transition: timeout due, desired empty, effects prepared, one dispatch,
  uncertain dispatch, both armed, first missing leg, orphan deadline, close
  send started, and final empty.
- Cursor and epoch tests prove that duplicate facts are ignored and gaps force
  a fresh snapshot before timed-exit decisions continue.
- Existing Worker scheduler, effect journal, relay protocol, and controller
  opaque-relay contract tests remain the prior art for lower-level behavior.
  They are extended only when the timed exit reveals a new contract, not to
  duplicate lifecycle scenarios already covered through the Runtime seam.
- Existing realtime Strategy tests verify only that loop maintenance runs
  during both quote delivery and quiet market-data timeouts, and that urgent
  close reasons bypass the timed waiting flow.
- A live acceptance exercise uses a test-sized protected pair and records:
  broker-verified activation, timeout arming, both protection updates, natural
  or fallback exits, final empty facts, and normal process shutdown.

## Out of Scope

- Guaranteed identical broker fill prices or atomic cross-broker execution.
- Controller-owned pair state, timeout decisions, compensation, or trade
  controls.
- Management-site controls for arming, cancelling, or forcing timed exits.
- Worker-to-Worker communication or one Worker inferring the opposite leg.
- Generic pluggable exit strategies, trailing-stop frameworks, or extraction
  of a separate Trader Execution module.
- Replacing urgent reverse-signal, risk, cutoff, or shutdown close behavior
  with the timed corridor.
- Deal-history-based reconstruction of the exact broker exit timestamp.
- Automatic mutation of manual, unrelated-strategy, or otherwise unowned
  exposure.
- More than one active protected pair per Strategy Runtime.

## Further Notes

- “Synchronized” means both legs target the same normalized market movement
  and then converge from authoritative facts. It does not mean identical
  numeric protection or fill prices.
- The natural corridor should first be deployed with conservative test-account
  exposure and transition logs. Its movement estimator and deadlines are
  operational tuning parameters, not correctness assumptions.
- If real broker evidence shows that translated trigger levels perform worse
  than identical numeric levels for a particular product pair, that should be
  addressed as a later measured policy change without moving lifecycle
  ownership out of the Strategy Runtime.
- The currently running live trade predates this feature and must not be
  migrated into a timed exit policy while active.
- To close crash windows, a successful broker verification adopts the durable
  timestamp recorded immediately before that verification attempt. Recovery
  falls back to earlier durable effect evidence rather than restart time, so a
  crash may shorten but can never extend a safety deadline.
- Runtime time is persisted as nondecreasing UTC across maintenance calls, and
  live volatility input requires at least 55 seconds of contiguous samples
  from the 60-second window. Feed gaps invalidate the window.
