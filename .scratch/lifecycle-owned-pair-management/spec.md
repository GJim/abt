# Lifecycle-Owned Protected Pair Management

Status: wontfix

Superseded by
[`../strategy-owned-pair-lifecycle/spec.md`](../strategy-owned-pair-lifecycle/spec.md).
The controller will not own protected-pair lifecycle.

## Problem Statement

Realtime arbitrage strategy currently manages an open 配對反向避險 through
individual Worker reads and generic Worker operations. The controller owns the
Account Recovery Lifecycle, recovery epoch, broker observations, and Worker
effect journal, but the strategy receives only accepted or rejected RPC
results. Consequently, it cannot distinguish a temporary `CATCHING_UP` state
from a terminal safety condition.

This creates unsafe and misleading failure handling. A strategy may submit a
daily-cutoff close while the controller is awaiting fresh broker observations;
the generic close is rejected, and the strategy reports that emergency
flattening is unavailable even though the correct action is to retain a
durable desired empty state and let recovery finish. Conversely, retrying the
same direct broker operation after a timeout or reconnect could duplicate an
unknown broker write.

The existing Account Recovery Lifecycle also has five coupled implementation
gaps: a dispatched but unconfirmed entry does not reach
`ENTRY_UNCONFIRMED`; protected-leg numeric evidence is representation
sensitive; ticket correlation can race or mask a real external change;
single-account observations can bypass paired current-epoch verification; and
an owner close of a verified pair does not immediately begin a correlated,
broker-verified empty convergence.

## Solution

Make the controller the exclusive owner of protected 配對反向避險 lifecycle
management. A Trader strategy submits idempotent protected-pair intents rather
than individual Worker writes for pair entry, protection, or exit. The
controller returns typed durable command status and emits owner-scoped
lifecycle events. It owns recovery state transitions, effect attribution,
fresh-observation requirements, retries, compensation, and terminal proof.

The highest test seam is the authenticated Trader-to-controller protected-pair
command and lifecycle-event contract. Behind that seam, the controller uses
the existing authenticated controller-to-Worker channel and Worker effect
journal to perform broker work. A strategy may retain generic Worker reads for
market data, sizing, and risk calculations, but must not manage a registered
protected pair through generic Worker write operations.

## User Stories

1. As a realtime arbitrage operator, I want the controller to own an open 配對反向避險 after it accepts it, so that strategy restart or temporary Worker disconnection does not split lifecycle authority.
2. As a realtime arbitrage operator, I want to submit one idempotent protected-pair entry intent, so that the broker entry, initial protection, and verification are one durable operation.
3. As a realtime arbitrage operator, I want a protected-pair entry accepted only when both accounts are `READY`, so that recovery work and new exposure never interleave.
4. As a realtime arbitrage operator, I want an entry whose broker send may have occurred to enter `ENTRY_UNCONFIRMED`, so that a possible one-leg fill is never treated as harmless.
5. As a realtime arbitrage operator, I want an entry effect that has crossed `send_started` never to be resent, so that reconnect recovery cannot duplicate market exposure.
6. As a realtime arbitrage operator, I want the controller to create a new compensating effect only after current broker evidence justifies it, so that recovery does not guess a broker outcome.
7. As a realtime arbitrage operator, I want the controller to establish the requested SL and TP as part of protected-pair management, so that the strategy does not separately own a dangerous post-entry protection window.
8. As a realtime arbitrage operator, I want a pair to become `ACTIVE_VERIFIED` only after both current broker snapshots prove the same protected pair revision, so that an accepted receipt alone cannot be mistaken for a hedge.
9. As a realtime arbitrage operator, I want matching numeric SL, TP, and volume facts to compare by value rather than formatting, so that harmless broker formatting does not flatten a valid hedge.
10. As a realtime arbitrage operator, I want invalid or non-canonical broker numeric facts to be explicit divergence evidence, so that malformed data cannot silently pass verification.
11. As a realtime arbitrage operator, I want a Worker reconnect to move an active protected pair to `CATCHING_UP`, so that fresh evidence is required before it is again considered verified.
12. As a realtime arbitrage operator, I want a matching two-sided current-epoch snapshot to restore `ACTIVE_VERIFIED`, so that a short network interruption does not unnecessarily close a sound protected pair.
13. As a realtime arbitrage operator, I want any ticket, symbol, side, volume, pending-order, SL, or TP mismatch to make both related accounts converge to `EMPTY`, so that a partial or altered hedge cannot remain active.
14. As a realtime arbitrage operator, I want price, spread, swap, margin, equity, and floating P/L changes ignored by protected-pair verification, so that normal market movement is not a divergence.
15. As a realtime arbitrage operator, I want a daily cutoff to submit one protected-pair close intent, so that cutoff flattening remains durable when an account is `CATCHING_UP`.
16. As a realtime arbitrage operator, I want a close intent accepted as a desired empty state during `ENTRY_UNCONFIRMED`, `ACTIVE_VERIFIED`, `CATCHING_UP`, or `CONVERGING_EMPTY`, so that reducing existing risk is not denied merely because new entry is unsafe.
17. As a realtime arbitrage operator, I want the controller to report a close in progress as pending or containing rather than as a failed generic close, so that the strategy does not falsely escalate a temporary recovery condition.
18. As a realtime arbitrage operator, I want the controller to cancel orders, observe, close positions, and observe empty accounts in that order, so that an order cannot refill exposure during pair closure.
19. As a realtime arbitrage operator, I want a normal pair close to reach broker-verified empty state as soon as both final snapshots arrive, so that new entry eligibility does not wait for incidental periodic reconciliation.
20. As a realtime arbitrage operator, I want reversal, risk-limit, shutdown, and daily-cutoff exits to use the same protected-pair close operation, so that every risk-reducing exit has identical recovery semantics.
21. As a Trader strategy, I want typed command statuses and lifecycle facts instead of parsing human-readable rejection text, so that my action policy is stable when diagnostics change.
22. As a Trader strategy, I want owner-scoped replayable lifecycle events, so that reconnect can recover a command outcome and pair state without querying another Trader's data.
23. As a Trader strategy, I want to stop producing new signals while my active pair is not verified, so that I do not combine a recovery state with a new entry or reversal.
24. As a Trader strategy, I want protection changes suspended during `CATCHING_UP`, so that I never blind-retry a potentially applied SL/TP modification.
25. As a Trader strategy, I want a protection change to be permitted only against an observed current pair revision, so that it cannot weaken or target a stale position.
26. As a controller operator, I want broker deltas correlated to the specific expected effect rather than only a remembered ticket, so that an external modification or closure of a controlled ticket remains detectable.
27. As a controller operator, I want a delta that arrives before its broker receipt held as pending attribution until a receipt or observation watermark resolves it, so that event ordering does not create a false external recovery incident.
28. As a controller operator, I want ticket correlation to expire with its effect and Worker epoch, so that a reused broker ticket cannot inherit obsolete ownership.
29. As a controller operator, I want every full recovery observation to carry authenticated Worker, epoch, cursor, snapshot identity, and target revision evidence, so that stale or single-sided observations cannot verify a pair.
30. As a controller operator, I want `CONVERGING_EMPTY`, `NEEDS_HUMAN`, and `REVOKED` to block ordinary Trader writes, so that recovery and authorization controls cannot be bypassed.
31. As a controller operator, I want the controller and Workers to continue a durable accepted close if the Trader disconnects, so that a local strategy outage cannot strand exposure.
32. As an auditor, I want every pair command, lifecycle transition, effect attempt, attribution decision, and final broker proof to be immutable, so that a live incident can be reconstructed without inferring intent from broker comment or magic fields.
33. As a test author, I want deterministic tests that force sends, lost receipts, reconnects, epoch changes, and delta ordering at the protected-pair command seam, so that lifecycle safety is demonstrated at durable boundaries.
34. As a management-station administrator, I want the existing recovery state and its reason visible for each protected pair, so that a pending close, automatic convergence, and `NEEDS_HUMAN` condition are distinguishable.

## Implementation Decisions

- Add one deep protected-pair lifecycle module in the controller. Its interface accepts authenticated owner intents and authenticated broker facts, and returns durable command status plus replayable lifecycle events. It owns pair locking, effect IDs, revisions, admission, recovery decisions, retries, compensation, and final proof. Strategy-specific retry loops must not replicate this implementation.

- Use the existing Trader WSS command/event contract as the single external seam for protected-pair management. The controller-to-Worker authenticated channel remains an internal remote-but-owned adapter. The Worker effect journal and broker adapter remain behind that controller implementation. Existing generic Worker reads remain available to Trader strategies; generic Worker writes remain for unregistered single-account work but cannot manage a registered protected pair.

- Define three protected-pair intents: open a protected pair, update protection on a verified protected pair, and close a protected pair. Each carries a Trader-provided idempotency ID, protected-pair ID, expiry when applicable, and expected pair revision where it mutates an existing pair. The controller derives the owner from the authenticated Trader session rather than trusting a caller-supplied owner identity.

- An open intent includes both market legs and the required initial protection targets. The controller validates both legs, canonicalizes decimal values using the applicable product specifications, records the command and effects before dispatch, and owns any required post-fill protection effect. A pair cannot be reported as active until broker facts prove both positions and both protections.

- A close intent means that the owner requests the protected pair's desired state to become `EMPTY`; it is not a request to resend a particular broker close. The controller accepts an eligible close exactly once and advances both accounts and the pair to an empty-convergence revision in the same durable transaction. It then drives cancel, observation, close, and final observation using new, evidence-justified effects as necessary.

- An update-protection intent is accepted only for the owner of an `ACTIVE_VERIFIED` pair at its current revision. It must not weaken stop-loss protection and must be validated against current observed tickets and product precision. It is suspended, rather than retried, when the pair is `CATCHING_UP`; its eventual outcome is established from broker evidence. Unmanaged direct SL/TP operations on a registered protected leg are rejected.

- Represent the externally observable command outcome with typed codes, stable reason codes, protected-pair ID, command ID, current revision, and lifecycle state. The contract distinguishes preflight rejection, accepted work, awaiting broker facts, automatic empty convergence, verified completion, and terminal human/authorization states. It must not require strategies to parse diagnostic prose or infer retryability from transport failure.

- Emit immutable owner-scoped lifecycle events for command acceptance, lifecycle/revision change, effect uncertainty, fresh-fact requirement, pair verification, empty convergence, verified completion, and terminal human/authorization escalation. Events use the existing at-least-once cursor and acknowledgement model. Reconnect replay must preserve ordering and allow a strategy to deduplicate by immutable event ID.

- Make `ENTRY_UNCONFIRMED` a production transition. Before either entry effect crosses `send_started`, the controller creates a durable pair record with the planned legs and effects. A send that may have reached the broker moves the pair and affected accounts to `ENTRY_UNCONFIRMED`; only current authenticated observations may prove the protected legs or justify empty convergence.

- Treat protected-leg numeric fields as typed canonical decimal values at the controller ingress. Ticket, symbol, and side remain exact domain values. A broker fact that is absent, boolean, non-finite, unparsable, or incompatible with the product's tick or volume constraints is rejected as invalid evidence and handled as divergence rather than being coerced through string conversion.

- Replace permanent ticket-only controlled classification with durable effect attribution. An effect records its expected broker transition, payload hash, Worker recovery epoch, send boundary, receipt evidence, and terminal broker evidence. A delta is controlled only when it matches an active expected transition. A delta arriving before attribution is durably pending until receipt, current snapshot, or cursor/epoch watermark resolves it. External changes to a correlated ticket still create divergence when they do not match an expected transition.

- Consolidate full broker observation through one recovery ingress. Every observation is authenticated and includes Worker identity, current recovery epoch, continuous cursor or an explicit full-snapshot baseline, snapshot identity, and applicable recovery revision. A protected pair is verified only from both current epochs and the same expected pair revision; no direct single-account observation may set a protected leg to verified independently.

- Preserve lifecycle permissions. `READY` is the only state that admits new entry. `ACTIVE_VERIFIED` admits only owner-authorized protected-pair management. `ENTRY_UNCONFIRMED`, `CATCHING_UP`, and `CONVERGING_EMPTY` never admit new entry or protection updates, but an idempotent owner close intent is accepted as a desired-state request. `NEEDS_HUMAN` and `REVOKED` reject all new Trader protected-pair intents.

- Make strategy behavior intent-driven. It submits an open intent only after its risk and market-data checks; it listens to lifecycle outcomes for the pair it owns; it submits one close intent for reversal, risk, shutdown, and daily cutoff; and it stops producing new strategy actions while a pair is pending recovery. A strategy must not retry a prior broker write, directly flatten the registered pair, or treat an awaiting-facts outcome as proof that containment failed.

- Preserve the Account Recovery Lifecycle ADR: broker observation remains authoritative, ledger desired state remains the recovery target, a `send_started` effect is never resent, routine uncertainty is automatic recovery rather than permanent freeze, and `REVOKED` never returns to `READY` automatically. This specification extends that ADR with a protected-pair Trader interface; it does not contradict it.

## Testing Decisions

- Test through the authenticated protected-pair command and lifecycle-event seam. A good test submits a command, supplies scripted Worker broker facts through the authenticated Worker adapter, and asserts durable command outcome, emitted owner event, admissible next command, and broker-observed final state. It must not assert SQL table layout, async task structure, queue ordering internals, or private reducer calls.

- Use the existing control-plane ledger, controller service, Trader session, account-recovery reducer, and realtime-arbitrage strategy test patterns as prior art. Extend their scripted socket, temporary ledger, fake Worker, and deterministic broker facilities rather than adding a second transport test framework.

- Add end-to-end command tests for successful protected-pair open, protection setup, and two-current-epoch snapshot verification. Assert that the externally visible result remains pending until both legs are broker-observed and protected.

- Add durable-boundary tests for entry failure before send, after `send_started` before receipt, after receipt before observation, one-leg confirmation, and controller or Worker restart. Assert that no `send_started` entry or protection effect is resent and that recovery reaches either verified pair or broker-verified empty accounts.

- Add decimal-equality tests proving equivalent volume, SL, and TP representations verify identically, while materially different, boolean, non-finite, or invalid values do not.

- Add attribution-order tests where a broker delta precedes its receipt, where an expected delta follows a receipt, and where an external close, volume change, SL change, or TP change targets a correlated ticket. Assert that only the expected effect is attributed and every other change begins paired empty convergence.

- Add current-epoch observation tests proving that a lone protected-leg snapshot, a stale epoch, a cursor gap, or a previous pair revision cannot make a pair `ACTIVE_VERIFIED`; both fresh matching snapshots must be required.

- Add close tests for `ACTIVE_VERIFIED`, `CATCHING_UP`, and `CONVERGING_EMPTY`. Assert that one idempotent close command changes desired state to `EMPTY`, blocks new entries, does not resend unknown physical closes, and becomes complete immediately after both final empty snapshots rather than after a periodic reconciliation interval.

- Add strategy tests proving that daily cutoff, reverse signal, risk-limit, and shutdown submit one close intent; a pending recovery close does not become a local emergency-flatten failure; new entry is never retried while non-`READY`; and an SL/TP update is not blindly retried after lifecycle uncertainty.

- Add authorization and replay tests proving that only the owning Trader can observe detailed pair lifecycle or submit management commands, same ID/same payload returns the original durable result, same ID/different payload is rejected, and reconnect event replay is ordered, at-least-once, and deduplicable.

## Out of Scope

- Changing strategy-owned aggregate risk policy, signal generation, symbol selection, sizing calculations, or New York cutoff time.
- Changing MT5 broker behavior, broker fill semantics, or the existing prohibition on comment and magic-number correlation.
- Replacing generic Worker reads used for market data, account information, symbol information, or sizing calculations.
- Redesigning management-station UI beyond exposing the new lifecycle facts already available from the controller.
- Automatic resolution of `NEEDS_HUMAN`, re-enrolling a revoked Worker, or allowing a revoked Worker to return to `READY`.
- Supporting arbitrary multi-leg portfolios or cross-Trader shared ownership in this iteration; the managed lifecycle applies to exactly two legs owned by one Trader.

## Further Notes

This specification intentionally treats a close as a desired-state declaration,
not a one-shot broker RPC. That is the critical distinction needed to make a
daily cutoff safe during `CATCHING_UP`: the controller retains the intent and
waits for evidence rather than rejecting the strategy or blindly replaying a
broker write.

The existing Account Recovery Lifecycle specification remains the source for
general recovery, Worker journal, and account convergence rules. This feature
adds the missing protected-pair management interface and closes the five
observed gaps without granting Trader strategies authority over broker facts
or recovery effects.
