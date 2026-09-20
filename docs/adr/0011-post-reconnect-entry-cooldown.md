# Post-Reconnect Entry Cooldown

> **Accepted 2026-09-20:** both Workers gate new entries behind a quiet window
> after any relay/rebuild instability. Implemented in `abt/pair_cell.py`
> (`post_reconnect_cooldown_seconds`, default `300`, `0` disables).

## Context

On 2026-09-18 the follower's controller session flapped five times in eight
minutes. Twice the leader entered within seconds of a follower rebuild: the
first attempt's relay arrived ~40s late, the second attempt's relay never
arrived at all (accepted by the controller but lost before the follower's
drain on a degrading link). Both legs were contained by the 5s
`follower_confirmation_timeout_seconds` within seconds, at a friction loss
each. The timeout is the last defense; nothing stopped entries from being
created into a known-unstable link in the first place.

## Decision

- Either Worker records a cooldown trigger on: peer session loss, peer
  session reconnect, peer publisher-epoch advance (rebuild), receipt of a
  peer `reseed_request`, and local recovery after restart. Every new trigger
  slides the window. Triggers and proof survive restarts in the route-scoped
  `cell_reconnect_cooldown` table.
- Exit needs both: a flap-free quiet window of shared, leader-authored
  `post_reconnect_cooldown_seconds`, *and* a peer handshake
  (`pairing_acceptance` / `policy` / `policy_ack` / `universe` /
  `sizing_plans` / `readiness` / `remaining_allowance`) observed strictly
  after the last trigger. The handshake itself is never delayed; only
  entries wait.
- While active, `_local_ready()` reports `peer reconnect cooldown ...`,
  which blocks leader attempt creation, fails follower attempt admission
  (`attempt_rejected`), and publishes not-ready to the peer. Closes and
  convergence paths are unaffected.
- The canonical policy hash covers the new field, so mixed-version pairings
  fail closed on policy acknowledgement instead of silently disagreeing.

## Consequences

- A fresh pairing is unaffected (no trigger without a prior flap, loss,
  reconnect, or restart), but any restart or flap delays the next entry by
  the quiet window even when the handshake recovers instantly.
- The confirmation timeout remains the backstop for loss *during* an
  attempt; the cooldown only reduces entries *into* instability.
- `relay_ack ok=True` still means accepted-for-forwarding, not delivered;
  this ADR deliberately does not add delivery receipts (see below).

## Alternatives rejected

- *Follower delivery receipt + retry for attempts*: stronger, but changes
  relay semantics shared with every other envelope kind; deferred.
  Execution intents are deliberately never retried (timeout contains
  instead); only queries and state sync are retried, and those retries are
  idempotent by construction (nonces, attempt IDs, versions, hashes).
- *Controller pushing peer connection changes to the surviving worker*:
  faster awareness, needs control-plane work; the cell-side gate works
  without it.

## Follow-up: bounded end-to-end retransmission (accepted 2026-09-20)

The 2026-09-18 post-mortem showed a second gap: once latched, the leader
stopped probing because steady peer traffic perpetually reset the probe
cadence, while the follower only answers explicit probes — a deadlock.
Retransmission now lives at the cell layer, over the unreliable relay:

- Proof probes run on their own clock (traffic resets escalation only),
  pause while the session is down, and fire once immediately on reconnect.
- Reseed asks reuse one nonce (peer dedups), retry every 10s up to 6 per
  session generation while the peer allowance is missing or stale, pause
  while down, reset on reconnect/rebuild, and are abandoned with one audit
  event past budget. Admission stays blocked throughout, and NEEDS_HUMAN
  remains the backstop for genuine silence.
