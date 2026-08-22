# 03 — Deliver Trader enrollment and event connection

**What to build:** Provide a native Windows `abt-trader` CLI that enrolls a Trader with a Windows CNG device identity, saves non-secret Trader configuration, and runs a foreground authenticated Trader WSS connection that safely delivers the Trader’s lifecycle events.

**Blocked by:** 01 — Save Worker identity and reconcile directly; 02 — Unify CNG identity and certificate rotation contracts.

**Status:** ready-for-human

- [ ] `abt-trader enroll` interactively collects missing controller, invite, strategy, and public-IP declaration values while preserving automation flags and never persisting the invite or private key.
- [ ] Public-IP discovery obtains a candidate from the approved HTTPS echo service, requires confirmation or override, falls back to manual input on failure, and is bypassable with an explicit public-IP value.
- [ ] Successful Trader enrollment writes non-secret, replace-protected configuration using the same safe configuration semantics as Worker enrollment; a pending enrollment is reported clearly by `connect`.
- [ ] `abt-trader connect` retrieves the current certificate only into memory, establishes the authenticated WSS session, sends and handles 30-second heartbeats, and reconnects safely.
- [ ] Trader lifecycle events are emitted as JSON Lines on standard output, diagnostics stay on standard error, and acknowledgements occur only after successful event output.
- [ ] The colocated Trader state file mirrors only control-plane-acknowledged cursors; missing or corrupt local cursor state cannot skip control-plane replay.
- [ ] Public CLI and REST/WSS tests cover enrollment, IP confirmation and fallback, approval waiting, certificate delivery, connection, heartbeat, replay, JSON Lines output, acknowledgement ordering, and state recovery.
