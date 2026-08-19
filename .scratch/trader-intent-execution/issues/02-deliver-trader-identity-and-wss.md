# 02 — Deliver Trader identity and reconnecting WSS

**What to build:** Add invite-gated Trader enrollment, administrator approval,
30-day hardware/OS-backed certificate lifecycle, and an authenticated
outbound WSS channel for Trader commands and personal lifecycle events.

**Blocked by:** 01 — Admit workers and Traders with registration invites.

**Status:** in_progress

- [x] Trader enrollment accepts only a `trader` invite, P-256 public-key proof,
  strategy name, and claimed public IP; it records no observed client IP.
- [x] Approval issues a Trader certificate bound to one Trader ID and
  non-exportable CNG/TPM/PKCS#11 P-256 key; rotation and revocation follow the
  approved 30-day lifecycle.
- [x] REST is limited to enrollment/certificate flow; intent and cancellation
  commands require authenticated WSS.
- [x] WSS uses 30-second heartbeats, marks stale after five minutes, and never
  pauses or cancels accepted intents because of staleness.
- [x] Event delivery is at least once with immutable event IDs, ACK cursors,
  and reconnect replay; a Trader can receive only its own events.
- [x] Revocation ends active WSS and blocks new commands without changing
  accepted intents.

## Review gaps

- [x] Add certificate rotation and administrator revocation for approved Traders.
- [x] Make Trader heartbeats bidirectional every 30 seconds and persist a stale
  state after five minutes without a valid signal.
- [x] Require verifiable CNG/TPM/PKCS#11 key attestation; PEM proof alone does
  not establish non-exportability.
- [x] Add administrator rejection for pending Trader registrations.
- [x] Add durable WSS command idempotency plus per-Trader immutable events,
  ACK cursors, and reconnect replay.
