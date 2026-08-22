# 05 — Automate Trader device-certificate rotation

**What to build:** Let a connected Trader automatically rotate its device certificate and CNG key while preserving at-least-once lifecycle-event delivery and the control plane’s authoritative acknowledgement cursor.

**Blocked by:** 02 — Unify CNG identity and certificate rotation contracts; 03 — Deliver Trader enrollment and event connection.

**Status:** ready-for-agent

- [ ] A Trader connection attempts rotation after half of its certificate lifetime and checks again every 24 hours while it remains connected, including in the final 24 hours.
- [ ] Successful rotation atomically makes the replacement CNG key current, uses the current certificate only in memory, and resumes the authenticated WSS session without changing the Trader identity.
- [ ] The Trader state file maintains the acknowledged-cursor mirror independently of retired-key cleanup, so rotation cannot advance, erase, or become the replay cursor authority.
- [ ] The predecessor key is retained through the one-hour certificate overlap and then cleaned up from durable state; cleanup failure warns and retries daily.
- [ ] Rotation failure retains a still-valid Trader session where possible; an expired certificate cannot reconnect and reports a clear action-required error.
- [ ] Public Trader CLI and REST/WSS contract tests cover timed rotation, event replay and ACK safety across rotation, key overlap, cleanup persistence, failure, and expiry.
