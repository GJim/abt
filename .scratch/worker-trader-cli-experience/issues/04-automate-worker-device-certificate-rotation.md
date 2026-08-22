# 04 — Automate Worker device-certificate rotation

**What to build:** Let a connected Worker automatically keep its device certificate current without administrator intervention, while retaining safe rollback time and durable cleanup of retired CNG keys.

**Blocked by:** 01 — Save Worker identity and reconcile directly; 02 — Unify CNG identity and certificate rotation contracts.

**Status:** ready-for-agent

- [ ] On a successful Worker connection, a certificate past half of its 30-day lifetime attempts rotation; a long-running reconciliation session checks again every 24 hours, including during the final 24 hours.
- [ ] Successful rotation creates a distinct replacement CNG key, atomically switches the saved Worker identity to it, and reconnects through the new device certificate.
- [ ] The Worker state file records retired-key cleanup only after the new identity is current; the old key is eligible for cleanup after the one-hour certificate overlap.
- [ ] Failed eligible cleanup emits a safe warning and retries every 24 hours without disturbing the active Worker identity.
- [ ] Failed rotation leaves the current valid identity usable; an expired certificate cannot resume a session and reports a clear action-required error.
- [ ] Public Worker CLI and control-plane contract tests cover half-life and daily scheduling, success, failure, expiry, overlap, atomic switch, persistent cleanup, and retry behavior.

