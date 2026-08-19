# 01 — Admit workers and Traders with registration invites

**What to build:** Replace client-IP enrollment admission with management-
issued, role-bound registration invites. Support issuance, one-time hashed
redemption, 60-minute expiry, unused-invite revocation, immutable auditing,
and the immediate expiration of legacy pending worker registrations.

**Blocked by:** None.

**Status:** ready-for-agent

- [ ] The ledger persists only invite hashes, target role, lifecycle timestamps,
  issuer identity, and immutable events; it never re-displays an invite.
- [ ] Management APIs/UI create an invite, display its secret exactly once,
  list its non-secret status, and revoke only unused invites.
- [ ] Worker and Trader enrollment reject missing, expired, revoked, used, or
  role-mismatched invites without creating a pending registration.
- [ ] Successful redemption consumes the invite atomically and starts a
  15-minute pending registration.
- [ ] Migration expires every unapproved legacy worker registration that was
  not created with an invite; approved workers remain unchanged.

