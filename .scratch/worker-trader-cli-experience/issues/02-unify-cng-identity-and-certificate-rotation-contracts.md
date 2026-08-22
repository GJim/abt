# 02 — Unify CNG identity and certificate rotation contracts

**What to build:** Make the control plane accept the same Windows CNG P-256 device-proof model for Trader and Worker enrollment and certificate rotation. Both roles gain equivalent 30-day device-certificate rotation contracts that preserve approval, revocation, certificate binding, expiry, and immutable audit evidence.

**Blocked by:** None — can start immediately.

**Status:** needs-triage

- [x] Trader enrollment and rotation no longer require, accept, or validate attestation JWTs or an attestation trust root; Windows CNG public-key proof is required instead.
- [x] Worker and Trader rotation require valid, unrevoked old-device proof and replacement-device proof, create a new certificate for the same role identity, and do not require another administrator approval.
- [x] A newly rotated certificate coexists with its predecessor for one hour; revoked or expired device certificates cannot rotate or establish a new session.
- [x] Each rotation produces immutable audit evidence and preserves the existing role-bound invite, approval, certificate-delivery, and session authorization guarantees.
- [x] Public control-plane REST/WSS contract tests cover successful and rejected enrollment, rotation, expiry, revocation, overlap, and audit behavior for both roles.

## Comments

- Implemented in `26cdd7b`, `51ecb07`, and `461c16e`; the full test suite passed (186 tests).
