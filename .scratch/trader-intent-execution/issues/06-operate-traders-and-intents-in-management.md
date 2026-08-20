# 06 — Operate Traders and intents in management

**What to build:** Extend the management SPA/API with Trader enrollment review
and an intent workspace for all origins.

**Blocked by:** 01 — Admit workers and Traders with registration invites; 03 — Model intent submissions and dual-broker preflight; 05 — Cancel zero-fill intents and flatten filled exposure.

**Status:** ready-for-human

- [x] Administrators can review, approve, reject, and revoke Trader identities
  without exposing certificates or private keys.
- [x] The default workspace lists active intents; filterable complete history
  and immutable event timelines expose every Trader- and management-originated
  intent.
- [x] Management can create intents only for active pairs and uses the same
  server-side command-idempotency rule as Trader WSS commands.
- [x] Creation, ordinary cancellation, and emergency flatten show exact
  preview data before one explicit confirm action.
- [x] Only zero-fill rows expose ordinary cancellation; filled/frozen rows
  expose emergency flatten.
- [x] All state-changing management actions use the existing CSRF/session
  controls and surface server rejection details.
