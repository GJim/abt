# 05 — Cancel zero-fill intents and flatten filled exposure

**What to build:** Implement race-safe ordinary cancellation for Trader and
management origins, plus management-only emergency flatten.

**Blocked by:** 04 — Execute paired FOK and IOC limit intents safely.

**Status:** ready-for-human

- [x] Ordinary cancellation accepts only while both legs are zero-fill and
  atomically blocks undispatched orders before broker work begins.
- [x] Already-hung zero-fill orders are cancelled and immediately reconciled;
  `cancelled` is emitted only after both broker acknowledgements and zero-fill
  evidence.
- [x] A fill racing cancellation emits `rejected_due_to_fill` and enters the
  normal FOK/IOC safety path.
- [x] Trader cancellation is WSS-only and limited to its own intents.
- [x] Emergency flatten is available only to management for filled exposure;
  it cancels unfilled legs, closes filled legs, freezes on failure, and does
  not require a reason.
