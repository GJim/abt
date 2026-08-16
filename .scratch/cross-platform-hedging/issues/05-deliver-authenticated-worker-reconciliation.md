# 05 — Deliver authenticated worker reconciliation

**What to build:** Give an approved 帳戶工作者 an authenticated WSS connection to the 主控台 so it can obtain its MT5 password in memory, report its account health and reconciliation state, and appear as a live account in the management UI without any broker write.

**Blocked by:** 04 — Deliver approved worker enrollment.

**Status:** ready-for-human

- [x] The worker and 主控台 establish WSS through P-256 device-certificate challenge-response; invalid, expired or mismatched identities cannot create a session.
- [x] The worker requests only its bound MT5 password over the authenticated WSS session; the 主控台 mediates the internal OpenBao read and the worker keeps the result only in memory.
- [x] The worker polls MT5 every minute, emits a full account snapshot every ten minutes and sends cursor-based deltas only for order/position lifecycle or volume changes.
- [x] The management UI displays worker connectivity, terminal/account health, latest snapshots and lifecycle deltas.
- [x] Contract tests demonstrate the behavior through the public REST/WSS and native-worker seams while proving no broker write method is invoked.
