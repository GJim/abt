# 04 — Deliver approved worker enrollment

**What to build:** Let an operator securely enroll one native Windows 帳戶工作者 for one MT5 account: the worker interactively enters its MT5 password, proves a successful local login with account/terminal evidence, submits a signed registration, and the administrator reviews and approves or rejects it with a unique active account binding.

**Blocked by:** 02 — Deliver auditable management access; 03 — Deliver sealed console runtime.

**Status:** resolved

- [x] A worker interactively enters the MT5 password, successfully obtains actual account/terminal evidence, then obtains a short-lived, one-time controller challenge and sends the password and a CNG-backed ECDSA P-256 signed registration through Cloudflare TLS; the console immediately stores the password in OpenBao and the worker never persists it.
- [x] A native Windows worker displays the registration ID, pairing code, and actual account/terminal evidence needed for the operator's out-of-band comparison; it never displays, echoes or logs the password it pushes to the console.
- [x] Pending registration expires after 15 minutes, cannot be reactivated after rejection, atomically leaves pending before credential cleanup, and retries failed OpenBao password-secret deletion when rejected or expired. The controller does not trust or store forwarded client IP addresses.
- [x] The management UI lets an authenticated 主控台管理員 compare evidence, approve or reject the request, and records the decision immutably.
- [x] Approval issues an auditable 裝置憑證 and rejects any second active worker binding for the same MT5 login/server; thereafter the worker requests its memory-only password through authenticated WSS for every MT5 login.

## Answer

Implemented and validated native CNG-backed enrollment, OpenBao-mediated pending and active credentials, evidence review, expiry cleanup retries, immutable approval decisions, and per-login proved WSS password mediation.
