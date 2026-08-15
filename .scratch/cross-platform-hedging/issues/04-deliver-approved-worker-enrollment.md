# 04 — Deliver approved worker enrollment

**What to build:** Let an operator securely enroll one native Windows 帳戶工作者 for one MT5 account: create a one-time 帳戶 bootstrap grant locally, submit a signed hardware-key registration from the worker, review it in the management UI, and approve or reject it with a unique active account binding.

**Blocked by:** 02 — Deliver auditable management access; 03 — Deliver sealed console runtime.

**Status:** ready-for-agent

- [ ] A console-host bootstrap grant stores the MT5 password in OpenBao, binds expected login/server and the worker public key, is single-use, and never persists the password on the worker.
- [ ] A native Windows worker uses CNG-backed ECDSA P-256 proof to submit actual MT5 account information, an eight-digit pairing code and a registration request.
- [ ] Pending registration is rate-limited, expires after 15 minutes, and cannot be reactivated after rejection.
- [ ] The management UI lets an authenticated 主控台管理員 compare evidence, approve or reject the request, and records the decision immutably.
- [ ] Approval issues an auditable 裝置憑證 and rejects any second active worker binding for the same MT5 login/server.
