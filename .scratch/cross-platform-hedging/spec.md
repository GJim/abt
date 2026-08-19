# Cross-platform hedging

Status: ready-for-agent

## Superseded delivery boundary

This document remains the historical specification for the completed
**安全通訊與唯讀對帳切片**. Its prohibition on broker writes, Trader
registration, and intent APIs is superseded for the next delivery by
[`../trader-intent-execution/spec.md`](../trader-intent-execution/spec.md).
That feature also replaces IP-based enrollment admission with role-bound,
single-use registration invites for both workers and Traders. It does not
retroactively change the behavior of already-approved workers.

## Problem Statement

目前人工測試 CLI 已驗證 MetaTrader 5 API 可用，但它只附著於單一 terminal，不能安全地管理跨網路、跨帳戶的自動化避險實驗。使用者需要先交付一個不會對 broker 寫入的控制平面，證明主控台可安全辨識、批准、監控及撤銷兩台位於不同網路的帳戶工作者，並在未來雙限價進場前建立可稽核的信任、對帳與故障處置基礎。

## Solution

建立以 Cloudflare Tunnel 發布的主控台 Web 服務、原生帳戶工作者與自建 OpenBao 秘密控制面的唯讀垂直切片。主控台以單一 ASGI process 的 DuckDB 控制平面帳本保存身分、狀態與不可變事件；管理員經同源 React SPA 批准工作者；原生 worker 使用 Windows CNG 或 Linux TPM 2.0/PKCS#11 的 ECDSA P-256 裝置身分建立 WSS 通道並只回報帳戶狀態。此切片明確不建立、修改、取消或平掉任何 broker 訂單，也不提供 Trader 註冊或 intent API。

## User Stories

1. As a 主控台管理員, I want to log into the management site with a generated account and high-entropy password, so that only an authorized operator can approve workers.
2. As a 主控台管理員, I want all login failures from the same Cloudflare client IP to lock after three failures, so that repeated password guessing is stopped until local intervention.
3. As a 主控台管理員, I want to unlock a blocked IP only from the console host CLI, so that the public management site has no account-recovery path.
4. As a 主控台管理員, I want management sessions to be HttpOnly, Secure, SameSite and CSRF-protected, so that browser tokens are not exposed to JavaScript or cross-site requests.
5. As a worker operator, I want to interactively enter the MT5 password during initial native-worker registration, so that it is not placed in a worker file or environment variable.
6. As a 主控台管理員, I want the console to store the initial password in OpenBao only after the worker proves a successful local MT5 login, so that the console remains the sole credential authority.
7. As a worker operator, I want the worker to use a non-exportable hardware/OS device key, so that its identity cannot be copied to another machine.
8. As a Windows worker operator, I want the worker to use CNG for its device key, so that it follows the platform-native hardware/OS key boundary.
9. As a Linux worker operator, I want the worker to use TPM 2.0/PKCS#11 for its device key, so that a future non-Windows worker has an equivalent trust boundary.
10. As a platform operator, I want hosts without a supported CNG/TPM/PKCS#11 keystore rejected as workers, so that software key-file fallback does not silently weaken the model.
11. As a worker, I want my initial password handoff and actual MT5 evidence bound to my hardware public key, so that another host cannot replay my registration.
12. As a worker, I want to submit actual MT5 `account_info`, `terminal_info`, my P-256 public key and an eight-digit pairing code during registration, and display the same evidence locally, so that the management site and operator have evidence to review.
13. As a 主控台管理員, I want to compare a worker’s pairing code and actual broker account before approval, so that I approve the intended host and account.
14. As a 主控台管理員, I want pending worker registrations to expire after 15 minutes and rejected registrations to stay rejected, so that stale approvals cannot be reused.
15. As a public service operator, I want ingress-level protection against registration abuse without trusting or storing forwarded client IP addresses, so that pending-registration storage cannot be exhausted by unauthenticated traffic.
16. As a 主控台管理員, I want one active 帳戶工作者 per MT5 login/server pair, so that two workers never control or observe the same transaction account concurrently.
17. As a worker, I want to receive an approved 裝置憑證 only after my public key and actual account have been reviewed, so that the WSS service can identify me as a specific worker.
18. As a worker, I want to prove possession of my ECDSA P-256 hardware key during WSS connection and rotation, so that a public certificate alone cannot authenticate a host.
19. As a worker, I want to rotate to a new key pair and certificate every 30 days, so that long-lived device-key exposure is bounded.
20. As a worker, I want to request my own MT5 password only through an authenticated WSS session, so that OpenBao remains internal to the console Compose network.
21. As a 主控台管理員, I want OpenBao to hold MT5 passwords, PKI material and controller signing keys, so that high-value secrets are centrally controlled and audited.
22. As a platform operator, I want OpenBao to auto-unseal using same-host SoftHSM and a pinned PKCS#11 plugin, so that a console restart can recover without weakening the Compose network boundary.
23. As a platform operator, I want startup health checks to prove OpenBao is unsealed and can read a least-privilege secret, so that the service is not declared healthy before its trust dependencies work.
24. As a 主控台管理員, I want to see approved workers, their terminal/account health and their latest reconciliation state, so that I can identify unhealthy accounts without entering a worker host.
25. As a worker, I want to send a full account reconciliation snapshot every 10 minutes, so that the controller has a regular truth anchor.
26. As a worker, I want to poll local MT5 every minute, so that order and position lifecycle changes are discovered promptly without high-frequency price-driven noise.
27. As a worker, I want to emit an immediate cursor-based delta only when an order or position is created, filled, cancelled, modified, closed or changes volume, so that urgent events represent exposure changes.
28. As a 主控台管理員, I want price, floating P&L and ordinary account-summary changes to remain in periodic snapshots, so that the urgent event stream is not flooded by market movement.
29. As a 主控台管理員, I want an external broker change to create an immutable event, high-priority alert and “needs human handling” worker state, so that manual activity is visible before trading automation exists.
30. As a worker, I want a bidirectional heartbeat every 30 seconds, so that both sides can distinguish an active WSS connection from a stale one.
31. As a future trading operator, I want a five-minute missed-heartbeat grace period to enter 失聯安全平倉, so that short WAN/Cloudflare interruptions do not cause unnecessary closures.
32. As a worker, I want terminal/API failures retried with bounded backoff and to stop after three failures or account mismatch, so that a wrong login or persistent terminal failure is escalated rather than hidden.
33. As a 主控台管理員, I want to revoke a worker certificate and immediately block reconnects, so that a lost or suspect host can no longer participate.
34. As a future trading operator, I want revocation to map to `REVOKE_AND_FLATTEN` and frozen pairs, so that certificate compromise has deterministic safety semantics when broker writes are introduced.
35. As a 主控台管理員, I want to inspect append-only events for approvals, rejections, sessions, locks, snapshots, deltas, alerts and local CLI actions, so that every control-plane decision is auditable.
36. As a platform operator, I want audit and reconciliation events retained for at least one year and never deleted through the Web UI, so that experiments remain explainable.
37. As a platform operator, I want hourly local DuckDB/OpenBao backups retaining 24 rotations and immediate backup after PKI changes, so that ordinary operator or storage mistakes have a bounded recovery point.
38. As a platform operator, I want to make a manual encrypted offsite backup at least weekly, so that a console-host loss has a documented recovery path.
39. As a deployment operator, I want the console Compose stack to expose no host ports and allow only cloudflared to reach the controller, so that Tunnel-only ingress cannot be bypassed.
40. As a deployment operator, I want OpenBao Raft, SoftHSM tokens, DuckDB ledger and Tunnel credentials on distinct persistent volumes, so that sensitive state has explicit persistence boundaries.
41. As a future Trader, I want a unique deployment identity and an idempotent `202 Accepted` intent submission boundary, so that intent submission can later be introduced without changing the control-plane trust model.
42. As a future Trader, I want to cancel only an intent whose two legs are both unfilled, so that a Trader cannot turn a filled paired position into an uncontrolled exit.
43. As a future 主控台管理員, I want a reasoned and audited emergency flatten operation, so that filled positions retain a human safety valve without granting Trader exit authority.
44. As a release operator, I want to validate the whole read-only slice across three network environments and two workers, so that the first broker-writing slice is built on demonstrated rather than assumed reliability.
45. As a release operator, I want automated and live verification to prove the slice performs zero broker writes, so that activating it against tested accounts cannot place, modify, cancel or close a trade.

## Implementation Decisions

- The first release is the **安全通訊與唯讀對帳切片**. It has no broker write capability, no Trader registration and no intent API.
- The **主控台 Web 服務** is a Docker Compose deployment exposed only through Cloudflare Tunnel. It contains the controller, OpenBao, SoftHSM/PKCS#11 auto-unseal support, cloudflared and the SPA delivery surface. No service publishes a host port.
- The management UI is a separately built React SPA delivered at the same origin as the API. It uses cookie sessions; it must not use localStorage bearer tokens or permissive CORS.
- The controller is a single FastAPI/ASGI process. It is the only process allowed to read/write the DuckDB ledger. REST, WSS and host-local CLI changes use one serialized transaction writer. This is ADR 0003’s non-negotiable concurrency boundary.
- The ledger is the authoritative store for administrator credentials and sessions, IP locks, worker enrollment, worker/account association, reconciliation cursors and immutable control-plane events.
- Administrator usernames are generated six uppercase letters. Passwords are generated from uppercase/lowercase letters, digits and `!@#$`, with a length of 20. Passwords use Argon2id with an independent salt.
- A successful administrator login rotates a server-side session. Sessions expire after 30 minutes of inactivity or eight hours absolute lifetime. Password reset invalidates every session for that administrator.
- Failed login attempts are tracked by the trusted Cloudflare client IP. Three failures permanently lock that IP until the console-host CLI unlocks it.
- Worker enrollment accepts only a valid ECDSA P-256 proof over canonical login/server/pairing-code data. The worker public key is P-256 and must be backed by CNG on Windows or TPM 2.0/PKCS#11 on Linux; no software-key fallback is permitted.
- During initial registration, the worker operator enters the MT5 password interactively. After successful local MT5 login, the native worker sends the password as a non-displayed, non-echoed request field alongside actual account/terminal evidence and a hardware-key proof through Cloudflare TLS. The controller immediately stores the password in OpenBao, never logs it, and deletes the pending secret when registration expires or is rejected.
- Approval uses the worker’s real MT5 `account_info`, pairing code and observed source. Approval creates the only active worker binding for a login/server pair. Replacing a worker requires revoking or disabling the old binding.
- An approved worker obtains an **裝置憑證** and uses challenge-response over WSS. Device certificates rotate every 30 days with a new hardware-backed key pair.
- OpenBao stores the controller signing material, device PKI and MT5 credentials. SoftHSM plus a version-locked PKCS#11 KMS plugin supplies auto-unseal. The startup health check must verify unseal and least-privilege secret access.
- Workers never connect to OpenBao. An approved authenticated worker uses WSS for every MT5 login to request its own credential; the controller verifies worker/account binding and mediates the OpenBao read. The password remains memory-only and trusts Cloudflare’s TLS proxy boundary.
- WSS heartbeats run every 30 seconds. A worker enters the existing five-minute lost-link safety state after no valid controller signal. In this slice it records/alerts only because no positions are ever created.
- Workers poll MT5 every minute, emit exposure-lifecycle deltas immediately and full snapshots every 10 minutes. Terminal/API recovery uses bounded backoff and stops after three failures or an account mismatch.
- External changes produce immutable events, high-priority alerts and a worker state requiring human attention. For later paired trading they also freeze affected pairs.
- The management UI includes login/logout, pending enrollment review/approval/rejection, approved-worker health, reconciliation snapshots/deltas, alert acknowledgement, certificate revocation and audit viewing.
- Backups run hourly locally with 24 rotations and immediately after PKI changes. A human performs encrypted offsite backup weekly. Audit/reconciliation retention is at least one year and cleanup is host-local, controlled and itself audited.
- Later trading behavior is already constrained: each Trader deployment has an independent identity; accepted intent submissions are idempotent `202` responses after durable ledger write; Trader cancellation is allowed only before either leg fills; only a management emergency-flatten operation can close filled exposure.

## Testing Decisions

- The highest testing seam is the observable console REST/WSS contract plus a real native-worker adapter. Tests must assert user-visible authorization, state transitions, events, reconciliation and absence of broker writes, not DuckDB SQL layout or private methods.
- Existing tests use Python `unittest`; new control-plane tests follow this style and use temporary isolated storage and API test clients.
- Unit tests cover ledger transactions, generated administrator credentials, Argon2id/session expiry, CSRF enforcement, one-account/one-active-worker enforcement, enrollment expiry/rejection, certificate/rotation validation and immutable event creation.
- API contract tests cover signed P-256 registration, approval/rejection, WSS authentication, and credential mediation authorization.
- Worker-adapter contract tests run the same high-level enrollment and reconciliation scenarios against CNG and TPM/PKCS#11 implementations where the platform supports them; unsupported keystores must fail closed.
- Reconciliation tests assert one-minute poll scheduling, ten-minute full snapshots, cursor continuity over reconnect and immediate deltas only for order/position lifecycle changes.
- Failure tests simulate terminal loss, MT5 API error, account mismatch, WSS loss, credential revocation and external broker change. They assert bounded retries, alerts, frozen/needs-human states and no broker write invocation.
- Compose integration tests assert no host ports, only cloudflared ingress, persistent-volume separation, OpenBao auto-unseal health and single-ASGI-process enforcement.
- The release gate runs against the console and two native workers in separate network environments. It proves registration/approval, password mediation, reconciliation, revocation, recovery and external-change alerts, then verifies no MT5 broker write API was called.

## Out of Scope

- Any broker write: market/limit/stop orders, modification, cancellation, close, close-by, protection-price writes and emergency flatten execution.
- Trader registration, Trader REST APIs, intent persistence/dispatch and dry-run intent APIs.
- Platform selection, cost sorting, symbol-pair configuration, FOK dual-limit entry, single-leg recovery, protective exits and paired-order lifecycle execution.
- A production multi-controller/HA database topology, multi-process DuckDB access, automatic offsite backup replication and zero-RPO recovery.
- Software-key fallback, passwords in worker files/environment variables, direct worker access to OpenBao, public OpenBao ingress and application-layer encryption beyond the accepted Cloudflare TLS boundary.
- MFA, Web password recovery, a general user-registration workflow, additional operator roles and a full trading/risk analytics dashboard.
- Manual use of a worker-owned terminal for trading.

## Further Notes

- The legacy manual test CLI remains separate from the controller/worker system. It may retain its existing local credential behavior; it must not become a controller, worker or paired-order decision maker.
- ADR 0002 governs OpenBao, SoftHSM and PKCS#11 auto-unseal. ADR 0003 supersedes the earlier SQLite-ledger decision and requires one ASGI process to own DuckDB.
- The current codebase contains an initial ledger/API foundation. The spec is authoritative for completing it; incomplete scaffolding must not be represented as a completed release gate.
