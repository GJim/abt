# 02 — Deliver auditable management access

**What to build:** Give a 主控台管理員 a complete, secure management-access path: create/reset generated credentials from the console host, sign in through the same-origin React management surface, view audit events, and recover a locked source IP only through the local CLI.

**Blocked by:** None — can start immediately.

**Status:** resolved

- [x] A local console-host command creates and resets six-letter administrator accounts with generated 20-character passwords, invalidates old sessions, and writes append-only audit events.
- [x] The management surface authenticates with Argon2id-backed, Secure/HttpOnly/SameSite server sessions; state-changing requests require CSRF validation.
- [x] Three failed logins from the trusted Cloudflare client IP permanently lock that IP until an audited local CLI unlock; the Web UI has no recovery path.
- [x] The React login and audit experience works against the single-process DuckDB control-plane seam without permissive CORS or browser-stored bearer tokens.

## Answer

Delivered the local administrator CLI, Argon2id/session/IP-lock control-plane API, same-origin React login and audit experience, and browser/API/CLI coverage. The controller now serves a configured SPA build directory and only trusts `CF-Connecting-IP` from explicitly configured proxy peers.
