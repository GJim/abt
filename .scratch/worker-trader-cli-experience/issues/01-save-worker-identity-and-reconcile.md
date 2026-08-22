# 01 — Save Worker identity and reconcile directly

**What to build:** Let a Worker operator enroll interactively when enrollment flags are absent, save only non-secret Worker identity configuration after enrollment, and start `reconcile` directly from that saved identity. The operator can select an alternate configuration, must explicitly approve replacing an existing identity, and receives a clear pending-approval result instead of re-enrolling.

**Blocked by:** None — can start immediately.

**Status:** needs-triage

- [x] `abt-worker enroll` prompts for absent controller, MT5 account, server, and registration-invite inputs while preserving flag-based automation and the non-echoing password prompt.
- [x] Successful enrollment atomically writes versioned, non-secret Worker identity configuration beside the executable or at the explicit configuration path; password and invite are never written or displayed.
- [x] Existing identity configuration is preserved unless enrollment uses explicit replacement; missing, invalid, or unwritable default configuration produces a clear safe failure.
- [x] `abt-worker reconcile` reads its Worker identity from configuration, reports pending approval clearly, and does not accept conflicting identity overrides.
- [x] Public CLI tests cover prompting, flag precedence, secret exclusion, replacement protection, direct reconcile, and configuration failure behavior.
