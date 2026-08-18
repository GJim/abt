# 08 — Build, replace, and retire product pairs in Console

**What to build:** Let a 主控台管理員 inspect a passing candidate's immutable evidence in the management SPA, explicitly Build it into an active 跨伺服器商品配對, atomically Replace an existing pair for the same endpoints, or retire a pair while retaining its history.

**Blocked by:** 07 — Launch and view product-pair analyses in Console.

**Status:** done

- [x] The candidate view presents both reference specifications, policy snapshot, source workers, UTC period, statistics, and 1:1 lot relationship before Build is enabled.
- [x] Build requires an explicit confirmation and handles normal build conflict, replacement, and server-side validation errors clearly.
- [x] The active-pair view lists pair status, unordered endpoints, approval evidence, source workers, latest re-test state, and retirement history.
- [x] The SPA provides explicit Replace and retire actions with CSRF protection and refreshes the active-pair state after each outcome.
- [x] Browser-level interaction tests cover Build confirmation, Replace, uniqueness conflict presentation, retirement, and no UI path that implies a broker-write action.
