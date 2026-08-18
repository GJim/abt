# 07 — Launch and view product-pair analyses in Console

**What to build:** Let a 主控台管理員 use the management SPA to select two approved, healthy, connected 帳戶工作者 from different exact MT5 servers, set an analysis policy, submit a 商品配對分析, and inspect its complete progress and evidence. The screen makes catalog, M15, M1, final candidate, exception, policy, calibration, statistic, retry, and failure information understandable without using a REST client.

**Blocked by:** None — can start immediately.

**Status:** done

- [x] The SPA loads eligible workers and prevents same-server, unhealthy, disconnected, or revoked selections before submitting.
- [x] The policy form exposes all backend-supported FX policy values and makes the submitted snapshot visible in the result.
- [x] The analysis view presents queued/running/succeeded/failed lifecycle, current stage, retry count, source workers, UTC period, and error reason.
- [x] The candidate view distinguishes passing, failing, and calculation-mode exception results and renders hard-block versus warning differences separately.
- [x] Browser-level interaction tests cover successful submission, validation failures, terminal failure rendering, and authenticated CSRF-protected requests.
