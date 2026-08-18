# 09 — Manage applicability and re-tests in Console

**What to build:** Let a 主控台管理員 use the management SPA to inspect a worker's applicability to an active 跨伺服器商品配對, run a manual compatibility check, explicitly exclude that one worker from that pair, and manually run 配對重新檢測 with eligible workers from the pair's two server endpoints.

**Blocked by:** 08 — Build, replace, and retire product pairs in Console.

**Status:** done

- [x] The active-pair view distinguishes uninspected-but-applicable, checked, and explicitly excluded workers without implying automatic exclusion from a detected difference.
- [x] A compatibility result renders live/reference specifications and separates hard-block from warning differences before an exclusion action is offered.
- [x] Exclusion is explicit, scoped to the selected worker/pair, and its completed state is visible after refresh.
- [x] The re-test form permits only healthy connected workers on the pair's two exact servers and shows the pair's original policy as fixed evidence.
- [x] Successful and failed re-tests show source workers, metrics, time, and alert/latest-failed state; failed re-tests never appear as an automatic retirement.
- [x] Browser-level interaction tests cover compatibility inspection, explicit exclusion, valid/invalid re-test worker selection, failed re-test alerts, and authenticated CSRF-protected actions.
