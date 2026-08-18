# Cross-server product-pair analysis

Status: done

## Problem Statement

主控台目前能安全地識別、批准與對帳帳戶工作者，但管理員仍要在終端機外手動比較兩個 MT5 server 的商品，判斷哪些外匯商品可建立反向等值曝險。這個流程無法重現、不能套用明確 threshold、沒有完整的審計證據，也無法讓同一對 server 的其他 worker 安全地共用已確認的跨伺服器商品配對。

管理員需要在 Console 選擇兩台不同 MT5 server 上、已批准、健康且已連線的帳戶工作者，設定或選擇分析政策，讓主控台向 worker 發起唯讀資料請求，篩選候選並建立可審核的跨伺服器商品配對。此功能必須維持目前控制平面的零 broker-write 保證。

## Solution

建立一個以 Console REST/WSS 商品配對分析契約為唯一主要接縫的唯讀垂直切片。管理員發起商品配對分析後，主控台向兩台選定 worker 請求完整商品目錄、規格與市場資料；分析依 UTC 最近完整交易週先以 M15 篩選、再以 M1 驗證。

第一版只建立兩端具有相同 base/profit currency 與相同原始 MT5 `trade_calc_mode` enum 的跨伺服器商品配對；主控台不得將 broker enum 映射為自訂類別。兩端必須位於不同、大小寫精確識別的 MT5 server。通過候選由管理員檢視不可變的分析證據與政策快照後 Build；若相同無方向端點已有 active 配對，管理員使用原子 Replace 取代它。配對 server-wide 適用於其他對應 worker；管理員可人工檢查個別 worker 的規格並核准排除。

## User Stories

1. As a 主控台管理員, I want to choose two healthy connected 帳戶工作者 on different MT5 servers, so that I can compare trusted sources rather than local terminal guesses.
2. As a 主控台管理員, I want the Console to reject two workers on the same exact MT5 server, so that a cross-server product pair never represents a same-server comparison.
3. As a 主控台管理員, I want to select or create an immutable 分析政策快照 before analysis, so that I can explain the thresholds used for every result.
4. As a 主控台管理員, I want to make a policy stricter or more permissive, so that different approved research criteria can be preserved instead of overwriting prior evidence.
5. As a 主控台管理員, I want the Console to request full symbol catalogs and specifications from both workers, so that candidate identity starts from broker-returned evidence.
6. As a 主控台管理員, I want automatic candidates to require the same base/profit currencies and exact broker-returned `trade_calc_mode` enum on both sides, so that suffix similarity or lossy enum translation cannot create a false hedge relationship.
7. As a 主控台管理員, I want products with mismatched calculation modes displayed as exceptions rather than automatically analyzed or built, so that broker-specific classifications remain auditable.
8. As a 主控台管理員, I want M15 screening followed by M1 verification over the same UTC complete trading week, so that expensive minute data is fetched only for plausible candidates.
9. As a 主控台管理員, I want all compared market times accompanied by raw broker epochs and calibration evidence, so that cross-server bar alignment can be audited.
10. As a 主控台管理員, I want an analysis to fail without buildable partial candidates when either worker disconnects, times out, or returns incomplete data, so that one-sided evidence cannot be approved.
11. As a worker operator, I want analysis requests to remain read-only MT5 operations, so that accepting analysis work cannot place, modify, cancel, or close broker orders.
12. As a platform operator, I want a worker to serve at most one analysis at a time and queue further requests, so that data collection does not crowd out reconciliation.
13. As a 主控台管理員, I want candidate results to show each hard-block difference, warning difference, coverage, correlation, price-difference statistics, policy, and source workers, so that I can make an informed Build decision.
14. As a 主控台管理員, I want hard-block fields and warning fields separated, so that price and quantity semantics are protected while account-specific warnings remain visible.
15. As a 主控台管理員, I want to Build a passing candidate only after an explicit confirmation, so that an analysis result cannot silently become an active trading relationship.
16. As a 主控台管理員, I want the built cross-server product pair to save an immutable 參考規格快照 and 分析政策快照, so that later worker checks have a fixed comparison baseline.
17. As a 主控台管理員, I want each FX v1 product pair to express a 1:1 lot relationship, so that risk sizing remains separate from product identity.
18. As a 主控台管理員, I want at most one active cross-server product pair for the same unordered server/symbol endpoints, so that opposite ordering cannot create duplicate active mappings.
19. As a 主控台管理員, I want to atomically Replace an existing active pair with a newly approved version, so that there is neither a duplicate active pair nor a no-active-pair gap.
20. As a 主控台管理員, I want other workers on the same two servers to be applicable by default, so that I do not need to rebuild the same product pair for every account.
21. As a 主控台管理員, I want to manually inspect a worker's live specification against the pair's reference specification, so that account-group differences are visible before I decide to exclude it.
22. As a 主控台管理員, I want a specification mismatch to remain evidence until I explicitly approve that worker's exclusion, so that the Console does not silently change my server-wide applicability policy.
23. As a 主控台管理員, I want an exclusion to affect only one worker and one product pair, so that unrelated products and workers remain available.
24. As a 主控台管理員, I want to manually re-test an active pair using any healthy worker from each of its two server endpoints, so that I can check current evidence without creating a new pair automatically.
25. As a 主控台管理員, I want re-tests to use the pair's original analysis policy, so that pass/fail results are comparable to the original approval.
26. As a 主控台管理員, I want a failed re-test to create an alert and visibly mark the latest test as failed while leaving the pair active, so that I retain the decision to retire it.
27. As a 主控台管理員, I want to retire an active pair without deleting its history, so that later analyses cannot use it while its approval trail remains explainable.
28. As a platform operator, I want every analysis, worker response summary, Build, Replace, compatibility check, exclusion, re-test, alert, and retirement recorded as immutable events, so that administrative decisions are auditable.
29. As a platform operator, I want analysis summaries, specifications, calibration evidence, and data hashes retained for at least one year without Web UI deletion, so that storage remains bounded while decisions remain reviewable.
30. As a release operator, I want automated proof that the entire analysis and administration flow invokes no broker-write operation, so that the read-only control-plane release gate remains true.

## Implementation Decisions

- Reuse the existing Console REST/WSS contract as the highest feature seam. The Console initiates and observes analysis, while the authenticated worker session carries correlated, controller-requested, read-only analysis requests and responses.
- The controller remains the only writer to the single-process DuckDB control-plane ledger. Analysis lifecycle, policy snapshots, candidates, product-pair lifecycle, worker applicability, alerts, and audit events are persisted through the existing serialized writer.
- An analysis may select only two approved, healthy, connected workers whose exact MT5 `server` strings differ. Server identity is the complete, case-sensitive MT5 `server` string; broker brand, aliases, terminal paths, and account funding size are not identity keys.
- An analysis is an atomic evidence job. It has catalog/specification, M15, and M1 stages. Each stage requires complete timestamped responses from both workers; timeout, disconnection, malformed data, or incomplete data fails the analysis and yields no buildable partial candidates. A failed stage may retry once.
- A worker serves no more than one analysis at a time. Further analyses involving that worker queue behind it; normal reconciliation retains priority over analysis traffic.
- Workers expose only read-only analysis operations: symbol catalog and specification inspection, plus bounded historical rate retrieval. No analysis request is allowed to reach a broker-write surface.
- The controller specifies the previous complete trading week using UTC bounds. Workers query using their market-data calibration and return raw epochs, derived UTC timestamps, and calibration evidence so the controller can align bars reproducibly.
- Historical completeness requires every returned FX symbol/timeframe series to include at least one UTC bar on each Monday through Friday in the requested week. Weekend closure is expected; broker maintenance, holidays, DST shifts, and intraday gaps do not imply missing evidence because this v1 policy intentionally does not infer broker trading-session schedules or a fixed intraday bar count. A missing weekday fails the stage before common-coverage evaluation.
- Candidate generation requires equal `currency_base`, `currency_profit`, and exact broker-returned `trade_calc_mode` enum on both symbols. The worker preserves the native enum; the controller does not translate it. A calculation-mode mismatch is rendered as an exception only and cannot enter automatic analysis or Build.
- Candidate evaluation is two-stage: common M15 bars screen every eligible FX candidate, then only candidates that pass M15 fetch common M1 bars for final verification.
- The initial policy defaults are: M15 return correlation at least 0.98; M1 return correlation at least 0.97; common-data coverage at least 99%; M1 median absolute price difference at most 2 target points; and M1 P99 absolute price difference at most 15 target points. Policies may set stricter or more permissive values, but every submitted analysis snapshots all values immutably.
- The reference comparison distinguishes hard-block fields from warning fields. Hard blocks include missing or non-tradeable symbols, digits, point, trade tick size, contract size, minimum volume, volume step, a common supported FOK or IOC filling mode, and the pair's required direction capability. Other filling modes do not make a pair buildable. Warnings include volume maximum, stops/freeze levels, tick value, margin/profit currency, and swap fields. `swap_mode` is a warning so that raw swap values are interpreted using their broker-provided calculation mode; a mismatch does not block Build.
- A passing candidate's Build screen presents immutable analysis evidence, policy snapshot, both reference specifications, thresholds, worker identities, UTC period, and calculated statistics. Explicit admin confirmation creates an active cross-server product pair and audit event.
- A cross-server product pair is unordered at the server/symbol endpoint level. It carries no execution direction: future common trading intent selects the main and hedge legs. FX v1 pairs have a fixed 1:1 lot relationship; margin and account-risk sizing remain a future intent concern.
- A database uniqueness invariant permits at most one active pair for an unordered endpoint pair. A normal Build conflicts with an existing active pair. Replace is a single ledger transaction that retires the old pair and creates the new active pair, with linked audit events.
- Build preserves the selected source workers as evidence sources but creates a server-wide pair. Other workers on either endpoint server default to uninspected-but-applicable for that pair.
- A compatibility check compares an individual worker's current symbol specification with the reference specification and reports hard-blocks and warnings. It never automatically changes applicability. Only an explicit admin exclusion removes that worker from that one pair; exclusion does not affect other workers or pairs.
- Active pairs have no automatic scheduled monitoring. An administrator can manually re-test an active pair using any healthy connected worker belonging to each of the pair's two exact server endpoints. The re-test uses the original policy snapshot, creates new immutable evidence, and does not rewrite the pair's reference snapshot.
- A failed re-test creates an alert and updates the visible latest-test status to failed, but leaves the pair active until an administrator retires it.
- Analysis storage retains immutable summaries, source identities, specifications, policy snapshots, calibration evidence, candidate statistics, and content hashes for at least one year. Full M15/M1 bar sequences are not persisted in the control-plane ledger and cannot be deleted through the Web UI.
- The management SPA adds worker-pair selection, policy selection/editing, queued/running/failed analysis visibility, candidate evidence, Build/Replace confirmation, active-pair listing, per-worker compatibility checks and exclusions, re-test, retirement, alerts, and audit navigation. Existing cookie-session and CSRF requirements apply to every administrative mutation.

## Testing Decisions

- The primary test surface is the observable Console REST/WSS product-pair analysis contract. Tests assert admin-visible lifecycle states, authenticated worker request/response behavior, persisted evidence, alerts, and authorization; they do not assert SQL layout, private scheduling fields, or individual MT5 wrapper calls.
- Follow the repository's existing `unittest` style with isolated temporary ledger storage, API clients, and fake authenticated worker sessions. Reuse existing worker reconciliation/session test patterns for WSS authentication, health, retries, and controller-to-worker communication.
- Contract tests cover selecting only approved healthy connected workers on different exact servers; rejecting same-server and unhealthy selections; requesting the catalog/M15/M1 stages; and preserving read-only behavior.
- Candidate tests cover currency and calculation-mode identity eligibility, calculation-mode exceptions, hard-block/warning classification, UTC alignment/calibration evidence, configurable immutable policy snapshots, M15-to-M1 staging, threshold pass/fail, and no partial result after a missing or malformed worker response.
- Queue tests prove one active analysis per worker, FIFO or documented queue behavior, one retry after transient failure, and reconciliation priority.
- Administrative contract tests cover Build confirmation, active endpoint uniqueness, atomic Replace, retirement, policy/audit linkage, and CSRF/session authorization.
- Applicability tests cover server-wide default applicability, compatibility evidence, no automatic exclusion after a mismatch, explicit per-worker-per-pair exclusion, and non-interference with other pairs.
- Re-test tests cover endpoint-server validation, use of the original policy snapshot, successful and failed evidence, failed-status alerts, and the invariant that failed re-tests do not automatically retire an active pair.
- Retention tests prove summaries and hashes are retained as audit evidence, raw bars are not stored, and Web endpoints cannot delete analysis history.
- Release-gate tests instrument the worker MT5 adapter and prove all analysis, Build, Replace, compatibility, re-test, and retirement scenarios perform zero broker writes.

## Out of Scope

- Broker writes of any kind, including market/pending orders, modifications, cancellations, closes, protection changes, emergency flattening, Trader registration, or intent dispatch.
- Automatic approval of calculation-mode mismatches.
- Quantity-ratio discovery, non-1:1 mappings, account-level risk sizing, margin allocation, execution sequencing, or hedged-order lifecycle management.
- Automatic scheduled re-testing, automatic suspension, or automatic worker exclusion after a failed analysis or compatibility check.
- Persisting complete M15/M1 bar sequences, adding a separate market-data warehouse, or allowing analysis history deletion from the Web UI.
- Server aliases, broker-brand grouping, or inference that account funding size proves a specification equivalence.
- Multi-controller/HA topology, multi-process ledger access, or any change to the existing controller ownership boundary.

## Further Notes

- The feature uses the domain terms **跨伺服器商品配對**, **參考規格快照**, **分析政策快照**, **商品配對分析**, **配對重新檢測**, and **工作者配對適用狀態** as defined in `CONTEXT.md`.
- The existing control-plane release remains read-only. This spec adds decision evidence and administration for future paired trading, not an execution capability.
