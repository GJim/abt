# 04 — 趨勢管線與持久化清理

**What to build:** 只為趨勢服務的報價採集與持久化殘留被清除，edge 所需的雙邊報價、skew、量規劃快取保留；持久格式不改版，不自動遷移舊資料。

**Blocked by:** 01 — 政策模式鍵刪除, 02 — 進場收斂為 edge 唯一語意, 03 — 保護對稱化與單飛移除.

**Status:** ready-for-human

- [x] leader 本地趨勢採集（分鐘級 mid buffer、去重 tape、趨勢逐門計數）若無其他消費者則一併刪除
- [x] 策略選擇鍵的持久化欄位與預填提示隨政策欄位刪除而清理，不做自動遷移舊資料語意
- [x] 量規劃、宇宙世代、配對路由、隔離狀態的持久格式維持不變
- [x] 影子路徑無趨勢分支殘留
- [x] 全量測試套件綠燈

## Comments

- 刪除 mid buffer、`TrendSeedEvent`／`TrendSeedPoint`、seed relay（adapter `fetch_trend_seed_points`、`_maybe_seed_trend`、`_trend_seeded`）、1 秒 tape 表與管線、admission stats 的 `entry_mode`／`trend_bias` 欄（經正式 migration 框架 `_pair_cell_schema_migrations` 丟棄，含新遷移測試）；`PRESERVED_SAFETY_TABLES` 同步移除 tape。
- 註：本票實作發現舊資料需遷移，故以 migration（非「不遷移」）處理欄位丟棄；量規劃／宇宙／路由／隔離格式確實未動。
- 驗證：`tests.test_pair_cell` 328 過、`tests.test_pair_cell_adapter` 122 過。
