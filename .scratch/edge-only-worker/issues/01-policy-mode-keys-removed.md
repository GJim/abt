# 01 — 政策模式鍵刪除

**What to build:** 配對執行單元的 canonical 政策不再存在策略選擇鍵，shared 政策只剩 edge 所需的鍵；任何仍帶策略選擇鍵或趨勢家族鍵的設定檔與已持久化政策一律 fail-closed，絕不 reinterpret 為 edge。

**Blocked by:** None — can start immediately.

**Status:** ready-for-human

- [x] 設定檔含策略選擇鍵或任何趨勢家族鍵時啟動失敗，而非靜默忽略
- [x] leader 發布的 canonical 政策內容與策略政策雜湊不再覆蓋已刪除的鍵
- [x] follower 收到含已刪除鍵的政策時拒絕接受並維持未就緒
- [x] 已持久化的舊趨勢政策在重啟後 fail-closed，需要操作員介入
- [x] 配對設定檔權責規則同步更新，follower 不得撰寫 shared 政策的不變量不變
- [x] 相關政策驗證單測通過，全量測試套件綠燈

## Comments

- 實作於分支 `edge-only-worker`：`StrategyPolicy`、`SHARED_POLICY_KEYS`、合成預設值、政策雜湊、worker adapter 預檢與設定模板不再含 `entry_mode` 與 10 個 `trend_*` 鍵；`_policy_from_canonical` 對舊持久化政策逐鍵 fail-closed；adapter 未知鍵即啟動錯誤。
- 驗證：`tests.test_pair_cell` 339 過、`tests.test_pair_cell_adapter` 131 過（含新增的拒絕測試）。
