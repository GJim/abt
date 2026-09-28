# 05 — 文件同步與最終驗收

**What to build:** 領域用語與當前目標文件反映 edge-only 現實，殘留概念零命中，全量測試綠燈，本系列票可關閉。

**Blocked by:** 01 — 政策模式鍵刪除, 02 — 進場收斂為 edge 唯一語意, 03 — 保護對稱化與單飛移除, 04 — 趨勢管線與持久化清理.

**Status:** ready-for-human

- [x] 根目錄領域詞彙中描述趨勢分流與獲利腿的詞條已改寫為 edge 唯一語意
- [x] 之前「一腿最大化獲利」的當前目標已更新或記錄廢止決策，不再與本系列衝突
- [x] 產品碼與測試對殘留詞彙貪婪搜尋零命中（策略選擇鍵、趨勢家族鍵、趨勢模式名、單飛、獲利腿）
- [x] 全量測試套件綠燈

## Comments

- `CONTEXT.md` 已改寫 4 個詞條（限時同步配對退出、最佳進場候選、策略政策雜湊、每帳戶紐約已實現虧損預算）；`AGENTS.md` Current Objective 已更新為 2026-09-26 edge-only 並明示廢止 2026-09-08 不對稱目標。
- 零命中例外（刻意保留）：`_policy_from_canonical` 的舊鍵拒絕清單、migration 丟棄步驟名、拒絕／遷移測試的斷言字串、歷史日誌分析工具對舊日誌 marker 的計數、`cryptography.asymmetric` 第三方庫名。
- 全量 `unittest discover`：854 tests，僅剩預先存在的環境性失敗——2 個 integration（基線未改動即失敗，reconnect cooldown 時序）與 3 個 `pkcs11` 缺模組的載入錯誤（Linux-only 依賴）；`test_pair_cell` 328 與 `test_pair_cell_adapter` 122 全過。
