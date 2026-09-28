# 06 — 預設值對齊營運 config（holding／mode 除外）

**What to build:** 無設定檔時的合成預設值與營運 config 一致（`maximum_holding_seconds` 與 `mode` 維持原預設），新預設：`edge_min_net_points -4`、`maximum_margin_fraction 0.15`、`maximum_loss_per_trade_usd 60`、`daily_loss_warning_threshold_usd 30`、`quote_max_age_seconds 6.0`、`quote_max_skew_seconds 6.0`、`follower_confirmation_timeout_seconds 60.0`、`relay_handling_timeout_seconds 30.0`、`trading_blackout 19:30–20:30`。

**Blocked by:** 01 — 政策模式鍵刪除.

**Status:** ready-for-human

- [x] 10 個預設常數更新（holding=None、mode=shadow 不動）
- [x] 斷言舊預設的測試已更新；行為測試的顯式 fixture 不受影響
- [x] 兩個全量相關套件綠燈（test_pair_cell 328、adapter 122）
- [x] `CONTEXT.md` 預設值敘述同步；ADR 與 research 歷史文件不動

## Comments

- 衍生修正：warning threshold 30 使兩個 `-90` 情境測試觸發暫停，改為 `-80` 並同步期望值；retention 斷言改為 5+30=35（harness 仍 pin follower_confirmation 5.0）；adapter/integration 的 sizing 與 mirror 測試按既有慣例寫死風險參數（0.01/40），因新手數下粗保護盒裝不下 fixture 的 90 點跨平台價差。
- 使用者營運 config 與新預設僅差 mode／holding，可保留原檔運作（鍵皆合法），也可精簡至僅保留有意偏離預設的鍵。
