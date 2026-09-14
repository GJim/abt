# Trend-following entry research (Donchian breakout / normalized momentum)

Status: ready-for-human
Type: research

## Question

除了現行跨平台價差 `edge_value` 進場外，有沒有更適合趨勢盤的進場計算邏輯？

## Context

- 現行進場 (`abt/pair_cell.py:1603 edge_value`) 是瞬時跨 broker 價差套利：
  `LONG: follower.bid - leader.ask`，`SHORT: leader.bid - follower.ask`，
  `edge_points = raw_edge / canonical_point >= entry_edge_points`（預設 4）。
- 獲利實際來自非對稱模型：leader（順 edge 方向）SL-only + trailing 單飛，
  follower 靜態 SL 犧牲。單邊趨勢出現時最賺，代表進場（均值回歸觸發）
  與獲利驅動（趨勢延續）是錯配的。
- 本研究只討論進場端，不動出場端（trailing / solo lock / maximum_holding_seconds）
  與風控端（allowance / sizing / quarantine / freshness-skew gates）。

## Candidates

### 1. Donchian 突破（首選）

- Rolling mid 高低點突破才進場，天生過濾盤整。
- 待決議：是否只用 leader 數據（避免 peer relay 延遲），init 拉歷史 + sliding window 維護。
- 詳見 `## Comments` 的 2026-09-13 討論。

### 2. 衝量 / 波動率正規化（次選）

- `score = (mid_now - mid_{now-T}) / vol`，`|score| > k` 才進場。
- 反應比 Donchian 快，適合趨勢前段衝量段，但追高風險較高。

### 3. EMA 快慢 + 強度過濾（備選，未深入）

- `trend = EMA_fast - EMA_slow`，`strength = |trend| / ATR`。
- 最穩但最慢，暖機與重啟空窗成本高，先不做。

### Hybrid（務實過渡）

- 保留現 edge 當計時器，加 trend gate：`direction != trend_bias` 直接跳過；
  或趨勢模式改要求 `spread < max` 而非 `edge > min`。可用 shadow 模式 A/B。

## Constraints for any future proposal

- Leader-only 決策；follower 不重算 edge/trend，只做非市場准入檢查。
- 現有 gates 不動：quote freshness / skew、sizing plan、quarantine、
  remaining allowance、one-attempt-per-decision-quote。
- 趨勢狀態必須是 durable + deterministic：重啟可恢復、舊版測試可重放。
- 不引入 signal-time RPC（不用進場當下 `copy_rates`）；歷史只在 init / refresh 階段拉。
- 先 shadow-mode 證據再談 live。

## Comments

### 2026-09-13 — user 選 1, 2 深入，提出兩個實作問題

1. 策略 1 是否可只用 leader 數據？init 先拉歷史、之後 sliding window 新進舊出？
2. 策略 1、2 分別需要哪些公式與數據？
3. 下一步：確認 leader-only 可行性與歷史來源（copy_rates vs tick buffer 自建），
   再把公式與參數起點定稿為 prototype ticket。

### 2026-09-13 — 補充釐清：tick vs bar 誤差

- 策略 1、2 正式邏輯都用 live `bid/ask` tick 算 `mid = (bid+ask)/2`。
- Phase B 若用 `copy_rates`（M1 OHLC）預填，只能拿到 bid 系的
  open/high/low/close，不是只有 close，但仍有三個失真：無 ask 側、
  盤中毛刺時序遺失、秒級以下精度遺失。只適合初始化 `upper/lower`，
  不適合直接觸發當下突破。
- bridge 另有 `copy_ticks_range`（`wine_mt5_bridge.py:296`）可拿歷史
  bid/ask tick（含 `time_msc`），無精度損失，但 payload 較重。
  Phase B 真要無損預填應該用它，而不是 `copy_rates`。
- Phase B 的好處不只省 30 分鐘暖機：重啟 / 重新部署 / 解除配對重配 /
  多商品并行暖機 / 回測 seeding 都受益。代價是協議擴充與回放標記複雜度。

## Decision (2026-09-13)

- 策略 1（Donchian）與策略 2（正規化衝量）都做，`config` 以 `entry_mode`
  三選一：`edge`（ legacy 預設）/ `donchian` / `momentum`。
- Phase A 與 Phase B 都做；buffer 採 1 秒重採樣（時間淘汰為主，
  `maxlen=4096` 只當安全閥），tick 狂吐不會縮 coverage。
- 只用 leader 本地數據算 bias；Phase B 優先 `copy_ticks_range`，
  `copy_rates_from_pos` M1 只當 degraded fallback。

## Implementation (2026-09-13)

- `abt/pair_cell.py`：`EntryMode` + 10 個 shared 政策欄位（`entry_mode`、
  `trend_lookback_seconds`、`trend_breakout_buffer_points`、
  `trend_min_range_points`、`trend_momentum_T_seconds`、
  `trend_vol_window_seconds`、`trend_momentum_k`、`trend_min_mom_points`、
  `trend_max_spread_points`、`trend_min_coverage`），預設 `edge` 行為不變；
  `donchian_bias` / `momentum_bias` 純函數；每商品 1 秒 mid deque；
  `TrendSeedEvent` 只寫 buffer（不碰 `_local_quotes`/relay/`_decided_quotes`）；
  `_candidates()` 在 trend 模式以 bias 定向 + spread 閘取代 edge 門檻，
  其餘 gates（freshness/skew/sizing/quarantine/allowance）不動。
- `abt/worker/pair_cell_adapter.py`：`fetch_trend_seed_points()`（ticks
  優先、bars degraded、全失敗回空且不拋）；`_maybe_seed_trend()` 每
  generation 每商品一次、best-effort；`parse_pair_cell_config()` 啟動期校驗。
- 測試：`test_pair_cell.py` 新增 `TrendBiasTests` + `TrendEntryTests`（15 例）；
  `test_pair_cell_adapter.py` 新增 config 權限/數值校驗 + `TrendSeedTests`（4 例）。
- 注意：政策 canonical 含新欄位 → hash 改變，換版需兩邊空倉重接受。
- `test_pair_cell_integration.py` 有 2 例在週日休市本來就會失敗（已在乾淨版驗證），
  與本次改動無關。

## Next

- shadow 模式跑 `donchian` vs `momentum` vs `edge` 同候選對比，看 trailing 後盈虧比。
- warmup 空窗若證實虧錢，再考慮把 seed 提前到 policy 接受前（目前只在 trend 政策下 seed）。

## Note (2026-09-13, 生產預設收緊)

- `DEFAULT_MODE`: `live` → `shadow`；`maximum_margin_fraction`: `0.10` → `0.01`；
  `daily_loss_fraction`: `0.03` → `0.02`；`trade_loss_fraction`: `0.02` → `0.01`。
- 舊配對的已接受政策不受影響（hash 凍結當時內容）；新配對無 config 即合成
  shadow 保守政策，`live` 需明確選擇。換版需兩邊空倉重接受的規則不變。
