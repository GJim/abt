# 單腿移動停損 vs 雙腿對稱停損：上線後績效比較

**Scope.** 比較 commit `60aa718`（非對稱獲利最大化：leader trailing、follower 靜態 SL-only、單飛續跑）
上線前後的實際交易績效。資料來源只有本機 `leader.log`（2026-09-09 01:17:58Z–14:32:27Z，全程新碼）、
遠端 `follwer-1.log`（01:17–02:54Z）、`follwer-2.log`（02:55–09:17Z）、`follower.log`（14:27–14:37Z），
以及 leader 本地 sqlite（`worker.paircell.sqlite`，新舊兩期 leader 腿皆完整）。
舊期 follower 腿無 log 留存（`20260908.log` 已輪替刪除），舊期 pair-net 因此無法重建。
本報告不建議立即切換任何參數；第 5 節是候選優化方向。

新碼上線點以 `leader.log` 首筆 `asymmetric_protection_applied`（01:18:18Z）為界；
舊期定義為 sqlite 中 `recorded_at < 2026-09-09T01:17` 的 leader 腿。

## 1. 數據總覽

### 舊期（雙腿對稱，~9/8 15:50Z–9/9 00:24Z，約 8.5h）

| 指標 | leader 腿（sqlite，完整） | follower 腿 | pair 淨 |
|---|---|---|---|
| 筆數 | 48 legs | 無資料 | 未知 |
| 總和 | −402.05 USD | 未知 | 未知 |
| 勝率 | 21/48（44%） | 未知 | 未知 |
| 均值 | −8.38／腿 | 未知 | 未知 |
| 最佳／最差 | +53.03／−52.36 | 未知 | 未知 |

舊模型強制兩腿鏡像 1:1 SL/TP（LONG TP = SHORT SL），單腿損益被箝制在 ±40 USD 附近
（follower `maximum_loss_per_trade_usd=40`），這是舊期單腿分佈窄的主因。

### 新期（單腿 trailing，9/9 01:18Z–14:32Z，約 13h，attempt ID 配對，6 對已平倉＋1 對進行中）

| # | 商品方向 | L（leader） | F（follower） | pair 淨 |
|---|---|---|---|---|
| 1 | USDJPY LONG | −12.60 | −45.40 | **−58.00** |
| 2 | USDJPY LONG | −78.81 | +59.21 | **−19.60** |
| 3 | XAUUSD LONG | +38.81 | −40.70 | **−1.89** |
| 4 | USDJPY LONG | −104.03 | +89.37 | **−14.66** |
| 5 | USDCAD LONG | −20.66 | +2.66 | **−18.00** |
| 6 | USDCAD LONG | +26.89 | −45.71 | **−18.82** |
| 合計 | | −150.40 | +19.43 | **−130.97（均 −21.8／對）** |

leader 出場原因：`owned_ticket_disappeared` ×3（自家 SL／TP 觸發）、`maximum_holding_seconds` ×3（2h 到期）。
trailing 實際觸發 82 次（均 ~12 次／對），follower 零 trailing（符合設計）。

## 2. 觀察

### 2.1 近端目標達成了：單腿真的能放大
舊期 leader 單腿最佳 +53.03；新期 follower 出現 +89.37、+59.21，leader +38.81，
虧損腿也被箝制在 cap 附近（−40 ~ −46）。非對稱 payoff（贏家奔跑、輸家封頂）確實發生，
尤其是 #3（XAUUSD：+38.81／−40.70，淨 −1.89，近乎教科書級對沖）和 #1（trailing 把
leader 虧損鎖在 −12.60，遠低於 $100 上限）。

### 2.2 但 pair-net 六戰六負：贏家出場仍被另一腿綁住
六對淨值全負（−58 ~ −2），平均每對摩擦約 **−15 ~ −20 USD**（雙邊點差＋滑價，0.98 手量級）。
機制上：輸家觸及靜態 SL → 贏家單飛 → 但贏家隨後被 `maximum_holding_seconds`（2h，六對中三對）
或自家 SL 收掉，**贏家沒有跑到能覆蓋「輸家 cap＋摩擦」的幅度**。#2（−78.81／+59.21）與
#4（−104.03／+89.37）更顯示：當 leader 看錯邊，trailing 也救不了整對（−104 甚至超過
$100 cap，超出部分是滑價／swap／點差）。
結論：保護形狀不是當前瓶頸，**出場耦合＋摩擦才是**。

### 2.3 進場頻率崩跌：~2.8 對／h → ~0.5 對／h
舊期約 24 對／8.5h；新期 7 次進場／13h。成因有三：
1. **follower 每對必停車一次**（見 2.4），期間＋重啟空窗無法進場；
2. `authenticated peer session lost` 在 leader.log 出現 574 次——遠端鏈路不穩，
   每次都移除進場就緒；
3. 單飛＋2h max-holding 佔住 attempt，一次只能一對（設計如此，舊期周轉快是因為對稱 защита 同時出場）。

### 2.4 新策略的副作用：follower 每次單飛後約 60 秒停車
`follwer-1.log` 01:20:04 follower 腿結清（−45.40）→ 01:21:04 `close_needs_human：
peer terminal proof unavailable`。同模式在 02:19、02:56、08:03、14:29 重演。
根因是設計衝突：follower 清空後等對側 terminal proof，但 leader 依新策略**故意不清仓、繼續單飛**，
60 秒（`follower_confirmation_timeout_seconds=10` ×6 升級倍率）後升級為 NEEDS_HUMAN。
此停車在 leader 終於出場並回報 empty 後會自動解除（`peer_terminal_proof_recovered`），
中間還附帶數百行的 `close_needs_human` log 洗版。follwer-1（01:21–02:54）、02:55 與 14:27 的重啟，
至少部分是看到停車狀態後的人工介入——其實等 leader 出場就會自動恢復。

### 2.5 次要：solo transition 重複
leader `peer_leg_empty_leader_continues_solo` 每對出現 6–13 次（共 32 次／7 對）。
follower 在單飛期間反覆回報 `empty`，每次都重新走一次 solo 分支。無害，但噪音大；
應只在 `previous_status != "empty"` 時轉換。

## 3. 與舊制比較的誠實結論
- **pair-net 口径：無法判定新優於舊。**舊期缺 follower 腿數據；若舊對稱模型 pair-net ≈ −摩擦，
  新期 −21.8／對並不更好。樣本也太小（新期僅 6 對）。
- **leader 腿口径：新期均值（−25.07）比舊期（−8.38）差，但這是預期內的**——新模型允許單腿跑到
  ±100 而舊模型箝制在 ±40，方差放大；舊期勝率 44% vs 新期 33% 同理。不能直接比均值。
- **能確定的正面訊號**：trailing 至少一次顯著鎖盈（#1 −12.60），#3 驗證了「贏家跑、輸家封頂」的完整路徑，
  82 次 trail 零 broker 拒絕（無 `profit_trail` 相關 containment），機制本身穩定。

## 4. 限制
- 舊期 follower 腿無資料；新期 n=6，結論皆為 provisional。
- NY 日切、swap、佣金未拆分；−104.03 超 cap 的組成（滑價 vs swap）需 deal history 細拆。
- 14:27 USDJPY 第 7 對在 log 截止時仍進行中，未納入。

## 5. 優化方向（依建議優先序）

1. **Solo-aware terminal proof（關鍵，已實作待部署）**：原方案是 leader 轉單飛時送顯式狀態；
   實際採用更小的修法——對側任何已驗證 envelope 都視為存活並重置等待窗，只有真正靜默才升級
   （`_accept_relay_envelope` 重置 `_peer_proof_wait_started`；等待上限改由對側的 maximum holding 界定）。
   同批加上 solo transition 去重（狀態變化才記）與 `_set_needs_human` 同因冪等（解 log 洗版）。
   舊測試 `test_repeated_identical_peer_chatter…` 已按 liveness 語意改寫為
   `test_live_peer_chatter_resets_the_terminal_proof_window`。
   參考：[abt/pair_cell.py](../../abt/pair_cell.py) `_await_peer_terminal_proof`、`_handle_peer_leg_status`。
2. **贏家出場**：`maximum_holding_seconds=7200` 砍掉 3 個贏家／半贏家。考慮對 profit leg 取消時間上限
   （只留 trailing＋blackout），或按商品波動率設不同上限；至少先量測「若不砍，贏家會多跑多少」。
3. **摩擦門檻**：~$18／對是硬成本。`entry_edge_points=1` 過低；用 DuckDB 歷史報價回測 edge 門檻
   （如 2–3 points）對進場數與 pair-net 的關係，目標讓期望 edge ≫ 摩擦。
4. **Trailing 變體**：固定 1×risk 距離在 #4 仍吃滿虧損。試驗 breakeven-fast（+0.5R 即移 SL 至成本）
   或 0.5×risk 緊 trailing，用 shadow 模式先比決策不下單（影子模式已支援，見 CONTEXT「影子模式」）。
5. **鏈路**：`peer session lost` ×574 需排查（遠端主機網路／relay），它直接吃掉進場就緒。
6. **樣本**：至少累積 30 對再下勝率結論；分析工具已入庫為
   [scripts/analyze_pair_performance.py](../../scripts/analyze_pair_performance.py)，
   用法見該檔 docstring（`--leader-log/--follower-log/--sqlite/--cutoff`）。

## 6. 追蹤：09/10 趨勢盤驗證與雙輸風險（2026-09-10 追加）

資料：`leader-3.log`／`follower-2.log`（09/10 04:59Z–13:49Z，USDJPY 趨勢盤，含 liveness 修復的新碼）。
此三對確認 solo 模型在趨勢盤的獲利能力，同時確認盤整盤的雙輸結構風險；**暫不改碼，繼續觀察**。

| # | 商品方向（進→出） | L | F | pair 淨 |
|---|---|---|---|---|
| 7 | USDJPY LONG（09:27→11:27，抱 2h） | +235.09 | −45.39 | **+189.70** |
| 8 | USDJPY LONG（11:28→13:02） | +92.89 | −46.57 | **+46.32** |
| 9 | USDJPY SHORT（13:02→13:57） | +67.07 | −46.56 | **+20.51** |
| 小計 | | +395.05 | −138.52 | **+256.53** |

solo 上線以來 9 對已平倉合計：−130.97＋256.53 ＝ **+125.56**，由負翻正。

### 確認的事
- 贏家倍數 2–5× cap（+235／+93／+67）完全覆蓋輸家 cap＋摩擦，趨勢盤 EV 為正。
- 方向 2、5 節部分緩解：本 session **零 NEEDS_HUMAN**（liveness 修復有效），trailing 134 次零拒絕。

### 仍未解決：盤整盤付雙份（結構性）
- 舊對稱模型：follower SL 觸發價＝leader TP，不可能雙輸（淨 ≈ −摩擦）。
- 新 solo 模型：follower 先停（−41），leader 單飛反轉也停（−19~−66），淨 −60~−108。
  XAUUSD 01:17–03:09 四連對（SHORT→LONG→SHORT→LONG，follower 2–13 分鐘被噪音洗掉、
  leader 單飛 15–30 分鐘後也被洗掉）即為此形，另含 02:01 上一對結束僅 16 秒就反手重打。
- 長期 EV 公式：EV ＝ P(趨勢)×(W−C−摩擦) − P(盤整)×(雙份Cap＋摩擦)。
  W 已證明夠大，**優化重點是砍盤整稅，而非放大獲利**。

### 候選優化（已記錄，待觀察後再決定）
1. **Solo 證明窗**：peer 清空後 leader 限 N 分鐘（如 15 分）證明（trail 推進或浮盈為正），否則收工；
   XAUUSD 三對雙輸會變成 −40＋小虧／打平。
2. **同商品虧損冷卻**：上一對虧損後同 symbol 暫停 N 分鐘（02:01 反手單根本不會發生）。
3. **震盪偵測**：1h 內同商品方向反覆 ≥2 次暫停該商品。
4. **贏家棘輪**：+1R 鎖 +0.5R、+2R 鎖 +1R；solo profit leg 取消 2h 上限（只留 trailing＋blackout）。
5. **Follower cap 檢討**：$40 在 XAUUSD 上 2 分鐘即觸發，是否放寬或與 leader 同步（代價是單次虧損變大，需定奪）。
