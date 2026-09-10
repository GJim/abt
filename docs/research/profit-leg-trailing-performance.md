# 獲利腿移動停損追蹤：非對稱保護上線後觀察

**Scope.** 追蹤 commit `60aa718`（非對稱獲利最大化：leader trailing 獲利腿、
follower 靜態 SL-only 避險腿、單飛續跑）上線後的實際交易。本報告只看法
**新制下的獲利腿表現與配對結果**，不含舊雙腿對稱期的任何比較。
資料來源：本機 `leader*.log`、遠端 `follower*.log`／`follwer-*.log`（attempt ID 跨兩邊配對）、
leader 本地 sqlite（`worker.paircell.sqlite`）。分析工具見
[scripts/analyze_pair_performance.py](../../scripts/analyze_pair_performance.py)。

## 1. 配對數據（新制，9 對已平倉）

| # | 商品方向 | L（獲利腿） | F（避險腿） | pair 淨 |
|---|---|---|---|---|
| 1 | USDJPY LONG | −12.60 | −45.40 | **−58.00** |
| 2 | USDJPY LONG | −78.81 | +59.21 | **−19.60** |
| 3 | XAUUSD LONG | +38.81 | −40.70 | **−1.89** |
| 4 | USDJPY LONG | −104.03 | +89.37 | **−14.66** |
| 5 | USDCAD LONG | −20.66 | +2.66 | **−18.00** |
| 6 | USDCAD LONG | +26.89 | −45.71 | **−18.82** |
| 7 | USDJPY LONG（09:27→11:27，抱 2h） | +235.09 | −45.39 | **+189.70** |
| 8 | USDJPY LONG（11:28→13:02） | +92.89 | −46.57 | **+46.32** |
| 9 | USDJPY SHORT（13:02→13:57） | +67.07 | −46.56 | **+20.51** |
| 合計 | | +244.65 | −119.09 | **+125.56** |

獲利腿出場：自家 SL／TP 觸發（`owned_ticket_disappeared`）與 `maximum_holding_seconds`（2h）
約各半；trailing 累計 200＋ 次零 broker 拒絕；follower 全程零 trailing（符合設計）。

## 2. 觀察

### 2.1 放大確認：獲利腿真的能跑
單腿出現 +235.09、+92.89、+89.37（follower 側）、+67.07，虧損腿箝制在 cap 附近
（−40 ~ −46，leader cap $100）。#3（+38.81／−40.70，淨 −1.89）走出完整的
「贏家跑、輸家封頂」路徑；#1 trailing 把獲利腿虧損鎖在 −12.60，遠低於上限；
#7–#9 三連勝（+189.70／+46.32／+20.51）證明趨勢盤的贏家倍數（2–5× cap）足以覆蓋輸家。

### 2.2 結構風險：盤整盤付雙份
XAUUSD 在 01:17–03:09 走盤整：SHORT→LONG→SHORT→LONG 來回巴，避險腿 2–13 分鐘
被噪音洗掉（−41 左右），獲利腿單飛 15–30 分鐘後反轉也被洗掉（−19~−66），
三對淨 −60／−108／−94。02:01 那對距離上一對結束僅 16 秒就反手重打，
直接跳進同一個絞肉機。長期 EV 公式：
EV ＝ P(趨勢)×(W−C−摩擦) − P(盤整)×(雙份Cap＋摩擦)，
W 已證明夠大，優化重點是砍盤整稅，而非放大獲利。

### 2.3 進場頻率偏低
新制約 7 次進場／13h。成因：follower 單飛後停車及其重啟空窗（已修復，見 2.4）、
`authenticated peer session lost` 高頻出現（遠端鏈路不穩，每次都移除進場就緒）、
單飛＋2h 上限佔住 attempt（一次只能一對，設計如此）。

### 2.4 已解決：follower 單飛後停車
follower 清空後等對側 terminal proof，但 leader 故意單飛不清仓，60 秒後升級
NEEDS_HUMAN（9/9 共五次）。已修為 liveness 語意：對側任何已驗證 envelope 都重置等待窗，
只有真正靜默才升級；9/10 session 零停車。另附 solo transition 去重與
`_set_needs_human` 同因冪等，解 log 洗版。

## 3. 結論（provisional，n=9）
- 9 對合計 **+125.56**，由 09/10 趨勢盤三連勝翻正；機制（trailing＋單飛＋靜態避險腿）運作如設計。
- 最大風險是盤整盤雙輸；樣本仍小，結論待累積至 30 對再定案。

## 4. 限制
- NY 日切、swap、佣金未拆分；超 cap 損失的組成（滑價 vs swap）需 deal history 細拆。
- n=9，結論皆為 provisional。

## 5. 候選優化（已記錄，待觀察後再決定）
1. **Solo 證明窗**：peer 清空後 leader 限 N 分鐘（如 15 分）證明（trail 推進或浮盈為正），否則收工。
2. **同商品虧損冷卻**：上一對虧損後同 symbol 暫停 N 分鐘。
3. **震盪偵測**：1h 內同商品方向反覆 ≥2 次暫停該商品。
4. **贏家棘輪**：+1R 鎖 +0.5R、+2R 鎖 +1R；solo profit leg 取消 2h 上限（只留 trailing＋blackout）。
5. **Follower cap 檢討**：$40 在 XAUUSD 上數分鐘即觸發，是否放寬或與 leader 同步（代價是單次虧損變大，需定奪）。
