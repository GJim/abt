# 盤整日復盤 2026-09-15：solo 雙虧與進場過鬆

**Scope.** 復盤 2026-09-15 台北 13:10~18:24（UTC 05:10~10:24）7 組已平倉 pair，
全數走非對稱保護＋單飛續跑（`prot_asym`＋`peer_leg_empty_leader_continues_solo`）。
只看當日這 7 對，不含舊對稱期比較。
資料來源：本機 `202609151329.log`（leader）、遠端 `fl-202609151310.log`（follower，
attempt ID 前 8 碼跨兩邊配對）、leader 本地 sqlite（`worker.paircell.sqlite`，
含 `cell_attempts`、`cell_realized_pnl`、`cell_market_tape_1s` 1 秒 tape）。
分析工具見 [scripts/analyze_pair_performance.py](../../scripts/analyze_pair_performance.py)；
MFE/MAE 與門檻重播為一次性腳本直讀 tape＋`momentum_bias` 重算，見下文方法。

## 1. 配對數據（7 對，pair net −127.88）

| 進場(UTC) | 商品方向(leader) | L | F | pair 淨 | 出場 |
|---|---|---|---|---|---|
| 05:30 AUDNZD SHORT 0.56 | +5.21 | −34.02 | **−28.81** | leader trail 成果出場，follower 先停 |
| 05:53 AUDCHF LONG 0.56 | −5.56 | −10.20 | **−15.76** | `peer_leg_empty` 後 leader 市價砍 |
| 06:09 USDCHF LONG 0.4 | −2.00 | −8.75 | **−10.75** | 雙停，trail 只保到 breakeven |
| 06:32 AUDJPY LONG 0.56 | −3.16 | −10.23 | **−13.39** | 5 次 trail 後反轉雙停 |
| 07:02 AUDUSD SHORT 0.56 | −33.04 | +16.80 | **−16.24** | leader SL（超 cap），follower 存活 |
| 07:40 EURUSD SHORT 0.34 | −3.06 | −2.72 | **−5.78** | `peer_leg_empty` 雙小虧 |
| 08:04 NZDCHF SHORT 0.69 | −3.46 | −33.69 | **−37.15** | follower 速停（超 cap），leader trail 保住 |
| 合計 | | −45.07 | −82.81 | **−127.88** |

（F 合計與 follower log 口徑 −83.19 差約 0.4，為 NY 切日／小數位；leader NY 日 sqlite
合計 −47.09，含此份 log 之外的今晨 −0.34、−1.68 兩腿。）

當日無 `maximum_holding_seconds`（5400s）出場，最長一對約 38 分鐘（含收斂）；
leader trail 20 次、solo 3 次（`1256ebaa、2c6e23c2、7a087816` 皆 follower 先停、
leader 續抱後也停）。

## 2. 方法：tape MFE/MAE 與門檻重播

- 每對以 leader 進場價為基準，對 `cell_market_tape_1s`（`source='live'`）進場後
  ~25–40 分鐘 mid 序列算 MFE／MAE：
  AUDNZD ＋87／−18、AUDCHF ＋15／−22、USDCHF ＋15／−60、AUDJPY ＋61／−45、
  AUDUSD ＋3／−77、EURUSD ＋1／−50、NZDCHF ＋25／−37（單位：各商品 canonical point 數）。
- 門檻重播：同一份 tape history＋進場當下 leader mid，直接呼叫
  `abt.pair_cell.momentum_bias`，比較舊參（`k=2.0、min_mom=5pt、T=120s、coverage=0.8`）
  與候選新參（`k=2.5、min_mom=8pt、T=180s、coverage=0.9`）。
  限制：tape 是 1 秒重採樣，運行期 buffer 是 tick＋seed；屬近似重播，
  方向性結論可信，邊界分數（±0.2）僅供參考。

## 3. 觀察

### 3.1 摩擦先行：方向選對也虧錢

AUDNZD 是唯一方向正確（MFE 87pt）的一對，leader trail 出 +5.21，
但 mirror 腿 −34.02，pair 淨 −28.81。單腿 $30 cap 下，2 個 spread＋滑價的
pair 摩擦可超過 $30——edge 2.7（strength 分數）根本沒清掉摩擦。
當日 log 的 `edge=2.3~2.9` 是動量 strength 分數，不是跨 broker 價差；
momentum 分支沒有 `entry_edge_points` USD 下限門（只做排名），
任何過 trend gate 的候選都會進場。

### 3.2 結構：盤整＋solo＝雙份虧損

4 對 MFE≤15pt（AUDCHF、USDCHF、EURUSD、AUDUSD 僅 1~15pt），
AUDJPY 是標準 whipsaw（先 ＋61pt 再 −45pt）。7 對中有 4 對雙腿全虧
（AUDCHF、USDCHF、AUDJPY、EURUSD）；solo 續跑的三次每次都以第二腿停損收場。
舊 mirror 制一停≈另一停利（net≈−成本），新制盤整盤付雙 cap＋摩擦。

### 3.3 緊停損是放大器，不是主因

SL 點距：AUDNZD ~90pt、AUDCHF ~42pt、USDCHF ~61pt、AUDJPY ~79pt、
AUDUSD ~53pt、EURUSD ~87pt、NZDCHF 僅 ~35pt（0.69 手）。
NZDCHF（MAE 37pt）與 AUDNZD follower 腿、AUDUSD leader 腿三筆實際虧損
−33~-34 超出 $30 指派上限：點距小於滑價／缺口時等於市價保證成交。
但 AUDJPY 79pt、EURUSD 87pt 不算緊照樣虧——緊停損加速失血，
不解釋方向錯誤本身。

### 3.4 門檻重播：加嚴擋掉 3/7，但擋不住假突破

候選新參（k=2.5、min_mom=8pt、T=180s、coverage=0.9）重播結果：

- 擋掉：USDCHF（2.36→1.87 未達標）、AUDJPY（2.74→1.08）、NZDCHF（2.43→1.75），
  三對合計 −61.29 可避開（含 NZDCHF follower −33.69 超標腿）。
- 擋不掉：AUDNZD（→3.80，更強，保留正確）、AUDCHF（→2.74）、
  AUDUSD（→2.57）、EURUSD（→3.03）。
  AUDUSD／EURUSD 進場當下動量是真的強（MFE 事後才塌為 1~3pt），
  任何事前閾值都分不出來——這是盤整反轉，不是門檻鬆。

反事實合計：−127.88＋61.29＝−66.59，仍為負。門檻加嚴減頻有效，
但不解決「進場後立即反轉」。

## 4. 結論（provisional，n=7）

- 主因排序：盤整＋solo 雙虧結構 ＞ 進場過頻（2.5h 打 7 發，含 CHF 三連敗：
  AUDCHF→USDCHF→NZDCHF follower −10/−8/−33）＞ 緊停損＋大 lots（0.56~0.69）
  致滑價超標。
- `maximum_margin_fraction=0.08` 是 spec 預設 0.01 的 8 倍：
  margin 預算 5000×0.08＝$400 ÷ 每手保證金 ~$712 ⇒ 0.56 手。
  降到 0.04 即 lots 減半（~0.28 手），同 $30 cap 下點距加倍，
  直接緩解 3.3 的超標問題。注意 pair lots 取兩側較低值，
  follower 側（policy 內同為 0.08）須同步調，否則只降 leader 側無效；
  且新政策需雙方空倉接受＋worker 重啟。

## 5. 限制

- n=7，單日樣本，結論皆為 provisional；需累積至 30 對再定案。
- follower 側 sqlite 不可得，F 腿 NY 日虧 −83.89 由
  `remaining $16.11 / 日限 $100` 反推，與 log 口徑 −83.19 差 0.7。
- swap、佣金未拆分；超 cap 損失的滑價／缺口組成需 deal history 細拆。
- 門檻重播為 1s tape 近似，非運行期 tick 精確重現。

## 6. 候選優化（已驗證其一，排序如下）

1. **`maximum_margin_fraction` 0.08→0.04**（leader＋follower 同步，見 §4；
   本次已改 leader 側，follower 側待遠端改＋雙方重啟＋空倉接受新政策）。
2. **動量門檻加嚴**（leader shared 政策即可）：`trend_momentum_k` 2.0→2.5、
   `trend_min_mom_points` 5→8、`trend_momentum_T_seconds` 120→180、
   `trend_min_coverage` 0.8→0.9。重播證據：擋 3/7、避 −61.29，
   但 AUDUSD／EURUSD 假突破仍會進（§3.4），需配第 3 項。
3. **虧損冷卻／相關性斷路器**（需改碼，無現成參數）：上一對虧損後同 symbol
   暫停 N 分鐘；同貨幣（如 CHF）連虧 2 次暫停全 CHF 商品。
   針對 3.4 擋不掉的「進場後反轉」型虧損。
4. **引用報價衛生收緊**（leader shared）：`trend_max_spread_points` 20→8~10、
   `trend_quote_max_age_seconds` 180→60（回預設值）。注意 AUDNZD 進場 spread
   12pt 會被 8pt 擋掉——spread 不是乾淨的 chop filter，建議先觀察。
5. **Solo 在 chop 的價值重估**：AUDJPY 5 次 trail 仍救不回；
   候選：peer 清空後限 N 分鐘（如 15 分）無 trail 推進即收工（見前篇 §5-1）。
6. **momentum 分支加 USD 期望下限**（需改碼）：目前 `entry_edge_points`
   在 momentum 模式只排名、不擋（§3.1），加 `expected_move_usd` 下限才能
   讓 edge 清掉摩擦。
