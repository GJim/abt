# MT5 時間語意：官方契約與觀測

## 官方契約

- MQL5 的 `datetime` 是自 1970-01-01 起的秒數；`*_msc` 欄位則是毫秒數。型別本身不保證每一種交易紀錄欄位採用 UTC 或 broker server 時區。
  - [datetime](https://www.mql5.com/en/docs/basis/types/integer/datetime)
  - [Order properties](https://www.mql5.com/en/docs/constants/tradingconstants/orderproperties)
  - [Deal properties](https://www.mql5.com/en/docs/constants/tradingconstants/dealproperties)
  - [Position properties](https://www.mql5.com/en/docs/constants/tradingconstants/positionproperties)

- Python `copy_rates_*` 和 `copy_ticks_*` 的官方文件明確要求以 UTC-aware `datetime` 指定輸入時間，且稱 bar 開盤與 tick 時間以 UTC 儲存、回傳。
  - [copy_rates_from](https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesfrom_py)
  - [copy_rates_range](https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesrange_py)
  - [copy_ticks_from](https://www.mql5.com/en/docs/python_metatrader5/mt5copyticksfrom_py)
  - [copy_ticks_range](https://www.mql5.com/en/docs/python_metatrader5/mt5copyticksrange_py)

- `symbol_info_tick()` 回傳 `MqlTick`；`time` 是最新價格更新時間、`time_msc` 是其毫秒時間。`TimeCurrent()` 則是 trade server 形成的最近報價時間，並非主機時鐘。不過官方沒有明示 Python `symbol_info_tick().time` 的 Unix epoch 一定以 broker 本地牆鐘編碼。
  - [MqlTick](https://www.mql5.com/en/docs/constants/structures/mqltick)
  - [TimeCurrent](https://www.mql5.com/en/docs/dateandtime/timecurrent)

- Python 的 orders、history orders/deals 與 positions 文件只說回傳 namedtuple；沒有為其時間欄位提供與 `copy_*` 同等明確的 UTC 契約。
  - [orders_get](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersget_py)
  - [history_orders_get](https://www.mql5.com/en/docs/python_metatrader5/mt5historyordersget_py)
  - [history_deals_get](https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py)
  - [positions_get](https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py)

## `ORDER_TIME_DAY` 與到期時間

`ORDER_TIME_DAY` 的官方定義只有「Good till current trade day」；未定義精確截止鐘點、時區或交易 session 邊界。`ORDER_TIME_SPECIFIED_DAY` 才定義為指定日的 23:59:59，若非交易時段則到最近交易時間。

- [ENUM_ORDER_TYPE_TIME](https://www.mql5.com/en/docs/constants/tradingconstants/orderproperties#enum_order_type_time)
- [MqlTradeRequest](https://www.mql5.com/en/docs/constants/structures/mqltraderequest)

官方把 `ORDER_TIME_EXPIRATION` 定義為 order expiration time，但沒有規定 DAY order 的回傳欄位必為實際未來截止點，或必須滿足 `time_expiration >= time_setup`。

本機對 FTMO 的 DAY order 觀測到：

```text
state           = ORDER_STATE_PLACED
type_time       = ORDER_TIME_DAY
time_setup      = 1786735404
time_expiration = 1786665600
```

兩個 raw epochs 相差 -69,804 秒（約 -19:23:24）；以相同 offset 校正不會改變其先後。因此這不是本專案時區呈現邏輯所造成。官方文件沒有明確定義或排除這種組合；「交易日錨點」或 broker backend 特例都只是未驗證推測。

## 對 CLI 的判讀

本 CLI 將 `symbol_info_tick.time` 與主機 UTC 中點比較，為 market data 保存校正；成功寫入後另從 order/deal/position 查回時間建立 trade-record 校正。這是為因應實測差異的相容層，不是 MetaQuotes 所定義的正式 clock family。

因此：

1. `copy_rates_*` 和 `copy_ticks_*` 應以 UTC-aware bounds 呼叫，並保留 raw 回傳值供驗證。
2. tick、order、deal、position 的 raw epochs 與校正樣本必須保存；不可將單一觀測推論到所有 API。
3. DAY order 的生命週期應以 `state` 與實際查回結果判定，不應只依 `time_expiration` 判定過期。
4. 需要可稽核的絕對截止時間時，使用 `ORDER_TIME_SPECIFIED`，送出後立即查回並驗證 broker 回傳值。
