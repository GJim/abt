# MT5 Live Updates and Trade State

**Scope:** MT5 terminal Python (`MetaTrader5`) integration for Worker monitoring
and automatic account recovery. Sources are official MetaQuotes documentation.

## Result

The Python API exposes no callback for ticks, orders, positions, or connection
changes. A worker must poll the terminal locally, then push changes over its
already-established WSS channel to the control plane.

`market_book_add(symbol)` subscribes the terminal to Depth of Market changes,
but Python does not expose the terminal's MQL5 `OnBookEvent` callback. Python
must still call `market_book_get(symbol)` to retrieve the current book. It is
not a broker-push solution for this UI, and market depth is not required for
the requested latest bid/ask/time table. `symbol_select(symbol, True)` merely
makes a symbol available in Market Watch.

## Required MT5 calls

| Need | Python API | Relevant fields / semantics |
| --- | --- | --- |
| Broker and terminal state | `terminal_info()` | `connected`, `trade_allowed`, `tradeapi_disabled`, `ping_last` |
| Account snapshot | `account_info()` | Login, balance, equity, margin, and trading flags |
| Latest quote | `symbol_info_tick(symbol)` | `bid`, `ask`, `time`, `time_msc`; compare `time_msc` to identify a newer tick |
| Market Watch eligibility | `symbol_info(symbol)`, `symbol_select(symbol, True)` | A symbol must be available in Market Watch before requesting ticks |
| Open pending orders | `orders_get()` | All open orders, or filter by symbol/group/ticket |
| Open positions | `positions_get()` | All open positions, or filter by symbol/group/ticket |
| Recent fills for recovery | `history_deals_get(from, to)` | Query a sliding lookback to find fills that occurred between state polls |
| Market open/close | `order_send()` with `TRADE_ACTION_DEAL` | A synchronous request; reconcile its result with positions/history |
| Position TP/SL update | `order_send()` with `TRADE_ACTION_SLTP` | Target the position and set `sl` and `tp` |
| Pending-order cancellation | `order_send()` with `TRADE_ACTION_REMOVE` | Target the pending order |

## Worker-to-controller update model

The worker should serialize all terminal calls and poll locally:

1. Poll `symbol_info_tick()` for each watched symbol at a bounded interval.
   When `time_msc` changes, push a quote update containing the bid, ask, and
   broker timestamp.
2. Poll `orders_get()` and `positions_get()` for state changes, and send diffs
   over WSS. Query `history_deals_get()` over a sliding interval as a safety net
   for fills that open and close between snapshots.
3. Poll `terminal_info()` and send a connection-state transition immediately.
4. On worker or WSS reconnection, send a complete account, order, and position
   snapshot before resuming diffs; the control plane must reconcile the gap.

The WSS channel is push from the worker to the control plane; it is not a
broker-push feed. Every UI value must retain the broker timestamp and the
controller receipt time so an administrator can assess freshness.

## Sources

- [MetaTrader5 Python integration](https://www.mql5.com/en/docs/python_metatrader5)
- [`terminal_info`](https://www.mql5.com/en/docs/python_metatrader5/mt5terminalinfo_py)
- [`symbol_info_tick`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfotick_py)
- [`symbol_info`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py)
- [`symbol_select`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolselect_py)
- [`market_book_add`](https://www.mql5.com/en/docs/python_metatrader5/mt5marketbookadd_py)
- [`market_book_get`](https://www.mql5.com/en/docs/python_metatrader5/mt5marketbookget_py)
- [MQL5 `MarketBookAdd` and `OnBookEvent`](https://www.mql5.com/en/docs/marketinformation/marketbookadd)
- [`orders_get`](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersget_py)
- [`positions_get`](https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py)
- [`history_deals_get`](https://www.mql5.com/en/docs/python_metatrader5/mt5historydealsget_py)
- [`order_send`](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py)
- [`TRADE_REQUEST_ACTIONS`](https://www.mql5.com/en/docs/constants/tradingconstants/enum_trade_request_actions)
