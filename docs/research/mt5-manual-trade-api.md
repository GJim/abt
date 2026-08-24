# MetaTrader 5 manual-trade API contract

- Market entry and position close use `TRADE_ACTION_DEAL`; the request includes
  `symbol`, `volume`, `type`, and executable `price`. Closing additionally
  identifies the open position with `position`.
  [MetaQuotes order_send](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py)
- `positions_get(ticket=...)` is the authoritative MT5 query for whether a
  tracked position remains open.
  [MetaQuotes positions_get](https://www.mql5.com/en/docs/python_metatrader5/mt5positionsget_py)
- The worker maps BUY requests to tick ask and SELL requests to tick bid. The
  controller tracks normalized worker order and position tickets rather than
  passing raw MT5 request structures across the WebSocket.
- A successful market-entry acknowledgement is not necessarily a fill record.
  Some MT5 broker integrations return `TRADE_RETCODE_DONE` with a nonzero
  order ticket while `deal`, `price`, `bid`, and `ask` are zero and no position
  ticket is present in the immediate `order_send` result. The controller treats
  that as an accepted order submission, then reconciles by its execution
  comment. It requires exactly one open position and a positive `price_open`
  before applying protection; missing or ambiguous reconciliation freezes the
  participating workers rather than sending the next leg.
