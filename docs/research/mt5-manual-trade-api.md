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
