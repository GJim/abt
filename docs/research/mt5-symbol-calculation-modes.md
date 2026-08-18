# MT5 symbol calculation modes

## Official sources

- [MQL5 `ENUM_SYMBOL_CALC_MODE`](https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants#enum_symbol_calc_mode)
- [MQL5 Python `symbol_info()`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py)

## `SYMBOL_CALC_MODE_FOREX`

MQL5 defines this as "Forex mode - calculation of profit and margin for Forex".
Its formulas are:

| Component | Formula |
| --- | --- |
| Margin | `Lots * Contract_Size / Leverage * Margin_Rate` |
| Profit | `(close_price - open_price) * Contract_Size * Lots` |

The official Python `symbol_info()` example for EURJPY reports
`trade_calc_mode=0`. The Python binding returns `SymbolInfo` as a named tuple,
whose `trade_calc_mode` field is an integer, rather than a named Python enum.

## `SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE`

MQL5 defines this as Forex calculation "without taking into account the
leverage". Its profit formula is the same as `FOREX`, but its margin formula
is:

`Lots * Contract_Size * Margin_Rate`

It therefore must not be silently classified as ordinary leveraged Forex.

## Implication for product-pair analysis

The worker must preserve the native MT5 `trade_calc_mode` value. The
control-plane compares the two raw values for equality and does not translate
them into an application `FOREX` category. This keeps
`FOREX_NO_LEVERAGE` distinct from ordinary Forex while allowing the
broker-returned mode to remain auditable.
