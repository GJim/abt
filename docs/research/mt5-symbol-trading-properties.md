# MT5 symbol trading properties

## Official sources

- [MQL5 symbol properties](https://www.mql5.com/en/docs/constants/environment_state/marketinfoconstants)
- [MQL5 order filling properties](https://www.mql5.com/en/docs/constants/tradingconstants/orderproperties)

## Filling mode

`SYMBOL_FILLING_MODE` is a bitmask. `SYMBOL_FILLING_FOK` is `1`,
`SYMBOL_FILLING_IOC` is `2`, and `SYMBOL_FILLING_BOC` is `4`. Therefore `3`
means FOK and IOC are both supported, while `2` means only IOC is supported.
It is not an ordinal enum.

FOK requires the requested volume to fill completely or be cancelled. IOC
allows the immediately available volume to fill and cancels the remainder.
Support also depends on the symbol execution mode: for Market Execution, FOK
and IOC are controlled by the bitmask and Return is disabled.

## Stops level

`SYMBOL_TRADE_STOPS_LEVEL` is the minimum distance, in points, between the
current close price and a Stop order. A value of `20` on a five-digit FX symbol
is 20 points, or 2 pips. A value of `0` means the broker does not publish a
fixed distance through this property; it does not guarantee that every
arbitrarily close stop will be accepted.

## Swaps

`SYMBOL_SWAP_LONG` and `SYMBOL_SWAP_SHORT` are the daily overnight swap
values for long and short positions. A positive value is a credit and a
negative value is a charge. Their unit is defined by `SYMBOL_SWAP_MODE`; it
may be points, a currency amount, or an annual interest percentage. Swap
values must not be compared semantically without also comparing swap mode.

`SYMBOL_SWAP_ROLLOVER3DAYS` is an `ENUM_DAY_OF_WEEK` value identifying the
weekday on which three-day rollover is charged. It is a day, not a multiplier:
`3` is Wednesday. Per-day multiplier properties provide the actual multiplier.

## Contract size

`SYMBOL_TRADE_CONTRACT_SIZE` is the quantity of the underlying asset in one
lot. Standard Forex commonly uses `100000` units of the base currency, but
the broker defines the value and it must be read from each symbol. It directly
affects the documented margin and profit formulas.
