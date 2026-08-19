# EURUSD upper-liquidity short experiment

**PROTOTYPE — not production trading code.** This research harness implements
the confirmed H1/M15 upper-liquidity short experiment. It deliberately ignores
spread, commissions, and slippage, so its result is a price-path signal test,
not a profitability claim.

## Data export

Export the required `mt5` JSON data in manageable batches. Keep all batches
from the same selected context and symbol, then combine their `records` arrays
without duplicate raw MT5 `time` values. The runner aligns H1 and M15 by that
shared market-data clock. It does **not** sort batches by `time_utc`: each
export's calibration is observed at query time and must not be treated as a
historical timestamp correction.

```powershell
uv run mt5 --output json rates-from-pos EURUSD H1 --start-pos 0 --count 20000 > eurusd-h1.json
uv run mt5 --output json rates-from-pos EURUSD M15 --start-pos 0 --count 20000 > eurusd-m15.json
```

## Run

```powershell
uv run python prototypes\liquidity-short-experiment\run.py `
  --h1-rates eurusd-h1.json `
  --m15-rates eurusd-m15.json `
  --output-dir results\liquidity-short
```

The report directory contains:

- `zones.json`: confirmed H1 swing-high liquidity zones.
- `orders.json`: every order and explicit fill/cancel reason.
- `trades.json`: every filled order with TP, SL, or timeout result.
- `report.json`: control and pre-registered feature-group statistics, split
  between 2024–2025 training and 2026 validation.

Only feature groups that meet the agreed training **and** validation gates are
marked `selected`: at least 30 trades, TP-first rate above 50%, Wilson 95%
lower bound above 50%, and at least five percentage points above control.

## Upper-liquidity breakout long control

The confirmed follow-up is an independent control experiment. It retains the
same H1 zone construction and M15 conservative sequencing, but places a
buy-stop at the upper-zone lower boundary. Its unfilled cancellation and filled
SL are both `entry - 10 pips`; TP is `entry + 15 pips`.

```powershell
uv run python prototypes\liquidity-short-experiment\run_upper_breakout_long.py `
  --h1-rates eurusd-h1.json `
  --m15-rates eurusd-m15.json `
  --output-dir results\liquidity-upper-breakout-long
```

It reports control only, split between the fixed 2024–2025 training and 2026
validation periods.

## Tick-executed NautilusTrader control

`run_nautilus_upper_breakout_long.py` is a separate **PROTOTYPE** runner that
keeps the agreed H1/M15 zone rules above, but executes the resulting orders in
an actual NautilusTrader `BacktestEngine` against City's EURUSDC bid/ask ticks.
The supplied Parquet files are not a Nautilus native catalog layout, so it uses
the engine's `add_data_iterator` API to convert each bounded Parquet record
batch to `QuoteTick`s. It never materializes all 13.8m ticks at once.

Run the full available 2026 catalog:

```powershell
uv run --with nautilus_trader --with pyarrow python `
  prototypes\liquidity-short-experiment\run_nautilus_upper_breakout_long.py `
  --output-dir results\liquidity-upper-breakout-long-nautilus
```

The runner defaults to the City data location supplied for this experiment.
Use explicit paths on another machine:

```powershell
uv run --with nautilus_trader --with pyarrow python `
  prototypes\liquidity-short-experiment\run_nautilus_upper_breakout_long.py `
  --h1-rates C:\path\eurusdc-h1-20k.json `
  --m15-rates C:\path\eurusdc-m15-20k.json `
  --tick-catalog C:\path\tick-catalog `
  --symbol-info C:\path\eurusdc-symbol.json `
  --output-dir results\liquidity-upper-breakout-long-nautilus
```

It writes `zones.json`, `orders.json`, `trades.json`, and `report.json`.
`report.json` records the streamed tick count and range, zero maker/taker fees,
the raw City swap values observed in the supplied symbol metadata, and every
material limitation. It explicitly excludes rollover financing: City currently
reports `swap_mode=1`, `swap_long=-10.11`, `swap_short=-1.46`, and Wednesday
triple rollover (`swap_rollover3days=3`), but those values are not applied.
It executes City's observed minimum `0.01` lot by default (1,000 EUR base
units); override it with `--volume-lots` in City `0.01`-lot increments.
The report includes `native_engine` position closures, winners/losers, and
realized USD PnL from Nautilus events. The runner fails rather than write a
report if those native closures cannot reconcile to its trade ledger.

## London-range-breakout exploratory screen

`screen_london_range_breakout.py` is a separate **PROTOTYPE**. It evaluates a
predefined grid of long and short range-breakout rules directly on City's
recorded EURUSDC bid/ask ticks. The Asian range is the 00:00–06:00 UTC ask high
and bid low; it permits one first-breakout entry during 07:00–12:00 UTC and
closes any open position at 21:00 UTC. Stops and targets are tested against
the executable-side quote, so observed spread and tick overshoot are included.

It selects no rule automatically. It reports 2026-01 through 2026-05
separately from the untouched 2026-06-onward holdout, and excludes commission
and financing.

```powershell
uv run --with pyarrow python `
  prototypes\liquidity-short-experiment\screen_london_range_breakout.py `
  --output results\london-range-screen.json
```

Small reproducible smoke slice (native entry fill observed):

```powershell
uv run --with nautilus_trader --with pyarrow python `
  prototypes\liquidity-short-experiment\run_nautilus_upper_breakout_long.py `
  --start 2026-01-06T10:00:00Z --end 2026-01-06T12:00:00Z `
  --output-dir results\liquidity-upper-breakout-long-nautilus-smoke
```

On this Windows i5-7300U host, the hedging runner completed a representative
four-week, 1,614,907-tick slice in 62.676 seconds and the full 2026 window
(13,596,751 ticks) in 460.346 seconds. The simulated venue uses `HEDGING`, so
every filled entry is an independent native position. Nautilus' order factory
cannot bind a conditional
reduce-only exit to a hedged position without a position ID, so the strategy
requests a native market close on the first tick that meets its unchanged TP,
SL, or timeout condition.

Run a parameter-only variant without changing the 10-pip cancellation rule:

```powershell
# TP 20 pips, SL 10 pips
uv run python prototypes\liquidity-short-experiment\run_upper_breakout_long.py `
  --h1-rates eurusd-h1.json --m15-rates eurusd-m15.json `
  --take-profit-pips 20 --stop-loss-pips 10 --output-dir results\long-tp20

# TP 15 pips, SL 5 pips
uv run python prototypes\liquidity-short-experiment\run_upper_breakout_long.py `
  --h1-rates eurusd-h1.json --m15-rates eurusd-m15.json `
  --take-profit-pips 15 --stop-loss-pips 5 --output-dir results\long-sl5
```
