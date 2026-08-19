# MT5 instrument selection for NautilusTrader backtests

**Purpose:** Choose a first backtest instrument from observable broker data,
not predicted profitability. This is the 2026-08-16 screen for the active
`fundednext-demo` MT5 context.

## Selection contract

1. Prefer a currently tradeable, non-expiring broker-native symbol.
2. Verify bar continuity, duplicate timestamps, and non-zero tick volume before
   using its history.
3. Use MT5 rate-bar `spread` only as diagnostic evidence. For a realistic
   execution simulation, ingest MT5 tick `bid` and `ask` data as Nautilus
   `QuoteTick` data.
4. Map MT5 `digits`, `point`, contract size, and volume increment exactly into
   the Nautilus instrument definition. MT5 bar times are seconds; Nautilus
   timestamps are nanoseconds.

Primary sources:

- MetaTrader 5 `copy_rates_*` fields and UTC input:
  <https://www.mql5.com/en/docs/python_metatrader5/mt5copyratesfrom_py>
- MetaTrader 5 tick `bid`, `ask`, and `time_msc` fields:
  <https://www.mql5.com/en/docs/python_metatrader5/mt5copyticksrange_py>
- MetaTrader 5 `symbol_info` metadata:
  <https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py>
- NautilusTrader bars:
  <https://nautilustrader.io/docs/latest/concepts/data/bar/>
- NautilusTrader quote ticks:
  <https://nautilustrader.io/docs/latest/concepts/data/quote_tick/>

## Observed result

`EURUSD` is the initial backtest instrument:

| Evidence | Result |
| --- | --- |
| Trading properties | Visible, fully tradeable, 5 digits, 0.01 lot minimum/step, 100,000 contract size, USD profit currency |
| Current quote spread | 12 points = 1.2 pips |
| M1 comparison | 20,000 recent bars; no duplicate timestamps or zero tick-volume bars; lower observed spread than GBPUSD (4.4 pips) and USDJPY (8.4 pips) at the screen time |
| H1 validation range | 16,289 bars from 2024-01-01 23:00 UTC through 2026-08-14 23:00 UTC |
| H1 continuity | No gaps over 72 hours; maximum gap 50 hours, consistent with a weekend |
| H1 activity | 0% zero tick-volume bars; median tick volume 3,005 |

The broker terminal exposes only M1 data from 2026-05-08 onward. Therefore,
the initial reproducible backtest is **EURUSD H1 (2024-01-01 through
2026-08-14)**. M1 is suitable only for a short-window experiment until a
longer tick or M1 data source is imported.

Rate-bar `spread` is not a cost model: EURUSD has many zero-spread historical
bars despite a non-zero live quote. Use tick bid/ask data before interpreting
any simulated performance.

## Historical tick verification

`mt5 ticks-range EURUSD --from 2026-08-13T00:00:00Z --to
2026-08-14T00:00:00Z --flags all` returned 102,742 ticks. Every tick had a
positive `bid` and `ask`; the exported fields include `time_msc`, `bid`, `ask`,
and the calibrated `time_msc_utc`.

The raw MT5 `time` and `time_msc` fields use the broker market-data clock.
Backtests consuming `mt5` JSON must use `time_utc`/`time_msc_utc`, or replicate
the recorded `time_metadata.offset_seconds_used` calibration before converting
to Nautilus nanoseconds. The one-day export is approximately 30 MiB, so
multi-year tick backtests need chunked export and storage planning.

## Upper-liquidity short experiment result

The first pre-registered EURUSD experiment is implemented in
`prototypes/liquidity-short-experiment/`. It uses H1 zones and M15 execution,
with the agreed 10-pip target, sub-5-pip zone-width stop, 24-H1 order/position
timeouts, and conservative M15 intrabar handling. It intentionally excludes
execution costs.

The 2024–2025 training window produced 274 eligible non-overlapping trades:
46 TP-first (16.79%) and 228 SL-first. The 2026 validation window produced 65:
10 TP-first (15.38%) and 55 SL-first. None of the pre-registered SL-distance,
zone-freshness, or ATR-regime groups met the sample, >50%, Wilson, and
five-percentage-point-over-control gates in both periods.

**Verdict:** this experiment has no usable signal under the agreed rules. Do
not promote it to a trading strategy or relax the gates on this data. Any
follow-up must be a newly pre-registered hypothesis.

The MT5 exports are processed on their shared raw market-data `time` clock.
`mt5` derives `time_utc` using calibration observed at query time; independently
exported batches can therefore have differing display offsets and must not be
joined by that derived field.

## Upper-liquidity breakout long control result

`prototypes/liquidity-short-experiment/run_upper_breakout_long.py` is a
separate control experiment. It uses the same H1 zone construction but enters
a buy-stop at the zone lower boundary, cancels unfilled orders at
`entry - 10 pips`, and uses a 10-pip SL with a 15-pip TP.

The 2024–2025 training window had 272 eligible non-overlapping trades: 106
TP-first (38.97%), 166 SL-first, and no timeouts. The 2026 validation window
had 67: 26 TP-first (38.81%), 41 SL-first, and no timeouts.

At the stated 15-pip TP and 10-pip SL, the no-cost breakeven TP-first rate is
40%. Both observed control rates are below it, so this long control has no
demonstrated gross edge. It is not comparable to the prior short result beyond
sharing the same data and conservative intrabar conventions.

## Upper-liquidity breakout long parameter experiments

Two independent parameter-only variants retained the 10-pip unfilled-order
cancellation, all zone rules, and all M15 conservative sequencing.

| Variant | Training TP-first | Training SL / timeout | Validation TP-first | Validation SL / timeout | Classification breakeven |
| --- | ---: | ---: | ---: | ---: | ---: |
| TP 20 / SL 10 pips | 88/277 (31.77%) | 188 / 1 | 22/67 (32.84%) | 44 / 1 | 33.33% |
| TP 15 / SL 5 pips | 64/272 (23.53%) | 208 / 0 | 12/67 (17.91%) | 55 / 0 | 25.00% |

Neither parameter change reached its no-cost classification breakeven rate in
both periods. Timeout exits have been classified as non-successes in this
table; their actual close prices remain in the prototype ledger and must be
included before calculating any total-return metric.

## City EURUSDC 2026-to-date NautilusTrader tick replay

`prototypes/liquidity-short-experiment/run_nautilus_upper_breakout_long.py`
streams CityTradersImperium's broker-native EURUSDC bid/ask ticks through the
NautilusTrader `BacktestEngine`. The replay processed 13,596,751 ticks from 33
validated Parquet chunks. Chunks have no overlapping boundaries and no gap over
72 hours; the largest boundary gap is 49.03 hours, consistent with a weekend.

The simulated trade size is City’s 0.01-lot minimum (1,000 EUR). It produced
80 independently tracked HEDGING positions: 27 TP, 53 SL, no timeouts, a
33.75% win rate, and **-$16.13 USD** native realized PnL. The native engine
closed-position count and winner/loser count reconcile exactly with the
prototype trade ledger.

### Cost coverage

The replay uses recorded bid/ask ticks, so each execution includes the observed
spread and tick-level price movement. City symbol metadata reports 100,000
contract size, 0.01 lot minimum/step, and current swap values of -10.11 long
and -1.46 short. The account had no EURUSDC deal history; its only observed
non-deposit trades were SOLUSDC with zero `commission` and `fee`. Per the
explicit temporary choice, Nautilus maker/taker fees were set to zero.

**Limit:** City rollover financing is not modelled, and EURUSDC-specific
commission remains unverified. This is therefore a full tick-execution replay
with a zero-commission cost assumption, **not** a complete broker-economic
backtest. The machine-readable report is
`prototypes/liquidity-short-experiment/results-full-reconciled/report.json`.

## London-range-breakout exploratory screen

This is a new mechanism, independent of the prior liquidity-zone rules. The
screen builds an Asian range from the 00:00–06:00 UTC ask high and bid low. It
then allows one entry on the first 07:00–12:00 UTC breakout: buy at the ask
above the range high or sell at the bid below the range low. Stops, targets,
and a 21:00 UTC time exit are evaluated tick-by-tick on the executable bid/ask
side.

The twelve predeclared candidates combine direction (long/short), stop
distance (5/10 pips), and target distance (5/10/15 pips). Candidate selection
used only 2026-01 through 2026-05. The top positive training candidate,
**short, 10-pip stop, 15-pip target**, was also positive in the untouched
2026-06 through available August data:

| Period | Trades | Target / stop / time | Win rate | Net pips | Mean pips/trade |
| --- | ---: | ---: | ---: | ---: | ---: |
| Selection period | 64 | 28 / 36 / 0 | 43.75% | +61.4 | +0.959 |
| Holdout | 27 | 13 / 13 / 1 | 51.85% | +66.4 | +2.459 |

The screen includes the recorded spread and observed tick overshoot; it
excludes commission and financing. The holdout has only 27 trades, and the
strategy was selected from 12 candidates, so this is promising evidence—not
a demonstrated trading edge. The complete candidate and trade ledger is
`results/london-range-screen.json`; rerun it with
`prototypes/liquidity-short-experiment/screen_london_range_breakout.py`.
