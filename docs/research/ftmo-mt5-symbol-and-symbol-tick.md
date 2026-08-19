# FTMO Demo: `symbol` and `symbol_info_tick`

**Scope:** MetaTrader 5 data returned by the configured `ftmo-demo` context
on 2026-08-17. `symbol` and `symbol_info_tick` are MetaQuotes MT5 concepts;
they are not an FTMO-specific schema.

## Observed through the CLI

```powershell
uv run mt5 --context ftmo-demo --output json symbols
uv run mt5 --context ftmo-demo --output json symbol EURUSD
uv run mt5 --context ftmo-demo --output json tick EURUSD
```

The selected FTMO Demo Market Watch included broker-native names such as
`EURUSD`, `GBPUSD`, `USDCHF`, and `USDJPY`. At the observation time,
`EURUSD` was selected and visible, had `digits: 5`, `point: 0.00001`,
`volume_min: 0.01`, `volume_step: 0.01`, `volume_max: 50.0`, and
`currency_base: EUR` / `currency_profit: USD`.

The latest `EURUSD` tick returned `bid: 1.15853`, `ask: 1.15854`,
`last: 0.0`, `volume: 0`, `time: 1786953484`, and
`time_msc: 1786953484058`. These prices are inherently transient and should
not be treated as a permanent quote or spread commitment.

## `symbol`

In MT5, a symbol is the **exact broker-defined instrument identifier string**.
Pass that string to `symbol_info(symbol)` to obtain one broad snapshot:

- identity and availability: `name`, `path`, `select`, `visible`
- price precision: `digits`, `point`
- trading constraints: `volume_min`, `volume_step`, `volume_max`,
  `trade_tick_size`, `trade_tick_value`, filling and order modes
- currencies and contract: `currency_base`, `currency_profit`,
  `currency_margin`, `trade_contract_size`
- current summary values: `bid`, `ask`, `last`, `spread`, `time`

The values are symbol-specific. For example, one FTMO Demo `EURUSD` point was
`0.00001`; a 5-digit FX point is one tenth of a conventional 4-digit pip.
Use `point` and `trade_tick_size` from the returned metadata rather than
hard-coding a pip or tick conversion.

The terminal, not a generic ticker convention, is authoritative. MT5
brokers can use suffixes or alternate names; MetaQuotes' own `symbols_get`
examples include names such as `EURUSD_T20` and `EURUSD4`. FTMO instructs
users to inspect instrument specifications in the platform's Market Watch.

## `symbol_info_tick`

`symbol_info_tick(symbol)` returns the **most recent price update** as an
`MqlTick`, rather than the whole instrument specification:

| Field | Meaning |
| --- | --- |
| `time` | Time of the most recent price update, in seconds. |
| `time_msc` | The same update time with millisecond precision. |
| `bid` / `ask` | Current bid and ask; for FX, these are normally the executable-side prices to use. |
| `last` | Last transaction price, when supplied by the venue. |
| `volume` / `volume_real` | Transaction volume, when supplied by the venue. |
| `flags` | Bit flags identifying what changed: bid, ask, last, volume, buy, or sell. |

`symbol_info` and `symbol_info_tick` address the same symbol but serve
different purposes: use `symbol_info` for constraints and interpretation;
use `symbol_info_tick` for the freshest quote. `symbol_info` also has price
fields, but it lacks the tick-specific `time_msc`, `flags`, and
`volume_real`.

For OTC FX, `last`, `volume`, and `volume_real` may legitimately be zero.
The observed FTMO Demo EURUSD tick did so; this does not make its bid/ask
invalid.

## Market Watch and time handling

Query the exact name returned by `symbols`. A symbol normally needs to be
selected in Market Watch before it can receive quotes. In this repository,
`symbol-select SYMBOL` performs that state-changing action; use it only when
the symbol is not already selected/visible.

```powershell
uv run mt5 --context ftmo-demo --output json symbols
uv run mt5 --context ftmo-demo --output json symbol "<exact-name>"
uv run mt5 --context ftmo-demo --output json tick "<exact-name>"
```

The CLI preserves the raw MT5 epoch and adds derived `time_utc` fields.
For the observed FTMO tick, the raw market-data clock required the CLI's
measured 10,796-second calibration. Consumers should retain raw fields and
use the emitted `time_metadata` when comparing results across calls.

## Primary sources

- MetaQuotes Python [`symbol_info`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfo_py)
- MetaQuotes Python [`symbol_info_tick`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolinfotick_py)
- MetaQuotes [`MqlTick` structure](https://www.mql5.com/en/docs/constants/structures/mqltick)
- MetaQuotes Python [`symbol_select`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolselect_py)
- MetaQuotes Python [`symbols_get`](https://www.mql5.com/en/docs/python_metatrader5/mt5symbolsget_py)
- FTMO [account specifications FAQ](https://ftmo.com/en/faq/what-are-the-account-specifications/)
- Repository CLI behavior: `README.md`, `abt\mt5\cli.py`
