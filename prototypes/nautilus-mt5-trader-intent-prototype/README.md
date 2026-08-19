# NautilusTrader / MT5 / Trader intent prototype

**Throwaway experiment.** It answers whether MT5 rates exported by `mt5` can
flow through a NautilusTrader backtest and form a shared-trade-intent request.

```powershell
uv run --with nautilus_trader --with pandas python prototypes\nautilus-mt5-trader-intent-prototype\run.py
```

To use MT5 data, first export rates and pass the JSON file to `--mt5-rates`:

```powershell
uv run mt5 --output json rates-range EURUSD M1 --from 2026-08-14T00:00:00Z --to 2026-08-14T01:00:00Z > rates.json
uv run --with nautilus_trader --with pandas python prototypes\nautilus-mt5-trader-intent-prototype\run.py --mt5-rates rates.json
```

The prototype builds the future Trader request but does not place broker
orders. The current console is intentionally read-only and has no Trader
identity or `/api/trader/intents` endpoint. Supplying `--console-url` makes
that missing boundary observable.
