# Realtime cross-broker arbitrage

`realtime_arbitrage.py` consumes live market-data events from two approved,
connected account Workers. Each Worker must own a different MT5 terminal; do
not use two named contexts that share one terminal.

The strategy accepts one symbol (default NZDUSD) for both endpoints. At startup
it asks the controller for `active_workers`, requires both specified Workers to
be connected and safe, then queries `symbol_info` on each Worker. It exits
before any broker operation if either Worker cannot trade the selected symbol.

The remaining defaults are a 0.4-pip entry edge, 0.1 lot per leg, at
most 100 completed trades, 3% daily account loss, and 2% per-trade account
loss. Daily loss resets at midnight in `America/New_York`.

It is dry-run unless `--execute` is passed. In execution mode, Ctrl+C, a risk
limit, and either unconfirmed entry leg flatten **every position** on both
selected accounts, as requested. Start only after confirming both accounts
contain no unrelated positions.

```powershell
uv run python strategy\realtime_arbitrage.py `
  --symbol NZDUSD `
  --execute
```

Without Worker flags, the script lists connected, safe active Workers through
the controller and prompts for two distinct selections. `trader.json` defaults
to the file beside the script: `strategy\trader.json`. To automate selection,
pass `--first-worker` and `--second-worker` together.

Pass `--execute` only after the dry-run output, worker connectivity, symbol
availability have been verified. Every hour it asks each Worker to calculate
the broker's live margin for one lot in both directions. If the requested
lots would consume over 50% of either account's equity, it warns, flattens
both accounts, and exits.
