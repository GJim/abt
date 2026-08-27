# Realtime cross-broker arbitrage

`realtime_arbitrage.py` consumes live market-data events from two approved,
connected account Workers. Each Worker must own a different MT5 terminal; do
not use two named contexts that share one terminal.

At startup the strategy asks the controller for `active_workers`, requires
both specified Workers to be connected and safe, then reads each selected
Worker's complete `symbols` catalog once. It fails startup if either catalog is
invalid or unavailable, and keeps the exact-name intersection of tradable
(trade mode 4, positive finite point) symbols. Live decisions use only this
cached shared catalog and fresh exact-name Market Watch quotes; they never make
per-event broker symbol-specification calls.

Immediately after that intersection, the strategy configures each selected
Worker's live quote set once with every shared symbol name in lexical order
(up to 64 symbols). Each Worker makes those symbols visible in MT5 Market Watch
and watches exactly that set for live quotes; it does not remove any existing
Market Watch symbols. Startup fails if either Worker cannot acknowledge the
exact configured names. This configuration also runs in dry-run mode: its only
side effect is Market Watch visibility, never a position or order change.

For every eligible shared symbol it considers both bid-to-opposite-ask
directions and selects the greatest edge meeting `--entry-edge-points`.
The threshold is a raw price difference equal to its value times the larger
of the two broker points. Ties select lexical symbol then direction. The
remaining defaults are a 4-point entry edge, 0.1 lot per leg, at
most 100 completed trades, 3% daily account loss, and 2% per-trade account
loss. Daily loss resets at midnight in `America/New_York`.

It is dry-run unless `--execute` is passed. In execution mode, Ctrl+C, a risk
limit, and either unconfirmed entry leg flatten **every position** on both
selected accounts, as requested. Start only after confirming both accounts
contain no unrelated positions.

After each confirmed market fill, the strategy uses the Worker broker
`calc_profit` read to derive an emergency SL and TP, each for
`--emergency-stop-loss-usd` (default: US$40) in absolute projected P/L, and
applies them together with `modify_sl_tp`. The matching dollar amount is used
rather than the other broker's price because the brokers can quote different
prices, spreads, precision, and contract terms.

A broker TP remains emergency protection, not the normal pair exit: the
strategy normally closes both legs together on its reverse signal. If either
broker protection closes a leg, the next market-data event immediately verifies
both expected positions; its missing ticket triggers an immediate market close
of the remaining leg and stops the strategy.

Every `--integrity-check-seconds` (default: five), it reads both accounts'
orders and positions while idle. While a pair is active, this check runs on
every market-data event. At startup both accounts must be empty; while a pair is active,
each account must contain exactly its expected ticket, symbol, direction, and
volume, with no pending orders. Any mismatch is treated as an external account
operation, logged, and followed by the configured full-account shutdown.

When the controller reports selected Worker market data unavailable, the
strategy pauses all broker reads and trading decisions for
`--worker-disconnect-grace-seconds` (default: 300). It resumes only after each
affected Worker reconnects and publishes market data again. If the deadline
expires, it follows the normal safety shutdown path.

Each pair must remain open for `--minimum-hold-seconds` (default: 180 seconds)
before a normal reverse-signal exit can close it. This minimum never delays
broker SL/TP, risk-limit, external-operation, or Ctrl+C shutdown handling.

At `--flatten-at-ny` (default: `16:00`), the strategy uses
`America/New_York` to unconditionally close both selected accounts and stop.
This daily cutoff also overrides the minimum hold time, preventing cross-day
positions while preserving the same wall-clock time through DST changes.

```powershell
uv run python strategy\realtime_arbitrage.py `
  --entry-edge-points 4 `
  --execute
```

Without Worker flags, the script lists connected, safe active Workers through
the controller and prompts for two distinct selections. `trader.json` defaults
to the file beside the script: `strategy\trader.json`. To automate selection,
pass `--first-worker` and `--second-worker` together.

Pass `--execute` only after the dry-run output and worker connectivity have
been verified. Before opening the selected candidate, and at most once per
hour per symbol, endpoint, and direction, it asks each Worker to calculate
the broker's live margin for one lot. If the requested lots would consume over
50% of either account's equity, it warns, flattens both accounts, and exits.
While a pair is open it evaluates only that pair's symbol for reversal and
exit; it cannot open another trade. After closing, the closed symbol must
clear its qualifying signal before re-entry on that symbol, but a different
valid shared-symbol candidate may be selected.
