# Realtime cross-broker arbitrage

`realtime_arbitrage.py` consumes live market-data events from two approved,
connected account Workers. Each Worker must own a different MT5 terminal; do
not use two named contexts that share one terminal.

At startup the strategy asks the controller for `active_workers`, requires
both specified Workers to be connected and safe, then reads each selected
Worker's complete `symbols` catalog once. It fails startup if either catalog is
invalid or unavailable, and keeps only exact-name symbols that are tradable
(trade mode 4), bidirectional, and have matching hard contract terms: trade
calculation mode, digits, point, tick size, contract size, minimum volume,
volume step, and allowed directions. The endpoints must also share an
executable filling mode; the strategy uses FOK when both support it, otherwise
IOC. Live decisions use only this cached shared catalog and fresh exact-name
Market Watch quotes; they never make per-event broker symbol-specification
calls.

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
remaining defaults are a 4-point entry edge, at most 100 completed trades, 3%
daily account loss, and 2% per-trade account loss. Daily loss resets at
midnight in `America/New_York`.

The strategy sizes every entry dynamically. `--max-exposure-usd` is the
per-leg capital allocation; when omitted, each endpoint uses its startup
equity. `--max-margin-ratio` defaults to `0.1` and cannot exceed `0.5`, so
each leg's broker-native margin budget is 10% of that endpoint's allocation.
It reserves a fixed internal 20% headroom from that budget: a cached plan and
its controller hard limit therefore use at most 80% of the configured amount
(8% of allocation at the default). This deliberate buffer protects against
quote, margin, and free-margin movement during the cache lifetime.

Once fresh quotes exist for every shared symbol, the strategy requests one
bounded `calc_margin_batch` read from each Worker for one lot in each
direction, then derives all common volumes locally. It refreshes this complete
snapshot roughly every ten minutes; it makes two Worker RPCs rather than
per-symbol/per-signal round trips. No entry is allowed before the initial
snapshot, and a missing or ten-minute-stale plan skips its signal. The broker
does the final safety check at entry rather than treating the cached estimate
as exact.
The 2% per-trade and 3% daily loss limits also use the per-leg allocation.
When prior closed trades have consumed part of an endpoint's daily loss limit,
the remainder further caps the broker SL/TP target; the strategy refuses new
entries once either endpoint has exhausted its daily loss budget.

It is dry-run unless `--execute` is passed. In execution mode, an entry is one
controller-owned `hedged_entry` command, not two strategy market operations.
Each signal has one five-second expiry shared by both legs: the controller takes
a deterministic pair-level Worker lock, runs both broker `order_check` calls
concurrently, and dispatches only if both checks and the remaining shared
deadline succeed. Checks do not reserve liquidity. Worker scheduling may reject
an expired request before broker submission, which rejects the pair without
sending either order; it must never present a post-send lost response as a safe
rejection. The controller persists the command, per-leg broker receipts, and
timings before reporting its outcome. FOK is selected whenever both Workers
support it; a failed FOK is rejected and never silently retried as IOC.
If either send fails or is unknown after a counterpart may have been sent, the
controller transitions both accounts to `CONVERGING_EMPTY`: it observes the
broker state, cancels observed orders, observes again, closes observed
positions, and requires a final empty observation. The strategy stops without
attempting an unsafe individual-order fallback. After both SL/TP writes, it
registers the expected protected legs; only a fresh full snapshot from each
Worker with matching ticket, symbol, side, volume, SL, TP, and no pending
orders marks the pair `ACTIVE_VERIFIED`. Any cross-broker mismatch converges
both accounts to empty. Ctrl+C
and risk or integrity failures still flatten every position on both selected
accounts. A disconnected Trader can recover the durable command outcome by its
original command ID through the replayed `hedged_entry_completed` event; until
then it treats the entry as potentially dispatched and does not race recovery
with direct closes. Start only after confirming both accounts contain no
unrelated positions.

After each confirmed market fill, the strategy uses the Worker broker
`calc_profit` read to derive an emergency SL and TP, each for
the lower of `--emergency-stop-loss-usd` (default: US$40) and the endpoint's
allocation-based 2% per-trade and remaining 3% daily loss limits in absolute
projected P/L, and applies them together with `modify_sl_tp`. The matching
dollar amount is used rather than the other broker's price because the brokers
can quote different prices, spreads, precision, and contract terms.

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
Worker read RPCs that fail because the Worker session is unavailable use this
same grace period. A Worker operation RPC whose result is unavailable pauses
for `--worker-write-disconnect-grace-seconds` (default: 30); it is never
retried, and expiry follows the normal safety shutdown path. Broker-declared
operation rejections remain immediate failures.

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
  --max-exposure-usd 5000 `
  --max-margin-ratio 0.1 `
  --execute
```

Without Worker flags, the script lists connected, safe active Workers through
the controller and prompts for two distinct selections. `trader.json` defaults
to the file beside the script: `strategy\trader.json`. To automate selection,
pass `--first-worker` and `--second-worker` together.

Pass `--execute` only after the dry-run output and worker connectivity have
been verified. The ten-minute sizing cache is intentionally approximate and
conservative; final parallel broker preflight remains authoritative. If no
cached common minimum volume fits, or either final preflight rejects, the
strategy skips that entry signal without sending an order.
While a pair is open it evaluates only that pair's symbol for reversal and
exit; it cannot open another trade. After closing, the closed symbol must
clear its qualifying signal before re-entry on that symbol, but a different
valid shared-symbol candidate may be selected.
