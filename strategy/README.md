# Realtime cross-broker arbitrage

`realtime_arbitrage.py` consumes live market-data events from two approved,
connected account Workers. Each Worker must own a different MT5 terminal; do
not use two named contexts that share one terminal.

At startup the strategy asks the controller for `active_workers`, requires
both specified Workers to be connected, then reads each selected
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

It is dry-run unless `--execute` is passed. Execution mode opens a local
SQLite/WAL Strategy Runtime before evaluating live signals. That runtime owns
the pair plan, command sequence, deterministic Worker effects, broker receipts,
actual tickets and fills, lifecycle transitions, desired state, and per-Worker
fact cursors. The controller authenticates and routes versioned envelopes but
does not interpret orders, positions, pairs, or lifecycle state.

Each signal has one five-second expiry shared by both legs. The Strategy
Runtime sends the two exact broker `order_check` requests concurrently and
dispatches only after both checks and the shared deadline succeed. Checks do
not reserve liquidity. Each market effect has a deterministic ID; the Worker
journal persists `send_started` immediately before MT5 submission and never
blindly resends an effect that crossed it. FOK is selected whenever both
Workers support it; a failed FOK is rejected and never silently retried as IOC.

Receipts alone never make a pair active. Fresh Worker snapshots must prove the
ticket, symbol, side, volume, SL, TP, and absence of pending orders on both
legs. A rejection, lost receipt, asymmetric result, or verification mismatch
moves the runtime toward `EMPTY`: it observes current facts, cancels only
pair-owned orders, closes only pair-owned positions, and requires a final
broker-verified empty snapshot. Any unrelated account exposure is preserved
and moves the runtime to `NEEDS_HUMAN`.

On restart or relay reconnect, the runtime resumes each Worker stream from its
last committed cursor and obtains fresh full snapshots before making lifecycle
decisions. Duplicate facts are ignored; an epoch change or cursor gap invalidates
the stream until a fresh snapshot arrives. Ctrl+C, the daily cutoff, risk
limits, and integrity failures use the same durable close convergence rather
than a controller command or direct fallback.

Before each market command, the strategy uses the Worker broker `calc_profit`
read and the current executable quote to derive requested absolute emergency
SL and TP targets, each for
the lower of `--emergency-stop-loss-usd` (default: US$40) and the endpoint's
allocation-based 2% per-trade and remaining 3% daily loss limits in absolute
projected P/L. It passes these targets in each Strategy Runtime entry effect; it never follows
a fill with a separate `modify_sl_tp` operation. The matching dollar amount is used rather than the
other broker's price because the brokers can quote different prices, spreads,
precision, and contract terms. The Strategy Runtime remains non-active until Worker facts verify both legs.

A broker TP remains emergency protection, not the normal pair exit: the
strategy normally closes both legs together on its reverse signal. Before a
timed exit is armed, a missing protected ticket triggers durable close
convergence for the remaining owned leg and stops the strategy.

Every `--integrity-check-seconds` (default: five), it reads both accounts'
orders and positions, including while the healthy market-data stream is quiet.
At startup both accounts must be empty; while a pair is active,
each account must contain exactly its expected ticket, symbol, direction, and
volume, with no pending orders. Any mismatch is treated as an external account operation and moves the runtime
toward `EMPTY` or `NEEDS_HUMAN` without mutating unrelated exposure.

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

Timed synchronized exit is disabled by default. Set
`--maximum-holding-seconds N` to snapshot a maximum holding age into each new
pair. The age starts only after fresh Worker facts verify both entry legs.
When it elapses, the Strategy Runtime reads current symbol stop/freeze
constraints and combines them with current spread and the last 60 seconds of
one-second midpoint movement. It then persists and concurrently sends a
near-market SL/TP corridor for both owned tickets. Fresh broker snapshots must
verify the exact protection before the corridor is considered armed.

If one leg exits while the timed corridor is armed, the remaining protected
leg receives a 15-second grace period to exit from the same market movement.
After that deadline only the remaining owned ticket and current volume are
market-closed. If both legs remain open for 30 seconds after arming, both are
market-closed concurrently. Missing or stale market evidence, invalid broker
constraints, and partially verified protection fall back to normal
owned-ticket close convergence. All deadlines and protection targets survive
Strategy Runtime restart. A reverse signal after the minimum hold, risk limit,
daily cutoff, or shutdown bypasses the timed waiting periods and immediately
converges the owned pair toward empty.

At `--flatten-at-ny` (default: `16:00`), the strategy uses
`America/New_York` to unconditionally close both selected accounts and stop.
This daily cutoff also overrides the minimum hold time, preventing cross-day
positions while preserving the same wall-clock time through DST changes.

```powershell
uv run python strategy\realtime_arbitrage.py `
  --entry-edge-points 4 `
  --max-exposure-usd 5000 `
  --max-margin-ratio 0.1 `
  --maximum-holding-seconds 900 `
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
