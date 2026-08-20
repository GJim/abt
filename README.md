# mt5

`mt5` is a local command-line interface for manually inspecting and operating
MetaTrader 5 accounts. It keeps named CLI contexts separate from MT5 terminal
configuration. Context metadata is stored beside the executable; passwords are
stored in Windows Credential Manager.

## Setup

```powershell
uv sync
uv run mt5 context add ftmo-demo `
  --terminal-path "C:\Program Files\MetaTrader 5\terminal64.exe" `
  --login 123456 `
  --server "Broker-Demo" `
  --timezone "Asia/Taipei"
```

The command prompts for the password, validates the login with MT5, and saves
the password only after a successful login. It never accepts a password as a
command-line argument. `--timezone` must be an IANA timezone name. Existing
contexts without a timezone can be repaired without connecting to a broker:

```powershell
uv run mt5 context set-timezone ftmo-demo Asia/Taipei
```

Select an existing context:

```powershell
uv run mt5 context use ftmo-demo
```

## Worker enrollment

On native Windows, `abt-worker enroll` prompts for omitted controller, MT5
account, server, and registration invite values. It writes versioned,
non-secret identity data to `worker.json` beside the executable; use
`--config PATH` to select another location. The password and invite are never
saved or printed.

```powershell
abt-worker enroll
abt-worker reconcile
```

Enrollment output reports `pending_approval` until an administrator approves
the worker. Existing identity configuration is protected; use
`abt-worker enroll --replace-config` only to replace it deliberately.

## Trader enrollment and event connection

On native Windows, `abt-trader enroll` prompts for omitted controller URL,
registration invite, strategy name, and a confirmed public-IP declaration. It
uses `https://api.ipify.org` only when `--public-ip` is omitted. The invite,
device private key, and Trader certificate are never written to disk.

```powershell
abt-trader enroll
abt-trader connect
```

`connect` keeps a foreground authenticated WSS session, writes lifecycle
events as JSON Lines to standard output, and sends diagnostics to standard
error. Existing Trader identity configuration is protected; use
`abt-trader enroll --replace-config` only for deliberate replacement.

If the current context has open orders or positions, an interactive switch
requires confirmation. `--yes` bypasses that prompt.

### Time calibration

MT5 exposes more than one broker clock. `tick`, `rates-*`, and `ticks-*`
use the market-data clock; `orders`, `positions`, and history use the
trade-record clock. `mt5` keeps independent, source-aware offsets in each
context's `mt5.toml`, together with the local calibration date, UTC timestamp,
status, and bounded samples. Older context files remain valid and are migrated
when saved.

There is no manual calibration command. On a market-data command, `mt5` fully
calibrates from the requested symbol's ticks when the saved calibration belongs
to another local day. During the same local day it probes the saved calibration
symbol and fully recalibrates if its integer offset drifts. If a probe cannot
run, it uses the latest saved market offset, or UTC when none exists.
The same selected market-data offset is applied when sending `rates-*` and
`ticks-*` datetime ranges to MT5, so user-local query bounds select the same
clock that their returned timestamps use for rendering.

After a successful trading command, `mt5` records host UTC immediately before
and after sending. It finds the resulting order, deal, or position by ticket
and persists a trade-record sample using the midpoint. Trade-record offsets
expire at the next date in the context timezone: rendering then explicitly
marks the expired layer before finally falling back to UTC. Inspect the active
layers without connecting to MT5:

```powershell
uv run mt5 context time-status
```

## Read-only commands

Commands write tables by default; add `--output json` for machine-readable
output. A selected context must have a timezone before any broker read/write
session can run.

```powershell
uv run mt5 account
uv run mt5 terminal
uv run mt5 symbols
uv run mt5 symbol EURUSD
uv run mt5 symbol-select SOLUSDC
uv run mt5 symbol-hide SOLUSDC
uv run mt5 tick EURUSD
uv run mt5 positions
uv run mt5 orders
uv run mt5 history-deals --since 7d
uv run mt5 rates-from EURUSD M1 --from 2026-08-14T00:00:00Z --count 100
uv run mt5 ticks-range EURUSD --from 2026-08-14T00:00:00Z --to 2026-08-14T01:00:00Z
uv run mt5 book EURUSD --watch 10
uv run mt5 calc-margin buy EURUSD 0.01 1.15000
uv run mt5 order-check --request-json '{"action":1,"symbol":"EURUSD","volume":0.01,"type":0,"price":1.15000}'
```

An unoffset ISO-8601 datetime is interpreted in the context timezone; `Z` and
explicit offsets are honored. Ambiguous or nonexistent local times at DST
transitions are rejected. History `--from YYYY-MM-DD` and `--to YYYY-MM-DD`
cover full local calendar days before being sent to MT5 as UTC. `--since`
accepts only positive hour/day durations such as `12h` or `7d`.

Tables render MT5 epoch time fields in the context timezone as
`YYYY-MM-DD HH:mm:ss`. JSON retains raw epoch values, adds `<field>_utc`
ISO-8601 values, and includes `time_metadata` with the source family,
calibration status, selected offset layer, offset seconds, timezone, and UTC
semantics. Table output includes the same source metadata above records. Raw
MT5 epochs are never rewritten; the derived UTC and displayed values subtract
the declared source offset. Write-result expiration epochs are explicitly
labelled as market-data-clock fields even when the surrounding result uses the
trade-record family.

`symbol-select SYMBOL` explicitly adds a broker symbol to the selected
terminal's Market Watch, allowing it to receive quotes; `symbol-hide SYMBOL`
removes it. These commands intentionally change terminal state and verify the
resulting visibility. MT5 can reject hiding a symbol that is in use by a chart
or another terminal feature.

`order-check` is an advanced broker-side validation endpoint. It never sends
the request as an order.

## Trading commands

Without `--yes`, typed write commands calculate and print the exact MT5
request, then require the literal response `yes` or `no`. Add `--yes` only
when the request has already been reviewed. After confirmation, `mt5` calls
`order_check` and sends only a request whose check has `retcode` zero. The
output contains the request, check result, send result, or broker error. Use
`--output json` for machine-readable records.

Market buy and sell require an explicit filling policy and allowed deviation:

```powershell
uv run mt5 buy EURUSD 0.01 --fill fok --deviation-points 10
uv run mt5 market sell EURUSD 0.01 --fill ioc --deviation-points 10 --yes
```

The pending types are `buy-limit`, `sell-limit`, `buy-stop`, `sell-stop`,
`buy-stop-limit`, and `sell-stop-limit`. Each requires `--fill` and
`--time gtc|day|specified`; stop-limit orders additionally require
`--stop-limit-price`. `--expires-in 5m`, `2h`, or `1d`, and `--expires-at`
with either an explicit offset or a context-local datetime are valid only with
`--time specified`.

```powershell
uv run mt5 buy-limit EURUSD 0.01 --price 1.15000 --fill return --time gtc
uv run mt5 sell-stop-limit EURUSD 0.01 --price 1.14000 --stop-limit-price 1.13950 `
  --fill fok --time specified --expires-at 2026-08-15T20:00:00
uv run mt5 cancel 12345678
uv run mt5 pending-modify 12345678 --price 1.15000 --clear-tp
```

For a specified pending expiration, `mt5` records the absolute input as a UTC
intent. Immediately before the broker check/send, it maps that intent using
the target symbol's latest tick time; the preview and result show the intent,
broker expiration epoch, and the broker's actual expiration when available.
Relative expirations start from that tick time and are rounded up to protect
the requested duration. If the broker reports a shorter actual duration,
`mt5` immediately cancels the pending order and reports failure.

`--magic` and `--comment` are included only when provided; comments longer
than 31 characters are rejected. New orders accept one value per protection:
`--sl`/`--tp` (absolute price), `--*-points`, `--*-pips` (Forex symbols only),
or `--*-percent`. Market protections use the executable bid/ask quote;
pending protections use the pending entry price. `--protection-from-fill` is
recognized but deliberately rejected until post-fill protection can be made
safe.

Position commands select according to the live account margin mode. Netting
accounts require `--symbol`; hedging accounts require `--ticket`. Omitting
`--volume` from `position-close` closes the full position. Modification
preserves a protection that is not named; use `--clear-sl` or `--clear-tp` to
remove it. Relative protection values on a position use its entry price.

```powershell
uv run mt5 position-modify --symbol EURUSD --sl-pips 20
uv run mt5 position-close --symbol EURUSD --fill ioc --deviation-points 10
uv run mt5 close-by 12345678 87654321
```

`close-by` is available only for two opposing positions in a hedging account.

For a broker-specific request that cannot be expressed by the typed commands,
use the deliberately explicit raw entry point. It follows the same preview,
check, and confirmation flow, but its JSON fields are not rewritten:

```powershell
uv run mt5 order-send --request-json '{"action":1,"symbol":"EURUSD","volume":0.01,"type":0,"price":1.15000}'
```
