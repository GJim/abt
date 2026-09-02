# MT5 `order_send` return codes and the Pair Execution Cell's `PRICE_OFF` policy

**Scope.** This note uses only MetaQuotes/MQL5 documentation for MT5 facts.
Repository statements describe the code and its executable specifications as
they stand on 2026-09-02; it does not recommend a policy change.

## Answer in brief

`MetaTrader5.order_send()` returns a trade-operation result whose `retcode`
is the trade-server response code; the Python reference demonstrates reading
`result.retcode`, while the native `OrderSend` reference says that a successful
basic call is *not* proof that the trade executed and directs callers to inspect
`MqlTradeResult`'s `retcode` (and, where necessary, `retcode_external`).
[Python `order_send`](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py);
[MQL5 `OrderSend`](https://www.mql5.com/en/docs/trading/ordersend);
[MqlTradeResult](https://www.mql5.com/en/docs/constants/structures/mqltraderesult).

`TRADE_RETCODE_PRICE_OFF` is `10021`, officially described as **“There are no
quotes to process the request.”** It is the only `TRADE_RETCODE` table entry
whose official description says that quotes are absent. This is a factual basis
for treating it as a distinct product-availability signal, rather than
conflating it with price movement, malformed input, account/symbol restrictions,
or a terminal/server connection failure. It does *not*, by itself, establish
why quotes are absent or prescribe recovery: MetaQuotes' enum description makes
neither claim. [MetaQuotes, *Return Codes of the Trade Server*](https://www.mql5.com/en/docs/constants/errorswarnings/enum_trade_return_codes).

The repository's policy is deliberately narrower than “quarantine all
rejections.” On an **entry** broker outcome categorised as `rejected`, the cell
always records `entry_rejected`, reports the rejected leg with its receipt
retcode, notifies the peer, and starts desired-`EMPTY` containment. It inserts a
durable, pair-wide product quarantine only if that receipt retcode is exactly
`10021`. [abt/pair_cell.py:5237-5264](../../abt/pair_cell.py#L5237-L5264).

## What `order_send` can report

There are two relevant result layers, which should not be mixed:

1. **Trade-server outcome:** when a result is returned, inspect
   `result.retcode`. The official Python example tests it against
   `mt5.TRADE_RETCODE_DONE` and prints the result fields; the native reference
   explains that a true/accepted `OrderSend` call still does not prove
   execution. [Python `order_send`](https://www.mql5.com/en/docs/python_metatrader5/mt5ordersend_py);
   [MQL5 `OrderSend`](https://www.mql5.com/en/docs/trading/ordersend).
2. **Python package/function failure:** `mt5.last_error()` is a separate
   MetaTrader5-library error tuple with its own `RES_*` values (for example
   invalid parameters and IPC failures), not the `TRADE_RETCODE` enumeration.
   [Python `last_error`](https://www.mql5.com/en/docs/python_metatrader5/mt5lasterror_py).

Thus the list below is the official **trade-server** `retcode` enumeration, not
an exhaustive list of every Python-library failure that can prevent obtaining a
result.

## Complete official `TRADE_RETCODE` enumeration

The following contains every code/constant in the official table. Descriptions
preserve the official meaning; a few unusually long table cells are condensed
for readability. `10037` is not listed by MetaQuotes; the table proceeds from
`10036` to `10038`. Source for every row:
[MetaQuotes, *Return Codes of the Trade Server*](https://www.mql5.com/en/docs/constants/errorswarnings/enum_trade_return_codes).

| Code | Constant | MetaQuotes description |
|---:|---|---|
| 10004 | `TRADE_RETCODE_REQUOTE` | Requote |
| 10006 | `TRADE_RETCODE_REJECT` | Request rejected |
| 10007 | `TRADE_RETCODE_CANCEL` | Request canceled by trader |
| 10008 | `TRADE_RETCODE_PLACED` | Order placed |
| 10009 | `TRADE_RETCODE_DONE` | Request completed |
| 10010 | `TRADE_RETCODE_DONE_PARTIAL` | Only part of the request was completed |
| 10011 | `TRADE_RETCODE_ERROR` | Request processing error |
| 10012 | `TRADE_RETCODE_TIMEOUT` | Request canceled by timeout |
| 10013 | `TRADE_RETCODE_INVALID` | Invalid request |
| 10014 | `TRADE_RETCODE_INVALID_VOLUME` | Invalid volume in the request |
| 10015 | `TRADE_RETCODE_INVALID_PRICE` | Invalid price in the request |
| 10016 | `TRADE_RETCODE_INVALID_STOPS` | Invalid stops in the request |
| 10017 | `TRADE_RETCODE_TRADE_DISABLED` | Trade is disabled |
| 10018 | `TRADE_RETCODE_MARKET_CLOSED` | Market is closed |
| 10019 | `TRADE_RETCODE_NO_MONEY` | There is not enough money to complete the request |
| 10020 | `TRADE_RETCODE_PRICE_CHANGED` | Prices changed |
| 10021 | `TRADE_RETCODE_PRICE_OFF` | There are no quotes to process the request |
| 10022 | `TRADE_RETCODE_INVALID_EXPIRATION` | Invalid order expiration date in the request |
| 10023 | `TRADE_RETCODE_ORDER_CHANGED` | Order state changed |
| 10024 | `TRADE_RETCODE_TOO_MANY_REQUESTS` | Too frequent requests |
| 10025 | `TRADE_RETCODE_NO_CHANGES` | No changes in request |
| 10026 | `TRADE_RETCODE_SERVER_DISABLES_AT` | Autotrading disabled by server |
| 10027 | `TRADE_RETCODE_CLIENT_DISABLES_AT` | Autotrading disabled by client terminal |
| 10028 | `TRADE_RETCODE_LOCKED` | Request locked for processing |
| 10029 | `TRADE_RETCODE_FROZEN` | Order or position frozen |
| 10030 | `TRADE_RETCODE_INVALID_FILL` | Invalid order filling type |
| 10031 | `TRADE_RETCODE_CONNECTION` | No connection with the trade server |
| 10032 | `TRADE_RETCODE_ONLY_REAL` | Operation is allowed only for live accounts |
| 10033 | `TRADE_RETCODE_LIMIT_ORDERS` | The number of pending orders has reached the limit |
| 10034 | `TRADE_RETCODE_LIMIT_VOLUME` | The volume of orders and positions for the symbol has reached the limit |
| 10035 | `TRADE_RETCODE_INVALID_ORDER` | Incorrect or prohibited order type |
| 10036 | `TRADE_RETCODE_POSITION_CLOSED` | Position with the specified `POSITION_IDENTIFIER` has already been closed |
| 10038 | `TRADE_RETCODE_INVALID_CLOSE_VOLUME` | A close volume exceeds the current position volume |
| 10039 | `TRADE_RETCODE_CLOSE_ORDER_EXIST` | A close order already exists for a specified position |
| 10040 | `TRADE_RETCODE_LIMIT_POSITIONS` | The number of open positions simultaneously present on an account can be limited by the server settings |
| 10041 | `TRADE_RETCODE_REJECT_CANCEL` | The pending order activation request is rejected, the order is canceled |
| 10042 | `TRADE_RETCODE_LONG_ONLY` | The request is rejected because only long positions are allowed for the symbol |
| 10043 | `TRADE_RETCODE_SHORT_ONLY` | The request is rejected because only short positions are allowed for the symbol |
| 10044 | `TRADE_RETCODE_CLOSE_ONLY` | The request is rejected because only position closing is allowed for the symbol |
| 10045 | `TRADE_RETCODE_FIFO_CLOSE` | The request is rejected because the account allows position closing only by FIFO rule |
| 10046 | `TRADE_RETCODE_HEDGE_PROHIBITED` | The request is rejected because opposite positions on one symbol are disabled for the account |

## Why `10021` is materially different—no extrapolation

The distinctions below are limited to the official wording:

| Code(s) | What the official description actually says | Difference from `10021` |
|---|---|---|
| `10020 PRICE_CHANGED`; `10004 REQUOTE` | Prices changed; requote. | Neither description says quotes are absent. |
| `10015 INVALID_PRICE`; `10016 INVALID_STOPS`; `10014 INVALID_VOLUME`; `10013 INVALID` | The request, price, stops, volume, or request itself is invalid. | These identify request validation conditions, not quote absence. |
| `10017 TRADE_DISABLED`; `10018 MARKET_CLOSED`; `10026`/`10027` autotrading disabled; `10032 ONLY_REAL`; `10042`–`10046` trading restrictions | Trading is disabled/closed/restricted by terminal, server, account, or symbol rule. | These identify authorization, schedule, mode, or policy restrictions, not quote absence. |
| `10019 NO_MONEY`; `10033`/`10034`/`10040` limits | Funds or account/order/position limits prevent the request. | These are capacity constraints, not quote absence. |
| `10011 ERROR`; `10012 TIMEOUT`; `10024 TOO_MANY_REQUESTS`; `10028 LOCKED`; `10029 FROZEN`; `10031 CONNECTION` | Processing, timing, rate, lock/freeze, or trade-server connectivity condition. | `10031` is specifically “No connection with the trade server,” whereas `10021` says the server has no quotes to process the request. |
| `10007 CANCEL`; `10023 ORDER_CHANGED`; `10025 NO_CHANGES`; `10035`–`10039`, `10041` | Cancellation, changed/no-change state, or order/position lifecycle condition. | These concern request/order/position state, not absent quotes. |

Source for the complete wording in the table:
[MetaQuotes, *Return Codes of the Trade Server*](https://www.mql5.com/en/docs/constants/errorswarnings/enum_trade_return_codes).

## Actual PairExecutionCell policy

### Trigger and replication

* The code fixes `PRICE_OFF_RETCODE = 10021`; entry success is separately
  recognised only for `10008` (`PLACED`) and `10009` (`DONE`).
  [abt/pair_cell.py:101-103](../../abt/pair_cell.py#L101-L103)
* A locally rejected **entry** quarantines only when
  `outcome.receipt["retcode"] == PRICE_OFF_RETCODE`. The code then includes that
  retcode in the rejected leg status; it does not test other retcodes for
  quarantine. [abt/pair_cell.py:5237-5264](../../abt/pair_cell.py#L5237-L5264)
* Receiving a peer leg-status message independently applies the same exact
  `10021` comparison, so either Worker's confirmed `PRICE_OFF` receipt produces
  the pair-wide effect. [abt/pair_cell.py:5876-5892](../../abt/pair_cell.py#L5876-L5892)
* The durable record is keyed by `product_id`, records offending worker,
  attempt, universe generation, receipt, and time, and transitions
  `product_quarantined`. Peer-advertised records are accepted only if their
  receipt is also exactly `10021`. [abt/pair_cell.py:5946-6045](../../abt/pair_cell.py#L5946-L6045)

The implementation/spec therefore means *product-specific*, not account-wide
or pair-wide-symbol-wide for new records. Its tests verify that both cells
quarantine the affected derived product while another product remains eligible.
[tests/test_pair_cell.py:3168-3180](../../tests/test_pair_cell.py#L3168-L3180)

### Lifetime and release

The quarantine is durable, carries the discovered product identity and
generation, and is not an automatic retry/backoff. The implementation rejects
release while an attempt remains unresolved; otherwise it writes a release
marker/audit row, deletes the quarantine, and records
`product_quarantine_released`. [abt/pair_cell.py:5946-6018](../../abt/pair_cell.py#L5946-L6018);
[abt/pair_cell.py:6047-6074](../../abt/pair_cell.py#L6047-L6074).
The executable specs verify survival through restart/no automatic expiry, refusal
while unresolved, and an explicit operator release.
[tests/test_pair_cell.py:3192-3237](../../tests/test_pair_cell.py#L3192-L3237)

This is also explicit in the feature specification: `10021` is pair-wide and
product-specific, survives restart, has no automatic expiry, and requires
authenticated operator release when no attempt is unresolved.
[.scratch/low-latency-pair-execution-cell/spec.md:605-617](../../.scratch/low-latency-pair-execution-cell/spec.md#L605-L617).

### What happens for every other rejected entry outcome

They are **not silently ignored**, and they are **not quarantined by this
policy**. The common rejection path:

1. marks/persists the local leg as `rejected` and records `entry_rejected`;
2. reports the rejected status and actual retcode to the peer;
3. sends a best-effort `pair_entry_failed`; and
4. starts desired-`EMPTY` convergence.

[abt/pair_cell.py:5237-5264](../../abt/pair_cell.py#L5237-L5264)

The peer treats `pair_entry_failed` as an immediate containment signal, while
the notification is expressly not required for safety.
[abt/pair_cell.py:5721-5738](../../abt/pair_cell.py#L5721-L5738)
`rejected` is durable proof of no peer broker exposure for terminal-proof
purposes; containment becomes `CONVERGING_EMPTY`, obtains broker facts, and
finalises only after local empty proof plus peer terminal proof when needed.
[abt/pair_cell.py:125-127](../../abt/pair_cell.py#L125-L127);
[abt/pair_cell.py:6077-6133](../../abt/pair_cell.py#L6077-L6133).

The spec states the intended common outcome precisely: if either entry rejects
(as well as unknown send/evidence/protection failures), desired state becomes
`EMPTY`; the notification accelerates peer containment but is not a safety
dependency. [.scratch/low-latency-pair-execution-cell/spec.md:878-897](../../.scratch/low-latency-pair-execution-cell/spec.md#L878-L897).
Tests cover leader rejection containing both legs, follower rejection containing
the leader, and both sides rejecting to broker-verified empty.
[tests/test_pair_cell.py:2956-2985](../../tests/test_pair_cell.py#L2956-L2985)

One separate, narrow exception is not an entry rejection rule:
`10025 TRADE_RETCODE_NO_CHANGES` is considered an idempotent success only for a
`modify_sl_tp` protection effect, with fresh broker evidence still required. It
is not successful for entry, close, cancel, or another effect.
[.scratch/low-latency-pair-execution-cell/spec.md:878-883](../../.scratch/low-latency-pair-execution-cell/spec.md#L878-L883).

## Interpretation boundary

The official evidence justifies the factual statement “`10021` means no quotes
to process the request.” The further decision to permanently quarantine the
affected derived product until operator release is this application's
safety/operational policy, documented by the implementation and spec—not a
MetaQuotes requirement or a claim that other retcodes are harmless.
