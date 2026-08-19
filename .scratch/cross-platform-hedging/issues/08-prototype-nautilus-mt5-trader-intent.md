# 08 — Prototype NautilusTrader MT5 backtest to Trader intent

**Type:** prototype

**Status:** ready-for-human

**Question:** Can MT5 rate exports drive a NautilusTrader backtest and produce
an intent at the console's future Trader boundary?

## Answer

Yes for the backtest-to-intent half: the prototype runs on
`nautilus-trader==1.231.0`, consumes the JSON emitted by `mt5 --output json
rates-range`, processes ten bars through `BarDataWrangler` and
`BacktestEngine`, and renders a shared-trade-intent candidate.

No for end-to-end delivery yet: an actual request to the local console at
`POST /api/trader/intents` returns `404 Not Found`. This is intentional in the
current read-only slice, which has no Trader identity, registration, or intent
API. The next implementation must define the approved Trader identity and
idempotent `202 Accepted` endpoint before any strategy can submit an intent.

**Prototype:** `prototypes/nautilus-mt5-trader-intent-prototype/run.py`
