# 04 — Execute paired FOK and IOC limit intents safely

**What to build:** Dispatch accepted intents through account workers and
implement their broker-observable FOK/IOC lifecycle, reconciliation, safety,
and event recording.

**Blocked by:** 03 — Model intent submissions and dual-broker preflight.

**Status:** ready-for-agent

- [x] Controller maps shared entry price and directional SL/TP pips to two
  opposite limit orders, respecting per-symbol pip and stops-level rules.
- [x] Dispatch sends only the preflighted order parameters and records broker
  request/response, tickets, prices, volumes, and timestamps immutably.
- [x] FOK and IOC are allowed only where both endpoints support the selected
  mode.
- [x] IOC retains matched volume, cancels residual orders, and closes
  unmatched exposure at market; it never performs IOC top-up orders.
- [x] Single-leg timeout, incomplete entry, protective exit, reconciliation
  mismatch, cancellation/close failure, and external change use the existing
  freeze and human-recovery semantics.
- [x] Every Trader receives its own lifecycle events; management receives the
  full immutable record.
