# 01 — Publish live worker market state

**What to build:** Let the management station receive and display fresh worker
connection, latest quote, open-order, and position state. Each worker polls
its local MT5 terminal on the agreed schedules, sends only changed state over
its authenticated WSS channel, and supplies a full state snapshot after
reconnection.

**Blocked by:** None — can start immediately.

**Status:** ready-for-agent

- [ ] Administrators can see each watched symbol's bid, ask, broker quote
  time, and controller receipt time, plus current worker connectivity.
- [ ] Administrators can see all open orders and positions for a worker, with
  freshness that remains understandable after worker or WSS reconnection.
- [ ] Contract tests prove diff-only updates, complete reconnect snapshots,
  preserved broker timestamps, and the 500 ms quote, 1 s order/position, and
  5 s connection polling contracts.

