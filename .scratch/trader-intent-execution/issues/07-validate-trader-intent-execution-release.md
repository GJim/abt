# 07 — Validate the Trader intent execution release

**What to build:** Prove the full broker-writing slice with automated contract
tests and a controlled multi-network release gate.

**Blocked by:** 02 — Deliver Trader identity and reconnecting WSS; 04 — Execute paired FOK and IOC limit intents safely; 05 — Cancel zero-fill intents and flatten exposure; 06 — Operate Traders and intents in management.

**Status:** ready-for-agent

- [ ] Automated tests cover invite lifecycle/migration, Trader identity and
  WSS replay, idempotent command handling, every preflight result, FOK/IOC
  execution, cancellation/fill races, and emergency flatten.
- [ ] Browser tests cover invite/trader management, active/history/event views,
  previews, confirmations, authorization, CSRF, and server errors.
- [ ] The controlled release gate proves two approved workers on separate
  networks plus one approved Trader can complete dual `order_check`, accepted
  intent execution, WSS reconnect replay, zero-fill cancellation, and
  management intervention.
- [ ] Release evidence distinguishes preflight failures from broker execution
  failures and retains immutable audit records for each scenario.
