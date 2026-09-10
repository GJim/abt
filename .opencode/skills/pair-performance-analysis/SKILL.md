---
name: pair-performance-analysis
description: Diagnose pair-cell trading performance from worker logs. Use when asked why profits lag losses, whether a strategy change helped, or to produce a pair-net report with optimization directions.
---

# Pair Performance Analysis

Evidence-first diagnosis of live pair-cell trading. Never assert a verdict
without log evidence; always state what data is missing.

## 0. Locate inputs

- Leader log: `leader*.log` (UTF-8). Follower logs: `follower*.log` / `follwer-*.log`
  (PowerShell Tee-Object output: **UTF-16LE**, `uv :` prefix, hard-wrapped mid-line).
- Deployment times: `git log --format='%h %ad %s' --date=iso` (commits are +0800;
  logs are UTC — convert explicitly).
- Live policy/risk caps: leader SQLite `cell_policy` payload
  (default `.venv/Scripts/worker.paircell.sqlite`).
- Relay audit (optional): copy `ledger.duckdb` (+WAL) out of the controller
  container; `worker_relay_facts` holds position snapshots/deltas per worker.

## 1. Establish the regime boundary

Find when the code under study went live (first marker event in the leader log,
e.g. `asymmetric_protection_applied`, `profit_trail_applied`), and split all
later comparisons at that timestamp. Note gaps (restarts, NEEDS_HUMAN parks)
as dead zones, not quiet markets.

## 2. Extract (prefer the tool)

Prefer `scripts/analyze_pair_performance.py` (handles encodings, wraps, and
attempt matching — see its docstring). It prints per-log counts, SQLite legs
split by `--cutoff`, and attempt-matched pair nets. Only hand-roll parsing
when the tool lacks a marker; if you do, record logical lines by rejoining
wrapped follower lines (a new record starts with a timestamp).

Key markers per side (`pc` = pair cell, `pcr` = relay adapter; every line is
`evt=<snake_event>` plus short IDs and flat `k=v`, no prose — transition names
double as log events, so greps and `cell_transitions` share one vocabulary):
- entries: `evt=entry_selected` (leader only: sym, dir, lots, edge, quote ages/skew);
  fills: `evt=entry_filled` (both sides: att, tkt).
- outcomes: `evt=realized_pnl_recorded` (ticket → USD), `evt=close_requested`
  (reason: `owned_ticket_disappeared` vs `maximum_holding_seconds` vs `peer_leg_empty`).
- mechanism: `evt=prot_trail` / `evt=prot_asym`, `evt=solo`, `evt=state` (human/host
  flags), relay `evt=relay_q|relay_ack|relay_rx`.

## 3. Match pairs and compute

Join leader attempt → leader ticket → realized PnL with follower attempt →
follower ticket → realized PnL on `attempt_id`. Report per-pair
(symbol, direction, L, F, net) plus totals. A pair net is the only
strategy-level number; single-leg stats (win rate, avg) are descriptive only
and must never be compared across regimes with different loss caps.

## 4. Diagnose with this checklist

1. **Friction first**: pairs where one leg wins big and the net is still
   negative reveal per-pair friction (2 spreads + slippage). Estimate it;
   every edge must clear it.
2. **Hedge-structure check**: under mirrored protection one stop implies the
   other side's take-profit (net ≈ −costs, double-loss impossible). Under
   solo continuation both legs can lose (double cap + costs). Attribute
   same-direction-opposite-result pairs to chop, not to sizing.
3. **Whipsaw scan**: same symbol alternating LONG/SHORT with minutes-apart
   re-entries after losses. Flag missing cooldowns / regime filters.
4. **Winner-exit audit**: count `maximum_holding_seconds` vs stop exits; a
   time cap cutting winners is a prime suspect when nets cluster near −friction.
5. **Trailing audit**: did trails fire? Did solo legs still hit full caps
   (sharp reversals trailing cannot save)?
6. **Availability audit**: NEEDS_HUMAN episodes (reason + duration + auto-recovery
   vs manual restart), `peer session lost` counts, dead zones. Operational
   loss (missed entries, restarts mid-attempt) is separate from strategy loss.
7. **Sample honesty**: state n, time window, and what is missing (e.g. old
   follower legs after log rotation). Old logs may be gone — say so instead
   of reconstructing.

## 5. Deliver

- Verdict first: did the change help on **pair-net** basis, with the caveat.
- One table of matched pairs; totals for old vs new leader legs from SQLite.
- Ranked optimization directions, each tied to one numbered observation.
- Offer (don't assume): implement the top fix, or record findings under
  `docs/research/` following the existing note style (scope, method,
  limitations, numbered directions).
