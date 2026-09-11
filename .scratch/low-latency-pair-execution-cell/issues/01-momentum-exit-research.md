# Momentum-based profit-taking research

Status: needs-triage
Type: research

## Question

Should a solo survivor exit early when upward momentum fades, instead of
riding the trailing stop / 2h timed exit all the way?

## Context (deliberately out of scope for the current change)

The 2026-09-11 review (`leader-5.log` / `follower-4.log`, pair net `-597.32`)
showed 10 solo continuations: 2 timed-exit wins (`+464`, `+72`) versus 8
reversal losses (`-62/-68/-77/-74/-11/-91/-164/-93`). The shipped v2 keeps
both-legs solo with an immediate solo lock (breakeven, else half-risk) plus a
5-minute / 5-tick trailing cadence, and explicitly defers any momentum exit.

## Candidates to evaluate next

- quote slope / tick momentum over N seconds per product;
- trailing stall: no trail advance for M minutes while in profit;
- give-back ratio: retracement of X% from the best favorable excursion;
- time-in-profit without new highs.

## Constraints for any future proposal

- SL-only + `tp=0` model stays; momentum exit must tighten, never widen.
- Respect `minimum_stop_distance`, tick grid, journal durability.
- Keep `maximum_holding_seconds` as the backstop; momentum is an earlier,
  optional exit, not a replacement.
- Needs shadow-mode evidence before live (compare solo trail vs momentum
  exit on the same candidates).

## Comments
