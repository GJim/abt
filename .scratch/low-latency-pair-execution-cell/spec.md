# Low-Latency Pair Execution Cell

Status: ready-for-agent

## Problem Statement

The current Pair Execution Cell proved that Worker-owned entry can complete a
real two-leg lifecycle, but its coordination and sizing model does not match
the intended architecture.

The deployed design requires controller-issued Pair Contracts and leases,
fixed per-Worker volumes, an arm/commit round trip, a common future execution
time, and several overlapping timing parameters. This delays the first broker
send and makes the controller part of the execution protocol. Fixed Worker
volumes are also unsafe for an automatic multi-product strategy because margin,
volume constraints, and account budgets vary by product and account.

The intended design is simpler:

- only the leader detects an edge and owns the pair lifecycle;
- the leader immediately starts its protected broker entry and concurrently
  relays one immutable attempt to the follower;
- the follower does not re-evaluate quotes or edge and immediately starts its
  protected broker entry after minimum non-market safety admission;
- both Workers use precomputed per-product sizing and emergency-protection
  plans, never signal-time remote reads;
- the leader must confirm both exact broker-observed positions within its
  bounded local timeout or contain any surviving exposure;
- the follower independently closes its own leg unless pair confirmation
  arrives within its receive-relative local timeout;
- precise protection is applied only after both exact positions are confirmed;
- the controller authenticates the two Workers and relays opaque envelopes,
  but does not issue trading leases, distribute strategy policy, interpret
  attempts, or own lifecycle state.

A second mismatch remains in how a pair comes into existence and how it learns
what it may trade. The deployed design requires an administrator to create the
leader/follower route through the controller, binds that route to a Trader
identity the cell never actually uses for trading, requires a per-Worker
configuration file that names the route and a pre-analyzed, explicitly Built
product universe, and refuses to start the cell at all when that file is
absent. That model cannot bootstrap: the Built universe for a pair cannot be
produced before the pair exists, the `trader_id` makes the controller look
like a trading coordinator, and every deployment needs an out-of-band admin
action before any Worker can trade.

The revised design makes a pair self-forming:

- a Worker declares a desired role on its first unpaired connection, and an
  omitted role means it is simply an available follower;
- a leader lists currently connected, available, unpaired followers from the
  controller and selects one, interactively or by explicit Worker ID;
- the selected follower automatically accepts only when it can prove locally
  that accepting is safe;
- the controller arbitrates exclusive route ownership through a two-phase
  reservation workflow — one atomic transaction reserves both Workers for a
  proposal, and a second atomic transaction creates the route — so two
  leaders can never claim the same follower, while the controller remains an
  identity/authentication and opaque control+data relay, never a trading
  coordinator;
- the route is durable across reconnect and restart, identified by a
  controller-generated globally unique `route_id`, until an explicit safe
  unpair that both Workers prove is safe;
- the follower authors its own risk tunables and its own frozen startup
  balance and sends them to the leader in a Pairing Acceptance payload, which
  the leader copies verbatim into the one canonical policy;
- no configuration file is required, an absent one means the runtime
  synthesizes its defaults, and a supplied file has role-specific authority
  rather than blanket override power; and
- the tradable universe is discovered by the two Workers themselves,
  immediately after the route exists and before any quote is processed.

Cross-broker execution is not atomic. The design deliberately accepts a short,
measured asymmetric-entry window in exchange for immediate broker dispatch,
while bounding that window with broker-native emergency protection, separate
five-second monotonic timers on the leader and follower, durable effect
journals, and immediate containment.

## Solution

### Worker-initiated pairing

There are no administrator-created pair routes. Pairing is Worker-initiated.

On its first connection while unpaired, a Worker declares its desired Pair
Execution Cell role. An omitted role means the Worker is an **available
follower**: authenticated, connected, unpaired, not currently reserved by
another pairing proposal, and willing to be selected.

A Worker that declared `leader` asks the controller for the list of currently
connected, available, unpaired followers and selects one. Selection is
interactive by default. An optional non-interactive `--follower-worker-id`
selects a specific Worker without a prompt, for unattended deployment; the
named Worker must still be connected, available, and unpaired at proposal
time or the leader fails closed.

A leader on a non-TTY process without `--follower-worker-id` never blocks on
input. It remains authenticated and unpaired, emits an actionable diagnostic
naming the missing selection and the flag that supplies it, and stays
available for a later operator action or restart with the flag. It does not
select a follower implicitly, does not silently downgrade itself to follower,
and does not exit the process. An interactive leader that is shown an empty
available-follower list behaves the same way: it stays authenticated and
unpaired with an explicit reason, and may re-list later.

### Two-phase pairing reservation

Reservation and route creation cannot be one compare-and-swap, because the
follower's acceptance decision happens between them and is made by the
follower, not the controller. The controller therefore runs a two-phase
control workflow with two separate atomic transactions.

**Phase one — reservation.** The leader proposes a pairing. One atomic
controller transaction generates a unique `proposal_id` and reserves **both**
Workers for it, succeeding only if, at that instant, both the proposing leader
and the selected follower:

- are authenticated and connected;
- appear in no other pair route and no other live pairing reservation;
- hold no conflicting live strategy-runtime ownership for their pair; and
- can claim `pair_execution_cell` execution-mode exclusivity, which is
  checked and taken for **both** Workers as part of this transaction.

The reservation carries a 30-second **pairing-reservation timeout**. That
timeout is pairing control metadata only. It is not a trading lease, not a
TTL on any route, and it never fences, authorizes, or expires a broker
effect, an attempt, or an established route.

Only after that transaction commits does the controller forward the proposal,
with its `proposal_id`, to the reserved follower.

**Phase two — final route creation.** The follower accepts or refuses. On
acceptance, a second atomic controller transaction creates the route by
compare-and-swap, succeeding only if the reservation for that `proposal_id`
is still valid and unexpired and both Workers remain eligible under the same
uniqueness constraints. That transaction generates the route's `route_id` and
records the ordered leader/follower Worker pair, the assigned roles, and the
execution-mode claim.

The reservation is released, and both Workers return to unpaired and
unreserved, when any of the following happens: the follower refuses, the
30-second pairing-reservation timeout elapses, the leader or follower
disconnects before the final transaction commits, or the final
compare-and-swap fails. A released reservation leaves no route, no partial
role assignment, no retained execution-mode claim, and no durable local
pairing state on either Worker.

Two leaders selecting the same follower therefore produce exactly one winner
and one deterministic conflict, detected at phase one: the loser's
reservation transaction fails immediately, it re-lists, and it selects again.
It never waits out the winner's 30-second reservation on the assumption that
the winner will fail.

The selected follower **automatically** accepts, with no human step, only when
all of the following hold locally at that moment:

- its account is broker-verified empty;
- it has no unresolved attempt and no unresolved local broker effect;
- it is entry-ready enough to pair — valid identity and session, usable MT5
  handle, readable account, and available broker clock calibration;
- it can read a positive account balance in a USD account currency; and
- it is still unpaired.

Any unmet condition is an explicit, reasoned refusal. A refusal never becomes
an implied acceptance, releases the reservation immediately, and the leader
re-lists rather than retrying blindly.

### Route identity and authority

Durable route identity is the ordered (`leader_worker_id`,
`follower_worker_id`) pair plus a controller-generated, globally unique
`route_id`. Every new pairing produces a new `route_id`; a `route_id` is
never reused, not even for the same two Workers pairing again after a safe
unpair. There is no `trader_id` on the Pair Execution Cell route, in its
envelopes, in its ownership records, in its configuration, or in its
controller APIs.

`route_id` is not a lease epoch. It has no TTL, does not expire, is not
renewed, does not fence broker effects, and confers no trading authorization.
It is only the durable name of one pairing relationship. The Worker-local
`recovery_epoch` remains an entirely separate concept: it is market evidence
about a Worker's own broker session continuity, it is carried on quote
evidence, and it is never derived from or compared against `route_id`.

The **controller's route record is authoritative** for relay permission and
for which Worker holds which role. The controller forwards an envelope only
when its `route_id` matches a live route record and the sending Worker holds
the sending role on that record. Each Worker's durable copy of the route is
**recovery evidence** about its own participation, not a competing authority.

When a Worker's durable copy disagrees with the controller's route record —
different `route_id`, different assigned role, or a route the controller no
longer holds — the Worker immediately removes entry readiness and raises a
metadata reconciliation. It never discards, closes, hides, or infers local
broker exposure because of a metadata disagreement, and it never adopts a
role the controller does not assign it. Containment of anything it already
owns continues from its own durable effects and exact broker facts.

### Execution-mode exclusivity

`pair_execution_cell` execution-mode exclusivity is claimed for **both**
Workers inside the pairing reservation transaction and confirmed inside the
final route-creation transaction. A pairing is rejected at phase one if
either Worker's pair is still held live by the legacy Strategy Runtime.

The Pair Execution Cell claims that mode with a fixed owner kind of
`pair_execution_cell` and no `trader_id`. The legacy Strategy Runtime retains
its own Trader identity on its own claims, unchanged.

### Controller role

Within the Pair Execution Cell, the controller is an identity and
authentication authority, the arbiter of exclusive route ownership through the
two-phase pairing workflow, and an opaque control-and-data relay. It is not a
trading coordinator. It authenticates both Workers, verifies that messages
stay on that route by `route_id` and sending role, enforces protocol version
and payload-size limits, and forwards envelopes unchanged and in order within
each live route session. It does not issue Pair Leases or Pair Contracts,
inspect trading payloads, parse policy, Pairing Acceptance payloads, product
catalogs, or discovered universe content, persist an authoritative attempt
projection, replay execution commands, calculate edges, or decide recovery. A
reconnect starts a new ordered live session; the controller does not replay
envelopes from the previous session.

### Durable route, reconnect, and safe unpair

A route is durable across reconnect and Worker restart until an explicit safe
unpair. It has no TTL, no lease, and no epoch. Its `route_id` is a durable
name, not a lease epoch: it is never renewed, never expires, and never fences
a broker effect.

An existing durable route always wins over startup role defaults and startup
role flags. A Worker that reconnects and finds itself already on a route
resumes its assigned role for that `route_id` and never enters the listing or
proposal flow. A declared role that contradicts the durable route is recorded
as a diagnostic and ignored; it never silently changes who leads.

#### Safe unpair through an explicit UNPAIRING state

Safe unpair is not a single message. The route record has an explicit
`UNPAIRING` state, and the route moves through it before removal.

Either authenticated Worker on the route may enter `UNPAIRING` with an
explicit unpair command naming the current `route_id`. There is no proposer
privilege: the follower may initiate exactly as the leader may.

Entering `UNPAIRING` takes effect immediately on both sides. The controller
records the state, notifies both Workers, and both Workers **immediately
remove entry readiness**. No new attempt may be started by either Worker while
the route is `UNPAIRING`; a leader in `UNPAIRING` stops candidate evaluation
and originates nothing. `UNPAIRING` never prevents either Worker from
protecting, reducing, or containing exposure it already owns.

While in `UNPAIRING`, each Worker independently produces a **safe-unpair
assertion** stating, from fresh local evidence, that:

- its own account is broker-verified empty from fresh broker observation; and
- every attempt and every local broker effect it owns is terminal.

Each assertion binds to the current `route_id` **plus that Worker's current
local attempt/effect state version**, a monotonic local version that advances
on any new or changed local attempt or broker effect. An assertion is valid
only while both bindings still hold. Any new or changed local effect — a
containment write, a late fill observation, an external change, a reappearing
position — advances the state version and invalidates that Worker's
outstanding assertion, which must then be re-produced from fresh evidence.

The controller removes the route only when it holds two **fresh** assertions,
one per Worker, both bound to the current `route_id` and each still matching
its Worker's current state version. A stale assertion, a one-sided assertion,
or an assertion bound to a superseded state version or a different `route_id`
does not unpair. The controller does not infer, verify, or derive emptiness or
terminality; it records two Worker assertions, checks their bindings, and
arbitrates the route record.

A route may leave `UNPAIRING` and return to normal operation only by an
explicit cancel from either Worker, which restores entry readiness subject to
the ordinary admission chain. After the route is removed, both Workers become
unpaired, all execution-mode claims for the pairing are released, and they are
eligible to pair again — which produces a new, never-reused `route_id`.

Emergency identity revocation remains a separate mechanism and is never a safe
unpair. Revoking a Worker's device credential removes authentication and
relay, and therefore removes entry readiness immediately, but it never implies
broker `EMPTY` and never authorizes the controller to conclude that any leg is
flat. Each Worker still converges through its own local containment from its
own durable effects and exact broker facts.

Loss of the authenticated peer session likewise removes entry readiness
immediately. It does not prevent either Worker from protecting or reducing
exposure it already owns. Role changes are allowed only after both accounts
are broker-verified empty and all prior attempts are terminal, that is, only
through a safe unpair followed by a new pairing.

### Policy construction and canonical policy

The leader is the sole holder and publisher of the one canonical strategy
policy and the sole edge decision maker. It is not, however, the author of the
follower's risk numbers.

#### Pairing Acceptance payload

A follower's acceptance is not a bare yes. It carries a **Pairing Acceptance
payload**, sent to the leader through the opaque controller relay, which the
controller forwards unchanged and never parses. The payload contains:

- the `proposal_id` it is accepting;
- the follower's `worker_id`;
- the follower's **frozen startup balance**: its broker account balance read
  once at that moment, together with the account currency, which must be USD;
  and
- the follower's own **authored risk tunables**: `maximum_margin_fraction`,
  `daily_loss_fraction`, `trade_loss_fraction`, and
  `maximum_loss_per_trade_usd`.

Those four tunables and that balance are the follower's own, taken from its
own configuration file if it supplies one and from the synthesized defaults
otherwise. The follower freezes them at acceptance and does not change them
for the life of the route.

The leader **copies them verbatim** into the canonical policy's
`follower_risk` block. It does not recompute, clamp, rescale, average,
substitute, or default them, and it may not overwrite them with its own
values. A leader that cannot copy the payload verbatim — it is missing,
malformed, non-USD, non-positive, or internally inconsistent — fails closed
and does not publish a policy.

The canonical policy therefore contains exactly two risk blocks:
`leader_risk`, authored by the leader for itself, and `follower_risk`, copied
verbatim from the Pairing Acceptance payload. Shared policy — `mode`,
`entry_edge_points`, `quote_max_age_seconds`, `quote_max_skew_seconds`,
`follower_confirmation_timeout_seconds`, `flatten_at_ny`, and
`maximum_holding_seconds` — is authored by the leader alone.

#### Policy acceptance and persistence

Before quote processing begins, the leader sends the canonical, versioned
policy and its hash to the follower through the opaque relay. The follower
validates that its own `follower_risk` block and frozen startup balance are
byte-identical to what it sent, and durably accepts that exact policy only
while its account is broker-verified empty. A mismatch in its own block is a
fail-closed refusal, not a renegotiation.

Both Workers **persist the full canonical policy content**, not only its hash.
The hash is an integrity check over content each side already holds durably;
it is never the sole durable record. Recovery, audit, and attempt validation
read the persisted content, so a restarted Worker never has to re-request the
policy to know what it agreed to.

Changed policy content requires a new hash and both Workers to be
broker-verified empty. The follower never originates an entry attempt.

#### Continuous remaining-allowed-leg-loss summary

Each Worker continuously publishes a **versioned remaining-allowed-leg-loss
summary** to its peer over the opaque relay, refreshed as its own realized
daily loss changes and always before any signal can consume it. The summary
carries the publishing Worker's `worker_id`, a monotonic version, and its
current `allowed_leg_loss_usd` — the same value computed under "Per-leg loss
policy" below.

The leader must have a current follower summary before it may create an
attempt, and it assigns each leg a loss no greater than that leg owner's
last-published remaining allowance. The assigned per-leg losses and the summary
versions they were derived from are frozen into the immutable attempt.

The summary is an optimization for the leader, never an authority over the
follower. The follower independently rejects an attempt whose assigned loss
for its own leg exceeds its **current local** remaining allowance at receipt,
even when that attempt was consistent with the summary the leader used. This
is a non-market local safety check and requires no quote, edge, or market
evaluation.

### Configuration and startup surface

No Pair Execution Cell configuration file is required. When none is present,
the runtime synthesizes its defaults and the cell is available; an absent file
never disables pairing and never blocks startup.

The conceptual command-line surface is:

- `--pair-cell-role {leader,follower}` — the desired role on first unpaired
  connection. Omitted means `follower`, that is, an available follower.
- `--follower-worker-id <worker_id>` — optional, leader only. It selects the
  follower non-interactively for unattended deployment. A leader without it
  is shown the controller's list of currently connected, available, unpaired
  followers and selects interactively; a non-TTY leader without it stays
  authenticated and unpaired with an actionable diagnostic instead of
  blocking on input.
- `--pair-cell-unpair` — conceptual operator surface that requests a safe
  unpair of this Worker's current route, available on either role.
- `--pair-cell-rediscover` — conceptual operator surface that requests an
  explicit rediscovery on this Worker's current route.

#### Role-specific configuration authority

A supplied configuration file does not have blanket override power. Its
authority depends on the role the Worker actually holds.

A **follower** override file may set only:

- its own per-Worker risk tunables — `maximum_margin_fraction`,
  `daily_loss_fraction`, `trade_loss_fraction`, and
  `maximum_loss_per_trade_usd` — which it authors and sends in its Pairing
  Acceptance payload; and
- its own local safety guard `allow_live` (see below).

A follower file may **not** set shared policy: `entry_edge_points`,
`quote_max_age_seconds`, `quote_max_skew_seconds`,
`follower_confirmation_timeout_seconds`, `flatten_at_ny`,
`maximum_holding_seconds`, or `mode`. Any of those in a follower's file is a
startup configuration error, not a silently ignored field.

A **leader** override file may set the shared policy values above and its own
per-Worker risk tunables. It may never set the follower's risk tunables or the
follower's budget; those arrive only in the Pairing Acceptance payload and are
copied verbatim.

Because a Worker's role is not known until it is on a route, a file that
contains only follower-permissible keys is valid for either role, and a file
containing shared-policy keys is valid only when that Worker actually becomes
the leader. A Worker that receives a route role contradicting its own file's
authority fails closed with an explicit diagnostic and does not pair or trade
under a partially applied file.

No configuration file, in either role, may contain a route, a `route_id`, a
`trader_id`, `built_products`, or `eligible_products`; any of those is a
startup configuration error, because they now belong to pairing and discovery,
not to local operator configuration.

#### Mode authority and the follower's fail-closed guard

`mode` is a **pair-level, leader-authored** value with default `live`. It is
part of the shared canonical policy, and the follower never authors it and
never rewrites it.

A follower may nonetheless refuse to trade live. Its local configuration may
carry an explicit `allow_live` safety setting, default `true`. When
`allow_live` is `false` and the canonical policy's `mode` is `live`, the
follower **fails closed**: it refuses the policy with an explicit reason,
publishes no acceptance of it, admits no attempt, and remains paired but not
entry-ready. It does not rewrite `mode` to `shadow`, does not silently run
shadow while the leader runs live, and does not negotiate.

The leader observes that refusal as an explicit peer-not-ready reason. It may
not treat the refusal as acceptance and may not proceed to originate attempts
against a follower that has not accepted. Resolving the disagreement is an
operator action: either the leader publishes a `shadow` policy while both
accounts are broker-verified empty, or the follower's `allow_live` is changed
and the follower re-evaluates the policy while still empty. Until then the pair
is paired, safe, and idle.

This command-line and configuration schema is migration work. The current
implementation still requires a route-bearing `--pair-cell-config` file and
has no role, follower-selection, or default-materialization options.

### Default policy materialization

The default execution mode is `live`. This is an intentional operator
decision: an unattended Worker that pairs itself is expected to trade, not to
sit in shadow mode until someone edits a file. Live by default does not weaken
any admission rule. Every Worker still fails closed unless pairing, initial
compatible product discovery, broker clock calibration, sizing, protection
constructibility, broker-verified emptiness, and peer readiness all pass.
Shadow mode remains selectable for rollout and comparison, and a follower may
always refuse live through `allow_live = false`.

The default `strategy_budget_usd` for each Worker is that Worker's startup
broker account **balance**. This is a deliberate divergence from
`strategy/realtime_arbitrage.py`, which allocates from startup **equity**
(`--allocation-usd` defaults to the endpoint's startup equity). Balance is
chosen because it excludes floating P&L from unrelated or leftover positions
and is therefore a stable, reproducible sizing base for a frozen policy.

Reading that balance requires, and fails closed without:

- an account currency of **USD**, because every risk figure in this
  specification is denominated in USD and no FX conversion of the budget is
  performed; and
- a strictly **positive** balance.

A non-USD account currency, an unreadable account, a zero or negative balance,
or a `None`/malformed account record is a fail-closed refusal to pair (for a
follower) or to publish policy (for a leader), never a substituted or
defaulted budget.

The balance is read once at startup, frozen into that pairing's canonical
policy, and covered by the policy hash. It is not re-read while the pairing
lives, so an intraday balance change never silently resizes an active
strategy. A later safe unpair and new pairing re-reads the startup balance;
if it differs, the resulting policy content differs and therefore produces a
new policy hash.

Legacy `realtime_arbitrage` also keeps a separate 20% margin headroom on top
of its margin ratio. This specification deliberately carries **no extra margin
headroom** beyond `maximum_margin_fraction`; that fraction is the whole margin
budget, and it is set conservatively for that reason.

Remaining defaults follow the existing `strategy/realtime_arbitrage.py`
behavior where an equivalent parameter exists, so the cell starts from the
already-operated strategy's numbers:

- `entry_edge_points` = `4`;
- `maximum_margin_fraction` = `0.10`;
- `daily_loss_fraction` = `0.03`;
- `trade_loss_fraction` = `0.02`;
- `maximum_loss_per_trade_usd` = `40`;
- `quote_max_age_seconds` = `1`;
- `follower_confirmation_timeout_seconds` = `5`;
- `flatten_at_ny` = `16:00`; and
- `maximum_holding_seconds` unset.

`quote_max_skew_seconds` has no legacy equivalent — the legacy realtime
strategy has no skew parameter — so it is defined here as a Pair Execution
Cell parameter with default `1`.

Existing runtime polling, refresh, and integrity intervals stay implementation
details of the runtime. They are not strategy policy, are not covered by the
policy hash, and are not exchanged with the peer.

### Initial Compatible Product Discovery

There is no `built_products` configuration, no `eligible_products` filter, and
no "active built products only" invariant. Product eligibility is discovered
by the two Workers themselves.

Immediately after the route is established and before any quote is processed,
both Workers exchange versioned catalog and specification summaries over the
opaque relay. The controller forwards those summaries unchanged and never
parses them.

Compatibility uses the legacy `realtime_arbitrage` rules exactly:

- a symbol enters the candidate set from either catalog only if its
  `trade_mode` is exactly `4` (full trading). This precondition is applied to
  each catalog **before** the intersection, matching
  `strategy/realtime_arbitrage.py`, which skips every catalog entry whose
  `trade_mode != 4`. A missing, non-integer, or boolean `trade_mode` makes
  that catalog invalid, not merely that symbol ineligible;
- candidate symbols are then the **exact symbol-name intersection** of the two
  filtered catalogs; there is no suffix normalization, aliasing, or fuzzy
  matching;
- a candidate is excluded on any inequality of `trade_calc_mode`,
  `contract_size`, `volume_min`, `volume_step`, or `allowed_directions`;
- a candidate is excluded unless both `LONG` and `SHORT` are allowed;
- the shared filling mode is `FOK` when both support it, otherwise `IOC` when
  both support it, otherwise the candidate is excluded.

Base currency, profit currency, `digits`, `point`, and `trade_tick_size` are
deliberately **not** required to be equal.

For each admitted shared symbol:

- the canonical execution point is the **maximum** of the two Workers' points,
  exactly as `realtime_arbitrage` does, so the coarser broker's resolution
  governs the edge threshold;
- the common `volume_max` is the **minimum** of the two; and
- product identity is deterministically derived from the exact symbol name
  plus the canonical compatibility summary and its hash, so the same two
  catalogs always produce the same identity.

Because `contract_size`, `volume_min`, and `volume_step` are equal by
admission, an admitted FX product has a 1:1 lot relationship.

Account-local facts remain local plan inputs and never enter product identity:
account-currency tick values, margin per lot, stop and freeze levels, and
current tradability. A missing or stale local fact removes only that product
from this Worker's eligibility.

Discovery fails closed. If either catalog is unavailable or invalid, or if no
compatible symbol survives, the pair has no eligible universe, no quote
processing begins, and no entry is admitted.

#### Universe generation

Discovery has its own identifier, separate from route identity. Each discovery
result carries a **`universe_generation`**, unique within its route and
monotonically increasing. It names a discovery result; it never names a
pairing, never replaces `route_id`, and never expires.

The discovered universe is frozen for the current `universe_generation`. The
hourly refresh revalidates the selected symbols and their sizing and protection
inputs, and may suspend a symbol that no longer matches its frozen
compatibility summary, but it never silently admits a symbol that appeared in a
broker catalog after discovery, and it never changes `universe_generation`.

Admitting newly appeared symbols requires an **explicit rediscovery**. Either
authenticated Worker may request one through the conceptual operator surface.
Rediscovery is admitted only while both accounts are broker-verified empty and
no attempt or local effect is unresolved on either side. It re-runs the catalog
and specification exchange and installs the result under a **new
`universe_generation` on the same route**. The route, its `route_id`, the
assigned roles, and the canonical policy are all unchanged; rediscovery is not
a re-pairing and never produces a new `route_id`. A rediscovery that fails
closed leaves the previous `universe_generation` in place and the pair not
entry-ready for the failed attempt's duration, rather than installing an empty
universe.

Route-scoped and universe-scoped state are carried separately:

- relay **envelopes carry `route_id`**, and the controller validates only
  `route_id`, the sending Worker's role on that route record, protocol
  version, and payload size. It never validates, parses, or compares universe
  content;
- **policy, attempt, product, sizing-plan, and quarantine state carry
  `universe_generation`** where product identity is in play. An attempt names
  the `universe_generation` its product came from and is rejected if that is
  not the follower's current one.

Durable MT5 `10021 TRADE_RETCODE_PRICE_OFF` quarantine remains pair-wide and
product-specific, keyed by the derived product identity and recorded with the
`universe_generation` in which it was observed. Because product identity is
deterministically derived from the exact symbol name and the canonical
compatibility summary hash, a quarantine survives a rediscovery whenever the
rediscovered product resolves to the same identity. It survives Worker
restart, has no automatic expiry, and requires explicit authenticated operator
release while no attempt is unresolved.

### Per-product sizing plans

Each Worker has its own:

- `strategy_budget_usd`;
- `maximum_margin_fraction`;
- `daily_loss_fraction`, initially `0.03`;
- `trade_loss_fraction`, initially `0.02`; and
- `maximum_loss_per_trade_usd`.

#### Sizing and protection plan timing

Sizing plans cannot be built at Worker startup, because there is no discovered
universe to size until the route exists and discovery has succeeded. The
startup ordering is therefore:

1. pair and establish the route;
2. run Initial Compatible Product Discovery and install a
   `universe_generation`;
3. accept the canonical policy;
4. **begin quote collection** for the discovered products, so that a current
   local quote exists for each product and direction;
5. build the **initial sizing and emergency-protection plans** for every
   discovered product and both directions, from a **current local quote and a
   current local MT5 margin calculation**; and
6. only then begin candidate evaluation.

**No candidate is processed until valid plans exist.** The leader does not
evaluate, rank, or select any candidate for a product that has no valid
positive plan version, and the follower rejects any attempt naming a product it
has no valid plan for. Quote collection running ahead of plan construction is
data gathering only; it never produces a signal.

After the initial build, plans refresh once per hour. A failed product refresh
removes only that product from eligibility; the next hourly refresh evaluates
it again.

For each direction, a Worker obtains current margin-per-lot from MT5 and
computes:

```text
margin_budget_usd = strategy_budget_usd * maximum_margin_fraction
local_max_lots = floor_to_volume_step(
    margin_budget_usd / margin_per_lot,
    volume_min,
    volume_max,
    volume_step,
)
```

Workers exchange versioned sizing-plan summaries before a signal. Because a
discovered product guarantees equal contract size, minimum volume, and volume
step, the selected pair volume is:

```text
pair_lots = min(leader_local_max_lots, follower_local_max_lots)
```

snapped to the shared volume step. Both legs use exactly `pair_lots`. A product
is ineligible when either side has no valid positive plan.

The plan also caches the canonical execution point, local point and tick size,
account-currency profit/loss tick values, current volume limits, common filling
mode, stop/freeze constraints, rough emergency SL/TP inputs, and USD-per-point
economics. The leader uses the selected common lots and the lower of the two
cached USD-per-point values to rank simultaneous candidates by conservative
expected edge USD.

#### Superseded plan retention

Each plan version is immutable and named. An attempt names the exact plan
version each leg was sized from.

A refresh does not delete the version it supersedes. Each superseded plan
version is **retained, and remains valid for validating an already-dispatched
attempt**, for at least `follower_confirmation_timeout_seconds` plus the
bounded relay-handling window — the maximum time the design allows between a
leader's dispatch and the follower's admission decision on it.

Without that retention, an attempt dispatched moments before an hourly refresh
boundary would be guaranteed to fail validation on arrival, turning a routine
refresh into a predictable entry failure. With it, an in-flight attempt
validates against the named immutable version it was actually sized from, while
every new candidate is evaluated only against the current version.

Retention applies to validation of an existing attempt only. A superseded plan
version never becomes eligible for a new candidate, is never re-selected, and
expires once the retention window passes with no attempt referencing it.

### Per-leg loss policy

Daily loss accounting is local to each Worker and resets at the
`America/New_York` calendar-day boundary. Only realized P&L from closed
strategy-owned legs is accumulated; floating P&L is not included in the daily
total.

Before admitting a new leg, each Worker computes:

```text
daily_loss_limit_usd = strategy_budget_usd * daily_loss_fraction
remaining_daily_loss_usd =
    max(0, daily_loss_limit_usd - accumulated_realized_loss_usd)

allowed_leg_loss_usd = min(
    strategy_budget_usd * trade_loss_fraction,
    maximum_loss_per_trade_usd,
    remaining_daily_loss_usd,
)
```

Each Worker uses its own risk block for this computation: the leader uses
`leader_risk`, the follower uses the `follower_risk` block it authored and the
leader copied verbatim.

The current value of `allowed_leg_loss_usd` is what each Worker publishes in
its versioned remaining-allowed-leg-loss summary. The leader may assign a leg
no more loss than its owner's last-published allowance, and the follower
re-checks the assigned loss against its **current local** allowance at receipt
and rejects the attempt if the assignment exceeds it.

No entry is allowed when `allowed_leg_loss_usd` is zero or when valid
broker-native protection cannot be constructed within that amount. Each leg's
take-profit target is 1:1 with its allowed stop-loss amount in USD.

`maximum_loss_per_trade_usd` defaults to `40`, the same number as legacy
`--emergency-stop-loss-usd`, but it is a **deliberate reinterpretation, not the
same semantic**. In legacy `realtime_arbitrage`, that value drives an
emergency stop-out check. Here it is a **hard per-trade loss cap**: one of
three ceilings that bound `allowed_leg_loss_usd` before any order is sent, and
therefore an admission input rather than an emergency reaction.

The design's rough emergency protection, attached to every initial order,
targets the **current computed `allowed_leg_loss_usd`** for that leg — not a
fixed USD constant and not `maximum_loss_per_trade_usd` directly. Precise
protection then re-targets the same `allowed_leg_loss_usd` from the actual
fill.

### Quote evidence and edge selection

Both Workers continuously publish versioned executable bid/ask evidence for
eligible products through the opaque controller relay. Evidence contains
Worker identity, derived product identity, local symbol, bid, ask, broker tick
time, local receive time, recovery epoch, and monotonic sequence.

`quote_max_age_seconds` rejects a local or peer quote that is too old.
`quote_max_skew_seconds` remains an explicit policy parameter and rejects a
leader/follower quote pair whose calibrated broker times are too far apart.
Out-of-order sequences, previous recovery epochs, symbol mismatches, stale
quotes, and excessive skew are never candidates.

Quote age, sequence, recovery-epoch, and skew checks are leader-side candidate
admission rules. The follower does not repeat them after receiving an attempt
and does not recalculate whether the edge still exists.

The leader evaluates both mirror directions for every eligible,
non-quarantined discovered product. A candidate must clear `entry_edge_points`
against that product's canonical execution point. Qualifying candidates are
ordered by:

1. conservative expected edge USD descending;
2. normalized edge points descending;
3. derived product identity ascending; and
4. direction ascending.

### Immediate entry protocol

When the leader selects the best candidate, it:

1. durably persists one immutable attempt and its local prepared broker effect;
2. starts its local monotonic follower-confirmation timer at the decision;
3. submits its own market entry with cached rough emergency SL/TP; and
4. concurrently sends the immutable attempt to the follower.

There is no arm phase, commit phase, common future execution time, execution
window, Pair Lease, or lease TTL.

The attempt contains the policy hash, attempt ID, `route_id`,
`universe_generation`, derived product identity, local symbols, assigned
directions, common lots, both quote revisions, sizing plan versions, the
remaining-allowed-leg-loss summary versions used, allowed loss for each leg,
rough emergency protection, decision time, and confirmation-timeout duration.
Decision time is evidence for audit and recovery, not a cross-machine
admission deadline. Its relay envelope carries `route_id`.

With verbose Worker logging enabled, each created attempt emits a correlated
entry trace: selected product/direction/volume and quote age/skew; monotonic
elapsed time for durable attempt creation, relay dispatch, broker send and
response, position observation, pair confirmation, precise protection, and
activation; and the broker position's reported entry-time fields with the
local observation timestamp. The trace is diagnostic only: it neither
changes broker effects nor uses wall-clock comparison across Workers.

The follower rejects an attempt without broker send when:

- it did not come from the authenticated leader on its durable route;
- its `route_id` is not the follower's current `route_id`, or the route is
  `UNPAIRING`;
- its `universe_generation` is not the follower's current
  `universe_generation`;
- its policy hash is not the exact locally accepted hash, or the follower has
  not accepted that policy (including a live policy refused under
  `allow_live = false`);
- its attempt ID is duplicated with changed content;
- the follower is not broker-verified ready and empty;
- the selected product identity is not locally eligible, or its named
  sizing-plan version is neither the current version nor a retained superseded
  version still inside its retention window;
- the common lots differ from the pair plan of the named plan version;
- the loss assigned to the follower's own leg exceeds the follower's **current
  local** `allowed_leg_loss_usd`;
- the product is quarantined; or
- valid rough emergency SL/TP cannot be attached to the initial order.

These are non-market safety checks only. The follower does not inspect current
quotes, quote age, quote skew, or edge before entry. The remaining-allowance
re-check is local risk arithmetic, not market evaluation, and adds no
market-data dependency.

After durable deduplication and effect preparation, the follower starts its
own local monotonic confirmation timer and immediately submits its protected
market order. It never waits for a second leader message before entry.
Same-ID/same-content delivery returns the durable outcome and never sends
twice.

There is deliberately no attempt-delivery-age check and no cross-machine
deadline comparison. A delayed attempt may therefore arrive and create a
follower leg after the leader's timer has expired and the leader has already
contained its leg. That follower exposure remains bounded by broker-native
rough protection and the follower's receive-relative five-second timer.

Each Worker reports its durable send state, broker receipt, and exact
broker-observed position evidence. A successful order receipt alone is not
position proof.

### Confirmation, orphan containment, and precise protection

Before its local five-second timer expires, the leader must obtain exact
broker-position evidence for both its own leg and the follower leg. Exact
evidence includes attempt ID, ticket, symbol, side, volume, fill price, and
attached rough SL/TP. Protection equality is equality at the immutable
attempt plan's executable tick: MT5 binary-float serialization of that same
requested tick is equivalent, but a different tick, an off-grid expected
price, or unavailable attempt-bound plan is inconsistent. An order receipt
alone is insufficient for either leg.

If both exact positions are confirmed in time, the leader emits
`pair_entry_confirmed`. Only then does each Worker compute precise SL/TP from:

- its own broker-observed fill price;
- its current point, tick size, profit/loss tick values, stop level, and freeze
  level;
- the attempt's immutable volume; and
- that Worker's immutable `allowed_leg_loss_usd`.

Precise SL and TP each target the same USD amount, producing a 1:1
risk-to-reward ratio. Each modification is a separate journaled broker effect.
The pair becomes `ACTIVE` only after both exact tickets are broker-observed
with the expected precise protection.

The follower accepts `pair_entry_confirmed` only for the exact unresolved
attempt and only before its own receive-relative local timer expires. If that
message does not arrive in time, the follower immediately sets desired state
to `EMPTY`, cancels attempt-owned pending orders, closes its exact
attempt-owned position, and requires fresh broker-verified empty facts. A
follower restart with an unresolved entered attempt contains immediately
rather than attempting to reconstruct elapsed monotonic time.

If either entry rejects, is unknown after send, cannot be observed, has
inconsistent evidence, misses the leader's confirmation timer, or fails
precise protection, desired state becomes `EMPTY`. The leader emits a
best-effort `pair_entry_failed` notification in addition to starting its own
containment. Receipt of that notification makes the follower contain
immediately rather than waiting for its local timer. Safety never depends on
the failure notification arriving.

Each Worker contains only from durable local effects and exact broker facts:
it immediately cancels attempt-owned pending orders, closes exact
attempt-owned positions, and requires fresh broker-verified empty facts.

### Durable recovery

Each Worker durably owns only its local broker effects and its copy of pair
state, including its durable route and `route_id`, its assigned role, the
current `universe_generation`, the frozen discovered universe, the **full
canonical policy content** and its hash, and its own local attempt/effect
state version. `prepared` is written before every MT5 write and `send_started`
immediately before the irreversible call. An effect that may have crossed
`send_started` is resolved from broker observation and is never blindly
resent.

Leader or follower restart recovers its durable route and unresolved attempts
before restoring entry readiness. A restarted Worker resumes its recorded
role; it never re-declares a role or re-enters follower selection while a
route exists, and it never re-requests the canonical policy in order to learn
what it agreed to. The follower replays durable outcomes, not broker writes.

On reconnect, each Worker reconciles its durable route copy against the
controller's authoritative route record. Agreement restores relay and, subject
to the rest of the admission chain, entry readiness. Disagreement — different
`route_id`, different assigned role, `UNPAIRING`, or no route at all —
immediately removes entry readiness and raises a metadata reconciliation. It
never causes the Worker to discard, close, hide, or infer local exposure, and
it never causes it to adopt an unassigned role.

Relay loss or process failure during an unresolved attempt causes local
protection and reduction according to available broker evidence; neither
Worker infers `EMPTY` from a peer acknowledgement or successful close receipt.
If a Worker reaches `NEEDS_HUMAN` solely because its bounded terminal-proof
recovery window expired, it remains fail-closed but continues bounded
terminal-proof probes. It may clear that specific stop only after the same
attempt has both locally broker-verified `EMPTY` evidence and an authenticated
peer terminal proof; it then terminalizes the attempt as `EMPTY`. No other
`NEEDS_HUMAN` reason is automatically cleared, and neither silence nor a
successful relay acknowledgement is terminal proof.

Timed exit, New York cutoff, risk breach, shutdown, integrity divergence,
entry failure, and protection failure all use the same desired-`EMPTY`
convergence path.

An interactive Worker interrupt requests this same shutdown convergence rather
than immediately terminating the process. The Worker keeps its authenticated
relay, broker observation, and Pair Execution Cell pump active until its
locally owned leg and the peer's terminal proof make the attempt terminal;
only then does it close the session and MT5 connection. A second interrupt may
force immediate process exit. This shutdown path acts only on the attempt's
durably owned orders and positions, never account exposure it cannot
attribute.

## User Stories

1. As a strategy operator, I want only the leader to detect edges and originate attempts, so that two Workers cannot create competing trades.
2. As a strategy operator, I want the leader's own protected order and the follower attempt dispatched without an arm/commit round trip, so that edge-to-send latency is minimized.
3. As a strategy operator, I want every initial market order to include cached rough SL/TP, so that immediate entry never intentionally creates a naked position.
4. As a strategy operator, I want the follower to enter without rechecking quotes or edge, so that it adds no avoidable market-data latency after receiving the attempt.
5. As a strategy operator, I want the leader to contain its leg unless both exact positions are proved within five seconds of its decision.
6. As a strategy operator, I want the follower to contain its own leg unless pair confirmation arrives within five seconds of receiving the attempt.
7. As a strategy operator, I want precise protection applied only after both exact positions are confirmed, so that final protection uses real fills without delaying initial entry.
8. As a strategy operator, I want one immutable attempt ID and policy hash across both legs, so that delivery, effects, evidence, and recovery remain correlated.
9. As a strategy operator, I want duplicate attempts deduplicated durably, so that relay duplication never creates a second broker send.
10. As a Worker operator, I want to start a Worker with no pair-cell role and have it become an available follower, so that the common deployment needs no flags and no configuration file.
11. As a Worker operator, I want to start a Worker as a leader and be shown the currently connected, available, unpaired followers, so that I can pair interactively without touching the controller.
12. As a Worker operator, I want an optional `--follower-worker-id`, so that an unattended leader can pair itself without a prompt.
13. As a Worker operator, I want the follower to accept automatically only when it is broker-verified empty, has nothing unresolved, is entry-ready, and is still unpaired, so that pairing can be autonomous without being unsafe.
14. As a controller operator, I want a two-phase pairing workflow — one atomic transaction that reserves both Workers for a unique `proposal_id` under a 30-second pairing-reservation timeout, and a second atomic compare-and-swap that creates the route — so that two leaders proposing the same follower produce exactly one route and one clean conflict even though the follower's decision happens in between.
15. As a controller operator, I want refusal, reservation timeout, leader disconnect, or a failed final commit to release the reservation completely, so that a failed pairing leaves no route, no role, no execution-mode claim, and no leaked reservation.
16. As a controller operator, I want per-Worker uniqueness enforced in both transactions, so that no Worker can appear in two routes, two reservations, or a conflicting live strategy-runtime ownership at once.
17. As a controller operator, I want no `trader_id` anywhere in the Pair Execution Cell route, envelopes, ownership records, configuration, or controller APIs, so that the relay is not confused with a trading identity.
18. As a Worker operator, I want the route to survive reconnect and restart, and the durable route to win over any startup role default or flag, so that a restart cannot silently swap leader and follower.
19. As a Worker operator, I want an explicit `UNPAIRING` route state that either Worker may enter and that immediately removes entry readiness on both sides, so that no new attempt can start while an unpair is being proved.
20. As a Worker operator, I want each safe-unpair assertion bound to `route_id` plus that Worker's current local attempt/effect state version, so that any new or changed local effect invalidates it and the controller can only unpair on two genuinely fresh assertions.
21. As a controller operator, I want emergency identity revocation to stay a separate mechanism that never implies broker `EMPTY`, so that a revoked Worker is not mistaken for a flat one.
22. As a Worker operator, I want the runtime to synthesize defaults when no configuration file exists, so that a fresh Worker is deployable without hand-authored JSON.
23. As a follower operator, I want my configuration file to author only my own risk tunables and my own `allow_live` guard, so that I control my account's risk without being able to rewrite shared pair policy.
24. As a leader operator, I want my configuration file to author shared policy and my own risk, and never the follower's risk, so that there is exactly one author per value.
25. As a follower operator, I want `allow_live = false` to make me refuse a `live` policy and stay paired, safe, and idle rather than silently running shadow, so that a mode disagreement is visible instead of divergent.
26. As a strategy operator, I want the follower to author its own frozen startup balance and risk tunables in a Pairing Acceptance payload that the leader copies verbatim into `follower_risk`, so that neither side guesses the other's account.
27. As a strategy operator, I want the default `strategy_budget_usd` to be that Worker's startup broker balance in a USD account, frozen into the pairing's policy, so that sizing matches the account without manual entry and cannot drift mid-pairing.
28. As a strategy operator, I want a non-USD currency, an unreadable account, or a non-positive balance to fail closed, so that no pairing ever proceeds on a substituted budget.
29. As a strategy operator, I want a new pairing after a different startup balance to produce a new policy hash, so that budget changes are explicit and both Workers re-accept them while empty.
30. As a strategy operator, I want live to be the default execution mode while every admission rule still fails closed, so that autonomous pairing is useful without being reckless.
31. As a Worker operator, I want both Workers to persist the full canonical policy content and not only its hash, so that a restarted Worker knows exactly what it agreed to without re-requesting it.
32. As a strategy operator, I want the two Workers to discover their compatible universe from their own catalogs after the route exists, so that no pre-approved Built list has to be produced before the pair exists.
33. As a strategy operator, I want `trade_mode == 4` applied to each catalog before the intersection, exactly as `strategy/realtime_arbitrage.py` does, so that non-fully-tradable symbols never reach compatibility comparison.
34. As a strategy operator, I want compatibility to use exact symbol-name intersection and hard equality of calculation mode, contract size, minimum volume, volume step, and directions, so that unlike contracts can never be paired.
35. As a strategy operator, I want the canonical execution point to be the larger of the two points and the common volume maximum to be the smaller, so that the threshold and the size are always the conservative one.
36. As a strategy operator, I want discovery to fail closed when either catalog is unavailable or no compatible symbol exists, so that an empty universe never degrades into a permissive one.
37. As a strategy operator, I want the discovered universe frozen for its `universe_generation`, so that a symbol appearing later in a broker catalog cannot silently become tradable mid-life.
38. As a Worker operator, I want an explicit rediscovery while both accounts are empty to change `universe_generation` on the same route without replacing `route_id`, so that refreshing the universe is not re-pairing.
39. As a strategy operator, I want volume calculated from each Worker's strategy budget and current product margin, so that one fixed volume is not applied across unlike products.
40. As a strategy operator, I want sizing and protection plans built from a current local quote and MT5 margin calculation after discovery and before any candidate is processed, and refreshed hourly, so that signal-time entry requires no margin RPC and no candidate is ever evaluated without a plan.
41. As a strategy operator, I want superseded plan versions retained for at least the confirmation timeout plus the bounded relay-handling window, so that an attempt in flight across a refresh boundary validates against its named immutable version instead of being guaranteed to fail.
42. As a strategy operator, I want one failed product refresh to suspend only that product until the next refresh, so that other products remain eligible.
43. As a strategy operator, I want each account to enforce its own 3% New York daily realized-loss limit, so that one account cannot conceal the other's budget use.
44. As a strategy operator, I want each leg's loss capped by the smallest of 2% of strategy budget, `maximum_loss_per_trade_usd`, and remaining daily allowance, understanding that the `40` default is a deliberate hard-cap reinterpretation of the legacy emergency-stop value rather than the same semantic.
45. As a strategy operator, I want each Worker to publish a continuous versioned remaining-allowed-leg-loss summary before any signal, so that the leader never assigns a leg more loss than its owner can accept.
46. As a follower operator, I want to reject an attempt whose assigned loss exceeds my current local remaining allowance, so that a stale summary can never spend more of my daily budget than I have left.
47. As a strategy operator, I want each leg's precise TP and SL to use equal USD amounts, so that the protection target is 1:1.
48. As a strategy operator, I want stale or excessively skewed quote pairs rejected by the leader, so that delayed market evidence does not become a false edge.
49. As a strategy operator, I want both mirror directions and all eligible products ranked deterministically by conservative expected edge USD.
50. As a strategy operator, I want MT5 `10021 No prices` to durably quarantine only the affected product identity while other products continue.
51. As a strategy operator, I want every asymmetric or uncertain outcome to converge through exact-ticket broker-verified `EMPTY`.
52. As a Worker operator, I want policy changes accepted only while both accounts are empty, so that an active lifecycle cannot change rules mid-trade.
53. As a Worker operator, I want local recovery to remain authoritative when the peer or relay is unavailable.
54. As a controller operator, I want the controller to authenticate Workers, arbitrate exclusive route ownership, and relay opaque messages only, so that it cannot become a trading coordinator.
55. As a controller operator, I want the controller's route record to be authoritative for relay permission and assigned role while each Worker's durable copy is only recovery evidence, so that role and relay never depend on a Worker's own claim.
56. As a Worker operator, I want a route-metadata disagreement to remove entry readiness and raise reconciliation without ever discarding or inferring my local exposure, so that a metadata bug can never become a position bug.
57. As a controller operator, I want `pair_execution_cell` execution-mode exclusivity claimed for both Workers inside the pairing reservation and confirmed in the final commit, using a fixed owner kind with no `trader_id`, so that a pair can never have two live lifecycle owners.
58. As a Worker operator, I want conceptual operator surfaces for safe unpair and rediscovery on either role, so that neither operation needs a controller admin action.
59. As a Worker operator, I want a non-TTY leader without `--follower-worker-id` to stay authenticated and unpaired with an actionable diagnostic rather than blocking on input, so that unattended deployment never hangs and never pairs by accident.
60. As a maintainer, I want one Pair Execution Cell module and one lifecycle interface shared by leader and follower roles.
61. As a maintainer, I want deterministic clock, relay, journal, and MT5 adapters, so that timeout, pairing, and crash races can be reproduced without live brokers.
62. As a strategy operator, I want shadow and controlled test-account rollout before production sizing is enabled.

## Implementation Decisions

- The Pair Execution Cell remains one deep module running on both Workers.
  Its external interface is pairing, policy acceptance, typed event handling,
  and restart recovery. Callers do not coordinate entry phases or broker
  effects.
- Pairing is Worker-initiated. There is no administrator-created route. A
  Worker declares a desired role on first unpaired connection; an omitted role
  means available follower.
- The leader lists available followers from the controller and selects one,
  interactively by default and non-interactively with an explicit follower
  Worker ID. A non-TTY leader without that flag stays authenticated and
  unpaired with an actionable diagnostic; it never blocks on input, never
  selects implicitly, and never exits. An interactive leader shown an empty
  list behaves the same way.
- Follower acceptance is automatic and purely local-evidence based:
  broker-verified empty, nothing unresolved, entry-ready, readable positive
  USD balance, still unpaired. There is no human approval step and no partial
  acceptance.
- Pairing is a **two-phase control workflow**, not one compare-and-swap,
  because the follower's decision happens between the two steps. Transaction
  one atomically reserves **both** Workers for a unique `proposal_id` with a
  30-second pairing-reservation timeout; the controller then forwards the
  proposal; the follower accepts or refuses; transaction two CAS-creates the
  route only if the reservation is still valid and both Workers remain
  eligible. Refusal, timeout, either disconnect, or a failed final commit
  releases the reservation entirely.
- The 30-second pairing-reservation timeout is pairing control metadata. It is
  not a trading lease, not a route TTL, and it never fences an effect, an
  attempt, or an established route.
- Both transactions enforce per-Worker uniqueness: neither Worker may appear in
  another route, another live reservation, or a conflicting live
  strategy-runtime ownership.
- `pair_execution_cell` execution-mode exclusivity is claimed for **both**
  Workers inside the reservation transaction and confirmed inside the final
  CAS. The Pair Cell claims it with the fixed owner kind
  `pair_execution_cell` and no `trader_id`; the legacy Strategy Runtime keeps
  its Trader identity on its own claims.
- The Pair Execution Cell route, its envelopes, its ownership records, its
  configuration, and its controller APIs carry no `trader_id`. Durable route
  identity is the ordered leader/follower Worker pair plus a
  controller-generated globally unique `route_id`, which is never reused. This
  removal is scoped to the Pair Execution Cell; the legacy Strategy Runtime
  and Trader RPC surfaces keep their own Trader identity unchanged.
- `route_id` is not a lease epoch: no TTL, no renewal, no fencing, no trading
  authorization. The Worker-local `recovery_epoch` remains separate market
  evidence about a Worker's own broker session and is never compared with
  `route_id`.
- Discovery identity is separate from route identity. Each discovery result
  carries a `universe_generation`, unique within its route. Envelopes carry
  `route_id`; policy, attempt, product, sizing-plan, and quarantine state
  carry `universe_generation` where product identity applies. The controller
  validates `route_id` and role only, never universe content.
- Roles come from the durable route. They are not leased and do not expire on
  a trading TTL. A durable route overrides startup role defaults and flags on
  every reconnect and restart.
- The controller's route record is authoritative for relay permission and
  assigned role. A Worker's durable route copy is recovery evidence only. A
  disagreement removes entry readiness and triggers metadata reconciliation;
  it never discards, closes, or infers local exposure and never lets a Worker
  adopt an unassigned role.
- Safe unpair runs through an explicit `UNPAIRING` route state that either
  authenticated Worker may enter by command. Entering it immediately removes
  entry readiness on both sides and forbids starting any new attempt, while
  never blocking protection or containment of existing exposure.
- Each safe-unpair assertion binds to `route_id` plus that Worker's current
  local attempt/effect state version. Any new or changed local effect advances
  that version and invalidates the assertion. The controller removes the route
  only on two fresh, currently-bound assertions and never derives emptiness or
  terminality itself. Identity revocation is a separate emergency mechanism
  and never implies `EMPTY`.
- No configuration file is required. When absent, the runtime synthesizes the
  documented defaults. Configuration authority is **role-specific**: a
  follower file may set only its own per-Worker risk tunables
  (`maximum_margin_fraction`, `daily_loss_fraction`, `trade_loss_fraction`,
  `maximum_loss_per_trade_usd`) and its own `allow_live` guard, and may not set
  shared edge, quote, timeout, flatten, or `mode` values; a leader file may set
  shared policy and its own risk, and never the follower's. A route,
  `route_id`, `trader_id`, `built_products`, or `eligible_products` in any file
  is a startup error, as is a file whose keys exceed the authority of the role
  the Worker actually receives.
- `mode` is pair-level and leader-authored, default `live`. A follower with
  `allow_live = false` fails closed against a `live` policy: it refuses with an
  explicit reason and stays paired, safe, and idle. It never rewrites policy,
  never silently runs shadow against a live leader, and the leader never treats
  the refusal as acceptance. Resolution is an operator action while both
  accounts are empty.
- Default mode is `live` by explicit operator decision; all fail-closed
  admission rules are unchanged. `shadow` remains available for rollout.
- The follower authors its own frozen startup balance and its own four risk
  tunables and sends them to the leader in a **Pairing Acceptance payload**
  through the opaque relay. The leader copies them **verbatim** into the
  canonical policy's `follower_risk` block and may not recompute, clamp, or
  substitute them; an unusable payload is a fail-closed refusal to publish
  policy.
- Default `strategy_budget_usd` is that Worker's startup broker **balance**,
  deliberately chosen over legacy startup **equity** because it excludes
  floating P&L. It requires a USD account currency and a strictly positive
  balance and otherwise fails closed. It is frozen into the pairing's canonical
  policy and covered by its hash; a later pairing with a different startup
  balance yields a different hash.
- There is no additional margin headroom beyond `maximum_margin_fraction`;
  legacy `realtime_arbitrage`'s extra 20% headroom is deliberately not carried
  forward.
- `maximum_loss_per_trade_usd = 40` reuses the legacy emergency-stop USD number
  as a **hard per-trade admission cap**, which is a deliberate reinterpretation
  rather than the legacy emergency-stop semantic. Rough emergency protection
  targets the current computed `allowed_leg_loss_usd`, not a fixed constant.
- Each Worker continuously publishes a versioned remaining-allowed-leg-loss
  summary before signals. The leader must hold a current peer summary to create
  an attempt and may assign no leg more than its owner's published allowance.
  The follower independently rejects an attempt whose assigned loss exceeds its
  current local allowance.
- Both Workers persist the **full canonical policy content**, not only its
  hash. The hash is an integrity check over durably held content, never the
  sole durable record.
- Runtime polling, refresh, and integrity intervals stay implementation
  details and are excluded from the policy hash.
- The eligible universe comes from Initial Compatible Product Discovery
  between the two Workers over the opaque relay, immediately after route
  establishment and before quote processing. The controller never parses
  catalogs.
- Compatibility is the legacy `realtime_arbitrage` rule set verbatim:
  `trade_mode == 4` (full trading) applied to each catalog **before** the
  intersection, exact symbol-name intersection, hard equality of
  `trade_calc_mode`, `contract_size`, `volume_min`, `volume_step`,
  `allowed_directions`, both directions required, shared FOK preferred over
  shared IOC, otherwise excluded. Currency, digits, point, and tick size
  equality are deliberately not required.
- Canonical execution point is the maximum of both points; common
  `volume_max` is the minimum. Product identity is derived deterministically
  from the exact symbol name plus the canonical compatibility summary hash,
  and it keys quarantine.
- Discovery fails closed on an unavailable or invalid catalog and on an empty
  intersection. The discovered universe is frozen for its
  `universe_generation`; hourly refresh revalidates and may suspend, never
  silently adds, and never changes `universe_generation`. Explicit rediscovery
  while both accounts are empty installs a new `universe_generation` on the
  same route and never produces a new `route_id`.
- FX v1 uses 1:1 lots because discovery admits only equal contract size,
  minimum volume, and volume step.
- The leader supplies the canonical strategy policy; the follower verifies its
  own block byte-for-byte, verifies the hash, and persists the full content.
  The controller never parses that policy.
- Each Worker computes direction-specific margin capacity locally. The common
  pair lots are the lower valid capacity and are frozen into the attempt.
- Plan construction order is: route, discovery, policy acceptance, quote
  collection, then initial sizing and protection plans built from a current
  local quote and a current MT5 margin calculation, and only then candidate
  evaluation. No candidate is processed before valid plans exist. Plans then
  refresh hourly. No signal-time remote read is added.
- Each superseded plan version is retained and remains valid for validating an
  already-dispatched attempt for at least
  `follower_confirmation_timeout_seconds` plus the bounded relay-handling
  window, so a refresh boundary does not guarantee rejection of an in-flight
  attempt. A retained version is never eligible for a new candidate.
- Quote age and calibrated cross-Worker quote skew remain independently
  configurable leader-side candidate-admission checks. The follower performs
  neither check after attempt receipt.
- `follower_confirmation_timeout_seconds` is the only entry-protocol timeout
  duration and defaults to `5`. The leader starts one local monotonic timer at
  decision; the follower starts a separate local monotonic timer at receipt.
  There is no shared absolute deadline or attempt-delivery-age limit. The
  30-second pairing-reservation timeout is control-plane metadata and is not an
  entry-protocol timeout.
- Leader local entry and follower attempt delivery begin from the same durable
  decision without an arm/commit handshake.
- Initial protection uses cached plan data and the decision quote; precise
  protection uses actual fills and fresh local broker constraints after pair
  confirmation.
- Daily realized loss is tracked per Worker on the America/New_York calendar
  day. Floating P&L is excluded from the daily accumulator.
- Runtime state persists the durable route and `route_id`, role,
  `universe_generation`, frozen discovered universe, full canonical policy
  content and hash, sizing-plan versions including retained superseded ones,
  the local attempt/effect state version, attempt, timer start evidence, quote
  evidence, local effects, peer outcomes, exact tickets, protection, desired
  state, quarantine, and transition history.
- Safe unpair and rediscovery both have conceptual Worker-side operator
  surfaces available on either role; neither requires a controller admin
  action.
- The controller may record ordinary connection, pairing, and delivery
  diagnostics, but it does not durably replay execution messages or store an
  authoritative lifecycle/audit projection. Attempt evidence belongs to the
  Workers.
- Existing account-local effect-journal, serialized MT5 access, no-blind-retry,
  exact-ticket containment, and broker-verified-empty rules remain mandatory.

## Parameters

### Startup surface

- `--pair-cell-role {leader,follower}` — desired role on first unpaired
  connection; omitted means `follower` (available follower).
- `--follower-worker-id <worker_id>` — optional, leader only, non-interactive
  follower selection for unattended deployment. Omitted means the leader gets
  the interactive list of currently connected, available, unpaired followers;
  omitted on a non-TTY process means the leader stays authenticated and
  unpaired with an actionable diagnostic.
- `--pair-cell-unpair` — conceptual operator surface requesting a safe unpair
  of this Worker's current route; available on either role.
- `--pair-cell-rediscover` — conceptual operator surface requesting an explicit
  rediscovery on this Worker's current route; available on either role.
- A configuration file is optional and its authority is role-specific.

### Strategy policy

Every value below has a default, so a pairing can be formed with no
configuration file at all. The **Authored by** column names the only role
permitted to set that value; anything else is a startup configuration error.

| Parameter | Default | Authored by |
| --- | --- | --- |
| `mode` | `live` | leader (pair-level) |
| `strategy_budget_usd` (per Worker) | that Worker's startup broker account **balance**, USD account and strictly positive required, frozen into the pairing | each Worker for itself; the follower's arrives verbatim in the Pairing Acceptance payload |
| `maximum_margin_fraction` (per Worker) | `0.10` | each Worker for itself |
| `daily_loss_fraction` (per Worker) | `0.03` | each Worker for itself |
| `trade_loss_fraction` (per Worker) | `0.02` | each Worker for itself |
| `maximum_loss_per_trade_usd` (per Worker) | `40` (hard per-trade cap; see note) | each Worker for itself |
| `entry_edge_points` | `4` | leader |
| `quote_max_age_seconds` | `1` | leader |
| `quote_max_skew_seconds` | `1` (Pair Cell parameter; legacy realtime strategy has no equivalent) | leader |
| `follower_confirmation_timeout_seconds` | `5` | leader |
| `flatten_at_ny` | `16:00` | leader |
| `maximum_holding_seconds` | unset | leader |

Defaults other than `quote_max_skew_seconds`, `mode`, and
`strategy_budget_usd` match the existing `strategy/realtime_arbitrage.py`
operating values, with one explicit reinterpretation:
`maximum_loss_per_trade_usd = 40` reuses legacy `--emergency-stop-loss-usd`'s
number as a hard per-trade admission cap rather than as an emergency stop-out
trigger. Rough emergency protection targets the current computed
`allowed_leg_loss_usd`.

### Local, non-policy settings

These are Worker-local and are not part of the canonical policy or its hash:

| Setting | Default | Authored by |
| --- | --- | --- |
| `allow_live` | `true` | each Worker for itself; meaningful on the follower as a fail-closed guard against a `live` policy |

### Control-plane parameters

These belong to the controller's pairing workflow, not to strategy policy, and
are never trading leases or TTLs:

| Parameter | Default |
| --- | --- |
| pairing-reservation timeout | `30` seconds |

### Removed from the interface

- `trader_id` on the Pair Execution Cell route, envelopes, ownership records,
  configuration, and controller APIs;
- administrator-created pair routes and their controller endpoints;
- **route generation** as an overloaded identifier, replaced by a
  controller-generated globally unique `route_id` for the pairing and a
  separate `universe_generation` for each discovery result;
- `built_products` configuration and the "active built products only"
  invariant;
- `eligible_products` policy filters;
- required Pair Execution Cell configuration files;
- follower authority over shared edge, quote, timeout, flatten, or `mode`
  values;
- `leader_volume` and `follower_volume`;
- controller-issued Pair Contracts;
- Pair Leases and `ttl_seconds`;
- `arm_timeout_seconds`;
- `commit_lead_seconds`;
- `execution_expiry_seconds`; and
- the arm/commit protocol itself.

Runtime polling, refresh, and integrity intervals remain implementation
details of the Worker runtime rather than strategy policy.

## Testing Decisions

- Pairing tests prove a Worker started with no role becomes an available
  follower, a leader without `--follower-worker-id` receives the list of
  currently connected, available, unpaired followers and pairs from an
  interactive selection, and a leader with `--follower-worker-id` pairs
  without any prompt.
- Pairing race tests prove that two leaders selecting the same follower
  produce exactly one durable route and one deterministic conflict for the
  loser at the reservation transaction, that the loser can immediately re-list
  and pair with a different follower without waiting out the winner's
  reservation, and that no reservation leaks when a proposal fails.
- Two-phase reservation tests prove the reservation transaction reserves both
  Workers atomically under a unique `proposal_id`, that the final
  route-creation compare-and-swap succeeds only while that reservation is
  valid and both Workers remain eligible, and that follower refusal, the
  30-second pairing-reservation timeout, a leader disconnect, a follower
  disconnect, and a failed final commit each release the reservation with no
  route, no role assignment, no retained execution-mode claim, and no durable
  local pairing state.
- Uniqueness tests prove neither Worker can be reserved or routed while it
  appears in another route, another live reservation, or a conflicting live
  strategy-runtime ownership, and that the rejection is deterministic in both
  transactions.
- Execution-mode tests prove `pair_execution_cell` exclusivity is claimed for
  both Workers in the reservation and confirmed in the final commit, that the
  Pair Cell claim carries the fixed owner kind and no `trader_id`, that a
  pairing is rejected while the legacy Strategy Runtime holds either Worker's
  pair live, and that the legacy Strategy Runtime's own Trader identity is
  untouched.
- Timeout-classification tests prove the 30-second pairing-reservation timeout
  never fences, expires, or invalidates an established route, an attempt, or a
  broker effect.
- Pairing tests prove a follower that disconnects between listing and proposal
  causes a clean proposal failure with no route, no reservation, and no
  partially accepted state on either side.
- Follower-safety tests prove refusal when the follower is not broker-verified
  empty, has an unresolved attempt or effect, is not entry-ready (unusable MT5
  handle, unreadable account, missing clock calibration), cannot read a
  positive USD balance, or is already paired, and prove that a refusal never
  becomes an implied acceptance and always releases the reservation.
- Pairing Acceptance tests prove the payload carries the follower's frozen
  startup balance and its four authored risk tunables, that the leader copies
  them byte-for-byte into `follower_risk`, that a leader attempting to
  recompute, clamp, or substitute them is a defect, and that a missing,
  malformed, non-USD, or non-positive payload makes the leader fail closed
  without publishing policy.
- Non-TTY leader tests prove a leader on a non-TTY process without
  `--follower-worker-id` stays authenticated and unpaired, emits an actionable
  diagnostic naming the missing selection and the flag, does not block on
  input, does not exit, and does not select a follower implicitly; and that an
  interactive leader shown an empty available-follower list behaves the same
  way and can re-list later.
- Durability tests prove the route survives reconnect and restart, that the
  durable route overrides a contradicting `--pair-cell-role` and any default,
  and that a reconnecting Worker resumes its assigned role without re-entering
  selection.
- Route-identity tests prove every new pairing receives a new, never-reused
  `route_id`, that re-pairing the same two Workers after a safe unpair produces
  a different `route_id`, that envelopes carry `route_id`, that the controller
  validates `route_id` and role but never universe content, and that
  `route_id` is never treated as a lease epoch or compared with
  `recovery_epoch`.
- Authority tests prove the controller's route record governs relay permission
  and assigned role, that a Worker whose durable copy disagrees on `route_id`,
  role, or route existence immediately loses entry readiness and raises
  metadata reconciliation, and that such a Worker never discards, closes, or
  infers local exposure and never adopts an unassigned role.
- Unpair tests prove that either authenticated Worker can enter `UNPAIRING` by
  command for the current `route_id`, that entering it immediately removes
  entry readiness on both sides, that no new attempt may start while
  `UNPAIRING`, and that containment and protection of existing exposure remain
  fully available.
- Unpair assertion tests prove each assertion binds to `route_id` plus the
  asserting Worker's current local attempt/effect state version, that any new
  or changed local effect advances that version and invalidates the
  outstanding assertion, that one-sided assertions and assertions bound to a
  superseded state version or a different `route_id` do not unpair, that the
  controller removes the route only on two fresh currently-bound assertions,
  and that the controller derives neither emptiness nor terminality itself.
- Unpair-cancel tests prove an explicit cancel from either Worker returns the
  route to normal operation subject to the ordinary admission chain, and that
  route removal releases both execution-mode claims.
- Revocation tests prove identity revocation removes entry readiness without
  removing the route as a safe unpair and without implying broker `EMPTY`.
- Configuration tests prove an absent configuration file starts the cell with
  the documented synthesized defaults including `live` mode and the startup
  broker balance as `strategy_budget_usd`, and that a route, `route_id`,
  `trader_id`, `built_products`, or `eligible_products` in a file is a startup
  error.
- Configuration-authority tests prove a follower file may set only its four
  risk tunables and `allow_live`, that a follower file containing
  `entry_edge_points`, `quote_max_age_seconds`, `quote_max_skew_seconds`,
  `follower_confirmation_timeout_seconds`, `flatten_at_ny`,
  `maximum_holding_seconds`, or `mode` is a startup error, that a leader file
  may set shared policy plus its own risk but never the follower's risk or
  budget, and that a Worker receiving a role contradicting its file's authority
  fails closed rather than applying the file partially.
- Mode-disagreement tests prove `mode` is leader-authored and defaults to
  `live`, that a follower with `allow_live = false` refuses a `live` policy
  with an explicit reason and stays paired but not entry-ready, that it never
  rewrites the policy and never silently runs shadow, that the leader treats
  the refusal as peer-not-ready and originates no attempt, and that publishing
  a `shadow` policy while both accounts are empty resolves it.
- Policy-persistence tests prove both Workers persist the full canonical policy
  content and not only its hash, and that a restarted Worker validates attempts
  from persisted content without re-requesting the policy.
- Policy-hash tests prove the frozen startup balance is covered by the hash,
  that an intraday balance change does not alter it, and that a new pairing
  after a changed startup balance produces a different hash accepted only
  while both accounts are empty.
- Budget-precondition tests prove balance rather than equity is used, that a
  non-USD account currency, a zero or negative balance, an unreadable account,
  and a `None`/malformed account record each fail closed rather than
  substituting a default, and that no margin headroom beyond
  `maximum_margin_fraction` is applied.
- Remaining-allowance tests prove each Worker publishes a versioned
  remaining-allowed-leg-loss summary before signals, that the leader cannot
  create an attempt without a current peer summary, that it never assigns a leg
  more than the published allowance, that the follower rejects an attempt whose
  assigned loss exceeds its current local allowance even when the attempt
  matched the summary the leader used, and that the rejection needs no quote or
  edge evaluation.
- Discovery tests use known literal catalogs and prove: `trade_mode == 4` is
  applied to each catalog before the intersection so a fully-tradable symbol on
  one side and a non-`4` symbol of the same name on the other never intersect;
  a missing, non-integer, or boolean `trade_mode` makes the whole catalog
  invalid rather than skipping one symbol; exact symbol-name intersection with
  no suffix normalization; exclusion on `trade_calc_mode`,
  `contract_size`, `volume_min`, `volume_step`, or `allowed_directions`
  inequality; exclusion when either `LONG` or `SHORT` is missing; shared FOK
  chosen over shared IOC; exclusion when neither is shared; and admission
  despite differing base/profit currency, digits, point, and tick size.
- Discovery tests prove the canonical execution point is the maximum of both
  points, the common `volume_max` is the minimum, and product identity is
  deterministic across repeated runs of the same catalogs.
- Discovery failure tests prove fail-closed behavior when either catalog is
  unavailable or invalid and when the intersection admits no symbol: no quote
  processing, no entry, and an explicit reason.
- Frozen-universe tests prove a symbol appearing in a broker catalog after
  discovery is not admitted by an hourly refresh, that the refresh may suspend
  a selected symbol whose specification drifted, and that the hourly refresh
  never changes `universe_generation`.
- Rediscovery tests prove an explicit rediscovery is admitted only while both
  accounts are broker-verified empty with nothing unresolved, that it installs
  a new `universe_generation` on the **same** route with the same `route_id`,
  roles, and canonical policy, that a failed rediscovery leaves the previous
  `universe_generation` in place rather than installing an empty universe, and
  that a quarantine for a product whose derived identity is unchanged survives
  it.
- Sizing-timing tests prove the ordering route → discovery → policy acceptance
  → quote collection → initial sizing/protection plans → candidate evaluation;
  that the initial plans are built from a current local quote and a current MT5
  margin calculation; and that no candidate is evaluated, ranked, or selected
  while any valid positive plan is missing.
- Plan-retention tests prove each superseded plan version is retained and
  remains valid for validating an already-dispatched attempt for at least
  `follower_confirmation_timeout_seconds` plus the bounded relay-handling
  window, that an attempt dispatched immediately before an hourly refresh is
  admitted by the follower against its named immutable version instead of being
  rejected at the boundary, that a retained version is never eligible for a new
  candidate, and that it expires once the window passes unreferenced.
- Sizing tests use known literals for margin-per-lot, budgets, volume limits,
  and shared steps. They prove same-lot selection uses the lower Worker
  capacity and that hourly refresh can suspend and later restore one product.
- Risk tests cross the New York day boundary and prove realized losses,
  profits, floating P&L, 3% daily allowance, 2% trade allowance, and fixed
  maximum-loss interaction, including that `maximum_loss_per_trade_usd` acts as
  an admission cap on `allowed_leg_loss_usd` and that rough protection targets
  the computed `allowed_leg_loss_usd` rather than a fixed constant.
- Immediate-entry tests prove the leader persists before dispatch, starts its
  local protected send without waiting for the follower, and concurrently
  emits one immutable attempt.
- Follower tests cover immediate entry without quote/edge revalidation,
  duplicate delivery, changed-content reuse, policy mismatch, unaccepted
  policy under `allow_live = false`, wrong `route_id`, `UNPAIRING` route,
  wrong `universe_generation`, stale sizing plan outside the retention window,
  assigned loss exceeding the current local allowance, quarantine, invalid
  protection, and unavailable broker facts.
- Timeout tests prove the leader requires exact evidence for both positions
  within five seconds of decision, missing evidence immediately contains the
  leader leg, and best-effort `pair_entry_failed` accelerates follower
  containment without being required for safety.
- Receive-relative timeout tests prove the follower contains its own leg when
  `pair_entry_confirmed` is absent five seconds after receipt, rejects a
  confirmation that arrives after that timer, and contains immediately after
  restart with an unresolved entered attempt.
- Delayed-delivery tests explicitly prove that an old but otherwise admissible
  attempt can enter, then is bounded by rough protection and the follower's
  receive-relative timer even when the leader has already contained.
- Entry-result tests cover both filled, either rejected, both rejected,
  unknown-after-send, receipt loss, broker delta before receipt, partial fill,
  and inconsistent exact-position evidence.
- Protection tests prove rough SL/TP is attached to every initial order and
  precise 1:1 protection is modified only after both exact positions are
  confirmed.
- Quarantine tests prove keys are the derived product identity, survive
  restart, do not block other discovered products, and release only by
  authenticated operator action with no unresolved attempt.
- Recovery tests crash both roles before prepare, after prepare, at
  `send_started`, after fill, before pair confirmation, during precise
  protection, during containment, between reservation and follower acceptance,
  between acceptance and the final route-creation commit, and while
  `UNPAIRING` with an outstanding assertion.
- Controller tests prove authenticated route forwarding keyed on `route_id`
  and sending role, opaque payload handling including catalog, policy,
  Pairing Acceptance, sizing-plan, and remaining-allowance summaries,
  in-session message ordering, protocol and size enforcement, the two-phase
  reservation and final route-creation transactions, absence of any
  `trader_id` in Pair Cell routes, envelopes, ownership records, and APIs,
  absence of an administrator route-creation surface, no trading-field
  parsing, no universe-content validation, no lease dependency, and no
  execution-message replay across reconnect.
- Operator-surface tests prove safe unpair and rediscovery are both requestable
  by either role without any controller admin action, and that each fails
  closed with an explicit reason when its preconditions are unmet.
- Controlled live acceptance requires autonomous pairing of two fresh Workers,
  successful discovery, one complete two-leg entry, initial protection, pair
  confirmation within five seconds, precise protection, `ACTIVE`, timed close,
  broker-verified `EMPTY`, an explicit rediscovery on the same route, and a
  safe unpair.

## Migration and Removal

The implementation currently in the repository does **not** satisfy this
revised specification. It still requires an administrator-created,
`trader_id`-bound route, a mandatory `--pair-cell-config` file carrying that
route and a `built_products` universe, and it has no pairing, discovery, or
default-materialization behavior at all. All of the following is new work.

Pairing and route ownership:

1. Add a Worker-initiated pairing protocol: role declaration on first
   unpaired connection, an authenticated controller listing of currently
   connected, available, unpaired followers, a leader pairing proposal, and
   automatic follower acceptance gated on local safety evidence.
2. Replace `ControlLedger.authorize_pair_route` and
   `ControlLedger.revoke_pair_route` and the `/api/admin/pairs/route` and
   `/api/admin/pairs/route/revoke` endpoints with Worker-facing pairing,
   listing, and unpair surfaces; remove administrator route creation.
3. Implement the two-phase pairing workflow in the `pair_routes` ledger: a
   reservation transaction that atomically reserves both Workers under a
   unique `proposal_id` with a 30-second pairing-reservation timeout, and a
   separate final route-creation compare-and-swap that succeeds only while the
   reservation is valid and both Workers remain eligible. Release the
   reservation on refusal, timeout, either disconnect, or a failed commit, and
   give the losing leader a deterministic conflict at the reservation step.
   Enforce per-Worker uniqueness in both transactions: no Worker in another
   route, another live reservation, or a conflicting live strategy-runtime
   ownership.
4. Remove `trader_id` from the `pair_routes` schema, from
   `ControlLedger.claim_pair_execution_mode`'s Pair Cell path, from
   `abt.trader_protocol.PairRouteClaim` and `PairCellRelayEnvelope`, from
   `abt.worker.pair_cell_adapter.PairRoute`, and from every Pair Cell
   controller API request and response. Legacy Strategy Runtime and Trader RPC
   surfaces keep their Trader identity untouched. Claim
   `pair_execution_cell` execution-mode exclusivity for **both** Workers inside
   the reservation transaction and confirm it in the final commit, using the
   fixed owner kind with no `trader_id`.
5. Add a controller-generated, globally unique, never-reused `route_id` to the
   route record, the relay envelopes, and each Worker's durable state, and add
   a separate `universe_generation` to discovery results and to policy,
   attempt, product, sizing-plan, and quarantine state. Controller validation
   uses `route_id` and sending role only. Keep `recovery_epoch` untouched as
   separate market evidence.
6. Implement durable-route precedence on reconnect and restart so the stored
   role always wins over `--pair-cell-role` and its default, and make the
   controller's route record authoritative for relay permission and assigned
   role. On disagreement with a Worker's durable copy, remove entry readiness
   and raise metadata reconciliation without discarding or inferring exposure.
7. Implement safe unpair through an explicit `UNPAIRING` route state
   enterable by either authenticated Worker, which immediately removes entry
   readiness on both sides and forbids new attempts. Bind each assertion to
   `route_id` plus that Worker's local attempt/effect state version, invalidate
   assertions on any new or changed local effect, and remove the route only on
   two fresh currently-bound assertions. Add a cancel path. Keep identity
   revocation separate and prove it never implies `EMPTY`.

Policy construction:

8. Add the Pairing Acceptance payload from follower to leader through the
   opaque controller relay, carrying the follower's `proposal_id`,
   `worker_id`, frozen startup balance with account currency, and its four
   authored risk tunables. Copy them verbatim into the canonical policy's
   `follower_risk` block; fail closed on a missing, malformed, non-USD, or
   non-positive payload.
9. Add the continuous versioned remaining-allowed-leg-loss summary in both
   directions, require a current peer summary before attempt creation, freeze
   the summary versions into the attempt, and add the follower's local
   re-check that rejects an assigned loss exceeding its current allowance.
10. Persist the **full canonical policy content** on both Workers, not only its
    hash.

Configuration and defaults:

11. Make `abt.worker.pair_cell_adapter.load_pair_cell_config` absence mean
    "synthesize defaults and run", not "cell disabled". Implement
    role-specific configuration authority: a follower file may set only its
    four risk tunables and `allow_live`; a leader file may set shared policy
    and its own risk; neither may set a route, `route_id`, `trader_id`,
    `built_products`, or `eligible_products`; a role contradicting a file's
    authority fails closed.
12. Add `--pair-cell-role` and `--follower-worker-id` to `abt.worker.cli`, with
    interactive follower selection for a leader that omits the latter, and a
    non-TTY leader without it staying authenticated and unpaired with an
    actionable diagnostic instead of blocking. Add the conceptual safe-unpair
    and rediscovery operator surfaces on either role.
13. Materialize the default policy: leader-authored pair-level `live` mode with
    the follower's `allow_live = false` fail-closed refusal path, startup
    broker **balance** (USD account, strictly positive, otherwise fail closed)
    as each Worker's `strategy_budget_usd` frozen into the pairing, no extra
    margin headroom beyond `maximum_margin_fraction`, and the documented
    parameter defaults including `quote_max_skew_seconds = 1` and
    `maximum_loss_per_trade_usd = 40` as a hard per-trade admission cap.

Product universe and sizing:

14. Replace `BuiltProduct`, `ProductCatalog.active_built_products`,
    `WorkerProductCatalog`, and `StrategyPolicy.eligible_products` with
    Initial Compatible Product Discovery: versioned catalog/spec summary
    exchange over the opaque relay, the `trade_mode == 4` catalog precondition
    applied before intersection, the legacy `realtime_arbitrage` compatibility
    rules, derived product identity, canonical maximum point, common minimum
    `volume_max`, fail-closed behavior, a universe frozen per
    `universe_generation`, and explicit rediscovery that changes
    `universe_generation` on the same route.
15. Re-key quarantine and all product-scoped durable state to the derived
    product identity, recorded with its `universe_generation`.
16. Implement the plan construction order — route, discovery, policy
    acceptance, quote collection, initial sizing/protection plans from a
    current local quote and MT5 margin calculation, then candidate evaluation —
    and block all candidate processing until valid plans exist. Retain each
    superseded plan version for at least
    `follower_confirmation_timeout_seconds` plus the bounded relay-handling
    window for validating already-dispatched attempts only.

Carried forward from the previous revision and still outstanding:

17. Keep hourly per-product sizing plans, per-Worker strategy-budget and
    realized-loss accounting, immediate leader dispatch with independent
    receive-relative timers, leader-sourced canonical policy, and the removal
    of Pair Lease, Pair Contract, TTL, common-execution, and execution-expiry
    dependencies.
18. Preserve quarantine semantics, effect journals, exact-ticket containment,
    and broker-verified convergence unchanged.
19. ADR-0010 and the domain glossary already describe this specified target
    design, including the two-phase pairing workflow, `route_id`,
    `universe_generation`, the `UNPAIRING` state, role-specific configuration
    authority, and the Pairing Acceptance payload. Implementation must
    therefore **verify** them against the shipped code and update them only if
    a name changes during implementation; there is no deferred "write the ADR
    later" step.

The existing implementation must remain parked or in shadow/test mode until
these changes pass deterministic and controlled live acceptance.

## Out of Scope

- Atomic transactions across two brokers or guaranteed simultaneous fills.
- Guaranteed equal fill prices, slippage, or execution latency.
- Independent follower edge detection or follower-originated attempts.
- Administrator-created pair routes or administrator approval of an individual
  pairing.
- Trader identity binding on a Pair Execution Cell route.
- Removing `trader_id` from the legacy Strategy Runtime or Trader RPC
  surfaces.
- Pre-analyzed Built product universes, `built_products` configuration, or
  `eligible_products` filters.
- Suffix normalization, symbol aliasing, or any inexact symbol matching in
  discovery.
- Requiring base/profit currency, digits, point, or tick-size equality for
  discovery admission.
- Silent mid-life expansion of the discovered universe.
- Follower authority over shared edge, quote, timeout, flatten, or `mode`
  policy values.
- Follower rewriting of the canonical policy, including downgrading a `live`
  policy to `shadow` locally.
- Leader authorship, recomputation, clamping, or substitution of the
  follower's risk tunables or budget.
- Non-USD account currencies, or any FX conversion of a strategy budget or
  loss figure.
- Legacy `realtime_arbitrage`'s extra 20% margin headroom beyond
  `maximum_margin_fraction`.
- Legacy equity-based budget derivation.
- Treating `route_id`, `universe_generation`, or the pairing-reservation
  timeout as a lease, epoch, TTL, or fencing token.
- Reusing a `route_id` for a later pairing of the same two Workers.
- Controller validation, parsing, or storage of discovered universe content.
- Non-1:1 quantity mappings in FX v1.
- Signal-time remote margin, symbol, or protection reads.
- Candidate evaluation before valid sizing and protection plans exist.
- Indefinite retention of superseded sizing-plan versions, or their reuse for
  new candidates.
- Intentional naked entry without broker-native rough protection.
- Blind retry of any effect that may have crossed MT5 `order_send`.
- Controller calculation of volume, edge, protection, compensation, or pair
  lifecycle state.
- Controller inference of broker emptiness or attempt terminality.
- Controller-issued trading leases, execution commands, or emergency flatten.
- Discarding, closing, or inferring local exposure because of a route-metadata
  disagreement.
- Direct Worker-to-Worker network connections that bypass authenticated relay.
- Automatic role transfer while an attempt or exposure is unresolved.
- Blocking an unattended leader on interactive input.
- More than two legs or multiple strategies concurrently sharing one account.
- Production rollout before revised shadow and controlled test acceptance.

## Further Notes

- Worker-initiated pairing trades a pre-approved, administrator-authorized
  universe for autonomous startup. The controller still arbitrates exclusive
  route ownership, so autonomy never means two leaders sharing one follower.
- Reservation and route creation cannot be one compare-and-swap because the
  follower's decision sits between them, and that decision is the follower's
  to make. Two transactions with an explicit, short-lived reservation is the
  honest shape of that workflow. The 30-second timeout exists so a leader that
  vanishes mid-proposal cannot strand a follower; it is control-plane
  bookkeeping and touches nothing about trading.
- The follower's automatic acceptance is safe because every condition it
  checks is local, current, and broker-verified. It is never a policy
  judgement and never a human approval. The Pairing Acceptance payload keeps
  it from also being a blank cheque: the follower states its own budget and
  its own risk limits, and the leader may only transcribe them.
- Splitting authorship this way removes the last place where one Worker could
  quietly size another's account. Shared policy has exactly one author, and
  each account's risk has exactly one author, which is that account's own
  Worker.
- `allow_live` is deliberately a refusal, not an override. A follower that
  disagrees about live trading stops, visibly, rather than running a different
  policy from its leader. Divergence is worse than idleness.
- Live by default is an intentional operator decision, not an accident of
  defaults. It changes what a correctly admitted pair does, never which pairs
  are admitted: pairing, discovery, clock calibration, sizing, protection,
  emptiness, and peer readiness must all pass first.
- Freezing `strategy_budget_usd` at the startup balance keeps sizing tied to
  real account capacity while keeping the policy hash stable for the life of
  the pairing. Balance rather than equity is chosen so an unrelated floating
  position cannot inflate or deflate the frozen budget, and the USD and
  positivity preconditions keep every downstream USD figure honest without
  hidden conversion.
- `maximum_loss_per_trade_usd = 40` keeps the legacy number while changing its
  job. Legacy treats it as an emergency stop-out trigger; here it is one of
  three ceilings evaluated before any order is sent. Reusing the number is a
  familiarity decision, not a claim of equivalent semantics, and rough
  emergency protection targets the computed `allowed_leg_loss_usd` instead.
- The remaining-allowance summary lets the leader avoid proposing a leg the
  follower cannot take, but the follower's own re-check is what actually
  protects its daily budget. Optimizations advise; local checks decide.
- One durable identifier was doing two jobs. `route_id` now names a pairing
  relationship and `universe_generation` names a discovery result, so
  refreshing the universe no longer implies re-pairing and re-pairing no
  longer implies rediscovery. `recovery_epoch` stays a third, unrelated thing:
  market evidence about one Worker's broker session.
- The discovered universe is frozen per `universe_generation` so the pair's
  behavior is reproducible from one durable fact rather than from whatever the
  brokers happened to expose at the last refresh.
- `UNPAIRING` exists because unpair safety is a claim about a moving target.
  Freezing entry first, then binding each assertion to a local state version,
  makes "both sides are empty and terminal" a statement that stays true
  between assertion and route removal instead of a snapshot that may already
  be stale.
- The controller's route record is authoritative for relay and role because
  exactly one arbiter must decide who may talk to whom. A Worker's durable copy
  is evidence about itself. When the two disagree, the Worker stops entering
  and asks; it never resolves a metadata question by touching a position.
- Sizing plans cannot exist before discovery, so "at startup" was always
  approximate. The real ordering is pair, discover, accept policy, collect
  quotes, plan, then evaluate — and nothing is evaluated before plans exist.
- Retaining superseded plan versions costs a little memory and removes a
  guaranteed failure mode: without it, every attempt in flight across an
  hourly boundary is rejected on arrival for naming a version that just
  disappeared.
- Immediate entry means no pre-entry follower acknowledgement. It does not mean
  unjournaled or unprotected entry.
- The leader's five-second timer starts at edge decision. The follower's
  independent five-second timer starts when it receives the attempt.
- Because there is no attempt-delivery-age guard, the follower window may
  extend to relay delay plus five seconds after the leader's decision. This is
  an explicit latency-over-orphan-risk tradeoff, not a simultaneous deadline.
- Rough protection bounds the crash and timeout window. Precise protection
  implements the per-leg USD loss and 1:1 profit target from actual fills.
- `ACTIVE` is broker-observed proof of both exact positions and precise
  protection, not a successful relay acknowledgement.
- `EMPTY` is fresh broker observation from both accounts, not a successful
  close receipt, not a controller conclusion, and never a consequence of
  identity revocation.
