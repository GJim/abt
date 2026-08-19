# Trader intent execution

Status: ready-for-agent

## Problem statement

The control plane can currently approve and reconcile account workers and
manage active 跨伺服器商品配對, but it cannot safely admit a strategy service
or turn a strategy decision into a broker-executable paired order. Operators
need to create, inspect, and cancel all共同交易意圖 in the management site.
Traders need an API-only, reconnect-safe channel to submit their own intents
and observe their own results without receiving management or MT5 credential
authority.

## Solution

Add a broker-writing paired-trading slice. A Trader deployment has an
independent hardware/OS-backed P-256 identity and is admitted with a
management-issued registration invite. After approval it obtains a 30-day
Trader certificate and connects through authenticated WSS. Intent and
cancellation commands use WSS command IDs and durable idempotency; a
reconnect-safe event stream returns only that Trader's intent lifecycle.

Before an intent exists, the controller sends exact two-leg broker
`order_check` requests through the selected workers. Only two successful
checks before the absolute expiry create an accepted intent. Accepted intents
use two limit orders against an active pair, with FOK or IOC selected by the
originator. The controller owns dispatch, reconciliation, cancellation races,
IOC netting, and the existing single-leg safety rules. The management SPA
adds invite management, Trader approval, and an intent workspace for all
origins.

## User stories

1. As a 主控台管理員, I can issue, inspect, and revoke unused role-bound
   registration invites in the management site, so that only an intended
   worker or Trader can begin enrollment.
2. As a Trader operator, I can enroll one deployment with an invite, its
   non-exportable P-256 public key, strategy name, and claimed public IP, so
   that an administrator can approve a distinct deployment identity.
3. As a 主控台管理員, I can approve or reject a pending Trader registration,
   so that only reviewed Trader deployments can submit broker-writing
   commands.
4. As a Trader, I can use authenticated WSS to submit an idempotent common
   trading intent and receive its accepted, rejected, dispatch, fill,
   cancellation, safety, and terminal lifecycle events, so that my strategy
   can act without management-site access.
5. As a Trader, I can reconnect after an interruption and receive missed
   events from my acknowledged cursor, so that transient network failures do
   not lose execution knowledge or duplicate commands.
6. As a Trader or 主控台管理員, I can create an intent only when the two target
   brokers both accept an exact `order_check` before expiry, so that an
   impossible order is not represented as an executable intent.
7. As a Trader or 主控台管理員, I can select FOK or IOC for an active product
   pair only when both endpoints support that mode, so that filling semantics
   are explicit and compatible.
8. As a Trader or 主控台管理員, I can cancel an intent only while both legs
   have zero fills, so that cancellation cannot turn filled exposure into an
   uncontrolled exit.
9. As a 主控台管理員, I can create, inspect, and cancel every intent from the
   management site, so that an operator can intervene even when a Trader is
   offline.
10. As a 主控台管理員, I can emergency-flatten an already filled pair after an
    explicit preview confirmation, so that filled exposure retains an
    operator safety valve.

## Domain and behavior decisions

### Registration and identity

- A management-site administrator issues a **註冊 Invite** for exactly one
  role: `worker` or `trader`. It is a cryptographically random, one-time
  secret valid for 60 minutes.
- The management site shows the full invite only at creation. The ledger
  stores only its hash, role, timestamps, status, and immutable issuance/use/
  expiry/revocation events. An administrator may revoke an unused invite;
  used, expired, and revoked invites are terminal.
- A successful invite redemption creates a pending registration valid for 15
  minutes. Rejected or expired registration IDs are terminal and require a
  new invite and enrollment.
- Worker enrollment now requires a `worker` invite. At rollout, every
  unapproved legacy worker registration is expired; already-approved workers
  remain active.
- Trader enrollment is REST-only and requires a `trader` invite, a new
  non-exportable ECDSA P-256 public key and proof, strategy name, and claimed
  public IP. The claimed IP is reviewed metadata only; the controller does
  not record, compare, or authorize against Cloudflare-observed client IP.
- Trader private keys must use CNG on Windows or TPM 2.0/PKCS#11 on Linux.
  Software key-file and bearer-token fallback are prohibited.
- After management approval, a Trader retrieves a 30-day Trader device
  certificate through the REST enrollment flow using challenge-response. It
  rotates by presenting a valid, unrevoked old key and a new hardware/OS key.
- Revoking a Trader certificate immediately closes its WSS connections and
  blocks new commands. It does not cancel or change already-accepted intents.

### Trader WSS API

- REST is limited to Trader enrollment and certificate retrieval. An approved
  Trader uses its certificate and private-key challenge-response to establish
  an outbound WSS session.
- The Trader and controller exchange application heartbeats every 30 seconds.
  Five minutes without a valid signal marks the session stale but never
  cancels, pauses, or flattens an accepted intent.
- Intent submission and cancellation commands carry a Trader command ID.
  The ledger deduplicates by `(originator identity, command ID, payload hash)`;
  same ID/same payload returns the prior result, and same ID/different payload
  is rejected.
- Trader lifecycle events are at-least-once delivered. The Trader ACKs a
  cursor; reconnect resumes from that cursor, and the Trader deduplicates
  immutable event IDs. A Trader cannot list/query global, other-Trader, or
  management data.

### Intent contract and preflight

- An intent targets one active 跨伺服器商品配對 and provides:
  `pair_id`, primary direction, one positive decimal `lots`, one shared
  `entry_price`, positive `stop_loss_pips`, positive
  `take_profit_pips`, filling mode (`FOK` or `IOC`), absolute expiry, and
  command ID.
- FX v1 maps the single lots value at the pair's fixed 1:1 ratio. The
  controller rejects, with a recorded reason, a value not exactly valid for
  both endpoints' minimum, maximum, and volume step.
- The same entry price is used for both limit legs. For a buy primary leg,
  TP is entry plus the requested pips and SL is entry minus them; the
  sell hedge leg mirrors those protective prices. Each endpoint's pip value
  and stops level must make the mapped order valid.
- The originator selects FOK or IOC. The controller accepts only a mode
  supported by both endpoints of the active pair.
- On receipt, the controller durably records a submission, verifies static
  eligibility, and sends the exact eventual two-leg order requests to both
  selected account workers for broker `order_check`.
- Only two successful checks before the controller-monotonic absolute expiry
  create and accept an intent. Any failed, malformed, disconnected, or timed
  out check, or expiry at either check boundary, creates a
  `rejected_preflight` audit event with both endpoint results and no intent.
  `order_check` never reserves broker liquidity and does not guarantee a
  subsequent send will succeed.

### Execution, cancellation, and safety

- An accepted intent is processed asynchronously; accepted does not mean an
  order has been created or filled at a broker.
- FOK and IOC use the existing two-limit-entry, single-leg timeout, and
  protective-exit rules. IOC applies the existing partial-fill netting rule:
  preserve only matched volume, cancel residual orders, and close unmatched
  exposure at market. A failed cancel, close, or immediate reconciliation
  freezes the pair for human recovery.
- A Trader or administrator may request ordinary cancellation only while both
  legs are zero-fill. The controller atomically marks the cancellation before
  dispatch; it suppresses undispatched orders and cancels already-hung
  zero-fill orders. `cancelled` is emitted only after broker acknowledgements
  and immediate reconciliation confirm both legs remain zero-fill.
- If a fill arrives during cancellation, emit `rejected_due_to_fill` and
  continue the applicable execution safety path. The cancellation acceptance
  event only means it was scheduled; it is not a final broker result.
- A management administrator can emergency-flatten filled exposure. It
  cancels unfilled legs and closes filled legs; failure freezes the pair.
  The action remains auditable but does not require a supplied reason.

### Management station

- The management site issues and revokes unused invites, reviews pending
  Trader registrations, and displays approved/revoked Trader health.
- The **管理員意圖工作區** provides an intent creation form restricted to
  active pairs, a default active-intent workspace, filterable full history,
  and immutable per-intent event timelines for every Trader- and
  administrator-originated intent.
- The form and ordinary cancellation show an exact pair, direction, lots,
  entry, SL/TP, filling mode, and expiry preview before explicit confirmation.
  Emergency flatten uses the same preview-and-confirm button, without a
  reason field.
- Management-originated create and cancel commands use the same durable
  `(originator identity, command ID, payload hash)` idempotency rule as
  Trader WSS commands.
- Ordinary cancellation is visible only for zero-fill intents. Filled or
  frozen intents expose only emergency flatten.

## Non-goals

- Trader access to the management SPA, worker administration, MT5
  credentials, global intent data, or other Traders' events.
- Software Trader key files, bearer tokens, source-IP authorization, or
  automatic handling of a Trader's claimed public IP change.
- Market orders, stop orders, arbitrary MT5 filling modes, separate per-leg
  entry prices, non-1:1 FX sizing, and automatic risk/notional sizing.
- Treating `order_check` as a liquidity reservation or success guarantee.
- Cancelling or flattening accepted intents merely because a Trader WSS
  session is stale or its certificate is revoked.

## Testing decisions

- API/WSS contract tests cover role-bound invite hashing, one-time use,
  expiry, revocation, role mismatch, legacy pending-worker expiration, Trader
  P-256 proof/certificate/rotation/revocation, heartbeats, stale state,
  command idempotency, cursor resume, at-least-once redelivery, and
  per-Trader event authorization.
- Execution tests simulate both exact `order_check` requests, successful
  preflight, every preflight failure class, expiry during preflight,
  post-check broker rejection, FOK rejection, IOC partial netting, a
  cancellation-versus-fill race, single-leg timeout, protective exits, and
  emergency flatten failure.
- Management API and browser tests cover invite issuance/revocation,
  Trader approval, active/history/event-timeline intent views, creation and
  cancellation previews, CSRF enforcement, idempotent retry, and emergency
  flatten visibility.
- Release validation uses two approved workers in separate networks and one
  approved Trader. It proves the invite flow, authenticated WSS reconnection,
  successful and failed dual `order_check`, both filling modes, zero-fill
  cancellation, IOC/FOK safety behavior, management intervention, and audit
  retention.
