# Automatic Account Recovery After Network Partitions

Status: ready-for-agent

## Problem Statement

帳戶工作者、主控台與 Trader 在實際網路環境中會頻繁短暫失聯。現行模型把大多數斷線、broker 回執遺失與交易異常轉成帳戶級的工作者凍結，後續必須由主控台管理員手動清空帳戶並 release。這雖然保守，但把一般網路波動變成反覆的人工作業；實務上管理員的處置也只是取消 orders、平掉 positions、確認 broker 空帳戶後恢復使用。

使用者需要移除 routine 凍結／人工復歸，改為讓系統在斷線、重連、controller restart、Worker restart 與不完整 broker 回執後，自動取得事實、比較斷線前後帳戶狀態、處理未確認 orders/positions，並在能安全證明結果時自動恢復新交易。這不能以盲目重送 broker write 達成：market、cancel、close 或 protection write 一旦可能已送至 broker，必須先觀測而不是重送。

已驗證且受 broker SL/TP 保護的配對反向避險，與剛送單但尚未確認的 entry，必須有不同處置。前者在單端失聯時仍可能是完整 hedge，不應立刻平另一腿；後者可能已形成單邊曝險，必須先處理仍可控制的一端，並在失聯端重連後清除任何殘留曝險。

## Solution

建立一個跨主控台與帳戶工作者的深層 **Account Recovery Lifecycle** module，取代 routine 工作者凍結與人工復歸。它的 interface 只接收不可變的 recovery event，並產出持久 desired account state 與下一個可執行的 recovery directive。它將 partition 偵測、reconciliation epoch、broker observation 比較、effect journal、重試、補償操作、恢復資格與告警隱藏在 implementation，讓 Trader、管理站、WSS transport 與個別交易生命週期不再分別實作斷線處理。

主控台 ledger 保存每個帳戶與配對反向避險的 desired state、revision、最後一份 verified broker state、未終結 effect 與 recovery incident。每個帳戶工作者保存本機耐久 journal，在任何 MT5 write 前記錄 effect 的 `prepared` 與 `send_started`，在取得 receipt 或新 observation 後記錄權威證據。重連時，Worker 先回傳帳戶身分、journal、cursor 與全量 broker snapshot；主控台以 broker observation 作事實、以 ledger desired state 作目標，驅動收斂。

系統使用 `ENTRY_UNCONFIRMED`、`ACTIVE_VERIFIED`、`CATCHING_UP`、`CONVERGING_EMPTY`、`READY`、`NEEDS_HUMAN` 與 `REVOKED` 等 lifecycle state，而非 routine `frozen`。任何 state 不是 `READY` 的帳戶都不可參與新進場，但系統仍持續觀測與自動補償。只有帳戶身分不符、journal/簽章完整性失敗、broker 長期無法觀測、或外部 actor 持續重建曝險等情況才會進入 `NEEDS_HUMAN`。

## User Stories

1. As a realtime arbitrage operator, I want a short Worker or controller network interruption to pause only new entries, so that routine connectivity instability does not require manual account release.
2. As a realtime arbitrage operator, I want the broker account state to be treated as fact after reconnect, so that recovery does not assume a prior RPC outcome.
3. As a realtime arbitrage operator, I want a just-dispatched entry with an unknown outcome to enter `ENTRY_UNCONFIRMED`, so that the system does not mistake possible single-leg exposure for a safe pair.
4. As a realtime arbitrage operator, I want the connected leg of an unconfirmed entry to be closed automatically, so that controllable exposure is reduced immediately.
5. As a realtime arbitrage operator, I want a reconnected leg from an unconfirmed entry to be reconciled and closed when it contains an order or position, so that no residual exposure survives uncertainty.
6. As a realtime arbitrage operator, I want an entry command with an unknown broker outcome never to be resent, so that recovery cannot create duplicate market exposure.
7. As a realtime arbitrage operator, I want a pair to become `ACTIVE_VERIFIED` only after both broker positions and both SL/TP protections are observed, so that a known hedge is truly protected before ordinary monitoring resumes.
8. As a realtime arbitrage operator, I want a verified, protected pair to remain open during a one-sided Worker disconnect, so that a connectivity event does not itself create directional exposure.
9. As a realtime arbitrage operator, I want new entries blocked while either participant of an active pair is catching up, so that recovery and new risk cannot interleave.
10. As a realtime arbitrage operator, I want reconnect reconciliation to compare tickets, symbols, sides, volumes, pending orders, SL, and TP, so that material account changes are detected without treating normal price and floating P/L movement as mismatches.
11. As a realtime arbitrage operator, I want a matching reconnected verified pair to resume unchanged, so that benign network interruptions do not force unnecessary closes.
12. As a realtime arbitrage operator, I want a mismatched reconnected pair to make both accounts converge to empty, so that a missing leg, changed volume, orphan order, or altered protection cannot remain active.
13. As a realtime arbitrage operator, I want an account convergence to cancel all pending orders before closing positions, so that an order cannot fill while the account is being flattened.
14. As a realtime arbitrage operator, I want each cancellation and close to be observed after execution, so that an empty account is broker-verified rather than inferred from a receipt.
15. As a realtime arbitrage operator, I want a controller loss affecting both verified Workers to preserve the pair while both broker protections remain valid, so that controller availability does not force a normal protected hedge to close.
16. As a realtime arbitrage operator, I want a Worker that loses controller contact and finds a missing or invalid broker protection to converge its account to empty, so that an offline account never holds an unprotected strategy position indefinitely.
17. As a Trader, I want my own disconnect to stop new signals but not alter an already verified pair's desired state, so that a strategy process restart cannot disrupt a safe open hedge.
18. As a Trader, I want the original command ID to recover its durable final outcome after reconnect, so that a local timeout does not cause another entry submission.
19. As a controller operator, I want a controller restart to create a reconciliation epoch before admitting new paired entries, so that in-progress effects are resolved from evidence rather than replayed blindly.
20. As a Worker operator, I want every broker write journaled before and after the broker call, so that Worker restart recovery can distinguish not-started work from unknown post-send work.
21. As a Worker operator, I want a Worker restart to replay its journal and send a complete broker snapshot before accepting new entry work, so that the controller can restore the correct lifecycle state.
22. As a controller operator, I want cursored Worker deltas and full snapshots to be deduplicated by stream and epoch, so that reconnect delivery cannot classify a missed delta as an external broker change.
23. As a controller operator, I want a gap or epoch change to require a full snapshot, so that recovery uses a known broker-state baseline.
24. As a controller operator, I want unexpected broker orders, positions, volume changes, or protection changes to create a recovery incident instead of a permanent freeze, so that observable exposure is automatically removed.
25. As a controller operator, I want an external closure of one verified leg to converge the remaining leg to empty, so that the system does not recreate a pair at an unknown market price.
26. As a controller operator, I want IOC partial-fill recovery to retain only broker-observed matched exposure and close the excess, so that the final account state has no unhedged volume.
27. As a controller operator, I want FOK partial-fill evidence to converge both accounts to empty, so that a broker anomaly cannot become a persistent incomplete hedge.
28. As a controller operator, I want close, cancel, and protection effects with a lost receipt to be observed before a new compensating effect is created, so that the recovery path obeys the same no-blind-retry rule as entry.
29. As a controller operator, I want recovery effects to use bounded retry with backoff and fresh observations, so that temporary network and market-closed failures recover automatically without busy looping.
30. As a controller operator, I want recovery to retain high-priority access to the serialized Worker scheduler, so that cancel, close, and broker verification are not delayed by historical, catalog, or sizing reads.
31. As a controller operator, I want one account recovery incident to include every relevant active counterpart, so that pair state and account state converge coherently without restoring unnecessary global freezes.
32. As a management-station administrator, I want to see an account's desired state, observed state, divergence reason, current effect, retry schedule, and last verified time, so that automatic recovery is explainable.
33. As a management-station administrator, I want ordinary recovery to complete without Cleanup and Release buttons, so that common network faults do not require manual intervention.
34. As a management-station administrator, I want a break-glass workflow only for `NEEDS_HUMAN`, so that manual intervention is reserved for facts the system cannot safely establish.
35. As a management-station administrator, I want a credential revocation to prevent future commands and request automatic account-empty convergence, so that revocation remains a security control even after routine freeze removal.
36. As a management-station administrator, I want a revoked Worker never to become `READY` through automatic reconnect recovery, so that a new authorization lifecycle remains required.
37. As an auditor, I want each recovery state transition, desired-state revision, broker observation, effect attempt, and terminal proof to be immutable, so that automatic actions remain reconstructable.
38. As an auditor, I want recovery correlation to use durable command/effect records, tickets, and before/after broker observations rather than MT5 comment or magic fields, so that broker metadata stays unused.
39. As a test author, I want deterministic clocks, journals, Worker channel adapters, and broker adapters at the recovery module's internal seams, so that every disconnect and crash boundary is reproducible.
40. As a test author, I want the lifecycle to prove that no unknown broker write is resent and that every `READY` account is either verified empty or part of a verified protected pair, so that recovery safety is measurable.

## Implementation Decisions

- Add one tier-spanning **Account Recovery Lifecycle** module at the seam between durable control-plane facts and authenticated Worker observations. Its external interface processes a closed set of recovery events and returns durable directives. The interface includes lifecycle invariants, command/effect IDs, recovery revisions, admission state, desired account state, and required observation scope; it does not expose SQL, WSS queues, MT5 calls, locks, retries, or per-order implementation mechanics.

- Use a reducer-shaped lifecycle interface: `advance(event) -> directive`. Events include Worker link lost/restored, Trader link lost/restored, controller restart, Worker restart, full broker observation, cursored broker delta, effect receipt, scheduler outcome, and recovery timer. Directives include admission changes, full-snapshot requests, desired account state revisions, and ordered recovery effects.

- Treat broker observation as the authoritative account fact. Treat controller ledger desired state as the intended safe target. A receipt alone is evidence of an attempted effect, not proof that a broker position or order exists.

- Maintain one durable recovery record per affected account and one pair recovery record for each active 配對反向避險. Both records carry monotonic revisions, incident IDs, last verified observation identifiers, desired account state, lifecycle state, and current effect IDs. A later revision supersedes only effects that have not crossed the broker-send boundary.

- Desired account states are `EMPTY` and `PROTECTED_LEG`. `PROTECTED_LEG` contains the expected position ticket, symbol, side, volume, SL, TP, and associated paired recovery revision. An account may be `READY` only when its most recent broker observation proves `EMPTY`, or when it is part of an `ACTIVE_VERIFIED` pair with exact expected `PROTECTED_LEG` state.

- Define lifecycle states:
  - `ENTRY_UNCONFIRMED`: an entry send may have happened but no broker facts prove both protected legs;
  - `ACTIVE_VERIFIED`: both legs, tickets, volumes, and protections are broker-observed and match the same pair revision;
  - `CATCHING_UP`: a relevant network link, cursor, epoch, or process restart requires a fresh snapshot before new work;
  - `CONVERGING_EMPTY`: ordered cancellation, closing, and confirmation effects are reducing an affected account to `EMPTY`;
  - `READY`: the account is eligible for new work;
  - `NEEDS_HUMAN`: automatic convergence cannot safely establish or achieve the desired state;
  - `REVOKED`: authorization is permanently withdrawn until a new enrollment lifecycle completes.

- A just-dispatched paired entry remains `ENTRY_UNCONFIRMED` until each account has a durable receipt and a fresh broker observation proving the expected position and valid SL/TP. A Worker disconnect, controller timeout, lost broker receipt, or process restart in this state sets the connected account's desired state to `EMPTY` immediately. The disconnected account receives the same desired state when its authenticated channel returns, and all observed orders/positions are removed.

- A one-sided disconnect after `ACTIVE_VERIFIED` changes the affected pair and accounts to `CATCHING_UP`, blocks new paired entries, and leaves both verified protected legs unchanged. On reconnect, require a full current snapshot from both accounts and compare only ticket, symbol, side, volume, pending orders, SL, and TP. Do not treat price, spread, swap, margin, equity, or floating P/L movement as a mismatch. Exact match returns the pair to `ACTIVE_VERIFIED`; any difference makes both accounts converge to `EMPTY`.

- If the controller loses contact with both `ACTIVE_VERIFIED` Workers, Workers retain their last signed `PROTECTED_LEG` desired state while broker protections remain valid. They do not enter or create new trades. A Worker detecting missing/invalid SL/TP, external account divergence, or an unconfirmed local journal effect transitions itself to `CONVERGING_EMPTY`, persists that fact, and reports it after reconnect.

- Trader disconnection does not alter an accepted desired state. The Trader stops new signals, preserves command IDs, reconnects, and consumes replayed lifecycle outcomes. The controller and Workers continue recovery independently.

- Each Worker has a local SQLite/WAL recovery journal. Before every broker write it persists `prepared`; immediately before the MT5 call it persists `send_started`; after a receipt or broker observation it persists the corresponding result and evidence. A `send_started` effect without sufficient evidence is `unknown_after_send` and is never sent again with the same effect ID.

- Worker journals and control-plane records use `(worker ID, effect ID, payload hash)` for idempotency. Same ID and hash returns the durable prior state; same ID with a different hash is rejected. A new compensating action always receives a new effect ID and must be justified by a fresh broker observation.

- Reconnect recovery begins with Worker identity verification, journal inventory, stream/epoch/cursor evidence, and a full broker snapshot containing orders, positions, relevant deals, and protection values. Cursor gaps, changed stream epochs, or incomplete acknowledgements require a fresh snapshot; missing deltas are not automatically classified as external changes.

- `CONVERGING_EMPTY` always executes the same ordered recovery plan: cancel all observed pending orders; obtain a fresh snapshot proving no orders; close all observed positions; obtain a final fresh snapshot proving zero orders and zero positions. An effect rejection, unknown close/cancel outcome, market closure, or temporary connection failure remains recoverable through observation and bounded retry; it does not reintroduce routine freeze.

- FOK and IOC recovery preserve existing broker semantics. FOK partial-fill evidence converges both accounts to `EMPTY`. IOC recovery computes observed matched volume using the product-pair ratio, cancels remainders, retains only the matched protected amount if both legs can be verified, and closes all excess. It never sends an IOC top-up order merely to restore a target volume.

- Unattributed external broker changes create a divergence recovery incident. Unknown pending orders are cancelled; unknown positions or missing counterpart legs converge the relevant paired accounts to `EMPTY`. Repeated external recreation of exposure after bounded automatic attempts escalates to `NEEDS_HUMAN`.

- Replace routine `frozen` admission checks with lifecycle admission checks. Any account in `ENTRY_UNCONFIRMED`, `CATCHING_UP`, `CONVERGING_EMPTY`, `NEEDS_HUMAN`, or `REVOKED` rejects new entries. Recovery close, cancel, snapshot, and reconciliation effects retain the highest scheduler priority needed to make progress.

- Remove ordinary administrator Cleanup and Release as recovery controls. The management station becomes an observability and break-glass surface. Only `NEEDS_HUMAN` exposes an explicit administrator action, which records the observed facts and may request a new recovery revision; it must not mark an account safe without a current broker observation.

- Credential revocation remains exceptional: it transitions the Worker to `REVOKED`, rejects future commands, sends a signed desired `EMPTY` directive when reachable, and preserves automatic local convergence where the Worker can still operate. A revoked Worker never returns to `READY` automatically.

- Update the Worker-Level Trading Isolation ADR and domain documentation to supersede permanent routine freeze/manual release. Preserve terminal exclusivity, authenticated signed commands, broker-verified account facts, account-wide empty convergence, and explicit `NEEDS_HUMAN` escalation.

- Migrate incrementally: introduce durable recovery records and Worker journal while retaining current freeze data for audit; run observation/reducer logic in shadow mode; enable automatic empty convergence for a limited Worker set; route all execution origins through the lifecycle; then retire routine freeze/cleanup/release paths. Existing frozen Workers remain under their historical recovery procedure unless explicitly migrated through a broker-verified empty snapshot.

## Testing Decisions

- Test the highest practical seam: submit recovery events to the Account Recovery Lifecycle module through the authenticated controller/Worker contract, then assert durable directives, observable Worker messages, final broker state, admission state, and Trader-visible lifecycle events. Do not test deque layout, individual asynchronous tasks, SQL table arrangement, or React local state.

- Use deterministic clock, in-memory Worker channel, temporary ledger/journal, and scripted MT5 broker adapters as internal seams. The production WSS channel and MT5 terminal remain adapters; tests must force their failures at every durable boundary.

- Add lifecycle tests for entry loss before send, after send before receipt, after receipt before observation, after one confirmed leg, and after both confirmed legs but before protection verification. Verify that unknown entry effects are never resent and connected unconfirmed legs converge to empty.

- Add reconnect tests for one-sided Worker loss after `ACTIVE_VERIFIED`: exact snapshot match resumes the pair; changes to ticket, symbol, side, volume, pending order inventory, SL, or TP converge both accounts to empty; changed price/PnL/equity does not trigger recovery.

- Add controller and Worker restart tests spanning journal writes around `prepared`, `send_started`, receipt persistence, snapshot publication, cursor acknowledgement, and effect completion. Verify that restarts never duplicate a broker write.

- Add full convergence tests covering cancellation before position close, cancel/close receipt loss, market-closed retry, broker rejection, stale snapshot rejection, final empty-account verification, and automatic return to `READY`.

- Add paired hedge tests for FOK partial fills, IOC partial fills, external one-leg closure, external pending orders, missing protection, and independently reconnecting Workers. Verify observed matched exposure rules and no IOC top-up behavior.

- Add scheduler integration tests proving recovery effects outrank normal and background reads, remain serialized per terminal, and retain no-blind-retry semantics after an unknown write.

- Add management-station contract and browser tests for recovery incident visibility, desired-versus-observed diff rendering, retry state, automatic readiness, `NEEDS_HUMAN` break-glass visibility, and the absence of routine release as an ordinary workflow.

- Reuse existing control-plane ledger, hedged-entry, Worker reconciliation/session, Trader session, and management-station test patterns. Replace shallow freeze/release implementation tests with lifecycle-interface tests once migration completes.

## Out of Scope

- Altering realtime arbitrage edge detection, shared-symbol eligibility, exposure sizing, margin headroom, daily-loss limits, or the normal minimum-hold strategy.
- Making broker writes distributed transactions, allowing concurrent MT5 access, or introducing direct Worker-to-Worker communication.
- Automatically creating a new entry after an unknown entry effect; a fresh market opportunity requires a new Trader decision and command ID.
- Retaining an unprotected position while the responsible Worker detects invalid or absent broker SL/TP.
- Replacing broker-native SL/TP with application-managed stops.
- Removing administrator break-glass capability for `NEEDS_HUMAN` or changing credential revocation authorization.
- Migrating existing frozen Worker records automatically without a current broker-verified empty snapshot.

## Further Notes

- This specification intentionally contradicts ADR-0007, Worker-Level Trading Isolation, for routine connectivity and execution recovery. It replaces permanent account freeze plus manual cleanup/release with automatic desired-state convergence. A new ADR must explicitly supersede that portion before implementation.

- Account-wide empty convergence remains the conservative fallback. The change is not to make uncertainty harmless; it is to make observable, repeatable recovery automatic and reserve humans for irreducible uncertainty.

- `ACTIVE_VERIFIED` is deliberately stricter than a successful order receipt. It requires both observed legs and their broker protections, because the selected disconnect policy permits a verified protected pair to remain open while controller connectivity is unavailable.
