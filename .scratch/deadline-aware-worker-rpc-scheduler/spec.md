# Deadline-Aware Worker RPC Scheduler

Status: ready-for-agent

## Problem Statement

即時跨平台避險策略在訊號成立後，會透過主控台對兩個帳戶工作者執行預檢與市價送單。帳戶工作者目前以無界 FIFO deque 接收 Trader RPC，且每次 reconciliation loop 最多處理一筆；因此請求可能在 queue 中等待任意時間，並在原始交易訊號、價格或風險條件已失效後仍抵達 broker。

目前各層的 30 秒 timeout 是等待回應的時間，不是「到期後不得執行」的操作期限。尤其配對反向避險會依序經歷並行預檢和並行送單，整體時間可能超過 Trader 的單次等待時間。若 Trader 先逾時並停止，但主控台或工作者稍後仍完成市價送單，策略會失去該交易的即時生命週期掌控。反過來，若 Worker 在已呼叫 broker 後只因回應遺失就回報一般拒絕，呼叫端可能重送市價單，造成重複或未避險曝險。

使用者需要一個能優先處理緊急交易操作、拒絕尚未開始且已失效的請求、完整保留送單後不確定 outcome 的工作者 RPC 排程機制；它必須讓主控台、Trader 與帳戶工作者對同一 command 的生命週期具有一致且可恢復的理解。

## Solution

建立一個由帳戶工作者擁有的 deadline-aware RPC scheduler module，取代目前無界 FIFO deque。主控台為每一個 Trader broker read 與 Trader worker operation 指派可稽核的 command ID、優先級與 controller-authoritative expiry；工作者只可在 expiry 前開始 broker 動作。scheduler 以有界 priority queue 執行工作，並在呼叫 broker 前、後明確區分「安全未執行」與「可能已執行但結果未知」。

配對反向避險維持主控台既有的 pair-level module 與 deterministic pair lock：兩端預檢及送單共用同一個整體 deadline，而非各自取得可累加的 30 秒等待期。兩端任一腿在 broker send 前過期或被排程拒絕時，整個 command 以 `rejected_preflight` 結束且不送任何市價單；任一腿在開始 broker send 後失去可證實 outcome 時，主控台依 Worker-Level Trading Isolation 凍結兩個參與的帳戶工作者，持久記錄 outcome，並由既有 cleanup/reconciliation 流程處置。

Trader 以同一 command ID 恢復任何本機等待逾時或 WSS 中斷的最終結果。Trader 的本機 timeout 不再代表主控台 command 已取消，也不得觸發市價單重送或自行對單腿平倉。這個 module 將 queue 排程、deadline 檢查、優先順序、背壓、結果分類與 telemetry 隱藏在單一 Worker seam 後，讓 controller 和策略只需處理明確的 lifecycle outcome。

## User Stories

1. As a realtime arbitrage strategy operator, I want an entry signal to expire before broker submission when it has waited too long, so that a stale quote cannot create a new hedge.
2. As a realtime arbitrage strategy operator, I want both legs of a paired hedge to use one shared execution deadline, so that one slow leg cannot silently extend the trade opportunity.
3. As a realtime arbitrage strategy operator, I want a pre-send expiry to state that no broker market order was attempted, so that I can safely halt without guessing whether exposure exists.
4. As a realtime arbitrage strategy operator, I want an order outcome to be marked unknown after broker submission loses its response, so that the system never retries a potentially filled market order.
5. As a realtime arbitrage strategy operator, I want command results to remain recoverable by command ID after a Trader reconnects, so that a local timeout cannot lose the authoritative execution result.
6. As a realtime arbitrage strategy operator, I want a local Trader wait timeout to halt new entries rather than imply cancellation, so that the strategy cannot submit duplicate market orders.
7. As a realtime arbitrage strategy operator, I want emergency flatten and recovery operations to run ahead of ordinary work, so that account containment is not delayed by market-data or sizing reads.
8. As a realtime arbitrage strategy operator, I want paired-entry preflight and dispatch work to outrank background sizing snapshots, so that valid fresh opportunities are not delayed by non-critical reads.
9. As a realtime arbitrage strategy operator, I want stale margin, catalog, and historical-data requests to be discarded before execution, so that they cannot consume Worker capacity after their usefulness ends.
10. As a realtime arbitrage strategy operator, I want queue saturation to return an explicit pre-send rejection, so that overload cannot turn into unbounded latency or hidden stale execution.
11. As a control-plane operator, I want queue depth, wait time, expiration, rejection, and unknown-outcome telemetry, so that I can distinguish broker latency from Worker saturation.
12. As a control-plane operator, I want each command lifecycle transition durably recorded, so that audit and recovery use facts rather than transient WebSocket state.
13. As a control-plane operator, I want the Worker to preserve idempotency for the same signed command ID and payload, so that reconnect delivery cannot execute a broker write twice.
14. As a control-plane operator, I want reuse of a command ID with a different payload to be rejected, so that idempotency cannot mask a changed order.
15. As an account safety operator, I want any dispatch outcome that cannot prove both legs’ state to freeze both participating workers, so that the existing account-level recovery model contains cross-account exposure.
16. As an account safety operator, I want a frozen worker to reject ordinary queued trading work, so that no new operation bypasses recovery isolation.
17. As an account safety operator, I want emergency cleanup work to remain admissible for a frozen worker, so that freezing does not prevent required cancellation, close, and broker-verified empty-account reconciliation.
18. As a Worker operator, I want the scheduler to process all MT5 access serially, so that priority handling does not introduce concurrent access to one terminal.
19. As a Worker operator, I want the scheduler to perform expiry checks immediately before a broker side effect, so that queue time and intervening reconciliation work cannot invalidate a previous check.
20. As a Worker operator, I want a command already handed to MT5 to complete its result handling even after its deadline, so that a late response is classified accurately rather than discarded.
21. As a strategy developer, I want broker reads and operations to expose stable lifecycle outcome categories, so that strategy logic does not parse transport exceptions to make safety decisions.
22. As a strategy developer, I want batch margin snapshots to be coalesced or rejected as low-priority work, so that duplicate sizing refreshes do not crowd out execution.
23. As a strategy developer, I want live quote polling to yield to ready high-priority worker commands whenever the terminal is free, so that the scheduler improves entry latency rather than merely ordering queued work.
24. As a test author, I want deterministic clock and scheduler adapters, so that expiry, priority, queue saturation, and unknown-outcome behavior can be verified without a real MT5 terminal.
25. As an auditor, I want final outcomes to identify each leg’s queue admission, dequeue, broker-send start, completion, and classification times, so that an entry anomaly can be reconstructed hop by hop.

## Implementation Decisions

- Introduce one Worker-local **Deadline-Aware Trader RPC Scheduler** module at the seam currently used to receive and execute Trader RPC. Its interface accepts a signed command envelope and returns a terminal command outcome. Its implementation owns bounded queues, priority selection, expiry enforcement, queue telemetry, idempotency handoff, and the transition from scheduled work to serialized MT5 access.

- Keep direct MT5 access serialized per account worker. Priority determines which eligible command begins next; it does not preempt an MT5 call that is already running. The reconciliation loop must give the scheduler a prompt execution opportunity before non-critical live quote polling when high-priority work is waiting.

- Replace the unbounded FIFO request deque with bounded queues ordered first by priority and then FIFO within the same priority. The priority classes are:
  - emergency: revocation flatten, disconnect safety flatten, frozen-worker cleanup, and broker-verified recovery;
  - execution: paired hedge preflight and market dispatch;
  - protection: position/order integrity, close, cancel, and stop-loss/take-profit modification;
  - normal: direct Trader broker reads required by an active lifecycle;
  - background: margin snapshots, product catalog work, historical ticks, and other coalescible reads.

- The scheduler may coalesce equivalent queued background reads, but it must never coalesce, reorder within a pair, or replace a Trader worker operation. Admission control must produce an explicit terminal `rejected_queue_full` outcome for work that was not admitted; it must not silently drop work.

- Add a command envelope to controller-to-Worker Trader RPC containing the stable command ID, immutable payload hash, priority, controller-issued expiry, command kind, and correlation metadata. The expiry is part of the authenticated/signed instruction. The controller remains the time authority for lifecycle decisions; the Worker enforces the issued expiry using synchronized time only to decide whether broker work may start.

- Define terminal outcome categories independent of transport exceptions:
  - `completed`: the Worker obtained and persisted an authoritative broker result;
  - `rejected_preflight`: no broker market send was begun, including invalid order check, queue admission rejection, or expiry before send;
  - `expired_not_started`: deadline elapsed before the relevant broker action began;
  - `unknown_after_send`: a broker send may have occurred, but an authoritative outcome is unavailable;
  - `rejected_worker_state`: the Worker was frozen, disconnected, or otherwise prohibited from ordinary work before send.
  Read commands may use the corresponding no-side-effect rejection categories, but only broker writes may return `unknown_after_send`.

- Check command expiry at queue admission, immediately before broker invocation, and at phase boundaries. Once the Worker starts `order_send`, cancel, close, or modify at MT5, it must retain and process the response even if the expiry passes. Loss of the response after that point is `unknown_after_send`, never an expiry rejection.

- Retain existing Worker-side persistent deduplication. Repeated delivery of a command ID and identical payload must return the original durable outcome or in-progress status; command-ID reuse with a different payload must be rejected before execution.

- Evolve the paired hedge module to use a single controller-monotonic execution budget. Both Worker `order_check` requests are scheduled in parallel with the shared expiry. Dispatch is eligible only when both preflights completed successfully and sufficient shared budget remains. Preflight must not reserve liquidity and must not extend the expiry.

- If either paired leg expires, is queue-rejected, or fails preflight before `order_send`, persist a pair-level `rejected_preflight` result with both leg outcomes and do not dispatch either market order. If either dispatch leg is `unknown_after_send`, fails after the counterpart might have been sent, or returns an incomplete broker outcome, persist a pair-level contained outcome and freeze both workers through the existing Worker-Level Trading Isolation workflow.

- The controller must durably create the command record before scheduling Worker work and durably record every final result before reporting it to the Trader. Pair-level results include the final category, per-leg category, original broker diagnostics where available, and timing evidence for admission, dequeue, broker-send start, and completion.

- Preserve the deterministic pair lock around a paired hedge through both preflight and dispatch. Queue priority must not allow another ordinary operation on either participant to interleave between a successful pair preflight and its dispatch attempt.

- The Trader transport must treat a local response timeout or WSS disconnect as `command_outcome_pending`, not cancellation. It retains the original command ID, blocks new entries involving the affected workers, reconnects, and obtains the durable terminal result using that same ID. It must not generate a fresh command ID, retry a market operation, or issue individual compensating closes while the controller outcome is pending.

- Trader-facing command responses must distinguish durable acceptance/in-progress from a terminal result. A final result is delivered with the same command ID and remains retrievable after reconnect. The strategy only resumes normal lifecycle handling after it has received a terminal completed, rejected, or contained outcome.

- Maintain current account-level containment: a pair with an unknown post-send outcome freezes both participating account workers, not merely the pair. Frozen workers reject ordinary new work, while authorized emergency cleanup and reconciliation retain the priority needed to reach a broker-verified empty account and explicit administrator release.

- Emit structured telemetry for admission timestamp, queue depth by priority, queue wait, deadline remaining at dequeue, expiry/rejection cause, MT5 action start, MT5 result receipt, final category, and pair preflight/dispatch elapsed time. Do not log credentials, signed command material, or broker secrets.

- Existing 30-second values become upper bounds for awaiting a response, not implied execution deadlines. The controller derives each Worker wait from the shared command deadline and remaining budget. Trader-local waits must encompass the command lifecycle or transition to recoverable pending state; they must never declare a command cancelled solely because their own wait elapsed.

## Testing Decisions

- Test the highest practical seam: a Trader command submitted through the controller command interface and executed through a deterministic Worker scheduler adapter. Assertions must concern durable command/pair outcomes, broker-action attempts, worker freeze state, and recoverable result delivery—not deque internals, private queue structures, or individual task scheduling mechanics.

- Add focused Worker scheduler tests using a fake monotonic/UTC clock and MT5 adapter. Verify priority ordering when the terminal becomes free, FIFO ordering within one priority, bounded admission, background-read coalescing, expired-before-send rejection, expiry immediately before broker invocation, and serialized MT5 execution.

- Add Worker operation tests that prove no market send is attempted for `expired_not_started`, `rejected_queue_full`, `rejected_worker_state`, or failed preflight; and prove a missing response after a fake `order_send` invocation becomes `unknown_after_send`, never a retryable rejection.

- Extend controller paired hedge tests to cover one shared deadline across concurrent preflights, no dispatch if either side expires or queues unsuccessfully, pair-lock preservation, durable per-leg timing evidence, and freezing both workers for partial/unknown post-send outcomes. Reuse the existing hedged-entry orchestration test style as prior art.

- Extend ledger tests to verify idempotent replay of in-progress and terminal command IDs, rejection of payload-hash changes, and durable final outcomes before Trader notification. Reuse the existing Trader command and hedged-entry ledger test patterns.

- Extend Trader session and realtime arbitrage tests to verify that a local wait expiry retains the original command ID, halts new risk, requests the final durable outcome after reconnect, and never resubmits a market command or falls back to individual close operations. Reuse current unacknowledged HedgedEntry containment tests as prior art.

- Add integration-style tests with deliberately delayed Worker responses to prove that a controller preflight/dispatch lifecycle exceeding a single 30-second client wait remains recoverable and cannot become a success-shaped cancellation.

- Validate observability with tests that inspect emitted structured fields and durable timing records for queue admission, broker-send boundary, terminal category, and pair containment reason. Do not assert exact wall-clock durations.

## Out of Scope

- Changing the strategy’s edge calculation, quote-batch atomicity, shared-symbol eligibility, dynamic exposure sizing, margin headroom, or default risk limits.
- Changing broker fill policy, including automatic FOK-to-IOC fallback.
- Replacing the existing post-fill stop-loss/take-profit calculation path or redesigning broker-native protection pricing.
- Automatically flattening a partially filled paired hedge outside the established frozen-worker cleanup and reconciliation authority.
- Introducing concurrent MT5 access, a separate external message broker, or cross-worker direct communication.
- Changing the administrator release requirement for a frozen worker.
- Retrofitting historical replay datasets or interpreting historical tick alignment as real-time execution evidence.

## Further Notes

- This specification is consistent with ADR-0007, Worker-Level Trading Isolation: any execution anomaly that cannot establish a safe paired outcome freezes every participating worker and uses the existing account-level cleanup path.

- The selected primary seam is the Worker’s Trader RPC execution interface. It is the highest seam that can enforce pre-send expiry immediately next to the broker side effect while keeping queue mechanics local. The controller and Trader receive a small lifecycle interface rather than managing Worker queues directly.

- A queue alone does not improve safety. Its value comes from the command deadline and the explicit distinction between “never sent” and “may have been sent.” Those meanings must remain stable across Worker, controller, ledger, and Trader recovery.
