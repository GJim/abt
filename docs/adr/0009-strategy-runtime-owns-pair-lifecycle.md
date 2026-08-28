# Strategy Runtime Owns Pair Lifecycle

The realtime strategy and Trader execution orchestration will remain one
durable Strategy Runtime until their lifecycle has stabilized. This
deliberately reverses the earlier controller-owned protected-pair split:
broker facts flow from Workers to the Strategy Runtime through an authenticated
controller relay, while the controller records audit copies without deriving
or controlling pair state. The management site provides no pair or trade
operations. A separate Trader Execution module may be extracted only after
multiple real strategies demonstrate a stable shared interface.

ADR-0008 still governs Worker broker observation, durable effect journaling,
the irreversible `send_started` boundary, and evidence-driven compensation.
Its controller-ledger ownership of protected-pair desired state is superseded
for realtime strategy pairs by this decision.
