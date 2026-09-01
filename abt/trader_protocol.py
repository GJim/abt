"""Shared, validated wire models for Trader-to-worker RPC."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator, model_validator


MAX_LIVE_SYMBOLS = 64
MAX_MARGIN_CALCULATIONS = MAX_LIVE_SYMBOLS * 2


class _ProtocolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


PROTOCOL_VERSION = 1


class BrokerReceipt(_ProtocolModel):
    retcode: int
    deal: int | None = None
    order: int | None = None
    volume: float | None = None
    price: float | None = None
    bid: float | None = None
    ask: float | None = None
    comment: str | None = None
    request_id: int | None = None
    retcode_external: int | None = None


class AccountInfoRead(_ProtocolModel):
    type: Literal["account_info"]


class SymbolInfoRead(_ProtocolModel):
    type: Literal["symbol_info"]
    symbol: str


class SymbolsRead(_ProtocolModel):
    type: Literal["symbols"]


class CalcMarginRead(_ProtocolModel):
    type: Literal["calc_margin"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    price: str


class MarginCalculation(_ProtocolModel):
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    price: str


class CalcMarginBatchRead(_ProtocolModel):
    type: Literal["calc_margin_batch"]
    calculations: list[MarginCalculation] = Field(min_length=1, max_length=MAX_MARGIN_CALCULATIONS)


class CalcProfitRead(_ProtocolModel):
    type: Literal["calc_profit"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    open_price: str
    close_price: str


class HistoricalTicksRead(_ProtocolModel):
    type: Literal["historical_ticks"]
    symbol: str
    from_utc: str
    to_utc: str
    flags: Literal["all", "info", "trade"]


class CurrentOrdersRead(_ProtocolModel):
    type: Literal["current_orders"]


class CurrentPositionsRead(_ProtocolModel):
    type: Literal["current_positions"]


class BrokerSnapshotRead(_ProtocolModel):
    type: Literal["broker_snapshot"]


TraderRead = Annotated[
    AccountInfoRead
    | SymbolInfoRead
    | SymbolsRead
    | CalcMarginRead
    | CalcMarginBatchRead
    | CalcProfitRead
    | HistoricalTicksRead
    | CurrentOrdersRead
    | CurrentPositionsRead
    | BrokerSnapshotRead,
    Field(discriminator="type"),
]


class MarketOperation(_ProtocolModel):
    type: Literal["market"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    filling_mode: Literal["FOK", "IOC"]


class PendingOperation(_ProtocolModel):
    type: Literal["pending"]
    pending_type: Literal["limit", "stop", "stop_limit"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    price: str
    sl: str
    tp: str
    filling_mode: Literal["FOK", "IOC"]
    expires_at: str
    stop_limit_price: str | None = None


class CancelOperation(_ProtocolModel):
    type: Literal["cancel"]
    ticket: str


class CloseOperation(_ProtocolModel):
    type: Literal["close"]
    ticket: str
    volume: str


class ModifySlTpOperation(_ProtocolModel):
    type: Literal["modify_sl_tp"]
    symbol: str
    position: str
    sl: str
    tp: str


class SetLiveSymbolsOperation(_ProtocolModel):
    type: Literal["set_live_symbols"]
    symbols: list[str] = Field(min_length=1, max_length=MAX_LIVE_SYMBOLS)

    @field_validator("symbols")
    @classmethod
    def unique_nonempty_symbols(cls, symbols: list[str]) -> list[str]:
        if not all(symbol for symbol in symbols) or len(set(symbols)) != len(symbols):
            raise ValueError("Live symbols must be nonempty and unique.")
        return symbols


TraderTradeOperation = Annotated[
    MarketOperation | PendingOperation | CancelOperation | CloseOperation | ModifySlTpOperation,
    Field(discriminator="type"),
]


TraderOperation = Annotated[
    MarketOperation | PendingOperation | CancelOperation | CloseOperation | ModifySlTpOperation | SetLiveSymbolsOperation,
    Field(discriminator="type"),
]


class TraderRpcRequest(_ProtocolModel):
    type: Literal["trader_rpc_request"] = "trader_rpc_request"
    request_id: str
    kind: Literal["read", "operation"]
    payload: TraderRead | TraderOperation

    @model_validator(mode="after")
    def matching_kind(self) -> TraderRpcRequest:
        if self.kind == "read" and isinstance(
            self.payload,
            (MarketOperation, PendingOperation, CancelOperation, CloseOperation, ModifySlTpOperation, SetLiveSymbolsOperation),
        ):
            raise ValueError("Trader read requests require a read payload.")
        if self.kind == "operation" and isinstance(
            self.payload,
            (
                AccountInfoRead, SymbolInfoRead, SymbolsRead, CalcMarginRead, CalcMarginBatchRead,
                CalcProfitRead, HistoricalTicksRead, CurrentOrdersRead, CurrentPositionsRead,
                BrokerSnapshotRead,
            ),
        ):
            raise ValueError("Trader operation requests require an operation payload.")
        return self


class TraderRpcSuccess(_ProtocolModel):
    type: Literal["trader_rpc_response"] = "trader_rpc_response"
    request_id: str
    kind: Literal["read", "operation"]
    accepted: Literal[True] = True
    result: dict[str, object]


class TraderRpcFailure(_ProtocolModel):
    type: Literal["trader_rpc_response"] = "trader_rpc_response"
    request_id: str
    kind: Literal["read", "operation"]
    accepted: Literal[False] = False
    reason: str


TraderRpcResponse = Annotated[TraderRpcSuccess | TraderRpcFailure, Field(discriminator="accepted")]

trader_rpc_request_adapter = TypeAdapter(TraderRpcRequest)
trader_rpc_response_adapter = TypeAdapter(TraderRpcResponse)


class RelayCorrelation(_ProtocolModel):
    pair_id: str | None = None
    leg: Literal["first", "second"] | None = None
    purpose: str


class ProtectedMarketOrder(_ProtocolModel):
    action: Literal["market"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    filling_mode: Literal["FOK", "IOC"]
    sl: str
    tp: str


class WorkerReadRequested(_ProtocolModel):
    type: Literal["worker_read_requested"] = "worker_read_requested"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    trader_id: str
    worker_id: str
    runtime_command_id: str
    request_id: str
    payload_hash: str
    correlation: RelayCorrelation
    payload: TraderRead


class WorkerEffectPayload(_ProtocolModel):
    operation: Literal["order_check", "order_execute", "trader_operation"]
    order: ProtectedMarketOrder | None = None
    request: TraderOperation | None = None

    @model_validator(mode="after")
    def matching_operation(self) -> WorkerEffectPayload:
        if self.operation in {"order_check", "order_execute"} and self.order is None:
            raise ValueError("Order effects require a protected market order.")
        if self.operation == "trader_operation" and self.request is None:
            raise ValueError("Trader operation effects require a request.")
        if self.order is not None and self.request is not None:
            raise ValueError("Worker effects contain either an order or a request.")
        return self


class WorkerEffectRequested(_ProtocolModel):
    type: Literal["worker_effect_requested"] = "worker_effect_requested"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    trader_id: str
    worker_id: str
    runtime_command_id: str
    effect_id: str
    request_id: str
    expires_at: str
    payload_hash: str
    correlation: RelayCorrelation
    payload: WorkerEffectPayload

    @field_validator("expires_at")
    @classmethod
    def valid_expiry(cls, value: str) -> str:
        from datetime import datetime

        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError("Worker effect expiry must include an offset.")
        return value


class WorkerReadCompleted(_ProtocolModel):
    type: Literal["worker_read_completed"] = "worker_read_completed"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    trader_id: str
    worker_id: str
    runtime_command_id: str
    request_id: str
    payload_hash: str
    correlation: RelayCorrelation
    recovery_epoch: str
    snapshot_baseline: int
    observed_at: str
    accepted: bool
    result: dict[str, object] | None = None
    reason: str | None = None


class WorkerEffectOutcome(_ProtocolModel):
    type: Literal["worker_effect_outcome"] = "worker_effect_outcome"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    trader_id: str
    worker_id: str
    runtime_command_id: str
    effect_id: str
    request_id: str
    payload_hash: str
    correlation: RelayCorrelation
    recovery_epoch: str
    observed_at: str
    accepted: bool
    execution_state: Literal["not_started", "sent", "receipt"] | None = None
    outcome: str
    result: dict[str, object]
    reason: str | None = None


class WorkerSnapshot(_ProtocolModel):
    type: Literal["worker_snapshot"] = "worker_snapshot"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    worker_id: str
    recovery_epoch: str
    snapshot_baseline: int
    observed_at: str
    orders: list[dict[str, object]]
    positions: list[dict[str, object]]


class WorkerBrokerDelta(_ProtocolModel):
    type: Literal["worker_broker_delta"] = "worker_broker_delta"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    worker_id: str
    recovery_epoch: str
    event_id: int
    observed_at: str
    entity: Literal["order", "position"]
    change: Literal["upsert", "remove"]
    record: dict[str, object]
    originating_effect_id: str | None = None


class WorkerStreamResumed(_ProtocolModel):
    type: Literal["worker_stream_resumed"] = "worker_stream_resumed"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    worker_id: str
    recovery_epoch: str
    after_event_id: int


class WorkerStreamGap(_ProtocolModel):
    type: Literal["worker_stream_gap"] = "worker_stream_gap"
    protocol_version: Literal[1] = PROTOCOL_VERSION
    worker_id: str
    recovery_epoch: str
    expected_event_id: int
    received_event_id: int


WorkerRequestEnvelope = Annotated[WorkerReadRequested | WorkerEffectRequested, Field(discriminator="type")]
WorkerFactEnvelope = Annotated[
    WorkerReadCompleted
    | WorkerEffectOutcome
    | WorkerSnapshot
    | WorkerBrokerDelta
    | WorkerStreamResumed
    | WorkerStreamGap,
    Field(discriminator="type"),
]

worker_request_envelope_adapter = TypeAdapter(WorkerRequestEnvelope)
worker_fact_envelope_adapter = TypeAdapter(WorkerFactEnvelope)


# --------------------------------------------------------------------------- #
# Pair Execution Cell opaque Worker-to-Worker relay
# --------------------------------------------------------------------------- #
#
# The controller is an authenticated identity authority, the arbiter of
# exclusive route ownership, and an opaque control-and-data relay -- nothing
# more.  It validates the two Worker identities, that the message stays on a
# live route named by its ``route_id``, that the sending Worker holds the
# sending role on the controller's own route record, the protocol version, and
# the payload size; it then forwards the envelope unchanged.  It never inspects
# ``payload`` (policy, quotes, attempts, discovered universe content, leg
# status), never issues a trading lease or contract, never derives
# pair-lifecycle state, and never replays an execution envelope after a
# reconnect.

MAX_PAIR_RELAY_ENVELOPE_BYTES = 65_536
PAIR_CELL_PROTOCOL_VERSION = 1


class PairRouteClaim(_ProtocolModel):
    """The route fields the controller validates on every relayed message.

    Durable route identity is the controller-generated, globally unique,
    never-reused ``route_id`` plus the ordered leader/follower Worker pair.
    There is deliberately no ``trader_id``: a Pair Execution Cell route binds
    no Trader identity anywhere -- not on the route, not in an envelope, not
    in an ownership record, and not in a controller API.  (The legacy Strategy
    Runtime and Trader RPC surfaces above keep their own Trader identity
    untouched.)

    ``route_id`` is *not* a lease epoch.  It has no TTL, is never renewed,
    never expires, fences no broker effect, confers no trading authorization,
    and is never derived from or compared against a Worker's own
    ``recovery_epoch``.  It is only the durable name of one pairing
    relationship.
    """

    route_id: str = Field(min_length=1)
    leader_worker_id: str
    follower_worker_id: str


class PairCellRelayEnvelope(_ProtocolModel):
    """One opaque, versioned pair-cell message relayed Worker-to-Worker.

    ``request_id`` identifies this message instance for ordinary connection
    and delivery diagnostics only.  It is deliberately *not* a durable
    idempotency key: the controller keeps no attempt projection, so it can
    neither deduplicate against an earlier session nor replay a message from
    one.  Duplicate suppression is the receiving Worker's own durable
    responsibility.
    """

    type: Literal["pair_cell_relay"] = "pair_cell_relay"
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    route: PairRouteClaim
    from_worker_id: str
    to_worker_id: str
    request_id: str = Field(min_length=1)
    payload_hash: str
    payload: dict[str, object]

    @model_validator(mode="after")
    def route_between_the_paired_workers(self) -> PairCellRelayEnvelope:
        pair = {self.route.leader_worker_id, self.route.follower_worker_id}
        if self.from_worker_id not in pair or self.to_worker_id not in pair or self.from_worker_id == self.to_worker_id:
            raise ValueError("The pair-cell relay route must be between the routed leader and follower.")
        return self


pair_cell_relay_envelope_adapter = TypeAdapter(PairCellRelayEnvelope)


# --------------------------------------------------------------------------- #
# Pair Execution Cell Worker-facing pairing control plane
# --------------------------------------------------------------------------- #
#
# Pairing is Worker-initiated: there is no administrator-created route and no
# administrator approval of an individual pairing.  A Worker declares a desired
# role on its first unpaired connection, a leader lists currently connected,
# available, unpaired followers and proposes one, and the selected follower
# accepts or refuses from its own local safety evidence.  Every message below
# is authenticated as the sending Worker's own session; none of them carries a
# ``trader_id``.


class PairCellRoleDeclaration(_ProtocolModel):
    """A Worker's desired Pair Execution Cell role on an unpaired connection.

    An omitted ``role`` means the Worker is simply an *available follower*.
    A durable route always wins over this declaration: a Worker that is
    already routed resumes its assigned role, and a contradicting declaration
    is recorded as a diagnostic rather than silently changing who leads.
    """

    type: Literal["pair_cell_role"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    role: Literal["leader", "follower"] | None = None


class PairCellFollowerListingRequest(_ProtocolModel):
    """A leader asking for the currently connected, available, unpaired followers."""

    type: Literal["pair_cell_available_followers"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)


class PairCellPairingProposal(_ProtocolModel):
    """Phase one: a leader proposing a pairing with one selected follower."""

    type: Literal["pair_cell_pairing_proposal"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    follower_worker_id: str = Field(min_length=1)


class PairCellPairingDecision(_ProtocolModel):
    """Phase two: the reserved follower's acceptance or reasoned refusal.

    An acceptance is not a bare yes.  It carries the follower's **Pairing
    Acceptance payload** -- its own frozen startup balance and its own four
    authored risk tunables -- which the controller forwards to the reserved
    leader unchanged and never parses.  ``acceptance_payload`` is therefore
    declared as an opaque mapping and ``payload_hash`` as an opaque string:
    the controller checks that they are present, encodable, and within the
    size limit, and nothing else.  The leader copies the block verbatim into
    the canonical policy's ``follower_risk``; only the two Workers know what
    the fields mean.

    A refusal never becomes an implied acceptance: it releases the
    reservation immediately, carries no acceptance payload, and may carry an
    opaque ``reason`` the controller likewise never inspects.
    """

    type: Literal["pair_cell_pairing_decision"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    proposal_id: str = Field(min_length=1)
    accepted: bool
    acceptance_payload: dict[str, object] | None = None
    payload_hash: str | None = None
    reason: str | None = None

    @model_validator(mode="after")
    def acceptance_carries_its_payload_and_refusal_carries_its_reason(self) -> PairCellPairingDecision:
        if self.accepted:
            if self.acceptance_payload is None or not self.payload_hash:
                raise ValueError(
                    "A pairing acceptance must carry its opaque Pairing Acceptance payload and hash."
                )
            if self.reason is not None:
                raise ValueError("A pairing acceptance carries no refusal reason.")
        elif self.acceptance_payload is not None or self.payload_hash is not None:
            raise ValueError("A pairing refusal carries no Pairing Acceptance payload.")
        return self


class PairCellRouteSyncRequest(_ProtocolModel):
    """A reconnecting Worker asking the controller for its authoritative route."""

    type: Literal["pair_cell_route_sync"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)


class PairCellUnpairCommand(_ProtocolModel):
    """Either routed Worker entering or cancelling the ``UNPAIRING`` state."""

    type: Literal["pair_cell_unpair"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    action: Literal["enter", "cancel"]


class PairCellStateVersionReport(_ProtocolModel):
    """A Worker's current local attempt/effect state version for this route.

    The version advances on any new or changed local attempt or broker effect,
    which invalidates that Worker's outstanding safe-unpair assertion.  The
    controller stores the number and compares it; it never derives emptiness
    or terminality itself.
    """

    type: Literal["pair_cell_state_version"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    state_version: int = Field(ge=0)


class PairCellUnpairAssertion(_ProtocolModel):
    """One Worker's safe-unpair assertion, bound to ``route_id`` + state version.

    The assertion's *content* -- broker-verified empty, every attempt and
    local effect terminal -- is the Worker's claim from its own fresh
    evidence.  The controller records it, checks both bindings, and removes
    the route only when it holds two fresh currently-bound assertions.
    """

    type: Literal["pair_cell_unpair_assertion"]
    protocol_version: Literal[1] = PAIR_CELL_PROTOCOL_VERSION
    request_id: str = Field(min_length=1)
    route_id: str = Field(min_length=1)
    state_version: int = Field(ge=0)


PairCellControlMessage = Annotated[
    PairCellRoleDeclaration
    | PairCellFollowerListingRequest
    | PairCellPairingProposal
    | PairCellPairingDecision
    | PairCellRouteSyncRequest
    | PairCellUnpairCommand
    | PairCellStateVersionReport
    | PairCellUnpairAssertion,
    Field(discriminator="type"),
]

pair_cell_control_message_adapter = TypeAdapter(PairCellControlMessage)

PAIR_CELL_CONTROL_MESSAGE_TYPES = frozenset(
    {
        "pair_cell_role",
        "pair_cell_available_followers",
        "pair_cell_pairing_proposal",
        "pair_cell_pairing_decision",
        "pair_cell_route_sync",
        "pair_cell_unpair",
        "pair_cell_state_version",
        "pair_cell_unpair_assertion",
    }
)
