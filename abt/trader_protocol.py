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
