"""Shared, validated wire models for Trader-to-worker RPC."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator


class _ProtocolModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


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


class CalcMarginRead(_ProtocolModel):
    type: Literal["calc_margin"]
    symbol: str
    volume: str
    direction: Literal["LONG", "SHORT"]
    price: str


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


TraderRead = Annotated[
    AccountInfoRead | SymbolInfoRead | CalcMarginRead | CalcProfitRead | HistoricalTicksRead | CurrentOrdersRead | CurrentPositionsRead,
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


TraderOperation = Annotated[
    MarketOperation | PendingOperation | CancelOperation | CloseOperation | ModifySlTpOperation,
    Field(discriminator="type"),
]


class TraderRpcRequest(_ProtocolModel):
    type: Literal["trader_rpc_request"] = "trader_rpc_request"
    request_id: str
    kind: Literal["read", "operation"]
    payload: TraderRead | TraderOperation

    @model_validator(mode="after")
    def matching_kind(self) -> TraderRpcRequest:
        if self.kind == "read" and isinstance(self.payload, (MarketOperation, PendingOperation, CancelOperation, CloseOperation, ModifySlTpOperation)):
            raise ValueError("Trader read requests require a read payload.")
        if self.kind == "operation" and isinstance(
            self.payload,                         (AccountInfoRead, SymbolInfoRead, CalcMarginRead, CalcProfitRead, HistoricalTicksRead, CurrentOrdersRead, CurrentPositionsRead)
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
