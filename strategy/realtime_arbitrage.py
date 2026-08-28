"""Risk-gated realtime arbitrage across any two approved account Workers."""

from __future__ import annotations

import argparse
from base64 import b64encode
from hashlib import sha256
import json
import logging
import math
import signal
import sys
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from decimal import Decimal, ROUND_DOWN, ROUND_HALF_UP
from datetime import UTC, datetime, time, timedelta
from pathlib import Path
from queue import Queue
from threading import Condition, Event, Lock, Thread
from time import monotonic, sleep
from uuid import uuid4
from zoneinfo import ZoneInfo

from abt.controlplane.crypto import trader_proof_payload
from abt.trader.cli import HTTPTraderTransport
from abt.trader.identity import load_identity
from abt.trader.runtime import PairLegPlan, PairPlan, StrategyRuntime, StrategyRuntimeError
from abt.trader.session import open_authenticated_trader_session
from abt.trader_protocol import WorkerEffectRequested, WorkerReadRequested
from abt.worker.keystore import WindowsCNGKeyStore

NY = ZoneInfo("America/New_York")
DEFAULT_TRADER_CONFIG = str(Path(__file__).with_name("trader.json"))
_LOGGER = logging.getLogger(__name__)
_MARGIN_SIZING_INTERVAL = timedelta(minutes=10)
_MARGIN_HEADROOM_FRACTION = 0.20
_PAIR_ENTRY_EXPIRY = timedelta(seconds=5)


class StrategyError(RuntimeError):
    """Raised when the strategy cannot safely retain or create exposure."""


class WorkerMarketDataUnavailable(StrategyError):
    """Raised when the controller reports that a selected Worker's feed is unavailable."""

    def __init__(self, worker_id: str, reason: str) -> None:
        self.worker_id, self.reason = worker_id, reason
        super().__init__(f"Worker {worker_id} market data became unavailable: {reason}")


class WorkerRpcUnavailable(StrategyError):
    """Raised when a Worker cannot complete a Trader RPC because its session is unavailable."""

    def __init__(self, worker_id: str, kind: str, reason: str) -> None:
        self.worker_id, self.kind, self.reason = worker_id, kind, reason
        super().__init__(f"Worker {worker_id} {kind} RPC became unavailable: {reason}")


class HedgedEntryContained(StrategyError):
    """Raised after the controller has isolated a potentially incomplete market hedge."""


@dataclass(frozen=True, slots=True)
class Endpoint:
    worker_id: str


@dataclass(frozen=True, slots=True)
class StrategyConfig:
    entry_edge_points: float
    maximum_trades: int
    daily_loss_fraction: float
    trade_loss_fraction: float
    max_exposure_usd: float | None
    max_margin_ratio: float
    quote_max_age_seconds: float
    emergency_stop_loss_usd: float
    integrity_check_seconds: float
    minimum_hold_seconds: float
    flatten_at_ny: time
    worker_disconnect_grace_seconds: float
    worker_write_disconnect_grace_seconds: float
    execute: bool


@dataclass(slots=True)
class Account:
    balance: float
    equity: float
    day_start_equity: float


@dataclass(frozen=True, slots=True)
class SymbolSpecification:
    name: str
    trade_mode: int
    trade_calc_mode: int | str
    digits: int
    point: float
    trade_tick_size: float
    contract_size: float
    volume_min: float
    volume_step: float
    volume_max: float
    allowed_directions: frozenset[str]
    filling_modes: frozenset[str]


@dataclass(frozen=True, slots=True)
class SharedSymbol:
    point: float
    trade_tick_size: float
    filling_mode: str
    volume_min: float
    volume_step: float
    volume_max: float


@dataclass(slots=True)
class Pair:
    symbol: str
    direction: str
    first_entry: float
    second_entry: float
    first_ticket: int
    second_ticket: int
    first_direction: str
    second_direction: str
    volume: float
    opened_at: datetime
    pair_id: str | None = None
    first_stop_loss: str | None = None
    first_take_profit: str | None = None
    second_stop_loss: str | None = None
    second_take_profit: str | None = None


@dataclass(frozen=True, slots=True)
class TradeCandidate:
    symbol: str
    direction: str
    edge: float


@dataclass(frozen=True, slots=True)
class SizingPlan:
    volume: float
    first_margin_limit: float
    second_margin_limit: float
    computed_at: datetime


class TraderGateway:
    """Authenticated in-process relay adapter used by the Strategy Runtime."""

    def __init__(
        self,
        socket: object,
        *,
        trader_id: str,
        worker_ids: tuple[str, ...],
        owned_resources: tuple[object, ...] = (),
        reconnect: Callable[[], object] | None = None,
    ) -> None:
        self._socket = socket
        self.trader_id = trader_id
        self._worker_ids = worker_ids
        self._owned_resources = owned_resources
        self._reconnect = reconnect
        self._closing = Event()
        self._connection_ready = Event()
        self._connection_ready.set()
        self._cursor_provider: Callable[[str], tuple[str | None, int, bool]] | None = None
        self._send_lock = Lock()
        self._fact_delivery_lock = Lock()
        self._fact_queue: Queue[dict[str, object] | Event | None] = Queue()
        self._messages_ready = Condition()
        self._inbox: deque[dict[str, object]] = deque()
        self._receiver_error: Exception | None = None
        self._fact_delivery_error: Exception | None = None
        self._disconnect_generation = 0
        self._fact_sink: Callable[[dict[str, object]], object] | None = None
        Thread(target=self._deliver_worker_facts_forever, name="strategy-runtime-facts", daemon=True).start()
        Thread(target=self._receive_forever, name="strategy-runtime-relay", daemon=True).start()
        self._send({"type": "subscribe", "worker_ids": list(worker_ids)})
        self._wait_for(lambda message: message.get("type") == "subscribed")
        self.resume_streams()

    @classmethod
    def connect(cls, config_path: Path, *, worker_ids: tuple[str, ...]) -> TraderGateway:
        identity = load_identity(config_path)
        transport = HTTPTraderTransport()
        key_store = WindowsCNGKeyStore(identity.key_name)
        def connect_socket() -> tuple[object, str]:
            challenge = transport.certificate_challenge(identity.controller_url, identity.enrollment_id)
            trader_id = _required_gateway_text(challenge, "trader_id")
            nonce = _required_gateway_text(challenge, "nonce")
            signature = key_store.sign(
                trader_proof_payload(purpose="certificate_delivery", trader_id=trader_id, nonce=nonce)
            )
            if not isinstance(signature, bytes):
                raise StrategyError("The Windows CNG key returned an invalid signature.")
            delivery = transport.certificate(
                identity.controller_url, identity.enrollment_id, b64encode(signature).decode("ascii")
            )
            if _required_gateway_text(delivery, "trader_id") != trader_id:
                raise StrategyError("The controller returned an invalid Trader certificate.")
            socket, _ = open_authenticated_trader_session(
                controller_url=identity.controller_url,
                enrollment_id=identity.enrollment_id,
                key_store=key_store,
                certificate=(trader_id, _required_gateway_text(delivery, "certificate")),
            )
            return socket, trader_id

        try:
            socket, trader_id = connect_socket()

            def reconnect_socket() -> object:
                replacement, replacement_trader_id = connect_socket()
                if replacement_trader_id != trader_id:
                    replacement.__exit__(None, None, None)
                    raise StrategyError("Trader identity changed during relay reconnection.")
                return replacement

            return cls(
                socket,
                trader_id=trader_id,
                worker_ids=worker_ids,
                owned_resources=(key_store, transport),
                reconnect=reconnect_socket,
            )
        except Exception:
            key_store.close()
            transport.close()
            raise

    def request(
        self,
        worker_id: str,
        *,
        kind: str,
        request: dict[str, object],
        runtime_command_id: str | None = None,
        request_id: str | None = None,
        effect_id: str | None = None,
        expires_at: datetime | None = None,
        correlation: dict[str, object] | None = None,
        _include_fact: bool = False,
    ) -> dict[str, object]:
        request_id = request_id or str(uuid4())
        runtime_command_id = runtime_command_id or request_id
        correlation = correlation or {"purpose": str(request.get("type", kind))}
        canonical = json.dumps(request, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        payload_hash = sha256(canonical.encode()).hexdigest()
        request_type = request.get("type", request.get("action", kind))
        _LOGGER.debug("rpc_request worker=%s kind=%s type=%s request_id=%s", worker_id, kind, request_type, request_id)
        if kind == "read":
            envelope = WorkerReadRequested(
                trader_id=self.trader_id,
                worker_id=worker_id,
                runtime_command_id=runtime_command_id,
                request_id=request_id,
                payload_hash=payload_hash,
                correlation=correlation,
                payload=request,
            ).model_dump(mode="json")
        else:
            operation = kind if kind in {"order_check", "order_execute"} else "trader_operation"
            payload = {"operation": operation, "order": request} if operation != "trader_operation" else {
                "operation": operation, "request": request
            }
            payload_hash = sha256(
                json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode()
            ).hexdigest()
            envelope = WorkerEffectRequested(
                trader_id=self.trader_id,
                worker_id=worker_id,
                runtime_command_id=runtime_command_id,
                request_id=request_id,
                effect_id=effect_id or f"{runtime_command_id}:{worker_id}:{request_id}",
                expires_at=(expires_at or datetime.now(UTC) + timedelta(seconds=30)).isoformat(),
                payload_hash=payload_hash,
                correlation=correlation,
                payload=payload,
            ).model_dump(mode="json", exclude_none=True)
        generation = self._disconnect_generation
        self._send({"type": "relay", "worker_id": worker_id, "envelope": envelope})
        message = self._wait_for(
            lambda candidate: candidate.get("type") == "relay"
            and isinstance(candidate.get("envelope"), dict)
            and candidate["envelope"].get("request_id") == request_id,
            generation=generation,
        )
        fact = message["envelope"]
        assert isinstance(fact, dict)
        if fact.get("accepted") is not True:
            reason = fact.get("reason")
            detail = reason if isinstance(reason, str) and reason else "no rejection reason was provided"
            if kind in {"order_check", "order_execute"}:
                return {
                    "accepted": False,
                    "execution_state": fact.get("execution_state"),
                    "outcome": fact.get("outcome", "rejected"),
                    "result": fact.get("result", {}),
                    "reason": detail,
                }
            if kind == "read":
                raise WorkerRpcUnavailable(worker_id, kind, detail)
            raise StrategyError(f"Worker {worker_id} rejected {request_type}: {detail}")
        result = fact.get("result")
        if not isinstance(result, dict):
            raise StrategyError(f"Worker {worker_id} returned an invalid relay outcome.")
        if kind in {"order_check", "order_execute"}:
            return {
                "accepted": True,
                **result,
                "result": result,
                "execution_state": fact.get("execution_state"),
                "outcome": fact.get("outcome"),
            }
        if _include_fact:
            return {
                **result,
                "recovery_epoch": fact.get("recovery_epoch"),
                "snapshot_baseline": fact.get("snapshot_baseline"),
                "observed_at": fact.get("observed_at"),
            }
        return result

    def snapshot(
        self,
        worker_id: str,
        *,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(
            worker_id,
            kind="read",
            request={"type": "broker_snapshot"},
            runtime_command_id=runtime_command_id,
            request_id=request_id,
            correlation=correlation,
            _include_fact=True,
        )

    def order_check(
        self,
        worker_id: str,
        *,
        order: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(
            worker_id, kind="order_check", request=order,
            runtime_command_id=runtime_command_id, request_id=request_id,
            expires_at=expires_at, correlation=correlation,
        )

    def order_execute(
        self,
        worker_id: str,
        *,
        order: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        effect_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(
            worker_id, kind="order_execute", request=order,
            runtime_command_id=runtime_command_id, request_id=request_id,
            effect_id=effect_id, expires_at=expires_at, correlation=correlation,
        )

    def operate(
        self,
        worker_id: str,
        *,
        operation: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        effect_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(
            worker_id, kind="operation", request=operation,
            runtime_command_id=runtime_command_id, request_id=request_id,
            effect_id=effect_id, expires_at=expires_at, correlation=correlation,
        )

    def query(self, query: str) -> dict[str, object]:
        request_id = str(uuid4())
        _LOGGER.debug("controller_query query=%s request_id=%s", query, request_id)
        generation = self._disconnect_generation
        self._send({"type": "query", "request_id": request_id, "query": query})
        message = self._wait_for(
            lambda candidate: candidate.get("type") == "trader_query_result"
            and candidate.get("request_id") == request_id,
            generation=generation,
        )
        if message.get("query") != query or not isinstance(message.get("result"), dict):
            raise StrategyError(f"Controller rejected {query}.")
        return message["result"]

    def market_data(self, *, timeout_seconds: float = 1) -> dict[str, object] | None:
        """Return the next market-data event, or None while the healthy stream is quiet."""

        try:
            message = self._wait_for(
                lambda candidate: candidate.get("type") in {"worker_stream", "market_data_unavailable"},
                timeout=timeout_seconds,
            )
        except TimeoutError:
            return None
        if message.get("type") == "worker_stream":
            worker_id = message.get("worker_id")
            envelope = message.get("envelope")
            if not isinstance(worker_id, str) or not isinstance(envelope, dict):
                raise StrategyError("Controller returned an invalid Worker stream envelope.")
            envelope_type = envelope.get("type")
            if envelope_type == "live_state_snapshot":
                connectivity = envelope.get("connectivity")
                quotes = envelope.get("quotes")
                observed_at = envelope.get("observed_at")
                if not isinstance(connectivity, bool) or not isinstance(quotes, list) or not isinstance(observed_at, str):
                    raise StrategyError("Worker returned invalid live-state evidence.")
                if not connectivity:
                    raise WorkerMarketDataUnavailable(worker_id, "worker_terminal_disconnected")
                return {"type": "market_data", "worker_id": worker_id, "observed_at": observed_at, "quotes": quotes}
            if envelope_type == "live_state_diff":
                entity = envelope.get("entity")
                value = envelope.get("value")
                observed_at = envelope.get("observed_at")
                if entity == "connectivity" and value is False:
                    raise WorkerMarketDataUnavailable(worker_id, "worker_terminal_disconnected")
                if entity == "quotes" and isinstance(value, list) and isinstance(observed_at, str):
                    return {"type": "market_data", "worker_id": worker_id, "observed_at": observed_at, "quotes": value}
                return None
            raise StrategyError("Worker returned an unsupported live-state envelope.")
        worker_id, reason = message.get("worker_id"), message.get("reason")
        if not isinstance(worker_id, str) or not worker_id or not isinstance(reason, str) or not reason:
            raise StrategyError("Controller returned invalid market-data availability.")
        raise WorkerMarketDataUnavailable(worker_id, reason)

    def close(self) -> None:
        self._closing.set()
        fact_queue = getattr(self, "_fact_queue", None)
        if fact_queue is not None:
            fact_queue.put(None)
        socket = getattr(self, "_socket", None)
        if socket is not None:
            socket.__exit__(None, None, None)
        for resource in getattr(self, "_owned_resources", ()):
            close = getattr(resource, "close", None)
            if callable(close):
                close()

    def set_fact_sink(self, sink: Callable[[dict[str, object]], object]) -> None:
        with self._fact_delivery_lock:
            with self._messages_ready:
                self._fact_sink = sink
                facts = [
                    message for message in self._inbox
                    if message.get("type") == "worker_fact" and isinstance(message.get("envelope"), dict)
                ]
                for message in facts:
                    self._inbox.remove(message)
            for message in facts:
                self._fact_queue.put(message["envelope"])  # type: ignore[arg-type]

    def set_cursor_provider(
        self, provider: Callable[[str], tuple[str | None, int, bool]]
    ) -> None:
        self._cursor_provider = provider

    def configure_workers(self, worker_ids: tuple[str, str]) -> None:
        self._worker_ids = worker_ids
        self._send({"type": "subscribe", "worker_ids": list(worker_ids)})
        self._wait_for(lambda message: message.get("type") == "subscribed")

    def resume_streams(self) -> None:
        cursors = []
        for worker_id in self._worker_ids:
            if worker_id == "*":
                continue
            epoch, event_id, _fresh = (
                self._cursor_provider(worker_id)
                if self._cursor_provider is not None
                else (None, 0, False)
            )
            cursors.append(
                {"worker_id": worker_id, "recovery_epoch": epoch, "event_id": event_id}
            )
        if not cursors:
            return
        self._fact_delivery_error = None
        self._send({"type": "resume_workers", "cursors": cursors})
        self._wait_for(lambda message: message.get("type") == "workers_resumed")
        self._wait_for_fact_delivery()

    def _send(self, message: dict[str, object]) -> None:
        if not self._connection_ready.wait(timeout=30):
            raise StrategyError("Trader relay did not reconnect before the request deadline.")
        self._send_raw(message)

    def _send_raw(self, message: dict[str, object]) -> None:
        try:
            with self._send_lock:
                self._socket.send(json.dumps(message, separators=(",", ":"), sort_keys=True))
        except (AttributeError, OSError) as error:
            raise StrategyError("Trader relay is not connected.") from error

    def _receive_forever(self) -> None:
        while not self._closing.is_set():
            try:
                self._receive_connected()
            except Exception as error:
                self._connection_ready.clear()
                with self._messages_ready:
                    self._disconnect_generation += 1
                    self._receiver_error = error
                    self._messages_ready.notify_all()
                if self._reconnect is None or self._closing.is_set():
                    return
                while not self._closing.is_set():
                    try:
                        socket = self._reconnect()
                        self._socket = socket
                        self._resume_after_reconnect()
                        with self._messages_ready:
                            self._receiver_error = None
                            self._messages_ready.notify_all()
                        self._connection_ready.set()
                        break
                    except Exception:
                        sleep(1)

    def _receive_connected(self) -> None:
        while not self._closing.is_set():
            value = json.loads(self._socket.recv())
            if not isinstance(value, dict):
                raise StrategyError("Trader relay returned invalid data.")
            self._accept_incoming(value)

    def _accept_incoming(self, value: dict[str, object]) -> None:
        if value.get("type") == "heartbeat":
            self._send_raw({"type": "heartbeat"})
            return
        if value.get("type") == "worker_fact_acknowledged":
            return
        if value.get("type") == "worker_fact" and isinstance(value.get("envelope"), dict):
            with self._fact_delivery_lock:
                if self._fact_sink is not None:
                    self._fact_queue.put(value["envelope"])
                else:
                    with self._messages_ready:
                        self._inbox.append(value)
                        self._messages_ready.notify_all()
            return
        with self._messages_ready:
            self._inbox.append(value)
            self._messages_ready.notify_all()

    def _deliver_worker_facts_forever(self) -> None:
        while not self._closing.is_set():
            envelope = self._fact_queue.get()
            if envelope is None:
                return
            if isinstance(envelope, Event):
                envelope.set()
                continue
            try:
                self._deliver_worker_fact(envelope)
            except Exception as error:
                with self._messages_ready:
                    self._fact_delivery_error = error
                    self._receiver_error = error
                    self._messages_ready.notify_all()

    def _wait_for_fact_delivery(self) -> None:
        barrier = Event()
        self._fact_queue.put(barrier)
        if not barrier.wait(30):
            raise StrategyError("Timed out waiting for replayed Worker facts to become durable.")
        if self._fact_delivery_error is not None:
            raise StrategyError("A replayed Worker fact could not become durable.") from self._fact_delivery_error

    def _deliver_worker_fact(self, envelope: dict[str, object]) -> None:
        assert self._fact_sink is not None
        worker_id = envelope.get("worker_id")
        if worker_id not in self._worker_ids and "*" not in self._worker_ids:
            return
        self._fact_sink(envelope)
        event_id = envelope.get("event_id", envelope.get("snapshot_baseline"))
        recovery_epoch = envelope.get("recovery_epoch")
        if (
            isinstance(worker_id, str)
            and isinstance(recovery_epoch, str)
            and isinstance(event_id, int)
            and not isinstance(event_id, bool)
        ):
            self._send_raw(
                {
                    "type": "ack_worker_fact",
                    "worker_id": worker_id,
                    "recovery_epoch": recovery_epoch,
                    "event_id": event_id,
                }
            )

    def _resume_after_reconnect(self) -> None:
        self._send_raw({"type": "subscribe", "worker_ids": list(self._worker_ids)})
        while True:
            value = json.loads(self._socket.recv())
            if not isinstance(value, dict):
                raise StrategyError("Trader relay returned invalid data.")
            if value.get("type") == "subscribed":
                break
            self._accept_incoming(value)
        cursors = []
        for worker_id in self._worker_ids:
            if worker_id == "*":
                continue
            epoch, event_id, _fresh = (
                self._cursor_provider(worker_id)
                if self._cursor_provider is not None
                else (None, 0, False)
            )
            cursors.append({"worker_id": worker_id, "recovery_epoch": epoch, "event_id": event_id})
        if not cursors:
            return
        self._fact_delivery_error = None
        self._send_raw({"type": "resume_workers", "cursors": cursors})
        while True:
            value = json.loads(self._socket.recv())
            if not isinstance(value, dict):
                raise StrategyError("Trader relay returned invalid data.")
            if value.get("type") == "workers_resumed":
                self._wait_for_fact_delivery()
                return
            self._accept_incoming(value)

    def _wait_for(
        self,
        predicate: Callable[[dict[str, object]], bool],
        *,
        timeout: float = 30,
        generation: int | None = None,
    ) -> dict[str, object]:
        deadline = monotonic() + timeout
        with self._messages_ready:
            while True:
                if generation is not None and generation != self._disconnect_generation:
                    raise StrategyError("Trader relay disconnected before the request outcome was received.")
                for message in tuple(self._inbox):
                    if predicate(message):
                        self._inbox.remove(message)
                        return message
                if self._receiver_error is not None:
                    raise StrategyError("Trader relay disconnected or returned invalid data.") from self._receiver_error
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError
                self._messages_ready.wait(remaining)


class RealtimeArbitrage:
    """Own one pair lifecycle; the caller only supplies endpoints and configuration."""

    def __init__(
        self,
        gateway: TraderGateway,
        *,
        first: Endpoint,
        second: Endpoint,
        config: StrategyConfig,
        runtime_path: Path | str | None = None,
    ) -> None:
        if first.worker_id == second.worker_id:
            raise StrategyError("Arbitrage endpoints must use different Workers.")
        self.gateway, self.endpoints, self.config = gateway, {"first": first, "second": second}, config
        _LOGGER.info(
            "strategy_initializing first_worker=%s second_worker=%s edge_points=%.2f max_trades=%d execute=%s",
            first.worker_id, second.worker_id, config.entry_edge_points, config.maximum_trades, config.execute,
        )
        self._verify_active_workers()
        self.shared_symbols = self._load_shared_symbols()
        self._configure_live_symbols()
        self.accounts = {name: self._account(endpoint) for name, endpoint in self.endpoints.items()}
        self.exposure_caps = {
            name: min(account.equity, config.max_exposure_usd if config.max_exposure_usd is not None else account.equity)
            for name, account in self.accounts.items()
        }
        for name, exposure in self.exposure_caps.items():
            _LOGGER.info(
                "exposure_cap_configured endpoint=%s exposure=%.2f margin_limit=%.2f hard_margin_limit=%.2f",
                name,
                exposure,
                exposure * self.config.max_margin_ratio,
                exposure * self.config.max_margin_ratio * (1 - _MARGIN_HEADROOM_FRACTION),
            )
        self.quotes: dict[str, dict[str, tuple[datetime, float, float]]] = {"first": {}, "second": {}}
        self.sizing_plans: dict[tuple[str, str], SizingPlan] = {}
        self.sizing_snapshot_at: datetime | None = None
        self.integrity_check_at: datetime | None = None
        self.pair: Pair | None = None
        self.runtime = (
            StrategyRuntime(runtime_path, gateway, worker_ids=(first.worker_id, second.worker_id))
            if config.execute and runtime_path is not None
            else None
        )
        self.unavailable_endpoints: set[str] = set()
        self.worker_disconnect_deadline: datetime | None = None
        self.unresolved_write_failure = False
        self.needs_human = False
        self.awaiting_clear_symbol: str | None = None
        self.stopped = False
        self.completed_trades = 0
        self._ny_date: object | None = None
        if self.runtime is not None:
            try:
                recovered = self.runtime.recover()
            except StrategyRuntimeError as error:
                raise StrategyError(str(error)) from error
            if recovered.state == "ACTIVE" and recovered.first is not None and recovered.second is not None:
                plan = self.runtime.active_plan
                if plan is None:
                    raise StrategyError("The runtime recovered active broker facts without a durable pair plan.")
                self.pair = Pair(
                    plan.first.symbol,
                    plan.direction,
                    recovered.first.entry_price,
                    recovered.second.entry_price,
                    recovered.first.ticket,
                    recovered.second.ticket,
                    plan.first.direction,
                    plan.second.direction,
                    float(plan.first.volume),
                    datetime.now(UTC),
                    plan.pair_id,
                    plan.first.stop_loss,
                    plan.first.take_profit,
                    plan.second.stop_loss,
                    plan.second.take_profit,
                )
            elif recovered.state == "NEEDS_HUMAN":
                self.needs_human = True
                self.stopped = True
                raise StrategyError(recovered.reason or "Strategy Runtime recovery requires operator action.")
        else:
            self._assert_empty_accounts()
        _LOGGER.info("strategy_initialized")

    def run(self, stop: Event) -> None:
        _LOGGER.info("strategy_running")
        while not stop.is_set() and not self.stopped:
            try:
                self._reset_daily_equity()
            except WorkerRpcUnavailable as error:
                self._suspend_for_worker_disconnect(error)
                continue
            if self._cutoff_reached():
                self._halt(f"New York daily cutoff reached at {self.config.flatten_at_ny:%H:%M}")
            if self.worker_disconnect_deadline is not None and datetime.now(UTC) >= self.worker_disconnect_deadline:
                self._halt("worker market data did not recover before disconnect grace deadline")
            try:
                message = self.gateway.market_data()
            except (WorkerMarketDataUnavailable, WorkerRpcUnavailable) as error:
                self._suspend_for_worker_disconnect(error)
                continue
            if message is None:
                continue
            if self._consume(message):
                self._recover_worker_market_data(message)
            if self.unavailable_endpoints:
                continue
            try:
                if self.pair is not None and self.config.execute:
                    self._check_active_pair_integrity()
                else:
                    self._check_integrity_if_due()
                if self.pair is not None and self._quotes_fresh(self.pair.symbol) and self._risk_breached():
                    _LOGGER.warning("risk_limit_triggered completed_trades=%d", self.completed_trades)
                    self._halt("risk limit reached")
                if self.pair is not None:
                    candidate = self._candidate_for_symbol(self.pair.symbol)
                    if candidate is not None and candidate.direction != self.pair.direction:
                        if not self._minimum_hold_elapsed():
                            remaining = self.config.minimum_hold_seconds - (datetime.now(UTC) - self.pair.opened_at).total_seconds()
                            _LOGGER.info(
                                "reverse_signal_deferred_minimum_hold old_direction=%s new_direction=%s remaining_seconds=%.1f",
                                self.pair.direction,
                                candidate.direction,
                                max(0, remaining),
                            )
                            continue
                        _LOGGER.info(
                            "reverse_signal_close symbol=%s old_direction=%s new_direction=%s edge=%.10g "
                            "first_bid/ask=%s second_bid/ask=%s",
                            self.pair.symbol,
                            self.pair.direction,
                            candidate.direction,
                            candidate.edge,
                            self._quote_text("first", self.pair.symbol),
                            self._quote_text("second", self.pair.symbol),
                        )
                        self._flatten_all()
                        self.completed_trades += 1
                        self.awaiting_clear_symbol = candidate.symbol
                        if self.completed_trades >= self.config.maximum_trades:
                            _LOGGER.info(
                                "maximum_trades_completed completed_trades=%d",
                                self.completed_trades,
                            )
                            return
                    continue
                self._refresh_sizing_snapshot_if_due()
                candidate = self._best_candidate()
                if self.awaiting_clear_symbol is not None:
                    if self._candidate_for_symbol(self.awaiting_clear_symbol) is None:
                        _LOGGER.info("reentry_rearmed")
                        self.awaiting_clear_symbol = None
                    elif candidate is None or candidate.symbol == self.awaiting_clear_symbol:
                        continue
                if candidate is not None and self.completed_trades < self.config.maximum_trades:
                    self._open(candidate)
                elif candidate is not None:
                    _LOGGER.info("entry_skipped_trade_limit completed_trades=%d maximum_trades=%d", self.completed_trades, self.config.maximum_trades)
            except WorkerRpcUnavailable as error:
                self._suspend_for_worker_disconnect(error)

    def shutdown(self) -> None:
        _LOGGER.info("strategy_shutdown_requested execute=%s", self.config.execute)
        if self.config.execute and not self.needs_human:
            self._flatten_all()
        _LOGGER.info("strategy_shutdown_complete")

    def _account(self, endpoint: Endpoint) -> Account:
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "account_info"})
        value = result.get("account")
        if not isinstance(value, dict):
            raise StrategyError(f"Worker {endpoint.worker_id} returned no account.")
        equity = _positive(value.get("equity"), "equity")
        balance = _positive(value.get("balance"), "balance")
        _LOGGER.info("account_loaded worker=%s balance=%.2f equity=%.2f", endpoint.worker_id, balance, equity)
        return Account(balance, equity, equity)

    def _verify_active_workers(self) -> None:
        result = self.gateway.query("active_workers")
        workers = result.get("workers")
        if not isinstance(workers, list):
            raise StrategyError("Controller returned invalid active Worker inventory.")
        _LOGGER.info("active_workers_loaded count=%d", len(workers))
        active = {
            worker.get("worker_id"): worker
            for worker in workers
            if isinstance(worker, dict) and isinstance(worker.get("worker_id"), str)
        }
        for endpoint in self.endpoints.values():
            worker = active.get(endpoint.worker_id)
            if not isinstance(worker, dict) or worker.get("connectivity") != "connected":
                raise StrategyError(f"Worker {endpoint.worker_id} is not active and routable.")
            _LOGGER.info("worker_verified worker=%s server=%s", endpoint.worker_id, worker.get("server"))

    def _load_shared_symbols(self) -> dict[str, SharedSymbol]:
        catalogs = {name: self._catalog_specifications(endpoint) for name, endpoint in self.endpoints.items()}
        shared: dict[str, SharedSymbol] = {}
        for symbol in sorted(catalogs["first"].keys() & catalogs["second"].keys()):
            first, second = catalogs["first"][symbol], catalogs["second"][symbol]
            mismatches = _hard_specification_mismatches(first, second)
            if mismatches:
                _LOGGER.info(
                    "shared_symbol_excluded symbol=%s reason=hard_specification_mismatch fields=%s",
                    symbol,
                    ",".join(mismatches),
                )
                continue
            if not {"LONG", "SHORT"}.issubset(first.allowed_directions):
                _LOGGER.info("shared_symbol_excluded symbol=%s reason=bidirectional_trading_unavailable", symbol)
                continue
            filling_mode = _preferred_shared_filling_mode(first.filling_modes, second.filling_modes)
            if filling_mode is None:
                _LOGGER.info("shared_symbol_excluded symbol=%s reason=no_shared_fok_or_ioc", symbol)
                continue
            shared[symbol] = SharedSymbol(
                point=first.point,
                trade_tick_size=first.trade_tick_size,
                filling_mode=filling_mode,
                volume_min=first.volume_min,
                volume_step=first.volume_step,
                volume_max=min(first.volume_max, second.volume_max),
            )
        if not shared:
            raise StrategyError("Selected Workers have no shared tradable symbols.")
        _LOGGER.info("shared_symbols_loaded count=%d", len(shared))
        return shared

    def _configure_live_symbols(self) -> None:
        symbols = sorted(self.shared_symbols)
        for endpoint in self.endpoints.values():
            try:
                result = self.gateway.request(
                    endpoint.worker_id,
                    kind="operation",
                    request={"type": "set_live_symbols", "symbols": symbols},
                )
            except StrategyError as error:
                raise StrategyError(f"Worker {endpoint.worker_id} rejected live symbol configuration.") from error
            if result.get("symbols") != symbols:
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid live symbol configuration.")
            _LOGGER.info("live_symbols_configured worker=%s count=%d", endpoint.worker_id, len(symbols))

    def _catalog_specifications(self, endpoint: Endpoint) -> dict[str, SymbolSpecification]:
        try:
            result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": "symbols"})
        except StrategyError as error:
            raise StrategyError(f"Worker {endpoint.worker_id} returned no symbol catalog.") from error
        symbols = result.get("symbols")
        if not isinstance(symbols, list):
            raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
        specifications: dict[str, SymbolSpecification] = {}
        for symbol in symbols:
            if not isinstance(symbol, dict):
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            trade_mode = symbol.get("trade_mode")
            if isinstance(trade_mode, bool) or not isinstance(trade_mode, int):
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            if trade_mode != 4:
                continue
            specification = _symbol_specification(symbol)
            if specification.name in specifications:
                raise StrategyError(f"Worker {endpoint.worker_id} returned an invalid symbol catalog.")
            specifications[specification.name] = specification
        return specifications

    def _consume(self, message: dict[str, object]) -> bool:
        worker, observed_at, quotes = message.get("worker_id"), message.get("observed_at"), message.get("quotes")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker), None)
        if name is None or not isinstance(observed_at, str) or not isinstance(quotes, list):
            return False
        try:
            timestamp = _utc(observed_at)
        except (StrategyError, ValueError):
            _LOGGER.warning("quote_ignored endpoint=%s reason=invalid_observed_at", name)
            return False
        consumed = False
        for quote in quotes:
            if not isinstance(quote, dict) or not isinstance(quote.get("symbol"), str) or not quote["symbol"]:
                continue
            try:
                bid, ask = _positive(quote.get("bid"), "bid"), _positive(quote.get("ask"), "ask")
            except StrategyError:
                _LOGGER.warning("quote_ignored endpoint=%s symbol=%s reason=invalid_price", name, quote["symbol"])
                continue
            self.quotes[name][quote["symbol"]] = (timestamp, bid, ask)
            _LOGGER.debug("quote_updated endpoint=%s symbol=%s bid=%s ask=%s observed_at=%s", name, quote["symbol"], bid, ask, observed_at)
            consumed = True
        return consumed

    def _suspend_for_worker_disconnect(self, error: WorkerMarketDataUnavailable | WorkerRpcUnavailable) -> None:
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == error.worker_id), None)
        if name is None:
            raise StrategyError(f"Controller reported market-data loss for unselected Worker {error.worker_id}.")
        self.quotes[name].clear()
        self.unavailable_endpoints.add(name)
        grace_seconds = (
            self.config.worker_write_disconnect_grace_seconds
            if isinstance(error, WorkerRpcUnavailable) and error.kind == "operation"
            else self.config.worker_disconnect_grace_seconds
        )
        if isinstance(error, WorkerRpcUnavailable) and error.kind == "operation":
            self.unresolved_write_failure = True
        deadline = datetime.now(UTC) + timedelta(seconds=grace_seconds)
        if self.worker_disconnect_deadline is None or deadline < self.worker_disconnect_deadline:
            self.worker_disconnect_deadline = deadline
            _LOGGER.warning(
                "worker_suspended endpoint=%s worker=%s source=%s reason=%s grace_seconds=%.0f deadline=%s",
                name,
                error.worker_id,
                "write_rpc" if isinstance(error, WorkerRpcUnavailable) and error.kind == "operation" else (
                    "read_rpc" if isinstance(error, WorkerRpcUnavailable) else "market_data"
                ),
                error.reason,
                grace_seconds,
                deadline.isoformat(),
            )
        else:
            _LOGGER.warning(
                "worker_market_data_still_unavailable endpoint=%s worker=%s reason=%s deadline=%s",
                name,
                error.worker_id,
                error.reason,
                self.worker_disconnect_deadline.isoformat(),
            )

    def _recover_worker_market_data(self, message: dict[str, object]) -> None:
        worker_id = message.get("worker_id")
        name = next((key for key, endpoint in self.endpoints.items() if endpoint.worker_id == worker_id), None)
        if name is None or name not in self.unavailable_endpoints:
            return
        self.unavailable_endpoints.remove(name)
        if not self.unavailable_endpoints:
            _LOGGER.info("worker_market_data_recovered endpoint=%s worker=%s", name, worker_id)
            self.worker_disconnect_deadline = None
            if self.unresolved_write_failure:
                self._halt("Worker operation outcome was unavailable after reconnect")

    def _quotes_fresh(self, symbol: str) -> bool:
        values = [self.quotes[name].get(symbol) for name in ("first", "second")]
        return all(value is not None and (datetime.now(UTC) - value[0]).total_seconds() <= self.config.quote_max_age_seconds for value in values)

    def _candidate_for_symbol(self, symbol: str) -> TradeCandidate | None:
        specification = self.shared_symbols.get(symbol)
        if specification is None or not self._quotes_fresh(symbol):
            return None
        _, first_bid, first_ask = self.quotes["first"][symbol]
        _, second_bid, second_ask = self.quotes["second"][symbol]
        threshold = self.config.entry_edge_points * specification.point
        candidates = (
            TradeCandidate(symbol, "short_first_long_second", first_bid - second_ask),
            TradeCandidate(symbol, "long_first_short_second", second_bid - first_ask),
        )
        eligible = [candidate for candidate in candidates if candidate.edge + 1e-12 >= threshold]
        return min(eligible, key=lambda candidate: (-candidate.edge, candidate.direction)) if eligible else None

    def _best_candidate(self) -> TradeCandidate | None:
        symbols = sorted(self.shared_symbols.keys() & self.quotes["first"].keys() & self.quotes["second"].keys())
        candidates = [candidate for symbol in symbols if (candidate := self._candidate_for_symbol(symbol)) is not None]
        return min(candidates, key=lambda candidate: (-candidate.edge, candidate.symbol, candidate.direction)) if candidates else None

    def _refresh_sizing_snapshot_if_due(self) -> None:
        now = datetime.now(UTC)
        if self.sizing_snapshot_at is not None and now - self.sizing_snapshot_at < _MARGIN_SIZING_INTERVAL:
            return
        symbols = sorted(self.shared_symbols)
        if not all(self._quotes_fresh(symbol) for symbol in symbols):
            return
        calculations = {
            name: [
                {
                    "symbol": symbol,
                    "volume": "1.00",
                    "direction": direction,
                    "price": f"{(self.quotes[name][symbol][2] if direction == 'LONG' else self.quotes[name][symbol][1]):.10f}",
                }
                for symbol in symbols
                for direction in ("LONG", "SHORT")
            ]
            for name in self.endpoints
        }
        margins = {
            name: self._margin_batch(
                name,
                calculations[name],
                self.gateway.request(
                    endpoint.worker_id,
                    kind="read",
                    request={"type": "calc_margin_batch", "calculations": calculations[name]},
                ),
            )
            for name, endpoint in self.endpoints.items()
        }
        first_margin_limit = self._hard_margin_limit("first")
        second_margin_limit = self._hard_margin_limit("second")
        plans: dict[tuple[str, str], SizingPlan] = {}
        for symbol, specification in self.shared_symbols.items():
            for direction, first_direction, second_direction in (
                ("short_first_long_second", "SHORT", "LONG"),
                ("long_first_short_second", "LONG", "SHORT"),
            ):
                maximum = min(
                    specification.volume_max,
                    first_margin_limit / margins["first"][symbol, first_direction],
                    second_margin_limit / margins["second"][symbol, second_direction],
                )
                volume = _floor_volume(maximum, specification.volume_min, specification.volume_step)
                if volume is not None:
                    plans[symbol, direction] = SizingPlan(
                        volume=volume,
                        first_margin_limit=first_margin_limit,
                        second_margin_limit=second_margin_limit,
                        computed_at=now,
                    )
        self.sizing_plans, self.sizing_snapshot_at = plans, now
        _LOGGER.info(
            "sizing_snapshot_refreshed symbols=%d executable_plans=%d hard_headroom_percent=%.0f",
            len(symbols),
            len(plans),
            _MARGIN_HEADROOM_FRACTION * 100,
        )

    def _margin_batch(
        self,
        name: str,
        calculations: list[dict[str, str]],
        result: dict[str, object],
    ) -> dict[tuple[str, str], float]:
        returned = result.get("margins")
        if not isinstance(returned, list) or len(returned) != len(calculations):
            raise StrategyError(f"Worker {self.endpoints[name].worker_id} returned an invalid margin batch.")
        expected = {(calculation["symbol"], calculation["direction"]): calculation for calculation in calculations}
        margins: dict[tuple[str, str], float] = {}
        for calculation in returned:
            if not isinstance(calculation, dict):
                raise StrategyError(f"Worker {self.endpoints[name].worker_id} returned an invalid margin batch.")
            key = calculation.get("symbol"), calculation.get("direction")
            requested = expected.get(key) if isinstance(key[0], str) and isinstance(key[1], str) else None
            if (
                requested is None
                or key in margins
                or set(calculation) != {"symbol", "volume", "direction", "price", "margin"}
                or any(calculation.get(field) != requested[field] for field in ("symbol", "volume", "direction", "price"))
            ):
                raise StrategyError(f"Worker {self.endpoints[name].worker_id} returned an invalid margin batch.")
            margins[key] = _positive(calculation.get("margin"), "calculated margin")
        if set(margins) != set(expected):
            raise StrategyError(f"Worker {self.endpoints[name].worker_id} returned an incomplete margin batch.")
        return margins

    def _hard_margin_limit(self, name: str) -> float:
        return self.exposure_caps[name] * self.config.max_margin_ratio * (1 - _MARGIN_HEADROOM_FRACTION)

    def _sizing_plan_for(self, symbol: str, direction: str) -> SizingPlan | None:
        plan = self.sizing_plans.get((symbol, direction))
        if plan is None or datetime.now(UTC) - plan.computed_at >= _MARGIN_SIZING_INTERVAL:
            return None
        return plan

    def _quote_text(self, name: str, symbol: str) -> str:
        quote = self.quotes[name].get(symbol)
        return "unavailable" if quote is None else f"{quote[1]:.10g}/{quote[2]:.10g}"

    def _open(self, candidate: TradeCandidate) -> None:
        symbol, direction = candidate.symbol, candidate.direction
        first_direction, second_direction = ("SHORT", "LONG") if direction == "short_first_long_second" else ("LONG", "SHORT")
        if any(self._remaining_daily_loss(name) <= 0 for name in self.endpoints):
            self._halt("daily loss budget exhausted")
        plan = self._sizing_plan_for(symbol, direction)
        if plan is None or plan.volume <= 0:
            _LOGGER.info("entry_skipped_no_current_sizing_plan symbol=%s direction=%s", symbol, direction)
            return
        volume = plan.volume
        _LOGGER.info(
            "entry_signal symbol=%s direction=%s edge=%.10g first_side=%s bid/ask=%s "
            "second_side=%s bid/ask=%s volume=%s",
            symbol,
            direction,
            candidate.edge,
            "SELL" if first_direction == "SHORT" else "BUY",
            self._quote_text("first", symbol),
            "SELL" if second_direction == "SHORT" else "BUY",
            self._quote_text("second", symbol),
            _volume_text(volume, self.shared_symbols[symbol].volume_step),
        )
        try:
            if not self.config.execute:
                _, first_bid, first_ask = self.quotes["first"][symbol]
                _, second_bid, second_ask = self.quotes["second"][symbol]
                self.pair = Pair(
                    symbol, direction, first_bid if first_direction == "SHORT" else first_ask,
                    second_ask if second_direction == "LONG" else second_bid,
                    0, 0, first_direction, second_direction,
                    volume,
                    datetime.now(UTC),
                )
                return
            specification = self.shared_symbols[symbol]
            first_entry = self._entry_price("first", symbol, first_direction)
            second_entry = self._entry_price("second", symbol, second_direction)
            first_stop_loss, first_take_profit = self._initial_protection_targets(
                "first", symbol, first_direction, first_entry, volume
            )
            second_stop_loss, second_take_profit = self._initial_protection_targets(
                "second", symbol, second_direction, second_entry, volume
            )
            try:
                if self.runtime is None:
                    self.runtime = StrategyRuntime(
                        ":memory:",
                        self.gateway,
                        worker_ids=(self.endpoints["first"].worker_id, self.endpoints["second"].worker_id),
                    )
                    self.runtime.recover()
                command_id = str(uuid4())
                pair_plan = PairPlan(
                    pair_id=command_id,
                    command_id=command_id,
                    direction=direction,
                    expires_at=datetime.now(UTC) + _PAIR_ENTRY_EXPIRY,
                    first=PairLegPlan(
                        "first", self.endpoints["first"].worker_id, symbol, first_direction,
                        _volume_text(volume, specification.volume_step), specification.filling_mode,
                        f"{plan.first_margin_limit:.2f}", first_stop_loss, first_take_profit,
                    ),
                    second=PairLegPlan(
                        "second", self.endpoints["second"].worker_id, symbol, second_direction,
                        _volume_text(volume, specification.volume_step), specification.filling_mode,
                        f"{plan.second_margin_limit:.2f}", second_stop_loss, second_take_profit,
                    ),
                )
                result = self.runtime.enter(pair_plan)
            except (StrategyError, StrategyRuntimeError) as error:
                self.stopped = True
                self.needs_human = True
                raise HedgedEntryContained(f"Strategy Runtime could not prove safe entry: {error}") from error
            if result.status == "empty":
                _LOGGER.info("entry_rejected_preflight symbol=%s direction=%s reason=%s", symbol, direction, result.reason)
                return
            if result.status != "active" or result.first is None or result.second is None:
                self.stopped = True
                self.needs_human = True
                raise HedgedEntryContained(result.reason or "Strategy Runtime could not verify both protected legs.")
            self.pair = Pair(
                symbol,
                direction,
                result.first.entry_price,
                result.second.entry_price,
                result.first.ticket,
                result.second.ticket,
                first_direction,
                second_direction,
                volume,
                datetime.now(UTC),
                pair_plan.pair_id,
                first_stop_loss,
                first_take_profit,
                second_stop_loss,
                second_take_profit,
            )
            _LOGGER.info(
                "pair_active_verified command_id=%s pair_id=%s first_ticket=%s second_ticket=%s",
                pair_plan.command_id,
                pair_plan.pair_id,
                result.first.ticket,
                result.second.ticket,
            )
            return
        except WorkerRpcUnavailable:
            raise
        except HedgedEntryContained:
            raise
        except Exception as error:
            self._halt(f"unhedged entry: {error}")
    def _entry_price(self, name: str, symbol: str, direction: str) -> float:
        quote = self.quotes[name].get(symbol)
        if quote is None:
            raise StrategyError(f"Worker {self.endpoints[name].worker_id} has no current entry quote.")
        return quote[2] if direction == "LONG" else quote[1]

    def _initial_protection_targets(
        self, name: str, symbol: str, direction: str, entry: float, volume: float
    ) -> tuple[str, str]:
        endpoint = self.endpoints[name]
        loss_limit = min(
            self.config.emergency_stop_loss_usd,
            self.exposure_caps[name] * self.config.trade_loss_fraction,
            self._remaining_daily_loss(name),
        )
        if loss_limit <= 0:
            raise StrategyError(f"Cannot protect {name} position: daily loss budget is exhausted.")
        specification = self.shared_symbols[symbol]
        sl = _tick_price_text(
            self._profit_target_price(endpoint, symbol, direction, entry, volume, -loss_limit),
            specification.trade_tick_size,
        )
        tp = _tick_price_text(
            self._profit_target_price(endpoint, symbol, direction, entry, volume, loss_limit),
            specification.trade_tick_size,
        )
        _LOGGER.info(
            "initial_protection_calculated endpoint=%s sl=%.10f tp=%.10f magnitude_usd=%.2f",
            name,
            float(sl),
            float(tp),
            loss_limit,
        )
        return sl, tp

    def _profit_target_price(
        self, endpoint: Endpoint, symbol: str, direction: str, entry: float, volume: float, target_profit: float
    ) -> float:
        lower, upper = entry * 0.5, entry * 1.5
        for _ in range(32):
            candidate = (lower + upper) / 2
            result = self.gateway.request(
                endpoint.worker_id,
                kind="read",
                request={
                    "type": "calc_profit", "symbol": symbol,
                    "volume": _volume_text(volume, self.shared_symbols[symbol].volume_step),
                    "direction": direction, "open_price": f"{entry:.10f}", "close_price": f"{candidate:.10f}",
                },
            )
            profit = result.get("profit")
            if isinstance(profit, bool) or not isinstance(profit, (int, float)):
                raise StrategyError("Worker returned invalid calculated profit.")
            profit_increases_with_price = direction == "LONG"
            if (profit < target_profit) == profit_increases_with_price:
                lower = candidate
            else:
                upper = candidate
        return (lower + upper) / 2

    def _risk_breached(self) -> bool:
        assert self.pair is not None
        for name, pnl in self._pnl().items():
            account = self.accounts[name]
            trade_loss_limit = self.exposure_caps[name] * self.config.trade_loss_fraction
            if pnl <= -trade_loss_limit or pnl <= -self._remaining_daily_loss(name):
                _LOGGER.warning("risk_breach endpoint=%s pnl=%.2f marked_equity=%.2f day_start_equity=%.2f", name, pnl, account.equity + pnl, account.day_start_equity)
                return True
        return False

    def _remaining_daily_loss(self, name: str) -> float:
        account = self.accounts[name]
        daily_loss_limit = self.exposure_caps[name] * self.config.daily_loss_fraction
        realized_loss = max(0, account.day_start_equity - account.equity)
        return max(0, daily_loss_limit - realized_loss)

    def _minimum_hold_elapsed(self) -> bool:
        assert self.pair is not None
        return (datetime.now(UTC) - self.pair.opened_at).total_seconds() >= self.config.minimum_hold_seconds

    def _cutoff_reached(self, now: datetime | None = None) -> bool:
        return (now or datetime.now(NY)).astimezone(NY).time() >= self.config.flatten_at_ny

    def _assert_empty_accounts(self) -> None:
        for name, endpoint in self.endpoints.items():
            positions = self._records(endpoint, "current_positions", "positions")
            orders = self._records(endpoint, "current_orders", "orders")
            if positions or orders:
                raise StrategyError(f"Worker {endpoint.worker_id} has existing positions or orders at startup.")
            _LOGGER.info("account_integrity_verified endpoint=%s worker=%s", name, endpoint.worker_id)

    def _check_integrity_if_due(self) -> None:
        if self.integrity_check_at is not None and datetime.now(UTC) < self.integrity_check_at:
            return
        self._check_integrity()

    def _check_active_pair_integrity(self) -> None:
        if self.pair is not None and self.config.execute:
            self._check_integrity()

    def _check_integrity(self) -> None:
        if self.runtime is not None and self.pair is not None and self.config.execute:
            result = self.runtime.verify_active()
            if result.state != "ACTIVE":
                self.pair = None
                self.stopped = True
                raise StrategyError(result.reason or "Protected pair integrity diverged; runtime converged toward empty.")
            self.integrity_check_at = datetime.now(UTC) + timedelta(seconds=self.config.integrity_check_seconds)
            return
        for name, endpoint in self.endpoints.items():
            positions = self._records(endpoint, "current_positions", "positions")
            orders = self._records(endpoint, "current_orders", "orders")
            expected_ticket = None if self.pair is None or not self.config.execute else (
                self.pair.first_ticket if name == "first" else self.pair.second_ticket
            )
            expected_direction = None if self.pair is None else (
                self.pair.first_direction if name == "first" else self.pair.second_direction
            )
            expected_type = 0 if expected_direction == "LONG" else 1
            expected_position = (
                len(positions) == 1
                and _ticket(positions[0]) == expected_ticket
                and positions[0].get("symbol") == self.pair.symbol
                and positions[0].get("type") == expected_type
                and _volume_matches(positions[0].get("volume"), self.pair.volume)
            )
            if orders or (expected_ticket is None and positions) or (expected_ticket is not None and not expected_position):
                _LOGGER.warning(
                    "external_account_change endpoint=%s worker=%s positions=%d orders=%d expected_ticket=%s",
                    name, endpoint.worker_id, len(positions), len(orders), expected_ticket,
                )
                self._halt("external position or order detected")
        self.integrity_check_at = datetime.now(UTC) + timedelta(seconds=self.config.integrity_check_seconds)

    def _records(self, endpoint: Endpoint, request_type: str, field: str) -> list[dict[str, object]]:
        result = self.gateway.request(endpoint.worker_id, kind="read", request={"type": request_type})
        values = result.get(field)
        if not isinstance(values, list) or not all(isinstance(value, dict) for value in values):
            raise StrategyError(f"Worker {endpoint.worker_id} returned invalid {field}.")
        return values

    def _pnl(self) -> dict[str, float]:
        assert self.pair is not None
        _, first_bid, first_ask = self.quotes["first"][self.pair.symbol]
        _, second_bid, second_ask = self.quotes["second"][self.pair.symbol]
        return {
            "first": self._calculated_profit(
                "first",
                self.pair.first_direction,
                self.pair.first_entry,
                first_ask if self.pair.first_direction == "SHORT" else first_bid,
            ),
            "second": self._calculated_profit(
                "second",
                self.pair.second_direction,
                self.pair.second_entry,
                second_ask if self.pair.second_direction == "SHORT" else second_bid,
            ),
        }

    def _calculated_profit(self, name: str, direction: str, open_price: float, close_price: float) -> float:
        assert self.pair is not None
        endpoint = self.endpoints[name]
        result = self.gateway.request(
            endpoint.worker_id,
            kind="read",
            request={
                "type": "calc_profit",
                "symbol": self.pair.symbol,
                "volume": _volume_text(self.pair.volume, self.shared_symbols[self.pair.symbol].volume_step),
                "direction": direction,
                "open_price": f"{open_price:.10f}",
                "close_price": f"{close_price:.10f}",
            },
        )
        profit = result.get("profit")
        if isinstance(profit, bool) or not isinstance(profit, (int, float)) or not math.isfinite(profit):
            raise StrategyError("Worker returned invalid calculated profit.")
        return float(profit)

    def _flatten_all(self) -> None:
        if not self.config.execute:
            return
        if self.runtime is None:
            raise StrategyError("Durable Strategy Runtime is unavailable; refusing direct account mutation.")
        try:
            result = self.runtime.desire_empty("strategy-requested-close")
        except StrategyRuntimeError as error:
            raise StrategyError(str(error)) from error
        if result.state != "EMPTY":
            raise StrategyError(result.reason or f"Strategy Runtime close ended in {result.state}.")
        self.pair = None
        _LOGGER.info("pair_empty_verified")

    def _halt(self, reason: str) -> None:
        self.stopped = True
        _LOGGER.warning("strategy_halt reason=%s", reason)
        try:
            self._flatten_all()
        except StrategyError as error:
            self.needs_human = True
            raise StrategyError(f"{reason}; Strategy Runtime close convergence unavailable: {error}") from error
        raise StrategyError(reason)

    def _reset_daily_equity(self) -> None:
        today = datetime.now(NY).date()
        if today != self._ny_date:
            _LOGGER.info("daily_equity_reset ny_date=%s", today)
            for name, endpoint in self.endpoints.items():
                self.accounts[name] = self._account(endpoint)
                self.accounts[name].day_start_equity = self.accounts[name].equity
            self._ny_date = today


def _positive(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value <= 0:
        raise StrategyError(f"Invalid {field}.")
    return float(value)


def _tick_price_text(price: float, tick_size: float) -> str:
    tick = Decimal(str(tick_size))
    normalized = (Decimal(str(price)) / tick).to_integral_value(rounding=ROUND_HALF_UP) * tick
    return format(normalized.normalize(), "f")


def _positive_finite(value: object) -> bool:
    return not isinstance(value, bool) and isinstance(value, (int, float)) and math.isfinite(value) and value > 0


def _floor_volume(maximum: float, minimum: float, step: float) -> float | None:
    maximum_decimal, minimum_decimal, step_decimal = map(Decimal, map(str, (maximum, minimum, step)))
    if maximum_decimal < minimum_decimal:
        return None
    steps = ((maximum_decimal - minimum_decimal) / step_decimal).to_integral_value(rounding=ROUND_DOWN)
    return float(minimum_decimal + steps * step_decimal)


def _volume_text(volume: float, step: float) -> str:
    places = max(0, -Decimal(str(step)).as_tuple().exponent)
    return f"{volume:.{places}f}"


def _volume_matches(value: object, expected: float) -> bool:
    return (
        not isinstance(value, bool)
        and isinstance(value, (int, float))
        and math.isclose(float(value), expected, rel_tol=0, abs_tol=1e-10)
    )


def _symbol_specification(value: object) -> SymbolSpecification:
    if not isinstance(value, dict):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    name = value.get("name")
    trade_mode = value.get("trade_mode")
    trade_calc_mode = value.get("trade_calc_mode")
    digits = value.get("digits")
    if not isinstance(name, str) or not name:
        raise StrategyError("Worker returned an invalid symbol catalog.")
    if isinstance(trade_mode, bool) or not isinstance(trade_mode, int):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    if isinstance(trade_calc_mode, bool) or not isinstance(trade_calc_mode, (int, str)):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    if isinstance(trade_calc_mode, str) and not trade_calc_mode:
        raise StrategyError("Worker returned an invalid symbol catalog.")
    if isinstance(digits, bool) or not isinstance(digits, int):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    return SymbolSpecification(
        name=name,
        trade_mode=trade_mode,
        trade_calc_mode=trade_calc_mode,
        digits=digits,
        point=_required_positive_catalog_number(value, "point"),
        trade_tick_size=_required_positive_catalog_number(value, "trade_tick_size"),
        contract_size=_required_positive_catalog_number(value, "trade_contract_size"),
        volume_min=_required_positive_catalog_number(value, "volume_min"),
        volume_step=_required_positive_catalog_number(value, "volume_step"),
        volume_max=_required_positive_catalog_number(value, "volume_max"),
        allowed_directions=_catalog_allowed_directions(value, trade_mode),
        filling_modes=_catalog_filling_modes(value),
    )


def _required_positive_catalog_number(value: dict[str, object], field: str) -> float:
    number = value.get(field)
    if not _positive_finite(number):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    return float(number)


def _catalog_allowed_directions(value: dict[str, object], trade_mode: int | str) -> frozenset[str]:
    order_mode = value.get("order_mode")
    if order_mode is not None:
        if isinstance(order_mode, bool) or not isinstance(order_mode, int):
            raise StrategyError("Worker returned an invalid symbol catalog.")
        directions = frozenset(
            direction
            for bit, direction in ((1, "LONG"), (2, "SHORT"))
            if order_mode & bit
        )
        if directions:
            return directions
    if isinstance(trade_mode, int):
        directions_by_trade_mode = {
            1: frozenset({"LONG"}),
            2: frozenset({"SHORT"}),
            4: frozenset({"LONG", "SHORT"}),
        }
        if trade_mode in directions_by_trade_mode:
            return directions_by_trade_mode[trade_mode]
    raise StrategyError("Worker returned an invalid symbol catalog.")


def _catalog_filling_modes(value: dict[str, object]) -> frozenset[str]:
    filling_mode = value.get("filling_mode")
    if isinstance(filling_mode, bool) or not isinstance(filling_mode, int):
        raise StrategyError("Worker returned an invalid symbol catalog.")
    modes = frozenset(
        mode
        for bit, mode in ((1, "FOK"), (2, "IOC"), (4, "BOC"))
        if filling_mode & bit
    )
    if modes:
        return modes
    legacy_modes = {0: frozenset({"FOK"})}.get(filling_mode)
    if legacy_modes is not None:
        return legacy_modes
    raise StrategyError("Worker returned an invalid symbol catalog.")


def _hard_specification_mismatches(
    first: SymbolSpecification,
    second: SymbolSpecification,
) -> list[str]:
    fields = (
        "trade_calc_mode",
        "digits",
        "point",
        "trade_tick_size",
        "contract_size",
        "volume_min",
        "volume_step",
        "allowed_directions",
    )
    return [field for field in fields if getattr(first, field) != getattr(second, field)]


def _preferred_shared_filling_mode(first: frozenset[str], second: frozenset[str]) -> str | None:
    shared = first & second
    return "FOK" if "FOK" in shared else "IOC" if "IOC" in shared else None


def _ticket(position: dict[str, object]) -> int:
    ticket = position.get("ticket")
    if isinstance(ticket, bool) or not isinstance(ticket, int):
        raise StrategyError("Worker returned a position without a ticket.")
    return ticket


def _required_gateway_text(value: object, field: str) -> str:
    candidate = value.get(field) if isinstance(value, dict) else None
    if not isinstance(candidate, str) or not candidate:
        raise StrategyError("The controller returned an invalid Trader identity response.")
    return candidate


def _utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise StrategyError("Quote timestamp must include an offset.")
    return parsed.astimezone(UTC)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--first-worker")
    parser.add_argument("--second-worker")
    parser.add_argument("--entry-edge-points", type=float, default=4)
    parser.add_argument(
        "--max-exposure-usd",
        type=float,
        help="per-leg allocation; defaults to the endpoint's startup equity",
    )
    parser.add_argument(
        "--max-margin-ratio",
        type=float,
        default=0.1,
        help="maximum margin as a fraction of each leg's allocation (0, 0.5]",
    )
    parser.add_argument("--max-trades", type=int, default=100)
    parser.add_argument("--daily-loss-percent", type=float, default=3)
    parser.add_argument("--trade-loss-percent", type=float, default=2)
    parser.add_argument("--quote-max-age-seconds", type=float, default=1)
    parser.add_argument("--emergency-stop-loss-usd", type=float, default=40)
    parser.add_argument("--integrity-check-seconds", type=float, default=5)
    parser.add_argument("--minimum-hold-seconds", type=float, default=180)
    parser.add_argument("--flatten-at-ny", type=_ny_time, default=time(16, 0), metavar="HH:MM")
    parser.add_argument("--worker-disconnect-grace-seconds", type=float, default=300)
    parser.add_argument("--worker-write-disconnect-grace-seconds", type=float, default=30)
    parser.add_argument("--trader-config", default=DEFAULT_TRADER_CONFIG)
    parser.add_argument(
        "--runtime-state",
        help="Strategy Runtime SQLite path; defaults beside the Trader identity.",
    )
    parser.add_argument("--execute", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true", help="emit DEBUG diagnostics")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    if (args.first_worker is None) != (args.second_worker is None):
        parser.error("--first-worker and --second-worker must be specified together.")
    if args.first_worker == args.second_worker and args.first_worker is not None:
        parser.error("Workers must differ.")
    positive_values = (
        args.entry_edge_points,
        args.max_trades,
        args.quote_max_age_seconds,
        args.daily_loss_percent,
        args.trade_loss_percent,
        args.emergency_stop_loss_usd,
        args.integrity_check_seconds,
        args.minimum_hold_seconds,
        args.worker_disconnect_grace_seconds, args.worker_write_disconnect_grace_seconds,
        args.max_margin_ratio,
    )
    if min(positive_values) <= 0:
        parser.error("All limits must be positive.")
    if args.max_margin_ratio > 0.5:
        parser.error("--max-margin-ratio must not exceed 0.5.")
    if args.max_exposure_usd is not None and args.max_exposure_usd <= 0:
        parser.error("--max-exposure-usd must be positive.")
    config = StrategyConfig(
        args.entry_edge_points, args.max_trades, args.daily_loss_percent / 100, args.trade_loss_percent / 100,
        args.max_exposure_usd, args.max_margin_ratio, args.quote_max_age_seconds, args.emergency_stop_loss_usd,
        args.integrity_check_seconds, args.minimum_hold_seconds,
        args.flatten_at_ny, args.worker_disconnect_grace_seconds, args.worker_write_disconnect_grace_seconds, args.execute,
    )
    configured_workers = (
        (args.first_worker, args.second_worker)
        if args.first_worker is not None
        else ("*",)
    )
    gateway = TraderGateway.connect(Path(args.trader_config), worker_ids=configured_workers)
    stop = Event()
    signal.signal(signal.SIGINT, lambda *_: (_LOGGER.info("interrupt_received"), stop.set()))
    strategy: RealtimeArbitrage | None = None
    exit_code = 0
    try:
        first_worker, second_worker = (
            (args.first_worker, args.second_worker)
            if args.first_worker is not None
            else _select_workers(gateway)
        )
        runtime_path = Path(args.runtime_state) if args.runtime_state else Path(args.trader_config).with_suffix(".runtime.sqlite3")
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint(first_worker),
            second=Endpoint(second_worker),
            config=config,
            runtime_path=runtime_path,
        )
        strategy.run(stop)
    except StrategyError as error:
        _LOGGER.error("strategy_stopped reason=%s", error)
        print(f"Strategy stopped: {error}", file=sys.stderr)
        exit_code = 1
    finally:
        if strategy is not None:
            try:
                strategy.shutdown()
            except StrategyError as error:
                print(f"Strategy Runtime close convergence failed: {error}", file=sys.stderr)
                exit_code = 1
        gateway.close()
        if strategy is not None and strategy.runtime is not None:
            strategy.runtime.close()
    return exit_code


def _select_workers(gateway: TraderGateway) -> tuple[str, str]:
    result = gateway.query("active_workers")
    workers = result.get("workers")
    if not isinstance(workers, list):
        raise StrategyError("Controller returned invalid active Worker inventory.")
    eligible = [
        worker
        for worker in workers
        if isinstance(worker, dict)
        and isinstance(worker.get("worker_id"), str)
        and worker.get("connectivity") == "connected"
    ]
    if len(eligible) < 2:
        raise StrategyError("At least two connected, safe active Workers are required.")
    print("Select two active Workers:")
    for index, worker in enumerate(eligible, start=1):
        print(f"  {index}. {worker['worker_id']} ({worker.get('server', 'unknown server')})")
    first = _selected_worker(eligible, "First Worker")
    second = _selected_worker(eligible, "Second Worker", excluded=first)
    return first, second


def _selected_worker(workers: list[dict[str, object]], label: str, *, excluded: str | None = None) -> str:
    while True:
        try:
            selected = int(input(f"{label} number: "))
        except ValueError:
            print("Enter a number from the list.")
            continue
        if not 1 <= selected <= len(workers):
            print("Enter a number from the list.")
            continue
        worker_id = workers[selected - 1]["worker_id"]
        assert isinstance(worker_id, str)
        if worker_id == excluded:
            print("Select a different Worker.")
            continue
        return worker_id


def _ny_time(value: str) -> time:
    try:
        parsed = datetime.strptime(value, "%H:%M").time()
    except ValueError as error:
        raise argparse.ArgumentTypeError("must use 24-hour HH:MM format") from error
    return parsed


if __name__ == "__main__":
    raise SystemExit(main())
