from __future__ import annotations

from datetime import timedelta
import json
import logging
from math import floor
from statistics import median
from collections.abc import Callable
from datetime import UTC, datetime
from dataclasses import dataclass, field
from time import monotonic
from typing import Protocol, Self, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed, InvalidStatus

from ..mt5.config import TimeCalibrationFamily
from ..mt5.output import render
from ..mt5.timecalibration import MARKET_DATA, render_calibration
from ..trader_protocol import (
    PAIR_CELL_CONTROL_MESSAGE_TYPES,
    PAIR_CELL_PROTOCOL_VERSION,
    pair_cell_control_message_adapter,
)
from .credentials import (
    WebSocketConnector,
    WorkerWebSocket,
    _message,
    _required_text,
    _send,
    _send_proof,
    _worker_endpoint,
)
from .enrollment import WorkerEnrollmentError, WorkerSessionDisconnected
from .scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc, TraderRpcOutcome


_LOGGER = logging.getLogger(__name__)
#: Bounded wait for a controller reply on the worker session. A half-open
#: TCP connection (e.g. a dropped Wi-Fi that never sends FIN/RST) would
#: otherwise block ``recv`` forever and freeze the whole reconciliation loop,
#: including local pair-cell protection. Expiry surfaces as
#: ``WorkerSessionDisconnected`` via ``_raise_closed_connection`` and triggers
#: the existing reconnect backoff.
_HEARTBEAT_TIMEOUT_SECONDS = 15.0
_LIVE_STATE_TIMEOUT_SECONDS = 15.0
_RESPONSE_TIMEOUT_SECONDS = 30.0
_HANDSHAKE_TIMEOUT_SECONDS = 30.0
_MAX_PAIR_RELAY_ACKS = 128
#: Pairing control traffic is small, bursty and entirely superseded by the
#: next authoritative route read, so its inboxes are bounded rather than
#: unbounded queues that a disconnected consumer could grow without limit.
_MAX_PAIR_CELL_CONTROL_MESSAGES = 256

#: Every Worker-facing pairing control message is answered with exactly one
#: ``<type>_result`` reply on the same authenticated session.
PAIR_CELL_CONTROL_RESULT_TYPES = frozenset(
    f"{message_type}_result" for message_type in PAIR_CELL_CONTROL_MESSAGE_TYPES
)
#: Unsolicited controller pushes that belong to the pairing control plane.
#: None of them is a trading instruction: they announce reservation, route and
#: unpair state that the Worker re-reads authoritatively on reconnect.
PAIR_CELL_CONTROL_PUSH_TYPES = frozenset(
    {
        "pair_cell_pairing_proposed",
        "pair_cell_pairing_refused",
        "pair_cell_pairing_failed",
        "pair_cell_pairing_reservation_released",
        "pair_cell_route_established",
        "pair_cell_route_state_changed",
        "pair_cell_route_removed",
    }
)
from .keystore import HardwareKeyStore


class ProductCatalogReadOnlyMT5(Protocol):
    def symbols_get(self) -> object: ...


class MarketDataReadOnlyMT5(Protocol):
    def copy_rates_range(self, symbol: str, timeframe: object, from_time: datetime, to_time: datetime) -> object: ...

    def symbol_info_tick(self, symbol: str) -> object: ...


@dataclass
class AuthenticatedWorkerSession:
    """A proved WSS channel for one approved 帳戶工作者."""

    socket: WorkerWebSocket
    reconciliation_cursor: int
    worker_id: str = ""
    certificate: str = ""
    recovery_epoch: str = field(default_factory=lambda: str(uuid4()))
    _trader_rpc_scheduler: DeadlineAwareTraderRpcScheduler = field(
        default_factory=DeadlineAwareTraderRpcScheduler, init=False, repr=False
    )
    _pair_relay_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)
    _pair_relay_ack_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)
    _pair_cell_result_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)
    _pair_cell_push_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        try:
            self.socket.__exit__(exc_type, exc_value, traceback)
        except Exception as error:
            _raise_closed_connection(error, "authenticated worker session")

    def request_password(self) -> str:
        try:
            _send(self.socket, {"type": "password_request"})
            response = self._response(timeout=_RESPONSE_TIMEOUT_SECONDS)
            if response.get("type") != "password":
                raise WorkerEnrollmentError("The controller returned an invalid worker response.")
            return _required_text(response, "password")
        except Exception as error:
            _raise_closed_connection(error, "password request")

    def send_reconciliation(self, message: dict[str, object]) -> None:
        try:
            cursor = message.get("cursor")
            if not isinstance(cursor, int) or isinstance(cursor, bool):
                raise WorkerEnrollmentError("The Worker reconciliation cursor is invalid.")
            if message.get("type") == "snapshot":
                envelope = {
                    "type": "worker_snapshot",
                    "protocol_version": 1,
                    "worker_id": self.worker_id,
                    "recovery_epoch": self.recovery_epoch,
                    "snapshot_baseline": cursor,
                    "observed_at": message.get("observed_at"),
                    "orders": message.get("orders"),
                    "positions": message.get("positions"),
                }
            elif message.get("type") == "delta":
                change = "remove" if message.get("change") == "closed" else "upsert"
                envelope = {
                    "type": "worker_broker_delta",
                    "protocol_version": 1,
                    "worker_id": self.worker_id,
                    "recovery_epoch": self.recovery_epoch,
                    "event_id": cursor,
                    "observed_at": message.get("observed_at"),
                    "entity": message.get("entity"),
                    "change": change,
                    "record": message.get("record"),
                }
            else:
                raise WorkerEnrollmentError("The Worker reconciliation message is invalid.")
            outgoing = {"type": "worker_fact", "cursor": cursor, "envelope": envelope}
            _send(self.socket, outgoing)
            response = self._response(timeout=_RESPONSE_TIMEOUT_SECONDS)
            if response.get("type") != "accepted" or response.get("cursor") != cursor:
                raise WorkerEnrollmentError("The controller rejected worker reconciliation.")
            self.reconciliation_cursor = cursor
        except Exception as error:
            _raise_closed_connection(error, "reconciliation")

    def send_live_state(self, message: dict[str, object]) -> None:
        try:
            _send(self.socket, message)
            if self._response(timeout=_LIVE_STATE_TIMEOUT_SECONDS) != {"type": "live_state_accepted"}:
                raise WorkerEnrollmentError("The controller rejected worker live state.")
        except Exception as error:
            _raise_closed_connection(error, "live-state publication")

    def heartbeat(self) -> bool:
        try:
            _send(self.socket, {"type": "heartbeat"})
            response = self._response(timeout=_HEARTBEAT_TIMEOUT_SECONDS)
            return response == {"type": "heartbeat_ack"}
        except Exception as error:
            _raise_closed_connection(error, "heartbeat")

    def send_recovery_state(self, state: str, reason: str) -> None:
        try:
            _send(self.socket, {"type": "recovery_state", "state": state, "reason": reason})
            response = self._response(timeout=_RESPONSE_TIMEOUT_SECONDS)
            if response != {"type": "accepted", "state": state}:
                raise WorkerEnrollmentError("The controller rejected the worker recovery state.")
        except Exception as error:
            _raise_closed_connection(error, "recovery-state update")

    def send_recovery_sync(self, journal: list[dict[str, object]]) -> None:
        try:
            _send(self.socket, {"type": "recovery_sync", "epoch": self.recovery_epoch, "journal": journal})
            if self._response(timeout=_RESPONSE_TIMEOUT_SECONDS) != {"type": "recovery_sync_accepted", "epoch": self.recovery_epoch}:
                raise WorkerEnrollmentError("The controller rejected Worker recovery sync.")
        except Exception as error:
            _raise_closed_connection(error, "recovery sync")

    def _response(self, *, timeout: float | None = _RESPONSE_TIMEOUT_SECONDS) -> dict[str, object]:
        deadline = None if timeout is None else monotonic() + timeout
        while True:
            if deadline is not None:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    raise TimeoutError("timed out while waiting for controller response")
            else:
                remaining = None
            response = _message(self.socket, timeout=remaining)
            response_type = response.get("type")
            if response_type == "pair_relay_deliver":
                self._queue_pair_relay_envelope(response)
                continue
            if response_type in PAIR_CELL_CONTROL_RESULT_TYPES:
                self._queue_pair_cell_result(response)
                continue
            if response_type in PAIR_CELL_CONTROL_PUSH_TYPES:
                self._queue_pair_cell_push(response)
                continue
            if response_type == "pair_relay_ack":
                self._queue_pair_relay_ack(response)
                continue  # fire-and-forget sends do not correlate this synchronously
            return response

    def dispatch_scheduler_outcome(self, outcome: TraderRpcOutcome) -> None:
        """Sink one scheduler-produced terminal outcome with no live requester.

        Trader-relay requesters are gone; the Pair Execution Cell consumes its
        own items directly. Anything arriving here is a foreign outcome nobody
        waits on, so it is dropped after the scheduler already terminalized it.
        """

        _ = outcome
        return

    def receive_worker_relay(self, timeout: float | None = None) -> bool:
        """Pull one pending controller-pushed message and route it by type.

        This single receive seam serves every push the controller may send on
        this connection: the Pair Execution Cell's own opaque ``pair_relay_deliver``/
        ``pair_relay_ack`` receipts, and the Worker-facing pairing control
        plane's ``*_result`` replies and route/reservation notifications. Each
        is queued for its own consumer so this method never confuses one kind
        of push for another, and interleaved traffic on the same socket is
        handled one message at a time in arrival order.  Quarantine release is
        worker-initiated over the opaque relay; the controller never pushes one.
        """

        try:
            response = _message(self.socket, timeout=timeout)
        except TimeoutError:
            return False
        except Exception as error:
            _raise_closed_connection(error, "Worker relay request")
        response_type = response.get("type")
        if response_type == "pair_relay_deliver":
            self._queue_pair_relay_envelope(response)
            return True
        if response_type in PAIR_CELL_CONTROL_RESULT_TYPES:
            self._queue_pair_cell_result(response)
            return True
        if response_type in PAIR_CELL_CONTROL_PUSH_TYPES:
            self._queue_pair_cell_push(response)
            return True
        if response_type == "pair_relay_ack":
            self._queue_pair_relay_ack(response)
            return True  # fire-and-forget sends do not correlate this synchronously
        raise WorkerEnrollmentError("The controller returned an invalid Worker relay request.")

    def _queue_pair_relay_envelope(self, response: dict[str, object]) -> None:
        if set(response) != {"type", "envelope"} or not isinstance(response.get("envelope"), dict):
            raise WorkerEnrollmentError("The controller returned an invalid Pair Execution Cell relay push.")
        self._pair_relay_inbox.append(cast(dict[str, object], response["envelope"]))

    def _queue_pair_relay_ack(self, response: dict[str, object]) -> None:
        """Retain one relay delivery receipt for the Pair Execution Cell.

        A rejected acknowledgement (most importantly "Peer Worker is
        disconnected") is the Worker's only genuine, controller-authenticated
        evidence that the authorized peer route session is not live, and the
        specification requires that loss to remove entry readiness
        immediately. The queue is bounded because nothing else consumes it
        when no cell is running.
        """

        accepted = response.get("accepted")
        if not isinstance(accepted, bool):
            return
        self._pair_relay_ack_inbox.append(
            {
                "request_id": response.get("request_id"),
                "accepted": accepted,
                "reason": response.get("reason"),
            }
        )
        if len(self._pair_relay_ack_inbox) > _MAX_PAIR_RELAY_ACKS:
            del self._pair_relay_ack_inbox[:-_MAX_PAIR_RELAY_ACKS]

    def _queue_pair_cell_result(self, response: dict[str, object]) -> None:
        """Retain one reply to this Worker's own pairing control message.

        Replies are queued rather than awaited inline so pairing never blocks
        the serialized Trader RPC path: the receive loop keeps draining
        ordinary relay traffic while a pairing negotiation is in flight, and
        the Pair Execution Cell runtime correlates replies by ``request_id``
        on its own cadence.
        """

        if not isinstance(response.get("accepted"), bool):
            raise WorkerEnrollmentError(
                "The controller returned an invalid Pair Execution Cell control reply."
            )
        self._pair_cell_result_inbox.append(dict(response))
        if len(self._pair_cell_result_inbox) > _MAX_PAIR_CELL_CONTROL_MESSAGES:
            del self._pair_cell_result_inbox[:-_MAX_PAIR_CELL_CONTROL_MESSAGES]

    def _queue_pair_cell_push(self, response: dict[str, object]) -> None:
        """Retain one unsolicited pairing/route notification.

        These are never replayed into a later session by the controller, so a
        reconnecting Worker re-reads the authoritative route record instead of
        depending on any of them arriving.
        """

        self._pair_cell_push_inbox.append(dict(response))
        if len(self._pair_cell_push_inbox) > _MAX_PAIR_CELL_CONTROL_MESSAGES:
            del self._pair_cell_push_inbox[:-_MAX_PAIR_CELL_CONTROL_MESSAGES]

    def drain_pair_cell_results(self) -> list[dict[str, object]]:
        """Pop every pairing control reply queued since the last drain."""

        results, self._pair_cell_result_inbox = self._pair_cell_result_inbox, []
        return results

    def drain_pair_cell_pushes(self) -> list[dict[str, object]]:
        """Pop every pairing/route notification queued since the last drain."""

        pushes, self._pair_cell_push_inbox = self._pair_cell_push_inbox, []
        return pushes

    def send_pair_cell_control(self, message: dict[str, object]) -> str:
        """Send one Worker-facing pairing control message and return its id.

        The message is validated against
        :data:`~abt.trader_protocol.pair_cell_control_message_adapter` before
        it leaves this process, so a malformed local request is a loud local
        error rather than a refused session. Nothing is awaited here: the
        controller's reply arrives as a queued ``<type>_result``.
        """

        request_id = message.get("request_id")
        if not isinstance(request_id, str) or not request_id:
            request_id = str(uuid4())
        outgoing = {**message, "request_id": request_id, "protocol_version": PAIR_CELL_PROTOCOL_VERSION}
        try:
            pair_cell_control_message_adapter.validate_python(outgoing)
        except ValidationError as error:
            raise WorkerEnrollmentError(
                "The Worker cannot send an invalid Pair Execution Cell control message."
            ) from error
        try:
            _send(self.socket, outgoing)
        except Exception as error:
            _raise_closed_connection(error, "Pair Execution Cell control message")
        return request_id

    def declare_pair_cell_role(self, role: str | None) -> str:
        """Declare a desired role on this unpaired connection.

        An omitted ``role`` means this Worker is simply an available
        follower; a durable route always wins over the declaration.
        """

        return self.send_pair_cell_control({"type": "pair_cell_role", "role": role})

    def request_available_pair_followers(self) -> str:
        return self.send_pair_cell_control({"type": "pair_cell_available_followers"})

    def propose_pair_cell_pairing(self, follower_worker_id: str) -> str:
        return self.send_pair_cell_control(
            {"type": "pair_cell_pairing_proposal", "follower_worker_id": follower_worker_id}
        )

    def send_pair_cell_pairing_decision(
        self,
        *,
        proposal_id: str,
        accepted: bool,
        acceptance_payload: dict[str, object] | None = None,
        payload_hash: str | None = None,
        reason: str | None = None,
    ) -> str:
        """Accept or refuse one pairing proposal.

        An acceptance is never a bare yes: it carries the follower's own
        opaque Pairing Acceptance payload and its hash, which the controller
        forwards to the reserved leader unchanged and never parses.  A
        refusal carries no payload at all.
        """

        message: dict[str, object] = {
            "type": "pair_cell_pairing_decision",
            "proposal_id": proposal_id,
            "accepted": accepted,
        }
        if accepted:
            message["acceptance_payload"] = acceptance_payload
            message["payload_hash"] = payload_hash
        elif reason is not None:
            message["reason"] = reason
        return self.send_pair_cell_control(message)

    def request_pair_cell_route_sync(self) -> str:
        return self.send_pair_cell_control({"type": "pair_cell_route_sync"})

    def send_pair_cell_unpair(self, *, route_id: str, action: str) -> str:
        return self.send_pair_cell_control(
            {"type": "pair_cell_unpair", "route_id": route_id, "action": action}
        )

    def send_pair_cell_state_version(self, *, route_id: str, state_version: int) -> str:
        return self.send_pair_cell_control(
            {"type": "pair_cell_state_version", "route_id": route_id, "state_version": state_version}
        )

    def send_pair_cell_unpair_assertion(self, *, route_id: str, state_version: int) -> str:
        return self.send_pair_cell_control(
            {
                "type": "pair_cell_unpair_assertion",
                "route_id": route_id,
                "state_version": state_version,
            }
        )

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]:
        """Pop every opaque Pair Execution Cell envelope queued since the last drain."""

        envelopes, self._pair_relay_inbox = self._pair_relay_inbox, []
        return envelopes
    def drain_pair_relay_acks(self) -> list[dict[str, object]]:
        """Pop every ``{request_id, accepted, reason}`` relay receipt queued since the last drain."""

        acks, self._pair_relay_ack_inbox = self._pair_relay_ack_inbox, []
        return acks

    def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None:
        """Send one opaque Pair Execution Cell envelope; fire-and-forget.

        The Pair Execution Cell's relay adapter never blocks on an
        acknowledgement (``PairRelayAdapter.send`` returns ``None``); a
        ``pair_relay_ack`` arriving later is tolerated (and ignored) by
        ``receive_worker_relay``/``_response`` wherever it happens to
        interleave with other traffic on this connection.
        """

        try:
            _send(self.socket, {"type": "pair_relay", "request_id": request_id, "envelope": envelope})
        except Exception as error:
            _raise_closed_connection(error, "Pair Execution Cell relay")

    @property
    def trader_rpc_scheduler(self) -> DeadlineAwareTraderRpcScheduler:
        """The single scheduler this Worker uses for every source of broker
        writes. Adapters (e.g. the Pair Execution Cell) must share this exact
        instance to preserve serialized MT5 access and priority ordering."""

        return self._trader_rpc_scheduler





def _market_data_symbol_evidence_with_retry(
    mt5: MarketDataReadOnlyMT5,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    calibration: dict[str, object],
) -> dict[str, object]:
    last_error: WorkerEnrollmentError | None = None
    for attempt in range(1, 4):
        try:
            return _market_data_symbol_evidence(mt5, symbol, timeframe, start, end, calibration)
        except WorkerEnrollmentError as error:
            last_error = error
            _LOGGER.debug(
                "Market-data evidence failed for symbol %s, timeframe %s, attempt %s/3: %s",
                symbol,
                timeframe,
                attempt,
                error,
            )
    assert last_error is not None
    raise WorkerEnrollmentError(
        f"Unable to collect {timeframe} market-data evidence for {symbol} after 3 attempts: {last_error}"
    ) from last_error


def _symbol_specification(symbol: object) -> dict[str, object]:
    source = _symbol_source(symbol)
    name = _symbol_field(source, "symbol", "name")
    trade_calc_mode = _symbol_field(source, "trade_calc_mode")
    currency_base = _symbol_field(source, "currency_base")
    currency_profit = _symbol_field(source, "currency_profit")
    digits = _symbol_field(source, "digits")
    point = _symbol_field(source, "point")
    if not isinstance(name, str) or not name:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if isinstance(trade_calc_mode, bool) or not isinstance(trade_calc_mode, (int, str)):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if isinstance(trade_calc_mode, str) and not trade_calc_mode:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(currency_base, str) or not currency_base:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(currency_profit, str) or not currency_profit:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(digits, int) or isinstance(digits, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    if not isinstance(point, (int, float)) or isinstance(point, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return {
        "symbol": name,
        "trade_calc_mode": trade_calc_mode,
        "currency_base": currency_base,
        "currency_profit": currency_profit,
        "digits": digits,
        "point": point,
        "trade_tick_size": _required_symbol_float(source, "trade_tick_size"),
        "contract_size": _required_symbol_float(source, "contract_size", "trade_contract_size"),
        "volume_min": _required_symbol_float(source, "volume_min"),
        "volume_step": _required_symbol_float(source, "volume_step"),
        "filling_modes": _symbol_filling_modes(source),
        "allowed_directions": _symbol_allowed_directions(source),
        "volume_max": _required_symbol_float(source, "volume_max"),
        "trade_stops_level": _required_symbol_int(source, "trade_stops_level"),
        "trade_freeze_level": _required_symbol_int(source, "trade_freeze_level"),
        "trade_tick_value": _required_symbol_float(source, "trade_tick_value"),
        "currency_margin": _required_symbol_text(source, "currency_margin"),
        "swap_long": _required_symbol_float(source, "swap_long"),
        "swap_short": _required_symbol_float(source, "swap_short"),
        "swap_mode": _required_symbol_int(source, "swap_mode"),
        "swap_rollover3days": _required_symbol_int(source, "swap_rollover3days"),
    }


def _symbol_source(symbol: object) -> dict[str, object]:
    if isinstance(symbol, dict):
        return symbol
    as_dict = getattr(symbol, "_asdict", None)
    if callable(as_dict):
        source = as_dict()
        if isinstance(source, dict):
            return source
    try:
        source = vars(symbol)
    except TypeError as error:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.") from error
    if not isinstance(source, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return source


def _symbol_field(source: dict[str, object], *names: str) -> object:
    for name in names:
        if name in source:
            return source[name]
    return None


def _required_symbol_float(source: dict[str, object], *names: str) -> float:
    value = _symbol_field(source, *names)
    if not isinstance(value, (int, float)) or isinstance(value, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return float(value)


def _required_symbol_int(source: dict[str, object], *names: str) -> int:
    value = _symbol_field(source, *names)
    if not isinstance(value, int) or isinstance(value, bool):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return value


def _required_symbol_text(source: dict[str, object], *names: str) -> str:
    value = _symbol_field(source, *names)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return value


def _required_symbol_text_list(source: dict[str, object], *names: str) -> list[str]:
    value = _symbol_field(source, *names)
    if not isinstance(value, list) or not value or not all(isinstance(item, str) and item for item in value):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
    return list(value)


def _symbol_filling_modes(source: dict[str, object]) -> list[str]:
    if isinstance(_symbol_field(source, "filling_modes"), list):
        return _required_symbol_text_list(source, "filling_modes")
    filling_mode = _symbol_field(source, "filling_mode")
    if isinstance(filling_mode, int) and not isinstance(filling_mode, bool):
        modes = [name for bit, name in ((1, "FOK"), (2, "IOC"), (4, "BOC")) if filling_mode & bit]
        if modes:
            return modes
        legacy_mode = {0: ["FOK"], 1: ["IOC"], 2: ["RETURN"], 3: ["BOC"]}.get(filling_mode)
        if legacy_mode is not None:
            return legacy_mode
    raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")


def _symbol_allowed_directions(source: dict[str, object]) -> list[str]:
    if isinstance(_symbol_field(source, "allowed_directions"), list):
        return _required_symbol_text_list(source, "allowed_directions")
    order_mode = _symbol_field(source, "order_mode")
    if isinstance(order_mode, int) and not isinstance(order_mode, bool):
        directions = []
        if order_mode & 1:
            directions.append("LONG")
        if order_mode & 2:
            directions.append("SHORT")
        if directions:
            return directions
    trade_mode = _symbol_field(source, "trade_mode")
    if isinstance(trade_mode, int) and not isinstance(trade_mode, bool):
        directions = {
            1: ["LONG"],
            2: ["SHORT"],
            4: ["LONG", "SHORT"],
        }.get(trade_mode)
        if directions is not None:
            return directions
    raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")


def _market_data_symbol_evidence(
    mt5: MarketDataReadOnlyMT5,
    symbol: str,
    timeframe: str,
    start: datetime,
    end: datetime,
    calibration: dict[str, object],
) -> dict[str, object]:
    raw_rates = mt5.copy_rates_range(symbol, _timeframe_value(mt5, timeframe), start, end - timedelta(seconds=1))
    bars = _structured_market_data(raw_rates)
    if not bars:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    rendered = json.loads(
        render(
            bars,
            "json",
            user_timezone=ZoneInfo("UTC"),
            source_family=MARKET_DATA,
            calibration=calibration,
        )
    )
    if not isinstance(rendered, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    records = rendered.get("records")
    time_metadata = rendered.get("time_metadata")
    if not isinstance(records, list) or not isinstance(time_metadata, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    return {"symbol": symbol, "bars": records, "time_metadata": time_metadata}


def _market_data_calibration(mt5: MarketDataReadOnlyMT5, symbols: list[str]) -> dict[str, object]:
    for symbol in symbols:
        samples = tuple(sample for _ in range(3) if (sample := _market_sample(mt5, symbol)) is not None)
        if not samples:
            continue
        offset = int(round(median(sample["offset_seconds"] for sample in samples)))
        selected = samples[-1]
        calibration = render_calibration(
            TimeCalibrationFamily(
                offset_seconds=offset,
                calibrated_local_date=_parse_utc(str(selected["calibrated_at_utc"])).date().isoformat(),
                calibrated_at_utc=str(selected["calibrated_at_utc"]),
                status="calibrated",
                calibration_symbol=symbol,
            ),
            MARKET_DATA,
            ZoneInfo("UTC"),
            now=_parse_utc(str(selected["calibrated_at_utc"])),
        )
        calibration["samples"] = list(samples)
        calibration["sample_count"] = len(samples)
        _LOGGER.debug("Using %s for shared market-data calibration.", symbol)
        return calibration
    raise WorkerEnrollmentError(
        "No valid symbol_info_tick.time was available across 3 calibration samples for any requested symbol."
    )


def _market_sample(mt5: MarketDataReadOnlyMT5, symbol: str) -> dict[str, object] | None:
    before = datetime.now(UTC)
    tick = mt5.symbol_info_tick(symbol)
    after = datetime.now(UTC)
    epoch = _field(tick, "time")
    if not isinstance(epoch, (int, float)) or isinstance(epoch, bool) or epoch <= 0:
        return None
    midpoint = before + (after - before) / 2
    difference = float(epoch) - midpoint.timestamp()
    offset = int(round(difference))
    error = abs(difference - offset) + (after - before).total_seconds() / 2
    return {
        "source": "symbol_info_tick.time",
        "calibrated_at_utc": after.isoformat().replace("+00:00", "Z"),
        "offset_seconds": offset,
        "error_seconds": round(error, 6),
        "symbol": symbol,
    }


def _timeframe_value(mt5: object, timeframe: str) -> object:
    value = getattr(mt5, f"TIMEFRAME_{timeframe}", None)
    return timeframe if value is None else value


def _structured_market_data(values: object) -> list[dict[str, object]]:
    if values is None:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    if isinstance(values, (list, tuple)):
        return [_market_bar(value) for value in values]
    names = getattr(getattr(values, "dtype", None), "names", None)
    if names is None:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    return [
        _market_bar({name: row[name].item() if hasattr(row[name], "item") else row[name] for name in names})
        for row in values
    ]


def _market_bar(value: object) -> dict[str, object]:
    source = value if isinstance(value, dict) else vars(value) if hasattr(value, "__dict__") else value
    if not isinstance(source, dict):
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    time = source.get("time")
    if not isinstance(time, (int, float)) or isinstance(time, bool) or time <= 0:
        raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
    bar = {"time": floor(float(time))}
    for field in ("open", "high", "low", "close"):
        numeric = source.get(field)
        if not isinstance(numeric, (int, float)) or isinstance(numeric, bool):
            raise WorkerEnrollmentError("The local MT5 terminal returned incomplete market-data evidence.")
        bar[field] = float(numeric)
    return bar


def _field(value: object, name: str) -> object:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _parse_utc(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def open_authenticated_worker_session(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    connect: WebSocketConnector | None = None,
    certificate_received: Callable[[str], None] | None = None,
) -> AuthenticatedWorkerSession:
    """Deliver the approved certificate, then prove the device key on one persistent WSS channel."""

    if connect is None:
        from websockets.sync.client import connect as websocket_connect

        connect = websocket_connect
    try:
        with connect(_worker_endpoint(controller_url, "/api/worker/certificate")) as certificate_socket:
            _send(certificate_socket, {"enrollment_id": enrollment_id})
            challenge = _message(certificate_socket, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
            worker_id = _required_text(challenge, "worker_id")
            _send_proof(certificate_socket, key_store, challenge, "certificate_delivery", worker_id)
            delivery = _message(certificate_socket, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
            if _required_text(delivery, "worker_id") != worker_id:
                raise WorkerEnrollmentError("The controller returned an invalid device certificate.")
            certificate = _required_text(delivery, "certificate")
            if certificate_received is not None:
                certificate_received(certificate)
    except Exception as error:
        _raise_closed_connection(error, "certificate delivery")

    socket = connect(_worker_endpoint(controller_url, "/api/worker/session"))
    try:
        socket.__enter__()
        _send(socket, {"worker_id": worker_id, "certificate": certificate})
        challenge = _message(socket, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
        _send_proof(socket, key_store, challenge, "worker_session", worker_id)
        authenticated = _message(socket, timeout=_HANDSHAKE_TIMEOUT_SECONDS)
        cursor = authenticated.get("cursor") if isinstance(authenticated, dict) else None
        if (
            not isinstance(cursor, int)
            or isinstance(cursor, bool)
            or cursor < 0
            or authenticated != {"type": "authenticated", "worker_id": worker_id, "cursor": cursor}
        ):
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    except BaseException as error:
        try:
            socket.__exit__(type(error), error, error.__traceback__)
        except Exception:
            pass
        if isinstance(error, Exception):
            _raise_closed_connection(error, "authenticated worker session")
        raise
    return AuthenticatedWorkerSession(socket, reconciliation_cursor=cursor, worker_id=worker_id, certificate=certificate)


def _required_utc_timestamp(response: dict[str, object], field: str) -> str:
    value = _required_text(response, field)
    try:
        timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise WorkerEnrollmentError("The controller returned an invalid request expiry.") from error
    if timestamp.tzinfo is None:
        raise WorkerEnrollmentError("The controller returned an invalid request expiry.")
    return timestamp.astimezone(UTC).isoformat()


def _raise_closed_connection(error: Exception, phase: str) -> None:
    if isinstance(error, (ConnectionClosed, InvalidStatus, OSError, TimeoutError)):
        raise WorkerSessionDisconnected(
            f"{phase} WebSocket disconnected ({type(error).__name__}: {error})."
        ) from error
    raise error
