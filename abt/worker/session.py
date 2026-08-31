from __future__ import annotations

from datetime import timedelta
import json
import logging
from math import floor
from statistics import median
from collections.abc import Callable
from datetime import UTC, datetime
from dataclasses import dataclass, field
from typing import Protocol, Self, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from pydantic import ValidationError
from websockets.exceptions import ConnectionClosed, InvalidStatus

from ..mt5.config import TimeCalibrationFamily
from ..mt5.output import render
from ..mt5.timecalibration import MARKET_DATA, render_calibration
from ..trader_protocol import (
    TraderRpcFailure,
    TraderRpcRequest,
    TraderRpcSuccess,
    WorkerEffectRequested,
    WorkerReadRequested,
    worker_request_envelope_adapter,
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
    _relay_requests: dict[str, WorkerReadRequested | WorkerEffectRequested] = field(
        default_factory=dict, init=False, repr=False
    )
    _pair_relay_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)
    _pair_cell_activation_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)
    _pair_cell_quarantine_release_inbox: list[dict[str, object]] = field(default_factory=list, init=False, repr=False)

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
            response = self._response()
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
            response = self._response()
            if response.get("type") != "accepted" or response.get("cursor") != cursor:
                raise WorkerEnrollmentError("The controller rejected worker reconciliation.")
            self.reconciliation_cursor = cursor
        except Exception as error:
            _raise_closed_connection(error, "reconciliation")

    def send_live_state(self, message: dict[str, object]) -> None:
        try:
            _send(self.socket, message)
            if self._response() != {"type": "live_state_accepted"}:
                raise WorkerEnrollmentError("The controller rejected worker live state.")
        except Exception as error:
            _raise_closed_connection(error, "live-state publication")

    def heartbeat(self) -> bool:
        try:
            _send(self.socket, {"type": "heartbeat"})
            response = self._response()
            return response == {"type": "heartbeat_ack"}
        except Exception as error:
            _raise_closed_connection(error, "heartbeat")

    def send_recovery_state(self, state: str, reason: str) -> None:
        try:
            _send(self.socket, {"type": "recovery_state", "state": state, "reason": reason})
            response = self._response()
            if response != {"type": "accepted", "state": state}:
                raise WorkerEnrollmentError("The controller rejected the worker recovery state.")
        except Exception as error:
            _raise_closed_connection(error, "recovery-state update")

    def send_recovery_sync(self, journal: list[dict[str, object]]) -> None:
        try:
            _send(self.socket, {"type": "recovery_sync", "epoch": self.recovery_epoch, "journal": journal})
            if self._response() != {"type": "recovery_sync_accepted", "epoch": self.recovery_epoch}:
                raise WorkerEnrollmentError("The controller rejected Worker recovery sync.")
        except Exception as error:
            _raise_closed_connection(error, "recovery sync")

    def _response(self) -> dict[str, object]:
        while True:
            response = _message(self.socket)
            response_type = response.get("type")
            if response_type == "worker_relay":
                self._queue_worker_relay(response)
                continue
            if response_type == "pair_relay_deliver":
                self._queue_pair_relay_envelope(response)
                continue
            if response_type == "pair_cell_activate":
                self._queue_pair_cell_activation(response)
                continue
            if response_type == "pair_cell_quarantine_release_request":
                self._queue_pair_cell_quarantine_release(response)
                continue
            if response_type == "pair_relay_ack":
                continue  # fire-and-forget sends do not correlate this synchronously
            return response

    def _parse_order_check(self, response: dict[str, object]) -> dict[str, object]:
        expected_fields = {"type", "request_id", "order"}
        if "expires_at" in response:
            expected_fields.add("expires_at")
        if set(response) != expected_fields or response.get("type") != "order_check_request":
            raise WorkerEnrollmentError("The controller returned an invalid order-check request.")
        request = {"request_id": _required_text(response, "request_id"), "order": response["order"]}
        if "expires_at" in response:
            request["expires_at"] = _required_utc_timestamp(response, "expires_at")
        return request

    def _queue_hedge_request(self, response: dict[str, object]) -> None:
        request_type = response.get("type")
        if request_type == "order_check_request":
            request = self._parse_order_check(response)
        elif request_type == "order_execute_request":
            request = self._parse_order_execute(response)
        else:
            raise WorkerEnrollmentError("The controller returned an invalid hedged-entry request.")
        request_id = str(request["request_id"])
        scheduled = {
            "request_id": request_id,
            "command_id": request.get("effect_id", f"{request_type}:{request_id}"),
            "kind": "operation",
            "priority": "execution",
            "payload": {"type": request_type, "order": request["order"]},
            "worker_request_type": request_type,
            "worker_request": request,
        }
        if "effect_id" in request:
            scheduled["effect_id"] = request["effect_id"]
        if "expires_at" in request:
            scheduled["expires_at"] = request["expires_at"]
        outcome = self._trader_rpc_scheduler.admit(scheduled)
        if outcome is None or not outcome.terminal:
            return
        self._send_hedge_scheduler_outcome(request_type, request, outcome)

    def _send_hedge_scheduler_outcome(
        self,
        request_type: str,
        request: dict[str, object],
        outcome: TraderRpcOutcome,
    ) -> None:
        request_id = _required_text(request, "request_id")
        reason = f"{outcome.category}: {outcome.reason}"
        order = request.get("order")
        if not isinstance(order, dict):
            raise WorkerEnrollmentError("The controller returned an invalid hedged-entry request.")
        if request_type == "order_check_request":
            if outcome.category == "expired_not_started":
                self.send_order_check(
                    request_id=request_id,
                    order=order,
                    accepted=False,
                    execution_state="not_started",
                    outcome="expired_not_started",
                )
            else:
                self.send_order_check_error(request_id=request_id, reason=reason)
            return
        if outcome.category == "expired_not_started":
            self.send_order_execute(
                request_id=request_id,
                order=order,
                accepted=False,
                result={},
                execution_state="not_started",
                outcome="expired_not_started",
            )
        else:
            self.send_order_execute_error(request_id=request_id, reason=reason)

    def send_order_check(
        self,
        *,
        request_id: str,
        order: dict[str, object],
        accepted: bool,
        diagnostics: dict[str, object] | None = None,
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None:
        if (execution_state, outcome) not in {(None, None), ("not_started", "expired_not_started")}:
            raise WorkerEnrollmentError("The worker cannot send an invalid order-check outcome.")
        response: dict[str, object] = {
            "type": "order_check_response",
            "analysis_id": "order_check",
            "request_id": request_id,
            "accepted": accepted,
            "order": order,
        }
        if diagnostics is not None:
            response["diagnostics"] = diagnostics
        if execution_state is not None:
            response["execution_state"] = execution_state
            response["outcome"] = outcome
        try:
            _send(self.socket, self._relay_response(request_id, response) or response)
        except Exception as error:
            _raise_closed_connection(error, "order-check response")

    def send_order_check_error(self, *, request_id: str, reason: str) -> None:
        try:
            response = {"type": "order_check_error", "analysis_id": "order_check", "request_id": request_id, "reason": reason}
            _send(self.socket, self._relay_response(request_id, response) or response)
        except Exception as error:
            _raise_closed_connection(error, "order-check error response")

    def _parse_order_execute(self, response: dict[str, object]) -> dict[str, object]:
        expected_fields = {"type", "request_id", "order"}
        if "expires_at" in response:
            expected_fields.add("expires_at")
        if "effect_id" in response:
            expected_fields.add("effect_id")
        if set(response) != expected_fields or response.get("type") != "order_execute_request":
            raise WorkerEnrollmentError("The controller returned an invalid order execution request.")
        order = response.get("order")
        if not isinstance(order, dict):
            raise WorkerEnrollmentError("The controller returned an invalid order execution request.")
        request = {"request_id": _required_text(response, "request_id"), "order": order}
        if "expires_at" in response:
            request["expires_at"] = _required_utc_timestamp(response, "expires_at")
        if "effect_id" in response:
            request["effect_id"] = _required_text(response, "effect_id")
        return request

    def send_order_execute(
        self,
        *,
        request_id: str,
        order: dict[str, object],
        accepted: bool,
        result: dict[str, object],
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None:
        if (execution_state, outcome) not in {
            (None, None),
            ("not_started", "expired_not_started"),
            ("sent", "unknown_after_send"),
        }:
            raise WorkerEnrollmentError("The worker cannot send an invalid order execution outcome.")
        response: dict[str, object] = {
            "type": "order_execute_response",
            "request_id": request_id,
            "accepted": accepted,
            "order": order,
            "result": result,
        }
        if execution_state is not None:
            response["execution_state"] = execution_state
            response["outcome"] = outcome
        try:
            _send(self.socket, self._relay_response(request_id, response) or response)
        except Exception as error:
            _raise_closed_connection(error, "order execution response")

    def send_order_execute_error(self, *, request_id: str, reason: str) -> None:
        try:
            response = {"type": "order_execute_error", "request_id": request_id, "reason": reason}
            _send(self.socket, self._relay_response(request_id, response) or response)
        except Exception as error:
            _raise_closed_connection(error, "order execution error response")

    def receive_trader_rpc(self) -> ScheduledTraderRpc | None:
        scheduled = self._trader_rpc_scheduler.next()
        if isinstance(scheduled, TraderRpcOutcome):
            self.dispatch_scheduler_outcome(scheduled)
            return None
        return scheduled

    def dispatch_scheduler_outcome(self, outcome: TraderRpcOutcome) -> None:
        """Deliver one scheduler-produced terminal outcome to its original requester.

        Used both by ``receive_trader_rpc`` for this session's own admitted
        work and by adapters (e.g. the Pair Execution Cell, via
        ``abt.worker.pair_cell_adapter``) that must fully serve any other
        scheduler item their own broker write happens to drain past, since
        both sides share this session's single
        :class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler`.
        """

        if outcome.worker_request_type is not None and outcome.worker_request is not None:
            self._send_hedge_scheduler_outcome(outcome.worker_request_type, outcome.worker_request, outcome)
            return
        self.send_trader_rpc(
            request_id=outcome.request_id,
            kind=outcome.kind,
            accepted=False,
            reason=f"{outcome.category}: {outcome.reason}",
        )

    def receive_worker_relay(self, timeout: float | None = None) -> bool:
        """Pull one pending controller-pushed message and route it by type.

        This single receive seam serves every push the controller may send on
        this connection: ordinary Trader-relay ``worker_relay`` requests, and
        the Pair Execution Cell's own opaque ``pair_relay_deliver``/
        ``pair_cell_activate``/``pair_cell_quarantine_release_request`` pushes
        and ``pair_relay_ack`` receipts. Each is queued for its own consumer
        so this method never confuses one kind of push for another, and
        interleaved traffic on the same socket is handled one message at a
        time in arrival order.
        """

        try:
            response = _message(self.socket, timeout=timeout)
        except TimeoutError:
            return False
        except Exception as error:
            _raise_closed_connection(error, "Worker relay request")
        response_type = response.get("type")
        if response_type == "worker_relay":
            self._queue_worker_relay(response)
            return True
        if response_type == "pair_relay_deliver":
            self._queue_pair_relay_envelope(response)
            return True
        if response_type == "pair_cell_activate":
            self._queue_pair_cell_activation(response)
            return True
        if response_type == "pair_cell_quarantine_release_request":
            self._queue_pair_cell_quarantine_release(response)
            return True
        if response_type == "pair_relay_ack":
            return True  # fire-and-forget sends do not correlate this synchronously
        raise WorkerEnrollmentError("The controller returned an invalid Worker relay request.")

    def _queue_pair_relay_envelope(self, response: dict[str, object]) -> None:
        if set(response) != {"type", "envelope"} or not isinstance(response.get("envelope"), dict):
            raise WorkerEnrollmentError("The controller returned an invalid Pair Execution Cell relay push.")
        self._pair_relay_inbox.append(cast(dict[str, object], response["envelope"]))

    def _queue_pair_cell_activation(self, response: dict[str, object]) -> None:
        if (
            set(response) != {"type", "contract", "lease"}
            or not isinstance(response.get("contract"), dict)
            or not isinstance(response.get("lease"), dict)
        ):
            raise WorkerEnrollmentError("The controller returned an invalid Pair Execution Cell activation push.")
        self._pair_cell_activation_inbox.append(
            {"contract": cast(dict[str, object], response["contract"]), "lease": cast(dict[str, object], response["lease"])}
        )

    def _queue_pair_cell_quarantine_release(self, response: dict[str, object]) -> None:
        if (
            set(response) != {"type", "request_id", "symbol", "actor", "reason", "observed_at"}
            or not isinstance(response.get("request_id"), str)
            or not response["request_id"]
            or not isinstance(response.get("symbol"), str)
            or not response["symbol"]
            or not isinstance(response.get("actor"), str)
            or not response["actor"]
            or not isinstance(response.get("reason"), str)
            or not response["reason"]
            or not isinstance(response.get("observed_at"), str)
            or not response["observed_at"]
        ):
            raise WorkerEnrollmentError(
                "The controller returned an invalid Pair Execution Cell quarantine-release request."
            )
        self._pair_cell_quarantine_release_inbox.append(
            {
                "request_id": cast(str, response["request_id"]),
                "symbol": cast(str, response["symbol"]),
                "actor": cast(str, response["actor"]),
                "reason": cast(str, response["reason"]),
                "observed_at": cast(str, response["observed_at"]),
            }
        )

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]:
        """Pop every opaque Pair Execution Cell envelope queued since the last drain."""

        envelopes, self._pair_relay_inbox = self._pair_relay_inbox, []
        return envelopes

    def drain_pair_cell_activations(self) -> list[dict[str, object]]:
        """Pop every controller-pushed ``{contract, lease}`` activation queued since the last drain."""

        activations, self._pair_cell_activation_inbox = self._pair_cell_activation_inbox, []
        return activations

    def drain_pair_cell_quarantine_releases(self) -> list[dict[str, object]]:
        """Pop every controller-pushed ``{request_id, symbol, actor, reason, observed_at}``
        quarantine-release request queued since the last drain."""

        releases, self._pair_cell_quarantine_release_inbox = self._pair_cell_quarantine_release_inbox, []
        return releases

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

    def send_pair_cell_quarantine_release_result(self, *, request_id: str, symbol: str, outcome: str) -> None:
        """Reply to one controller-pushed quarantine-release request with
        this Worker's own actual applied/rejected outcome.

        The controller correlates this by ``request_id`` against the pending
        ``pair_cell_quarantine_release_request`` it is awaiting, so the
        admin route never reports success unless every Worker's own cell
        genuinely applied (or trivially satisfied, i.e. the product was
        never quarantined here) the release -- it cannot be inferred from
        the push alone, since ``PairExecutionCell`` silently rejects a
        release while an attempt is unresolved.
        """

        try:
            _send(
                self.socket,
                {
                    "type": "pair_cell_quarantine_release_result",
                    "request_id": request_id,
                    "symbol": symbol,
                    "outcome": outcome,
                },
            )
        except Exception as error:
            _raise_closed_connection(error, "Pair Execution Cell quarantine-release result")

    def request_pair_cell_activation(self) -> dict[str, object] | None:
        """Synchronously ask the controller for this Worker's current pair
        lease and contract, used once at Worker startup (or reconnect) before
        entering the low-latency event loop -- not on any hot path."""

        try:
            _send(self.socket, {"type": "pair_cell_sync_request"})
            response = self._response()
            if response.get("type") != "pair_cell_sync" or set(response) != {"type", "contract", "lease"}:
                raise WorkerEnrollmentError("The controller returned an invalid Pair Execution Cell sync response.")
            contract = response["contract"]
            if contract is None:
                return None
            lease = response["lease"]
            if not isinstance(contract, dict) or not isinstance(lease, dict):
                raise WorkerEnrollmentError("The controller returned an invalid Pair Execution Cell sync response.")
            return {"contract": contract, "lease": lease}
        except Exception as error:
            _raise_closed_connection(error, "Pair Execution Cell activation sync")

    @property
    def trader_rpc_scheduler(self) -> DeadlineAwareTraderRpcScheduler:
        """The single scheduler this Worker uses for every source of broker
        writes. Adapters (e.g. the Pair Execution Cell) must share this exact
        instance to preserve serialized MT5 access and priority ordering."""

        return self._trader_rpc_scheduler

    def _queue_trader_rpc(self, response: dict[str, object]) -> None:
        envelope_fields = {
            "type", "request_id", "kind", "payload", "command_id", "effect_id",
            "payload_hash", "priority", "expires_at", "correlation",
        }
        if not set(response).issubset(envelope_fields):
            raise WorkerEnrollmentError("The controller returned an invalid Trader RPC request.")
        try:
            request = TraderRpcRequest.model_validate(
                {field: response[field] for field in ("type", "request_id", "kind", "payload")}
            )
        except ValidationError as error:
            raise WorkerEnrollmentError("The controller returned an invalid Trader RPC request.") from error
        scheduled = {
            **request.model_dump(mode="json", exclude_none=True),
            **{
                field: response[field]
                for field in ("command_id", "effect_id", "payload_hash", "priority", "expires_at", "correlation")
                if field in response
            },
        }
        outcome = self._trader_rpc_scheduler.admit(scheduled)
        if outcome is not None and outcome.terminal:
            self.send_trader_rpc(
                request_id=request.request_id,
                kind=request.kind,
                accepted=False,
                reason=f"{outcome.category}: {outcome.reason}",
            )

    def send_trader_rpc(
        self, *, request_id: str, kind: str, accepted: bool,
        result: dict[str, object] | None = None,
        reason: str | None = None,
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None:
        if kind not in {"read", "operation"} or not request_id or (accepted and result is None) or (not accepted and not reason):
            raise WorkerEnrollmentError("The worker cannot send an invalid Trader RPC response.")
        response = (
            TraderRpcSuccess(request_id=request_id, kind=kind, result=result).model_dump(mode="json")
            if accepted
            else TraderRpcFailure(request_id=request_id, kind=kind, reason=reason).model_dump(mode="json")
        )
        if not accepted:
            if execution_state is not None:
                response["execution_state"] = execution_state
            if outcome is not None:
                response["outcome"] = outcome
        try:
            _send(self.socket, self._relay_response(request_id, response) or response)
        except Exception as error:
            _raise_closed_connection(error, "Trader RPC response")

    def _queue_worker_relay(self, response: dict[str, object]) -> None:
        if set(response) not in ({"type", "envelope"}, {"type", "request_id", "envelope"}) or not isinstance(response.get("envelope"), dict):
            raise WorkerEnrollmentError("The controller returned an invalid Worker relay request.")
        try:
            envelope = worker_request_envelope_adapter.validate_python(response["envelope"])
        except ValidationError as error:
            raise WorkerEnrollmentError("The controller returned an invalid Worker relay request.") from error
        if self.worker_id and envelope.worker_id != self.worker_id:
            raise WorkerEnrollmentError("The controller routed a Worker relay request to the wrong Worker.")
        self._relay_requests[envelope.request_id] = envelope
        dumped = envelope.model_dump(mode="json")
        if isinstance(envelope, WorkerReadRequested):
            self._queue_trader_rpc(
                {
                    "type": "trader_rpc_request",
                    "request_id": envelope.request_id,
                    "kind": "read",
                    "payload": dumped["payload"],
                    "command_id": envelope.request_id,
                    "payload_hash": envelope.payload_hash,
                    "priority": "normal",
                    "correlation": dumped["correlation"],
                }
            )
            return
        operation = envelope.payload.operation
        if operation == "trader_operation":
            self._queue_trader_rpc(
                {
                    "type": "trader_rpc_request",
                    "request_id": envelope.request_id,
                    "kind": "operation",
                    "payload": dumped["payload"]["request"],
                    "command_id": envelope.effect_id,
                    "effect_id": envelope.effect_id,
                    "priority": "protection",
                    "expires_at": dumped["expires_at"],
                    "correlation": dumped["correlation"],
                }
            )
            return
        request = {
            "type": f"{operation}_request",
            "request_id": envelope.request_id,
            "order": dumped["payload"]["order"],
            "expires_at": dumped["expires_at"],
        }
        if operation == "order_execute":
            request["effect_id"] = envelope.effect_id
        self._queue_hedge_request(request)

    def _relay_response(
        self, request_id: str, response: dict[str, object]
    ) -> dict[str, object] | None:
        envelope = self._relay_requests.pop(request_id, None)
        if envelope is None:
            return None
        common = {
            "protocol_version": envelope.protocol_version,
            "trader_id": envelope.trader_id,
            "worker_id": envelope.worker_id,
            "runtime_command_id": envelope.runtime_command_id,
            "request_id": envelope.request_id,
            "payload_hash": envelope.payload_hash,
            "correlation": envelope.model_dump(mode="json")["correlation"],
            "recovery_epoch": self.recovery_epoch,
            "observed_at": datetime.now(UTC).isoformat(),
        }
        if isinstance(envelope, WorkerReadRequested):
            accepted = response.get("accepted") is True
            fact = {
                "type": "worker_read_completed",
                **common,
                "accepted": accepted,
                "snapshot_baseline": self.reconciliation_cursor,
                "result": response.get("result") if accepted else None,
                "reason": None if accepted else response.get("reason", "Worker read was rejected."),
            }
        else:
            accepted = response.get("accepted") is True
            result = response.get("result")
            if not isinstance(result, dict):
                result = {
                    key: response[key]
                    for key in ("order", "diagnostics")
                    if key in response
                }
            execution_state = response.get("execution_state")
            if execution_state is None:
                execution_state = "receipt" if accepted else "not_started"
            fact = {
                "type": "worker_effect_outcome",
                **common,
                "effect_id": envelope.effect_id,
                "accepted": accepted,
                "execution_state": execution_state,
                "outcome": response.get("outcome", "accepted" if accepted else "rejected"),
                "result": result,
                "reason": None if accepted else response.get("reason"),
            }
        return {"type": "worker_relay", "request_id": request_id, "envelope": fact}

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
            challenge = _message(certificate_socket)
            worker_id = _required_text(challenge, "worker_id")
            _send_proof(certificate_socket, key_store, challenge, "certificate_delivery", worker_id)
            delivery = _message(certificate_socket)
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
        challenge = _message(socket)
        _send_proof(socket, key_store, challenge, "worker_session", worker_id)
        authenticated = _message(socket)
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
