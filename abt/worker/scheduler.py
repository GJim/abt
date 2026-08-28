"""Deadline-aware scheduling for serialized Worker Trader RPC access."""

from __future__ import annotations

from collections import OrderedDict, deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from hashlib import sha256
import json
from typing import Literal


TraderRpcPriority = Literal["emergency", "execution", "protection", "normal", "background"]
TraderRpcOutcomeCategory = Literal[
    "completed",
    "rejected_preflight",
    "expired_not_started",
    "unknown_after_send",
    "rejected_worker_state",
]

_PRIORITIES: tuple[TraderRpcPriority, ...] = (
    "emergency",
    "execution",
    "protection",
    "normal",
    "background",
)
_BACKGROUND_OPERATIONS = {"calc_margin", "calc_margin_batch", "symbols", "historical_ticks"}
_PROTECTION_OPERATIONS = {"cancel", "close", "modify_sl_tp"}


class BrokerActionNotStarted(Exception):
    """The scheduler rejected a broker side effect before it was invoked."""

    def __init__(self, outcome: TraderRpcOutcome) -> None:
        super().__init__(outcome.reason)
        self.outcome = outcome


@dataclass(frozen=True)
class TraderRpcOutcome:
    """A stable Worker-local lifecycle classification and its timing evidence."""

    request_id: str
    category: TraderRpcOutcomeCategory
    reason: str
    telemetry: dict[str, object]
    terminal: bool = True
    kind: str = "read"
    worker_request_type: str | None = None
    worker_request: dict[str, object] | None = None


@dataclass
class ScheduledTraderRpc:
    """One selected MT5 access, including coalesced equivalent background reads."""

    requests: list[dict[str, object]]
    command_id: str
    payload_hash: str
    priority: TraderRpcPriority
    expires_at: datetime | None
    telemetry: dict[str, object]
    _scheduler: DeadlineAwareTraderRpcScheduler = field(repr=False)

    @property
    def request(self) -> dict[str, object]:
        return self.requests[0]

    def before_broker_send(self) -> None:
        """Check expiry at the last safe point, then record the MT5 send boundary."""

        outcome = self._scheduler._before_broker_send(self)
        if outcome is not None:
            raise BrokerActionNotStarted(outcome)

    def complete(
        self,
        category: TraderRpcOutcomeCategory,
        reason: str,
    ) -> TraderRpcOutcome:
        """Record and retain a terminal outcome for duplicate delivery."""

        return self._scheduler._complete(self, category, reason)


class DeadlineAwareTraderRpcScheduler:
    """Bound, prioritize, and classify serial Trader RPC work for one Worker.

    Callers admit raw, already-authenticated Trader RPC requests, take the next
    eligible item, invoke MT5 serially, and call ``complete``.  Broker-write
    implementations must call ``ScheduledTraderRpc.before_broker_send``
    immediately before ``order_send``.
    """

    def __init__(
        self,
        *,
        capacity: int = 128,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 1:
            raise ValueError("The Trader RPC scheduler capacity must be a positive integer.")
        self._capacity = capacity
        self._now = now
        self._queues: dict[TraderRpcPriority, deque[ScheduledTraderRpc]] = {
            priority: deque() for priority in _PRIORITIES
        }
        self._known: dict[str, tuple[str, ScheduledTraderRpc | TraderRpcOutcome]] = {}
        self._completed: OrderedDict[str, TraderRpcOutcome] = OrderedDict()
        self._frozen = False
        self._active: ScheduledTraderRpc | None = None

    @property
    def queued_count(self) -> int:
        return sum(len(queue) for queue in self._queues.values())

    def queue_depths(self) -> dict[str, int]:
        return {priority: len(self._queues[priority]) for priority in _PRIORITIES}

    def freeze(self) -> None:
        """Reject ordinary new work while retaining emergency recovery admission."""

        self._frozen = True

    def release(self) -> None:
        self._frozen = False

    def admit(self, request: Mapping[str, object]) -> TraderRpcOutcome | None:
        """Admit work or return its explicit pre-send/in-progress classification."""

        raw = dict(request)
        request_id = raw.get("request_id")
        payload = raw.get("payload")
        if not isinstance(request_id, str) or not request_id or not isinstance(payload, dict):
            return self._outcome(
                str(request_id) if isinstance(request_id, str) else "",
                "rejected_preflight",
                "The Trader RPC request is invalid before MT5 execution.",
                {},
                kind=str(raw.get("kind", "read")),
            )
        command_id = raw.get("command_id", request_id)
        if not isinstance(command_id, str) or not command_id:
            return self._outcome(
                request_id, "rejected_preflight", "The Trader command ID is invalid.", {}, kind=str(raw.get("kind", "read"))
            )
        try:
            payload_hash = _payload_hash(payload)
        except (TypeError, ValueError):
            return self._outcome(
                request_id,
                "rejected_preflight",
                "The Trader RPC payload is not canonical JSON.",
                {},
                kind=str(raw.get("kind", "read")),
            )
        declared_hash = raw.get("payload_hash")
        if declared_hash is not None and (not isinstance(declared_hash, str) or declared_hash != payload_hash):
            return self._outcome(
                request_id,
                "rejected_preflight",
                "The Trader RPC payload hash does not match.",
                {},
                kind=str(raw.get("kind", "read")),
            )
        known = self._known.get(command_id)
        if known is not None:
            known_hash, known_value = known
            if known_hash != payload_hash:
                return self._outcome(
                    request_id,
                    "rejected_preflight",
                    "The Trader command ID was reused with a different payload.",
                    {},
                    kind=str(raw.get("kind", "read")),
                )
            if isinstance(known_value, TraderRpcOutcome):
                return known_value
            return self._outcome(
                request_id,
                "rejected_preflight",
                "The Trader command is already in progress.",
                known_value.telemetry,
                terminal=False,
                kind=str(raw.get("kind", "read")),
            )
        try:
            priority = _priority(raw, payload)
            expires_at = _expiry(raw)
        except ValueError as error:
            return self._outcome(
                request_id, "rejected_preflight", str(error), {}, kind=str(raw.get("kind", "read"))
            )
        admitted_at = self._now_utc()
        telemetry = {
            "admitted_at": admitted_at.isoformat(),
            "queue_depth_by_priority": self.queue_depths(),
        }
        if self._frozen and priority != "emergency":
            outcome = self._outcome(
                request_id,
                "rejected_worker_state",
                "The Worker is frozen and only emergency recovery work is admissible.",
                telemetry,
                kind=str(raw.get("kind", "read")),
            )
            self._remember(command_id, payload_hash, outcome)
            return outcome
        durable_effect = isinstance(raw.get("effect_id"), str) and bool(raw.get("effect_id"))
        if expires_at is not None and admitted_at >= expires_at and not durable_effect:
            outcome = self._outcome(
                request_id,
                "expired_not_started",
                "The controller-issued expiry elapsed before queue admission.",
                telemetry,
                kind=str(raw.get("kind", "read")),
            )
            self._remember(command_id, payload_hash, outcome)
            return outcome
        if priority == "background":
            equivalent = self._equivalent_background(payload_hash)
            if equivalent is not None:
                equivalent.requests.append(raw)
                self._known[command_id] = (payload_hash, equivalent)
                return None
        if self.queued_count >= self._capacity:
            outcome = self._outcome(
                request_id,
                "rejected_preflight",
                "rejected_queue_full: the Worker Trader RPC queue is full.",
                telemetry,
                kind=str(raw.get("kind", "read")),
            )
            self._remember(command_id, payload_hash, outcome)
            return outcome
        scheduled = ScheduledTraderRpc(
            requests=[raw],
            command_id=command_id,
            payload_hash=payload_hash,
            priority=priority,
            expires_at=expires_at,
            telemetry=telemetry,
            _scheduler=self,
        )
        self._queues[priority].append(scheduled)
        telemetry["queue_depth_by_priority"] = self.queue_depths()
        self._known[command_id] = (payload_hash, scheduled)
        return None

    def next(self) -> ScheduledTraderRpc | TraderRpcOutcome | None:
        """Return the highest-priority FIFO item that may still start."""

        if self._active is not None:
            return None
        for priority in _PRIORITIES:
            queue = self._queues[priority]
            if not queue:
                continue
            scheduled = queue.popleft()
            now = self._now_utc()
            scheduled.telemetry["dequeued_at"] = now.isoformat()
            scheduled.telemetry["queue_wait_seconds"] = (
                now - _parse_timestamp(str(scheduled.telemetry["admitted_at"]))
            ).total_seconds()
            if scheduled.expires_at is not None:
                scheduled.telemetry["deadline_remaining_seconds"] = (scheduled.expires_at - now).total_seconds()
                durable_effect = isinstance(scheduled.request.get("effect_id"), str) and bool(
                    scheduled.request.get("effect_id")
                )
                if now >= scheduled.expires_at and not durable_effect:
                    return scheduled.complete(
                        "expired_not_started",
                        "The controller-issued expiry elapsed before MT5 execution began.",
                    )
            if self._frozen and scheduled.priority != "emergency":
                return scheduled.complete(
                    "rejected_worker_state",
                    "The Worker is frozen and ordinary queued work cannot execute.",
                )
            self._active = scheduled
            return scheduled
        return None

    def _before_broker_send(self, scheduled: ScheduledTraderRpc) -> TraderRpcOutcome | None:
        now = self._now_utc()
        if self._frozen and scheduled.priority != "emergency":
            return scheduled.complete(
                "rejected_worker_state",
                "The Worker is frozen and ordinary work cannot begin an MT5 action.",
            )
        if scheduled.expires_at is not None and now >= scheduled.expires_at:
            return scheduled.complete(
                "expired_not_started",
                "The controller-issued expiry elapsed before the MT5 action began.",
            )
        scheduled.telemetry["mt5_action_started_at"] = now.isoformat()
        return None

    def _complete(
        self,
        scheduled: ScheduledTraderRpc,
        category: TraderRpcOutcomeCategory,
        reason: str,
    ) -> TraderRpcOutcome:
        telemetry = dict(scheduled.telemetry)
        telemetry["completed_at"] = self._now_utc().isoformat()
        telemetry["final_category"] = category
        outcome = self._outcome(
            str(scheduled.request["request_id"]),
            category,
            reason,
            telemetry,
            kind=str(scheduled.request["kind"]),
            worker_request_type=_optional_text(scheduled.request, "worker_request_type"),
            worker_request=_optional_mapping(scheduled.request, "worker_request"),
        )
        for request in scheduled.requests:
            request_id = str(request["request_id"])
            command_id = request.get("command_id", request_id)
            if isinstance(command_id, str):
                self._remember(command_id, scheduled.payload_hash, replace(outcome, request_id=request_id, kind=str(request["kind"])))
        if self._active is scheduled:
            self._active = None
        return outcome

    def _equivalent_background(self, payload_hash: str) -> ScheduledTraderRpc | None:
        for scheduled in self._queues["background"]:
            if scheduled.payload_hash == payload_hash:
                return scheduled
        return None

    def _remember(self, command_id: str, payload_hash: str, outcome: TraderRpcOutcome) -> None:
        self._known[command_id] = (payload_hash, outcome)
        self._completed[command_id] = outcome
        self._completed.move_to_end(command_id)
        while len(self._completed) > self._capacity:
            stale_command_id, _ = self._completed.popitem(last=False)
            known = self._known.get(stale_command_id)
            if known is not None and isinstance(known[1], TraderRpcOutcome):
                del self._known[stale_command_id]

    def _outcome(
        self,
        request_id: str,
        category: TraderRpcOutcomeCategory,
        reason: str,
        telemetry: dict[str, object],
        *,
        terminal: bool = True,
        kind: str = "read",
        worker_request_type: str | None = None,
        worker_request: dict[str, object] | None = None,
    ) -> TraderRpcOutcome:
        return TraderRpcOutcome(
            request_id,
            category,
            reason,
            dict(telemetry),
            terminal,
            kind,
            worker_request_type,
            worker_request,
        )

    def _now_utc(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("The Trader RPC scheduler clock must return an aware UTC timestamp.")
        return value.astimezone(UTC)


def _priority(request: dict[str, object], payload: dict[str, object]) -> TraderRpcPriority:
    value = request.get("priority")
    if value is not None:
        if value in _PRIORITIES:
            return value
        raise ValueError("The Trader RPC priority is invalid.")
    operation = payload.get("type")
    if operation in {"market", "pending"}:
        return "execution"
    if operation in _PROTECTION_OPERATIONS:
        return "protection"
    if operation in _BACKGROUND_OPERATIONS:
        return "background"
    return "normal"


def _expiry(request: dict[str, object]) -> datetime | None:
    value = request.get("expires_at")
    if value is None:
        return None
    if not isinstance(value, str) or not value:
        raise ValueError("The controller-issued Trader RPC expiry is invalid.")
    try:
        return _parse_timestamp(value)
    except ValueError as error:
        raise ValueError("The controller-issued Trader RPC expiry is invalid.") from error


def _parse_timestamp(value: str) -> datetime:
    timestamp = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if timestamp.tzinfo is None:
        raise ValueError
    return timestamp.astimezone(UTC)


def _payload_hash(payload: dict[str, object]) -> str:
    return sha256(json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()).hexdigest()


def _optional_text(request: dict[str, object], field: str) -> str | None:
    value = request.get(field)
    return value if isinstance(value, str) and value else None


def _optional_mapping(request: dict[str, object], field: str) -> dict[str, object] | None:
    value = request.get(field)
    return value if isinstance(value, dict) else None
