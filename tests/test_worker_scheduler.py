from __future__ import annotations

from datetime import UTC, datetime, timedelta
import json
import unittest

from abt.worker.reconciliation import _serve_scheduled_trader_rpc
from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc, TraderRpcOutcome
from abt.worker.session import AuthenticatedWorkerSession


class WorkerSchedulerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = _Clock()

    def test_selects_priority_then_fifo_without_parallel_execution(self) -> None:
        scheduler = self._scheduler()
        for request_id, priority in (
            ("background", "background"),
            ("normal", "normal"),
            ("execution-one", "execution"),
            ("execution-two", "execution"),
            ("emergency", "emergency"),
        ):
            self.assertIsNone(scheduler.admit(_request(request_id, priority=priority)))

        selected: list[ScheduledTraderRpc | TraderRpcOutcome | None] = []
        for _ in range(5):
            item = scheduler.next()
            selected.append(item)
            self.assertIsInstance(item, ScheduledTraderRpc)
            item.complete("completed", "test completion")  # type: ignore[union-attr]

        self.assertEqual(
            ["emergency", "execution-one", "execution-two", "normal", "background"],
            [item.request["request_id"] for item in selected if isinstance(item, ScheduledTraderRpc)],
        )

    def test_bounds_admission_and_coalesces_equivalent_background_reads(self) -> None:
        scheduler = self._scheduler(capacity=1)

        self.assertIsNone(scheduler.admit(_request("catalog-one", priority="background")))
        self.assertIsNone(scheduler.admit(_request("catalog-two", priority="background")))
        saturated = scheduler.admit(_request("market", priority="execution"))

        self.assertIsInstance(saturated, TraderRpcOutcome)
        self.assertEqual("rejected_preflight", saturated.category)  # type: ignore[union-attr]
        self.assertIn("rejected_queue_full", saturated.reason)  # type: ignore[union-attr]
        self.assertEqual(1, scheduler.queued_count)
        selected = scheduler.next()
        self.assertIsInstance(selected, ScheduledTraderRpc)
        self.assertEqual(["catalog-one", "catalog-two"], [request["request_id"] for request in selected.requests])  # type: ignore[union-attr]

    def test_rejects_expired_work_at_admission_and_dequeue(self) -> None:
        scheduler = self._scheduler()
        expired = scheduler.admit(
            _request("already-expired", priority="execution", expires_at=self.clock.now() - timedelta(seconds=1))
        )
        self.assertIsNone(
            scheduler.admit(
                _request("expires-queued", priority="execution", expires_at=self.clock.now() + timedelta(seconds=1))
            )
        )
        self.clock.advance(seconds=1)
        dequeued = scheduler.next()

        self.assertEqual("expired_not_started", expired.category)  # type: ignore[union-attr]
        self.assertIsInstance(dequeued, TraderRpcOutcome)
        self.assertEqual("expired_not_started", dequeued.category)  # type: ignore[union-attr]
        self.assertIn("admitted_at", dequeued.telemetry)  # type: ignore[union-attr]
        self.assertIn("dequeued_at", dequeued.telemetry)  # type: ignore[union-attr]
        self.assertIn("completed_at", dequeued.telemetry)  # type: ignore[union-attr]

    def test_rechecks_expiry_immediately_before_order_send(self) -> None:
        scheduler = self._scheduler()
        self.assertIsNone(scheduler.admit(_request("expired-before-send", priority="execution", expires_at=self.clock.now() + timedelta(seconds=1))))
        scheduled = scheduler.next()
        self.assertIsInstance(scheduled, ScheduledTraderRpc)
        self.clock.advance(seconds=1)
        mt5 = _TradingMT5()
        session = _Session()

        _serve_scheduled_trader_rpc(mt5, session, scheduled)  # type: ignore[arg-type]

        self.assertEqual(0, mt5.order_send_calls)
        self.assertEqual("expired_not_started", session.responses[0]["reason"].split(":", 1)[0])

    def test_classifies_missing_order_send_response_as_unknown_after_send(self) -> None:
        scheduler = self._scheduler()
        self.assertIsNone(scheduler.admit(_request("unknown-response", priority="execution")))
        scheduled = scheduler.next()
        self.assertIsInstance(scheduled, ScheduledTraderRpc)
        mt5 = _TradingMT5(fail_after_send=True)
        session = _Session()

        _serve_scheduled_trader_rpc(mt5, session, scheduled)  # type: ignore[arg-type]

        self.assertEqual(1, mt5.order_send_calls)
        self.assertEqual("unknown_after_send", session.responses[0]["reason"].split(":", 1)[0])

    def test_freeze_rejects_ordinary_work_but_admits_emergency(self) -> None:
        scheduler = self._scheduler()
        scheduler.freeze()

        rejected = scheduler.admit(_request("normal", priority="normal"))
        self.assertIsNone(scheduler.admit(_request("emergency", priority="emergency")))
        emergency = scheduler.next()

        self.assertEqual("rejected_worker_state", rejected.category)  # type: ignore[union-attr]
        self.assertIsInstance(emergency, ScheduledTraderRpc)
        self.assertEqual("emergency", emergency.priority)  # type: ignore[union-attr]

    def test_does_not_release_another_mt5_call_until_active_work_completes(self) -> None:
        scheduler = self._scheduler()
        self.assertIsNone(scheduler.admit(_request("first", priority="execution")))
        self.assertIsNone(scheduler.admit(_request("second", priority="execution")))
        first = scheduler.next()

        self.assertIsInstance(first, ScheduledTraderRpc)
        self.assertIsNone(scheduler.next())
        first.complete("completed", "test completion")  # type: ignore[union-attr]
        second = scheduler.next()

        self.assertIsInstance(second, ScheduledTraderRpc)
        self.assertEqual("second", second.request["request_id"])  # type: ignore[union-attr]

    def test_preserves_command_idempotency_and_rejects_changed_payloads(self) -> None:
        scheduler = self._scheduler()
        request = _request("command", priority="execution")
        self.assertIsNone(scheduler.admit(request))

        in_progress = scheduler.admit(request)
        changed = {**request, "request_id": "changed-delivery", "payload": {**request["payload"], "volume": "0.02"}}
        changed_payload = scheduler.admit(changed)
        scheduled = scheduler.next()
        self.assertIsInstance(scheduled, ScheduledTraderRpc)
        scheduled.complete("completed", "authoritative receipt")  # type: ignore[union-attr]
        replay = scheduler.admit(request)

        self.assertFalse(in_progress.terminal)  # type: ignore[union-attr]
        self.assertEqual("rejected_preflight", changed_payload.category)  # type: ignore[union-attr]
        self.assertIn("different payload", changed_payload.reason)  # type: ignore[union-attr]
        self.assertEqual("completed", replay.category)  # type: ignore[union-attr]

    def test_session_routes_trader_requests_through_the_bounded_scheduler(self) -> None:
        session = AuthenticatedWorkerSession(socket=object(), reconciliation_cursor=0)  # type: ignore[arg-type]
        session._trader_rpc_scheduler = self._scheduler(capacity=3)

        session._queue_trader_rpc(_request("background", priority="background"))
        session._queue_hedge_request(
            {
                "type": "order_check_request",
                "analysis_id": "order_check",
                "request_id": "execution",
                "order": {"action": "market"},
            }
        )
        session._queue_hedge_request(
            {
                "type": "order_execute_request",
                "request_id": "execution-send",
                "order": {"action": "market"},
            }
        )
        scheduled = session.receive_trader_rpc()

        self.assertIsInstance(scheduled, ScheduledTraderRpc)
        self.assertEqual("order_check_request", scheduled.request["worker_request_type"])  # type: ignore[union-attr]
        self.assertEqual("execution", scheduled.request["worker_request"]["request_id"])  # type: ignore[index,union-attr]
        scheduled.complete("completed", "test completion")  # type: ignore[union-attr]
        dispatched = session.receive_trader_rpc()
        self.assertIsInstance(dispatched, ScheduledTraderRpc)
        self.assertEqual("order_execute_request", dispatched.request["worker_request_type"])  # type: ignore[union-attr]

    def test_session_accepts_aware_iso_expiry_on_order_requests(self) -> None:
        session = AuthenticatedWorkerSession(socket=object(), reconciliation_cursor=0)  # type: ignore[arg-type]
        order = {"action": "market"}

        order_check = session._parse_order_check(
            {
                "type": "order_check_request",
                "analysis_id": "order_check",
                "request_id": "check",
                "order": order,
                "expires_at": "2026-08-27T00:00:01Z",
            }
        )
        order_execute = session._parse_order_execute(
            {
                "type": "order_execute_request",
                "request_id": "execute",
                "order": order,
                "expires_at": "2026-08-27T00:00:01+00:00",
            }
        )

        self.assertEqual("2026-08-27T00:00:01+00:00", order_check["expires_at"])
        self.assertEqual("2026-08-27T00:00:01+00:00", order_execute["expires_at"])

    def test_queued_expired_hedge_request_returns_its_order_schema(self) -> None:
        socket = _RecordingSocket()
        session = AuthenticatedWorkerSession(socket=socket, reconciliation_cursor=0)  # type: ignore[arg-type]
        session._trader_rpc_scheduler = self._scheduler()
        order = {"action": "market"}
        session._queue_hedge_request(
            {
                "type": "order_execute_request",
                "request_id": "expired-hedge",
                "order": order,
                "expires_at": (self.clock.now() + timedelta(seconds=1)).isoformat(),
            }
        )
        self.clock.advance(seconds=1)

        self.assertIsNone(session.receive_trader_rpc())
        self.assertEqual(
            {
                "type": "order_execute_response",
                "request_id": "expired-hedge",
                "accepted": False,
                "order": order,
                "result": {},
                "execution_state": "not_started",
                "outcome": "expired_not_started",
            },
            json.loads(socket.sent[0]),
        )

    def _scheduler(self, *, capacity: int = 8) -> DeadlineAwareTraderRpcScheduler:
        return DeadlineAwareTraderRpcScheduler(capacity=capacity, now=self.clock.now)


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 27, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, *, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


class _Session:
    def __init__(self) -> None:
        self.responses: list[dict[str, object]] = []

    def send_trader_rpc(self, **response: object) -> None:
        self.responses.append(response)


class _TradingMT5:
    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    TRADE_ACTION_DEAL = 1
    TRADE_RETCODE_DONE = 10009

    def __init__(self, *, fail_after_send: bool = False) -> None:
        self.fail_after_send = fail_after_send
        self.order_send_calls = 0

    def terminal_info(self) -> object:
        return {"trade_allowed": True, "tradeapi_disabled": False}

    def symbol_info_tick(self, _symbol: str) -> object:
        return {"bid": 1.1, "ask": 1.1001}

    def order_send(self, _request: dict[str, object]) -> object:
        self.order_send_calls += 1
        if self.fail_after_send:
            raise TimeoutError("MT5 response unavailable")
        return {"retcode": self.TRADE_RETCODE_DONE}


class _RecordingSocket:
    def __init__(self) -> None:
        self.sent: list[str] = []

    def send(self, message: str) -> None:
        self.sent.append(message)


def _request(
    request_id: str,
    *,
    priority: str,
    expires_at: datetime | None = None,
) -> dict[str, object]:
    request: dict[str, object] = {
        "type": "trader_rpc_request",
        "request_id": request_id,
        "command_id": request_id,
        "kind": "operation",
        "priority": priority,
        "payload": {
            "type": "market",
            "symbol": "EURUSD",
            "volume": "0.01",
            "direction": "LONG",
            "filling_mode": "IOC",
        },
    }
    if expires_at is not None:
        request["expires_at"] = expires_at.isoformat()
    return request
