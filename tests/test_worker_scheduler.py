from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc, TraderRpcOutcome


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

    def test_expired_durable_effect_reaches_journal_replay_before_send_guard(self) -> None:
        scheduler = self._scheduler()
        request = _request(
            "durable-replay",
            priority="execution",
            expires_at=self.clock.now() - timedelta(seconds=1),
        )
        request["effect_id"] = "effect-1"

        self.assertIsNone(scheduler.admit(request))
        self.assertIsInstance(scheduler.next(), ScheduledTraderRpc)


    def _scheduler(self, *, capacity: int = 8) -> DeadlineAwareTraderRpcScheduler:
        return DeadlineAwareTraderRpcScheduler(capacity=capacity, now=self.clock.now)


class _Clock:
    def __init__(self) -> None:
        self.value = datetime(2026, 8, 27, tzinfo=UTC)

    def now(self) -> datetime:
        return self.value

    def advance(self, *, seconds: float) -> None:
        self.value += timedelta(seconds=seconds)


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
