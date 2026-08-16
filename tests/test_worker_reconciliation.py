from __future__ import annotations

from datetime import UTC, datetime, timedelta
import unittest

from abt.worker.reconciliation import MT5ReconciliationAdapter, reconcile_authenticated_worker


class ReadOnlyMT5:
    def __init__(self) -> None:
        self.orders = [{"ticket": 41, "state": "placed", "volume_current": 1.0, "price_current": 2.0}]
        self.positions = [{"ticket": 51, "volume": 2.0, "profit": 3.0}]
        self.calls: list[str] = []
        self.broker_write_calls = 0

    def account_info(self) -> object:
        self.calls.append("account_info")
        return {"login": 123456, "server": "Broker-Demo", "balance": 1000}

    def terminal_info(self) -> object:
        self.calls.append("terminal_info")
        return {"connected": True, "trade_allowed": False}

    def orders_get(self) -> object:
        self.calls.append("orders_get")
        return self.orders

    def positions_get(self) -> object:
        self.calls.append("positions_get")
        return self.positions

    def order_send(self, *_: object, **__: object) -> object:
        self.broker_write_calls += 1
        raise AssertionError("Broker writes are prohibited.")


class WorkerReconciliationTests(unittest.TestCase):
    def test_emits_ten_minute_snapshot_and_only_lifecycle_or_volume_deltas(self) -> None:
        mt5 = ReadOnlyMT5()
        emitted: list[dict[str, object]] = []
        adapter = MT5ReconciliationAdapter(mt5, emit=emitted.append)
        started = datetime(2026, 8, 16, tzinfo=UTC)

        adapter.poll(started)
        mt5.orders[0]["price_current"] = 2.5
        mt5.positions[0]["profit"] = 4.0
        adapter.poll(started + timedelta(minutes=1))
        mt5.orders[0]["state"] = "filled"
        mt5.positions[0]["volume"] = 1.0
        adapter.poll(started + timedelta(minutes=2))
        adapter.poll(started + timedelta(minutes=10))

        self.assertEqual(["snapshot", "delta", "delta", "snapshot"], [event["type"] for event in emitted])
        self.assertEqual("state_changed", emitted[1]["change"])
        self.assertEqual("volume_changed", emitted[2]["change"])
        self.assertEqual(2, emitted[-1]["cursor"])
        self.assertEqual(["account_info", "terminal_info", "orders_get", "positions_get"] * 4, mt5.calls)
        self.assertEqual(0, mt5.broker_write_calls)

    def test_schedules_each_read_only_poll_one_minute_apart(self) -> None:
        mt5 = ReadOnlyMT5()
        adapter = MT5ReconciliationAdapter(mt5, emit=lambda _: None)
        moments = iter([
            datetime(2026, 8, 16, tzinfo=UTC),
            datetime(2026, 8, 16, 0, 1, tzinfo=UTC),
        ])
        sleeps: list[float] = []

        def stop_after_one_wait(seconds: float) -> None:
            sleeps.append(seconds)
            raise StopIteration

        with self.assertRaises(StopIteration):
            adapter.run_forever(now=lambda: next(moments), sleep=stop_after_one_wait)

        self.assertEqual([60], sleeps)
        self.assertEqual(["account_info", "terminal_info", "orders_get", "positions_get"], mt5.calls)

    def test_authenticates_with_memory_only_password_before_read_only_reconciliation(self) -> None:
        mt5 = ReadOnlyMT5()
        mt5.initialize = lambda: True  # type: ignore[attr-defined]
        mt5.login = lambda login, *, password, server: login == 123456 and password == "memory-only" and server == "Broker-Demo"  # type: ignore[attr-defined]
        mt5.shutdown = lambda: mt5.calls.append("shutdown")  # type: ignore[attr-defined]
        emitted: list[dict[str, object]] = []

        with self.assertRaises(StopIteration):
            reconcile_authenticated_worker(
                mt5=mt5,
                session=MemoryOnlySession(emitted),
                login=123456,
                server="Broker-Demo",
                now=lambda: datetime(2026, 8, 16, tzinfo=UTC),
                sleep=lambda _: (_ for _ in ()).throw(StopIteration),
            )

        self.assertEqual(["account_info", "terminal_info", "orders_get", "positions_get", "shutdown"], mt5.calls)
        self.assertEqual("snapshot", emitted[0]["type"])
        self.assertEqual(0, mt5.broker_write_calls)


class MemoryOnlySession:
    def __init__(self, emitted: list[dict[str, object]]) -> None:
        self._emitted = emitted

    def request_password(self) -> str:
        return "memory-only"

    def send_reconciliation(self, message: dict[str, object]) -> None:
        self._emitted.append(message)
