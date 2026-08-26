from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, time, timedelta
from queue import Queue
import unittest

from strategy.realtime_arbitrage import (
    Endpoint,
    Pair,
    RealtimeArbitrage,
    StrategyConfig,
    StrategyError,
    TraderGateway,
    WorkerMarketDataUnavailable,
)


class FakeGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.positions: dict[str, list[dict[str, object]]] = {}

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        self.calls.append((worker_id, kind, request))
        if request["type"] == "account_info":
            return {"account": {"balance": 5_000.0, "equity": 5_000.0}}
        if request["type"] == "symbol_info":
            return {"symbol": {"name": request["symbol"], "trade_mode": 4}}
        if request["type"] == "current_positions":
            return {"positions": self.positions.get(worker_id, [])}
        if request["type"] == "current_orders":
            return {"orders": []}
        if request["type"] == "calc_profit":
            direction = request["direction"]
            open_price = float(request["open_price"])
            close_price = float(request["close_price"])
            profit = (close_price - open_price) * 10_000 if direction == "LONG" else (open_price - close_price) * 10_000
            return {"profit": profit}
        return {"operation": request["type"], "result": {}}

    def query(self, query: str) -> dict[str, object]:
        self.calls.append(("", "query", {"query": query}))
        return {
            "workers": [
                {"worker_id": "audacity", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "ftmo", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "worker-a", "connectivity": "connected", "safety_state": "connected"},
                {"worker_id": "worker-b", "connectivity": "connected", "safety_state": "connected"},
            ]
        }


def configuration(*, execute: bool = True) -> StrategyConfig:
    return StrategyConfig(0.4, 0.1, 100, 0.03, 0.02, 1, 40, 5, 180, time(16), 300, execute)


class RealtimeArbitrageTests(unittest.TestCase):
    def test_quiet_trader_stream_is_not_treated_as_a_disconnect(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque()

        self.assertIsNone(gateway.market_data(timeout_seconds=0.001))

    def test_market_data_unavailable_identifies_the_disconnected_worker(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque(
            [{"type": "market_data_unavailable", "worker_id": "worker-a", "reason": "worker_session_unavailable"}]
        )

        with self.assertRaisesRegex(WorkerMarketDataUnavailable, "worker-a") as raised:
            gateway.market_data()

        self.assertEqual("worker-a", raised.exception.worker_id)

    def test_market_data_disconnect_suspends_strategy_until_the_grace_deadline(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration()
        )

        strategy._suspend_for_worker_disconnect(WorkerMarketDataUnavailable("worker-a", "worker_session_unavailable"))

        self.assertFalse(strategy.stopped)
        self.assertEqual({"first"}, strategy.unavailable_endpoints)
        self.assertIsNotNone(strategy.worker_disconnect_deadline)

    def test_shutdown_closes_every_position_on_each_worker_when_execution_is_enabled(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("audacity"), second=Endpoint("ftmo"), symbol="NZDUSD", config=configuration()
        )
        gateway.positions = {
            "audacity": [{"ticket": 11, "volume": 0.1}],
            "ftmo": [{"ticket": 22, "volume": 0.1}],
        }

        strategy.shutdown()

        closes = [(worker, request) for worker, kind, request in gateway.calls if kind == "operation" and request["type"] == "close"]
        self.assertEqual(
            [
                ("audacity", {"type": "close", "ticket": "11", "volume": "0.10"}),
                ("ftmo", {"type": "close", "ticket": "22", "volume": "0.10"}),
            ],
            closes,
        )

    def test_halts_before_any_order_when_calculated_margin_exceeds_half_equity(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration(execute=False)
        )
        strategy.margin_per_lot = {("first", "SHORT"): 30_000, ("second", "LONG"): 1_000}

        with self.assertRaisesRegex(StrategyError, "exceeds 50%"):
            strategy._open("short_first_long_second")

        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls))

    def test_emergency_protection_sets_broker_sl_and_tp_at_equal_dollar_amounts(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration()
        )

        strategy._set_emergency_protection(
            "first",
            {"ticket": 11, "price_open": 1.2},
            "LONG",
        )

        operation = next(
            request
            for worker, kind, request in gateway.calls
            if worker == "worker-a" and kind == "operation" and request["type"] == "modify_sl_tp"
        )
        self.assertLess(float(operation["sl"]), 1.2)
        self.assertGreater(float(operation["tp"]), 1.2)
        self.assertAlmostEqual(40, (1.2 - float(operation["sl"])) * 10_000, places=3)
        self.assertAlmostEqual(40, (float(operation["tp"]) - 1.2) * 10_000, places=3)

    def test_missing_active_pair_ticket_immediately_flattens_the_remaining_leg(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration()
        )
        strategy.pair = Pair(
            "long_first_short_second", 1.2, 1.2, 5_000, 5_000, 11, 22, "LONG", "SHORT", datetime.now(UTC)
        )
        gateway.positions = {
            "worker-a": [],
            "worker-b": [{"ticket": 22, "symbol": "EURUSD", "type": 1, "volume": 0.1}],
        }

        with self.assertRaisesRegex(StrategyError, "external position or order"):
            strategy._check_active_pair_integrity()

        self.assertIn(
            ("worker-b", "operation", {"type": "close", "ticket": "22", "volume": "0.10"}),
            gateway.calls,
        )

    def test_pair_cannot_close_from_reverse_signal_before_minimum_hold_time(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration()
        )
        strategy.pair = Pair(
            "long_first_short_second",
            1.2,
            1.2,
            5_000,
            5_000,
            11,
            22,
            "LONG",
            "SHORT",
            datetime.now(UTC) - timedelta(seconds=179),
        )

        self.assertFalse(strategy._minimum_hold_elapsed())
        strategy.pair.opened_at = datetime.now(UTC) - timedelta(seconds=180)
        self.assertTrue(strategy._minimum_hold_elapsed())

    def test_new_york_daily_cutoff_includes_the_configured_minute(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), symbol="EURUSD", config=configuration()
        )

        self.assertFalse(strategy._cutoff_reached(datetime(2026, 8, 26, 15, 59, tzinfo=UTC)))
        self.assertTrue(strategy._cutoff_reached(datetime(2026, 8, 26, 20, 0, tzinfo=UTC)))


if __name__ == "__main__":
    unittest.main()
