from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, time, timedelta
import math
from queue import Queue
from threading import Event
import unittest

from strategy.realtime_arbitrage import (
    Endpoint,
    Pair,
    RealtimeArbitrage,
    StrategyConfig,
    StrategyError,
    TradeCandidate,
    TraderGateway,
    WorkerMarketDataUnavailable,
    main,
)


class FakeGateway:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.positions: dict[str, list[dict[str, object]]] = {}
        self.symbols: dict[str, list[dict[str, object]]] = {}
        self.catalog_results: dict[str, dict[str, object]] = {}
        self.unavailable_catalog_workers: set[str] = set()
        self.live_symbol_results: dict[str, dict[str, object]] = {}
        self.live_symbol_rejections: set[str] = set()

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        self.calls.append((worker_id, kind, request))
        if request["type"] == "account_info":
            return {"account": {"balance": 5_000.0, "equity": 5_000.0}}
        if request["type"] == "symbols":
            if worker_id in self.unavailable_catalog_workers:
                raise StrategyError("Worker broker read unavailable.")
            if worker_id in self.catalog_results:
                return self.catalog_results[worker_id]
            return {
                "symbols": self.symbols.get(
                    worker_id,
                    [
                        {"name": "EURUSD", "trade_mode": 4, "point": 0.00001},
                        {"name": "GBPUSD", "trade_mode": 4, "point": 0.00001},
                        {"name": "XAUUSD", "trade_mode": 4, "point": 0.01},
                    ],
                )
            }
        if request["type"] == "set_live_symbols":
            if worker_id in self.live_symbol_rejections:
                raise StrategyError("Worker rejected live symbol configuration.")
            return self.live_symbol_results.get(worker_id, {"symbols": request["symbols"]})
        if request["type"] == "calc_margin":
            return {"margin": 1_000.0}
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

        strategy._suspend_for_worker_disconnect(WorkerMarketDataUnavailable("worker-a", "worker_session_unavailable"))

        self.assertFalse(strategy.stopped)
        self.assertEqual({"first"}, strategy.unavailable_endpoints)
        self.assertIsNotNone(strategy.worker_disconnect_deadline)

    def test_shutdown_closes_every_position_on_each_worker_when_execution_is_enabled(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("audacity"), second=Endpoint("ftmo"), config=configuration())
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration(execute=False))
        strategy.quotes["first"]["EURUSD"] = (datetime.now(UTC), 1.2, 1.2001)
        strategy.quotes["second"]["EURUSD"] = (datetime.now(UTC), 1.2, 1.2001)
        strategy.margin_per_lot = {
            ("EURUSD", "first", "SHORT"): (datetime.now(UTC), 30_000),
            ("EURUSD", "second", "LONG"): (datetime.now(UTC), 1_000),
        }
        calls_before_open = len(gateway.calls)

        with self.assertRaisesRegex(StrategyError, "exceeds 50%"):
            strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))

        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls[calls_before_open:]))

    def test_emergency_protection_sets_broker_sl_and_tp_at_equal_dollar_amounts(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

        strategy._set_emergency_protection(
            "first", "EURUSD",
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        strategy.pair = Pair(
            "EURUSD", "long_first_short_second", 1.2, 1.2, 5_000, 5_000, 11, 22, "LONG", "SHORT", datetime.now(UTC)
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        strategy.pair = Pair(
            "EURUSD",
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
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

        self.assertFalse(strategy._cutoff_reached(datetime(2026, 8, 26, 15, 59, tzinfo=UTC)))
        self.assertTrue(strategy._cutoff_reached(datetime(2026, 8, 26, 20, 0, tzinfo=UTC)))

    def test_uses_startup_catalog_for_common_tradable_live_market_watch_quotes(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        now = datetime.now(UTC).isoformat()

        strategy._consume({"worker_id": "worker-a", "observed_at": now, "quotes": [{"symbol": "EURUSD", "bid": 1.1010, "ask": 1.1011}, {"symbol": "GBPUSD", "bid": 1.3, "ask": 1.3001}]})
        strategy._consume({"worker_id": "worker-b", "observed_at": now, "quotes": [{"symbol": "EURUSD", "bid": 1.1000, "ask": 1.1001}, {"symbol": "USDJPY", "bid": 150.0, "ask": 150.01}]})

        candidate = strategy._best_candidate()

        self.assertEqual(("EURUSD", "short_first_long_second"), (candidate.symbol, candidate.direction))
        catalog_reads = [worker for worker, kind, request in gateway.calls if kind == "read" and request["type"] == "symbols"]
        self.assertEqual(["worker-a", "worker-b"], catalog_reads)

    def test_configures_every_shared_symbol_once_on_both_workers_at_startup(self) -> None:
        gateway = FakeGateway()
        gateway.symbols = {
            "worker-a": [
                {"name": "XAUUSD", "trade_mode": 4, "point": 0.01},
                {"name": "EURUSD", "trade_mode": 4, "point": 0.00001},
                {"name": "GBPUSD", "trade_mode": 4, "point": 0.00001},
            ],
            "worker-b": [
                {"name": "GBPUSD", "trade_mode": 4, "point": 0.00001},
                {"name": "XAUUSD", "trade_mode": 4, "point": 0.01},
                {"name": "EURUSD", "trade_mode": 4, "point": 0.00001},
            ],
        }

        RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration(execute=False))

        requests = [
            (worker, request)
            for worker, kind, request in gateway.calls
            if kind == "operation" and request["type"] == "set_live_symbols"
        ]
        self.assertEqual(
            [
                ("worker-a", {"type": "set_live_symbols", "symbols": ["EURUSD", "GBPUSD", "XAUUSD"]}),
                ("worker-b", {"type": "set_live_symbols", "symbols": ["EURUSD", "GBPUSD", "XAUUSD"]}),
            ],
            requests,
        )

    def test_invalid_live_symbol_configuration_acknowledgment_fails_closed(self) -> None:
        gateway = FakeGateway()
        gateway.live_symbol_results["worker-a"] = {"symbols": ["EURUSD"]}

        with self.assertRaisesRegex(StrategyError, "invalid live symbol configuration"):
            RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration(execute=False))

    def test_selects_greatest_edge_across_shared_symbols(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        now = datetime.now(UTC)
        strategy.quotes["first"] = {
            "GBPUSD": (now, 1.4020, 1.4021),
            "EURUSD": (now, 1.2010, 1.2011),
        }
        strategy.quotes["second"] = {
            "GBPUSD": (now, 1.4000, 1.4001),
            "EURUSD": (now, 1.2000, 1.2001),
        }

        candidate = strategy._best_candidate()

        self.assertEqual(("GBPUSD", "short_first_long_second"), (candidate.symbol, candidate.direction))

    def test_exact_name_intersection_does_not_normalize_symbol_suffixes(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        now = datetime.now(UTC)
        strategy.quotes["first"]["EURUSD"] = (now, 1.1010, 1.1011)
        strategy.quotes["second"]["EURUSD.a"] = (now, 1.1000, 1.1001)
        calls_before_selection = len(gateway.calls)

        self.assertIsNone(strategy._best_candidate())
        self.assertEqual([], gateway.calls[calls_before_selection:])

    def test_entry_threshold_uses_the_larger_broker_point(self) -> None:
        gateway = FakeGateway()
        gateway.symbols = {
            "worker-a": [{"name": "EURUSD", "trade_mode": 4, "point": 0.00001}],
            "worker-b": [{"name": "EURUSD", "trade_mode": 4, "point": 0.0001}],
        }
        config = StrategyConfig(5, 0.1, 100, 0.03, 0.02, 1, 40, 5, 180, time(16), 300, False)
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=config)
        now = datetime.now(UTC)
        strategy.quotes["first"]["EURUSD"] = (now, 1.10055, 1.10065)
        strategy.quotes["second"]["EURUSD"] = (now, 1.10000, 1.10010)

        self.assertIsNone(strategy._candidate_for_symbol("EURUSD"))
        strategy.quotes["first"]["EURUSD"] = (now, 1.10060, 1.10070)
        self.assertEqual("short_first_long_second", strategy._candidate_for_symbol("EURUSD").direction)

    def test_best_candidate_makes_no_broker_rpc_after_startup_catalog_load(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        now = datetime.now(UTC)
        strategy.quotes["first"]["EURUSD"] = (now, 1.1010, 1.1011)
        strategy.quotes["second"]["EURUSD"] = (now, 1.1000, 1.1001)
        calls_before_selection = len(gateway.calls)

        candidate = strategy._best_candidate()

        self.assertEqual(("EURUSD", "short_first_long_second"), (candidate.symbol, candidate.direction))
        self.assertEqual([], gateway.calls[calls_before_selection:])

    def test_invalid_or_unavailable_startup_catalog_fails_closed(self) -> None:
        for result in (
            {"symbols": "not-a-list"},
            {"symbols": [{"name": "EURUSD", "trade_mode": 4, "point": math.nan}]},
        ):
            with self.subTest(result=result):
                gateway = FakeGateway()
                gateway.catalog_results["worker-a"] = result

                with self.assertRaisesRegex(StrategyError, "invalid symbol catalog"):
                    RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

    def test_startup_catalog_ignores_untradable_symbols(self) -> None:
        gateway = FakeGateway()
        gateway.symbols = {
            "worker-a": [
                {"name": "EURUSD", "trade_mode": 4, "point": 0.00001},
                {"name": "GBPUSD", "trade_mode": 3, "point": 0.00001},
            ],
            "worker-b": [
                {"name": "EURUSD", "trade_mode": 4, "point": 0.00001},
                {"name": "GBPUSD", "trade_mode": 3, "point": 0.00001},
            ],
        }

        strategy = RealtimeArbitrage(
            gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration()
        )

        self.assertEqual({"EURUSD": 0.00001}, strategy.shared_symbols)

    def test_unavailable_startup_catalog_fails_closed(self) -> None:
        gateway = FakeGateway()
        gateway.unavailable_catalog_workers.add("worker-a")

        with self.assertRaisesRegex(StrategyError, "no symbol catalog"):
            RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

    def test_active_pair_never_opens_a_different_symbol(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration(execute=False))
        strategy.pair = Pair("EURUSD", "short_first_long_second", 1.2, 1.2, 5_000, 5_000, 0, 0, "SHORT", "LONG", datetime.now(UTC))
        opened: list[TradeCandidate] = []
        strategy._open = opened.append  # type: ignore[method-assign]
        strategy._cutoff_reached = lambda: False  # type: ignore[method-assign]
        timestamp = datetime.now(UTC).isoformat()
        stop, messages = Event(), [
            {"worker_id": "worker-b", "observed_at": timestamp, "quotes": [{"symbol": "GBPUSD", "bid": 1.4, "ask": 1.4001}]},
            {"worker_id": "worker-a", "observed_at": timestamp, "quotes": [{"symbol": "GBPUSD", "bid": 1.401, "ask": 1.4011}]},
        ]

        def market_data() -> dict[str, object] | None:
            if messages:
                return messages.pop()
            stop.set()
            return None

        gateway.market_data = market_data  # type: ignore[method-assign]
        strategy.run(stop)

        self.assertEqual([], opened)

    def test_active_pair_symbol_drives_integrity_and_pnl(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        strategy.pair = Pair("XAUUSD", "short_first_long_second", 2_000, 2_000, 5_000, 5_000, 11, 22, "SHORT", "LONG", datetime.now(UTC))
        gateway.positions = {
            "worker-a": [{"ticket": 11, "symbol": "XAUUSD", "type": 1, "volume": 0.1}],
            "worker-b": [{"ticket": 22, "symbol": "XAUUSD", "type": 0, "volume": 0.1}],
        }
        now = datetime.now(UTC)
        strategy.quotes["first"]["XAUUSD"] = (now, 1_999, 2_001)
        strategy.quotes["second"]["XAUUSD"] = (now, 2_002, 2_003)

        strategy._check_integrity()
        self.assertEqual({"first": -10_000.0, "second": 20_000.0}, strategy._pnl())
        profit_requests = [
            request
            for _, kind, request in gateway.calls
            if kind == "read" and request["type"] == "calc_profit"
        ]
        self.assertEqual(
            [
                {
                    "type": "calc_profit",
                    "symbol": "XAUUSD",
                    "volume": "0.10",
                    "direction": "SHORT",
                    "open_price": "2000.0000000000",
                    "close_price": "2001.0000000000",
                },
                {
                    "type": "calc_profit",
                    "symbol": "XAUUSD",
                    "volume": "0.10",
                    "direction": "LONG",
                    "open_price": "2000.0000000000",
                    "close_price": "2002.0000000000",
                },
            ],
            profit_requests,
        )

    def test_removed_symbol_and_pip_flags_are_rejected(self) -> None:
        with self.assertRaises(SystemExit):
            main(["--symbol", "EURUSD"])
        with self.assertRaises(SystemExit):
            main(["--entry-edge-pips", "0.4"])


if __name__ == "__main__":
    unittest.main()
