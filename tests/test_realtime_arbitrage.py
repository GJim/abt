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
    HedgedEntryContained,
    SizingPlan,
    StrategyConfig,
    StrategyError,
    TradeCandidate,
    TraderGateway,
    WorkerMarketDataUnavailable,
    WorkerRpcUnavailable,
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
        self.hedged_entries: list[dict[str, object]] = []
        self.hedged_entry_result: dict[str, object] = {"command_id": "entry", "status": "accepted", "legs": []}
        self.protected_pairs: list[tuple[str, list[dict[str, object]]]] = []

    def request(self, worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
        self.calls.append((worker_id, kind, request))
        if request["type"] == "account_info":
            return {"account": {"balance": 5_000.0, "equity": 5_000.0}}
        if request["type"] == "symbols":
            if worker_id in self.unavailable_catalog_workers:
                raise StrategyError("Worker broker read unavailable.")
            if worker_id in self.catalog_results:
                return self.catalog_results[worker_id]
            catalog = self.symbols.get(
                worker_id,
                [
                    {"name": "EURUSD"},
                    {"name": "GBPUSD"},
                    {"name": "XAUUSD", "point": 0.01, "trade_tick_size": 0.01, "digits": 2},
                ],
            )
            return {"symbols": [self.symbol_specification(symbol) for symbol in catalog]}
        if request["type"] == "set_live_symbols":
            if worker_id in self.live_symbol_rejections:
                raise StrategyError("Worker rejected live symbol configuration.")
            return self.live_symbol_results.get(worker_id, {"symbols": request["symbols"]})
        if request["type"] == "calc_margin":
            return {"margin": 1_000.0 * float(request["volume"])}
        if request["type"] == "calc_margin_batch":
            return {
                "margins": [
                    {**calculation, "margin": 1_000.0 * float(calculation["volume"])}
                    for calculation in request["calculations"]
                ]
            }
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

    def protected_pair(self, pair_id: str, legs: list[dict[str, object]]) -> dict[str, object]:
        self.protected_pairs.append((pair_id, legs))
        return {"pair_id": pair_id, "lifecycle_state": "CATCHING_UP"}

    def hedged_entry(self, payload: dict[str, object]) -> dict[str, object]:
        self.hedged_entries.append(payload)
        return self.hedged_entry_result

    @staticmethod
    def symbol_specification(symbol: dict[str, object]) -> dict[str, object]:
        return {
            "trade_mode": 4,
            "trade_calc_mode": 0,
            "digits": 5,
            "point": 0.00001,
            "trade_tick_size": 0.00001,
            "trade_contract_size": 100_000.0,
            "volume_min": 0.01,
            "volume_step": 0.01,
            "volume_max": 100.0,
            "filling_mode": 3,
            "order_mode": 3,
            **symbol,
        }

    def query(self, query: str) -> dict[str, object]:
        self.calls.append(("", "query", {"query": query}))
        return {
            "workers": [
                {"worker_id": "audacity", "connectivity": "connected"},
                {"worker_id": "ftmo", "connectivity": "connected"},
                {"worker_id": "worker-a", "connectivity": "connected"},
                {"worker_id": "worker-b", "connectivity": "connected"},
            ]
        }


def configuration(
    *,
    execute: bool = True,
    max_exposure_usd: float | None = None,
    max_margin_ratio: float = 0.1,
) -> StrategyConfig:
    return StrategyConfig(
        0.4, 100, 0.03, 0.02, max_exposure_usd, max_margin_ratio, 1, 40, 5, 180, time(16), 300, 30, execute
    )


class RealtimeArbitrageTests(unittest.TestCase):
    def test_quiet_trader_stream_is_not_treated_as_a_disconnect(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque()

        self.assertIsNone(gateway.market_data(timeout_seconds=0.001))

    def test_rpc_rejection_preserves_worker_reason(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque()

        def reject(payload: dict[str, object]) -> None:
            gateway._messages.put(
                {
                    "type": "trader_rpc_result",
                    "request_id": payload["request_id"],
                    "status": "rejected",
                    "result": {"reason": "The MT5 market order was rejected: retcode=10030 comment='Unsupported filling mode'."},
                }
            )

        gateway._send = reject

        with self.assertRaisesRegex(StrategyError, "retcode=10030.*Unsupported filling mode"):
            gateway.request("worker-a", kind="operation", request={"type": "market"})

    def test_retryable_rpc_rejection_identifies_unavailable_worker_and_kind(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque()

        def reject(payload: dict[str, object]) -> None:
            gateway._messages.put(
                {
                    "type": "trader_rpc_result",
                    "request_id": payload["request_id"],
                    "status": "rejected",
                    "result": {"reason": "Selected worker is disconnected.", "retryable": True},
                }
            )

        gateway._send = reject

        with self.assertRaises(WorkerRpcUnavailable) as raised:
            gateway.request("worker-a", kind="read", request={"type": "current_orders"})

        self.assertEqual(("worker-a", "read"), (raised.exception.worker_id, raised.exception.kind))

    def test_hedged_entry_recovers_its_durable_outcome_from_a_replayed_event(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages = Queue()
        gateway._deferred = deque()

        def replay(payload: dict[str, object]) -> None:
            gateway._messages.put(
                {
                    "type": "event",
                    "event_id": 17,
                    "event_type": "hedged_entry_completed",
                    "payload": {
                        "command_id": payload["command_id"],
                        "outcome": {"command_id": payload["command_id"], "status": "accepted", "legs": []},
                    },
                }
            )

        gateway._send = replay

        result = gateway.hedged_entry({"type": "hedged_entry"})

        self.assertEqual("accepted", result["status"])

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

    def test_read_rpc_disconnect_suspends_active_pair_for_read_grace_period(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        strategy.pair = Pair(
            "EURUSD", "long_first_short_second", 1.2, 1.2, 11, 22, "LONG", "SHORT", 0.1, datetime.now(UTC)
        )
        gateway.positions = {
            "worker-a": [{"ticket": 11, "symbol": "EURUSD", "type": 0, "volume": 0.1}],
            "worker-b": [{"ticket": 22, "symbol": "EURUSD", "type": 1, "volume": 0.1}],
        }
        request = gateway.request

        def unavailable(worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
            if worker_id == "worker-b" and request["type"] == "current_orders":
                raise WorkerRpcUnavailable(worker_id, kind, "Worker session is unavailable.")
            return request_gateway(worker_id, kind=kind, request=request)

        request_gateway = request
        gateway.request = unavailable  # type: ignore[method-assign]

        with self.assertRaises(WorkerRpcUnavailable):
            strategy._check_active_pair_integrity()
        strategy._suspend_for_worker_disconnect(WorkerRpcUnavailable("worker-b", "read", "Worker session is unavailable."))

        assert strategy.worker_disconnect_deadline is not None
        remaining = (strategy.worker_disconnect_deadline - datetime.now(UTC)).total_seconds()
        self.assertFalse(strategy.stopped)
        self.assertGreater(remaining, 299)
        self.assertLessEqual(remaining, 300)

    def test_write_rpc_disconnect_uses_short_grace_and_requires_safety_shutdown_after_recovery(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        gateway.positions = {"worker-a": [{"ticket": 11, "volume": 0.1}]}

        strategy._suspend_for_worker_disconnect(WorkerRpcUnavailable("worker-b", "operation", "Worker session is unavailable."))

        assert strategy.worker_disconnect_deadline is not None
        remaining = (strategy.worker_disconnect_deadline - datetime.now(UTC)).total_seconds()
        self.assertTrue(strategy.unresolved_write_failure)
        self.assertGreater(remaining, 29)
        self.assertLessEqual(remaining, 30)
        with self.assertRaisesRegex(StrategyError, "operation outcome was unavailable"):
            strategy._recover_worker_market_data({"worker_id": "worker-b"})
        self.assertTrue(strategy.stopped)
        self.assertIn(
            ("worker-a", "operation", {"type": "close", "ticket": "11", "volume": "0.10"}),
            gateway.calls,
        )

    def test_daily_equity_read_disconnect_suspends_before_any_trading_decision(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        stop = Event()
        request = gateway.request

        def unavailable(worker_id: str, *, kind: str, request: dict[str, object]) -> dict[str, object]:
            if worker_id == "worker-a" and request["type"] == "account_info":
                stop.set()
                raise WorkerRpcUnavailable(worker_id, kind, "Worker session is unavailable.")
            return request_gateway(worker_id, kind=kind, request=request)

        request_gateway = request
        gateway.request = unavailable  # type: ignore[method-assign]

        strategy.run(stop)

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

    def test_skips_entry_when_margin_budget_cannot_cover_minimum_volume(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False, max_exposure_usd=5_000),
        )
        strategy.quotes["first"]["EURUSD"] = (datetime.now(UTC), 1.2, 1.2001)
        strategy.quotes["second"]["EURUSD"] = (datetime.now(UTC), 1.2, 1.2001)
        strategy.sizing_plans = {
            ("EURUSD", "short_first_long_second"): SizingPlan(
                volume=0.0,
                first_margin_limit=400,
                second_margin_limit=400,
                computed_at=datetime.now(UTC),
            )
        }
        calls_before_open = len(gateway.calls)

        strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))

        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls[calls_before_open:]))

    def test_cached_sizing_snapshot_reserves_headroom_and_becomes_stale_after_ten_minutes(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False, max_exposure_usd=5_000),
        )
        now = datetime.now(UTC)
        strategy.quotes["first"]["EURUSD"] = (now, 1.2, 1.2001)
        strategy.quotes["second"]["EURUSD"] = (now, 1.2, 1.2001)

        for symbol in strategy.shared_symbols:
            strategy.quotes["first"][symbol] = (now, 1.2, 1.2001)
            strategy.quotes["second"][symbol] = (now, 1.2, 1.2001)

        strategy._refresh_sizing_snapshot_if_due()

        plan = strategy._sizing_plan_for("EURUSD", "short_first_long_second")
        assert plan is not None
        self.assertEqual(0.4, plan.volume)
        self.assertEqual((400.0, 400.0), (plan.first_margin_limit, plan.second_margin_limit))
        batches = [
            (worker, request["calculations"])
            for worker, kind, request in gateway.calls
            if kind == "read" and request["type"] == "calc_margin_batch"
        ]
        self.assertEqual(2, len(batches))
        strategy.sizing_plans[("EURUSD", "short_first_long_second")] = SizingPlan(
            volume=0.4,
            first_margin_limit=400,
            second_margin_limit=400,
            computed_at=datetime.now(UTC) - timedelta(minutes=10),
        )
        self.assertIsNone(strategy._sizing_plan_for("EURUSD", "short_first_long_second"))

    def test_open_uses_one_hedged_entry_command_not_two_market_operations(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=True, max_exposure_usd=5_000),
        )
        now = datetime.now(UTC)
        for symbol in strategy.shared_symbols:
            strategy.quotes["first"][symbol] = (now, 1.2, 1.2001)
            strategy.quotes["second"][symbol] = (now, 1.2, 1.2001)
        strategy._refresh_sizing_snapshot_if_due()
        gateway.positions = {
            "worker-a": [{"ticket": 11, "symbol": "EURUSD", "type": 1, "volume": 0.4, "price_open": 1.2}],
            "worker-b": [{"ticket": 22, "symbol": "EURUSD", "type": 0, "volume": 0.4, "price_open": 1.2001}],
        }
        calls_before_open = len(gateway.calls)

        strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))

        self.assertEqual(1, len(gateway.hedged_entries))
        self.assertIn("expires_at", gateway.hedged_entries[0])
        self.assertEqual(
            [
                {
                    "worker_id": "worker-a",
                    "symbol": "EURUSD",
                    "volume": "0.40",
                    "direction": "SHORT",
                    "filling_mode": "FOK",
                    "margin_limit": "400.00",
                },
                {
                    "worker_id": "worker-b",
                    "symbol": "EURUSD",
                    "volume": "0.40",
                    "direction": "LONG",
                    "filling_mode": "FOK",
                    "margin_limit": "400.00",
                },
            ],
            [
                gateway.hedged_entries[0]["first"],
                gateway.hedged_entries[0]["second"],
            ],
        )
        self.assertFalse(
            any(
                kind == "operation" and request["type"] == "market"
                for _, kind, request in gateway.calls[calls_before_open:]
            )
        )
        self.assertEqual("entry", gateway.protected_pairs[0][0])
        self.assertEqual(["worker-a", "worker-b"], [leg["worker_id"] for leg in gateway.protected_pairs[0][1]])

    def test_contained_hedged_entry_never_falls_back_to_individual_market_or_close_operations(self) -> None:
        gateway = FakeGateway()
        gateway.hedged_entry_result = {
            "command_id": "entry",
            "status": "contained",
            "reason": "A market-leg outcome was failed or unknown after paired dispatch.",
            "legs": [],
        }
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=True, max_exposure_usd=5_000),
        )
        now = datetime.now(UTC)
        for symbol in strategy.shared_symbols:
            strategy.quotes["first"][symbol] = (now, 1.2, 1.2001)
            strategy.quotes["second"][symbol] = (now, 1.2, 1.2001)
        strategy._refresh_sizing_snapshot_if_due()
        calls_before_open = len(gateway.calls)

        with self.assertRaises(HedgedEntryContained):
            strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))
        strategy.shutdown()

        self.assertTrue(strategy.stopped)
        self.assertTrue(strategy.controller_containment_required)
        self.assertFalse(
            any(
                kind == "operation" and request["type"] in {"market", "close"}
                for _, kind, request in gateway.calls[calls_before_open:]
            )
        )

    def test_unacknowledged_hedged_entry_never_falls_back_to_individual_close_operations(self) -> None:
        gateway = FakeGateway()

        def unavailable(payload: dict[str, object]) -> dict[str, object]:
            gateway.hedged_entries.append(payload)
            raise StrategyError("Timed out waiting for the controller hedged-entry outcome.")

        gateway.hedged_entry = unavailable  # type: ignore[method-assign]
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=True, max_exposure_usd=5_000),
        )
        now = datetime.now(UTC)
        for symbol in strategy.shared_symbols:
            strategy.quotes["first"][symbol] = (now, 1.2, 1.2001)
            strategy.quotes["second"][symbol] = (now, 1.2, 1.2001)
        strategy._refresh_sizing_snapshot_if_due()
        calls_before_open = len(gateway.calls)

        with self.assertRaisesRegex(HedgedEntryContained, "acknowledgement was unavailable"):
            strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))
        strategy.shutdown()

        self.assertTrue(strategy.stopped)
        self.assertTrue(strategy.controller_containment_required)
        self.assertFalse(
            any(
                kind == "operation" and request["type"] in {"market", "close"}
                for _, kind, request in gateway.calls[calls_before_open:]
            )
        )

    def test_exposure_cap_never_exceeds_current_equity(self) -> None:
        strategy = RealtimeArbitrage(
            FakeGateway(),
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=10_000),
        )

        self.assertEqual({"first": 5_000, "second": 5_000}, strategy.exposure_caps)

    def test_emergency_protection_sets_broker_sl_and_tp_at_equal_dollar_amounts(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

        strategy._set_emergency_protection(
            "first", "EURUSD",
            {"ticket": 11, "price_open": 1.2},
            "LONG",
            0.1,
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

    def test_emergency_stop_loss_does_not_exceed_exposure_trade_loss_limit(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=1_000),
        )

        strategy._set_emergency_protection("first", "EURUSD", {"ticket": 11, "price_open": 1.2}, "LONG", 0.1)

        operation = next(
            request
            for worker, kind, request in gateway.calls
            if worker == "worker-a" and kind == "operation" and request["type"] == "modify_sl_tp"
        )
        self.assertAlmostEqual(20, (1.2 - float(operation["sl"])) * 10_000, places=3)
        self.assertAlmostEqual(20, (float(operation["tp"]) - 1.2) * 10_000, places=3)

    def test_emergency_stop_loss_does_not_exceed_remaining_daily_loss_limit(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=5_000),
        )
        strategy.accounts["first"].equity = 4_880

        strategy._set_emergency_protection("first", "EURUSD", {"ticket": 11, "price_open": 1.2}, "LONG", 0.1)

        operation = next(
            request
            for worker, kind, request in gateway.calls
            if worker == "worker-a" and kind == "operation" and request["type"] == "modify_sl_tp"
        )
        self.assertAlmostEqual(30, (1.2 - float(operation["sl"])) * 10_000, places=3)
        self.assertAlmostEqual(30, (float(operation["tp"]) - 1.2) * 10_000, places=3)

    def test_rejects_entry_after_daily_loss_budget_is_exhausted(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False, max_exposure_usd=5_000),
        )
        strategy.accounts["first"].equity = 4_850
        calls_before_open = len(gateway.calls)

        with self.assertRaisesRegex(StrategyError, "daily loss budget exhausted"):
            strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.001))

        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls[calls_before_open:]))

    def test_missing_active_pair_ticket_immediately_flattens_the_remaining_leg(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        strategy.pair = Pair(
            "EURUSD", "long_first_short_second", 1.2, 1.2, 11, 22, "LONG", "SHORT", 0.1, datetime.now(UTC)
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
            11,
            22,
            "LONG",
            "SHORT",
            0.1,
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

    def test_rejects_mismatched_hard_contract_terms(self) -> None:
        mismatches = {
            "trade_calc_mode": 1,
            "digits": 4,
            "point": 0.0001,
            "trade_tick_size": 0.0001,
            "trade_contract_size": 10_000.0,
            "volume_min": 0.1,
            "volume_step": 0.1,
            "order_mode": 1,
        }
        for field, second_value in mismatches.items():
            with self.subTest(field=field):
                gateway = FakeGateway()
                first = FakeGateway.symbol_specification({"name": "EURUSD"})
                second = {**first, field: second_value}
                gateway.symbols = {"worker-a": [first], "worker-b": [second]}

                with self.assertRaisesRegex(StrategyError, "no shared tradable symbols"):
                    RealtimeArbitrage(
                        gateway,
                        first=Endpoint("worker-a"),
                        second=Endpoint("worker-b"),
                        config=configuration(execute=False),
                    )

    def test_prefers_shared_fok_and_falls_back_to_shared_ioc(self) -> None:
        for first_filling_mode, second_filling_mode, expected in (
            (3, 3, "FOK"),
            (2, 2, "IOC"),
        ):
            with self.subTest(expected=expected):
                gateway = FakeGateway()
                gateway.symbols = {
                    "worker-a": [{"name": "EURUSD", "filling_mode": first_filling_mode}],
                    "worker-b": [{"name": "EURUSD", "filling_mode": second_filling_mode}],
                }
                strategy = RealtimeArbitrage(
                    gateway,
                    first=Endpoint("worker-a"),
                    second=Endpoint("worker-b"),
                    config=configuration(execute=True),
                )

                self.assertEqual(expected, strategy.shared_symbols["EURUSD"].filling_mode)

    def test_rejects_symbols_without_a_shared_executable_filling_mode(self) -> None:
        gateway = FakeGateway()
        gateway.symbols = {
            "worker-a": [{"name": "EURUSD", "filling_mode": 1}],
            "worker-b": [{"name": "EURUSD", "filling_mode": 2}],
        }

        with self.assertRaisesRegex(StrategyError, "no shared tradable symbols"):
            RealtimeArbitrage(
                gateway,
                first=Endpoint("worker-a"),
                second=Endpoint("worker-b"),
                config=configuration(execute=False),
            )

    def test_uses_trade_mode_directions_when_order_mode_is_unavailable(self) -> None:
        gateway = FakeGateway()
        gateway.symbols = {
            "worker-a": [{"name": "EURUSD", "order_mode": None}],
            "worker-b": [{"name": "EURUSD", "order_mode": None}],
        }

        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False),
        )

        self.assertEqual({"EURUSD"}, set(strategy.shared_symbols))

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

        self.assertEqual({"EURUSD"}, set(strategy.shared_symbols))

    def test_unavailable_startup_catalog_fails_closed(self) -> None:
        gateway = FakeGateway()
        gateway.unavailable_catalog_workers.add("worker-a")

        with self.assertRaisesRegex(StrategyError, "no symbol catalog"):
            RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())

    def test_active_pair_never_opens_a_different_symbol(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration(execute=False))
        strategy.pair = Pair("EURUSD", "short_first_long_second", 1.2, 1.2, 0, 0, "SHORT", "LONG", 0.1, datetime.now(UTC))
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
        strategy.pair = Pair("XAUUSD", "short_first_long_second", 2_000, 2_000, 11, 22, "SHORT", "LONG", 0.1, datetime.now(UTC))
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

    def test_removed_and_invalid_sizing_flags_are_rejected(self) -> None:
        for arguments in (
            ["--symbol", "EURUSD"],
            ["--entry-edge-pips", "0.4"],
            ["--lots", "0.1"],
            ["--max-margin-ratio", "0.51"],
            ["--max-exposure-usd", "0"],
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaises(SystemExit):
                    main(arguments)


if __name__ == "__main__":
    unittest.main()
