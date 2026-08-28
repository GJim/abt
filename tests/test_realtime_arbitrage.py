from __future__ import annotations

from collections import deque
from datetime import UTC, datetime, time, timedelta
from hashlib import sha256
import json
import math
from queue import Queue
from threading import Condition, Event, Lock, Thread
from time import monotonic, sleep
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
    def test_worker_identity_selection_ignores_controller_recovery_projection(self) -> None:
        gateway = FakeGateway()
        original_query = gateway.query

        def query(name: str) -> dict[str, object]:
            result = original_query(name)
            for worker in result["workers"]:  # type: ignore[index]
                worker["recovery"] = {"lifecycle_state": "NEEDS_HUMAN"}  # type: ignore[index]
            return result

        gateway.query = query  # type: ignore[method-assign]

        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False),
        )

        self.assertIsNone(strategy.pair)

    def test_gateway_reconnects_resubscribes_and_resumes_worker_cursor(self) -> None:
        class Socket:
            def __init__(self) -> None:
                self.incoming: Queue[object] = Queue()
                self.sent: list[dict[str, object]] = []

            def send(self, raw: str) -> None:
                message = json.loads(raw)
                self.sent.append(message)
                if message["type"] == "subscribe":
                    self.incoming.put({"type": "subscribed", "worker_ids": message["worker_ids"]})
                elif message["type"] == "resume_workers":
                    self.incoming.put({"type": "workers_resumed", "worker_ids": ["worker-a"]})

            def recv(self, timeout: float | None = None) -> str:
                value = self.incoming.get(timeout=timeout)
                if isinstance(value, BaseException):
                    raise value
                return json.dumps(value)

            def __exit__(self, *_: object) -> None:
                self.incoming.put(OSError("closed"))

        first, second = Socket(), Socket()
        reconnected = Event()

        def reconnect() -> Socket:
            reconnected.set()
            return second

        gateway = TraderGateway(
            first,
            trader_id="trader-1",
            worker_ids=("worker-a",),
            reconnect=reconnect,
        )
        self.addCleanup(gateway.close)
        received_facts: list[dict[str, object]] = []
        gateway.set_fact_sink(received_facts.append)
        gateway.set_cursor_provider(lambda _: ("epoch-1", 7, True))
        first.incoming.put(OSError("disconnected"))

        self.assertTrue(reconnected.wait(1))
        deadline = monotonic() + 1
        while not any(message.get("type") == "resume_workers" for message in second.sent):
            if monotonic() >= deadline:
                self.fail("Gateway did not resume Worker streams after reconnect.")
            sleep(0.01)
        resume = next(message for message in second.sent if message["type"] == "resume_workers")
        self.assertEqual(
            [{"worker_id": "worker-a", "recovery_epoch": "epoch-1", "event_id": 7}],
            resume["cursors"],
        )
        fact = {
            "type": "worker_broker_delta",
            "protocol_version": 1,
            "worker_id": "worker-a",
            "recovery_epoch": "epoch-1",
            "event_id": 8,
            "observed_at": "2026-08-28T08:00:00+00:00",
            "entity": "position",
            "change": "remove",
            "record": {"ticket": 1},
        }
        second.incoming.put({"type": "worker_fact", "worker_id": "worker-a", "envelope": fact})
        deadline = monotonic() + 1
        while not any(message.get("type") == "ack_worker_fact" for message in second.sent):
            if monotonic() >= deadline:
                self.fail("Gateway did not acknowledge the committed Worker fact.")
            sleep(0.01)
        self.assertEqual([fact], received_facts)

    def test_gateway_disconnect_before_effect_outcome_is_explicit_uncertainty(self) -> None:
        class Socket:
            def __init__(self) -> None:
                self.incoming: Queue[object] = Queue()

            def send(self, raw: str) -> None:
                message = json.loads(raw)
                if message["type"] == "subscribe":
                    self.incoming.put({"type": "subscribed", "worker_ids": ["worker-a"]})
                elif message["type"] == "resume_workers":
                    self.incoming.put({"type": "workers_resumed", "worker_ids": ["worker-a"]})
                elif message["type"] == "relay":
                    self.incoming.put(OSError("lost after delivery"))

            def recv(self, timeout: float | None = None) -> str:
                value = self.incoming.get(timeout=timeout)
                if isinstance(value, BaseException):
                    raise value
                return json.dumps(value)

            def __exit__(self, *_: object) -> None:
                self.incoming.put(OSError("closed"))

        gateway = TraderGateway(
            Socket(), trader_id="trader-1", worker_ids=("worker-a",)
        )
        self.addCleanup(gateway.close)

        with self.assertRaisesRegex(StrategyError, "before the request outcome"):
            gateway.order_execute(
                "worker-a",
                order={
                    "action": "market",
                    "symbol": "EURUSD",
                    "volume": "0.10",
                    "direction": "LONG",
                    "filling_mode": "FOK",
                    "sl": "1.09",
                    "tp": "1.12",
                },
                runtime_command_id="command-1",
                request_id="execute-1",
                effect_id="effect-1",
                expires_at=datetime.now(UTC) + timedelta(seconds=30),
                correlation={"pair_id": "pair-1", "leg": "first", "purpose": "entry"},
            )

    def test_gateway_hashes_the_exact_serialized_effect_payload(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway.trader_id = "trader-1"
        gateway._disconnect_generation = 0
        sent: list[dict[str, object]] = []
        gateway._send = sent.append
        gateway._wait_for = lambda *_args, **_kwargs: {
            "type": "relay",
            "envelope": {
                "request_id": "execute-1",
                "accepted": True,
                "result": {"position": 101},
            },
        }

        gateway.request(
            "worker-a",
            kind="order_execute",
            request={
                "action": "market",
                "symbol": "EURUSD",
                "volume": "0.10",
                "direction": "LONG",
                "filling_mode": "FOK",
                "sl": "1.09",
                "tp": "1.12",
            },
            runtime_command_id="command-1",
            request_id="execute-1",
            effect_id="effect-1",
        )

        envelope = sent[0]["envelope"]
        self.assertIsInstance(envelope, dict)
        payload = envelope["payload"]  # type: ignore[index]
        self.assertNotIn("request", payload)
        canonical = json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        self.assertEqual(sha256(canonical.encode()).hexdigest(), envelope["payload_hash"])  # type: ignore[index]

    def test_fact_sink_ignores_buffered_unselected_workers_and_preserves_order(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._fact_delivery_lock = Lock()
        gateway._fact_queue = Queue()
        gateway._messages_ready = Condition()
        gateway._closing = Event()
        gateway._worker_ids = ("worker-a", "worker-b")
        gateway._fact_sink = None
        gateway._inbox = deque([
            {"type": "worker_fact", "envelope": {"worker_id": "worker-c", "recovery_epoch": "epoch", "event_id": 1}},
            {"type": "worker_fact", "envelope": {"worker_id": "worker-a", "recovery_epoch": "epoch", "event_id": 2}},
        ])
        gateway._send_raw = lambda _message: None
        delivered: list[int] = []
        fact_thread = Thread(target=gateway._deliver_worker_facts_forever, daemon=True)
        fact_thread.start()

        gateway.set_fact_sink(lambda fact: delivered.append(fact["event_id"]))  # type: ignore[arg-type]
        gateway._accept_incoming(
            {"type": "worker_fact", "envelope": {"worker_id": "worker-a", "recovery_epoch": "epoch", "event_id": 3}}
        )

        deadline = monotonic() + 1
        while delivered != [2, 3] and monotonic() < deadline:
            sleep(0.01)
        gateway._closing.set()
        gateway._fact_queue.put(None)
        self.assertEqual([2, 3], delivered)

    def test_market_data_unavailable_identifies_the_disconnected_worker(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages_ready = Condition()
        gateway._inbox = deque(
            [{"type": "market_data_unavailable", "worker_id": "worker-a", "reason": "worker_session_unavailable"}]
        )
        gateway._receiver_error = None

        with self.assertRaisesRegex(WorkerMarketDataUnavailable, "worker-a") as raised:
            gateway.market_data()
        self.assertEqual("worker-a", raised.exception.worker_id)

    def test_market_data_interprets_the_opaque_worker_stream_at_the_runtime_seam(self) -> None:
        gateway = TraderGateway.__new__(TraderGateway)
        gateway._messages_ready = Condition()
        gateway._inbox = deque([{
            "type": "worker_stream",
            "worker_id": "worker-a",
            "envelope": {
                "type": "live_state_snapshot",
                "observed_at": "2026-08-28T08:00:00+00:00",
                "connectivity": True,
                "quotes": [{"symbol": "EURUSD", "bid": 1.1, "ask": 1.2}],
            },
        }])
        gateway._receiver_error = None

        self.assertEqual(
            {
                "type": "market_data",
                "worker_id": "worker-a",
                "observed_at": "2026-08-28T08:00:00+00:00",
                "quotes": [{"symbol": "EURUSD", "bid": 1.1, "ask": 1.2}],
            },
            gateway.market_data(),
        )

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

    def test_write_rpc_disconnect_never_falls_back_to_direct_account_mutation(self) -> None:
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
        self.assertFalse(any(kind == "operation" and request["type"] == "close" for _, kind, request in gateway.calls))

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

    def test_shutdown_refuses_direct_account_mutation_without_the_runtime(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("audacity"), second=Endpoint("ftmo"), config=configuration())
        gateway.positions = {
            "audacity": [{"ticket": 11, "volume": 0.1}],
            "ftmo": [{"ticket": 22, "volume": 0.1}],
        }

        with self.assertRaisesRegex(StrategyError, "Durable Strategy Runtime is unavailable"):
            strategy.shutdown()

        closes = [(worker, request) for worker, kind, request in gateway.calls if kind == "operation" and request["type"] == "close"]
        self.assertEqual([], closes)

    def test_entry_log_includes_bid_ask_context(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(execute=False, max_exposure_usd=5_000),
        )
        now = datetime.now(UTC)
        strategy.quotes["first"]["EURUSD"] = (now, 1.1000, 1.1002)
        strategy.quotes["second"]["EURUSD"] = (now, 1.0998, 1.1000)
        strategy.sizing_plans = {
            ("EURUSD", "short_first_long_second"): SizingPlan(
                volume=0.1,
                first_margin_limit=400,
                second_margin_limit=400,
                computed_at=now,
            )
        }

        with self.assertLogs("strategy.realtime_arbitrage", "INFO") as captured:
            strategy._open(TradeCandidate("EURUSD", "short_first_long_second", 0.0001))

        self.assertTrue(
            any(
                "entry_signal symbol=EURUSD direction=short_first_long_second edge=0.0001 "
                "first_side=SELL bid/ask=1.1/1.1002 second_side=BUY bid/ask=1.0998/1.1 volume=0.10" in line
                for line in captured.output
            )
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

    def test_exposure_cap_never_exceeds_current_equity(self) -> None:
        strategy = RealtimeArbitrage(
            FakeGateway(),
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=10_000),
        )

        self.assertEqual({"first": 5_000, "second": 5_000}, strategy.exposure_caps)

    def test_initial_protection_targets_equal_dollar_amounts_without_a_worker_write(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        calls_before_targets = len(gateway.calls)

        sl, tp = strategy._initial_protection_targets("first", "EURUSD", "LONG", 1.2, 0.1)

        self.assertLess(float(sl), 1.2)
        self.assertGreater(float(tp), 1.2)
        self.assertAlmostEqual(40, (1.2 - float(sl)) * 10_000, places=3)
        self.assertAlmostEqual(40, (float(tp) - 1.2) * 10_000, places=3)
        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls[calls_before_targets:]))

    def test_initial_protection_targets_use_broker_tick_price_before_entry(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(gateway, first=Endpoint("worker-a"), second=Endpoint("worker-b"), config=configuration())
        targets = iter((1.3904754166, 1.3812362498))
        strategy._profit_target_price = lambda *_: next(targets)  # type: ignore[method-assign]
        calls_before_targets = len(gateway.calls)

        sl, tp = strategy._initial_protection_targets("first", "EURUSD", "SHORT", 1.38584, 0.12)

        self.assertEqual(("1.39048", "1.38124"), (sl, tp))
        self.assertFalse(any(kind == "operation" for _, kind, _ in gateway.calls[calls_before_targets:]))

    def test_emergency_stop_loss_does_not_exceed_exposure_trade_loss_limit(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=1_000),
        )

        sl, tp = strategy._initial_protection_targets("first", "EURUSD", "LONG", 1.2, 0.1)

        self.assertAlmostEqual(20, (1.2 - float(sl)) * 10_000, places=3)
        self.assertAlmostEqual(20, (float(tp) - 1.2) * 10_000, places=3)

    def test_emergency_stop_loss_does_not_exceed_remaining_daily_loss_limit(self) -> None:
        gateway = FakeGateway()
        strategy = RealtimeArbitrage(
            gateway,
            first=Endpoint("worker-a"),
            second=Endpoint("worker-b"),
            config=configuration(max_exposure_usd=5_000),
        )
        strategy.accounts["first"].equity = 4_880

        sl, tp = strategy._initial_protection_targets("first", "EURUSD", "LONG", 1.2, 0.1)

        self.assertAlmostEqual(30, (1.2 - float(sl)) * 10_000, places=3)
        self.assertAlmostEqual(30, (float(tp) - 1.2) * 10_000, places=3)

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

        self.assertFalse(any(kind == "operation" and request["type"] == "close" for _, kind, request in gateway.calls))

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
