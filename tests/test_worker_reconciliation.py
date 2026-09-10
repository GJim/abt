from __future__ import annotations

from collections import namedtuple
from datetime import UTC, datetime, timedelta
from pathlib import Path
import tempfile
import unittest

from abt.worker.enrollment import WorkerEnrollmentError, WorkerSessionDisconnected
from abt.worker.effect_journal import WorkerEffectJournal
from abt.worker.reconciliation import (
    AccountMismatchError,
    LiveWorkerMarketStateAdapter,
    MT5ReconciliationAdapter,
    WorkerSafetyAdapter,
    _GracefulShutdown,
    _mt5_last_error,
    _broker_order_request,
    _broker_receipt,
    _json_evidence,
    reconnect_worker_session,
    reconcile_authenticated_worker,
)
from abt.trader_protocol import MAX_LIVE_SYMBOLS


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

    def symbols_get(self) -> object:
        self.calls.append("symbols_get")
        return [{"name": "EURUSD", "trade_mode": 4, "point": 0.00001}]

    def order_send(self, *_: object, **__: object) -> object:
        self.broker_write_calls += 1
        raise AssertionError("Broker writes are prohibited.")


class WorkerReconciliationTests(unittest.TestCase):


    def test_receipt_adapter_ignores_unknown_nested_mt5_request(self) -> None:
        OrderSendResult = namedtuple(
            "OrderSendResult",
            ("retcode", "deal", "order", "volume", "price", "comment", "request"),
        )

        receipt = _broker_receipt(
            OrderSendResult(10009, 123456, 123456, 0.01, 1.16587, "Done", object()),
            "order execution",
        )

        self.assertEqual(
            {
                "retcode": 10009,
                "deal": 123456,
                "order": 123456,
                "volume": 0.01,
                "price": 1.16587,
                "comment": "Done",
            },
            receipt,
        )

    def test_serializes_nested_mt5_order_send_receipt(self) -> None:
        TradeRequest = namedtuple("TradeRequest", ("action", "symbol", "volume"))
        OrderSendResult = namedtuple("OrderSendResult", ("retcode", "deal", "price", "request"))

        evidence = _json_evidence(
            OrderSendResult(10009, 123456, 1.16587, TradeRequest(1, "EURUSD", 0.01)),
            "order execution",
        )

        self.assertEqual(
            {
                "retcode": 10009,
                "deal": 123456,
                "price": 1.16587,
                "request": {"action": 1, "symbol": "EURUSD", "volume": 0.01},
            },
            evidence,
        )








    def test_reconciliation_rejects_unavailable_exposure_records(self) -> None:
        for field in ("orders", "positions"):
            with self.subTest(field=field):
                mt5 = ReadOnlyMT5()
                setattr(mt5, field, None)
                adapter = MT5ReconciliationAdapter(mt5, emit=lambda _: None)

                with self.assertRaisesRegex(WorkerEnrollmentError, f"no {field[:-1]} records"):
                    adapter.poll(datetime(2026, 8, 16, tzinfo=UTC))


    def test_mt5_last_error_diagnostic_is_safe_when_unavailable(self) -> None:
        self.assertIsNone(_mt5_last_error(object()))

        class BrokenMT5:
            def last_error(self) -> object:
                raise RuntimeError("unavailable")

        self.assertEqual("unavailable", _mt5_last_error(BrokenMT5()))

    def test_market_order_uses_executable_tick_price(self) -> None:
        class TradingMT5:
            TRADE_ACTION_DEAL = 1
            ORDER_TYPE_BUY = 0
            ORDER_TYPE_SELL = 1
            ORDER_FILLING_FOK = 0
            ORDER_FILLING_IOC = 1

            def symbol_info_tick(self, _symbol: str) -> object:
                return {"bid": 1.1, "ask": 1.1002}

        buy = _broker_order_request(TradingMT5(), {
            "action": "market", "symbol": "EURUSD", "volume": "0.1", "direction": "LONG",
            "filling_mode": "FOK",
        })
        sell = _broker_order_request(TradingMT5(), {
            "action": "market", "symbol": "EURUSD", "volume": "0.1", "direction": "SHORT",
            "filling_mode": "IOC",
        })

        self.assertEqual(1.1002, buy["price"])
        self.assertEqual(1.1, sell["price"])
        self.assertNotIn("comment", buy)
        self.assertNotIn("magic", buy)

    def test_controller_market_order_includes_validated_initial_protection(self) -> None:
        class TradingMT5:
            TRADE_ACTION_DEAL = 1
            ORDER_TYPE_BUY = 0
            ORDER_FILLING_FOK = 0

            def symbol_info_tick(self, _symbol: str) -> object:
                return {"bid": 1.1, "ask": 1.1002}

        protected = _broker_order_request(
            TradingMT5(),
            {
                "action": "market",
                "symbol": "EURUSD",
                "volume": "0.1",
                "direction": "LONG",
                "filling_mode": "FOK",
                "sl": "1.0900",
                "tp": "1.1100",
            },
        )

        self.assertEqual((1.09, 1.11), (protected["sl"], protected["tp"]))
        for invalid in (
            {"sl": "1.0900"},
            {"sl": "not-a-price", "tp": "1.1100"},
            {"sl": "nan", "tp": "1.1100"},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(WorkerEnrollmentError, "invalid market order"):
                    _broker_order_request(
                        TradingMT5(),
                        {
                            "action": "market",
                            "symbol": "EURUSD",
                            "volume": "0.1",
                            "direction": "LONG",
                            "filling_mode": "FOK",
                            **invalid,
                        },
                    )





    def test_publishes_diff_only_live_market_state_on_the_required_schedules(self) -> None:
        class LiveMT5(ReadOnlyMT5):
            def symbol_info_tick(self, symbol: str) -> object:
                self.calls.append(f"symbol_info_tick:{symbol}")
                return {"bid": 1.0821, "ask": 1.0823, "last": 1.0822, "time": 1_787_068_740}

        mt5 = LiveMT5()
        emitted: list[dict[str, object]] = []
        adapter = LiveWorkerMarketStateAdapter(mt5, watched_symbols=["EURUSD"], emit=emitted.append)
        started = datetime(2026, 8, 16, tzinfo=UTC)

        adapter.poll(started)
        adapter.poll(started + timedelta(milliseconds=500))
        adapter.poll(started + timedelta(seconds=1))
        adapter.poll(started + timedelta(seconds=5))

        self.assertEqual("live_state_snapshot", emitted[0]["type"])
        self.assertEqual(
            {
                "symbol": "EURUSD",
                "bid": 1.0821,
                "ask": 1.0823,
                "last": 1.0822,
                "broker_time": "2026-08-18T15:59:00+00:00",
            },
            emitted[0]["quotes"][0],
        )
        self.assertEqual([], emitted[1:])
        self.assertEqual(4, mt5.calls.count("symbol_info_tick:EURUSD"))
        self.assertEqual(0, mt5.calls.count("orders_get"))
        self.assertEqual(0, mt5.calls.count("positions_get"))
        self.assertEqual(2, mt5.calls.count("terminal_info"))


    def test_reconnects_disconnected_wss_sessions_with_exponential_backoff(self) -> None:
        opened = 0
        waits: list[float] = []
        runs = 0

        def open_session() -> ReconnectSession:
            nonlocal opened
            opened += 1
            return ReconnectSession()

        def run_reconciliation(**_: object) -> None:
            nonlocal runs
            runs += 1
            if runs < 3:
                raise WorkerSessionDisconnected("controller disconnected")

        reconnect_worker_session(
            open_session=open_session,
            mt5=ReadOnlyMT5(),
            login=123456,
            server="Broker-Demo",
            sleep=waits.append,
            run_reconciliation=run_reconciliation,
        )

        self.assertEqual(3, opened)
        self.assertEqual([5.0, 10.0], waits)

    def test_resets_reconnect_backoff_after_five_minutes_of_connected_reconciliation(self) -> None:
        elapsed_seconds = 0.0
        waits: list[float] = []
        runs = 0

        def run_reconciliation(**_: object) -> None:
            nonlocal elapsed_seconds, runs
            runs += 1
            if runs == 2:
                elapsed_seconds += 300
            if runs < 4:
                raise WorkerSessionDisconnected("controller disconnected")

        reconnect_worker_session(
            open_session=ReconnectSession,
            mt5=ReadOnlyMT5(),
            login=123456,
            server="Broker-Demo",
            sleep=waits.append,
            run_reconciliation=run_reconciliation,
            monotonic_clock=lambda: elapsed_seconds,
        )

        self.assertEqual([5.0, 5.0, 10.0], waits)

    def test_logs_each_session_reconnect_attempt(self) -> None:
        runs = 0

        def run_reconciliation(**_: object) -> None:
            nonlocal runs
            runs += 1
            if runs == 1:
                raise WorkerSessionDisconnected("controller disconnected")

        with self.assertLogs("abt.worker.reconciliation", level="WARNING") as logs:
            reconnect_worker_session(
                open_session=ReconnectSession,
                mt5=ReadOnlyMT5(),
                login=123456,
                server="Broker-Demo",
                sleep=lambda _: None,
                run_reconciliation=run_reconciliation,
            )

        self.assertEqual(
            ["WARNING:abt.worker.reconciliation:Worker session disconnected (controller disconnected); "
             "reconnecting attempt 1/10 in 5 seconds."],
            logs.output,
        )

    def test_reconnect_preserves_a_graceful_shutdown_request(self) -> None:
        seen: list[_GracefulShutdown] = []
        runs = 0

        def run_reconciliation(*, graceful_shutdown: _GracefulShutdown, **_: object) -> None:
            nonlocal runs
            runs += 1
            seen.append(graceful_shutdown)
            if runs == 1:
                graceful_shutdown.requested = True
                raise WorkerSessionDisconnected("controller disconnected")
            self.assertTrue(graceful_shutdown.requested)

        reconnect_worker_session(
            open_session=ReconnectSession,
            mt5=ReadOnlyMT5(),
            login=123456,
            server="Broker-Demo",
            sleep=lambda _: None,
            run_reconciliation=run_reconciliation,
        )

        self.assertEqual(2, runs)
        self.assertIs(seen[0], seen[1])

    def test_stops_reconnecting_after_ten_wss_retry_attempts(self) -> None:
        opened = 0
        waits: list[float] = []

        def open_session() -> ReconnectSession:
            nonlocal opened
            opened += 1
            return ReconnectSession()

        with self.assertRaisesRegex(WorkerSessionDisconnected, "controller disconnected"):
            reconnect_worker_session(
                open_session=open_session,
                mt5=ReadOnlyMT5(),
                login=123456,
                server="Broker-Demo",
                sleep=waits.append,
                run_reconciliation=lambda **_: (_ for _ in ()).throw(WorkerSessionDisconnected("controller disconnected")),
            )

        self.assertEqual(11, opened)
        self.assertEqual([float(5 * 2 ** attempt) for attempt in range(10)], waits)

    def test_enters_read_only_lost_link_safety_after_five_missed_minutes_and_recovers(self) -> None:
        session = SafetySession([False, False, False, True])
        adapter = WorkerSafetyAdapter(session)
        started = datetime(2026, 8, 16, tzinfo=UTC)

        self.assertFalse(adapter.heartbeat(started))
        self.assertFalse(adapter.heartbeat(started + timedelta(seconds=30)))
        self.assertEqual("connected", adapter.state)
        self.assertFalse(adapter.heartbeat(started + timedelta(minutes=5)))
        self.assertEqual("lost_link_safety", adapter.state)
        self.assertTrue(adapter.heartbeat(started + timedelta(minutes=5, seconds=30)))

        self.assertEqual("connected", adapter.state)
        self.assertEqual(["lost_link_safety", "connected"], session.states)

    def test_stops_terminal_retries_after_three_failures_and_escalates_account_mismatch_immediately(self) -> None:
        session = SafetySession([])
        adapter = WorkerSafetyAdapter(session)
        attempts = 0
        waits: list[float] = []

        def terminal_failure() -> None:
            nonlocal attempts
            attempts += 1
            raise WorkerEnrollmentError("terminal unavailable")

        with self.assertRaises(WorkerEnrollmentError):
            adapter.run_with_retries(terminal_failure, sleep=waits.append)
        self.assertEqual((3, [1.0, 2.0], ["needs_human"]), (attempts, waits, session.states))

        mismatch_attempts = 0
        with self.assertRaises(AccountMismatchError):
            def account_mismatch() -> None:
                nonlocal mismatch_attempts
                mismatch_attempts += 1
                raise AccountMismatchError("unexpected account")
            adapter.run_with_retries(account_mismatch, sleep=waits.append)
        self.assertEqual(1, mismatch_attempts)

    def test_emits_ten_minute_snapshots_and_relevant_deltas(self) -> None:
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
        self.assertEqual(4, emitted[-1]["cursor"])
        self.assertEqual(["account_info", "terminal_info", "orders_get", "positions_get"] * 4, mt5.calls)
        self.assertEqual(0, mt5.broker_write_calls)

    def test_emits_modified_deltas_but_ignores_price_and_floating_profit(self) -> None:
        mt5 = ReadOnlyMT5()
        emitted: list[dict[str, object]] = []
        adapter = MT5ReconciliationAdapter(mt5, emit=emitted.append)
        started = datetime(2026, 8, 16, tzinfo=UTC)

        adapter.poll(started)
        mt5.orders[0]["price_current"] = 2.5
        mt5.positions[0]["profit"] = 4.0
        adapter.poll(started + timedelta(minutes=1))
        mt5.orders[0]["type"] = "sell_limit"
        mt5.positions[0]["tp"] = 1.5
        adapter.poll(started + timedelta(minutes=2))

        self.assertEqual(["snapshot", "delta", "delta"], [event["type"] for event in emitted])
        self.assertEqual(["modified", "modified"], [event["change"] for event in emitted[1:]])
        self.assertEqual(0, mt5.broker_write_calls)

    def test_resumes_delta_cursor_with_a_full_baseline_snapshot(self) -> None:
        mt5 = ReadOnlyMT5()
        emitted: list[dict[str, object]] = []
        adapter = MT5ReconciliationAdapter(mt5, emit=emitted.append, initial_cursor=7)
        started = datetime(2026, 8, 16, tzinfo=UTC)

        adapter.poll(started)
        mt5.positions[0]["volume"] = 1.0
        adapter.poll(started + timedelta(minutes=1))

        self.assertEqual([8, 9], [event["cursor"] for event in emitted])
        self.assertEqual(["snapshot", "delta"], [event["type"] for event in emitted])

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

    def test_runs_certificate_maintenance_daily_including_the_final_day(self) -> None:
        mt5 = ReadOnlyMT5()
        adapter = MT5ReconciliationAdapter(mt5, emit=lambda _: None)
        started = datetime(2026, 8, 16, tzinfo=UTC)
        moments = iter([started, started, started + timedelta(hours=23, minutes=59), started + timedelta(days=1)])
        maintenance: list[datetime] = []
        waits = 0

        def wait(_: float) -> None:
            nonlocal waits
            waits += 1
            if waits == 3:
                raise StopIteration

        with self.assertRaises(StopIteration):
            adapter.run_forever(
                now=lambda: next(moments),
                sleep=wait,
                maintenance=lambda: maintenance.append(datetime.now(UTC)),
            )

        self.assertEqual(2, len(maintenance))

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

        self.assertEqual(
            ["account_info", "account_info", "terminal_info", "orders_get", "positions_get", "shutdown"], mt5.calls
        )
        self.assertEqual("snapshot", emitted[0]["type"])
        self.assertEqual(0, mt5.broker_write_calls)

    def test_pair_cell_factory_is_constructed_pumped_and_closed(self) -> None:
        """Wiring test for gap #1/#2: a supplied ``pair_cell_factory`` must be
        constructed once MT5 login succeeds, pumped every loop iteration, and
        closed on shutdown -- this must fail if that wiring is removed."""

        mt5 = ReadOnlyMT5()
        mt5.initialize = lambda: True  # type: ignore[attr-defined]
        mt5.login = lambda login, *, password, server: True  # type: ignore[attr-defined]
        mt5.shutdown = lambda: None  # type: ignore[attr-defined]

        class OneShotSession(MemoryOnlySession):
            def __init__(self, emitted: list[dict[str, object]]) -> None:
                super().__init__(emitted)
                self._relay_calls = 0

            def receive_worker_relay(self, timeout: float | None = None) -> bool:
                self._relay_calls += 1
                if self._relay_calls > 2:
                    raise StopIteration
                return False

        class FakePairCell:
            def __init__(self) -> None:
                self.drain_calls = 0
                self.pump_calls: list[object] = []
                self.closed = False

            def drain_relay(self) -> object:
                self.drain_calls += 1
                return None

            def pump(self, observed_at: object) -> object:
                self.pump_calls.append(observed_at)
                return None

            def close(self) -> None:
                self.closed = True

        fake_pair_cell = FakePairCell()
        factory_calls: list[tuple[object, object, object, int, str]] = []

        def factory(mt5_client: object, session: object, journal: object, login: int, server: str) -> FakePairCell:
            factory_calls.append((mt5_client, session, journal, login, server))
            return fake_pair_cell

        with self.assertRaises(StopIteration):
            reconcile_authenticated_worker(
                mt5=mt5,
                session=OneShotSession([]),
                login=123456,
                server="Broker-Demo",
                now=lambda: datetime(2026, 8, 16, tzinfo=UTC),
                pair_cell_factory=factory,
            )

        self.assertEqual(1, len(factory_calls))
        self.assertEqual((123456, "Broker-Demo"), factory_calls[0][3:])
        self.assertGreater(fake_pair_cell.drain_calls, 0)
        self.assertGreater(len(fake_pair_cell.pump_calls), 0)
        self.assertTrue(fake_pair_cell.closed)

    def test_pair_cell_can_end_reconciliation_cleanly_after_safe_unpair(self) -> None:
        mt5 = ReadOnlyMT5()
        mt5.initialize = lambda: True  # type: ignore[attr-defined]
        mt5.login = lambda login, *, password, server: True  # type: ignore[attr-defined]
        mt5.shutdown = lambda: None  # type: ignore[attr-defined]

        class Session(MemoryOnlySession):
            def receive_worker_relay(self, timeout: float | None = None) -> bool:
                return False

        class CompletedUnpairCell:
            def __init__(self) -> None:
                self.closed = False

            def drain_relay(self) -> object:
                return None

            def pump(self, observed_at: object) -> object:
                return None

            @property
            def should_exit(self) -> bool:
                return True

            def close(self) -> None:
                self.closed = True

        cell = CompletedUnpairCell()
        reconcile_authenticated_worker(
            mt5=mt5,
            session=Session([]),
            login=123456,
            server="Broker-Demo",
            now=lambda: datetime(2026, 8, 16, tzinfo=UTC),
            pair_cell_factory=lambda *_: cell,
        )

        self.assertTrue(cell.closed)

    def test_first_ctrl_c_closes_pair_cell_before_reconciliation_exits(self) -> None:
        mt5 = ReadOnlyMT5()
        mt5.initialize = lambda: True  # type: ignore[attr-defined]
        mt5.login = lambda login, *, password, server: True  # type: ignore[attr-defined]
        mt5.shutdown = lambda: None  # type: ignore[attr-defined]

        class Session(MemoryOnlySession):
            def __init__(self) -> None:
                super().__init__([])
                self.calls = 0

            def receive_worker_relay(self, timeout: float | None = None) -> bool:
                self.calls += 1
                if self.calls == 1:
                    raise KeyboardInterrupt
                return False

        class ClosingPairCell:
            def __init__(self) -> None:
                self.close_reasons: list[str] = []
                self.pump_calls = 0
                self.closed = False

            def drain_relay(self) -> object:
                return None

            def request_close(self, reason: str) -> object:
                self.close_reasons.append(reason)
                return None

            def pump(self, observed_at: object) -> object:
                self.pump_calls += 1
                return None

            @property
            def shutdown_complete(self) -> bool:
                return self.pump_calls > 0

            @property
            def should_exit(self) -> bool:
                return False

            def close(self) -> None:
                self.closed = True

        cell = ClosingPairCell()
        reconcile_authenticated_worker(
            mt5=mt5,
            session=Session(),
            login=123456,
            server="Broker-Demo",
            now=lambda: datetime(2026, 8, 16, tzinfo=UTC),
            pair_cell_factory=lambda *_: cell,
        )

        self.assertEqual(["operator interrupt"], cell.close_reasons)
        self.assertGreater(cell.pump_calls, 0)
        self.assertTrue(cell.closed)

    def _serve_market_data_analysis(self, stage: str, timeframe: str) -> dict[str, object]:
        mt5 = AnalysisMT5()
        session = AnalysisSession(
            {
                "analysis_id": "analysis-123",
                "request_id": "request-123",
                "stage": stage,
                "policy": {},
                "timeframe": timeframe,
                "period_start_utc": "2026-08-10T00:00:00Z",
                "period_end_utc": "2026-08-17T00:00:00Z",
                "symbols": ["EURUSD"],
            }
        )

        with self.assertRaises(StopIteration):
            reconcile_authenticated_worker(
                mt5=mt5,
                session=session,
                login=123456,
                server="Broker-Demo",
                now=lambda: datetime(2026, 8, 16, tzinfo=UTC),
                sleep=lambda _: None,
            )

        self.assertEqual("EURUSD", session.analysis_response["symbols"][0]["symbol"])
        self.assertEqual(["EURUSD"], mt5.market_data_symbols)
        self.assertEqual(0, mt5.broker_write_calls)
        return session.analysis_response


class MemoryOnlySession:
    def __init__(self, emitted: list[dict[str, object]]) -> None:
        self._emitted = emitted
        self.reconciliation_cursor = 0

    def request_password(self) -> str:
        return "memory-only"

    def send_reconciliation(self, message: dict[str, object]) -> None:
        self._emitted.append(message)

    def receive_worker_relay(self, timeout: float | None = None) -> bool:
        raise StopIteration


class ReconnectSession:
    def __enter__(self) -> ReconnectSession:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        pass


class SafetySession:
    def __init__(self, heartbeats: list[bool]) -> None:
        self._heartbeats = iter(heartbeats)
        self.states: list[str] = []

    def heartbeat(self) -> bool:
        return next(self._heartbeats)

    def send_recovery_state(self, state: str, reason: str) -> None:
        self.states.append(state)


class AnalysisMT5(ReadOnlyMT5):
    def initialize(self) -> bool:
        return True

    def login(self, login: int, *, password: str, server: str) -> bool:
        return login == 123456 and password == "memory-only" and server == "Broker-Demo"

    def shutdown(self) -> None:
        self.calls.append("shutdown")

    def __init__(self) -> None:
        super().__init__()
        self.market_data_symbols: list[str] = []

    def symbols_get(self) -> object:
        return [
            {
                "name": "EURUSD",
                "trade_calc_mode": 0,
                "currency_base": "EUR",
                "currency_profit": "USD",
                "digits": 5,
                "point": 0.00001,
                "trade_tick_size": 0.00001,
                "trade_contract_size": 100000.0,
                "volume_min": 0.01,
                "volume_step": 0.01,
                "filling_mode": 0,
                "order_mode": 3,
                "volume_max": 100.0,
                "trade_stops_level": 0,
                "trade_freeze_level": 0,
                "trade_tick_value": 1.0,
                "currency_margin": "EUR",
                "swap_long": 0.0,
                "swap_short": 0.0,
                "swap_mode": 1,
                "swap_rollover3days": 3,
            }
        ]

    def copy_rates_range(self, symbol: str, timeframe: object, start: datetime, end: datetime) -> object:
        self.market_data_symbols.append(symbol)
        return [{"time": 1_785_945_600, "open": 1.1, "high": 1.2, "low": 1.0, "close": 1.15}]

    def symbol_info_tick(self, symbol: str) -> object:
        return {"time": 1_785_945_600}


class AnalysisSession(MemoryOnlySession):
    def __init__(self, request: dict[str, object]) -> None:
        super().__init__([])
        self._requests = [request, None]
        self.analysis_response: dict[str, object] = {}
        self.analysis_error: dict[str, object] = {}

    def receive_product_catalog_analysis(self, timeout: float | None = None) -> dict[str, object] | None:
        return self._requests.pop(0)

    def send_product_catalog_analysis(self, **response: object) -> None:
        self.analysis_response = response

    def send_product_catalog_analysis_error(self, **response: object) -> None:
        self.analysis_error = response

    def heartbeat(self) -> bool:
        raise StopIteration

    def send_recovery_state(self, state: str, reason: str) -> None:
        pass
