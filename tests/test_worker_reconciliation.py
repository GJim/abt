from __future__ import annotations

from collections import namedtuple
from datetime import UTC, datetime, timedelta
import unittest

from abt.worker.enrollment import WorkerEnrollmentError
from abt.worker.reconciliation import (
    AccountMismatchError,
    MT5ReconciliationAdapter,
    WorkerSafetyAdapter,
    reconcile_authenticated_worker,
)
from abt.worker.session import collect_product_catalog_evidence


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
        self.assertEqual(2, emitted[-1]["cursor"])
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

        self.assertEqual([7, 8], [event["cursor"] for event in emitted])
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

    def test_serves_catalog_analysis_requests_during_reconciliation(self) -> None:
        mt5 = AnalysisMT5()
        session = AnalysisSession(
            {
                "analysis_id": "analysis-123",
                "request_id": "request-123",
                "stage": "catalog",
                "policy": {},
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

        self.assertEqual("analysis-123", session.analysis_response["analysis_id"])
        self.assertEqual("request-123", session.analysis_response["request_id"])
        self.assertEqual("catalog", session.analysis_response["stage"])
        self.assertEqual(["EURUSD"], [symbol["symbol"] for symbol in session.analysis_response["symbols"]])
        self.assertEqual(0, mt5.broker_write_calls)

    def test_serves_m15_analysis_requests_during_reconciliation(self) -> None:
        response = self._serve_market_data_analysis("m15_screening", "M15")

        self.assertEqual("m15_screening", response["stage"])
        self.assertEqual("M15", response["timeframe"])

    def test_serves_m1_analysis_requests_during_reconciliation(self) -> None:
        response = self._serve_market_data_analysis("m1_verification", "M1")

        self.assertEqual("m1_verification", response["stage"])
        self.assertEqual("M1", response["timeframe"])

    def test_collects_catalog_evidence_from_mt5_namedtuple_symbol_info(self) -> None:
        mt5 = AnalysisMT5()
        specification = mt5.symbols_get()[0]
        symbol_info = namedtuple("SymbolInfo", specification)(**specification)
        mt5.symbols_get = lambda: [symbol_info]  # type: ignore[method-assign]

        evidence = collect_product_catalog_evidence(mt5, collected_at=datetime(2026, 8, 16, tzinfo=UTC))

        self.assertEqual("EURUSD", evidence["symbols"][0]["symbol"])

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


class SafetySession:
    def __init__(self, heartbeats: list[bool]) -> None:
        self._heartbeats = iter(heartbeats)
        self.states: list[str] = []

    def heartbeat(self) -> bool:
        return next(self._heartbeats)

    def send_safety_state(self, state: str, reason: str) -> None:
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

    def receive_product_catalog_analysis(self, timeout: float | None = None) -> dict[str, object] | None:
        return self._requests.pop(0)

    def send_product_catalog_analysis(self, **response: object) -> None:
        self.analysis_response = response

    def heartbeat(self) -> bool:
        raise StopIteration

    def send_safety_state(self, state: str, reason: str) -> None:
        pass
