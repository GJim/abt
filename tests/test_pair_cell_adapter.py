"""Unit tests for the Pair Execution Cell <-> Worker session production wiring
(``abt.worker.pair_cell_adapter``): genuine MT5 request translation, the
relay envelope wire format, quote/readiness evidence reading, shared
-scheduler foreign-item draining, and restart construction/recovery.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from abt.pair_cell import (
    LocalQuoteEvent,
    PairContract,
    PairExecutionCellError,
    PairLease,
    ProtectionCalibration,
    ReadinessFactsEvent,
)
from abt.worker.effect_journal import WorkerEffectJournal
from abt.worker.pair_cell_adapter import (
    PairCellRuntime,
    WorkerPairCellMT5,
    WorkerPairCellRelay,
    _pair_cell_broker_request,
    read_local_quote,
    read_readiness_facts,
    refresh_protection_calibration,
    unwrap_pair_relay_envelope,
)
from abt.worker.reconciliation import WorkerEnrollmentError
from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler


class FakeMT5:
    """A deterministic MT5-shaped fake exercising the exact request/constant
    conventions ``abt.worker.reconciliation`` already relies on."""

    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_SLTP = 2
    TRADE_ACTION_REMOVE = 3
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_PLACED = 10008

    def __init__(self) -> None:
        self.account = {"login": 555111, "server": "Broker-Demo", "margin_free": 10_000.0}
        self.terminal = {"connected": True, "trade_allowed": True, "tradeapi_disabled": False}
        self.orders: list[dict[str, object]] = []
        self.positions: list[dict[str, object]] = [
            {"ticket": 900, "symbol": "EURUSD", "type": self.POSITION_TYPE_BUY, "volume": 0.10}
        ]
        self.symbol = {
            "trade_tick_size": 0.00001,
            "point": 0.00001,
            "trade_stops_level": 10,
            "trade_freeze_level": 0,
            "trade_tick_value": 1.0,
        }
        self.tick = {"bid": 1.1000, "ask": 1.1002, "last": 1.1001, "time": 1_700_000_000, "time_msc": 1_700_000_000_123}
        self.sent_requests: list[dict[str, object]] = []
        self.reject = False

    def account_info(self) -> object:
        return dict(self.account)

    def terminal_info(self) -> object:
        return dict(self.terminal)

    def orders_get(self) -> object:
        return [dict(o) for o in self.orders]

    def positions_get(self) -> object:
        return [dict(p) for p in self.positions]

    def symbol_info(self, symbol: str) -> object:
        return dict(self.symbol)

    def symbol_info_tick(self, symbol: str) -> object:
        return dict(self.tick)

    def order_send(self, request: dict[str, object]) -> object:
        self.sent_requests.append(request)
        if self.reject:
            return {"retcode": 10013, "comment": "rejected"}
        return {"retcode": self.TRADE_RETCODE_DONE, "position": 901}


class BrokerRequestTranslationTests(unittest.TestCase):
    """Fixes "actual MT5 request compatibility": abt.pair_cell builds the same
    abstract trader-operation payload the legacy Trader-relay path uses, and
    this adapter must turn it into a genuine MT5 request before order_send."""

    def test_market_entry_becomes_a_genuine_trade_action_deal_request(self) -> None:
        mt5 = FakeMT5()

        request = _pair_cell_broker_request(
            mt5,
            {
                "type": "market",
                "symbol": "EURUSD",
                "volume": "0.10",
                "direction": "LONG",
                "filling_mode": "FOK",
                "sl": "1.0950",
                "tp": "1.1050",
            },
        )

        self.assertEqual(mt5.TRADE_ACTION_DEAL, request["action"])
        self.assertEqual(mt5.ORDER_TYPE_BUY, request["type"])
        self.assertEqual(mt5.ORDER_FILLING_FOK, request["type_filling"])
        self.assertEqual(1.1002, request["price"])  # the ask, for a LONG entry
        self.assertEqual(1.0950, request["sl"])
        self.assertEqual(1.1050, request["tp"])

    def test_short_entry_prices_from_the_bid(self) -> None:
        mt5 = FakeMT5()

        request = _pair_cell_broker_request(
            mt5,
            {
                "type": "market", "symbol": "EURUSD", "volume": "0.10", "direction": "SHORT",
                "filling_mode": "IOC", "sl": "1.1050", "tp": "1.0950",
            },
        )

        self.assertEqual(mt5.ORDER_TYPE_SELL, request["type"])
        self.assertEqual(1.1000, request["price"])

    def test_modify_sl_tp_becomes_a_genuine_trade_action_sltp_request(self) -> None:
        mt5 = FakeMT5()

        request = _pair_cell_broker_request(
            mt5, {"type": "modify_sl_tp", "symbol": "EURUSD", "position": "900", "sl": "1.0960", "tp": "1.1040"}
        )

        self.assertEqual(
            {"action": mt5.TRADE_ACTION_SLTP, "symbol": "EURUSD", "position": 900, "sl": 1.0960, "tp": 1.1040},
            request,
        )

    def test_cancel_becomes_a_genuine_trade_action_remove_request(self) -> None:
        mt5 = FakeMT5()

        request = _pair_cell_broker_request(mt5, {"type": "cancel", "ticket": "42"})

        self.assertEqual({"action": mt5.TRADE_ACTION_REMOVE, "order": 42}, request)

    def test_close_looks_up_the_owned_position_and_prices_it_correctly(self) -> None:
        mt5 = FakeMT5()

        request = _pair_cell_broker_request(mt5, {"type": "close", "ticket": "900", "volume": "0.10"})

        self.assertEqual(mt5.TRADE_ACTION_DEAL, request["action"])
        self.assertEqual(900, request["position"])
        self.assertEqual(mt5.ORDER_TYPE_SELL, request["type"])  # closing a BUY position sells
        self.assertEqual(1.1000, request["price"])

    def test_invalid_operation_is_rejected_before_any_broker_call(self) -> None:
        mt5 = FakeMT5()

        with self.assertRaises(WorkerEnrollmentError):
            _pair_cell_broker_request(mt5, {"type": "not-a-real-operation"})

    def test_order_send_refuses_when_algorithmic_trading_is_disabled(self) -> None:
        mt5 = FakeMT5()
        mt5.terminal["trade_allowed"] = False
        adapter = WorkerPairCellMT5(mt5)

        with self.assertRaises(WorkerEnrollmentError):
            adapter.order_send(
                {"type": "market", "symbol": "EURUSD", "volume": "0.10", "direction": "LONG", "filling_mode": "FOK",
                 "sl": "1.0950", "tp": "1.1050"}
            )
        self.assertEqual([], mt5.sent_requests)

    def test_order_send_translates_then_calls_the_real_mt5_handle(self) -> None:
        mt5 = FakeMT5()
        adapter = WorkerPairCellMT5(mt5)

        receipt = adapter.order_send(
            {"type": "market", "symbol": "EURUSD", "volume": "0.10", "direction": "LONG", "filling_mode": "FOK",
             "sl": "1.0950", "tp": "1.1050"}
        )

        self.assertEqual({"retcode": mt5.TRADE_RETCODE_DONE, "position": 901}, receipt)
        self.assertEqual(mt5.TRADE_ACTION_DEAL, mt5.sent_requests[0]["action"])


class QuoteTimestampCalibrationTests(unittest.TestCase):
    """Fixes "quote timestamp calibration": the pair cell's sub-second
    freshness/skew budgets need millisecond broker time, not the whole
    -second resolution the legacy live-state quote feed uses."""

    def test_prefers_millisecond_broker_tick_time_over_whole_seconds(self) -> None:
        mt5 = FakeMT5()

        quote = read_local_quote(
            mt5, symbol="EURUSD", recovery_epoch="epoch-1", sequence=1, now=lambda: datetime(2023, 11, 14, tzinfo=UTC)
        )

        expected = datetime.fromtimestamp(1_700_000_000_123 / 1000.0, UTC)
        self.assertEqual(expected, quote.broker_time)
        self.assertNotEqual(datetime.fromtimestamp(1_700_000_000, UTC), quote.broker_time)

    def test_falls_back_to_whole_second_time_when_milliseconds_are_unavailable(self) -> None:
        mt5 = FakeMT5()
        del mt5.tick["time_msc"]

        quote = read_local_quote(
            mt5, symbol="EURUSD", recovery_epoch="epoch-1", sequence=1, now=lambda: datetime.now(UTC)
        )

        self.assertEqual(datetime.fromtimestamp(1_700_000_000, UTC), quote.broker_time)

    def test_invalid_tick_evidence_is_a_read_failure_not_a_stale_quote(self) -> None:
        mt5 = FakeMT5()
        mt5.tick["bid"] = None

        with self.assertRaises(WorkerEnrollmentError):
            read_local_quote(mt5, symbol="EURUSD", recovery_epoch="epoch-1", sequence=1, now=lambda: datetime.now(UTC))


class ProtectionCalibrationTests(unittest.TestCase):
    def test_calibration_is_built_from_symbol_constraints(self) -> None:
        mt5 = FakeMT5()

        calibration = refresh_protection_calibration(mt5, "EURUSD")

        assert calibration is not None
        self.assertEqual("0.00001", calibration.tick_size)
        self.assertEqual("1.0", calibration.loss_tick_value)

    def test_missing_symbol_constraints_yield_no_calibration(self) -> None:
        mt5 = FakeMT5()
        del mt5.symbol["trade_tick_size"]

        self.assertIsNone(refresh_protection_calibration(mt5, "EURUSD"))


class ReadinessFactsTests(unittest.TestCase):
    def test_ready_facts_reflect_empty_exposure_and_margin_headroom(self) -> None:
        mt5 = FakeMT5()
        mt5.positions = []

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo",
            calibration=ProtectionCalibration("0.00001", "0.00001", "0.0001", "1", "1"),
            calibration_fresh=True, now=datetime.now(UTC),
        )

        self.assertTrue(facts.terminal_connected)
        self.assertEqual([], facts.orders)
        self.assertEqual([], facts.positions)
        self.assertTrue(facts.margin_headroom_ok)

    def test_unavailable_broker_reads_are_never_treated_as_empty(self) -> None:
        class NoneReadsMT5(FakeMT5):
            def orders_get(self) -> object:
                return None

        mt5 = NoneReadsMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo", calibration=None, calibration_fresh=False, now=datetime.now(UTC)
        )

        self.assertIsNone(facts.orders)

    def test_account_mismatch_is_not_ready(self) -> None:
        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=999999, server="Broker-Demo", calibration=None, calibration_fresh=False, now=datetime.now(UTC)
        )

        self.assertFalse(facts.margin_headroom_ok)

    def test_insufficient_margin_headroom_is_not_ready(self) -> None:
        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo", calibration=None, calibration_fresh=False, now=datetime.now(UTC),
            minimum_margin_headroom_usd=Decimal("50000"),
        )

        self.assertFalse(facts.margin_headroom_ok)


class RelayEnvelopeWireFormatTests(unittest.TestCase):
    """The cell's small internal envelope <-> the full, controller-validated
    ``PairCellRelayEnvelope`` wire format used for dedup, audit, and route
    -authorization by the controller."""

    def _contract(self) -> PairContract:
        return PairContract(
            contract_id="contract-1", leader_worker_id="worker-a", follower_worker_id="worker-b",
            trader_id="trader-1", symbol="EURUSD", leader_direction="LONG", follower_direction="SHORT",
            leader_volume="0.10", follower_volume="0.10", filling_mode="FOK", edge_threshold="0.0002",
            risk_budget_usd="50", quote_max_age_seconds=1.0, quote_max_skew_seconds=1.0,
            arm_timeout_seconds=1.0, commit_lead_seconds=0.2, execution_expiry_seconds=1.0,
        )

    def _lease(self, contract: PairContract) -> PairLease:
        now = datetime.now(UTC)
        return PairLease(
            leader_worker_id=contract.leader_worker_id, follower_worker_id=contract.follower_worker_id,
            trader_id=contract.trader_id, contract_hash=contract.hash, epoch=1,
            issued_at=now, expires_at=now + timedelta(minutes=5),
        )

    def test_send_builds_a_structurally_valid_wire_envelope(self) -> None:
        sent: list[dict[str, object]] = []

        class Session:
            def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None:
                self.envelope = envelope
                sent.append(envelope)

        relay = WorkerPairCellRelay(Session())  # type: ignore[arg-type]
        contract = self._contract()
        lease = self._lease(contract)
        relay.bind(contract, lease)

        relay.send(
            {
                "kind": "leg_status",
                "from_worker_id": "worker-a",
                "to_worker_id": "worker-b",
                "lease_epoch": lease.epoch,
                "payload": {"attempt_id": "attempt-1", "status": "filled"},
            }
        )

        envelope = sent[0]
        self.assertEqual("pair_cell_relay", envelope["type"])
        self.assertEqual(1, envelope["protocol_version"])
        self.assertEqual("attempt-1", envelope["attempt_id"])
        self.assertTrue(envelope["request_id"])
        payload_json = json.dumps(envelope["payload"], separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        from hashlib import sha256

        self.assertEqual(sha256(payload_json.encode()).hexdigest(), envelope["payload_hash"])

    def test_send_before_activation_is_a_silent_no_op(self) -> None:
        sent: list[dict[str, object]] = []

        class Session:
            def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None:
                sent.append(envelope)

        relay = WorkerPairCellRelay(Session())  # type: ignore[arg-type]

        relay.send({"kind": "quote", "from_worker_id": "a", "to_worker_id": "b", "lease_epoch": 1, "payload": {}})

        self.assertEqual([], sent)

    def test_unwrap_round_trips_kind_and_payload(self) -> None:
        wire_envelope = {
            "type": "pair_cell_relay",
            "protocol_version": 1,
            "lease": {
                "leader_worker_id": "worker-a", "follower_worker_id": "worker-b",
                "trader_id": "trader-1", "contract_hash": "hash-1", "lease_epoch": 3,
            },
            "from_worker_id": "worker-a", "to_worker_id": "worker-b",
            "request_id": "message-1", "attempt_id": None, "payload_hash": "unused",
            "payload": {"kind": "quote", "payload": {"symbol": "EURUSD"}},
        }

        inner = unwrap_pair_relay_envelope(wire_envelope)

        assert inner is not None
        self.assertEqual("quote", inner["kind"])
        self.assertEqual(3, inner["lease_epoch"])
        self.assertEqual({"symbol": "EURUSD"}, inner["payload"])

    def test_unwrap_rejects_a_structurally_malformed_envelope(self) -> None:
        self.assertIsNone(unwrap_pair_relay_envelope({"type": "pair_cell_relay"}))


class _FakeSession:
    """A minimal ``PairCellWorkerSession`` used to construct ``PairCellRuntime``
    without a real network connection."""

    def __init__(self, *, activation: dict[str, object] | None = None) -> None:
        self.worker_id = "worker-a"
        self.recovery_epoch = "epoch-a"
        self.trader_rpc_scheduler = DeadlineAwareTraderRpcScheduler()
        self._activation = activation
        self.sent_relay: list[dict[str, object]] = []
        self.dispatched_outcomes: list[object] = []
        self._relay_inbox: list[dict[str, object]] = []
        self._activation_inbox: list[dict[str, object]] = []

    def dispatch_scheduler_outcome(self, outcome: object) -> None:
        self.dispatched_outcomes.append(outcome)

    def send_trader_rpc(self, **kwargs: object) -> None:
        self.dispatched_outcomes.append(("trader_rpc", kwargs))

    def send_order_check(self, **kwargs: object) -> None:
        self.dispatched_outcomes.append(("order_check", kwargs))

    def send_order_check_error(self, **kwargs: object) -> None:
        self.dispatched_outcomes.append(("order_check_error", kwargs))

    def send_order_execute(self, **kwargs: object) -> None:
        self.dispatched_outcomes.append(("order_execute", kwargs))

    def send_order_execute_error(self, **kwargs: object) -> None:
        self.dispatched_outcomes.append(("order_execute_error", kwargs))

    def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None:
        self.sent_relay.append(envelope)

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]:
        envelopes, self._relay_inbox = self._relay_inbox, []
        return envelopes

    def drain_pair_cell_activations(self) -> list[dict[str, object]]:
        activations, self._activation_inbox = self._activation_inbox, []
        return activations

    def request_pair_cell_activation(self) -> dict[str, object] | None:
        return self._activation


def _activation_payload() -> dict[str, object]:
    contract = PairContract(
        contract_id="contract-1", leader_worker_id="worker-a", follower_worker_id="worker-b",
        trader_id="trader-1", symbol="EURUSD", leader_direction="LONG", follower_direction="SHORT",
        leader_volume="0.10", follower_volume="0.10", filling_mode="FOK", edge_threshold="0.0002",
        risk_budget_usd="50", quote_max_age_seconds=5.0, quote_max_skew_seconds=5.0,
        arm_timeout_seconds=5.0, commit_lead_seconds=0.2, execution_expiry_seconds=5.0,
    )
    now = datetime.now(UTC)
    return {
        "contract": contract.canonical(),
        "lease": {
            "leader_worker_id": contract.leader_worker_id, "follower_worker_id": contract.follower_worker_id,
            "trader_id": contract.trader_id, "contract_hash": contract.hash, "epoch": 1,
            "issued_at": now.isoformat(), "expires_at": (now + timedelta(minutes=30)).isoformat(),
        },
    }


class PairCellRuntimeConstructionTests(unittest.TestCase):
    """Requirement: durable cell DB beside worker identity/effect journal;
    activation/recovery from a controller-delivered contract+lease; restart
    construction; clean shutdown closes the cell DB."""

    def _tmp_dir(self) -> Path:
        directory = Path(tempfile.mkdtemp(prefix="pair_cell_adapter_test_"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def test_construction_is_inert_without_any_activation(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        session = _FakeSession(activation=None)

        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        self.assertFalse(runtime.activated)
        self.assertIsNone(runtime.pump(datetime.now(UTC)))
        self.assertTrue(db_path.exists())

    def test_construction_activates_from_a_controller_delivered_contract_and_lease(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        session = _FakeSession(activation=_activation_payload())

        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        self.assertTrue(runtime.activated)

    def test_restart_recovers_activation_without_a_reachable_controller(self) -> None:
        """Simulates a Worker process restart: the first run durably applies a
        controller-delivered activation; the second run's session has nothing
        to deliver (offline controller), yet the adapter is still activated
        because it recovers the same contract/lease locally."""

        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal_path = directory / "worker.effects.sqlite"
        activation = _activation_payload()

        first_journal = WorkerEffectJournal(journal_path)
        first_session = _FakeSession(activation=activation)
        first_runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=first_session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=first_journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.assertTrue(first_runtime.activated)
        first_runtime.close()
        first_journal.close()

        second_journal = WorkerEffectJournal(journal_path)
        self.addCleanup(second_journal.close)
        second_session = _FakeSession(activation=None)  # controller unreachable this run
        second_runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=second_session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=second_journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(second_runtime.close)

        self.assertTrue(second_runtime.activated)

    def test_close_closes_the_durable_cell_database(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        session = _FakeSession(activation=None)
        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )

        runtime.close()

        # The cell's own sqlite connection is closed; a further internal
        # operation must fail rather than silently succeed against a
        # closed handle.
        with self.assertRaises(Exception):
            runtime._cell._db.execute("SELECT 1")


class SharedSchedulerForeignItemTests(unittest.TestCase):
    """Fixes "scheduler/effect journal ownership": the cell must share the
    Worker's single scheduler, and any other Worker traffic that scheduler
    dequeues ahead of the cell's own item must still be fully served (never
    silently dropped, never deadlocking the scheduler's single slot)."""

    def test_a_foreign_higher_priority_item_is_served_before_the_cells_own_write(self) -> None:
        directory = Path(tempfile.mkdtemp(prefix="pair_cell_adapter_test_"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        session = _FakeSession(activation=_activation_payload())
        mt5 = FakeMT5()
        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=mt5, effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        # Queue a foreign, higher-priority (emergency) item on the SAME
        # shared scheduler before the cell admits its own (execution
        # -priority) write.
        scheduler = session.trader_rpc_scheduler
        admitted = scheduler.admit(
            {
                "request_id": "foreign-1", "command_id": "foreign-1", "kind": "operation",
                "payload": {"type": "close", "ticket": "900", "volume": "0.10"},
                "priority": "emergency",
            }
        )
        self.assertIsNone(admitted)  # queued, not yet ready

        journal.prepare("own-effect-1", {"type": "cancel", "ticket": "42"})
        outcome = runtime._cell._run_broker_write(
            "own-effect-1",
            {"type": "cancel", "ticket": "42"},
            priority="execution",
        )

        self.assertEqual("completed", outcome)
        # The foreign emergency item was drained and fully served (not
        # left dangling in the scheduler, which would otherwise deadlock
        # every future dequeue behind its single `_active` slot).
        self.assertIsNone(scheduler._active)
        self.assertEqual(2, len(mt5.sent_requests))


if __name__ == "__main__":
    unittest.main()
