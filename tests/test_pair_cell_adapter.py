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
from types import SimpleNamespace
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
    eligible_symbols_of,
    preload_eligible_symbols,
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
        # Multi-symbol support: symbols/ticks not registered here fall back to
        # the single ``self.symbol``/``self.tick`` above, preserving every
        # existing single-symbol test unchanged.
        self.symbols_by_name: dict[str, dict[str, object]] = {}
        self.ticks_by_symbol: dict[str, dict[str, object]] = {}
        self.visible_symbols: set[str] = {"EURUSD"}
        self.select_should_fail: set[str] = set()
        self.select_calls: list[str] = []

    def account_info(self) -> object:
        return dict(self.account)

    def terminal_info(self) -> object:
        return dict(self.terminal)

    def orders_get(self) -> object:
        return [dict(o) for o in self.orders]

    def positions_get(self) -> object:
        return [dict(p) for p in self.positions]

    def symbol_info(self, symbol: str) -> object:
        data = dict(self.symbols_by_name.get(symbol, self.symbol))
        data.setdefault("visible", symbol in self.visible_symbols)
        return data

    def symbols_get(self) -> object:
        names = {"EURUSD", *self.symbols_by_name}
        return [
            {"name": name, "trade_mode": self.symbols_by_name.get(name, {}).get("trade_mode", 4)}
            for name in sorted(names)
        ]

    def symbol_info_tick(self, symbol: str) -> object:
        return dict(self.ticks_by_symbol.get(symbol, self.tick))

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        self.select_calls.append(symbol)
        if symbol in self.select_should_fail:
            return False
        if enable:
            self.visible_symbols.add(symbol)
        else:
            self.visible_symbols.discard(symbol)
        return True

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

    def test_distinct_profit_and_loss_tick_values_are_relayed_separately(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["XAUUSD"] = {
            "trade_tick_size": 0.01,
            "point": 0.01,
            "trade_stops_level": 20,
            "trade_freeze_level": 0,
            "trade_tick_value": 1.0,
            "trade_tick_value_profit": 1.25,
            "trade_tick_value_loss": 0.75,
        }

        calibration = refresh_protection_calibration(mt5, "XAUUSD")

        assert calibration is not None
        self.assertEqual("1.25", calibration.profit_tick_value)
        self.assertEqual("0.75", calibration.loss_tick_value)

    def test_falls_back_to_symmetric_tick_value_when_profit_and_loss_are_absent(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["GBPUSD"] = {
            "trade_tick_size": 0.00001,
            "point": 0.00001,
            "trade_stops_level": 10,
            "trade_freeze_level": 0,
            "trade_tick_value": 1.0,
        }

        calibration = refresh_protection_calibration(mt5, "GBPUSD")

        assert calibration is not None
        self.assertEqual("1.0", calibration.profit_tick_value)
        self.assertEqual("1.0", calibration.loss_tick_value)


class EligibleSymbolsTests(unittest.TestCase):
    """Requirement: enumerate a policy contract's full eligible product
    universe, or a legacy contract's one fixed symbol."""

    def _automatic_contract(self, eligible_symbols: tuple[str, ...]) -> PairContract:
        return PairContract(
            contract_id="contract-1", leader_worker_id="worker-a", follower_worker_id="worker-b",
            trader_id="trader-1", leader_volume="0.10", follower_volume="0.10", filling_mode="FOK",
            risk_budget_usd="50", quote_max_age_seconds=5.0, quote_max_skew_seconds=5.0,
            arm_timeout_seconds=5.0, commit_lead_seconds=0.2, execution_expiry_seconds=5.0,
            entry_edge_points="1.5", eligible_symbols=eligible_symbols,
        )

    def test_automatic_contract_reports_its_full_eligible_symbol_tuple(self) -> None:
        contract = self._automatic_contract(("EURUSD", "GBPUSD", "XAUUSD"))

        self.assertEqual(("EURUSD", "GBPUSD", "XAUUSD"), eligible_symbols_of(contract))

    def test_legacy_contract_reports_its_one_fixed_symbol(self) -> None:
        contract = PairContract(
            contract_id="contract-1", leader_worker_id="worker-a", follower_worker_id="worker-b",
            trader_id="trader-1", symbol="EURUSD", leader_direction="LONG", follower_direction="SHORT",
            leader_volume="0.10", follower_volume="0.10", filling_mode="FOK", edge_threshold="0.0002",
            risk_budget_usd="50", quote_max_age_seconds=5.0, quote_max_skew_seconds=5.0,
            arm_timeout_seconds=5.0, commit_lead_seconds=0.2, execution_expiry_seconds=5.0,
        )

        self.assertEqual(("EURUSD",), eligible_symbols_of(contract))


class PreloadEligibleSymbolsTests(unittest.TestCase):
    def test_empty_policy_filter_discovers_the_brokers_executable_catalog(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["USDJPY"] = dict(mt5.symbol)

        visible = preload_eligible_symbols(mt5, ())

        self.assertEqual(("EURUSD", "USDJPY"), visible)
        self.assertEqual(["EURUSD", "USDJPY"], mt5.select_calls)

    def test_full_catalog_discovery_excludes_direction_limited_and_close_only_products(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["LONGONLY"] = {**mt5.symbol, "trade_mode": 1}
        mt5.symbols_by_name["SHORTONLY"] = {**mt5.symbol, "trade_mode": 2}
        mt5.symbols_by_name["CLOSEONLY"] = {**mt5.symbol, "trade_mode": 3}

        visible = preload_eligible_symbols(mt5, ())

        self.assertEqual(("EURUSD",), visible)

    """Requirement: preload (Market Watch select) and enumerate only the
    eligible symbols genuinely visible in this Worker's MT5 terminal."""

    def test_every_eligible_symbol_is_selected_and_returned_when_visible(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["GBPUSD"] = dict(mt5.symbol)

        visible = preload_eligible_symbols(mt5, ("EURUSD", "GBPUSD"))

        self.assertEqual(("EURUSD", "GBPUSD"), visible)
        self.assertEqual(["EURUSD", "GBPUSD"], mt5.select_calls)

    def test_a_symbol_this_broker_does_not_carry_is_skipped_not_fatal(self) -> None:
        mt5 = FakeMT5()
        mt5.select_should_fail.add("UNKNOWN")

        visible = preload_eligible_symbols(mt5, ("EURUSD", "UNKNOWN"))

        self.assertEqual(("EURUSD",), visible)

    def test_a_symbol_that_cannot_be_made_visible_is_skipped(self) -> None:
        mt5 = FakeMT5()
        mt5.symbols_by_name["GBPUSD"] = dict(mt5.symbol)
        mt5.visible_symbols.discard("GBPUSD")
        mt5.select_should_fail.add("GBPUSD")  # symbol_select fails, so visibility never flips to True

        visible = preload_eligible_symbols(mt5, ("EURUSD", "GBPUSD"))

        self.assertEqual(("EURUSD",), visible)


class ReadinessFactsTests(unittest.TestCase):
    def test_ready_facts_reflect_empty_exposure_and_margin_headroom(self) -> None:
        mt5 = FakeMT5()
        mt5.positions = []

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo",
            calibrations={"EURUSD": ProtectionCalibration("0.00001", "0.00001", "0.0001", "1", "1")},
            calibration_fresh=True, now=datetime.now(UTC),
        )

        self.assertTrue(facts.terminal_connected)
        self.assertEqual([], facts.orders)
        self.assertEqual([], facts.positions)
        self.assertTrue(facts.margin_headroom_ok)

    def test_a_single_calibrated_symbol_backfills_the_legacy_protection_calibration_field(self) -> None:
        """A legacy (fixed one-symbol) contract's readiness gate reads
        ``protection_calibration`` directly rather than ``symbol_calibrations``,
        so a single calibrated symbol must still populate it."""

        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo",
            calibrations={"EURUSD": ProtectionCalibration("0.00001", "0.00001", "0.0001", "1", "1")},
            calibration_fresh=True, now=datetime.now(UTC),
        )

        assert facts.protection_calibration is not None
        self.assertEqual("1", facts.protection_calibration.loss_tick_value)

    def test_symbol_calibrations_map_carries_every_eligible_symbols_plan(self) -> None:
        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo",
            calibrations={
                "EURUSD": ProtectionCalibration("0.00001", "0.00001", "0.0001", "1", "1"),
                "GBPUSD": ProtectionCalibration("0.00001", "0.00001", "0.0001", "1.1", "0.9"),
            },
            calibration_fresh=True, now=datetime.now(UTC),
        )

        self.assertEqual({"EURUSD", "GBPUSD"}, set(facts.symbol_calibrations))
        self.assertEqual("0.9", facts.symbol_calibrations["GBPUSD"].loss_tick_value)
        self.assertIsNone(facts.protection_calibration)

    def test_unavailable_broker_reads_are_never_treated_as_empty(self) -> None:
        class NoneReadsMT5(FakeMT5):
            def orders_get(self) -> object:
                return None

        mt5 = NoneReadsMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo", calibrations={}, calibration_fresh=False, now=datetime.now(UTC)
        )

        self.assertIsNone(facts.orders)

    def test_account_mismatch_is_not_ready(self) -> None:
        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=999999, server="Broker-Demo", calibrations={}, calibration_fresh=False, now=datetime.now(UTC)
        )

        self.assertFalse(facts.margin_headroom_ok)

    def test_insufficient_margin_headroom_is_not_ready(self) -> None:
        mt5 = FakeMT5()

        facts = read_readiness_facts(
            mt5, login=555111, server="Broker-Demo", calibrations={}, calibration_fresh=False, now=datetime.now(UTC),
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
        self._quarantine_release_inbox: list[dict[str, object]] = []
        self.sent_quarantine_release_results: list[dict[str, object]] = []

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

    def drain_pair_cell_quarantine_releases(self) -> list[dict[str, object]]:
        releases, self._quarantine_release_inbox = self._quarantine_release_inbox, []
        return releases

    def send_pair_cell_quarantine_release_result(self, *, request_id: str, symbol: str, outcome: str) -> None:
        self.sent_quarantine_release_results.append(
            {"request_id": request_id, "symbol": symbol, "outcome": outcome}
        )

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


def _automatic_activation_payload(eligible_symbols: tuple[str, ...]) -> dict[str, object]:
    """A policy-activated contract (``entry_edge_points``/``eligible_symbols``)
    rather than one fixed legacy symbol/direction pair."""

    contract = PairContract(
        contract_id="contract-2", leader_worker_id="worker-a", follower_worker_id="worker-b",
        trader_id="trader-1", leader_volume="0.10", follower_volume="0.10", filling_mode="FOK",
        risk_budget_usd="50", quote_max_age_seconds=5.0, quote_max_skew_seconds=5.0,
        arm_timeout_seconds=5.0, commit_lead_seconds=0.2, execution_expiry_seconds=5.0,
        entry_edge_points="1.5", eligible_symbols=eligible_symbols,
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


class PairCellRuntimeMultiSymbolTests(unittest.TestCase):
    """Requirement: a policy-activated (``entry_edge_points``/
    ``eligible_symbols``) contract enumerates, preloads, and polls every
    eligible visible symbol -- not only one fixed legacy symbol -- and feeds
    a per-symbol :class:`ProtectionCalibration` map without a signal-time RPC."""

    def _tmp_dir(self) -> Path:
        directory = Path(tempfile.mkdtemp(prefix="pair_cell_adapter_multi_test_"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def _gbpusd_symbol_and_tick(self) -> tuple[dict[str, object], dict[str, object]]:
        symbol = {
            "trade_tick_size": 0.00001, "point": 0.00001, "trade_stops_level": 10, "trade_freeze_level": 0,
            "trade_tick_value": 1.0, "trade_tick_value_profit": 1.2, "trade_tick_value_loss": 0.8,
        }
        tick = {"bid": 1.2500, "ask": 1.2502, "last": 1.2501, "time": 1_700_000_000, "time_msc": 1_700_000_000_456}
        return symbol, tick

    def test_activation_enumerates_and_preloads_the_full_eligible_symbol_universe(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        mt5 = FakeMT5()
        mt5.symbols_by_name["GBPUSD"], mt5.ticks_by_symbol["GBPUSD"] = self._gbpusd_symbol_and_tick()
        session = _FakeSession(activation=_automatic_activation_payload(("EURUSD", "GBPUSD")))

        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=mt5, effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        self.assertTrue(runtime.activated)
        self.assertEqual(("EURUSD", "GBPUSD"), runtime._eligible_symbols)
        self.assertEqual(("EURUSD", "GBPUSD"), runtime._visible_symbols)
        self.assertIn("GBPUSD", mt5.select_calls)

    def test_an_eligible_symbol_this_broker_does_not_carry_is_excluded_but_others_still_activate(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        mt5 = FakeMT5()
        mt5.select_should_fail.add("XAUUSD")
        mt5.visible_symbols.discard("XAUUSD")
        session = _FakeSession(activation=_automatic_activation_payload(("EURUSD", "XAUUSD")))

        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=mt5, effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        self.assertTrue(runtime.activated)
        self.assertEqual(("EURUSD", "XAUUSD"), runtime._eligible_symbols)
        self.assertEqual(("EURUSD",), runtime._visible_symbols)

    def test_pump_polls_quotes_and_relays_a_per_symbol_calibration_map(self) -> None:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        mt5 = FakeMT5()
        mt5.symbols_by_name["GBPUSD"], mt5.ticks_by_symbol["GBPUSD"] = self._gbpusd_symbol_and_tick()
        session = _FakeSession(activation=_automatic_activation_payload(("EURUSD", "GBPUSD")))

        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=mt5, effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)

        runtime.pump(datetime.now(UTC))

        # Both eligible symbols were calibrated, each keeping its own
        # distinct profit/loss tick value (never a signal-time RPC: this is
        # entirely local MT5 evidence read on a bounded polling schedule).
        self.assertEqual({"EURUSD", "GBPUSD"}, set(runtime._calibrations))
        self.assertEqual("1.0", runtime._calibrations["EURUSD"].loss_tick_value)
        self.assertEqual("0.8", runtime._calibrations["GBPUSD"].loss_tick_value)
        self.assertEqual("1.2", runtime._calibrations["GBPUSD"].profit_tick_value)


class QuarantineReleaseDrainTests(unittest.TestCase):
    """Requirement: an admin-issued quarantine release, pushed by the
    controller, must reach this Worker's own ``PairExecutionCell`` (with
    actor/reason) via the session's inbox -- delivered regardless of whether
    a contract happens to be activated, since quarantine is Worker-local
    product state rather than contract-scoped."""

    def _tmp_dir(self) -> Path:
        directory = Path(tempfile.mkdtemp(prefix="pair_cell_adapter_test_"))
        self.addCleanup(shutil.rmtree, directory, ignore_errors=True)
        return directory

    def _runtime(self, *, activation: dict[str, object] | None) -> tuple[PairCellRuntime, _FakeSession]:
        directory = self._tmp_dir()
        db_path = directory / "worker.paircell.sqlite"
        journal = WorkerEffectJournal(directory / "worker.effects.sqlite")
        self.addCleanup(journal.close)
        session = _FakeSession(activation=activation)
        runtime = PairCellRuntime(
            worker_id="worker-a", db_path=db_path, session=session,  # type: ignore[arg-type]
            mt5=FakeMT5(), effect_journal=journal, recovery_epoch="epoch-a",
            login=555111, server="Broker-Demo",
        )
        self.addCleanup(runtime.close)
        return runtime, session

    def test_a_pushed_release_clears_a_quarantined_product_and_reports_applied(self) -> None:
        runtime, session = self._runtime(activation=_activation_payload())
        runtime._cell._quarantine_product(
            "EURUSD", offending_worker_id="worker-a", attempt_id="attempt-1", receipt={"retcode": 10021}
        )
        self.assertEqual(("EURUSD",), runtime.quarantined_products())
        session._quarantine_release_inbox.append(
            {
                "request_id": "req-1",
                "symbol": "EURUSD",
                "actor": "alice",
                "reason": "broker confirmed the symbol is tradable again",
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )

        runtime.drain_relay()

        self.assertEqual((), runtime.quarantined_products())
        self.assertEqual(
            [{"request_id": "req-1", "symbol": "EURUSD", "outcome": "applied"}],
            session.sent_quarantine_release_results,
        )

    def test_a_release_for_a_never_quarantined_symbol_is_trivially_applied(self) -> None:
        """Requirement: not-found counts as applied -- there is nothing to
        release, so a retry (or a Worker that never quarantined the symbol
        in the first place) must never be reported as a failure."""

        runtime, session = self._runtime(activation=_activation_payload())
        self.assertEqual((), runtime.quarantined_products())
        session._quarantine_release_inbox.append(
            {
                "request_id": "req-1",
                "symbol": "EURUSD",
                "actor": "alice",
                "reason": "broker confirmed the symbol is tradable again",
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )

        runtime.drain_relay()

        self.assertEqual((), runtime.quarantined_products())
        self.assertEqual(
            [{"request_id": "req-1", "symbol": "EURUSD", "outcome": "applied"}],
            session.sent_quarantine_release_results,
        )

    def test_a_release_blocked_by_an_unresolved_attempt_reports_rejected_not_applied(self) -> None:
        """Requirement: ``PairExecutionCell`` silently keeps a quarantine in
        place while an attempt is unresolved -- it never raises and never
        reports this itself. The adapter must detect this (by comparing
        ``quarantined_products()`` before/after the event) and report
        "rejected", never a false "applied"."""

        runtime, session = self._runtime(activation=None)
        runtime._cell._quarantine_product(
            "EURUSD", offending_worker_id="worker-a", attempt_id="attempt-1", receipt={"retcode": 10021}
        )
        # A non-None attempt with the default "IDLE" state (never armed/committed
        # here) is enough to make the cell's own unresolved-attempt check true,
        # without needing a fully-populated Attempt/leg (which the cell never
        # dereferences while ``self._contract`` is also None, other than for
        # transition-logging ``attempt_id``).
        runtime._cell._attempt = SimpleNamespace(attempt_id="fake-attempt-1")  # type: ignore[assignment]
        session._quarantine_release_inbox.append(
            {
                "request_id": "req-1",
                "symbol": "EURUSD",
                "actor": "alice",
                "reason": "broker confirmed the symbol is tradable again",
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )

        runtime.drain_relay()

        self.assertEqual(("EURUSD",), runtime.quarantined_products())
        self.assertEqual(
            [{"request_id": "req-1", "symbol": "EURUSD", "outcome": "rejected"}],
            session.sent_quarantine_release_results,
        )

    def test_unresolved_attempt_rejects_release_even_when_symbol_was_not_quarantined_here(self) -> None:
        runtime, session = self._runtime(activation=None)
        runtime._cell._attempt = SimpleNamespace(attempt_id="fake-attempt-1")  # type: ignore[assignment]
        session._quarantine_release_inbox.append(
            {
                "request_id": "req-1",
                "symbol": "EURUSD",
                "actor": "alice",
                "reason": "broker confirmed the symbol is tradable again",
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )

        runtime.drain_relay()

        self.assertEqual(
            [{"request_id": "req-1", "symbol": "EURUSD", "outcome": "rejected"}],
            session.sent_quarantine_release_results,
        )

    def test_a_pushed_release_is_delivered_even_without_any_activated_contract(self) -> None:
        """Quarantine is Worker-local durable product state, not gated by
        whether this Worker currently has a contract activated."""

        runtime, session = self._runtime(activation=None)
        self.assertFalse(runtime.activated)
        runtime._cell._quarantine_product(
            "EURUSD", offending_worker_id="worker-a", attempt_id="attempt-1", receipt={"retcode": 10021}
        )
        session._quarantine_release_inbox.append(
            {
                "request_id": "req-1",
                "symbol": "EURUSD",
                "actor": "alice",
                "reason": "broker confirmed the symbol is tradable again",
                "observed_at": datetime.now(UTC).isoformat(),
            }
        )

        runtime.drain_relay()

        self.assertEqual((), runtime.quarantined_products())
        self.assertEqual("applied", session.sent_quarantine_release_results[0]["outcome"])

    def test_a_structurally_invalid_push_missing_identifiers_is_skipped_without_a_reply(self) -> None:
        """A push missing ``request_id``/``symbol`` cannot be correlated to
        any pending controller request, so it is logged and dropped rather
        than guessed at -- the controller times the request out instead of
        being sent a reply it cannot match, which is safer than a
        best-effort guess."""

        runtime, session = self._runtime(activation=_activation_payload())
        runtime._cell._quarantine_product(
            "EURUSD", offending_worker_id="worker-a", attempt_id="attempt-1", receipt={"retcode": 10021}
        )
        session._quarantine_release_inbox.append({"actor": "alice"})  # missing request_id and symbol

        runtime.drain_relay()  # must not raise

        self.assertEqual(("EURUSD",), runtime.quarantined_products())
        self.assertEqual([], session.sent_quarantine_release_results)

    def test_a_structurally_invalid_push_missing_reason_reports_rejected(self) -> None:
        """A push with identifiers but missing actor/reason/observed_at can
        still be correlated to a pending controller request, so it is
        answered definitively as "rejected" (never a false "applied") rather
        than left to time out."""

        runtime, session = self._runtime(activation=_activation_payload())
        runtime._cell._quarantine_product(
            "EURUSD", offending_worker_id="worker-a", attempt_id="attempt-1", receipt={"retcode": 10021}
        )
        session._quarantine_release_inbox.append({"request_id": "req-1", "symbol": "EURUSD"})

        runtime.drain_relay()  # must not raise

        self.assertEqual(("EURUSD",), runtime.quarantined_products())
        self.assertEqual(
            [{"request_id": "req-1", "symbol": "EURUSD", "outcome": "rejected"}],
            session.sent_quarantine_release_results,
        )


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

        self.assertEqual("completed", outcome.category)
        # The foreign emergency item was drained and fully served (not
        # left dangling in the scheduler, which would otherwise deadlock
        # every future dequeue behind its single `_active` slot).
        self.assertIsNone(scheduler._active)
        self.assertEqual(2, len(mt5.sent_requests))


if __name__ == "__main__":
    unittest.main()
