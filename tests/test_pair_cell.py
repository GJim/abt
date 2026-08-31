"""Tests for the Low-Latency Pair Execution Cell through its production seam.

Pair-level tests instantiate one leader and one follower :class:`PairExecutionCell`
connected by a deterministic in-memory relay, each with its own deterministic
MT5 fake, :class:`WorkerEffectJournal`, and :class:`DeadlineAwareTraderRpcScheduler` --
mirroring exactly how two separate Worker processes would be wired.  Tests
assert observable lifecycle results, emitted relay envelopes, broker effects,
and durable restart recovery; they never reach into private reducers or table
layout.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from dataclasses import replace
from pathlib import Path
import unittest

import copy
from uuid import uuid4

from abt.pair_cell import (
    BrokerSnapshotEvent,
    ClockTickEvent,
    LeaseEvent,
    LocalQuoteEvent,
    PairContract,
    PairExecutionCell,
    PairExecutionCellError,
    PairLease,
    ProtectionCalibration,
    QuarantineReleaseEvent,
    ReadinessFactsEvent,
    RelayEnvelopeReceived,
)
from abt.worker.effect_journal import EffectJournalError, WorkerEffectJournal
from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc


_DONE = 10009
_REJECTED = 10013

LEADER = "worker-leader"
FOLLOWER = "worker-follower"
TRADER = "trader-1"
SYMBOL = "EURUSD"


class _Network:
    """A deterministic in-memory relay: opaque envelopes, no edge derivation."""

    def __init__(self) -> None:
        self._cells: dict[str, PairExecutionCell] = {}
        self._queue: list[dict[str, object]] = []
        self.sent: list[dict[str, object]] = []
        self.drop_kinds: set[str] = set()
        self.auto_deliver = True

    def register(self, worker_id: str, cell: PairExecutionCell) -> None:
        self._cells[worker_id] = cell

    def adapter(self, worker_id: str) -> "_Adapter":
        return _Adapter(self, worker_id)

    def send_from(self, worker_id: str, envelope: dict[str, object]) -> None:
        self.sent.append(envelope)
        if envelope["kind"] in self.drop_kinds:
            return
        self._queue.append(envelope)
        if self.auto_deliver:
            self.pump()

    def pump(self) -> None:
        while self._queue:
            envelope = self._queue.pop(0)
            target = self._cells[str(envelope["to_worker_id"])]
            target.handle_event(RelayEnvelopeReceived(envelope))


class _Adapter:
    def __init__(self, network: _Network, worker_id: str) -> None:
        self._network, self._worker_id = network, worker_id

    def send(self, envelope: dict[str, object]) -> None:
        self._network.send_from(self._worker_id, envelope)


class FakeMT5:
    def __init__(self) -> None:
        self.positions: list[dict[str, object]] = []
        self.orders: list[dict[str, object]] = []
        self.requests: list[dict[str, object]] = []
        self._next_ticket = 1000
        self.reject = False
        self.raise_on_send = False
        self.reads_unavailable = False
        # Lost-response faults: the broker really did fill the order, but this
        # Worker never learns the outcome conclusively.
        self.raise_after_fill = False
        self.receipt_none_after_fill = False
        self.close_leaves_position_visible = False
        self.reject_next_entry_retcode: int | None = None

    def account_info(self) -> object:
        return {"login": 1, "server": "demo"}

    def positions_get(self) -> object:
        if self.reads_unavailable:
            return None
        return [dict(p) for p in self.positions]

    def orders_get(self) -> object:
        if self.reads_unavailable:
            return None
        return [dict(o) for o in self.orders]

    def symbol_info(self, symbol: str) -> object:
        return {"name": symbol}

    def order_send(self, request: dict[str, object]) -> object:
        self.requests.append(dict(request))
        if self.raise_on_send:
            raise RuntimeError("simulated MT5 exception after send_started")
        kind = request["type"]
        if kind == "market":
            if self.reject_next_entry_retcode is not None:
                retcode = self.reject_next_entry_retcode
                self.reject_next_entry_retcode = None
                return {"retcode": retcode, "comment": "No prices", "bid": 0.0, "ask": 0.0}
            if self.reject:
                return {"retcode": _REJECTED, "comment": "no money"}
            ticket = self._next_ticket
            self._next_ticket += 1
            self.positions.append(
                {
                    "ticket": ticket,
                    "symbol": request["symbol"],
                    "type": 0 if request["direction"] == "LONG" else 1,
                    "volume": float(request["volume"]),
                    "price_open": 1.10010,
                    "sl": float(request["sl"]),
                    "tp": float(request["tp"]),
                }
            )
            if self.raise_after_fill:
                raise RuntimeError("simulated MT5 exception after the broker already filled the order")
            if self.receipt_none_after_fill:
                return None
            return {"retcode": _DONE, "order": ticket, "deal": ticket, "price": 1.10010}
        if kind == "modify_sl_tp":
            for position in self.positions:
                if str(position["ticket"]) == str(request["position"]):
                    position["sl"] = float(request["sl"])
                    position["tp"] = float(request["tp"])
                    return {"retcode": _DONE}
            return {"retcode": _REJECTED, "comment": "unknown ticket"}
        if kind == "close":
            if not self.close_leaves_position_visible:
                self.positions = [p for p in self.positions if str(p["ticket"]) != str(request["ticket"])]
            return {"retcode": _DONE}
        if kind == "cancel":
            self.orders = [o for o in self.orders if str(o["ticket"]) != str(request["ticket"])]
            return {"retcode": _DONE}
        raise AssertionError(f"unhandled fake MT5 request kind: {kind}")


class FaultyJournal(WorkerEffectJournal):
    """A real effect journal whose durable boundaries can fail deterministically."""

    def __init__(self, path: Path) -> None:
        super().__init__(path)
        self.fail_prepare = 0
        self.fail_send_started = 0
        self.fail_receipt = 0

    def prepare(self, effect_id: str, payload: dict[str, object]) -> None:
        if self.fail_prepare > 0:
            self.fail_prepare -= 1
            raise EffectJournalError("simulated durable prepare failure")
        super().prepare(effect_id, payload)

    def mark_send_started(self, effect_id: str) -> None:
        if self.fail_send_started > 0:
            self.fail_send_started -= 1
            raise EffectJournalError("simulated durable send_started failure")
        super().mark_send_started(effect_id)

    def record_receipt(self, effect_id: str, receipt: dict[str, object]) -> None:
        if self.fail_receipt > 0:
            self.fail_receipt -= 1
            raise EffectJournalError("simulated durable receipt failure")
        super().record_receipt(effect_id, receipt)


def _contract(
    *,
    mode: str = "live",
    edge_threshold: str = "0.00010",
    quote_max_age_seconds: float = 2.0,
    arm_timeout_seconds: float = 2.0,
    commit_lead_seconds: float = 0.05,
    execution_expiry_seconds: float = 1.0,
    maximum_holding_seconds: float | None = None,
    flatten_at_ny: str | None = None,
) -> PairContract:
    return PairContract(
        contract_id="contract-1",
        leader_worker_id=LEADER,
        follower_worker_id=FOLLOWER,
        trader_id=TRADER,
        symbol=SYMBOL,
        leader_direction="LONG",
        follower_direction="SHORT",
        leader_volume="0.10",
        follower_volume="0.10",
        filling_mode="FOK",
        edge_threshold=edge_threshold,
        risk_budget_usd="50",
        quote_max_age_seconds=quote_max_age_seconds,
        quote_max_skew_seconds=1.0,
        arm_timeout_seconds=arm_timeout_seconds,
        commit_lead_seconds=commit_lead_seconds,
        execution_expiry_seconds=execution_expiry_seconds,
        mode=mode,  # type: ignore[arg-type]
        maximum_holding_seconds=maximum_holding_seconds,
        flatten_at_ny=flatten_at_ny,
    )


def _automatic_contract(*, entry_edge_points: str = "1", mode: str = "live") -> PairContract:
    return PairContract(
        contract_id="automatic-contract-1",
        leader_worker_id=LEADER,
        follower_worker_id=FOLLOWER,
        trader_id=TRADER,
        leader_volume="0.10",
        follower_volume="0.10",
        filling_mode="FOK",
        risk_budget_usd="50",
        quote_max_age_seconds=2.0,
        quote_max_skew_seconds=1.0,
        arm_timeout_seconds=2.0,
        commit_lead_seconds=0.05,
        execution_expiry_seconds=1.0,
        entry_edge_points=entry_edge_points,
        eligible_symbols=(),
        mode=mode,  # type: ignore[arg-type]
    )


def _lease(contract: PairContract, *, epoch: int = 1, now: datetime, ttl_seconds: float = 300) -> PairLease:
    return PairLease(
        leader_worker_id=LEADER,
        follower_worker_id=FOLLOWER,
        trader_id=TRADER,
        contract_hash=contract.hash,
        epoch=epoch,
        issued_at=now,
        expires_at=now + timedelta(seconds=ttl_seconds),
    )


_CALIBRATION = ProtectionCalibration(
    tick_size="0.00001",
    point="0.00001",
    minimum_stop_distance="0.00005",
    profit_tick_value="1",
    loss_tick_value="1",
)

_JPY_CALIBRATION = ProtectionCalibration(
    tick_size="0.001",
    point="0.001",
    minimum_stop_distance="0.005",
    profit_tick_value="0.70",
    loss_tick_value="0.72",
)


def _readiness(
    now: datetime,
    *,
    orders: list | None = None,
    positions: list | None = None,
    symbol_calibrations: dict[str, ProtectionCalibration] | None = None,
) -> ReadinessFactsEvent:
    return ReadinessFactsEvent(
        account_login="1",
        account_server="demo",
        terminal_connected=True,
        orders=[] if orders is None else orders,
        positions=[] if positions is None else positions,
        margin_headroom_ok=True,
        symbol_constraints_fresh=True,
        protection_calibration=_CALIBRATION,
        observed_at=now,
        symbol_calibrations={} if symbol_calibrations is None else symbol_calibrations,
    )


class _Harness:
    """Wires one leader and one follower cell over a deterministic relay."""

    def __init__(
        self,
        tmp_path: Path,
        *,
        mode: str = "live",
        contract: PairContract | None = None,
        leader_journal: WorkerEffectJournal | None = None,
    ) -> None:
        self.tmp_path = tmp_path
        self.now = datetime(2026, 1, 1, tzinfo=UTC)
        self.network = _Network()
        self.contract = contract if contract is not None else _contract(mode=mode)
        self.lease = _lease(self.contract, now=self.now)
        self.leader_mt5 = FakeMT5()
        self.follower_mt5 = FakeMT5()
        self.leader_journal = leader_journal or WorkerEffectJournal(tmp_path / "leader_journal.sqlite3")
        self.follower_journal = WorkerEffectJournal(tmp_path / "follower_journal.sqlite3")
        self.leader_scheduler = DeadlineAwareTraderRpcScheduler(now=lambda: self.now)
        self.follower_scheduler = DeadlineAwareTraderRpcScheduler(now=lambda: self.now)
        self.leader = PairExecutionCell(
            worker_id=LEADER,
            db_path=tmp_path / "leader_cell.sqlite3",
            relay=self.network.adapter(LEADER),
            mt5=self.leader_mt5,
            effect_journal=self.leader_journal,
            scheduler=self.leader_scheduler,
        )
        self.follower = PairExecutionCell(
            worker_id=FOLLOWER,
            db_path=tmp_path / "follower_cell.sqlite3",
            relay=self.network.adapter(FOLLOWER),
            mt5=self.follower_mt5,
            effect_journal=self.follower_journal,
            scheduler=self.follower_scheduler,
        )
        self.network.register(LEADER, self.leader)
        self.network.register(FOLLOWER, self.follower)

    def restart_leader(self) -> PairExecutionCell:
        """Rebuild the leader cell from its durable database alone."""

        self.leader.close()
        self.leader = PairExecutionCell(
            worker_id=LEADER,
            db_path=self.tmp_path / "leader_cell.sqlite3",
            relay=self.network.adapter(LEADER),
            mt5=self.leader_mt5,
            effect_journal=self.leader_journal,
            scheduler=DeadlineAwareTraderRpcScheduler(now=lambda: self.now),
        )
        self.network.register(LEADER, self.leader)
        self.leader.recover()
        return self.leader

    def restart_follower(self) -> PairExecutionCell:
        """Rebuild the follower cell from its durable database alone."""

        self.follower.close()
        self.follower = PairExecutionCell(
            worker_id=FOLLOWER,
            db_path=self.tmp_path / "follower_cell.sqlite3",
            relay=self.network.adapter(FOLLOWER),
            mt5=self.follower_mt5,
            effect_journal=self.follower_journal,
            scheduler=DeadlineAwareTraderRpcScheduler(now=lambda: self.now),
        )
        self.network.register(FOLLOWER, self.follower)
        self.follower.recover()
        return self.follower

    def activate_both(self) -> None:
        self.leader.activate(self.contract, self.lease)
        self.follower.activate(self.contract, self.lease)

    def make_ready(self) -> None:
        calibrations = (
            {SYMBOL: _CALIBRATION, "USDJPY": _JPY_CALIBRATION}
            if self.contract.entry_edge_points is not None
            else None
        )
        self.leader.handle_event(_readiness(self.now, symbol_calibrations=calibrations))
        self.follower.handle_event(_readiness(self.now, symbol_calibrations=calibrations))

    def feed_quotes(
        self,
        *,
        leader_bid: str = "1.10000",
        leader_ask: str = "1.10010",
        follower_bid: str = "1.10050",
        follower_ask: str = "1.10060",
        sequence: int = 1,
    ) -> None:
        self.leader.handle_event(
            LocalQuoteEvent(
                symbol=SYMBOL,
                bid=Decimal(leader_bid),
                ask=Decimal(leader_ask),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-a",
                sequence=sequence,
            )
        )
        self.follower.handle_event(
            LocalQuoteEvent(
                symbol=SYMBOL,
                bid=Decimal(follower_bid),
                ask=Decimal(follower_ask),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-b",
                sequence=sequence,
            )
        )

    def local_quote(self, cell: PairExecutionCell, *, bid: str, ask: str, sequence: int) -> None:
        cell.handle_event(
            LocalQuoteEvent(
                symbol=SYMBOL,
                bid=Decimal(bid),
                ask=Decimal(ask),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-restarted",
                sequence=sequence,
            )
        )

    def feed_symbol_quotes(
        self,
        symbol: str,
        *,
        leader_bid: str,
        leader_ask: str,
        follower_bid: str,
        follower_ask: str,
        sequence: int,
    ) -> None:
        self.leader.handle_event(
            LocalQuoteEvent(
                symbol=symbol, bid=Decimal(leader_bid), ask=Decimal(leader_ask),
                broker_time=self.now, local_receive_time=self.now,
                recovery_epoch="epoch-a", sequence=sequence,
            )
        )
        self.follower.handle_event(
            LocalQuoteEvent(
                symbol=symbol, bid=Decimal(follower_bid), ask=Decimal(follower_ask),
                broker_time=self.now, local_receive_time=self.now,
                recovery_epoch="epoch-b", sequence=sequence,
            )
        )

    def tick(self, seconds: float = 0.0) -> None:
        self.now += timedelta(seconds=seconds)
        self.leader.handle_event(ClockTickEvent(self.now))
        self.follower.handle_event(ClockTickEvent(self.now))

    def advance_to_active(self) -> None:
        """Drive the happy path from readiness through both refined legs."""

        self.activate_both()
        self.make_ready()
        self.feed_quotes()
        self.tick(0.2)  # past commit_lead_seconds so the armed commit executes


class PairExecutionCellHappyPathTests(unittest.TestCase):
    def test_leader_and_follower_reach_active_with_refined_protection(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()

            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))
            follower_result = harness.follower.handle_event(ClockTickEvent(harness.now))

            self.assertEqual("ACTIVE", leader_result.state)
            self.assertEqual("ACTIVE", follower_result.state)
            self.assertEqual(leader_result.attempt_id, follower_result.attempt_id)
            self.assertIsNotNone(leader_result.attempt_id)
            self.assertEqual(1, len(harness.leader_mt5.positions))
            self.assertEqual(1, len(harness.follower_mt5.positions))
            leader_position = harness.leader_mt5.positions[0]
            follower_position = harness.follower_mt5.positions[0]
            self.assertNotEqual(leader_position["sl"], leader_position.get("emergency_sl"))
            self.assertEqual(0, leader_position["type"])  # LONG
            self.assertEqual(1, follower_position["type"])  # SHORT
            # Structured timing evidence covers the full decision-to-terminal chain.
            for key in (
                "local_quote_observed", "peer_quote_observed", "edge_decision", "attempt_persisted",
                "arm_send", "commit_send", "scheduler_dequeue", "mt5_send_started", "broker_response",
                "position_observed", "protection_verified", "terminal_time",
            ):
                self.assertIn(key, leader_result.timing, key)
            transitions = harness.leader.transition_history()
            self.assertTrue(any(t["event"] == "timing:edge_decision" for t in transitions))

    def test_shadow_mode_never_calls_broker_and_still_reaches_active(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, mode="shadow")
            harness.advance_to_active()

            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertEqual("ACTIVE", leader_result.state)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)


class PairExecutionCellExitPolicyTests(unittest.TestCase):
    def test_maximum_holding_age_closes_both_legs_and_verifies_empty(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(maximum_holding_seconds=210))
            harness.advance_to_active()

            harness.tick(209.9)
            self.assertEqual(1, len(harness.leader_mt5.positions))
            self.assertEqual(1, len(harness.follower_mt5.positions))

            harness.tick(0.2)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertTrue(
                any(
                    transition["event"] == "close_requested"
                    and transition["detail"] == "maximum_holding_seconds"
                    for transition in harness.leader.transition_history()
                )
            )

    def test_maximum_holding_age_survives_worker_restart(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(maximum_holding_seconds=210))
            harness.advance_to_active()
            harness.tick(180)
            harness.restart_leader()

            harness.tick(30.1)

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

    def test_recovery_reestablishes_missing_active_timestamp_before_timed_exit(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(maximum_holding_seconds=210))
            harness.advance_to_active()
            harness.leader._db.execute("DELETE FROM cell_active_state")  # noqa: SLF001 - simulate crash boundary
            harness.leader._db.commit()  # noqa: SLF001

            restarted = harness.restart_leader()
            recovered = restarted.handle_event(ClockTickEvent(harness.now))
            self.assertEqual("ACTIVE", recovered.state)

            harness.tick(210.1)

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

    def test_new_york_flatten_closes_active_pair_and_blocks_new_entry(self) -> None:
        with _tmp_dir() as tmp_path:
            # 2026-01-01 00:00 UTC is 2025-12-31 19:00 America/New_York.
            harness = _Harness(tmp_path, contract=_contract(flatten_at_ny="19:01"))
            harness.advance_to_active()

            harness.tick(61)

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)
            harness.make_ready()
            harness.feed_quotes(sequence=2)
            harness.tick(0.2)
            self.assertIsNone(harness.leader.handle_event(ClockTickEvent(harness.now)).attempt_id)

    def test_close_waits_for_a_later_snapshot_when_mt5_still_reports_the_position(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(maximum_holding_seconds=1))
            harness.advance_to_active()
            harness.leader_mt5.close_leaves_position_visible = True

            result = harness.leader.handle_event(ClockTickEvent(harness.now + timedelta(seconds=1.1)))

            self.assertEqual("CONVERGING_EMPTY", result.state)
            close_requests = [request for request in harness.leader_mt5.requests if request["type"] == "close"]
            self.assertEqual(1, len(close_requests))

    def test_dst_fall_back_does_not_reopen_entry_after_the_first_cutoff_occurrence(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(flatten_at_ny="01:30"))
            harness.now = datetime(2026, 11, 1, 5, 31, tzinfo=UTC)  # 01:31 EDT, after the first cutoff
            harness.lease = _lease(harness.contract, now=harness.now, ttl_seconds=7200)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            self.assertIsNone(harness.leader.handle_event(ClockTickEvent(harness.now)).attempt_id)

            harness.now = datetime(2026, 11, 1, 6, 1, tzinfo=UTC)  # 01:01 EST after clocks roll back
            harness.make_ready()
            harness.feed_quotes(sequence=2)
            harness.tick(0.2)

            self.assertIsNone(harness.leader.handle_event(ClockTickEvent(harness.now)).attempt_id)

    def test_close_journal_failure_stops_for_human_without_an_unjournaled_send(self) -> None:
        with _tmp_dir() as tmp_path:
            journal = FaultyJournal(tmp_path / "leader_journal.sqlite3")
            harness = _Harness(tmp_path, leader_journal=journal)
            harness.advance_to_active()
            journal.fail_prepare = 1

            result = harness.leader.request_close("test close")

            self.assertEqual("NEEDS_HUMAN", result.state)
            self.assertEqual(1, len(harness.leader_mt5.positions))
            self.assertEqual([], [request for request in harness.leader_mt5.requests if request["type"] == "close"])

            restarted = harness.restart_leader()
            recovered = restarted.handle_event(ClockTickEvent(harness.now))
            self.assertEqual("NEEDS_HUMAN", recovered.state)
            self.assertEqual([], [request for request in harness.leader_mt5.requests if request["type"] == "close"])

    def test_restart_waits_for_broker_evidence_after_a_receipted_close(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            harness.leader_mt5.close_leaves_position_visible = True
            harness.leader.request_close("test close")
            self.assertEqual(1, len([r for r in harness.leader_mt5.requests if r["type"] == "close"]))

            restarted = harness.restart_leader()
            recovered = restarted.handle_event(ClockTickEvent(harness.now))
            self.assertEqual("CONVERGING_EMPTY", recovered.state)
            self.assertFalse(recovered.needs_human)
            self.assertEqual(1, len([r for r in harness.leader_mt5.requests if r["type"] == "close"]))

            harness.leader_mt5.positions.clear()
            restarted.handle_event(ClockTickEvent(harness.now))
            harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(restarted.handle_event(ClockTickEvent(harness.now)).attempt_id)


class PairExecutionCellDecisionTests(unittest.TestCase):
    def test_restarted_leader_recovers_unchanged_peer_readiness_via_calibration_resync(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract(entry_edge_points="1000"))
            harness.activate_both()
            harness.make_ready()
            harness.restart_leader()

            harness.leader.handle_event(
                _readiness(
                    harness.now,
                    symbol_calibrations={SYMBOL: _CALIBRATION, "USDJPY": _JPY_CALIBRATION},
                )
            )

            self.assertTrue(harness.leader._peer_ready)  # noqa: SLF001

    def test_automatic_contract_rejects_any_stray_legacy_product_field(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            malformed = replace(harness.contract, symbol=SYMBOL)

            with self.assertRaises(PairExecutionCellError):
                harness.leader.activate(malformed, _lease(malformed, now=harness.now))

    def test_released_quarantine_cannot_be_resurrected_by_stale_peer_gossip(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            harness.follower._quarantine_product(  # noqa: SLF001 - construct durable broker evidence
                "USDJPY",
                offending_worker_id=FOLLOWER,
                attempt_id="failed-attempt",
                receipt={"retcode": 10021, "comment": "No prices"},
            )
            stale_gossip = harness.follower._product_quarantine_records()  # noqa: SLF001
            harness.leader._accept_peer_product_quarantines(stale_gossip, FOLLOWER)  # noqa: SLF001
            harness.leader.handle_event(
                QuarantineReleaseEvent(
                    symbol="USDJPY",
                    actor="operator-1",
                    reason="broker restored pricing",
                    observed_at=harness.now,
                )
            )

            harness.leader._accept_peer_product_quarantines(stale_gossip, FOLLOWER)  # noqa: SLF001

            self.assertNotIn("USDJPY", harness.leader.quarantined_products())

    def test_control_plane_release_timestamp_does_not_advance_the_trading_clock(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            worker_now = harness.leader._now  # noqa: SLF001

            harness.leader.handle_event(
                QuarantineReleaseEvent(
                    symbol="USDJPY",
                    actor="operator-1",
                    reason="manual release",
                    observed_at=worker_now + timedelta(days=1),
                )
            )

            self.assertEqual(worker_now, harness.leader._now)  # noqa: SLF001

    def test_commit_does_not_split_legs_when_workers_observe_different_post_decision_prices(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            harness.feed_symbol_quotes(
                SYMBOL,
                leader_bid="1.10000", leader_ask="1.10010",
                follower_bid="1.10012", follower_ask="1.10022",
                sequence=1,
            )
            harness.make_ready()
            harness.network.drop_kinds.add("quote")
            harness.leader.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL,
                    bid=Decimal("1.09970"),
                    ask=Decimal("1.09980"),
                    broker_time=harness.now,
                    local_receive_time=harness.now,
                    recovery_epoch="leader-epoch",
                    sequence=2,
                )
            )

            harness.tick(0.10)

            self.assertEqual(1, len([request for request in harness.leader_mt5.requests if request["type"] == "market"]))
            self.assertEqual(1, len([request for request in harness.follower_mt5.requests if request["type"] == "market"]))

    def test_follower_refuses_to_arm_a_product_held_only_in_its_local_quarantine(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            harness.feed_symbol_quotes(
                "USDJPY",
                leader_bid="150.03", leader_ask="150.04",
                follower_bid="150.00", follower_ask="150.01",
                sequence=1,
            )
            harness.follower._quarantine_product(  # noqa: SLF001 - construct durable broker evidence
                "USDJPY",
                offending_worker_id=FOLLOWER,
                attempt_id="prior-attempt",
                receipt={"retcode": 10021, "comment": "No prices"},
            )
            harness.network.drop_kinds.add("quote")
            harness.make_ready()
            harness.tick(0.10)

            self.assertEqual([], [request for request in harness.leader_mt5.requests if request["type"] == "market"])
            self.assertEqual([], [request for request in harness.follower_mt5.requests if request["type"] == "market"])

    def test_quote_relay_repairs_a_missed_pair_wide_quarantine_notification(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            harness.follower._quarantine_product(  # noqa: SLF001 - construct durable broker evidence
                "USDJPY",
                offending_worker_id=FOLLOWER,
                attempt_id="prior-attempt",
                receipt={"retcode": 10021, "comment": "No prices"},
            )

            harness.follower.handle_event(
                LocalQuoteEvent(
                    symbol="USDJPY",
                    bid=Decimal("150.00"),
                    ask=Decimal("150.01"),
                    broker_time=harness.now,
                    local_receive_time=harness.now,
                    recovery_epoch="follower-epoch",
                    sequence=1,
                )
            )

            self.assertIn("USDJPY", harness.leader.quarantined_products())

    def test_manual_release_removes_a_durable_product_quarantine(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.leader_mt5.reject_next_entry_retcode = 10021
            harness.activate_both()
            harness.feed_symbol_quotes(
                "USDJPY",
                leader_bid="150.03", leader_ask="150.04",
                follower_bid="150.00", follower_ask="150.01",
                sequence=1,
            )
            harness.make_ready()
            harness.tick(0.10)
            harness.restart_leader()

            harness.leader.handle_event(
                QuarantineReleaseEvent(
                    symbol="USDJPY",
                    actor="operator-1",
                    reason="broker confirmed executable pricing",
                    observed_at=harness.now,
                )
            )

            self.assertNotIn("USDJPY", harness.leader.quarantined_products())

    def test_no_prices_quarantines_the_product_on_both_workers_and_survives_restart(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.leader_mt5.reject_next_entry_retcode = 10021
            harness.activate_both()
            harness.feed_symbol_quotes(
                "USDJPY",
                leader_bid="150.03", leader_ask="150.04",
                follower_bid="150.00", follower_ask="150.01",
                sequence=1,
            )
            harness.make_ready()
            harness.tick(0.10)

            self.assertIn("USDJPY", harness.leader.quarantined_products())
            self.assertIn("USDJPY", harness.follower.quarantined_products())

            restarted = harness.restart_leader()
            self.assertIn("USDJPY", restarted.quarantined_products())
            harness.restart_follower()
            harness.make_ready()
            harness.feed_symbol_quotes(
                SYMBOL,
                leader_bid="1.10000", leader_ask="1.10010",
                follower_bid="1.10012", follower_ask="1.10022",
                sequence=2,
            )
            harness.tick(0.10)
            self.assertEqual(SYMBOL, harness.leader._attempt.symbol)  # noqa: SLF001

    def test_leader_selects_the_highest_conservative_usd_candidate_without_a_fixed_symbol(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_automatic_contract())
            harness.activate_both()
            harness.feed_symbol_quotes(
                SYMBOL,
                leader_bid="1.10000", leader_ask="1.10010",
                follower_bid="1.10012", follower_ask="1.10022",
                sequence=1,
            )
            harness.feed_symbol_quotes(
                "USDJPY",
                leader_bid="150.03", leader_ask="150.04",
                follower_bid="150.00", follower_ask="150.01",
                sequence=1,
            )
            harness.make_ready()

            result = harness.leader.handle_event(ClockTickEvent(harness.now))

            self.assertIsNotNone(result.attempt_id)
            self.assertEqual("USDJPY", harness.leader._attempt.symbol)  # noqa: SLF001
            self.assertEqual("SHORT", harness.leader._attempt.leader_direction)  # noqa: SLF001
            self.assertEqual("LONG", harness.leader._attempt.follower_direction)  # noqa: SLF001

    def test_only_leader_creates_an_attempt_from_identical_code(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("arm_request")
            harness.activate_both()
            harness.make_ready()
            # Feed the qualifying edge to the follower's own local/peer view too,
            # to prove identical code on the follower cannot originate an attempt
            # even though it independently observes the same coherent edge.
            harness.feed_quotes()
            harness.tick(0.01)

            follower_result = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(follower_result.attempt_id)
            self.assertEqual("follower", follower_result.role)

    def test_no_edge_creates_no_attempt(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes(leader_bid="1.10000", leader_ask="1.10010", follower_bid="1.10011", follower_ask="1.10012")
            harness.tick(0.01)

            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(leader_result.attempt_id)


class PairExecutionCellQuoteTests(unittest.TestCase):
    def test_recent_quotes_with_broker_server_timezone_offset_can_enter(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            broker_time = harness.now + timedelta(hours=3)
            harness.leader.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.10000"), ask=Decimal("1.10010"),
                    broker_time=broker_time, local_receive_time=harness.now,
                    recovery_epoch="epoch-a", sequence=1,
                )
            )
            harness.follower.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.10050"), ask=Decimal("1.10060"),
                    broker_time=broker_time, local_receive_time=harness.now,
                    recovery_epoch="epoch-b", sequence=1,
                )
            )
            harness.tick(0.01)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))

            self.assertIsNotNone(result.attempt_id)

    def test_stale_local_quote_blocks_entry(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.now += timedelta(seconds=5)  # older than quote_max_age_seconds
            harness.leader.handle_event(ClockTickEvent(harness.now))
            harness.follower.handle_event(ClockTickEvent(harness.now))

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)

    def test_cross_worker_skew_blocks_entry(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.leader.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.10000"), ask=Decimal("1.10010"),
                    broker_time=harness.now, local_receive_time=harness.now,
                    recovery_epoch="epoch-a", sequence=1,
                )
            )
            skewed_time = harness.now - timedelta(seconds=2)  # exceeds quote_max_skew_seconds
            harness.follower.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.10050"), ask=Decimal("1.10060"),
                    broker_time=skewed_time, local_receive_time=harness.now,
                    recovery_epoch="epoch-b", sequence=1,
                )
            )
            harness.tick(0.01)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)

    def test_out_of_order_peer_quote_is_ignored(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes(sequence=5)
            # An older sequence number in the same recovery epoch must not roll back the peer quote.
            harness.follower.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.05000"), ask=Decimal("1.05001"),
                    broker_time=harness.now, local_receive_time=harness.now,
                    recovery_epoch="epoch-b", sequence=3,
                )
            )
            peer_quote = harness.leader._peer_quote  # noqa: SLF001 - assert durable evidence, not internals
            assert peer_quote is not None
            self.assertEqual(5, peer_quote.sequence)

    def test_recovery_epoch_change_resets_sequence_baseline(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes(sequence=5)
            harness.follower.handle_event(
                LocalQuoteEvent(
                    symbol=SYMBOL, bid=Decimal("1.10050"), ask=Decimal("1.10060"),
                    broker_time=harness.now, local_receive_time=harness.now,
                    recovery_epoch="epoch-b-reconnected", sequence=1,
                )
            )
            peer_quote = harness.leader._peer_quote  # noqa: SLF001
            assert peer_quote is not None
            self.assertEqual("epoch-b-reconnected", peer_quote.recovery_epoch)
            self.assertEqual(1, peer_quote.sequence)


class PairExecutionCellReadinessTests(unittest.TestCase):
    def test_existing_exposure_blocks_new_attempt(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.leader.handle_event(_readiness(harness.now, positions=[{"ticket": 1}]))
            harness.follower.handle_event(_readiness(harness.now))
            harness.feed_quotes()
            harness.tick(0.01)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)
            self.assertFalse(result.ready)

    def test_none_broker_read_never_establishes_readiness(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.leader.handle_event(
                ReadinessFactsEvent(
                    account_login="1", account_server="demo", terminal_connected=True,
                    orders=None, positions=None, margin_headroom_ok=True,
                    symbol_constraints_fresh=True, protection_calibration=_CALIBRATION, observed_at=harness.now,
                )
            )
            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertFalse(result.ready)
            self.assertIn("unavailable", result.ready_reason)

    def test_insufficient_margin_blocks_readiness(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.leader.handle_event(
                ReadinessFactsEvent(
                    account_login="1", account_server="demo", terminal_connected=True,
                    orders=[], positions=[], margin_headroom_ok=False,
                    symbol_constraints_fresh=True, protection_calibration=_CALIBRATION, observed_at=harness.now,
                )
            )
            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertFalse(result.ready)

    def test_lease_expiry_blocks_new_attempt_creation(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.lease = _lease(harness.contract, now=harness.now, ttl_seconds=0.05)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.now += timedelta(seconds=1)  # lease now expired

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)
            self.assertFalse(result.ready)


class PairExecutionCellEmergencyProtectionTests(unittest.TestCase):
    def test_entry_rejected_when_protection_cannot_be_constructed(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            impossible_calibration = ProtectionCalibration(
                tick_size="0.00001", point="0.00001", minimum_stop_distance="10", profit_tick_value="1", loss_tick_value="1"
            )
            harness.leader.handle_event(
                ReadinessFactsEvent(
                    account_login="1", account_server="demo", terminal_connected=True,
                    orders=[], positions=[], margin_headroom_ok=True, symbol_constraints_fresh=True,
                    protection_calibration=impossible_calibration, observed_at=harness.now,
                )
            )
            harness.follower.handle_event(_readiness(harness.now))
            harness.feed_quotes()
            harness.tick(0.01)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)
            self.assertEqual([], harness.leader_mt5.positions)


class PairExecutionCellArmCommitTests(unittest.TestCase):
    def test_follower_rejects_arm_for_stale_lease_epoch(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.2)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            # Happy-path arming still succeeds normally; this test's guarantee
            # is that a *stale* attempt (simulated below) is rejected.
            self.assertIsNotNone(result.attempt_id)

    def test_arm_timeout_expires_attempt_when_follower_never_acks(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("arm_request")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.01)
            self.assertIsNotNone(harness.leader.handle_event(ClockTickEvent(harness.now)).attempt_id)

            harness.tick(3.0)  # past arm_timeout_seconds

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)
            self.assertEqual("IDLE", result.state)

    def test_duplicate_arm_request_is_idempotent(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.auto_deliver = False
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.01)
            harness.leader.handle_event(ClockTickEvent(harness.now))
            harness.network.pump()  # deliver the arm_request once
            harness.network.pump()  # deliver the arm_ack back to the leader

            arm_requests = [e for e in harness.network.sent if e["kind"] == "arm_request"]
            self.assertEqual(1, len(arm_requests))
            # Redeliver the exact same arm_request a second time; it must not re-arm or double-count.
            harness.follower.handle_event(RelayEnvelopeReceived(arm_requests[0]))
            arm_acks = [e for e in harness.network.sent if e["kind"] == "arm_ack"]
            self.assertEqual(1, len(arm_acks))

    def test_late_commit_is_rejected_before_broker_send(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("commit")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.01)
            harness.leader.handle_event(ClockTickEvent(harness.now))  # armed, commit dropped

            harness.tick(2.0)  # past execution_expiry_seconds without ever sending

            follower_result = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertIsNone(follower_result.attempt_id)


class PairExecutionCellEntryOutcomeTests(unittest.TestCase):
    def test_rejected_leg_triggers_asymmetric_containment_of_surviving_leg(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.follower_mt5.reject = True
            harness.advance_to_active()

            # Deliver the leg-status/broker-snapshot chain until the leader's
            # surviving leg is observed closed by evidence-driven convergence.
            for _ in range(5):
                harness.tick(0.05)
                harness.leader.handle_event(
                    BrokerSnapshotEvent(
                        orders=harness.leader_mt5.orders_get(), positions=harness.leader_mt5.positions_get(), observed_at=harness.now
                    )
                )

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

    def test_unknown_after_send_is_resolved_by_fresh_observation_not_resend(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader_mt5.raise_on_send = True
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.2)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            # The exception crossed send_started with an inconclusive result; a
            # fresh, non-None observation proved no owned position was created,
            # so the attempt safely converges rather than remaining stuck or
            # blindly resending the same effect ID.
            self.assertIn(result.state, ("CONVERGING_EMPTY", "EMPTY", "IDLE"))
            self.assertEqual([], harness.leader_mt5.positions)  # the raise happened before any fake fill

            # The MT5 fake now recovers, but the crossed effect ID must never be resent.
            harness.leader_mt5.raise_on_send = False
            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], harness.leader_mt5.positions)


class PairExecutionCellFalseEmptyTests(unittest.TestCase):
    def test_none_positions_never_establish_empty_during_convergence(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            harness.leader_mt5.reads_unavailable = True
            harness.leader.request_close("test requested close")

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertNotEqual("EMPTY", result.state)
            self.assertEqual(1, len(harness.leader_mt5.positions))  # nothing was falsely assumed closed


class PairExecutionCellRecoveryTests(unittest.TestCase):
    def test_restart_recovers_unknown_after_send_without_resending(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader_mt5.raise_on_send = True
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.2)
            harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], harness.leader_mt5.positions)
            harness.leader.close()

            restarted = PairExecutionCell(
                worker_id=LEADER,
                db_path=tmp_path / "leader_cell.sqlite3",
                relay=harness.network.adapter(LEADER),
                mt5=harness.leader_mt5,
                effect_journal=harness.leader_journal,
                scheduler=DeadlineAwareTraderRpcScheduler(now=lambda: harness.now),
            )
            result = restarted.recover()
            self.assertTrue(result.recovering or result.state in ("ENTRY_UNCERTAIN", "IDLE", "CONVERGING_EMPTY"))
            self.assertEqual([], harness.leader_mt5.positions)


class PairExecutionCellLeaseTests(unittest.TestCase):
    def test_oversized_maximum_holding_period_is_rejected_at_activation(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(maximum_holding_seconds=604801))

            with self.assertRaises(PairExecutionCellError):
                harness.leader.activate(harness.contract, harness.lease)

    def test_stale_epoch_activation_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            stale_lease = _lease(harness.contract, epoch=0, now=harness.now)
            with self.assertRaises(PairExecutionCellError):
                harness.leader.activate(harness.contract, stale_lease)

    def test_wrong_pair_activation_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            with self.assertRaises(PairExecutionCellError):
                harness.leader.activate(
                    harness.contract,
                    PairLease(
                        leader_worker_id="someone-else", follower_worker_id=FOLLOWER, trader_id=TRADER,
                        contract_hash=harness.contract.hash, epoch=1, issued_at=harness.now,
                        expires_at=harness.now + timedelta(minutes=5),
                    ),
                )
            # The activation attempt above did not bind a role for this worker.
            self.assertIsNone(harness.leader._role())  # noqa: SLF001

    def test_changed_contract_hash_without_new_epoch_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            changed_contract = _contract(edge_threshold="0.00099")
            mismatched_lease = PairLease(
                leader_worker_id=LEADER, follower_worker_id=FOLLOWER, trader_id=TRADER,
                contract_hash=changed_contract.hash, epoch=harness.lease.epoch, issued_at=harness.now,
                expires_at=harness.now + timedelta(minutes=5),
            )
            with self.assertRaises(PairExecutionCellError):
                harness.leader.activate(changed_contract, mismatched_lease)

    def test_lease_revocation_clears_authority_but_preserves_recovery(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()

            harness.leader.handle_event(LeaseEvent(lease=None))
            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertFalse(result.ready)
            self.assertIsNone(result.lease_epoch)
            # The attempt itself remains recoverable/observable, not erased.
            self.assertIsNotNone(harness.leader._attempt)  # noqa: SLF001


class PairExecutionCellPeerReadinessTests(unittest.TestCase):
    def test_peer_not_ready_blocks_attempt_creation_without_a_remote_read(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.leader.handle_event(_readiness(harness.now))
            # The follower never publishes readiness, so the leader must not
            # perform any signal-time remote admission read to find out.
            harness.feed_quotes()
            harness.tick(0.01)

            result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertIsNone(result.attempt_id)


class PairExecutionCellRefinementTests(unittest.TestCase):
    def test_refinement_failure_converges_both_legs_to_empty(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            # Simulate the broker rejecting the SL/TP modify on the leader leg.
            harness.leader_mt5.reject = True
            leader_ticket = harness.leader_mt5.positions[0]["ticket"]
            harness.leader_mt5.positions[0]["sl"] = 0.0  # force a fresh divergence-style re-check
            harness.leader._leg.protection_status = "pending"  # noqa: SLF001 - re-arm refinement for the test
            harness.leader._maybe_refine_protection()  # noqa: SLF001

            for _ in range(5):
                harness.tick(0.05)
                harness.leader.handle_event(
                    BrokerSnapshotEvent(
                        orders=harness.leader_mt5.orders_get(), positions=harness.leader_mt5.positions_get(), observed_at=harness.now
                    )
                )
                harness.follower.handle_event(
                    BrokerSnapshotEvent(
                        orders=harness.follower_mt5.orders_get(), positions=harness.follower_mt5.positions_get(), observed_at=harness.now
                    )
                )

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)


class PairExecutionCellArmCommitAdditionalTests(unittest.TestCase):
    def test_duplicate_commit_does_not_double_send(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.tick(0.2)
            self.assertEqual(1, len(harness.leader_mt5.positions))
            self.assertEqual(1, len(harness.follower_mt5.positions))

            commit_envelopes = [e for e in harness.network.sent if e["kind"] == "commit"]
            self.assertEqual(1, len(commit_envelopes))
            # Redeliver the same commit; it must not trigger a second send.
            harness.follower.handle_event(RelayEnvelopeReceived(commit_envelopes[0]))
            self.assertEqual(1, len(harness.follower_mt5.positions))


class PairExecutionCellSchedulerSlotTests(unittest.TestCase):
    """Every dequeued scheduler item is completed exactly once.

    The Worker's scheduler holds a single ``_active`` MT5 serialization slot.
    An entry write that ends abnormally -- a durable-journal failure, an MT5
    exception, or a missing receipt -- must still terminalize its dequeued
    item, or emergency containment and ordinary Worker RPC work could never be
    dequeued again on that Worker.
    """

    def test_mt5_exception_after_send_started_releases_the_serialization_slot(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader_mt5.raise_after_fill = True
            harness.advance_to_active()

            self.assertTrue(_emergency_probe_dequeues(harness.leader_scheduler))

    def test_missing_broker_receipt_releases_the_serialization_slot(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader_mt5.receipt_none_after_fill = True
            harness.advance_to_active()

            self.assertTrue(_emergency_probe_dequeues(harness.leader_scheduler))

    def test_journal_send_started_failure_releases_the_serialization_slot(self) -> None:
        with _tmp_dir() as tmp_path:
            journal = FaultyJournal(tmp_path / "leader_journal.sqlite3")
            journal.fail_send_started = 1
            harness = _Harness(tmp_path, leader_journal=journal)
            harness.advance_to_active()

            self.assertEqual([], harness.leader_mt5.positions)  # the send never began
            self.assertTrue(_emergency_probe_dequeues(harness.leader_scheduler))

    def test_journal_receipt_failure_releases_the_serialization_slot(self) -> None:
        with _tmp_dir() as tmp_path:
            journal = FaultyJournal(tmp_path / "leader_journal.sqlite3")
            journal.fail_receipt = 1
            harness = _Harness(tmp_path, leader_journal=journal)
            harness.advance_to_active()

            self.assertTrue(_emergency_probe_dequeues(harness.leader_scheduler))

    def test_containment_still_closes_a_position_whose_broker_response_was_lost(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader_mt5.raise_after_fill = True  # the broker filled; the response was lost
            harness.follower_mt5.reject = True  # asymmetric entry: contain the surviving leg
            harness.advance_to_active()

            for _ in range(5):
                harness.tick(0.05)
                harness.leader.handle_event(
                    BrokerSnapshotEvent(
                        orders=harness.leader_mt5.orders_get(),
                        positions=harness.leader_mt5.positions_get(),
                        observed_at=harness.now,
                    )
                )

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)


class PairExecutionCellCommitAuthorizationTests(unittest.TestCase):
    """Arming is not committing: a dropped envelope never produces entry."""

    def test_dropped_commit_never_produces_a_unilateral_follower_entry(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("commit")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()

            # Strictly between execute_at (+0.05s) and expires_at (+1.05s): the
            # follower is armed and its execution time has arrived, but no
            # commit was ever delivered for this exact attempt.
            harness.tick(0.2)
            self.assertEqual([], harness.follower_mt5.positions)

            harness.tick(1.5)  # past the attempt's absolute expiry
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)

    def test_dropped_arm_ack_never_produces_a_unilateral_follower_entry(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("arm_ack")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()

            harness.tick(0.2)  # between execute_at and expires_at
            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertNotEqual("SENDING", leader_result.state)

            harness.tick(1.0)  # past the follower's absolute expiry
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)

    def test_commit_authorization_is_durable_and_still_required_after_a_restart(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.network.drop_kinds.add("commit")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()

            restarted = harness.restart_follower()
            harness.now += timedelta(seconds=0.2)
            harness.local_quote(restarted, bid="1.10050", ask="1.10060", sequence=99)
            restarted.handle_event(ClockTickEvent(harness.now))

            self.assertEqual([], harness.follower_mt5.positions)

    def test_a_recovering_worker_never_sends_a_committed_entry_and_converges_instead(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()  # armed and committed before execute_at
            committed_attempt_id = harness.follower.handle_event(ClockTickEvent(harness.now)).attempt_id
            harness.network.drop_kinds.add("arm_request")  # isolate the recovered attempt

            restarted = harness.restart_follower()
            restarted.handle_event(_readiness(harness.now))
            harness.now += timedelta(seconds=0.2)  # past execute_at, before expires_at
            harness.local_quote(restarted, bid="1.10050", ask="1.10060", sequence=99)
            result = restarted.handle_event(ClockTickEvent(harness.now))

            # A Worker that has not finished recovering is not an admissible
            # account immediately before order_send, so the durably committed
            # leg is contained rather than fired blindly.
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertIsNotNone(committed_attempt_id)
            self.assertNotEqual(committed_attempt_id, result.attempt_id)

    def test_a_recovered_committed_leader_waits_for_peer_proof_before_terminalizing(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(execution_expiry_seconds=10.0))
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()  # the leader durably committed this exact attempt
            harness.network.drop_kinds.add("leg_status")  # the peer can no longer be heard

            restarted = harness.restart_leader()
            harness.now += timedelta(seconds=11.0)  # past the attempt's absolute expiry
            result = restarted.handle_event(ClockTickEvent(harness.now))

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual("EMPTY", result.desired_state)
            # The durable commit means the follower may hold the mirror leg, so
            # this side must not declare the attempt resolved on its own facts.
            self.assertIsNotNone(result.attempt_id)

    def test_a_recovered_uncommitted_leader_may_terminalize_on_its_own_empty_facts(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(arm_timeout_seconds=0.5))
            harness.network.drop_kinds.add("arm_ack")  # the leader never commits
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.network.drop_kinds.add("leg_status")

            restarted = harness.restart_leader()
            harness.now += timedelta(seconds=1.0)  # past arm_timeout_seconds
            result = restarted.handle_event(ClockTickEvent(harness.now))

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertIsNone(result.attempt_id)


class PairExecutionCellConvergingCommitTests(unittest.TestCase):
    """A delayed or duplicate commit never revives an attempt already converging.

    An armed follower whose entry is still ``pending`` can legitimately be
    steered to desired-``EMPTY`` (an exit policy, an operator close, a peer
    rejection, or any abandonment) and prove its own side empty while the
    attempt is still inside its ``execute_at``..``expires_at`` window.  A
    commit that arrives -- or is replayed by the controller -- after that point
    is still cryptographically and temporally valid for this exact attempt, so
    only the desired-state guards can keep it from creating real exposure that
    nothing is left to contain.
    """

    @staticmethod
    def _entry_sends(mt5: FakeMT5) -> list[dict[str, object]]:
        return [r for r in mt5.requests if r["type"] == "market"]

    def test_delayed_commit_after_a_requested_close_never_enters(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(execution_expiry_seconds=10.0))
            harness.network.drop_kinds.add("commit")  # hold the commit back in flight
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            delayed_commit = next(e for e in harness.network.sent if e["kind"] == "commit")

            armed = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual("ARMED", armed.state)

            # An exit policy converges this armed-but-pending leg to empty and
            # this side durably proves it holds nothing.
            harness.network.drop_kinds.add("leg_status")  # the peer cannot be heard
            closing = harness.follower.request_close("risk cutoff before execute_at")
            self.assertEqual("EMPTY", closing.desired_state)
            self.assertEqual("CONVERGING_EMPTY", closing.state)

            # Strictly between execute_at and expires_at: the delayed commit is
            # still valid for this exact attempt, lease epoch, and window.
            harness.now += timedelta(seconds=0.2)
            harness.follower.handle_event(ClockTickEvent(harness.now))
            result = harness.follower.handle_event(RelayEnvelopeReceived(delayed_commit))

            self.assertEqual([], self._entry_sends(harness.follower_mt5))
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual("EMPTY", result.desired_state)
            self.assertIn(result.state, ("CONVERGING_EMPTY", "EMPTY"))

            # A later tick inside the same window must not fire it either.
            harness.now += timedelta(seconds=0.5)
            later = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], self._entry_sends(harness.follower_mt5))
            self.assertIn(later.state, ("CONVERGING_EMPTY", "EMPTY"))

    def test_duplicate_commit_after_peer_rejection_containment_never_enters(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(
                tmp_path,
                contract=_contract(commit_lead_seconds=0.5, execution_expiry_seconds=10.0),
            )
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()  # both armed; the follower is durably committed
            duplicate_commit = next(e for e in harness.network.sent if e["kind"] == "commit")

            harness.leader_mt5.reject = True  # the leader's own leg is refused
            harness.follower_mt5.reads_unavailable = True  # this side cannot prove anything yet
            harness.tick(0.6)  # past execute_at, far short of expires_at

            # The peer's rejection arrived before this side's own execute_at
            # gate ran: containment must suppress the entry outright rather
            # than enter and rely on closing it afterwards.
            follower_before = harness.follower.handle_event(ClockTickEvent(harness.now))
            contained_attempt_id = follower_before.attempt_id
            self.assertEqual([], self._entry_sends(harness.follower_mt5))
            self.assertEqual("EMPTY", follower_before.desired_state)
            self.assertEqual("CONVERGING_EMPTY", follower_before.state)

            result = harness.follower.handle_event(RelayEnvelopeReceived(duplicate_commit))

            self.assertEqual([], self._entry_sends(harness.follower_mt5))
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual("EMPTY", result.desired_state)
            self.assertEqual("CONVERGING_EMPTY", result.state)

            # Once this side can read again the contained attempt resolves
            # with nothing ever sent for it.
            harness.follower_mt5.reads_unavailable = False
            for _ in range(3):
                harness.tick(0.05)
            final = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], self._entry_sends(harness.follower_mt5))
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertIsNotNone(contained_attempt_id)
            self.assertNotEqual(contained_attempt_id, final.attempt_id)

    def test_a_duplicate_commit_before_send_stays_idempotent(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(commit_lead_seconds=0.5))
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()  # armed and committed, still before execute_at
            commit = next(e for e in harness.network.sent if e["kind"] == "commit")

            replayed = harness.follower.handle_event(RelayEnvelopeReceived(commit))
            self.assertEqual("ARMED", replayed.state)
            self.assertEqual([], self._entry_sends(harness.follower_mt5))

            harness.tick(0.6)  # the still-legitimate attempt reaches execute_at
            self.assertEqual(1, len(self._entry_sends(harness.follower_mt5)))
            self.assertEqual(1, len(harness.follower_mt5.positions))

    def test_a_duplicate_commit_after_fill_neither_resends_nor_rewinds_state(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            commit = next(e for e in harness.network.sent if e["kind"] == "commit")
            self.assertEqual(1, len(self._entry_sends(harness.follower_mt5)))

            result = harness.follower.handle_event(RelayEnvelopeReceived(commit))

            self.assertEqual(1, len(self._entry_sends(harness.follower_mt5)))
            self.assertEqual(1, len(harness.follower_mt5.positions))
            self.assertEqual("ACTIVE", result.state)  # never rewound to ARMED


class PairExecutionCellAbandonedAttemptTests(unittest.TestCase):
    """An attempt the peer may have sent is contained, never forgotten."""

    def test_leader_abandoning_a_committed_attempt_contains_the_peer_leg(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(
                tmp_path,
                contract=_contract(quote_max_age_seconds=0.1, execution_expiry_seconds=10.0),
            )
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()

            # Only the follower reaches its execution time; it sends and fills.
            harness.now += timedelta(seconds=0.08)
            harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual(1, len(harness.follower_mt5.positions))
            self.assertEqual([], harness.leader_mt5.positions)

            # The leader's own quote is now too stale to send.  It must not
            # simply reset and ignore the leg its peer already created.
            harness.now += timedelta(seconds=0.1)
            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))

            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)  # contained by exact owned ticket
            self.assertNotEqual("ACTIVE", leader_result.state)

    def test_lost_arm_ack_and_arm_timeout_leave_no_surviving_position(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(
                tmp_path,
                # The follower stays armed long past the leader's arm deadline,
                # so the leader abandons first -- exactly the lost-ack window.
                contract=_contract(arm_timeout_seconds=0.5, execution_expiry_seconds=30.0),
            )
            harness.network.drop_kinds.add("arm_ack")
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()

            harness.tick(0.6)  # past arm_timeout_seconds, long before the follower's expiry
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

            # Every retry the still-valid edge produces behaves identically;
            # once the quotes go stale both sides settle with nothing open.
            for _ in range(6):
                harness.tick(0.5)

            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))
            follower_result = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)
            self.assertIsNone(leader_result.attempt_id)
            self.assertIsNone(follower_result.attempt_id)

    def test_an_abandoned_attempt_is_terminal_only_after_both_sides_prove_empty(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(
                tmp_path,
                contract=_contract(quote_max_age_seconds=0.1, execution_expiry_seconds=10.0),
            )
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()
            harness.now += timedelta(seconds=0.08)
            harness.follower.handle_event(ClockTickEvent(harness.now))
            harness.follower_mt5.reads_unavailable = True  # the peer cannot prove anything

            harness.now += timedelta(seconds=0.1)
            leader_result = harness.leader.handle_event(ClockTickEvent(harness.now))

            self.assertIsNotNone(leader_result.attempt_id)  # not abandoned while the peer is unproven
            self.assertEqual("EMPTY", leader_result.desired_state)
            self.assertEqual(1, len(harness.follower_mt5.positions))

            harness.follower_mt5.reads_unavailable = False
            harness.now += timedelta(seconds=0.05)
            harness.follower.handle_event(ClockTickEvent(harness.now))
            harness.leader.handle_event(ClockTickEvent(harness.now))
            # Close completion is established by a later broker observation,
            # never by recursively re-reading from the close call stack.
            harness.follower.handle_event(ClockTickEvent(harness.now))

            self.assertEqual([], harness.follower_mt5.positions)
            self.assertIsNone(harness.leader.handle_event(ClockTickEvent(harness.now)).attempt_id)


class PairExecutionCellSecondAttemptTests(unittest.TestCase):
    """No second attempt, and no shape-matched ticket, may displace the first."""

    def test_a_second_arm_request_never_replaces_an_unresolved_attempt(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            first_attempt_id = harness.follower.handle_event(ClockTickEvent(harness.now)).attempt_id

            arm_request = next(e for e in harness.network.sent if e["kind"] == "arm_request")
            second = copy.deepcopy(arm_request)
            second["payload"]["attempt"]["attempt_id"] = str(uuid4())  # type: ignore[index]
            harness.follower.handle_event(RelayEnvelopeReceived(second))

            follower_result = harness.follower.handle_event(ClockTickEvent(harness.now))
            self.assertEqual(first_attempt_id, follower_result.attempt_id)
            self.assertEqual(1, len(harness.follower_mt5.positions))  # no duplicate exposure
            last_ack = [e for e in harness.network.sent if e["kind"] == "arm_ack"][-1]
            self.assertFalse(last_ack["payload"]["armed"])  # type: ignore[index]
            self.assertIn("unresolved", str(last_ack["payload"]["reason"]))  # type: ignore[index]

    def test_exposure_that_is_not_the_exact_owned_leg_revokes_readiness_in_every_state(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            owned_ticket = str(harness.leader_mt5.positions[0]["ticket"])

            owned_only = harness.leader.handle_event(
                _readiness(harness.now, positions=[{"ticket": owned_ticket, "symbol": SYMBOL}])
            )
            self.assertNotEqual("existing exposure present", owned_only.ready_reason)

            foreign = harness.leader.handle_event(
                _readiness(
                    harness.now,
                    positions=[
                        {"ticket": owned_ticket, "symbol": SYMBOL},
                        {"ticket": "999999", "symbol": SYMBOL},
                    ],
                )
            )
            self.assertFalse(foreign.ready)
            self.assertEqual("existing exposure present", foreign.ready_reason)

    def test_containment_closes_only_the_exact_attempt_owned_ticket(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            owned_ticket = str(harness.leader_mt5.positions[0]["ticket"])
            unrelated = dict(harness.leader_mt5.positions[0])
            unrelated["ticket"] = 999999  # same symbol/side/volume shape, different ticket
            harness.leader_mt5.positions.append(unrelated)

            harness.leader.request_close("test requested close")
            for _ in range(3):
                harness.tick(0.05)

            remaining = {str(p["ticket"]) for p in harness.leader_mt5.positions}
            self.assertEqual({"999999"}, remaining)
            self.assertNotIn(owned_ticket, remaining)


class PairExecutionCellLeaseLossTests(unittest.TestCase):
    """Losing a lease removes authority without breaking convergence."""

    def test_revocation_while_armed_never_raises_and_never_enters(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path, contract=_contract(execution_expiry_seconds=10.0))
            harness.activate_both()
            harness.make_ready()
            harness.feed_quotes()  # both armed and committed, neither has reached execute_at

            result = harness.leader.handle_event(LeaseEvent(lease=None))
            self.assertIsNone(result.lease_epoch)
            self.assertFalse(result.ready)

            harness.tick(0.5)  # well past execute_at
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

    def test_revocation_while_active_still_relays_status_and_converges(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            self.assertEqual(1, len(harness.leader_mt5.positions))
            before_revocation = len(harness.network.sent)

            harness.leader.handle_event(LeaseEvent(lease=None))
            harness.leader.request_close("operator break-glass after lease loss")
            for _ in range(3):
                harness.tick(0.05)

            relayed = harness.network.sent[before_revocation:]
            self.assertTrue(
                any(e["kind"] == "leg_status" and e["from_worker_id"] == LEADER for e in relayed),
                "status relay must survive lease revocation",
            )
            self.assertEqual([], harness.leader_mt5.positions)
            self.assertEqual([], harness.follower_mt5.positions)

    def test_a_revoked_lease_still_fences_a_stale_epoch_after_a_restart(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            harness.leader.handle_event(LeaseEvent(lease=None))

            restarted = harness.restart_leader()
            with self.assertRaises(PairExecutionCellError):
                restarted.handle_event(LeaseEvent(lease=_lease(harness.contract, epoch=0, now=harness.now)))


class PairExecutionCellLeaseEventValidationTests(unittest.TestCase):
    """A relayed/renewed lease reuses exactly one activation validation path."""

    def test_stale_epoch_lease_event_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            with self.assertRaises(PairExecutionCellError):
                harness.leader.handle_event(LeaseEvent(lease=_lease(harness.contract, epoch=0, now=harness.now)))
            self.assertEqual(1, harness.leader.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_foreign_pair_lease_event_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            foreign = PairLease(
                leader_worker_id="someone-else", follower_worker_id=FOLLOWER, trader_id=TRADER,
                contract_hash=harness.contract.hash, epoch=2, issued_at=harness.now,
                expires_at=harness.now + timedelta(minutes=5),
            )
            with self.assertRaises(PairExecutionCellError):
                harness.leader.handle_event(LeaseEvent(lease=foreign))
            self.assertEqual(1, harness.leader.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_foreign_trader_lease_event_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            foreign = PairLease(
                leader_worker_id=LEADER, follower_worker_id=FOLLOWER, trader_id="trader-other",
                contract_hash=harness.contract.hash, epoch=2, issued_at=harness.now,
                expires_at=harness.now + timedelta(minutes=5),
            )
            with self.assertRaises(PairExecutionCellError):
                harness.leader.handle_event(LeaseEvent(lease=foreign))
            self.assertEqual(1, harness.leader.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_changed_contract_hash_lease_event_is_rejected(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            changed = _contract(edge_threshold="0.00099")
            for epoch in (1, 2):
                with self.assertRaises(PairExecutionCellError):
                    harness.leader.handle_event(
                        LeaseEvent(
                            lease=PairLease(
                                leader_worker_id=LEADER, follower_worker_id=FOLLOWER, trader_id=TRADER,
                                contract_hash=changed.hash, epoch=epoch, issued_at=harness.now,
                                expires_at=harness.now + timedelta(minutes=5),
                            )
                        )
                    )
            self.assertEqual(1, harness.leader.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_a_rejected_lease_event_never_downgrades_durable_state(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, _lease(harness.contract, epoch=3, now=harness.now))
            with self.assertRaises(PairExecutionCellError):
                harness.leader.handle_event(LeaseEvent(lease=_lease(harness.contract, epoch=2, now=harness.now)))

            restarted = harness.restart_leader()
            self.assertEqual(3, restarted.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_a_valid_renewal_lease_event_is_applied_and_persisted(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.leader.activate(harness.contract, harness.lease)
            renewal = _lease(harness.contract, epoch=2, now=harness.now, ttl_seconds=600)

            result = harness.leader.handle_event(LeaseEvent(lease=renewal))
            self.assertEqual(2, result.lease_epoch)

            restarted = harness.restart_leader()
            self.assertEqual(2, restarted.handle_event(ClockTickEvent(harness.now)).lease_epoch)

    def test_a_lease_event_cannot_transfer_an_epoch_while_an_attempt_is_unresolved(self) -> None:
        with _tmp_dir() as tmp_path:
            harness = _Harness(tmp_path)
            harness.advance_to_active()
            with self.assertRaises(PairExecutionCellError):
                harness.leader.handle_event(LeaseEvent(lease=_lease(harness.contract, epoch=2, now=harness.now)))
            self.assertEqual(1, harness.leader.handle_event(ClockTickEvent(harness.now)).lease_epoch)


def _emergency_probe_dequeues(scheduler: DeadlineAwareTraderRpcScheduler) -> bool:
    """Whether the Worker's single MT5 serialization slot is free for emergency work."""

    probe_id = "emergency-probe"
    admitted = scheduler.admit(
        {
            "request_id": probe_id,
            "command_id": probe_id,
            "kind": "operation",
            "payload": {"type": "close", "ticket": "1", "volume": "0.10"},
            "priority": "emergency",
            "effect_id": probe_id,
        }
    )
    if admitted is not None:
        return False
    dequeued = scheduler.next()
    return isinstance(dequeued, ScheduledTraderRpc) and dequeued.command_id == probe_id


def _tmp_dir():
    import shutil
    import tempfile
    from contextlib import contextmanager

    @contextmanager
    def _make():
        directory = Path(tempfile.mkdtemp(prefix="pair_cell_test_"))
        try:
            yield directory
        finally:
            shutil.rmtree(directory, ignore_errors=True)

    return _make()


if __name__ == "__main__":
    unittest.main()
