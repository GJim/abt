from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Barrier
import unittest

from abt.trader.runtime import (
    PairLegMarket,
    PairLegPlan,
    PairMarketObservation,
    PairPlan,
    StrategyRuntime,
    StrategyRuntimeError,
    TimedExitPolicy,
)


class FatalDispatch(BaseException):
    pass


class ScriptedRelay:
    def __init__(self) -> None:
        self.orders = {"worker-a": [], "worker-b": []}
        self.positions = {"worker-a": [], "worker-b": []}
        self.effects: list[tuple[str, str, str | None]] = []
        self.events: list[str] = []
        self.reject_worker: str | None = None
        self.lose_receipt_worker: str | None = None
        self.next_ticket = 100
        self.entry_order_ticket: dict[str, int] = {}
        self.recovery_epoch = {"worker-a": "epoch-a", "worker-b": "epoch-b"}
        self.cursor = {"worker-a": 0, "worker-b": 0}
        self.preserve_close = False
        self.raise_after_send_worker: str | None = None
        self.reject_modify_worker: str | None = None
        self.raise_after_modify_worker: str | None = None
        self.raise_without_modify_worker: str | None = None
        self.fatal_without_modify_worker: str | None = None
        self.exit_during_modify_worker: str | None = None
        self.partial_during_modify_worker: str | None = None
        self.stops_level_points = {"worker-a": 10, "worker-b": 10}
        self.symbol_digits = 5
        self.symbol_point = 0.00001
        self.symbol_tick_size = 0.00001
        self.fail_timed_exit_snapshot = False
        self.close_barrier: Barrier | None = None
        self.corrupt_entry_worker: str | None = None
        self.reappear_on_orphan_close: tuple[str, dict[str, object]] | None = None

    def snapshot(
        self,
        worker_id: str,
        *,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        if (
            self.reappear_on_orphan_close is not None
            and "orphan-close-observe" in runtime_command_id
        ):
            reappear_worker, reappear_position = self.reappear_on_orphan_close
            self.positions[reappear_worker] = [dict(reappear_position)]
            self.reappear_on_orphan_close = None
        if self.fail_timed_exit_snapshot and "timed-exit-observe" in runtime_command_id:
            raise ConnectionError("Worker snapshot unavailable")
        return {
            "orders": [dict(value) for value in self.orders[worker_id]],
            "positions": [dict(value) for value in self.positions[worker_id]],
            "recovery_epoch": self.recovery_epoch[worker_id],
            "snapshot_baseline": self.cursor[worker_id],
        }

    def order_check(
        self,
        worker_id: str,
        *,
        order: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(worker_id, kind="order_check", request=order)

    def symbol_info(
        self,
        worker_id: str,
        *,
        symbol: str,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return {
            "symbol": {
                "name": symbol,
                "digits": self.symbol_digits,
                "point": self.symbol_point,
                "trade_tick_size": self.symbol_tick_size,
                "trade_stops_level": self.stops_level_points[worker_id],
                "trade_freeze_level": 0,
            }
        }

    def calc_profit(
        self,
        worker_id: str,
        *,
        symbol: str,
        volume: str,
        direction: str,
        open_price: float,
        close_price: float,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        self.events.append("calc_profit")
        profit = (
            (close_price - open_price) * 10_000
            if direction == "LONG"
            else (open_price - close_price) * 10_000
        )
        return {"profit": profit}

    def order_execute(
        self,
        worker_id: str,
        *,
        order: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        effect_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(worker_id, kind="order_execute", request=order, effect_id=effect_id)

    def operate(
        self,
        worker_id: str,
        *,
        operation: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        effect_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]:
        return self.request(worker_id, kind="operation", request=operation, effect_id=effect_id)

    def request(
        self,
        worker_id: str,
        *,
        kind: str,
        request: dict[str, object],
        runtime_command_id: str | None = None,
        request_id: str | None = None,
        effect_id: str | None = None,
        expires_at: datetime | None = None,
        correlation: dict[str, object] | None = None,
    ) -> dict[str, object]:
        if kind == "order_check":
            return {
                "accepted": worker_id != self.reject_worker,
                "diagnostics": {"margin": 10.0},
            }
        if kind == "order_execute":
            self.events.append("entry")
            self.effects.append((worker_id, kind, effect_id))
            if worker_id == self.reject_worker:
                return {"accepted": False, "execution_state": "not_started", "result": {}}
            self.next_ticket += 1
            ticket = self.next_ticket
            self.entry_order_ticket[worker_id] = ticket + 1000
            self.positions[worker_id] = [
                {
                    "ticket": ticket,
                    "symbol": request["symbol"],
                    "type": 0 if request["direction"] == "LONG" else 1,
                    "volume": float(str(request["volume"])),
                    "sl": float(str(request["sl"])),
                    "tp": float(str(request["tp"])),
                    "price_open": 1.1,
                }
            ]
            if worker_id == self.corrupt_entry_worker:
                self.positions[worker_id][0]["sl"] = float(str(request["sl"])) + 0.001
            if worker_id == self.raise_after_send_worker:
                raise ConnectionError("relay disconnected after Worker send_started")
            if worker_id == self.lose_receipt_worker:
                return {
                    "accepted": False,
                    "execution_state": "sent",
                    "outcome": "unknown_after_send",
                    "result": {},
                }
            return {
                "accepted": True,
                "execution_state": "receipt",
                "result": {"position": ticket, "order": ticket + 1000, "price": 1.1},
            }
        if kind == "operation":
            self.events.append(str(request["type"]))
            self.effects.append((worker_id, str(request["type"]), effect_id))
            if request["type"] == "cancel":
                if not self.preserve_close:
                    self.orders[worker_id] = []
            elif request["type"] == "close":
                if self.close_barrier is not None:
                    self.close_barrier.wait(timeout=1)
                if not self.preserve_close:
                    self.positions[worker_id] = []
            elif request["type"] == "modify_sl_tp":
                if worker_id == self.reject_modify_worker:
                    return {
                        "accepted": False,
                        "execution_state": "not_started",
                        "result": {},
                    }
                if worker_id == self.raise_without_modify_worker:
                    raise ConnectionError("relay disconnected with unknown protection outcome")
                if worker_id == self.fatal_without_modify_worker:
                    raise FatalDispatch
                position = next(
                    value
                    for value in self.positions[worker_id]
                    if value["ticket"] == int(str(request["position"]))
                )
                position["sl"] = float(str(request["sl"]))
                position["tp"] = float(str(request["tp"]))
                if worker_id == self.partial_during_modify_worker:
                    position["volume"] = 0.05
                if worker_id == self.exit_during_modify_worker:
                    self.positions[worker_id] = []
                if worker_id == self.raise_after_modify_worker:
                    raise ConnectionError("relay disconnected after protection send_started")
            return {"operation": request["type"], "result": {}}
        raise AssertionError((worker_id, kind, request))


def pair_plan(policy: TimedExitPolicy | None = None) -> PairPlan:
    return PairPlan(
        pair_id="pair-1",
        command_id="command-1",
        direction="long_first_short_second",
        expires_at=datetime.now(UTC) + timedelta(seconds=30),
        first=PairLegPlan(
            "first", "worker-a", "EURUSD", "LONG", "0.10", "FOK", "100.00", "1.09", "1.12"
        ),
        second=PairLegPlan(
            "second", "worker-b", "EURUSD", "SHORT", "0.10", "FOK", "100.00", "1.12", "1.09"
        ),
        timed_exit=policy,
    )


def market_observation(now: datetime) -> PairMarketObservation:
    return PairMarketObservation(
        first=PairLegMarket(
            worker_id="worker-a",
            observed_at=now,
            bid=1.10000,
            ask=1.10010,
            recent_midpoint_moves=(0.00010, 0.00020, 0.00010),
        ),
        second=PairLegMarket(
            worker_id="worker-b",
            observed_at=now,
            bid=1.10020,
            ask=1.10030,
            recent_midpoint_moves=(0.00010, 0.00020, 0.00010),
        ),
    )


class StrategyRuntimeTests(unittest.TestCase):
    runtime_path = Path("test-results\\strategy-runtime-test.sqlite3")

    def setUp(self) -> None:
        self._clean_runtime_files()
        self.addCleanup(self._clean_runtime_files)

    def _clean_runtime_files(self) -> None:
        for path in (
            self.runtime_path,
            self.runtime_path.with_name(f"{self.runtime_path.name}-wal"),
            self.runtime_path.with_name(f"{self.runtime_path.name}-shm"),
        ):
            path.unlink(missing_ok=True)

    def test_entry_is_active_only_after_fresh_broker_fact_verification(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)

        result = runtime.enter(pair_plan())

        self.assertEqual(("active", "ACTIVE"), (result.status, result.state))
        self.assertEqual(2, len(relay.effects))
        self.assertEqual({"command-1:worker-a:entry", "command-1:worker-b:entry"}, {
            effect_id for _, _, effect_id in relay.effects
        })

    def test_entry_refines_fast_protection_after_broker_verified_entry(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        plan = replace(
            pair_plan(),
            first=replace(pair_plan().first, protection_loss_usd="40.00"),
            second=replace(pair_plan().second, protection_loss_usd="40.00"),
        )

        result = runtime.enter(plan)

        self.assertEqual(("active", "ACTIVE"), (result.status, result.state))
        self.assertEqual(["entry", "entry"], relay.events[:2])
        self.assertEqual(128, relay.events.count("calc_profit"))
        self.assertEqual(2, relay.events.count("modify_sl_tp"))
        active_plan = runtime.active_plan
        assert active_plan is not None
        self.assertEqual(1, active_plan.protection_revision)
        self.assertEqual(("1.09601", "1.10399"), (
            active_plan.first.stop_loss, active_plan.first.take_profit,
        ))
        self.assertEqual(("1.10399", "1.09601"), (
            active_plan.second.stop_loss, active_plan.second.take_profit,
        ))
        self.assertTrue(all(
            effect_id is not None and "entry-protection-refinement" in effect_id
            for _worker_id, kind, effect_id in relay.effects
            if kind == "modify_sl_tp"
        ))

    def test_maximum_holding_age_arms_a_verified_synchronized_exit_corridor(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None

        before_due = runtime.maintain(
            entered.activated_at + timedelta(seconds=59),
            market_observation(entered.activated_at + timedelta(seconds=59)),
        )
        at_due = runtime.maintain(
            entered.activated_at + timedelta(seconds=60),
            market_observation(entered.activated_at + timedelta(seconds=60)),
        )

        self.assertEqual(("active", "ACTIVE"), (before_due.status, before_due.state))
        self.assertEqual(("exiting", "TIMED_EXIT_ARMED"), (at_due.status, at_due.state))
        self.assertEqual("EMPTY", runtime.desired_state)
        self.assertEqual(
            {"modify_sl_tp"},
            {kind for _, kind, _ in relay.effects if kind == "modify_sl_tp"},
        )

    def test_timed_exit_is_disabled_when_the_pair_has_no_policy(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan())
        assert entered.activated_at is not None

        result = runtime.maintain(
            entered.activated_at + timedelta(days=1),
            market_observation(entered.activated_at + timedelta(days=1)),
        )

        self.assertEqual(("active", "ACTIVE"), (result.status, result.state))
        self.assertFalse(any(kind == "modify_sl_tp" for _, kind, _ in relay.effects))

    def test_stale_market_evidence_falls_back_to_owned_pair_close(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(
            due,
            market_observation(due - timedelta(seconds=3)),
        )

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertFalse(any(kind == "modify_sl_tp" for _, kind, _ in relay.effects))

    def test_overdue_pair_persists_empty_intent_before_broker_snapshot(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        relay.fail_timed_exit_snapshot = True

        with self.assertRaises(ConnectionError):
            runtime.maintain(
                entered.activated_at + timedelta(seconds=60),
                market_observation(entered.activated_at + timedelta(seconds=60)),
            )

        self.assertEqual("EMPTY", runtime.desired_state)
        self.assertEqual("TIMED_EXIT_ARMING", runtime.state)

    def test_timeout_never_overwrites_external_protection_drift(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        relay.positions["worker-a"][0]["tp"] = 1.115
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertFalse(any(kind == "modify_sl_tp" for _, kind, _ in relay.effects))

    def test_corridor_never_widens_existing_loss_protection(self) -> None:
        relay = ScriptedRelay()
        policy = TimedExitPolicy(maximum_holding_seconds=60)
        plan = pair_plan(policy)
        plan = replace(
            plan,
            first=replace(plan.first, stop_loss="1.09990"),
            second=replace(plan.second, stop_loss="1.10040"),
        )
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(plan)
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual("TIMED_EXIT_ARMED", result.state)
        self.assertEqual(1.09990, relay.positions["worker-a"][0]["sl"])
        self.assertEqual(1.10040, relay.positions["worker-b"][0]["sl"])

    def test_corridor_uses_current_worker_stop_constraints(self) -> None:
        relay = ScriptedRelay()
        relay.stops_level_points["worker-a"] = 40
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual("TIMED_EXIT_ARMED", result.state)
        self.assertEqual(1.09958, relay.positions["worker-a"][0]["sl"])
        self.assertEqual(1.10072, relay.positions["worker-b"][0]["sl"])

    def test_one_natural_exit_gets_fifteen_seconds_before_only_the_orphan_is_closed(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions["worker-a"] = []

        grace = runtime.maintain(due + timedelta(seconds=1), None)
        before_deadline = runtime.maintain(due + timedelta(seconds=15), None)
        closed = runtime.maintain(due + timedelta(seconds=16), None)

        self.assertEqual(("exiting", "ORPHAN_GRACE"), (grace.status, grace.state))
        self.assertEqual(("exiting", "ORPHAN_GRACE"), (before_deadline.status, before_deadline.state))
        self.assertEqual(("empty", "EMPTY"), (closed.status, closed.state))
        close_workers = [
            worker_id for worker_id, kind, _ in relay.effects if kind == "close"
        ]
        self.assertEqual(["worker-b"], close_workers)

    def test_orphan_leg_reappearance_requires_human_instead_of_reusing_its_deadline(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        first_position = dict(relay.positions["worker-a"][0])
        relay.positions["worker-a"] = []
        runtime.maintain(due + timedelta(seconds=1), None)
        relay.positions["worker-a"] = [first_position]

        result = runtime.maintain(due + timedelta(seconds=2), None)

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        self.assertFalse(any(kind == "close" for _, kind, _ in relay.effects))

    def test_orphan_reappearance_at_deadline_never_closes_either_leg(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        first_position = dict(relay.positions["worker-a"][0])
        relay.positions["worker-a"] = []
        runtime.maintain(due + timedelta(seconds=1), None)
        relay.reappear_on_orphan_close = ("worker-a", first_position)

        result = runtime.maintain(due + timedelta(seconds=16), None)

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        self.assertFalse(any(kind == "close" for _, kind, _ in relay.effects))

    def test_backward_clock_adjustment_never_extends_orphan_deadline(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions["worker-a"] = []
        runtime.maintain(due + timedelta(seconds=1), None)
        runtime.maintain(due + timedelta(seconds=15), None)

        result = runtime.maintain(due - timedelta(hours=1), None)

        self.assertEqual("ORPHAN_GRACE", result.state)
        at_deadline = runtime.maintain(due + timedelta(seconds=16), None)
        self.assertEqual("EMPTY", at_deadline.state)

    def test_restart_preserves_an_armed_corridor_instead_of_closing_it_early(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        effects_before_restart = list(relay.effects)
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual(("exiting", "TIMED_EXIT_ARMED"), (result.status, result.state))
        assert result.first is not None and result.second is not None
        self.assertEqual({101, 102}, {result.first.ticket, result.second.ticket})
        self.assertEqual(effects_before_restart, relay.effects)
        self.assertEqual(1, len(relay.positions["worker-a"]))
        self.assertEqual(1, len(relay.positions["worker-b"]))

    def test_restart_preserves_the_original_orphan_deadline(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions["worker-a"] = []
        runtime.maintain(due + timedelta(seconds=1), None)
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        recovery = recovered.recover()
        before_deadline = recovered.maintain(
            due + timedelta(seconds=15, milliseconds=999), None
        )
        at_deadline = recovered.maintain(due + timedelta(seconds=16), None)

        self.assertEqual("ORPHAN_GRACE", recovery.state)
        self.assertEqual("ORPHAN_GRACE", before_deadline.state)
        self.assertEqual("EMPTY", at_deadline.state)

    def test_restart_resumes_arming_from_persisted_targets_before_any_send(self) -> None:
        class Crash(BaseException):
            pass

        relay = ScriptedRelay()
        arming_transitions = 0

        def crash_after(state: str) -> None:
            nonlocal arming_transitions
            if state == "TIMED_EXIT_ARMING":
                arming_transitions += 1
            if arming_transitions == 2:
                raise Crash

        runtime = StrategyRuntime(
            self.runtime_path,
            relay,
            worker_ids=("worker-a", "worker-b"),
            after_transition=crash_after,
        )
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        with self.assertRaises(Crash):
            runtime.maintain(due, market_observation(due))
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual(("exiting", "TIMED_EXIT_ARMED"), (result.status, result.state))
        self.assertEqual(
            2,
            len([effect for effect in relay.effects if effect[1] == "modify_sl_tp"]),
        )

    def test_restart_never_resends_an_arming_effect_that_crossed_dispatch(self) -> None:
        relay = ScriptedRelay()
        relay.fatal_without_modify_worker = "worker-a"
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        with self.assertRaises(FatalDispatch):
            runtime.maintain(due, market_observation(due))
        runtime.close()
        relay.fatal_without_modify_worker = None
        effects_before_recovery = [
            effect for effect in relay.effects if effect[1] == "modify_sl_tp"
        ]

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertEqual(
            effects_before_recovery,
            [effect for effect in relay.effects if effect[1] == "modify_sl_tp"],
        )

    def test_armed_corridor_market_closes_both_legs_at_its_deadline(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))

        before_deadline = runtime.maintain(due + timedelta(seconds=29), None)
        closed = runtime.maintain(due + timedelta(seconds=30), None)

        self.assertEqual(("exiting", "TIMED_EXIT_ARMED"), (before_deadline.status, before_deadline.state))
        self.assertEqual(("empty", "EMPTY"), (closed.status, closed.state))
        self.assertEqual(
            {"worker-a", "worker-b"},
            {worker_id for worker_id, kind, _ in relay.effects if kind == "close"},
        )

    def test_armed_protection_drift_immediately_converges_owned_pair_empty(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions["worker-a"][0]["tp"] += 0.00001

        result = runtime.maintain(due + timedelta(seconds=1), None)

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertEqual(
            {"worker-a", "worker-b"},
            {worker_id for worker_id, kind, _ in relay.effects if kind == "close"},
        )

    def test_armed_verification_rejects_a_single_broker_tick_mismatch(self) -> None:
        relay = ScriptedRelay()
        relay.symbol_digits = 8
        relay.symbol_point = 0.00000001
        relay.symbol_tick_size = 0.00000001
        relay.stops_level_points = {"worker-a": 1, "worker-b": 1}
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions["worker-a"][0]["tp"] += 0.00000001

        result = runtime.maintain(due + timedelta(seconds=1), None)

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))

    def test_unknown_arming_receipt_is_resolved_by_fresh_broker_facts(self) -> None:
        relay = ScriptedRelay()
        relay.raise_after_modify_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual(("exiting", "TIMED_EXIT_ARMED"), (result.status, result.state))
        self.assertEqual(
            2,
            len([effect for effect in relay.effects if effect[1] == "modify_sl_tp"]),
        )
        self.assertFalse(any(kind == "close" for _, kind, _ in relay.effects))

    def test_unknown_partial_arming_is_observed_then_closed_without_resending_protection(self) -> None:
        relay = ScriptedRelay()
        relay.raise_without_modify_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertEqual(
            2,
            len([effect for effect in relay.effects if effect[1] == "modify_sl_tp"]),
        )
        self.assertEqual(
            {"worker-a", "worker-b"},
            {worker_id for worker_id, kind, _ in relay.effects if kind == "close"},
        )

    def test_leg_that_exits_while_arming_starts_orphan_grace(self) -> None:
        relay = ScriptedRelay()
        relay.exit_during_modify_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual(("exiting", "ORPHAN_GRACE"), (result.status, result.state))
        self.assertFalse(any(kind == "close" for _, kind, _ in relay.effects))
        self.assertEqual(1, len(relay.positions["worker-b"]))

    def test_partial_volume_during_arming_never_becomes_armed(self) -> None:
        relay = ScriptedRelay()
        relay.partial_during_modify_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)

        result = runtime.maintain(due, market_observation(due))

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))

    def test_natural_corridor_exit_of_both_legs_needs_no_market_close(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        entered = runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        assert entered.activated_at is not None
        due = entered.activated_at + timedelta(seconds=60)
        runtime.maintain(due, market_observation(due))
        relay.positions = {"worker-a": [], "worker-b": []}

        result = runtime.maintain(due + timedelta(seconds=1), None)

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertFalse(any(kind == "close" for _, kind, _ in relay.effects))

    def test_restart_recovers_active_pair_from_worker_facts(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(self.runtime_path, relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        runtime.enter(pair_plan())
        runtime.close()

        recovered = StrategyRuntime(self.runtime_path, relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual(("active", "ACTIVE"), (result.status, result.state))
        self.assertEqual("pair-1", recovered.active_plan.pair_id)  # type: ignore[union-attr]
        recovered.close()

    def test_activation_deadline_is_persisted_before_active_transition_returns(self) -> None:
        class Crash(BaseException):
            pass

        relay = ScriptedRelay()
        crashed_at: datetime | None = None

        def crash_after(state: str) -> None:
            nonlocal crashed_at
            if state == "ACTIVE":
                crashed_at = datetime.now(UTC)
                raise Crash

        runtime = StrategyRuntime(
            self.runtime_path,
            relay,
            worker_ids=("worker-a", "worker-b"),
            after_transition=crash_after,
        )
        with self.assertRaises(Crash):
            runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        runtime.close()
        assert crashed_at is not None

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        assert result.activated_at is not None
        self.assertLessEqual(result.activated_at, crashed_at)

    def test_unknown_entry_outcome_contains_only_receipt_owned_leg_without_resend(self) -> None:
        relay = ScriptedRelay()
        relay.lose_receipt_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)

        result = runtime.enter(pair_plan())

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        entry_effects = [effect for effect in relay.effects if effect[1] == "order_execute"]
        self.assertEqual(2, len(entry_effects))
        self.assertEqual(1, len(relay.positions["worker-a"]))
        self.assertEqual(1, len(relay.positions["worker-b"]))

    def test_disconnect_after_dispatch_is_uncertain_and_never_blindly_resends_entry(self) -> None:
        relay = ScriptedRelay()
        relay.raise_after_send_worker = "worker-a"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)

        result = runtime.enter(pair_plan())

        entry_effects = [effect for effect in relay.effects if effect[1] == "order_execute"]
        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        self.assertEqual(2, len(entry_effects))
        self.assertEqual(2, len({effect_id for _, _, effect_id in entry_effects}))

    def test_rejected_preflight_emits_no_entry_effect(self) -> None:
        relay = ScriptedRelay()
        relay.reject_worker = "worker-b"
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)

        result = runtime.enter(pair_plan())

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertEqual([], relay.effects)

    def test_desired_empty_cancels_then_closes_and_requires_final_empty_snapshot(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        runtime.enter(pair_plan())
        relay.orders["worker-a"] = [{"ticket": relay.entry_order_ticket["worker-a"]}]

        result = runtime.desire_empty("shutdown")

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))
        self.assertEqual([], relay.orders["worker-a"])
        self.assertEqual([], relay.positions["worker-a"])
        self.assertEqual([], relay.positions["worker-b"])

    def test_desired_empty_dispatches_both_leg_closes_concurrently(self) -> None:
        relay = ScriptedRelay()
        relay.close_barrier = Barrier(2)
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        runtime.enter(pair_plan())

        result = runtime.desire_empty("shutdown")

        self.assertEqual(("empty", "EMPTY"), (result.status, result.state))

    def test_worker_fact_cursor_deduplicates_and_forces_snapshot_after_gap_or_epoch_change(self) -> None:
        runtime = StrategyRuntime(":memory:", ScriptedRelay(), worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        snapshot = {
            "type": "worker_snapshot",
            "protocol_version": 1,
            "worker_id": "worker-a",
            "recovery_epoch": "epoch-1",
            "snapshot_baseline": 7,
            "observed_at": "2026-08-28T08:00:00+00:00",
            "orders": [],
            "positions": [],
        }
        delta = {
            "type": "worker_broker_delta",
            "protocol_version": 1,
            "worker_id": "worker-a",
            "recovery_epoch": "epoch-1",
            "event_id": 8,
            "observed_at": "2026-08-28T08:00:01+00:00",
            "entity": "position",
            "change": "upsert",
            "record": {"ticket": 1},
        }

        self.assertEqual("snapshot", runtime.ingest_fact(snapshot))
        self.assertEqual("applied", runtime.ingest_fact(delta))
        self.assertEqual("duplicate", runtime.ingest_fact(delta))
        self.assertEqual("gap", runtime.ingest_fact({**delta, "event_id": 10}))
        self.assertEqual(
            "snapshot_required",
            runtime.ingest_fact({**delta, "recovery_epoch": "epoch-2", "event_id": 1}),
        )

    def test_changed_ticket_or_protection_requires_human_without_mutating_unknown_exposure(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        runtime.enter(pair_plan())
        relay.positions["worker-a"][0]["ticket"] = 999
        relay.positions["worker-a"][0]["sl"] = 1.08

        result = runtime.verify_active()

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        self.assertEqual("EMPTY", runtime.desired_state)

    def test_unrelated_orders_and_positions_are_never_cancelled_or_closed(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)
        runtime.enter(pair_plan())
        effects_before = list(relay.effects)
        relay.orders["worker-a"].append({"ticket": 9001, "symbol": "GBPUSD"})
        relay.positions["worker-b"].append(
            {
                "ticket": 9002,
                "symbol": "GBPUSD",
                "type": 0,
                "volume": 0.5,
                "sl": 1.0,
                "tp": 2.0,
                "price_open": 1.5,
            }
        )

        result = runtime.desire_empty("shutdown")

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, result.state))
        self.assertEqual(effects_before, relay.effects)
        self.assertEqual(9001, relay.orders["worker-a"][0]["ticket"])
        self.assertEqual(2, len(relay.positions["worker-b"]))

    def test_restart_recovers_every_durable_entry_transition(self) -> None:
        class Crash(BaseException):
            pass

        for crash_state, expected in (
            ("PREFLIGHTING", "EMPTY"),
            ("DISPATCHING", "EMPTY"),
            ("VERIFYING", "ACTIVE"),
            ("ACTIVE", "ACTIVE"),
            ("ENTRY_UNCERTAIN", "NEEDS_HUMAN"),
            ("CLOSING", "EMPTY"),
        ):
            with self.subTest(crash_state=crash_state):
                self._clean_runtime_files()
                relay = ScriptedRelay()
                if crash_state == "ENTRY_UNCERTAIN":
                    relay.lose_receipt_worker = "worker-a"
                if crash_state == "CLOSING":
                    relay.reject_worker = "worker-b"

                def crash_after(state: str) -> None:
                    if state == crash_state:
                        raise Crash

                runtime = StrategyRuntime(
                    self.runtime_path,
                    relay,
                    worker_ids=("worker-a", "worker-b"),
                    after_transition=crash_after,
                )
                with self.assertRaises(Crash):
                    runtime.enter(pair_plan())
                runtime.close()
                recovered = StrategyRuntime(
                    self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
                )
                result = recovered.recover()
                self.assertEqual(expected, result.state)
                recovered.close()

    def test_restart_retries_close_only_after_fresh_facts_with_a_new_effect_id(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        runtime.enter(pair_plan())
        relay.preserve_close = True
        uncertain = runtime.desire_empty("shutdown")
        first_close_effects = [
            effect_id for _, kind, effect_id in relay.effects if kind == "close"
        ]
        self.assertEqual("CLOSE_UNCERTAIN", uncertain.state)
        runtime.close()

        relay.preserve_close = False
        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        result = recovered.recover()
        all_close_effects = [
            effect_id for _, kind, effect_id in relay.effects if kind == "close"
        ]

        self.assertEqual("EMPTY", result.state)
        self.assertEqual(2, len(first_close_effects))
        self.assertEqual(4, len(all_close_effects))
        self.assertEqual(4, len(set(all_close_effects)))
        recovered.close()

    def test_restart_preserves_pair_projection_while_close_remains_uncertain(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        runtime.enter(pair_plan(TimedExitPolicy(maximum_holding_seconds=60)))
        relay.preserve_close = True
        self.assertEqual("CLOSE_UNCERTAIN", runtime.desire_empty("shutdown").state)
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual("CLOSE_UNCERTAIN", result.state)
        self.assertIsNotNone(result.first)
        self.assertIsNotNone(result.second)

    def test_restart_keeps_unverified_entry_close_retryable_without_pair_projection(self) -> None:
        relay = ScriptedRelay()
        relay.corrupt_entry_worker = "worker-a"
        relay.preserve_close = True
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.assertEqual("CLOSE_UNCERTAIN", runtime.enter(pair_plan()).state)
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        self.addCleanup(recovered.close)
        result = recovered.recover()

        self.assertEqual(("uncertain", "CLOSE_UNCERTAIN"), (result.status, result.state))
        self.assertIsNone(result.first)
        self.assertIsNone(result.second)

    def test_restart_preserves_needs_human_for_unowned_exposure_without_mutation(self) -> None:
        relay = ScriptedRelay()
        runtime = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        runtime.enter(pair_plan())
        relay.positions["worker-a"].append(
            {
                "ticket": 9999,
                "symbol": "GBPUSD",
                "type": 0,
                "volume": 1.0,
                "sl": 1.0,
                "tp": 2.0,
                "price_open": 1.5,
            }
        )
        self.assertEqual("NEEDS_HUMAN", runtime.desire_empty("shutdown").state)
        effects_before_restart = list(relay.effects)
        runtime.close()

        recovered = StrategyRuntime(
            self.runtime_path, relay, worker_ids=("worker-a", "worker-b")
        )
        result = recovered.recover()

        self.assertEqual("NEEDS_HUMAN", result.state)
        self.assertEqual(effects_before_restart, relay.effects)
        recovered.close()

    def test_startup_with_unowned_exposure_persists_needs_human(self) -> None:
        relay = ScriptedRelay()
        relay.positions["worker-a"] = [
            {
                "ticket": 9999,
                "symbol": "GBPUSD",
                "type": 0,
                "volume": 1.0,
                "sl": 1.0,
                "tp": 2.0,
                "price_open": 1.5,
            }
        ]
        runtime = StrategyRuntime(":memory:", relay, worker_ids=("worker-a", "worker-b"))
        self.addCleanup(runtime.close)

        result = runtime.recover()

        self.assertEqual(("needs_human", "NEEDS_HUMAN"), (result.status, runtime.state))
        self.assertEqual([], relay.effects)

    def test_legacy_generic_request_adapter_is_not_accepted(self) -> None:
        class LegacyRelay:
            def request(self, *_: object, **__: object) -> dict[str, object]:
                raise AssertionError("The runtime must not downgrade to generic requests.")

        with self.assertRaisesRegex(StrategyRuntimeError, "typed interface"):
            StrategyRuntime(
                ":memory:", LegacyRelay(), worker_ids=("worker-a", "worker-b")  # type: ignore[arg-type]
            )
