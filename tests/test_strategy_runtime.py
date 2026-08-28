from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
import unittest

from abt.trader.runtime import PairLegPlan, PairPlan, StrategyRuntime, StrategyRuntimeError


class ScriptedRelay:
    def __init__(self) -> None:
        self.orders = {"worker-a": [], "worker-b": []}
        self.positions = {"worker-a": [], "worker-b": []}
        self.effects: list[tuple[str, str, str | None]] = []
        self.reject_worker: str | None = None
        self.lose_receipt_worker: str | None = None
        self.next_ticket = 100
        self.entry_order_ticket: dict[str, int] = {}
        self.recovery_epoch = {"worker-a": "epoch-a", "worker-b": "epoch-b"}
        self.cursor = {"worker-a": 0, "worker-b": 0}
        self.preserve_close = False
        self.raise_after_send_worker: str | None = None

    def snapshot(
        self,
        worker_id: str,
        *,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]:
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
            self.effects.append((worker_id, str(request["type"]), effect_id))
            if request["type"] == "cancel":
                if not self.preserve_close:
                    self.orders[worker_id] = []
            elif request["type"] == "close":
                if not self.preserve_close:
                    self.positions[worker_id] = []
            return {"operation": request["type"], "result": {}}
        raise AssertionError((worker_id, kind, request))


def pair_plan() -> PairPlan:
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
