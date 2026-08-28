"""Durable strategy-owned orchestration for one protected pair."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
from pathlib import Path
import sqlite3
from threading import RLock
from collections.abc import Callable
from typing import Protocol
from uuid import uuid4

from ..trader_protocol import (
    WorkerBrokerDelta,
    WorkerSnapshot,
    WorkerStreamGap,
    WorkerStreamResumed,
    worker_fact_envelope_adapter,
)


class StrategyRuntimeError(RuntimeError):
    """Raised when broker facts cannot prove a safe pair lifecycle state."""


class RuntimeRelay(Protocol):
    def snapshot(
        self,
        worker_id: str,
        *,
        runtime_command_id: str,
        request_id: str,
        correlation: dict[str, object],
    ) -> dict[str, object]: ...

    def order_check(
        self,
        worker_id: str,
        *,
        order: dict[str, object],
        runtime_command_id: str,
        request_id: str,
        expires_at: datetime,
        correlation: dict[str, object],
    ) -> dict[str, object]: ...

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
    ) -> dict[str, object]: ...

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
    ) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class PairLegPlan:
    name: str
    worker_id: str
    symbol: str
    direction: str
    volume: str
    filling_mode: str
    margin_limit: str
    stop_loss: str
    take_profit: str


@dataclass(frozen=True, slots=True)
class PairPlan:
    pair_id: str
    command_id: str
    direction: str
    expires_at: datetime
    first: PairLegPlan
    second: PairLegPlan


@dataclass(frozen=True, slots=True)
class VerifiedLeg:
    worker_id: str
    ticket: int
    entry_price: float
    position: dict[str, object]
    receipt: dict[str, object]


@dataclass(frozen=True, slots=True)
class PairExecutionResult:
    status: str
    state: str
    first: VerifiedLeg | None = None
    second: VerifiedLeg | None = None
    reason: str | None = None


class StrategyRuntime:
    """Own paired preflight, effects, broker verification, and empty convergence."""

    def __init__(
        self,
        path: Path | str,
        relay: RuntimeRelay,
        *,
        worker_ids: tuple[str, str],
        after_transition: Callable[[str], None] | None = None,
    ) -> None:
        missing = [
            name for name in ("snapshot", "order_check", "order_execute", "operate")
            if not callable(getattr(relay, name, None))
        ]
        if missing:
            raise StrategyRuntimeError(
                f"Runtime relay does not implement the typed interface: {', '.join(missing)}."
            )
        self.relay = relay
        self.worker_ids = worker_ids
        self._lock = RLock()
        self._after_transition = after_transition
        self._connection = sqlite3.connect(str(path), check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.executescript(
            """
            CREATE TABLE IF NOT EXISTS runtime_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pair_runtime (
                singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                pair_id TEXT NOT NULL,
                command_id TEXT NOT NULL,
                desired_state TEXT NOT NULL,
                lifecycle_state TEXT NOT NULL,
                plan TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_commands (
                command_id TEXT PRIMARY KEY,
                sequence INTEGER NOT NULL UNIQUE,
                payload_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_effects (
                effect_id TEXT PRIMARY KEY,
                command_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                payload_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                delivery_state TEXT NOT NULL,
                outcome TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS runtime_verified_legs (
                pair_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                ticket INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                position TEXT NOT NULL,
                receipt TEXT NOT NULL,
                PRIMARY KEY (pair_id, worker_id)
            );
            CREATE TABLE IF NOT EXISTS runtime_owned_tickets (
                pair_id TEXT NOT NULL,
                worker_id TEXT NOT NULL,
                entity TEXT NOT NULL,
                ticket INTEGER NOT NULL,
                PRIMARY KEY (pair_id, worker_id, entity, ticket)
            );
            CREATE TABLE IF NOT EXISTS runtime_transitions (
                transition_id INTEGER PRIMARY KEY AUTOINCREMENT,
                pair_id TEXT,
                lifecycle_state TEXT NOT NULL,
                desired_state TEXT NOT NULL,
                detail TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS worker_cursors (
                worker_id TEXT PRIMARY KEY,
                recovery_epoch TEXT,
                event_id INTEGER NOT NULL DEFAULT 0,
                fresh_snapshot INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS worker_facts (
                worker_id TEXT NOT NULL,
                recovery_epoch TEXT NOT NULL,
                event_id INTEGER NOT NULL,
                envelope TEXT NOT NULL,
                PRIMARY KEY (worker_id, recovery_epoch, event_id)
            );
            """
        )
        self._connection.execute(
            """INSERT OR IGNORE INTO runtime_meta(key, value) VALUES
               ('schema_version', '1'), ('command_sequence', '0'), ('standalone_state', 'EMPTY')"""
        )
        for worker_id in worker_ids:
            self._connection.execute(
                "INSERT OR IGNORE INTO worker_cursors(worker_id, event_id, fresh_snapshot) VALUES (?, 0, 0)",
                (worker_id,),
            )
        self._connection.commit()
        configure_workers = getattr(relay, "configure_workers", None)
        if callable(configure_workers):
            configure_workers(worker_ids)
        set_fact_sink = getattr(relay, "set_fact_sink", None)
        if callable(set_fact_sink):
            set_fact_sink(self.ingest_fact)
        set_cursor_provider = getattr(relay, "set_cursor_provider", None)
        if callable(set_cursor_provider):
            set_cursor_provider(self.worker_cursor)
        resume_streams = getattr(relay, "resume_streams", None)
        if callable(resume_streams):
            resume_streams()

    def close(self) -> None:
        if getattr(self, "_closed", False):
            return
        self._connection.close()
        self._closed = True

    @property
    def state(self) -> str:
        row = self._connection.execute("SELECT lifecycle_state FROM pair_runtime WHERE singleton = 1").fetchone()
        if row is not None:
            return str(row[0])
        return str(self._connection.execute(
            "SELECT value FROM runtime_meta WHERE key = 'standalone_state'"
        ).fetchone()[0])

    @property
    def desired_state(self) -> str:
        row = self._connection.execute("SELECT desired_state FROM pair_runtime WHERE singleton = 1").fetchone()
        return "EMPTY" if row is None else str(row[0])

    @property
    def active_plan(self) -> PairPlan | None:
        row = self._connection.execute("SELECT plan FROM pair_runtime WHERE singleton = 1").fetchone()
        return None if row is None else _plan(json.loads(row[0]))

    def recover(self) -> PairExecutionResult:
        """Reconcile durable intent against fresh Worker facts before admitting entries."""

        with self._lock:
            row = self._connection.execute("SELECT * FROM pair_runtime WHERE singleton = 1").fetchone()
            facts = {worker_id: self._snapshot(worker_id, "startup-recovery") for worker_id in self.worker_ids}
            if row is None:
                if any(fact["orders"] or fact["positions"] for fact in facts.values()):
                    self._record_transition(None, "NEEDS_HUMAN", "EMPTY", "Unowned broker exposure exists at startup.")
                    self._set_standalone_state("NEEDS_HUMAN")
                    return PairExecutionResult(
                        "needs_human", "NEEDS_HUMAN", reason="Unowned broker exposure exists at startup."
                    )
                self._set_standalone_state("EMPTY")
                return PairExecutionResult("empty", "EMPTY")
            plan = _plan(json.loads(row["plan"]))
            if row["desired_state"] == "ACTIVE":
                verified = self._verify(plan, facts, receipts={})
                if verified is not None:
                    self._record_verified(plan, verified)
                    self._set_state(plan, "ACTIVE", "ACTIVE", "Recovered broker-verified active pair.")
                    return PairExecutionResult("active", "ACTIVE", *verified)
            self._set_state(plan, "CLOSING", "EMPTY", "Startup recovery converging to desired empty.")
            return self.converge_empty("startup-recovery")

    def worker_cursor(self, worker_id: str) -> tuple[str | None, int, bool]:
        row = self._connection.execute(
            "SELECT recovery_epoch, event_id, fresh_snapshot FROM worker_cursors WHERE worker_id = ?",
            (worker_id,),
        ).fetchone()
        if row is None:
            raise StrategyRuntimeError("Worker is not part of this Strategy Runtime.")
        return row[0], int(row[1]), bool(row[2])

    def ingest_fact(self, envelope: dict[str, object]) -> str:
        """Durably deduplicate Worker facts and force snapshots after gaps or epoch changes."""

        with self._lock:
            return self._ingest_fact(envelope)

    def _ingest_fact(self, envelope: dict[str, object]) -> str:
        fact = worker_fact_envelope_adapter.validate_python(envelope)
        if fact.worker_id not in self.worker_ids:
            raise StrategyRuntimeError("Worker fact is not routed to this Strategy Runtime.")
        epoch, cursor, fresh = self.worker_cursor(fact.worker_id)
        if isinstance(fact, WorkerSnapshot):
            self._connection.execute(
                """UPDATE worker_cursors SET recovery_epoch = ?, event_id = ?, fresh_snapshot = 1
                   WHERE worker_id = ?""",
                (fact.recovery_epoch, fact.snapshot_baseline, fact.worker_id),
            )
            self._connection.commit()
            return "snapshot"
        if isinstance(fact, WorkerStreamGap):
            self._connection.execute(
                "UPDATE worker_cursors SET recovery_epoch = ?, fresh_snapshot = 0 WHERE worker_id = ?",
                (fact.recovery_epoch, fact.worker_id),
            )
            self._connection.commit()
            return "gap"
        if isinstance(fact, WorkerStreamResumed):
            if epoch is not None and epoch != fact.recovery_epoch:
                fresh = False
            self._connection.execute(
                "UPDATE worker_cursors SET recovery_epoch = ?, fresh_snapshot = ? WHERE worker_id = ?",
                (fact.recovery_epoch, int(fresh), fact.worker_id),
            )
            self._connection.commit()
            return "resumed" if fresh else "snapshot_required"
        if not isinstance(fact, WorkerBrokerDelta):
            return "recorded"
        if epoch != fact.recovery_epoch:
            self._connection.execute(
                "UPDATE worker_cursors SET recovery_epoch = ?, fresh_snapshot = 0 WHERE worker_id = ?",
                (fact.recovery_epoch, fact.worker_id),
            )
            self._connection.commit()
            return "snapshot_required"
        if fact.event_id <= cursor:
            return "duplicate"
        if fact.event_id != cursor + 1 or not fresh:
            self._connection.execute(
                "UPDATE worker_cursors SET fresh_snapshot = 0 WHERE worker_id = ?", (fact.worker_id,)
            )
            self._connection.commit()
            return "gap"
        canonical = _canonical(envelope)
        self._connection.execute(
            "INSERT OR IGNORE INTO worker_facts VALUES (?, ?, ?, ?)",
            (fact.worker_id, fact.recovery_epoch, fact.event_id, canonical),
        )
        self._connection.execute(
            "UPDATE worker_cursors SET event_id = ? WHERE worker_id = ?",
            (fact.event_id, fact.worker_id),
        )
        self._connection.commit()
        return "applied"

    def enter(self, plan: PairPlan) -> PairExecutionResult:
        with self._lock:
            if self.state != "EMPTY":
                raise StrategyRuntimeError("A new pair cannot start until broker-verified empty.")
            admission_facts = {
                worker_id: self._snapshot(worker_id, f"{plan.command_id}:entry-admission")
                for worker_id in self.worker_ids
            }
            if any(fact["orders"] or fact["positions"] for fact in admission_facts.values()):
                raise StrategyRuntimeError("A new pair cannot start while current Worker facts show exposure.")
            self._record_command(plan.command_id, _plan_json(plan))
            effects = {
                leg.name: f"{plan.command_id}:{leg.worker_id}:entry"
                for leg in (plan.first, plan.second)
            }
            for leg in (plan.first, plan.second):
                self._prepare_effect(effects[leg.name], plan.command_id, leg.worker_id, _order(leg))
            self._set_state(plan, "PREFLIGHTING", "ACTIVE", "Both exact broker checks are outstanding.")

        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                leg.name: executor.submit(self._safe_preflight, plan, leg)
                for leg in (plan.first, plan.second)
            }
            checks = {name: future.result() for name, future in futures.items()}
        rejected = [name for name, result in checks.items() if not _preflight_accepted(result, getattr(plan, name))]
        if rejected or datetime.now(UTC) >= plan.expires_at:
            self._set_state(plan, "CLOSING", "EMPTY", f"Entry preflight rejected: {', '.join(rejected) or 'expired'}.")
            return self.converge_empty("entry-preflight-rejected")

        self._set_state(plan, "DISPATCHING", "ACTIVE", "Both entry effects may reach their Workers.")
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                leg.name: executor.submit(self._safe_execute, plan, leg, effects[leg.name])
                for leg in (plan.first, plan.second)
            }
            outcomes = {name: future.result() for name, future in futures.items()}
        for name, outcome in outcomes.items():
            self._record_effect_outcome(effects[name], outcome)
        if not all(_effect_accepted(outcome) for outcome in outcomes.values()):
            self._set_state(plan, "ENTRY_UNCERTAIN", "EMPTY", "At least one entry effect was rejected or uncertain.")
            return self.converge_empty("asymmetric-or-uncertain-entry")

        self._set_state(plan, "VERIFYING", "ACTIVE", "Receipts exist; fresh Worker facts must prove the pair.")
        facts = {
            plan.first.worker_id: self._snapshot(plan.first.worker_id, plan.command_id),
            plan.second.worker_id: self._snapshot(plan.second.worker_id, plan.command_id),
        }
        verified = self._verify(plan, facts, outcomes)
        if verified is None:
            self._set_state(plan, "ENTRY_UNCERTAIN", "EMPTY", "Broker facts do not prove the requested protected pair.")
            return self.converge_empty("entry-verification-failed")
        self._record_verified(plan, verified)
        self._set_state(plan, "ACTIVE", "ACTIVE", "Current Worker facts prove the protected pair.")
        return PairExecutionResult("active", "ACTIVE", *verified)

    def verify_active(self) -> PairExecutionResult:
        row = self._connection.execute("SELECT plan FROM pair_runtime WHERE singleton = 1").fetchone()
        if row is None:
            return PairExecutionResult("empty", "EMPTY")
        plan = _plan(json.loads(row[0]))
        facts = {
            plan.first.worker_id: self._snapshot(plan.first.worker_id, f"{plan.command_id}:integrity"),
            plan.second.worker_id: self._snapshot(plan.second.worker_id, f"{plan.command_id}:integrity"),
        }
        verified = self._verify(plan, facts, receipts={})
        if verified is not None:
            return PairExecutionResult("active", "ACTIVE", *verified)
        self._set_state(plan, "CLOSING", "EMPTY", "Current Worker facts diverged from the protected pair.")
        return self.converge_empty("integrity-divergence")

    def desire_empty(self, reason: str) -> PairExecutionResult:
        row = self._connection.execute("SELECT plan FROM pair_runtime WHERE singleton = 1").fetchone()
        if row is None:
            facts = {worker_id: self._snapshot(worker_id, reason) for worker_id in self.worker_ids}
            if all(not fact["orders"] and not fact["positions"] for fact in facts.values()):
                return PairExecutionResult("empty", "EMPTY")
            raise StrategyRuntimeError("Cannot automatically close exposure without an owned pair.")
        plan = _plan(json.loads(row[0]))
        self._set_state(plan, "CLOSING", "EMPTY", reason)
        return self.converge_empty(reason)

    def converge_empty(self, reason: str) -> PairExecutionResult:
        row = self._connection.execute("SELECT plan FROM pair_runtime WHERE singleton = 1").fetchone()
        if row is None:
            return PairExecutionResult("empty", "EMPTY")
        plan = _plan(json.loads(row[0]))
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:close-observe")
            for worker_id in self.worker_ids
        }
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return self._needs_human(plan, "Fresh facts contain exposure not owned by the durable pair.")
        for worker_id, owned in exposure.items():
            for order in owned["orders"]:
                ticket = _ticket(order)
                self._generic_effect(
                    plan, worker_id, f"cancel:{ticket}", {"type": "cancel", "ticket": str(ticket)}
                )
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:post-cancel-observe")
            for worker_id in self.worker_ids
        }
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return self._needs_human(plan, "Unowned exposure appeared while cancelling pair-owned orders.")
        if any(owned["orders"] for owned in exposure.values()):
            self._set_state(plan, "CLOSE_UNCERTAIN", "EMPTY", "Pair-owned orders remain after cancellation.")
            return PairExecutionResult("uncertain", "CLOSE_UNCERTAIN", reason="Pair-owned orders remain.")
        for worker_id, owned in exposure.items():
            for position in owned["positions"]:
                ticket = _ticket(position)
                volume = position.get("volume")
                if isinstance(volume, bool) or not isinstance(volume, (int, float)):
                    return self._needs_human(plan, "Pair-owned position volume is unobservable.")
                self._generic_effect(
                    plan, worker_id, f"close:{ticket}", {"type": "close", "ticket": str(ticket), "volume": str(volume)}
                )
        final = {worker_id: self._snapshot(worker_id, f"{plan.command_id}:final-empty") for worker_id in self.worker_ids}
        final_exposure = self._owned_exposure(plan, final)
        if final_exposure is None:
            return self._needs_human(plan, "Final facts contain unowned exposure.")
        if any(owned["orders"] or owned["positions"] for owned in final_exposure.values()):
            self._set_state(plan, "CLOSE_UNCERTAIN", "EMPTY", f"Broker facts remain non-empty after {reason}.")
            return PairExecutionResult("uncertain", "CLOSE_UNCERTAIN", reason="Broker facts remain non-empty.")
        self._record_transition(plan.pair_id, "EMPTY", "EMPTY", f"Broker-verified empty after {reason}.")
        self._connection.execute("DELETE FROM pair_runtime WHERE singleton = 1")
        self._connection.execute(
            "UPDATE runtime_meta SET value = 'EMPTY' WHERE key = 'standalone_state'"
        )
        self._connection.commit()
        return PairExecutionResult("empty", "EMPTY")

    def _owned_exposure(
        self,
        plan: PairPlan,
        facts: dict[str, dict[str, list[dict[str, object]]]],
    ) -> dict[str, dict[str, list[dict[str, object]]]] | None:
        result: dict[str, dict[str, list[dict[str, object]]]] = {}
        for leg in (plan.first, plan.second):
            owned_position_tickets, owned_order_tickets = self._owned_tickets(plan, leg)
            positions = facts[leg.worker_id]["positions"]
            for position in positions:
                ticket = _ticket(position)
                if ticket in owned_order_tickets:
                    owned_position_tickets.add(ticket)
                    self._record_owned_ticket(plan.pair_id, leg.worker_id, "position", ticket)
            owned_positions = [
                position for position in positions if _ticket(position) in owned_position_tickets
            ]
            owned_orders = [
                order for order in facts[leg.worker_id]["orders"]
                if _ticket(order) in owned_order_tickets
            ]
            if len(owned_positions) != len(positions) or len(owned_orders) != len(facts[leg.worker_id]["orders"]):
                return None
            result[leg.worker_id] = {"orders": owned_orders, "positions": owned_positions}
        return result

    def _owned_tickets(self, plan: PairPlan, leg: PairLegPlan) -> tuple[set[int], set[int]]:
        rows = self._connection.execute(
            "SELECT entity, ticket FROM runtime_owned_tickets WHERE pair_id = ? AND worker_id = ?",
            (plan.pair_id, leg.worker_id),
        ).fetchall()
        positions = {int(row[1]) for row in rows if row[0] == "position"}
        orders = {int(row[1]) for row in rows if row[0] == "order"}
        outcomes = self._connection.execute(
            """SELECT outcome FROM runtime_effects
               WHERE command_id = ? AND worker_id = ? AND outcome IS NOT NULL""",
            (plan.command_id, leg.worker_id),
        ).fetchall()
        for row in outcomes:
            outcome = json.loads(row[0])
            result = outcome.get("result") if isinstance(outcome, dict) else None
            if not isinstance(result, dict):
                continue
            for entity, field, target in (
                ("position", "position", positions),
                ("order", "order", orders),
            ):
                ticket = _receipt_ticket(result, field)
                if ticket is not None:
                    target.add(ticket)
                    self._record_owned_ticket(plan.pair_id, leg.worker_id, entity, ticket)
        return positions, orders

    def _record_owned_ticket(self, pair_id: str, worker_id: str, entity: str, ticket: int) -> None:
        self._connection.execute(
            "INSERT OR IGNORE INTO runtime_owned_tickets VALUES (?, ?, ?, ?)",
            (pair_id, worker_id, entity, ticket),
        )
        self._connection.commit()

    def _needs_human(self, plan: PairPlan, reason: str) -> PairExecutionResult:
        self._set_state(plan, "NEEDS_HUMAN", "EMPTY", reason)
        return PairExecutionResult("needs_human", "NEEDS_HUMAN", reason=reason)

    def _set_standalone_state(self, state: str) -> None:
        self._connection.execute(
            "UPDATE runtime_meta SET value = ? WHERE key = 'standalone_state'", (state,)
        )
        self._connection.commit()

    def _preflight(self, plan: PairPlan, leg: PairLegPlan) -> dict[str, object]:
        return self.relay.order_check(
            leg.worker_id,
            order=_order(leg),
            runtime_command_id=plan.command_id,
            request_id=f"{plan.command_id}:{leg.name}:check",
            expires_at=plan.expires_at,
            correlation={"pair_id": plan.pair_id, "leg": leg.name, "purpose": "entry-preflight"},
        )

    def _safe_preflight(self, plan: PairPlan, leg: PairLegPlan) -> dict[str, object]:
        try:
            return self._preflight(plan, leg)
        except Exception as error:
            return {"accepted": False, "execution_state": "not_started", "reason": str(error)}

    def _execute(self, plan: PairPlan, leg: PairLegPlan, effect_id: str) -> dict[str, object]:
        with self._lock:
            self._connection.execute(
                "UPDATE runtime_effects SET delivery_state = 'dispatching', updated_at = ? WHERE effect_id = ?",
                (_now(), effect_id),
            )
            self._connection.commit()
        return self.relay.order_execute(
            leg.worker_id,
            order=_order(leg),
            runtime_command_id=plan.command_id,
            request_id=f"{plan.command_id}:{leg.name}:execute",
            effect_id=effect_id,
            expires_at=plan.expires_at,
            correlation={"pair_id": plan.pair_id, "leg": leg.name, "purpose": "entry-dispatch"},
        )

    def _safe_execute(self, plan: PairPlan, leg: PairLegPlan, effect_id: str) -> dict[str, object]:
        try:
            return self._execute(plan, leg, effect_id)
        except Exception as error:
            return {
                "accepted": False,
                "execution_state": "sent",
                "outcome": "unknown_after_send",
                "result": {},
                "reason": str(error),
            }

    def _generic_effect(
        self, plan: PairPlan, worker_id: str, suffix: str, request: dict[str, object]
    ) -> dict[str, object]:
        effect_id = self._next_effect_id(plan.command_id, worker_id, suffix)
        self._prepare_effect(effect_id, plan.command_id, worker_id, request)
        try:
            outcome = self.relay.operate(
                worker_id,
                operation=request,
                runtime_command_id=plan.command_id,
                request_id=effect_id,
                effect_id=effect_id,
                expires_at=datetime.now(UTC) + timedelta(seconds=30),
                correlation={"pair_id": plan.pair_id, "purpose": suffix},
            )
        except Exception as error:
            outcome = {
                "accepted": False,
                "execution_state": "sent",
                "outcome": "unknown_after_send",
                "result": {},
                "reason": str(error),
            }
        if "accepted" not in outcome:
            outcome = {
                "accepted": True,
                "execution_state": "receipt",
                "outcome": "accepted",
                "result": outcome,
            }
        self._record_effect_outcome(effect_id, outcome)
        return outcome

    def _next_effect_id(self, command_id: str, worker_id: str, suffix: str) -> str:
        prefix = f"{command_id}:{worker_id}:{suffix}:attempt-"
        rows = self._connection.execute(
            "SELECT effect_id FROM runtime_effects WHERE effect_id LIKE ?", (f"{prefix}%",)
        ).fetchall()
        attempts = [
            int(str(row[0]).removeprefix(prefix))
            for row in rows
            if str(row[0]).removeprefix(prefix).isdigit()
        ]
        return f"{prefix}{max(attempts, default=0) + 1}"

    def _snapshot(self, worker_id: str, command_id: str) -> dict[str, list[dict[str, object]]]:
        snapshot = self.relay.snapshot(
            worker_id,
            runtime_command_id=command_id,
            request_id=f"{command_id}:{worker_id}:snapshot:{uuid4()}",
            correlation={"purpose": "fresh-snapshot"},
        )
        orders = snapshot.get("orders")
        positions = snapshot.get("positions")
        if not isinstance(orders, list) or not all(isinstance(value, dict) for value in orders):
            raise StrategyRuntimeError(f"Worker {worker_id} returned invalid current orders.")
        if not isinstance(positions, list) or not all(isinstance(value, dict) for value in positions):
            raise StrategyRuntimeError(f"Worker {worker_id} returned invalid current positions.")
        recovery_epoch = snapshot.get("recovery_epoch")
        snapshot_baseline = snapshot.get("snapshot_baseline", self.worker_cursor(worker_id)[1])
        if (
            not isinstance(recovery_epoch, str)
            or not recovery_epoch
            or isinstance(snapshot_baseline, bool)
            or not isinstance(snapshot_baseline, int)
        ):
            raise StrategyRuntimeError(f"Worker {worker_id} returned snapshot without stream metadata.")
        self._connection.execute(
            """UPDATE worker_cursors SET recovery_epoch = ?, event_id = ?, fresh_snapshot = 1
               WHERE worker_id = ?""",
            (recovery_epoch, snapshot_baseline, worker_id),
        )
        self._connection.commit()
        return {"orders": orders, "positions": positions}

    def _verify(
        self,
        plan: PairPlan,
        facts: dict[str, dict[str, list[dict[str, object]]]],
        receipts: dict[str, dict[str, object]],
    ) -> tuple[VerifiedLeg, VerifiedLeg] | None:
        verified: list[VerifiedLeg] = []
        for leg in (plan.first, plan.second):
            fact = facts[leg.worker_id]
            if fact["orders"] or len(fact["positions"]) != 1:
                return None
            position = fact["positions"][0]
            expected_type = 0 if leg.direction == "LONG" else 1
            if (
                position.get("symbol") != leg.symbol
                or position.get("type") != expected_type
                or not _number_equal(position.get("volume"), leg.volume)
                or not _number_equal(position.get("sl"), leg.stop_loss)
                or not _number_equal(position.get("tp"), leg.take_profit)
            ):
                return None
            ticket = _ticket(position)
            expected_ticket = self._connection.execute(
                "SELECT ticket FROM runtime_verified_legs WHERE pair_id = ? AND worker_id = ?",
                (plan.pair_id, leg.worker_id),
            ).fetchone()
            owned_position_tickets, _ = self._owned_tickets(plan, leg)
            receipt = receipts.get(leg.name, {})
            receipt_result = receipt.get("result")
            if isinstance(receipt_result, dict):
                receipt_ticket = _receipt_ticket(receipt_result, "position")
                if receipt_ticket is None:
                    receipt_ticket = _receipt_ticket(receipt_result, "order")
                if receipt_ticket == ticket:
                    owned_position_tickets.add(ticket)
                    self._record_owned_ticket(plan.pair_id, leg.worker_id, "position", ticket)
            if ticket not in owned_position_tickets:
                return None
            if expected_ticket is not None and int(expected_ticket[0]) != ticket:
                return None
            if isinstance(receipt_result, dict):
                receipt_ticket = _receipt_ticket(receipt_result, "position")
                if receipt_ticket is None:
                    receipt_ticket = _receipt_ticket(receipt_result, "order")
                if receipt_ticket is not None and receipt_ticket != ticket:
                    return None
            price = position.get("price_open")
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                price = receipt_result.get("price") if isinstance(receipt_result, dict) else None
            if isinstance(price, bool) or not isinstance(price, (int, float)):
                return None
            verified.append(VerifiedLeg(leg.worker_id, ticket, float(price), position, receipt))
        return verified[0], verified[1]

    def _record_verified(
        self, plan: PairPlan, verified: tuple[VerifiedLeg, VerifiedLeg]
    ) -> None:
        for leg in verified:
            existing = self._connection.execute(
                "SELECT ticket FROM runtime_verified_legs WHERE pair_id = ? AND worker_id = ?",
                (plan.pair_id, leg.worker_id),
            ).fetchone()
            if existing is not None and int(existing[0]) != leg.ticket:
                raise StrategyRuntimeError("Current broker ticket differs from the durable verified leg.")
            if existing is not None:
                continue
            self._connection.execute(
                """INSERT INTO runtime_verified_legs
                   (pair_id, worker_id, ticket, entry_price, position, receipt)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (
                    plan.pair_id,
                    leg.worker_id,
                    leg.ticket,
                    leg.entry_price,
                    _canonical(leg.position),
                    _canonical(leg.receipt),
                ),
            )
            self._record_owned_ticket(plan.pair_id, leg.worker_id, "position", leg.ticket)
        self._connection.commit()

    def _record_command(self, command_id: str, payload: dict[str, object]) -> None:
        canonical = _canonical(payload)
        payload_hash = _hash(canonical)
        existing = self._connection.execute(
            "SELECT payload_hash FROM runtime_commands WHERE command_id = ?", (command_id,)
        ).fetchone()
        if existing is not None:
            if existing[0] != payload_hash:
                raise StrategyRuntimeError("Runtime command ID was reused with a different payload.")
            return
        sequence = int(self._connection.execute(
            "SELECT value FROM runtime_meta WHERE key = 'command_sequence'"
        ).fetchone()[0]) + 1
        self._connection.execute(
            "INSERT INTO runtime_commands VALUES (?, ?, ?, ?, ?)",
            (command_id, sequence, payload_hash, canonical, _now()),
        )
        self._connection.execute(
            "UPDATE runtime_meta SET value = ? WHERE key = 'command_sequence'", (str(sequence),)
        )
        self._connection.commit()

    def _prepare_effect(
        self, effect_id: str, command_id: str, worker_id: str, payload: dict[str, object]
    ) -> None:
        canonical = _canonical(payload)
        payload_hash = _hash(canonical)
        row = self._connection.execute(
            "SELECT payload_hash, delivery_state FROM runtime_effects WHERE effect_id = ?", (effect_id,)
        ).fetchone()
        if row is not None:
            if row[0] != payload_hash:
                raise StrategyRuntimeError("Runtime effect ID was reused with a different payload.")
            if row[1] != "prepared":
                raise StrategyRuntimeError("An effect that may have reached a Worker cannot be blindly resent.")
            return
        self._connection.execute(
            "INSERT INTO runtime_effects VALUES (?, ?, ?, ?, ?, 'prepared', NULL, ?)",
            (effect_id, command_id, worker_id, payload_hash, canonical, _now()),
        )
        self._connection.commit()

    def _record_effect_outcome(self, effect_id: str, outcome: dict[str, object]) -> None:
        state = "receipt" if _effect_accepted(outcome) else (
            "send_started" if outcome.get("execution_state") == "sent" else "not_started"
        )
        self._connection.execute(
            "UPDATE runtime_effects SET delivery_state = ?, outcome = ?, updated_at = ? WHERE effect_id = ?",
            (state, _canonical(outcome), _now(), effect_id),
        )
        self._connection.commit()

    def _set_state(self, plan: PairPlan, state: str, desired: str, detail: str) -> None:
        self._connection.execute(
            """
            INSERT INTO pair_runtime(singleton, pair_id, command_id, desired_state, lifecycle_state, plan, updated_at)
            VALUES (1, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(singleton) DO UPDATE SET desired_state=excluded.desired_state,
                lifecycle_state=excluded.lifecycle_state, plan=excluded.plan, updated_at=excluded.updated_at
            """,
            (plan.pair_id, plan.command_id, desired, state, _canonical(_plan_json(plan)), _now()),
        )
        self._record_transition(plan.pair_id, state, desired, detail)
        self._connection.commit()
        if self._after_transition is not None:
            self._after_transition(state)

    def _record_transition(self, pair_id: str | None, state: str, desired: str, detail: str) -> None:
        self._connection.execute(
            "INSERT INTO runtime_transitions(pair_id, lifecycle_state, desired_state, detail, recorded_at) VALUES (?, ?, ?, ?, ?)",
            (pair_id, state, desired, detail, _now()),
        )


def _order(leg: PairLegPlan) -> dict[str, object]:
    return {
        "action": "market",
        "symbol": leg.symbol,
        "volume": leg.volume,
        "direction": leg.direction,
        "filling_mode": leg.filling_mode,
        "sl": leg.stop_loss,
        "tp": leg.take_profit,
    }


def _preflight_accepted(result: dict[str, object], leg: PairLegPlan) -> bool:
    if result.get("accepted") is not True:
        return False
    diagnostics = result.get("diagnostics")
    margin = diagnostics.get("margin") if isinstance(diagnostics, dict) else None
    try:
        return margin is not None and float(str(margin)) <= float(leg.margin_limit)
    except (TypeError, ValueError):
        return False


def _effect_accepted(result: dict[str, object]) -> bool:
    return result.get("accepted") is True and result.get("execution_state") not in {"sent", "not_started"}


def _ticket(record: dict[str, object]) -> int:
    ticket = record.get("ticket")
    if isinstance(ticket, bool) or not isinstance(ticket, int):
        raise StrategyRuntimeError("Worker returned broker evidence without an integer ticket.")
    return ticket


def _receipt_ticket(result: dict[str, object], field: str) -> int | None:
    value = result.get(field)
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    if isinstance(value, dict):
        try:
            return _ticket(value)
        except StrategyRuntimeError:
            return None
    nested = result.get("result")
    if isinstance(nested, dict):
        return _receipt_ticket(nested, field)
    return None


def _number_equal(value: object, expected: str) -> bool:
    try:
        return abs(float(str(value)) - float(expected)) <= 1e-8
    except (TypeError, ValueError):
        return False


def _plan_json(plan: PairPlan) -> dict[str, object]:
    return {
        "pair_id": plan.pair_id,
        "command_id": plan.command_id,
        "direction": plan.direction,
        "expires_at": plan.expires_at.isoformat(),
        "first": asdict(plan.first),
        "second": asdict(plan.second),
    }


def _plan(value: dict[str, object]) -> PairPlan:
    return PairPlan(
        pair_id=str(value["pair_id"]),
        command_id=str(value["command_id"]),
        direction=str(value["direction"]),
        expires_at=datetime.fromisoformat(str(value["expires_at"])),
        first=PairLegPlan(**value["first"]),  # type: ignore[arg-type]
        second=PairLegPlan(**value["second"]),  # type: ignore[arg-type]
    )


def _canonical(value: dict[str, object]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()
