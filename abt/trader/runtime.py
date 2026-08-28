"""Durable strategy-owned orchestration for one protected pair."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from datetime import UTC, datetime, timedelta
from hashlib import sha256
import json
import logging
import math
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

_LOGGER = logging.getLogger(__name__)


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

    def symbol_info(
        self,
        worker_id: str,
        *,
        symbol: str,
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
class TimedExitPolicy:
    maximum_holding_seconds: float
    orphan_grace_seconds: float = 15
    corridor_deadline_seconds: float = 30
    market_max_age_seconds: float = 2
    spread_multiplier: float = 3
    movement_multiplier: float = 1

    def __post_init__(self) -> None:
        values = (
            self.maximum_holding_seconds,
            self.orphan_grace_seconds,
            self.corridor_deadline_seconds,
            self.market_max_age_seconds,
            self.spread_multiplier,
            self.movement_multiplier,
        )
        if any(not math.isfinite(value) or value <= 0 for value in values):
            raise ValueError("Timed exit policy values must be finite and positive.")


@dataclass(frozen=True, slots=True)
class PairLegMarket:
    worker_id: str
    observed_at: datetime
    bid: float
    ask: float
    recent_midpoint_moves: tuple[float, ...]


@dataclass(frozen=True, slots=True)
class PairMarketObservation:
    first: PairLegMarket
    second: PairLegMarket


@dataclass(frozen=True, slots=True)
class PairPlan:
    pair_id: str
    command_id: str
    direction: str
    expires_at: datetime
    first: PairLegPlan
    second: PairLegPlan
    timed_exit: TimedExitPolicy | None = None


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
    activated_at: datetime | None = None


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
            name for name in ("snapshot", "symbol_info", "order_check", "order_execute", "operate")
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
            CREATE TABLE IF NOT EXISTS runtime_timed_exit (
                pair_id TEXT PRIMARY KEY,
                activated_at TEXT NOT NULL,
                exit_due_at TEXT NOT NULL,
                phase TEXT NOT NULL,
                corridor TEXT,
                first_target TEXT,
                second_target TEXT,
                armed_at TEXT,
                corridor_deadline_at TEXT,
                missing_worker_id TEXT,
                orphan_deadline_at TEXT,
                surviving_worker_id TEXT,
                surviving_ticket INTEGER,
                last_runtime_at TEXT,
                updated_at TEXT NOT NULL
            );
            """
        )
        timed_exit_columns = {
            str(row[1])
            for row in self._connection.execute(
                "PRAGMA table_info(runtime_timed_exit)"
            ).fetchall()
        }
        if "corridor" not in timed_exit_columns:
            self._connection.execute(
                "ALTER TABLE runtime_timed_exit ADD COLUMN corridor TEXT"
            )
        for column, kind in (
            ("surviving_worker_id", "TEXT"),
            ("surviving_ticket", "INTEGER"),
            ("last_runtime_at", "TEXT"),
        ):
            if column not in timed_exit_columns:
                self._connection.execute(
                    f"ALTER TABLE runtime_timed_exit ADD COLUMN {column} {kind}"
                )
        self._connection.execute(
            """INSERT OR IGNORE INTO runtime_meta(key, value) VALUES
               ('schema_version', '1'), ('command_sequence', '0'), ('standalone_state', 'EMPTY')"""
        )
        self._connection.execute(
            "UPDATE runtime_meta SET value = '2' WHERE key = 'schema_version'"
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
            plan = _plan(json.loads(row["plan"])) if row is not None else None
            if (
                row is not None
                and row["desired_state"] == "ACTIVE"
                and plan is not None
                and plan.timed_exit is not None
            ):
                self._record_activation(plan)
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
            assert plan is not None
            if row["desired_state"] == "ACTIVE":
                verified = self._verify(plan, facts, receipts={})
                if verified is not None:
                    activated_at = self._record_activation(plan)
                    self._record_verified(plan, verified)
                    self._set_state(plan, "ACTIVE", "ACTIVE", "Recovered broker-verified active pair.")
                    return PairExecutionResult(
                        "active", "ACTIVE", *verified, activated_at=activated_at
                    )
            if (
                row["desired_state"] == "EMPTY"
                and row["lifecycle_state"] in {"TIMED_EXIT_ARMED", "ORPHAN_GRACE"}
            ):
                return self._recovered_pair_result(
                    plan, self.maintain(datetime.now(UTC), None)
                )
            if (
                row["desired_state"] == "EMPTY"
                and row["lifecycle_state"] == "TIMED_EXIT_ARMING"
                and plan.timed_exit is not None
            ):
                return self._recovered_pair_result(
                    plan,
                    self._recover_timed_exit_arming(
                        plan, plan.timed_exit, datetime.now(UTC)
                    ),
                )
            timed = self._connection.execute(
                "SELECT * FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
            ).fetchone()
            if (
                row["desired_state"] == "EMPTY"
                and row["lifecycle_state"] in {"CLOSING", "CLOSE_UNCERTAIN"}
                and timed is not None
                and timed["orphan_deadline_at"] is not None
            ):
                return self._recovered_pair_result(
                    plan, self._close_orphan_survivor(plan, timed)
                )
            self._set_state(plan, "CLOSING", "EMPTY", "Startup recovery converging to desired empty.")
            return self._recovered_pair_result(
                plan, self.converge_empty("startup-recovery")
            )

    def _recovered_pair_result(
        self,
        plan: PairPlan,
        result: PairExecutionResult,
    ) -> PairExecutionResult:
        if result.state in {"EMPTY", "NEEDS_HUMAN"}:
            return result
        verified: list[VerifiedLeg] = []
        for leg in (plan.first, plan.second):
            row = self._connection.execute(
                """SELECT ticket, entry_price, position, receipt
                   FROM runtime_verified_legs
                   WHERE pair_id = ? AND worker_id = ?""",
                (plan.pair_id, leg.worker_id),
            ).fetchone()
            if row is None:
                if result.state in {"CLOSING", "CLOSE_UNCERTAIN"}:
                    return result
                return self._needs_human(
                    plan, "Timed exit recovery lacks durable verified leg evidence."
                )
            verified.append(
                VerifiedLeg(
                    worker_id=leg.worker_id,
                    ticket=int(row["ticket"]),
                    entry_price=float(row["entry_price"]),
                    position=json.loads(str(row["position"])),
                    receipt=json.loads(str(row["receipt"])),
                )
            )
        timed = self._connection.execute(
            "SELECT activated_at FROM runtime_timed_exit WHERE pair_id = ?",
            (plan.pair_id,),
        ).fetchone()
        activated_at = (
            datetime.fromisoformat(str(timed[0])) if timed is not None else None
        )
        return PairExecutionResult(
            result.status,
            result.state,
            verified[0],
            verified[1],
            result.reason,
            activated_at,
        )

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

        self._record_activation(plan, datetime.now(UTC))
        self._set_state(plan, "VERIFYING", "ACTIVE", "Receipts exist; fresh Worker facts must prove the pair.")
        facts = {
            plan.first.worker_id: self._snapshot(plan.first.worker_id, plan.command_id),
            plan.second.worker_id: self._snapshot(plan.second.worker_id, plan.command_id),
        }
        verified = self._verify(plan, facts, outcomes)
        if verified is None:
            self._set_state(plan, "ENTRY_UNCERTAIN", "EMPTY", "Broker facts do not prove the requested protected pair.")
            return self.converge_empty("entry-verification-failed")
        activated_at = self._record_activation(plan, datetime.now(UTC))
        self._record_verified(plan, verified)
        self._set_state(plan, "ACTIVE", "ACTIVE", "Current Worker facts prove the protected pair.")
        return PairExecutionResult("active", "ACTIVE", *verified, activated_at=activated_at)

    def maintain(
        self,
        now: datetime,
        market: PairMarketObservation | None,
    ) -> PairExecutionResult:
        """Advance the durable timed-exit lifecycle from current time and market evidence."""

        if now.tzinfo is None:
            raise StrategyRuntimeError("Strategy Runtime maintenance time must include an offset.")
        with self._lock:
            return self._maintain(now, market)

    def _maintain(
        self,
        now: datetime,
        market: PairMarketObservation | None,
    ) -> PairExecutionResult:
        now = now.astimezone(UTC)
        row = self._connection.execute("SELECT * FROM pair_runtime WHERE singleton = 1").fetchone()
        if row is None:
            return PairExecutionResult("empty", "EMPTY")
        plan = _plan(json.loads(row["plan"]))
        policy = plan.timed_exit
        if row["lifecycle_state"] == "NEEDS_HUMAN":
            return PairExecutionResult(
                "needs_human", "NEEDS_HUMAN",
                reason="Strategy Runtime requires operator action.",
            )
        if policy is None:
            if row["lifecycle_state"] in {"CLOSING", "CLOSE_UNCERTAIN"}:
                return self.converge_empty("runtime-close-convergence")
            return PairExecutionResult("active", str(row["lifecycle_state"]))
        timed = self._connection.execute(
            "SELECT * FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
        ).fetchone()
        if timed is None:
            activated_at = self._record_activation(plan)
            timed = self._connection.execute(
                "SELECT * FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
            ).fetchone()
            assert timed is not None
        else:
            activated_at = datetime.fromisoformat(str(timed["activated_at"]))
        last_runtime_at = timed["last_runtime_at"]
        if last_runtime_at is not None:
            now = max(now, datetime.fromisoformat(str(last_runtime_at)))
        self._connection.execute(
            "UPDATE runtime_timed_exit SET last_runtime_at = ?, updated_at = ? WHERE pair_id = ?",
            (now.isoformat(), _now(), plan.pair_id),
        )
        self._connection.commit()
        if (
            row["lifecycle_state"] in {"CLOSING", "CLOSE_UNCERTAIN"}
            and timed["orphan_deadline_at"] is not None
        ):
            return self._close_orphan_survivor(plan, timed)
        if row["lifecycle_state"] in {"CLOSING", "CLOSE_UNCERTAIN"}:
            return self.converge_empty("runtime-close-convergence")
        if row["lifecycle_state"] == "ACTIVE":
            exit_due_at = datetime.fromisoformat(str(timed["exit_due_at"]))
            if now < exit_due_at:
                return PairExecutionResult("active", "ACTIVE", activated_at=activated_at)
            _LOGGER.info(
                "timed_exit_due pair_id=%s activated_at=%s exit_due_at=%s",
                plan.pair_id,
                activated_at.isoformat(),
                exit_due_at.isoformat(),
            )
            if market is None:
                _LOGGER.warning(
                    "timed_exit_fallback_close pair_id=%s reason=market_unavailable",
                    plan.pair_id,
                )
                self._set_state(
                    plan, "CLOSING", "EMPTY",
                    "Timed exit was due without fresh market evidence; converging empty.",
                )
                return self.converge_empty("timed-exit-market-unavailable")
            self._set_state(
                plan, "TIMED_EXIT_ARMING", "EMPTY",
                "Maximum holding age elapsed; gathering fresh timed exit evidence.",
            )
            calculated = self._timed_exit_targets(plan, policy, now, market)
            if calculated is None:
                _LOGGER.warning(
                    "timed_exit_fallback_close pair_id=%s reason=corridor_invalid",
                    plan.pair_id,
                )
                self._set_state(
                    plan, "CLOSING", "EMPTY",
                    "Timed exit corridor was invalid; converging empty.",
                )
                return self.converge_empty("timed-exit-corridor-invalid")
            targets, corridor = calculated
            return self._arm_timed_exit(plan, policy, now, targets, corridor)
        if row["lifecycle_state"] in {"TIMED_EXIT_ARMED", "ORPHAN_GRACE"}:
            return self._maintain_armed_exit(plan, policy, now, timed)
        if row["lifecycle_state"] == "TIMED_EXIT_ARMING":
            return self._recover_timed_exit_arming(plan, policy, now)
        return PairExecutionResult(
            "exiting", str(row["lifecycle_state"]), activated_at=activated_at
        )

    def _maintain_armed_exit(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
        timed: sqlite3.Row,
    ) -> PairExecutionResult:
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:timed-exit-maintain")
            for worker_id in self.worker_ids
        }
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return self._needs_human(
                plan, "Timed exit facts contain exposure not owned by the durable pair."
            )
        if any(owned["orders"] for owned in exposure.values()):
            self._set_state(
                plan, "CLOSING", "EMPTY",
                "Pair-owned orders appeared during timed exit.",
            )
            return self.converge_empty("timed-exit-owned-orders")
        if timed["first_target"] is None or timed["second_target"] is None:
            self._set_state(
                plan, "CLOSING", "EMPTY",
                "Timed exit targets are unavailable during armed maintenance.",
            )
            return self.converge_empty("timed-exit-targets-missing")
        targets = {
            "first": json.loads(str(timed["first_target"])),
            "second": json.loads(str(timed["second_target"])),
        }
        for name, leg in (("first", plan.first), ("second", plan.second)):
            positions = exposure[leg.worker_id]["positions"]
            if positions and not self._timed_position_matches(
                plan, leg, positions, targets[name]
            ):
                self._set_state(
                    plan, "CLOSING", "EMPTY",
                    f"Worker {leg.worker_id} timed exit position or protection diverged.",
                )
                return self.converge_empty("timed-exit-protection-divergence")
        present_workers = [
            worker_id
            for worker_id, owned in exposure.items()
            if owned["positions"]
        ]
        persisted_missing_worker = timed["missing_worker_id"]
        if persisted_missing_worker is not None and len(present_workers) == 2:
            return self._needs_human(
                plan, "A leg reappeared after authoritative orphan evidence."
            )
        if not present_workers:
            return self.converge_empty("timed-exit-natural-empty")
        if len(present_workers) == 1:
            missing_worker = next(
                worker_id for worker_id in self.worker_ids if worker_id not in present_workers
            )
            if (
                persisted_missing_worker is not None
                and str(persisted_missing_worker) != missing_worker
            ):
                return self._needs_human(
                    plan, "The missing timed-exit leg changed during orphan grace."
                )
            orphan_deadline_text = timed["orphan_deadline_at"]
            if orphan_deadline_text is None:
                orphan_deadline = now + timedelta(seconds=policy.orphan_grace_seconds)
                surviving_worker = present_workers[0]
                surviving_positions = exposure[surviving_worker]["positions"]
                if len(surviving_positions) != 1:
                    return self._needs_human(
                        plan, "Orphan evidence does not identify one surviving owned ticket."
                    )
                surviving_ticket = _ticket(surviving_positions[0])
                self._connection.execute(
                    """UPDATE runtime_timed_exit
                       SET phase = 'ORPHAN_GRACE', missing_worker_id = ?,
                           orphan_deadline_at = ?, surviving_worker_id = ?,
                           surviving_ticket = ?, updated_at = ? WHERE pair_id = ?""",
                    (
                        missing_worker,
                        orphan_deadline.isoformat(),
                        surviving_worker,
                        surviving_ticket,
                        _now(),
                        plan.pair_id,
                    ),
                )
                self._connection.commit()
                _LOGGER.info(
                    "timed_exit_orphan_grace pair_id=%s missing_worker=%s deadline=%s",
                    plan.pair_id,
                    missing_worker,
                    orphan_deadline.isoformat(),
                )
                self._set_state(
                    plan, "ORPHAN_GRACE", "EMPTY",
                    f"Worker {missing_worker} no longer reports its owned leg; orphan grace started.",
                )
                return PairExecutionResult("exiting", "ORPHAN_GRACE")
            orphan_deadline = datetime.fromisoformat(str(orphan_deadline_text))
            if now < orphan_deadline:
                return PairExecutionResult("exiting", "ORPHAN_GRACE")
            _LOGGER.warning(
                "timed_exit_orphan_deadline pair_id=%s remaining_worker=%s",
                plan.pair_id,
                present_workers[0],
            )
            return self._close_orphan_survivor(plan, timed)
        corridor_deadline = datetime.fromisoformat(str(timed["corridor_deadline_at"]))
        if now >= corridor_deadline:
            _LOGGER.warning(
                "timed_exit_corridor_deadline pair_id=%s deadline=%s",
                plan.pair_id,
                corridor_deadline.isoformat(),
            )
            self._set_state(
                plan, "CLOSING", "EMPTY",
                "Timed exit corridor deadline elapsed with both legs present.",
            )
            return self.converge_empty("timed-exit-corridor-deadline")
        return PairExecutionResult("exiting", "TIMED_EXIT_ARMED")

    def _close_orphan_survivor(
        self,
        plan: PairPlan,
        timed: sqlite3.Row,
    ) -> PairExecutionResult:
        surviving_worker = timed["surviving_worker_id"]
        surviving_ticket = timed["surviving_ticket"]
        missing_worker = timed["missing_worker_id"]
        if (
            not isinstance(surviving_worker, str)
            or isinstance(surviving_ticket, bool)
            or not isinstance(surviving_ticket, int)
            or not isinstance(missing_worker, str)
        ):
            return self._needs_human(plan, "Orphan close intent is incomplete.")
        facts = {
            worker_id: self._snapshot(
                worker_id, f"{plan.command_id}:orphan-close-observe"
            )
            for worker_id in self.worker_ids
        }
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return self._needs_human(
                plan, "Orphan close facts contain unowned exposure."
            )
        if exposure[missing_worker]["orders"] or exposure[missing_worker]["positions"]:
            return self._needs_human(
                plan, "The missing leg reappeared before orphan close."
            )
        surviving = exposure[surviving_worker]
        if surviving["orders"]:
            return self._needs_human(
                plan, "Owned orders appeared before orphan close."
            )
        if not surviving["positions"]:
            return self._finalize_empty(plan, "timed-exit-orphan-natural-empty")
        if (
            len(surviving["positions"]) != 1
            or _ticket(surviving["positions"][0]) != surviving_ticket
        ):
            return self._needs_human(
                plan, "The orphan survivor ticket changed before close."
            )
        volume = surviving["positions"][0].get("volume")
        if isinstance(volume, bool) or not isinstance(volume, (int, float)):
            return self._needs_human(
                plan, "The orphan survivor volume is unobservable."
            )
        self._set_state(
            plan, "CLOSING", "EMPTY",
            "Orphan grace elapsed; closing the exact surviving owned ticket.",
        )
        self._generic_effect(
            plan,
            surviving_worker,
            f"orphan-close:{surviving_ticket}",
            {
                "type": "close",
                "ticket": str(surviving_ticket),
                "volume": str(volume),
            },
        )
        final = {
            worker_id: self._snapshot(
                worker_id, f"{plan.command_id}:orphan-final-empty"
            )
            for worker_id in self.worker_ids
        }
        final_exposure = self._owned_exposure(plan, final)
        if final_exposure is None:
            return self._needs_human(plan, "Final orphan facts contain unowned exposure.")
        if any(
            owned["orders"] or owned["positions"]
            for owned in final_exposure.values()
        ):
            self._set_state(
                plan, "CLOSE_UNCERTAIN", "EMPTY",
                "The exact orphan survivor remains after close.",
            )
            return PairExecutionResult(
                "uncertain", "CLOSE_UNCERTAIN",
                reason="The exact orphan survivor remains after close.",
            )
        return self._finalize_empty(plan, "timed-exit-orphan-deadline")

    def _timed_position_matches(
        self,
        plan: PairPlan,
        leg: PairLegPlan,
        positions: list[dict[str, object]],
        target: dict[str, str],
    ) -> bool:
        if len(positions) != 1:
            return False
        position = positions[0]
        expected_type = 0 if leg.direction == "LONG" else 1
        return (
            _ticket(position) == self._verified_ticket(plan, leg.worker_id)
            and position.get("symbol") == leg.symbol
            and position.get("type") == expected_type
            and _number_equal(position.get("volume"), leg.volume)
            and _price_equal(position.get("sl"), target.get("sl"))
            and _price_equal(position.get("tp"), target.get("tp"))
        )

    def _record_activation(
        self,
        plan: PairPlan,
        verified_at: datetime | None = None,
    ) -> datetime:
        row = self._connection.execute(
            "SELECT activated_at FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
        ).fetchone()
        if row is not None:
            return datetime.fromisoformat(str(row[0]))
        activated_at = verified_at or self._effect_evidence_time(
            plan.command_id, ":entry"
        ) or datetime.now(UTC)
        if plan.timed_exit is not None:
            exit_due_at = activated_at + timedelta(
                seconds=plan.timed_exit.maximum_holding_seconds
            )
            self._connection.execute(
                """INSERT INTO runtime_timed_exit(
                       pair_id, activated_at, exit_due_at, phase, updated_at
                   ) VALUES (?, ?, ?, 'ACTIVE', ?)""",
                (
                    plan.pair_id,
                    activated_at.isoformat(),
                    exit_due_at.isoformat(),
                    _now(),
                ),
            )
            self._connection.commit()
        return activated_at

    def _effect_evidence_time(
        self,
        command_id: str,
        effect_suffix: str,
    ) -> datetime | None:
        rows = self._connection.execute(
            """SELECT updated_at FROM runtime_effects
               WHERE command_id = ? AND effect_id LIKE ?""",
            (command_id, f"{command_id}:%{effect_suffix}%"),
        ).fetchall()
        if not rows:
            return None
        return max(datetime.fromisoformat(str(row[0])) for row in rows)

    def _timed_exit_targets(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
        market: PairMarketObservation,
    ) -> tuple[dict[str, dict[str, str]], dict[str, object]] | None:
        legs = {"first": market.first, "second": market.second}
        constraints = self._current_symbol_constraints(plan)
        if constraints is None:
            return None
        for name, leg_plan in (("first", plan.first), ("second", plan.second)):
            evidence = legs[name]
            constraint = constraints[name]
            if evidence.observed_at.tzinfo is None:
                return None
            age = (now - evidence.observed_at.astimezone(UTC)).total_seconds()
            if (
                evidence.worker_id != leg_plan.worker_id
                or age < -1
                or age > policy.market_max_age_seconds
                or not all(math.isfinite(value) for value in (
                    evidence.bid, evidence.ask,
                    float(constraint["point"]), float(constraint["tick_size"]),
                ))
                or evidence.bid <= 0
                or evidence.ask <= evidence.bid
                or not evidence.recent_midpoint_moves
            ):
                return None
        spreads = [value.ask - value.bid for value in legs.values()]
        minimum_distances = [
            max(int(value["stops_level"]), int(value["freeze_level"]))
            * float(value["point"])
            + 2 * float(value["tick_size"])
            for value in constraints.values()
        ]
        movements = sorted(
            abs(move)
            for value in legs.values()
            for move in value.recent_midpoint_moves
            if math.isfinite(move)
        )
        if not movements:
            return None
        movement = movements[min(len(movements) - 1, math.ceil(len(movements) * 0.75) - 1)]
        distance = max(
            max(spreads) * policy.spread_multiplier,
            max(minimum_distances),
            movement * policy.movement_multiplier,
        )
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:timed-exit-observe")
            for worker_id in self.worker_ids
        }
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return None
        targets: dict[str, dict[str, str]] = {}
        for name, leg_plan in (("first", plan.first), ("second", plan.second)):
            positions = exposure[leg_plan.worker_id]["positions"]
            if len(positions) != 1 or exposure[leg_plan.worker_id]["orders"]:
                return None
            position = positions[0]
            if not self._active_position_matches(plan, leg_plan, position):
                return None
            evidence = legs[name]
            constraint = constraints[name]
            tick_size = float(constraint["tick_size"])
            digits = int(constraint["digits"])
            old_sl = _finite_float(position.get("sl"))
            if old_sl is None or old_sl <= 0:
                return None
            distance_decimal = Decimal(str(distance))
            if leg_plan.direction == "LONG":
                stop_loss = max(
                    Decimal(str(evidence.bid)) - distance_decimal,
                    Decimal(_round_price(
                        old_sl, tick_size, digits, ROUND_CEILING
                    )),
                )
                take_profit = Decimal(str(evidence.bid)) + distance_decimal
                stop_loss = _round_price(stop_loss, tick_size, digits, ROUND_FLOOR)
                take_profit = _round_price(take_profit, tick_size, digits, ROUND_CEILING)
            else:
                stop_loss = min(
                    Decimal(str(evidence.ask)) + distance_decimal,
                    Decimal(_round_price(
                        old_sl, tick_size, digits, ROUND_FLOOR
                    )),
                )
                take_profit = Decimal(str(evidence.ask)) - distance_decimal
                stop_loss = _round_price(stop_loss, tick_size, digits, ROUND_CEILING)
                take_profit = _round_price(take_profit, tick_size, digits, ROUND_FLOOR)
            minimum_distance = max(
                int(constraint["stops_level"]), int(constraint["freeze_level"])
            ) * float(constraint["point"])
            if (
                leg_plan.direction == "LONG"
                and (
                    evidence.bid - float(stop_loss) + 1e-12 < minimum_distance
                    or float(take_profit) - evidence.bid + 1e-12 < minimum_distance
                )
            ) or (
                leg_plan.direction == "SHORT"
                and (
                    float(stop_loss) - evidence.ask + 1e-12 < minimum_distance
                    or evidence.ask - float(take_profit) + 1e-12 < minimum_distance
                )
            ):
                return None
            targets[name] = {"sl": stop_loss, "tp": take_profit}
        first_midpoint = (market.first.bid + market.first.ask) / 2
        second_midpoint = (market.second.bid + market.second.ask) / 2
        reference_midpoint = (first_midpoint + second_midpoint) / 2
        corridor: dict[str, object] = {
            "reference_midpoint": str(reference_midpoint),
            "distance": str(distance),
            "first_basis": str(first_midpoint - reference_midpoint),
            "second_basis": str(second_midpoint - reference_midpoint),
            "first_observed_at": market.first.observed_at.astimezone(UTC).isoformat(),
            "second_observed_at": market.second.observed_at.astimezone(UTC).isoformat(),
        }
        return targets, corridor

    def _active_position_matches(
        self,
        plan: PairPlan,
        leg: PairLegPlan,
        position: dict[str, object],
    ) -> bool:
        expected_type = 0 if leg.direction == "LONG" else 1
        return (
            _ticket(position) == self._verified_ticket(plan, leg.worker_id)
            and position.get("symbol") == leg.symbol
            and position.get("type") == expected_type
            and _number_equal(position.get("volume"), leg.volume)
            and _price_equal(position.get("sl"), leg.stop_loss)
            and _price_equal(position.get("tp"), leg.take_profit)
        )

    def _current_symbol_constraints(
        self,
        plan: PairPlan,
    ) -> dict[str, dict[str, float | int]] | None:
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                name: executor.submit(
                    self.relay.symbol_info,
                    leg.worker_id,
                    symbol=leg.symbol,
                    runtime_command_id=plan.command_id,
                    request_id=f"{plan.command_id}:{name}:timed-exit-symbol",
                    correlation={
                        "pair_id": plan.pair_id,
                        "leg": name,
                        "purpose": "timed-exit-symbol-constraints",
                    },
                )
                for name, leg in (("first", plan.first), ("second", plan.second))
            }
            try:
                responses = {name: future.result() for name, future in futures.items()}
            except Exception as error:
                _LOGGER.warning(
                    "timed_exit_symbol_constraints_unavailable pair_id=%s reason=%s",
                    plan.pair_id,
                    error,
                )
                return None
        result: dict[str, dict[str, float | int]] = {}
        for name, leg in (("first", plan.first), ("second", plan.second)):
            symbol = responses[name].get("symbol")
            if not isinstance(symbol, dict) or symbol.get("name") != leg.symbol:
                return None
            digits = symbol.get("digits")
            stops_level = symbol.get("trade_stops_level")
            freeze_level = symbol.get("trade_freeze_level")
            point = _finite_float(symbol.get("point"))
            tick_size = _finite_float(symbol.get("trade_tick_size"))
            if (
                isinstance(digits, bool)
                or not isinstance(digits, int)
                or digits < 0
                or isinstance(stops_level, bool)
                or not isinstance(stops_level, int)
                or stops_level < 0
                or isinstance(freeze_level, bool)
                or not isinstance(freeze_level, int)
                or freeze_level < 0
                or point is None
                or point <= 0
                or tick_size is None
                or tick_size <= 0
            ):
                return None
            result[name] = {
                "digits": digits,
                "stops_level": stops_level,
                "freeze_level": freeze_level,
                "point": point,
                "tick_size": tick_size,
            }
        return result

    def _arm_timed_exit(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
        targets: dict[str, dict[str, str]],
        corridor: dict[str, object] | None = None,
    ) -> PairExecutionResult:
        self._connection.execute(
            """UPDATE runtime_timed_exit
               SET phase = 'TIMED_EXIT_ARMING', corridor = COALESCE(?, corridor),
                   first_target = ?, second_target = ?,
                   updated_at = ? WHERE pair_id = ?""",
            (
                _canonical(corridor) if corridor is not None else None,
                _canonical(targets["first"]),
                _canonical(targets["second"]),
                _now(),
                plan.pair_id,
            ),
        )
        self._connection.commit()
        _LOGGER.info(
            "timed_exit_corridor_prepared pair_id=%s first_sl=%s first_tp=%s "
            "second_sl=%s second_tp=%s",
            plan.pair_id,
            targets["first"]["sl"],
            targets["first"]["tp"],
            targets["second"]["sl"],
            targets["second"]["tp"],
        )
        self._set_state(
            plan, "TIMED_EXIT_ARMING", "EMPTY",
            "Maximum holding age elapsed; timed exit corridor is being armed.",
        )
        prepared: dict[str, tuple[PairLegPlan, str, dict[str, object]]] = {}
        for name, leg in (("first", plan.first), ("second", plan.second)):
            effect_id = self._prepared_or_next_effect_id(
                plan.command_id, leg.worker_id, "timed-exit-arm"
            )
            target = targets[name]
            operation = {
                "type": "modify_sl_tp",
                "symbol": leg.symbol,
                "position": str(self._verified_ticket(plan, leg.worker_id)),
                "sl": target["sl"],
                "tp": target["tp"],
            }
            self._prepare_effect(effect_id, plan.command_id, leg.worker_id, operation)
            prepared[name] = (leg, effect_id, operation)
        self._connection.executemany(
            """UPDATE runtime_effects SET delivery_state = 'dispatching', updated_at = ?
               WHERE effect_id = ? AND delivery_state = 'prepared'""",
            [(_now(), effect_id) for _leg, effect_id, _operation in prepared.values()],
        )
        self._connection.commit()
        with ThreadPoolExecutor(max_workers=2) as executor:
            futures = {
                name: executor.submit(
                    self._safe_operate, plan, leg, effect_id, operation, "timed-exit-arm"
                )
                for name, (leg, effect_id, operation) in prepared.items()
            }
            outcomes = {name: future.result() for name, future in futures.items()}
        for name, outcome in outcomes.items():
            self._record_effect_outcome(prepared[name][1], outcome)
        self._persist_arm_verification_started(plan, policy, now)
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:timed-exit-verify")
            for worker_id in self.worker_ids
        }
        return self._finish_timed_exit_arming(
            plan, policy, now, targets, facts, verified_at=now
        )

    def _finish_timed_exit_arming(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
        targets: dict[str, dict[str, str]],
        facts: dict[str, dict[str, list[dict[str, object]]]],
        *,
        verified_at: datetime | None = None,
    ) -> PairExecutionResult:
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return self._needs_human(
                plan, "Timed exit verification found exposure not owned by the durable pair."
            )
        present = [
            (name, leg, exposure[leg.worker_id]["positions"])
            for name, leg in (("first", plan.first), ("second", plan.second))
            if exposure[leg.worker_id]["positions"]
        ]
        protection_verified = (
            not any(owned["orders"] for owned in exposure.values())
            and all(
                self._timed_position_matches(plan, leg, positions, targets[name])
                for name, leg, positions in present
            )
        )
        if not protection_verified:
            self._set_state(
                plan, "CLOSING", "EMPTY",
                "Timed exit corridor was not broker-verified on both legs.",
            )
            return self.converge_empty("timed-exit-arm-failed")
        self._mark_timed_exit_armed(plan, policy, now, verified_at=verified_at)
        if len(present) < 2:
            timed = self._connection.execute(
                "SELECT * FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
            ).fetchone()
            assert timed is not None
            return self._maintain_armed_exit(plan, policy, now, timed)
        return PairExecutionResult("exiting", "TIMED_EXIT_ARMED")

    def _mark_timed_exit_armed(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
        *,
        verified_at: datetime | None = None,
    ) -> None:
        existing = self._connection.execute(
            """SELECT armed_at, corridor_deadline_at FROM runtime_timed_exit
               WHERE pair_id = ?""",
            (plan.pair_id,),
        ).fetchone()
        armed_at = (
            datetime.fromisoformat(str(existing["armed_at"]))
            if existing is not None and existing["armed_at"] is not None
            else verified_at
            or self._effect_evidence_time(plan.command_id, ":timed-exit-arm:")
            or now
        )
        corridor_deadline = (
            datetime.fromisoformat(str(existing["corridor_deadline_at"]))
            if existing is not None and existing["corridor_deadline_at"] is not None
            else armed_at + timedelta(seconds=policy.corridor_deadline_seconds)
        )
        self._connection.execute(
            """UPDATE runtime_timed_exit
               SET phase = 'TIMED_EXIT_ARMED', armed_at = ?,
                   corridor_deadline_at = ?, updated_at = ? WHERE pair_id = ?""",
            (armed_at.isoformat(), corridor_deadline.isoformat(), _now(), plan.pair_id),
        )
        self._set_state(
            plan, "TIMED_EXIT_ARMED", "EMPTY",
            "Fresh Worker facts prove timed exit protection on every remaining owned leg.",
        )
        _LOGGER.info(
            "timed_exit_armed pair_id=%s corridor_deadline=%s",
            plan.pair_id,
            corridor_deadline.isoformat(),
        )

    def _persist_arm_verification_started(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
    ) -> None:
        self._connection.execute(
            """UPDATE runtime_timed_exit
               SET armed_at = COALESCE(armed_at, ?),
                   corridor_deadline_at = COALESCE(corridor_deadline_at, ?),
                   updated_at = ?
               WHERE pair_id = ?""",
            (
                now.isoformat(),
                (now + timedelta(seconds=policy.corridor_deadline_seconds)).isoformat(),
                _now(),
                plan.pair_id,
            ),
        )
        self._connection.commit()

    def _recover_timed_exit_arming(
        self,
        plan: PairPlan,
        policy: TimedExitPolicy,
        now: datetime,
    ) -> PairExecutionResult:
        timed = self._connection.execute(
            "SELECT * FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
        ).fetchone()
        if timed is None or timed["first_target"] is None or timed["second_target"] is None:
            self._set_state(
                plan, "CLOSING", "EMPTY",
                "Timed exit arming state has no durable targets.",
            )
            return self.converge_empty("timed-exit-recovery-missing-targets")
        targets = {
            "first": json.loads(str(timed["first_target"])),
            "second": json.loads(str(timed["second_target"])),
        }
        self._persist_arm_verification_started(plan, policy, now)
        facts = {
            worker_id: self._snapshot(worker_id, f"{plan.command_id}:timed-exit-recover")
            for worker_id in self.worker_ids
        }
        rows = self._connection.execute(
            """SELECT delivery_state FROM runtime_effects
               WHERE command_id = ? AND effect_id LIKE ?""",
            (plan.command_id, f"{plan.command_id}:%:timed-exit-arm:attempt-%"),
        ).fetchall()
        if (
            all(str(row[0]) == "prepared" for row in rows)
            and not self._protection_matches(plan, facts, targets)
        ):
            return self._arm_timed_exit(plan, policy, now, targets)
        return self._finish_timed_exit_arming(plan, policy, now, targets, facts)

    def _safe_operate(
        self,
        plan: PairPlan,
        leg: PairLegPlan,
        effect_id: str,
        operation: dict[str, object],
        purpose: str,
    ) -> dict[str, object]:
        try:
            outcome = self.relay.operate(
                leg.worker_id,
                operation=operation,
                runtime_command_id=plan.command_id,
                request_id=effect_id,
                effect_id=effect_id,
                expires_at=datetime.now(UTC) + timedelta(seconds=30),
                correlation={"pair_id": plan.pair_id, "leg": leg.name, "purpose": purpose},
            )
        except Exception as error:
            return {
                "accepted": False,
                "execution_state": "sent",
                "outcome": "unknown_after_send",
                "result": {},
                "reason": str(error),
            }
        if "accepted" in outcome:
            return outcome
        return {
            "accepted": True,
            "execution_state": "receipt",
            "outcome": "accepted",
            "result": outcome,
        }

    def _verified_ticket(self, plan: PairPlan, worker_id: str) -> int:
        row = self._connection.execute(
            "SELECT ticket FROM runtime_verified_legs WHERE pair_id = ? AND worker_id = ?",
            (plan.pair_id, worker_id),
        ).fetchone()
        if row is None:
            raise StrategyRuntimeError("Timed exit requires a durable verified ticket.")
        return int(row[0])

    def _protection_matches(
        self,
        plan: PairPlan,
        facts: dict[str, dict[str, list[dict[str, object]]]],
        targets: dict[str, dict[str, str]],
    ) -> bool:
        exposure = self._owned_exposure(plan, facts)
        if exposure is None:
            return False
        for name, leg in (("first", plan.first), ("second", plan.second)):
            positions = exposure[leg.worker_id]["positions"]
            if len(positions) != 1 or exposure[leg.worker_id]["orders"]:
                return False
            if (
                not _price_equal(positions[0].get("sl"), targets[name]["sl"])
                or not _price_equal(positions[0].get("tp"), targets[name]["tp"])
            ):
                return False
        return True

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
        closes: list[tuple[PairLegPlan, str, dict[str, object]]] = []
        legs_by_worker = {
            leg.worker_id: leg for leg in (plan.first, plan.second)
        }
        for worker_id, owned in exposure.items():
            for position in owned["positions"]:
                ticket = _ticket(position)
                volume = position.get("volume")
                if isinstance(volume, bool) or not isinstance(volume, (int, float)):
                    return self._needs_human(plan, "Pair-owned position volume is unobservable.")
                closes.append(
                    (
                        legs_by_worker[worker_id],
                        f"close:{ticket}",
                        {"type": "close", "ticket": str(ticket), "volume": str(volume)},
                    )
                )
        self._generic_effects_concurrently(plan, closes)
        final = {worker_id: self._snapshot(worker_id, f"{plan.command_id}:final-empty") for worker_id in self.worker_ids}
        final_exposure = self._owned_exposure(plan, final)
        if final_exposure is None:
            return self._needs_human(plan, "Final facts contain unowned exposure.")
        if any(owned["orders"] or owned["positions"] for owned in final_exposure.values()):
            self._set_state(plan, "CLOSE_UNCERTAIN", "EMPTY", f"Broker facts remain non-empty after {reason}.")
            return PairExecutionResult("uncertain", "CLOSE_UNCERTAIN", reason="Broker facts remain non-empty.")
        return self._finalize_empty(plan, reason)

    def _finalize_empty(
        self,
        plan: PairPlan,
        reason: str,
    ) -> PairExecutionResult:
        self._record_transition(plan.pair_id, "EMPTY", "EMPTY", f"Broker-verified empty after {reason}.")
        self._connection.execute("DELETE FROM pair_runtime WHERE singleton = 1")
        self._connection.execute(
            "DELETE FROM runtime_timed_exit WHERE pair_id = ?", (plan.pair_id,)
        )
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

    def _generic_effects_concurrently(
        self,
        plan: PairPlan,
        effects: list[tuple[PairLegPlan, str, dict[str, object]]],
    ) -> None:
        if not effects:
            return
        prepared: list[tuple[PairLegPlan, str, str, dict[str, object]]] = []
        for leg, suffix, request in effects:
            effect_id = self._next_effect_id(plan.command_id, leg.worker_id, suffix)
            self._prepare_effect(effect_id, plan.command_id, leg.worker_id, request)
            prepared.append((leg, suffix, effect_id, request))
        self._connection.executemany(
            """UPDATE runtime_effects SET delivery_state = 'dispatching', updated_at = ?
               WHERE effect_id = ? AND delivery_state = 'prepared'""",
            [(_now(), effect_id) for _leg, _suffix, effect_id, _request in prepared],
        )
        self._connection.commit()
        with ThreadPoolExecutor(max_workers=len(prepared)) as executor:
            futures = [
                (
                    effect_id,
                    executor.submit(
                        self._safe_operate,
                        plan,
                        leg,
                        effect_id,
                        request,
                        suffix,
                    ),
                )
                for leg, suffix, effect_id, request in prepared
            ]
            outcomes = [
                (effect_id, future.result()) for effect_id, future in futures
            ]
        for effect_id, outcome in outcomes:
            self._record_effect_outcome(effect_id, outcome)

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

    def _prepared_or_next_effect_id(
        self, command_id: str, worker_id: str, suffix: str
    ) -> str:
        prefix = f"{command_id}:{worker_id}:{suffix}:attempt-"
        row = self._connection.execute(
            """SELECT effect_id FROM runtime_effects
               WHERE command_id = ? AND worker_id = ? AND effect_id LIKE ?
                 AND delivery_state = 'prepared'
               ORDER BY effect_id DESC LIMIT 1""",
            (command_id, worker_id, f"{prefix}%"),
        ).fetchone()
        return str(row[0]) if row is not None else self._next_effect_id(
            command_id, worker_id, suffix
        )

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
                or not _price_equal(position.get("sl"), leg.stop_loss)
                or not _price_equal(position.get("tp"), leg.take_profit)
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


def _price_equal(value: object, expected: object) -> bool:
    if isinstance(value, bool) or isinstance(expected, bool):
        return False
    try:
        actual_price = Decimal(str(value))
        expected_price = Decimal(str(expected))
    except (InvalidOperation, ValueError):
        return False
    return (
        actual_price.is_finite()
        and expected_price.is_finite()
        and actual_price == expected_price
    )


def _plan_json(plan: PairPlan) -> dict[str, object]:
    return {
        "pair_id": plan.pair_id,
        "command_id": plan.command_id,
        "direction": plan.direction,
        "expires_at": plan.expires_at.isoformat(),
        "first": asdict(plan.first),
        "second": asdict(plan.second),
        "timed_exit": asdict(plan.timed_exit) if plan.timed_exit is not None else None,
    }


def _plan(value: dict[str, object]) -> PairPlan:
    return PairPlan(
        pair_id=str(value["pair_id"]),
        command_id=str(value["command_id"]),
        direction=str(value["direction"]),
        expires_at=datetime.fromisoformat(str(value["expires_at"])),
        first=PairLegPlan(**value["first"]),  # type: ignore[arg-type]
        second=PairLegPlan(**value["second"]),  # type: ignore[arg-type]
        timed_exit=(
            TimedExitPolicy(**value["timed_exit"])  # type: ignore[arg-type]
            if isinstance(value.get("timed_exit"), dict)
            else None
        ),
    )


def _canonical(value: dict[str, object]) -> str:
    return json.dumps(value, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _hash(value: str) -> str:
    return sha256(value.encode("utf-8")).hexdigest()


def _now() -> str:
    return datetime.now(UTC).isoformat()


def _finite_float(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(str(value))
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _round_price(
    value: Decimal | float,
    tick_size: float,
    digits: int,
    rounding: str,
) -> str:
    tick = Decimal(str(tick_size))
    ticks = (Decimal(str(value)) / tick).to_integral_value(rounding=rounding)
    rounded = ticks * tick
    quantum = Decimal(1).scaleb(-digits)
    return format(rounded.quantize(quantum), f".{digits}f")
