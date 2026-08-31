"""Low-Latency Pair Execution Cell.

One deep module runs on both paired account Workers and preserves exactly one
protected-pair lifecycle owner while colocating entry decisions with
account-local MT5 execution.  See ``.scratch/low-latency-pair-execution-cell``
and ADR-0010 for the product and architecture rationale this module
implements; it supersedes ADR-0009's owner placement only for pairs that opt
into this module ("Pair Execution Cell pairs").

Both paired Workers construct the identical :class:`PairExecutionCell` class;
an immutable leased role (``leader``/``follower``) selects behavior.  The
external interface has exactly three lifecycle entry points:

* :meth:`PairExecutionCell.activate` - bind an immutable pair contract and
  lease.
* :meth:`PairExecutionCell.handle_event` - accept one typed local, peer,
  broker, or clock event and return the current observable result.
* :meth:`PairExecutionCell.recover` - resume durable state after a process
  restart before any new quote is evaluated.

Callers never manage arming, effect IDs, retries, protection refinement, or
compensation directly; those mechanics are hidden behind this seam.  Durable
state (lease epoch, contract, readiness, quotes, attempts, effects, outcomes,
desired state, deadlines, and recovery) is kept in a private SQLite/WAL
database.  Broker-side effects reuse the Worker's existing
:class:`~abt.worker.effect_journal.WorkerEffectJournal` (ADR-0008 no-blind
-retry semantics) and :class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler`
(priority and expiry-before-send enforcement) so pair-cell entries share the
same durability and serialization invariants as every other Worker broker
write.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from hashlib import sha256
import json
import math
import sqlite3
from pathlib import Path
from typing import Literal, Protocol, cast
from uuid import uuid4
from zoneinfo import ZoneInfo

from .worker.effect_journal import EffectJournalError, WorkerEffectJournal
from .worker.scheduler import (
    BrokerActionNotStarted,
    DeadlineAwareTraderRpcScheduler,
    ScheduledTraderRpc,
    TraderRpcOutcome,
    TraderRpcOutcomeCategory,
)


class PairExecutionCellError(RuntimeError):
    """Raised for malformed activation, contract, lease, or event input."""


Role = Literal["leader", "follower"]
ExecutionMode = Literal["live", "shadow"]
Direction = Literal["LONG", "SHORT"]
_MAXIMUM_HOLDING_SECONDS = 7 * 24 * 60 * 60
PairState = Literal[
    "IDLE",
    "ARMING",
    "ARMED",
    "SENDING",
    "ENTRY_UNCERTAIN",
    "VERIFYING_PROTECTION",
    "ACTIVE",
    "CONVERGING_EMPTY",
    "EMPTY",
    "NEEDS_HUMAN",
]
DesiredState = Literal["NONE", "ACTIVE", "EMPTY"]


# --------------------------------------------------------------------------- #
# Immutable contract, lease, and quote types
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class PairContract:
    """Immutable pair policy snapshot bound to one lease epoch's contract hash."""

    contract_id: str
    leader_worker_id: str
    follower_worker_id: str
    trader_id: str
    leader_volume: str
    follower_volume: str
    filling_mode: Literal["FOK", "IOC"]
    risk_budget_usd: str
    quote_max_age_seconds: float
    quote_max_skew_seconds: float
    arm_timeout_seconds: float
    commit_lead_seconds: float
    execution_expiry_seconds: float
    mode: ExecutionMode = "live"
    maximum_holding_seconds: float | None = None
    flatten_at_ny: str | None = None
    entry_edge_points: str | None = None
    eligible_symbols: tuple[str, ...] = ()
    symbol: str | None = None
    leader_direction: Direction | None = None
    follower_direction: Direction | None = None
    edge_threshold: str | None = None

    def canonical(self) -> dict[str, object]:
        value: dict[str, object] = {
            "contract_id": self.contract_id,
            "leader_worker_id": self.leader_worker_id,
            "follower_worker_id": self.follower_worker_id,
            "trader_id": self.trader_id,
            "leader_volume": self.leader_volume,
            "follower_volume": self.follower_volume,
            "filling_mode": self.filling_mode,
            "risk_budget_usd": self.risk_budget_usd,
            "quote_max_age_seconds": self.quote_max_age_seconds,
            "quote_max_skew_seconds": self.quote_max_skew_seconds,
            "arm_timeout_seconds": self.arm_timeout_seconds,
            "commit_lead_seconds": self.commit_lead_seconds,
            "execution_expiry_seconds": self.execution_expiry_seconds,
            "mode": self.mode,
        }
        if self.maximum_holding_seconds is not None:
            value["maximum_holding_seconds"] = self.maximum_holding_seconds
        if self.flatten_at_ny is not None:
            value["flatten_at_ny"] = self.flatten_at_ny
        if self.entry_edge_points is not None:
            value["entry_edge_points"] = self.entry_edge_points
            value["eligible_symbols"] = list(self.eligible_symbols)
        if self.symbol is not None:
            value["symbol"] = self.symbol
        if self.leader_direction is not None:
            value["leader_direction"] = self.leader_direction
        if self.follower_direction is not None:
            value["follower_direction"] = self.follower_direction
        if self.edge_threshold is not None:
            value["edge_threshold"] = self.edge_threshold
        return value

    @property
    def hash(self) -> str:
        return _hash(_canonical_json(self.canonical()))

    def peer_of(self, worker_id: str) -> str:
        if worker_id == self.leader_worker_id:
            return self.follower_worker_id
        if worker_id == self.follower_worker_id:
            return self.leader_worker_id
        raise PairExecutionCellError("Worker ID is not part of this pair contract.")

    def role_of(self, worker_id: str) -> Role:
        if worker_id == self.leader_worker_id:
            return "leader"
        if worker_id == self.follower_worker_id:
            return "follower"
        raise PairExecutionCellError("Worker ID is not part of this pair contract.")

    def direction_of(self, worker_id: str) -> Direction:
        direction = self.leader_direction if self.role_of(worker_id) == "leader" else self.follower_direction
        if direction is None:
            raise PairExecutionCellError("Automatic pair contracts choose direction per attempt.")
        return direction

    def volume_of(self, worker_id: str) -> str:
        return self.leader_volume if self.role_of(worker_id) == "leader" else self.follower_volume


@dataclass(frozen=True, slots=True)
class PairLease:
    """An epoch-fenced, controller-issued authority binding one Worker pair."""

    leader_worker_id: str
    follower_worker_id: str
    trader_id: str
    contract_hash: str
    epoch: int
    issued_at: datetime
    expires_at: datetime

    def active(self, now: datetime) -> bool:
        return now < self.expires_at


@dataclass(frozen=True, slots=True)
class QuoteSnapshot:
    worker_id: str
    symbol: str
    bid: Decimal
    ask: Decimal
    broker_time: datetime
    local_receive_time: datetime
    recovery_epoch: str
    sequence: int

    def fresh(self, now: datetime, max_age_seconds: float) -> bool:
        if self.broker_time > now + timedelta(seconds=1):
            return False  # a broker time ahead of calibrated now is not trustworthy evidence
        return (now - self.broker_time).total_seconds() <= max_age_seconds

    def canonical(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "symbol": self.symbol,
            "bid": str(self.bid),
            "ask": str(self.ask),
            "broker_time": self.broker_time.isoformat(),
            "recovery_epoch": self.recovery_epoch,
            "sequence": self.sequence,
        }


# --------------------------------------------------------------------------- #
# Typed events
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class LocalQuoteEvent:
    symbol: str
    bid: Decimal
    ask: Decimal
    broker_time: datetime
    local_receive_time: datetime
    recovery_epoch: str
    sequence: int


@dataclass(frozen=True, slots=True)
class LocalQuoteBatchEvent:
    quotes: tuple[LocalQuoteEvent, ...]


@dataclass(frozen=True, slots=True)
class ProtectionCalibration:
    """Symbol constraints refreshed on a bounded schedule, off the critical path."""

    tick_size: str
    point: str
    minimum_stop_distance: str
    profit_tick_value: str
    loss_tick_value: str


@dataclass(frozen=True, slots=True)
class ReadinessFactsEvent:
    """Continuously maintained local account facts; never a signal-time read."""

    account_login: str
    account_server: str
    terminal_connected: bool
    orders: list[dict[str, object]] | None
    positions: list[dict[str, object]] | None
    margin_headroom_ok: bool
    symbol_constraints_fresh: bool
    protection_calibration: ProtectionCalibration | None
    observed_at: datetime
    symbol_calibrations: dict[str, ProtectionCalibration] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class LeaseEvent:
    lease: PairLease | None  # None represents lease revocation


@dataclass(frozen=True, slots=True)
class BrokerSnapshotEvent:
    orders: list[dict[str, object]] | None
    positions: list[dict[str, object]] | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ClockTickEvent:
    now: datetime


@dataclass(frozen=True, slots=True)
class QuarantineReleaseEvent:
    symbol: str
    actor: str
    reason: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class RelayEnvelopeReceived:
    """An authenticated, already identity/route/lease-validated opaque envelope."""

    envelope: Mapping[str, object]


PairCellEvent = (
    LocalQuoteEvent
    | LocalQuoteBatchEvent
    | ReadinessFactsEvent
    | LeaseEvent
    | BrokerSnapshotEvent
    | ClockTickEvent
    | QuarantineReleaseEvent
    | RelayEnvelopeReceived
)


# --------------------------------------------------------------------------- #
# Adapter seams (production implementations are injected; tests use fakes)
# --------------------------------------------------------------------------- #


class PairRelayAdapter(Protocol):
    """Sends one opaque envelope toward the peer Worker via the controller."""

    def send(self, envelope: dict[str, object]) -> None: ...


class PairCellMT5(Protocol):
    """The broker access surface the cell drives directly, serialized per Worker."""

    def account_info(self) -> object: ...

    def positions_get(self) -> object: ...

    def orders_get(self) -> object: ...

    def symbol_info(self, symbol: str) -> object: ...

    def order_send(self, request: dict[str, object]) -> object: ...


@dataclass(frozen=True, slots=True)
class PairResult:
    """The current observable pair result; no internal names are exposed."""

    worker_id: str
    role: Role | None
    lease_epoch: int | None
    state: PairState
    ready: bool
    ready_reason: str
    attempt_id: str | None
    desired_state: DesiredState
    needs_human: bool
    needs_human_reason: str | None
    recovering: bool
    timing: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BrokerWriteResult:
    category: Literal["completed", "rejected", "expired_not_started", "unknown_after_send"]
    receipt: dict[str, object] | None = None


# --------------------------------------------------------------------------- #
# Pure helpers: quote coherence, edge, and protection arithmetic
# --------------------------------------------------------------------------- #


def _hash(canonical: str) -> str:
    return sha256(canonical.encode("utf-8")).hexdigest()


def _canonical_json(payload: object) -> str:
    return json.dumps(payload, separators=(",", ":"), sort_keys=True, allow_nan=False)


def _to_decimal(value: object) -> Decimal | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, Decimal):
        return value if value.is_finite() else None
    if isinstance(value, int):
        return Decimal(value)
    if isinstance(value, float):
        if not math.isfinite(value):
            return None
        return Decimal(str(value))
    if isinstance(value, str):
        try:
            result = Decimal(value)
        except (InvalidOperation, ValueError):
            return None
        return result if result.is_finite() else None
    return None


def edge_value(leader_quote: QuoteSnapshot, follower_quote: QuoteSnapshot, leader_direction: Direction) -> Decimal:
    """The favorable price differential of executing both assigned legs now."""

    if leader_direction == "LONG":
        return follower_quote.bid - leader_quote.ask
    return leader_quote.bid - follower_quote.ask


def compute_protection(
    *,
    entry: Decimal,
    direction: Direction,
    volume: Decimal,
    calibration: ProtectionCalibration,
    risk_budget_usd: Decimal,
) -> tuple[str, str] | None:
    """Conservative emergency/refined SL/TP from current constraints, or ``None``."""

    tick_size = _to_decimal(calibration.tick_size)
    minimum_stop_distance = _to_decimal(calibration.minimum_stop_distance)
    profit_tick_value = _to_decimal(calibration.profit_tick_value)
    loss_tick_value = _to_decimal(calibration.loss_tick_value)
    if (
        tick_size is None or tick_size <= 0
        or minimum_stop_distance is None or minimum_stop_distance < 0
        or profit_tick_value is None or profit_tick_value <= 0
        or loss_tick_value is None or loss_tick_value <= 0
        or volume <= 0
    ):
        return None
    loss_ticks = int((risk_budget_usd / (loss_tick_value * volume)).to_integral_value(rounding="ROUND_FLOOR"))
    profit_ticks = int((risk_budget_usd / (profit_tick_value * volume)).to_integral_value(rounding="ROUND_FLOOR"))
    if loss_ticks < 1 or profit_ticks < 1:
        return None
    loss_distance = loss_ticks * tick_size
    profit_distance = profit_ticks * tick_size
    if loss_distance < minimum_stop_distance or profit_distance < minimum_stop_distance:
        return None
    if direction == "LONG":
        sl = _round_to_tick(entry - loss_distance, tick_size, ROUND_CEILING)
        tp = _round_to_tick(entry + profit_distance, tick_size, ROUND_FLOOR)
    else:
        sl = _round_to_tick(entry + loss_distance, tick_size, ROUND_FLOOR)
        tp = _round_to_tick(entry - profit_distance, tick_size, ROUND_CEILING)
    if sl <= 0 or tp <= 0:
        return None
    return str(sl), str(tp)


def _round_to_tick(price: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return (price / tick).to_integral_value(rounding=rounding) * tick


# --------------------------------------------------------------------------- #
# Durable attempt record
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Attempt:
    """Immutable, identical on both Workers; per-role emergency protection is
    computed locally by each side and never transmitted."""

    attempt_id: str
    lease_epoch: int
    contract_hash: str
    symbol: str
    leader_direction: Direction
    follower_direction: Direction
    leader_volume: str
    follower_volume: str
    leader_quote: QuoteSnapshot
    follower_quote: QuoteSnapshot
    decision_time: datetime
    arm_deadline: datetime
    execute_at: datetime
    expires_at: datetime

    def canonical(self) -> dict[str, object]:
        return {
            "attempt_id": self.attempt_id,
            "lease_epoch": self.lease_epoch,
            "contract_hash": self.contract_hash,
            "symbol": self.symbol,
            "leader_direction": self.leader_direction,
            "follower_direction": self.follower_direction,
            "leader_volume": self.leader_volume,
            "follower_volume": self.follower_volume,
            "leader_quote": self.leader_quote.canonical(),
            "follower_quote": self.follower_quote.canonical(),
            "decision_time": self.decision_time.isoformat(),
            "arm_deadline": self.arm_deadline.isoformat(),
            "execute_at": self.execute_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }

    @property
    def payload_hash(self) -> str:
        return _hash(_canonical_json(self.canonical()))

    def direction_of(self, role: Role) -> Direction:
        return self.leader_direction if role == "leader" else self.follower_direction

    def volume_of(self, role: Role) -> str:
        return self.leader_volume if role == "leader" else self.follower_volume

    def quote_of(self, role: Role) -> QuoteSnapshot:
        return self.leader_quote if role == "leader" else self.follower_quote


def _attempt_from_canonical(value: dict[str, object]) -> Attempt:
    def _quote(raw: dict[str, object]) -> QuoteSnapshot:
        return QuoteSnapshot(
            worker_id=cast(str, raw["worker_id"]),
            symbol=cast(str, raw["symbol"]),
            bid=Decimal(cast(str, raw["bid"])),
            ask=Decimal(cast(str, raw["ask"])),
            broker_time=_parse_utc(cast(str, raw["broker_time"])),
            local_receive_time=_parse_utc(cast(str, raw["broker_time"])),
            recovery_epoch=cast(str, raw["recovery_epoch"]),
            sequence=cast(int, raw["sequence"]),
        )

    return Attempt(
        attempt_id=cast(str, value["attempt_id"]),
        lease_epoch=cast(int, value["lease_epoch"]),
        contract_hash=cast(str, value["contract_hash"]),
        symbol=cast(str, value["symbol"]),
        leader_direction=cast(Direction, value["leader_direction"]),
        follower_direction=cast(Direction, value["follower_direction"]),
        leader_volume=cast(str, value["leader_volume"]),
        follower_volume=cast(str, value["follower_volume"]),
        leader_quote=_quote(cast(dict[str, object], value["leader_quote"])),
        follower_quote=_quote(cast(dict[str, object], value["follower_quote"])),
        decision_time=_parse_utc(cast(str, value["decision_time"])),
        arm_deadline=_parse_utc(cast(str, value["arm_deadline"])),
        execute_at=_parse_utc(cast(str, value["execute_at"])),
        expires_at=_parse_utc(cast(str, value["expires_at"])),
    )


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise PairExecutionCellError("A persisted or relayed timestamp is missing its UTC offset.")
    return parsed.astimezone(UTC)


LegEntryStatus = Literal["pending", "sent", "rejected", "expired_not_started", "unknown_after_send", "filled"]
LegProtectionStatus = Literal["pending", "refined", "failed"]


@dataclass(slots=True)
class LegState:
    attempt_id: str
    role: Role
    arm_state: Literal["pending", "armed", "rejected"] = "pending"
    arm_reason: str = ""
    # Durable, exact-attempt commit authorization.  Arming alone never permits
    # a broker send: the follower sets this only when it validates the
    # leader's commit for this exact attempt, and the leader sets it only when
    # it issues that commit.  A dropped arm_ack or commit therefore leaves both
    # legs unauthorized instead of producing a unilateral entry.
    commit_authorized: bool = False
    emergency_sl: str | None = None
    emergency_tp: str | None = None
    entry_effect_id: str | None = None
    entry_status: LegEntryStatus = "pending"
    ticket: str | None = None
    fill_price: str | None = None
    protection_effect_id: str | None = None
    protection_status: LegProtectionStatus = "pending"
    refined_sl: str | None = None
    refined_tp: str | None = None
    cancel_effect_id: str | None = None
    close_effect_id: str | None = None
    empty_verified: bool = False


@dataclass(slots=True)
class PeerLegView:
    """The peer's self-reported leg status, relayed opaquely through the controller."""

    status: str = "unknown"
    ticket: str | None = None
    fill_price: str | None = None
    sl: str | None = None
    tp: str | None = None
    reason: str = ""


# --------------------------------------------------------------------------- #
# The deep module
# --------------------------------------------------------------------------- #


class PairExecutionCell:
    """Runs as leader or follower for one Pair Execution Cell pair.

    Construct once per Worker process per opted-in pair with this Worker's own
    durable adapters (its single effect journal and RPC scheduler), then call
    :meth:`activate` (first run) or :meth:`recover` (process restart) before
    feeding events via :meth:`handle_event`.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        db_path: Path,
        relay: PairRelayAdapter,
        mt5: PairCellMT5,
        effect_journal: WorkerEffectJournal,
        scheduler: DeadlineAwareTraderRpcScheduler,
        serve_foreign_scheduler_item: Callable[[ScheduledTraderRpc], None] | None = None,
        dispatch_foreign_scheduler_outcome: Callable[[TraderRpcOutcome], None] | None = None,
    ) -> None:
        """``scheduler`` must be the same
        :class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler` instance
        the rest of this Worker process uses for ordinary Trader RPC and hedge
        work, never a private instance -- the scheduler's single ``_active``
        slot is the Worker's actual MT5 serialization boundary, and priority
        (``emergency`` > ``execution`` > ``protection`` > ``normal`` >
        ``background``) is only meaningful when every source of broker writes
        shares one queue.

        Because ``scheduler.next()`` returns the highest-priority *globally*
        ready item -- not necessarily the item this cell just admitted --
        ``serve_foreign_scheduler_item`` and ``dispatch_foreign_scheduler_outcome``
        let the production wiring fully serve any other Worker traffic (an
        ordinary Trader RPC or hedge request, or that traffic's own terminal
        outcome) that surfaces first, so this cell keeps draining the shared
        queue in priority order instead of misappropriating someone else's
        dequeued request.  Tests may omit both: a scheduler dedicated to one
        cell under test never yields foreign work.
        """

        self._worker_id = worker_id
        self._relay = relay
        self._mt5 = mt5
        self._journal = effect_journal
        self._scheduler = scheduler
        self._serve_foreign_scheduler_item = serve_foreign_scheduler_item
        self._dispatch_foreign_scheduler_outcome = dispatch_foreign_scheduler_outcome
        self._db = sqlite3.connect(db_path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._create_schema()
        self._contract: PairContract | None = None
        self._lease: PairLease | None = None
        self._lease_revoked = False
        self._local_quote: QuoteSnapshot | None = None
        self._peer_quote: QuoteSnapshot | None = None
        self._local_quotes: dict[str, QuoteSnapshot] = {}
        self._peer_quotes: dict[str, QuoteSnapshot] = {}
        self._local_calibrations: dict[str, ProtectionCalibration] = {}
        self._peer_calibrations: dict[str, ProtectionCalibration] = {}
        self._peer_plan_batches: dict[str, dict[int, dict[str, object]]] = {}
        self._peer_plan_hash: str | None = None
        self._local_plan_payload: dict[str, dict[str, object]] = {}
        self._local_plan_hash = _hash(_canonical_json({}))
        self._local_facts: ReadinessFactsEvent | None = None
        self._peer_ready = False
        self._peer_ready_reason = "no peer readiness observed yet"
        self._attempt: Attempt | None = None
        self._leg: LegState | None = None
        self._peer_leg: PeerLegView = PeerLegView()
        self._active_since: datetime | None = None
        self._state: PairState = "IDLE"
        self._desired: DesiredState = "NONE"
        self._needs_human: str | None = None
        self._now: datetime = datetime(1970, 1, 1, tzinfo=UTC)
        self._recovering = False
        self._last_timing: dict[str, object] = {}
        self._last_published_ready: bool | None = None
        self._last_published_reason: str | None = None
        self._last_published_plan_hash: str | None = None
        self._load_persisted_state()

    # -- schema -------------------------------------------------------- #

    def _create_schema(self) -> None:
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS cell_lease (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                leader_worker_id TEXT NOT NULL,
                follower_worker_id TEXT NOT NULL,
                trader_id TEXT NOT NULL,
                contract_hash TEXT NOT NULL,
                epoch INTEGER NOT NULL,
                issued_at TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                revoked INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS cell_contract (
                contract_hash TEXT PRIMARY KEY,
                payload TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_attempts (
                attempt_id TEXT PRIMARY KEY,
                payload_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                terminal INTEGER NOT NULL DEFAULT 0,
                terminal_reason TEXT
            );
            CREATE TABLE IF NOT EXISTS cell_leg_state (
                attempt_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_peer_leg (
                attempt_id TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_desired_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                desired_state TEXT NOT NULL,
                attempt_id TEXT,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_active_state (
                attempt_id TEXT PRIMARY KEY,
                active_since TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_operator_stop (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                reason TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_product_quarantine (
                symbol TEXT PRIMARY KEY,
                offending_worker_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                receipt TEXT NOT NULL,
                quarantined_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_product_quarantine_release (
                symbol TEXT NOT NULL,
                offending_worker_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                released_at TEXT NOT NULL,
                PRIMARY KEY (symbol, offending_worker_id, attempt_id)
            );
            CREATE TABLE IF NOT EXISTS cell_product_release_marker (
                symbol TEXT PRIMARY KEY,
                marker TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_transitions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                attempt_id TEXT,
                event TEXT NOT NULL,
                detail TEXT,
                at TEXT NOT NULL
            );
            """
        )
        self._db.commit()

    def _load_persisted_state(self) -> None:
        transition_row = self._db.execute(
            "SELECT at FROM cell_transitions ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if transition_row is not None:
            self._now = max(self._now, _parse_utc(transition_row[0]))
        operator_stop_row = self._db.execute(
            "SELECT reason FROM cell_operator_stop WHERE id = 1"
        ).fetchone()
        if operator_stop_row is not None:
            self._needs_human = operator_stop_row[0]
        lease_row = self._db.execute(
            "SELECT leader_worker_id, follower_worker_id, trader_id, contract_hash, epoch, issued_at, expires_at, revoked "
            "FROM cell_lease WHERE id = 1"
        ).fetchone()
        if lease_row is not None:
            # A revoked lease is still loaded: its fenced epoch and identity are
            # the durable metadata every remaining status relay, epoch fence,
            # and recovery step needs.  Authority is expressed separately by
            # ``_lease_authority``, never by discarding the lease.
            self._lease_revoked = bool(lease_row[7])
            self._lease = PairLease(
                leader_worker_id=lease_row[0],
                follower_worker_id=lease_row[1],
                trader_id=lease_row[2],
                contract_hash=lease_row[3],
                epoch=lease_row[4],
                issued_at=_parse_utc(lease_row[5]),
                expires_at=_parse_utc(lease_row[6]),
            )
            contract_row = self._db.execute(
                "SELECT payload FROM cell_contract WHERE contract_hash = ?", (self._lease.contract_hash,)
            ).fetchone()
            if contract_row is not None:
                self._contract = _contract_from_canonical(json.loads(contract_row[0]))
        attempt_row = self._db.execute(
            "SELECT attempt_id, payload, terminal FROM cell_attempts ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        if attempt_row is not None and not attempt_row[2]:
            self._attempt = _attempt_from_canonical(json.loads(attempt_row[1]))
            leg_row = self._db.execute(
                "SELECT payload FROM cell_leg_state WHERE attempt_id = ?", (attempt_row[0],)
            ).fetchone()
            if leg_row is not None:
                self._leg = _leg_from_json(json.loads(leg_row[0]))
            peer_row = self._db.execute(
                "SELECT payload FROM cell_peer_leg WHERE attempt_id = ?", (attempt_row[0],)
            ).fetchone()
            if peer_row is not None:
                self._peer_leg = PeerLegView(**json.loads(peer_row[0]))
            desired_row = self._db.execute(
                "SELECT desired_state FROM cell_desired_state WHERE id = 1"
            ).fetchone()
            self._desired = cast(DesiredState, desired_row[0]) if desired_row is not None else "NONE"
            if self._leg is not None:
                active_row = self._db.execute(
                    "SELECT active_since FROM cell_active_state WHERE attempt_id = ?", (attempt_row[0],)
                ).fetchone()
                if active_row is not None:
                    self._active_since = _parse_utc(active_row[0])
                self._recovering = True
                self._state = self._recovered_state()

    def _recovered_state(self) -> PairState:
        assert self._leg is not None
        if self._leg.entry_status == "unknown_after_send":
            return "ENTRY_UNCERTAIN"
        if self._desired == "EMPTY":
            return "CONVERGING_EMPTY"
        if self._leg.protection_status == "refined" and self._active_since is not None:
            return "ACTIVE"
        if self._leg.entry_status == "filled":
            return "VERIFYING_PROTECTION"
        if self._leg.arm_state == "armed":
            if self._leg.role == "leader" and not self._leg.commit_authorized:
                # A leader that armed itself but never issued a commit is still
                # waiting for its peer's acknowledgement, so recovery must
                # resume under the arm deadline rather than the send deadline.
                return "ARMING"
            return "ARMED"
        return "ARMING"

    # -- persistence helpers -------------------------------------------- #

    def _persist_lease(self) -> None:
        assert self._lease is not None
        self._db.execute("DELETE FROM cell_lease WHERE id = 1")
        self._db.execute(
            "INSERT INTO cell_lease (id, leader_worker_id, follower_worker_id, trader_id, contract_hash, epoch,"
            " issued_at, expires_at, revoked) VALUES (1, ?, ?, ?, ?, ?, ?, ?, 0)",
            (
                self._lease.leader_worker_id,
                self._lease.follower_worker_id,
                self._lease.trader_id,
                self._lease.contract_hash,
                self._lease.epoch,
                self._lease.issued_at.isoformat(),
                self._lease.expires_at.isoformat(),
            ),
        )
        self._db.commit()

    def _revoke_lease(self) -> None:
        """Drop authority without erasing the fenced lease metadata.

        Lease loss must not make status relay, epoch fencing, or recovery
        impossible: an armed or active attempt still has to notify its peer and
        converge on broker-observed facts.  Only the *authority* to create or
        commit a new entry is removed (see :meth:`_lease_authority`).
        """

        self._db.execute("UPDATE cell_lease SET revoked = 1 WHERE id = 1")
        self._db.commit()
        self._lease_revoked = True
        self._transition("lease_revoked", "" if self._lease is None else f"epoch={self._lease.epoch}")
        self._db.commit()

    def _lease_authority(self) -> PairLease | None:
        """The lease that currently authorizes new sends, or ``None``."""

        if self._lease is None or self._lease_revoked:
            return None
        if not self._lease.active(self._now):
            return None
        return self._lease

    def _attempt_authority(self, attempt: Attempt) -> PairLease | None:
        """The authority that still covers this exact attempt's epoch."""

        lease = self._lease_authority()
        if lease is None or lease.epoch != attempt.lease_epoch:
            return None
        return lease

    def _persist_contract(self) -> None:
        assert self._contract is not None
        self._db.execute(
            "INSERT OR IGNORE INTO cell_contract (contract_hash, payload) VALUES (?, ?)",
            (self._contract.hash, _canonical_json(self._contract.canonical())),
        )
        self._db.commit()

    def _persist_attempt(self, attempt: Attempt) -> None:
        self._db.execute(
            "INSERT INTO cell_attempts (attempt_id, payload_hash, payload, created_at, terminal) VALUES (?, ?, ?, ?, 0)",
            (attempt.attempt_id, attempt.payload_hash, _canonical_json(attempt.canonical()), _iso(self._now)),
        )
        self._db.commit()

    def _terminate_attempt(self, reason: str) -> None:
        if self._attempt is None:
            return
        self._db.execute(
            "UPDATE cell_attempts SET terminal = 1, terminal_reason = ? WHERE attempt_id = ?",
            (reason, self._attempt.attempt_id),
        )
        self._transition(reason)
        self._db.commit()

    def _persist_leg(self) -> None:
        if self._attempt is None or self._leg is None:
            return
        self._db.execute(
            "INSERT INTO cell_leg_state (attempt_id, payload, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(attempt_id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
            (self._attempt.attempt_id, json.dumps(_leg_to_json(self._leg)), _iso(self._now)),
        )
        self._db.commit()

    def _persist_peer_leg(self) -> None:
        if self._attempt is None:
            return
        self._db.execute(
            "INSERT INTO cell_peer_leg (attempt_id, payload, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(attempt_id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
            (self._attempt.attempt_id, json.dumps(asdict(self._peer_leg)), _iso(self._now)),
        )
        self._db.commit()

    def _persist_desired(self) -> None:
        self._db.execute(
            "INSERT INTO cell_desired_state (id, desired_state, attempt_id, updated_at) VALUES (1, ?, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET desired_state = excluded.desired_state,"
            " attempt_id = excluded.attempt_id, updated_at = excluded.updated_at",
            (self._desired, None if self._attempt is None else self._attempt.attempt_id, _iso(self._now)),
        )
        self._db.commit()

    def _persist_active_since(self) -> None:
        if self._attempt is None or self._active_since is None:
            return
        self._db.execute(
            "INSERT INTO cell_active_state (attempt_id, active_since) VALUES (?, ?)"
            " ON CONFLICT(attempt_id) DO UPDATE SET active_since = excluded.active_since",
            (self._attempt.attempt_id, _iso(self._active_since)),
        )
        self._db.commit()

    def _set_needs_human(self, reason: str, detail: str) -> None:
        self._needs_human = reason
        self._db.execute(
            "INSERT INTO cell_operator_stop (id, reason, updated_at) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET reason = excluded.reason, updated_at = excluded.updated_at",
            (reason, _iso(self._now)),
        )
        self._transition("close_needs_human", detail)
        self._db.commit()

    def _transition(self, event: str, detail: str = "") -> None:
        self._db.execute(
            "INSERT INTO cell_transitions (attempt_id, event, detail, at) VALUES (?, ?, ?, ?)",
            (None if self._attempt is None else self._attempt.attempt_id, event, detail, _iso(self._now)),
        )

    def _record_timing(self, key: str) -> None:
        """Durably record one structured timing-evidence point for this attempt."""

        self._last_timing[key] = _iso(self._now)
        self._transition(f"timing:{key}")

    def transition_history(self) -> list[dict[str, object]]:
        rows = self._db.execute(
            "SELECT attempt_id, event, detail, at FROM cell_transitions ORDER BY id"
        ).fetchall()
        return [{"attempt_id": r[0], "event": r[1], "detail": r[2], "at": r[3]} for r in rows]

    # -- public lifecycle ------------------------------------------------ #

    def activate(self, contract: PairContract, lease: PairLease) -> PairResult:
        """Bind an immutable contract and lease; a stale epoch is rejected."""

        self._validate_lease(contract, lease)
        self._contract = contract
        self._lease = lease
        self._lease_revoked = False
        self._persist_contract()
        self._persist_lease()
        self._transition("activated", f"epoch={lease.epoch}")
        return self.handle_event(ClockTickEvent(lease.issued_at))

    def _validate_lease(self, contract: PairContract, lease: PairLease) -> None:
        """The single fencing/identity/contract-hash gate for every lease input.

        :meth:`activate` and a :class:`LeaseEvent` carrying a renewed or
        replacement lease both pass through here, so a stale epoch, a foreign
        pair or trader, or a changed contract hash is rejected identically and
        never downgrades already-durable state.
        """

        if self._worker_id not in (contract.leader_worker_id, contract.follower_worker_id):
            raise PairExecutionCellError("This Worker is not part of the pair contract.")
        automatic = contract.entry_edge_points is not None
        legacy_fields = (
            contract.symbol,
            contract.leader_direction,
            contract.follower_direction,
            contract.edge_threshold,
        )
        legacy = all(
            value is not None
            for value in legacy_fields
        )
        if automatic and any(value is not None for value in legacy_fields):
            raise PairExecutionCellError("Automatic pair contracts cannot include legacy product or direction fields.")
        if automatic == legacy:
            raise PairExecutionCellError(
                "A pair contract must define either automatic entry_edge_points policy or one complete legacy product."
            )
        if automatic:
            threshold = _to_decimal(contract.entry_edge_points)
            if threshold is None or threshold <= 0:
                raise PairExecutionCellError("Entry edge points must be a positive finite decimal.")
            if any(not symbol.strip() for symbol in contract.eligible_symbols):
                raise PairExecutionCellError("Eligible symbols must be non-empty canonical product names.")
        if (
            contract.maximum_holding_seconds is not None
            and (
                not math.isfinite(contract.maximum_holding_seconds)
                or contract.maximum_holding_seconds <= 0
                or contract.maximum_holding_seconds > _MAXIMUM_HOLDING_SECONDS
            )
        ):
            raise PairExecutionCellError("Maximum holding seconds must be positive and no greater than seven days.")
        if contract.flatten_at_ny is not None:
            try:
                time.fromisoformat(contract.flatten_at_ny)
            except ValueError as error:
                raise PairExecutionCellError("New York flatten time must use HH:MM format.") from error
        if lease.contract_hash != contract.hash:
            raise PairExecutionCellError("The lease contract hash does not match the pair contract.")
        if (
            lease.leader_worker_id != contract.leader_worker_id
            or lease.follower_worker_id != contract.follower_worker_id
            or lease.trader_id != contract.trader_id
        ):
            raise PairExecutionCellError("The lease identity does not match the pair contract.")
        current = self._lease
        if current is None:
            return
        if lease.epoch < current.epoch:
            raise PairExecutionCellError("A stale lease epoch cannot supersede the current lease.")
        if lease.epoch == current.epoch and lease.contract_hash != current.contract_hash:
            raise PairExecutionCellError("The contract changed without a new lease epoch.")
        if lease.epoch != current.epoch and self._contract is not None and not self._attempt_terminal():
            raise PairExecutionCellError(
                "Leadership or contract transfer is blocked while an earlier epoch has an unresolved attempt."
            )

    def _apply_lease_event(self, lease: PairLease | None) -> None:
        if lease is None:
            self._revoke_lease()
            return
        if self._contract is None:
            raise PairExecutionCellError("A pair lease cannot be applied before a pair contract is activated.")
        self._validate_lease(self._contract, lease)
        self._lease = lease
        self._lease_revoked = False
        self._persist_lease()
        self._transition("lease_applied", f"epoch={lease.epoch}")
        self._db.commit()

    def recover(self) -> PairResult:
        """Resume durable state after a restart before any new quote is used."""

        if self._attempt is not None and not self._attempt_terminal():
            self._recovering = True
            if self._leg is not None and self._leg.entry_status == "unknown_after_send":
                self._request_broker_read()
            self._progress()
        else:
            self._recovering = False
        return self._result()

    def handle_event(self, event: PairCellEvent) -> PairResult:
        if isinstance(event, ClockTickEvent):
            self._now = _as_utc(event.now)
        elif isinstance(event, LocalQuoteEvent):
            self._now = max(self._now, _as_utc(event.local_receive_time))
            self._accept_local_quote(event)
        elif isinstance(event, LocalQuoteBatchEvent):
            accepted: list[QuoteSnapshot] = []
            for quote in event.quotes:
                self._now = max(self._now, _as_utc(quote.local_receive_time))
                if self._accept_local_quote(quote, publish=False):
                    accepted.append(self._local_quotes[quote.symbol])
            if accepted:
                self._publish_quote_batch(tuple(accepted))
            else:
                self._publish_quarantine_batches()
        elif isinstance(event, ReadinessFactsEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
            self._local_facts = event
            self._local_calibrations = dict(event.symbol_calibrations)
            if (
                not self._local_calibrations
                and self._contract is not None
                and self._contract.symbol is not None
                and event.protection_calibration is not None
            ):
                self._local_calibrations[self._contract.symbol] = event.protection_calibration
            self._local_plan_payload = {
                symbol: asdict(plan) for symbol, plan in sorted(self._local_calibrations.items())
            }
            self._local_plan_hash = _hash(_canonical_json(self._local_plan_payload))
        elif isinstance(event, LeaseEvent):
            self._apply_lease_event(event.lease)
        elif isinstance(event, BrokerSnapshotEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
            self._apply_broker_snapshot(event)
        elif isinstance(event, QuarantineReleaseEvent):
            _as_utc(event.observed_at)
            self._release_product_quarantine(event)
        elif isinstance(event, RelayEnvelopeReceived):
            self._accept_relay_envelope(event.envelope)
        else:
            raise PairExecutionCellError("Unknown Pair Execution Cell event type.")
        self._progress()
        return self._result()

    # -- readiness -------------------------------------------------------- #

    def _local_ready(self) -> tuple[bool, str]:
        admissible, reason = self._entry_admissible()
        if not admissible:
            return False, reason
        if self._attempt is not None:
            # An attempt that is still arming, sending, verifying, or converging
            # is unresolved work; no new entry may be admitted on either Worker
            # until it reaches a broker-proven terminal state.
            return False, "an attempt is unresolved"
        return True, "ready"

    def _entry_admissible(self) -> tuple[bool, str]:
        """Every continuously maintained entry invariant except attempt state.

        This is the account-local half of ``PAIR_READY``: it is what must still
        be true immediately before a committed leg reaches ``order_send``, when
        the attempt itself is legitimately in flight.
        """

        if self._flatten_reached():
            return False, "New York flatten cutoff has passed"
        facts = self._local_facts
        if facts is None:
            return False, "no local readiness facts observed yet"
        if not facts.terminal_connected:
            return False, "terminal disconnected"
        if facts.orders is None or facts.positions is None:
            return False, "broker order/position read unavailable"
        exposure = self._unowned_exposure_reason(facts.orders, facts.positions)
        if exposure is not None:
            return False, exposure
        if not facts.margin_headroom_ok:
            return False, "insufficient margin headroom"
        if not facts.symbol_constraints_fresh:
            return False, "symbol constraints are stale"
        if self._contract is not None and self._contract.entry_edge_points is not None:
            if not self._local_calibrations:
                return False, "no eligible symbol calibration is available"
        elif facts.protection_calibration is None:
            return False, "protection calibration is unavailable"
        try:
            unresolved = self._journal.unresolved()
        except EffectJournalError:
            return False, "effect journal is unhealthy"
        if unresolved:
            return False, "effect journal has unresolved effects"
        if self._lease_authority() is None:
            return False, "lease is missing or expired"
        if self._recovering:
            return False, "recovering durable state after restart"
        return True, "ready"

    def _unowned_exposure_reason(
        self, orders: list[dict[str, object]], positions: list[dict[str, object]]
    ) -> str | None:
        """Any exposure that is not this attempt's exact owned leg revokes readiness.

        Readiness is revoked in *every* state, not only ``IDLE``: an unresolved
        or externally created order/position must never be silently tolerated
        just because this cell happens to be arming, sending, verifying, or
        converging.  The single exception is the exact attempt-owned position
        ticket this cell is currently managing, which is recognized by ticket
        identity and never by symbol/side/volume shape.
        """

        if not orders and not positions:
            return None
        leg = self._leg
        if orders:
            # V1 entries are market orders only; an attempt never owns a pending order.
            return "existing exposure present"
        if leg is None or self._attempt is None or leg.ticket is None or leg.empty_verified:
            return "existing exposure present"
        if leg.entry_status != "filled":
            return "existing exposure present"
        if self._state not in ("SENDING", "ENTRY_UNCERTAIN", "VERIFYING_PROTECTION", "ACTIVE", "CONVERGING_EMPTY"):
            return "existing exposure present"
        if {str(position.get("ticket")) for position in positions} != {leg.ticket}:
            return "existing exposure present"
        return None

    def _peer_ready_now(self) -> tuple[bool, str]:
        return self._peer_ready, self._peer_ready_reason

    # -- quotes ------------------------------------------------------------ #

    def _accept_local_quote(self, event: LocalQuoteEvent, *, publish: bool = True) -> bool:
        if self._contract is not None:
            if self._contract.symbol is not None and event.symbol != self._contract.symbol:
                return False
            if self._contract.eligible_symbols and event.symbol not in self._contract.eligible_symbols:
                return False
        existing = self._local_quotes.get(event.symbol)
        if (
            existing is not None
            and existing.recovery_epoch == event.recovery_epoch
            and event.sequence <= existing.sequence
        ):
            return False
        broker_time = _as_utc(event.broker_time)
        if (
            existing is not None
            and existing.recovery_epoch == event.recovery_epoch
            and existing.bid == event.bid
            and existing.ask == event.ask
            and existing.broker_time == broker_time
        ):
            return False
        quote = QuoteSnapshot(
            worker_id=self._worker_id,
            symbol=event.symbol,
            bid=event.bid,
            ask=event.ask,
            broker_time=broker_time,
            local_receive_time=_as_utc(event.local_receive_time),
            recovery_epoch=event.recovery_epoch,
            sequence=event.sequence,
        )
        self._local_quotes[event.symbol] = quote
        self._local_quote = quote
        if publish:
            self._publish_quote(quote)
        self._record_timing("local_quote_observed")
        return True

    def _accept_peer_quote(self, payload: Mapping[str, object]) -> None:
        if self._contract is None:
            return
        peer_id = self._contract.peer_of(self._worker_id)
        symbol = payload.get("symbol")
        if payload.get("worker_id") != peer_id or not isinstance(symbol, str):
            return
        if self._contract.symbol is not None and symbol != self._contract.symbol:
            return
        if self._contract.eligible_symbols and symbol not in self._contract.eligible_symbols:
            return
        self._accept_peer_product_quarantines(payload.get("product_quarantines"), peer_id)
        bid, ask = _to_decimal(payload.get("bid")), _to_decimal(payload.get("ask"))
        broker_time_raw, sequence = payload.get("broker_time"), payload.get("sequence")
        epoch = payload.get("recovery_epoch")
        if (
            bid is None
            or ask is None
            or not isinstance(broker_time_raw, str)
            or not isinstance(epoch, str)
            or not epoch
            or isinstance(sequence, bool)
            or not isinstance(sequence, int)
        ):
            return
        try:
            broker_time = _parse_utc(broker_time_raw)
        except (ValueError, PairExecutionCellError):
            return
        existing = self._peer_quotes.get(symbol)
        if existing is not None and existing.recovery_epoch == epoch and sequence <= existing.sequence:
            return  # duplicate or out-of-order within the same recovery epoch
        quote = QuoteSnapshot(
            worker_id=peer_id,
            symbol=symbol,
            bid=bid,
            ask=ask,
            broker_time=broker_time,
            local_receive_time=self._now,
            recovery_epoch=epoch,
            sequence=sequence,
        )
        self._peer_quotes[symbol] = quote
        self._peer_quote = quote
        self._record_timing("peer_quote_observed")

    def _publish_quote(self, quote: QuoteSnapshot) -> None:
        if self._contract is None or self._lease is None:
            return
        payload = quote.canonical()
        self._publish_quarantine_batches()
        self._relay.send(
            {
                "kind": "quote",
                "from_worker_id": self._worker_id,
                "to_worker_id": self._contract.peer_of(self._worker_id),
                "lease_epoch": self._lease.epoch,
                "payload": payload,
            }
        )

    def _publish_quote_batch(self, quotes: tuple[QuoteSnapshot, ...]) -> None:
        if self._contract is None or self._lease is None:
            return
        canonical_quotes = {str(index): quote.canonical() for index, quote in enumerate(quotes)}
        self._publish_quarantine_batches()
        for batch in self._bounded_mapping_batches(canonical_quotes):
            self._relay.send(
                {
                    "kind": "quote_batch",
                    "from_worker_id": self._worker_id,
                    "to_worker_id": self._contract.peer_of(self._worker_id),
                    "lease_epoch": self._lease.epoch,
                    "payload": {
                        "quotes": list(batch.values()),
                    },
                }
            )

    def _maybe_publish_readiness(self) -> None:
        if self._contract is None or self._lease is None:
            return
        ready, reason = self._local_ready()
        plans_changed = self._local_plan_hash != self._last_published_plan_hash
        first_publication = self._last_published_plan_hash is None
        if (ready, reason, self._local_plan_hash) == (
            self._last_published_ready,
            self._last_published_reason,
            self._last_published_plan_hash,
        ):
            return
        self._last_published_ready, self._last_published_reason = ready, reason
        self._last_published_plan_hash = self._local_plan_hash
        if plans_changed:
            self._publish_calibration_batches()
        self._relay.send(
            {
                "kind": "readiness",
                "from_worker_id": self._worker_id,
                "to_worker_id": self._contract.peer_of(self._worker_id),
                "lease_epoch": self._lease.epoch,
                "payload": {
                    "ready": ready,
                    "reason": reason,
                    "plan_hash": self._local_plan_hash,
                },
            }
        )
        if first_publication:
            self._relay.send(
                {
                    "kind": "calibration_request",
                    "from_worker_id": self._worker_id,
                    "to_worker_id": self._contract.peer_of(self._worker_id),
                    "lease_epoch": self._lease.epoch,
                    "payload": {},
                }
            )

    def _publish_calibration_batches(self) -> None:
        if self._contract is None or self._lease is None:
            return
        batches = self._bounded_mapping_batches(self._local_plan_payload)
        for index, batch in enumerate(batches):
            self._relay.send(
                {
                    "kind": "calibration_batch",
                    "from_worker_id": self._worker_id,
                    "to_worker_id": self._contract.peer_of(self._worker_id),
                    "lease_epoch": self._lease.epoch,
                    "payload": {
                        "plan_hash": self._local_plan_hash,
                        "batch_index": index,
                        "batch_count": len(batches),
                        "symbol_calibrations": batch,
                    },
                }
            )

    def _publish_quarantine_batches(self) -> None:
        if self._contract is None or self._lease is None:
            return
        records = {str(index): record for index, record in enumerate(self._product_quarantine_records())}
        if not records:
            return
        for batch in self._bounded_mapping_batches(records):
            self._relay.send(
                {
                    "kind": "quarantine_batch",
                    "from_worker_id": self._worker_id,
                    "to_worker_id": self._contract.peer_of(self._worker_id),
                    "lease_epoch": self._lease.epoch,
                    "payload": {"product_quarantines": list(batch.values())},
                }
            )

    @staticmethod
    def _bounded_mapping_batches(
        payload: Mapping[str, dict[str, object]], *, max_bytes: int = 32_768
    ) -> list[dict[str, dict[str, object]]]:
        batches: list[dict[str, dict[str, object]]] = []
        current: dict[str, dict[str, object]] = {}
        current_size = 2
        for symbol, value in payload.items():
            entry_size = len(_canonical_json({symbol: value}).encode("utf-8")) - 2
            separator_size = 1 if current else 0
            if current and current_size + separator_size + entry_size > max_bytes:
                batches.append(current)
                current = {symbol: value}
                current_size = 2 + entry_size
            else:
                current[symbol] = value
                current_size += separator_size + entry_size
        batches.append(current)
        return batches

    def _coherent_quotes(self, symbol: str | None = None) -> tuple[QuoteSnapshot, QuoteSnapshot] | None:
        assert self._contract is not None
        selected = symbol if symbol is not None else self._contract.symbol
        if selected is None:
            return None
        local, peer = self._local_quotes.get(selected), self._peer_quotes.get(selected)
        if local is None or peer is None:
            return None
        if not local.fresh(self._now, self._contract.quote_max_age_seconds):
            return None
        if not peer.fresh(self._now, self._contract.quote_max_age_seconds):
            return None
        if abs((local.broker_time - peer.broker_time).total_seconds()) > self._contract.quote_max_skew_seconds:
            return None
        leader = local if self._contract.role_of(self._worker_id) == "leader" else peer
        follower = peer if leader is local else local
        return leader, follower

    # -- relay envelope handling -------------------------------------------- #

    def _accept_relay_envelope(self, envelope: Mapping[str, object]) -> None:
        kind = envelope.get("kind")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            return
        if self._contract is not None and envelope.get("from_worker_id") != self._contract.peer_of(self._worker_id):
            return  # never trust a message that does not originate from the bound peer
        if kind == "quote":
            self._accept_peer_quote(payload)
        elif kind == "quote_batch":
            raw_quotes = payload.get("quotes")
            if isinstance(raw_quotes, list):
                for quote in raw_quotes:
                    if isinstance(quote, dict):
                        self._accept_peer_quote(quote)
        elif kind == "calibration_batch":
            self._accept_peer_calibration_batch(payload)
        elif kind == "calibration_request":
            self._publish_calibration_batches()
        elif kind == "quarantine_batch":
            if self._contract is not None:
                self._accept_peer_product_quarantines(
                    payload.get("product_quarantines"),
                    self._contract.peer_of(self._worker_id),
                )
        elif kind == "readiness":
            ready = payload.get("ready")
            reason = payload.get("reason")
            if isinstance(ready, bool) and isinstance(reason, str):
                self._peer_ready, self._peer_ready_reason = ready, reason
                raw_plans = payload.get("symbol_calibrations")
                if isinstance(raw_plans, dict):
                    self._peer_calibrations = self._parse_calibrations(raw_plans)
                    self._peer_plan_hash = _hash(_canonical_json(raw_plans))
                peer_plan_hash = payload.get("plan_hash")
                if (
                    isinstance(peer_plan_hash, str)
                    and peer_plan_hash != self._peer_plan_hash
                    and self._contract is not None
                    and self._lease is not None
                ):
                    self._relay.send(
                        {
                            "kind": "calibration_request",
                            "from_worker_id": self._worker_id,
                            "to_worker_id": self._contract.peer_of(self._worker_id),
                            "lease_epoch": self._lease.epoch,
                            "payload": {},
                        }
                    )
        elif kind == "arm_request":
            self._handle_arm_request(payload)
        elif kind == "arm_ack":
            self._handle_arm_ack(payload)
        elif kind == "commit":
            self._handle_commit(payload)
        elif kind == "leg_status":
            self._handle_peer_leg_status(payload)

    @staticmethod
    def _parse_calibrations(raw_plans: Mapping[object, object]) -> dict[str, ProtectionCalibration]:
        parsed: dict[str, ProtectionCalibration] = {}
        for symbol, raw in raw_plans.items():
            if isinstance(symbol, str) and isinstance(raw, dict):
                try:
                    parsed[symbol] = ProtectionCalibration(
                        tick_size=str(raw["tick_size"]),
                        point=str(raw["point"]),
                        minimum_stop_distance=str(raw["minimum_stop_distance"]),
                        profit_tick_value=str(raw["profit_tick_value"]),
                        loss_tick_value=str(raw["loss_tick_value"]),
                    )
                except KeyError:
                    continue
        return parsed

    def _accept_peer_calibration_batch(self, payload: Mapping[str, object]) -> None:
        plan_hash = payload.get("plan_hash")
        batch_index = payload.get("batch_index")
        batch_count = payload.get("batch_count")
        raw_plans = payload.get("symbol_calibrations")
        if (
            not isinstance(plan_hash, str)
            or isinstance(batch_index, bool)
            or not isinstance(batch_index, int)
            or isinstance(batch_count, bool)
            or not isinstance(batch_count, int)
            or batch_count <= 0
            or batch_index < 0
            or batch_index >= batch_count
            or not isinstance(raw_plans, dict)
        ):
            return
        batches = self._peer_plan_batches.setdefault(plan_hash, {})
        batches[batch_index] = dict(raw_plans)
        if len(batches) != batch_count:
            return
        combined: dict[object, object] = {}
        for index in range(batch_count):
            combined.update(batches[index])
        if _hash(_canonical_json(combined)) != plan_hash:
            self._peer_plan_batches.pop(plan_hash, None)
            return
        self._peer_calibrations = self._parse_calibrations(combined)
        self._peer_plan_hash = plan_hash
        self._peer_plan_batches = {plan_hash: batches}

    # -- leader: attempt creation and arming -------------------------------- #

    def _role(self) -> Role | None:
        if self._contract is None:
            return None
        return self._contract.role_of(self._worker_id)

    def _attempt_terminal(self) -> bool:
        return self._state in ("EMPTY", "NEEDS_HUMAN") or self._attempt is None

    def _progress(self) -> None:
        if self._needs_human is not None:
            self._state = "NEEDS_HUMAN"
            return
        self._enforce_exit_policy()
        self._maybe_publish_readiness()
        self._enforce_attempt_authority()
        self._expire_stale_attempt()
        if self._role() == "leader" and self._attempt is None and self._state == "IDLE":
            self._maybe_create_attempt()
        self._maybe_send_commit()
        self._maybe_progress_convergence()
        self._maybe_declare_active()

    def _enforce_exit_policy(self) -> None:
        contract = self._contract
        if contract is None:
            return
        if self._attempt is not None and self._desired != "EMPTY" and self._flatten_reached():
            self._begin_close("flatten_at_ny")
            return
        if (
            self._attempt is not None
            and self._desired != "EMPTY"
            and self._active_since is not None
            and contract.maximum_holding_seconds is not None
            and self._now >= self._active_since + timedelta(seconds=contract.maximum_holding_seconds)
        ):
            self._begin_close("maximum_holding_seconds")

    def _flatten_reached(self) -> bool:
        if self._contract is None or self._contract.flatten_at_ny is None:
            return False
        try:
            cutoff = time.fromisoformat(self._contract.flatten_at_ny)
        except ValueError:
            return True
        ny_zone = ZoneInfo("America/New_York")
        ny_now = self._now.astimezone(ny_zone)
        cutoff_at = datetime.combine(ny_now.date(), cutoff, tzinfo=ny_zone)
        return self._now >= cutoff_at.astimezone(UTC)

    def _enforce_attempt_authority(self) -> None:
        """Lease loss before this leg's send abandons the attempt safely.

        Losing authority never cancels an effect that already crossed
        ``send_started`` and never erases the attempt: it only stops a *new*
        send and hands the attempt to the same evidence-driven desired-empty
        convergence every other exit path uses.
        """

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None:
            return
        if leg.entry_status != "pending" or self._state not in ("ARMING", "ARMED"):
            return
        if self._attempt_authority(attempt) is None:
            self._abandon_attempt("lease_lost_before_send")

    def _expire_stale_attempt(self) -> None:
        if self._attempt is None or self._leg is None:
            return
        if self._state == "ARMING" and self._now >= self._attempt.arm_deadline:
            self._abandon_attempt("arm_timeout_expired")
        elif (
            self._state == "ARMED"
            and self._leg.entry_status == "pending"
            and self._now >= self._attempt.expires_at
        ):
            self._abandon_attempt("expired_not_started")

    def _abandon_attempt(self, reason: str) -> None:
        """Stop trying to enter, tell the peer, and converge on broker evidence.

        An attempt is never simply forgotten.  Even when this leg provably
        never started, the peer may hold the mirror leg, so this records the
        durable outcome, relays it, switches desired state to ``EMPTY``, and
        lets :meth:`_maybe_progress_convergence` prove emptiness from fresh
        broker facts.  Only :meth:`_maybe_finalize_empty` may terminalize.
        """

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None:
            return
        if leg.entry_status == "pending":
            leg.entry_status = "expired_not_started"
            self._persist_leg()
        self._transition("attempt_abandoned", reason)
        if self._desired != "EMPTY":
            self._record_timing("containment_start")
        self._desired = "EMPTY"
        self._persist_desired()
        self._state = "CONVERGING_EMPTY"
        # Report this leg's real status, never a blanket "not started": the
        # peer decides its own containment from what this side actually did.
        self._report_leg_status(leg.entry_status, reason=reason)

    def _reset_attempt(self) -> None:
        self._attempt = None
        self._leg = None
        self._peer_leg = PeerLegView()
        self._active_since = None
        self._desired = "NONE"
        self._persist_desired()
        self._recovering = False
        self._state = "IDLE"

    def _best_entry_candidate(
        self,
    ) -> tuple[str, Direction, Direction, QuoteSnapshot, QuoteSnapshot, ProtectionCalibration] | None:
        contract = self._contract
        if contract is None:
            return None
        if contract.entry_edge_points is None:
            coherent = self._coherent_quotes()
            if (
                coherent is None
                or contract.symbol is None
                or contract.leader_direction is None
                or contract.follower_direction is None
                or contract.edge_threshold is None
                or contract.symbol in self.quarantined_products()
            ):
                return None
            leader_quote, follower_quote = coherent
            threshold = _to_decimal(contract.edge_threshold)
            calibration = self._local_calibrations.get(contract.symbol)
            if (
                threshold is None
                or edge_value(leader_quote, follower_quote, contract.leader_direction) < threshold
                or calibration is None
            ):
                return None
            return (
                contract.symbol,
                contract.leader_direction,
                contract.follower_direction,
                leader_quote,
                follower_quote,
                calibration,
            )

        threshold_points = _to_decimal(contract.entry_edge_points)
        leader_volume = _to_decimal(contract.leader_volume)
        follower_volume = _to_decimal(contract.follower_volume)
        if threshold_points is None or leader_volume is None or follower_volume is None:
            return None
        symbols = set(self._local_quotes) & set(self._peer_quotes) & set(self._local_calibrations) & set(
            self._peer_calibrations
        )
        symbols -= set(self.quarantined_products())
        if contract.eligible_symbols:
            symbols &= set(contract.eligible_symbols)
        ranked: list[
            tuple[
                Decimal,
                Decimal,
                str,
                Direction,
                Direction,
                QuoteSnapshot,
                QuoteSnapshot,
                ProtectionCalibration,
            ]
        ] = []
        for symbol in symbols:
            coherent = self._coherent_quotes(symbol)
            if coherent is None:
                continue
            leader_quote, follower_quote = coherent
            leader_calibration = self._local_calibrations[symbol]
            follower_calibration = self._peer_calibrations[symbol]
            point = _to_decimal(leader_calibration.point)
            peer_point = _to_decimal(follower_calibration.point)
            if point is None or peer_point is None or point <= 0 or peer_point != point:
                continue
            for leader_direction, follower_direction in (("LONG", "SHORT"), ("SHORT", "LONG")):
                direction = cast(Direction, leader_direction)
                edge_points = edge_value(leader_quote, follower_quote, direction) / point
                if edge_points < threshold_points:
                    continue
                leader_usd = self._usd_per_point(leader_calibration, leader_volume)
                follower_usd = self._usd_per_point(follower_calibration, follower_volume)
                if leader_usd is None or follower_usd is None:
                    continue
                ranked.append(
                    (
                        edge_points * min(leader_usd, follower_usd),
                        edge_points,
                        symbol,
                        direction,
                        cast(Direction, follower_direction),
                        leader_quote,
                        follower_quote,
                        leader_calibration,
                    )
                )
        if not ranked:
            return None
        ranked.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
        _, _, symbol, leader_direction, follower_direction, leader_quote, follower_quote, calibration = ranked[0]
        return symbol, leader_direction, follower_direction, leader_quote, follower_quote, calibration

    @staticmethod
    def _usd_per_point(calibration: ProtectionCalibration, volume: Decimal) -> Decimal | None:
        point = _to_decimal(calibration.point)
        tick_size = _to_decimal(calibration.tick_size)
        profit_value = _to_decimal(calibration.profit_tick_value)
        loss_value = _to_decimal(calibration.loss_tick_value)
        if (
            point is None
            or tick_size is None
            or profit_value is None
            or loss_value is None
            or point <= 0
            or tick_size <= 0
            or profit_value <= 0
            or loss_value <= 0
        ):
            return None
        return point / tick_size * min(profit_value, loss_value) * volume

    def _maybe_create_attempt(self) -> None:
        contract, lease = self._contract, self._lease_authority()
        if contract is None or lease is None:
            return
        ready, _ = self._local_ready()
        peer_ready, _ = self._peer_ready_now()
        if not ready or not peer_ready:
            return
        candidate = self._best_entry_candidate()
        if candidate is None:
            return
        symbol, leader_direction, follower_direction, leader_quote, follower_quote, calibration = candidate
        self._record_timing("edge_decision")
        leader_entry = leader_quote.ask if leader_direction == "LONG" else leader_quote.bid
        leader_volume = _to_decimal(contract.leader_volume)
        if leader_volume is None or calibration is None:
            return
        risk_budget = _to_decimal(contract.risk_budget_usd)
        if risk_budget is None:
            return
        protection = compute_protection(
            entry=leader_entry,
            direction=leader_direction,
            volume=leader_volume,
            calibration=calibration,
            risk_budget_usd=risk_budget,
        )
        if protection is None:
            self._transition("attempt_rejected", "cannot construct valid emergency protection for the leader leg")
            return
        attempt = Attempt(
            attempt_id=str(uuid4()),
            lease_epoch=lease.epoch,
            contract_hash=contract.hash,
            symbol=symbol,
            leader_direction=leader_direction,
            follower_direction=follower_direction,
            leader_volume=contract.leader_volume,
            follower_volume=contract.follower_volume,
            leader_quote=leader_quote,
            follower_quote=follower_quote,
            decision_time=self._now,
            arm_deadline=self._now + timedelta(seconds=contract.arm_timeout_seconds),
            execute_at=self._now + timedelta(seconds=contract.commit_lead_seconds),
            expires_at=self._now
            + timedelta(seconds=contract.commit_lead_seconds + contract.execution_expiry_seconds),
        )
        self._attempt = attempt
        self._persist_attempt(attempt)
        self._record_timing("attempt_persisted")
        self._leg = LegState(attempt_id=attempt.attempt_id, role="leader")
        sl, tp = protection
        self._leg.emergency_sl, self._leg.emergency_tp = sl, tp
        self._leg.arm_state = "armed"
        self._persist_leg()
        self._desired = "ACTIVE"
        self._persist_desired()
        self._state = "ARMING"
        self._transition("attempt_created", attempt.attempt_id)
        self._record_timing("arm_send")
        self._relay.send(
            {
                "kind": "arm_request",
                "from_worker_id": self._worker_id,
                "to_worker_id": contract.peer_of(self._worker_id),
                "lease_epoch": lease.epoch,
                "payload": {"attempt": attempt.canonical()},
            }
        )

    def _handle_arm_request(self, payload: Mapping[str, object]) -> None:
        contract, lease = self._contract, self._lease_authority()
        if contract is None or contract.role_of(self._worker_id) != "follower":
            return
        raw_attempt = payload.get("attempt")
        if not isinstance(raw_attempt, dict):
            return
        try:
            attempt = _attempt_from_canonical(raw_attempt)
        except (KeyError, ValueError, PairExecutionCellError):
            return
        if self._attempt is not None and self._attempt.attempt_id == attempt.attempt_id:
            if self._attempt.payload_hash != attempt.payload_hash:
                self._send_arm_ack(attempt.attempt_id, False, "attempt ID reused with different content")
            return  # durable replay: same content, no re-arming
        if self._attempt is not None and not self._attempt_terminal():
            # A second attempt must never replace an unresolved first one: the
            # follower would otherwise lose the durable leg that owns any
            # already-created exposure.
            self._send_arm_ack(attempt.attempt_id, False, "an earlier attempt is still unresolved")
            return
        if attempt.symbol in self.quarantined_products():
            self._send_arm_ack(
                attempt.attempt_id,
                False,
                "product is quarantined on the follower",
                symbol=attempt.symbol,
            )
            return
        if lease is None:
            self._send_arm_ack(attempt.attempt_id, False, "follower not ready: lease is missing or expired")
            return
        if attempt.lease_epoch != lease.epoch or attempt.contract_hash != contract.hash:
            self._send_arm_ack(attempt.attempt_id, False, "lease epoch or contract hash mismatch")
            return
        ready, reason = self._local_ready()
        if not ready:
            self._send_arm_ack(attempt.attempt_id, False, f"follower not ready: {reason}")
            return
        follower_entry = attempt.follower_quote.ask if attempt.follower_direction == "LONG" else attempt.follower_quote.bid
        follower_volume = _to_decimal(attempt.follower_volume)
        calibration = self._local_calibrations.get(attempt.symbol)
        risk_budget = _to_decimal(contract.risk_budget_usd)
        protection = None
        if follower_volume is not None and calibration is not None and risk_budget is not None:
            protection = compute_protection(
                entry=follower_entry,
                direction=attempt.follower_direction,
                volume=follower_volume,
                calibration=calibration,
                risk_budget_usd=risk_budget,
            )
        if protection is None:
            self._send_arm_ack(attempt.attempt_id, False, "cannot construct valid emergency protection for the follower leg")
            return
        self._attempt = attempt
        self._persist_attempt(attempt)
        self._leg = LegState(attempt_id=attempt.attempt_id, role="follower")
        self._leg.emergency_sl, self._leg.emergency_tp = protection
        self._leg.arm_state = "armed"
        self._persist_leg()
        self._desired = "ACTIVE"
        self._persist_desired()
        self._state = "ARMED"
        self._transition("armed", attempt.attempt_id)
        self._record_timing("arm_receive")
        self._send_arm_ack(attempt.attempt_id, True, "armed")

    def _send_arm_ack(self, attempt_id: str, armed: bool, reason: str, *, symbol: str | None = None) -> None:
        if self._contract is None or self._lease is None:
            return
        self._relay.send(
            {
                "kind": "arm_ack",
                "from_worker_id": self._worker_id,
                "to_worker_id": self._contract.peer_of(self._worker_id),
                "lease_epoch": self._lease.epoch,
                "payload": {
                    "attempt_id": attempt_id,
                    "armed": armed,
                    "reason": reason,
                    "symbol": symbol,
                    "release_marker": None if symbol is None else self._release_marker(symbol),
                },
            }
        )

    def _handle_arm_ack(self, payload: Mapping[str, object]) -> None:
        if self._attempt is None or self._leg is None or self._role() != "leader":
            return
        if payload.get("attempt_id") != self._attempt.attempt_id:
            return
        if payload.get("armed") is not True:
            if payload.get("reason") == "product is quarantined on the follower":
                symbol = payload.get("symbol")
                if isinstance(symbol, str) and self._contract is not None:
                    self._quarantine_product(
                        symbol,
                        offending_worker_id=self._contract.peer_of(self._worker_id),
                        attempt_id=self._attempt.attempt_id,
                        receipt={
                            "retcode": 10021,
                            "source": "follower_arm_rejection",
                            "release_marker": payload.get("release_marker"),
                        },
                        local_evidence=True,
                    )
            self._abandon_attempt(f"follower_arm_rejected: {payload.get('reason')}")
            return
        if self._state != "ARMING" or self._leg.arm_state != "armed":
            return  # a duplicate or late acknowledgement never re-commits
        self._state = "ARMED"
        self._transition("both_armed", self._attempt.attempt_id)
        self._record_timing("arm_ack")
        self._send_commit()

    def _send_commit(self) -> None:
        attempt, contract, leg = self._attempt, self._contract, self._leg
        if attempt is None or contract is None or leg is None or self._lease is None:
            return
        if self._attempt_authority(attempt) is None:
            self._abandon_attempt("lease_lost_before_commit")
            return
        # Durably authorize this exact attempt *before* the peer can act on the
        # commit: a crash between relay and persistence must never leave the
        # follower authorized while this leader is not.
        leg.commit_authorized = True
        self._persist_leg()
        self._transition("commit_authorized", attempt.attempt_id)
        self._relay.send(
            {
                "kind": "commit",
                "from_worker_id": self._worker_id,
                "to_worker_id": contract.peer_of(self._worker_id),
                "lease_epoch": self._lease.epoch,
                "payload": {"attempt_id": attempt.attempt_id},
            }
        )
        self._transition("commit_sent", attempt.attempt_id)
        self._record_timing("commit_send")

    def _handle_commit(self, payload: Mapping[str, object]) -> None:
        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None or self._role() != "follower":
            return
        if payload.get("attempt_id") != attempt.attempt_id:
            return
        if self._attempt_authority(attempt) is None:
            return  # a commit under a missing, revoked, expired, or superseded lease is not authority
        if self._now >= attempt.expires_at:
            return  # a late commit is rejected without any durable authorization
        ineligible = self._entry_ineligible_reason()
        if ineligible is not None:
            # A duplicate or delayed commit is still perfectly valid for this
            # exact attempt, epoch, and window, so validity alone cannot decide
            # it: an attempt that already left the entry path (desired EMPTY,
            # proven empty, converging, terminal, or past its own send
            # boundary) must never be re-armed back onto it.  Recording it and
            # returning also keeps a post-fill replay idempotent instead of
            # rewinding the lifecycle to ``ARMED``.
            self._transition("commit_ignored", ineligible)
            self._db.commit()
            return
        if not leg.commit_authorized:
            leg.commit_authorized = True
            self._persist_leg()
            self._transition("commit_authorized", attempt.attempt_id)
        self._transition("commit_received", attempt.attempt_id)
        self._record_timing("commit_receive")
        self._state = "ARMED"

    def _entry_ineligible_reason(self) -> str | None:
        """Why this attempt may no longer enter the market, or ``None``.

        One fail-closed predicate answers that question for every path that
        could still reach ``order_send``: an inbound commit and the send gate
        itself both consult it, so no ordering between them, and no future
        caller, can reopen the entry path once this cell decided to be empty.
        """

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None:
            return "no attempt is in flight"
        if self._needs_human is not None:
            return "the attempt requires human intervention"
        if self._desired != "ACTIVE":
            return f"desired state is {self._desired}"
        if leg.empty_verified:
            return "this leg is already proven empty"
        if self._state in ("CONVERGING_EMPTY", "EMPTY", "NEEDS_HUMAN"):
            return f"lifecycle state is {self._state}"
        if leg.arm_state != "armed":
            return f"arm state is {leg.arm_state}"
        if leg.entry_status != "pending":
            return f"entry status is {leg.entry_status}"
        return None

    def _maybe_send_commit(self) -> None:
        """Immediately before broker send: the last, fencing-adjacent check."""

        attempt, leg, contract = self._attempt, self._leg, self._contract
        if attempt is None or leg is None or contract is None or self._state != "ARMED":
            return
        if leg.entry_status != "pending":
            return
        ineligible = self._entry_ineligible_reason()
        if ineligible is not None:
            # Fail closed independently of every caller and of ``_progress``
            # ordering: convergence may have been decided by a peer status, an
            # operator close, or a broker fact earlier in this same pass, and
            # no such attempt may still reach MT5 entry.
            self._transition("entry_send_suppressed", ineligible)
            self._db.commit()
            return
        if self._now >= attempt.expires_at:
            self._abandon_attempt("expired_not_started")
            return
        if self._attempt_authority(attempt) is None:
            self._abandon_attempt("stale_lease_at_commit")
            return
        if not leg.commit_authorized:
            return  # armed is not committed: no durable commit exists for this exact attempt
        if self._now < attempt.execute_at:
            return
        admissible, admission_reason = self._entry_admissible()
        if not admissible:
            self._abandon_attempt(f"not_admissible_at_commit: {admission_reason}")
            return
        role = self._role()
        assert role is not None
        quote = self._local_quotes.get(attempt.symbol)
        if quote is None or not quote.fresh(self._now, contract.quote_max_age_seconds):
            self._abandon_attempt("quote_stale_at_commit")
            return
        self._state = "SENDING"
        self._transition("send_started", attempt.attempt_id)
        self._send_entry(attempt, role)

    # -- entry execution ---------------------------------------------------- #

    def _send_entry(self, attempt: Attempt, role: Role) -> None:
        assert self._leg is not None and self._contract is not None
        leg = self._leg
        effect_id = f"{attempt.attempt_id}:{self._worker_id}:entry"
        leg.entry_effect_id = effect_id
        direction = attempt.direction_of(role)
        volume = attempt.volume_of(role)
        payload = {
            "type": "market",
            "symbol": attempt.symbol,
            "volume": volume,
            "direction": direction,
            "filling_mode": self._contract.filling_mode,
            "sl": leg.emergency_sl,
            "tp": leg.emergency_tp,
        }
        if self._contract.mode == "shadow":
            quote = attempt.quote_of(role)
            fill_price = quote.ask if direction == "LONG" else quote.bid
            leg.entry_status = "filled"
            leg.ticket = f"shadow:{attempt.attempt_id}:{self._worker_id}"
            leg.fill_price = str(fill_price)
            self._persist_leg()
            self._state = "VERIFYING_PROTECTION"
            self._transition("shadow_entry_simulated", attempt.attempt_id)
            self._report_leg_status("filled", ticket=leg.ticket, fill_price=leg.fill_price)
            self._maybe_refine_protection()
            return
        try:
            self._journal.prepare(effect_id, payload)
        except EffectJournalError as error:
            leg.entry_status = "rejected"
            self._persist_leg()
            self._transition("entry_rejected_preflight", str(error))
            self._report_leg_status("rejected", reason=str(error))
            self._desired = "EMPTY"
            self._persist_desired()
            return
        outcome = self._run_broker_write(effect_id, payload, priority="execution")
        if outcome.category == "rejected":
            leg.entry_status = "rejected"
            self._persist_leg()
            self._transition("entry_rejected", attempt.attempt_id)
            receipt_retcode = outcome.receipt.get("retcode") if outcome.receipt is not None else None
            if receipt_retcode == 10021:
                self._quarantine_product(
                    attempt.symbol,
                    offending_worker_id=self._worker_id,
                    attempt_id=attempt.attempt_id,
                    receipt=outcome.receipt,
                    local_evidence=True,
                )
            self._report_leg_status("rejected", receipt_retcode=receipt_retcode)
            self._desired = "EMPTY"
            self._persist_desired()
        elif outcome.category == "expired_not_started":
            leg.entry_status = "expired_not_started"
            self._persist_leg()
            self._transition("entry_expired_not_started", attempt.attempt_id)
            self._report_leg_status("expired_not_started")
            self._desired = "EMPTY"
            self._persist_desired()
        elif outcome.category == "unknown_after_send":
            leg.entry_status = "unknown_after_send"
            self._persist_leg()
            self._state = "ENTRY_UNCERTAIN"
            self._transition("entry_unknown_after_send", attempt.attempt_id)
            self._request_broker_read()
        else:
            leg.entry_status = "sent"
            self._persist_leg()
            self._transition("entry_sent", attempt.attempt_id)
            self._report_leg_status("sent")
            self._request_broker_read()

    def _run_broker_write(
        self, effect_id: str, payload: dict[str, object], *, priority: Literal["execution", "protection", "emergency"]
    ) -> BrokerWriteResult:
        request = {
            "request_id": effect_id,
            "command_id": effect_id,
            "kind": "operation",
            "payload": payload,
            "priority": priority,
            "effect_id": effect_id,
        }
        admitted = self._scheduler.admit(request)
        if isinstance(admitted, TraderRpcOutcome):
            return BrokerWriteResult(_map_scheduler_category(admitted.category))
        scheduled = self._dequeue_own_item(effect_id)
        if isinstance(scheduled, TraderRpcOutcome):
            return BrokerWriteResult(_map_scheduler_category(scheduled.category))
        if scheduled is None:
            return BrokerWriteResult("unknown_after_send")
        self._record_timing("scheduler_dequeue")
        try:
            scheduled.before_broker_send()
        except BrokerActionNotStarted as error:
            # The scheduler already recorded this item's terminal outcome and
            # released its single serialization slot.
            return BrokerWriteResult(_map_scheduler_category(error.outcome.category))
        # From here the item owns the Worker's one MT5 serialization slot.  It
        # must be completed exactly once on *every* path -- including journal
        # errors, MT5 exceptions, and missing receipts -- or emergency
        # containment and ordinary Worker RPC work could never run again.
        category: TraderRpcOutcomeCategory = "unknown_after_send"
        reason = "the broker write ended without a conclusive result"
        try:
            try:
                self._journal.mark_send_started(effect_id)
            except EffectJournalError as error:
                reason = f"the effect journal refused the send_started boundary: {error}"
                self._transition("effect_journal_refused_send_started", str(error))
                return BrokerWriteResult("unknown_after_send")
            self._record_timing("mt5_send_started")
            try:
                raw = self._mt5.order_send(payload)
            except Exception as error:  # the irreversible boundary was crossed; never resend
                self._transition("mt5_call_raised", str(error))
                reason = f"the MT5 call raised after send_started: {error}"
                return BrokerWriteResult("unknown_after_send")
            receipt = _receipt(raw)
            if receipt is None:
                self._transition("mt5_returned_no_receipt", effect_id)
                reason = "MT5 returned no usable receipt after send_started"
                return BrokerWriteResult("unknown_after_send")
            try:
                self._journal.record_receipt(effect_id, receipt)
            except EffectJournalError as error:
                self._transition("effect_journal_refused_receipt", str(error))
                reason = f"the effect journal refused the broker receipt: {error}"
                return BrokerWriteResult("unknown_after_send")
            self._record_timing("broker_response")
            category, reason = "completed", "broker receipt observed"
            if receipt.get("retcode") not in (10008, 10009):  # TRADE_RETCODE_PLACED / DONE
                return BrokerWriteResult("rejected", receipt)
            return BrokerWriteResult("completed", receipt)
        finally:
            scheduled.complete(category, reason)

    def _dequeue_own_item(self, effect_id: str) -> ScheduledTraderRpc | TraderRpcOutcome | None:
        """Drain the shared Worker scheduler until *this* admitted item surfaces.

        ``scheduler.next()`` returns the highest-priority ready item across
        every source of Worker broker writes, not necessarily the item this
        call just admitted. Any other dequeued item is fully served (and, for
        the terminal-outcome case, dispatched) through the injected callbacks
        before this loop asks the scheduler for the next one, so a foreign
        item is never left half-dequeued (which would otherwise deadlock the
        scheduler's single ``_active`` slot) and priority/serialization are
        preserved exactly as for ordinary Trader RPC work.
        """

        while True:
            candidate = self._scheduler.next()
            if candidate is None:
                return None
            if isinstance(candidate, TraderRpcOutcome):
                if candidate.request_id == effect_id:
                    return candidate
                if self._dispatch_foreign_scheduler_outcome is not None:
                    self._dispatch_foreign_scheduler_outcome(candidate)
                continue
            if candidate.command_id == effect_id:
                return candidate
            if self._serve_foreign_scheduler_item is None:
                # No production wiring supplied a way to serve unrelated Worker
                # scheduler work; only reachable if this cell was given a
                # scheduler shared with other traffic without also wiring the
                # foreign-item callbacks.  The item is still explicitly
                # terminalized (never silently dropped, never resent) so the
                # scheduler's single serialization slot is released and this
                # Worker's emergency work stays runnable.
                outcome = candidate.complete(
                    "rejected_worker_state",
                    "No Worker handler is wired to serve this scheduled Trader RPC.",
                )
                self._transition("foreign_scheduler_item_unservable", candidate.command_id)
                if self._dispatch_foreign_scheduler_outcome is not None:
                    self._dispatch_foreign_scheduler_outcome(outcome)
                continue
            self._serve_foreign_scheduler_item(candidate)

    def _request_broker_read(self) -> None:
        """Actively verify owned facts with execution-priority scheduling."""

        if self._attempt is None:
            return
        try:
            positions = self._mt5.positions_get()
            orders = self._mt5.orders_get()
        except Exception:
            return  # a transient read failure never establishes any fact; retried by the next event
        self._apply_broker_snapshot(BrokerSnapshotEvent(orders=_records(orders), positions=_records(positions), observed_at=self._now))

    # -- broker fact application (false-empty-safe) -------------------------- #

    def _apply_broker_snapshot(self, event: BrokerSnapshotEvent) -> None:
        if self._attempt is None or self._leg is None:
            return
        if event.orders is None or event.positions is None:
            return  # None/unavailable never establishes any fact
        attempt, leg = self._attempt, self._leg
        protection_already_refined = leg.protection_status == "refined"
        role = leg.role
        symbol, direction, volume = attempt.symbol, attempt.direction_of(role), attempt.volume_of(role)
        matches = [p for p in event.positions if _position_matches(p, symbol, direction, volume)]
        if leg.entry_status in ("sent", "unknown_after_send") and len(matches) > 1:
            # Two same-shape positions cannot be attributed to this attempt by
            # shape alone.  Claiming one would risk mutating unrelated exposure
            # and leaving real attempt exposure untracked, so this stays
            # unattributed, converges, and remains operator-inspectable.
            self._transition("ambiguous_position_attribution", attempt.attempt_id)
            if self._desired != "EMPTY":
                self._record_timing("containment_start")
            self._desired = "EMPTY"
            self._persist_desired()
        elif leg.entry_status in ("sent", "unknown_after_send") and matches:
            position = matches[0]
            leg.ticket = str(position.get("ticket"))
            fill = _to_decimal(position.get("price_open", position.get("fill_price")))
            leg.fill_price = None if fill is None else str(fill)
            leg.entry_status = "filled"
            self._persist_leg()
            self._state = "VERIFYING_PROTECTION"
            self._transition("entry_filled_observed", leg.ticket)
            self._record_timing("position_observed")
            self._report_leg_status("filled", ticket=leg.ticket, fill_price=leg.fill_price)
            self._maybe_refine_protection()
        elif leg.entry_status == "unknown_after_send" and not matches:
            leg.entry_status = "rejected"
            self._persist_leg()
            self._transition("unknown_after_send_resolved_absent", attempt.attempt_id)
            self._report_leg_status("rejected", reason="fresh observation proves no owned position exists")
            self._desired = "EMPTY"
            self._persist_desired()
        if leg.ticket is not None:
            owned = next((p for p in event.positions if str(p.get("ticket")) == leg.ticket), None)
            if owned is None and leg.entry_status == "filled" and not leg.empty_verified:
                # the owned ticket disappeared: treat it as an external/close event needing convergence
                self._desired = "EMPTY"
                self._persist_desired()
            elif owned is not None and protection_already_refined:
                # Compare only against a snapshot captured before this call's own
                # refinement effect, never against the just-mutated in-flight state.
                observed_sl, observed_tp = _to_decimal(owned.get("sl")), _to_decimal(owned.get("tp"))
                expected_sl, expected_tp = _to_decimal(leg.refined_sl), _to_decimal(leg.refined_tp)
                if observed_sl != expected_sl or observed_tp != expected_tp:
                    self._transition("integrity_divergence", "protection mismatch")
                    self._desired = "EMPTY"
                    self._persist_desired()
        if self._desired == "EMPTY":
            self._advance_convergence(event)

    def _maybe_refine_protection(self) -> None:
        attempt, leg, contract = self._attempt, self._leg, self._contract
        if attempt is None or leg is None or contract is None or leg.protection_status != "pending":
            return
        if leg.entry_status != "filled" or leg.fill_price is None:
            return
        role = leg.role
        direction = attempt.direction_of(role)
        volume = _to_decimal(attempt.volume_of(role))
        calibration = self._local_calibrations.get(attempt.symbol)
        risk_budget = _to_decimal(contract.risk_budget_usd)
        fill_price = _to_decimal(leg.fill_price)
        protection = None
        if volume is not None and calibration is not None and risk_budget is not None and fill_price is not None:
            protection = compute_protection(
                entry=fill_price, direction=direction, volume=volume, calibration=calibration, risk_budget_usd=risk_budget
            )
        if protection is None:
            leg.protection_status = "failed"
            self._persist_leg()
            self._transition("protection_refinement_failed", "cannot construct refined protection")
            self._report_leg_status("protection_failed")
            self._desired = "EMPTY"
            self._persist_desired()
            return
        sl, tp = protection
        effect_id = f"{attempt.attempt_id}:{self._worker_id}:protection"
        leg.protection_effect_id = effect_id
        payload = {"type": "modify_sl_tp", "symbol": attempt.symbol, "position": leg.ticket, "sl": sl, "tp": tp}
        if contract.mode == "shadow":
            leg.refined_sl, leg.refined_tp, leg.protection_status = sl, tp, "refined"
            self._persist_leg()
            self._report_leg_status("protection_refined", sl=sl, tp=tp)
            return
        try:
            self._journal.prepare(effect_id, payload)
        except EffectJournalError:
            leg.protection_status = "failed"
            self._persist_leg()
            self._desired = "EMPTY"
            self._persist_desired()
            return
        outcome = self._run_broker_write(effect_id, payload, priority="protection")
        if outcome.category == "completed":
            leg.refined_sl, leg.refined_tp, leg.protection_status = sl, tp, "refined"
            self._persist_leg()
            self._transition("protection_refined", attempt.attempt_id)
            self._record_timing("protection_verified")
            self._report_leg_status("protection_refined", sl=sl, tp=tp)
        else:
            leg.protection_status = "failed"
            self._persist_leg()
            self._transition("protection_refinement_failed", outcome.category)
            self._report_leg_status("protection_failed", reason=outcome.category)
            self._desired = "EMPTY"
            self._persist_desired()

    def _maybe_declare_active(self) -> None:
        if self._leg is None or self._leg.protection_status != "refined":
            return
        if self._peer_leg.status != "protection_refined":
            return
        if self._desired != "EMPTY" and (self._state != "ACTIVE" or self._active_since is None):
            self._state = "ACTIVE"
            if self._active_since is None:
                self._active_since = self._now
                self._persist_active_since()
                self._transition("active_verified", _iso(self._active_since))
                self._record_timing("terminal_time")

    def _report_leg_status(
        self,
        status: str,
        *,
        ticket: str | None = None,
        fill_price: str | None = None,
        sl: str | None = None,
        tp: str | None = None,
        reason: str = "",
        receipt_retcode: object = None,
    ) -> None:
        if self._contract is None or self._lease is None or self._attempt is None:
            return
        self._relay.send(
            {
                "kind": "leg_status",
                "from_worker_id": self._worker_id,
                "to_worker_id": self._contract.peer_of(self._worker_id),
                "lease_epoch": self._lease.epoch,
                "payload": {
                    "attempt_id": self._attempt.attempt_id,
                    "status": status,
                    "ticket": ticket,
                    "fill_price": fill_price,
                    "sl": sl,
                    "tp": tp,
                    "reason": reason,
                    "symbol": self._attempt.symbol,
                    "receipt_retcode": receipt_retcode,
                    "release_marker": self._release_marker(self._attempt.symbol),
                },
            }
        )

    def _handle_peer_leg_status(self, payload: Mapping[str, object]) -> None:
        status = payload.get("status")
        attempt_id = payload.get("attempt_id")
        if not isinstance(status, str) or not isinstance(attempt_id, str):
            return
        symbol = payload.get("symbol")
        if payload.get("receipt_retcode") == 10021 and isinstance(symbol, str) and self._contract is not None:
            self._quarantine_product(
                symbol,
                offending_worker_id=self._contract.peer_of(self._worker_id),
                attempt_id=attempt_id,
                receipt={
                    "retcode": 10021,
                    "source": "peer_leg_status",
                    "release_marker": payload.get("release_marker"),
                },
            )
        if self._attempt is None or attempt_id != self._attempt.attempt_id:
            # A status for an attempt this Worker already terminalized (both
            # sides proven safe) or never saw is still recorded durably so an
            # operator can locate every piece of peer evidence; it never
            # resurrects or mutates a resolved attempt.
            self._record_late_peer_leg_status(attempt_id, status, payload)
            return
        self._peer_leg = PeerLegView(
            status=status,
            ticket=cast(str | None, payload.get("ticket")),
            fill_price=cast(str | None, payload.get("fill_price")),
            sl=cast(str | None, payload.get("sl")),
            tp=cast(str | None, payload.get("tp")),
            reason=cast(str, payload.get("reason") or ""),
        )
        self._persist_peer_leg()
        self._transition("peer_leg_status", status)
        if status in ("rejected", "expired_not_started", "protection_failed"):
            # asymmetric entry: contain this side's exposure immediately.
            if self._desired != "EMPTY":
                self._record_timing("containment_start")
            self._desired = "EMPTY"
            self._persist_desired()
        elif status == "empty":
            # The peer converged to empty (whether by rejection, expiry, or a
            # clean close); any exposure on this side is now asymmetric.
            if self._desired != "EMPTY":
                self._record_timing("containment_start")
            self._desired = "EMPTY"
            self._persist_desired()
            self._maybe_finalize_empty()

    def _quarantine_product(
        self,
        symbol: str,
        *,
        offending_worker_id: str,
        attempt_id: str,
        receipt: Mapping[str, object],
        local_evidence: bool = False,
        commit: bool = True,
    ) -> bool:
        marker = self._release_marker(symbol)
        evidence = dict(receipt)
        if local_evidence:
            evidence["release_marker"] = marker
        elif evidence.get("release_marker") != marker:
            return False
        existing = self._db.execute(
            "SELECT 1 FROM cell_product_quarantine WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        if existing is not None:
            return False
        released = self._db.execute(
            """
            SELECT 1 FROM cell_product_quarantine_release
            WHERE symbol = ? AND offending_worker_id = ? AND attempt_id = ?
            """,
            (symbol, offending_worker_id, attempt_id),
        ).fetchone()
        if released is not None:
            return False
        inserted = self._db.execute(
            """
            INSERT INTO cell_product_quarantine (
                symbol, offending_worker_id, attempt_id, receipt, quarantined_at
            ) VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(symbol) DO NOTHING
            """,
            (symbol, offending_worker_id, attempt_id, _canonical_json(evidence), _iso(self._now)),
        ).rowcount
        if inserted:
            self._transition("product_quarantined", f"{symbol}: MT5 retcode 10021")
        if commit and inserted:
            self._db.commit()
        return bool(inserted)

    def _product_quarantine_records(self) -> list[dict[str, object]]:
        rows = self._db.execute(
            """
            SELECT symbol, offending_worker_id, attempt_id, receipt, quarantined_at
            FROM cell_product_quarantine
            ORDER BY symbol
            """
        ).fetchall()
        return [
            {
                "symbol": str(row[0]),
                "offending_worker_id": str(row[1]),
                "attempt_id": str(row[2]),
                "receipt": json.loads(str(row[3])),
                "quarantined_at": str(row[4]),
            }
            for row in rows
        ]

    def _accept_peer_product_quarantines(self, raw: object, peer_id: str) -> None:
        if not isinstance(raw, list):
            return
        changed = False
        for item in raw:
            if not isinstance(item, dict):
                continue
            symbol = item.get("symbol")
            attempt_id = item.get("attempt_id")
            offending_worker_id = item.get("offending_worker_id")
            receipt = item.get("receipt")
            if (
                not isinstance(symbol, str)
                or not isinstance(attempt_id, str)
                or offending_worker_id != peer_id
                or not isinstance(receipt, dict)
                or receipt.get("retcode") != 10021
            ):
                continue
            changed = self._quarantine_product(
                symbol,
                offending_worker_id=peer_id,
                attempt_id=attempt_id,
                receipt=receipt,
                commit=False,
            ) or changed
        if changed:
            self._db.commit()

    def _release_product_quarantine(self, event: QuarantineReleaseEvent) -> None:
        if self._attempt is not None and not self._attempt_terminal():
            self._transition("product_quarantine_release_rejected", f"{event.symbol}: unresolved attempt")
            self._db.commit()
            return
        self._db.execute(
            """
            INSERT INTO cell_product_release_marker (symbol, marker) VALUES (?, ?)
            ON CONFLICT(symbol) DO UPDATE SET marker = excluded.marker
            """,
            (event.symbol, _iso(event.observed_at)),
        )
        self._db.execute(
            """
            INSERT OR IGNORE INTO cell_product_quarantine_release (
                symbol, offending_worker_id, attempt_id, released_at
            )
            SELECT symbol, offending_worker_id, attempt_id, ?
            FROM cell_product_quarantine
            WHERE symbol = ?
            """,
            (_iso(self._now), event.symbol),
        )
        deleted = self._db.execute(
            "DELETE FROM cell_product_quarantine WHERE symbol = ?",
            (event.symbol,),
        ).rowcount
        if deleted:
            self._transition(
                "product_quarantine_released",
                f"{event.symbol}: actor={event.actor}; reason={event.reason}",
            )
        self._db.commit()

    def _release_marker(self, symbol: str) -> str | None:
        row = self._db.execute(
            "SELECT marker FROM cell_product_release_marker WHERE symbol = ?",
            (symbol,),
        ).fetchone()
        return None if row is None else str(row[0])

    def _record_late_peer_leg_status(self, attempt_id: str, status: str, payload: Mapping[str, object]) -> None:
        self._db.execute(
            "INSERT INTO cell_peer_leg (attempt_id, payload, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(attempt_id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
            (attempt_id, json.dumps(dict(payload), default=str), _iso(self._now)),
        )
        self._db.execute(
            "INSERT INTO cell_transitions (attempt_id, event, detail, at) VALUES (?, ?, ?, ?)",
            (attempt_id, "late_peer_leg_status", status, _iso(self._now)),
        )
        self._db.commit()

    # -- desired-EMPTY convergence (one implementation for every exit path) --- #

    def request_close(self, reason: str) -> PairResult:
        """Any exit policy (risk, cutoff, timed exit, shutdown, integrity, break
        -glass) converges through this single owned desired-empty path."""

        self._begin_close(reason)
        if self._attempt is not None:
            self._request_broker_read()
            self._progress()
        return self._result()

    def _begin_close(self, reason: str) -> None:
        if self._attempt is None or self._desired == "EMPTY":
            return
        self._desired = "EMPTY"
        self._persist_desired()
        self._transition("close_requested", reason)

    def _maybe_progress_convergence(self) -> None:
        if self._desired != "EMPTY" or self._attempt is None or self._leg is None:
            return
        self._state = "CONVERGING_EMPTY"
        if self._leg.empty_verified:
            return  # this side's proof is already durable; avoid a redundant read/relay loop
        self._request_broker_read()

    def _maybe_finalize_empty(self) -> None:
        if self._attempt is None or self._leg is None:
            return
        if not self._leg.empty_verified:
            return
        if self._peer_leg.status != "empty" and self._peer_send_possible():
            return  # the peer may hold the mirror leg; never declare terminal on one side's proof
        self._state = "EMPTY"
        self._record_timing("terminal_time")
        self._terminate_attempt("both_empty_verified")
        self._reset_attempt()

    def _peer_send_possible(self) -> bool:
        """Whether the peer could have crossed its own irreversible send boundary.

        This is a structural statement about the arm/commit protocol, not a
        peer claim: a follower can never send without this leader's durable
        commit, so an attempt this leader never committed is provably
        one-sided and may terminalize on this Worker's own fresh empty facts.
        Every other case waits for the peer's broker-observed empty proof.
        """

        leg = self._leg
        if leg is None:
            return False
        if self._peer_leg.status not in ("unknown", "rejected", "expired_not_started", "empty"):
            return True  # the peer already reported starting or holding something
        if leg.role == "leader":
            return leg.commit_authorized
        return True  # the leader may send its own leg as soon as this follower acknowledged its arm

    def _advance_convergence(self, event: BrokerSnapshotEvent) -> None:
        if self._attempt is None or self._leg is None:
            return  # a nested, recursively delivered message already resolved this attempt
        attempt, leg = self._attempt, self._leg
        assert event.orders is not None and event.positions is not None
        # V1 entries are market orders only; there is no attempt-owned pending
        # order to cancel before close. Only the exact attempt-owned position
        # ticket is ever mutated (never matched by symbol/shape alone).
        if leg.ticket is not None:
            owned_positions = [p for p in event.positions if str(p.get("ticket")) == leg.ticket]
            if owned_positions:
                if self._needs_human is not None:
                    return
                self._close_owned(owned_positions[0])
                return
        if not event.orders and not event.positions and not leg.empty_verified:
            leg.empty_verified = True
            self._persist_leg()
            self._transition("empty_verified", attempt.attempt_id)
            self._state = "CONVERGING_EMPTY"
            self._report_leg_status("empty")
            self._maybe_finalize_empty()

    def _close_owned(self, position: dict[str, object]) -> None:
        assert self._attempt is not None and self._leg is not None and self._contract is not None
        ticket = str(position.get("ticket"))
        volume = position.get("volume")
        effect_id = f"{self._attempt.attempt_id}:{self._worker_id}:close:{ticket}"
        self._leg.close_effect_id = effect_id
        payload = {"type": "close", "ticket": ticket, "volume": str(volume)}
        if self._contract.mode == "shadow":
            self._request_broker_read()
            return
        try:
            effect_state = self._journal.prepare(effect_id, payload)
        except EffectJournalError as error:
            self._set_needs_human(
                f"close effect {effect_id} could not be prepared: {error}",
                "effect journal prepare failed",
            )
            return
        if effect_state in ("receipt", "observed"):
            try:
                evidence = self._journal.evidence(effect_id)
            except EffectJournalError as error:
                self._set_needs_human(
                    f"close effect {effect_id} has invalid replay evidence: {error}",
                    "invalid close replay evidence",
                )
                return
            if evidence.get("retcode") in (10008, 10009):
                self._transition("close_receipt_recovered", effect_id)
                self._db.commit()
                return
            self._set_needs_human(
                f"close effect {effect_id} has non-success replay evidence",
                "close replay evidence was not successful",
            )
            return
        if effect_state == "send_started":
            self._set_needs_human(
                f"close effect {effect_id} has no conclusive broker receipt",
                "close remained uncertain after send_started",
            )
            return
        outcome = self._run_broker_write(effect_id, payload, priority="emergency")
        if outcome.category != "completed":
            self._set_needs_human(f"close effect {effect_id} ended {outcome.category}", outcome.category)

    # -- observable result --------------------------------------------------- #

    def _result(self) -> PairResult:
        ready, reason = self._local_ready() if self._contract is not None else (False, "no contract activated")
        return PairResult(
            worker_id=self._worker_id,
            role=self._role(),
            lease_epoch=None if self._lease is None or self._lease_revoked else self._lease.epoch,
            state=self._state,
            ready=ready,
            ready_reason=reason,
            attempt_id=None if self._attempt is None else self._attempt.attempt_id,
            desired_state=self._desired,
            needs_human=self._needs_human is not None,
            needs_human_reason=self._needs_human,
            recovering=self._recovering,
            timing=dict(self._last_timing),
        )

    def close(self) -> None:
        self._db.close()

    def quarantined_products(self) -> tuple[str, ...]:
        rows = self._db.execute("SELECT symbol FROM cell_product_quarantine ORDER BY symbol").fetchall()
        return tuple(str(row[0]) for row in rows)

    def quarantine_release_marker(self, symbol: str) -> str | None:
        return self._release_marker(symbol)


# --------------------------------------------------------------------------- #
# Small module-level converters
# --------------------------------------------------------------------------- #


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise PairExecutionCellError("Pair Execution Cell timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat()


def _contract_from_canonical(value: dict[str, object]) -> PairContract:
    return PairContract(
        contract_id=cast(str, value["contract_id"]),
        leader_worker_id=cast(str, value["leader_worker_id"]),
        follower_worker_id=cast(str, value["follower_worker_id"]),
        trader_id=cast(str, value["trader_id"]),
        symbol=cast(str | None, value.get("symbol")),
        leader_direction=cast(Direction | None, value.get("leader_direction")),
        follower_direction=cast(Direction | None, value.get("follower_direction")),
        leader_volume=cast(str, value["leader_volume"]),
        follower_volume=cast(str, value["follower_volume"]),
        filling_mode=cast(Literal["FOK", "IOC"], value["filling_mode"]),
        edge_threshold=cast(str | None, value.get("edge_threshold")),
        risk_budget_usd=cast(str, value["risk_budget_usd"]),
        quote_max_age_seconds=cast(float, value["quote_max_age_seconds"]),
        quote_max_skew_seconds=cast(float, value["quote_max_skew_seconds"]),
        arm_timeout_seconds=cast(float, value["arm_timeout_seconds"]),
        commit_lead_seconds=cast(float, value["commit_lead_seconds"]),
        execution_expiry_seconds=cast(float, value["execution_expiry_seconds"]),
        mode=cast(ExecutionMode, value.get("mode", "live")),
        maximum_holding_seconds=cast(float | None, value.get("maximum_holding_seconds")),
        flatten_at_ny=cast(str | None, value.get("flatten_at_ny")),
        entry_edge_points=cast(str | None, value.get("entry_edge_points")),
        eligible_symbols=tuple(cast(list[str], value.get("eligible_symbols", []))),
    )


def _leg_to_json(leg: LegState) -> dict[str, object]:
    return {
        "attempt_id": leg.attempt_id,
        "role": leg.role,
        "arm_state": leg.arm_state,
        "arm_reason": leg.arm_reason,
        "commit_authorized": leg.commit_authorized,
        "emergency_sl": leg.emergency_sl,
        "emergency_tp": leg.emergency_tp,
        "entry_effect_id": leg.entry_effect_id,
        "entry_status": leg.entry_status,
        "ticket": leg.ticket,
        "fill_price": leg.fill_price,
        "protection_effect_id": leg.protection_effect_id,
        "protection_status": leg.protection_status,
        "refined_sl": leg.refined_sl,
        "refined_tp": leg.refined_tp,
        "cancel_effect_id": leg.cancel_effect_id,
        "close_effect_id": leg.close_effect_id,
        "empty_verified": leg.empty_verified,
    }


def _leg_from_json(value: dict[str, object]) -> LegState:
    return LegState(**value)


def _map_scheduler_category(
    category: str,
) -> Literal["completed", "rejected", "expired_not_started", "unknown_after_send"]:
    if category == "completed":
        return "completed"
    if category == "expired_not_started":
        return "expired_not_started"
    if category in ("rejected_preflight", "rejected_worker_state"):
        return "rejected"
    return "unknown_after_send"


def _receipt(raw: object) -> dict[str, object] | None:
    if raw is None:
        return None
    if isinstance(raw, dict):
        return raw
    result: dict[str, object] = {}
    for field_name in ("retcode", "deal", "order", "volume", "price", "bid", "ask", "comment"):
        if hasattr(raw, field_name):
            result[field_name] = getattr(raw, field_name)
    return result if result else None


def _records(value: object) -> list[dict[str, object]] | None:
    """MT5 ``None``/malformed results are read failures, never an empty fact."""

    if value is None:
        return None
    if not isinstance(value, (list, tuple)):
        return None
    records: list[dict[str, object]] = []
    for item in value:
        if isinstance(item, dict):
            records.append(item)
        elif hasattr(item, "_asdict"):
            records.append(item._asdict())
        else:
            return None
        continue
    return records


def _position_matches(position: dict[str, object], symbol: str, direction: Direction, volume: str) -> bool:
    if str(position.get("symbol")) != symbol:
        return False
    side = position.get("type", position.get("side"))
    expected_side = 0 if direction == "LONG" else 1
    if side not in (expected_side, direction, "0" if direction == "LONG" else "1"):
        return False
    observed_volume = _to_decimal(position.get("volume"))
    expected_volume = _to_decimal(volume)
    if observed_volume is None or expected_volume is None:
        return False
    return observed_volume == expected_volume
