"""Low-Latency Pair Execution Cell.

One deep module runs on both paired account Workers.  The pair route is
durable and carries exactly one leader and one follower; the controller only
authenticates the two Workers, arbitrates exclusive route ownership, and
relays opaque envelopes.  There is no controller-issued Pair Contract, no Pair
Lease, no arm/commit round trip, no common future execution time, and no
execution expiry.

The external interface is deliberately tiny:

* :meth:`PairExecutionCell.freeze_pairing_acceptance` - the follower authors
  its own frozen startup balance and risk tunables and publishes them to the
  leader, which copies them verbatim into the one canonical policy.
* :meth:`PairExecutionCell.accept_policy` - the leader supplies the canonical,
  versioned strategy policy (shared decision rules *and* both Workers' risk
  configurations); the follower durably accepts the full content only while
  its account is broker-verified empty and only when its own block is a
  byte-identical copy of what it authored.
* :meth:`PairExecutionCell.handle_event` - accept one typed local, peer,
  broker, route, or clock event and return the current observable result.
* :meth:`PairExecutionCell.recover` - resume durable state after a process
  restart before entry readiness is restored.
* :meth:`PairExecutionCell.request_close` - any exit policy converges through
  one desired-``EMPTY`` path.
* :meth:`PairExecutionCell.safe_unpair_assertion` - the local, state-version
  bound evidence the controller arbitrates a safe unpair from.

Identity is split three ways and the three are never compared with each other:

* ``route_id`` names one pairing relationship.  It is controller-generated,
  globally unique, never reused, has no TTL, is never renewed, never fences a
  broker effect, and confers no trading authorization.
* ``universe_generation`` names one Initial Compatible Product Discovery
  result within a route.  An explicit rediscovery installs a new generation on
  the *same* route.
* ``recovery_epoch`` is market evidence about one Worker's own broker session
  continuity and travels only on quote evidence.

There is no Trader identity anywhere in this module.

Callers never manage effect IDs, retries, timers, protection refinement, or
containment directly.  Durable state (durable route and ``route_id``, assigned
role, current ``universe_generation``, the frozen discovered universe, the full
canonical policy content and its hash, sizing-plan versions including retained
superseded ones, the local attempt/effect state version, attempt, timer start
evidence, quote evidence, local effects, peer outcomes, exact tickets,
protection, desired state, quarantine, realized loss, and transition history)
lives in a private SQLite/WAL database.  Broker-side effects reuse the Worker's
existing :class:`~abt.worker.effect_journal.WorkerEffectJournal` (ADR-0008
no-blind-retry semantics) and
:class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler` (single MT5
serialization slot, priority ordering).

Clock, relay, journal, and MT5 access are all injected adapters so timeout,
discovery, unpair, and crash races are reproducible without a live broker.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_EVEN
from hashlib import sha256
import json
import logging
import math
import sqlite3
import time as _time
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


_LOGGER = logging.getLogger(__name__)


class PairExecutionCellError(RuntimeError):
    """Raised for malformed policy, route, discovery, or event input."""


Role = Literal["leader", "follower"]
ExecutionMode = Literal["live", "shadow"]
Direction = Literal["LONG", "SHORT"]
FillingMode = Literal["FOK", "IOC"]
RouteState = Literal["ACTIVE", "UNPAIRING"]

PROTOCOL_VERSION = 4
NEW_YORK = ZoneInfo("America/New_York")
PRICE_OFF_RETCODE = 10021
_SUCCESS_RETCODES = (10008, 10009)
_SIZING_REFRESH_SECONDS = 3600.0
_MAXIMUM_HOLDING_SECONDS = 7 * 24 * 60 * 60
# The bounded relay-handling window: the maximum time the design allows between
# a leader's dispatch and the follower's admission decision on it.  Superseded
# sizing-plan versions stay valid for validating an already-dispatched attempt
# for the confirmation timeout *plus* this window, so a routine hourly refresh
# never guarantees rejection of an attempt that is already in flight.
RELAY_HANDLING_WINDOW_SECONDS = 5.0
# An explicit rediscovery is one bounded round trip.  The requesting Worker
# resends its idempotent request on this cadence and gives up at the deadline,
# so a dropped request or a leader restart mid-attempt can never strand it.
REDISCOVERY_RETRY_SECONDS = 5.0
REDISCOVERY_TIMEOUT_SECONDS = 30.0
# The MT5 `trade_mode` that means full trading; discovery applies it to each
# catalog *before* the intersection, exactly as `strategy/realtime_arbitrage`.
FULL_TRADING_MODE = 4
# Bounded recovery when the peer's own terminal proof is missing: probe every
# confirmation timeout, then become loud.  `EMPTY` is never inferred from
# silence.
_PEER_PROOF_PROBE_MULTIPLIER = 1.0
_PEER_PROOF_ESCALATION_MULTIPLIER = 6.0
# Statuses that are durable proof the peer created no broker exposure.
_PEER_NON_SEND_STATUSES = ("rejected", "expired_not_started", "no_attempt_record")
_PEER_TERMINAL_STATUSES = _PEER_NON_SEND_STATUSES + ("empty",)

PairState = Literal[
    "IDLE",
    "ENTERING",
    "ENTRY_UNCERTAIN",
    "AWAITING_PAIR_CONFIRMATION",
    "PROTECTING",
    "ACTIVE",
    "CONVERGING_EMPTY",
    "EMPTY",
    "NEEDS_HUMAN",
]
DesiredState = Literal["NONE", "ACTIVE", "EMPTY"]


# --------------------------------------------------------------------------- #
# Pure helpers
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


def decimal_text(value: object) -> str | None:
    """One canonical plain-decimal spelling, so ``"1.0"`` and ``1`` agree.

    Derived product identity is a hash over exactly this spelling, which is why
    two brokers reporting the same contract size in different notations must
    still produce the same identity.
    """

    number = _to_decimal(value)
    if number is None:
        return None
    normalized = number.normalize()
    exponent = normalized.as_tuple().exponent
    if isinstance(exponent, int) and exponent > 0:
        normalized = normalized.quantize(Decimal(1))
    return format(normalized, "f")


def _positive_text(value: object) -> str | None:
    number = _to_decimal(value)
    if number is None or number <= 0:
        return None
    return decimal_text(number)


def _parse_ny_time(value: str) -> time:
    try:
        hour_text, minute_text = value.split(":")
        return time(int(hour_text), int(minute_text))
    except (ValueError, TypeError) as error:
        raise PairExecutionCellError("flatten_at_ny must be 'HH:MM'.") from error


def _parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise PairExecutionCellError("A persisted or relayed timestamp is missing its UTC offset.")
    return parsed.astimezone(UTC)


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        raise PairExecutionCellError("Pair Execution Cell timestamps must be timezone-aware.")
    return value.astimezone(UTC)


def _iso(value: datetime) -> str:
    return value.isoformat()


def _ny_day(value: datetime) -> date:
    return _as_utc(value).astimezone(NEW_YORK).date()


def floor_to_volume_step(
    raw_lots: Decimal, volume_min: Decimal, volume_max: Decimal, volume_step: Decimal
) -> Decimal | None:
    """Snap ``raw_lots`` down onto the shared broker volume grid, or ``None``."""

    if volume_step <= 0 or volume_min <= 0 or volume_max < volume_min or raw_lots < volume_min:
        return None
    capped = min(raw_lots, volume_max)
    steps = ((capped - volume_min) / volume_step).to_integral_value(rounding=ROUND_FLOOR)
    lots = volume_min + steps * volume_step
    if lots < volume_min or lots > volume_max:
        return None
    return lots.normalize() + Decimal(0)


def _mirror(direction: Direction) -> Direction:
    return "SHORT" if direction == "LONG" else "LONG"


def _table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    try:
        rows = connection.execute(f"PRAGMA table_info({table})").fetchall()
    except sqlite3.Error:  # pragma: no cover - a missing table reads as no columns
        return set()
    return {str(row[1]) for row in rows}


def _pair_cell_schema_migrations(connection: sqlite3.Connection) -> list[tuple[str, str]]:
    """The idempotent DDL this database still needs, as ``(name, statement)``.

    Every step is decided from ``PRAGMA table_info`` rather than from a version
    counter, so re-running it on an already-current database is a no-op and a
    half-applied migration simply resumes.
    """

    steps: list[tuple[str, str]] = []
    attempts = _table_columns(connection, "cell_attempts")
    if attempts and "route_id" not in attempts:
        steps.append(("cell_attempts.route_id", "ALTER TABLE cell_attempts ADD COLUMN route_id TEXT"))
    desired = _table_columns(connection, "cell_desired_state")
    if desired and "pair_confirmed" not in desired:
        steps.append(
            (
                "cell_desired_state.pair_confirmed",
                "ALTER TABLE cell_desired_state ADD COLUMN pair_confirmed INTEGER NOT NULL DEFAULT 0",
            )
        )
    # Quarantine used to be keyed by local symbol; it is now keyed by the
    # derived product identity.  Renaming the column preserves every legacy row
    # and its audit trail: a legacy key can then only ever block a product, so
    # the conservative direction, and an operator can still release it.
    for table in (
        "cell_product_quarantine",
        "cell_product_quarantine_release",
        "cell_product_release_marker",
    ):
        columns = _table_columns(connection, table)
        if columns and "symbol" in columns and "product_id" not in columns:
            steps.append(
                (f"{table}.product_id", f"ALTER TABLE {table} RENAME COLUMN symbol TO product_id")
            )
    quarantine = _table_columns(connection, "cell_product_quarantine")
    if quarantine and "universe_generation" not in quarantine:
        steps.append(
            (
                "cell_product_quarantine.universe_generation",
                "ALTER TABLE cell_product_quarantine ADD COLUMN universe_generation INTEGER",
            )
        )
    return steps


def _migrate_pair_cell_schema(connection: sqlite3.Connection) -> list[str]:
    """Apply pending migrations atomically; returns the step names applied."""

    steps = _pair_cell_schema_migrations(connection)
    if not steps:
        return []
    if connection.in_transaction:
        connection.commit()
    connection.execute("BEGIN IMMEDIATE")
    try:
        for _, statement in steps:
            connection.execute(statement)
    except sqlite3.Error:
        connection.execute("ROLLBACK")
        raise
    connection.execute("COMMIT")
    return [name for name, _ in steps]


def is_derived_product_identity(value: str) -> bool:
    """Whether a quarantine key is a derived product identity.

    Derived identity is always ``<symbol>:<16 hex>``.  Anything else is a bare
    symbol recorded by a revision that keyed quarantine by local symbol, and is
    therefore symbol-wide rather than product-specific.
    """

    _, separator, digest = value.rpartition(":")
    return bool(separator) and len(digest) == 16 and all(c in "0123456789abcdef" for c in digest)


def product_identity_symbol(value: str) -> str:
    """The canonical symbol a product identity (or a legacy key) names."""

    if not is_derived_product_identity(value):
        return value
    return value.rpartition(":")[0]


def migrate_pair_cell_database(db_path: Path) -> list[str]:
    """Bring one Pair Execution Cell database to the current schema in place.

    Callers that read durable cell state without constructing a cell -- an
    unpaired Worker checking its own quarantine, for example -- must run this
    first, so a legacy column name can never surface as a query error that
    reads like "nothing is quarantined".
    """

    connection = sqlite3.connect(db_path)
    try:
        return _migrate_pair_cell_schema(connection)
    finally:
        connection.close()


def durable_quarantined_products(db_path: Path) -> tuple[str, ...]:
    """Quarantined product identities read straight from durable state.

    Safe on a legacy database and on one that has never been created: it
    migrates first and reports an absent table as an empty universe of
    *records*, never as proof that nothing is quarantined.
    """

    if not Path(db_path).exists():
        return ()
    connection = sqlite3.connect(db_path)
    try:
        _migrate_pair_cell_schema(connection)
        rows = connection.execute(
            "SELECT product_id FROM cell_product_quarantine ORDER BY product_id"
        ).fetchall()
    except sqlite3.Error as error:
        raise PairExecutionCellError(
            f"The Pair Execution Cell quarantine could not be read from {db_path}: {error}"
        ) from error
    finally:
        connection.close()
    return tuple(str(row[0]) for row in rows)


# --------------------------------------------------------------------------- #
# Route identity and authority
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class RouteAssignment:
    """The controller's authoritative route record as this Worker received it.

    Durable route identity is the ordered leader/follower Worker pair plus the
    controller-generated, globally unique ``route_id``.  There is no
    ``trader_id``, no lease, no epoch, and no TTL.  A Worker never authors this;
    it is handed the assignment its adapter read from the controller.
    """

    route_id: str
    role: Role
    leader_worker_id: str
    follower_worker_id: str
    state: RouteState = "ACTIVE"

    def __post_init__(self) -> None:
        if not self.route_id:
            raise PairExecutionCellError("A pair route requires a controller-generated route_id.")
        if self.role not in ("leader", "follower"):
            raise PairExecutionCellError("A pair role must be 'leader' or 'follower'.")
        if not self.leader_worker_id or not self.follower_worker_id:
            raise PairExecutionCellError("A pair route requires two Worker identities.")
        if self.leader_worker_id == self.follower_worker_id:
            raise PairExecutionCellError("A pair route requires two distinct Worker identities.")
        if self.state not in ("ACTIVE", "UNPAIRING"):
            raise PairExecutionCellError("A pair route state must be 'ACTIVE' or 'UNPAIRING'.")

    def worker_of(self, role: Role) -> str:
        return self.leader_worker_id if role == "leader" else self.follower_worker_id

    def peer_of(self, worker_id: str) -> str | None:
        if worker_id == self.leader_worker_id:
            return self.follower_worker_id
        if worker_id == self.follower_worker_id:
            return self.leader_worker_id
        return None

    def role_of(self, worker_id: str) -> Role | None:
        if worker_id == self.leader_worker_id:
            return "leader"
        if worker_id == self.follower_worker_id:
            return "follower"
        return None

    def canonical(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "role": self.role,
            "leader_worker_id": self.leader_worker_id,
            "follower_worker_id": self.follower_worker_id,
            "state": self.state,
        }


def _route_from_canonical(value: Mapping[str, object]) -> RouteAssignment:
    return RouteAssignment(
        route_id=cast(str, value["route_id"]),
        role=cast(Role, value["role"]),
        leader_worker_id=cast(str, value["leader_worker_id"]),
        follower_worker_id=cast(str, value["follower_worker_id"]),
        state=cast(RouteState, value.get("state", "ACTIVE")),
    )


@dataclass(frozen=True, slots=True)
class SafeUnpairAssertion:
    """One Worker's local, state-version bound safe-unpair evidence.

    The controller records two of these and checks their bindings.  It never
    derives emptiness or terminality itself, and an assertion bound to a
    superseded ``state_version`` or a different ``route_id`` does not unpair.
    """

    worker_id: str
    route_id: str
    state_version: int
    broker_verified_empty: bool
    attempts_and_effects_terminal: bool
    reason: str
    asserted_at: datetime

    @property
    def safe(self) -> bool:
        return self.broker_verified_empty and self.attempts_and_effects_terminal

    def canonical(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "route_id": self.route_id,
            "state_version": self.state_version,
            "broker_verified_empty": self.broker_verified_empty,
            "attempts_and_effects_terminal": self.attempts_and_effects_terminal,
            "reason": self.reason,
            "asserted_at": _iso(self.asserted_at),
        }


# --------------------------------------------------------------------------- #
# Per-Worker risk, the Pairing Acceptance payload, and the canonical policy
# --------------------------------------------------------------------------- #


_RISK_FIELDS = (
    "strategy_budget_usd",
    "maximum_margin_fraction",
    "maximum_loss_per_trade_usd",
    "daily_loss_fraction",
    "trade_loss_fraction",
)

DEFAULT_MODE: ExecutionMode = "live"
DEFAULT_ALLOW_LIVE = True
DEFAULT_ENTRY_EDGE_POINTS = "4"
DEFAULT_MAXIMUM_MARGIN_FRACTION = "0.10"
DEFAULT_DAILY_LOSS_FRACTION = "0.03"
DEFAULT_TRADE_LOSS_FRACTION = "0.02"
DEFAULT_MAXIMUM_LOSS_PER_TRADE_USD = "40"
DEFAULT_QUOTE_MAX_AGE_SECONDS = 1.0
DEFAULT_QUOTE_MAX_SKEW_SECONDS = 1.0
DEFAULT_FOLLOWER_CONFIRMATION_TIMEOUT_SECONDS = 5.0
DEFAULT_FLATTEN_AT_NY = "16:00"
DEFAULT_MAXIMUM_HOLDING_SECONDS: float | None = None
REQUIRED_ACCOUNT_CURRENCY = "USD"

#: Shared policy values.  Only the leader may author any of these.
SHARED_POLICY_KEYS = (
    "mode",
    "entry_edge_points",
    "quote_max_age_seconds",
    "quote_max_skew_seconds",
    "follower_confirmation_timeout_seconds",
    "flatten_at_ny",
    "maximum_holding_seconds",
)
#: Per-Worker values every Worker authors for itself, in either role.
WORKER_RISK_KEYS = (
    "maximum_margin_fraction",
    "daily_loss_fraction",
    "trade_loss_fraction",
    "maximum_loss_per_trade_usd",
)
#: Worker-local, non-policy settings.
LOCAL_SETTING_KEYS = ("allow_live",)
#: Keys that belong to pairing and discovery, never to operator configuration.
FORBIDDEN_CONFIG_KEYS = (
    "route",
    "route_id",
    "trader_id",
    "built_products",
    "eligible_products",
)


def default_shared_policy_values() -> dict[str, object]:
    """The documented leader-authored defaults, for a runtime with no file."""

    return {
        "mode": DEFAULT_MODE,
        "entry_edge_points": DEFAULT_ENTRY_EDGE_POINTS,
        "quote_max_age_seconds": DEFAULT_QUOTE_MAX_AGE_SECONDS,
        "quote_max_skew_seconds": DEFAULT_QUOTE_MAX_SKEW_SECONDS,
        "follower_confirmation_timeout_seconds": DEFAULT_FOLLOWER_CONFIRMATION_TIMEOUT_SECONDS,
        "flatten_at_ny": DEFAULT_FLATTEN_AT_NY,
        "maximum_holding_seconds": DEFAULT_MAXIMUM_HOLDING_SECONDS,
    }


def default_worker_risk_values() -> dict[str, object]:
    """The documented per-Worker risk defaults, excluding the frozen budget."""

    return {
        "maximum_margin_fraction": DEFAULT_MAXIMUM_MARGIN_FRACTION,
        "daily_loss_fraction": DEFAULT_DAILY_LOSS_FRACTION,
        "trade_loss_fraction": DEFAULT_TRADE_LOSS_FRACTION,
        "maximum_loss_per_trade_usd": DEFAULT_MAXIMUM_LOSS_PER_TRADE_USD,
    }


def validate_configuration_authority(raw: Mapping[str, object], *, role: Role | None) -> None:
    """Role-specific configuration authority; a violation is a startup error.

    ``role`` is ``None`` before this Worker is on a route, where a file holding
    only follower-permissible keys is valid for either role and a file holding
    shared policy is valid only once this Worker actually becomes the leader.
    """

    for key in FORBIDDEN_CONFIG_KEYS:
        if key in raw:
            raise PairExecutionCellError(
                f"'{key}' belongs to pairing and discovery, not to Pair Execution Cell configuration."
            )
    shared = sorted(key for key in SHARED_POLICY_KEYS if key in raw)
    if not shared:
        return
    if role == "follower":
        raise PairExecutionCellError(
            "A follower configuration file may not set shared policy: " + ", ".join(shared) + "."
        )
    if role is None:
        return


def default_worker_risk_limits(
    *,
    startup_balance_usd: object,
    account_currency: str,
    overrides: Mapping[str, object] | None = None,
) -> WorkerRiskLimits:
    """Materialize one Worker's own risk block from its startup balance.

    ``strategy_budget_usd`` is the Worker's startup broker **balance**, not its
    equity, and a non-USD account currency or a non-positive/unreadable balance
    is a fail-closed refusal rather than a substituted default.
    """

    if account_currency != REQUIRED_ACCOUNT_CURRENCY:
        raise PairExecutionCellError(
            "A Pair Execution Cell account currency must be USD; no FX conversion is performed."
        )
    balance = _positive_text(startup_balance_usd)
    if balance is None:
        raise PairExecutionCellError(
            "A Pair Execution Cell strategy budget requires a strictly positive startup balance."
        )
    values = default_worker_risk_values()
    for key, value in dict(overrides or {}).items():
        if key not in WORKER_RISK_KEYS:
            raise PairExecutionCellError(f"'{key}' is not a per-Worker risk tunable.")
        values[key] = value
    return WorkerRiskLimits(
        strategy_budget_usd=balance,
        maximum_margin_fraction=str(values["maximum_margin_fraction"]),
        maximum_loss_per_trade_usd=str(values["maximum_loss_per_trade_usd"]),
        daily_loss_fraction=str(values["daily_loss_fraction"]),
        trade_loss_fraction=str(values["trade_loss_fraction"]),
    )


@dataclass(frozen=True, slots=True)
class WorkerRiskLimits:
    """One Worker's budget and loss configuration.

    Each Worker authors its own.  The leader authors ``leader_risk`` for
    itself; the follower authors its own block and its frozen startup balance
    in the Pairing Acceptance payload and the leader copies them *verbatim*.
    A cell always enforces the accepted canonical copy for its own role.
    """

    strategy_budget_usd: str
    maximum_margin_fraction: str
    maximum_loss_per_trade_usd: str
    daily_loss_fraction: str = DEFAULT_DAILY_LOSS_FRACTION
    trade_loss_fraction: str = DEFAULT_TRADE_LOSS_FRACTION

    def __post_init__(self) -> None:
        for name in _RISK_FIELDS:
            value = _to_decimal(getattr(self, name))
            if value is None or value <= 0:
                raise PairExecutionCellError(f"{name} must be a positive number.")
        for name in ("maximum_margin_fraction", "daily_loss_fraction", "trade_loss_fraction"):
            if cast(Decimal, _to_decimal(getattr(self, name))) > 1:
                raise PairExecutionCellError(f"{name} must not exceed 1.")

    def canonical(self) -> dict[str, object]:
        return {name: getattr(self, name) for name in _RISK_FIELDS}

    def matches(self, other: WorkerRiskLimits) -> bool:
        """Numeric equality, so ``"1000"`` and ``"1000.0"`` are not a mismatch."""

        return all(
            _to_decimal(getattr(self, name)) == _to_decimal(getattr(other, name))
            for name in _RISK_FIELDS
        )

    def is_verbatim_copy_of(self, other: WorkerRiskLimits) -> bool:
        """Byte-identical canonical equality, which is what "verbatim" means."""

        return _canonical_json(self.canonical()) == _canonical_json(other.canonical())

    @property
    def budget(self) -> Decimal:
        return cast(Decimal, _to_decimal(self.strategy_budget_usd))

    @property
    def margin_budget_usd(self) -> Decimal:
        """The whole margin budget; no extra headroom beyond the fraction."""

        return self.budget * cast(Decimal, _to_decimal(self.maximum_margin_fraction))

    @property
    def daily_loss_limit_usd(self) -> Decimal:
        return self.budget * cast(Decimal, _to_decimal(self.daily_loss_fraction))

    @property
    def trade_loss_limit_usd(self) -> Decimal:
        return self.budget * cast(Decimal, _to_decimal(self.trade_loss_fraction))

    @property
    def leg_loss_cap_usd(self) -> Decimal:
        """The per-leg ceiling before this Worker's remaining daily allowance.

        ``maximum_loss_per_trade_usd`` is a hard per-trade *admission* cap here,
        a deliberate reinterpretation of the legacy emergency-stop number.
        """

        return min(
            self.trade_loss_limit_usd,
            cast(Decimal, _to_decimal(self.maximum_loss_per_trade_usd)),
        )


def _risk_from_canonical(value: Mapping[str, object]) -> WorkerRiskLimits:
    return WorkerRiskLimits(**{name: str(value[name]) for name in _RISK_FIELDS})


@dataclass(frozen=True, slots=True)
class PairingAcceptance:
    """The follower's acceptance payload: its own budget and its own risk.

    The controller forwards it unchanged and never parses it.  The leader may
    only transcribe it; it may not recompute, clamp, rescale, average,
    substitute, or default any of these values.
    """

    proposal_id: str
    worker_id: str
    startup_balance_usd: str
    account_currency: str
    maximum_margin_fraction: str
    daily_loss_fraction: str
    trade_loss_fraction: str
    maximum_loss_per_trade_usd: str

    def __post_init__(self) -> None:
        if not self.proposal_id:
            raise PairExecutionCellError("A Pairing Acceptance payload requires its proposal_id.")
        if not self.worker_id:
            raise PairExecutionCellError("A Pairing Acceptance payload requires the follower's worker_id.")
        if self.account_currency != REQUIRED_ACCOUNT_CURRENCY:
            raise PairExecutionCellError(
                "A Pairing Acceptance payload requires a USD account currency."
            )
        if _positive_text(self.startup_balance_usd) is None:
            raise PairExecutionCellError(
                "A Pairing Acceptance payload requires a strictly positive frozen startup balance."
            )
        # Constructing the risk block validates the four authored tunables.
        self.risk_limits()

    def risk_limits(self) -> WorkerRiskLimits:
        return WorkerRiskLimits(
            strategy_budget_usd=self.startup_balance_usd,
            maximum_margin_fraction=self.maximum_margin_fraction,
            maximum_loss_per_trade_usd=self.maximum_loss_per_trade_usd,
            daily_loss_fraction=self.daily_loss_fraction,
            trade_loss_fraction=self.trade_loss_fraction,
        )

    def canonical(self) -> dict[str, object]:
        return {
            "proposal_id": self.proposal_id,
            "worker_id": self.worker_id,
            "startup_balance_usd": self.startup_balance_usd,
            "account_currency": self.account_currency,
            "maximum_margin_fraction": self.maximum_margin_fraction,
            "daily_loss_fraction": self.daily_loss_fraction,
            "trade_loss_fraction": self.trade_loss_fraction,
            "maximum_loss_per_trade_usd": self.maximum_loss_per_trade_usd,
        }


def _acceptance_from_canonical(value: Mapping[str, object]) -> PairingAcceptance:
    return PairingAcceptance(
        proposal_id=cast(str, value["proposal_id"]),
        worker_id=cast(str, value["worker_id"]),
        startup_balance_usd=str(value["startup_balance_usd"]),
        account_currency=cast(str, value["account_currency"]),
        maximum_margin_fraction=str(value["maximum_margin_fraction"]),
        daily_loss_fraction=str(value["daily_loss_fraction"]),
        trade_loss_fraction=str(value["trade_loss_fraction"]),
        maximum_loss_per_trade_usd=str(value["maximum_loss_per_trade_usd"]),
    )


def build_pairing_acceptance(
    *,
    proposal_id: str,
    worker_id: str,
    startup_balance_usd: object,
    account_currency: str,
    overrides: Mapping[str, object] | None = None,
) -> PairingAcceptance:
    """Freeze a follower's own budget and risk tunables for one pairing."""

    limits = default_worker_risk_limits(
        startup_balance_usd=startup_balance_usd,
        account_currency=account_currency,
        overrides=overrides,
    )
    return PairingAcceptance(
        proposal_id=proposal_id,
        worker_id=worker_id,
        startup_balance_usd=limits.strategy_budget_usd,
        account_currency=account_currency,
        maximum_margin_fraction=limits.maximum_margin_fraction,
        daily_loss_fraction=limits.daily_loss_fraction,
        trade_loss_fraction=limits.trade_loss_fraction,
        maximum_loss_per_trade_usd=limits.maximum_loss_per_trade_usd,
    )


@dataclass(frozen=True, slots=True)
class StrategyPolicy:
    """The leader-published canonical, versioned strategy policy.

    Shared decision rules are authored by the leader alone.  ``leader_risk`` is
    the leader's own block; ``follower_risk`` is a verbatim copy of the
    follower's Pairing Acceptance payload.  There is no product filter here:
    the eligible universe comes from Initial Compatible Product Discovery.
    """

    policy_version: str
    entry_edge_points: str
    quote_max_age_seconds: float
    quote_max_skew_seconds: float
    leader_risk: WorkerRiskLimits
    follower_risk: WorkerRiskLimits
    follower_confirmation_timeout_seconds: float = DEFAULT_FOLLOWER_CONFIRMATION_TIMEOUT_SECONDS
    maximum_holding_seconds: float | None = None
    flatten_at_ny: str | None = None
    mode: ExecutionMode = DEFAULT_MODE

    def __post_init__(self) -> None:
        if not self.policy_version:
            raise PairExecutionCellError("A strategy policy requires a version.")
        if _to_decimal(self.entry_edge_points) is None:
            raise PairExecutionCellError("entry_edge_points must be a finite number.")
        for name in (
            "quote_max_age_seconds",
            "quote_max_skew_seconds",
            "follower_confirmation_timeout_seconds",
        ):
            value = getattr(self, name)
            if (
                not isinstance(value, (int, float))
                or isinstance(value, bool)
                or not math.isfinite(value)
                or value <= 0
            ):
                raise PairExecutionCellError(f"{name} must be a positive number.")
        for name in ("leader_risk", "follower_risk"):
            if not isinstance(getattr(self, name), WorkerRiskLimits):
                raise PairExecutionCellError(f"{name} must be a WorkerRiskLimits.")
        if self.maximum_holding_seconds is not None and (
            not math.isfinite(self.maximum_holding_seconds)
            or not 0 < self.maximum_holding_seconds <= _MAXIMUM_HOLDING_SECONDS
        ):
            raise PairExecutionCellError("maximum_holding_seconds is out of range.")
        if self.flatten_at_ny is not None:
            _parse_ny_time(self.flatten_at_ny)
        if self.mode not in ("live", "shadow"):
            raise PairExecutionCellError("mode must be 'live' or 'shadow'.")

    def risk_of(self, role: Role) -> WorkerRiskLimits:
        return self.leader_risk if role == "leader" else self.follower_risk

    @property
    def plan_retention_seconds(self) -> float:
        """How long a superseded plan version stays valid for validation only."""

        return self.follower_confirmation_timeout_seconds + RELAY_HANDLING_WINDOW_SECONDS

    def canonical(self) -> dict[str, object]:
        return {
            "policy_version": self.policy_version,
            "entry_edge_points": self.entry_edge_points,
            "quote_max_age_seconds": self.quote_max_age_seconds,
            "quote_max_skew_seconds": self.quote_max_skew_seconds,
            "follower_confirmation_timeout_seconds": self.follower_confirmation_timeout_seconds,
            "maximum_holding_seconds": self.maximum_holding_seconds,
            "flatten_at_ny": self.flatten_at_ny,
            "mode": self.mode,
            "leader_risk": self.leader_risk.canonical(),
            "follower_risk": self.follower_risk.canonical(),
        }

    @property
    def hash(self) -> str:
        return _hash(_canonical_json(self.canonical()))


def _policy_from_canonical(value: Mapping[str, object]) -> StrategyPolicy:
    return StrategyPolicy(
        policy_version=cast(str, value["policy_version"]),
        entry_edge_points=cast(str, value["entry_edge_points"]),
        quote_max_age_seconds=float(cast(float, value["quote_max_age_seconds"])),
        quote_max_skew_seconds=float(cast(float, value["quote_max_skew_seconds"])),
        leader_risk=_risk_from_canonical(cast(Mapping[str, object], value["leader_risk"])),
        follower_risk=_risk_from_canonical(cast(Mapping[str, object], value["follower_risk"])),
        follower_confirmation_timeout_seconds=float(
            cast(float, value["follower_confirmation_timeout_seconds"])
        ),
        maximum_holding_seconds=cast(float | None, value.get("maximum_holding_seconds")),
        flatten_at_ny=cast(str | None, value.get("flatten_at_ny")),
        mode=cast(ExecutionMode, value.get("mode", DEFAULT_MODE)),
    )


def canonical_policy_from_acceptance(
    *,
    policy_version: str,
    leader_risk: WorkerRiskLimits,
    acceptance: PairingAcceptance,
    shared: Mapping[str, object] | None = None,
) -> StrategyPolicy:
    """Compose the one canonical policy: leader-authored shared plus two blocks.

    ``follower_risk`` is taken verbatim from ``acceptance``; there is no path
    through this function that lets the leader author the follower's numbers.
    """

    values = default_shared_policy_values()
    for key, value in dict(shared or {}).items():
        if key not in SHARED_POLICY_KEYS:
            raise PairExecutionCellError(f"'{key}' is not a shared, leader-authored policy value.")
        values[key] = value
    return StrategyPolicy(
        policy_version=policy_version,
        entry_edge_points=str(values["entry_edge_points"]),
        quote_max_age_seconds=float(cast(float, values["quote_max_age_seconds"])),
        quote_max_skew_seconds=float(cast(float, values["quote_max_skew_seconds"])),
        leader_risk=leader_risk,
        follower_risk=acceptance.risk_limits(),
        follower_confirmation_timeout_seconds=float(
            cast(float, values["follower_confirmation_timeout_seconds"])
        ),
        maximum_holding_seconds=cast(float | None, values["maximum_holding_seconds"]),
        flatten_at_ny=cast(str | None, values["flatten_at_ny"]),
        mode=cast(ExecutionMode, values["mode"]),
    )


@dataclass(frozen=True, slots=True)
class RemainingAllowanceSummary:
    """One Worker's versioned remaining-allowed-leg-loss publication.

    It is an optimization for the leader, never an authority over the follower:
    the follower still re-checks an assigned loss against its own *current*
    local allowance when the attempt arrives.

    ``publisher_epoch`` is the publishing Worker's durable, per-route recovery
    session number.  ``version`` alone is only monotonic *within* one publisher
    session, so a rebuilt Worker restarts it; ordering therefore compares
    ``(publisher_epoch, version)`` and a newer session always wins.  Without
    that, a surviving peer would reject every summary from a rebuilt publisher
    and keep enforcing a stale -- possibly larger -- allowance.
    """

    worker_id: str
    publisher_epoch: int
    version: int
    allowed_leg_loss_usd: str

    @property
    def ordering(self) -> tuple[int, int]:
        return self.publisher_epoch, self.version

    def canonical(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "publisher_epoch": self.publisher_epoch,
            "version": self.version,
            "allowed_leg_loss_usd": self.allowed_leg_loss_usd,
        }


# --------------------------------------------------------------------------- #
# Initial Compatible Product Discovery
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class CatalogEntry:
    """One raw broker catalog symbol as this Worker's own MT5 reports it.

    Base/profit currency, ``digits``, ``point``, and ``trade_tick_size`` are
    carried for evidence and local economics but are deliberately *not*
    required to be equal across the two brokers.
    """

    name: str
    trade_mode: object
    trade_calc_mode: object
    digits: int
    point: str
    trade_tick_size: str
    contract_size: str
    volume_min: str
    volume_step: str
    volume_max: str
    allowed_directions: tuple[Direction, ...]
    filling_modes: tuple[FillingMode, ...]
    base_currency: str = ""
    profit_currency: str = ""

    def canonical(self) -> dict[str, object]:
        return {
            "name": self.name,
            "trade_mode": self.trade_mode,
            "trade_calc_mode": self.trade_calc_mode,
            "digits": self.digits,
            "point": self.point,
            "trade_tick_size": self.trade_tick_size,
            "contract_size": self.contract_size,
            "volume_min": self.volume_min,
            "volume_step": self.volume_step,
            "volume_max": self.volume_max,
            "allowed_directions": sorted(self.allowed_directions),
            "filling_modes": sorted(self.filling_modes),
            "base_currency": self.base_currency,
            "profit_currency": self.profit_currency,
        }


def _catalog_entry_from_canonical(value: Mapping[str, object]) -> CatalogEntry:
    return CatalogEntry(
        name=cast(str, value["name"]),
        trade_mode=value["trade_mode"],
        trade_calc_mode=value["trade_calc_mode"],
        digits=cast(int, value["digits"]),
        point=str(value["point"]),
        trade_tick_size=str(value["trade_tick_size"]),
        contract_size=str(value["contract_size"]),
        volume_min=str(value["volume_min"]),
        volume_step=str(value["volume_step"]),
        volume_max=str(value["volume_max"]),
        allowed_directions=tuple(cast(list[Direction], value["allowed_directions"])),
        filling_modes=tuple(cast(list[FillingMode], value["filling_modes"])),
        base_currency=cast(str, value.get("base_currency", "")),
        profit_currency=cast(str, value.get("profit_currency", "")),
    )


@dataclass(frozen=True, slots=True)
class CatalogSummary:
    """A versioned catalog/specification summary exchanged over the relay.

    The controller forwards it unchanged and never parses it.
    ``publisher_epoch`` carries the same recovery-session semantics as the
    remaining-allowance summary: ``version`` restarts when the publishing
    Worker is rebuilt, so ordering compares ``(publisher_epoch, version)``.
    """

    worker_id: str
    publisher_epoch: int
    version: int
    entries: tuple[CatalogEntry, ...]

    @property
    def ordering(self) -> tuple[int, int]:
        return self.publisher_epoch, self.version

    def canonical(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "publisher_epoch": self.publisher_epoch,
            "version": self.version,
            "entries": [entry.canonical() for entry in sorted(self.entries, key=lambda e: e.name)],
        }

    @property
    def hash(self) -> str:
        return _hash(_canonical_json(self.canonical()))


def _catalog_summary_from_canonical(value: Mapping[str, object]) -> CatalogSummary:
    entries = value["entries"]
    if not isinstance(entries, list):
        raise PairExecutionCellError("A catalog summary requires a list of entries.")
    epoch = value["publisher_epoch"]
    version = value["version"]
    if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch <= 0:
        raise PairExecutionCellError("A catalog summary requires a positive publisher_epoch.")
    if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
        raise PairExecutionCellError("A catalog summary requires a positive version.")
    return CatalogSummary(
        worker_id=cast(str, value["worker_id"]),
        publisher_epoch=epoch,
        version=version,
        entries=tuple(
            _catalog_entry_from_canonical(cast(Mapping[str, object], entry)) for entry in entries
        ),
    )


@dataclass(frozen=True, slots=True)
class DiscoveredProduct:
    """One admitted shared symbol and its canonical compatibility summary.

    Product identity is derived deterministically from the exact symbol name
    plus this summary's hash, so the same two catalogs always produce the same
    identity -- which is why a quarantine survives an explicit rediscovery.
    """

    symbol: str
    trade_calc_mode: object
    contract_size: str
    volume_min: str
    volume_step: str
    volume_max: str
    canonical_point: str
    filling_mode: FillingMode
    directions: tuple[Direction, ...] = ("LONG", "SHORT")

    def compatibility_summary(self) -> dict[str, object]:
        return {
            "symbol": self.symbol,
            "trade_calc_mode": self.trade_calc_mode,
            "contract_size": self.contract_size,
            "volume_min": self.volume_min,
            "volume_step": self.volume_step,
            "volume_max": self.volume_max,
            "canonical_point": self.canonical_point,
            "filling_mode": self.filling_mode,
            "directions": sorted(self.directions),
        }

    @property
    def compatibility_hash(self) -> str:
        return _hash(_canonical_json(self.compatibility_summary()))

    @property
    def product_id(self) -> str:
        """The derived product identity that keys plans, attempts, quarantine."""

        return f"{self.symbol}:{self.compatibility_hash[:16]}"

    def canonical(self) -> dict[str, object]:
        summary = self.compatibility_summary()
        summary["product_id"] = self.product_id
        return summary


def _product_from_canonical(value: Mapping[str, object]) -> DiscoveredProduct:
    product = DiscoveredProduct(
        symbol=cast(str, value["symbol"]),
        trade_calc_mode=value["trade_calc_mode"],
        contract_size=str(value["contract_size"]),
        volume_min=str(value["volume_min"]),
        volume_step=str(value["volume_step"]),
        volume_max=str(value["volume_max"]),
        canonical_point=str(value["canonical_point"]),
        filling_mode=cast(FillingMode, value["filling_mode"]),
        directions=tuple(cast(list[Direction], value["directions"])),
    )
    declared = value.get("product_id")
    if isinstance(declared, str) and declared and declared != product.product_id:
        raise PairExecutionCellError("A discovered product's identity does not match its summary.")
    return product


@dataclass(frozen=True, slots=True)
class DiscoveredUniverse:
    """One frozen discovery result, named by its ``universe_generation``.

    The generation names a discovery result; it never names a pairing, never
    replaces ``route_id``, and never expires.  The hourly refresh revalidates
    and may suspend, but never silently admits a newly appeared symbol and
    never changes the generation.
    """

    route_id: str
    universe_generation: int
    products: tuple[DiscoveredProduct, ...]
    leader_catalog_hash: str
    follower_catalog_hash: str

    def canonical(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "universe_generation": self.universe_generation,
            "products": [product.canonical() for product in self.products],
            "leader_catalog_hash": self.leader_catalog_hash,
            "follower_catalog_hash": self.follower_catalog_hash,
        }

    @property
    def hash(self) -> str:
        """Identifies exactly this discovery result, content and generation."""

        return _hash(_canonical_json(self.canonical()))

    def product(self, product_id: str) -> DiscoveredProduct | None:
        return next((p for p in self.products if p.product_id == product_id), None)


def _universe_from_canonical(value: Mapping[str, object]) -> DiscoveredUniverse:
    products = value["products"]
    if not isinstance(products, list):
        raise PairExecutionCellError("A discovered universe requires a list of products.")
    return DiscoveredUniverse(
        route_id=cast(str, value["route_id"]),
        universe_generation=cast(int, value["universe_generation"]),
        products=tuple(
            _product_from_canonical(cast(Mapping[str, object], product)) for product in products
        ),
        leader_catalog_hash=cast(str, value["leader_catalog_hash"]),
        follower_catalog_hash=cast(str, value["follower_catalog_hash"]),
    )


@dataclass(frozen=True, slots=True)
class DiscoveryOutcome:
    """The deterministic result of one compatibility run over two catalogs."""

    products: tuple[DiscoveredProduct, ...]
    reason: str = ""
    excluded: tuple[tuple[str, str], ...] = ()

    @property
    def admitted(self) -> bool:
        return not self.reason


_COMPATIBILITY_EQUALITY_FIELDS = (
    "trade_calc_mode",
    "contract_size",
    "volume_min",
    "volume_step",
    "allowed_directions",
)


def _validated_catalog(summary: CatalogSummary) -> tuple[dict[str, CatalogEntry], str]:
    """Apply the ``trade_mode == 4`` precondition to one whole catalog.

    A missing, non-integer, or boolean ``trade_mode`` makes the *catalog*
    invalid rather than merely making that one symbol ineligible.
    """

    filtered: dict[str, CatalogEntry] = {}
    for entry in summary.entries:
        if not isinstance(entry.name, str) or not entry.name:
            return {}, f"{summary.worker_id} returned a catalog entry without a symbol name"
        trade_mode = entry.trade_mode
        if isinstance(trade_mode, bool) or not isinstance(trade_mode, int):
            return {}, (
                f"{summary.worker_id} returned an invalid catalog: {entry.name} has no integer trade_mode"
            )
        if entry.name in filtered:
            return {}, f"{summary.worker_id} returned a duplicated catalog symbol {entry.name}"
        if trade_mode != FULL_TRADING_MODE:
            continue
        filtered[entry.name] = entry
    return filtered, ""


def discover_compatible_products(
    local: CatalogSummary, peer: CatalogSummary
) -> DiscoveryOutcome:
    """Run the legacy ``realtime_arbitrage`` compatibility rules, fail closed.

    ``trade_mode == 4`` is applied to each catalog *before* the exact
    symbol-name intersection.  Admission then requires equality of
    ``trade_calc_mode``, ``contract_size``, ``volume_min``, ``volume_step``, and
    ``allowed_directions``, both ``LONG`` and ``SHORT``, and a shared ``FOK``
    (preferred) or ``IOC`` filling mode.  Currency, ``digits``, ``point``, and
    tick size are explicitly allowed to differ.
    """

    first, first_reason = _validated_catalog(local)
    if first_reason:
        return DiscoveryOutcome((), first_reason)
    second, second_reason = _validated_catalog(peer)
    if second_reason:
        return DiscoveryOutcome((), second_reason)
    admitted: list[DiscoveredProduct] = []
    excluded: list[tuple[str, str]] = []
    for symbol in sorted(set(first) & set(second)):
        one, two = first[symbol], second[symbol]
        mismatches = _hard_mismatches(one, two)
        if mismatches:
            excluded.append((symbol, "hard specification mismatch: " + ",".join(mismatches)))
            continue
        if not {"LONG", "SHORT"}.issubset(set(one.allowed_directions)):
            excluded.append((symbol, "bidirectional trading is unavailable"))
            continue
        shared_filling = _preferred_shared_filling_mode(one.filling_modes, two.filling_modes)
        if shared_filling is None:
            excluded.append((symbol, "no shared FOK or IOC filling mode"))
            continue
        canonical_point = _max_decimal_text(one.point, two.point)
        common_volume_max = _min_decimal_text(one.volume_max, two.volume_max)
        contract_size = decimal_text(one.contract_size)
        volume_min = decimal_text(one.volume_min)
        volume_step = decimal_text(one.volume_step)
        if (
            canonical_point is None
            or common_volume_max is None
            or contract_size is None
            or volume_min is None
            or volume_step is None
        ):
            excluded.append((symbol, "catalog economics are not usable numbers"))
            continue
        admitted.append(
            DiscoveredProduct(
                symbol=symbol,
                trade_calc_mode=one.trade_calc_mode,
                contract_size=contract_size,
                volume_min=volume_min,
                volume_step=volume_step,
                volume_max=common_volume_max,
                canonical_point=canonical_point,
                filling_mode=shared_filling,
                directions=("LONG", "SHORT"),
            )
        )
    if not admitted:
        return DiscoveryOutcome(
            (), "no compatible symbol survived discovery", tuple(excluded)
        )
    return DiscoveryOutcome(tuple(admitted), "", tuple(excluded))


def _hard_mismatches(first: CatalogEntry, second: CatalogEntry) -> list[str]:
    mismatches: list[str] = []
    for name in _COMPATIBILITY_EQUALITY_FIELDS:
        one, two = getattr(first, name), getattr(second, name)
        if name == "allowed_directions":
            if set(cast(tuple[str, ...], one)) != set(cast(tuple[str, ...], two)):
                mismatches.append(name)
            continue
        if name == "trade_calc_mode":
            if one != two:
                mismatches.append(name)
            continue
        one_text, two_text = decimal_text(one), decimal_text(two)
        if one_text is None or two_text is None or one_text != two_text:
            mismatches.append(name)
    return mismatches


def _preferred_shared_filling_mode(
    first: Iterable[str], second: Iterable[str]
) -> FillingMode | None:
    shared = set(first) & set(second)
    if "FOK" in shared:
        return "FOK"
    if "IOC" in shared:
        return "IOC"
    return None


def _max_decimal_text(first: object, second: object) -> str | None:
    one, two = _to_decimal(first), _to_decimal(second)
    if one is None or two is None or one <= 0 or two <= 0:
        return None
    return decimal_text(max(one, two))


def _min_decimal_text(first: object, second: object) -> str | None:
    one, two = _to_decimal(first), _to_decimal(second)
    if one is None or two is None or one <= 0 or two <= 0:
        return None
    return decimal_text(min(one, two))


# --------------------------------------------------------------------------- #
# Per-product sizing and emergency-protection plans
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class SizingPlan:
    """One precomputed per-product, per-direction plan; never a signal-time read.

    Built from a current local quote and a current local MT5 margin
    calculation after discovery, then refreshed hourly.  Each version is
    immutable and named, and an attempt names the exact version each leg was
    sized from.
    """

    universe_generation: int
    product_id: str
    symbol: str
    direction: Direction
    margin_per_lot: str
    local_max_lots: str
    canonical_point: str
    point: str
    tick_size: str
    profit_tick_value: str
    loss_tick_value: str
    volume_min: str
    volume_step: str
    volume_max: str
    filling_mode: FillingMode
    minimum_stop_distance: str
    usd_per_point_per_lot: str

    def canonical(self) -> dict[str, object]:
        return {
            "universe_generation": self.universe_generation,
            "product_id": self.product_id,
            "symbol": self.symbol,
            "direction": self.direction,
            "margin_per_lot": self.margin_per_lot,
            "local_max_lots": self.local_max_lots,
            "canonical_point": self.canonical_point,
            "point": self.point,
            "tick_size": self.tick_size,
            "profit_tick_value": self.profit_tick_value,
            "loss_tick_value": self.loss_tick_value,
            "volume_min": self.volume_min,
            "volume_step": self.volume_step,
            "volume_max": self.volume_max,
            "filling_mode": self.filling_mode,
            "minimum_stop_distance": self.minimum_stop_distance,
            "usd_per_point_per_lot": self.usd_per_point_per_lot,
        }

    @property
    def version(self) -> str:
        return _hash(_canonical_json(self.canonical()))[:16]


def _plan_from_canonical(value: Mapping[str, object]) -> SizingPlan:
    return SizingPlan(
        universe_generation=cast(int, value["universe_generation"]),
        product_id=cast(str, value["product_id"]),
        symbol=cast(str, value["symbol"]),
        direction=cast(Direction, value["direction"]),
        margin_per_lot=cast(str, value["margin_per_lot"]),
        local_max_lots=cast(str, value["local_max_lots"]),
        canonical_point=cast(str, value["canonical_point"]),
        point=cast(str, value["point"]),
        tick_size=cast(str, value["tick_size"]),
        profit_tick_value=cast(str, value["profit_tick_value"]),
        loss_tick_value=cast(str, value["loss_tick_value"]),
        volume_min=cast(str, value["volume_min"]),
        volume_step=cast(str, value["volume_step"]),
        volume_max=cast(str, value["volume_max"]),
        filling_mode=cast(FillingMode, value["filling_mode"]),
        minimum_stop_distance=cast(str, value["minimum_stop_distance"]),
        usd_per_point_per_lot=cast(str, value["usd_per_point_per_lot"]),
    )


@dataclass(frozen=True, slots=True)
class LocalProductFacts:
    """Account-local MT5 facts for one discovered symbol; never product identity."""

    point: str
    tick_size: str
    profit_tick_value: str
    loss_tick_value: str
    stop_level_points: str
    freeze_level_points: str
    volume_max: str
    tradable: bool = True


def _round_to_tick(price: Decimal, tick: Decimal, rounding: str) -> Decimal:
    return (price / tick).to_integral_value(rounding=rounding) * tick


def _same_protection_price(
    expected: object, observed: object, tick_size: object
) -> bool:
    """Whether broker evidence preserves one requested executable protection price."""

    expected_price = _to_decimal(expected)
    observed_price = _to_decimal(observed)
    tick = _to_decimal(tick_size)
    if (
        expected_price is None
        or observed_price is None
        or tick is None
        or tick <= 0
        or expected_price != _round_to_tick(expected_price, tick, ROUND_HALF_EVEN)
    ):
        return False
    if observed_price == expected_price:
        return True
    # MT5 returns binary floats. Accept only their tiny decimal rendering error,
    # never a broker-side movement to another executable price.
    serialization_error = min(
        tick / Decimal("1000000"),
        max(Decimal("1e-12"), max(abs(expected_price), abs(observed_price)) * Decimal("1e-12")),
    )
    return (
        abs(observed_price - expected_price) <= serialization_error
        and _round_to_tick(observed_price, tick, ROUND_HALF_EVEN) == expected_price
    )


def edge_value(
    leader_quote: QuoteSnapshot, follower_quote: QuoteSnapshot, leader_direction: Direction
) -> Decimal:
    """The favorable price differential of executing both mirror legs now."""

    if leader_direction == "LONG":
        return follower_quote.bid - leader_quote.ask
    return leader_quote.bid - follower_quote.ask


def compute_protection(
    *,
    entry: Decimal,
    direction: Direction,
    volume: Decimal,
    plan: SizingPlan,
    allowed_loss_usd: Decimal,
) -> tuple[str, str] | None:
    """1:1 SL/TP in equal USD amounts from current constraints, or ``None``.

    Used for both rough protection (cached plan plus the decision quote) and
    precise protection (fresh plan plus the broker-observed fill price).  Both
    target the current computed ``allowed_leg_loss_usd`` for that leg, never a
    fixed USD constant.
    """

    tick_size = _to_decimal(plan.tick_size)
    minimum_stop_distance = _to_decimal(plan.minimum_stop_distance)
    profit_tick_value = _to_decimal(plan.profit_tick_value)
    loss_tick_value = _to_decimal(plan.loss_tick_value)
    if (
        tick_size is None or tick_size <= 0
        or minimum_stop_distance is None or minimum_stop_distance < 0
        or profit_tick_value is None or profit_tick_value <= 0
        or loss_tick_value is None or loss_tick_value <= 0
        or volume <= 0 or allowed_loss_usd <= 0
    ):
        return None
    loss_ticks = int((allowed_loss_usd / (loss_tick_value * volume)).to_integral_value(rounding=ROUND_FLOOR))
    profit_ticks = int((allowed_loss_usd / (profit_tick_value * volume)).to_integral_value(rounding=ROUND_FLOOR))
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


# --------------------------------------------------------------------------- #
# Quotes and typed events
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class BrokerClockCalibration:
    """One broker clock's calibrated offset from UTC and its uncertainty bound.

    Mirrors the Worker's existing MT5 time-calibration prior art
    (:mod:`abt.mt5.timecalibration`): ``offset_seconds`` is the broker clock
    minus UTC and ``error_seconds`` is the residual sampling uncertainty.  The
    cell charges that uncertainty conservatively, so an uncalibrated or stale
    clock can never make a quote look fresher or better aligned than it is.
    """

    offset_seconds: float = 0.0
    error_seconds: float = 0.0
    status: Literal["calibrated", "stale", "uncalibrated"] = "calibrated"

    def __post_init__(self) -> None:
        for name in ("offset_seconds", "error_seconds"):
            value = getattr(self, name)
            if not isinstance(value, (int, float)) or isinstance(value, bool) or not math.isfinite(value):
                raise PairExecutionCellError(f"{name} must be a finite number.")
        if self.error_seconds < 0:
            raise PairExecutionCellError("error_seconds must not be negative.")

    @property
    def usable(self) -> bool:
        return self.status == "calibrated"

    def canonical(self) -> dict[str, object]:
        return {
            "offset_seconds": self.offset_seconds,
            "error_seconds": self.error_seconds,
            "status": self.status,
        }


def _calibration_from_canonical(value: object) -> BrokerClockCalibration:
    if not isinstance(value, Mapping):
        return BrokerClockCalibration(status="uncalibrated")
    try:
        return BrokerClockCalibration(
            offset_seconds=float(cast(float, value.get("offset_seconds", 0.0))),
            error_seconds=float(cast(float, value.get("error_seconds", 0.0))),
            status=cast(
                Literal["calibrated", "stale", "uncalibrated"], value.get("status", "uncalibrated")
            ),
        )
    except (TypeError, ValueError, PairExecutionCellError):
        return BrokerClockCalibration(status="uncalibrated")


@dataclass(frozen=True, slots=True)
class QuoteSnapshot:
    """Versioned executable bid/ask evidence for one discovered product.

    ``recovery_epoch`` is market evidence about this Worker's own broker
    session continuity.  It is never derived from or compared against
    ``route_id`` or ``universe_generation``.
    """

    worker_id: str
    product_id: str
    symbol: str
    bid: Decimal
    ask: Decimal
    broker_time: datetime
    local_receive_time: datetime
    recovery_epoch: str
    sequence: int
    calibration: BrokerClockCalibration = BrokerClockCalibration()

    @property
    def calibrated_broker_time(self) -> datetime:
        """The broker tick time expressed on the calibrated UTC timeline."""

        return self.broker_time - timedelta(seconds=self.calibration.offset_seconds)

    def age_seconds(self, now: datetime) -> float:
        """Worst-case age of the *market evidence*, never of its transport."""

        return (now - self.calibrated_broker_time).total_seconds() + self.calibration.error_seconds

    def fresh(self, now: datetime, max_age_seconds: float) -> bool:
        """Calibrated-broker-time freshness, so a frozen tick ages out.

        Local receive time is deliberately not used: a repeated or resequenced
        identical broker tick keeps arriving but its market evidence does not
        get any newer.
        """

        if not self.calibration.usable:
            return False
        age = self.age_seconds(now)
        return 0 <= age <= max_age_seconds

    def canonical(self) -> dict[str, object]:
        return {
            "worker_id": self.worker_id,
            "product_id": self.product_id,
            "symbol": self.symbol,
            "bid": str(self.bid),
            "ask": str(self.ask),
            "broker_time": self.broker_time.isoformat(),
            "recovery_epoch": self.recovery_epoch,
            "sequence": self.sequence,
            "calibration": self.calibration.canonical(),
        }


def calibrated_skew_seconds(leader: QuoteSnapshot, follower: QuoteSnapshot) -> float:
    """Worst-case distance between two calibrated broker clocks."""

    skew = abs((leader.calibrated_broker_time - follower.calibrated_broker_time).total_seconds())
    return skew + leader.calibration.error_seconds + follower.calibration.error_seconds


@dataclass(frozen=True, slots=True)
class LocalQuoteEvent:
    product_id: str
    symbol: str
    bid: Decimal
    ask: Decimal
    broker_time: datetime
    local_receive_time: datetime
    recovery_epoch: str
    sequence: int
    calibration: BrokerClockCalibration = BrokerClockCalibration()


@dataclass(frozen=True, slots=True)
class LocalQuoteBatchEvent:
    quotes: tuple[LocalQuoteEvent, ...]


@dataclass(frozen=True, slots=True)
class LocalCatalogEvent:
    """This Worker's own broker catalog snapshot, for discovery only.

    It never changes the frozen universe by itself: a symbol that appears here
    after discovery is admitted only by an explicit rediscovery.
    """

    entries: tuple[CatalogEntry, ...]
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class RouteMetadataEvent:
    """The controller's authoritative route record, observed on (re)connect.

    A disagreement with this Worker's durable copy -- different ``route_id``,
    different assigned role, or a route the controller no longer holds --
    removes entry readiness and raises a metadata reconciliation.  It never
    discards, closes, hides, or infers local broker exposure.
    """

    route_id: str | None
    role: Role | None
    leader_worker_id: str | None
    follower_worker_id: str | None
    state: RouteState | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class RediscoveryRequestEvent:
    """An explicit operator rediscovery request, available on either role."""

    actor: str
    reason: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ReadinessFactsEvent:
    """Continuously maintained local account facts; never a signal-time read."""

    account_login: str
    account_server: str
    terminal_connected: bool
    orders: list[dict[str, object]] | None
    positions: list[dict[str, object]] | None
    margin_headroom_ok: bool
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class BrokerSnapshotEvent:
    orders: list[dict[str, object]] | None
    positions: list[dict[str, object]] | None
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class ClockTickEvent:
    now: datetime


@dataclass(frozen=True, slots=True)
class PeerSessionEvent:
    """Authenticated peer route session state; loss removes entry readiness."""

    connected: bool
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class RealizedPnLEvent:
    """Realized P&L of one closed strategy-owned leg; floating P&L is excluded."""

    attempt_id: str
    realized_usd: str
    closed_at: datetime
    event_id: str | None = None


@dataclass(frozen=True, slots=True)
class QuarantineReleaseEvent:
    product_id: str
    actor: str
    reason: str
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class RelayEnvelopeReceived:
    """An authenticated, already identity/route-validated opaque envelope."""

    envelope: Mapping[str, object]


PairCellEvent = (
    LocalQuoteEvent
    | LocalQuoteBatchEvent
    | LocalCatalogEvent
    | RouteMetadataEvent
    | RediscoveryRequestEvent
    | ReadinessFactsEvent
    | BrokerSnapshotEvent
    | ClockTickEvent
    | PeerSessionEvent
    | RealizedPnLEvent
    | QuarantineReleaseEvent
    | RelayEnvelopeReceived
)


# --------------------------------------------------------------------------- #
# Adapter seams
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

    def order_calc_margin(self, request: Mapping[str, object]) -> object: ...

    def order_send(self, request: dict[str, object]) -> object: ...


@dataclass(frozen=True, slots=True)
class PairResult:
    """The current observable pair result; no internal names are exposed."""

    worker_id: str
    role: Role
    route_id: str
    route_state: RouteState
    universe_generation: int | None
    state_version: int
    state: PairState
    ready: bool
    ready_reason: str
    attempt_id: str | None
    desired_state: DesiredState
    policy_hash: str | None
    policy_accepted: bool
    plan_set_version: str
    pair_confirmed: bool
    needs_human: bool
    needs_human_reason: str | None
    metadata_reconciliation: str | None
    discovery_reason: str
    rediscovery_failure: str | None
    recovering: bool
    timing: dict[str, object] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class BrokerWriteResult:
    category: Literal["completed", "rejected", "expired_not_started", "unknown_after_send"]
    receipt: dict[str, object] | None = None


# --------------------------------------------------------------------------- #
# Immutable attempt
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class Attempt:
    """One immutable leader-originated pair entry attempt.

    ``decision_time`` is audit and recovery evidence only.  It is never a
    cross-machine admission deadline: each Worker runs its own local monotonic
    timer instead.  ``route_id`` fences the attempt to one pairing and
    ``universe_generation`` fences it to one discovery result.
    """

    attempt_id: str
    policy_hash: str
    route_id: str
    universe_generation: int
    product_id: str
    symbol: str
    leader_direction: Direction
    follower_direction: Direction
    lots: str
    leader_quote: QuoteSnapshot
    follower_quote: QuoteSnapshot
    leader_plan_version: str
    follower_plan_version: str
    leader_allowance_epoch: int
    follower_allowance_epoch: int
    leader_allowance_version: int
    follower_allowance_version: int
    leader_allowed_loss_usd: str
    follower_allowed_loss_usd: str
    leader_rough_sl: str
    leader_rough_tp: str
    follower_rough_sl: str
    follower_rough_tp: str
    filling_mode: FillingMode
    decision_time: datetime
    confirmation_timeout_seconds: float

    def canonical(self) -> dict[str, object]:
        return {
            "attempt_id": self.attempt_id,
            "policy_hash": self.policy_hash,
            "route_id": self.route_id,
            "universe_generation": self.universe_generation,
            "product_id": self.product_id,
            "symbol": self.symbol,
            "leader_direction": self.leader_direction,
            "follower_direction": self.follower_direction,
            "lots": self.lots,
            "leader_quote": self.leader_quote.canonical(),
            "follower_quote": self.follower_quote.canonical(),
            "leader_plan_version": self.leader_plan_version,
            "follower_plan_version": self.follower_plan_version,
            "leader_allowance_epoch": self.leader_allowance_epoch,
            "follower_allowance_epoch": self.follower_allowance_epoch,
            "leader_allowance_version": self.leader_allowance_version,
            "follower_allowance_version": self.follower_allowance_version,
            "leader_allowed_loss_usd": self.leader_allowed_loss_usd,
            "follower_allowed_loss_usd": self.follower_allowed_loss_usd,
            "leader_rough_sl": self.leader_rough_sl,
            "leader_rough_tp": self.leader_rough_tp,
            "follower_rough_sl": self.follower_rough_sl,
            "follower_rough_tp": self.follower_rough_tp,
            "filling_mode": self.filling_mode,
            "decision_time": self.decision_time.isoformat(),
            "confirmation_timeout_seconds": self.confirmation_timeout_seconds,
        }

    @property
    def payload_hash(self) -> str:
        return _hash(_canonical_json(self.canonical()))

    def symbol_of(self, role: Role) -> str:
        """Both legs trade the exact same symbol name: discovery intersects
        symbol names exactly and performs no suffix normalization."""

        return self.symbol

    def direction_of(self, role: Role) -> Direction:
        return self.leader_direction if role == "leader" else self.follower_direction

    def quote_of(self, role: Role) -> QuoteSnapshot:
        return self.leader_quote if role == "leader" else self.follower_quote

    def plan_version_of(self, role: Role) -> str:
        return self.leader_plan_version if role == "leader" else self.follower_plan_version

    def allowance_version_of(self, role: Role) -> int:
        return self.leader_allowance_version if role == "leader" else self.follower_allowance_version

    def allowance_epoch_of(self, role: Role) -> int:
        return self.leader_allowance_epoch if role == "leader" else self.follower_allowance_epoch

    def allowed_loss_of(self, role: Role) -> str:
        return self.leader_allowed_loss_usd if role == "leader" else self.follower_allowed_loss_usd

    def rough_of(self, role: Role) -> tuple[str, str]:
        if role == "leader":
            return self.leader_rough_sl, self.leader_rough_tp
        return self.follower_rough_sl, self.follower_rough_tp


def _quote_from_canonical(raw: Mapping[str, object]) -> QuoteSnapshot:
    broker_time = _parse_utc(cast(str, raw["broker_time"]))
    return QuoteSnapshot(
        worker_id=cast(str, raw["worker_id"]),
        product_id=cast(str, raw["product_id"]),
        symbol=cast(str, raw["symbol"]),
        bid=Decimal(cast(str, raw["bid"])),
        ask=Decimal(cast(str, raw["ask"])),
        broker_time=broker_time,
        local_receive_time=broker_time,
        recovery_epoch=cast(str, raw["recovery_epoch"]),
        sequence=cast(int, raw["sequence"]),
        calibration=_calibration_from_canonical(raw.get("calibration")),
    )


def _attempt_from_canonical(value: Mapping[str, object]) -> Attempt:
    return Attempt(
        attempt_id=cast(str, value["attempt_id"]),
        policy_hash=cast(str, value["policy_hash"]),
        route_id=cast(str, value["route_id"]),
        universe_generation=cast(int, value["universe_generation"]),
        product_id=cast(str, value["product_id"]),
        symbol=cast(str, value["symbol"]),
        leader_direction=cast(Direction, value["leader_direction"]),
        follower_direction=cast(Direction, value["follower_direction"]),
        lots=cast(str, value["lots"]),
        leader_quote=_quote_from_canonical(cast(Mapping[str, object], value["leader_quote"])),
        follower_quote=_quote_from_canonical(cast(Mapping[str, object], value["follower_quote"])),
        leader_plan_version=cast(str, value["leader_plan_version"]),
        follower_plan_version=cast(str, value["follower_plan_version"]),
        leader_allowance_epoch=cast(int, value["leader_allowance_epoch"]),
        follower_allowance_epoch=cast(int, value["follower_allowance_epoch"]),
        leader_allowance_version=cast(int, value["leader_allowance_version"]),
        follower_allowance_version=cast(int, value["follower_allowance_version"]),
        leader_allowed_loss_usd=cast(str, value["leader_allowed_loss_usd"]),
        follower_allowed_loss_usd=cast(str, value["follower_allowed_loss_usd"]),
        leader_rough_sl=cast(str, value["leader_rough_sl"]),
        leader_rough_tp=cast(str, value["leader_rough_tp"]),
        follower_rough_sl=cast(str, value["follower_rough_sl"]),
        follower_rough_tp=cast(str, value["follower_rough_tp"]),
        filling_mode=cast(FillingMode, value["filling_mode"]),
        decision_time=_parse_utc(cast(str, value["decision_time"])),
        confirmation_timeout_seconds=float(cast(float, value["confirmation_timeout_seconds"])),
    )


LegEntryStatus = Literal[
    "pending", "sent", "rejected", "expired_not_started", "unknown_after_send", "filled"
]
LegProtectionStatus = Literal["rough", "precise", "failed"]


@dataclass(slots=True)
class LegState:
    attempt_id: str
    role: Role
    rough_sl: str | None = None
    rough_tp: str | None = None
    entry_effect_id: str | None = None
    entry_status: LegEntryStatus = "pending"
    ticket: str | None = None
    fill_price: str | None = None
    observed_volume: str | None = None
    observed_sl: str | None = None
    observed_tp: str | None = None
    protection_effect_id: str | None = None
    protection_status: LegProtectionStatus = "rough"
    precise_sl: str | None = None
    precise_tp: str | None = None
    close_effect_id: str | None = None
    empty_verified: bool = False
    timer_started_at: str | None = None
    # Durable intent markers, both written *before* the action they describe:
    # ``attempt_relayed`` before the immutable attempt leaves for the peer, and
    # ``entry_dispatched`` before the irreversible local broker path.  A crash
    # immediately after either action therefore still recovers the conservative
    # truth instead of concluding nothing happened.
    attempt_relayed: bool = False
    entry_dispatched: bool = False


@dataclass(slots=True)
class PeerLegView:
    """The peer's self-reported leg status, relayed opaquely through the controller."""

    status: str = "unknown"
    ticket: str | None = None
    symbol: str | None = None
    side: str | None = None
    volume: str | None = None
    fill_price: str | None = None
    sl: str | None = None
    tp: str | None = None
    reason: str = ""


# --------------------------------------------------------------------------- #
# The deep module
# --------------------------------------------------------------------------- #


class PairExecutionCell:
    """Runs as leader or follower for one durable Worker pair route.

    Construct once per Worker process with the controller's authoritative route
    assignment and this Worker's own durable adapters (its single effect
    journal and RPC scheduler), then call :meth:`recover` (process restart) and
    feed events via :meth:`handle_event`.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        route: RouteAssignment,
        db_path: Path,
        relay: PairRelayAdapter,
        mt5: PairCellMT5,
        local_risk_limits: WorkerRiskLimits,
        effect_journal: WorkerEffectJournal,
        scheduler: DeadlineAwareTraderRpcScheduler,
        allow_live: bool = DEFAULT_ALLOW_LIVE,
        monotonic: Callable[[], float] = _time.monotonic,
        serve_foreign_scheduler_item: Callable[[ScheduledTraderRpc], None] | None = None,
        dispatch_foreign_scheduler_outcome: Callable[[TraderRpcOutcome], None] | None = None,
    ) -> None:
        """``route`` is the controller's authoritative record as this Worker's
        adapter received it; the cell never authors a role and never adopts one
        the controller has not assigned.  It carries no ``trader_id``, no
        lease, and no TTL.

        ``scheduler`` must be the same
        :class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler` instance
        the rest of this Worker process uses, because its single ``_active``
        slot is the Worker's actual MT5 serialization boundary.

        ``monotonic`` supplies the independent leader-decision and
        follower-receipt timers.  It is injected so timeout races are
        reproducible without wall-clock sleeps.

        ``local_risk_limits`` is this Worker's *own authored* budget and loss
        configuration, whose budget is its frozen startup broker balance.  It
        is never enforced directly: the accepted canonical policy's copy for
        this role is.

        ``allow_live`` is a Worker-local, non-policy safety guard.  A follower
        with ``allow_live=False`` refuses a ``live`` canonical policy outright
        rather than silently running shadow against a live leader.
        """

        if not worker_id:
            raise PairExecutionCellError("A Pair Execution Cell requires a Worker identity.")
        assigned = route.role_of(worker_id)
        if assigned is None:
            raise PairExecutionCellError("This Worker is not on the assigned pair route.")
        if assigned != route.role:
            raise PairExecutionCellError(
                "The assigned role does not match this Worker's position on the route record."
            )
        self._worker_id = worker_id
        self._route = route
        self._role: Role = route.role
        self._peer_worker_id = cast(str, route.peer_of(worker_id))
        self._relay = relay
        self._mt5 = mt5
        self._declared_limits = local_risk_limits
        self._allow_live = bool(allow_live)
        self._journal = effect_journal
        self._scheduler = scheduler
        self._monotonic = monotonic
        self._serve_foreign_scheduler_item = serve_foreign_scheduler_item
        self._dispatch_foreign_scheduler_outcome = dispatch_foreign_scheduler_outcome
        self._db = sqlite3.connect(db_path)
        self._db.execute("PRAGMA journal_mode=WAL")
        self._db.execute("PRAGMA synchronous=FULL")
        self._create_schema()

        self._route_state: RouteState = route.state
        self._metadata_reconciliation: str | None = None
        self._state_version = 0
        # Every rebuilt cell publishes under a strictly greater durable epoch,
        # so a surviving peer can tell a re-seeded summary from a stale one.
        # It is allocated after durable state loads, when `_now` is meaningful.
        self._publisher_epoch = 0
        self._peer_publisher_epoch = 0
        self._reseed_requested = False
        self._reseed_ask_nonce: str | None = None
        self._served_reseed_nonces: list[str] = []
        self._foreign_route_attempt: str | None = None
        self._local_acceptance: PairingAcceptance | None = None
        self._peer_acceptance: PairingAcceptance | None = None
        self._policy: StrategyPolicy | None = None
        self._policy_accepted = False
        self._policy_refusal: str | None = None
        self._peer_policy_hash: str | None = None
        self._peer_empty_claim = False
        self._peer_nothing_unresolved = False
        self._local_catalog: CatalogSummary | None = None
        self._peer_catalog: CatalogSummary | None = None
        self._catalog_version = 0
        self._universe: DiscoveredUniverse | None = None
        self._staged_universe: DiscoveredUniverse | None = None
        self._pending_universe: DiscoveredUniverse | None = None
        self._discovery_reason = "no catalog summary has been exchanged yet"
        self._rediscovery_pending = False
        self._rediscovery_failure: str | None = None
        self._rediscovery_request_id: str | None = None
        self._rediscovery_actor = ""
        self._rediscovery_reason = ""
        self._rediscovery_started_at: datetime | None = None
        self._rediscovery_last_sent_at: datetime | None = None
        self._serving_rediscovery_request_id: str | None = None
        self._answered_rediscovery: tuple[str, tuple[str, dict[str, object]]] | None = None
        self._plans: dict[tuple[str, Direction], SizingPlan] = {}
        self._plan_index: dict[str, SizingPlan] = {}
        self._plan_set_version = _hash(_canonical_json([]))[:16]
        self._plans_refreshed_at: datetime | None = None
        self._last_plan_quote_set: frozenset[str] = frozenset()
        self._suspended_products: dict[str, str] = {}
        self._peer_plans: dict[tuple[str, Direction], SizingPlan] = {}
        self._peer_plan_index: dict[str, SizingPlan] = {}
        self._peer_plan_set_version: str | None = None
        self._peer_plan_batches: dict[str, dict[int, list[object]]] = {}
        self._allowance_version = 0
        self._published_allowance: str | None = None
        self._peer_allowance: RemainingAllowanceSummary | None = None
        self._local_quotes: dict[str, QuoteSnapshot] = {}
        self._peer_quotes: dict[str, QuoteSnapshot] = {}
        self._decided_quotes: dict[str, tuple[str, int, str, int]] = {}
        self._quote_epochs: dict[str, list[str]] = {}
        self._local_facts: ReadinessFactsEvent | None = None
        self._peer_session = True
        self._peer_ready = False
        self._peer_ready_reason = "no peer readiness observed yet"
        self._attempt: Attempt | None = None
        self._leg: LegState | None = None
        self._peer_leg = PeerLegView()
        self._pair_confirmed = False
        self._timer_deadline: float | None = None
        self._attempt_timing_origin: float | None = None
        self._peer_proof_wait_started: float | None = None
        self._peer_proof_last_probe: float | None = None
        self._peer_proof_probes_sent = 0
        self._active_since: datetime | None = None
        self._state: PairState = "IDLE"
        self._desired: DesiredState = "NONE"
        self._needs_human: str | None = None
        self._now = datetime(1970, 1, 1, tzinfo=UTC)
        self._recovering = False
        self._last_timing: dict[str, object] = {}
        self._last_published_readiness: tuple[object, ...] | None = None
        self._last_published_plan_version: str | None = None
        self._last_published_catalog_hash: str | None = None
        self._load_persisted_state()
        self._publisher_epoch = self._next_publisher_epoch()

    # -- schema ---------------------------------------------------------- #

    def _create_schema(self) -> None:
        # Legacy shapes are migrated *before* anything creates or queries the
        # current ones, so no code path ever reads a column that is not there.
        _migrate_pair_cell_schema(self._db)
        self._db.executescript(
            """
            CREATE TABLE IF NOT EXISTS cell_route (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                payload TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_state_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                version INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_publisher_epoch (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                epoch INTEGER NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_pairing_acceptance (
                owner TEXT PRIMARY KEY,
                payload TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_universe (
                universe_generation INTEGER PRIMARY KEY,
                route_id TEXT NOT NULL,
                payload TEXT NOT NULL,
                installed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_policy (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                policy_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                accepted_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_plan_version (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                plan_set_version TEXT NOT NULL,
                refreshed_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_sizing_plans (
                plan_version TEXT PRIMARY KEY,
                universe_generation INTEGER NOT NULL,
                product_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                payload TEXT NOT NULL,
                installed_at TEXT NOT NULL,
                superseded_at TEXT
            );
            CREATE TABLE IF NOT EXISTS cell_peer_sizing_plans (
                plan_version TEXT PRIMARY KEY,
                universe_generation INTEGER NOT NULL,
                product_id TEXT NOT NULL,
                direction TEXT NOT NULL,
                payload TEXT NOT NULL,
                plan_set_version TEXT NOT NULL,
                current INTEGER NOT NULL DEFAULT 0,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_attempts (
                attempt_id TEXT PRIMARY KEY,
                payload_hash TEXT NOT NULL,
                payload TEXT NOT NULL,
                created_at TEXT NOT NULL,
                route_id TEXT,
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
                pair_confirmed INTEGER NOT NULL DEFAULT 0,
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
            CREATE TABLE IF NOT EXISTS cell_realized_pnl (
                event_id TEXT PRIMARY KEY,
                ny_day TEXT NOT NULL,
                amount_usd TEXT NOT NULL,
                recorded_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_product_quarantine (
                product_id TEXT PRIMARY KEY,
                offending_worker_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                universe_generation INTEGER,
                receipt TEXT NOT NULL,
                quarantined_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS cell_product_quarantine_release (
                product_id TEXT NOT NULL,
                offending_worker_id TEXT NOT NULL,
                attempt_id TEXT NOT NULL,
                released_at TEXT NOT NULL,
                PRIMARY KEY (product_id, offending_worker_id, attempt_id)
            );
            CREATE TABLE IF NOT EXISTS cell_product_release_marker (
                product_id TEXT PRIMARY KEY,
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
        self._backfill_attempt_routes()

    def _backfill_attempt_routes(self) -> None:
        """Name the owning route on attempt rows written before it was stored.

        Runs only after the schema migration has guaranteed the column exists.
        An attempt authored by this revision has always carried its own
        ``route_id``; this only lifts it out of the payload so the load path can
        filter on it.  A row from a revision that had no ``route_id`` at all
        stays ``NULL`` and is isolated rather than adopted.
        """

        if "route_id" not in _table_columns(self._db, "cell_attempts"):  # pragma: no cover
            return
        rows = self._db.execute(
            "SELECT attempt_id, payload FROM cell_attempts WHERE route_id IS NULL"
        ).fetchall()
        for attempt_id, payload in rows:
            try:
                route_id = json.loads(payload).get("route_id")
            except (ValueError, AttributeError):
                route_id = None
            if isinstance(route_id, str) and route_id:
                self._db.execute(
                    "UPDATE cell_attempts SET route_id = ? WHERE attempt_id = ?",
                    (route_id, attempt_id),
                )
        if rows:
            self._db.commit()

    def _load_persisted_state(self) -> None:
        row = self._db.execute("SELECT at FROM cell_transitions ORDER BY id DESC LIMIT 1").fetchone()
        if row is not None:
            self._now = max(self._now, _parse_utc(row[0]))
        version_row = self._db.execute("SELECT version FROM cell_state_version WHERE id = 1").fetchone()
        if version_row is not None:
            self._state_version = int(version_row[0])
        route_row = self._db.execute("SELECT payload FROM cell_route WHERE id = 1").fetchone()
        if route_row is None:
            self._persist_route()
        else:
            stored = _route_from_canonical(json.loads(route_row[0]))
            disagreement = self._route_disagreement(
                route_id=stored.route_id,
                role=stored.role,
                leader_worker_id=stored.leader_worker_id,
                follower_worker_id=stored.follower_worker_id,
            )
            if disagreement is not None:
                # The controller's record is authoritative for relay permission
                # and role; the durable copy is only recovery evidence.  A
                # disagreement is loud and never resolved by touching exposure.
                self._metadata_reconciliation = disagreement
                self._transition("route_metadata_disagreement", disagreement)
            else:
                self._route_state = stored.state
                self._persist_route()
        for owner, payload in self._db.execute(
            "SELECT owner, payload FROM cell_pairing_acceptance"
        ).fetchall():
            acceptance = _acceptance_from_canonical(json.loads(payload))
            if owner == "local":
                self._local_acceptance = acceptance
            else:
                self._peer_acceptance = acceptance
        universe_row = self._db.execute(
            "SELECT payload FROM cell_universe ORDER BY universe_generation DESC LIMIT 1"
        ).fetchone()
        if universe_row is not None:
            universe = _universe_from_canonical(json.loads(universe_row[0]))
            if universe.route_id == self._route.route_id:
                self._universe = universe
                self._discovery_reason = ""
        stop_row = self._db.execute("SELECT reason FROM cell_operator_stop WHERE id = 1").fetchone()
        if stop_row is not None:
            self._needs_human = stop_row[0]
        policy_row = self._db.execute("SELECT payload FROM cell_policy WHERE id = 1").fetchone()
        if policy_row is not None:
            # The full canonical content is durable, so a restarted Worker
            # never has to re-request the policy to know what it agreed to.
            self._policy = _policy_from_canonical(json.loads(policy_row[0]))
            self._policy_accepted = True
        self._load_persisted_plans()
        self._isolate_foreign_route_attempt()
        attempt_row = self._db.execute(
            "SELECT attempt_id, payload, terminal FROM cell_attempts"
            " WHERE route_id = ? ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (self._route.route_id,),
        ).fetchone()
        if attempt_row is None or attempt_row[2]:
            return
        self._attempt = _attempt_from_canonical(json.loads(attempt_row[1]))
        leg_row = self._db.execute(
            "SELECT payload FROM cell_leg_state WHERE attempt_id = ?", (attempt_row[0],)
        ).fetchone()
        if leg_row is not None:
            self._leg = LegState(**json.loads(leg_row[0]))
        peer_row = self._db.execute(
            "SELECT payload FROM cell_peer_leg WHERE attempt_id = ?", (attempt_row[0],)
        ).fetchone()
        if peer_row is not None:
            self._peer_leg = PeerLegView(**json.loads(peer_row[0]))
        desired_row = self._db.execute(
            "SELECT desired_state, pair_confirmed FROM cell_desired_state WHERE id = 1"
        ).fetchone()
        if desired_row is not None:
            self._desired = cast(DesiredState, desired_row[0])
            self._pair_confirmed = bool(desired_row[1])
        active_row = self._db.execute(
            "SELECT active_since FROM cell_active_state WHERE attempt_id = ?", (attempt_row[0],)
        ).fetchone()
        if active_row is not None:
            self._active_since = _parse_utc(active_row[0])
        if self._leg is not None:
            self._recovering = True
            self._state = self._recovered_state()
        elif self._attempt is not None:
            self._recovering = True

    def _load_persisted_plans(self) -> None:
        """Restore the current plan set and every still-retained superseded one."""

        if self._universe is None:
            return
        generation = self._universe.universe_generation
        for version, payload, superseded in self._db.execute(
            "SELECT plan_version, payload, superseded_at FROM cell_sizing_plans"
            " WHERE universe_generation = ?",
            (generation,),
        ).fetchall():
            plan = _plan_from_canonical(json.loads(payload))
            self._plan_index[str(version)] = plan
            if superseded is None:
                self._plans[(plan.product_id, plan.direction)] = plan
        for version, payload, current, plan_set in self._db.execute(
            "SELECT plan_version, payload, current, plan_set_version FROM cell_peer_sizing_plans"
            " WHERE universe_generation = ?",
            (generation,),
        ).fetchall():
            plan = _plan_from_canonical(json.loads(payload))
            self._peer_plan_index[str(version)] = plan
            if current:
                self._peer_plans[(plan.product_id, plan.direction)] = plan
                self._peer_plan_set_version = str(plan_set)
        if self._plans:
            self._recompute_plan_version(persist=False)
        row = self._db.execute("SELECT refreshed_at FROM cell_plan_version WHERE id = 1").fetchone()
        if row is not None and self._plans:
            self._plans_refreshed_at = _parse_utc(row[0])

    def _isolate_foreign_route_attempt(self) -> None:
        """Never adopt an unresolved attempt this route does not own.

        Durable state reused under a new ``route_id`` -- or migrated from a
        revision that never recorded one -- may still hold an attempt from a
        different pairing.  Adopting it would relay containment and leg status
        for foreign exposure onto a route that never authorized it, so the row
        is left untouched and this Worker goes loud instead.  Its
        desired/active rows are likewise not applied to the new route, while
        remaining on disk for whoever owns the previous route's recovery.
        """

        row = self._db.execute(
            "SELECT attempt_id, route_id FROM cell_attempts"
            " WHERE terminal = 0 AND (route_id IS NULL OR route_id != ?)"
            " ORDER BY created_at DESC, rowid DESC LIMIT 1",
            (self._route.route_id,),
        ).fetchone()
        if row is None:
            return
        attempt_id = str(row[0])
        origin = "no recorded route" if row[1] is None else f"route {row[1]}"
        self._foreign_route_attempt = attempt_id
        self._set_needs_human(
            f"attempt {attempt_id} belongs to {origin} and is unresolved;"
            f" this Worker is now on route {self._route.route_id} and may not act on it",
            "foreign-route attempt isolated",
        )

    def foreign_route_attempt(self) -> str | None:
        """An unresolved attempt from another route, isolated and never acted on."""

        return self._foreign_route_attempt

    def _recovered_state(self) -> PairState:
        assert self._leg is not None
        if self._leg.entry_status == "unknown_after_send":
            return "ENTRY_UNCERTAIN"
        if self._desired == "EMPTY":
            return "CONVERGING_EMPTY"
        if self._leg.protection_status == "precise" and self._active_since is not None:
            return "ACTIVE"
        if self._leg.entry_status == "filled":
            return "AWAITING_PAIR_CONFIRMATION"
        return "ENTERING"

    # -- persistence helpers --------------------------------------------- #

    def _transition(self, event: str, detail: str = "") -> None:
        self._db.execute(
            "INSERT INTO cell_transitions (attempt_id, event, detail, at) VALUES (?, ?, ?, ?)",
            (None if self._attempt is None else self._attempt.attempt_id, event, detail, _iso(self._now)),
        )
        self._db.commit()
        _LOGGER.debug(
            "Pair Execution Cell transition: event=%s state=%s attempt_id=%s detail=%s.",
            event,
            self._state,
            None if self._attempt is None else self._attempt.attempt_id,
            detail or "-",
        )

    def _record_timing(self, key: str) -> None:
        self._last_timing[key] = _iso(self._now)
        self._transition(f"timing:{key}")
        if key == "attempt_persisted":
            self._attempt_timing_origin = self._monotonic()
        origin = self._attempt_timing_origin
        elapsed = "-" if origin is None else f"{(self._monotonic() - origin) * 1000:.1f}"
        _LOGGER.debug(
            "Pair Execution Cell attempt timing: attempt_id=%s phase=%s elapsed_ms=%s observed_at=%s.",
            None if self._attempt is None else self._attempt.attempt_id,
            key,
            elapsed,
            _iso(self._now),
        )

    def transition_history(self) -> list[dict[str, object]]:
        rows = self._db.execute(
            "SELECT attempt_id, event, detail, at FROM cell_transitions ORDER BY id"
        ).fetchall()
        return [{"attempt_id": r[0], "event": r[1], "detail": r[2], "at": r[3]} for r in rows]

    def _bump_state_version(self) -> None:
        """Advance the local attempt/effect state version.

        Every new or changed local attempt or broker effect advances it, which
        is exactly what invalidates an outstanding safe-unpair assertion.
        """

        self._state_version += 1
        self._db.execute(
            "INSERT INTO cell_state_version (id, version, updated_at) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET version = excluded.version, updated_at = excluded.updated_at",
            (self._state_version, _iso(self._now)),
        )
        self._db.commit()

    def _next_publisher_epoch(self) -> int:
        """Allocate this cell instance's durable, per-route publisher epoch.

        It advances once per construction, which is exactly what makes a
        rebuilt Worker's re-seeded summaries distinguishable from the versions
        its previous process had already published.
        """

        row = self._db.execute("SELECT epoch FROM cell_publisher_epoch WHERE id = 1").fetchone()
        epoch = 1 if row is None else int(row[0]) + 1
        self._db.execute(
            "INSERT INTO cell_publisher_epoch (id, epoch, updated_at) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET epoch = excluded.epoch, updated_at = excluded.updated_at",
            (epoch, _iso(self._now)),
        )
        self._db.commit()
        return epoch

    def publisher_epoch(self) -> int:
        return self._publisher_epoch

    def peer_publisher_epoch(self) -> int:
        return self._peer_publisher_epoch

    def _note_peer_publisher_epoch(self, epoch: int) -> None:
        """A greater peer epoch means the peer was rebuilt: re-seed everything.

        The peer's own publication caches were reset with it, but *this*
        Worker's caches would otherwise suppress every value the rebuilt peer
        now needs, leaving it permanently short of state it never received.
        """

        if epoch <= self._peer_publisher_epoch:
            return
        previous = self._peer_publisher_epoch
        self._peer_publisher_epoch = epoch
        if self._peer_allowance is not None and self._peer_allowance.publisher_epoch < epoch:
            # Fail closed: a surviving summary from the peer's previous session
            # may be larger than what it can accept now.
            self._peer_allowance = None
        if previous:
            self._transition("peer_publisher_epoch_advanced", f"{previous} -> {epoch}")
        self._request_reseed(f"peer publisher epoch {epoch}")

    def _request_reseed(self, reason: str, *, ask_peer: bool = False) -> None:
        """Drop every publication cache so all peer-required state is resent.

        ``ask_peer`` covers the one-sided outage: only this Worker observed the
        relay transition, so its peer never re-seeded and its own caches would
        suppress exactly the state this Worker just discarded.  The request is
        answered at most once per nonce and is never answered with another
        request, so it cannot become a message storm.
        """

        self._reseed_requested = True
        self._last_published_readiness = None
        self._last_published_plan_version = None
        self._last_published_catalog_hash = None
        self._published_allowance = None
        if ask_peer:
            self._reseed_ask_nonce = str(uuid4())
        self._transition("republish_peer_state", reason)

    def _accept_reseed_request(self, payload: Mapping[str, object]) -> None:
        """Re-seed once for this nonce and never answer with a request."""

        nonce = payload.get("nonce")
        if not isinstance(nonce, str) or not nonce or nonce in self._served_reseed_nonces:
            return
        self._served_reseed_nonces.append(nonce)
        del self._served_reseed_nonces[:-64]
        self._request_reseed(f"the peer asked for a re-seed: {payload.get('reason') or nonce}")

    def _republish_peer_state(self) -> None:
        """Resend the state a peer cannot derive locally.  Idempotent.

        Every message here is accepted as a no-op by a peer that already holds
        it: an identical policy is answered "already durably accepted", an
        equal ``universe_generation`` is ignored, and a re-recorded Pairing
        Acceptance payload overwrites itself with the same bytes.
        """

        if self._role == "leader":
            self._publish_policy()
            staged = self._staged_universe or self._universe
            if staged is not None:
                self._publish_universe(staged)
        elif self._local_acceptance is not None:
            self._relay.send(
                self._envelope(
                    "pairing_acceptance", {"acceptance": self._local_acceptance.canonical()}
                )
            )

    def _persist_route(self) -> None:
        payload = dict(self._route.canonical())
        payload["state"] = self._route_state
        self._db.execute(
            "INSERT INTO cell_route (id, payload, updated_at) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
            (_canonical_json(payload), _iso(self._now)),
        )
        self._db.commit()

    def _persist_acceptance(self, owner: str, acceptance: PairingAcceptance) -> None:
        self._db.execute(
            "INSERT INTO cell_pairing_acceptance (owner, payload, recorded_at) VALUES (?, ?, ?)"
            " ON CONFLICT(owner) DO UPDATE SET payload = excluded.payload,"
            " recorded_at = excluded.recorded_at",
            (owner, _canonical_json(acceptance.canonical()), _iso(self._now)),
        )
        self._db.commit()

    def _persist_policy(self, policy: StrategyPolicy) -> None:
        self._db.execute("DELETE FROM cell_policy WHERE id = 1")
        self._db.execute(
            "INSERT INTO cell_policy (id, policy_hash, payload, accepted_at) VALUES (1, ?, ?, ?)",
            (policy.hash, _canonical_json(policy.canonical()), _iso(self._now)),
        )
        self._db.commit()

    def _persist_attempt(self, attempt: Attempt) -> None:
        self._db.execute(
            "INSERT INTO cell_attempts (attempt_id, payload_hash, payload, created_at, route_id,"
            " terminal) VALUES (?, ?, ?, ?, ?, 0)",
            (
                attempt.attempt_id,
                attempt.payload_hash,
                _canonical_json(attempt.canonical()),
                _iso(self._now),
                attempt.route_id,
            ),
        )
        self._db.commit()
        self._bump_state_version()

    def _terminate_attempt(self, reason: str) -> None:
        if self._attempt is None:
            return
        self._db.execute(
            "UPDATE cell_attempts SET terminal = 1, terminal_reason = ? WHERE attempt_id = ?",
            (reason, self._attempt.attempt_id),
        )
        self._transition(reason)
        self._bump_state_version()

    def _persist_leg(self) -> None:
        if self._attempt is None or self._leg is None:
            return
        self._db.execute(
            "INSERT INTO cell_leg_state (attempt_id, payload, updated_at) VALUES (?, ?, ?)"
            " ON CONFLICT(attempt_id) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at",
            (self._attempt.attempt_id, json.dumps(asdict(self._leg)), _iso(self._now)),
        )
        self._db.commit()
        self._bump_state_version()

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
            "INSERT INTO cell_desired_state (id, desired_state, attempt_id, pair_confirmed, updated_at)"
            " VALUES (1, ?, ?, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET desired_state = excluded.desired_state,"
            " attempt_id = excluded.attempt_id, pair_confirmed = excluded.pair_confirmed,"
            " updated_at = excluded.updated_at",
            (
                self._desired,
                None if self._attempt is None else self._attempt.attempt_id,
                1 if self._pair_confirmed else 0,
                _iso(self._now),
            ),
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

    def _peer_terminal_proof_stop_reason(self, attempt_id: str) -> str:
        return (
            f"attempt {attempt_id} has no peer terminal proof after the bounded recovery path"
        )

    def _clear_resolved_peer_terminal_proof_stop(self, attempt_id: str) -> None:
        if self._needs_human != self._peer_terminal_proof_stop_reason(attempt_id):
            return
        self._needs_human = None
        self._state = "EMPTY"
        self._db.execute("DELETE FROM cell_operator_stop WHERE id = 1")
        self._transition("peer_terminal_proof_recovered", attempt_id)

    def _recover_terminal_proof_stop(self) -> None:
        """Clear only a legacy missing-proof stop with durable terminal evidence."""

        reason = self._needs_human
        prefix, suffix = "attempt ", " has no peer terminal proof after the bounded recovery path"
        if not isinstance(reason, str) or not reason.startswith(prefix) or not reason.endswith(suffix):
            return
        attempt_id = reason.removeprefix(prefix).removesuffix(suffix)
        if not attempt_id:
            return
        row = self._db.execute(
            "SELECT 1 FROM cell_attempts WHERE attempt_id = ? AND route_id = ?"
            " AND terminal = 1 AND terminal_reason = 'both_empty_verified'",
            (attempt_id, self._route.route_id),
        ).fetchone()
        if row is not None:
            self._clear_resolved_peer_terminal_proof_stop(attempt_id)

    # -- pairing acceptance ------------------------------------------------ #

    def freeze_pairing_acceptance(
        self, *, proposal_id: str, account_currency: str = REQUIRED_ACCOUNT_CURRENCY
    ) -> PairingAcceptance:
        """Follower-only: freeze and publish this Worker's acceptance payload.

        The frozen startup balance and the four authored risk tunables are this
        Worker's own and do not change for the life of the route.
        """

        if self._role != "follower":
            raise PairExecutionCellError("Only the follower authors a Pairing Acceptance payload.")
        if account_currency != REQUIRED_ACCOUNT_CURRENCY:
            raise PairExecutionCellError(
                "A Pairing Acceptance payload requires a USD account currency."
            )
        acceptance = PairingAcceptance(
            proposal_id=proposal_id,
            worker_id=self._worker_id,
            startup_balance_usd=self._declared_limits.strategy_budget_usd,
            account_currency=account_currency,
            maximum_margin_fraction=self._declared_limits.maximum_margin_fraction,
            daily_loss_fraction=self._declared_limits.daily_loss_fraction,
            trade_loss_fraction=self._declared_limits.trade_loss_fraction,
            maximum_loss_per_trade_usd=self._declared_limits.maximum_loss_per_trade_usd,
        )
        self._local_acceptance = acceptance
        self._persist_acceptance("local", acceptance)
        self._transition("pairing_acceptance_frozen", acceptance.proposal_id)
        self._relay.send(
            self._envelope("pairing_acceptance", {"acceptance": acceptance.canonical()})
        )
        return acceptance

    def pairing_acceptance(self) -> PairingAcceptance | None:
        """This Worker's own frozen acceptance payload, if it authored one."""

        return self._local_acceptance

    def peer_pairing_acceptance(self) -> PairingAcceptance | None:
        """Leader-only view of the follower's payload, for verbatim copying."""

        return self._peer_acceptance

    def _accept_pairing_acceptance(self, payload: Mapping[str, object]) -> None:
        if self._role != "leader":
            return
        raw = payload.get("acceptance")
        if not isinstance(raw, dict):
            self._transition("pairing_acceptance_refused", "payload is malformed")
            return
        try:
            acceptance = _acceptance_from_canonical(raw)
        except (KeyError, TypeError, ValueError, PairExecutionCellError) as error:
            self._transition("pairing_acceptance_refused", str(error))
            return
        if acceptance.worker_id != self._peer_worker_id:
            self._transition("pairing_acceptance_refused", "payload names another Worker")
            return
        self._peer_acceptance = acceptance
        self._persist_acceptance("peer", acceptance)
        self._transition("pairing_acceptance_recorded", acceptance.proposal_id)

    # -- public lifecycle ------------------------------------------------- #

    def accept_policy(self, policy: StrategyPolicy) -> PairResult:
        """Leader-only: durably adopt and publish the canonical strategy policy.

        ``leader_risk`` must be the leader's own declaration and
        ``follower_risk`` must be a byte-identical copy of the follower's
        Pairing Acceptance payload.  A leader that cannot copy that payload
        verbatim fails closed and publishes no policy.
        """

        if self._role != "leader":
            raise PairExecutionCellError("Only the leader supplies the canonical strategy policy.")
        if not policy.risk_of("leader").matches(self._declared_limits):
            raise PairExecutionCellError(
                "Policy refused: its leader risk configuration does not match this Worker's declaration."
            )
        acceptance = self._peer_acceptance
        if acceptance is None:
            raise PairExecutionCellError(
                "Policy refused: no Pairing Acceptance payload has been received from the follower."
            )
        if not policy.follower_risk.is_verbatim_copy_of(acceptance.risk_limits()):
            raise PairExecutionCellError(
                "Policy refused: follower_risk is not a verbatim copy of the Pairing Acceptance payload."
            )
        if self._policy is not None and self._policy.hash == policy.hash:
            self._publish_policy()
            return self._result()
        blocked = self._policy_change_blocked()
        if blocked is not None:
            raise PairExecutionCellError(f"Policy change refused: {blocked}")
        self._policy = policy
        self._policy_accepted = True
        self._policy_refusal = None
        self._persist_policy(policy)
        self._plans_refreshed_at = None
        self._transition("policy_accepted", policy.hash)
        self._publish_policy()
        return self._result()

    def recover(self) -> PairResult:
        """Resume durable state after restart before entry readiness returns.

        Elapsed monotonic time cannot be reconstructed across a restart, so an
        unresolved attempt that already reached the broker contains
        immediately rather than guessing how much of its timer is left.
        """

        self._recovering = True
        if self._attempt is not None and self._leg is None:
            # The local leg row is written before the entry effect is prepared
            # and before anything is relayed or sent, so an attempt without one
            # provably produced no effect, no relay, and no broker call.
            self._transition("recovered_attempt_without_local_leg", self._attempt.attempt_id)
            self._terminate_attempt("no_local_effect_was_ever_prepared")
            self._reset_attempt()
            self._state = "EMPTY"
        if self._attempt is not None and self._leg is not None:
            self._reconcile_entry_effect_state()
            if self._state != "ACTIVE":
                self._transition("recovery_containment", self._attempt.attempt_id)
                self._begin_close("unresolved_attempt_after_restart")
        self._transition("recovered", self._state)
        self._recovering = False
        self._recover_terminal_proof_stop()
        # A running peer may have published an unchanged readiness snapshot
        # while this Worker was offline. Explicitly request its state instead
        # of relying on a later epoch change or market event to trigger it.
        self._request_reseed("local Worker recovered", ask_peer=True)
        if self._attempt is not None:
            self._request_broker_read()
            self._progress()
        return self._result()

    def request_close(self, reason: str) -> PairResult:
        """Every exit path (timed exit, cutoff, risk, shutdown, integrity
        divergence, entry failure, protection failure) converges here."""

        self._begin_close(reason)
        if self._attempt is not None:
            self._request_broker_read()
            self._progress()
        return self._result()

    def close(self) -> None:
        self._db.close()

    # -- route metadata and safe unpair ------------------------------------ #

    def route_id(self) -> str:
        return self._route.route_id

    def status(self) -> PairResult:
        """Return the current observable state for Worker diagnostics."""

        return self._result()

    def route_state(self) -> RouteState:
        return self._route_state

    def assigned_role(self) -> Role:
        return self._role

    def state_version(self) -> int:
        """This Worker's monotonic local attempt/effect state version."""

        return self._state_version

    def metadata_reconciliation(self) -> str | None:
        return self._metadata_reconciliation

    def _route_disagreement(
        self,
        *,
        route_id: str | None,
        role: Role | None,
        leader_worker_id: str | None,
        follower_worker_id: str | None,
    ) -> str | None:
        if route_id is None:
            return "the controller holds no route for this Worker"
        if route_id != self._route.route_id:
            return "the controller's route_id does not match this Worker's durable route"
        if role is not None and role != self._role:
            return "the controller assigns this Worker a different role"
        if leader_worker_id is not None and leader_worker_id != self._route.leader_worker_id:
            return "the controller's route names a different leader"
        if follower_worker_id is not None and follower_worker_id != self._route.follower_worker_id:
            return "the controller's route names a different follower"
        return None

    def _apply_route_metadata(self, event: RouteMetadataEvent) -> None:
        disagreement = self._route_disagreement(
            route_id=event.route_id,
            role=event.role,
            leader_worker_id=event.leader_worker_id,
            follower_worker_id=event.follower_worker_id,
        )
        if disagreement is not None:
            if self._metadata_reconciliation != disagreement:
                self._metadata_reconciliation = disagreement
                self._last_published_readiness = None
                self._transition("route_metadata_disagreement", disagreement)
            # Containment of anything this Worker already owns continues from
            # its own durable effects and exact broker facts; nothing here
            # discards, closes, hides, or infers exposure.
            return
        if self._metadata_reconciliation is not None:
            self._metadata_reconciliation = None
            self._last_published_readiness = None
            self._transition("route_metadata_agreed", self._route.route_id)
        state = event.state or "ACTIVE"
        if state != self._route_state:
            self._route_state = state
            self._persist_route()
            self._last_published_readiness = None
            self._transition("route_state", state)

    def safe_unpair_assertion(self) -> SafeUnpairAssertion:
        """Produce this Worker's safe-unpair assertion from fresh local evidence.

        The assertion is valid only while it stays bound to the current
        ``route_id`` *and* this Worker's current ``state_version``; any new or
        changed local attempt or broker effect invalidates it.
        """

        empty = self._broker_verified_empty()
        terminal_reason = self._unresolved_local_work()
        reasons: list[str] = []
        if not empty:
            reasons.append("this account is not broker-verified empty from fresh observation")
        if terminal_reason is not None:
            reasons.append(terminal_reason)
        assertion = SafeUnpairAssertion(
            worker_id=self._worker_id,
            route_id=self._route.route_id,
            state_version=self._state_version,
            broker_verified_empty=empty,
            attempts_and_effects_terminal=terminal_reason is None,
            reason="; ".join(reasons),
            asserted_at=self._now,
        )
        self._transition(
            "safe_unpair_assertion",
            f"safe={assertion.safe}; state_version={assertion.state_version}",
        )
        return assertion

    def safe_unpair_assertion_is_current(self, assertion: SafeUnpairAssertion) -> bool:
        """Whether an outstanding assertion still binds to current local facts."""

        return (
            assertion.worker_id == self._worker_id
            and assertion.route_id == self._route.route_id
            and assertion.state_version == self._state_version
        )

    def _unresolved_local_work(self) -> str | None:
        if self._attempt is not None:
            return "an attempt is unresolved"
        row = self._db.execute("SELECT COUNT(*) FROM cell_attempts WHERE terminal = 0").fetchone()
        if row is not None and int(row[0]):
            return "a durable attempt is not terminal"
        try:
            unresolved = self._journal.unresolved()
        except EffectJournalError:
            return "the effect journal is unhealthy"
        if unresolved:
            return "a local broker effect is unresolved"
        return None


    # -- Initial Compatible Product Discovery ------------------------------ #

    def universe_generation(self) -> int | None:
        return None if self._universe is None else self._universe.universe_generation

    def discovered_universe(self) -> DiscoveredUniverse | None:
        return self._universe

    def discovery_reason(self) -> str:
        return self._discovery_reason

    def last_rediscovery_failure(self) -> str | None:
        """Why the most recent explicit rediscovery attempt failed closed."""

        return self._rediscovery_failure

    def request_rediscovery(self, *, actor: str, reason: str = "") -> PairResult:
        """Either role may request an explicit rediscovery on the same route."""

        return self.handle_event(
            RediscoveryRequestEvent(actor=actor, reason=reason, observed_at=self._now)
        )

    def _handle_rediscovery_request(self, actor: str, reason: str) -> None:
        self._rediscovery_pending = True
        self._rediscovery_failure = None
        self._last_published_readiness = None
        self._transition("rediscovery_requested", f"{actor}: {reason}")
        if self._role == "follower":
            # A rediscovery is not a re-pairing; the leader installs the new
            # generation on the same route and republishes it.  The request is
            # retried until the leader answers or the attempt deadline passes,
            # so a dropped envelope or a leader restart cannot strand this side.
            self._rediscovery_request_id = str(uuid4())
            self._rediscovery_actor, self._rediscovery_reason = actor, reason
            self._rediscovery_started_at = self._now
            self._send_rediscovery_request()
            return
        self._rediscovery_started_at = self._now
        self._rediscovery_last_sent_at = self._now
        self._run_discovery(rediscovery=self._universe is not None)

    def _send_rediscovery_request(self) -> None:
        self._rediscovery_last_sent_at = self._now
        self._relay.send(
            self._envelope(
                "rediscovery_request",
                {
                    "request_id": self._rediscovery_request_id,
                    "actor": self._rediscovery_actor,
                    "reason": self._rediscovery_reason,
                },
            )
        )

    def _enforce_rediscovery_deadline(self) -> None:
        """Bound a rediscovery attempt on both sides: retry, then release.

        The follower resends its idempotent request; the leader re-publishes
        the candidate it is holding uncommitted.  Either way the attempt ends
        at the deadline with the previous ``universe_generation`` intact.
        """

        if not self._rediscovery_pending:
            return
        started = self._rediscovery_started_at
        if started is None:
            return
        if (self._now - started).total_seconds() >= REDISCOVERY_TIMEOUT_SECONDS:
            self._abandon_rediscovery(
                "the rediscovery attempt was not completed before its deadline"
            )
            return
        last_sent = self._rediscovery_last_sent_at
        if last_sent is not None and (self._now - last_sent).total_seconds() < REDISCOVERY_RETRY_SECONDS:
            return
        self._rediscovery_last_sent_at = self._now
        if self._role == "follower":
            if self._rediscovery_request_id is not None:
                # Safe to resend: the leader answers a repeated request_id from
                # its recorded answer instead of running discovery again.
                self._send_rediscovery_request()
        elif self._staged_universe is not None:
            self._relay.send(
                self._envelope("universe", {"universe": self._staged_universe.canonical()})
            )

    def _abandon_rediscovery(self, reason: str) -> None:
        """End an attempt that will never complete, and tell the peer once."""

        self._discard_staged_universe()
        self._pending_universe = None
        self._relay.send(
            self._envelope(
                "rediscovery_result",
                {
                    "ok": False,
                    "reason": reason,
                    "universe_generation": self.universe_generation(),
                    "request_id": self._rediscovery_request_id,
                },
            )
        )
        self._fail_rediscovery(reason)

    def _serve_rediscovery_request(self, payload: Mapping[str, object]) -> None:
        """Leader-only: answer one request, idempotently for a repeated ID."""

        if self._role != "leader":
            return
        request_id = payload.get("request_id")
        request_id = request_id if isinstance(request_id, str) and request_id else None
        if (
            request_id is not None
            and self._answered_rediscovery is not None
            and self._answered_rediscovery[0] == request_id
        ):
            kind, answer = self._answered_rediscovery[1]
            self._relay.send(self._envelope(kind, dict(answer)))
            self._transition("rediscovery_answer_replayed", request_id)
            return
        self._rediscovery_pending = True
        self._rediscovery_failure = None
        self._last_published_readiness = None
        self._transition(
            "rediscovery_requested", str(payload.get("actor") or self._peer_worker_id)
        )
        self._serving_rediscovery_request_id = request_id
        self._rediscovery_started_at = self._now
        self._rediscovery_last_sent_at = self._now
        try:
            self._run_discovery(rediscovery=self._universe is not None)
        finally:
            self._serving_rediscovery_request_id = None

    def _record_rediscovery_answer(self, kind: str, payload: dict[str, object]) -> None:
        if self._serving_rediscovery_request_id is not None:
            self._answered_rediscovery = (
                self._serving_rediscovery_request_id,
                (kind, dict(payload)),
            )

    def _fail_rediscovery(self, reason: str) -> None:
        """End one failed rediscovery attempt without stranding the pair.

        A failed rediscovery leaves the previous ``universe_generation`` in
        place and blocks entry only for the duration of that attempt.  Leaving
        the request pending forever would turn one refused operator action into
        a permanently unusable route.
        """

        self._rediscovery_pending = False
        self._rediscovery_failure = reason
        self._rediscovery_request_id = None
        self._rediscovery_started_at = None
        self._rediscovery_last_sent_at = None
        self._last_published_readiness = None
        self._transition("rediscovery_failed", reason)
        if self._universe is None:
            # Nothing was ever installed, so there is no previous generation to
            # fall back to and the pair stays fail-closed.
            self._discovery_reason = reason
        else:
            self._discovery_reason = ""

    def _accept_local_catalog(self, event: LocalCatalogEvent) -> None:
        entries = tuple(event.entries)
        if self._local_catalog is not None and _canonical_entries(
            self._local_catalog.entries
        ) == _canonical_entries(entries):
            return
        self._catalog_version += 1
        self._local_catalog = CatalogSummary(
            worker_id=self._worker_id,
            publisher_epoch=self._publisher_epoch,
            version=self._catalog_version,
            entries=entries,
        )
        self._publish_catalog_summary()
        self._maybe_discover()

    def _publish_catalog_summary(self) -> None:
        summary = self._local_catalog
        if summary is None or summary.hash == self._last_published_catalog_hash:
            return
        self._last_published_catalog_hash = summary.hash
        self._relay.send(self._envelope("catalog_summary", {"catalog": summary.canonical()}))

    def _accept_peer_catalog(self, payload: Mapping[str, object]) -> None:
        raw = payload.get("catalog")
        if not isinstance(raw, dict):
            return
        try:
            summary = _catalog_summary_from_canonical(raw)
        except (KeyError, TypeError, ValueError, PairExecutionCellError):
            self._transition("peer_catalog_refused", "catalog summary is malformed")
            return
        if summary.worker_id != self._peer_worker_id:
            return
        self._note_peer_publisher_epoch(summary.publisher_epoch)
        if self._peer_catalog is not None and summary.ordering <= self._peer_catalog.ordering:
            # `version` restarts whenever the peer is rebuilt, so a bare
            # version comparison would reject every re-seeded catalog.
            return
        self._peer_catalog = summary
        self._maybe_discover()

    def _catalogs_by_role(self) -> tuple[CatalogSummary, CatalogSummary] | None:
        """``(leader_catalog, follower_catalog)`` once both summaries exist."""

        if self._local_catalog is None or self._peer_catalog is None:
            return None
        if self._role == "leader":
            return self._local_catalog, self._peer_catalog
        return self._peer_catalog, self._local_catalog

    def _maybe_discover(self) -> None:
        if self._role == "leader":
            if self._universe is None or self._rediscovery_pending:
                self._run_discovery(rediscovery=self._universe is not None)
            return
        self._maybe_install_pending_universe()

    def _rediscovery_blocked(self) -> str | None:
        if not self._broker_verified_empty():
            return "this account is not broker-verified empty"
        if self._unresolved_local_work() is not None:
            return "this Worker has unresolved attempts or effects"
        if not self._peer_empty_claim:
            return "the peer account is not broker-verified empty"
        if not self._peer_nothing_unresolved:
            return "the peer has unresolved attempts or effects"
        return None

    def _run_discovery(self, *, rediscovery: bool) -> None:
        """Leader-only: run compatibility and install a ``universe_generation``.

        Discovery fails closed: an unavailable or invalid catalog, or an empty
        intersection, leaves the previous generation in place rather than
        installing an empty universe.  An *explicit* rediscovery is one bounded
        attempt: it either installs a new generation or terminates as a
        recorded failure, and never stays pending indefinitely.
        """

        if self._role != "leader":
            return
        catalogs = self._catalogs_by_role()
        if catalogs is None:
            reason = "a catalog summary is unavailable from one Worker"
            if rediscovery:
                self._finish_rediscovery(reason)
            else:
                self._note_discovery_failure(reason, "discovery_failed")
            return
        if rediscovery:
            blocked = self._rediscovery_blocked()
            if blocked is not None:
                self._finish_rediscovery(f"rediscovery refused: {blocked}")
                return
        leader_catalog, follower_catalog = catalogs
        outcome = discover_compatible_products(leader_catalog, follower_catalog)
        if not outcome.admitted:
            if rediscovery:
                self._finish_rediscovery(outcome.reason)
            else:
                self._note_discovery_failure(outcome.reason, "discovery_failed")
            return
        generation = self._next_universe_generation()
        universe = DiscoveredUniverse(
            route_id=self._route.route_id,
            universe_generation=generation,
            products=outcome.products,
            leader_catalog_hash=leader_catalog.hash,
            follower_catalog_hash=follower_catalog.hash,
        )
        answer = {"universe": universe.canonical()}
        self._record_rediscovery_answer("universe", answer)
        if rediscovery:
            # A rediscovery is only committed once the follower has verified and
            # acknowledged it.  Installing first would leave the pair split
            # across two generations whenever the follower refuses, while the
            # specification says a failed rediscovery leaves the previous one.
            self._staged_universe = universe
            self._transition("universe_staged", f"generation={generation}")
            self._relay.send(self._envelope("universe", answer))
            return
        self._install_universe(universe)
        self._rediscovery_pending = False
        self._rediscovery_failure = None
        self._relay.send(self._envelope("universe", answer))

    def _commit_staged_universe(self, generation: object, universe_hash: object) -> None:
        """Leader-only: install the candidate the follower just acknowledged."""

        staged = self._staged_universe
        if staged is None:
            return
        if generation != staged.universe_generation or universe_hash != staged.hash:
            # The follower acknowledged something other than the live candidate.
            self._transition("staged_universe_acknowledgement_ignored", str(generation))
            return
        self._staged_universe = None
        self._install_universe(staged)
        self._rediscovery_pending = False
        self._rediscovery_failure = None
        self._rediscovery_started_at = None
        self._rediscovery_last_sent_at = None

    def _discard_staged_universe(self) -> None:
        if self._staged_universe is None:
            return
        self._transition(
            "staged_universe_discarded", f"generation={self._staged_universe.universe_generation}"
        )
        self._staged_universe = None

    def _finish_rediscovery(self, reason: str) -> None:
        """Leader-only: record the failed attempt and release both sides."""

        self._fail_rediscovery(reason)
        answer: dict[str, object] = {
            "ok": False,
            "reason": reason,
            "universe_generation": self.universe_generation(),
            "request_id": self._serving_rediscovery_request_id,
        }
        self._record_rediscovery_answer("rediscovery_result", answer)
        self._relay.send(self._envelope("rediscovery_result", answer))

    def _publish_universe(self, universe: DiscoveredUniverse) -> None:
        self._relay.send(self._envelope("universe", {"universe": universe.canonical()}))

    def _note_discovery_failure(self, reason: str, event: str) -> None:
        """Fail closed without turning a bounded retry into a transition storm."""

        if self._discovery_reason == reason:
            return
        self._discovery_reason = reason
        self._last_published_readiness = None
        self._transition(event, reason)

    def _next_universe_generation(self) -> int:
        row = self._db.execute(
            "SELECT MAX(universe_generation) FROM cell_universe WHERE route_id = ?",
            (self._route.route_id,),
        ).fetchone()
        current = 0 if row is None or row[0] is None else int(row[0])
        if self._universe is not None:
            current = max(current, self._universe.universe_generation)
        return current + 1

    def _install_universe(self, universe: DiscoveredUniverse) -> None:
        self._db.execute(
            "INSERT INTO cell_universe (universe_generation, route_id, payload, installed_at)"
            " VALUES (?, ?, ?, ?) ON CONFLICT(universe_generation) DO UPDATE SET"
            " route_id = excluded.route_id, payload = excluded.payload,"
            " installed_at = excluded.installed_at",
            (
                universe.universe_generation,
                universe.route_id,
                _canonical_json(universe.canonical()),
                _iso(self._now),
            ),
        )
        self._db.commit()
        self._universe = universe
        self._discovery_reason = ""
        self._pending_universe = None
        # Product-scoped state is universe-scoped: plans, peer plans, and
        # decision-quote fencing all restart under the new generation.  Durable
        # quarantine is keyed by derived product identity and therefore
        # survives whenever a rediscovered product resolves to the same one.
        self._plans = {}
        self._plan_index = {}
        self._peer_plans = {}
        self._peer_plan_index = {}
        self._peer_plan_set_version = None
        self._peer_plan_batches = {}
        self._plans_refreshed_at = None
        self._last_plan_quote_set = frozenset()
        self._suspended_products = {}
        self._decided_quotes = {}
        self._local_quotes = {
            product_id: quote
            for product_id, quote in self._local_quotes.items()
            if universe.product(product_id) is not None
        }
        self._peer_quotes = {
            product_id: quote
            for product_id, quote in self._peer_quotes.items()
            if universe.product(product_id) is not None
        }
        self._recompute_plan_version()
        self._last_published_readiness = None
        self._transition(
            "universe_installed",
            f"generation={universe.universe_generation}; products={len(universe.products)}",
        )

    def _accept_universe(self, payload: Mapping[str, object]) -> None:
        """Follower-only: verify a published universe against its own catalog."""

        if self._role != "follower":
            return
        raw = payload.get("universe")
        if not isinstance(raw, dict):
            return
        try:
            universe = _universe_from_canonical(raw)
        except (KeyError, TypeError, ValueError, PairExecutionCellError) as error:
            self._discovery_reason = f"the published universe is malformed: {error}"
            self._transition("universe_refused", self._discovery_reason)
            return
        if universe.route_id != self._route.route_id:
            self._transition("universe_refused", "the published universe names another route")
            return
        if self._universe is not None and universe.universe_generation < self._universe.universe_generation:
            return
        if self._universe is not None and universe.universe_generation == self._universe.universe_generation:
            # Already installed; re-acknowledge so a lost ack cannot strand the
            # leader on a candidate it may never commit.
            self._acknowledge_universe(self._universe)
            return
        self._pending_universe = universe
        self._maybe_install_pending_universe()

    def _acknowledge_universe(self, universe: DiscoveredUniverse) -> None:
        self._relay.send(
            self._envelope(
                "rediscovery_result",
                {
                    "ok": True,
                    "reason": "",
                    "universe_generation": universe.universe_generation,
                    "universe_hash": universe.hash,
                    "request_id": self._rediscovery_request_id,
                },
            )
        )

    def _maybe_install_pending_universe(self) -> None:
        universe = self._pending_universe
        if universe is None or self._role != "follower":
            return
        catalogs = self._catalogs_by_role()
        if catalogs is None:
            # Not yet verifiable rather than refused: retry on the next tick,
            # bounded by the rediscovery attempt deadline.
            self._note_discovery_failure(
                "a catalog summary is unavailable from one Worker", "universe_unverifiable"
            )
            return
        leader_catalog, follower_catalog = catalogs
        outcome = discover_compatible_products(leader_catalog, follower_catalog)
        if not outcome.admitted:
            self._refuse_universe(outcome.reason)
            return
        if _canonical_json([p.canonical() for p in outcome.products]) != _canonical_json(
            [p.canonical() for p in universe.products]
        ):
            self._refuse_universe(
                "the published universe does not match this Worker's own compatibility result"
            )
            return
        self._install_universe(universe)
        self._acknowledge_universe(universe)
        self._rediscovery_pending = False
        self._rediscovery_failure = None
        self._rediscovery_request_id = None
        self._rediscovery_started_at = None
        self._rediscovery_last_sent_at = None

    def _refuse_universe(self, reason: str) -> None:
        """Follower-only: refuse a published universe and end the attempt.

        The previous ``universe_generation`` stays in force, the refusal is an
        explicit diagnostic on both sides, and the pending request is released
        rather than left waiting for a universe that will never verify.
        """

        request_id = self._rediscovery_request_id
        self._pending_universe = None
        self._transition("universe_refused", reason)
        self._relay.send(
            self._envelope(
                "rediscovery_result",
                {
                    "ok": False,
                    "reason": f"the follower refused the rediscovered universe: {reason}",
                    "universe_generation": self.universe_generation(),
                    "request_id": request_id,
                },
            )
        )
        self._fail_rediscovery(reason)

    def _accept_rediscovery_result(self, payload: Mapping[str, object]) -> None:
        """Record the far side's verdict on a rediscovery attempt.

        Never answered with another message, so a verdict cannot ping-pong.
        """

        if payload.get("ok") is True:
            self._commit_staged_universe(
                payload.get("universe_generation"), payload.get("universe_hash")
            )
            return
        if payload.get("ok") is not False:
            return
        request_id = payload.get("request_id")
        if (
            self._rediscovery_pending
            and self._rediscovery_request_id is not None
            and isinstance(request_id, str)
            and request_id != self._rediscovery_request_id
        ):
            self._transition("stale_rediscovery_result_ignored", request_id)
            return
        # The candidate is abandoned and the previous generation stays in force.
        self._discard_staged_universe()
        reason = payload.get("reason")
        self._fail_rediscovery(str(reason or "the peer refused the rediscovery"))

    # -- observable projections ------------------------------------------- #

    def sizing_plans(self) -> dict[tuple[str, Direction], SizingPlan]:
        return dict(self._plans)

    def retained_plan_versions(self) -> tuple[str, ...]:
        """Superseded plan versions still valid for validating a dispatched attempt."""

        current = {plan.version for plan in self._plans.values()}
        return tuple(
            sorted(
                version
                for version in self._plan_index
                if version not in current and self._retained_plan(version) is not None
            )
        )

    def plan_set_version(self) -> str:
        return self._plan_set_version

    def eligible_products(self) -> tuple[str, ...]:
        return tuple(sorted({product_id for product_id, _ in self._plans}))

    def suspended_products(self) -> dict[str, str]:
        return dict(self._suspended_products)

    def remaining_allowance_summary(self) -> RemainingAllowanceSummary:
        return RemainingAllowanceSummary(
            worker_id=self._worker_id,
            publisher_epoch=self._publisher_epoch,
            version=self._allowance_version,
            allowed_leg_loss_usd=str(self.allowed_leg_loss_usd()),
        )

    def peer_remaining_allowance(self) -> RemainingAllowanceSummary | None:
        return self._peer_allowance

    def allowed_leg_loss_usd(self) -> Decimal:
        """This leg's ceiling from the accepted canonical policy and local realized loss."""

        limits = self._limits
        if limits is None or self._risk_configuration_drift() is not None:
            return Decimal(0)
        remaining = max(Decimal(0), limits.daily_loss_limit_usd - self.accumulated_realized_loss_usd())
        return min(limits.leg_loss_cap_usd, remaining)

    def accumulated_realized_loss_usd(self) -> Decimal:
        row = self._db.execute(
            "SELECT amount_usd FROM cell_realized_pnl WHERE ny_day = ?", (_ny_day(self._now).isoformat(),)
        ).fetchall()
        net = sum((cast(Decimal, _to_decimal(value[0])) for value in row), Decimal(0))
        return max(Decimal(0), -net)

    def quarantined_products(self) -> tuple[str, ...]:
        rows = self._db.execute("SELECT product_id FROM cell_product_quarantine ORDER BY product_id").fetchall()
        return tuple(str(row[0]) for row in rows)

    def symbol_wide_quarantines(self) -> tuple[str, ...]:
        """Quarantine keys that are bare symbols rather than derived identities.

        A revision before derived product identity keyed quarantine by local
        symbol.  Migration renames the column but cannot re-derive an identity,
        because that needs both catalogs and they may be unavailable.  Such a
        key therefore covers *every* discovered product on that symbol until an
        authenticated operator releases it -- the conservative direction.
        """

        return tuple(
            key for key in self.quarantined_products() if not is_derived_product_identity(key)
        )

    def quarantine_reason(self, product_id: str, *, symbol: str | None = None) -> str | None:
        """The single predicate every quarantine gate uses.

        Ranking, attempt admission, and any caller share this one answer so the
        exact and symbol-wide cases can never diverge between them.
        """

        recorded = self.quarantined_products()
        if product_id in recorded:
            return "product is quarantined"
        covered = symbol if symbol is not None else product_identity_symbol(product_id)
        if covered in recorded and not is_derived_product_identity(covered):
            return f"a symbol-wide quarantine on {covered} covers this product"
        return None

    def is_quarantined(self, product_id: str, *, symbol: str | None = None) -> bool:
        return self.quarantine_reason(product_id, symbol=symbol) is not None

    def quarantine_release_marker(self, product_id: str) -> str | None:
        return self._release_marker(product_id)

    def quarantine_generation(self, product_id: str) -> int | None:
        row = self._db.execute(
            "SELECT universe_generation FROM cell_product_quarantine WHERE product_id = ?",
            (product_id,),
        ).fetchone()
        return None if row is None or row[0] is None else int(row[0])

    # -- canonical risk configuration -------------------------------------- #

    @property
    def _limits(self) -> WorkerRiskLimits | None:
        """This Worker's enforced limits: always the accepted canonical copy."""

        if self._policy is None or not self._policy_accepted:
            return None
        return self._policy.risk_of(self._role)

    def _risk_configuration_drift(self) -> str | None:
        """Why the local declaration and the accepted canonical copy disagree."""

        limits = self._limits
        if limits is None:
            return None
        if limits.matches(self._declared_limits):
            return None
        return "the local risk configuration does not match the accepted canonical policy"

    def enforced_risk_limits(self) -> WorkerRiskLimits | None:
        return self._limits

    # -- typed events ------------------------------------------------------ #

    def handle_event(self, event: PairCellEvent) -> PairResult:
        if isinstance(event, ClockTickEvent):
            self._now = max(self._now, _as_utc(event.now))
        elif isinstance(event, ReadinessFactsEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
            self._local_facts = event
        elif isinstance(event, BrokerSnapshotEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, PeerSessionEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, RealizedPnLEvent):
            self._now = max(self._now, _as_utc(event.closed_at))
        elif isinstance(event, QuarantineReleaseEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, RouteMetadataEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, RediscoveryRequestEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, LocalCatalogEvent):
            self._now = max(self._now, _as_utc(event.observed_at))
        elif isinstance(event, LocalQuoteEvent):
            self._now = max(self._now, _as_utc(event.local_receive_time))
        elif isinstance(event, LocalQuoteBatchEvent) and event.quotes:
            self._now = max(self._now, max(_as_utc(q.local_receive_time) for q in event.quotes))

        if isinstance(event, LocalQuoteEvent):
            self._accept_local_quote(event)
        elif isinstance(event, LocalQuoteBatchEvent):
            accepted = tuple(q for q in event.quotes if self._accept_local_quote(q, publish=False))
            if accepted:
                self._publish_quote_batch(
                    tuple(self._local_quotes[q.product_id] for q in accepted if q.product_id in self._local_quotes)
                )
        elif isinstance(event, LocalCatalogEvent):
            self._accept_local_catalog(event)
        elif isinstance(event, RouteMetadataEvent):
            self._apply_route_metadata(event)
        elif isinstance(event, RediscoveryRequestEvent):
            self._handle_rediscovery_request(event.actor, event.reason)
        elif isinstance(event, PeerSessionEvent):
            connected_before = self._peer_session
            self._peer_session = event.connected
            if not event.connected:
                self._peer_ready = False
                self._peer_ready_reason = "authenticated peer session lost"
                # Fail closed: the peer may be rebuilt behind this outage, so
                # its last published allowance stops being usable evidence.
                self._peer_allowance = None
                self._transition("peer_session_lost")
            elif not connected_before:
                # The peer may have been rebuilt while it was gone, and may not
                # have observed this transition at all; ask it to re-seed.
                self._request_reseed("peer session reconnected", ask_peer=True)
        elif isinstance(event, RealizedPnLEvent):
            self._record_realized_pnl(event)
        elif isinstance(event, BrokerSnapshotEvent):
            self._apply_broker_snapshot(event)
        elif isinstance(event, QuarantineReleaseEvent):
            self._release_product_quarantine(event)
        elif isinstance(event, RelayEnvelopeReceived):
            self._accept_relay_envelope(event.envelope)

        self._progress()
        return self._result()

    # -- realized loss (America/New_York calendar day) --------------------- #

    def _record_realized_pnl(self, event: RealizedPnLEvent) -> None:
        amount = _to_decimal(event.realized_usd)
        if amount is None:
            return
        event_id = event.event_id or f"{event.attempt_id}:realized"
        self._db.execute(
            "INSERT OR IGNORE INTO cell_realized_pnl (event_id, ny_day, amount_usd, recorded_at)"
            " VALUES (?, ?, ?, ?)",
            (event_id, _ny_day(event.closed_at).isoformat(), str(amount), _iso(self._now)),
        )
        self._db.commit()
        self._transition("realized_pnl_recorded", f"{event_id}={amount}")

    # -- sizing and emergency-protection plans ------------------------------ #

    def _quoted_products(self) -> frozenset[str]:
        """Discovered products with a current local quote, the plan precondition."""

        policy = self._policy
        if policy is None or self._universe is None:
            return frozenset()
        return frozenset(
            product.product_id
            for product in self._universe.products
            if (quote := self._local_quotes.get(product.product_id)) is not None
            and quote.fresh(self._now, policy.quote_max_age_seconds)
        )

    def _refresh_due(self) -> bool:
        # Plans cannot exist before the route, discovery, and policy do; quote
        # collection running ahead of plan construction is data gathering only.
        if self._universe is None or self._limits is None:
            return False
        if self._risk_configuration_drift() is not None:
            return False
        quoted = self._quoted_products()
        if self._plans_refreshed_at is None:
            return bool(quoted)
        missing = quoted - {product_id for product_id, _ in self._plans}
        if missing and quoted != self._last_plan_quote_set:
            return True
        return (self._now - self._plans_refreshed_at).total_seconds() >= _SIZING_REFRESH_SECONDS

    def _refresh_sizing_plans(self) -> None:
        """Rebuild every discovered product/direction plan; suspend failures.

        The frozen universe is revalidated, never expanded: a symbol that
        appeared in a broker catalog after discovery is not admitted here, and
        this never changes ``universe_generation``.
        """

        universe = self._universe
        limits = self._limits
        assert universe is not None and limits is not None  # guarded by _refresh_due
        policy = cast(StrategyPolicy, self._policy)
        catalog = {entry.name: entry for entry in (self._local_catalog.entries if self._local_catalog else ())}
        plans: dict[tuple[str, Direction], SizingPlan] = {}
        suspended: dict[str, str] = {}
        quoted = self._quoted_products()
        for product in universe.products:
            product_id = product.product_id
            drift = _compatibility_drift(catalog.get(product.symbol), product)
            if drift is not None:
                suspended[product_id] = drift
                continue
            quote = self._local_quotes.get(product_id)
            if product_id not in quoted or quote is None:
                suspended[product_id] = "no current local quote exists for this product"
                continue
            facts = self._read_local_product_facts(product.symbol)
            if facts is None:
                suspended[product_id] = "local symbol economics are unavailable"
                continue
            if not facts.tradable:
                suspended[product_id] = "product is not currently tradable"
                continue
            volume_min = cast(Decimal, _to_decimal(product.volume_min))
            volume_step = cast(Decimal, _to_decimal(product.volume_step))
            common_max = cast(Decimal, _to_decimal(product.volume_max))
            local_max = _to_decimal(facts.volume_max)
            point = _to_decimal(facts.point)
            tick_size = _to_decimal(facts.tick_size)
            loss_tick_value = _to_decimal(facts.loss_tick_value)
            profit_tick_value = _to_decimal(facts.profit_tick_value)
            stop_level = _to_decimal(facts.stop_level_points)
            freeze_level = _to_decimal(facts.freeze_level_points)
            if (
                local_max is None or local_max <= 0
                or point is None or point <= 0
                or tick_size is None or tick_size <= 0
                or loss_tick_value is None or loss_tick_value <= 0
                or profit_tick_value is None or profit_tick_value <= 0
                or stop_level is None or freeze_level is None
            ):
                suspended[product_id] = "specification is missing usable economics"
                continue
            volume_max = min(common_max, local_max)
            minimum_stop_distance = max(stop_level, freeze_level) * point
            usd_per_point_per_lot = loss_tick_value * (point / tick_size)
            directions_built = 0
            for direction in sorted(product.directions):
                typed = cast(Direction, direction)
                price = quote.ask if typed == "LONG" else quote.bid
                margin_per_lot = self._margin_per_lot(product.symbol, typed, price)
                if margin_per_lot is None or margin_per_lot <= 0:
                    continue
                local_max_lots = floor_to_volume_step(
                    limits.margin_budget_usd / margin_per_lot, volume_min, volume_max, volume_step
                )
                if local_max_lots is None:
                    continue
                plan = SizingPlan(
                    universe_generation=universe.universe_generation,
                    product_id=product_id,
                    symbol=product.symbol,
                    direction=typed,
                    margin_per_lot=str(margin_per_lot),
                    local_max_lots=str(local_max_lots),
                    canonical_point=product.canonical_point,
                    point=facts.point,
                    tick_size=facts.tick_size,
                    profit_tick_value=facts.profit_tick_value,
                    loss_tick_value=facts.loss_tick_value,
                    volume_min=product.volume_min,
                    volume_step=product.volume_step,
                    volume_max=decimal_text(volume_max) or str(volume_max),
                    filling_mode=product.filling_mode,
                    minimum_stop_distance=str(minimum_stop_distance),
                    usd_per_point_per_lot=str(usd_per_point_per_lot),
                )
                plans[(product_id, typed)] = plan
                directions_built += 1
            if directions_built == 0:
                suspended[product_id] = "no direction has a valid positive sizing plan"
        self._install_plans(plans, suspended, policy)

    def _install_plans(
        self,
        plans: dict[tuple[str, Direction], SizingPlan],
        suspended: dict[str, str],
        policy: StrategyPolicy,
    ) -> None:
        """Install the new plan set and retain the versions it supersedes.

        A refresh never deletes the version it supersedes: each retained
        version stays valid for validating an *already-dispatched* attempt for
        the confirmation timeout plus the bounded relay-handling window, and is
        never eligible for a new candidate.
        """

        generation = cast(DiscoveredUniverse, self._universe).universe_generation
        new_versions = {plan.version for plan in plans.values()}
        self._db.execute(
            "UPDATE cell_sizing_plans SET superseded_at = ?"
            " WHERE universe_generation = ? AND superseded_at IS NULL"
            " AND plan_version NOT IN (%s)"
            % (",".join("?" for _ in new_versions) or "''"),
            (_iso(self._now), generation, *sorted(new_versions)),
        )
        for plan in plans.values():
            self._db.execute(
                "INSERT INTO cell_sizing_plans (plan_version, universe_generation, product_id,"
                " direction, payload, installed_at, superseded_at) VALUES (?, ?, ?, ?, ?, ?, NULL)"
                " ON CONFLICT(plan_version) DO UPDATE SET superseded_at = NULL",
                (
                    plan.version,
                    generation,
                    plan.product_id,
                    plan.direction,
                    _canonical_json(plan.canonical()),
                    _iso(self._now),
                ),
            )
            self._plan_index[plan.version] = plan
        self._db.commit()
        self._plans, self._suspended_products = plans, suspended
        self._plans_refreshed_at = self._now
        self._last_plan_quote_set = self._quoted_products()
        self._purge_expired_plan_versions(policy)
        self._recompute_plan_version()
        self._transition("sizing_plans_refreshed", self._plan_set_version)

    def _purge_expired_plan_versions(self, policy: StrategyPolicy) -> None:
        """Retention is bounded: a superseded version expires once unreferenced."""

        referenced = {
            self._attempt.plan_version_of(self._role) if self._attempt is not None else "",
        }
        cutoff = self._now - timedelta(seconds=policy.plan_retention_seconds)
        expired: list[str] = []
        for version, superseded in self._db.execute(
            "SELECT plan_version, superseded_at FROM cell_sizing_plans WHERE superseded_at IS NOT NULL"
        ).fetchall():
            if str(version) in referenced:
                continue
            if _parse_utc(str(superseded)) <= cutoff:
                expired.append(str(version))
        for version in expired:
            self._db.execute("DELETE FROM cell_sizing_plans WHERE plan_version = ?", (version,))
            self._plan_index.pop(version, None)
        if expired:
            self._db.commit()

    def _retained_plan(self, version: str) -> SizingPlan | None:
        """A superseded version still inside its retention window, or ``None``."""

        policy = self._policy
        if policy is None:
            return None
        row = self._db.execute(
            "SELECT payload, superseded_at FROM cell_sizing_plans WHERE plan_version = ?", (version,)
        ).fetchone()
        if row is None or row[1] is None:
            return None
        if (self._now - _parse_utc(str(row[1]))).total_seconds() > policy.plan_retention_seconds:
            return None
        return _plan_from_canonical(json.loads(row[0]))

    def _plan_for_validation(
        self, product_id: str, direction: Direction, version: str
    ) -> SizingPlan | None:
        """The current plan for this leg, or a retained superseded version."""

        current = self._plans.get((product_id, direction))
        if current is not None and current.version == version:
            return current
        retained = self._retained_plan(version)
        if retained is None:
            return None
        if retained.product_id != product_id or retained.direction != direction:
            return None
        return retained

    def _margin_per_lot(self, symbol: str, direction: Direction, price: Decimal) -> Decimal | None:
        try:
            raw = self._mt5.order_calc_margin(
                {"direction": direction, "symbol": symbol, "volume": "1", "price": str(price)}
            )
        except Exception:
            return None
        if isinstance(raw, Mapping):
            raw = raw.get("margin")
        return _to_decimal(raw)

    def _read_local_product_facts(self, symbol: str) -> LocalProductFacts | None:
        try:
            raw = self._mt5.symbol_info(symbol)
        except Exception:
            return None
        evidence = _mapping(raw)
        if evidence is None:
            return None
        point = _positive_text(evidence.get("point"))
        tick_size = _positive_text(evidence.get("trade_tick_size"))
        volume_max = _positive_text(evidence.get("volume_max"))
        tick_value = evidence.get("trade_tick_value")
        profit_tick_value = _positive_text(evidence.get("trade_tick_value_profit")) or _positive_text(tick_value)
        loss_tick_value = _positive_text(evidence.get("trade_tick_value_loss")) or _positive_text(tick_value)
        stops_level = decimal_text(evidence.get("trade_stops_level", 0))
        freeze_level = decimal_text(evidence.get("trade_freeze_level", 0))
        if (
            point is None or tick_size is None or volume_max is None
            or profit_tick_value is None or loss_tick_value is None
            or stops_level is None or freeze_level is None
        ):
            return None
        return LocalProductFacts(
            point=point,
            tick_size=tick_size,
            profit_tick_value=profit_tick_value,
            loss_tick_value=loss_tick_value,
            stop_level_points=stops_level,
            freeze_level_points=freeze_level,
            volume_max=volume_max,
            tradable=evidence.get("visible") is not False,
        )

    def _recompute_plan_version(self, *, persist: bool = True) -> None:
        canonical = [plan.canonical() for _, plan in sorted(self._plans.items())]
        version = _hash(_canonical_json(canonical))[:16]
        if version == self._plan_set_version:
            return
        self._plan_set_version = version
        if not persist:
            return
        self._db.execute(
            "INSERT INTO cell_plan_version (id, plan_set_version, refreshed_at) VALUES (1, ?, ?)"
            " ON CONFLICT(id) DO UPDATE SET plan_set_version = excluded.plan_set_version,"
            " refreshed_at = excluded.refreshed_at",
            (version, _iso(self._now)),
        )
        self._db.commit()

    def _pair_lots(self, product_id: str, direction: Direction) -> tuple[Decimal, SizingPlan, SizingPlan] | None:
        """The common lots, and both plans, for one product/direction pair."""

        local_direction = direction if self._role == "leader" else _mirror(direction)
        peer_direction = _mirror(local_direction)
        leader_plan = self._plans.get((product_id, local_direction)) if self._role == "leader" else self._peer_plans.get(
            (product_id, peer_direction)
        )
        follower_plan = self._peer_plans.get((product_id, peer_direction)) if self._role == "leader" else self._plans.get(
            (product_id, local_direction)
        )
        if leader_plan is None or follower_plan is None:
            return None
        return _combine_pair_lots(leader_plan, follower_plan)


    # -- readiness --------------------------------------------------------- #

    def _local_ready(self) -> tuple[bool, str]:
        if self._needs_human is not None:
            return False, "operator intervention is required"
        if self._recovering:
            return False, "recovering durable state after restart"
        if self._metadata_reconciliation is not None:
            return False, f"route metadata reconciliation: {self._metadata_reconciliation}"
        if self._route_state == "UNPAIRING":
            return False, "the route is UNPAIRING"
        if not self._peer_session:
            return False, "authenticated peer session lost"
        if self._universe is None:
            return False, f"no discovered universe: {self._discovery_reason}"
        if self._rediscovery_pending:
            # Entry stops for the duration of the attempt only; a failed
            # rediscovery releases this and leaves the previous generation.
            return False, "an explicit rediscovery is in progress"
        if self._policy_refusal is not None:
            return False, self._policy_refusal
        if self._policy is None or not self._policy_accepted:
            return False, "no canonical strategy policy is accepted"
        drift = self._risk_configuration_drift()
        if drift is not None:
            return False, drift
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
        try:
            unresolved = self._journal.unresolved()
        except EffectJournalError:
            return False, "effect journal is unhealthy"
        if unresolved:
            return False, "effect journal has unresolved effects"
        if not self._plans:
            return False, "no discovered product has a valid sizing plan"
        if self.allowed_leg_loss_usd() <= 0:
            return False, "the New York daily realized-loss allowance is exhausted"
        if self._attempt is not None and self._state not in ("EMPTY", "IDLE"):
            return False, "an attempt is unresolved"
        return True, "ready"

    def _broker_verified_empty(self) -> bool:
        facts = self._local_facts
        if facts is None or facts.orders is None or facts.positions is None:
            return False
        return not facts.orders and not facts.positions and facts.terminal_connected

    def _policy_change_blocked(self) -> str | None:
        """Policy content may only change while both accounts are proven empty."""

        if self._attempt is not None and self._state not in ("EMPTY", "IDLE"):
            return "an attempt is unresolved"
        if not self._broker_verified_empty():
            return "this account is not broker-verified empty"
        if self._role == "leader" and not self._peer_empty_claim:
            return "the peer account is not broker-verified empty"
        return None

    def _unowned_exposure_reason(
        self, orders: list[dict[str, object]], positions: list[dict[str, object]]
    ) -> str | None:
        """Any exposure that is not this attempt's exact owned leg revokes readiness."""

        if not orders and not positions:
            return None
        leg = self._leg
        if orders:
            return "existing exposure present"
        if leg is None or self._attempt is None or leg.ticket is None or leg.empty_verified:
            return "existing exposure present"
        if leg.entry_status != "filled":
            return "existing exposure present"
        if {str(position.get("ticket")) for position in positions} != {leg.ticket}:
            return "existing exposure present"
        return None

    # -- quotes ------------------------------------------------------------ #

    def _accept_local_quote(self, event: LocalQuoteEvent, *, publish: bool = True) -> bool:
        if self._universe is not None and self._universe.product(event.product_id) is None:
            return False
        existing = self._local_quotes.get(event.product_id)
        if existing is not None:
            if existing.recovery_epoch == event.recovery_epoch and event.sequence <= existing.sequence:
                return False
            if self._is_previous_epoch(event.product_id, event.recovery_epoch):
                return False
        self._note_epoch(event.product_id, event.recovery_epoch)
        quote = QuoteSnapshot(
            worker_id=self._worker_id,
            product_id=event.product_id,
            symbol=event.symbol,
            bid=event.bid,
            ask=event.ask,
            broker_time=_as_utc(event.broker_time),
            local_receive_time=_as_utc(event.local_receive_time),
            recovery_epoch=event.recovery_epoch,
            sequence=event.sequence,
            calibration=event.calibration,
        )
        self._local_quotes[event.product_id] = quote
        if publish:
            self._publish_quote(quote)
        return True

    def _note_epoch(self, product_id: str, epoch: str) -> None:
        seen = self._quote_epochs.setdefault(product_id, [])
        if epoch not in seen:
            seen.append(epoch)

    def _is_previous_epoch(self, product_id: str, epoch: str) -> bool:
        seen = self._quote_epochs.get(product_id, [])
        return epoch in seen and seen[-1] != epoch

    def _accept_peer_quote(self, payload: Mapping[str, object]) -> None:
        product_id = payload.get("product_id")
        symbol = payload.get("symbol")
        if payload.get("worker_id") != self._peer_worker_id or not isinstance(product_id, str):
            return
        if not isinstance(symbol, str):
            return
        if self._universe is not None and self._universe.product(product_id) is None:
            return
        bid, ask = _to_decimal(payload.get("bid")), _to_decimal(payload.get("ask"))
        broker_time_raw, sequence = payload.get("broker_time"), payload.get("sequence")
        epoch = payload.get("recovery_epoch")
        if (
            bid is None or ask is None
            or not isinstance(broker_time_raw, str)
            or not isinstance(epoch, str) or not epoch
            or isinstance(sequence, bool) or not isinstance(sequence, int)
        ):
            return
        try:
            broker_time = _parse_utc(broker_time_raw)
        except (ValueError, PairExecutionCellError):
            return
        existing = self._peer_quotes.get(product_id)
        if existing is not None and existing.recovery_epoch == epoch and sequence <= existing.sequence:
            return
        self._peer_quotes[product_id] = QuoteSnapshot(
            worker_id=self._peer_worker_id,
            product_id=product_id,
            symbol=symbol,
            bid=bid,
            ask=ask,
            broker_time=broker_time,
            local_receive_time=self._now,
            recovery_epoch=epoch,
            sequence=sequence,
            calibration=_calibration_from_canonical(payload.get("calibration")),
        )

    # -- relay publication -------------------------------------------------- #

    def _envelope(self, kind: str, payload: dict[str, object]) -> dict[str, object]:
        """Every relay envelope carries ``route_id``; the controller validates
        only that, the sending role, protocol version, and payload size."""

        return {
            "protocol_version": PROTOCOL_VERSION,
            "kind": kind,
            "route_id": self._route.route_id,
            "from_worker_id": self._worker_id,
            "from_role": self._role,
            "to_worker_id": self._peer_worker_id,
            "payload": payload,
        }

    def _publish_policy(self) -> None:
        if self._policy is None:
            return
        self._relay.send(
            self._envelope(
                "policy",
                {
                    "policy": self._policy.canonical(),
                    "policy_hash": self._policy.hash,
                    "leader_broker_verified_empty": self._broker_verified_empty(),
                },
            )
        )

    def _publish_quote(self, quote: QuoteSnapshot) -> None:
        self._relay.send(self._envelope("quote", quote.canonical()))

    def _publish_quote_batch(self, quotes: tuple[QuoteSnapshot, ...]) -> None:
        if not quotes:
            return
        self._relay.send(
            self._envelope("quote_batch", {"quotes": [quote.canonical() for quote in quotes]})
        )

    def _publish_remaining_allowance(self) -> None:
        """Refreshed as realized daily loss changes, always before any signal."""

        current = str(self.allowed_leg_loss_usd())
        if current == self._published_allowance:
            return
        self._published_allowance = current
        self._allowance_version += 1
        self._relay.send(
            self._envelope(
                "remaining_allowance", self.remaining_allowance_summary().canonical()
            )
        )

    def _maybe_publish_state(self) -> None:
        if self._reseed_requested:
            self._reseed_requested = False
            self._republish_peer_state()
        if self._reseed_ask_nonce is not None:
            nonce, self._reseed_ask_nonce = self._reseed_ask_nonce, None
            self._relay.send(
                self._envelope(
                    "reseed_request",
                    {"nonce": nonce, "reason": "this Worker observed a relay transition"},
                )
            )
        self._publish_catalog_summary()
        self._publish_remaining_allowance()
        if self._plan_set_version != self._last_published_plan_version:
            self._last_published_plan_version = self._plan_set_version
            self._publish_plan_batches()
        ready, reason = self._local_ready()
        snapshot = (
            ready,
            reason,
            self._plan_set_version,
            None if self._policy is None else self._policy.hash,
            self._policy_accepted,
            self._broker_verified_empty(),
            self._unresolved_local_work() is None,
            self._route_state,
            self.universe_generation(),
            self.quarantined_products(),
        )
        if snapshot == self._last_published_readiness:
            return
        self._last_published_readiness = snapshot
        self._relay.send(
            self._envelope(
                "readiness",
                {
                    "ready": ready,
                    "reason": reason,
                    "plan_set_version": self._plan_set_version,
                    "policy_hash": None if self._policy is None else self._policy.hash,
                    "policy_accepted": self._policy_accepted,
                    "broker_verified_empty": self._broker_verified_empty(),
                    "nothing_unresolved": self._unresolved_local_work() is None,
                    "route_state": self._route_state,
                    "universe_generation": self.universe_generation(),
                    "product_quarantines": self._product_quarantine_records(),
                },
            )
        )

    def _publish_plan_batches(self) -> None:
        canonical = [plan.canonical() for _, plan in sorted(self._plans.items())]
        batches = _bounded_batches(canonical)
        for index, batch in enumerate(batches):
            self._relay.send(
                self._envelope(
                    "sizing_plans",
                    {
                        "plan_set_version": self._plan_set_version,
                        "universe_generation": self.universe_generation(),
                        "batch_index": index,
                        "batch_count": len(batches),
                        "plans": batch,
                    },
                )
            )

    def _accept_peer_plan_batch(self, payload: Mapping[str, object]) -> None:
        version = payload.get("plan_set_version")
        batch_index = payload.get("batch_index")
        batch_count = payload.get("batch_count")
        plans = payload.get("plans")
        generation = payload.get("universe_generation")
        if generation != self.universe_generation():
            return  # sizing-plan state is universe-scoped
        if (
            not isinstance(version, str)
            or isinstance(batch_index, bool) or not isinstance(batch_index, int)
            or isinstance(batch_count, bool) or not isinstance(batch_count, int)
            or batch_count <= 0 or not 0 <= batch_index < batch_count
            or not isinstance(plans, list)
        ):
            return
        batches = self._peer_plan_batches.setdefault(version, {})
        batches[batch_index] = list(plans)
        if len(batches) != batch_count:
            return
        combined: list[object] = []
        for index in range(batch_count):
            combined.extend(batches[index])
        if _hash(_canonical_json(combined))[:16] != version:
            self._peer_plan_batches.pop(version, None)
            return
        parsed: dict[tuple[str, Direction], SizingPlan] = {}
        for raw in combined:
            if not isinstance(raw, dict):
                continue
            try:
                plan = _plan_from_canonical(raw)
            except (KeyError, TypeError):
                continue
            parsed[(plan.product_id, plan.direction)] = plan
            self._peer_plan_index[plan.version] = plan
        self._peer_plans = parsed
        self._peer_plan_set_version = version
        self._peer_plan_batches = {version: batches}
        self._persist_peer_plans(parsed, version, cast(int, generation))

    def _persist_peer_plans(
        self, plans: dict[tuple[str, Direction], SizingPlan], plan_set_version: str, generation: int
    ) -> None:
        """A restarted Worker must still know the peer plan versions it agreed on."""

        self._db.execute(
            "UPDATE cell_peer_sizing_plans SET current = 0 WHERE universe_generation = ?",
            (generation,),
        )
        for plan in plans.values():
            self._db.execute(
                "INSERT INTO cell_peer_sizing_plans (plan_version, universe_generation, product_id,"
                " direction, payload, plan_set_version, current, recorded_at)"
                " VALUES (?, ?, ?, ?, ?, ?, 1, ?)"
                " ON CONFLICT(plan_version) DO UPDATE SET current = 1,"
                " plan_set_version = excluded.plan_set_version, recorded_at = excluded.recorded_at",
                (
                    plan.version,
                    generation,
                    plan.product_id,
                    plan.direction,
                    _canonical_json(plan.canonical()),
                    plan_set_version,
                    _iso(self._now),
                ),
            )
        self._db.commit()

    def _accept_peer_allowance(self, payload: Mapping[str, object]) -> None:
        worker_id = payload.get("worker_id")
        epoch = payload.get("publisher_epoch")
        version = payload.get("version")
        allowed = payload.get("allowed_leg_loss_usd")
        if worker_id != self._peer_worker_id:
            return
        if isinstance(epoch, bool) or not isinstance(epoch, int) or epoch <= 0:
            return
        if isinstance(version, bool) or not isinstance(version, int) or version <= 0:
            return
        if not isinstance(allowed, str) or _to_decimal(allowed) is None:
            return
        self._note_peer_publisher_epoch(epoch)
        if self._peer_allowance is not None and (epoch, version) <= self._peer_allowance.ordering:
            # A rebuilt peer restarts `version` at 1; ordering by
            # `(publisher_epoch, version)` is what stops this Worker from
            # keeping the peer's stale -- possibly larger -- allowance.
            return
        self._peer_allowance = RemainingAllowanceSummary(
            worker_id=worker_id,
            publisher_epoch=epoch,
            version=version,
            allowed_leg_loss_usd=allowed,
        )

    # -- relay envelope handling -------------------------------------------- #

    def _accept_relay_envelope(self, envelope: Mapping[str, object]) -> None:
        if envelope.get("protocol_version") != PROTOCOL_VERSION:
            return
        if envelope.get("route_id") != self._route.route_id:
            self._transition("envelope_off_route", str(envelope.get("route_id")))
            return
        if envelope.get("from_worker_id") != self._peer_worker_id:
            return
        if envelope.get("to_worker_id") != self._worker_id:
            return
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            return
        kind = envelope.get("kind")
        if kind == "quote":
            self._accept_peer_quote(payload)
        elif kind == "quote_batch":
            raw_quotes = payload.get("quotes")
            if isinstance(raw_quotes, list):
                for quote in raw_quotes:
                    if isinstance(quote, dict):
                        self._accept_peer_quote(quote)
        elif kind == "catalog_summary":
            self._accept_peer_catalog(payload)
        elif kind == "universe":
            self._accept_universe(payload)
        elif kind == "rediscovery_request":
            self._serve_rediscovery_request(payload)
        elif kind == "rediscovery_result":
            self._accept_rediscovery_result(payload)
        elif kind == "reseed_request":
            self._accept_reseed_request(payload)
        elif kind == "pairing_acceptance":
            self._accept_pairing_acceptance(payload)
        elif kind == "remaining_allowance":
            self._accept_peer_allowance(payload)
        elif kind == "sizing_plans":
            self._accept_peer_plan_batch(payload)
        elif kind == "readiness":
            self._accept_peer_readiness(payload)
        elif kind == "policy":
            self._accept_leader_policy(payload)
        elif kind == "policy_ack":
            self._accept_policy_ack(payload)
        elif kind == "attempt":
            self._handle_attempt(payload)
        elif kind == "leg_status":
            self._handle_peer_leg_status(payload)
        elif kind == "leg_status_request":
            self._handle_leg_status_request(payload)
        elif kind == "pair_entry_confirmed":
            self._handle_pair_entry_confirmed(payload)
        elif kind == "pair_entry_failed":
            self._handle_pair_entry_failed(payload)

    def _accept_peer_readiness(self, payload: Mapping[str, object]) -> None:
        ready, reason = payload.get("ready"), payload.get("reason")
        if isinstance(ready, bool) and isinstance(reason, str):
            self._peer_ready, self._peer_ready_reason = ready, reason
        peer_hash = payload.get("policy_hash")
        accepted = payload.get("policy_accepted")
        if isinstance(peer_hash, str) and accepted is True:
            self._peer_policy_hash = peer_hash
        elif accepted is False:
            self._peer_policy_hash = None
        empty = payload.get("broker_verified_empty")
        if isinstance(empty, bool):
            self._peer_empty_claim = empty
        unresolved = payload.get("nothing_unresolved")
        if isinstance(unresolved, bool):
            self._peer_nothing_unresolved = unresolved
        self._accept_peer_product_quarantines(payload.get("product_quarantines"))

    def _accept_leader_policy(self, payload: Mapping[str, object]) -> None:
        """Follower-only: durably accept the exact leader policy while empty."""

        if self._role != "follower":
            return
        raw = payload.get("policy")
        declared = payload.get("policy_hash")
        if not isinstance(raw, dict) or not isinstance(declared, str):
            return
        try:
            policy = _policy_from_canonical(raw)
        except (KeyError, TypeError, PairExecutionCellError):
            self._send_policy_ack(str(declared), False, "policy payload is malformed")
            return
        if policy.hash != declared:
            self._send_policy_ack(declared, False, "policy hash does not match its content")
            return
        if policy.mode == "live" and not self._allow_live:
            # A refusal, never an override: this Worker stays paired, safe, and
            # idle rather than silently running shadow against a live leader.
            reason = "allow_live is false and the canonical policy mode is live"
            self._policy_refusal = reason
            self._last_published_readiness = None
            self._transition("policy_refused", reason)
            self._send_policy_ack(policy.hash, False, reason)
            return
        if self._policy is not None and self._policy.hash == policy.hash:
            self._send_policy_ack(policy.hash, True, "policy already durably accepted")
            return
        acceptance = self._local_acceptance
        if acceptance is None:
            self._send_policy_ack(
                policy.hash, False, "this Worker has no frozen Pairing Acceptance payload"
            )
            return
        if not policy.follower_risk.is_verbatim_copy_of(acceptance.risk_limits()):
            self._send_policy_ack(
                policy.hash,
                False,
                "follower_risk is not a verbatim copy of this Worker's Pairing Acceptance payload",
            )
            return
        blocked = self._policy_change_blocked()
        if blocked is not None:
            self._send_policy_ack(policy.hash, False, blocked)
            return
        if payload.get("leader_broker_verified_empty") is not True:
            self._send_policy_ack(policy.hash, False, "the leader account is not broker-verified empty")
            return
        self._policy = policy
        self._policy_accepted = True
        self._policy_refusal = None
        self._persist_policy(policy)
        self._plans_refreshed_at = None
        self._transition("policy_accepted", policy.hash)
        self._last_published_readiness = None
        self._send_policy_ack(policy.hash, True, "accepted")

    def _send_policy_ack(self, policy_hash: str, accepted: bool, reason: str) -> None:
        self._relay.send(
            self._envelope(
                "policy_ack", {"policy_hash": policy_hash, "accepted": accepted, "reason": reason}
            )
        )

    def _accept_policy_ack(self, payload: Mapping[str, object]) -> None:
        if self._role != "leader":
            return
        policy_hash = payload.get("policy_hash")
        if payload.get("accepted") is True and isinstance(policy_hash, str):
            self._peer_policy_hash = policy_hash
        else:
            self._peer_policy_hash = None
            reason = payload.get("reason")
            if isinstance(reason, str) and reason:
                self._peer_ready = False
                self._peer_ready_reason = f"peer refused the canonical policy: {reason}"

    # -- progress ----------------------------------------------------------- #

    def _progress(self) -> None:
        if self._needs_human is not None:
            self._state = "NEEDS_HUMAN"
            self._maybe_finalize_empty()
            if self._needs_human is not None:
                self._await_peer_terminal_proof()
            # A stopped cell may not trade or advance an attempt, but peers
            # still need its current fail-closed readiness to avoid waiting
            # indefinitely for a state snapshot after reconnect or restart.
            self._maybe_publish_state()
            return
        if self._role == "follower":
            self._maybe_install_pending_universe()
        self._enforce_rediscovery_deadline()
        if self._refresh_due():
            self._refresh_sizing_plans()
        self._enforce_exit_policy()
        self._enforce_timers()
        self._maybe_publish_state()
        if (
            self._role == "leader"
            and self._route_state == "ACTIVE"
            and self._attempt is None
            and self._state in ("IDLE", "EMPTY")
        ):
            self._maybe_create_attempt()
        self._maybe_emit_pair_confirmation()
        self._maybe_apply_precise_protection()
        self._maybe_progress_convergence()
        self._maybe_finalize_empty()
        self._await_peer_terminal_proof()
        self._maybe_declare_active()

    def _enforce_exit_policy(self) -> None:
        policy = self._policy
        if policy is None or self._attempt is None or self._desired == "EMPTY":
            return
        if self._flatten_reached():
            self._begin_close("flatten_at_ny")
            return
        if (
            self._active_since is not None
            and policy.maximum_holding_seconds is not None
            and self._now >= self._active_since + timedelta(seconds=policy.maximum_holding_seconds)
        ):
            self._begin_close("maximum_holding_seconds")

    def _flatten_reached(self) -> bool:
        policy = self._policy
        if policy is None or policy.flatten_at_ny is None:
            return False
        cutoff = _parse_ny_time(policy.flatten_at_ny)
        local = self._now.astimezone(NEW_YORK)
        return local.time() >= cutoff

    def _enforce_timers(self) -> None:
        """One independent local monotonic timer per role; never a shared deadline."""

        if self._timer_deadline is None or self._attempt is None or self._leg is None:
            return
        if self._pair_confirmed or self._desired == "EMPTY":
            return
        if self._monotonic() < self._timer_deadline:
            return
        self._timer_deadline = None
        if self._role == "leader":
            self._transition("leader_confirmation_timeout", self._attempt.attempt_id)
            self._notify_entry_failed("leader confirmation timer expired without exact evidence for both legs")
            self._begin_close("leader_confirmation_timeout")
        else:
            self._transition("follower_confirmation_timeout", self._attempt.attempt_id)
            self._begin_close("follower_confirmation_timeout")


    # -- leader: candidate admission and ranking ----------------------------- #

    def _candidates(self) -> list[tuple[Decimal, Decimal, str, Direction]]:
        """Leader-side admitted candidates ordered by the specification's keys.

        No candidate is evaluated, ranked, or selected while any valid positive
        plan is missing, and none at all while the route is ``UNPAIRING``.
        """

        policy = self._policy
        if policy is None or self._universe is None or self._route_state == "UNPAIRING":
            return []
        threshold = cast(Decimal, _to_decimal(policy.entry_edge_points))
        ranked: list[tuple[Decimal, Decimal, str, Direction]] = []
        for product_id in sorted({key[0] for key in self._plans}):
            product = self._universe.product(product_id)
            if product is None:
                continue
            if self.is_quarantined(product_id, symbol=product.symbol):
                continue
            local_quote = self._local_quotes.get(product_id)
            peer_quote = self._peer_quotes.get(product_id)
            if local_quote is None or peer_quote is None:
                continue
            if not local_quote.fresh(self._now, policy.quote_max_age_seconds):
                continue
            if not peer_quote.fresh(self._now, policy.quote_max_age_seconds):
                continue
            if calibrated_skew_seconds(local_quote, peer_quote) > policy.quote_max_skew_seconds:
                continue
            if self._decided_quotes.get(product_id) == _quote_key(local_quote, peer_quote):
                continue  # one attempt per decision quote revision; never a retry storm
            for leader_direction in ("LONG", "SHORT"):
                direction = cast(Direction, leader_direction)
                sized = self._pair_lots(product_id, direction)
                if sized is None:
                    continue
                lots, leader_plan, follower_plan = sized
                if local_quote.symbol != leader_plan.symbol or peer_quote.symbol != follower_plan.symbol:
                    continue
                # The canonical execution point is the coarser broker's, so the
                # edge threshold is always measured on the conservative one.
                point = _to_decimal(leader_plan.canonical_point)
                if point is None or point <= 0:
                    continue
                raw_edge = edge_value(local_quote, peer_quote, direction)
                edge_points = raw_edge / point
                if edge_points < threshold:
                    continue
                conservative_usd_per_point = min(
                    cast(Decimal, _to_decimal(leader_plan.usd_per_point_per_lot)),
                    cast(Decimal, _to_decimal(follower_plan.usd_per_point_per_lot)),
                )
                expected_edge_usd = edge_points * conservative_usd_per_point * lots
                ranked.append((expected_edge_usd, edge_points, product_id, direction))
        ranked.sort(key=lambda item: (-item[0], -item[1], item[2], item[3]))
        return ranked

    def entry_candidates(self) -> list[dict[str, object]]:
        """Observable, deterministically ranked leader candidate admission."""

        return [
            {
                "expected_edge_usd": str(edge_usd),
                "edge_points": str(points),
                "product_id": product_id,
                "leader_direction": direction,
            }
            for edge_usd, points, product_id, direction in self._candidates()
        ]

    def entry_admission_diagnostic(self) -> str:
        """Return the current, most specific reason a leader cannot enter.

        This deliberately describes only durable safety and admission gates;
        it never includes account credentials, order payloads, or raw broker
        records.  The Worker emits it at debug level for ``abt-worker -v``.
        """

        if self._role != "leader":
            return "this Worker is the follower; only the leader creates entries"
        ready, reason = self._local_ready()
        if not ready:
            return f"local admission blocked: {reason}"
        if not self._peer_ready:
            return f"peer admission blocked: {self._peer_ready_reason or 'no current peer readiness'}"
        if not self._peer_session:
            return "peer admission blocked: authenticated peer session lost"
        policy = self._policy
        if policy is None or self._peer_policy_hash != policy.hash:
            return "peer admission blocked: canonical policy acknowledgement is not current"
        if self._peer_allowance is None:
            return "peer admission blocked: no current remaining-loss allowance"
        if self._peer_allowance.publisher_epoch < self._peer_publisher_epoch:
            return "peer admission blocked: remaining-loss allowance has not been reseeded"
        candidates = self._candidates()
        if not candidates:
            return (
                "no entry candidate passed sizing, quote freshness/skew, quarantine, "
                f"and edge threshold gates (entry_edge_points={policy.entry_edge_points})"
            )
        candidate = candidates[0]
        return (
            f"entry candidate selected: product_id={candidate[2]} direction={candidate[3]} "
            f"edge_points={candidate[1]} expected_edge_usd={candidate[0]}"
        )

    # -- leader: immediate entry -------------------------------------------- #

    def _maybe_create_attempt(self) -> None:
        policy = self._policy
        universe = self._universe
        if policy is None or universe is None or self._role != "leader":
            return
        ready, _ = self._local_ready()
        if not ready or not self._peer_ready or not self._peer_session:
            return
        if self._peer_policy_hash != policy.hash:
            return
        peer_allowance = self._peer_allowance
        if peer_allowance is None:
            return  # a current peer summary is required before an attempt exists
        if peer_allowance.publisher_epoch < self._peer_publisher_epoch:
            # The peer was rebuilt after this summary; it has not re-seeded yet.
            return
        candidates = self._candidates()
        if not candidates:
            return
        _, _, product_id, leader_direction = candidates[0]
        sized = self._pair_lots(product_id, leader_direction)
        if sized is None:
            return
        lots, leader_plan, follower_plan = sized
        leader_quote = self._local_quotes[product_id]
        follower_quote = self._peer_quotes[product_id]
        self._decided_quotes[product_id] = _quote_key(leader_quote, follower_quote)
        follower_direction = _mirror(leader_direction)
        leader_allowed = self.allowed_leg_loss_usd()
        # A leg is never assigned more loss than its owner's last published
        # remaining allowance; the canonical policy still bounds it, so a peer
        # can never claim more than both sides agreed to.
        follower_allowed = min(
            cast(Decimal, _to_decimal(peer_allowance.allowed_leg_loss_usd)),
            policy.risk_of("follower").leg_loss_cap_usd,
        )
        if leader_allowed <= 0 or follower_allowed <= 0:
            return
        leader_entry = leader_quote.ask if leader_direction == "LONG" else leader_quote.bid
        follower_entry = follower_quote.ask if follower_direction == "LONG" else follower_quote.bid
        leader_rough = compute_protection(
            entry=leader_entry,
            direction=leader_direction,
            volume=lots,
            plan=leader_plan,
            allowed_loss_usd=leader_allowed,
        )
        follower_rough = compute_protection(
            entry=follower_entry,
            direction=follower_direction,
            volume=lots,
            plan=follower_plan,
            allowed_loss_usd=follower_allowed,
        )
        if leader_rough is None or follower_rough is None:
            self._transition("attempt_rejected", "no valid rough emergency protection could be constructed")
            return
        attempt = Attempt(
            attempt_id=str(uuid4()),
            policy_hash=policy.hash,
            route_id=self._route.route_id,
            universe_generation=universe.universe_generation,
            product_id=product_id,
            symbol=leader_plan.symbol,
            leader_direction=leader_direction,
            follower_direction=follower_direction,
            lots=str(lots),
            leader_quote=leader_quote,
            follower_quote=follower_quote,
            leader_plan_version=leader_plan.version,
            follower_plan_version=follower_plan.version,
            leader_allowance_epoch=self._publisher_epoch,
            follower_allowance_epoch=peer_allowance.publisher_epoch,
            leader_allowance_version=self._allowance_version,
            follower_allowance_version=peer_allowance.version,
            leader_allowed_loss_usd=str(leader_allowed),
            follower_allowed_loss_usd=str(follower_allowed),
            leader_rough_sl=leader_rough[0],
            leader_rough_tp=leader_rough[1],
            follower_rough_sl=follower_rough[0],
            follower_rough_tp=follower_rough[1],
            filling_mode=leader_plan.filling_mode,
            decision_time=self._now,
            confirmation_timeout_seconds=policy.follower_confirmation_timeout_seconds,
        )
        _LOGGER.debug(
            "Pair Execution Cell entry signal selected: attempt_id=%s product_id=%s symbol=%s "
            "direction=%s lots=%s edge_points=%s local_quote_age_ms=%.1f peer_quote_age_ms=%.1f "
            "quote_skew_ms=%.1f decision_at=%s.",
            attempt.attempt_id,
            product_id,
            attempt.symbol,
            leader_direction,
            lots,
            candidates[0][1],
            leader_quote.age_seconds(self._now) * 1000,
            follower_quote.age_seconds(self._now) * 1000,
            calibrated_skew_seconds(leader_quote, follower_quote) * 1000,
            _iso(self._now),
        )
        # 1. durably persist the immutable attempt and the prepared local effect
        self._attempt = attempt
        self._persist_attempt(attempt)
        self._record_timing("attempt_persisted")
        leg = LegState(attempt_id=attempt.attempt_id, role="leader")
        leg.rough_sl, leg.rough_tp = leader_rough
        self._leg = leg
        self._desired = "ACTIVE"
        self._persist_desired()
        self._state = "ENTERING"
        self._transition("attempt_created", attempt.attempt_id)
        effect_id = self._entry_effect_id(attempt)
        leg.entry_effect_id = effect_id
        # The leg row names the effect before the effect exists, so a prepared
        # or send_started effect can always be attributed after a restart.
        self._persist_leg()
        prepared = self._prepare_entry_effect(effect_id, attempt, "leader")
        if not prepared:
            return
        self._persist_leg()
        # 2. start the local monotonic follower-confirmation timer at the decision
        self._start_timer(attempt)
        # 3. durably admit that the peer may receive this attempt, *before* it can
        leg.attempt_relayed = True
        self._persist_leg()
        self._transition("attempt_relay_intent", attempt.attempt_id)
        # 4. concurrently relay one immutable attempt and start the local send
        self._relay.send(self._envelope("attempt", {"attempt": attempt.canonical()}))
        self._record_timing("attempt_dispatched")
        self._send_entry(attempt, "leader")

    def _start_timer(self, attempt: Attempt) -> None:
        assert self._leg is not None
        self._timer_deadline = self._monotonic() + attempt.confirmation_timeout_seconds
        self._leg.timer_started_at = _iso(self._now)
        self._persist_leg()
        self._record_timing(
            "leader_timer_started" if self._role == "leader" else "follower_timer_started"
        )

    def _entry_effect_id(self, attempt: Attempt) -> str:
        return f"{attempt.attempt_id}:{self._worker_id}:entry"

    def _prepare_entry_effect(self, effect_id: str, attempt: Attempt, role: Role) -> bool:
        if self._policy is not None and self._policy.mode == "shadow":
            return True
        try:
            self._journal.prepare(effect_id, self._entry_payload(attempt, role))
        except EffectJournalError as error:
            leg = self._leg
            if leg is not None:
                leg.entry_status = "rejected"
                self._persist_leg()
            self._transition("entry_rejected_preflight", str(error))
            self._report_leg_status("rejected", reason=str(error))
            self._begin_close("entry_effect_prepare_failed")
            return False
        return True

    def _entry_payload(self, attempt: Attempt, role: Role) -> dict[str, object]:
        rough_sl, rough_tp = attempt.rough_of(role)
        return {
            "type": "market",
            "symbol": attempt.symbol_of(role),
            "volume": attempt.lots,
            "direction": attempt.direction_of(role),
            "filling_mode": attempt.filling_mode,
            "sl": rough_sl,
            "tp": rough_tp,
        }

    # -- follower: non-market admission and immediate entry ------------------ #

    def _handle_attempt(self, payload: Mapping[str, object]) -> None:
        if self._role != "follower":
            return
        raw = payload.get("attempt")
        if not isinstance(raw, dict):
            return
        try:
            attempt = _attempt_from_canonical(raw)
        except (KeyError, TypeError, ValueError, PairExecutionCellError):
            self._transition("attempt_rejected", "attempt payload is malformed")
            return
        row = self._db.execute(
            "SELECT payload_hash FROM cell_attempts WHERE attempt_id = ?", (attempt.attempt_id,)
        ).fetchone()
        if row is not None:
            if row[0] != attempt.payload_hash:
                self._transition("attempt_rejected", "attempt ID reused with different content")
                self._report_leg_status(
                    "rejected", attempt_id=attempt.attempt_id, reason="attempt ID reused with different content"
                )
                return
            # Durable same-content delivery replays the durable outcome and
            # never reaches the broker a second time.
            self._transition("attempt_duplicate_ignored", attempt.attempt_id)
            self._replay_leg_status(attempt)
            return
        reason = self._attempt_admission_reason(attempt)
        if reason is not None:
            # Persist the refusal terminally: it is this Worker's durable proof
            # that no broker exposure was ever created for this attempt ID.
            self._persist_rejected_attempt(attempt, reason)
            self._transition("attempt_rejected", reason)
            self._report_leg_status("rejected", attempt_id=attempt.attempt_id, reason=reason)
            return
        self._attempt = attempt
        self._persist_attempt(attempt)
        self._record_timing("attempt_persisted")
        leg = LegState(attempt_id=attempt.attempt_id, role="follower")
        leg.rough_sl, leg.rough_tp = attempt.rough_of("follower")
        self._leg = leg
        self._desired = "ACTIVE"
        self._persist_desired()
        self._state = "ENTERING"
        self._transition("attempt_admitted", attempt.attempt_id)
        effect_id = self._entry_effect_id(attempt)
        leg.entry_effect_id = effect_id
        self._persist_leg()
        prepared = self._prepare_entry_effect(effect_id, attempt, "follower")
        if not prepared:
            return
        self._persist_leg()
        self._start_timer(attempt)
        self._send_entry(attempt, "follower")

    def _attempt_admission_reason(self, attempt: Attempt) -> str | None:
        """Minimum non-market safety checks only.

        The follower never inspects current quotes, quote age, quote skew, or
        edge before entry, and never re-derives the decision.  The
        remaining-allowance re-check is local risk arithmetic, not market
        evaluation, and adds no market-data dependency.
        """

        if attempt.route_id != self._route.route_id:
            return "the attempt names another route"
        if self._route_state == "UNPAIRING":
            return "the route is UNPAIRING"
        if self._metadata_reconciliation is not None:
            return f"route metadata reconciliation: {self._metadata_reconciliation}"
        if self._universe is None:
            return "no discovered universe is installed"
        if attempt.universe_generation != self._universe.universe_generation:
            return "the attempt names another universe_generation"
        if self._policy is None or not self._policy_accepted:
            return self._policy_refusal or "no canonical strategy policy is accepted"
        if attempt.policy_hash != self._policy.hash:
            return "policy hash is not the exact locally accepted hash"
        if self._attempt is not None and self._state not in ("EMPTY", "IDLE"):
            return "an earlier attempt is still unresolved"
        quarantined = self.quarantine_reason(attempt.product_id, symbol=attempt.symbol)
        if quarantined is not None:
            return quarantined
        ready, ready_reason = self._local_ready()
        if not ready:
            return f"follower not ready: {ready_reason}"
        if not self._broker_verified_empty():
            return "follower is not broker-verified empty"
        product = self._universe.product(attempt.product_id)
        if product is None:
            return "selected product identity is not locally eligible"
        plan = self._plan_for_validation(
            attempt.product_id, attempt.follower_direction, attempt.follower_plan_version
        )
        if plan is None:
            if self._plans.get((attempt.product_id, attempt.follower_direction)) is None:
                return "selected product or direction is not locally eligible"
            return "sizing-plan version is neither current nor a retained superseded version"
        if plan.symbol != attempt.symbol:
            return "local symbol does not match the attempt"
        allowed = _to_decimal(attempt.allowed_loss_of("follower"))
        if allowed is None or allowed <= 0:
            return "the attempt assigns no usable allowed leg loss"
        if allowed > self.allowed_leg_loss_usd():
            return "the assigned leg loss exceeds this Worker's current local remaining allowance"
        peer_plan = self._peer_plan_index.get(attempt.leader_plan_version)
        if peer_plan is None:
            return "no cached leader plan exists for the named sizing-plan version"
        sized = _combine_pair_lots(peer_plan, plan)
        if sized is None or sized[0] != _to_decimal(attempt.lots):
            return "common lots differ from the pair plan of the named plan version"
        if not self._rough_protection_attachable(attempt, plan):
            return "valid rough emergency protection cannot be attached to the initial order"
        return None

    def _rough_protection_attachable(self, attempt: Attempt, plan: SizingPlan) -> bool:
        sl = _to_decimal(attempt.follower_rough_sl)
        tp = _to_decimal(attempt.follower_rough_tp)
        minimum = _to_decimal(plan.minimum_stop_distance)
        quote = attempt.follower_quote
        entry = quote.ask if attempt.follower_direction == "LONG" else quote.bid
        if sl is None or tp is None or minimum is None or sl <= 0 or tp <= 0:
            return False
        if attempt.follower_direction == "LONG":
            if not sl < entry < tp:
                return False
        elif not tp < entry < sl:
            return False
        return abs(entry - sl) >= minimum and abs(tp - entry) >= minimum

    def _persist_rejected_attempt(self, attempt: Attempt, reason: str) -> None:
        self._db.execute(
            "INSERT OR IGNORE INTO cell_attempts (attempt_id, payload_hash, payload, created_at,"
            " route_id, terminal, terminal_reason) VALUES (?, ?, ?, ?, ?, 1, ?)",
            (
                attempt.attempt_id,
                attempt.payload_hash,
                _canonical_json(attempt.canonical()),
                _iso(self._now),
                attempt.route_id,
                f"admission_rejected: {reason}",
            ),
        )
        self._db.commit()

    def _replay_leg_status(self, attempt: Attempt) -> None:
        """Answer a same-content duplicate from the durable outcome only."""

        leg = self._leg
        if leg is not None and leg.attempt_id == attempt.attempt_id:
            self._report_current_leg_status()
            return
        row = self._db.execute(
            "SELECT terminal, terminal_reason FROM cell_attempts WHERE attempt_id = ?",
            (attempt.attempt_id,),
        ).fetchone()
        if row is not None and row[0]:
            reason = str(row[1] or "")
            self._report_leg_status(
                _terminal_status_for(reason), attempt_id=attempt.attempt_id, reason=reason
            )

    # -- entry execution ----------------------------------------------------- #

    def _send_entry(self, attempt: Attempt, role: Role) -> None:
        leg = self._leg
        assert leg is not None
        # Durably admit that MT5 may be called before it can be, so a crash
        # inside the irreversible path never recovers as "nothing happened".
        leg.entry_dispatched = True
        self._persist_leg()
        payload = self._entry_payload(attempt, role)
        effect_id = leg.entry_effect_id or self._entry_effect_id(attempt)
        if self._policy is not None and self._policy.mode == "shadow":
            quote = attempt.quote_of(role)
            direction = attempt.direction_of(role)
            leg.entry_status = "filled"
            leg.ticket = f"shadow:{attempt.attempt_id}:{self._worker_id}"
            leg.fill_price = str(quote.ask if direction == "LONG" else quote.bid)
            leg.observed_volume = attempt.lots
            leg.observed_sl, leg.observed_tp = attempt.rough_of(role)
            self._persist_leg()
            self._state = "AWAITING_PAIR_CONFIRMATION"
            self._transition("shadow_entry_simulated", attempt.attempt_id)
            self._report_leg_status(
                "filled",
                ticket=leg.ticket,
                fill_price=leg.fill_price,
                sl=leg.observed_sl,
                tp=leg.observed_tp,
            )
            return
        outcome = self._run_broker_write(effect_id, payload, priority="execution")
        if outcome.category == "rejected":
            leg.entry_status = "rejected"
            self._persist_leg()
            self._transition("entry_rejected", attempt.attempt_id)
            retcode = outcome.receipt.get("retcode") if outcome.receipt is not None else None
            if retcode == PRICE_OFF_RETCODE:
                self._quarantine_product(
                    attempt.product_id,
                    offending_worker_id=self._worker_id,
                    attempt_id=attempt.attempt_id,
                    receipt=outcome.receipt or {"retcode": PRICE_OFF_RETCODE},
                    local_evidence=True,
                )
            self._report_leg_status("rejected", receipt_retcode=retcode)
            self._notify_entry_failed("the local entry was rejected by the broker")
            self._begin_close("entry_rejected")
        elif outcome.category == "expired_not_started":
            leg.entry_status = "expired_not_started"
            self._persist_leg()
            self._transition("entry_expired_not_started", attempt.attempt_id)
            self._report_leg_status("expired_not_started")
            self._notify_entry_failed("the local entry never started")
            self._begin_close("entry_expired_not_started")
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

    def _record_effect_observation(self, effect_id: str | None, observation: dict[str, object]) -> None:
        """Conclusively resolve a ``prepared``/``send_started`` effect.

        The journal is the Worker's readiness gate: an effect left unresolved
        blocks every future entry.  Broker observation (or proof the MT5 call
        never began) is exactly the evidence ADR-0008 asks for, and recording
        it never resends anything.
        """

        if not effect_id:
            return
        try:
            if not any(entry["effect_id"] == effect_id for entry in self._journal.unresolved()):
                return
            self._journal.record_observation(effect_id, observation)
        except EffectJournalError as error:
            self._transition("effect_observation_refused", f"{effect_id}: {error}")
            return
        self._transition("effect_resolved_by_observation", effect_id)
        self._bump_state_version()

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
            return self._not_started(effect_id, admitted.category, admitted.reason)
        scheduled = self._dequeue_own_item(effect_id)
        if isinstance(scheduled, TraderRpcOutcome):
            return self._not_started(effect_id, scheduled.category, scheduled.reason)
        if scheduled is None:
            return BrokerWriteResult("unknown_after_send")
        try:
            scheduled.before_broker_send()
        except BrokerActionNotStarted as error:
            return self._not_started(effect_id, error.outcome.category, error.outcome.reason)
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
            except Exception as error:  # the irreversible boundary was crossed
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
            if receipt.get("retcode") not in _SUCCESS_RETCODES:
                return BrokerWriteResult("rejected", receipt)
            return BrokerWriteResult("completed", receipt)
        finally:
            scheduled.complete(category, reason)

    def _not_started(self, effect_id: str, category: str, reason: str) -> BrokerWriteResult:
        """A terminal outcome decided before MT5 was ever called."""

        self._record_effect_observation(
            effect_id,
            {"observed": "not_started", "category": category, "reason": reason, "at": _iso(self._now)},
        )
        return BrokerWriteResult(_map_scheduler_category(category))

    def _dequeue_own_item(self, effect_id: str) -> ScheduledTraderRpc | TraderRpcOutcome | None:
        """Drain the shared Worker scheduler until *this* admitted item surfaces."""

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
        if self._attempt is None:
            return
        try:
            positions = self._mt5.positions_get()
            orders = self._mt5.orders_get()
        except Exception:
            return  # a transient read failure never establishes any fact
        self._apply_broker_snapshot(
            BrokerSnapshotEvent(
                orders=_records(orders), positions=_records(positions), observed_at=self._now
            )
        )

    # -- durable recovery of the entry effect --------------------------------- #

    def _unresolved_effect_state(self, effect_id: str) -> str | None:
        try:
            for entry in self._journal.unresolved():
                if entry.get("effect_id") == effect_id:
                    return str(entry.get("state"))
        except EffectJournalError:
            return None
        return None

    def _reconcile_entry_effect_state(self) -> None:
        """Derive a restart-safe send state for the entry effect.

        A crash between ``prepare`` and this cell's own return leaves the
        durable leg at ``pending`` even though MT5 may already hold a real
        position.  The effect journal -- not the leg -- is the authority on
        whether the irreversible boundary may have been crossed, so a
        ``prepared`` effect is provably unsent while a ``send_started`` (or
        already-evidenced) effect must be resolved from broker observation.
        Nothing is ever resent on either path.
        """

        leg = self._leg
        if leg is None or leg.entry_status != "pending" or leg.entry_effect_id is None:
            return
        effect_id = leg.entry_effect_id
        state = self._unresolved_effect_state(effect_id)
        if state is None:
            try:
                self._journal.evidence(effect_id)
            except EffectJournalError:
                self._mark_entry_never_started(leg, effect_id, record_observation=False)
                return
            self._mark_entry_may_have_sent(leg, effect_id)
            return
        if state == "prepared":
            self._mark_entry_never_started(leg, effect_id, record_observation=True)
            return
        self._mark_entry_may_have_sent(leg, effect_id)

    def _mark_entry_never_started(self, leg: LegState, effect_id: str, *, record_observation: bool) -> None:
        leg.entry_status = "expired_not_started"
        self._persist_leg()
        if record_observation:
            self._record_effect_observation(
                effect_id,
                {
                    "observed": "not_started",
                    "category": "prepared_never_sent",
                    "reason": "the MT5 send boundary was never crossed before the restart",
                    "at": _iso(self._now),
                },
            )
        self._transition("entry_effect_never_started", effect_id)

    def _mark_entry_may_have_sent(self, leg: LegState, effect_id: str) -> None:
        leg.entry_status = "unknown_after_send"
        leg.entry_dispatched = True
        self._persist_leg()
        self._transition("entry_effect_may_have_sent", effect_id)


    # -- broker fact application --------------------------------------------- #

    def _apply_broker_snapshot(self, event: BrokerSnapshotEvent) -> None:
        if self._attempt is None or self._leg is None:
            return
        if event.orders is None or event.positions is None:
            return  # None/unavailable never establishes any fact
        self._reconcile_entry_effect_state()
        attempt, leg = self._attempt, self._leg
        if leg is None:
            return
        role = leg.role
        precise_already = leg.protection_status == "precise"
        symbol = attempt.symbol_of(role)
        direction = attempt.direction_of(role)
        matches = [p for p in event.positions if _position_matches(p, symbol, direction, attempt.lots)]
        if leg.entry_status in ("sent", "unknown_after_send") and len(matches) > 1:
            self._transition("ambiguous_position_attribution", attempt.attempt_id)
            self._begin_close("ambiguous_position_attribution")
        elif leg.entry_status in ("sent", "unknown_after_send") and matches:
            position = matches[0]
            leg.ticket = str(position.get("ticket"))
            fill = _to_decimal(position.get("price_open", position.get("price")))
            leg.fill_price = None if fill is None else str(fill)
            observed_volume = _to_decimal(position.get("volume"))
            leg.observed_volume = None if observed_volume is None else str(observed_volume)
            observed_sl, observed_tp = _to_decimal(position.get("sl")), _to_decimal(position.get("tp"))
            leg.observed_sl = None if observed_sl is None else str(observed_sl)
            leg.observed_tp = None if observed_tp is None else str(observed_tp)
            leg.entry_status = "filled"
            self._persist_leg()
            self._record_effect_observation(
                leg.entry_effect_id,
                {
                    "observed": "position",
                    "ticket": leg.ticket,
                    "symbol": symbol,
                    "side": direction,
                    "volume": leg.observed_volume,
                    "price_open": leg.fill_price,
                    "sl": leg.observed_sl,
                    "tp": leg.observed_tp,
                    "at": _iso(event.observed_at),
                },
            )
            self._state = "AWAITING_PAIR_CONFIRMATION"
            self._transition("entry_filled_observed", leg.ticket)
            self._record_timing("position_observed")
            _LOGGER.debug(
                "Pair Execution Cell entry position observed: attempt_id=%s ticket=%s symbol=%s "
                "side=%s volume=%s fill_price=%s broker_entry_time=%s broker_entry_time_msc=%s "
                "observation_at=%s.",
                attempt.attempt_id,
                leg.ticket,
                symbol,
                direction,
                leg.observed_volume,
                leg.fill_price,
                position.get("time", "-"),
                position.get("time_msc", "-"),
                _iso(event.observed_at),
            )
            inconsistent = self._own_evidence_inconsistency()
            if inconsistent is not None:
                self._transition("inconsistent_exact_evidence", inconsistent)
                self._report_leg_status("inconsistent", reason=inconsistent)
                self._notify_entry_failed(inconsistent)
                self._begin_close("inconsistent_exact_evidence")
            else:
                self._report_leg_status(
                    "filled",
                    ticket=leg.ticket,
                    fill_price=leg.fill_price,
                    sl=leg.observed_sl,
                    tp=leg.observed_tp,
                )
        elif leg.entry_status == "unknown_after_send" and not matches:
            leg.entry_status = "rejected"
            self._persist_leg()
            self._record_effect_observation(
                leg.entry_effect_id,
                {
                    "observed": "absent",
                    "symbol": symbol,
                    "side": direction,
                    "volume": attempt.lots,
                    "at": _iso(event.observed_at),
                },
            )
            self._transition("unknown_after_send_resolved_absent", attempt.attempt_id)
            self._report_leg_status("rejected", reason="fresh observation proves no owned position exists")
            self._notify_entry_failed("the local entry could not be observed")
            self._begin_close("unknown_after_send_resolved_absent")
        if leg.ticket is not None:
            owned = next((p for p in event.positions if str(p.get("ticket")) == leg.ticket), None)
            if owned is not None:
                # Any in-flight protection or close effect for this exact ticket
                # is now conclusively observable.
                self._record_effect_observation(
                    leg.protection_effect_id,
                    {
                        "observed": "position",
                        "ticket": leg.ticket,
                        "sl": None if owned.get("sl") is None else str(_to_decimal(owned.get("sl"))),
                        "tp": None if owned.get("tp") is None else str(_to_decimal(owned.get("tp"))),
                        "at": _iso(event.observed_at),
                    },
                )
            else:
                for effect_id in (leg.protection_effect_id, leg.close_effect_id):
                    self._record_effect_observation(
                        effect_id,
                        {"observed": "absent", "ticket": leg.ticket, "at": _iso(event.observed_at)},
                    )
            if owned is None and leg.entry_status == "filled" and not leg.empty_verified:
                self._begin_close("owned_ticket_disappeared")
            elif owned is not None and precise_already:
                observed_sl, observed_tp = _to_decimal(owned.get("sl")), _to_decimal(owned.get("tp"))
                expected_sl, expected_tp = _to_decimal(leg.precise_sl), _to_decimal(leg.precise_tp)
                plan = self._attempt_plan_for_role(attempt, leg.role)
                if (
                    plan is None
                    or not _same_protection_price(expected_sl, observed_sl, plan.tick_size)
                    or not _same_protection_price(expected_tp, observed_tp, plan.tick_size)
                ):
                    self._transition("integrity_divergence", "protection mismatch")
                    self._begin_close("integrity_divergence")
                else:
                    leg.observed_sl = None if observed_sl is None else str(observed_sl)
                    leg.observed_tp = None if observed_tp is None else str(observed_tp)
                    self._persist_leg()
        if self._desired == "EMPTY":
            self._advance_convergence(event)

    def _attempt_plan_for_role(self, attempt: Attempt, role: Role) -> SizingPlan | None:
        """The attempt-bound local or peer plan used to validate executable prices."""

        plans = self._plan_index if role == self._role else self._peer_plan_index
        plan = plans.get(attempt.plan_version_of(role))
        if (
            plan is None
            or plan.product_id != attempt.product_id
            or plan.direction != attempt.direction_of(role)
        ):
            return None
        return plan

    def _own_evidence_inconsistency(self) -> str | None:
        """Exact evidence is ticket, symbol, side, volume, fill price, and SL/TP."""

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None or leg.entry_status != "filled":
            return "no exact position evidence"
        if leg.ticket is None:
            return "the observed position has no ticket"
        if leg.fill_price is None or _to_decimal(leg.fill_price) is None:
            return "the observed position has no fill price"
        observed_volume = _to_decimal(leg.observed_volume)
        if observed_volume is None or observed_volume != _to_decimal(attempt.lots):
            return "the observed volume does not match the immutable attempt volume"
        rough_sl, rough_tp = attempt.rough_of(leg.role)
        plan = self._attempt_plan_for_role(attempt, leg.role)
        if plan is None:
            return "no sizing plan exists for the immutable attempt version"
        if leg.protection_status == "rough":
            if not _same_protection_price(rough_sl, leg.observed_sl, plan.tick_size):
                return "the observed stop loss does not match the attached rough protection"
            if not _same_protection_price(rough_tp, leg.observed_tp, plan.tick_size):
                return "the observed take profit does not match the attached rough protection"
        return None

    # -- pair confirmation and precise 1:1 protection ------------------------ #

    def _peer_evidence_state(self) -> tuple[bool, str | None]:
        """``(exact, inconsistency)`` for the peer's self-reported leg evidence."""

        attempt = self._attempt
        peer = self._peer_leg
        if attempt is None or peer.status != "filled":
            return False, None
        peer_role: Role = "follower" if self._role == "leader" else "leader"
        if peer.ticket is None or not peer.ticket:
            return False, "the peer position has no ticket"
        if peer.fill_price is None or _to_decimal(peer.fill_price) is None:
            return False, "the peer position has no fill price"
        if peer.symbol != attempt.symbol_of(peer_role):
            return False, "the peer position symbol does not match the attempt"
        if peer.side != attempt.direction_of(peer_role):
            return False, "the peer position side does not match the attempt"
        if _to_decimal(peer.volume) != _to_decimal(attempt.lots):
            return False, "the peer position volume does not match the immutable attempt volume"
        rough_sl, rough_tp = attempt.rough_of(peer_role)
        plan = self._attempt_plan_for_role(attempt, peer_role)
        if plan is None:
            return False, "no sizing plan exists for the peer immutable attempt version"
        if (
            not _same_protection_price(rough_sl, peer.sl, plan.tick_size)
            or not _same_protection_price(rough_tp, peer.tp, plan.tick_size)
        ):
            return False, "the peer position is missing its attached rough protection"
        return True, None

    def _maybe_emit_pair_confirmation(self) -> None:
        if self._role != "leader" or self._pair_confirmed:
            return
        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None or self._desired != "ACTIVE":
            return
        if self._own_evidence_inconsistency() is not None:
            return
        exact, inconsistency = self._peer_evidence_state()
        if inconsistency is not None:
            self._transition("inconsistent_peer_evidence", inconsistency)
            self._notify_entry_failed(inconsistency)
            self._begin_close("inconsistent_peer_evidence")
            return
        if not exact:
            return
        self._pair_confirmed = True
        self._timer_deadline = None
        self._persist_desired()
        self._transition("pair_entry_confirmed", attempt.attempt_id)
        self._record_timing("pair_entry_confirmed")
        self._relay.send(
            self._envelope("pair_entry_confirmed", {"attempt_id": attempt.attempt_id})
        )

    def _handle_pair_entry_confirmed(self, payload: Mapping[str, object]) -> None:
        if self._role != "follower":
            return
        attempt, leg = self._attempt, self._leg
        attempt_id = payload.get("attempt_id")
        if attempt is None or leg is None or attempt_id != attempt.attempt_id:
            return
        if self._desired != "ACTIVE" or self._state in ("CONVERGING_EMPTY", "EMPTY", "NEEDS_HUMAN"):
            self._transition("pair_confirmation_rejected", "the attempt already left the entry path")
            return
        if self._timer_deadline is None or self._monotonic() >= self._timer_deadline:
            self._transition("pair_confirmation_rejected", "confirmation arrived after the local receipt timer")
            self._begin_close("pair_confirmation_after_timer")
            return
        self._pair_confirmed = True
        self._timer_deadline = None
        self._persist_desired()
        self._transition("pair_entry_confirmed", attempt.attempt_id)
        self._record_timing("pair_entry_confirmed")

    def _notify_entry_failed(self, reason: str) -> None:
        """Best effort only; safety never depends on this notification arriving."""

        if self._attempt is None:
            return
        self._relay.send(
            self._envelope(
                "pair_entry_failed", {"attempt_id": self._attempt.attempt_id, "reason": reason}
            )
        )

    def _handle_pair_entry_failed(self, payload: Mapping[str, object]) -> None:
        attempt = self._attempt
        attempt_id = payload.get("attempt_id")
        if attempt is None or attempt_id != attempt.attempt_id:
            return
        self._transition("peer_reported_entry_failed", str(payload.get("reason") or ""))
        self._begin_close("peer_reported_entry_failed")

    def _maybe_apply_precise_protection(self) -> None:
        """Precise 1:1 protection is modified only after pair confirmation."""

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None or not self._pair_confirmed:
            return
        if leg.protection_status != "rough" or leg.entry_status != "filled" or self._desired != "ACTIVE":
            return
        plan = self._plans.get((attempt.product_id, attempt.direction_of(leg.role)))
        if plan is None:
            plan = self._plan_for_validation(
                attempt.product_id, attempt.direction_of(leg.role), attempt.plan_version_of(leg.role)
            )
        fill_price = _to_decimal(leg.fill_price)
        volume = _to_decimal(attempt.lots)
        allowed = _to_decimal(attempt.allowed_loss_of(leg.role))
        protection = None
        if plan is not None and fill_price is not None and volume is not None and allowed is not None:
            protection = compute_protection(
                entry=fill_price,
                direction=attempt.direction_of(leg.role),
                volume=volume,
                plan=plan,
                allowed_loss_usd=allowed,
            )
        if protection is None:
            leg.protection_status = "failed"
            self._persist_leg()
            self._transition("precise_protection_failed", "no valid precise protection could be constructed")
            self._report_leg_status("protection_failed")
            self._notify_entry_failed("precise protection could not be constructed")
            self._begin_close("precise_protection_failed")
            return
        sl, tp = protection
        effect_id = f"{attempt.attempt_id}:{self._worker_id}:protection"
        leg.protection_effect_id = effect_id
        payload = {
            "type": "modify_sl_tp",
            "symbol": attempt.symbol_of(leg.role),
            "position": leg.ticket,
            "sl": sl,
            "tp": tp,
        }
        if self._policy is not None and self._policy.mode == "shadow":
            leg.precise_sl, leg.precise_tp, leg.protection_status = sl, tp, "precise"
            leg.observed_sl, leg.observed_tp = sl, tp
            self._persist_leg()
            self._report_leg_status("protection_precise", sl=sl, tp=tp)
            return
        try:
            self._journal.prepare(effect_id, payload)
        except EffectJournalError as error:
            leg.protection_status = "failed"
            self._persist_leg()
            self._transition("precise_protection_failed", str(error))
            self._notify_entry_failed("precise protection could not be journaled")
            self._begin_close("precise_protection_prepare_failed")
            return
        outcome = self._run_broker_write(effect_id, payload, priority="protection")
        if outcome.category == "completed":
            leg.precise_sl, leg.precise_tp, leg.protection_status = sl, tp, "precise"
            self._persist_leg()
            self._state = "PROTECTING"
            self._transition("precise_protection_applied", attempt.attempt_id)
            self._record_timing("precise_protection_applied")
            self._report_leg_status("protection_precise", sl=sl, tp=tp)
            self._request_broker_read()
        else:
            leg.protection_status = "failed"
            self._persist_leg()
            self._transition("precise_protection_failed", outcome.category)
            self._report_leg_status("protection_failed", reason=outcome.category)
            self._notify_entry_failed("precise protection failed at the broker")
            self._begin_close("precise_protection_failed")

    def _maybe_declare_active(self) -> None:
        leg = self._leg
        if leg is None or not self._pair_confirmed or self._desired != "ACTIVE":
            return
        if leg.protection_status != "precise" or self._peer_leg.status != "protection_precise":
            return
        if _to_decimal(leg.observed_sl) != _to_decimal(leg.precise_sl):
            return
        if _to_decimal(leg.observed_tp) != _to_decimal(leg.precise_tp):
            return
        self._state = "ACTIVE"
        if self._active_since is None:
            self._active_since = self._now
            self._persist_active_since()
            self._transition("active_verified", _iso(self._active_since))
            self._record_timing("active_verified")

    # -- leg status relay ----------------------------------------------------- #

    def _report_leg_status(
        self,
        status: str,
        *,
        attempt_id: str | None = None,
        ticket: str | None = None,
        fill_price: str | None = None,
        sl: str | None = None,
        tp: str | None = None,
        reason: str = "",
        receipt_retcode: object = None,
    ) -> None:
        attempt = self._attempt
        resolved_id = attempt_id or (None if attempt is None else attempt.attempt_id)
        if resolved_id is None:
            return
        own = attempt if attempt is not None and attempt.attempt_id == resolved_id else None
        product_id = None if own is None else own.product_id
        symbol = None if own is None else own.symbol_of(self._role)
        side = None if own is None else own.direction_of(self._role)
        volume = None if own is None else own.lots
        self._relay.send(
            self._envelope(
                "leg_status",
                {
                    "attempt_id": resolved_id,
                    "status": status,
                    "ticket": ticket,
                    "symbol": symbol,
                    "side": side,
                    "volume": volume,
                    "fill_price": fill_price,
                    "sl": sl,
                    "tp": tp,
                    "reason": reason,
                    "product_id": product_id,
                    "receipt_retcode": receipt_retcode,
                    "release_marker": None if product_id is None else self._release_marker(product_id),
                },
            )
        )

    def _handle_peer_leg_status(self, payload: Mapping[str, object]) -> None:
        status = payload.get("status")
        attempt_id = payload.get("attempt_id")
        if not isinstance(status, str) or not isinstance(attempt_id, str):
            return
        product_id = payload.get("product_id")
        if payload.get("receipt_retcode") == PRICE_OFF_RETCODE and isinstance(product_id, str):
            self._quarantine_product(
                product_id,
                offending_worker_id=self._peer_worker_id,
                attempt_id=attempt_id,
                receipt={
                    "retcode": PRICE_OFF_RETCODE,
                    "source": "peer_leg_status",
                    "release_marker": payload.get("release_marker"),
                },
            )
        if self._attempt is None or attempt_id != self._attempt.attempt_id:
            self._record_late_peer_leg_status(attempt_id, status, payload)
            return
        if status == "no_attempt_record" and self._peer_proof_probes_sent == 0:
            # Only an answer to this side's bounded probe is non-send proof; an
            # unsolicited one could simply mean the attempt is still in transit.
            self._transition("unsolicited_no_attempt_record_ignored", attempt_id)
            return
        previous_status = self._peer_leg.status
        self._peer_leg = PeerLegView(
            status=status,
            ticket=cast(str | None, payload.get("ticket")),
            symbol=cast(str | None, payload.get("symbol")),
            side=cast(str | None, payload.get("side")),
            volume=cast(str | None, payload.get("volume")),
            fill_price=cast(str | None, payload.get("fill_price")),
            sl=cast(str | None, payload.get("sl")),
            tp=cast(str | None, payload.get("tp")),
            reason=cast(str, payload.get("reason") or ""),
        )
        self._persist_peer_leg()
        self._transition("peer_leg_status", status)
        if status != previous_status:
            # Only real progress restarts the bounded recovery window; repeated
            # identical chatter must not extend it forever.
            self._peer_proof_wait_started = None
            self._peer_proof_last_probe = None
        if status in ("rejected", "expired_not_started", "protection_failed", "inconsistent", "no_attempt_record"):
            self._begin_close(f"peer_leg_{status}")
            self._maybe_finalize_empty()
        elif status == "empty":
            self._begin_close("peer_leg_empty")
            self._maybe_finalize_empty()

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
        if status not in _PEER_TERMINAL_STATUSES:
            # The peer is still waiting on this side; answer once from durable
            # facts.  Terminal statuses need no reply, which keeps a lost or
            # duplicated status from becoming a relay ping-pong.
            self._answer_leg_status(attempt_id, allow_no_record=False)

    # -- product quarantine ---------------------------------------------------- #

    def _quarantine_product(
        self,
        product_id: str,
        *,
        offending_worker_id: str,
        attempt_id: str,
        receipt: Mapping[str, object],
        local_evidence: bool = False,
        commit: bool = True,
    ) -> bool:
        """Durable, pair-wide, product-specific MT5 ``10021`` quarantine.

        The key is the derived product identity and the observed
        ``universe_generation`` is recorded beside it, so a quarantine survives
        an explicit rediscovery whenever the rediscovered product resolves to
        the same identity.
        """

        marker = self._release_marker(product_id)
        evidence = dict(receipt)
        if local_evidence:
            evidence["release_marker"] = marker
        elif evidence.get("release_marker") != marker:
            return False
        existing = self._db.execute(
            "SELECT 1 FROM cell_product_quarantine WHERE product_id = ?", (product_id,)
        ).fetchone()
        if existing is not None:
            return False
        released = self._db.execute(
            "SELECT 1 FROM cell_product_quarantine_release"
            " WHERE product_id = ? AND offending_worker_id = ? AND attempt_id = ?",
            (product_id, offending_worker_id, attempt_id),
        ).fetchone()
        if released is not None:
            return False
        inserted = self._db.execute(
            "INSERT INTO cell_product_quarantine (product_id, offending_worker_id, attempt_id,"
            " universe_generation, receipt, quarantined_at) VALUES (?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(product_id) DO NOTHING",
            (
                product_id,
                offending_worker_id,
                attempt_id,
                self.universe_generation(),
                _canonical_json(evidence),
                _iso(self._now),
            ),
        ).rowcount
        if inserted:
            self._transition("product_quarantined", f"{product_id}: MT5 retcode 10021")
            self._last_published_readiness = None
        if commit and inserted:
            self._db.commit()
        return bool(inserted)

    def _product_quarantine_records(self) -> list[dict[str, object]]:
        rows = self._db.execute(
            "SELECT product_id, offending_worker_id, attempt_id, receipt, quarantined_at,"
            " universe_generation FROM cell_product_quarantine ORDER BY product_id"
        ).fetchall()
        return [
            {
                "product_id": str(row[0]),
                "offending_worker_id": str(row[1]),
                "attempt_id": str(row[2]),
                "receipt": json.loads(str(row[3])),
                "quarantined_at": str(row[4]),
                "universe_generation": None if row[5] is None else int(row[5]),
            }
            for row in rows
        ]

    def _accept_peer_product_quarantines(self, raw: object) -> None:
        if not isinstance(raw, list):
            return
        changed = False
        for item in raw:
            if not isinstance(item, dict):
                continue
            product_id = item.get("product_id")
            attempt_id = item.get("attempt_id")
            receipt = item.get("receipt")
            if (
                not isinstance(product_id, str)
                or not isinstance(attempt_id, str)
                or item.get("offending_worker_id") != self._peer_worker_id
                or not isinstance(receipt, dict)
                or receipt.get("retcode") != PRICE_OFF_RETCODE
            ):
                continue
            changed = self._quarantine_product(
                product_id,
                offending_worker_id=self._peer_worker_id,
                attempt_id=attempt_id,
                receipt=receipt,
                commit=False,
            ) or changed
        if changed:
            self._db.commit()

    def _release_product_quarantine(self, event: QuarantineReleaseEvent) -> None:
        if self._attempt is not None and self._state not in ("EMPTY", "IDLE", "NEEDS_HUMAN"):
            self._transition("product_quarantine_release_rejected", f"{event.product_id}: unresolved attempt")
            return
        self._db.execute(
            "INSERT INTO cell_product_release_marker (product_id, marker) VALUES (?, ?)"
            " ON CONFLICT(product_id) DO UPDATE SET marker = excluded.marker",
            (event.product_id, _iso(event.observed_at)),
        )
        self._db.execute(
            "INSERT OR IGNORE INTO cell_product_quarantine_release (product_id, offending_worker_id,"
            " attempt_id, released_at) SELECT product_id, offending_worker_id, attempt_id, ?"
            " FROM cell_product_quarantine WHERE product_id = ?",
            (_iso(self._now), event.product_id),
        )
        deleted = self._db.execute(
            "DELETE FROM cell_product_quarantine WHERE product_id = ?", (event.product_id,)
        ).rowcount
        self._db.commit()
        if deleted:
            self._last_published_readiness = None
            self._transition(
                "product_quarantine_released",
                f"{event.product_id}: actor={event.actor}; reason={event.reason}",
            )

    def _release_marker(self, product_id: str) -> str | None:
        row = self._db.execute(
            "SELECT marker FROM cell_product_release_marker WHERE product_id = ?", (product_id,)
        ).fetchone()
        return None if row is None else str(row[0])

    # -- desired-EMPTY containment -------------------------------------------- #

    def _begin_close(self, reason: str) -> None:
        """``UNPAIRING`` and metadata reconciliation never block this path."""

        if self._attempt is None or self._desired == "EMPTY":
            return
        self._desired = "EMPTY"
        self._timer_deadline = None
        self._persist_desired()
        self._transition("close_requested", reason)
        self._record_timing("containment_start")

    def _maybe_progress_convergence(self) -> None:
        if self._desired != "EMPTY" or self._attempt is None or self._leg is None:
            return
        self._state = "CONVERGING_EMPTY"
        if self._leg.empty_verified:
            return
        self._request_broker_read()

    def _maybe_finalize_empty(self) -> None:
        if self._attempt is None or self._leg is None:
            return
        if not self._leg.empty_verified:
            return
        if self._peer_leg.status != "empty" and self._peer_send_possible():
            return
        self._state = "EMPTY"
        self._record_timing("terminal_time")
        attempt_id = self._attempt.attempt_id
        self._terminate_attempt("both_empty_verified")
        self._reset_attempt()
        self._clear_resolved_peer_terminal_proof_stop(attempt_id)

    def _peer_send_possible(self) -> bool:
        """Whether the peer could still hold the mirror leg.

        Explicit durable non-send proof (an admission rejection, an
        expired-before-start effect, or a peer that has no durable record of
        the attempt at all) means there is nothing to wait for.  Silence is
        never such a proof.
        """

        leg = self._leg
        if leg is None:
            return False
        status = self._peer_leg.status
        if status in _PEER_TERMINAL_STATUSES:
            return False  # explicit durable proof that no peer exposure exists
        if status != "unknown":
            return True  # the peer reported starting or holding something
        if self._role == "leader" and not leg.attempt_relayed:
            return False  # provably one-sided: no attempt ever left this leader
        return True

    def _await_peer_terminal_proof(self) -> None:
        """Probe, retransmit, then escalate; never infer a peer ``EMPTY``."""

        attempt, leg = self._attempt, self._leg
        if attempt is None or leg is None or not leg.empty_verified:
            self._peer_proof_wait_started = None
            self._peer_proof_last_probe = None
            return
        if not self._peer_send_possible() or self._peer_leg.status in _PEER_TERMINAL_STATUSES:
            return  # finalization handles this
        if (
            self._needs_human is not None
            and self._needs_human != self._peer_terminal_proof_stop_reason(attempt.attempt_id)
        ):
            return
        now = self._monotonic()
        timeout = attempt.confirmation_timeout_seconds
        if self._peer_proof_wait_started is None:
            self._peer_proof_wait_started = now
            self._peer_proof_last_probe = now
            return
        if now - self._peer_proof_wait_started >= timeout * _PEER_PROOF_ESCALATION_MULTIPLIER:
            self._set_needs_human(
                self._peer_terminal_proof_stop_reason(attempt.attempt_id),
                "peer terminal proof unavailable",
            )
            return
        if self._peer_proof_last_probe is None or now - self._peer_proof_last_probe >= timeout * _PEER_PROOF_PROBE_MULTIPLIER:
            self._peer_proof_last_probe = now
            self._peer_proof_probes_sent += 1
            self._report_leg_status("empty")
            self._relay.send(
                self._envelope("leg_status_request", {"attempt_id": attempt.attempt_id})
            )
            self._transition("peer_terminal_proof_probe", attempt.attempt_id)

    def _handle_leg_status_request(self, payload: Mapping[str, object]) -> None:
        """Answer an explicit peer probe from durable local facts only."""

        attempt_id = payload.get("attempt_id")
        if not isinstance(attempt_id, str) or not attempt_id:
            return
        self._answer_leg_status(attempt_id, allow_no_record=True)

    def _answer_leg_status(self, attempt_id: str, *, allow_no_record: bool) -> None:
        """Report this Worker's durable position on one attempt ID.

        ``allow_no_record`` is only true for an explicit probe: an attempt that
        is merely still in transit would otherwise be reported as provably
        absent, which is exactly the false non-send proof the bounded recovery
        path exists to avoid.
        """

        if self._attempt is not None and self._attempt.attempt_id == attempt_id:
            self._report_current_leg_status()
            return
        row = self._db.execute(
            "SELECT terminal, terminal_reason FROM cell_attempts WHERE attempt_id = ?", (attempt_id,)
        ).fetchone()
        if row is None:
            if allow_no_record:
                # Every attempt is durably persisted before any broker write, so
                # no record after a bounded probe is proof this Worker never sent.
                self._report_leg_status(
                    "no_attempt_record",
                    attempt_id=attempt_id,
                    reason="this Worker has no durable record of the attempt",
                )
            return
        if row[0]:
            reason = str(row[1] or "")
            self._report_leg_status(
                _terminal_status_for(reason), attempt_id=attempt_id, reason=reason
            )

    def _report_current_leg_status(self) -> None:
        leg = self._leg
        if leg is None or self._attempt is None:
            return
        if leg.empty_verified:
            self._report_leg_status("empty")
        elif leg.protection_status == "precise":
            self._report_leg_status("protection_precise", sl=leg.precise_sl, tp=leg.precise_tp)
        elif leg.entry_status == "filled":
            self._report_leg_status(
                "filled",
                ticket=leg.ticket,
                fill_price=leg.fill_price,
                sl=leg.observed_sl,
                tp=leg.observed_tp,
            )
        else:
            self._report_leg_status(leg.entry_status)

    def _advance_convergence(self, event: BrokerSnapshotEvent) -> None:
        if self._attempt is None or self._leg is None:
            return
        attempt, leg = self._attempt, self._leg
        assert event.orders is not None and event.positions is not None
        symbol = attempt.symbol_of(leg.role)
        attempt_orders = [o for o in event.orders if str(o.get("symbol")) == symbol]
        if attempt_orders:
            self._cancel_owned(attempt_orders[0])
            return
        if leg.ticket is not None:
            owned = [p for p in event.positions if str(p.get("ticket")) == leg.ticket]
            if owned:
                if self._needs_human is not None:
                    return
                self._close_owned(owned[0])
                return
        if event.positions or event.orders:
            # Exposure this cell cannot attribute to its exact attempt-owned
            # ticket is never closed on a guess and never treated as empty:
            # parking silently is exactly how a real filled position could stay
            # open forever, so it becomes loud instead.
            self._set_needs_human(
                f"attempt {attempt.attempt_id} left exposure this Worker cannot attribute by exact ticket",
                "exact-ticket attribution failed during containment",
            )
            return
        if not leg.empty_verified:
            leg.empty_verified = True
            self._persist_leg()
            self._transition("empty_verified", attempt.attempt_id)
            self._state = "CONVERGING_EMPTY"
            self._report_leg_status("empty")
            self._maybe_finalize_empty()

    def _cancel_owned(self, order: dict[str, object]) -> None:
        assert self._attempt is not None and self._leg is not None
        ticket = str(order.get("ticket"))
        effect_id = f"{self._attempt.attempt_id}:{self._worker_id}:cancel:{ticket}"
        payload = {"type": "cancel", "ticket": ticket}
        self._journaled_emergency_write(effect_id, payload)

    def _close_owned(self, position: dict[str, object]) -> None:
        assert self._attempt is not None and self._leg is not None
        ticket = str(position.get("ticket"))
        effect_id = f"{self._attempt.attempt_id}:{self._worker_id}:close:{ticket}"
        self._leg.close_effect_id = effect_id
        payload = {"type": "close", "ticket": ticket, "volume": str(position.get("volume"))}
        self._journaled_emergency_write(effect_id, payload)

    def _journaled_emergency_write(self, effect_id: str, payload: dict[str, object]) -> None:
        if self._policy is not None and self._policy.mode == "shadow":
            self._request_broker_read()
            return
        try:
            effect_state = self._journal.prepare(effect_id, payload)
        except EffectJournalError as error:
            self._set_needs_human(
                f"containment effect {effect_id} could not be prepared: {error}",
                "effect journal prepare failed",
            )
            return
        self._bump_state_version()
        if effect_state in ("receipt", "observed"):
            try:
                evidence = self._journal.evidence(effect_id)
            except EffectJournalError as error:
                self._set_needs_human(
                    f"containment effect {effect_id} has invalid replay evidence: {error}",
                    "invalid containment replay evidence",
                )
                return
            if evidence.get("retcode") in _SUCCESS_RETCODES:
                self._transition("containment_receipt_recovered", effect_id)
                return
            self._set_needs_human(
                f"containment effect {effect_id} has non-success replay evidence",
                "containment replay evidence was not successful",
            )
            return
        if effect_state == "send_started":
            # An effect that may have crossed send_started is never blindly resent.
            self._set_needs_human(
                f"containment effect {effect_id} has no conclusive broker receipt",
                "containment remained uncertain after send_started",
            )
            return
        outcome = self._run_broker_write(effect_id, payload, priority="emergency")
        if outcome.category != "completed":
            self._set_needs_human(f"containment effect {effect_id} ended {outcome.category}", outcome.category)
            return
        self._request_broker_read()

    def _reset_attempt(self) -> None:
        self._attempt = None
        self._leg = None
        self._peer_leg = PeerLegView()
        self._pair_confirmed = False
        self._timer_deadline = None
        self._peer_proof_wait_started = None
        self._peer_proof_last_probe = None
        self._peer_proof_probes_sent = 0
        self._active_since = None
        self._desired = "NONE"
        self._persist_desired()
        self._bump_state_version()
        self._last_published_readiness = None

    # -- observable result ------------------------------------------------------ #

    def _result(self) -> PairResult:
        ready, reason = self._local_ready()
        return PairResult(
            worker_id=self._worker_id,
            role=self._role,
            route_id=self._route.route_id,
            route_state=self._route_state,
            universe_generation=self.universe_generation(),
            state_version=self._state_version,
            state=self._state,
            ready=ready,
            ready_reason=reason,
            attempt_id=None if self._attempt is None else self._attempt.attempt_id,
            desired_state=self._desired,
            policy_hash=None if self._policy is None else self._policy.hash,
            policy_accepted=self._policy_accepted,
            plan_set_version=self._plan_set_version,
            pair_confirmed=self._pair_confirmed,
            needs_human=self._needs_human is not None,
            needs_human_reason=self._needs_human,
            metadata_reconciliation=self._metadata_reconciliation,
            discovery_reason=self._discovery_reason,
            rediscovery_failure=self._rediscovery_failure,
            recovering=self._recovering,
            timing=dict(self._last_timing),
        )


# --------------------------------------------------------------------------- #
# Small module-level converters
# --------------------------------------------------------------------------- #


def _canonical_entries(entries: Iterable[CatalogEntry]) -> str:
    return _canonical_json([entry.canonical() for entry in sorted(entries, key=lambda e: e.name)])


def _combine_pair_lots(
    leader_plan: SizingPlan, follower_plan: SizingPlan
) -> tuple[Decimal, SizingPlan, SizingPlan] | None:
    """The common lots for two named plan versions, snapped to the shared step."""

    leader_lots = _to_decimal(leader_plan.local_max_lots)
    follower_lots = _to_decimal(follower_plan.local_max_lots)
    volume_min = _to_decimal(leader_plan.volume_min)
    volume_step = _to_decimal(leader_plan.volume_step)
    if leader_lots is None or follower_lots is None or volume_min is None or volume_step is None:
        return None
    lower = min(leader_lots, follower_lots)
    lots = floor_to_volume_step(lower, volume_min, lower, volume_step)
    if lots is None or lots <= 0:
        return None
    return lots, leader_plan, follower_plan


def _compatibility_drift(entry: CatalogEntry | None, product: DiscoveredProduct) -> str | None:
    """Why this Worker's current catalog no longer matches a frozen product."""

    if entry is None:
        return "the symbol is no longer present in this Worker's catalog"
    trade_mode = entry.trade_mode
    if isinstance(trade_mode, bool) or not isinstance(trade_mode, int) or trade_mode != FULL_TRADING_MODE:
        return "the symbol is no longer fully tradable"
    if entry.trade_calc_mode != product.trade_calc_mode:
        return "the current specification no longer matches the frozen compatibility summary"
    for name, frozen in (
        ("contract_size", product.contract_size),
        ("volume_min", product.volume_min),
        ("volume_step", product.volume_step),
    ):
        if decimal_text(getattr(entry, name)) != frozen:
            return "the current specification no longer matches the frozen compatibility summary"
    if not set(product.directions).issubset(set(entry.allowed_directions)):
        return "the current specification no longer allows both directions"
    if product.filling_mode not in entry.filling_modes:
        return "the shared filling mode is no longer supported"
    return None


def _quote_key(local: QuoteSnapshot, peer: QuoteSnapshot) -> tuple[str, int, str, int]:
    return local.recovery_epoch, local.sequence, peer.recovery_epoch, peer.sequence


def _terminal_status_for(terminal_reason: str) -> str:
    """The durable leg status a terminalized attempt should report to the peer."""

    return "rejected" if terminal_reason.startswith("admission_rejected") else "empty"


def _bounded_batches(payload: list[dict[str, object]], *, max_bytes: int = 32_768) -> list[list[dict[str, object]]]:
    batches: list[list[dict[str, object]]] = []
    current: list[dict[str, object]] = []
    current_size = 2
    for entry in payload:
        entry_size = len(_canonical_json(entry).encode("utf-8")) + 1
        if current and current_size + entry_size > max_bytes:
            batches.append(current)
            current, current_size = [entry], 2 + entry_size
        else:
            current.append(entry)
            current_size += entry_size
    batches.append(current)
    return batches


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


def _mapping(raw: object) -> Mapping[str, object] | None:
    """MT5 results arrive as dicts or named tuples; both are read the same way."""

    if raw is None:
        return None
    if isinstance(raw, Mapping):
        return raw
    if hasattr(raw, "_asdict"):
        return cast(Mapping[str, object], raw._asdict())
    return None


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
