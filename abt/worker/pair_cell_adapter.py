"""Production wiring between an authenticated Worker session and one
:class:`abt.pair_cell.PairExecutionCell`.

``abt.pair_cell`` is deliberately broker-, catalog- and transport-agnostic: it
depends only on the small :class:`~abt.pair_cell.PairRelayAdapter` and
:class:`~abt.pair_cell.PairCellMT5` protocols and drives its own abstract,
already-proven trader-operation payload shape (``market``, ``cancel``,
``close``, ``modify_sl_tp``) toward whatever ``mt5`` object is injected -- the
same shape the ordinary Trader-relay path
(``abt.worker.reconciliation._trader_operation``) already sends.  This module
is the seam that:

* drives **Worker-initiated pairing**: it declares this Worker's desired role
  on its first unpaired connection, lists the controller's currently
  connected, available, unpaired followers for a leader, proposes one
  (interactively or by ``--follower-worker-id``), and answers a proposal on
  the follower side with an automatic acceptance that is gated purely on
  local safety evidence;
* keeps this Worker's **durable copy of the route** (its ``route_id``,
  assigned role and frozen startup balance) beside the cell's own database,
  and treats the controller's route record as authoritative by feeding every
  observed record to the cell as a :class:`~abt.pair_cell.RouteMetadataEvent`;
* constructs the :class:`~abt.pair_cell.PairExecutionCell` **only once an
  authoritative route assignment exists**, so there is never a partially
  routed cell;
* reads this account's **full MT5 catalog and specification** into a
  versioned :class:`~abt.pair_cell.CatalogSummary` for Initial Compatible
  Product Discovery, and thereafter revalidates only the frozen universe's
  symbols so a symbol that appears in a broker catalog later can never be
  silently admitted;
* translates the cell's abstract broker payload into a genuine MetaTrader5
  request before the real ``order_send`` call, and exposes
  ``order_calc_margin`` through the same translation the Trader-relay path
  already proves correct;
* wraps this Worker's :class:`~abt.worker.session.AuthenticatedWorkerSession`
  as the cell's opaque Worker-to-Worker relay, translating the cell's small
  internal envelope shape to and from the controller-validated
  :class:`~abt.trader_protocol.PairCellRelayEnvelope` wire format, whose only
  identity fields are the :class:`~abt.trader_protocol.PairRouteClaim`
  (``route_id`` plus the ordered Worker pair) and the two Worker IDs; and
* continuously feeds quote evidence, readiness facts, broker snapshots,
  realized per-leg loss facts, route metadata and peer-session state to the
  cell, publishes its local attempt/effect state version and its safe-unpair
  assertions to the controller, and owns restart recovery and clean shutdown
  for ``abt.worker.reconciliation``/``abt.worker.cli``.

There is no ``trader_id``, no Pair Contract, no Pair Lease, no administrator
route, and no ``built_products`` configuration anywhere in this module.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
import json
import logging
from hashlib import sha256
import os
from pathlib import Path
import sqlite3
from statistics import median
import time as _time
from typing import Protocol, TypeVar, cast
from uuid import uuid4

from ..pair_cell import (
    PROTOCOL_VERSION as PAIR_CELL_ENVELOPE_VERSION,
    DEFAULT_ALLOW_LIVE,
    LOCAL_SETTING_KEYS,
    REQUIRED_ACCOUNT_CURRENCY,
    SHARED_POLICY_KEYS,
    WORKER_RISK_KEYS,
    BrokerClockCalibration,
    BrokerSnapshotEvent,
    CatalogEntry,
    ClockTickEvent,
    Direction,
    FillingMode,
    LocalCatalogEvent,
    LocalQuoteBatchEvent,
    LocalQuoteEvent,
    PairExecutionCell,
    PairExecutionCellError,
    PairResult,
    PairingAcceptance,
    PeerSessionEvent,
    QuarantineReleaseEvent,
    ReadinessFactsEvent,
    RealizedPnLEvent,
    RelayEnvelopeReceived,
    Role,
    RouteAssignment,
    RouteMetadataEvent,
    RouteState,
    WorkerRiskLimits,
    _parse_utc,
    build_pairing_acceptance,
    canonical_policy_from_acceptance,
    default_worker_risk_limits,
    validate_configuration_authority,
)
from ..trader_protocol import PAIR_CELL_PROTOCOL_VERSION
from .effect_journal import EffectJournalError, WorkerEffectJournal
from .reconciliation import (
    ReadOnlyMT5,
    WorkerEnrollmentError,
    _broker_close_request,
    _broker_order_request,
    _dispatch_scheduled_trader_rpc,
    _evidence,
    _require_terminal_trading_permission,
    _trader_margin,
)
from .scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc, TraderRpcOutcome


_LOGGER = logging.getLogger(__name__)

_T = TypeVar("_T")

_MT5_FILLING_FOK_BIT = 1
_MT5_FILLING_IOC_BIT = 2
_MAX_REMEMBERED_CLOSED_TICKETS = 512
# MT5 ``DEAL_ENTRY_OUT``/``DEAL_ENTRY_INOUT``/``DEAL_ENTRY_OUT_BY``.
_MT5_DEAL_EXIT_ENTRIES = (1, 2, 3)
# A quote is never published on an unmeasured clock: the cell refuses an
# unusable calibration rather than guessing a zero offset.
_UNCALIBRATED = BrokerClockCalibration(status="uncalibrated")
# Broker-clock calibration mirrors ``abt.mt5.timecalibration``: three spaced,
# strictly advancing tick samples, or no calibration at all.
_CALIBRATION_SAMPLE_COUNT = 3
_CALIBRATION_SAMPLE_INTERVAL = timedelta(seconds=1)
_MAX_CALIBRATION_DISPERSION_SECONDS = 5.0
# Calibration is a per-terminal fact, so only a bounded number of symbols is
# tried before giving up for this cycle.
_MAX_CALIBRATION_SYMBOLS = 8
_STATE_RELAY_KINDS = frozenset(
    {
        "attempt",
        "catalog_summary",
        "leg_status",
        "pair_entry_confirmed",
        "pair_entry_failed",
        "pairing_acceptance",
        "policy",
        "policy_ack",
        "readiness",
        "remaining_allowance",
        "reseed_request",
        "sizing_plans",
        "universe",
    }
)
# A pairing reservation lives for 30 seconds.  A Worker that is still waiting
# on a one-shot decision reply or route-established notice keeps re-reading the
# authoritative route until comfortably past that, so an in-flight pairing is
# never mistaken for an abandoned one, and an abandoned one never strands the
# Worker with a frozen acceptance it can no longer use.
PAIRING_RESERVATION_GRACE_SECONDS = 90.0

#: Every key a Pair Execution Cell configuration file may carry.  A file is
#: *optional* and tunables-only: an absent one means "synthesize the
#: documented defaults and run", never "the cell is disabled".
PAIR_CELL_CONFIG_KEYS = frozenset(WORKER_RISK_KEYS) | frozenset(SHARED_POLICY_KEYS) | frozenset(LOCAL_SETTING_KEYS)

#: Durable cell tables whose rows belong to **one pairing**.  They are cleared
#: when a route is removed, because the next pairing must start from a clean
#: route, policy, universe and plan set.
ROUTE_SCOPED_TABLES = (
    "cell_route",
    "cell_pairing_acceptance",
    "cell_universe",
    "cell_policy",
    "cell_plan_version",
    "cell_sizing_plans",
    "cell_peer_sizing_plans",
)
#: Attempt evidence, cleared **as one set** and only once every attempt is
#: provably terminal.  ``cell_desired_state`` and ``cell_active_state`` belong
#: here rather than with route state because the cell reads them only while an
#: unresolved attempt row exists: clearing them while retaining that attempt
#: would leave a recovered attempt with no desired state to converge through,
#: and retaining them past terminal cleanup would let a new route inherit a
#: previous pairing's desired state.
ATTEMPT_SCOPED_TABLES = (
    "cell_attempts",
    "cell_leg_state",
    "cell_peer_leg",
    "cell_desired_state",
    "cell_active_state",
)
#: Account- and product-scoped **safety history**, which is not route state and
#: must outlive any number of pairings.  The New York daily realized-loss
#: accumulator keeps a same-day re-pair from resetting its own loss budget, the
#: MT5 ``10021`` quarantine (and its release audit) is keyed by derived product
#: identity and has no automatic expiry, an operator stop is a human decision,
#: and the local attempt/effect state version and publisher epoch must never
#: move backwards.  The reset is a deny-list, so any table this module does not
#: name is preserved; ``test_pair_cell_adapter`` fails when a new durable cell
#: table is left unclassified.
PRESERVED_SAFETY_TABLES = (
    "cell_realized_pnl",
    "cell_product_quarantine",
    "cell_product_quarantine_release",
    "cell_product_release_marker",
    "cell_operator_stop",
    "cell_state_version",
    "cell_publisher_epoch",
    "cell_transitions",
)


def _atomic_write_json(path: Path, payload: Mapping[str, object]) -> None:
    """Write one durable JSON record so a crash can never leave it truncated.

    The record is written to a sibling temporary file, flushed and fsynced,
    and then renamed over the target.  ``os.replace`` is atomic on Windows and
    POSIX alike, so a reader either sees the whole previous record or the
    whole new one -- never half of either.
    """

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f"{path.name}.tmp")
    encoded = json.dumps(payload, separators=(",", ":"), sort_keys=True)
    with open(temporary, "w", encoding="utf-8", newline="\n") as handle:
        handle.write(encoded)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


class PairCellWorkerSession(Protocol):
    """The subset of ``AuthenticatedWorkerSession`` this adapter drives."""

    worker_id: str

    @property
    def trader_rpc_scheduler(self) -> DeadlineAwareTraderRpcScheduler: ...

    def dispatch_scheduler_outcome(self, outcome: TraderRpcOutcome) -> None: ...

    def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None: ...

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]: ...

    def drain_pair_relay_acks(self) -> list[dict[str, object]]: ...

    def drain_pair_cell_quarantine_releases(self) -> list[dict[str, object]]: ...

    def send_pair_cell_quarantine_release_result(self, *, request_id: str, symbol: str, outcome: str) -> None: ...

    def drain_pair_cell_results(self) -> list[dict[str, object]]: ...

    def drain_pair_cell_pushes(self) -> list[dict[str, object]]: ...

    def declare_pair_cell_role(self, role: str | None) -> str: ...

    def request_available_pair_followers(self) -> str: ...

    def propose_pair_cell_pairing(self, follower_worker_id: str) -> str: ...

    def send_pair_cell_pairing_decision(
        self,
        *,
        proposal_id: str,
        accepted: bool,
        acceptance_payload: dict[str, object] | None = None,
        payload_hash: str | None = None,
        reason: str | None = None,
    ) -> str: ...

    def request_pair_cell_route_sync(self) -> str: ...

    def send_pair_cell_unpair(self, *, route_id: str, action: str) -> str: ...

    def send_pair_cell_state_version(self, *, route_id: str, state_version: int) -> str: ...

    def send_pair_cell_unpair_assertion(self, *, route_id: str, state_version: int) -> str: ...


# --------------------------------------------------------------------------- #
# Optional, tunables-only, role-specific configuration
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class PairCellConfig:
    """One Worker's optional Pair Execution Cell configuration file.

    A configuration file is never required and never carries a route, a
    ``route_id``, a ``trader_id``, ``built_products`` or ``eligible_products``
    -- those belong to pairing and discovery.  What it may carry depends on
    the role this Worker actually receives:

    * a **follower** file may set only its own four risk tunables and its own
      ``allow_live`` guard; and
    * a **leader** file may additionally set the shared, leader-authored
      policy values.

    Because the role is unknown until the Worker is on a route, a file holding
    only follower-permissible keys is valid for either role and a file holding
    shared policy is valid only once this Worker actually becomes the leader.
    """

    raw: Mapping[str, object]
    risk_overrides: Mapping[str, object]
    shared_policy: Mapping[str, object]
    allow_live: bool = DEFAULT_ALLOW_LIVE
    source: str = "<defaults>"

    @property
    def declares_shared_policy(self) -> bool:
        return bool(self.shared_policy)

    def role_authority_error(self, role: Role) -> str | None:
        """Why this file exceeds the authority of the role actually assigned."""

        try:
            validate_configuration_authority(self.raw, role=role)
        except PairExecutionCellError as error:
            return str(error)
        return None


def default_pair_cell_config() -> PairCellConfig:
    """The synthesized defaults used when no configuration file exists."""

    return PairCellConfig(raw={}, risk_overrides={}, shared_policy={}, allow_live=DEFAULT_ALLOW_LIVE)


def load_pair_cell_config(path: Path | None) -> PairCellConfig:
    """Load one Worker's optional configuration, synthesizing defaults if absent.

    An absent file is the ordinary deployment: the runtime synthesizes the
    documented defaults (``live`` mode, the startup broker balance as
    ``strategy_budget_usd``, ``allow_live = true`` and the documented risk and
    shared-policy numbers) and the cell is available.  A present but malformed
    or over-reaching file is an explicit startup configuration error.
    """

    if path is None or not path.exists():
        return default_pair_cell_config()
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as error:
        raise WorkerEnrollmentError(
            f"The Pair Execution Cell configuration is invalid: {path}."
        ) from error
    return parse_pair_cell_config(raw, source=path)


def parse_pair_cell_config(raw: object, *, source: object = "<memory>") -> PairCellConfig:
    if not isinstance(raw, Mapping):
        raise WorkerEnrollmentError(f"The Pair Execution Cell configuration is invalid: {source}.")
    values = dict(raw)
    try:
        # Route, ``route_id``, ``trader_id``, ``built_products`` and
        # ``eligible_products`` are startup errors in either role.
        validate_configuration_authority(values, role=None)
    except PairExecutionCellError as error:
        raise WorkerEnrollmentError(
            f"The Pair Execution Cell configuration is invalid: {source}: {error}"
        ) from error
    unknown = sorted(key for key in values if key not in PAIR_CELL_CONFIG_KEYS)
    if unknown:
        raise WorkerEnrollmentError(
            f"The Pair Execution Cell configuration is invalid: {source}: "
            f"unknown settings {', '.join(unknown)}."
        )
    allow_live = values.get("allow_live", DEFAULT_ALLOW_LIVE)
    if not isinstance(allow_live, bool):
        raise WorkerEnrollmentError(
            f"The Pair Execution Cell configuration is invalid: {source}: allow_live must be a boolean."
        )
    risk_overrides = {key: values[key] for key in WORKER_RISK_KEYS if key in values}
    shared_policy = {key: values[key] for key in SHARED_POLICY_KEYS if key in values}
    config = PairCellConfig(
        raw=values,
        risk_overrides=risk_overrides,
        shared_policy=shared_policy,
        allow_live=allow_live,
        source=str(source),
    )
    # Fail on unusable numbers now rather than at pairing time.
    try:
        default_worker_risk_limits(
            startup_balance_usd="1", account_currency=REQUIRED_ACCOUNT_CURRENCY, overrides=risk_overrides
        )
    except PairExecutionCellError as error:
        raise WorkerEnrollmentError(
            f"The Pair Execution Cell configuration is invalid: {source}: {error}"
        ) from error
    return config


# --------------------------------------------------------------------------- #
# The frozen startup balance and this Worker's own authored risk block
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class StartupBalance:
    """This Worker's broker account **balance**, read once and then frozen.

    Balance rather than equity is deliberate: it excludes floating P&L from
    unrelated or leftover positions and is therefore a stable, reproducible
    sizing base for a frozen policy.
    """

    amount_usd: str
    account_currency: str = REQUIRED_ACCOUNT_CURRENCY


def read_startup_balance(mt5: object) -> StartupBalance:
    """Read the frozen sizing base, failing closed rather than substituting one.

    A non-USD account currency, an unreadable account, a zero or negative
    balance, or a ``None``/malformed account record is a refusal, never a
    defaulted budget.
    """

    evidence = _evidence(getattr(mt5, "account_info")(), "account")
    currency = evidence.get("currency")
    if not isinstance(currency, str) or currency != REQUIRED_ACCOUNT_CURRENCY:
        raise WorkerEnrollmentError(
            "A Pair Execution Cell account currency must be USD; no FX conversion is performed."
        )
    balance = evidence.get("balance")
    if not _positive_number(balance):
        raise WorkerEnrollmentError(
            "A Pair Execution Cell strategy budget requires a strictly positive startup balance."
        )
    return StartupBalance(amount_usd=_decimal_text(balance), account_currency=currency)


def worker_risk_limits(balance: StartupBalance, config: PairCellConfig) -> WorkerRiskLimits:
    """This Worker's own authored risk block, budgeted by its frozen balance."""

    try:
        return default_worker_risk_limits(
            startup_balance_usd=balance.amount_usd,
            account_currency=balance.account_currency,
            overrides=config.risk_overrides,
        )
    except PairExecutionCellError as error:
        raise WorkerEnrollmentError(str(error)) from error


# --------------------------------------------------------------------------- #
# The Worker's durable copy of its route: recovery evidence, never authority
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class DurableRouteRecord:
    """This Worker's durable copy of the route it participates in.

    It is *recovery evidence about this Worker's own participation*, never a
    competing authority: the controller's route record decides relay
    permission and who holds which role.  The frozen startup balance travels
    with it so a restart never re-reads the broker balance and silently
    resizes a live pairing.
    """

    route_id: str
    role: Role
    leader_worker_id: str
    follower_worker_id: str
    frozen_balance_usd: str
    account_currency: str = REQUIRED_ACCOUNT_CURRENCY
    state: RouteState = "ACTIVE"
    proposal_id: str | None = None

    def assignment(self) -> RouteAssignment:
        return RouteAssignment(
            route_id=self.route_id,
            role=self.role,
            leader_worker_id=self.leader_worker_id,
            follower_worker_id=self.follower_worker_id,
            state=self.state,
        )

    def balance(self) -> StartupBalance:
        return StartupBalance(
            amount_usd=self.frozen_balance_usd, account_currency=self.account_currency
        )

    def canonical(self) -> dict[str, object]:
        return {
            "route_id": self.route_id,
            "role": self.role,
            "leader_worker_id": self.leader_worker_id,
            "follower_worker_id": self.follower_worker_id,
            "frozen_balance_usd": self.frozen_balance_usd,
            "account_currency": self.account_currency,
            "state": self.state,
            "proposal_id": self.proposal_id,
        }


def load_durable_route(path: Path) -> DurableRouteRecord | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise TypeError
        role = str(raw["role"])
        state = str(raw.get("state", "ACTIVE"))
        if role not in ("leader", "follower") or state not in ("ACTIVE", "UNPAIRING"):
            raise TypeError
        proposal_id = raw.get("proposal_id")
        return DurableRouteRecord(
            route_id=str(raw["route_id"]),
            role=cast(Role, role),
            leader_worker_id=str(raw["leader_worker_id"]),
            follower_worker_id=str(raw["follower_worker_id"]),
            frozen_balance_usd=str(raw["frozen_balance_usd"]),
            account_currency=str(raw.get("account_currency", REQUIRED_ACCOUNT_CURRENCY)),
            state=cast(RouteState, state),
            proposal_id=None if proposal_id is None else str(proposal_id),
        )
    except (OSError, ValueError, KeyError, TypeError):
        _LOGGER.warning("The durable Pair Execution Cell route copy is unreadable.", exc_info=True)
        return None


def save_durable_route(path: Path, record: DurableRouteRecord) -> None:
    """Persist this Worker's route copy atomically.

    A half-written route copy is worse than none at all: it would be
    unreadable on restart, which would drop this Worker back into pairing
    while it may still own broker exposure.
    """

    _atomic_write_json(path, record.canonical())


def clear_durable_route(path: Path) -> None:
    for candidate in (path, path.with_name(f"{path.name}.tmp")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            continue
        except OSError:  # pragma: no cover - best effort local cleanup
            _LOGGER.warning("Could not remove the durable Pair Execution Cell route copy.", exc_info=True)


# --------------------------------------------------------------------------- #
# The follower's frozen Pairing Acceptance payload: durable and reconstructable
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class FrozenAcceptance:
    """The Pairing Acceptance payload this follower froze for one proposal.

    It is persisted the instant it is sent with the pairing decision --
    *before* any route exists -- because every message that would otherwise
    carry it is one-shot.  If the follower misses its own route-established
    notice, or the leader misses the controller's forwarded copy, this record
    lets the follower rebuild and re-publish exactly the same block on the
    authoritative route without re-reading the broker balance and without
    weakening any proposal, route or hash check.
    """

    proposal_id: str
    payload: Mapping[str, object]
    payload_hash: str

    def balance(self) -> StartupBalance:
        return StartupBalance(
            amount_usd=str(self.payload["startup_balance_usd"]),
            account_currency=str(self.payload["account_currency"]),
        )

    def matches_own_hash(self) -> bool:
        return self.payload_hash == _payload_hash(self.payload)

    def canonical(self) -> dict[str, object]:
        return {
            "proposal_id": self.proposal_id,
            "payload": dict(self.payload),
            "payload_hash": self.payload_hash,
        }


def save_frozen_acceptance(path: Path, record: FrozenAcceptance) -> None:
    _atomic_write_json(path, record.canonical())


def load_frozen_acceptance(path: Path) -> FrozenAcceptance | None:
    """Recover the frozen payload, refusing one that fails its own hash."""

    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise TypeError
        payload = raw["payload"]
        if not isinstance(payload, dict):
            raise TypeError
        record = FrozenAcceptance(
            proposal_id=str(raw["proposal_id"]),
            payload=payload,
            payload_hash=str(raw["payload_hash"]),
        )
    except (OSError, ValueError, KeyError, TypeError):
        _LOGGER.warning("The durable Pairing Acceptance payload is unreadable.", exc_info=True)
        return None
    if not record.proposal_id or not record.matches_own_hash():
        _LOGGER.warning("The durable Pairing Acceptance payload does not match its own hash.")
        return None
    return record


def clear_frozen_acceptance(path: Path) -> None:
    for candidate in (path, path.with_name(f"{path.name}.tmp")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            continue
        except OSError:  # pragma: no cover - best effort local cleanup
            _LOGGER.warning("Could not remove the durable Pairing Acceptance payload.", exc_info=True)


# --------------------------------------------------------------------------- #
# The pairing's frozen startup balance, bound to the proposal that created it
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class FrozenBudget:
    """One pairing's frozen startup balance, bound to its proposal.

    The startup balance is read **once** and frozen into the pairing, and is
    never re-read while that pairing lives: re-reading it would silently
    resize a strategy the peer already copied verbatim and would change the
    canonical policy hash.  Persisting it *before* the proposal is sent (the
    leader) or the acceptance is sent (the follower) is what makes it
    recoverable when a crash lands between the controller committing the
    route and this Worker writing its own durable route copy.

    ``role`` and ``counterparty_worker_id`` bind the record to exactly one
    pairing, so a record left over from an abandoned proposal can never be
    attached to a different route, a different peer, or the opposite role.
    """

    proposal_id: str
    role: Role
    counterparty_worker_id: str
    startup_balance_usd: str
    account_currency: str = REQUIRED_ACCOUNT_CURRENCY

    def balance(self) -> StartupBalance:
        return StartupBalance(
            amount_usd=self.startup_balance_usd, account_currency=self.account_currency
        )

    def bound_to(
        self, *, role: Role, counterparty_worker_id: str, proposal_id: str | None
    ) -> bool:
        """Whether this record provably belongs to the pairing being adopted."""

        if self.role != role or self.counterparty_worker_id != counterparty_worker_id:
            return False
        return proposal_id is None or proposal_id == self.proposal_id

    def canonical(self) -> dict[str, object]:
        return {
            "proposal_id": self.proposal_id,
            "role": self.role,
            "counterparty_worker_id": self.counterparty_worker_id,
            "startup_balance_usd": self.startup_balance_usd,
            "account_currency": self.account_currency,
        }

    @property
    def record_hash(self) -> str:
        return _payload_hash(self.canonical())


def save_frozen_budget(path: Path, record: FrozenBudget) -> None:
    _atomic_write_json(path, {"record": record.canonical(), "record_hash": record.record_hash})


def load_frozen_budget(path: Path) -> FrozenBudget | None:
    """Recover the frozen budget, refusing one that fails its own integrity hash."""

    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            raise TypeError
        payload = raw["record"]
        if not isinstance(payload, dict):
            raise TypeError
        role = str(payload["role"])
        if role not in ("leader", "follower"):
            raise TypeError
        record = FrozenBudget(
            proposal_id=str(payload["proposal_id"]),
            role=cast(Role, role),
            counterparty_worker_id=str(payload["counterparty_worker_id"]),
            startup_balance_usd=str(payload["startup_balance_usd"]),
            account_currency=str(payload["account_currency"]),
        )
        declared = str(raw["record_hash"])
    except (OSError, ValueError, KeyError, TypeError):
        _LOGGER.warning("The durable frozen startup balance is unreadable.", exc_info=True)
        return None
    if (
        not record.proposal_id
        or not record.counterparty_worker_id
        or declared != record.record_hash
    ):
        _LOGGER.warning("The durable frozen startup balance failed its own integrity check.")
        return None
    return record


def clear_frozen_budget(path: Path) -> None:
    for candidate in (path, path.with_name(f"{path.name}.tmp")):
        try:
            candidate.unlink()
        except FileNotFoundError:
            continue
        except OSError:  # pragma: no cover - best effort local cleanup
            _LOGGER.warning("Could not remove the durable frozen startup balance.", exc_info=True)


# --------------------------------------------------------------------------- #
# Relay: translate the cell's internal envelope <-> the wire relay envelope
# --------------------------------------------------------------------------- #


class WorkerPairCellRelay:
    """Implements :class:`~abt.pair_cell.PairRelayAdapter` over one authenticated session.

    The wire envelope carries only the :class:`~abt.trader_protocol.PairRouteClaim`
    the controller authorizes against -- the ``route_id`` and the ordered
    Worker pair -- the two Worker identities, a diagnostic ``request_id``, and
    one opaque payload.  There is deliberately no ``trader_id``, no top-level
    attempt ID, no lease and no contract hash: the controller must not be able
    to correlate, deduplicate or interpret trading messages.
    """

    def __init__(
        self,
        session: PairCellWorkerSession,
        *,
        route: RouteAssignment,
        observer: Callable[[str, Mapping[str, object]], None] | None = None,
    ) -> None:
        self._session = session
        self._route = route
        self._observer = observer
        self.last_send_failed = False
        self._state_relay_request_ids: set[str] = set()

    def claim(self) -> dict[str, object]:
        return {
            "route_id": self._route.route_id,
            "leader_worker_id": self._route.leader_worker_id,
            "follower_worker_id": self._route.follower_worker_id,
        }

    def send(self, envelope: dict[str, object]) -> None:
        kind = str(envelope["kind"])
        inner = envelope["payload"]
        if self._observer is not None and isinstance(inner, Mapping):
            # Every outbound envelope is the cell's *own* durable evidence
            # about its own leg, emitted the moment the cell establishes it.
            # Observing it here is how this Worker learns its exact ticket
            # without racing the adapter's own broker polling cadence.
            try:
                self._observer(kind, inner)
            except Exception:  # pragma: no cover - observability must never break the relay
                _LOGGER.warning("Pair Execution Cell relay observer failed.", exc_info=True)
        wire_payload = {
            "protocol_version": envelope.get("protocol_version", PAIR_CELL_ENVELOPE_VERSION),
            "kind": kind,
            "route_id": envelope.get("route_id", self._route.route_id),
            "from_role": envelope.get("from_role"),
            "payload": inner,
        }
        payload_json = json.dumps(wire_payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        request_id = str(uuid4())
        wire_envelope = {
            "type": "pair_cell_relay",
            "protocol_version": PAIR_CELL_PROTOCOL_VERSION,
            "route": self.claim(),
            "from_worker_id": str(envelope["from_worker_id"]),
            "to_worker_id": str(envelope["to_worker_id"]),
            "request_id": request_id,
            "payload_hash": sha256(payload_json.encode("utf-8")).hexdigest(),
            "payload": wire_payload,
        }
        try:
            self._session.send_pair_relay(wire_envelope, request_id=request_id)
            if kind in _STATE_RELAY_KINDS:
                self._state_relay_request_ids.add(request_id)
                _LOGGER.debug(
                    "Pair Execution Cell relay queued: kind=%s request_id=%s target_worker_id=%s.",
                    kind,
                    request_id,
                    wire_envelope["to_worker_id"],
                )
        except Exception:
            # A relay send never propagates into the cell's decision path: the
            # authenticated-session loss is reported as a peer-session event,
            # which removes entry readiness without touching exposure this
            # Worker already owns.
            self.last_send_failed = True
            _LOGGER.warning("Pair Execution Cell relay send failed.", exc_info=True)

    def consume_state_relay_ack(self, request_id: object) -> bool:
        """Whether an acknowledgement belongs to a logged state envelope."""

        if not isinstance(request_id, str) or request_id not in self._state_relay_request_ids:
            return False
        self._state_relay_request_ids.remove(request_id)
        return True


def unwrap_pair_relay_envelope(envelope: Mapping[str, object]) -> dict[str, object] | None:
    """The inverse of :meth:`WorkerPairCellRelay.send`: wire envelope -> cell envelope.

    Returns ``None`` for a structurally malformed envelope rather than
    raising, since a delivery push must never crash the Worker's event loop;
    the cell's own protocol-version, ``route_id`` and peer-identity checks
    reject anything that slips through.
    """

    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return None
    kind, inner_payload = payload.get("kind"), payload.get("payload")
    protocol_version = payload.get("protocol_version")
    if (
        not isinstance(kind, str)
        or not isinstance(inner_payload, dict)
        or isinstance(protocol_version, bool)
        or not isinstance(protocol_version, int)
    ):
        return None
    route = envelope.get("route")
    route_id = payload.get("route_id")
    if not isinstance(route_id, str) and isinstance(route, Mapping):
        route_id = route.get("route_id")
    return {
        "protocol_version": protocol_version,
        "kind": kind,
        "route_id": route_id,
        "from_role": payload.get("from_role"),
        "from_worker_id": envelope.get("from_worker_id"),
        "to_worker_id": envelope.get("to_worker_id"),
        "payload": inner_payload,
    }


# --------------------------------------------------------------------------- #
# MT5: translate the cell's abstract broker payload into a genuine MT5 request
# --------------------------------------------------------------------------- #


class WorkerPairCellMT5:
    """Implements :class:`~abt.pair_cell.PairCellMT5` over the Worker's single MT5 handle."""

    def __init__(self, mt5: object) -> None:
        self._mt5 = mt5

    def account_info(self) -> object:
        return self._mt5.account_info()

    def positions_get(self) -> object:
        return self._mt5.positions_get()

    def orders_get(self) -> object:
        return self._mt5.orders_get()

    def symbol_info(self, symbol: str) -> object:
        return self._mt5.symbol_info(symbol)

    def order_calc_margin(self, request: Mapping[str, object]) -> object:
        """The cell's abstract margin request through the proven translation.

        The cell asks for margin per lot in its own vocabulary
        (``direction``/``symbol``/``volume``/``price``); this is the exact
        translation ``abt.worker.reconciliation`` already uses for the
        Trader-relay ``calc_margin`` read, so both paths stay identical.  An
        unusable answer raises, and the cell treats that as "this direction
        has no valid plan" rather than guessing a margin.
        """

        return _trader_margin(cast(ReadOnlyMT5, self._mt5), dict(request))

    def order_send(self, request: dict[str, object]) -> object:
        _require_terminal_trading_permission(self._mt5)
        broker_request = _pair_cell_broker_request(self._mt5, request)
        return self._mt5.order_send(broker_request)


def _pair_cell_broker_request(mt5: object, payload: dict[str, object]) -> dict[str, object]:
    operation = payload.get("type")
    if operation == "market":
        order: dict[str, object] = {"action": "market"}
        for field_name in ("symbol", "volume", "direction", "filling_mode"):
            if field_name in payload:
                order[field_name] = payload[field_name]
        if payload.get("sl") is not None and payload.get("tp") is not None:
            order["sl"], order["tp"] = payload["sl"], payload["tp"]
        return _broker_order_request(mt5, order)
    if operation == "modify_sl_tp":
        return _broker_order_request(
            mt5,
            {
                "action": "protect",
                "symbol": payload.get("symbol"),
                "position": payload.get("position"),
                "sl": payload.get("sl"),
                "tp": payload.get("tp"),
            },
        )
    if operation == "cancel":
        return {"action": getattr(mt5, "TRADE_ACTION_REMOVE"), "order": int(str(payload.get("ticket")))}
    if operation == "close":
        return _broker_close_request(mt5, int(str(payload.get("ticket"))), float(str(payload.get("volume"))))
    raise WorkerEnrollmentError(f"The Pair Execution Cell requested an invalid broker operation: {operation!r}.")


# --------------------------------------------------------------------------- #
# Quotes, broker-clock calibration and readiness: local facts only
# --------------------------------------------------------------------------- #


def read_local_quote(
    mt5: object,
    *,
    product_id: str,
    symbol: str,
    recovery_epoch: str,
    sequence: int,
    now: Callable[[], datetime],
    calibration: BrokerClockCalibration = _UNCALIBRATED,
) -> LocalQuoteEvent:
    """One local quote event with calibrated, millisecond broker tick time."""

    return read_local_quote_evidence(
        mt5,
        product_id=product_id,
        symbol=symbol,
        recovery_epoch=recovery_epoch,
        sequence=sequence,
        now=now,
        calibration=calibration,
    )[0]


def read_local_quote_evidence(
    mt5: object,
    *,
    product_id: str,
    symbol: str,
    recovery_epoch: str,
    sequence: int,
    now: Callable[[], datetime],
    calibration: BrokerClockCalibration = _UNCALIBRATED,
) -> tuple[LocalQuoteEvent, _TickCursor]:
    """One local quote event plus the *raw* tick identity it was read from.

    The ordinary live-state quote feed (``reconciliation._live_quote``) only
    reads ``symbol_info_tick().time`` (whole seconds), which is fine for a
    dashboard but far too coarse for a cell whose quote-age and calibrated
    cross-Worker skew budgets are configured in fractions of a second: a tick
    could be up to 999ms staler than it appears. This reads ``time_msc``
    first and only falls back to ``time`` when milliseconds are unavailable.

    ``broker_time`` stays exactly as MT5 reported it -- broker server time
    interpreted as a Unix epoch -- and the measured
    :class:`~abt.pair_cell.BrokerClockCalibration` travels with it, because
    the cell is what charges the offset and its residual uncertainty when it
    ages a quote and when it compares two brokers' clocks.

    The returned :class:`_TickCursor` is the raw identity of the tick MT5
    served.  A caller polling faster than the feed updates must use it to
    suppress republication: re-stamping an unchanged tick with a new sequence
    would burn relay bandwidth and misrepresent one repeated tick as a stream
    of distinct market evidence.
    """

    evidence = _evidence(mt5.symbol_info_tick(symbol), "symbol tick")
    bid, ask = evidence.get("bid"), evidence.get("ask")
    if (
        isinstance(bid, bool)
        or not isinstance(bid, (int, float))
        or isinstance(ask, bool)
        or not isinstance(ask, (int, float))
    ):
        raise WorkerEnrollmentError("The local MT5 terminal returned invalid symbol tick evidence.")
    epoch_milliseconds = _tick_epoch_milliseconds(evidence)
    quote = LocalQuoteEvent(
        product_id=product_id,
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        broker_time=datetime.fromtimestamp(epoch_milliseconds / 1000.0, UTC),
        local_receive_time=now(),
        recovery_epoch=recovery_epoch,
        sequence=sequence,
        calibration=calibration,
    )
    return quote, _TickCursor(epoch_milliseconds=epoch_milliseconds, bid=quote.bid, ask=quote.ask)


@dataclass(frozen=True, slots=True)
class _TickCursor:
    """The raw identity of one MT5 tick: its epoch and its executable prices."""

    epoch_milliseconds: int
    bid: Decimal
    ask: Decimal

    def advanced_from(self, previous: _TickCursor | None) -> bool:
        """Whether this is genuinely new market evidence, not a repeated read.

        A regressed broker epoch is never accepted: it is either a garbled
        read or a replayed tick, and treating it as new evidence would move
        the quote's freshness forward on the strength of older data.
        """

        if previous is None:
            return True
        if self.epoch_milliseconds < previous.epoch_milliseconds:
            return False
        # A whole-second ``time`` fallback cannot separate two ticks inside
        # the same second, so a changed executable price is also genuine
        # evidence of a new tick.
        return self != previous


def _tick_epoch_milliseconds(evidence: Mapping[str, object]) -> int:
    milliseconds = evidence.get("time_msc")
    if isinstance(milliseconds, (int, float)) and not isinstance(milliseconds, bool) and milliseconds > 0:
        return int(milliseconds)
    seconds = evidence.get("time")
    if isinstance(seconds, (int, float)) and not isinstance(seconds, bool) and seconds > 0:
        return int(seconds * 1000)
    raise WorkerEnrollmentError("The local MT5 terminal returned invalid symbol tick evidence.")


@dataclass(frozen=True, slots=True)
class BrokerClockSample:
    """One measured broker-clock calibration and when it was taken."""

    calibration: BrokerClockCalibration
    sampled_at: datetime


def sample_broker_clock_calibration(
    mt5: object,
    symbol: str,
    *,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], None] = _time.sleep,
    sample_count: int = _CALIBRATION_SAMPLE_COUNT,
    interval: timedelta = _CALIBRATION_SAMPLE_INTERVAL,
) -> BrokerClockSample | None:
    """Measure this broker's clock offset from UTC, or ``None``.

    Follows the Worker's existing MT5 time-calibration prior art
    (``abt.mt5.timecalibration._market_samples``): several spaced samples,
    every one of which must carry a **strictly advancing** tick epoch, and the
    whole set is discarded otherwise.  That requirement is the safety-critical
    part here.  A single sample cannot distinguish "the broker clock is behind
    UTC" from "this tick is simply old": a tick stale by *s* seconds yields an
    offset *s* too small, which shifts the calibrated timeline *later*, makes
    every subsequent quote look fresher than it is, and would keep doing so
    for a whole refresh cycle.  A frozen or replayed feed therefore never
    calibrates at all -- the cell refuses those quotes instead of aging them
    against a clock that absorbed their staleness.

    ``offset_seconds`` is the median sample offset (prior art).
    ``error_seconds`` is deliberately pessimistic: it charges the residual
    between the median and the *least stale* (largest) sample, the dispersion
    across samples, and half the widest read span, so any staleness the median
    still absorbs is repaid as extra measured age rather than silently
    widening the freshness window.
    """

    if sample_count < 2:
        raise ValueError("A broker clock calibration needs at least two advancing samples.")
    offsets: list[float] = []
    spans: list[float] = []
    previous_epoch: int | None = None
    observed_at: datetime | None = None
    seconds = interval.total_seconds()
    for index in range(sample_count):
        if index and seconds > 0:
            sleep(seconds)
        before = now()
        try:
            evidence = _evidence(mt5.symbol_info_tick(symbol), "symbol tick")
            epoch_milliseconds = _tick_epoch_milliseconds(evidence)
        except WorkerEnrollmentError:
            return None
        after = now()
        if previous_epoch is not None and epoch_milliseconds <= previous_epoch:
            return None  # frozen, halted or replayed feed: never calibrate from it
        previous_epoch = epoch_milliseconds
        midpoint = before + (after - before) / 2
        offsets.append(epoch_milliseconds / 1000.0 - midpoint.timestamp())
        spans.append((after - before).total_seconds())
        observed_at = after
    if observed_at is None:
        return None
    offset = float(median(offsets))
    dispersion = max(offsets) - min(offsets)
    if dispersion > _MAX_CALIBRATION_DISPERSION_SECONDS:
        return None  # the samples disagree too much to bound the clock usefully
    residual = max(0.0, max(offsets) - offset)
    error = residual + dispersion + max(spans) / 2
    return BrokerClockSample(
        calibration=BrokerClockCalibration(
            offset_seconds=offset, error_seconds=round(error, 6), status="calibrated"
        ),
        sampled_at=observed_at,
    )


def _positive_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def read_readiness_facts(
    mt5: ReadOnlyMT5,
    *,
    login: int,
    server: str,
    now: datetime,
    minimum_margin_headroom_usd: Decimal = Decimal("0"),
) -> ReadinessFactsEvent:
    """Continuously-maintained local account facts; never a signal-time read.

    Mirrors the same broker-evidence rules ``reconciliation``/``pair_cell``
    already enforce everywhere else: MT5 ``None``/malformed/unavailable
    records are read failures, never an empty or ready fact.
    """

    try:
        terminal = _evidence(mt5.terminal_info(), "terminal")
        terminal_connected = bool(terminal.get("connected") is True and terminal.get("trade_allowed") is not False)
    except WorkerEnrollmentError:
        terminal_connected = False
    orders = _optional_records(mt5.orders_get())
    positions = _optional_records(mt5.positions_get())
    margin_ok = False
    try:
        account = _evidence(mt5.account_info(), "account")
        if account.get("login") == login and account.get("server") == server:
            margin_free = account.get("margin_free")
            if _positive_number(margin_free) or (isinstance(margin_free, (int, float)) and margin_free == 0):
                margin_ok = Decimal(str(margin_free)) >= minimum_margin_headroom_usd
    except WorkerEnrollmentError:
        margin_ok = False
    return ReadinessFactsEvent(
        account_login=str(login),
        account_server=server,
        terminal_connected=terminal_connected,
        orders=orders,
        positions=positions,
        margin_headroom_ok=margin_ok,
        observed_at=now,
    )


def _optional_records(value: object) -> list[dict[str, object]] | None:
    if value is None:
        return None
    if not isinstance(value, (list, tuple)):
        return None
    try:
        return [dict(_evidence(item, "record")) for item in value]
    except WorkerEnrollmentError:
        return None


def _decimal_text(value: object) -> str:
    """One canonical decimal spelling for every catalog and plan number."""

    amount = Decimal(str(value))
    if amount == amount.to_integral_value():
        return str(int(amount))
    return format(amount.normalize(), "f")


def _optional_decimal_text(value: object) -> str:
    """A canonical decimal spelling, or ``""`` when the broker value is unusable.

    An empty string is deliberate rather than a substituted zero: the cell's
    compatibility rules read it as "catalog economics are not usable numbers"
    and exclude that symbol, instead of admitting one on a number nobody
    actually reported.
    """

    if isinstance(value, bool) or not isinstance(value, (int, float, str, Decimal)):
        return ""
    try:
        return _decimal_text(value)
    except (InvalidOperation, ValueError, TypeError):
        return ""


# --------------------------------------------------------------------------- #
# Initial Compatible Product Discovery: this account's full MT5 catalog
# --------------------------------------------------------------------------- #


def read_catalog_entries(mt5: object) -> tuple[CatalogEntry, ...]:
    """Read this account's whole MT5 catalog and specification for discovery.

    Everything the broker exposes is carried through **raw** where the cell's
    compatibility rules care about rawness: ``trade_mode`` and
    ``trade_calc_mode`` are passed exactly as MT5 reported them, so a missing,
    non-integer or boolean ``trade_mode`` invalidates the whole catalog inside
    :func:`~abt.pair_cell.discover_compatible_products` rather than quietly
    dropping one symbol here.  Economics that cannot be spelled as a decimal
    become ``""``, which excludes that candidate for an explicit reason.

    The *whole* catalog is read on every cycle so both Workers always hold a
    current summary of what their brokers actually list.  That never expands
    the frozen universe by itself -- the cell admits a newly appeared symbol
    only through an explicit rediscovery -- but it does mean an operator's
    rediscovery sees the same catalogs on both sides instead of one stale one.
    An unavailable catalog raises, and discovery then fails closed.
    """

    raw = getattr(mt5, "symbols_get")()
    if raw is None or not isinstance(raw, (list, tuple)):
        raise WorkerEnrollmentError("The local MT5 terminal returned no product catalog.")
    records: list[tuple[str, Mapping[str, object]]] = []
    for record in raw:
        evidence = _maybe_evidence(record)
        if evidence is None:
            raise WorkerEnrollmentError("The local MT5 terminal returned an invalid product catalog.")
        name = evidence.get("name") or evidence.get("symbol")
        if not isinstance(name, str) or not name:
            raise WorkerEnrollmentError(
                "The local MT5 terminal returned a catalog entry without a symbol name."
            )
        records.append((name, evidence))
    entries: list[CatalogEntry] = []
    seen: set[str] = set()
    for name, listed in records:
        if name in seen:
            continue
        seen.add(name)
        specification = _symbol_specification_evidence(mt5, name)
        evidence: dict[str, object] = {**dict(listed), **dict(specification or {})}
        entries.append(_catalog_entry(name, evidence))
    return tuple(entries)


def _symbol_specification_evidence(mt5: object, symbol: str) -> Mapping[str, object] | None:
    select = getattr(mt5, "symbol_select", None)
    if callable(select):
        try:
            select(symbol, True)
        except Exception:  # pragma: no cover - best effort Market Watch preload
            _LOGGER.debug("Could not select %s in MT5 Market Watch.", symbol, exc_info=True)
    try:
        return _maybe_evidence(mt5.symbol_info(symbol))
    except Exception:
        _LOGGER.debug("Could not read the MT5 specification for %s.", symbol, exc_info=True)
        return None


def _catalog_entry(name: str, evidence: Mapping[str, object]) -> CatalogEntry:
    digits = evidence.get("digits")
    contract_size = evidence.get("trade_contract_size")
    if contract_size is None:
        contract_size = evidence.get("contract_size")
    base_currency = evidence.get("currency_base")
    profit_currency = evidence.get("currency_profit")
    return CatalogEntry(
        name=name,
        # Raw on purpose: the catalog-level ``trade_mode == 4`` precondition
        # and its invalid-catalog rule belong to the cell, not to this reader.
        trade_mode=evidence.get("trade_mode"),
        trade_calc_mode=evidence.get("trade_calc_mode"),
        digits=digits if isinstance(digits, int) and not isinstance(digits, bool) else 0,
        point=_optional_decimal_text(evidence.get("point")),
        trade_tick_size=_optional_decimal_text(evidence.get("trade_tick_size")),
        contract_size=_optional_decimal_text(contract_size),
        volume_min=_optional_decimal_text(evidence.get("volume_min")),
        volume_step=_optional_decimal_text(evidence.get("volume_step")),
        volume_max=_optional_decimal_text(evidence.get("volume_max")),
        allowed_directions=_catalog_directions(evidence),
        filling_modes=_catalog_filling_modes(evidence),
        base_currency=base_currency if isinstance(base_currency, str) else "",
        profit_currency=profit_currency if isinstance(profit_currency, str) else "",
    )


def _catalog_directions(evidence: Mapping[str, object]) -> tuple[Direction, ...]:
    declared = evidence.get("allowed_directions")
    if isinstance(declared, (list, tuple)):
        return tuple(
            cast(Direction, value) for value in declared if value in ("LONG", "SHORT")
        )
    order_mode = evidence.get("order_mode")
    if isinstance(order_mode, int) and not isinstance(order_mode, bool):
        directions = [
            direction
            for bit, direction in ((1, "LONG"), (2, "SHORT"))
            if order_mode & bit
        ]
        if directions:
            return tuple(cast(list[Direction], directions))
    trade_mode = evidence.get("trade_mode")
    if isinstance(trade_mode, int) and not isinstance(trade_mode, bool):
        mapped = {1: ("LONG",), 2: ("SHORT",), 4: ("LONG", "SHORT")}.get(trade_mode)
        if mapped is not None:
            return cast(tuple[Direction, ...], mapped)
    return ()


def _catalog_filling_modes(evidence: Mapping[str, object]) -> tuple[FillingMode, ...]:
    declared = evidence.get("filling_modes")
    if isinstance(declared, (list, tuple)):
        return tuple(cast(FillingMode, value) for value in declared if value in ("FOK", "IOC"))
    mask = evidence.get("filling_mode")
    if isinstance(mask, int) and not isinstance(mask, bool):
        modes = [
            mode
            for bit, mode in ((_MT5_FILLING_FOK_BIT, "FOK"), (_MT5_FILLING_IOC_BIT, "IOC"))
            if mask & bit
        ]
        return tuple(cast(list[FillingMode], modes))
    return ()


def _maybe_evidence(value: object) -> Mapping[str, object] | None:
    if isinstance(value, Mapping):
        return dict(value)
    as_dict = getattr(value, "_asdict", None)
    if callable(as_dict):
        try:
            return dict(as_dict())
        except Exception:  # pragma: no cover - defensive against odd MT5 shims
            return None
    try:
        attributes = vars(value)
    except TypeError:
        return None
    return dict(attributes) if isinstance(attributes, dict) else None


# --------------------------------------------------------------------------- #
# Automatic follower acceptance: purely local, current, broker-verified facts
# --------------------------------------------------------------------------- #


def _payload_hash(payload: Mapping[str, object]) -> str:
    """The integrity hash the controller forwards without ever interpreting it."""

    return sha256(
        json.dumps(payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def evaluate_follower_acceptance(
    *,
    unpaired: bool,
    facts: ReadinessFactsEvent | None,
    calibration: BrokerClockCalibration,
    effect_journal: WorkerEffectJournal | None,
    balance: StartupBalance | None,
    balance_error: str | None = None,
) -> str | None:
    """``None`` when accepting is provably safe, otherwise the refusal reason.

    Every condition is local, current and broker-verified.  It is never a
    policy judgement, never a human approval, and an unmet condition is an
    explicit reasoned refusal that never becomes an implied acceptance.
    """

    if not unpaired:
        return "this Worker already appears on a Pair Execution Cell route"
    if facts is None:
        return "no local readiness facts have been observed yet"
    if not facts.terminal_connected:
        return "the local MT5 terminal is not usable"
    if facts.orders is None or facts.positions is None:
        return "the broker order/position read is unavailable"
    if facts.orders or facts.positions:
        return "this account is not broker-verified empty"
    if effect_journal is not None:
        try:
            unresolved = effect_journal.unresolved()
        except EffectJournalError:
            return "the local effect journal is unhealthy"
        if unresolved:
            return "a local broker effect is unresolved"
    if not calibration.usable:
        return "no current broker clock calibration is available"
    if balance is None:
        return balance_error or "a positive USD account balance is unreadable"
    return None


# --------------------------------------------------------------------------- #
# Orchestrator: pairing, discovery, per-loop pumping, unpair, and shutdown
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class PairCellPollingConfig:
    quote_interval: timedelta = timedelta(milliseconds=200)
    readiness_interval: timedelta = timedelta(milliseconds=250)
    broker_snapshot_interval: timedelta = timedelta(seconds=1)
    sizing_refresh_interval: timedelta = timedelta(hours=1)
    clock_sample_interval: timedelta = _CALIBRATION_SAMPLE_INTERVAL
    minimum_margin_headroom_usd: Decimal = Decimal("0")
    #: How often an unresolved route-metadata disagreement re-reads the
    #: controller's authoritative record instead of latching forever.
    route_sync_interval: timedelta = timedelta(seconds=30)
    #: How often a routed follower whose pair has agreed no policy yet
    #: re-publishes its frozen Pairing Acceptance payload.
    acceptance_republish_interval: timedelta = timedelta(seconds=30)
    #: How often fresh safe-unpair evidence is re-derived while ``UNPAIRING``
    #: or while reconciling an absent controller route.
    safe_unpair_probe_interval: timedelta = timedelta(seconds=1)


@dataclass(frozen=True, slots=True)
class PairCellStartupOptions:
    """The conceptual command-line surface, as the runtime receives it.

    ``role`` is the desired role on this Worker's *first unpaired
    connection*; omitting it means "available follower".  A durable route
    always wins over it, and a contradicting declaration is a diagnostic
    rather than a reassignment.
    """

    role: Role | None = None
    follower_worker_id: str | None = None
    interactive: bool = False
    select_follower: Callable[[Sequence[Mapping[str, object]]], str | None] | None = None
    unpair: bool = False
    cancel_unpair: bool = False
    rediscover: bool = False


@dataclass(slots=True)
class _OwnedLeg:
    attempt_id: str
    symbol: str


class PairCellRuntime:
    """Owns one Worker's pairing, Pair Execution Cell construction, restart
    recovery, catalog/clock refresh, per-loop event pumping, safe-unpair
    signalling, and shutdown.

    Constructing this is always safe and cheap.  A Worker with no
    configuration file is an available follower with the documented
    synthesized defaults; a Worker that is not (yet) on a route simply has no
    cell, and :meth:`pump`/:meth:`drain_relay` keep running the pairing
    control plane instead of trading.
    """

    def __init__(
        self,
        *,
        worker_id: str,
        db_path: Path,
        session: PairCellWorkerSession,
        mt5: object,
        effect_journal: WorkerEffectJournal,
        recovery_epoch: str,
        login: int,
        server: str,
        config: PairCellConfig | None = None,
        config_path: Path | None = None,
        options: PairCellStartupOptions = PairCellStartupOptions(),
        polling: PairCellPollingConfig = PairCellPollingConfig(),
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = _time.sleep,
    ) -> None:
        self._worker_id = worker_id
        self._db_path = db_path
        self._route_path = db_path.with_suffix(".route.json")
        self._session = session
        self._raw_mt5 = mt5
        self._effect_journal = effect_journal
        self._recovery_epoch = recovery_epoch
        self._login, self._server = login, server
        self._options = options
        self._polling = polling
        self._now = now
        self._sleep = sleep
        # A malformed or over-reaching configuration file is a startup error,
        # never a partially applied file.
        self._config = config if config is not None else load_pair_cell_config(config_path)

        self._cell: PairExecutionCell | None = None
        self._relay: WorkerPairCellRelay | None = None
        self._limits: WorkerRiskLimits | None = None
        self._durable_route: DurableRouteRecord | None = load_durable_route(self._route_path)
        self._acceptance_path = db_path.with_suffix(".acceptance.json")
        self._frozen_acceptance: FrozenAcceptance | None = load_frozen_acceptance(self._acceptance_path)
        self._budget_path = db_path.with_suffix(".budget.json")
        self._frozen_budget: FrozenBudget | None = load_frozen_budget(self._budget_path)
        self._startup_balance: StartupBalance | None = None
        self._balance_error: str | None = None
        self._fail_closed_reason: str | None = None
        self._acceptance_wait: str | None = None
        self._pairing_state = "unstarted"
        self._pairing_reason = ""
        self._proposal_id: str | None = None
        self._proposal_attempts = 0
        self._published_state_version: int | None = None
        self._asserted_state_version: int | None = None
        self._probed_state_version: int | None = None
        self._operator_requests_applied = False
        self._exit_after_safe_unpair = False
        self._published_policy = False
        self._next_route_sync_at = epoch_placeholder = now()
        self._next_acceptance_publication = epoch_placeholder
        self._next_safe_unpair_probe = epoch_placeholder
        self._next_acceptance_diagnostic = epoch_placeholder
        self._last_state_diagnostic: tuple[object, ...] | None = None
        self._pending_budget: tuple[str, StartupBalance] | None = None
        self._pairing_deadline: datetime | None = None

        self._sequence = 0
        self._catalog_entries: tuple[CatalogEntry, ...] | None = None
        self._catalog_generation = 0
        self._fed_catalog_generation = 0
        self._clock_sample: BrokerClockSample | None = None
        self._refresh_cycle = 0
        self._peer_session_connected = True
        self._fed_peer_session = True
        self._owned_legs: dict[str, _OwnedLeg] = {}
        self._realized_reported: dict[str, None] = {}
        self._observed_open: dict[str, None] = {}
        self._quote_cursors: dict[str, _TickCursor] = {}
        self._local_facts: ReadinessFactsEvent | None = None
        epoch = now()
        self._next_quote_at = epoch
        self._next_readiness_at = epoch
        self._next_snapshot_at = epoch
        self._next_refresh_at = epoch

        record = self._durable_route
        if record is not None:
            # A durable route always wins over the startup role default and
            # any ``--pair-cell-role`` flag; a contradicting declaration is a
            # diagnostic and never silently changes who leads.
            if options.role is not None and options.role != record.role:
                _LOGGER.warning(
                    "The declared Pair Execution Cell role %r contradicts the durable route's assigned"
                    " role %r; the durable route wins and the declaration is ignored.",
                    options.role,
                    record.role,
                )
            self._construct_cell(record)
            _LOGGER.debug(
                "Pair Execution Cell restored: route_id=%s role=%s peer_worker_id=%s.",
                record.route_id,
                record.role,
                record.assignment().peer_of(worker_id),
            )

    # -- observability ------------------------------------------------------ #

    @property
    def enabled(self) -> bool:
        """Whether an authoritative route exists and the cell is constructed."""

        return self._cell is not None

    @property
    def cell(self) -> PairExecutionCell | None:
        return self._cell

    @property
    def role(self) -> Role | None:
        return None if self._durable_route is None else self._durable_route.role

    @property
    def peer_worker_id(self) -> str | None:
        record = self._durable_route
        if record is None:
            return None
        return record.assignment().peer_of(self._worker_id)

    @property
    def route_id(self) -> str | None:
        return None if self._durable_route is None else self._durable_route.route_id

    @property
    def pairing_state(self) -> str:
        return self._pairing_state

    @property
    def should_exit(self) -> bool:
        """Whether an invoked ``--pair-cell-unpair`` completed safely."""

        return self._exit_after_safe_unpair

    @property
    def pairing_diagnostic(self) -> str:
        """The actionable reason this Worker is authenticated but not trading."""

        return self._fail_closed_reason or self._acceptance_wait or self._pairing_reason

    @property
    def acceptance_diagnostic(self) -> str | None:
        """Why this pair cannot agree a policy yet, for an operator surface."""

        return self._acceptance_wait

    @property
    def config(self) -> PairCellConfig:
        return self._config

    @property
    def policy_accepted(self) -> bool:
        return self._cell is not None and self._cell.enforced_risk_limits() is not None

    def enforced_risk_limits(self) -> WorkerRiskLimits | None:
        """The risk configuration this cell actually enforces.

        Always the accepted canonical policy's copy for this role, never the
        locally declared one, so an operator surface reading this can never
        show a budget the pair did not agree on.
        """

        return None if self._cell is None else self._cell.enforced_risk_limits()

    def declared_risk_limits(self) -> WorkerRiskLimits | None:
        return self._limits

    def universe_generation(self) -> int | None:
        return None if self._cell is None else self._cell.universe_generation()

    def accumulated_realized_loss_usd(self) -> Decimal:
        """This Worker's own realized loss so far on the current New York day."""

        return Decimal(0) if self._cell is None else self._cell.accumulated_realized_loss_usd()

    def quarantined_products(self) -> tuple[str, ...]:
        return () if self._cell is None else self._cell.quarantined_products()

    def broker_clock_calibration(self, now: datetime) -> BrokerClockCalibration:
        """This broker's clock calibration, downgraded once it goes stale.

        A calibration that outlived two refresh cycles is reported as
        ``stale`` rather than reused: the cell then refuses the quote outright
        instead of aging it against a clock nobody has re-measured.
        """

        sample = self._clock_sample
        if sample is None:
            return _UNCALIBRATED
        if now - sample.sampled_at > 2 * self._polling.sizing_refresh_interval:
            return replace(sample.calibration, status="stale")
        return sample.calibration

    # -- operator surfaces (available on either role) ----------------------- #

    def request_safe_unpair(self) -> bool:
        """Ask the controller to move this Worker's route into ``UNPAIRING``."""

        return self._send_unpair_command("enter")

    def cancel_safe_unpair(self) -> bool:
        """Return this route to normal operation, subject to ordinary admission."""

        return self._send_unpair_command("cancel")

    def request_rediscovery(self, *, actor: str = "operator", reason: str = "") -> PairResult | None:
        """Ask for an explicit rediscovery on this route's current ``route_id``."""

        cell = self._cell
        if cell is None:
            self._pairing_reason = "this Worker is not on a Pair Execution Cell route"
            return None
        # Rediscovery must run against this account's current catalog, not the
        # one that happened to be read an hour ago.
        observed_at = self._now()
        if self._refresh_local_evidence(observed_at):
            self._next_refresh_at = observed_at + self._polling.sizing_refresh_interval
        self._feed_catalog(observed_at)
        return cell.request_rediscovery(actor=actor, reason=reason)

    def relist_available_followers(self) -> bool:
        """Re-enter interactive follower selection after an empty list."""

        if self._cell is not None or self._fail_closed_reason is not None:
            return False
        self._pairing_state = "selecting"
        self._proposal_attempts = 0
        return True

    def _send_unpair_command(self, action: str) -> bool:
        record = self._durable_route
        if record is None:
            self._pairing_reason = "this Worker is not on a Pair Execution Cell route"
            return False
        try:
            self._session.send_pair_cell_unpair(route_id=record.route_id, action=action)
        except WorkerEnrollmentError:
            _LOGGER.warning("The Pair Execution Cell unpair command could not be sent.", exc_info=True)
            return False
        return True


    # -- pairing control plane ---------------------------------------------- #

    def _drain_control(self) -> None:
        """Process every queued pairing reply and notification, then advance."""

        for reply in self._session.drain_pair_cell_results():
            try:
                self._handle_control_result(reply)
            except WorkerEnrollmentError:
                _LOGGER.warning("A Pair Execution Cell pairing reply could not be served.", exc_info=True)
        for push in self._session.drain_pair_cell_pushes():
            try:
                self._handle_control_push(push)
            except WorkerEnrollmentError:
                _LOGGER.warning("A Pair Execution Cell pairing push could not be served.", exc_info=True)
        self._advance_pairing()

    def _advance_pairing(self) -> None:
        if self._cell is not None or self._fail_closed_reason is not None:
            return
        if self._pairing_state == "unstarted":
            try:
                self._session.declare_pair_cell_role(self._options.role)
            except WorkerEnrollmentError:
                _LOGGER.warning("The Pair Execution Cell role declaration failed.", exc_info=True)
                return
            self._pairing_state = "declaring"
            return
        if self._pairing_state == "selecting":
            self._begin_selection()
            return
        if self._pairing_state in ("awaiting_decision", "deciding"):
            # Every message that would tell this Worker the pairing completed
            # is one-shot.  Re-reading the authoritative route on a bounded
            # cadence is what stops a lost decision reply or route-established
            # notice from stranding a Worker that is, in fact, already routed.
            now = self._now()
            if now < self._next_route_sync_at:
                return
            self._next_route_sync_at = now + self._polling.route_sync_interval
            try:
                self._session.request_pair_cell_route_sync()
            except WorkerEnrollmentError:
                _LOGGER.warning("The authoritative route sync request failed.", exc_info=True)

    def _begin_selection(self) -> None:
        """Leader-only: select one follower, by flag or interactively."""

        named = self._options.follower_worker_id
        if named:
            if self._proposal_attempts:
                self._idle_unpaired(
                    f"the follower named by --follower-worker-id ({named}) could not be paired:"
                    f" {self._pairing_reason}"
                )
                return
            self._propose(named)
            return
        if not self._options.interactive or self._options.select_follower is None:
            # Never block on input, never select implicitly, never downgrade
            # to follower, never exit: stay authenticated and unpaired with an
            # actionable diagnostic.
            self._idle_unpaired(
                "this leader has no follower selection: it is not attached to an interactive"
                " terminal, so restart it with --follower-worker-id <worker_id> to pair"
                " unattended"
            )
            return
        try:
            self._session.request_available_pair_followers()
        except WorkerEnrollmentError:
            _LOGGER.warning("The available-follower listing request failed.", exc_info=True)
            return
        self._pairing_state = "listing"

    def _propose(self, follower_worker_id: str) -> None:
        # The startup balance is read here, once, and frozen into this
        # pairing.  Persisting it before the route can exist is what lets a
        # leader that crashes between the controller committing the route and
        # its own durable route write recover the same budget instead of
        # re-reading a balance that may since have moved.
        balance = self._read_startup_balance()
        if balance is None:
            self._idle_unpaired(
                self._balance_error
                or "a positive USD startup balance is required before this Worker may propose"
            )
            return
        self._proposal_attempts += 1
        try:
            self._session.propose_pair_cell_pairing(follower_worker_id)
        except WorkerEnrollmentError:
            _LOGGER.warning("The Pair Execution Cell pairing proposal failed.", exc_info=True)
            return
        self._pending_budget = (follower_worker_id, balance)
        self._pairing_state = "proposing"

    def _idle_unpaired(self, reason: str) -> None:
        """Remain authenticated and unpaired with an actionable diagnostic."""

        self._pairing_state = "unpaired"
        self._pairing_reason = reason
        _LOGGER.warning("This Worker is authenticated but unpaired: %s.", reason)

    def _fail_closed(self, reason: str) -> None:
        self._fail_closed_reason = reason
        self._pairing_state = "unpaired"
        self._pairing_reason = reason
        _LOGGER.error("The Pair Execution Cell failed closed: %s.", reason)

    def _pairing_conflict(self, reason: str) -> None:
        """A deterministic conflict: re-list and select again, never wait it out."""

        self._pairing_reason = reason
        self._proposal_id = None
        # A pairing that did not become a route leaves no frozen budget or
        # acceptance behind, so nothing can later be attached to a different
        # route or a different peer.
        self._discard_proposal_records()
        if self._options.follower_worker_id or not self._options.interactive:
            self._idle_unpaired(reason)
            return
        self._pairing_state = "selecting"

    def _handle_control_result(self, reply: Mapping[str, object]) -> None:
        message_type = reply.get("type")
        accepted = reply.get("accepted") is True
        reason = str(reply.get("reason") or "")

        if message_type == "pair_cell_role_result":
            if not accepted:
                self._idle_unpaired(f"the controller refused this role declaration: {reason}")
                return
            route = reply.get("route")
            if isinstance(route, Mapping):
                self._adopt_controller_route(route)
                return
            if self._options.unpair:
                self._pairing_state = "unpaired"
                self._pairing_reason = "no Pair Execution Cell route exists to unpair"
                self._exit_after_safe_unpair = True
                _LOGGER.info("No Pair Execution Cell route exists; the safe-unpair request is already complete.")
                return
            declared = reply.get("declared_role")
            resolved = reply.get("role")
            if resolved == "leader":
                self._pairing_state = "selecting"
                self._begin_selection()
                return
            self._pairing_state = "available"
            self._pairing_reason = (
                "available follower"
                if declared is not None
                else "available follower (no role was declared)"
            )
            return

        if message_type == "pair_cell_available_followers_result":
            if not accepted:
                self._idle_unpaired(f"the available-follower listing was refused: {reason}")
                return
            followers = reply.get("followers")
            self._select_from_listing(followers if isinstance(followers, list) else [])
            return

        if message_type == "pair_cell_pairing_proposal_result":
            if not accepted:
                self._pairing_conflict(f"the pairing proposal was refused: {reason}")
                return
            proposal_id = reply.get("proposal_id")
            # The leader remembers the proposal it made, so a forwarded
            # Pairing Acceptance payload must be bound to that proposal
            # before it may be transcribed.
            self._proposal_id = proposal_id if isinstance(proposal_id, str) else None
            if self._proposal_id is not None and self._pending_budget is not None:
                # The route cannot exist until the follower decides, so this
                # lands strictly before any route this proposal could create.
                counterparty, balance = self._pending_budget
                self._freeze_budget(
                    proposal_id=self._proposal_id,
                    role="leader",
                    counterparty_worker_id=counterparty,
                    balance=balance,
                )
            self._pairing_state = "awaiting_decision"
            self._pairing_deadline = self._now() + timedelta(seconds=PAIRING_RESERVATION_GRACE_SECONDS)
            self._pairing_reason = (
                f"awaiting the follower's decision on proposal {proposal_id}"
            )
            return

        if message_type == "pair_cell_pairing_decision_result":
            if not accepted:
                self._proposal_id = None
                self._discard_proposal_records()
                self._pairing_state = "available"
                self._pairing_reason = f"the pairing decision was refused: {reason}"
                return
            route = reply.get("route")
            if reply.get("outcome") == "paired" and isinstance(route, Mapping):
                self._adopt_controller_route(route, proposal_id=self._proposal_id)
                return
            self._proposal_id = None
            self._pairing_state = "available"
            return

        if message_type == "pair_cell_route_sync_result":
            if not accepted:
                return
            route = reply.get("route")
            if isinstance(route, Mapping):
                self._adopt_controller_route(route)
            elif self._cell is not None:
                # The controller holds no route for this Worker.  That is
                # either a completed safe unpair this Worker was offline for,
                # or a metadata disagreement -- never an inference about
                # exposure.
                self._reconcile_absent_controller_route()
            else:
                self._reconcile_absent_pairing()
            return

        if message_type == "pair_cell_unpair_result":
            if not accepted:
                self._pairing_reason = f"the unpair command was refused: {reason}"
                return
            route = reply.get("route")
            if isinstance(route, Mapping):
                self._feed_route_metadata(route)
            return

        if message_type == "pair_cell_unpair_assertion_result":
            if accepted and reply.get("route_removed") is True:
                self._route_removed("safe_unpair")
            elif not accepted:
                # A refused assertion is re-produced from fresh evidence.
                self._asserted_state_version = None
            return

        if message_type == "pair_cell_state_version_result" and not accepted:
            self._published_state_version = None

    def _reconcile_absent_pairing(self) -> None:
        """No route yet, and the controller has none for this Worker either.

        While a proposal could still be in flight this says nothing: the
        decision and the route creation are separate controller transactions
        and a sync can legitimately land between them.  Only once the pairing
        reservation could not possibly still be live is the pairing treated as
        abandoned, and only then is the frozen acceptance discarded -- doing it
        sooner could throw away the frozen startup balance of a route that was
        in fact created a moment later.
        """

        if self._pairing_state not in ("awaiting_decision", "deciding"):
            return
        if self._pairing_deadline is not None and self._now() < self._pairing_deadline:
            return
        self._pairing_deadline = None
        self._discard_proposal_records()
        self._proposal_id = None
        reason = "the pairing reservation expired without a route"
        if self._pairing_state == "deciding":
            self._pairing_state = "available"
            self._pairing_reason = reason
            return
        self._pairing_conflict(reason)

    def _reconcile_absent_controller_route(self) -> None:
        """The controller holds no route while this Worker still has a copy.

        There is exactly one safe way to conclude the route genuinely went
        away: this Worker had already durably entered ``UNPAIRING``, and fresh
        local evidence *right now* still proves its own account
        broker-verified empty with every attempt and local effect terminal.
        That is the same assertion the controller would have arbitrated, so a
        safe unpair that completed while this Worker was offline is adopted
        rather than stranding it forever.

        Anything else -- a route that was never unpairing, or an account that
        cannot presently prove itself empty and terminal -- is a metadata
        reconciliation that removes entry readiness and never discards,
        closes, hides or infers local exposure.
        """

        cell, record = self._cell, self._durable_route
        if cell is None or record is None:
            return
        if record.state != "UNPAIRING":
            self._feed_route_metadata(None)
            return
        assertion = cell.safe_unpair_assertion()
        if not assertion.safe or assertion.route_id != record.route_id:
            self._feed_route_metadata(None)
            _LOGGER.warning(
                "The controller holds no route for %s, but this Worker cannot prove a completed"
                " safe unpair from fresh local evidence (%s); reconciling fail-closed.",
                record.route_id,
                assertion.reason or "no reason reported",
            )
            return
        _LOGGER.info(
            "Adopting the safe unpair of route %s that completed while this Worker was offline;"
            " fresh local evidence still proves this account broker-verified empty and terminal.",
            record.route_id,
        )
        self._route_removed("safe_unpair_completed_while_offline")

    def _select_from_listing(self, followers: Sequence[object]) -> None:
        listed = [entry for entry in followers if isinstance(entry, Mapping)]
        if not listed:
            self._idle_unpaired(
                "the controller listed no connected, available, unpaired follower; this leader"
                " stays authenticated and unpaired and may list again later"
            )
            return
        selector = self._options.select_follower
        if selector is None:  # pragma: no cover - guarded by _begin_selection
            self._idle_unpaired("no interactive follower selection is available")
            return
        try:
            chosen = selector(listed)
        except Exception:
            _LOGGER.warning("Interactive follower selection failed.", exc_info=True)
            chosen = None
        if not chosen:
            self._idle_unpaired("no follower was selected from the available-follower list")
            return
        self._propose(str(chosen))

    def _handle_control_push(self, push: Mapping[str, object]) -> None:
        message_type = push.get("type")
        if message_type == "pair_cell_pairing_proposed":
            self._answer_pairing_proposal(push)
            return
        if message_type in (
            "pair_cell_pairing_refused",
            "pair_cell_pairing_failed",
            "pair_cell_pairing_reservation_released",
        ):
            if self._cell is not None:
                return
            reason = str(push.get("reason") or message_type)
            self._proposal_id = None
            # A released reservation leaves no durable local pairing state.
            self._discard_proposal_records()
            if self.role is None and self._options.role != "leader" and self._pairing_state in (
                "available",
                "deciding",
            ):
                # A follower whose reservation went away simply becomes
                # available again; no partial local pairing state is kept.
                self._pairing_state = "available"
                self._pairing_reason = reason
                return
            self._pairing_conflict(reason)
            return
        if message_type == "pair_cell_route_established":
            route = push.get("route")
            if isinstance(route, Mapping):
                self._adopt_controller_route(route, proposal_id=self._proposal_id)
                self._accept_forwarded_acceptance(push.get("acceptance"))
            return
        if message_type == "pair_cell_route_state_changed":
            route = push.get("route")
            if isinstance(route, Mapping):
                self._feed_route_metadata(route)
            return
        if message_type == "pair_cell_route_removed":
            record = self._durable_route
            if record is not None and push.get("route_id") == record.route_id:
                self._route_removed(str(push.get("reason") or "safe_unpair"))

    def _answer_pairing_proposal(self, push: Mapping[str, object]) -> None:
        proposal_id = push.get("proposal_id")
        if not isinstance(proposal_id, str) or not proposal_id:
            return
        reason = self._follower_refusal_reason()
        acceptance: PairingAcceptance | None = None
        if reason is None:
            balance = cast(StartupBalance, self._read_startup_balance())
            try:
                # The follower authors its own frozen startup balance and its
                # own four risk tunables, and freezes them for the life of the
                # route.  The leader may only transcribe them.
                acceptance = build_pairing_acceptance(
                    proposal_id=proposal_id,
                    worker_id=self._worker_id,
                    startup_balance_usd=balance.amount_usd,
                    account_currency=balance.account_currency,
                    overrides=self._config.risk_overrides,
                )
            except PairExecutionCellError as error:
                reason = f"the Pairing Acceptance payload could not be frozen: {error}"
        try:
            if reason is None and acceptance is not None:
                payload = acceptance.canonical()
                frozen = FrozenAcceptance(
                    proposal_id=proposal_id, payload=payload, payload_hash=_payload_hash(payload)
                )
                # Persisted *before* the decision leaves this process: every
                # message that would otherwise carry this block is one-shot,
                # so a disconnect around the decision must not lose the only
                # copy of the follower's frozen balance and risk tunables.
                self._frozen_acceptance = frozen
                save_frozen_acceptance(self._acceptance_path, frozen)
                leader_worker_id = push.get("leader_worker_id")
                self._freeze_budget(
                    proposal_id=proposal_id,
                    role="follower",
                    counterparty_worker_id=str(leader_worker_id or ""),
                    balance=cast(StartupBalance, self._read_startup_balance()),
                )
                self._proposal_id = proposal_id
                self._session.send_pair_cell_pairing_decision(
                    proposal_id=proposal_id,
                    accepted=True,
                    acceptance_payload=payload,
                    payload_hash=frozen.payload_hash,
                )
                self._pairing_state = "deciding"
                self._pairing_deadline = self._now() + timedelta(seconds=PAIRING_RESERVATION_GRACE_SECONDS)
                self._pairing_reason = f"accepted proposal {proposal_id}"
                return
            self._session.send_pair_cell_pairing_decision(
                proposal_id=proposal_id, accepted=False, reason=reason
            )
        except WorkerEnrollmentError:
            _LOGGER.warning("The pairing decision could not be sent.", exc_info=True)
            return
        self._pairing_reason = f"refused proposal {proposal_id}: {reason}"

    def _follower_refusal_reason(self) -> str | None:
        """Automatic acceptance, gated only on local, current broker evidence."""

        if self._fail_closed_reason is not None:
            return self._fail_closed_reason
        now = self._now()
        facts = self._local_facts
        try:
            facts = read_readiness_facts(
                cast(ReadOnlyMT5, self._raw_mt5),
                login=self._login,
                server=self._server,
                now=now,
                minimum_margin_headroom_usd=self._polling.minimum_margin_headroom_usd,
            )
            self._local_facts = facts
        except Exception:
            _LOGGER.warning("The follower's readiness read failed.", exc_info=True)
            facts = None
        balance = self._read_startup_balance()
        return evaluate_follower_acceptance(
            unpaired=self._cell is None and self._durable_route is None,
            facts=facts,
            calibration=self.broker_clock_calibration(now),
            effect_journal=self._effect_journal,
            balance=balance,
            balance_error=self._balance_error,
        )

    def _read_startup_balance(self) -> StartupBalance | None:
        """Read and freeze this Worker's startup balance once, or fail closed."""

        if self._startup_balance is not None:
            return self._startup_balance
        try:
            balance = read_startup_balance(self._raw_mt5)
        except WorkerEnrollmentError as error:
            self._balance_error = str(error)
            return None
        try:
            worker_risk_limits(balance, self._config)
        except WorkerEnrollmentError as error:
            self._balance_error = str(error)
            return None
        self._balance_error = None
        self._startup_balance = balance
        return balance


    def _publish_local_acceptance(self) -> bool:
        """Follower-only: (re)freeze and re-publish this Worker's own payload.

        Freezing is idempotent by construction: the payload is derived from
        the durable route's *frozen* startup balance and this Worker's own
        configured tunables, so every republication produces byte-identical
        content and a second arrival is a no-op for the leader.  This is what
        makes the acceptance recoverable when the one-shot decision reply,
        route-established notice, or relay publication is lost around a
        disconnect.
        """

        cell, record = self._cell, self._durable_route
        if cell is None or record is None or record.role != "follower":
            return False
        existing = cell.pairing_acceptance()
        proposal_id = (
            (existing.proposal_id if existing is not None else None)
            or record.proposal_id
            or (self._frozen_acceptance.proposal_id if self._frozen_acceptance is not None else None)
        )
        if not proposal_id:
            self._acceptance_wait = (
                "this follower is on route "
                f"{record.route_id} but holds no durable Pairing Acceptance payload or proposal"
                " to rebuild one from, so the leader can publish no canonical policy;"
                " a safe unpair and a fresh pairing are required"
            )
            _LOGGER.error("%s.", self._acceptance_wait)
            return False
        try:
            acceptance = cell.freeze_pairing_acceptance(proposal_id=proposal_id)
        except PairExecutionCellError as error:
            self._fail_closed(f"the Pairing Acceptance payload could not be frozen: {error}")
            return False
        payload = acceptance.canonical()
        frozen = FrozenAcceptance(
            proposal_id=proposal_id, payload=payload, payload_hash=_payload_hash(payload)
        )
        if self._frozen_acceptance != frozen:
            self._frozen_acceptance = frozen
            save_frozen_acceptance(self._acceptance_path, frozen)
        self._acceptance_wait = None
        return True

    def _maybe_republish_acceptance(self, observed_at: datetime) -> None:
        """Keep republishing until the pair has actually agreed a policy."""

        cell, record = self._cell, self._durable_route
        if cell is None or record is None or record.role != "follower":
            return
        if cell.metadata_reconciliation() is not None or cell.route_state() != "ACTIVE":
            return
        if cell.enforced_risk_limits() is not None:
            return  # the leader published a policy, so it holds the payload
        if observed_at < self._next_acceptance_publication:
            return
        self._next_acceptance_publication = observed_at + self._polling.acceptance_republish_interval
        self._publish_local_acceptance()

    def _note_missing_acceptance(self, observed_at: datetime) -> None:
        """Leader-only: an actionable diagnostic while the payload is absent."""

        cell, record = self._cell, self._durable_route
        if cell is None or record is None or record.role != "leader":
            return
        if cell.peer_pairing_acceptance() is not None or cell.enforced_risk_limits() is not None:
            self._acceptance_wait = None
            return
        self._acceptance_wait = (
            f"this leader is on route {record.route_id} but holds no Pairing Acceptance payload"
            f" from follower {record.follower_worker_id}; no canonical policy can be published"
            " until the follower republishes it, which it retries while both sessions are live"
        )
        if observed_at >= self._next_acceptance_diagnostic:
            self._next_acceptance_diagnostic = observed_at + self._polling.acceptance_republish_interval
            _LOGGER.warning("%s.", self._acceptance_wait)

    def _freeze_budget(
        self,
        *,
        proposal_id: str,
        role: Role,
        counterparty_worker_id: str,
        balance: StartupBalance,
    ) -> None:
        record = FrozenBudget(
            proposal_id=proposal_id,
            role=role,
            counterparty_worker_id=counterparty_worker_id,
            startup_balance_usd=balance.amount_usd,
            account_currency=balance.account_currency,
        )
        self._frozen_budget = record
        save_frozen_budget(self._budget_path, record)

    def _discard_proposal_records(self) -> None:
        """Forget an abandoned proposal's frozen evidence entirely.

        A released reservation leaves no durable local pairing state, so a
        record from an abandoned proposal can never be picked up later and
        attached to a different route.
        """

        clear_frozen_acceptance(self._acceptance_path)
        clear_frozen_budget(self._budget_path)
        self._frozen_acceptance = None
        self._frozen_budget = None
        self._pending_budget = None

    def _frozen_balance_for(
        self, *, route_id: str, role: Role, peer_worker_id: str, proposal_id: str | None
    ) -> StartupBalance | None:
        """The balance this pairing froze, or ``None`` when it cannot be proved.

        The broker balance is deliberately **never** read here.  It is read
        once, at proposal time (leader) or acceptance time (follower), and
        frozen into the pairing; re-reading it while that pairing lives would
        silently resize a strategy the peer already copied verbatim and would
        change the canonical policy hash.  Adopting a route therefore only
        ever *recovers* the frozen number -- from this Worker's own durable
        route copy, or from the proposal-bound record written before the route
        could exist -- and fails closed when neither is available.
        """

        existing = self._durable_route
        if existing is not None and existing.route_id == route_id:
            return existing.balance()
        budget = self._frozen_budget
        if budget is not None and budget.bound_to(
            role=role, counterparty_worker_id=peer_worker_id, proposal_id=proposal_id
        ):
            return budget.balance()
        return None

    # -- route adoption and cell construction -------------------------------- #

    def _accept_forwarded_acceptance(self, record: object) -> None:
        """Leader-only: adopt the Pairing Acceptance payload the controller relayed.

        The controller forwards the follower's own opaque block unchanged with
        the route establishment, so the leader holds it without waiting for
        the follower's later relay publication.  The follower's own route
        notice carries the route alone and never an acceptance, so this path
        is leader-only by construction.

        Three bindings are checked before anything is transcribed: the record
        names the proposal this leader actually made, it comes from the Worker
        the controller routed as the follower, and its hash matches its own
        content.  A record that fails any of them is *not* copied -- the
        leader publishes no policy from it and waits for the follower's
        relayed copy, which the cell validates for itself.  Verbatim copying
        is only ever applied to a payload this leader can verify it received
        intact.
        """

        cell, durable = self._cell, self._durable_route
        if cell is None or durable is None or durable.role != "leader":
            return
        if not isinstance(record, Mapping):
            return
        payload = record.get("payload")
        peer = durable.assignment().peer_of(self._worker_id)
        if not isinstance(payload, Mapping) or peer is None:
            self._pairing_reason = "the forwarded Pairing Acceptance payload is malformed"
            _LOGGER.warning("The forwarded Pairing Acceptance payload is malformed.")
            return
        proposal_id = record.get("proposal_id")
        if self._proposal_id is not None and proposal_id != self._proposal_id:
            _LOGGER.warning(
                "A forwarded Pairing Acceptance payload names proposal %r rather than this"
                " leader's own %r; it is not copied.",
                proposal_id,
                self._proposal_id,
            )
            return
        if record.get("from_worker_id") != peer:
            _LOGGER.warning(
                "A forwarded Pairing Acceptance payload names Worker %r rather than the routed"
                " follower %r; it is not copied.",
                record.get("from_worker_id"),
                peer,
            )
            return
        if record.get("payload_hash") != _payload_hash(payload):
            _LOGGER.warning(
                "A forwarded Pairing Acceptance payload does not match its own hash; it is not copied."
            )
            return
        cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": PAIR_CELL_ENVELOPE_VERSION,
                    "kind": "pairing_acceptance",
                    "route_id": durable.route_id,
                    "from_worker_id": peer,
                    "to_worker_id": self._worker_id,
                    "payload": {"acceptance": dict(payload)},
                }
            )
        )

    def _adopt_controller_route(
        self, view: Mapping[str, object], *, proposal_id: str | None = None
    ) -> None:
        """The controller's route record is authoritative; adopt or reconcile it."""

        route_id = view.get("route_id")
        leader = view.get("leader_worker_id")
        follower = view.get("follower_worker_id")
        if not isinstance(route_id, str) or not isinstance(leader, str) or not isinstance(follower, str):
            return
        if self._cell is not None:
            self._feed_route_metadata(view)
            return
        if self._worker_id == leader:
            role: Role = "leader"
        elif self._worker_id == follower:
            role = "follower"
        else:
            self._idle_unpaired("the controller's route record does not name this Worker")
            return
        if self._options.role is not None and self._options.role != role:
            # The route record is authoritative for who leads; a contradicting
            # declaration is recorded and ignored, never a reassignment.
            _LOGGER.warning(
                "The declared Pair Execution Cell role %r contradicts the role %r the controller"
                " assigns on route %s; the controller's record wins.",
                self._options.role,
                role,
                route_id,
            )
        raw_state = view.get("state")
        state: RouteState = cast(RouteState, raw_state) if raw_state in ("ACTIVE", "UNPAIRING") else "ACTIVE"
        existing = self._durable_route
        budget = self._frozen_budget
        peer_worker_id = follower if role == "leader" else leader
        resolved_proposal = proposal_id
        if resolved_proposal is None and existing is not None and existing.route_id == route_id:
            resolved_proposal = existing.proposal_id
        if (
            resolved_proposal is None
            and budget is not None
            and budget.bound_to(
                role=role, counterparty_worker_id=peer_worker_id, proposal_id=None
            )
        ):
            resolved_proposal = budget.proposal_id
        balance = self._frozen_balance_for(
            route_id=route_id,
            role=role,
            peer_worker_id=peer_worker_id,
            proposal_id=resolved_proposal,
        )
        if balance is None:
            self._fail_closed(
                f"the controller routes this Worker as {role} with {peer_worker_id} on"
                f" {route_id}, but it holds neither a durable route copy nor a proposal-bound"
                " frozen startup balance for that pairing; the frozen budget cannot be"
                " recovered and re-reading the broker balance would silently resize a live"
                " pairing, so this Worker will not trade. Safe-unpair this route and pair again."
            )
            return
        record = DurableRouteRecord(
            route_id=route_id,
            role=role,
            leader_worker_id=leader,
            follower_worker_id=follower,
            frozen_balance_usd=balance.amount_usd,
            account_currency=balance.account_currency,
            state=state,
            proposal_id=resolved_proposal,
        )
        if not self._construct_cell(record):
            return
        if role == "follower":
            self._publish_local_acceptance()

    def _construct_cell(self, record: DurableRouteRecord) -> bool:
        authority = self._config.role_authority_error(record.role)
        if authority is not None:
            self._fail_closed(
                "the assigned Pair Execution Cell role contradicts this Worker's configuration"
                f" file ({self._config.source}): {authority}"
            )
            return False
        try:
            limits = worker_risk_limits(record.balance(), self._config)
        except WorkerEnrollmentError as error:
            self._fail_closed(str(error))
            return False
        assignment = record.assignment()
        relay = WorkerPairCellRelay(
            self._session, route=assignment, observer=self._observe_outbound_envelope
        )
        try:
            cell = PairExecutionCell(
                worker_id=self._worker_id,
                route=assignment,
                db_path=self._db_path,
                relay=relay,
                mt5=WorkerPairCellMT5(self._raw_mt5),
                local_risk_limits=limits,
                effect_journal=self._effect_journal,
                scheduler=self._session.trader_rpc_scheduler,
                allow_live=self._config.allow_live,
                serve_foreign_scheduler_item=self._serve_foreign_scheduler_item,
                dispatch_foreign_scheduler_outcome=self._session.dispatch_scheduler_outcome,
            )
        except PairExecutionCellError as error:
            self._fail_closed(f"the Pair Execution Cell could not be constructed: {error}")
            return False
        except sqlite3.Error as error:
            # Opening the durable state runs schema creation and any migration
            # of a database written by an earlier build.  A failure there must
            # fail *this cell* closed with something an operator can act on --
            # never escape and take the whole Worker's reconciliation loop down
            # with it, which would also stop ordinary broker reconciliation.
            self._fail_closed(
                "the Pair Execution Cell durable database could not be opened or migrated"
                f" ({self._db_path}): {type(error).__name__}: {error}. This Worker stays"
                " authenticated and will not trade; move or repair that file and restart."
            )
            return False
        except (ValueError, KeyError, TypeError) as error:
            # Malformed persisted content (an unreadable JSON payload, a
            # column that changed shape) is the same class of problem.
            self._fail_closed(
                "the Pair Execution Cell durable state could not be recovered"
                f" ({self._db_path}): {type(error).__name__}: {error}. This Worker stays"
                " authenticated and will not trade; move or repair that file and restart."
            )
            return False
        self._cell, self._relay, self._limits = cell, relay, limits
        self._durable_route = record
        self._startup_balance = record.balance()
        save_durable_route(self._route_path, record)
        self._pairing_state = "routed"
        self._pairing_reason = ""
        observed_at = self._now()
        # Discovery needs this account's whole catalog immediately after the
        # route exists and before any quote is processed.
        if self._catalog_entries is None:
            if self._refresh_local_evidence(observed_at):
                self._next_refresh_at = observed_at + self._polling.sizing_refresh_interval
        self._feed_catalog(observed_at)
        try:
            cell.recover()
        except PairExecutionCellError:
            _LOGGER.warning("Pair Execution Cell durable state failed to recover cleanly.", exc_info=True)
        try:
            self._session.request_pair_cell_route_sync()
        except WorkerEnrollmentError:
            _LOGGER.warning("The authoritative route sync request failed.", exc_info=True)
        self._apply_startup_operator_requests()
        return True

    def _apply_startup_operator_requests(self) -> None:
        if self._operator_requests_applied or self._cell is None:
            return
        self._operator_requests_applied = True
        if self._options.unpair:
            self.request_safe_unpair()
        if self._options.cancel_unpair:
            self.cancel_safe_unpair()
        if self._options.rediscover:
            self.request_rediscovery(actor="startup", reason="--pair-cell-rediscover")

    def _feed_route_metadata(self, view: Mapping[str, object] | None) -> PairResult | None:
        """Hand the controller's record to the cell, which arbitrates agreement."""

        cell = self._cell
        record = self._durable_route
        if cell is None or record is None:
            return None
        observed_at = self._now()
        if view is None:
            return cell.handle_event(
                RouteMetadataEvent(
                    route_id=None,
                    role=None,
                    leader_worker_id=None,
                    follower_worker_id=None,
                    state=None,
                    observed_at=observed_at,
                )
            )
        route_id = view.get("route_id")
        leader = view.get("leader_worker_id")
        follower = view.get("follower_worker_id")
        raw_state = view.get("state")
        state = cast(RouteState, raw_state) if raw_state in ("ACTIVE", "UNPAIRING") else "ACTIVE"
        role = view.get("role")
        if role not in ("leader", "follower"):
            if self._worker_id == leader:
                role = "leader"
            elif self._worker_id == follower:
                role = "follower"
            else:
                role = None
        result = cell.handle_event(
            RouteMetadataEvent(
                route_id=route_id if isinstance(route_id, str) else None,
                role=cast(Role, role) if role in ("leader", "follower") else None,
                leader_worker_id=leader if isinstance(leader, str) else None,
                follower_worker_id=follower if isinstance(follower, str) else None,
                state=state,
                observed_at=observed_at,
            )
        )
        if cell.metadata_reconciliation() is None and record.state != cell.route_state():
            self._durable_route = replace(record, state=cell.route_state())
            save_durable_route(self._route_path, self._durable_route)
            if cell.route_state() == "UNPAIRING":
                # Entering ``UNPAIRING`` discards any earlier assertion, so
                # this Worker re-proves safety from fresh evidence at once.
                self._asserted_state_version = None
                self._probed_state_version = None
        return result

    def _route_removed(self, reason: str) -> None:
        """The route is gone: become unpaired, unreserved and pairable again.

        Route-scoped durable state is cleared so the next pairing starts from
        a clean route, policy, universe and plan set.  Account- and
        product-scoped **safety history** is deliberately kept: the New York
        daily realized-loss accumulator, every MT5 ``10021`` product
        quarantine and its release audit, an operator stop, and this Worker's
        monotonic local state version all outlive any number of pairings.
        """

        cell, self._cell = self._cell, None
        if cell is not None:
            try:
                cell.close()
            except Exception:  # pragma: no cover - shutdown must never raise
                _LOGGER.warning("Closing the Pair Execution Cell failed.", exc_info=True)
        self._relay = None
        self._limits = None
        self._durable_route = None
        clear_durable_route(self._route_path)
        self._discard_proposal_records()
        self._reset_route_scoped_state()
        self._proposal_id = None
        self._published_state_version = None
        self._asserted_state_version = None
        self._probed_state_version = None
        self._published_policy = False
        self._acceptance_wait = None
        self._operator_requests_applied = True
        self._proposal_attempts = 0
        self._quote_cursors = {}
        self._owned_legs = {}
        self._observed_open = {}
        # A new pairing re-reads the startup balance and therefore produces a
        # different policy hash whenever that balance changed.
        self._startup_balance = None
        self._pairing_state = "unstarted"
        self._pairing_reason = f"the Pair Execution Cell route was removed ({reason})"
        _LOGGER.info("The Pair Execution Cell route was removed: %s.", reason)
        if self._options.unpair and reason in {"safe_unpair", "safe_unpair_completed_while_offline"}:
            self._exit_after_safe_unpair = True

    def _reset_route_scoped_state(self) -> None:
        """Clear one pairing's durable state, keeping local safety history.

        A removed route must leave no route, policy, universe, sizing plan or
        pairing acceptance behind, or the next pairing would inherit them.  It
        must equally never erase this account's own realized-loss accounting
        or a durable product quarantine: those are not route state, they have
        no automatic expiry, and a quarantine is released only by an explicit
        authenticated operator action.

        Attempt evidence -- the attempt, both legs, and the desired/active
        state the cell converges through -- is cleared **as one set** and only
        when every attempt is provably terminal, which a safe unpair has
        already proved.  Anything unresolved keeps its whole set, so
        containment still has consistent durable facts rather than an attempt
        with its desired state amputated.
        """

        if not self._db_path.exists():
            return
        try:
            connection = sqlite3.connect(self._db_path)
        except sqlite3.Error:  # pragma: no cover - defensive
            _LOGGER.warning("Could not open the Pair Execution Cell database to clear route state.", exc_info=True)
            return
        try:
            present = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            unresolved = 0
            if "cell_attempts" in present:
                row = connection.execute(
                    "SELECT COUNT(*) FROM cell_attempts WHERE terminal = 0"
                ).fetchone()
                unresolved = 0 if row is None else int(row[0])
            for table in ROUTE_SCOPED_TABLES:
                if table in present:
                    connection.execute(f"DELETE FROM {table}")  # noqa: S608 - fixed literal names
            if unresolved:
                # Route removal requires two fresh assertions that every
                # attempt is terminal, so this is a contradiction: say so
                # loudly and keep every piece of the attempt's evidence.
                _LOGGER.error(
                    "%s unresolved attempt(s) survive route removal; their durable evidence and"
                    " desired state are kept intact so local containment still has exact facts"
                    " to converge from.",
                    unresolved,
                )
            else:
                for table in ATTEMPT_SCOPED_TABLES:
                    if table in present:
                        connection.execute(f"DELETE FROM {table}")  # noqa: S608 - fixed literal names
            connection.commit()
        except sqlite3.Error:  # pragma: no cover - defensive
            _LOGGER.warning("Could not clear route-scoped Pair Execution Cell state.", exc_info=True)
        finally:
            connection.close()

    # -- serialized MT5 access ----------------------------------------------- #

    def _serve_foreign_scheduler_item(self, scheduled: ScheduledTraderRpc) -> None:
        _dispatch_scheduled_trader_rpc(
            cast(ReadOnlyMT5, self._raw_mt5), self._session, scheduled, None, self._effect_journal
        )

    def _run_serialized(self, command_id: str, payload: dict[str, object], read: Callable[[], _T]) -> _T | None:
        """Run one MT5 read batch through the Worker's single serialization slot.

        The Pair Execution Cell shares this Worker's one
        :class:`~abt.worker.scheduler.DeadlineAwareTraderRpcScheduler`, whose
        single ``_active`` slot *is* the Worker's MT5 serialization boundary,
        so the catalog and clock refresh cannot interleave with an in-flight
        broker write.
        """

        scheduler = self._session.trader_rpc_scheduler
        admitted = scheduler.admit(
            {
                "request_id": command_id,
                "command_id": command_id,
                "kind": "read",
                "payload": payload,
                "priority": "background",
            }
        )
        if isinstance(admitted, TraderRpcOutcome):
            return None
        scheduled = self._dequeue_own_item(command_id)
        if scheduled is None or isinstance(scheduled, TraderRpcOutcome):
            return None
        try:
            return read()
        finally:
            scheduled.complete("completed", "The Pair Execution Cell serialized MT5 read completed.")

    def _dequeue_own_item(self, command_id: str) -> ScheduledTraderRpc | TraderRpcOutcome | None:
        scheduler = self._session.trader_rpc_scheduler
        while True:
            candidate = scheduler.next()
            if candidate is None:
                return None
            if isinstance(candidate, TraderRpcOutcome):
                if candidate.request_id == command_id:
                    return candidate
                self._session.dispatch_scheduler_outcome(candidate)
                continue
            if candidate.command_id == command_id:
                return candidate
            self._serve_foreign_scheduler_item(candidate)

    # -- catalog and broker-clock refresh ------------------------------------ #

    def _refresh_local_evidence(self, observed_at: datetime) -> bool:
        """Startup/hourly catalog and clock refresh; ``False`` means "retry".

        Broker-time calibration rides the same serialized cycle: it is a
        bounded local measurement, never a signal-time read, and it is what
        makes the leader's cross-Worker ``quote_max_skew_seconds`` comparison
        meaningful between two brokers whose server timezones differ.
        """

        self._refresh_cycle += 1
        cycle = f"{self._worker_id}:pair-cell-catalog-refresh:{self._refresh_cycle}"
        ran = self._run_serialized(
            cycle,
            {"type": "symbols", "purpose": "pair_cell_catalog_refresh", "cycle": cycle},
            lambda: self._refresh_locally(observed_at),
        )
        if ran is not True:
            _LOGGER.debug(
                "Pair Execution Cell catalog refresh was not admitted by the MT5 scheduler: cycle=%s.",
                cycle,
            )
        return ran is True

    def _refresh_locally(self, observed_at: datetime) -> bool:
        """Read this account's catalog, then re-measure this broker's clock.

        Reading the whole catalog keeps both Workers' summaries current, which
        is what makes an explicit rediscovery symmetric.  It never expands the
        frozen universe: the cell admits a newly appeared symbol only through
        an explicit rediscovery, and the hourly sizing and protection refresh
        revalidates only the frozen universe's own products.
        """

        try:
            entries = read_catalog_entries(self._raw_mt5)
        except WorkerEnrollmentError:
            # Discovery fails closed on an unavailable catalog; the previous
            # summary is kept rather than replaced with an empty one.
            _LOGGER.warning("The local MT5 product catalog is unavailable.", exc_info=True)
            entries = None
        except Exception:
            _LOGGER.warning("The local MT5 product catalog read failed.", exc_info=True)
            entries = None
        if entries is not None and entries != self._catalog_entries:
            self._catalog_entries = entries
            self._catalog_generation += 1
            _LOGGER.debug(
                "Pair Execution Cell catalog refreshed: generation=%s entries=%s.",
                self._catalog_generation,
                len(entries),
            )
        elif entries is not None:
            _LOGGER.debug(
                "Pair Execution Cell catalog refresh found no change: generation=%s entries=%s.",
                self._catalog_generation,
                len(entries),
            )
        self._calibrate()
        return True

    def _calibrate(self) -> None:
        for symbol in self._calibration_symbols():
            sample = sample_broker_clock_calibration(
                self._raw_mt5,
                symbol,
                now=self._now,
                sleep=self._sleep,
                interval=self._polling.clock_sample_interval,
            )
            if sample is not None:
                self._clock_sample = sample
                return
        _LOGGER.warning("No eligible symbol could calibrate this broker's clock this cycle.")

    def _calibration_symbols(self) -> tuple[str, ...]:
        cell = self._cell
        if cell is not None:
            universe = cell.discovered_universe()
            if universe is not None and universe.products:
                return tuple(sorted({product.symbol for product in universe.products}))[
                    :_MAX_CALIBRATION_SYMBOLS
                ]
        entries = self._catalog_entries or ()
        return tuple(entry.name for entry in entries)[:_MAX_CALIBRATION_SYMBOLS]

    def _feed_catalog(self, observed_at: datetime) -> PairResult | None:
        cell = self._cell
        entries = self._catalog_entries
        if cell is None or entries is None:
            return None
        if self._catalog_generation == self._fed_catalog_generation:
            return None
        self._fed_catalog_generation = self._catalog_generation
        _LOGGER.debug(
            "Pair Execution Cell submitted local catalog to discovery: generation=%s entries=%s.",
            self._catalog_generation,
            len(entries),
        )
        return cell.handle_event(LocalCatalogEvent(entries=entries, observed_at=observed_at))

    # -- per-loop pumping ----------------------------------------------------- #

    def drain_relay(self) -> PairResult | None:
        """Process every already-queued pairing message, quarantine release,
        relay ack, and opaque relay envelope now.

        Called every iteration of the Worker's receive loop (not gated by a
        polling interval): immediate attempt delivery is this feature's
        latency-critical path, and pairing replies must not wait a whole
        polling cycle either.
        """

        self._drain_control()
        result: PairResult | None = None
        for release in self._session.drain_pair_cell_quarantine_releases():
            outcome = self._apply_quarantine_release(release)
            if outcome is None:
                continue
            request_id, symbol, applied, release_result = outcome
            result = release_result or result
            self._session.send_pair_cell_quarantine_release_result(
                request_id=request_id, symbol=symbol, outcome="applied" if applied else "rejected"
            )
        for ack in self._session.drain_pair_relay_acks():
            request_id = ack.get("request_id")
            accepted = ack.get("accepted") is True
            reason = ack.get("reason")
            peer_disconnected = not accepted and reason == "Peer Worker is disconnected."
            if peer_disconnected:
                self._set_peer_session_connected(
                    False, "the controller reported that the peer Worker is disconnected"
                )
            elif not accepted or (
                self._relay is not None and self._relay.consume_state_relay_ack(request_id)
            ):
                _LOGGER.debug(
                    "Pair Execution Cell relay acknowledgement: request_id=%s accepted=%s%s.",
                    request_id if isinstance(request_id, str) else "-",
                    accepted,
                    f" reason={reason}" if not accepted and isinstance(reason, str) else "",
                )
            if ack.get("accepted") is False:
                self._set_peer_session_connected(False, "a relay acknowledgement was rejected")
        envelopes = self._session.drain_pair_relay_envelopes()
        if envelopes and not self._peer_session_connected:
            self._set_peer_session_connected(True, "a peer relay envelope was received")
        for envelope in envelopes:
            inner = unwrap_pair_relay_envelope(envelope)
            if inner is not None and self._cell is not None:
                kind = inner.get("kind")
                if kind in _STATE_RELAY_KINDS:
                    _LOGGER.debug(
                        "Pair Execution Cell relay received: kind=%s source_worker_id=%s.",
                        kind,
                        inner.get("from_worker_id"),
                    )
                result = self._cell.handle_event(RelayEnvelopeReceived(inner))
        if self._cell is not None:
            result = self._maybe_publish_policy() or result
            self._sync_route_control()
        return result

    def pump(self, observed_at: datetime) -> PairResult | None:
        """Drain relay and pairing traffic, then advance time-gated evidence."""

        result = self.drain_relay()
        if self._exit_after_safe_unpair:
            return result
        cell = self._cell
        if observed_at >= self._next_refresh_at:
            if self._refresh_local_evidence(observed_at):
                self._next_refresh_at = observed_at + self._polling.sizing_refresh_interval
        if cell is None:
            return result  # authenticated, unpaired, and still pairable
        result = self._feed_catalog(observed_at) or result
        if self._relay is not None and self._relay.last_send_failed:
            self._relay.last_send_failed = False
            self._set_peer_session_connected(False, "a peer relay send failed")
        if self._peer_session_connected != self._fed_peer_session:
            self._fed_peer_session = self._peer_session_connected
            if self._peer_session_connected:
                # A reconnected peer may have missed this Worker's one-shot
                # pairing publication; republish on the next pump.
                self._next_acceptance_publication = observed_at
            result = cell.handle_event(
                PeerSessionEvent(connected=self._peer_session_connected, observed_at=observed_at)
            )
        result = cell.handle_event(ClockTickEvent(observed_at))
        if cell.metadata_reconciliation() is not None and observed_at >= self._next_route_sync_at:
            # A disagreement is re-checked against the authoritative record
            # rather than latching forever; that is also how a safe unpair
            # completed while this Worker was offline is eventually adopted.
            self._next_route_sync_at = observed_at + self._polling.route_sync_interval
            try:
                self._session.request_pair_cell_route_sync()
            except WorkerEnrollmentError:
                _LOGGER.warning("The authoritative route sync request failed.", exc_info=True)
        self._maybe_republish_acceptance(observed_at)
        self._note_missing_acceptance(observed_at)
        if observed_at >= self._next_readiness_at:
            self._next_readiness_at = observed_at + self._polling.readiness_interval
            try:
                facts = read_readiness_facts(
                    cast(ReadOnlyMT5, self._raw_mt5),
                    login=self._login,
                    server=self._server,
                    now=observed_at,
                    minimum_margin_headroom_usd=self._polling.minimum_margin_headroom_usd,
                )
                self._local_facts = facts
                result = cell.handle_event(facts)
            except Exception:
                _LOGGER.warning("Pair Execution Cell readiness read failed.", exc_info=True)
        result = self._maybe_publish_policy() or result
        if observed_at >= self._next_quote_at:
            self._next_quote_at = observed_at + self._polling.quote_interval
            quotes = self._read_quotes(observed_at)
            if quotes:
                result = cell.handle_event(LocalQuoteBatchEvent(quotes))
        if observed_at >= self._next_snapshot_at:
            self._next_snapshot_at = observed_at + self._polling.broker_snapshot_interval
            snapshot = self._read_broker_snapshot(observed_at)
            if snapshot is not None:
                result = cell.handle_event(snapshot)
                for realized in self._realized_loss_facts(snapshot, observed_at, result):
                    result = cell.handle_event(realized)
        self._log_state_diagnostic(cell)
        self._sync_route_control()
        return result

    def _set_peer_session_connected(self, connected: bool, reason: str) -> None:
        if self._peer_session_connected == connected:
            return
        self._peer_session_connected = connected
        _LOGGER.debug(
            "Pair Execution Cell peer session %s: %s.",
            "connected" if connected else "disconnected",
            reason,
        )

    def _log_state_diagnostic(self, cell: PairExecutionCell) -> None:
        """Log a concise state snapshot only when an observable value changes."""

        status = cell.status()
        diagnostic = cell.entry_admission_diagnostic()
        snapshot = (
            status.route_state,
            status.universe_generation,
            status.state,
            status.attempt_id,
            status.desired_state,
            status.ready,
            status.ready_reason,
            status.policy_accepted,
            status.plan_set_version,
            status.pair_confirmed,
            status.needs_human,
            status.needs_human_reason,
            status.metadata_reconciliation,
            diagnostic,
        )
        if snapshot == self._last_state_diagnostic:
            return
        self._last_state_diagnostic = snapshot
        _LOGGER.debug(
            "Pair Execution Cell state: route_id=%s route_state=%s universe_generation=%s "
            "state=%s desired=%s attempt_id=%s ready=%s reason=%s policy_accepted=%s "
            "plan_set_version=%s pair_confirmed=%s needs_human=%s needs_human_reason=%s admission=%s.",
            status.route_id,
            status.route_state,
            status.universe_generation,
            status.state,
            status.desired_state,
            status.attempt_id or "-",
            status.ready,
            status.ready_reason,
            status.policy_accepted,
            status.plan_set_version or "-",
            status.pair_confirmed,
            status.needs_human,
            status.needs_human_reason or "-",
            diagnostic,
        )

    def _sync_route_control(self) -> None:
        """Publish the local state version and, while ``UNPAIRING``, assertions.

        The controller stores the version and compares it; it never derives
        emptiness or terminality.  An assertion is sent only when the cell's
        own fresh evidence says this account is broker-verified empty and
        every attempt and local effect it owns is terminal, and any new or
        changed local effect advances the version and invalidates it.
        """

        cell, record = self._cell, self._durable_route
        if cell is None or record is None or cell.metadata_reconciliation() is not None:
            return
        version = cell.state_version()
        if version != self._published_state_version:
            try:
                self._session.send_pair_cell_state_version(
                    route_id=record.route_id, state_version=version
                )
            except WorkerEnrollmentError:
                _LOGGER.warning("The local state version could not be published.", exc_info=True)
                return
            self._published_state_version = version
            self._asserted_state_version = None
        if cell.route_state() != "UNPAIRING":
            return
        # Deriving an assertion is not free: it reads fresh broker facts and
        # journals a durable transition under ``synchronous=FULL``.  It is
        # therefore rate-limited whenever nothing local has moved -- including
        # while the account is *not* yet safe, which is exactly the state a
        # Worker can sit in for a long time -- and probed immediately when the
        # local attempt/effect state version does move.
        now = self._now()
        if version == self._probed_state_version and now < self._next_safe_unpair_probe:
            return
        self._probed_state_version = version
        self._next_safe_unpair_probe = now + self._polling.safe_unpair_probe_interval
        assertion = cell.safe_unpair_assertion()
        if not assertion.safe or self._asserted_state_version == assertion.state_version:
            return
        try:
            self._session.send_pair_cell_unpair_assertion(
                route_id=record.route_id, state_version=assertion.state_version
            )
        except WorkerEnrollmentError:
            _LOGGER.warning("The safe-unpair assertion could not be sent.", exc_info=True)
            return
        self._asserted_state_version = assertion.state_version

    def _maybe_publish_policy(self) -> PairResult | None:
        """Leader-only: compose and adopt the one canonical policy.

        ``follower_risk`` is copied **verbatim** from the follower's Pairing
        Acceptance payload; there is no path here that lets the leader
        recompute, clamp, rescale or substitute the follower's numbers.  A
        missing or unusable payload simply means no policy is published.
        """

        cell, limits = self._cell, self._limits
        record = self._durable_route
        if cell is None or limits is None or record is None or record.role != "leader":
            return None
        if cell.enforced_risk_limits() is not None and self._published_policy:
            return None
        acceptance = cell.peer_pairing_acceptance()
        if acceptance is None:
            return None
        try:
            policy = canonical_policy_from_acceptance(
                policy_version=record.route_id,
                leader_risk=limits,
                acceptance=acceptance,
                shared=self._config.shared_policy,
            )
        except PairExecutionCellError as error:
            self._fail_closed(f"the canonical policy could not be composed: {error}")
            return None
        try:
            result = cell.accept_policy(policy)
        except PairExecutionCellError:
            # Policy content may only be adopted while both accounts are
            # broker-verified empty; this retries quietly.
            return None
        self._published_policy = True
        _LOGGER.info("The Pair Execution Cell leader adopted strategy policy %s.", policy.policy_version)
        return result

    def _apply_quarantine_release(
        self, release: Mapping[str, object]
    ) -> tuple[str, str, bool, PairResult | None] | None:
        try:
            request_id = cast(str, release["request_id"])
            symbol = cast(str, release["symbol"])
        except KeyError:
            _LOGGER.warning("Received a malformed Pair Execution Cell quarantine-release request.", exc_info=True)
            return None
        if self._cell is None:
            # A product quarantine is durable, route-independent, Worker-local
            # state that outlives any pairing, so "this Worker is not routed"
            # is *not* evidence that there is nothing to release.  Reporting
            # applied here would tell the operator a release succeeded while a
            # quarantine that only the cell may lift is still in force.
            outstanding = self._durable_quarantined_products(symbol)
            if not outstanding:
                return request_id, symbol, True, None  # genuinely nothing to release
            _LOGGER.warning(
                "Refusing a quarantine release for %s while this Worker is not on a Pair"
                " Execution Cell route: %s remain quarantined and only the cell may lift them"
                " with their release marker and audit intact. Retry once this Worker is routed.",
                symbol,
                ", ".join(outstanding),
            )
            return request_id, symbol, False, None
        result: PairResult | None = None
        try:
            observed_at = _parse_utc(cast(str, release["observed_at"]))
            actor = cast(str, release["actor"])
            reason = cast(str, release["reason"])
        except (KeyError, ValueError, PairExecutionCellError):
            _LOGGER.warning("Received an invalid Pair Execution Cell quarantine-release request.", exc_info=True)
            return request_id, symbol, False, None
        applied = True
        for product_id in self._release_targets(symbol):
            result = self._cell.handle_event(
                QuarantineReleaseEvent(
                    product_id=product_id, actor=actor, reason=reason, observed_at=observed_at
                )
            )
            if self._cell.quarantine_release_marker(product_id) != observed_at.isoformat():
                applied = False
        return request_id, symbol, applied, result

    def _durable_quarantined_products(self, symbol: str) -> tuple[str, ...]:
        """Quarantined identities for one symbol, read without a cell.

        Read-only on purpose: lifting a quarantine writes a release marker and
        a release audit row that only :class:`~abt.pair_cell.PairExecutionCell`
        owns, and reproducing that here would fork the rule that decides
        whether a later re-quarantine of the same product is suppressed.

        The key column is discovered rather than assumed, because a database
        written by an earlier build keys the quarantine by ``symbol`` while the
        current one keys it by the derived ``product_id``.  Every outcome other
        than "this database provably holds no quarantine" is conservative: an
        unreadable file, an unrecognised shape, or a failed read all report the
        symbol as still quarantined, so an operator is told to retry instead of
        being told a release succeeded that never ran.
        """

        if not self._db_path.exists():
            return ()  # nothing has ever been quarantined here
        try:
            connection = sqlite3.connect(self._db_path)
        except sqlite3.Error:
            _LOGGER.warning(
                "Could not open %s to check for a durable quarantine of %s.",
                self._db_path,
                symbol,
                exc_info=True,
            )
            return (symbol,)
        try:
            present = {
                str(row[0])
                for row in connection.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'"
                ).fetchall()
            }
            if "cell_product_quarantine" not in present:
                return ()
            columns = [
                str(row[1])
                for row in connection.execute("PRAGMA table_info(cell_product_quarantine)").fetchall()
            ]
            key = next((column for column in ("product_id", "symbol") if column in columns), None)
            if key is None:
                _LOGGER.warning(
                    "The durable quarantine table in %s has no recognised key column %s;"
                    " treating %s as still quarantined.",
                    self._db_path,
                    columns,
                    symbol,
                )
                return (symbol,)
            rows = connection.execute(
                f"SELECT {key} FROM cell_product_quarantine ORDER BY {key}"  # noqa: S608 - fixed column names
            ).fetchall()
        except sqlite3.Error:
            _LOGGER.warning(
                "Could not read the durable quarantine of %s from %s.",
                symbol,
                self._db_path,
                exc_info=True,
            )
            return (symbol,)
        finally:
            connection.close()
        return tuple(
            str(row[0])
            for row in rows
            if str(row[0]) == symbol or str(row[0]).startswith(f"{symbol}:")
        )

    def _release_targets(self, symbol: str) -> tuple[str, ...]:
        """An operator releases a *symbol*; the cell quarantines a *product*.

        Targets are resolved from the discovered universe **and** from the
        durable quarantine itself, because a quarantine outlives any universe:
        it survives restart, rediscovery and re-pairing, so a release must
        still reach it while this pair has not (yet) installed a universe that
        happens to contain the symbol.  A value that is already a derived
        product identity is honored directly, so an operator can name either.
        """

        cell = self._cell
        if cell is None:
            return (symbol,)
        universe = cell.discovered_universe()
        targets = {
            product.product_id
            for product in (() if universe is None else universe.products)
            if product.symbol == symbol
        }
        targets |= {
            product_id
            for product_id in cell.quarantined_products()
            if product_id == symbol or product_id.startswith(f"{symbol}:")
        }
        return tuple(sorted(targets)) or (symbol,)

    def _read_quotes(self, observed_at: datetime) -> tuple[LocalQuoteEvent, ...]:
        """Only genuinely new market evidence becomes a quote event.

        Polling faster than the broker feed updates is normal and desirable
        (it minimizes detection latency), but re-stamping an unchanged tick
        with a new sequence would burn relay bandwidth and misrepresent one
        repeated tick as a stream of distinct market evidence.
        """

        cell = self._cell
        universe = None if cell is None else cell.discovered_universe()
        if universe is None:
            return ()
        calibration = self.broker_clock_calibration(observed_at)
        quotes: list[LocalQuoteEvent] = []
        for product in universe.products:
            try:
                quote, cursor = read_local_quote_evidence(
                    self._raw_mt5,
                    product_id=product.product_id,
                    symbol=product.symbol,
                    recovery_epoch=self._recovery_epoch,
                    sequence=self._sequence + 1,
                    now=self._now,
                    calibration=calibration,
                )
            except WorkerEnrollmentError:
                continue
            if not cursor.advanced_from(self._quote_cursors.get(product.product_id)):
                continue
            self._quote_cursors[product.product_id] = cursor
            self._sequence += 1
            quotes.append(quote)
        return tuple(quotes)

    def _read_broker_snapshot(self, observed_at: datetime) -> BrokerSnapshotEvent | None:
        try:
            orders = _optional_records(self._raw_mt5.orders_get())
            positions = _optional_records(self._raw_mt5.positions_get())
        except Exception:
            _LOGGER.warning("Pair Execution Cell broker snapshot read failed.", exc_info=True)
            return None
        return BrokerSnapshotEvent(orders=orders, positions=positions, observed_at=observed_at)

    # -- realized per-leg loss facts ------------------------------------------ #

    def _observe_outbound_envelope(self, kind: str, payload: Mapping[str, object]) -> None:
        """Seed exact-ticket loss attribution from the cell's own evidence.

        ``leg_status`` is emitted the moment the cell durably establishes its
        own exact broker-observed position, from the cell's own broker read
        rather than this adapter's polling cadence.  Attributing from it
        removes the race where a leg that fills and is contained between two
        adapter snapshots would never be attributed at all and would silently
        bypass the New York daily realized-loss allowance.
        """

        if kind != "leg_status":
            return
        ticket, attempt_id = payload.get("ticket"), payload.get("attempt_id")
        if not isinstance(ticket, str) or not ticket or not isinstance(attempt_id, str) or not attempt_id:
            return
        if ticket in self._realized_reported:
            return
        self._owned_legs.setdefault(
            ticket, _OwnedLeg(attempt_id=attempt_id, symbol=str(payload.get("symbol") or ""))
        )

    def _realized_loss_facts(
        self, snapshot: BrokerSnapshotEvent, observed_at: datetime, result: PairResult | None
    ) -> list[RealizedPnLEvent]:
        """Realized P&L of this Worker's own closed strategy-owned legs.

        Attribution is seeded from the cell's own durable exact ticket (see
        :meth:`_observe_outbound_envelope`); observing a ticket in a snapshot
        while an attempt is unresolved is only a redundant second source.
        The *amount* always comes from broker deal history and is only ever
        recorded once that history proves the position actually closed --
        never from a close receipt, a floating value, or a ticket's mere
        absence from a snapshot that may simply predate the fill.
        """

        if snapshot.positions is None:
            return []
        attempt_id = None if result is None else result.attempt_id
        present: set[str] = set()
        for position in snapshot.positions:
            ticket = position.get("ticket")
            if ticket is None:
                continue
            key = str(ticket)
            present.add(key)
            self._observed_open.setdefault(key, None)
            if attempt_id is not None and key not in self._owned_legs and key not in self._realized_reported:
                self._owned_legs[key] = _OwnedLeg(attempt_id=attempt_id, symbol=str(position.get("symbol", "")))
        facts: list[RealizedPnLEvent] = []
        for key in [ticket for ticket in self._owned_legs if ticket not in present]:
            realized = self._read_closed_realized_usd(key, observed_open=key in self._observed_open)
            if realized is None:
                continue  # not provably closed yet; re-checked on the next snapshot
            owned = self._owned_legs.pop(key)
            self._observed_open.pop(key, None)
            self._realized_reported[key] = None
            while len(self._realized_reported) > _MAX_REMEMBERED_CLOSED_TICKETS:
                self._realized_reported.pop(next(iter(self._realized_reported)))
            facts.append(
                RealizedPnLEvent(
                    attempt_id=owned.attempt_id,
                    realized_usd=_decimal_text(realized),
                    closed_at=observed_at,
                    event_id=f"position:{key}",
                )
            )
        return facts

    def _read_closed_realized_usd(self, ticket: str, *, observed_open: bool) -> Decimal | None:
        """This ticket's realized amount, only once the broker proves it closed.

        An exit deal (``DEAL_ENTRY_OUT``/``INOUT``/``OUT_BY``) is the
        authoritative fact.  A broker whose history does not expose ``entry``
        at all falls back to "this Worker saw the position open and it is now
        gone", which is why a ticket that has never been observed open cannot
        be closed out on absence alone.
        """

        history = getattr(self._raw_mt5, "history_deals_get", None)
        if not callable(history):
            return None
        try:
            deals = history(position=int(ticket))
        except (ValueError, TypeError):
            return None
        except Exception:
            _LOGGER.warning("Pair Execution Cell realized-loss read failed for ticket %s.", ticket, exc_info=True)
            return None
        if deals is None or not isinstance(deals, (list, tuple)) or not deals:
            return None
        total = Decimal(0)
        amounts_seen = False
        entry_seen = False
        closed = False
        for deal in deals:
            try:
                evidence = _evidence(deal, "deal")
            except WorkerEnrollmentError:
                return None
            entry = evidence.get("entry")
            if not isinstance(entry, bool) and isinstance(entry, int):
                entry_seen = True
                closed = closed or entry in _MT5_DEAL_EXIT_ENTRIES
            for component in ("profit", "swap", "commission", "fee"):
                value = evidence.get(component)
                if isinstance(value, bool) or not isinstance(value, (int, float)):
                    continue
                total += Decimal(str(value))
                amounts_seen = True
        if entry_seen and not closed:
            return None
        if not entry_seen and not observed_open:
            return None
        return total if amounts_seen else None

    # -- exit and shutdown ----------------------------------------------------- #

    def request_close(self, reason: str) -> PairResult | None:
        if self._cell is None:
            return None
        return self._cell.request_close(reason)

    def close(self) -> None:
        if self._cell is not None:
            self._cell.close()
