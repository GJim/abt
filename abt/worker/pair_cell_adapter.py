"""Production wiring between an authenticated Worker session and one
:class:`abt.pair_cell.PairExecutionCell`.

``abt.pair_cell`` is deliberately broker- and transport-agnostic: it depends
only on the small :class:`~abt.pair_cell.PairRelayAdapter` and
:class:`~abt.pair_cell.PairCellMT5` protocols, and it drives its own
abstract, already-proven trader-operation payload shape (``market``,
``cancel``, ``close``, ``modify_sl_tp``) toward whatever ``mt5`` object is
injected -- the same shape the legacy Trader-relay path
(``abt.worker.reconciliation._trader_operation``) already sends. This module
is the seam that:

* translates that abstract payload into a genuine MetaTrader5 request before
  the real ``order_send`` call (reusing the exact translation the ordinary
  Trader-relay path already proves correct, so both paths stay identical);
* wraps this Worker's :class:`~abt.worker.session.AuthenticatedWorkerSession`
  as the cell's opaque Worker-to-Worker relay, translating the cell's small
  internal envelope shape to and from the full, controller-validated
  :class:`~abt.trader_protocol.PairCellRelayEnvelope` wire format;
* reads local quote evidence and continuously-maintained readiness facts from
  the same serialized MT5 handle reconciliation already uses -- enumerating,
  preloading (``symbol_select``), and polling every symbol in the active
  contract's eligible product universe (not only one fixed symbol), and
  relaying a per-symbol :class:`~abt.pair_cell.ProtectionCalibration` map
  with distinct profit/loss tick values where MT5 exposes them; and
* owns one :class:`~abt.pair_cell.PairExecutionCell`'s activation, restart
  recovery, per-loop event pumping, and clean shutdown for
  ``abt.worker.reconciliation``/``abt.worker.cli``.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
import logging
from hashlib import sha256
from pathlib import Path
from typing import Protocol, cast
from uuid import uuid4

from ..pair_cell import (
    BrokerSnapshotEvent,
    ClockTickEvent,
    LocalQuoteBatchEvent,
    LocalQuoteEvent,
    PairContract,
    PairExecutionCell,
    PairExecutionCellError,
    PairLease,
    PairResult,
    ProtectionCalibration,
    QuarantineReleaseEvent,
    ReadinessFactsEvent,
    RelayEnvelopeReceived,
    _contract_from_canonical,
    _parse_utc,
)
from .effect_journal import WorkerEffectJournal
from .reconciliation import (
    ReadOnlyMT5,
    WorkerEnrollmentError,
    _broker_close_request,
    _broker_order_request,
    _dispatch_scheduled_trader_rpc,
    _evidence,
    _require_terminal_trading_permission,
)
from .scheduler import DeadlineAwareTraderRpcScheduler, ScheduledTraderRpc, TraderRpcOutcome


_LOGGER = logging.getLogger(__name__)


class PairCellWorkerSession(Protocol):
    """The subset of ``AuthenticatedWorkerSession`` this adapter drives."""

    @property
    def trader_rpc_scheduler(self) -> DeadlineAwareTraderRpcScheduler: ...

    def dispatch_scheduler_outcome(self, outcome: TraderRpcOutcome) -> None: ...

    def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None: ...

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]: ...

    def drain_pair_cell_activations(self) -> list[dict[str, object]]: ...

    def drain_pair_cell_quarantine_releases(self) -> list[dict[str, object]]: ...

    def send_pair_cell_quarantine_release_result(self, *, request_id: str, symbol: str, outcome: str) -> None: ...

    def request_pair_cell_activation(self) -> dict[str, object] | None: ...


# --------------------------------------------------------------------------- #
# Relay: translate the cell's internal envelope <-> the wire relay envelope
# --------------------------------------------------------------------------- #


class WorkerPairCellRelay:
    """Implements :class:`~abt.pair_cell.PairRelayAdapter` over one authenticated session."""

    def __init__(self, session: PairCellWorkerSession) -> None:
        self._session = session
        self._contract: PairContract | None = None
        self._lease: PairLease | None = None

    def bind(self, contract: PairContract, lease: PairLease) -> None:
        self._contract, self._lease = contract, lease

    def send(self, envelope: dict[str, object]) -> None:
        contract, lease = self._contract, self._lease
        if contract is None or lease is None:
            return  # not yet activated: nothing to relay
        kind = str(envelope["kind"])
        inner_payload = envelope["payload"]
        wire_payload = {"kind": kind, "payload": inner_payload}
        payload_json = json.dumps(wire_payload, separators=(",", ":"), sort_keys=True, ensure_ascii=False)
        request_id = str(uuid4())
        wire_envelope = {
            "type": "pair_cell_relay",
            "protocol_version": 1,
            "lease": {
                "leader_worker_id": contract.leader_worker_id,
                "follower_worker_id": contract.follower_worker_id,
                "trader_id": contract.trader_id,
                "contract_hash": contract.hash,
                "lease_epoch": lease.epoch,
            },
            "from_worker_id": str(envelope["from_worker_id"]),
            "to_worker_id": str(envelope["to_worker_id"]),
            "request_id": request_id,
            "attempt_id": _attempt_id_for(kind, inner_payload if isinstance(inner_payload, dict) else {}),
            "payload_hash": sha256(payload_json.encode("utf-8")).hexdigest(),
            "payload": wire_payload,
        }
        self._session.send_pair_relay(wire_envelope, request_id=request_id)


def _attempt_id_for(kind: str, payload: Mapping[str, object]) -> str | None:
    if kind == "arm_request":
        attempt = payload.get("attempt")
        if isinstance(attempt, dict):
            attempt_id = attempt.get("attempt_id")
            return attempt_id if isinstance(attempt_id, str) else None
        return None
    if kind in ("arm_ack", "commit", "leg_status"):
        attempt_id = payload.get("attempt_id")
        return attempt_id if isinstance(attempt_id, str) else None
    return None  # quote / readiness relay is unscoped: continuous, not attempt-durable


def unwrap_pair_relay_envelope(envelope: Mapping[str, object]) -> dict[str, object] | None:
    """The inverse of :meth:`WorkerPairCellRelay.send`: wire envelope -> cell envelope.

    Returns ``None`` for a structurally malformed envelope rather than
    raising, since a delivery push must never crash the Worker's event loop;
    the cell's own peer-identity and lease-epoch checks reject anything that
    slips through maliciously or from a superseded epoch.
    """

    payload = envelope.get("payload")
    lease = envelope.get("lease")
    if not isinstance(payload, dict) or not isinstance(lease, dict):
        return None
    kind, inner_payload = payload.get("kind"), payload.get("payload")
    lease_epoch = lease.get("lease_epoch")
    if (
        not isinstance(kind, str)
        or not isinstance(inner_payload, dict)
        or isinstance(lease_epoch, bool)
        or not isinstance(lease_epoch, int)
    ):
        return None
    return {
        "kind": kind,
        "from_worker_id": envelope.get("from_worker_id"),
        "to_worker_id": envelope.get("to_worker_id"),
        "lease_epoch": lease_epoch,
        "payload": inner_payload,
    }


def _lease_from_wire(value: Mapping[str, object]) -> PairLease:
    return PairLease(
        leader_worker_id=cast(str, value["leader_worker_id"]),
        follower_worker_id=cast(str, value["follower_worker_id"]),
        trader_id=cast(str, value["trader_id"]),
        contract_hash=cast(str, value["contract_hash"]),
        epoch=cast(int, value["epoch"]),
        issued_at=_parse_utc(cast(str, value["issued_at"])),
        expires_at=_parse_utc(cast(str, value["expires_at"])),
    )


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

    def order_send(self, request: dict[str, object]) -> object:
        _require_terminal_trading_permission(self._mt5)
        broker_request = _pair_cell_broker_request(self._mt5, request)
        return self._mt5.order_send(broker_request)


def _pair_cell_broker_request(mt5: object, payload: dict[str, object]) -> dict[str, object]:
    operation = payload.get("type")
    if operation == "market":
        order: dict[str, object] = {"action": "market"}
        for field in ("symbol", "volume", "direction", "filling_mode"):
            if field in payload:
                order[field] = payload[field]
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
# Quotes and readiness: local facts already available to reconciliation
# --------------------------------------------------------------------------- #


def read_local_quote(
    mt5: object, *, symbol: str, recovery_epoch: str, sequence: int, now: Callable[[], datetime]
) -> LocalQuoteEvent:
    """One local quote event with broker tick time at millisecond resolution.

    The ordinary live-state quote feed (``reconciliation._live_quote``) only
    reads ``symbol_info_tick().time`` (whole seconds), which is fine for a
    dashboard but far too coarse for a cell whose quote-age/skew budgets and
    arm/commit deadlines are configured in fractions of a second: a tick
    could be up to 999ms staler than it appears. This reads ``time_msc``
    first and only falls back to ``time`` when milliseconds are unavailable.
    """

    evidence = _evidence(mt5.symbol_info_tick(symbol), "symbol tick")
    bid, ask = evidence.get("bid"), evidence.get("ask")
    if isinstance(bid, bool) or not isinstance(bid, (int, float)) or isinstance(ask, bool) or not isinstance(ask, (int, float)):
        raise WorkerEnrollmentError("The local MT5 terminal returned invalid symbol tick evidence.")
    return LocalQuoteEvent(
        symbol=symbol,
        bid=Decimal(str(bid)),
        ask=Decimal(str(ask)),
        broker_time=_tick_broker_time(evidence),
        local_receive_time=now(),
        recovery_epoch=recovery_epoch,
        sequence=sequence,
    )


def _tick_broker_time(evidence: dict[str, object]) -> datetime:
    milliseconds = evidence.get("time_msc")
    if isinstance(milliseconds, (int, float)) and not isinstance(milliseconds, bool) and milliseconds > 0:
        return datetime.fromtimestamp(milliseconds / 1000.0, UTC)
    seconds = evidence.get("time")
    if isinstance(seconds, (int, float)) and not isinstance(seconds, bool) and seconds > 0:
        return datetime.fromtimestamp(seconds, UTC)
    raise WorkerEnrollmentError("The local MT5 terminal returned invalid symbol tick evidence.")


def refresh_protection_calibration(mt5: object, symbol: str) -> ProtectionCalibration | None:
    """Refresh emergency/refined-protection calibration on a bounded schedule.

    Never called at signal time (see ``PairCellRuntime``'s bounded refresh
    interval): a stale plan simply removes readiness rather than triggering a
    signal-time recalculation.

    MT5 reports a single symmetric ``trade_tick_value`` for most products,
    but for some (notably futures-style and certain CFD products) it also
    exposes distinct ``trade_tick_value_profit``/``trade_tick_value_loss``
    fields whose values differ. When present and positive those distinct
    values are used; otherwise both sides fall back to ``trade_tick_value``
    so behavior is unchanged for ordinary symmetric-value products.
    """

    try:
        evidence = _evidence(mt5.symbol_info(symbol), "symbol")
    except WorkerEnrollmentError:
        return None
    tick_size = evidence.get("trade_tick_size")
    point = evidence.get("point")
    stops_level = evidence.get("trade_stops_level")
    freeze_level = evidence.get("trade_freeze_level")
    tick_value = evidence.get("trade_tick_value")
    profit_tick_value = evidence.get("trade_tick_value_profit")
    if not _positive_number(profit_tick_value):
        profit_tick_value = tick_value
    loss_tick_value = evidence.get("trade_tick_value_loss")
    if not _positive_number(loss_tick_value):
        loss_tick_value = tick_value
    if (
        not _positive_number(tick_size)
        or not _positive_number(point)
        or not _nonnegative_number(stops_level)
        or not _nonnegative_number(freeze_level)
        or not _positive_number(profit_tick_value)
        or not _positive_number(loss_tick_value)
    ):
        return None
    minimum_stop_distance = max(float(stops_level), float(freeze_level)) * float(point)
    return ProtectionCalibration(
        tick_size=str(Decimal(str(tick_size))),
        point=str(Decimal(str(point))),
        minimum_stop_distance=str(Decimal(str(minimum_stop_distance))),
        profit_tick_value=str(Decimal(str(profit_tick_value))),
        loss_tick_value=str(Decimal(str(loss_tick_value))),
    )


def eligible_symbols_of(contract: PairContract) -> tuple[str, ...]:
    """The full eligible product universe this contract authorizes.

    An empty automatic-policy tuple intentionally means the broker's entire
    executable catalog; :func:`preload_eligible_symbols` resolves it locally.
    A legacy contract instead names exactly one fixed ``symbol``.
    """

    if contract.entry_edge_points is not None:
        return contract.eligible_symbols
    if contract.symbol is not None:
        return (contract.symbol,)
    return ()


def preload_eligible_symbols(mt5: object, symbols: tuple[str, ...]) -> tuple[str, ...]:
    """Best-effort enumerate and preload every eligible symbol this broker exposes.

    Adds each symbol to MT5 Market Watch (``symbol_select``) so its quotes
    and calibration are available off the critical path, then returns only
    the subset genuinely visible in this terminal. A symbol this broker does
    not carry (or cannot make visible) is skipped with a warning rather than
    failing the whole pair -- the leader simply never selects it.
    """

    candidates = symbols
    if not candidates:
        try:
            raw_symbols = mt5.symbols_get()
        except Exception:
            _LOGGER.warning("Could not enumerate the MT5 product catalog.", exc_info=True)
            return ()
        if raw_symbols is None:
            _LOGGER.warning("MT5 returned no product catalog.")
            return ()
        discovered: list[str] = []
        for raw_symbol in raw_symbols:
            try:
                evidence = _evidence(raw_symbol, "symbol")
            except WorkerEnrollmentError:
                continue
            name = evidence.get("name")
            trade_mode = evidence.get("trade_mode")
            if isinstance(name, str) and name and trade_mode == 4:
                discovered.append(name)
        candidates = tuple(sorted(set(discovered)))

    visible: list[str] = []
    for symbol in candidates:
        try:
            mt5.symbol_select(symbol, True)
        except Exception:
            _LOGGER.warning("Could not select eligible symbol %s in MT5 Market Watch.", symbol)
        try:
            evidence = _evidence(mt5.symbol_info(symbol), "symbol")
        except WorkerEnrollmentError:
            _LOGGER.warning("Eligible symbol %s is not available in this MT5 terminal.", symbol)
            continue
        if evidence.get("visible") is False:
            _LOGGER.warning("Eligible symbol %s could not be made visible in MT5 Market Watch.", symbol)
            continue
        visible.append(symbol)
    return tuple(visible)


def _positive_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0


def _nonnegative_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool) and value >= 0


def read_readiness_facts(
    mt5: ReadOnlyMT5,
    *,
    login: int,
    server: str,
    calibrations: Mapping[str, ProtectionCalibration],
    calibration_fresh: bool,
    now: datetime,
    minimum_margin_headroom_usd: Decimal = Decimal("0"),
) -> ReadinessFactsEvent:
    """Continuously-maintained local account facts; never a signal-time read.

    Mirrors the same broker-evidence rules ``reconciliation``/``pair_cell``
    already enforce everywhere else: MT5 ``None``/malformed/unavailable
    records are read failures, never an empty or ready fact.

    ``calibrations`` carries one refreshed :class:`ProtectionCalibration` per
    currently visible eligible symbol (see ``PairCellRuntime``); the cell
    relays this map to the peer Worker as ``symbol_calibrations`` and uses it
    to evaluate every coherent eligible symbol, not only one fixed product.
    ``protection_calibration`` (the legacy single-symbol field the cell's
    readiness gate still reads for a fixed-symbol contract) is populated
    whenever exactly one symbol is currently calibrated.
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
    single_calibration = next(iter(calibrations.values())) if len(calibrations) == 1 else None
    return ReadinessFactsEvent(
        account_login=str(login),
        account_server=server,
        terminal_connected=terminal_connected,
        orders=orders,
        positions=positions,
        margin_headroom_ok=margin_ok,
        symbol_constraints_fresh=calibration_fresh,
        protection_calibration=single_calibration,
        observed_at=now,
        symbol_calibrations=dict(calibrations),
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


# --------------------------------------------------------------------------- #
# Orchestrator: activation/recovery, per-loop pumping, and shutdown
# --------------------------------------------------------------------------- #


@dataclass(frozen=True, slots=True)
class PairCellPollingConfig:
    quote_interval: timedelta = timedelta(milliseconds=200)
    readiness_interval: timedelta = timedelta(milliseconds=250)
    broker_snapshot_interval: timedelta = timedelta(seconds=1)
    calibration_interval: timedelta = timedelta(minutes=5)
    minimum_margin_headroom_usd: Decimal = Decimal("0")


class PairCellRuntime:
    """Owns one Worker's Pair Execution Cell construction, controller-driven
    activation/recovery, per-loop event pumping, and clean shutdown.

    Constructing this is always safe (and cheap) even for a Worker that has
    not opted any pair into the Pair Execution Cell: with no contract
    activated, :meth:`pump` and :meth:`drain_relay` are no-ops. This lets
    ``abt.worker.cli`` always create the durable cell database beside the
    Worker's identity/effect-journal files and always attempt controller
    sync, satisfying restart-construction without requiring opt-in
    configuration to be threaded through the CLI.
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
        polling: PairCellPollingConfig = PairCellPollingConfig(),
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session = session
        self._raw_mt5 = mt5
        self._effect_journal = effect_journal
        self._recovery_epoch = recovery_epoch
        self._login, self._server = login, server
        self._polling = polling
        self._now = now
        self._sequence = 0
        self._contract: PairContract | None = None
        self._eligible_symbols: tuple[str, ...] = ()
        self._visible_symbols: tuple[str, ...] = ()
        self._calibrations: dict[str, ProtectionCalibration] = {}
        epoch = now()
        self._next_quote_at = epoch
        self._next_readiness_at = epoch
        self._next_snapshot_at = epoch
        self._next_calibration_at = epoch
        self._relay = WorkerPairCellRelay(session)
        self._cell_mt5 = WorkerPairCellMT5(mt5)
        self._activation_path = db_path.with_suffix(".activation.json")
        self._cell = PairExecutionCell(
            worker_id=worker_id,
            db_path=db_path,
            relay=self._relay,
            mt5=self._cell_mt5,
            effect_journal=effect_journal,
            scheduler=session.trader_rpc_scheduler,
            serve_foreign_scheduler_item=self._serve_foreign_scheduler_item,
            dispatch_foreign_scheduler_outcome=session.dispatch_scheduler_outcome,
        )
        try:
            self._cell.recover()
        except PairExecutionCellError:
            _LOGGER.warning("Pair Execution Cell durable state failed to recover cleanly.", exc_info=True)
        self._load_local_activation()
        self._sync_activation_from_controller()

    def _serve_foreign_scheduler_item(self, scheduled: ScheduledTraderRpc) -> None:
        _dispatch_scheduled_trader_rpc(cast(ReadOnlyMT5, self._raw_mt5), self._session, scheduled, None, self._effect_journal)

    def _load_local_activation(self) -> None:
        """Rebind this adapter's own contract/lease from the last activation
        this Worker durably applied, independent of controller reachability.

        ``PairExecutionCell.recover()`` already restores the *cell's* own
        lease/contract from its private database; this sidecar file is what
        lets this *adapter* (which owns quote/readiness pumping and the
        outgoing relay's lease/contract binding) resume identically after a
        restart even when the controller cannot be reached, matching "a
        follower may immediately apply account-local emergency recovery when
        the leader or relay is unavailable."
        """

        if not self._activation_path.exists():
            return
        try:
            activation = json.loads(self._activation_path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            _LOGGER.warning("Pair Execution Cell local activation file is unreadable.", exc_info=True)
            return
        if isinstance(activation, dict):
            self._apply_activation(activation, persist=False)

    def _sync_activation_from_controller(self) -> None:
        try:
            activation = self._session.request_pair_cell_activation()
        except Exception:
            _LOGGER.warning("Pair Execution Cell controller sync failed at startup.", exc_info=True)
            return
        if activation is not None:
            self._apply_activation(activation)

    def _apply_activation(self, activation: Mapping[str, object], *, persist: bool = True) -> None:
        try:
            contract = _contract_from_canonical(cast(dict[str, object], activation["contract"]))
            lease = _lease_from_wire(cast(dict[str, object], activation["lease"]))
            self._relay.bind(contract, lease)
            self._cell.activate(contract, lease)
            self._contract = contract
        except (KeyError, ValueError, PairExecutionCellError):
            _LOGGER.warning("Received an invalid Pair Execution Cell activation push.", exc_info=True)
            return
        # Enumerate and preload this contract's full eligible product universe
        # immediately: readiness/quote polling must never wait on a later,
        # signal-adjacent calibration cycle to discover which symbols exist.
        self._eligible_symbols = eligible_symbols_of(contract)
        self._visible_symbols = preload_eligible_symbols(self._raw_mt5, self._eligible_symbols)
        self._calibrations = {
            symbol: calibration
            for symbol, calibration in self._calibrations.items()
            if symbol in self._visible_symbols
        }
        if persist:
            try:
                self._activation_path.write_text(
                    json.dumps({"contract": activation["contract"], "lease": activation["lease"]}), encoding="utf-8"
                )
            except OSError:
                _LOGGER.warning("Could not persist the Pair Execution Cell activation sidecar file.", exc_info=True)

    def drain_relay(self) -> PairResult | None:
        """Process every already-queued activation, quarantine release, and
        relay envelope now.

        Called every iteration of the Worker's receive loop (not gated by a
        polling interval): arm/commit turnaround is this feature's latency
        -critical path. A pushed quarantine release is delivered to this
        Worker's own cell regardless of whether a contract is currently
        activated -- quarantine is durable, Worker-local product state, not
        contract-scoped -- so an admin release always reaches
        ``PairExecutionCell.handle_event`` even between activations. Every
        release request is answered with this Worker's own actual
        applied/rejected outcome (correlated by ``request_id``), so the
        controller's admin route never has to guess and never reports
        success unless every Worker genuinely applied it.
        """

        result: PairResult | None = None
        for activation in self._session.drain_pair_cell_activations():
            self._apply_activation(activation)
        for release in self._session.drain_pair_cell_quarantine_releases():
            try:
                request_id = cast(str, release["request_id"])
                symbol = cast(str, release["symbol"])
            except KeyError:
                _LOGGER.warning(
                    "Received a malformed Pair Execution Cell quarantine-release request.", exc_info=True
                )
                continue
            try:
                observed_at = _parse_utc(cast(str, release["observed_at"]))
                result = self._cell.handle_event(
                    QuarantineReleaseEvent(
                        symbol=symbol,
                        actor=cast(str, release["actor"]),
                        reason=cast(str, release["reason"]),
                        observed_at=observed_at,
                    )
                )
                outcome = (
                    "applied"
                    if self._cell.quarantine_release_marker(symbol) == observed_at.isoformat()
                    else "rejected"
                )
            except (KeyError, ValueError, PairExecutionCellError):
                _LOGGER.warning(
                    "Received an invalid Pair Execution Cell quarantine-release request.", exc_info=True
                )
                outcome = "rejected"
            self._session.send_pair_cell_quarantine_release_result(
                request_id=request_id, symbol=symbol, outcome=outcome
            )
        for envelope in self._session.drain_pair_relay_envelopes():
            inner = unwrap_pair_relay_envelope(envelope)
            if inner is not None and self._contract is not None:
                result = self._cell.handle_event(RelayEnvelopeReceived(inner))
        return result

    def quarantined_products(self) -> tuple[str, ...]:
        """This Worker's own currently quarantined products, passed through
        for observability (e.g. admin tooling or health surfaces)."""

        return self._cell.quarantined_products()

    def pump(self, observed_at: datetime) -> PairResult | None:
        """Drain relay traffic, then advance time-gated local evidence.

        Every symbol in the contract's eligible product universe is
        preloaded, calibrated, and quoted -- not only one fixed symbol -- so
        the leader can evaluate every coherent eligible product without a
        signal-time RPC.
        """

        result = self.drain_relay()
        if self._contract is None:
            return result  # this Worker has not opted any pair into the cell
        result = self._cell.handle_event(ClockTickEvent(observed_at))
        if observed_at >= self._next_calibration_at:
            self._next_calibration_at = observed_at + self._polling.calibration_interval
            self._visible_symbols = preload_eligible_symbols(self._raw_mt5, self._eligible_symbols)
            for symbol in self._visible_symbols:
                calibration = refresh_protection_calibration(self._raw_mt5, symbol)
                if calibration is not None:
                    self._calibrations[symbol] = calibration
                else:
                    self._calibrations.pop(symbol, None)
            for stale_symbol in set(self._calibrations) - set(self._visible_symbols):
                self._calibrations.pop(stale_symbol, None)
        if observed_at >= self._next_readiness_at:
            self._next_readiness_at = observed_at + self._polling.readiness_interval
            calibration_fresh = bool(self._calibrations)
            try:
                facts = read_readiness_facts(
                    cast(ReadOnlyMT5, self._raw_mt5),
                    login=self._login,
                    server=self._server,
                    calibrations=self._calibrations,
                    calibration_fresh=calibration_fresh,
                    now=observed_at,
                    minimum_margin_headroom_usd=self._polling.minimum_margin_headroom_usd,
                )
                result = self._cell.handle_event(facts)
            except Exception:
                _LOGGER.warning("Pair Execution Cell readiness read failed.", exc_info=True)
        if observed_at >= self._next_quote_at:
            self._next_quote_at = observed_at + self._polling.quote_interval
            quotes: list[LocalQuoteEvent] = []
            for symbol in self._visible_symbols:
                try:
                    self._sequence += 1
                    quote = read_local_quote(
                        self._raw_mt5,
                        symbol=symbol,
                        recovery_epoch=self._recovery_epoch,
                        sequence=self._sequence,
                        now=self._now,
                    )
                    quotes.append(quote)
                except WorkerEnrollmentError:
                    continue
            if quotes:
                result = self._cell.handle_event(LocalQuoteBatchEvent(tuple(quotes)))
        if observed_at >= self._next_snapshot_at:
            self._next_snapshot_at = observed_at + self._polling.broker_snapshot_interval
            try:
                orders = _optional_records(self._raw_mt5.orders_get())
                positions = _optional_records(self._raw_mt5.positions_get())
                result = self._cell.handle_event(
                    BrokerSnapshotEvent(orders=orders, positions=positions, observed_at=observed_at)
                )
            except Exception:
                _LOGGER.warning("Pair Execution Cell broker snapshot read failed.", exc_info=True)
        return result

    def request_close(self, reason: str) -> PairResult | None:
        if self._contract is None:
            return None
        return self._cell.request_close(reason)

    @property
    def activated(self) -> bool:
        """Whether a contract/lease is currently bound (recovered or pushed)."""

        return self._contract is not None

    def close(self) -> None:
        self._cell.close()
