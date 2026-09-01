from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
import logging
import math
from time import monotonic, sleep as _sleep
from typing import ContextManager, Protocol

from pydantic import ValidationError

from ..trader_protocol import BrokerReceipt, CalcMarginBatchRead, MAX_LIVE_SYMBOLS
from .enrollment import WorkerEnrollmentError, WorkerSessionDisconnected
from .effect_journal import EffectJournalError, WorkerEffectJournal
from .scheduler import BrokerActionNotStarted, ScheduledTraderRpc


_LOGGER = logging.getLogger(__name__)
_RECONNECT_BACKOFF_RESET_SECONDS = 5 * 60


class ReadOnlyMT5(Protocol):
    """The broker read surface used by the account-worker reconciler."""

    def account_info(self) -> object: ...

    def terminal_info(self) -> object: ...

    def orders_get(self) -> object: ...

    def positions_get(self) -> object: ...

    def symbols_get(self) -> object: ...

    def symbol_info(self, symbol: str) -> object: ...

    def symbol_select(self, symbol: str, enable: bool) -> bool: ...

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> object: ...

    def order_calc_profit(self, action: int, symbol: str, volume: float, open_price: float, close_price: float) -> object: ...

    def order_check(self, request: dict[str, object]) -> object: ...

    def order_send(self, request: dict[str, object]) -> object: ...

    def symbol_info_tick(self, symbol: str) -> object: ...

    def copy_ticks_range(self, symbol: str, from_time: datetime, to_time: datetime, flags: int) -> object: ...


class AuthenticatedReconciliationSession(Protocol):
    reconciliation_cursor: int

    def request_password(self) -> str: ...

    def send_reconciliation(self, message: dict[str, object]) -> None: ...

    def send_live_state(self, message: dict[str, object]) -> None: ...


class WorkerSafetySession(Protocol):
    def heartbeat(self) -> bool: ...

    def send_recovery_state(self, state: str, reason: str) -> None: ...


class PairCellPump(Protocol):
    """The subset of ``abt.worker.pair_cell_adapter.PairCellRuntime`` this
    loop drives; injected via ``pair_cell_factory`` rather than imported
    directly, since that adapter module itself depends on this one."""

    def drain_relay(self) -> object: ...

    def pump(self, observed_at: datetime) -> object: ...

    @property
    def should_exit(self) -> bool: ...

    def close(self) -> None: ...


PairCellFactory = Callable[
    ["ReadOnlyMT5", "AuthenticatedReconciliationSession", "WorkerEffectJournal | None", int, str], PairCellPump
]


class AnalysisWorkerSession(Protocol):
    def receive_trader_rpc(self) -> ScheduledTraderRpc | dict[str, object] | None: ...

    def receive_worker_relay(self, timeout: float | None = None) -> bool: ...

    def send_trader_rpc(
        self, *, request_id: str, kind: str, accepted: bool, result: dict[str, object] | None = None,
        reason: str | None = None,
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None: ...

    def send_order_check(
        self,
        *,
        request_id: str,
        order: dict[str, object],
        accepted: bool,
        diagnostics: dict[str, object] | None = None,
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None: ...

    def send_order_check_error(self, *, request_id: str, reason: str) -> None: ...

    def send_order_execute(
        self,
        *,
        request_id: str,
        order: dict[str, object],
        accepted: bool,
        result: dict[str, object],
        execution_state: str | None = None,
        outcome: str | None = None,
    ) -> None: ...

    def send_order_execute_error(self, *, request_id: str, reason: str) -> None: ...


class WorkerSafetyAdapter:
    """Maintain the native worker's read-only lost-link safety state."""

    def __init__(self, session: WorkerSafetySession, *, lost_link_after: timedelta = timedelta(minutes=5)) -> None:
        self._session = session
        self._lost_link_after = lost_link_after
        self._last_controller_signal: datetime | None = None
        self.state = "connected"

    def heartbeat(self, observed_at: datetime) -> bool:
        """Exchange the required 30-second heartbeat and report safe state transitions."""

        if self._last_controller_signal is None:
            self._last_controller_signal = observed_at
        try:
            received = self._session.heartbeat()
        except WorkerSessionDisconnected:
            raise
        except WorkerEnrollmentError:
            received = False
        if received:
            self._last_controller_signal = observed_at
            if self.state != "connected":
                self.state = "connected"
                self._session.send_recovery_state("connected", "controller_signal_recovered")
            return True
        if observed_at - self._last_controller_signal >= self._lost_link_after and self.state != "lost_link_safety":
            self.state = "lost_link_safety"
            self._session.send_recovery_state("lost_link_safety", "five_minute_missed_heartbeat")
        return False

    def run_with_retries(
        self, operation: Callable[[], None], *, sleep: Callable[[float], None] = _sleep
    ) -> None:
        """Retry terminal/API failures at most three times before escalating to a human."""

        for attempt in range(3):
            try:
                operation()
                return
            except WorkerSessionDisconnected:
                raise
            except AccountMismatchError:
                self._needs_human("account_mismatch")
                raise
            except WorkerEnrollmentError:
                if attempt == 2:
                    self._needs_human("terminal_or_api_failure")
                    raise
                sleep(float(2 ** attempt))

    def _needs_human(self, reason: str) -> None:
        self.state = "needs_human"
        self._session.send_recovery_state("needs_human", reason)


class AccountMismatchError(WorkerEnrollmentError):
    """The terminal is logged into an account other than the worker's binding."""


class BrokerReceiptRejected(WorkerEnrollmentError):
    """MT5 returned an authoritative non-success receipt after a broker send."""


class BrokerEffectUncertain(WorkerEnrollmentError):
    """A durable effect crossed the send boundary without conclusive evidence."""


class MT5ReconciliationAdapter:
    """Poll one MT5 terminal without exposing a broker-write operation."""

    def __init__(
        self, mt5: ReadOnlyMT5, *, emit: Callable[[dict[str, object]], None], initial_cursor: int = 0
    ) -> None:
        if isinstance(initial_cursor, bool) or not isinstance(initial_cursor, int) or initial_cursor < 0:
            raise ValueError("The initial reconciliation cursor must be a non-negative integer.")
        self._mt5 = mt5
        self._emit = emit
        self._cursor = initial_cursor
        self._last_snapshot_at: datetime | None = None
        self._orders: dict[str, dict[str, object]] = {}
        self._positions: dict[str, dict[str, object]] = {}

    def poll(self, observed_at: datetime) -> None:
        """Read health and exposure once; callers schedule this every minute."""

        account = _evidence(self._mt5.account_info(), "account")
        terminal = _evidence(self._mt5.terminal_info(), "terminal")
        orders = _records(self._mt5.orders_get(), "order")
        positions = _records(self._mt5.positions_get(), "position")
        order_changes = _changes("order", self._orders, orders) if self._last_snapshot_at is not None else []
        position_changes = _changes("position", self._positions, positions) if self._last_snapshot_at is not None else []

        for change in order_changes + position_changes:
            self._cursor += 1
            self._emit(
                {
                    "type": "delta",
                    "cursor": self._cursor,
                    "observed_at": observed_at.isoformat(),
                    **change,
                }
            )
        self._orders = orders
        self._positions = positions

        if self._last_snapshot_at is None or observed_at - self._last_snapshot_at >= timedelta(minutes=10):
            self._cursor += 1
            self._emit(
                {
                    "type": "snapshot",
                    "cursor": self._cursor,
                    "observed_at": observed_at.isoformat(),
                    "account": account,
                    "terminal": terminal,
                    "orders": list(orders.values()),
                    "positions": list(positions.values()),
                }
            )
            self._last_snapshot_at = observed_at

    def run_forever(
        self,
        *,
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        sleep: Callable[[float], None] = _sleep,
        heartbeat: Callable[[], bool] | None = None,
        maintenance: Callable[[], None] | None = None,
        live_state: LiveWorkerMarketStateAdapter | None = None,
    ) -> None:
        """Run the required one-minute read-only polling cadence and 30-second heartbeats."""

        next_maintenance = now()
        next_reconciliation = next_maintenance
        next_heartbeat = next_maintenance
        while True:
            observed_at = now()
            if maintenance is not None and observed_at >= next_maintenance:
                maintenance()
                next_maintenance = observed_at + timedelta(days=1)
            if live_state is not None:
                if observed_at >= next_reconciliation:
                    self.poll(observed_at)
                    next_reconciliation = observed_at + timedelta(minutes=1)
                live_state.poll(observed_at)
                if heartbeat is not None and observed_at >= next_heartbeat:
                    heartbeat()
                    next_heartbeat = observed_at + timedelta(seconds=30)
                sleep(0.5)
                continue
            self.poll(observed_at)
            if heartbeat is None:
                sleep(60)
            else:
                heartbeat()
                sleep(30)
                heartbeat()
                sleep(30)


class LiveWorkerMarketStateAdapter:
    """Publish the latest terminal state at the live-management polling cadences."""

    def __init__(
        self,
        mt5: ReadOnlyMT5,
        *,
        watched_symbols: list[str],
        emit: Callable[[dict[str, object]], None],
    ) -> None:
        if not all(isinstance(symbol, str) and symbol for symbol in watched_symbols):
            raise ValueError("Watched symbols must be text values.")
        self._mt5 = mt5
        self._watched_symbols = list(dict.fromkeys(watched_symbols))
        self._emit = emit
        self._last_quotes_at: datetime | None = None
        self._last_connectivity_at: datetime | None = None
        self._quotes: dict[str, dict[str, object]] = {}
        self._connectivity: bool | None = None
        self._sent_snapshot = False

    def set_watched_symbols(self, symbols: list[str]) -> None:
        """Replace the quote set and require the next scheduled poll to snapshot it."""

        if (
            not symbols
            or len(symbols) > MAX_LIVE_SYMBOLS
            or not all(isinstance(symbol, str) and symbol for symbol in symbols)
            or len(set(symbols)) != len(symbols)
        ):
            raise ValueError("Watched symbols must be a unique, nonempty list within the configured limit.")
        self._watched_symbols = list(symbols)
        self._quotes = {}
        self._last_quotes_at = None
        self._sent_snapshot = False

    def poll(self, observed_at: datetime) -> None:
        """Poll due sources and publish only state that differs from the last observation."""

        changes: list[tuple[str, object]] = []
        if self._last_quotes_at is None or observed_at - self._last_quotes_at >= timedelta(milliseconds=500):
            quotes = {symbol: _live_quote(self._mt5, symbol) for symbol in self._watched_symbols}
            self._last_quotes_at = observed_at
            if quotes != self._quotes:
                self._quotes = quotes
                changes.append(("quotes", list(quotes.values())))
        if self._last_connectivity_at is None or observed_at - self._last_connectivity_at >= timedelta(seconds=5):
            terminal = _evidence(self._mt5.terminal_info(), "terminal")
            connected = terminal.get("connected")
            if not isinstance(connected, bool):
                raise WorkerEnrollmentError("The local MT5 terminal returned invalid connectivity evidence.")
            self._last_connectivity_at = observed_at
            if connected != self._connectivity:
                self._connectivity = connected
                changes.append(("connectivity", connected))

        if not self._sent_snapshot:
            self._emit(
                {
                    "type": "live_state_snapshot",
                    "observed_at": observed_at.isoformat(),
                    "connectivity": self._connectivity,
                    "quotes": list(self._quotes.values()),
                }
            )
            self._sent_snapshot = True
            return
        for entity, value in changes:
            self._emit({"type": "live_state_diff", "observed_at": observed_at.isoformat(), "entity": entity, "value": value})


def reconcile_authenticated_worker(
    *,
    mt5: ReadOnlyMT5,
    session: AuthenticatedReconciliationSession,
    login: int,
    server: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], None] = _sleep,
    maintenance: Callable[[], None] | None = None,
    effect_journal: WorkerEffectJournal | None = None,
    pair_cell_factory: PairCellFactory | None = None,
) -> None:
    """Use an authenticated session's memory-only password for read-only MT5 reconciliation."""

    send_recovery_sync = getattr(session, "send_recovery_sync", None)
    if callable(send_recovery_sync):
        send_recovery_sync([] if effect_journal is None else effect_journal.unresolved())
    password = session.request_password()
    initialized = False
    pair_cell: PairCellPump | None = None
    try:
        initialize = getattr(mt5, "initialize", None)
        login_mt5 = getattr(mt5, "login", None)
        if not callable(initialize) or not callable(login_mt5) or not initialize():
            raise WorkerEnrollmentError("Unable to initialize the local MT5 terminal.")
        initialized = True
        if not login_mt5(login, password=password, server=server):
            raise WorkerEnrollmentError("The local MT5 login was not accepted.")
        account = _evidence(mt5.account_info(), "account")
        if account.get("login") != login or account.get("server") != server:
            raise AccountMismatchError("The local MT5 account does not match the approved worker binding.")
        safety: WorkerSafetyAdapter | None = None
        if callable(getattr(session, "heartbeat", None)) and callable(getattr(session, "send_recovery_state", None)):
            safety = WorkerSafetyAdapter(session)  # type: ignore[arg-type]
        reconciliation = MT5ReconciliationAdapter(
            mt5, emit=session.send_reconciliation, initial_cursor=getattr(session, "reconciliation_cursor", 0)
        )
        send_live_state = getattr(session, "send_live_state", None)
        live_state = None
        if callable(send_live_state):
            live_state = LiveWorkerMarketStateAdapter(
                mt5, watched_symbols=_visible_symbols(mt5), emit=send_live_state
            )
        if pair_cell_factory is not None:
            pair_cell = pair_cell_factory(mt5, session, effect_journal, login, server)
        _run_reconciliation_with_relay(
            reconciliation,
            mt5=mt5,
            session=session,  # type: ignore[arg-type]
            now=now,
            safety=safety,
            maintenance=maintenance,
            live_state=live_state,
            effect_journal=effect_journal,
            pair_cell=pair_cell,
        )
    finally:
        if pair_cell is not None:
            pair_cell.close()
        password = ""
        if initialized:
            shutdown = getattr(mt5, "shutdown", None)
            if callable(shutdown):
                shutdown()


def reconcile_with_safety(
    *,
    mt5: ReadOnlyMT5,
    session: AuthenticatedReconciliationSession & WorkerSafetySession,
    login: int,
    server: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], None] = _sleep,
    maintenance: Callable[[], None] | None = None,
    effect_journal: WorkerEffectJournal | None = None,
    pair_cell_factory: PairCellFactory | None = None,
) -> None:
    """Run reconciliation with bounded terminal/API recovery and human escalation."""

    WorkerSafetyAdapter(session).run_with_retries(
        lambda: reconcile_authenticated_worker(
            mt5=mt5, session=session, login=login, server=server, now=now, sleep=sleep, maintenance=maintenance,
            effect_journal=effect_journal, pair_cell_factory=pair_cell_factory,
        ),
        sleep=sleep,
    )


def reconnect_worker_session(
    *,
    open_session: Callable[[], ContextManager[AuthenticatedReconciliationSession & WorkerSafetySession]],
    mt5: ReadOnlyMT5,
    login: int,
    server: str,
    sleep: Callable[[float], None] = _sleep,
    run_reconciliation: Callable[..., None] = reconcile_with_safety,
    maintenance: Callable[[AuthenticatedReconciliationSession], None] | None = None,
    effect_journal: WorkerEffectJournal | None = None,
    monotonic_clock: Callable[[], float] = monotonic,
    pair_cell_factory: PairCellFactory | None = None,
) -> None:
    """Reconnect an interrupted proved WSS session with a bounded exponential backoff."""

    attempts = 0
    while True:
        connected_at: float | None = None
        try:
            with open_session() as session:
                connected_at = monotonic_clock()
                run_reconciliation(
                    mt5=mt5,
                    session=session,
                    login=login,
                    server=server,
                    sleep=sleep,
                    maintenance=None if maintenance is None else lambda: maintenance(session),
                    effect_journal=effect_journal,
                    pair_cell_factory=pair_cell_factory,
                )
            return
        except WorkerSessionDisconnected as error:
            if connected_at is not None and monotonic_clock() - connected_at >= _RECONNECT_BACKOFF_RESET_SECONDS:
                attempts = 0
            if attempts >= 10:
                raise
            delay = float(5 * 2 ** attempts)
            _LOGGER.warning(
                "Worker session disconnected (%s); reconnecting attempt %s/10 in %.0f seconds.",
                error,
                attempts + 1,
                delay,
            )
            sleep(delay)
            attempts += 1


def _run_reconciliation_with_relay(
    reconciliation: MT5ReconciliationAdapter,
    *,
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    now: Callable[[], datetime],
    safety: WorkerSafetyAdapter | None,
    maintenance: Callable[[], None] | None,
    live_state: LiveWorkerMarketStateAdapter | None,
    effect_journal: WorkerEffectJournal | None,
    pair_cell: PairCellPump | None = None,
) -> None:
    next_maintenance = now()
    next_reconciliation = next_maintenance
    while True:
        observed_at = now()
        if maintenance is not None and observed_at >= next_maintenance:
            maintenance()
            next_maintenance = observed_at + timedelta(days=1)
        if observed_at >= next_reconciliation:
            reconciliation.poll(observed_at)
            next_reconciliation = observed_at + timedelta(minutes=1)
        _serve_pending_trader_rpc(mt5, session, live_state, effect_journal)
        if live_state is not None:
            live_state.poll(observed_at)
        for _ in range(2):
            session.receive_worker_relay(timeout=0.25 if live_state is not None else 30.0)
            if pair_cell is not None:
                # Arm/commit relay turnaround is this feature's latency
                # -critical path, so it is drained every inner iteration
                # rather than gated behind the outer polling cadence below.
                pair_cell.drain_relay()
            _serve_pending_trader_rpc(mt5, session, live_state, effect_journal)
            if safety is not None:
                safety.heartbeat(now())
        if pair_cell is not None:
            pair_cell.pump(observed_at)
            if bool(getattr(pair_cell, "should_exit", False)):
                return


def _serve_pending_trader_rpc(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    live_state: LiveWorkerMarketStateAdapter | None,
    effect_journal: WorkerEffectJournal | None,
) -> None:
    receive_trader_rpc = getattr(session, "receive_trader_rpc", None)
    if not callable(receive_trader_rpc):
        return
    trader_rpc = receive_trader_rpc()
    if trader_rpc is None:
        return
    if isinstance(trader_rpc, ScheduledTraderRpc):
        _dispatch_scheduled_trader_rpc(mt5, session, trader_rpc, live_state, effect_journal)
        return
    _serve_trader_rpc(mt5, session, trader_rpc, live_state=live_state)


def _dispatch_scheduled_trader_rpc(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    scheduled: ScheduledTraderRpc,
    live_state: LiveWorkerMarketStateAdapter | None,
    effect_journal: WorkerEffectJournal | None,
) -> None:
    """Route one already-dequeued :class:`ScheduledTraderRpc` to its handler and complete it.

    Shared by the ordinary ``receive_trader_rpc`` pump above and by
    ``abt.worker.pair_cell_adapter``'s foreign-scheduler-item callback: the
    Pair Execution Cell shares this Worker's single scheduler (see
    ``abt.pair_cell.PairExecutionCell``'s constructor), so a foreign item its
    own broker write drains ahead of must be served through this exact same
    routing -- never a second, divergent dispatch path.
    """

    request_type = scheduled.request.get("worker_request_type")
    worker_request = scheduled.request.get("worker_request")
    if request_type in {"order_check_request", "order_execute_request"} and isinstance(worker_request, dict):
        _serve_scheduled_hedge_request(mt5, session, scheduled, request_type, worker_request, effect_journal)
        return
    _serve_scheduled_trader_rpc(mt5, session, scheduled, live_state=live_state, effect_journal=effect_journal)


def _serve_scheduled_hedge_request(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    scheduled: ScheduledTraderRpc,
    request_type: str,
    request: dict[str, object],
    effect_journal: WorkerEffectJournal | None,
) -> None:
    category = (
        _serve_order_check(mt5, session, request, before_broker_action=scheduled.before_broker_send)
        if request_type == "order_check_request"
        else _serve_order_execute(
            mt5, session, request, before_broker_send=scheduled.before_broker_send, effect_journal=effect_journal
        )
    )
    scheduled.complete(category, f"The scheduled {request_type} reached a terminal Worker outcome.")


def _serve_order_check(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    request: dict[str, object],
    *,
    before_broker_action: Callable[[], None] | None = None,
) -> str:
    request_id = _request_text(request, "request_id")
    order = request.get("order")
    if not isinstance(order, dict):
        session.send_order_check_error(request_id=request_id, reason="The controller requested an invalid order check.")
        return "rejected_preflight"
    if _request_expired(request):
        session.send_order_check(
            request_id=request_id,
            order=order,
            accepted=False,
            execution_state="not_started",
            outcome="expired_not_started",
        )
        return "expired_not_started"
    try:
        _require_terminal_trading_permission(mt5)
        if before_broker_action is not None:
            before_broker_action()
        result = mt5.order_check(_broker_order_request(mt5, order))
        diagnostics = _order_check_diagnostics(mt5, str(order["symbol"]), result)
        accepted = diagnostics["retcode"] == 0
        _LOGGER.debug(
            "Order check %s for %s completed with retcode %s (%s).",
            request_id, order["symbol"], diagnostics["retcode"], "accepted" if accepted else "rejected",
        )
        session.send_order_check(request_id=request_id, order=order, accepted=accepted, diagnostics=diagnostics)
        return "completed"
    except BrokerActionNotStarted as error:
        if error.outcome.category == "expired_not_started":
            session.send_order_check(
                request_id=request_id,
                order=order,
                accepted=False,
                execution_state="not_started",
                outcome="expired_not_started",
            )
        else:
            session.send_order_check_error(request_id=request_id, reason=f"{error.outcome.category}: {error.outcome.reason}")
        return error.outcome.category
    except WorkerEnrollmentError as error:
        session.send_order_check_error(request_id=request_id, reason=str(error))
        return "rejected_preflight"
    except Exception:
        session.send_order_check_error(request_id=request_id, reason="The local MT5 order check failed.")
        return "rejected_preflight"


def _order_check_diagnostics(mt5: ReadOnlyMT5, symbol: str, result: object) -> dict[str, object]:
    evidence = _evidence(result, "order check")
    retcode = evidence.get("retcode")
    if isinstance(retcode, bool) or not isinstance(retcode, int):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid order check result.")
    diagnostics: dict[str, object] = {"retcode": retcode}
    comment = evidence.get("comment")
    if isinstance(comment, str) and comment:
        diagnostics["comment"] = comment
    for field in ("margin", "margin_free", "margin_level"):
        value = evidence.get(field)
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            diagnostics[field] = float(value)
    try:
        tick = _evidence(mt5.symbol_info_tick(symbol), "symbol tick")
    except WorkerEnrollmentError:
        return diagnostics
    quote = {
        field: tick[field]
        for field in ("bid", "ask", "time")
        if isinstance(tick.get(field), (int, float)) and not isinstance(tick.get(field), bool)
    }
    if quote:
        diagnostics["quote"] = quote
    return diagnostics


def _serve_order_execute(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    request: dict[str, object],
    *,
    before_broker_send: Callable[[], None] | None = None,
    effect_journal: WorkerEffectJournal | None = None,
) -> str:
    request_id = _request_text(request, "request_id")
    order = request.get("order")
    if not isinstance(order, dict):
        session.send_order_execute_error(request_id=request_id, reason="The controller requested an invalid order execution.")
        return "rejected_preflight"
    order_send_started = False
    broker_response_received = False
    try:
        effect_id = request.get("effect_id", f"hedged-entry:{request_id}")
        if not isinstance(effect_id, str) or not effect_id:
            raise WorkerEnrollmentError("The controller requested an invalid broker effect ID.")
        journal_state = effect_journal.prepare(effect_id, order) if effect_journal is not None else "prepared"
        if journal_state in {"receipt", "observed"}:
            assert effect_journal is not None
            result = effect_journal.evidence(effect_id)
            retcode = result.get("retcode")
            accepted = isinstance(retcode, int) and retcode in {
                getattr(mt5, "TRADE_RETCODE_DONE", -1),
                getattr(mt5, "TRADE_RETCODE_PLACED", -2),
            }
            session.send_order_execute(request_id=request_id, order=order, accepted=accepted, result=result)
            return "completed"
        if journal_state != "prepared":
            _send_unknown_order_execute(session, request_id, order)
            return "unknown_after_send"
        if _request_expired(request):
            _send_expired_order_execute(session, request_id, order)
            return "expired_not_started"
        _require_terminal_trading_permission(mt5)
        positions_before = (
            {}
            if journal_state in {"receipt", "observed"}
            else _records(mt5.positions_get(), "position")
        )
        broker_request = _broker_order_request(mt5, order)
        if _request_expired(request):
            _send_expired_order_execute(session, request_id, order)
            return "expired_not_started"
        order_send_started = True
        sent = _journaled_order_send(
            mt5, broker_request, journal_payload=order, effect_journal=effect_journal,
            effect_id=effect_id, before_broker_send=before_broker_send
        )
        result = _evidence(sent, "order execution")
        broker_response_received = True
        if effect_journal is not None:
            effect_journal.record_receipt(effect_id, result)
        retcode = result.get("retcode")
        accepted = isinstance(retcode, int) and retcode in {
            getattr(mt5, "TRADE_RETCODE_DONE", -1),
            getattr(mt5, "TRADE_RETCODE_PLACED", -2),
        }
        if accepted and order.get("action") == "market" and not isinstance(result.get("position"), (int, dict)):
            result["position"] = _new_market_position(mt5, order, positions_before)
        if effect_journal is not None:
            effect_journal.record_receipt(effect_id, result)
        if not accepted:
            _LOGGER.warning(
                "MT5 order execution was rejected: action=%s symbol=%s volume=%s retcode=%r comment=%r last_error=%r",
                order.get("action"), order.get("symbol"), order.get("volume"), retcode, result.get("comment"),
                _mt5_last_error(mt5),
            )
        session.send_order_execute(request_id=request_id, order=order, accepted=accepted, result=result)
        return "completed"
    except BrokerActionNotStarted as error:
        if error.outcome.category == "expired_not_started":
            _send_expired_order_execute(session, request_id, order)
        else:
            session.send_order_execute_error(
                request_id=request_id,
                reason=f"{error.outcome.category}: {error.outcome.reason}",
            )
        return error.outcome.category
    except EffectJournalError as error:
        _LOGGER.warning("MT5 order execution was blocked by the local effect journal: %s", error)
        session.send_order_execute_error(request_id=request_id, reason=str(error))
        return "rejected_preflight"
    except WorkerEnrollmentError as error:
        if order_send_started and not broker_response_received:
            _send_unknown_order_execute(session, request_id, order)
            return "unknown_after_send"
        _LOGGER.warning(
            "MT5 order execution could not produce broker evidence: action=%s symbol=%s volume=%s reason=%s last_error=%r",
            order.get("action"), order.get("symbol"), order.get("volume"), error, _mt5_last_error(mt5),
        )
        session.send_order_execute_error(request_id=request_id, reason=str(error))
        return "rejected_preflight"
    except Exception:
        if order_send_started and not broker_response_received:
            _LOGGER.warning(
                "MT5 order execution outcome is unknown after broker send: action=%s symbol=%s volume=%s last_error=%r",
                order.get("action"), order.get("symbol"), order.get("volume"), _mt5_last_error(mt5),
            )
            _send_unknown_order_execute(session, request_id, order)
            return "unknown_after_send"
        _LOGGER.exception(
            "MT5 order execution failed: action=%s symbol=%s volume=%s last_error=%r",
            order.get("action"), order.get("symbol"), order.get("volume"), _mt5_last_error(mt5),
        )
        session.send_order_execute_error(request_id=request_id, reason="The local MT5 order execution failed.")
        return "rejected_preflight"


def _request_expired(request: dict[str, object], *, now: Callable[[], datetime] = lambda: datetime.now(UTC)) -> bool:
    expires_at = request.get("expires_at")
    if expires_at is None:
        return False
    if not isinstance(expires_at, str):
        raise WorkerEnrollmentError("The controller requested an invalid request expiry.")
    try:
        expiry = _trader_utc(expires_at)
    except ValueError as error:
        raise WorkerEnrollmentError("The controller requested an invalid request expiry.") from error
    return now() >= expiry


def _send_expired_order_execute(session: AnalysisWorkerSession, request_id: str, order: dict[str, object]) -> None:
    session.send_order_execute(
        request_id=request_id,
        order=order,
        accepted=False,
        result={},
        execution_state="not_started",
        outcome="expired_not_started",
    )


def _send_unknown_order_execute(session: AnalysisWorkerSession, request_id: str, order: dict[str, object]) -> None:
    session.send_order_execute(
        request_id=request_id,
        order=order,
        accepted=False,
        result={},
        execution_state="sent",
        outcome="unknown_after_send",
    )


def _serve_trader_rpc(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    request: dict[str, object],
    *,
    live_state: LiveWorkerMarketStateAdapter | None = None,
) -> None:
    request_id = _request_text(request, "request_id")
    kind = _request_text(request, "kind")
    payload = request.get("payload")
    if kind not in {"read", "operation"} or not isinstance(payload, dict):
        raise WorkerEnrollmentError("The controller requested an invalid Trader RPC request.")
    try:
        result = (
            _trader_read(mt5, payload)
            if kind == "read"
            else _set_live_symbols(mt5, live_state, payload)
            if payload.get("type") == "set_live_symbols"
            else _trader_operation(mt5, payload)
        )
        session.send_trader_rpc(request_id=request_id, kind=kind, accepted=True, result=result)
    except (WorkerEnrollmentError, ValueError) as error:
        session.send_trader_rpc(
            request_id=request_id,
            kind=kind,
            accepted=False,
            reason=str(error),
        )
    except Exception:
        _LOGGER.exception("Trader %s request failed.", kind)
        session.send_trader_rpc(
            request_id=request_id,
            kind=kind,
            accepted=False,
            reason="The local MT5 terminal failed while handling the Trader request.",
        )


def _serve_scheduled_trader_rpc(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    scheduled: ScheduledTraderRpc,
    *,
    live_state: LiveWorkerMarketStateAdapter | None = None,
    effect_journal: WorkerEffectJournal | None = None,
) -> None:
    """Execute one selected RPC once, broadcasting a coalesced read result."""

    request = scheduled.request
    request_id = _request_text(request, "request_id")
    kind = _request_text(request, "kind")
    payload = request.get("payload")
    if kind not in {"read", "operation"} or not isinstance(payload, dict):
        outcome = scheduled.complete(
            "rejected_preflight",
            "The controller requested an invalid Trader RPC request.",
        )
        _send_scheduled_trader_rpc_failure(session, scheduled, outcome.category, outcome.reason)
        return
    try:
        result = (
            _trader_read(mt5, payload)
            if kind == "read"
            else _set_live_symbols(mt5, live_state, payload)
            if payload.get("type") == "set_live_symbols"
            else _trader_operation(
                mt5,
                payload,
                before_broker_send=scheduled.before_broker_send,
                effect_journal=effect_journal,
                effect_id=scheduled.command_id,
            )
        )
        scheduled.complete("completed", "The MT5 operation completed with an authoritative result.")
        for response_request in scheduled.requests:
            session.send_trader_rpc(
                request_id=_request_text(response_request, "request_id"),
                kind=kind,
                accepted=True,
                result=result,
            )
    except BrokerActionNotStarted as error:
        _send_scheduled_trader_rpc_failure(session, scheduled, error.outcome.category, error.outcome.reason)
    except BrokerReceiptRejected as error:
        scheduled.complete("completed", str(error))
        _send_scheduled_trader_rpc_failure(session, scheduled, "completed", str(error))
    except BrokerEffectUncertain as error:
        scheduled.complete("unknown_after_send", str(error))
        _send_scheduled_trader_rpc_failure(session, scheduled, "unknown_after_send", str(error))
    except (WorkerEnrollmentError, ValueError) as error:
        category = "unknown_after_send" if "mt5_action_started_at" in scheduled.telemetry else "rejected_preflight"
        scheduled.complete(category, str(error))
        _send_scheduled_trader_rpc_failure(session, scheduled, category, str(error))
    except Exception:
        category = "unknown_after_send" if "mt5_action_started_at" in scheduled.telemetry else "rejected_preflight"
        reason = "The local MT5 terminal failed while handling the Trader request."
        if category == "unknown_after_send":
            _LOGGER.warning("Scheduled Trader %s request has an unknown outcome after MT5 send.", kind)
        else:
            _LOGGER.exception("Scheduled Trader %s request failed before MT5 send.", kind)
        scheduled.complete(category, reason)
        _send_scheduled_trader_rpc_failure(session, scheduled, category, reason)


def _send_scheduled_trader_rpc_failure(
    session: AnalysisWorkerSession,
    scheduled: ScheduledTraderRpc,
    category: str,
    reason: str,
) -> None:
    for request in scheduled.requests:
        session.send_trader_rpc(
            request_id=_request_text(request, "request_id"),
            kind=_request_text(request, "kind"),
            accepted=False,
            reason=f"{category}: {reason}",
            execution_state="sent" if category == "unknown_after_send" else "not_started",
            outcome=category,
        )


def _set_live_symbols(
    mt5: ReadOnlyMT5, live_state: LiveWorkerMarketStateAdapter | None, payload: dict[str, object]
) -> dict[str, object]:
    if live_state is None:
        raise WorkerEnrollmentError("Live market state is unavailable.")
    symbols = payload.get("symbols")
    if (
        set(payload) != {"type", "symbols"}
        or not isinstance(symbols, list)
        or not symbols
        or len(symbols) > MAX_LIVE_SYMBOLS
        or not all(isinstance(symbol, str) and symbol for symbol in symbols)
        or len(set(symbols)) != len(symbols)
    ):
        raise WorkerEnrollmentError("The controller requested invalid live symbols.")
    for symbol in symbols:
        evidence = _evidence(mt5.symbol_info(symbol), "symbol")
        trade_mode, point = evidence.get("trade_mode"), evidence.get("point")
        if (
            isinstance(trade_mode, bool)
            or not isinstance(trade_mode, int)
            or trade_mode != 4
            or isinstance(point, bool)
            or not isinstance(point, (int, float))
            or not math.isfinite(point)
            or point <= 0
        ):
            raise WorkerEnrollmentError(f"Symbol {symbol} is not tradeable for live quotes.")
    for symbol in symbols:
        if not mt5.symbol_select(symbol, True):
            raise WorkerEnrollmentError(f"Could not add {symbol} to Market Watch.")
    live_state.set_watched_symbols(symbols)
    return {"symbols": symbols}


def _trader_read(mt5: ReadOnlyMT5, payload: dict[str, object]) -> dict[str, object]:
    operation = _request_text(payload, "type")
    if operation == "account_info" and set(payload) == {"type"}:
        return {"account": _json_evidence(mt5.account_info(), "account")}
    if operation == "symbol_info" and set(payload) == {"type", "symbol"}:
        return {"symbol": _json_evidence(mt5.symbol_info(_request_text(payload, "symbol")), "symbol")}
    if operation == "symbols" and set(payload) == {"type"}:
        symbols = mt5.symbols_get()
        if not isinstance(symbols, (list, tuple)):
            raise WorkerEnrollmentError("The local MT5 terminal returned an invalid symbol catalog.")
        return {"symbols": [_json_evidence(value, "symbol") for value in symbols]}
    if operation == "calc_margin" and set(payload) == {"type", "symbol", "volume", "direction", "price"}:
        return {"margin": _trader_margin(mt5, payload)}
    if operation == "calc_margin_batch":
        try:
            batch = CalcMarginBatchRead.model_validate(payload)
        except ValidationError as error:
            raise WorkerEnrollmentError("The controller requested an invalid Trader broker read.") from error
        calculations = [calculation.model_dump(mode="json") for calculation in batch.calculations]
        return {
            "margins": [
                {**calculation, "margin": _trader_margin(mt5, calculation)}
                for calculation in calculations
            ]
        }
    if operation == "calc_profit" and set(payload) == {"type", "symbol", "volume", "direction", "open_price", "close_price"}:
        direction = _request_text(payload, "direction")
        action = getattr(mt5, "ORDER_TYPE_BUY") if direction == "LONG" else getattr(mt5, "ORDER_TYPE_SELL")
        profit = mt5.order_calc_profit(
            action,
            _request_text(payload, "symbol"),
            float(_request_text(payload, "volume")),
            float(_request_text(payload, "open_price")),
            float(_request_text(payload, "close_price")),
        )
        if isinstance(profit, bool) or not isinstance(profit, (int, float)):
            raise WorkerEnrollmentError("The local MT5 terminal returned an invalid calculated profit.")
        return {"profit": profit}
    if operation == "current_orders" and set(payload) == {"type"}:
        return {"orders": [_json_evidence(value, "order") for value in _record_values(mt5.orders_get(), "orders")]}
    if operation == "current_positions" and set(payload) == {"type"}:
        return {"positions": [_json_evidence(value, "position") for value in _record_values(mt5.positions_get(), "positions")]}
    if operation == "broker_snapshot" and set(payload) == {"type"}:
        return {
            "orders": [_json_evidence(value, "order") for value in _record_values(mt5.orders_get(), "orders")],
            "positions": [_json_evidence(value, "position") for value in _record_values(mt5.positions_get(), "positions")],
        }
    if operation == "historical_ticks" and set(payload) == {"type", "symbol", "from_utc", "to_utc", "flags"}:
        start = _trader_utc(_request_text(payload, "from_utc"))
        end = _trader_utc(_request_text(payload, "to_utc"))
        if start >= end:
            raise ValueError
        flags = {"all": getattr(mt5, "COPY_TICKS_ALL"), "info": getattr(mt5, "COPY_TICKS_INFO"), "trade": getattr(mt5, "COPY_TICKS_TRADE")}.get(
            _request_text(payload, "flags")
        )
        if flags is None:
            raise ValueError
        records = _structured_records(mt5.copy_ticks_range(_request_text(payload, "symbol"), start, end, flags))
        return {"ticks": records[:1000]}
    raise WorkerEnrollmentError("The controller requested an invalid Trader broker read.")


def _trader_margin(mt5: ReadOnlyMT5, calculation: dict[str, object]) -> float:
    direction = _request_text(calculation, "direction")
    action = getattr(mt5, "ORDER_TYPE_BUY") if direction == "LONG" else getattr(mt5, "ORDER_TYPE_SELL")
    margin = mt5.order_calc_margin(
        action,
        _request_text(calculation, "symbol"),
        float(_request_text(calculation, "volume")),
        float(_request_text(calculation, "price")),
    )
    if (
        isinstance(margin, bool)
        or not isinstance(margin, (int, float))
        or not math.isfinite(margin)
        or margin <= 0
    ):
        raise WorkerEnrollmentError("The local MT5 terminal returned an invalid calculated margin.")
    return float(margin)


def _trader_operation(
    mt5: ReadOnlyMT5,
    payload: dict[str, object],
    *,
    before_broker_send: Callable[[], None] | None = None,
    effect_journal: WorkerEffectJournal | None = None,
    effect_id: str | None = None,
) -> dict[str, object]:
    operation = _request_text(payload, "type")
    if effect_journal is not None:
        if not effect_id:
            raise EffectJournalError("A broker effect ID is required for a journaled MT5 write.")
        journal_state = effect_journal.prepare(effect_id, payload)
        if journal_state in {"receipt", "observed"}:
            result = effect_journal.evidence(effect_id)
            _require_completed_trader_receipt(mt5, result, operation)
            return {"operation": operation, "result": result}
        if journal_state != "prepared":
            raise BrokerEffectUncertain("A durable broker effect must be observed before any retry.")
    _require_terminal_trading_permission(mt5)
    if operation in {"market", "pending"}:
        order = _trader_order(payload)
        broker_request = _broker_order_request(mt5, order)
        result = _broker_receipt(
            _journaled_order_send(
                mt5, broker_request, journal_payload=payload, effect_journal=effect_journal,
                effect_id=effect_id, before_broker_send=before_broker_send
            ),
            "order execution",
        )
        _record_journal_receipt(effect_journal, effect_id, result)
        _require_completed_trader_receipt(mt5, result, "order execution")
        return {"operation": operation, "result": result}
    if operation == "cancel" and set(payload) == {"type", "ticket"}:
        ticket = int(_request_text(payload, "ticket"))
        result = _broker_receipt(
            _journaled_order_send(
                mt5,
                {"action": getattr(mt5, "TRADE_ACTION_REMOVE"), "order": ticket},
                journal_payload=payload,
                effect_journal=effect_journal,
                effect_id=effect_id,
                before_broker_send=before_broker_send,
            ),
            "order cancellation",
        )
        _record_journal_receipt(effect_journal, effect_id, result)
        _require_completed_trader_receipt(mt5, result, "order cancellation")
        return {"operation": operation, "result": result}
    if operation == "close" and set(payload) == {"type", "ticket", "volume"}:
        ticket = int(_request_text(payload, "ticket"))
        volume = float(_request_text(payload, "volume"))
        result = _broker_receipt(
            _journaled_order_send(
                mt5,
                _broker_close_request(mt5, ticket, volume),
                journal_payload=payload,
                effect_journal=effect_journal,
                effect_id=effect_id,
                before_broker_send=before_broker_send,
            ),
            "position close",
        )
        _record_journal_receipt(effect_journal, effect_id, result)
        _require_completed_trader_receipt(mt5, result, "position close")
        return {"operation": operation, "result": result}
    if operation == "modify_sl_tp" and set(payload) == {"type", "symbol", "position", "sl", "tp"}:
        result = _broker_receipt(
            _journaled_order_send(
                mt5,
                {
                    "action": getattr(mt5, "TRADE_ACTION_SLTP"),
                    "symbol": _request_text(payload, "symbol"),
                    "position": int(_request_text(payload, "position")),
                    "sl": float(_request_text(payload, "sl")),
                    "tp": float(_request_text(payload, "tp")),
                },
                journal_payload=payload,
                effect_journal=effect_journal,
                effect_id=effect_id,
                before_broker_send=before_broker_send,
            ),
            "SL/TP modification",
        )
        _record_journal_receipt(effect_journal, effect_id, result)
        _require_completed_trader_receipt(mt5, result, "SL/TP modification")
        return {"operation": operation, "result": result}
    raise WorkerEnrollmentError("The controller requested an invalid Trader operation.")


def _journaled_order_send(
    mt5: ReadOnlyMT5,
    request: dict[str, object],
    *,
    journal_payload: dict[str, object],
    effect_journal: WorkerEffectJournal | None,
    effect_id: str | None,
    before_broker_send: Callable[[], None] | None,
) -> object:
    if effect_journal is not None:
        if not effect_id:
            raise EffectJournalError("A broker effect ID is required for a journaled MT5 write.")
        state = effect_journal.prepare(effect_id, journal_payload)
        if state in {"receipt", "observed"}:
            return effect_journal.evidence(effect_id)
        if state != "prepared":
            raise EffectJournalError("A durable broker effect must be observed before any retry.")
    if before_broker_send is not None:
        before_broker_send()
    if effect_journal is not None:
        effect_journal.mark_send_started(effect_id)
    return mt5.order_send(request)


def _record_journal_receipt(
    effect_journal: WorkerEffectJournal | None, effect_id: str | None, receipt: dict[str, object]
) -> None:
    if effect_journal is not None:
        if not effect_id:
            raise EffectJournalError("A broker effect ID is required for a journaled MT5 write.")
        effect_journal.record_receipt(effect_id, receipt)


def _trader_order(payload: dict[str, object]) -> dict[str, object]:
    operation = _request_text(payload, "type")
    if operation == "market" and set(payload) == {"type", "symbol", "volume", "direction", "filling_mode"}:
        return {"action": "market", **{field: payload[field] for field in ("symbol", "volume", "direction", "filling_mode")}}
    if operation != "pending":
        raise WorkerEnrollmentError("The controller requested an invalid Trader operation.")
    required = {"type", "pending_type", "symbol", "volume", "direction", "price", "sl", "tp", "filling_mode", "expires_at"}
    pending_type = _request_text(payload, "pending_type")
    if pending_type == "stop_limit":
        required.add("stop_limit_price")
    if set(payload) != required or pending_type not in {"limit", "stop", "stop_limit"}:
        raise WorkerEnrollmentError("The controller requested an invalid pending order.")
    order = {
        "action": f"pending_{pending_type}",
        **{field: payload[field] for field in required - {"type", "pending_type"}},
    }
    return order


def _trader_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError
    return parsed.astimezone(UTC)


def _record_values(value: object, kind: str) -> list[object]:
    if value is None:
        raise WorkerEnrollmentError(f"The local MT5 terminal returned no {kind}.")
    if not isinstance(value, (list, tuple)):
        raise WorkerEnrollmentError(f"The local MT5 terminal returned invalid {kind} records.")
    return list(value)


def _structured_records(value: object) -> list[dict[str, object]]:
    if value is None:
        raise WorkerEnrollmentError("The local MT5 terminal returned no tick records.")
    names = getattr(getattr(value, "dtype", None), "names", None)
    if not isinstance(names, tuple):
        raise WorkerEnrollmentError("The local MT5 terminal returned invalid tick records.")
    return [
        {name: _json_value(row[name]) for name in names}
        for row in value
    ]


def _json_evidence(value: object, kind: str) -> dict[str, object]:
    return {key: _json_value(item) for key, item in _evidence(value, kind).items()}


def _broker_receipt(value: object, kind: str) -> dict[str, object]:
    evidence = _evidence(value, kind)
    fields = {
        field: _json_value(evidence[field])
        for field in ("retcode", "deal", "order", "volume", "price", "bid", "ask", "comment", "request_id", "retcode_external")
        if field in evidence
    }
    try:
        return BrokerReceipt.model_validate(fields).model_dump(mode="json", exclude_none=True)
    except ValidationError as error:
        raise WorkerEnrollmentError(f"The local MT5 terminal returned invalid {kind} evidence.") from error


def _require_completed_trader_receipt(mt5: object, receipt: dict[str, object], operation: str) -> None:
    if receipt["retcode"] != getattr(mt5, "TRADE_RETCODE_DONE"):
        raise BrokerReceiptRejected(
            f"The MT5 {operation} was rejected: retcode={receipt['retcode']} comment={receipt.get('comment', '')!r}."
        )


def _json_value(value: object) -> object:
    item = value.item() if callable(getattr(value, "item", None)) else value
    if isinstance(item, datetime):
        return item.isoformat()
    if isinstance(item, (str, int, float, bool)) or item is None:
        return item
    if isinstance(item, Mapping):
        if not all(isinstance(key, str) for key in item):
            raise WorkerEnrollmentError("The local MT5 terminal returned non-serializable evidence.")
        return {key: _json_value(nested) for key, nested in item.items()}
    as_dict = getattr(item, "_asdict", None)
    if callable(as_dict):
        return _json_value(as_dict())
    if isinstance(item, (list, tuple)):
        return [_json_value(nested) for nested in item]
    raise WorkerEnrollmentError("The local MT5 terminal returned non-serializable evidence.")


def _broker_order_request(mt5: object, order: dict[str, object]) -> dict[str, object]:
    if order.get("action") == "market":
        required = ("symbol", "volume", "direction", "filling_mode")
        protection = ("sl", "tp")
        expected_fields = {"action", *required}
        if all(field in order for field in protection):
            expected_fields.update(protection)
        if set(order) != expected_fields or order.get("direction") not in {"LONG", "SHORT"}:
            raise WorkerEnrollmentError("The controller requested an invalid market order.")
        if order.get("filling_mode") not in {"FOK", "IOC"} or not all(isinstance(order[field], str) and order[field] for field in required):
            raise WorkerEnrollmentError("The controller requested an invalid market order.")
        try:
            order_type = getattr(mt5, "ORDER_TYPE_BUY" if order["direction"] == "LONG" else "ORDER_TYPE_SELL")
            tick = _evidence(mt5.symbol_info_tick(str(order["symbol"])), "market order tick")
            price_key = "ask" if order_type == getattr(mt5, "ORDER_TYPE_BUY") else "bid"
            price = tick.get(price_key)
            if isinstance(price, bool) or not isinstance(price, (int, float)) or price <= 0:
                raise ValueError
            request = {
                "action": getattr(mt5, "TRADE_ACTION_DEAL"),
                "symbol": order["symbol"],
                "volume": float(str(order["volume"])),
                "type": order_type,
                "price": price,
                "type_filling": getattr(mt5, "ORDER_FILLING_FOK" if order["filling_mode"] == "FOK" else "ORDER_FILLING_IOC"),
            }
            if protection[0] in order:
                values = {field: float(str(order[field])) for field in protection}
                if not all(math.isfinite(value) and value > 0 for value in values.values()):
                    raise ValueError
                request.update(values)
            return request
        except (TypeError, ValueError, AttributeError) as error:
            raise WorkerEnrollmentError("The controller requested an invalid market order.") from error
    if order.get("action") == "protect":
        required = ("symbol", "position", "sl", "tp")
        if set(order) != {"action", *required} or not all(isinstance(order[field], str) and order[field] for field in required):
            raise WorkerEnrollmentError("The controller requested an invalid protection update.")
        try:
            return {
                "action": getattr(mt5, "TRADE_ACTION_SLTP"),
                "symbol": order["symbol"],
                "position": int(order["position"]),
                "sl": float(order["sl"]),
                "tp": float(order["tp"]),
            }
        except (TypeError, ValueError, AttributeError) as error:
            raise WorkerEnrollmentError("The controller requested an invalid protection update.") from error
    return _broker_limit_request(mt5, order)


def _broker_close_request(mt5: object, ticket: int, volume: float) -> dict[str, object]:
    """Build a genuine MT5 ``TRADE_ACTION_DEAL`` close request for one owned ticket.

    Shared by the ordinary Trader-relay ``close`` operation and the Pair
    Execution Cell's containment adapter (``abt.worker.pair_cell_adapter``),
    so both broker-write paths translate an abstract close request into the
    exact MT5 request shape identically.
    """

    if volume <= 0:
        raise ValueError
    position = next(
        (item for item in _records(mt5.positions_get(), "position").values() if str(item.get("ticket")) == str(ticket)),
        None,
    )
    if position is None or not isinstance(position.get("symbol"), str) or position.get("type") is None:
        raise WorkerEnrollmentError("The position is no longer observable.")
    close_type = getattr(mt5, "ORDER_TYPE_SELL") if position["type"] == getattr(mt5, "POSITION_TYPE_BUY") else getattr(mt5, "ORDER_TYPE_BUY")
    tick = _evidence(mt5.symbol_info_tick(position["symbol"]), "position close tick")
    price_key = "bid" if close_type == getattr(mt5, "ORDER_TYPE_SELL") else "ask"
    price = tick.get(price_key)
    if isinstance(price, bool) or not isinstance(price, (int, float)) or price <= 0:
        raise WorkerEnrollmentError("The position close price is unavailable.")
    return {
        "action": getattr(mt5, "TRADE_ACTION_DEAL"),
        "position": ticket,
        "symbol": position["symbol"],
        "volume": volume,
        "type": close_type,
        "price": price,
        "type_filling": getattr(mt5, "ORDER_FILLING_IOC"),
    }


def _require_terminal_trading_permission(mt5: object) -> None:
    terminal_info = getattr(mt5, "terminal_info", None)
    if not callable(terminal_info):
        raise WorkerEnrollmentError("The local MT5 terminal status is unavailable.")
    terminal = _evidence(terminal_info(), "terminal info")
    if terminal.get("trade_allowed") is False:
        raise WorkerEnrollmentError("The local MT5 terminal has algorithmic trading disabled.")
    if terminal.get("tradeapi_disabled") is True:
        raise WorkerEnrollmentError("The local MT5 terminal has disabled Python trading.")


def _new_market_position(
    mt5: object, order: dict[str, object], positions_before: dict[str, dict[str, object]]
) -> dict[str, object]:
    try:
        expected_type = getattr(mt5, "POSITION_TYPE_BUY" if order["direction"] == "LONG" else "POSITION_TYPE_SELL")
        expected_volume = float(str(order["volume"]))
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise WorkerEnrollmentError("The controller requested an invalid market order.") from error
    for _ in range(3):
        positions_after = _records(mt5.positions_get(), "position")
        candidates = [
            position for ticket, position in positions_after.items()
            if ticket not in positions_before
            and position.get("symbol") == order["symbol"]
            and position.get("type") == expected_type
            and _volume(position) == expected_volume
        ]
        if len(candidates) == 1:
            position = candidates[0]
            price_open = position.get("price_open")
            if isinstance(price_open, (int, float)) and not isinstance(price_open, bool) and price_open > 0:
                return position
        _sleep(0.2)
    raise WorkerEnrollmentError("The local MT5 terminal did not provide unique new-position evidence.")


def _broker_limit_request(mt5: object, order: dict[str, object]) -> dict[str, object]:
    required = ("symbol", "volume", "direction", "price", "sl", "tp", "filling_mode", "expires_at")
    action = order.get("action")
    if action not in {"pending_limit", "pending_stop", "pending_stop_limit"}:
        raise WorkerEnrollmentError("The controller requested an invalid pending order.")
    required_fields = {"action", *required}
    if action == "pending_stop_limit":
        required_fields.add("stop_limit_price")
    if set(order) != required_fields:
        raise WorkerEnrollmentError("The controller requested an invalid pending order.")
    direction = order.get("direction")
    filling_mode = order.get("filling_mode")
    if direction not in {"LONG", "SHORT"} or filling_mode not in {"FOK", "IOC"}:
        raise WorkerEnrollmentError("The controller requested an invalid pending order.")
    if not all(isinstance(order[field], str) and order[field] for field in required):
        raise WorkerEnrollmentError("The controller requested an invalid pending order.")
    try:
        expires_at = datetime.fromisoformat(str(order["expires_at"]).replace("Z", "+00:00"))
        if expires_at.tzinfo is None:
            raise ValueError
        tick = _evidence(mt5.symbol_info_tick(str(order["symbol"])), "pending order tick")
        broker_now = tick.get("time")
        if isinstance(broker_now, bool) or not isinstance(broker_now, (int, float)) or broker_now <= 0:
            raise ValueError
        minimum_expiration = int(broker_now) + math.ceil((expires_at.astimezone(UTC) - datetime.now(UTC)).total_seconds())
        if minimum_expiration <= int(broker_now):
            raise ValueError
        expiration = ((minimum_expiration + 59) // 60) * 60
        type_name = {
            "pending_limit": "BUY_LIMIT" if direction == "LONG" else "SELL_LIMIT",
            "pending_stop": "BUY_STOP" if direction == "LONG" else "SELL_STOP",
            "pending_stop_limit": "BUY_STOP_LIMIT" if direction == "LONG" else "SELL_STOP_LIMIT",
        }[action]
        request = {
            "action": getattr(mt5, "TRADE_ACTION_PENDING"),
            "symbol": order["symbol"],
            "volume": float(str(order["volume"])),
            "type": getattr(mt5, f"ORDER_TYPE_{type_name}"),
            "price": float(str(order["price"])),
            "sl": float(str(order["sl"])),
            "tp": float(str(order["tp"])),
            "type_filling": getattr(mt5, "ORDER_FILLING_FOK" if filling_mode == "FOK" else "ORDER_FILLING_IOC"),
            "type_time": getattr(mt5, "ORDER_TIME_SPECIFIED"),
            "expiration": expiration,
        }
        if action == "pending_stop_limit":
            request["stoplimit"] = float(str(order["stop_limit_price"]))
        return request
    except (TypeError, ValueError, AttributeError) as error:
        raise WorkerEnrollmentError("The controller requested an invalid pending order.") from error


def _request_text(request: dict[str, object], field: str) -> str:
    value = request.get(field)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The controller returned an invalid product catalog analysis request.")
    return value


def _evidence(value: object, kind: str) -> dict[str, object]:
    if isinstance(value, Mapping):
        return dict(value)
    as_dict = getattr(value, "_asdict", None)
    if callable(as_dict):
        return dict(as_dict())
    raise WorkerEnrollmentError(f"The local MT5 terminal returned invalid {kind} evidence.")


def _mt5_last_error(mt5: object) -> object:
    last_error = getattr(mt5, "last_error", None)
    if not callable(last_error):
        return None
    try:
        return last_error()
    except Exception:
        return "unavailable"


def _visible_symbols(mt5: object) -> list[str]:
    symbols_get = getattr(mt5, "symbols_get", None)
    if not callable(symbols_get):
        return []
    try:
        symbols = symbols_get()
    except Exception:
        _LOGGER.warning("Unable to enumerate watched MT5 symbols for live-state publication.")
        return []
    if not isinstance(symbols, (list, tuple)):
        return []
    watched: list[str] = []
    for symbol in symbols:
        try:
            evidence = _evidence(symbol, "symbol")
        except WorkerEnrollmentError:
            continue
        name = evidence.get("name", evidence.get("symbol"))
        if evidence.get("visible") is True and isinstance(name, str) and name:
            watched.append(name)
    return watched


def _live_quote(mt5: ReadOnlyMT5, symbol: str) -> dict[str, object]:
    tick = _evidence(mt5.symbol_info_tick(symbol), "symbol tick")
    bid = tick.get("bid")
    ask = tick.get("ask")
    last = tick.get("last")
    timestamp = tick.get("time")
    if (
        isinstance(bid, bool) or not isinstance(bid, (int, float))
        or isinstance(ask, bool) or not isinstance(ask, (int, float))
        or isinstance(last, bool) or not isinstance(last, (int, float))
        or isinstance(timestamp, bool) or not isinstance(timestamp, (int, float))
    ):
        raise WorkerEnrollmentError("The local MT5 terminal returned invalid symbol tick evidence.")
    return {
        "symbol": symbol,
        "bid": float(bid),
        "ask": float(ask),
        "last": float(last),
        "broker_time": datetime.fromtimestamp(timestamp, UTC).isoformat(),
    }


def _records(value: object, kind: str) -> dict[str, dict[str, object]]:
    if value is None:
        raise WorkerEnrollmentError(f"The local MT5 terminal returned no {kind} records.")
    if not isinstance(value, (list, tuple)):
        raise WorkerEnrollmentError(f"The local MT5 terminal returned invalid {kind} records.")
    records: dict[str, dict[str, object]] = {}
    for item in value:
        record = _evidence(item, kind)
        comment = record.get("comment")
        if isinstance(comment, str) and comment.startswith("abt:"):
            record["control_plane_command_id"] = comment
        ticket = record.get("ticket")
        if isinstance(ticket, bool) or not isinstance(ticket, int):
            raise WorkerEnrollmentError(f"The local MT5 terminal returned a {kind} without a ticket.")
        records[str(ticket)] = record
    return records


def _changes(
    entity: str, previous: dict[str, dict[str, object]], current: dict[str, dict[str, object]]
) -> list[dict[str, object]]:
    changes: list[dict[str, object]] = []
    for ticket, record in current.items():
        old = previous.get(ticket)
        if old is None:
            changes.append({"entity": entity, "ticket": ticket, "change": "created", "record": record})
        elif old.get("state") != record.get("state"):
            changes.append({"entity": entity, "ticket": ticket, "change": "state_changed", "record": record})
        elif _volume(old) != _volume(record):
            changes.append({"entity": entity, "ticket": ticket, "change": "volume_changed", "record": record})
        elif _relevant_fields(entity, old) != _relevant_fields(entity, record):
            changes.append({"entity": entity, "ticket": ticket, "change": "modified", "record": record})
    for ticket, record in previous.items():
        if ticket not in current:
            changes.append({"entity": entity, "ticket": ticket, "change": "closed", "record": record})
    return changes


def _volume(record: dict[str, object]) -> object:
    return record.get("volume_current", record.get("volume"))


_RELEVANT_FIELDS = {
    "order": frozenset(
        {
            "comment",
            "external_id",
            "magic",
            "price_stoplimit",
            "position_by_id",
            "position_id",
            "sl",
            "symbol",
            "time_expiration",
            "tp",
            "type",
            "type_filling",
            "type_time",
        }
    ),
    "position": frozenset({"comment", "external_id", "identifier", "magic", "sl", "symbol", "tp", "type"}),
}


def _relevant_fields(entity: str, record: dict[str, object]) -> dict[str, object]:
    return {field: record.get(field) for field in _RELEVANT_FIELDS[entity]}
