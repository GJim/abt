from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from time import sleep as _sleep
from typing import Protocol

from .enrollment import WorkerEnrollmentError
from .session import collect_market_data_evidence, collect_product_catalog_evidence


class ReadOnlyMT5(Protocol):
    """The broker read surface used by the account-worker reconciler."""

    def account_info(self) -> object: ...

    def terminal_info(self) -> object: ...

    def orders_get(self) -> object: ...

    def positions_get(self) -> object: ...


class AuthenticatedReconciliationSession(Protocol):
    reconciliation_cursor: int

    def request_password(self) -> str: ...

    def send_reconciliation(self, message: dict[str, object]) -> None: ...


class WorkerSafetySession(Protocol):
    def heartbeat(self) -> bool: ...

    def send_safety_state(self, state: str, reason: str) -> None: ...


class AnalysisWorkerSession(Protocol):
    def receive_product_catalog_analysis(self, timeout: float | None = None) -> dict[str, object] | None: ...

    def send_product_catalog_analysis(
        self,
        *,
        analysis_id: str,
        request_id: str,
        collected_at: str,
        symbols: list[dict[str, object]],
        stage: str = "catalog",
        timeframe: str | None = None,
        period_start_utc: str | None = None,
        period_end_utc: str | None = None,
    ) -> None: ...


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
        except WorkerEnrollmentError:
            received = False
        if received:
            self._last_controller_signal = observed_at
            if self.state != "connected":
                self.state = "connected"
                self._session.send_safety_state("connected", "controller_signal_recovered")
            return True
        if observed_at - self._last_controller_signal >= self._lost_link_after and self.state != "lost_link_safety":
            self.state = "lost_link_safety"
            self._session.send_safety_state("lost_link_safety", "five_minute_missed_heartbeat")
        return False

    def run_with_retries(
        self, operation: Callable[[], None], *, sleep: Callable[[float], None] = _sleep
    ) -> None:
        """Retry terminal/API failures at most three times before escalating to a human."""

        for attempt in range(3):
            try:
                operation()
                return
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
        self._session.send_safety_state("needs_human", reason)


class AccountMismatchError(WorkerEnrollmentError):
    """The terminal is logged into an account other than the worker's binding."""


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
    ) -> None:
        """Run the required one-minute read-only polling cadence and 30-second heartbeats."""

        while True:
            self.poll(now())
            if heartbeat is None:
                sleep(60)
            else:
                heartbeat()
                sleep(30)
                heartbeat()
                sleep(30)


def reconcile_authenticated_worker(
    *,
    mt5: ReadOnlyMT5,
    session: AuthenticatedReconciliationSession,
    login: int,
    server: str,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
    sleep: Callable[[float], None] = _sleep,
) -> None:
    """Use an authenticated session's memory-only password for read-only MT5 reconciliation."""

    password = session.request_password()
    initialized = False
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
        if callable(getattr(session, "heartbeat", None)) and callable(getattr(session, "send_safety_state", None)):
            safety = WorkerSafetyAdapter(session)  # type: ignore[arg-type]
        reconciliation = MT5ReconciliationAdapter(
            mt5, emit=session.send_reconciliation, initial_cursor=getattr(session, "reconciliation_cursor", 0)
        )
        if callable(getattr(session, "receive_product_catalog_analysis", None)) and callable(
            getattr(session, "send_product_catalog_analysis", None)
        ):
            _run_reconciliation_with_analysis(
                reconciliation,
                mt5=mt5,
                session=session,  # type: ignore[arg-type]
                now=now,
                safety=safety,
            )
        else:
            reconciliation.run_forever(
                now=now,
                sleep=sleep,
                heartbeat=None if safety is None else lambda: safety.heartbeat(now()),
            )
    finally:
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
) -> None:
    """Run reconciliation with bounded terminal/API recovery and human escalation."""

    WorkerSafetyAdapter(session).run_with_retries(
        lambda: reconcile_authenticated_worker(
            mt5=mt5, session=session, login=login, server=server, now=now, sleep=sleep
        ),
        sleep=sleep,
    )


def _run_reconciliation_with_analysis(
    reconciliation: MT5ReconciliationAdapter,
    *,
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    now: Callable[[], datetime],
    safety: WorkerSafetyAdapter | None,
) -> None:
    while True:
        reconciliation.poll(now())
        for _ in range(2):
            request = session.receive_product_catalog_analysis(timeout=30.0)
            if request is not None:
                _serve_product_catalog_analysis(mt5, session, request, now)
            if safety is not None:
                safety.heartbeat(now())


def _serve_product_catalog_analysis(
    mt5: ReadOnlyMT5,
    session: AnalysisWorkerSession,
    request: dict[str, object],
    now: Callable[[], datetime],
) -> None:
    analysis_id = _request_text(request, "analysis_id")
    request_id = _request_text(request, "request_id")
    stage = _request_text(request, "stage")
    collected_at = now()
    if stage == "catalog":
        evidence = collect_product_catalog_evidence(mt5, collected_at=collected_at)  # type: ignore[arg-type]
        session.send_product_catalog_analysis(
            analysis_id=analysis_id,
            request_id=request_id,
            stage=stage,
            collected_at=str(evidence["collected_at"]),
            symbols=list(evidence["symbols"]),
        )
        return
    if stage not in {"m15_screening", "m1_verification"}:
        raise WorkerEnrollmentError("The controller requested an invalid product catalog analysis stage.")
    timeframe = _request_text(request, "timeframe")
    period_start_utc = _request_text(request, "period_start_utc")
    period_end_utc = _request_text(request, "period_end_utc")
    symbols = request.get("symbols")
    if not isinstance(symbols, list) or not all(isinstance(symbol, str) and symbol for symbol in symbols):
        raise WorkerEnrollmentError("The controller requested invalid market-data symbols.")
    evidence = collect_market_data_evidence(
        mt5,  # type: ignore[arg-type]
        symbols=symbols,
        timeframe=timeframe,
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
        collected_at=collected_at,
    )
    session.send_product_catalog_analysis(
        analysis_id=analysis_id,
        request_id=request_id,
        stage=stage,
        collected_at=str(evidence["collected_at"]),
        symbols=list(evidence["symbols"]),
        timeframe=timeframe,
        period_start_utc=period_start_utc,
        period_end_utc=period_end_utc,
    )


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


def _records(value: object, kind: str) -> dict[str, dict[str, object]]:
    if value is None:
        return {}
    if not isinstance(value, (list, tuple)):
        raise WorkerEnrollmentError(f"The local MT5 terminal returned invalid {kind} records.")
    records: dict[str, dict[str, object]] = {}
    for item in value:
        record = _evidence(item, kind)
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
