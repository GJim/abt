from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import UTC, datetime, timedelta
from time import sleep as _sleep
from typing import Protocol

from .enrollment import WorkerEnrollmentError


class ReadOnlyMT5(Protocol):
    """The broker read surface used by the account-worker reconciler."""

    def account_info(self) -> object: ...

    def terminal_info(self) -> object: ...

    def orders_get(self) -> object: ...

    def positions_get(self) -> object: ...


class AuthenticatedReconciliationSession(Protocol):
    def request_password(self) -> str: ...

    def send_reconciliation(self, message: dict[str, object]) -> None: ...


class MT5ReconciliationAdapter:
    """Poll one MT5 terminal without exposing a broker-write operation."""

    def __init__(self, mt5: ReadOnlyMT5, *, emit: Callable[[dict[str, object]], None]) -> None:
        self._mt5 = mt5
        self._emit = emit
        self._cursor = 0
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
    ) -> None:
        """Run the required one-minute read-only polling cadence."""

        while True:
            self.poll(now())
            sleep(60)


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
        MT5ReconciliationAdapter(mt5, emit=session.send_reconciliation).run_forever(now=now, sleep=sleep)
    finally:
        password = ""
        if initialized:
            shutdown = getattr(mt5, "shutdown", None)
            if callable(shutdown):
                shutdown()


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
    for ticket, record in previous.items():
        if ticket not in current:
            changes.append({"entity": entity, "ticket": ticket, "change": "closed", "record": record})
    return changes


def _volume(record: dict[str, object]) -> object:
    return record.get("volume_current", record.get("volume"))
