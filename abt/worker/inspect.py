"""Offline inspection of one Worker's durable Pair Execution Cell state.

Everything here reads the Worker's own SQLite database and its sibling JSON
records without opening a controller session or touching MT5, so it is safe
to run on a holiday, on a stopped Worker, or while another process owns the
live session.  What it reports is the *last persisted* state:

* the frozen discovered universe (admitted symbols),
* the frozen route/budget/acceptance records,
* the accepted canonical policy,
* the current (non-superseded) sizing plans, and
* the durable product quarantine.

Suspend reasons that only exist in a live process (no current quote, a
drifted catalog, an unusable margin read) are *not* durable and therefore
cannot be reported here; a universe product without a current persisted plan
is reported as such, honestly, rather than with an invented reason.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import json
import sqlite3


class InspectError(RuntimeError):
    """Reading the durable state failed or nothing was ever persisted."""


@dataclass(frozen=True, slots=True)
class UniverseProduct:
    symbol: str
    product_id: str


@dataclass(frozen=True, slots=True)
class UniverseView:
    route_id: str
    universe_generation: int
    products: tuple[UniverseProduct, ...] = ()
    leader_catalog_hash: str = ""
    follower_catalog_hash: str = ""


@dataclass(frozen=True, slots=True)
class PolicyView:
    policy_hash: str
    mode: str
    strategy_budget_usd: str
    leader_budget_usd: str
    follower_budget_usd: str
    sizing_refresh_seconds: object = None
    relay_handling_timeout_seconds: object = None


@dataclass(frozen=True, slots=True)
class PlanView:
    product_id: str
    symbol: str
    directions: tuple[str, ...] = ()
    plan_versions: tuple[str, ...] = ()
    local_max_lots: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class QuarantineView:
    product_id: str
    symbol: str
    offending_worker_id: str
    attempt_id: str
    universe_generation: object = None
    quarantined_at: str = ""
    retcode: object = None
    comment: object = None


@dataclass(frozen=True, slots=True)
class SuspendView:
    product_id: str
    symbol: str
    universe_generation: int = 0
    reason: str = ""
    recorded_at: str = ""


@dataclass(frozen=True, slots=True)
class DiscoveryExcludedView:
    symbol: str
    universe_generation: int = 0
    reason: str = ""
    recorded_at: str = ""


@dataclass(frozen=True, slots=True)
class FrozenView:
    route: dict[str, object] = field(default_factory=dict)
    budget: dict[str, object] = field(default_factory=dict)
    acceptance: dict[str, object] = field(default_factory=dict)
    policy: PolicyView | None = None
    universe: UniverseView | None = None


@dataclass(frozen=True, slots=True)
class SymbolsSnapshot:
    db_path: Path
    universe: UniverseView | None = None
    policy: PolicyView | None = None
    plans: tuple[PlanView, ...] = ()
    quarantine: tuple[QuarantineView, ...] = ()
    suspended: tuple[SuspendView, ...] = ()
    discovery_excluded: tuple[DiscoveryExcludedView, ...] = ()
    frozen: FrozenView | None = None


def snapshot_symbols(db_path: Path) -> SymbolsSnapshot:
    """Read every durable symbols-related record beside one cell database."""

    db_path = Path(db_path)
    if not db_path.exists():
        raise InspectError(f"No Pair Execution Cell database exists: {db_path}.")
    try:
        from ..pair_cell import migrate_pair_cell_database

        migrate_pair_cell_database(db_path)
        connection = sqlite3.connect(db_path)
    except sqlite3.Error as error:
        raise InspectError(f"The Pair Execution Cell database cannot be read: {db_path}.") from error
    try:
        universe = _read_universe(connection)
        policy = _read_policy(connection)
        plans = _read_current_plans(connection, universe)
        quarantine = _read_quarantine(connection)
        suspended = _read_suspended(connection, universe)
        discovery_excluded = _read_discovery_excluded(connection, universe)
    except sqlite3.Error as error:
        raise InspectError(f"The Pair Execution Cell database cannot be read: {db_path}.") from error
    finally:
        connection.close()
    frozen = FrozenView(
        route=_read_json(db_path.with_suffix(".route.json")),
        budget=_read_json(record_json(db_path, "budget")),
        acceptance=_read_json(record_json(db_path, "acceptance")),
        policy=policy,
        universe=universe,
    )
    return SymbolsSnapshot(
        db_path=db_path,
        universe=universe,
        policy=policy,
        plans=plans,
        quarantine=quarantine,
        suspended=suspended,
        discovery_excluded=discovery_excluded,
        frozen=frozen,
    )


def record_json(db_path: Path, kind: str) -> Path:
    """Sibling durable JSON record path, mirroring the runtime's derivation."""

    return db_path.with_suffix(f".{kind}.json")


def allowed_products(snapshot: SymbolsSnapshot) -> tuple[UniverseProduct, ...]:
    """Frozen universe products that are not quarantined: the tradable set."""

    if snapshot.universe is None:
        return ()
    quarantined = {entry.product_id for entry in snapshot.quarantine}
    return tuple(p for p in snapshot.universe.products if p.product_id not in quarantined)


def edge_searchable_products(snapshot: SymbolsSnapshot) -> tuple[PlanView, ...]:
    """Last persisted current plans on non-quarantined universe products.

    A plan per direction is what the edge search sizes from; this is the last
    known-good set, not a live guarantee (quotes, catalog drift and margin
    are re-checked on every live refresh).
    """

    if snapshot.universe is None:
        return ()
    quarantined = {entry.product_id for entry in snapshot.quarantine}
    admitted = {p.product_id for p in snapshot.universe.products}
    return tuple(
        plan for plan in snapshot.plans
        if plan.product_id in admitted and plan.product_id not in quarantined
    )


def excluded_products(snapshot: SymbolsSnapshot) -> tuple[dict[str, object], ...]:
    """Universe products with no current persisted plan, plus quarantined ones.

    Reasons come from durable records in priority order: the quarantine
    receipt, the last sizing refresh's suspend reason, the discovery run's
    exclusion reason, and only then a fallback stating that no reason was
    persisted (e.g. the refresh has not run yet under the current generation).
    """

    rows: list[dict[str, object]] = []
    suspended = {entry.product_id: entry for entry in snapshot.suspended}
    if snapshot.universe is not None:
        admitted_symbols = {product.symbol for product in snapshot.universe.products}
        planned = {plan.product_id for plan in snapshot.plans}
        quarantined = {entry.product_id for entry in snapshot.quarantine}
        for product in snapshot.universe.products:
            if product.product_id in quarantined:
                continue
            if product.product_id not in planned:
                suspend = suspended.get(product.product_id)
                if suspend is not None:
                    reason = (
                        f"suspended at the last sizing refresh: {suspend.reason}"
                        f" (recorded {suspend.recorded_at})"
                    )
                else:
                    reason = (
                        "no current sizing plan persisted and no reason recorded yet"
                        " (the sizing refresh has not run under this generation,"
                        " or the row predates it)"
                    )
                rows.append(
                    {
                        "symbol": product.symbol,
                        "product_id": product.product_id,
                        "reason": reason,
                    }
                )
        for entry in snapshot.discovery_excluded:
            if entry.symbol in admitted_symbols:
                continue
            rows.append(
                {
                    "symbol": entry.symbol,
                    "product_id": "",
                    "reason": (
                        f"never admitted by the installed discovery run: {entry.reason}"
                        f" (recorded {entry.recorded_at})"
                    ),
                }
            )
    for entry in snapshot.quarantine:
        rows.append(
            {
                "symbol": entry.symbol,
                "product_id": entry.product_id,
                "reason": f"quarantined by {entry.offending_worker_id}"
                f" on attempt {entry.attempt_id}"
                + (f" (MT5 retcode {entry.retcode})" if entry.retcode is not None else "")
                + (f" {entry.comment}" if entry.comment else ""),
            }
        )
    return tuple(rows)


def _read_universe(connection: sqlite3.Connection) -> UniverseView | None:
    try:
        row = connection.execute(
            "SELECT universe_generation, route_id, payload FROM cell_universe"
            " ORDER BY universe_generation DESC LIMIT 1"
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    try:
        payload = json.loads(str(row[2]))
        products = tuple(
            UniverseProduct(symbol=str(item["symbol"]), product_id=str(item["product_id"]))
            for item in payload.get("products", [])
        )
    except (ValueError, KeyError, TypeError, AttributeError):
        raise InspectError("The durable discovered universe is unreadable.")
    return UniverseView(
        route_id=str(row[1]),
        universe_generation=int(row[0]),
        products=products,
        leader_catalog_hash=str(payload.get("leader_catalog_hash", "")),
        follower_catalog_hash=str(payload.get("follower_catalog_hash", "")),
    )


def _read_policy(connection: sqlite3.Connection) -> PolicyView | None:
    try:
        row = connection.execute(
            "SELECT policy_hash, payload FROM cell_policy WHERE id = 1"
        ).fetchone()
    except sqlite3.Error:
        return None
    if row is None:
        return None
    try:
        payload = json.loads(str(row[1]))
        leader = payload.get("leader_risk", {})
        follower = payload.get("follower_risk", {})
    except (ValueError, AttributeError):
        raise InspectError("The durable canonical policy is unreadable.")
    if not isinstance(leader, dict) or not isinstance(follower, dict):
        raise InspectError("The durable canonical policy is unreadable.")
    return PolicyView(
        policy_hash=str(row[0]),
        mode=str(payload.get("mode", "")),
        strategy_budget_usd=str(payload.get("strategy_budget_usd", "")),
        leader_budget_usd=str(leader.get("strategy_budget_usd", "")),
        follower_budget_usd=str(follower.get("strategy_budget_usd", "")),
        sizing_refresh_seconds=payload.get("sizing_refresh_seconds"),
        relay_handling_timeout_seconds=payload.get("relay_handling_timeout_seconds"),
    )


def _read_current_plans(
    connection: sqlite3.Connection, universe: UniverseView | None
) -> tuple[PlanView, ...]:
    if universe is None:
        return ()
    try:
        rows = connection.execute(
            "SELECT product_id, direction, plan_version, payload FROM cell_sizing_plans"
            " WHERE universe_generation = ? AND superseded_at IS NULL"
            " ORDER BY product_id, direction",
            (universe.universe_generation,),
        ).fetchall()
    except sqlite3.Error:
        return ()
    grouped: dict[str, dict[str, object]] = {}
    for product_id, direction, plan_version, payload in rows:
        entry = grouped.setdefault(str(product_id), {"directions": [], "versions": [], "lots": [], "symbol": ""})
        try:
            data = json.loads(str(payload))
            symbol = str(data.get("symbol", ""))
            lots = str(data.get("local_max_lots", ""))
        except ValueError:
            symbol, lots = "", ""
        if symbol and not entry["symbol"]:
            entry["symbol"] = symbol
        entry["directions"].append(str(direction))  # type: ignore[union-attr]
        entry["versions"].append(str(plan_version))  # type: ignore[union-attr]
        entry["lots"].append(lots)  # type: ignore[union-attr]
    return tuple(
        PlanView(
            product_id=product_id,
            symbol=str(entry["symbol"]),
            directions=tuple(entry["directions"]),  # type: ignore[arg-type]
            plan_versions=tuple(entry["versions"]),  # type: ignore[arg-type]
            local_max_lots=tuple(entry["lots"]),  # type: ignore[arg-type]
        )
        for product_id, entry in sorted(grouped.items())
    )


def _read_quarantine(connection: sqlite3.Connection) -> tuple[QuarantineView, ...]:
    try:
        rows = connection.execute(
            "SELECT product_id, offending_worker_id, attempt_id, receipt, quarantined_at,"
            " universe_generation FROM cell_product_quarantine ORDER BY product_id"
        ).fetchall()
    except sqlite3.Error:
        return ()
    views: list[QuarantineView] = []
    for product_id, offending, attempt, receipt, quarantined_at, generation in rows:
        try:
            evidence = json.loads(str(receipt))
        except ValueError:
            evidence = {}
        if not isinstance(evidence, dict):
            evidence = {}
        views.append(
            QuarantineView(
                product_id=str(product_id),
                symbol=_identity_symbol(str(product_id)),
                offending_worker_id=str(offending),
                attempt_id=str(attempt),
                universe_generation=generation,
                quarantined_at=str(quarantined_at),
                retcode=evidence.get("retcode"),
                comment=evidence.get("comment"),
            )
        )
    return tuple(views)


def _read_suspended(
    connection: sqlite3.Connection, universe: UniverseView | None
) -> tuple[SuspendView, ...]:
    if universe is None:
        return ()
    try:
        rows = connection.execute(
            "SELECT product_id, symbol, universe_generation, reason, recorded_at"
            " FROM cell_product_suspend WHERE universe_generation = ? ORDER BY product_id",
            (universe.universe_generation,),
        ).fetchall()
    except sqlite3.Error:
        # Databases written before suspend persistence carry no table.
        return ()
    return tuple(
        SuspendView(
            product_id=str(product_id),
            symbol=str(symbol),
            universe_generation=int(generation),
            reason=str(reason),
            recorded_at=str(recorded_at),
        )
        for product_id, symbol, generation, reason, recorded_at in rows
    )


def _read_discovery_excluded(
    connection: sqlite3.Connection, universe: UniverseView | None
) -> tuple[DiscoveryExcludedView, ...]:
    if universe is None:
        return ()
    try:
        rows = connection.execute(
            "SELECT symbol, universe_generation, reason, recorded_at"
            " FROM cell_discovery_excluded WHERE universe_generation = ? ORDER BY symbol",
            (universe.universe_generation,),
        ).fetchall()
    except sqlite3.Error:
        # Databases written before exclusion persistence carry no table.
        return ()
    return tuple(
        DiscoveryExcludedView(
            symbol=str(symbol),
            universe_generation=int(generation),
            reason=str(reason),
            recorded_at=str(recorded_at),
        )
        for symbol, generation, reason, recorded_at in rows
    )


def _identity_symbol(product_id: str) -> str:
    try:
        from ..pair_cell import product_identity_symbol

        return product_identity_symbol(product_id)
    except Exception:
        return product_id


def _read_json(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        raise InspectError(f"The durable record is unreadable: {path}.")
    return raw if isinstance(raw, dict) else {}
