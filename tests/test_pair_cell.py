"""Tests for the Low-Latency Pair Execution Cell through its production seam.

One leader and one follower :class:`PairExecutionCell` are wired exactly as two
separate Worker processes would be: a deterministic in-memory opaque relay that
forwards on ``route_id`` only, a deterministic MT5 fake that answers
``symbol_info``/``order_calc_margin``/``order_send``, a real
:class:`WorkerEffectJournal`, a real
:class:`DeadlineAwareTraderRpcScheduler`, and an injected monotonic clock.

The two Workers discover their own compatible universe from literal catalogs;
nothing here configures a product list.  Tests assert observable lifecycle
results, emitted relay envelopes, broker effects, and durable restart recovery;
they never reach into private reducers or table layout.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import cast
import unittest

from abt import pair_cell
from abt.pair_cell import (
    Attempt,
    BrokerClockCalibration,
    BrokerSnapshotEvent,
    CatalogEntry,
    CatalogSummary,
    ClockTickEvent,
    DiscoveredProduct,
    LocalCatalogEvent,
    LocalQuoteEvent,
    PairExecutionCell,
    PairExecutionCellError,
    PairingAcceptance,
    PeerSessionEvent,
    QuarantineReleaseEvent,
    ReadinessFactsEvent,
    RealizedPnLEvent,
    RediscoveryRequestEvent,
    RelayEnvelopeReceived,
    RouteAssignment,
    RouteMetadataEvent,
    StrategyPolicy,
    WorkerRiskLimits,
    build_pairing_acceptance,
    calibrated_skew_seconds,
    canonical_policy_from_acceptance,
    default_shared_policy_values,
    default_worker_risk_limits,
    discover_compatible_products,
    floor_to_volume_step,
    is_derived_product_identity,
    validate_configuration_authority,
)
from abt.worker.effect_journal import EffectJournalError, WorkerEffectJournal
from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler

_DONE = 10009
_REJECTED = 10013
_PRICE_OFF = 10021

LEADER = "worker-leader"
FOLLOWER = "worker-follower"
ROUTE_ID = "route-0001"
SYMBOL = "EURUSD"
OTHER_SYMBOL = "GBPUSD"
PROPOSAL = "proposal-1"

BASE_NOW = datetime(2026, 3, 4, 14, 0, tzinfo=UTC)
_UNSET = object()

# Each Worker authors its own block.  The follower's arrives verbatim in its
# Pairing Acceptance payload and the leader may only transcribe it.
LEADER_RISK = WorkerRiskLimits(
    strategy_budget_usd="10000",
    maximum_margin_fraction="0.5",
    maximum_loss_per_trade_usd="150",
)
FOLLOWER_RISK = WorkerRiskLimits(
    strategy_budget_usd="4000",
    maximum_margin_fraction="0.5",
    maximum_loss_per_trade_usd="150",
)
CALIBRATED = BrokerClockCalibration(offset_seconds=0.0, error_seconds=0.0, status="calibrated")

DEFAULT_SYMBOL_FACTS: dict[str, object] = {
    "point": "0.00001",
    "trade_tick_size": "0.00001",
    "trade_tick_value_profit": "1",
    "trade_tick_value_loss": "1",
    "trade_stops_level": 0,
    "trade_freeze_level": 0,
    "volume_max": "50",
    "visible": True,
}


# --------------------------------------------------------------------------- #
# Deterministic adapters
# --------------------------------------------------------------------------- #


class _Clock:
    """An injected monotonic source; timeouts never depend on wall-clock sleeps."""

    def __init__(self) -> None:
        self.value = 1000.0

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class CrashError(BaseException):
    """Simulated process death at an exact durable boundary.

    It derives from :class:`BaseException` so the cell's own ``except
    Exception`` recovery paths cannot absorb it: everything durable written
    before the raise is exactly the state a killed process would leave behind.
    """


class _Network:
    """A deterministic opaque relay: it forwards on ``route_id`` and role only."""

    def __init__(self, trace: list[tuple[str, str]]) -> None:
        self._cells: dict[str, PairExecutionCell] = {}
        self.route_id = ROUTE_ID
        self.queue: list[dict[str, object]] = []
        self.held: list[dict[str, object]] = []
        self.sent: list[dict[str, object]] = []
        self.hold_kinds: set[str] = set()
        self.drop_kinds: set[str] = set()
        self.drop_from: set[tuple[str, str]] = set()
        self.crash_kinds: set[str] = set()
        self.drop_statuses: set[str] = set()
        self._trace = trace

    def register(self, worker_id: str, cell: PairExecutionCell) -> None:
        self._cells[worker_id] = cell

    def adapter(self, worker_id: str) -> "_Adapter":
        return _Adapter(self, worker_id)

    def send_from(self, worker_id: str, envelope: dict[str, object]) -> None:
        self.sent.append(envelope)
        self._trace.append(("relay", f"{worker_id}:{envelope['kind']}"))
        if envelope.get("route_id") != self.route_id:
            return  # the controller forwards only on a live route record
        kind = str(envelope["kind"])
        if kind in self.drop_kinds or (worker_id, kind) in self.drop_from:
            return
        if kind == "leg_status":
            status = dict(envelope["payload"]).get("status")  # type: ignore[arg-type]
            if status in self.drop_statuses:
                return
        if kind in self.hold_kinds:
            self.held.append(envelope)
            return
        self.queue.append(envelope)
        if kind in self.crash_kinds:
            # The envelope really did leave this process; the caller did not
            # survive to record that fact in its own durable state.
            self.crash_kinds.discard(kind)
            raise CrashError(f"process died immediately after relaying {kind}")

    def release_held(self) -> None:
        self.queue.extend(self.held)
        self.held.clear()

    def pump(self, limit: int = 600) -> None:
        steps = 0
        while self.queue and steps < limit:
            envelope = self.queue.pop(0)
            steps += 1
            target = self._cells.get(str(envelope["to_worker_id"]))
            if target is not None:
                target.handle_event(RelayEnvelopeReceived(envelope))
        if steps >= limit:  # pragma: no cover - a runaway relay is a test bug
            raise AssertionError("the deterministic relay did not converge")

    def mark(self) -> int:
        return len(self.sent)

    def kinds_since(self, marker: int, worker_id: str | None = None) -> list[str]:
        return [
            str(envelope["kind"])
            for envelope in self.sent[marker:]
            if worker_id is None or envelope["from_worker_id"] == worker_id
        ]

    def kinds(self, worker_id: str | None = None) -> list[str]:
        return [
            str(envelope["kind"])
            for envelope in self.sent
            if worker_id is None or envelope["from_worker_id"] == worker_id
        ]

    def payloads(self, kind: str, worker_id: str | None = None) -> list[dict[str, object]]:
        return [
            dict(envelope["payload"])  # type: ignore[arg-type]
            for envelope in self.sent
            if envelope["kind"] == kind and (worker_id is None or envelope["from_worker_id"] == worker_id)
        ]

    def envelopes(self, kind: str, worker_id: str | None = None) -> list[dict[str, object]]:
        return [
            envelope
            for envelope in self.sent
            if envelope["kind"] == kind and (worker_id is None or envelope["from_worker_id"] == worker_id)
        ]


class _Adapter:
    def __init__(self, network: _Network, worker_id: str) -> None:
        self._network, self._worker_id = network, worker_id

    def send(self, envelope: dict[str, object]) -> None:
        self._network.send_from(self._worker_id, envelope)


class FakeMT5:
    def __init__(self, worker_id: str, trace: list[tuple[str, str]], fill_price: float) -> None:
        self.worker_id = worker_id
        self.positions: list[dict[str, object]] = []
        self.orders: list[dict[str, object]] = []
        self.requests: list[dict[str, object]] = []
        self.margin_requests: list[dict[str, object]] = []
        self.symbol_info_calls: list[str] = []
        self._trace = trace
        self._next_ticket = 5000 if worker_id == LEADER else 9000
        self.fill_price = fill_price
        self.margin_per_lot: dict[str, str] = {}
        self.default_margin = "1000"
        self.symbol_facts: dict[str, dict[str, object]] = {}
        self.symbol_unavailable: set[str] = set()
        self.margin_unavailable: set[str] = set()
        self.reject = False
        self.reject_next_entry_retcode: int | None = None
        self.reads_unavailable = False
        self.raise_after_fill = False
        self.receipt_none_after_fill = False
        self.partial_volume: str | None = None
        self.raise_before_fill = False
        self.crash_after_fill = False
        self.close_leaves_position_visible = False

    def account_info(self) -> object:
        return {"login": 1, "server": "demo", "balance": 10000.0, "currency": "USD"}

    def positions_get(self) -> object:
        if self.reads_unavailable:
            return None
        return [dict(position) for position in self.positions]

    def orders_get(self) -> object:
        if self.reads_unavailable:
            return None
        return [dict(order) for order in self.orders]

    def symbol_info(self, symbol: str) -> object:
        self.symbol_info_calls.append(symbol)
        if symbol in self.symbol_unavailable:
            return None
        facts = dict(DEFAULT_SYMBOL_FACTS)
        facts.update(self.symbol_facts.get(symbol, {}))
        return facts

    def order_calc_margin(self, request: object) -> object:
        payload = dict(cast(dict, request))
        self.margin_requests.append(payload)
        symbol = str(payload["symbol"])
        if symbol in self.margin_unavailable:
            return None
        return self.margin_per_lot.get(symbol, self.default_margin)

    def order_send(self, request: dict[str, object]) -> object:
        self.requests.append(dict(request))
        self._trace.append(("order_send", f"{self.worker_id}:{request['type']}"))
        kind = request["type"]
        if kind == "market":
            if self.reject_next_entry_retcode is not None:
                retcode = self.reject_next_entry_retcode
                self.reject_next_entry_retcode = None
                return {"retcode": retcode, "comment": "No prices"}
            if self.reject:
                return {"retcode": _REJECTED, "comment": "no money"}
            if self.raise_before_fill:
                raise RuntimeError("MT5 raised before the broker created any position")
            ticket = self._next_ticket
            self._next_ticket += 1
            self.positions.append(
                {
                    "ticket": ticket,
                    "symbol": request["symbol"],
                    "type": 0 if request["direction"] == "LONG" else 1,
                    "volume": float(self.partial_volume or request["volume"]),
                    "price_open": self.fill_price,
                    "sl": float(request["sl"]),
                    "tp": float(request["tp"]),
                }
            )
            if self.raise_after_fill:
                raise RuntimeError("MT5 raised after the broker already filled the order")
            if self.crash_after_fill:
                self.crash_after_fill = False
                raise CrashError("process died after the broker filled the order")
            if self.receipt_none_after_fill:
                return None
            return {"retcode": _DONE, "order": ticket, "deal": ticket, "price": self.fill_price}
        if kind == "modify_sl_tp":
            for position in self.positions:
                if str(position["ticket"]) == str(request["position"]):
                    position["sl"] = float(str(request["sl"]))
                    position["tp"] = float(str(request["tp"]))
                    return {"retcode": _DONE}
            return {"retcode": _REJECTED, "comment": "unknown ticket"}
        if kind == "close":
            if not self.close_leaves_position_visible:
                self.positions = [p for p in self.positions if str(p["ticket"]) != str(request["ticket"])]
            return {"retcode": _DONE}
        if kind == "cancel":
            self.orders = [o for o in self.orders if str(o["ticket"]) != str(request["ticket"])]
            return {"retcode": _DONE}
        raise AssertionError(f"unhandled fake MT5 request kind: {kind}")


class FaultyJournal(WorkerEffectJournal):
    """A real effect journal whose durable boundaries can fail deterministically."""

    def __init__(self, path: Path, trace: list[tuple[str, str]], worker_id: str) -> None:
        super().__init__(path)
        self._trace = trace
        self._worker_id = worker_id
        self.fail_prepare = 0
        self.fail_send_started = 0
        self.fail_receipt = 0
        self.crash_after_prepare = False

    def prepare(self, effect_id: str, payload: dict[str, object]) -> str:
        if self.fail_prepare > 0:
            self.fail_prepare -= 1
            raise EffectJournalError("simulated durable prepare failure")
        state = super().prepare(effect_id, payload)
        self._trace.append(("prepare", f"{self._worker_id}:{effect_id}"))
        if self.crash_after_prepare:
            self.crash_after_prepare = False
            raise CrashError("process died after the effect was durably prepared")
        return state

    def mark_send_started(self, effect_id: str) -> None:
        if self.fail_send_started > 0:
            self.fail_send_started -= 1
            raise EffectJournalError("simulated durable send_started failure")
        super().mark_send_started(effect_id)

    def record_receipt(self, effect_id: str, receipt: dict[str, object]) -> None:
        if self.fail_receipt > 0:
            self.fail_receipt -= 1
            raise EffectJournalError("simulated durable receipt failure")
        super().record_receipt(effect_id, receipt)


# --------------------------------------------------------------------------- #
# Fixture builders
# --------------------------------------------------------------------------- #


def _entry(name: str = SYMBOL, **overrides: object) -> CatalogEntry:
    values: dict[str, object] = {
        "name": name,
        "trade_mode": 4,
        "trade_calc_mode": 0,
        "digits": 5,
        "point": "0.00001",
        "trade_tick_size": "0.00001",
        "contract_size": "100000",
        "volume_min": "0.01",
        "volume_step": "0.01",
        "volume_max": "50",
        "allowed_directions": ("LONG", "SHORT"),
        "filling_modes": ("FOK", "IOC"),
        "base_currency": "EUR",
        "profit_currency": "USD",
    }
    values.update(overrides)
    return CatalogEntry(**values)  # type: ignore[arg-type]


def _summary(worker_id: str, *entries: CatalogEntry, epoch: int = 1, version: int = 1) -> CatalogSummary:
    return CatalogSummary(
        worker_id=worker_id, publisher_epoch=epoch, version=version, entries=tuple(entries)
    )


def _route(role: str) -> RouteAssignment:
    return RouteAssignment(
        route_id=ROUTE_ID,
        role=cast(pair_cell.Role, role),
        leader_worker_id=LEADER,
        follower_worker_id=FOLLOWER,
    )


class _Side:
    def __init__(
        self,
        *,
        tmp: Path,
        worker_id: str,
        role: str,
        network: _Network,
        limits: WorkerRiskLimits,
        clock: _Clock,
        trace: list[tuple[str, str]],
        fill_price: float,
        allow_live: bool = True,
    ) -> None:
        self.worker_id = worker_id
        self.role = role
        self.tmp = tmp
        self.trace = trace
        self.network = network
        self.clock = clock
        self.limits = limits
        self.allow_live = allow_live
        self.route = _route(role)
        self.mt5 = FakeMT5(worker_id, trace, fill_price)
        self.journal = FaultyJournal(tmp / f"{worker_id}-journal.db", trace, worker_id)
        self.scheduler = DeadlineAwareTraderRpcScheduler()
        self.cell = self._build()
        network.register(worker_id, self.cell)

    def _build(self) -> PairExecutionCell:
        return PairExecutionCell(
            worker_id=self.worker_id,
            route=self.route,
            db_path=self.tmp / f"{self.worker_id}-cell.db",
            relay=self.network.adapter(self.worker_id),
            mt5=self.mt5,
            local_risk_limits=self.limits,
            effect_journal=self.journal,
            scheduler=self.scheduler,
            allow_live=self.allow_live,
            monotonic=self.clock,
        )

    def restart(self) -> PairExecutionCell:
        """Reopen the same durable state exactly as a restarted Worker would."""

        self.cell.close()
        self.journal.close()
        self.journal = FaultyJournal(self.tmp / f"{self.worker_id}-journal.db", self.trace, self.worker_id)
        self.scheduler = DeadlineAwareTraderRpcScheduler()
        self.cell = self._build()
        self.network.register(self.worker_id, self.cell)
        return self.cell

    def facts(
        self,
        now: datetime,
        *,
        orders: list[dict[str, object]] | None | object = _UNSET,
        positions: list[dict[str, object]] | None | object = _UNSET,
        terminal_connected: bool = True,
        margin_headroom_ok: bool = True,
    ) -> ReadinessFactsEvent:
        return ReadinessFactsEvent(
            account_login=f"{self.worker_id}-login",
            account_server="demo",
            terminal_connected=terminal_connected,
            orders=cast(list, [] if orders is _UNSET else orders),
            positions=cast(list, [] if positions is _UNSET else positions),
            margin_headroom_ok=margin_headroom_ok,
            observed_at=now,
        )

    def entry_requests(self) -> list[dict[str, object]]:
        return [r for r in self.mt5.requests if r["type"] == "market"]

    def modify_requests(self) -> list[dict[str, object]]:
        return [r for r in self.mt5.requests if r["type"] == "modify_sl_tp"]

    def close_requests(self) -> list[dict[str, object]]:
        return [r for r in self.mt5.requests if r["type"] == "close"]


class PairCellTestCase(unittest.TestCase):
    """Shared two-Worker fixture, pairing acceptance, discovery, and bootstrap."""

    leader_limits = LEADER_RISK
    follower_limits = FOLLOWER_RISK
    follower_allow_live = True

    def setUp(self) -> None:
        import shutil
        import tempfile

        self.tmp = Path(tempfile.mkdtemp(prefix="pair_cell_test_"))
        self.now = BASE_NOW
        self.trace: list[tuple[str, str]] = []
        self.clock = _Clock()
        self.net = _Network(self.trace)
        self.leader = _Side(
            tmp=self.tmp,
            worker_id=LEADER,
            role="leader",
            network=self.net,
            limits=self.leader_limits,
            clock=self.clock,
            trace=self.trace,
            fill_price=1.10010,
        )
        self.follower = _Side(
            tmp=self.tmp,
            worker_id=FOLLOWER,
            role="follower",
            network=self.net,
            limits=self.follower_limits,
            clock=self.clock,
            trace=self.trace,
            fill_price=1.10040,
            allow_live=self.follower_allow_live,
        )
        self.leader_entries: list[CatalogEntry] = [_entry()]
        self.follower_entries: list[CatalogEntry] = [_entry()]
        self.accepted_policy: StrategyPolicy | None = None
        self.addCleanup(lambda: shutil.rmtree(self.tmp, ignore_errors=True))
        self.addCleanup(self._close_cells)

    def _close_cells(self) -> None:
        for side in (self.leader, self.follower):
            for closeable in (side.cell, side.journal):
                try:
                    closeable.close()
                except Exception:  # pragma: no cover - already closed
                    pass

    # -- bootstrap --------------------------------------------------------- #

    def add_second_product(self) -> None:
        self.leader_entries.append(_entry(OTHER_SYMBOL))
        self.follower_entries.append(_entry(OTHER_SYMBOL))

    def tick(self, seconds: float = 0.0) -> None:
        self.now += timedelta(seconds=seconds)
        for side in (self.leader, self.follower):
            side.cell.handle_event(side.facts(self.now))
            side.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()

    def feed_catalogs(self) -> None:
        self.follower.cell.handle_event(
            LocalCatalogEvent(entries=tuple(self.follower_entries), observed_at=self.now)
        )
        self.net.pump()
        self.leader.cell.handle_event(
            LocalCatalogEvent(entries=tuple(self.leader_entries), observed_at=self.now)
        )
        self.net.pump()

    def accept(self, proposal_id: str = PROPOSAL) -> PairingAcceptance:
        acceptance = self.follower.cell.freeze_pairing_acceptance(proposal_id=proposal_id)
        self.net.pump()
        return acceptance

    def policy(self, **shared: object) -> StrategyPolicy:
        values: dict[str, object] = {
            "entry_edge_points": "1",
            "quote_max_age_seconds": 2.0,
            "quote_max_skew_seconds": 1.0,
            "follower_confirmation_timeout_seconds": 5.0,
        }
        values.update(shared)
        acceptance = self.leader.cell.peer_pairing_acceptance()
        assert acceptance is not None, "the leader has no Pairing Acceptance payload"
        return canonical_policy_from_acceptance(
            policy_version="policy-1",
            leader_risk=self.leader.limits,
            acceptance=acceptance,
            shared=values,
        )

    def product_id(self, symbol: str = SYMBOL, *, side: _Side | None = None) -> str:
        universe = (side or self.leader).cell.discovered_universe()
        assert universe is not None, "no universe has been discovered"
        product = next(p for p in universe.products if p.symbol == symbol)
        return product.product_id

    def prime(
        self, policy: StrategyPolicy | None = None, *, shared: dict[str, object] | None = None
    ) -> None:
        """Route, discovery, policy acceptance, quote collection, then plans."""

        self.tick()
        self.accept()
        self.feed_catalogs()
        self.accepted_policy = policy or self.policy(**(shared or {}))
        self.leader.cell.accept_policy(self.accepted_policy)
        self.net.pump()
        self.tick()
        self.warm(SYMBOL)

    def warm(self, symbol: str, *, sequence: int = 1) -> None:
        """Collect a first, deliberately non-qualifying quote for one product."""

        self.feed_quotes(
            symbol=symbol,
            sequence=sequence,
            leader_bid="1.10000",
            leader_ask="1.10010",
            follower_bid="1.10000",
            follower_ask="1.10010",
        )
        self.tick()
        self.net.pump()

    def feed_quotes(
        self,
        *,
        symbol: str = SYMBOL,
        product_id: str | None = None,
        leader_symbol: str | None = None,
        follower_symbol: str | None = None,
        leader_bid: str = "1.10000",
        leader_ask: str = "1.10010",
        follower_bid: str = "1.10050",
        follower_ask: str = "1.10060",
        sequence: int = 2,
        epoch: str = "epoch-1",
        deliver: bool = True,
        leader_broker_time: datetime | None = None,
        follower_broker_time: datetime | None = None,
        leader_calibration: BrokerClockCalibration = CALIBRATED,
        follower_calibration: BrokerClockCalibration = CALIBRATED,
    ) -> None:
        """Feed both local quotes, then deliver, so one decision uses both."""

        resolved = product_id or self.product_id(symbol)
        self.follower.cell.handle_event(
            LocalQuoteEvent(
                product_id=resolved,
                symbol=follower_symbol or symbol,
                bid=Decimal(follower_bid),
                ask=Decimal(follower_ask),
                broker_time=follower_broker_time or self.now,
                local_receive_time=self.now,
                recovery_epoch=epoch,
                sequence=sequence,
                calibration=follower_calibration,
            )
        )
        self.leader.cell.handle_event(
            LocalQuoteEvent(
                product_id=resolved,
                symbol=leader_symbol or symbol,
                bid=Decimal(leader_bid),
                ask=Decimal(leader_ask),
                broker_time=leader_broker_time or self.now,
                local_receive_time=self.now,
                recovery_epoch=epoch,
                sequence=sequence,
                calibration=leader_calibration,
            )
        )
        if deliver:
            self.net.pump()

    def run_entry(self) -> None:
        self.prime()
        self.feed_quotes()

    def attempt_payloads(self) -> list[dict[str, object]]:
        return self.net.payloads("attempt", LEADER)

    def last_attempt(self) -> Attempt:
        payloads = self.attempt_payloads()
        self.assertTrue(payloads, "no attempt was dispatched")
        return pair_cell._attempt_from_canonical(payloads[-1]["attempt"])  # type: ignore[arg-type]

    def deliver_attempt(self, attempt: Attempt, *, route_id: str = ROUTE_ID) -> None:
        """Deliver a hand-built attempt straight to the follower's relay seam."""

        self.follower.cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": pair_cell.PROTOCOL_VERSION,
                    "kind": "attempt",
                    "route_id": route_id,
                    "from_worker_id": LEADER,
                    "from_role": "leader",
                    "to_worker_id": FOLLOWER,
                    "payload": {"attempt": attempt.canonical()},
                }
            )
        )

    def deliver_policy(self, policy: StrategyPolicy, *, leader_empty: bool = True) -> None:
        self.follower.cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": pair_cell.PROTOCOL_VERSION,
                    "kind": "policy",
                    "route_id": ROUTE_ID,
                    "from_worker_id": LEADER,
                    "from_role": "leader",
                    "to_worker_id": FOLLOWER,
                    "payload": {
                        "policy": policy.canonical(),
                        "policy_hash": policy.hash,
                        "leader_broker_verified_empty": leader_empty,
                    },
                }
            )
        )
        self.net.pump()

    def route_event(
        self,
        side: _Side,
        *,
        route_id: str | None = ROUTE_ID,
        role: str | None = None,
        leader_worker_id: str | None = LEADER,
        follower_worker_id: str | None = FOLLOWER,
        state: str | None = "ACTIVE",
    ) -> None:
        side.cell.handle_event(
            RouteMetadataEvent(
                route_id=route_id,
                role=cast(pair_cell.Role | None, role or side.role),
                leader_worker_id=leader_worker_id,
                follower_worker_id=follower_worker_id,
                state=cast(pair_cell.RouteState | None, state),
                observed_at=self.now,
            )
        )
        self.net.pump()

    def unpairing(self, state: str = "UNPAIRING") -> None:
        for side in (self.leader, self.follower):
            self.route_event(side, state=state)

    def peer_session(self, connected: bool) -> None:
        """Both Workers observe the same authenticated relay transition."""

        for side in (self.leader, self.follower):
            side.cell.handle_event(PeerSessionEvent(connected=connected, observed_at=self.now))
        self.net.pump()


# --------------------------------------------------------------------------- #
# Removed architecture and removed configuration model
# --------------------------------------------------------------------------- #


class RemovedArchitectureTests(unittest.TestCase):
    def test_contract_lease_arm_commit_and_built_products_are_gone(self) -> None:
        for removed in (
            "PairContract",
            "PairLease",
            "LeaseEvent",
            "ProtectionCalibration",
            "BuiltProduct",
            "BuiltProductReference",
            "ProductCatalog",
            "ProductSpecification",
        ):
            self.assertFalse(hasattr(pair_cell, removed), f"{removed} must not exist")
        source = Path(pair_cell.__file__).read_text(encoding="utf-8")
        for token in (
            "arm_timeout_seconds",
            "commit_lead_seconds",
            "execution_expiry_seconds",
            "leader_volume",
            "follower_volume",
            "ttl_seconds",
            "arm_request",
            "arm_ack",
            "active_built_products",
            "route_generation",
        ):
            self.assertNotIn(token, source, f"{token} must not remain in the core module")
        # These survive only as configuration keys the cell explicitly rejects.
        for name in ("trader_id", "built_products", "eligible_products"):
            self.assertIn(name, pair_cell.FORBIDDEN_CONFIG_KEYS)
            for token in (f"{name}=", f'"{name}":', f"self._{name}"):
                self.assertNotIn(token, source, f"{token} must not remain in the core module")
        for named in (pair_cell.RouteAssignment, pair_cell.Attempt, pair_cell.PairResult):
            self.assertNotIn("trader_id", named.__dataclass_fields__)

    def test_eligible_products_is_not_a_policy_filter(self) -> None:
        acceptance = build_pairing_acceptance(
            proposal_id=PROPOSAL,
            worker_id=FOLLOWER,
            startup_balance_usd="4000",
            account_currency="USD",
        )
        policy = canonical_policy_from_acceptance(
            policy_version="policy-1", leader_risk=LEADER_RISK, acceptance=acceptance
        )
        self.assertNotIn("eligible_products", policy.canonical())
        with self.assertRaises(TypeError):
            StrategyPolicy(  # type: ignore[call-arg]
                policy_version="policy-1",
                entry_edge_points="4",
                quote_max_age_seconds=1.0,
                quote_max_skew_seconds=1.0,
                leader_risk=LEADER_RISK,
                follower_risk=FOLLOWER_RISK,
                eligible_products=("a",),
            )

    def test_policy_parameters_match_the_specification(self) -> None:
        acceptance = build_pairing_acceptance(
            proposal_id=PROPOSAL,
            worker_id=FOLLOWER,
            startup_balance_usd="4000",
            account_currency="USD",
        )
        policy = canonical_policy_from_acceptance(
            policy_version="policy-1",
            leader_risk=LEADER_RISK,
            acceptance=acceptance,
            shared={"maximum_holding_seconds": 60.0, "flatten_at_ny": "16:55"},
        )
        self.assertEqual(
            set(policy.canonical()),
            {
                "policy_version",
                "entry_edge_points",
                "quote_max_age_seconds",
                "quote_max_skew_seconds",
                "follower_confirmation_timeout_seconds",
                "maximum_holding_seconds",
                "flatten_at_ny",
                "mode",
                "leader_risk",
                "follower_risk",
            },
        )
        self.assertEqual(policy.follower_confirmation_timeout_seconds, 5.0)

    def test_a_canonical_policy_round_trips_through_its_relay_form(self) -> None:
        acceptance = build_pairing_acceptance(
            proposal_id=PROPOSAL,
            worker_id=FOLLOWER,
            startup_balance_usd="4000",
            account_currency="USD",
        )
        policy = canonical_policy_from_acceptance(
            policy_version="policy-1",
            leader_risk=LEADER_RISK,
            acceptance=acceptance,
            shared={"maximum_holding_seconds": 90.0},
        )
        restored = pair_cell._policy_from_canonical(policy.canonical())
        self.assertEqual(restored, policy)
        self.assertEqual(restored.hash, policy.hash)


# --------------------------------------------------------------------------- #
# Configuration authority and default materialization
# --------------------------------------------------------------------------- #


class ConfigurationAuthorityTests(unittest.TestCase):
    def test_a_follower_file_may_set_only_its_own_risk_and_allow_live(self) -> None:
        raw = {
            "maximum_margin_fraction": "0.2",
            "daily_loss_fraction": "0.03",
            "trade_loss_fraction": "0.02",
            "maximum_loss_per_trade_usd": "40",
            "allow_live": False,
        }
        validate_configuration_authority(raw, role="follower")
        validate_configuration_authority(raw, role="leader")
        validate_configuration_authority(raw, role=None)

    def test_a_follower_file_containing_shared_policy_is_a_startup_error(self) -> None:
        for key, value in (
            ("entry_edge_points", "4"),
            ("quote_max_age_seconds", 1.0),
            ("quote_max_skew_seconds", 1.0),
            ("follower_confirmation_timeout_seconds", 5.0),
            ("flatten_at_ny", "16:00"),
            ("maximum_holding_seconds", 60.0),
            ("mode", "shadow"),
        ):
            with self.assertRaises(PairExecutionCellError) as raised:
                validate_configuration_authority({key: value}, role="follower")
            self.assertIn(key, str(raised.exception))

    def test_a_leader_file_may_set_shared_policy_but_a_role_conflict_fails_closed(self) -> None:
        raw = {"entry_edge_points": "6", "maximum_margin_fraction": "0.2"}
        validate_configuration_authority(raw, role="leader")
        validate_configuration_authority(raw, role=None)
        with self.assertRaises(PairExecutionCellError):
            validate_configuration_authority(raw, role="follower")

    def test_pairing_and_discovery_keys_are_never_operator_configuration(self) -> None:
        for key in ("route", "route_id", "trader_id", "built_products", "eligible_products"):
            for role in ("leader", "follower", None):
                with self.assertRaises(PairExecutionCellError) as raised:
                    validate_configuration_authority(
                        {key: "x"}, role=cast(pair_cell.Role | None, role)
                    )
                self.assertIn(key, str(raised.exception))

    def test_a_leader_file_can_never_author_the_follower_risk_block(self) -> None:
        with self.assertRaises(PairExecutionCellError):
            default_worker_risk_limits(
                startup_balance_usd="4000",
                account_currency="USD",
                overrides={"strategy_budget_usd": "999"},
            )


class DefaultMaterializationTests(unittest.TestCase):
    def test_the_synthesized_defaults_match_the_specification(self) -> None:
        self.assertEqual(
            default_shared_policy_values(),
            {
                "mode": "live",
                "entry_edge_points": "4",
                "quote_max_age_seconds": 1.0,
                "quote_max_skew_seconds": 1.0,
                "follower_confirmation_timeout_seconds": 5.0,
                "flatten_at_ny": "16:00",
                "maximum_holding_seconds": None,
            },
        )
        limits = default_worker_risk_limits(startup_balance_usd=12345.0, account_currency="USD")
        self.assertEqual(limits.strategy_budget_usd, "12345")
        self.assertEqual(limits.maximum_margin_fraction, "0.10")
        self.assertEqual(limits.daily_loss_fraction, "0.03")
        self.assertEqual(limits.trade_loss_fraction, "0.02")
        self.assertEqual(limits.maximum_loss_per_trade_usd, "40")

    def test_the_default_budget_is_the_startup_balance_with_no_margin_headroom(self) -> None:
        limits = default_worker_risk_limits(startup_balance_usd="10000", account_currency="USD")
        # 0.10 is the whole margin budget; legacy's extra 20% headroom is gone.
        self.assertEqual(limits.margin_budget_usd, Decimal("1000.00"))
        self.assertEqual(limits.daily_loss_limit_usd, Decimal("300.00"))
        self.assertEqual(limits.trade_loss_limit_usd, Decimal("200.00"))
        # 40 is a hard per-trade admission cap, not an emergency stop trigger.
        self.assertEqual(limits.leg_loss_cap_usd, Decimal("40"))

    def test_a_non_usd_or_non_positive_balance_fails_closed(self) -> None:
        for balance, currency in (
            ("10000", "EUR"),
            ("0", "USD"),
            ("-1", "USD"),
            (None, "USD"),
            ("not-a-number", "USD"),
        ):
            with self.assertRaises(PairExecutionCellError):
                default_worker_risk_limits(
                    startup_balance_usd=balance, account_currency=currency
                )


# --------------------------------------------------------------------------- #
# Initial Compatible Product Discovery: the compatibility rules themselves
# --------------------------------------------------------------------------- #


class DiscoveryCompatibilityTests(unittest.TestCase):
    def run_discovery(
        self, first: list[CatalogEntry], second: list[CatalogEntry]
    ) -> pair_cell.DiscoveryOutcome:
        return discover_compatible_products(
            _summary(LEADER, *first), _summary(FOLLOWER, *second)
        )

    def test_trade_mode_four_is_applied_to_each_catalog_before_the_intersection(self) -> None:
        outcome = self.run_discovery([_entry()], [_entry(trade_mode=3)])
        self.assertFalse(outcome.admitted)
        self.assertEqual(outcome.reason, "no compatible symbol survived discovery")
        self.assertEqual(outcome.excluded, ())

    def test_a_missing_non_integer_or_boolean_trade_mode_invalidates_the_catalog(self) -> None:
        for bad in (None, "4", 4.0, True):
            outcome = self.run_discovery([_entry()], [_entry(trade_mode=bad)])
            self.assertFalse(outcome.admitted)
            self.assertIn("invalid catalog", outcome.reason)
            self.assertIn(FOLLOWER, outcome.reason)

    def test_a_duplicated_catalog_symbol_invalidates_the_catalog(self) -> None:
        outcome = self.run_discovery([_entry(), _entry()], [_entry()])
        self.assertFalse(outcome.admitted)
        self.assertIn("duplicated", outcome.reason)

    def test_symbol_intersection_is_exact_with_no_suffix_normalization(self) -> None:
        outcome = self.run_discovery([_entry("EURUSD")], [_entry("EURUSD.r")])
        self.assertFalse(outcome.admitted)
        self.assertEqual(outcome.reason, "no compatible symbol survived discovery")

    def test_hard_specification_inequality_excludes_the_candidate(self) -> None:
        for field, value in (
            ("trade_calc_mode", 1),
            ("contract_size", "10000"),
            ("volume_min", "0.02"),
            ("volume_step", "0.05"),
        ):
            outcome = self.run_discovery([_entry()], [_entry(**{field: value})])
            self.assertFalse(outcome.admitted)
            self.assertEqual(outcome.excluded[0][0], SYMBOL)
            self.assertIn(field, outcome.excluded[0][1])

    def test_differing_allowed_directions_are_a_hard_mismatch(self) -> None:
        outcome = self.run_discovery(
            [_entry()], [_entry(allowed_directions=("LONG",))]
        )
        self.assertFalse(outcome.admitted)
        self.assertIn("allowed_directions", outcome.excluded[0][1])

    def test_a_symbol_missing_either_direction_is_excluded(self) -> None:
        for directions in (("LONG",), ("SHORT",)):
            outcome = self.run_discovery(
                [_entry(allowed_directions=directions)], [_entry(allowed_directions=directions)]
            )
            self.assertFalse(outcome.admitted)
            self.assertEqual(outcome.excluded[0][1], "bidirectional trading is unavailable")

    def test_shared_fok_is_preferred_over_shared_ioc(self) -> None:
        both = self.run_discovery([_entry()], [_entry()])
        self.assertEqual(both.products[0].filling_mode, "FOK")
        ioc = self.run_discovery([_entry()], [_entry(filling_modes=("IOC",))])
        self.assertEqual(ioc.products[0].filling_mode, "IOC")

    def test_no_shared_filling_mode_excludes_the_candidate(self) -> None:
        outcome = self.run_discovery(
            [_entry(filling_modes=("FOK",))], [_entry(filling_modes=("IOC",))]
        )
        self.assertFalse(outcome.admitted)
        self.assertEqual(outcome.excluded[0][1], "no shared FOK or IOC filling mode")

    def test_currency_digits_point_and_tick_size_may_differ(self) -> None:
        """Compatibility is deliberately looser than the removed Built model."""

        outcome = self.run_discovery(
            [_entry()],
            [
                _entry(
                    base_currency="XXX",
                    profit_currency="JPY",
                    digits=3,
                    point="0.001",
                    trade_tick_size="0.001",
                )
            ],
        )
        self.assertTrue(outcome.admitted, outcome.reason)
        self.assertEqual(len(outcome.products), 1)

    def test_the_canonical_point_is_the_maximum_and_volume_max_the_minimum(self) -> None:
        outcome = self.run_discovery(
            [_entry(point="0.00001", volume_max="50")],
            [_entry(point="0.001", volume_max="30")],
        )
        product = outcome.products[0]
        self.assertEqual(product.canonical_point, "0.001")
        self.assertEqual(product.volume_max, "30")

    def test_product_identity_is_deterministic_across_repeated_runs(self) -> None:
        first = self.run_discovery([_entry()], [_entry()]).products[0]
        second = self.run_discovery([_entry()], [_entry()]).products[0]
        self.assertEqual(first.product_id, second.product_id)
        self.assertTrue(first.product_id.startswith(f"{SYMBOL}:"))
        # Equivalent decimal spellings are the same product identity.
        third = self.run_discovery(
            [_entry(contract_size="100000.0")], [_entry(contract_size=100000)]
        ).products[0]
        self.assertEqual(first.product_id, third.product_id)
        # A different compatibility summary is a different identity.
        fourth = self.run_discovery(
            [_entry(volume_min="0.02")], [_entry(volume_min="0.02")]
        ).products[0]
        self.assertNotEqual(first.product_id, fourth.product_id)

    def test_an_empty_intersection_fails_closed(self) -> None:
        outcome = self.run_discovery([_entry("AUDUSD")], [_entry("NZDUSD")])
        self.assertFalse(outcome.admitted)
        self.assertEqual(outcome.products, ())

    def test_an_admitted_product_has_a_one_to_one_lot_relationship(self) -> None:
        product = self.run_discovery([_entry()], [_entry()]).products[0]
        self.assertEqual(product.contract_size, "100000")
        self.assertEqual(product.volume_min, "0.01")
        self.assertEqual(product.volume_step, "0.01")


# --------------------------------------------------------------------------- #
# Discovery over the live route
# --------------------------------------------------------------------------- #


class DiscoveryLifecycleTests(PairCellTestCase):
    def test_discovery_runs_after_the_route_and_before_any_quote_is_processed(self) -> None:
        self.tick()
        self.accept()
        self.assertIsNone(self.leader.cell.universe_generation())
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)
        self.feed_catalogs()
        self.assertEqual(self.leader.cell.universe_generation(), 1)
        self.assertEqual(self.follower.cell.universe_generation(), 1)
        self.assertEqual(
            self.leader.cell.discovered_universe().products[0].symbol,  # type: ignore[union-attr]
            SYMBOL,
        )

    def test_catalog_summaries_are_versioned_and_relayed_opaquely(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        summaries = self.net.payloads("catalog_summary")
        self.assertTrue(summaries)
        catalog = cast(dict, summaries[0]["catalog"])
        self.assertEqual(catalog["version"], 1)
        self.assertIn("entries", catalog)
        for envelope in self.net.envelopes("catalog_summary"):
            self.assertEqual(envelope["route_id"], ROUTE_ID)

    def test_an_unavailable_catalog_fails_closed_with_an_explicit_reason(self) -> None:
        self.tick()
        self.accept()
        self.leader.cell.handle_event(
            LocalCatalogEvent(entries=tuple(self.leader_entries), observed_at=self.now)
        )
        self.net.pump()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertIsNone(result.universe_generation)
        self.assertFalse(result.ready)
        self.assertIn("no discovered universe", result.ready_reason)

    def test_an_invalid_catalog_fails_closed_and_admits_no_entry(self) -> None:
        self.follower_entries = [_entry(trade_mode=None)]
        self.tick()
        self.accept()
        self.feed_catalogs()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertIsNone(result.universe_generation)
        self.assertIn("invalid catalog", result.discovery_reason)
        self.feed_quotes(product_id="fake-product")
        self.assertEqual(self.attempt_payloads(), [])
        self.assertEqual(self.leader.entry_requests(), [])

    def test_an_empty_intersection_starts_no_quote_processing_and_admits_no_entry(self) -> None:
        self.follower_entries = [_entry("NZDUSD")]
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.assertIsNone(self.leader.cell.universe_generation())
        self.assertEqual(
            self.leader.cell.discovery_reason(), "no compatible symbol survived discovery"
        )
        self.feed_quotes(product_id="fake-product")
        self.assertEqual(self.attempt_payloads(), [])

    def test_the_follower_refuses_a_universe_it_cannot_reproduce_itself(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        forged = pair_cell.DiscoveredUniverse(
            route_id=ROUTE_ID,
            universe_generation=99,
            products=(
                DiscoveredProduct(
                    symbol="NZDUSD",
                    trade_calc_mode=0,
                    contract_size="100000",
                    volume_min="0.01",
                    volume_step="0.01",
                    volume_max="50",
                    canonical_point="0.00001",
                    filling_mode="FOK",
                ),
            ),
            leader_catalog_hash="x",
            follower_catalog_hash="y",
        )
        self.follower.cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": pair_cell.PROTOCOL_VERSION,
                    "kind": "universe",
                    "route_id": ROUTE_ID,
                    "from_worker_id": LEADER,
                    "from_role": "leader",
                    "to_worker_id": FOLLOWER,
                    "payload": {"universe": forged.canonical()},
                }
            )
        )
        self.assertEqual(self.follower.cell.universe_generation(), 1)
        self.assertIn(
            "does not match this Worker's own compatibility result",
            cast(str, self.follower.cell.last_rediscovery_failure()),
        )

    def test_the_universe_is_frozen_and_an_hourly_refresh_never_admits_a_new_symbol(self) -> None:
        self.prime()
        generation = self.leader.cell.universe_generation()
        self.add_second_product()
        self.feed_catalogs()
        self.tick(seconds=61 * 60)
        self.feed_quotes(sequence=3, symbol=SYMBOL)
        self.assertEqual(self.leader.cell.universe_generation(), generation)
        self.assertEqual(len(self.leader.cell.eligible_products()), 1)
        self.assertEqual(
            [p.symbol for p in self.leader.cell.discovered_universe().products],  # type: ignore[union-attr]
            [SYMBOL],
        )

    def test_the_hourly_refresh_suspends_a_symbol_whose_specification_drifted(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader_entries = [_entry(contract_size="10000")]
        self.feed_catalogs()
        self.tick(seconds=61 * 60)
        self.feed_quotes(sequence=3)
        self.assertEqual(self.leader.cell.eligible_products(), ())
        self.assertIn(
            "frozen compatibility summary", self.leader.cell.suspended_products()[product]
        )
        self.assertEqual(self.leader.cell.universe_generation(), 1)

    def test_a_symbol_that_stops_being_fully_tradable_is_suspended(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader_entries = [_entry(trade_mode=3)]
        self.feed_catalogs()
        self.tick(seconds=61 * 60)
        self.feed_quotes(sequence=3)
        self.assertIn("fully tradable", self.leader.cell.suspended_products()[product])


class RediscoveryTests(PairCellTestCase):
    def test_explicit_rediscovery_installs_a_new_generation_on_the_same_route(self) -> None:
        self.prime()
        policy_hash = self.leader.cell.handle_event(ClockTickEvent(self.now)).policy_hash
        self.add_second_product()
        self.feed_catalogs()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            self.assertEqual(side.cell.universe_generation(), 2)
            self.assertEqual(side.cell.route_id(), ROUTE_ID)
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.policy_hash, policy_hash)
        self.assertEqual(self.leader.cell.assigned_role(), "leader")
        self.assertEqual(
            sorted(p.symbol for p in self.leader.cell.discovered_universe().products),  # type: ignore[union-attr]
            [SYMBOL, OTHER_SYMBOL] if SYMBOL < OTHER_SYMBOL else [OTHER_SYMBOL, SYMBOL],
        )

    def test_either_role_can_request_a_rediscovery_without_a_controller_action(self) -> None:
        self.prime()
        self.add_second_product()
        self.feed_catalogs()
        self.follower.cell.handle_event(
            RediscoveryRequestEvent(actor="follower-operator", reason="new symbol", observed_at=self.now)
        )
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 2)
        self.assertEqual(self.follower.cell.universe_generation(), 2)

    def test_rediscovery_is_refused_while_an_attempt_is_unresolved(self) -> None:
        self.run_entry()
        self.assertIsNotNone(self.leader.cell.handle_event(ClockTickEvent(self.now)).attempt_id)
        result = self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.assertEqual(self.leader.cell.universe_generation(), 1)
        self.assertIn("rediscovery refused", cast(str, result.rediscovery_failure))
        # The refused attempt is over; it never stays pending.
        self.assertIn("rediscovery refused", cast(str, self.leader.cell.last_rediscovery_failure()))

    def test_a_failed_rediscovery_keeps_the_previous_generation_and_releases_entry(self) -> None:
        self.prime()
        self.follower_entries = [_entry("NZDUSD")]
        self.feed_catalogs()
        result = self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.assertEqual(result.universe_generation, 1)
        self.assertEqual(
            result.rediscovery_failure, "no compatible symbol survived discovery"
        )
        # The failed attempt is bounded: the previous generation stays in force
        # and entry readiness returns instead of being stranded forever.
        ready = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(ready.ready, ready.ready_reason)
        self.assertEqual(ready.universe_generation, 1)
        self.feed_quotes(sequence=4)
        self.assertEqual(len(self.attempt_payloads()), 1)

    def test_a_quarantine_survives_a_rediscovery_with_an_unchanged_identity(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        self.tick()
        self.assertIn(product, self.leader.cell.quarantined_products())
        self.assertEqual(self.leader.cell.quarantine_generation(product), 1)
        self.tick()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 2)
        self.assertIn(product, self.leader.cell.quarantined_products())


# --------------------------------------------------------------------------- #
# Route identity, controller authority, and UNPAIRING
# --------------------------------------------------------------------------- #


class RouteIdentityTests(PairCellTestCase):
    def test_a_route_requires_two_distinct_workers_and_a_route_id(self) -> None:
        with self.assertRaises(PairExecutionCellError):
            RouteAssignment(route_id="", role="leader", leader_worker_id=LEADER, follower_worker_id=FOLLOWER)
        with self.assertRaises(PairExecutionCellError):
            RouteAssignment(route_id=ROUTE_ID, role="leader", leader_worker_id=LEADER, follower_worker_id=LEADER)
        with self.assertRaises(PairExecutionCellError):
            RouteAssignment(  # type: ignore[arg-type]
                route_id=ROUTE_ID, role="observer", leader_worker_id=LEADER, follower_worker_id=FOLLOWER
            )
        self.assertNotIn("trader_id", RouteAssignment.__dataclass_fields__)

    def test_a_worker_never_adopts_a_role_the_route_does_not_assign(self) -> None:
        with self.assertRaises(PairExecutionCellError):
            PairExecutionCell(
                worker_id="worker-stranger",
                route=_route("leader"),
                db_path=self.tmp / "stranger.db",
                relay=self.net.adapter("worker-stranger"),
                mt5=self.leader.mt5,
                local_risk_limits=LEADER_RISK,
                effect_journal=self.leader.journal,
                scheduler=self.leader.scheduler,
            )

    def test_every_relay_envelope_carries_the_route_id(self) -> None:
        self.run_entry()
        self.assertTrue(self.net.sent)
        for envelope in self.net.sent:
            self.assertEqual(envelope["route_id"], ROUTE_ID)
            self.assertIn(envelope["from_role"], ("leader", "follower"))
            self.assertNotIn("trader_id", envelope)

    def test_the_attempt_names_its_route_and_universe_generation(self) -> None:
        self.run_entry()
        attempt = self.last_attempt()
        self.assertEqual(attempt.route_id, ROUTE_ID)
        self.assertEqual(attempt.universe_generation, 1)
        self.assertEqual(attempt.product_id, self.product_id())

    def test_an_envelope_on_another_route_is_ignored(self) -> None:
        self.prime()
        self.follower.cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": pair_cell.PROTOCOL_VERSION,
                    "kind": "policy",
                    "route_id": "route-other",
                    "from_worker_id": LEADER,
                    "from_role": "leader",
                    "to_worker_id": FOLLOWER,
                    "payload": {"policy": {}, "policy_hash": "x"},
                }
            )
        )
        self.assertTrue(self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_accepted)

    def test_an_attempt_from_another_route_is_rejected_without_a_broker_send(self) -> None:
        from dataclasses import replace

        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        attempt = self.last_attempt()
        self.deliver_attempt(replace(attempt, route_id="route-other"))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "another route",
            [str(p.get("reason")) for p in self.net.payloads("leg_status", FOLLOWER)][-1],
        )

    def test_an_attempt_from_another_universe_generation_is_rejected(self) -> None:
        from dataclasses import replace

        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        attempt = self.last_attempt()
        self.deliver_attempt(replace(attempt, universe_generation=7))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "another universe_generation",
            [str(p.get("reason")) for p in self.net.payloads("leg_status", FOLLOWER)][-1],
        )

    def test_route_id_is_never_compared_with_recovery_epoch(self) -> None:
        import io
        import tokenize

        source = Path(pair_cell.__file__).read_text(encoding="utf-8")
        names: dict[int, set[str]] = {}
        for token in tokenize.generate_tokens(io.StringIO(source).readline):
            if token.type == tokenize.NAME:
                names.setdefault(token.start[0], set()).add(token.string)
        for line_number, line_names in names.items():
            self.assertFalse(
                "route_id" in line_names and "recovery_epoch" in line_names,
                f"line {line_number} mixes route identity with market recovery evidence",
            )
        # A recovery epoch that happens to spell the route_id changes nothing.
        self.prime()
        self.feed_quotes(epoch=ROUTE_ID, sequence=9)
        self.assertEqual(len(self.attempt_payloads()), 1)
        self.assertEqual(self.last_attempt().leader_quote.recovery_epoch, ROUTE_ID)


class RouteAuthorityTests(PairCellTestCase):
    def test_a_route_id_disagreement_removes_readiness_and_raises_reconciliation(self) -> None:
        self.prime()
        self.route_event(self.leader, route_id="route-other")
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertIn("route metadata reconciliation", result.ready_reason)
        self.assertIsNotNone(result.metadata_reconciliation)
        self.feed_quotes(sequence=5)
        self.assertEqual(self.attempt_payloads(), [])

    def test_a_role_disagreement_never_makes_a_worker_adopt_the_other_role(self) -> None:
        self.prime()
        self.route_event(self.leader, role="follower")
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.cell.assigned_role(), "leader")
        self.assertEqual(result.role, "leader")
        self.assertIn("different role", cast(str, result.metadata_reconciliation))

    def test_a_route_the_controller_no_longer_holds_removes_readiness(self) -> None:
        self.prime()
        self.route_event(self.follower, route_id=None, state=None)
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertIn("holds no route", cast(str, result.metadata_reconciliation))

    def test_a_metadata_disagreement_never_discards_or_infers_local_exposure(self) -> None:
        self.run_entry()
        self.tick()
        self.assertEqual(len(self.leader.mt5.positions), 1)
        closes_before = len(self.leader.close_requests())
        self.route_event(self.leader, route_id="route-other")
        self.tick()
        self.assertEqual(len(self.leader.mt5.positions), 1)
        self.assertEqual(len(self.leader.close_requests()), closes_before)
        self.assertEqual(
            self.leader.cell.handle_event(ClockTickEvent(self.now)).desired_state, "ACTIVE"
        )

    def test_agreement_restores_entry_readiness(self) -> None:
        self.prime()
        self.route_event(self.leader, route_id="route-other")
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)
        self.route_event(self.leader)
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)
        self.assertIsNone(result.metadata_reconciliation)

    def test_a_durable_route_copy_disagreeing_at_restart_is_reconciled_not_adopted(self) -> None:
        self.prime()
        self.leader.cell.close()
        self.leader.journal.close()
        self.leader.route = RouteAssignment(
            route_id="route-0002", role="leader", leader_worker_id=LEADER, follower_worker_id=FOLLOWER
        )
        self.leader.journal = FaultyJournal(
            self.tmp / f"{LEADER}-journal.db", self.trace, LEADER
        )
        self.leader.scheduler = DeadlineAwareTraderRpcScheduler()
        self.leader.cell = self.leader._build()
        self.net.register(LEADER, self.leader.cell)
        result = self.leader.cell.recover()
        self.assertFalse(result.ready)
        self.assertIsNotNone(result.metadata_reconciliation)
        self.assertEqual(result.route_id, "route-0002")


class UnpairingTests(PairCellTestCase):
    def test_entering_unpairing_immediately_removes_entry_readiness_on_both_sides(self) -> None:
        self.prime()
        self.unpairing()
        for side in (self.leader, self.follower):
            result = side.cell.handle_event(ClockTickEvent(self.now))
            self.assertFalse(result.ready)
            self.assertEqual(result.ready_reason, "the route is UNPAIRING")
            self.assertEqual(result.route_state, "UNPAIRING")

    def test_a_leader_in_unpairing_stops_candidate_evaluation_and_originates_nothing(self) -> None:
        self.prime()
        self.unpairing()
        self.feed_quotes()
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])
        self.assertEqual(self.leader.entry_requests(), [])

    def test_a_follower_in_unpairing_rejects_an_in_flight_attempt(self) -> None:
        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        self.route_event(self.follower, state="UNPAIRING")
        self.net.hold_kinds = set()
        self.net.release_held()
        self.net.pump()
        self.assertEqual(self.follower.entry_requests(), [])
        reasons = [str(p.get("reason")) for p in self.net.payloads("leg_status", FOLLOWER)]
        self.assertIn("the route is UNPAIRING", reasons)

    def test_unpairing_never_prevents_containment_of_existing_exposure(self) -> None:
        self.run_entry()
        self.tick()
        self.assertEqual(len(self.leader.mt5.positions), 1)
        self.unpairing()
        self.leader.cell.request_close("operator_unpair")
        self.follower.cell.request_close("operator_unpair")
        self.tick()
        self.net.pump()
        self.tick()
        self.assertTrue(self.leader.close_requests())
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])

    def test_a_safe_unpair_assertion_states_emptiness_and_terminality(self) -> None:
        self.prime()
        self.unpairing()
        assertion = self.leader.cell.safe_unpair_assertion()
        self.assertTrue(assertion.safe)
        self.assertEqual(assertion.route_id, ROUTE_ID)
        self.assertEqual(assertion.worker_id, LEADER)
        self.assertTrue(self.leader.cell.safe_unpair_assertion_is_current(assertion))
        self.assertEqual(assertion.state_version, self.leader.cell.state_version())

    def test_an_unsafe_worker_asserts_an_explicit_reason_rather_than_emptiness(self) -> None:
        self.run_entry()
        self.tick()
        self.unpairing()
        self.leader.cell.handle_event(
            self.leader.facts(self.now, positions=list(self.leader.mt5.positions))
        )
        assertion = self.leader.cell.safe_unpair_assertion()
        self.assertFalse(assertion.safe)
        self.assertIn("not broker-verified empty", assertion.reason)
        self.assertIn("attempt is unresolved", assertion.reason)

    def test_any_new_local_effect_advances_the_state_version_and_invalidates_it(self) -> None:
        self.prime()
        assertion = self.leader.cell.safe_unpair_assertion()
        self.assertTrue(assertion.safe)
        self.feed_quotes()
        self.assertGreater(self.leader.cell.state_version(), assertion.state_version)
        self.assertFalse(self.leader.cell.safe_unpair_assertion_is_current(assertion))
        self.assertFalse(self.leader.cell.safe_unpair_assertion().safe)

    def test_an_assertion_bound_to_another_route_is_never_current(self) -> None:
        from dataclasses import replace

        self.prime()
        assertion = self.leader.cell.safe_unpair_assertion()
        self.assertFalse(
            self.leader.cell.safe_unpair_assertion_is_current(replace(assertion, route_id="route-other"))
        )
        self.assertFalse(
            self.leader.cell.safe_unpair_assertion_is_current(
                replace(assertion, state_version=assertion.state_version - 1)
            )
        )
        self.assertFalse(
            self.leader.cell.safe_unpair_assertion_is_current(replace(assertion, worker_id=FOLLOWER))
        )

    def test_containment_advances_the_state_version_so_an_assertion_must_be_reproduced(self) -> None:
        self.run_entry()
        self.tick()
        self.unpairing()
        before = self.leader.cell.state_version()
        self.leader.cell.request_close("operator_unpair")
        self.tick()
        self.assertGreater(self.leader.cell.state_version(), before)

    def test_an_explicit_cancel_returns_the_route_to_normal_operation(self) -> None:
        self.prime()
        self.unpairing()
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)
        self.unpairing(state="ACTIVE")
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)
        self.assertEqual(result.route_state, "ACTIVE")
        self.feed_quotes()
        self.assertEqual(len(self.attempt_payloads()), 1)

    def test_the_unpairing_state_survives_restart_as_recovery_evidence(self) -> None:
        self.prime()
        self.unpairing()
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(result.route_state, "UNPAIRING")
        self.assertFalse(result.ready)

    def test_peer_session_loss_removes_readiness_without_removing_the_route(self) -> None:
        self.prime()
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertEqual(result.ready_reason, "authenticated peer session lost")
        self.assertEqual(result.route_id, ROUTE_ID)
        self.assertEqual(result.route_state, "ACTIVE")


# --------------------------------------------------------------------------- #
# Pairing Acceptance, policy construction, persistence, and the live guard
# --------------------------------------------------------------------------- #


class PairingAcceptanceTests(PairCellTestCase):
    def test_the_payload_carries_the_frozen_balance_and_four_authored_tunables(self) -> None:
        self.tick()
        acceptance = self.accept()
        self.assertEqual(acceptance.proposal_id, PROPOSAL)
        self.assertEqual(acceptance.worker_id, FOLLOWER)
        self.assertEqual(acceptance.startup_balance_usd, "4000")
        self.assertEqual(acceptance.account_currency, "USD")
        self.assertEqual(
            set(acceptance.canonical()),
            {
                "proposal_id",
                "worker_id",
                "startup_balance_usd",
                "account_currency",
                "maximum_margin_fraction",
                "daily_loss_fraction",
                "trade_loss_fraction",
                "maximum_loss_per_trade_usd",
            },
        )
        self.assertEqual(self.leader.cell.peer_pairing_acceptance(), acceptance)

    def test_only_the_follower_authors_a_pairing_acceptance_payload(self) -> None:
        self.tick()
        with self.assertRaises(PairExecutionCellError):
            self.leader.cell.freeze_pairing_acceptance(proposal_id=PROPOSAL)

    def test_the_leader_copies_the_payload_byte_for_byte_into_follower_risk(self) -> None:
        self.tick()
        acceptance = self.accept()
        self.feed_catalogs()
        policy = self.policy()
        self.assertTrue(policy.follower_risk.is_verbatim_copy_of(acceptance.risk_limits()))
        self.assertEqual(policy.follower_risk.strategy_budget_usd, "4000")
        self.leader.cell.accept_policy(policy)
        self.net.pump()
        self.assertTrue(self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_accepted)

    def test_a_leader_that_recomputes_or_clamps_the_follower_block_fails_closed(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        for rewritten in (
            WorkerRiskLimits(
                strategy_budget_usd="2000",
                maximum_margin_fraction="0.5",
                maximum_loss_per_trade_usd="150",
            ),
            WorkerRiskLimits(
                strategy_budget_usd="4000.0",
                maximum_margin_fraction="0.5",
                maximum_loss_per_trade_usd="150",
            ),
        ):
            rogue = StrategyPolicy(
                policy_version="policy-1",
                entry_edge_points="1",
                quote_max_age_seconds=2.0,
                quote_max_skew_seconds=1.0,
                leader_risk=self.leader.limits,
                follower_risk=rewritten,
            )
            with self.assertRaises(PairExecutionCellError) as raised:
                self.leader.cell.accept_policy(rogue)
            self.assertIn("verbatim", str(raised.exception))
        self.assertEqual(self.net.payloads("policy", LEADER), [])

    def test_a_leader_without_the_payload_publishes_no_policy(self) -> None:
        self.tick()
        self.feed_catalogs()
        acceptance = build_pairing_acceptance(
            proposal_id=PROPOSAL,
            worker_id=FOLLOWER,
            startup_balance_usd="4000",
            account_currency="USD",
        )
        policy = canonical_policy_from_acceptance(
            policy_version="policy-1", leader_risk=self.leader.limits, acceptance=acceptance
        )
        with self.assertRaises(PairExecutionCellError) as raised:
            self.leader.cell.accept_policy(policy)
        self.assertIn("Pairing Acceptance", str(raised.exception))
        self.assertEqual(self.net.payloads("policy", LEADER), [])

    def test_a_non_usd_or_non_positive_payload_is_never_constructible(self) -> None:
        for overrides in (
            {"account_currency": "EUR"},
            {"startup_balance_usd": "0"},
            {"startup_balance_usd": "-5"},
            {"maximum_margin_fraction": "0"},
        ):
            values: dict[str, object] = {
                "proposal_id": PROPOSAL,
                "worker_id": FOLLOWER,
                "startup_balance_usd": "4000",
                "account_currency": "USD",
                "maximum_margin_fraction": "0.5",
                "daily_loss_fraction": "0.03",
                "trade_loss_fraction": "0.02",
                "maximum_loss_per_trade_usd": "150",
            }
            values.update(overrides)
            with self.assertRaises(PairExecutionCellError):
                PairingAcceptance(**values)  # type: ignore[arg-type]

    def test_the_follower_refuses_a_policy_whose_block_is_not_its_own(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        rogue = StrategyPolicy(
            policy_version="policy-1",
            entry_edge_points="1",
            quote_max_age_seconds=2.0,
            quote_max_skew_seconds=1.0,
            leader_risk=self.leader.limits,
            follower_risk=WorkerRiskLimits(
                strategy_budget_usd="99000",
                maximum_margin_fraction="0.5",
                maximum_loss_per_trade_usd="150",
            ),
        )
        self.deliver_policy(rogue)
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.policy_accepted)
        self.assertEqual(self.follower.cell.sizing_plans(), {})
        acks = self.net.payloads("policy_ack", FOLLOWER)
        self.assertFalse(acks[-1]["accepted"])
        self.assertIn("verbatim copy", str(acks[-1]["reason"]))


class PolicyPersistenceTests(PairCellTestCase):
    def test_both_workers_persist_the_full_canonical_policy_content(self) -> None:
        self.prime()
        policy = cast(StrategyPolicy, self.accepted_policy)
        for side in (self.leader, self.follower):
            cell = side.restart()
            result = cell.recover()
            self.assertTrue(result.policy_accepted)
            self.assertEqual(result.policy_hash, policy.hash)
            self.assertEqual(cell.enforced_risk_limits(), policy.risk_of(side.role))  # type: ignore[arg-type]

    def test_a_restarted_follower_validates_attempts_without_re_requesting_policy(self) -> None:
        self.prime()
        policy = cast(StrategyPolicy, self.accepted_policy)
        acks_before = len(self.net.payloads("policy_ack", FOLLOWER))
        cell = self.follower.restart()
        # The durable content is enough on its own: the policy hash is known
        # before a single envelope is exchanged with the leader.
        result = cell.recover()
        self.assertTrue(result.policy_accepted)
        self.assertEqual(result.policy_hash, policy.hash)
        self.assertEqual(len(self.net.payloads("policy_ack", FOLLOWER)), acks_before)
        cell.handle_event(self.follower.facts(self.now))
        self.feed_quotes()
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_the_policy_hash_covers_the_frozen_startup_balance(self) -> None:
        first = build_pairing_acceptance(
            proposal_id=PROPOSAL,
            worker_id=FOLLOWER,
            startup_balance_usd="4000",
            account_currency="USD",
        )
        second = build_pairing_acceptance(
            proposal_id="proposal-2",
            worker_id=FOLLOWER,
            startup_balance_usd="4100",
            account_currency="USD",
        )
        one = canonical_policy_from_acceptance(
            policy_version="policy-1", leader_risk=LEADER_RISK, acceptance=first
        )
        two = canonical_policy_from_acceptance(
            policy_version="policy-1", leader_risk=LEADER_RISK, acceptance=second
        )
        self.assertNotEqual(one.hash, two.hash)

    def test_an_intraday_balance_change_never_resizes_a_live_pairing(self) -> None:
        self.prime()
        before = cast(StrategyPolicy, self.accepted_policy).hash
        self.follower.mt5.default_margin = "1000"
        self.tick(seconds=61 * 60)
        self.feed_quotes(sequence=3)
        self.assertEqual(
            self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_hash, before
        )
        self.assertEqual(
            self.follower.cell.enforced_risk_limits().strategy_budget_usd, "4000"  # type: ignore[union-attr]
        )

    def test_policy_change_requires_both_accounts_to_be_broker_verified_empty(self) -> None:
        self.run_entry()
        with self.assertRaises(PairExecutionCellError):
            self.leader.cell.accept_policy(self.policy(entry_edge_points="9"))

    def test_a_new_policy_hash_is_accepted_while_both_are_empty(self) -> None:
        self.prime()
        changed = self.policy(entry_edge_points="9")
        self.leader.cell.accept_policy(changed)
        self.net.pump()
        self.assertEqual(
            self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_hash, changed.hash
        )

    def test_a_policy_whose_hash_does_not_match_its_content_is_refused(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.follower.cell.handle_event(
            RelayEnvelopeReceived(
                {
                    "protocol_version": pair_cell.PROTOCOL_VERSION,
                    "kind": "policy",
                    "route_id": ROUTE_ID,
                    "from_worker_id": LEADER,
                    "from_role": "leader",
                    "to_worker_id": FOLLOWER,
                    "payload": {
                        "policy": self.policy().canonical(),
                        "policy_hash": "0" * 64,
                        "leader_broker_verified_empty": True,
                    },
                }
            )
        )
        self.assertFalse(self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_accepted)

    def test_an_envelope_from_an_unauthorized_worker_or_version_is_ignored(self) -> None:
        self.prime()
        policy = cast(StrategyPolicy, self.accepted_policy)
        rogue = self.policy(entry_edge_points="77")
        for override in ({"from_worker_id": "worker-intruder"}, {"protocol_version": 99}):
            envelope: dict[str, object] = {
                "protocol_version": pair_cell.PROTOCOL_VERSION,
                "kind": "policy",
                "route_id": ROUTE_ID,
                "from_worker_id": LEADER,
                "from_role": "leader",
                "to_worker_id": FOLLOWER,
                "payload": {
                    "policy": rogue.canonical(),
                    "policy_hash": rogue.hash,
                    "leader_broker_verified_empty": True,
                },
            }
            envelope.update(override)
            self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
            self.assertEqual(
                self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_hash, policy.hash
            )


class ModeDisagreementTests(PairCellTestCase):
    follower_allow_live = False

    def test_a_follower_with_allow_live_false_refuses_a_live_policy_and_stays_idle(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        live = self.policy()
        self.assertEqual(live.mode, "live")
        self.leader.cell.accept_policy(live)
        self.net.pump()
        self.tick()
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.policy_accepted)
        self.assertFalse(result.ready)
        self.assertIn("allow_live is false", result.ready_reason)
        self.assertEqual(result.route_id, ROUTE_ID)  # still paired
        acks = self.net.payloads("policy_ack", FOLLOWER)
        self.assertFalse(acks[-1]["accepted"])

    def test_the_leader_treats_the_refusal_as_peer_not_ready_and_originates_nothing(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.leader.cell.accept_policy(self.policy())
        self.net.pump()
        self.tick()
        self.warm(SYMBOL)
        self.feed_quotes()
        self.assertEqual(self.attempt_payloads(), [])
        self.assertEqual(self.leader.entry_requests(), [])

    def test_publishing_a_shadow_policy_while_both_are_empty_resolves_it(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.leader.cell.accept_policy(self.policy())
        self.net.pump()
        self.tick()
        shadow = self.policy(mode="shadow")
        self.leader.cell.accept_policy(shadow)
        self.net.pump()
        self.tick()
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.policy_accepted)
        self.assertEqual(result.policy_hash, shadow.hash)

    def test_the_follower_never_rewrites_the_policy_to_shadow_by_itself(self) -> None:
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.leader.cell.accept_policy(self.policy())
        self.net.pump()
        self.tick()
        self.assertIsNone(self.follower.cell.handle_event(ClockTickEvent(self.now)).policy_hash)
        self.assertEqual(self.follower.cell.sizing_plans(), {})


# --------------------------------------------------------------------------- #
# Continuous remaining-allowed-leg-loss summaries
# --------------------------------------------------------------------------- #


class RemainingAllowanceTests(PairCellTestCase):
    def test_each_worker_publishes_a_versioned_summary_before_any_signal(self) -> None:
        self.prime()
        summaries = self.net.payloads("remaining_allowance", FOLLOWER)
        self.assertTrue(summaries)
        self.assertEqual(summaries[-1]["worker_id"], FOLLOWER)
        self.assertEqual(Decimal(str(summaries[-1]["allowed_leg_loss_usd"])), Decimal("80"))
        versions = [int(cast(int, s["version"])) for s in summaries]
        self.assertEqual(versions, sorted(versions))
        first_attempt = next(
            (index for index, e in enumerate(self.net.sent) if e["kind"] == "attempt"), None
        )
        self.assertIsNone(first_attempt, "no attempt exists yet")
        self.feed_quotes()
        attempt_index = next(
            index for index, e in enumerate(self.net.sent) if e["kind"] == "attempt"
        )
        summary_index = next(
            index
            for index, e in enumerate(self.net.sent)
            if e["kind"] == "remaining_allowance" and e["from_worker_id"] == FOLLOWER
        )
        self.assertLess(summary_index, attempt_index)

    def test_the_leader_cannot_create_an_attempt_without_a_current_peer_summary(self) -> None:
        self.net.drop_kinds.add("remaining_allowance")
        self.prime()
        self.assertIsNone(self.leader.cell.peer_remaining_allowance())
        self.feed_quotes()
        self.assertEqual(self.attempt_payloads(), [])
        self.assertEqual(self.leader.entry_requests(), [])

    def test_the_attempt_freezes_both_summary_versions_and_assigned_losses(self) -> None:
        self.prime()
        peer = self.leader.cell.peer_remaining_allowance()
        self.assertIsNotNone(peer)
        self.feed_quotes()
        attempt = self.last_attempt()
        self.assertEqual(attempt.follower_allowance_version, cast(int, peer).version)
        self.assertEqual(
            attempt.leader_allowance_version,
            self.leader.cell.remaining_allowance_summary().version,
        )
        self.assertEqual(Decimal(attempt.leader_allowed_loss_usd), Decimal("150"))
        self.assertEqual(Decimal(attempt.follower_allowed_loss_usd), Decimal("80"))

    def test_a_peer_claiming_more_than_its_canonical_cap_is_never_assigned_more(self) -> None:
        self.prime()
        forged = {
            "protocol_version": pair_cell.PROTOCOL_VERSION,
            "kind": "remaining_allowance",
            "route_id": ROUTE_ID,
            "from_worker_id": FOLLOWER,
            "from_role": "follower",
            "to_worker_id": LEADER,
            "payload": {"worker_id": FOLLOWER, "version": 999, "allowed_leg_loss_usd": "5000"},
        }
        self.leader.cell.handle_event(RelayEnvelopeReceived(forged))
        self.feed_quotes()
        self.assertEqual(Decimal(self.last_attempt().follower_allowed_loss_usd), Decimal("80"))

    def test_a_lower_published_allowance_lowers_the_assigned_leg_loss(self) -> None:
        self.prime()
        self.follower.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-90", closed_at=self.now)
        )
        self.tick()
        self.assertEqual(self.follower.cell.allowed_leg_loss_usd(), Decimal("30"))
        self.feed_quotes()
        self.assertEqual(Decimal(self.last_attempt().follower_allowed_loss_usd), Decimal("30"))

    def test_the_follower_rejects_an_over_assignment_against_its_current_allowance(self) -> None:
        from dataclasses import replace

        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        attempt = self.last_attempt()
        # The assignment matched the summary the leader used, but the
        # follower's own current allowance has since shrunk.
        self.follower.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-60", closed_at=self.now)
        )
        self.deliver_attempt(replace(attempt, follower_allowed_loss_usd="80"))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "exceeds this Worker's current local remaining allowance",
            " ".join(str(row["detail"]) for row in self.follower.cell.transition_history()),
        )

    def test_the_over_assignment_re_check_needs_no_quote_or_edge_evaluation(self) -> None:
        from dataclasses import replace

        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        attempt = self.last_attempt()
        # Every quote the follower holds is now far beyond quote_max_age.
        self.now += timedelta(seconds=600)
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.deliver_attempt(replace(attempt, follower_allowed_loss_usd="5000"))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "exceeds this Worker's current local remaining allowance",
            " ".join(str(row["detail"]) for row in self.follower.cell.transition_history()),
        )


# --------------------------------------------------------------------------- #
# Sizing and emergency-protection plan timing, literals, and retention
# --------------------------------------------------------------------------- #


class SizingTimingTests(PairCellTestCase):
    def test_no_plan_exists_before_discovery_policy_and_a_current_quote(self) -> None:
        self.tick()
        self.assertEqual(self.leader.cell.sizing_plans(), {})
        self.accept()
        self.feed_catalogs()
        self.assertEqual(self.leader.cell.sizing_plans(), {}, "discovery alone builds no plan")
        self.leader.cell.accept_policy(self.policy())
        self.net.pump()
        self.tick()
        self.assertEqual(
            self.leader.cell.sizing_plans(), {}, "policy acceptance alone builds no plan"
        )
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.warm(SYMBOL)
        self.assertTrue(self.leader.cell.sizing_plans())

    def test_no_candidate_is_evaluated_while_a_valid_positive_plan_is_missing(self) -> None:
        self.leader.mt5.margin_unavailable.add(SYMBOL)
        self.tick()
        self.accept()
        self.feed_catalogs()
        self.leader.cell.accept_policy(self.policy())
        self.net.pump()
        self.tick()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertIn("no discovered product has a valid sizing plan", result.ready_reason)
        self.feed_quotes()
        self.assertEqual(self.leader.cell.sizing_plans(), {})
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_the_initial_plan_uses_a_current_quote_and_a_current_mt5_margin_call(self) -> None:
        self.prime()
        self.assertTrue(self.leader.mt5.margin_requests)
        self.assertIn(SYMBOL, self.leader.mt5.symbol_info_calls)
        long_call = next(r for r in self.leader.mt5.margin_requests if r["direction"] == "LONG")
        short_call = next(r for r in self.leader.mt5.margin_requests if r["direction"] == "SHORT")
        self.assertEqual(long_call["price"], "1.10010")  # the current local ask
        self.assertEqual(short_call["price"], "1.10000")  # the current local bid
        self.assertEqual(long_call["volume"], "1")

    def test_local_max_lots_use_known_margin_and_budget_literals(self) -> None:
        self.prime()
        product = self.product_id()
        leader_plan = self.leader.cell.sizing_plans()[(product, "LONG")]
        follower_plan = self.follower.cell.sizing_plans()[(product, "SHORT")]
        # 10000 * 0.5 / 1000 = 5 lots; 4000 * 0.5 / 1000 = 2 lots.
        self.assertEqual(Decimal(leader_plan.local_max_lots), Decimal("5"))
        self.assertEqual(Decimal(follower_plan.local_max_lots), Decimal("2"))
        self.assertEqual(leader_plan.filling_mode, "FOK")
        self.assertEqual(leader_plan.universe_generation, 1)
        self.assertEqual(leader_plan.canonical_point, "0.00001")

    def test_pair_lots_use_the_lower_worker_capacity_on_the_shared_step(self) -> None:
        self.run_entry()
        attempt = self.last_attempt()
        self.assertEqual(Decimal(attempt.lots), Decimal("2"))
        self.assertEqual(self.leader.entry_requests()[0]["volume"], attempt.lots)
        self.assertEqual(self.follower.entry_requests()[0]["volume"], attempt.lots)

    def test_volume_step_snapping_uses_the_shared_grid(self) -> None:
        self.assertEqual(
            floor_to_volume_step(Decimal("2.037"), Decimal("0.01"), Decimal("50"), Decimal("0.05")),
            Decimal("2.01"),
        )
        self.assertIsNone(
            floor_to_volume_step(Decimal("0.004"), Decimal("0.01"), Decimal("50"), Decimal("0.01"))
        )

    def test_the_common_volume_maximum_caps_the_local_plan(self) -> None:
        self.leader.mt5.symbol_facts[SYMBOL] = {"volume_max": "3"}
        self.prime()
        product = self.product_id()
        self.assertEqual(
            Decimal(self.leader.cell.sizing_plans()[(product, "LONG")].local_max_lots), Decimal("3")
        )

    def test_a_failed_product_refresh_suspends_only_that_product(self) -> None:
        self.add_second_product()
        self.leader.mt5.symbol_unavailable.add(OTHER_SYMBOL)
        self.prime()
        self.warm(OTHER_SYMBOL)
        product, other = self.product_id(), self.product_id(OTHER_SYMBOL)
        self.assertEqual(self.leader.cell.eligible_products(), (product,))
        self.assertIn(other, self.leader.cell.suspended_products())

    def test_an_hourly_refresh_restores_a_previously_suspended_product(self) -> None:
        self.add_second_product()
        self.leader.mt5.symbol_unavailable.add(OTHER_SYMBOL)
        self.prime()
        self.warm(OTHER_SYMBOL)
        product, other = self.product_id(), self.product_id(OTHER_SYMBOL)
        self.assertEqual(self.leader.cell.eligible_products(), (product,))
        self.leader.mt5.symbol_unavailable.discard(OTHER_SYMBOL)
        self.tick(seconds=59 * 60)
        self.assertEqual(self.leader.cell.eligible_products(), (product,))
        self.now += timedelta(seconds=61 * 60)
        self.warm(SYMBOL, sequence=70)
        self.warm(OTHER_SYMBOL, sequence=70)
        self.assertEqual(sorted(self.leader.cell.eligible_products()), sorted((product, other)))

    def test_no_positive_plan_makes_the_product_ineligible(self) -> None:
        self.leader.mt5.default_margin = "10000000"
        self.prime()
        self.assertEqual(self.leader.cell.eligible_products(), ())
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)

    def test_an_unavailable_margin_calculation_removes_only_that_product(self) -> None:
        self.leader.mt5.margin_unavailable.add(SYMBOL)
        self.prime()
        self.assertEqual(self.leader.cell.eligible_products(), ())
        self.assertIn(
            "no direction has a valid positive sizing plan",
            self.leader.cell.suspended_products()[self.product_id()],
        )


class PlanRetentionTests(PairCellTestCase):
    def setUp(self) -> None:
        super().setUp()
        self._sequence = 50

    def _refresh_follower_plans(self, *, seconds: float, margin: str | None = None) -> None:
        """Advance past the hourly boundary with a fresh quote in hand."""

        if margin is not None:
            self.follower.mt5.default_margin = margin
        self._sequence += 1
        self.now += timedelta(seconds=seconds)
        self.follower.cell.handle_event(
            LocalQuoteEvent(
                product_id=self.product_id(),
                symbol=SYMBOL,
                bid=Decimal("1.10000"),
                ask=Decimal("1.10010"),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-1",
                sequence=self._sequence,
                calibration=CALIBRATED,
            )
        )

    def test_the_retention_window_is_the_timeout_plus_the_relay_handling_window(self) -> None:
        self.prime()
        policy = cast(StrategyPolicy, self.accepted_policy)
        self.assertEqual(pair_cell.RELAY_HANDLING_WINDOW_SECONDS, 5.0)
        self.assertEqual(policy.plan_retention_seconds, 10.0)

    def test_an_attempt_in_flight_across_a_refresh_validates_against_its_version(self) -> None:
        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        named = self.last_attempt().follower_plan_version
        self._refresh_follower_plans(seconds=61 * 60, margin="2000")
        current = self.follower.cell.sizing_plans()[(self.product_id(), "SHORT")].version
        self.assertNotEqual(named, current, "the hourly refresh really did supersede it")
        self.assertIn(named, self.follower.cell.retained_plan_versions())
        self.net.hold_kinds = set()
        self.net.release_held()
        self.net.pump()
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertEqual(self.follower.entry_requests()[0]["volume"], "2")

    def test_a_retained_version_is_never_eligible_for_a_new_candidate(self) -> None:
        self.prime()
        product = self.product_id()
        before = self.follower.cell.sizing_plans()[(product, "SHORT")].version
        self._refresh_follower_plans(seconds=61 * 60, margin="2000")
        self.tick()
        self.net.pump()
        self.assertIn(before, self.follower.cell.retained_plan_versions())
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.feed_quotes(sequence=60)
        attempt = self.last_attempt()
        self.assertNotEqual(attempt.follower_plan_version, before)
        self.assertEqual(
            attempt.follower_plan_version,
            self.follower.cell.sizing_plans()[(product, "SHORT")].version,
        )

    def test_a_retained_version_expires_once_the_window_passes_unreferenced(self) -> None:
        from dataclasses import replace

        self.net.hold_kinds = {"attempt"}
        self.run_entry()
        named = self.last_attempt().follower_plan_version
        self._refresh_follower_plans(seconds=61 * 60, margin="2000")
        self.assertIn(named, self.follower.cell.retained_plan_versions())
        self._refresh_follower_plans(seconds=61 * 60)
        self.assertNotIn(named, self.follower.cell.retained_plan_versions())
        self.deliver_attempt(replace(self.last_attempt(), attempt_id="attempt-late"))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "sizing-plan version is neither current nor a retained superseded version",
            [str(row["detail"]) for row in self.follower.cell.transition_history()],
        )


# --------------------------------------------------------------------------- #
# Per-leg loss policy
# --------------------------------------------------------------------------- #


class LossPolicyTests(PairCellTestCase):
    def test_allowed_leg_loss_is_the_smallest_of_three_limits(self) -> None:
        self.prime()
        # min(2% * 10000 = 200, maximum 150, remaining daily 300) = 150.
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("150"))
        # min(2% * 4000 = 80, maximum 150, remaining daily 120) = 80.
        self.assertEqual(self.follower.cell.allowed_leg_loss_usd(), Decimal("80"))
        self.assertEqual(self.leader.cell.enforced_risk_limits(), LEADER_RISK)
        self.assertEqual(self.follower.cell.enforced_risk_limits(), FOLLOWER_RISK)

    def test_maximum_loss_per_trade_is_an_admission_cap_on_the_leg_loss(self) -> None:
        limits = WorkerRiskLimits(
            strategy_budget_usd="10000",
            maximum_margin_fraction="0.5",
            maximum_loss_per_trade_usd="40",
        )
        # 2% of 10000 is 200; the hard cap of 40 wins before any order is sent.
        self.assertEqual(limits.leg_loss_cap_usd, Decimal("40"))

    def test_rough_protection_targets_the_computed_allowance_not_a_constant(self) -> None:
        self.prime()
        self.follower.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-90", closed_at=self.now)
        )
        self.tick()
        self.feed_quotes()
        attempt = self.last_attempt()
        self.assertEqual(Decimal(attempt.follower_allowed_loss_usd), Decimal("30"))
        # 30 USD over 2 lots at 1 USD/tick is 15 ticks of 0.00001.
        self.assertEqual(
            Decimal(attempt.follower_rough_sl), Decimal("1.10050") + Decimal("0.00015")
        )

    def test_remaining_daily_allowance_caps_the_leg_loss(self) -> None:
        self.prime()
        self.leader.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-220", closed_at=self.now)
        )
        self.assertEqual(self.leader.cell.accumulated_realized_loss_usd(), Decimal("220"))
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("80"))

    def test_realized_profit_offsets_realized_loss_in_the_same_new_york_day(self) -> None:
        self.prime()
        self.leader.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-220", closed_at=self.now)
        )
        self.leader.cell.handle_event(
            RealizedPnLEvent(attempt_id="a2", realized_usd="120", closed_at=self.now)
        )
        self.assertEqual(self.leader.cell.accumulated_realized_loss_usd(), Decimal("100"))
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("150"))

    def test_exhausted_daily_allowance_blocks_entry(self) -> None:
        self.prime()
        self.leader.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-300", closed_at=self.now)
        )
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertIn("daily realized-loss allowance", result.ready_reason)
        self.feed_quotes()
        self.assertEqual(self.attempt_payloads(), [])

    def test_daily_realized_loss_resets_at_the_new_york_calendar_boundary(self) -> None:
        self.prime()
        self.leader.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-300", closed_at=self.now)
        )
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("0"))
        # 2026-03-04 14:00Z is 09:00 New York; +16h crosses midnight in New York.
        self.now += timedelta(hours=16)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.cell.accumulated_realized_loss_usd(), Decimal("0"))
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("150"))

    def test_floating_pnl_is_never_accumulated(self) -> None:
        self.prime()
        self.leader.cell.handle_event(
            self.leader.facts(
                self.now,
                positions=[{"ticket": 1, "symbol": SYMBOL, "type": 0, "volume": 2.0, "profit": -900.0}],
            )
        )
        self.assertEqual(self.leader.cell.accumulated_realized_loss_usd(), Decimal("0"))
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("150"))

    def test_realized_events_are_deduplicated(self) -> None:
        self.prime()
        for _ in range(3):
            self.leader.cell.handle_event(
                RealizedPnLEvent(attempt_id="a1", realized_usd="-100", closed_at=self.now)
            )
        self.assertEqual(self.leader.cell.accumulated_realized_loss_usd(), Decimal("100"))

    def test_no_sizing_capacity_exists_before_a_canonical_policy_is_accepted(self) -> None:
        self.tick()
        self.assertIsNone(self.leader.cell.enforced_risk_limits())
        self.assertEqual(self.leader.cell.sizing_plans(), {})
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("0"))

    def test_local_risk_drift_after_acceptance_removes_readiness_loudly(self) -> None:
        self.prime()
        self.assertTrue(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)
        self.leader.cell.close()
        self.leader.limits = WorkerRiskLimits(
            strategy_budget_usd="20000",
            maximum_margin_fraction="0.5",
            maximum_loss_per_trade_usd="150",
        )
        self.leader.cell = self.leader._build()
        self.net.register(LEADER, self.leader.cell)
        result = self.leader.cell.recover()
        self.assertFalse(result.ready)
        self.assertIn("does not match the accepted canonical policy", result.ready_reason)
        self.assertEqual(self.leader.cell.allowed_leg_loss_usd(), Decimal("0"))

    def test_numeric_equality_is_not_treated_as_configuration_drift(self) -> None:
        self.prime()
        self.leader.cell.close()
        self.leader.limits = WorkerRiskLimits(
            strategy_budget_usd="10000.00",
            maximum_margin_fraction="0.50",
            maximum_loss_per_trade_usd="150.0",
            daily_loss_fraction="0.030",
            trade_loss_fraction="0.020",
        )
        self.leader.cell = self.leader._build()
        self.net.register(LEADER, self.leader.cell)
        self.leader.cell.recover()
        self.leader.cell.handle_event(self.leader.facts(self.now))
        self.assertTrue(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)


# --------------------------------------------------------------------------- #
# Leader candidate admission and ranking
# --------------------------------------------------------------------------- #


class CandidateAdmissionTests(PairCellTestCase):
    def test_candidates_are_ranked_by_conservative_expected_edge_usd(self) -> None:
        self.add_second_product()
        self.prime()
        self.warm(OTHER_SYMBOL)
        # Suspend entry origination so both products stay observable candidates.
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.feed_quotes(symbol=OTHER_SYMBOL, leader_ask="1.10010", follower_bid="1.10090")
        self.feed_quotes(symbol=SYMBOL)
        candidates = self.leader.cell.entry_candidates()
        self.assertEqual(self.attempt_payloads(), [])
        self.assertEqual(candidates[0]["product_id"], self.product_id(OTHER_SYMBOL))
        self.assertEqual(candidates[0]["leader_direction"], "LONG")
        self.assertEqual(Decimal(str(candidates[0]["expected_edge_usd"])), Decimal("160"))
        self.assertEqual(Decimal(str(candidates[1]["expected_edge_usd"])), Decimal("80"))

    def test_the_canonical_execution_point_governs_the_edge_threshold(self) -> None:
        # The coarser broker's point is the canonical one, so a 40-point edge
        # on the finer broker is only 4 points against the canonical point.
        self.follower_entries = [_entry(point="0.0001")]
        self.prime(shared={"entry_edge_points": "10"})
        product = self.product_id()
        self.assertEqual(
            self.leader.cell.discovered_universe().product(product).canonical_point,  # type: ignore[union-attr]
            "0.0001",
        )
        self.feed_quotes()
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_a_fresh_coherent_quote_pair_creates_exactly_one_attempt(self) -> None:
        self.prime()
        self.feed_quotes()
        self.assertEqual(len(self.attempt_payloads()), 1)
        self.assertIn(
            "attempt_created", [row["event"] for row in self.leader.cell.transition_history()]
        )

    def test_one_decision_quote_revision_creates_at_most_one_attempt(self) -> None:
        self.prime()
        self.leader.mt5.reject = True
        self.follower.mt5.reject = True
        self.feed_quotes()
        self.net.pump()
        for _ in range(3):
            self.leader.cell.handle_event(ClockTickEvent(self.now))
            self.net.pump()
        self.assertEqual(len(self.attempt_payloads()), 1)

    def test_an_excessively_stale_quote_pair_is_rejected(self) -> None:
        self.prime(shared={"quote_max_age_seconds": 0.5})
        product = self.product_id()
        self.follower.cell.handle_event(
            LocalQuoteEvent(
                product_id=product,
                symbol=SYMBOL,
                bid=Decimal("1.10050"),
                ask=Decimal("1.10060"),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-1",
                sequence=2,
            )
        )
        self.now += timedelta(seconds=5)
        self.leader.cell.handle_event(
            LocalQuoteEvent(
                product_id=product,
                symbol=SYMBOL,
                bid=Decimal("1.10000"),
                ask=Decimal("1.10010"),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-1",
                sequence=3,
            )
        )
        self.net.pump()
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_excessive_calibrated_broker_time_skew_is_rejected(self) -> None:
        self.prime(shared={"quote_max_skew_seconds": 0.25})
        self.feed_quotes(follower_broker_time=self.now - timedelta(seconds=3))
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_out_of_order_sequences_and_previous_epochs_are_ignored(self) -> None:
        self.prime()
        self.feed_quotes(sequence=5, epoch="epoch-2")
        self.leader.cell.handle_event(
            LocalQuoteEvent(
                product_id=self.product_id(),
                symbol=SYMBOL,
                bid=Decimal("9.00000"),
                ask=Decimal("9.00010"),
                broker_time=self.now,
                local_receive_time=self.now,
                recovery_epoch="epoch-1",
                sequence=99,
            )
        )
        self.assertEqual(self.last_attempt().leader_quote.ask, Decimal("1.10010"))

    def test_edge_below_the_threshold_is_not_a_candidate(self) -> None:
        self.prime(shared={"entry_edge_points": "100"})
        self.feed_quotes()
        self.assertEqual(self.attempt_payloads(), [])


# --------------------------------------------------------------------------- #
# Immediate entry protocol
# --------------------------------------------------------------------------- #


class ImmediateEntryTests(PairCellTestCase):
    def test_leader_persists_and_prepares_before_dispatch_and_never_waits(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        # The follower has not seen anything yet, but the leader already sent.
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertEqual(len(self.leader.entry_requests()), 1)
        relay_attempt = [
            index for index, (kind, name) in enumerate(self.trace) if kind == "relay" and name.endswith(":attempt")
        ]
        prepare_index = [
            index
            for index, (kind, name) in enumerate(self.trace)
            if kind == "prepare" and name.startswith(LEADER) and name.endswith(":entry")
        ]
        order_index = [
            index
            for index, (kind, name) in enumerate(self.trace)
            if kind == "order_send" and name == f"{LEADER}:market"
        ]
        self.assertTrue(prepare_index)
        self.assertLess(prepare_index[0], relay_attempt[0], "the effect is prepared before dispatch")
        self.assertLess(relay_attempt[0], order_index[0], "the attempt is relayed concurrently with the send")
        self.assertEqual(len(relay_attempt), 1, "exactly one immutable attempt is emitted")

    def test_attempt_carries_the_full_immutable_contract(self) -> None:
        self.run_entry()
        attempt = self.last_attempt()
        policy = cast(StrategyPolicy, self.accepted_policy)
        self.assertEqual(attempt.policy_hash, policy.hash)
        self.assertEqual(attempt.route_id, ROUTE_ID)
        self.assertEqual(attempt.universe_generation, 1)
        self.assertEqual(attempt.product_id, self.product_id())
        self.assertEqual(attempt.symbol, SYMBOL)
        self.assertEqual(attempt.leader_direction, "LONG")
        self.assertEqual(attempt.follower_direction, "SHORT")
        self.assertEqual(Decimal(attempt.leader_allowed_loss_usd), Decimal("150"))
        self.assertEqual(Decimal(attempt.follower_allowed_loss_usd), Decimal("80"))
        self.assertEqual(attempt.confirmation_timeout_seconds, 5.0)
        self.assertEqual(attempt.decision_time, self.now)
        plans = self.leader.cell.sizing_plans()
        self.assertEqual(attempt.leader_plan_version, plans[(self.product_id(), "LONG")].version)

    def test_every_initial_order_carries_rough_protection(self) -> None:
        self.run_entry()
        attempt = self.last_attempt()
        leader_request = self.leader.entry_requests()[0]
        follower_request = self.follower.entry_requests()[0]
        self.assertEqual(leader_request["sl"], attempt.leader_rough_sl)
        self.assertEqual(leader_request["tp"], attempt.leader_rough_tp)
        self.assertEqual(follower_request["sl"], attempt.follower_rough_sl)
        self.assertEqual(follower_request["tp"], attempt.follower_rough_tp)
        # 150 USD over 2 lots at 1 USD/tick is 75 ticks of 0.00001.
        self.assertEqual(Decimal(attempt.leader_rough_sl), Decimal("1.10010") - Decimal("0.00075"))
        self.assertEqual(Decimal(attempt.leader_rough_tp), Decimal("1.10010") + Decimal("0.00075"))

    def test_both_legs_reach_active_with_precise_one_to_one_protection(self) -> None:
        self.run_entry()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        self.assertEqual(self.follower.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        leader_modify = self.leader.modify_requests()[-1]
        # Precise protection uses the real leader fill of 1.10010 and 150 USD.
        self.assertEqual(Decimal(str(leader_modify["sl"])), Decimal("1.09935"))
        self.assertEqual(Decimal(str(leader_modify["tp"])), Decimal("1.10085"))
        follower_modify = self.follower.modify_requests()[-1]
        # The follower filled at 1.10040 with an 80 USD allowance (40 ticks).
        self.assertEqual(Decimal(str(follower_modify["sl"])), Decimal("1.10080"))
        self.assertEqual(Decimal(str(follower_modify["tp"])), Decimal("1.10000"))

    def test_precise_protection_is_applied_only_after_pair_confirmation(self) -> None:
        self.prime()
        self.net.hold_kinds.add("leg_status")
        self.feed_quotes()
        self.net.pump()
        self.assertEqual(self.leader.modify_requests(), [])
        self.assertEqual(self.follower.modify_requests(), [])
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).pair_confirmed)
        self.net.hold_kinds.discard("leg_status")
        self.net.release_held()
        self.net.pump()
        self.assertTrue(self.leader.cell.handle_event(ClockTickEvent(self.now)).pair_confirmed)
        self.assertEqual(len(self.leader.modify_requests()), 1)
        self.assertEqual(len(self.net.payloads("pair_entry_confirmed", LEADER)), 1)


# --------------------------------------------------------------------------- #
# Follower admission
# --------------------------------------------------------------------------- #


class FollowerAdmissionTests(PairCellTestCase):
    def _dispatch_attempt(self, **overrides: object) -> dict[str, object]:
        """Build one syntactically valid attempt envelope with overrides."""

        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        envelope = next(e for e in self.net.sent if e["kind"] == "attempt")
        payload = {"attempt": dict(dict(envelope["payload"])["attempt"])}  # type: ignore[index]
        payload["attempt"].update(overrides)  # type: ignore[union-attr]
        return {
            "protocol_version": pair_cell.PROTOCOL_VERSION,
            "kind": "attempt",
            "route_id": ROUTE_ID,
            "from_worker_id": LEADER,
            "from_role": "leader",
            "to_worker_id": FOLLOWER,
            "payload": payload,
        }

    def _details(self) -> str:
        return " ".join(str(row["detail"]) for row in self.follower.cell.transition_history())

    def test_follower_enters_without_revalidating_quotes_or_edge(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        # By delivery time the follower's own quotes are far past
        # ``quote_max_age_seconds`` and the edge no longer exists; it still
        # enters immediately after non-market admission.
        self.now += timedelta(seconds=120)
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.net.hold_kinds.discard("attempt")
        self.net.release_held()
        self.net.pump()
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertEqual(self.follower.entry_requests()[0]["symbol"], SYMBOL)

    def test_same_content_duplicate_delivery_never_sends_twice(self) -> None:
        self.run_entry()
        envelope = next(e for e in self.net.sent if e["kind"] == "attempt")
        self.follower.cell.handle_event(RelayEnvelopeReceived(dict(envelope)))
        self.follower.cell.handle_event(RelayEnvelopeReceived(dict(envelope)))
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertIn(
            "attempt_duplicate_ignored",
            [row["event"] for row in self.follower.cell.transition_history()],
        )

    def test_attempt_id_reused_with_changed_content_is_rejected(self) -> None:
        self.run_entry()
        envelope = next(e for e in self.net.sent if e["kind"] == "attempt")
        mutated = dict(envelope)
        payload = dict(mutated["payload"])  # type: ignore[arg-type]
        attempt = dict(payload["attempt"])  # type: ignore[arg-type]
        attempt["lots"] = "4"
        payload["attempt"] = attempt
        mutated["payload"] = payload
        self.follower.cell.handle_event(RelayEnvelopeReceived(mutated))
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertIn("attempt ID reused with different content", self._details())

    def test_policy_hash_mismatch_rejects_without_a_broker_send(self) -> None:
        envelope = self._dispatch_attempt(policy_hash="0" * 64)
        self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn("policy hash is not the exact locally accepted hash", self._details())

    def test_a_stale_sizing_plan_version_rejects_without_a_broker_send(self) -> None:
        envelope = self._dispatch_attempt(follower_plan_version="stale")
        self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "sizing-plan version is neither current nor a retained superseded version",
            self._details(),
        )

    def test_lots_differing_from_the_named_pair_plan_reject(self) -> None:
        envelope = self._dispatch_attempt(lots="1")
        self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn("common lots differ from the pair plan", self._details())

    def test_an_unknown_product_identity_rejects_without_a_broker_send(self) -> None:
        envelope = self._dispatch_attempt(product_id="EURUSD:deadbeefdeadbeef")
        self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn("not locally eligible", self._details())

    def test_invalid_rough_protection_rejects_without_a_broker_send(self) -> None:
        # A SHORT leg requires take profit < entry < stop loss; this is inverted.
        envelope = self._dispatch_attempt(follower_rough_sl="1.10000", follower_rough_tp="1.10100")
        self.follower.cell.handle_event(RelayEnvelopeReceived(envelope))
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "valid rough emergency protection cannot be attached to the initial order",
            self._details(),
        )

    def test_a_quarantined_product_rejects_without_a_broker_send(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.follower.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        envelope = next(e for e in self.net.sent if e["kind"] == "attempt")
        self.net.hold_kinds.discard("attempt")
        self.net.release_held()
        self.net.pump()
        self.assertEqual(self.follower.cell.quarantined_products(), (self.product_id(),))
        replay = dict(envelope)
        payload = {"attempt": dict(dict(envelope["payload"])["attempt"])}  # type: ignore[index]
        payload["attempt"]["attempt_id"] = "attempt-2"  # type: ignore[index]
        replay["payload"] = payload
        before = len(self.follower.entry_requests())
        self.follower.cell.handle_event(RelayEnvelopeReceived(replay))
        self.assertEqual(len(self.follower.entry_requests()), before)

    def test_unavailable_broker_facts_reject_without_a_broker_send(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        self.follower.cell.handle_event(self.follower.facts(self.now, orders=None, positions=None))
        self.net.hold_kinds.discard("attempt")
        self.net.release_held()
        self.net.pump()
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn("broker order/position read unavailable", self._details())

    def test_follower_never_originates_an_attempt(self) -> None:
        self.prime()
        self.feed_quotes()
        self.assertEqual(self.net.payloads("attempt", FOLLOWER), [])
        with self.assertRaises(PairExecutionCellError):
            self.follower.cell.accept_policy(self.policy(entry_edge_points="3"))


# --------------------------------------------------------------------------- #
# Independent monotonic timers
# --------------------------------------------------------------------------- #


class ConfirmationTimerTests(PairCellTestCase):
    def test_leader_requires_exact_evidence_for_both_positions(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).pair_confirmed)
        self.clock.advance(4.9)
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.desired_state, "ACTIVE")
        self.clock.advance(0.2)
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(len(self.leader.close_requests()), 1)

    def test_order_receipt_alone_is_not_position_proof(self) -> None:
        self.prime()
        self.net.drop_statuses.add("filled")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        statuses = [payload["status"] for payload in self.net.payloads("leg_status", FOLLOWER)]
        self.assertIn("sent", statuses)
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).pair_confirmed)
        self.assertEqual(self.leader.modify_requests(), [])
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.mt5.positions, [])

    def test_leader_timeout_emits_best_effort_pair_entry_failed(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(self.net.payloads("pair_entry_failed", LEADER))

    def test_pair_entry_failed_accelerates_follower_containment(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        # The follower contained before its own five-second timer elapsed.
        self.assertEqual(self.follower.mt5.positions, [])
        self.assertEqual(len(self.follower.close_requests()), 1)

    def test_follower_contains_when_confirmation_is_absent_after_five_seconds(self) -> None:
        self.prime()
        self.net.drop_kinds.add("pair_entry_confirmed")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.clock.advance(4.5)
        self.assertEqual(
            self.follower.cell.handle_event(ClockTickEvent(self.now)).desired_state, "ACTIVE"
        )
        self.clock.advance(1.0)
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.follower.mt5.positions, [])

    def test_follower_rejects_a_confirmation_that_arrives_after_its_timer(self) -> None:
        self.prime()
        self.net.hold_kinds.add("pair_entry_confirmed")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.net.drop_kinds.add("leg_status")
        self.clock.advance(6)
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.follower.mt5.positions, [])
        self.net.hold_kinds.discard("pair_entry_confirmed")
        self.net.release_held()
        self.net.pump()
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.pair_confirmed)
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.follower.modify_requests(), [])
        self.assertIn(
            "pair_confirmation_rejected",
            [row["event"] for row in self.follower.cell.transition_history()],
        )

    def test_timers_are_independent_and_a_delayed_attempt_still_enters(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.net.drop_kinds.add("pair_entry_confirmed")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        # A delayed attempt: the leader's timer expires and it contains first.
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.mt5.positions, [])
        # The old but otherwise admissible attempt still enters.
        self.net.hold_kinds.discard("attempt")
        self.net.release_held()
        self.net.pump()
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertEqual(len(self.follower.mt5.positions), 1)
        attempt = self.last_attempt()
        self.assertEqual(
            Decimal(str(self.follower.mt5.positions[0]["sl"])), Decimal(attempt.follower_rough_sl)
        )
        # It is bounded by rough protection plus the follower's own timer.
        self.clock.advance(5.1)
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.follower.mt5.positions, [])

    def test_follower_restart_with_an_unresolved_entered_attempt_contains(self) -> None:
        self.prime()
        self.net.drop_kinds.add("pair_entry_confirmed")
        self.net.drop_kinds.add("pair_entry_failed")
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.assertEqual(len(self.follower.mt5.positions), 1)
        cell = self.follower.restart()
        result = cell.recover()
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.follower.mt5.positions, [])


# --------------------------------------------------------------------------- #
# Entry results
# --------------------------------------------------------------------------- #


class EntryResultTests(PairCellTestCase):
    def test_leader_entry_rejection_contains_both_legs(self) -> None:
        self.prime()
        self.leader.mt5.reject = True
        self.feed_quotes()
        self.net.pump()
        self.assertIn(
            "entry_rejected", [row["event"] for row in self.leader.cell.transition_history()]
        )
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")

    def test_follower_entry_rejection_contains_the_leader(self) -> None:
        self.prime()
        self.follower.mt5.reject = True
        self.feed_quotes()
        self.net.pump()
        self.assertIn(
            "peer_leg_status", [row["event"] for row in self.leader.cell.transition_history()]
        )
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(len(self.leader.close_requests()), 1)

    def test_both_rejected_converges_to_broker_verified_empty(self) -> None:
        self.prime()
        self.leader.mt5.reject = True
        self.follower.mt5.reject = True
        self.feed_quotes()
        self.net.pump()
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")
        self.assertEqual(self.follower.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")

    def test_unknown_after_send_resolves_from_broker_observation(self) -> None:
        self.prime()
        self.leader.mt5.raise_after_fill = True
        self.feed_quotes()
        history = [row["event"] for row in self.leader.cell.transition_history()]
        self.assertIn("entry_unknown_after_send", history)
        self.assertIn("entry_filled_observed", history)
        self.assertEqual(len(self.leader.entry_requests()), 1, "an uncertain send is never blindly retried")

    def test_receipt_loss_resolves_from_broker_observation(self) -> None:
        self.prime()
        self.leader.mt5.receipt_none_after_fill = True
        self.feed_quotes()
        history = [row["event"] for row in self.leader.cell.transition_history()]
        self.assertIn("mt5_returned_no_receipt", history)
        self.assertIn("entry_filled_observed", history)
        self.assertEqual(len(self.leader.entry_requests()), 1)

    def test_broker_delta_before_receipt_never_produces_a_false_empty(self) -> None:
        self.prime()
        self.leader.mt5.raise_after_fill = True
        self.leader.mt5.partial_volume = "1"
        self.feed_quotes()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertNotEqual(result.state, "EMPTY")
        self.assertTrue(result.needs_human)

    def test_partial_fill_is_never_attributed_to_the_attempt(self) -> None:
        self.prime()
        self.follower.mt5.partial_volume = "1"
        self.feed_quotes()
        self.net.pump()
        self.assertNotEqual(
            self.follower.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE"
        )
        self.assertFalse(self.leader.cell.handle_event(ClockTickEvent(self.now)).pair_confirmed)

    def test_inconsistent_exact_peer_evidence_contains(self) -> None:
        self.prime()
        self.net.hold_kinds.add("leg_status")
        self.feed_quotes()
        self.net.pump()
        forged = None
        for envelope in self.net.held:
            payload = dict(envelope["payload"])  # type: ignore[arg-type]
            if envelope["from_worker_id"] == FOLLOWER and payload.get("status") == "filled":
                payload["volume"] = "7"
                envelope["payload"] = payload
                forged = envelope
        self.assertIsNotNone(forged)
        self.net.hold_kinds.discard("leg_status")
        self.net.release_held()
        self.net.pump()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.pair_confirmed)
        self.assertIn(
            "inconsistent_peer_evidence",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self.assertEqual(self.leader.modify_requests(), [])
        self.assertEqual(self.leader.mt5.positions, [])

    def test_entry_effect_prepare_failure_contains_without_sending(self) -> None:
        self.prime()
        self.leader.journal.fail_prepare = 1
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        self.assertEqual(self.leader.entry_requests(), [])
        self.assertIn(
            "entry_rejected_preflight",
            [row["event"] for row in self.leader.cell.transition_history()],
        )


# --------------------------------------------------------------------------- #
# Durable recovery
# --------------------------------------------------------------------------- #


class RecoveryTests(PairCellTestCase):
    def test_restart_before_any_attempt_restores_readiness_and_the_route(self) -> None:
        self.prime()
        cell = self.leader.restart()
        result = cell.recover()
        self.assertIsNone(result.attempt_id)
        self.assertEqual(result.policy_hash, cast(StrategyPolicy, self.accepted_policy).hash)
        self.assertTrue(result.policy_accepted)
        self.assertEqual(result.route_id, ROUTE_ID)
        self.assertEqual(result.role, "leader")
        self.assertEqual(result.universe_generation, 1)
        cell.handle_event(self.leader.facts(self.now))
        self.assertTrue(cell.handle_event(ClockTickEvent(self.now)).ready)

    def test_a_restarted_worker_resumes_its_role_and_frozen_universe(self) -> None:
        self.prime()
        universe = self.leader.cell.discovered_universe()
        cell = self.leader.restart()
        cell.recover()
        self.assertEqual(cell.discovered_universe(), universe)
        self.assertEqual(cell.assigned_role(), "leader")
        self.assertEqual(cell.eligible_products(), (self.product_id(),))

    def test_restart_after_fill_before_confirmation_contains(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.assertEqual(len(self.leader.mt5.positions), 1)
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.leader.mt5.positions, [])

    def test_restart_during_containment_never_blindly_resends(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.leader.journal.fail_receipt = 1
        self.leader.mt5.close_leaves_position_visible = True
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        closes_before = len(self.leader.close_requests())
        self.assertEqual(closes_before, 1)
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(len(self.leader.close_requests()), closes_before)
        self.assertTrue(result.needs_human)

    def test_restart_after_active_resumes_without_containment(self) -> None:
        self.run_entry()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(result.state, "ACTIVE")
        self.assertEqual(len(self.leader.mt5.positions), 1)
        self.assertEqual(self.leader.close_requests(), [])

    def test_prepared_but_unsent_effect_blocks_readiness_until_resolved(self) -> None:
        self.prime()
        self.leader.journal.prepare("orphan", {"type": "market", "symbol": SYMBOL})
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(result.ready)
        self.assertIn("unresolved effects", result.ready_reason)

    def test_an_entry_that_crossed_send_started_is_never_resent_after_restart(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.leader.journal.fail_receipt = 1
        self.feed_quotes()
        self.assertEqual(len(self.leader.entry_requests()), 1)
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(result.desired_state, "EMPTY")
        self.assertEqual(self.leader.mt5.positions, [])


# --------------------------------------------------------------------------- #
# Shadow rollout
# --------------------------------------------------------------------------- #


class ShadowModeTests(PairCellTestCase):
    def test_shadow_mode_completes_the_lifecycle_without_touching_the_broker(self) -> None:
        self.prime(shared={"mode": "shadow"})
        entries_before = len(self.leader.entry_requests())
        self.feed_quotes()
        self.assertEqual(len(self.leader.entry_requests()), entries_before)
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        self.assertEqual(self.follower.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")


# --------------------------------------------------------------------------- #
# Quarantine
# --------------------------------------------------------------------------- #


class QuarantineTests(PairCellTestCase):
    def test_price_off_quarantines_only_the_affected_product_pair_wide(self) -> None:
        self.add_second_product()
        self.prime()
        self.warm(OTHER_SYMBOL)
        product, other = self.product_id(), self.product_id(OTHER_SYMBOL)
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes(symbol=SYMBOL)
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.quarantined_products(), (product,))
        self.assertEqual(self.follower.cell.quarantined_products(), (product,))
        self.assertIn(other, self.leader.cell.eligible_products())

    def test_quarantine_is_keyed_by_the_derived_product_identity(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.quarantined_products(), (product,))
        self.assertTrue(product.startswith(f"{SYMBOL}:"))
        self.assertEqual(self.leader.cell.quarantine_generation(product), 1)

    def test_quarantine_survives_restart_and_has_no_automatic_expiry(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        self.net.pump()
        self.tick(seconds=7200)
        cell = self.leader.restart()
        cell.recover()
        self.assertEqual(cell.quarantined_products(), (product,))

    def test_release_is_refused_while_an_attempt_is_unresolved(self) -> None:
        self.prime()
        product = self.product_id()
        self.net.drop_kinds.add("leg_status")
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        self.leader.cell.handle_event(
            QuarantineReleaseEvent(
                product_id=product, actor="operator", reason="fixed", observed_at=self.now
            )
        )
        self.assertIn(
            "product_quarantine_release_rejected",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self.assertEqual(self.leader.cell.quarantined_products(), (product,))

    def test_explicit_operator_release_restores_the_product(self) -> None:
        self.prime()
        product = self.product_id()
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes()
        self.net.pump()
        self.tick()
        self.leader.cell.handle_event(
            QuarantineReleaseEvent(
                product_id=product,
                actor="operator",
                reason="broker restored prices",
                observed_at=self.now,
            )
        )
        self.assertEqual(self.leader.cell.quarantined_products(), ())
        self.assertIsNotNone(self.leader.cell.quarantine_release_marker(product))


# --------------------------------------------------------------------------- #
# Exit convergence
# --------------------------------------------------------------------------- #


class ExitConvergenceTests(PairCellTestCase):
    def test_timed_exit_uses_the_desired_empty_path(self) -> None:
        self.prime(shared={"maximum_holding_seconds": 1.0})
        self.feed_quotes()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        self.now += timedelta(seconds=5)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])

    def test_new_york_cutoff_uses_the_desired_empty_path(self) -> None:
        self.prime(shared={"flatten_at_ny": "09:30"})
        self.feed_quotes()
        self.now = datetime(2026, 3, 4, 14, 35, tzinfo=UTC)  # 09:35 New York
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        self.assertEqual(self.leader.mt5.positions, [])

    def test_integrity_divergence_contains(self) -> None:
        self.run_entry()
        self.leader.mt5.positions[0]["sl"] = 1.05000
        self.leader.cell.handle_event(
            BrokerSnapshotEvent(
                orders=[], positions=[dict(p) for p in self.leader.mt5.positions], observed_at=self.now
            )
        )
        self.assertEqual(
            self.leader.cell.handle_event(ClockTickEvent(self.now)).desired_state, "EMPTY"
        )

    def test_both_workers_converge_to_terminal_empty(self) -> None:
        self.run_entry()
        self.leader.cell.request_close("operator shutdown")
        self.net.pump()
        for _ in range(3):
            self.tick()
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")


# --------------------------------------------------------------------------- #
# Calibrated broker-time quote admission
# --------------------------------------------------------------------------- #


class CalibratedQuoteTimeTests(PairCellTestCase):
    def test_a_frozen_broker_tick_ages_out_however_often_it_is_relayed(self) -> None:
        self.prime()
        frozen = self.now - timedelta(seconds=10)
        for sequence in (2, 3, 4):
            self.feed_quotes(
                sequence=sequence, leader_broker_time=frozen, follower_broker_time=frozen
            )
            self.now += timedelta(seconds=1)
            self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_calibrated_offsets_align_brokers_running_different_clocks(self) -> None:
        self.prime()
        # Two brokers whose raw clocks are 90 minutes apart, both calibrated.
        self.feed_quotes(
            leader_broker_time=self.now + timedelta(hours=1),
            leader_calibration=BrokerClockCalibration(offset_seconds=3600.0),
            follower_broker_time=self.now - timedelta(minutes=30),
            follower_calibration=BrokerClockCalibration(offset_seconds=-1800.0),
        )
        attempt = self.last_attempt()
        self.assertEqual(attempt.leader_quote.calibrated_broker_time, self.now)
        self.assertEqual(attempt.follower_quote.calibrated_broker_time, self.now)
        self.assertEqual(
            calibrated_skew_seconds(attempt.leader_quote, attempt.follower_quote), 0.0
        )

    def test_raw_broker_clock_distance_is_not_used_for_skew(self) -> None:
        self.prime(shared={"quote_max_skew_seconds": 1.0})
        self.feed_quotes(
            leader_broker_time=self.now + timedelta(hours=1),
            leader_calibration=BrokerClockCalibration(offset_seconds=3600.0),
        )
        # A raw comparison would be 3600 seconds apart and reject this pair.
        self.assertEqual(len(self.attempt_payloads()), 1)

    def test_calibration_uncertainty_is_charged_against_freshness(self) -> None:
        self.prime(shared={"quote_max_age_seconds": 2.0})
        self.feed_quotes(leader_calibration=BrokerClockCalibration(error_seconds=2.5))
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_calibration_uncertainty_is_charged_against_skew(self) -> None:
        self.prime(shared={"quote_max_skew_seconds": 1.0})
        self.feed_quotes(
            leader_calibration=BrokerClockCalibration(error_seconds=0.6),
            follower_calibration=BrokerClockCalibration(error_seconds=0.6),
        )
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])

    def test_an_uncalibrated_or_stale_broker_clock_is_never_a_candidate(self) -> None:
        self.prime()
        self.feed_quotes(follower_calibration=BrokerClockCalibration(status="uncalibrated"))
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [])
        self.feed_quotes(sequence=3, leader_calibration=BrokerClockCalibration(status="stale"))
        self.assertEqual(self.leader.cell.entry_candidates(), [])

    def test_calibration_is_relayed_and_frozen_into_the_attempt(self) -> None:
        self.prime()
        self.feed_quotes(
            follower_broker_time=self.now + timedelta(seconds=120),
            follower_calibration=BrokerClockCalibration(offset_seconds=120.0, error_seconds=0.25),
        )
        attempt = self.last_attempt()
        self.assertEqual(attempt.follower_quote.calibration.offset_seconds, 120.0)
        self.assertEqual(attempt.follower_quote.calibration.error_seconds, 0.25)
        self.assertEqual(attempt.follower_quote.calibrated_broker_time, self.now)

    def test_a_quote_from_beyond_its_own_uncertainty_is_rejected(self) -> None:
        self.prime()
        # Calibrated 30 seconds into the future with no uncertainty budget.
        self.feed_quotes(leader_broker_time=self.now + timedelta(seconds=30))
        self.assertEqual(self.leader.cell.entry_candidates(), [])


# --------------------------------------------------------------------------- #
# Peer terminal proof and bounded recovery
# --------------------------------------------------------------------------- #


class PeerTerminalProofTests(PairCellTestCase):
    def _enter_and_strand_leader(self) -> None:
        """Leader holds a filled leg while the peer says nothing at all."""

        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.mt5.positions, [])

    def _leg_status(self, payload: dict[str, object]) -> dict[str, object]:
        return {
            "protocol_version": pair_cell.PROTOCOL_VERSION,
            "kind": "leg_status",
            "route_id": ROUTE_ID,
            "from_worker_id": FOLLOWER,
            "from_role": "follower",
            "to_worker_id": LEADER,
            "payload": payload,
        }

    def test_a_follower_admission_rejection_lets_the_leader_terminalize(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        self.follower.cell.handle_event(self.follower.facts(self.now, orders=None, positions=None))
        self.net.hold_kinds.discard("attempt")
        self.net.release_held()
        self.net.pump()
        self.assertEqual(self.follower.entry_requests(), [])
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.state, "EMPTY")
        self.assertFalse(result.needs_human)
        self.assertEqual(self.leader.mt5.positions, [])

    def test_a_durable_rejection_is_replayed_to_a_duplicate_delivery(self) -> None:
        self.prime()
        self.net.hold_kinds.add("attempt")
        self.feed_quotes()
        envelope = next(e for e in self.net.sent if e["kind"] == "attempt")
        self.follower.cell.handle_event(self.follower.facts(self.now, orders=None, positions=None))
        self.follower.cell.handle_event(RelayEnvelopeReceived(dict(envelope)))
        self.follower.cell.handle_event(self.follower.facts(self.now))
        self.follower.cell.handle_event(RelayEnvelopeReceived(dict(envelope)))
        self.assertEqual(self.follower.entry_requests(), [])
        statuses = [p["status"] for p in self.net.payloads("leg_status", FOLLOWER)]
        self.assertEqual(statuses.count("rejected"), 2, "the durable refusal is replayed, not re-decided")

    def test_a_dropped_leg_status_is_recovered_by_the_bounded_probe(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.net.drop_kinds.add("pair_entry_failed")
        self.net.drop_kinds.add("pair_entry_confirmed")
        self.feed_quotes()
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertEqual(self.follower.mt5.positions, [])
        # Both sides are empty but neither has the other's proof yet.
        self.assertNotEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")
        self.net.drop_kinds.discard("leg_status")
        self.clock.advance(5.1)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        self.assertIn(
            "peer_terminal_proof_probe",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        leader = self.leader.cell.handle_event(ClockTickEvent(self.now))
        follower = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(leader.state, "EMPTY")
        self.assertEqual(follower.state, "EMPTY")
        self.assertFalse(leader.needs_human)

    def test_a_peer_with_no_durable_attempt_record_proves_non_send(self) -> None:
        self.prime()
        self.net.drop_kinds.add("attempt")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.assertEqual(self.follower.entry_requests(), [])
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.clock.advance(5.1)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.state, "EMPTY")
        self.assertFalse(result.needs_human)
        self.assertIn(
            "no_attempt_record",
            [str(p["status"]) for p in self.net.payloads("leg_status", FOLLOWER)],
        )

    def test_an_unsolicited_no_attempt_record_is_not_accepted_as_proof(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        attempt_id = self.last_attempt().attempt_id
        self.clock.advance(6)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.leader.cell.handle_event(
            RelayEnvelopeReceived(
                self._leg_status(
                    {"attempt_id": attempt_id, "status": "no_attempt_record", "reason": "spoofed"}
                )
            )
        )
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertNotEqual(result.state, "EMPTY")
        self.assertIn(
            "unsolicited_no_attempt_record_ignored",
            [row["event"] for row in self.leader.cell.transition_history()],
        )

    def test_a_truly_unavailable_peer_becomes_loud_rather_than_parking(self) -> None:
        self._enter_and_strand_leader()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        for _ in range(9):
            self.clock.advance(5.1)
            result = self.leader.cell.handle_event(ClockTickEvent(self.now))
            self.net.queue.clear()  # every answer is lost in transit
            if result.needs_human:
                break
        self.assertTrue(result.needs_human)
        self.assertIn("peer terminal proof", str(result.needs_human_reason))
        self.assertNotEqual(result.state, "EMPTY", "EMPTY is never inferred from silence")

    def test_the_bounded_probe_retransmits_this_workers_terminal_status(self) -> None:
        self._enter_and_strand_leader()
        self.net.drop_kinds.discard("leg_status")
        self.clock.advance(5.1)
        self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertIn("leg_status_request", self.net.kinds(LEADER))
        self.assertIn("empty", [str(p["status"]) for p in self.net.payloads("leg_status", LEADER)])

    def test_repeated_identical_peer_chatter_does_not_extend_the_window(self) -> None:
        self._enter_and_strand_leader()
        attempt_id = self.last_attempt().attempt_id
        chatter = self._leg_status(
            {
                "attempt_id": attempt_id,
                "status": "filled",
                "ticket": "9000",
                "symbol": SYMBOL,
                "side": "SHORT",
                "volume": "2",
                "fill_price": "1.10040",
            }
        )
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        for _ in range(12):
            self.clock.advance(5.1)
            self.leader.cell.handle_event(RelayEnvelopeReceived(dict(chatter)))
            result = self.leader.cell.handle_event(ClockTickEvent(self.now))
            self.net.queue.clear()
            if result.needs_human:
                break
        self.assertTrue(result.needs_human)
        self.assertNotEqual(result.state, "EMPTY")


# --------------------------------------------------------------------------- #
# Effect-journal resolution from broker observation
# --------------------------------------------------------------------------- #


class EffectJournalResolutionTests(PairCellTestCase):
    def _converge_and_settle(self) -> None:
        self.leader.cell.request_close("operator shutdown")
        self.net.pump()
        for _ in range(3):
            self.tick()

    def test_receipt_loss_resolves_the_entry_effect_and_restores_readiness(self) -> None:
        self.prime()
        self.leader.mt5.receipt_none_after_fill = True
        self.feed_quotes()
        self.assertIn(
            "mt5_returned_no_receipt", [row["event"] for row in self.leader.cell.transition_history()]
        )
        self.assertEqual(self.leader.journal.unresolved(), [], "observation resolves the effect")
        self.assertIn(
            "effect_resolved_by_observation",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self._converge_and_settle()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)

    def test_unknown_after_send_with_a_fill_resolves_the_entry_effect(self) -> None:
        self.prime()
        self.leader.mt5.raise_after_fill = True
        self.feed_quotes()
        self.assertEqual(self.leader.journal.unresolved(), [])
        self._converge_and_settle()
        self.assertTrue(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)

    def test_unknown_after_send_resolved_absent_resolves_the_entry_effect(self) -> None:
        self.prime()
        self.leader.mt5.raise_before_fill = True
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.net.pump()
        self.assertIn(
            "unknown_after_send_resolved_absent",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self.assertEqual(self.leader.journal.unresolved(), [])
        self.assertEqual(len(self.leader.entry_requests()), 1, "never blindly resent")
        for _ in range(3):
            self.tick()
        self.assertTrue(self.leader.cell.handle_event(ClockTickEvent(self.now)).ready)

    def test_a_pre_send_abort_resolves_the_prepared_effect(self) -> None:
        self.prime()
        self.leader.scheduler.freeze()
        self.net.drop_kinds.add("pair_entry_failed")
        self.feed_quotes()
        self.net.pump()
        self.assertEqual(self.leader.entry_requests(), [], "MT5 was never called")
        self.assertEqual(self.leader.journal.unresolved(), [])
        self.assertIn(
            "effect_resolved_by_observation",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self.leader.scheduler.release()
        for _ in range(3):
            self.tick()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)

    def test_an_effect_that_crossed_send_started_is_still_never_resent(self) -> None:
        self.prime()
        self.leader.journal.fail_receipt = 1
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        # The observation resolves the journal without a second broker write.
        self.assertEqual(self.leader.journal.unresolved(), [])
        self.assertEqual(len(self.leader.entry_requests()), 1)

    def test_a_protection_effect_is_resolved_by_the_next_observation(self) -> None:
        self.run_entry()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        self.assertEqual(self.leader.journal.unresolved(), [])
        self.assertEqual(self.follower.journal.unresolved(), [])


# --------------------------------------------------------------------------- #
# Crashes at exact durable boundaries
# --------------------------------------------------------------------------- #


class CrashBoundaryTests(PairCellTestCase):
    """Kill a Worker at each irreversible boundary and prove recovery.

    Every case asserts the same three invariants: the effect journal ends
    resolved (so readiness can return), MT5 is never called a second time for
    the same effect, and ``EMPTY`` is never declared without proof.
    """

    def _effect_states(self, side: _Side) -> list[str]:
        return [str(entry["state"]) for entry in side.journal.unresolved()]

    def test_crash_after_prepare_before_any_send(self) -> None:
        self.prime()
        self.leader.journal.crash_after_prepare = True
        with self.assertRaises(CrashError):
            self.feed_quotes()
        self.assertEqual(self._effect_states(self.leader), ["prepared"])
        self.assertEqual(self.leader.entry_requests(), [], "MT5 was never called")
        self.assertEqual(self.attempt_payloads(), [], "the attempt never left this Worker")

        cell = self.leader.restart()
        result = cell.recover()
        self.assertIn(
            "entry_effect_never_started", [row["event"] for row in cell.transition_history()]
        )
        self.assertEqual(self.leader.journal.unresolved(), [], "the prepared effect is resolved")
        self.assertEqual(self.leader.entry_requests(), [], "never blindly resent")
        self.assertEqual(self.leader.mt5.positions, [])
        # Nothing was relayed, so this side is provably one-sided and may finish.
        self.assertEqual(result.state, "EMPTY")
        self.assertFalse(result.needs_human)
        cell.handle_event(self.leader.facts(self.now))
        self.assertTrue(cell.handle_event(ClockTickEvent(self.now)).ready)

    def test_crash_after_prepare_with_unattributable_exposure_is_loud(self) -> None:
        self.prime()
        self.leader.journal.crash_after_prepare = True
        with self.assertRaises(CrashError):
            self.feed_quotes()
        # Exposure this attempt provably did not create must never be closed on
        # a guess, and must never be silently ignored either.
        self.leader.mt5.positions.append(
            {
                "ticket": 4242,
                "symbol": SYMBOL,
                "type": 0,
                "volume": 2.0,
                "price_open": 1.10010,
                "sl": 1.09935,
                "tp": 1.10085,
            }
        )
        cell = self.leader.restart()
        result = cell.recover()
        self.assertTrue(result.needs_human)
        self.assertNotEqual(result.state, "EMPTY")
        self.assertEqual(self.leader.close_requests(), [], "unattributable exposure is not guessed at")
        self.assertEqual(self.leader.journal.unresolved(), [])

    def test_crash_after_relay_before_the_local_broker_send(self) -> None:
        self.prime()
        self.net.crash_kinds.add("attempt")
        self.net.drop_kinds.add("pair_entry_failed")
        # The peer's own status never reaches this leader, so the only durable
        # evidence it has is that the attempt was relayed before the crash.
        self.net.drop_kinds.add("leg_status")
        with self.assertRaises(CrashError):
            self.feed_quotes()
        self.assertEqual(self.leader.entry_requests(), [], "the local send never began")
        self.assertEqual(len(self.attempt_payloads()), 1, "the attempt did leave this Worker")
        # The follower really does receive it and takes on exposure.
        self.net.pump()
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertEqual(len(self.follower.mt5.positions), 1)

        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(self.leader.journal.unresolved(), [])
        self.assertEqual(self.leader.entry_requests(), [], "never blindly resent")
        self.assertNotEqual(
            result.state, "EMPTY", "a relayed attempt is never terminalized without peer proof"
        )
        # The follower's own receive-relative timer bounds its exposure.
        self.net.drop_kinds.discard("leg_status")
        self.clock.advance(5.1)
        self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        self.assertEqual(self.follower.mt5.positions, [])
        self.assertEqual(cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")

    def test_crash_after_send_started_with_a_real_fill(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.net.drop_kinds.add("pair_entry_failed")
        self.leader.mt5.crash_after_fill = True
        with self.assertRaises(CrashError):
            self.feed_quotes()
        self.assertEqual(self._effect_states(self.leader), ["send_started"])
        self.assertEqual(len(self.leader.mt5.positions), 1, "the broker really did fill")

        cell = self.leader.restart()
        result = cell.recover()
        history = [row["event"] for row in cell.transition_history()]
        self.assertIn("entry_effect_may_have_sent", history)
        self.assertIn("entry_filled_observed", history)
        self.assertEqual(self.leader.journal.unresolved(), [], "observation resolves the effect")
        self.assertEqual(len(self.leader.entry_requests()), 1, "never blindly resent")
        self.assertEqual(self.leader.mt5.positions, [], "the exact position is contained")
        self.assertEqual(len(self.leader.close_requests()), 1)
        self.assertNotEqual(result.state, "EMPTY", "the relayed peer leg still owes its own proof")
        self.assertFalse(result.needs_human)

    def test_a_crashed_leader_probes_the_peer_instead_of_parking(self) -> None:
        self.prime()
        self.net.drop_kinds.add("attempt")
        self.net.drop_kinds.add("pair_entry_failed")
        self.leader.mt5.crash_after_fill = True
        with self.assertRaises(CrashError):
            self.feed_quotes()
        cell = self.leader.restart()
        cell.recover()
        self.assertEqual(self.leader.mt5.positions, [])
        self.assertNotEqual(cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")
        self.clock.advance(5.1)
        cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        result = cell.handle_event(ClockTickEvent(self.now))
        self.assertIn(
            "peer_terminal_proof_probe", [row["event"] for row in cell.transition_history()]
        )
        self.assertEqual(result.state, "EMPTY")
        self.assertFalse(result.needs_human)

    def test_a_follower_crash_after_send_started_contains_its_own_leg(self) -> None:
        self.prime()
        self.net.drop_kinds.add("pair_entry_confirmed")
        self.net.drop_kinds.add("pair_entry_failed")
        self.follower.mt5.crash_after_fill = True
        with self.assertRaises(CrashError):
            self.feed_quotes()
        self.assertEqual(self._effect_states(self.follower), ["send_started"])
        self.assertEqual(len(self.follower.mt5.positions), 1)
        cell = self.follower.restart()
        result = cell.recover()
        self.assertEqual(self.follower.journal.unresolved(), [])
        self.assertEqual(len(self.follower.entry_requests()), 1)
        self.assertEqual(self.follower.mt5.positions, [], "restart contains immediately")
        self.assertEqual(result.desired_state, "EMPTY")

    def test_a_crash_while_unpairing_leaves_the_assertion_to_be_reproduced(self) -> None:
        self.prime()
        self.unpairing()
        assertion = self.leader.cell.safe_unpair_assertion()
        self.assertTrue(assertion.safe)
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(result.route_state, "UNPAIRING")
        cell.handle_event(self.leader.facts(self.now))
        reproduced = cell.safe_unpair_assertion()
        self.assertTrue(reproduced.safe)
        self.assertTrue(cell.safe_unpair_assertion_is_current(reproduced))
        self.assertGreaterEqual(reproduced.state_version, assertion.state_version)


# --------------------------------------------------------------------------- #
# Independent peer rebuild and reconnect re-seeding
# --------------------------------------------------------------------------- #


class PeerRebuildReconnectTests(PairCellTestCase):
    """A Worker may be rebuilt while its peer survives with full state.

    Every versioned publication restarts its counter with the rebuilt process,
    and every publication cache on the *survivor* would otherwise suppress
    exactly the state the rebuilt Worker no longer has.
    """

    def _peer_allowance(self, side: _Side) -> pair_cell.RemainingAllowanceSummary:
        summary = side.cell.peer_remaining_allowance()
        assert summary is not None, "no peer remaining-allowance summary is held"
        return summary

    def test_the_publisher_epoch_is_durable_and_strictly_increases_per_rebuild(self) -> None:
        self.prime()
        first = self.follower.cell.publisher_epoch()
        self.follower.restart().recover()
        second = self.follower.cell.publisher_epoch()
        self.follower.restart().recover()
        third = self.follower.cell.publisher_epoch()
        self.assertLess(first, second)
        self.assertLess(second, third)

    def test_the_summary_carries_its_publisher_epoch_over_the_relay(self) -> None:
        self.prime()
        payload = self.net.payloads("remaining_allowance", FOLLOWER)[-1]
        self.assertEqual(payload["publisher_epoch"], self.follower.cell.publisher_epoch())
        self.assertEqual(
            set(payload), {"worker_id", "publisher_epoch", "version", "allowed_leg_loss_usd"}
        )
        catalog = cast(dict, self.net.payloads("catalog_summary", FOLLOWER)[-1]["catalog"])
        self.assertEqual(catalog["publisher_epoch"], self.follower.cell.publisher_epoch())

    def test_a_rebuilt_follower_replaces_the_leaders_stale_larger_allowance(self) -> None:
        self.prime()
        stale = self._peer_allowance(self.leader)
        self.assertEqual(Decimal(stale.allowed_leg_loss_usd), Decimal("80"))

        self.follower.restart().recover()
        self.follower.cell.handle_event(self.follower.facts(self.now))
        # The rebuilt follower has less daily allowance left than the leader
        # last saw, and its version counter restarted with the process.
        self.follower.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-60", closed_at=self.now)
        )
        self.net.pump()
        self.tick()

        current = self._peer_allowance(self.leader)
        self.assertGreater(current.publisher_epoch, stale.publisher_epoch)
        self.assertLessEqual(
            current.version, stale.version, "the version counter really did restart"
        )
        self.assertEqual(Decimal(current.allowed_leg_loss_usd), Decimal("60"))

    def test_a_rebuilt_follower_never_causes_a_leader_only_broker_entry(self) -> None:
        self.prime()
        self.follower.restart().recover()
        self.follower.cell.handle_event(self.follower.facts(self.now))
        self.follower.cell.handle_event(
            RealizedPnLEvent(attempt_id="a1", realized_usd="-60", closed_at=self.now)
        )
        self.net.pump()
        self.tick()
        self.feed_quotes()
        attempt = self.last_attempt()
        self.assertEqual(Decimal(attempt.follower_allowed_loss_usd), Decimal("60"))
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(
            len(self.follower.entry_requests()), 1, "the follower admitted the assigned loss"
        )
        self.assertEqual(len(self.follower.mt5.positions), 1)

    def test_the_attempt_freezes_both_allowance_epochs(self) -> None:
        from dataclasses import replace

        self.run_entry()
        attempt = self.last_attempt()
        self.assertEqual(attempt.leader_allowance_epoch, self.leader.cell.publisher_epoch())
        self.assertEqual(attempt.follower_allowance_epoch, self.follower.cell.publisher_epoch())
        self.assertIn("leader_allowance_epoch", attempt.canonical())
        self.assertIn("follower_allowance_epoch", attempt.canonical())
        self.assertNotEqual(
            replace(attempt, follower_allowance_epoch=99).payload_hash, attempt.payload_hash
        )

    def test_peer_session_loss_drops_the_peer_allowance_and_fails_closed(self) -> None:
        self.prime()
        self.assertIsNotNone(self.leader.cell.peer_remaining_allowance())
        # Nothing can re-seed the leader while the relay stays one-way.
        self.net.drop_kinds.add("reseed_request")
        self.net.drop_kinds.add("remaining_allowance")
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.assertIsNone(
            self.leader.cell.peer_remaining_allowance(),
            "a summary from a session that may have been rebuilt is not evidence",
        )
        self.leader.cell.handle_event(PeerSessionEvent(connected=True, observed_at=self.now))
        self.feed_quotes()
        self.assertEqual(self.attempt_payloads(), [], "no attempt without a re-seeded summary")
        self.assertEqual(self.leader.entry_requests(), [])
        # A current peer summary is what releases entry again.
        self.net.drop_kinds.clear()
        self.follower.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.follower.cell.handle_event(PeerSessionEvent(connected=True, observed_at=self.now))
        self.net.pump()
        self.tick()
        self.assertIsNotNone(self.leader.cell.peer_remaining_allowance())
        self.feed_quotes(sequence=3)
        self.assertEqual(len(self.attempt_payloads()), 1)

    def test_a_reconnect_republishes_all_peer_required_state_idempotently(self) -> None:
        self.prime()
        marker = self.net.mark()
        self.peer_session(False)
        self.peer_session(True)
        self.tick()
        self.net.pump()
        self.tick()
        follower_kinds = set(self.net.kinds_since(marker, FOLLOWER))
        leader_kinds = set(self.net.kinds_since(marker, LEADER))
        for kind in ("catalog_summary", "remaining_allowance", "readiness", "sizing_plans"):
            self.assertIn(kind, follower_kinds, f"the follower did not re-publish {kind}")
            self.assertIn(kind, leader_kinds, f"the leader did not re-publish {kind}")
        self.assertIn("pairing_acceptance", follower_kinds)
        self.assertIn("policy", leader_kinds)
        self.assertIn("universe", leader_kinds)
        # Re-publication is idempotent: nothing about the pair actually changed.
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)
        self.assertEqual(result.universe_generation, 1)
        self.assertEqual(result.policy_hash, cast(StrategyPolicy, self.accepted_policy).hash)
        self.assertIsNone(result.metadata_reconciliation)
        self.feed_quotes()
        self.assertEqual(len(self.attempt_payloads()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_a_rebuilt_leader_is_re_seeded_by_the_surviving_follower(self) -> None:
        self.prime()
        self.leader.restart().recover()
        self.leader.cell.handle_event(self.leader.facts(self.now))
        self.assertIsNone(self.leader.cell.peer_remaining_allowance())
        # The follower's caches would suppress everything the rebuilt leader
        # needs; the greater publisher epoch is what re-seeds them.
        self.tick()
        self.net.pump()
        self.tick()
        self.assertIsNotNone(self.leader.cell.peer_remaining_allowance())
        self.feed_quotes()
        self.assertEqual(len(self.attempt_payloads()), 1)
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_a_rebuilt_peer_catalog_is_accepted_despite_its_reset_version(self) -> None:
        self.prime()
        # The surviving leader stores the follower's catalog at version 2.
        self.follower_entries = [_entry(), _entry("AUDUSD")]
        self.feed_catalogs()
        stale_epoch = self.leader.cell.peer_publisher_epoch()

        self.follower.restart().recover()
        self.follower.cell.handle_event(self.follower.facts(self.now))
        self.tick()
        # The rebuilt follower republishes different content from version 1.
        self.follower_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.leader_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.feed_catalogs()
        self.assertGreater(self.leader.cell.peer_publisher_epoch(), stale_epoch)

        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 2)
        universe = self.leader.cell.discovered_universe()
        assert universe is not None
        self.assertEqual(
            sorted(product.symbol for product in universe.products),
            sorted([SYMBOL, OTHER_SYMBOL]),
            "the stale pre-rebuild catalog must not decide the intersection",
        )


# --------------------------------------------------------------------------- #
# Bounded explicit rediscovery attempts
# --------------------------------------------------------------------------- #


class RediscoveryFailureRecoveryTests(PairCellTestCase):
    def _break_the_intersection(self) -> None:
        self.follower_entries = [_entry("NZDUSD")]
        self.feed_catalogs()

    def _restore_a_wider_universe(self) -> None:
        self.follower_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.leader_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.feed_catalogs()

    def test_a_follower_initiated_rediscovery_blocks_entry_only_while_it_runs(self) -> None:
        self.prime()
        self._break_the_intersection()
        self.follower.cell.request_rediscovery(actor="follower-operator")
        pending = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertFalse(pending.ready)
        self.assertEqual(pending.ready_reason, "an explicit rediscovery is in progress")
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            result = side.cell.handle_event(ClockTickEvent(self.now))
            self.assertEqual(result.universe_generation, 1, "the previous generation remains")
            self.assertEqual(
                result.rediscovery_failure, "no compatible symbol survived discovery"
            )
            self.assertNotEqual(result.ready_reason, "an explicit rediscovery is in progress")

    def test_a_failed_rediscovery_never_leaves_the_request_pending_forever(self) -> None:
        self.prime()
        self._break_the_intersection()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        for _ in range(5):
            self.tick()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertTrue(result.ready, result.ready_reason)
        self.assertEqual(result.universe_generation, 1)
        self.feed_quotes(sequence=6)
        self.assertEqual(len(self.attempt_payloads()), 1, "entry resumes on the previous universe")

    def test_a_refused_rediscovery_is_a_bounded_attempt_not_a_stuck_state(self) -> None:
        self.run_entry()
        result = self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.assertIn("rediscovery refused", cast(str, result.rediscovery_failure))
        self.assertEqual(result.universe_generation, 1)
        # Converge to empty, then the very next request is evaluated afresh.
        self.leader.cell.request_close("operator shutdown")
        self.net.pump()
        for _ in range(3):
            self.tick()
        self._restore_a_wider_universe()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 2)
        self.assertIsNone(self.leader.cell.last_rediscovery_failure())

    def test_a_rediscovery_after_a_failed_attempt_can_still_succeed(self) -> None:
        self.prime()
        self._break_the_intersection()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.assertIsNotNone(self.leader.cell.last_rediscovery_failure())
        self._restore_a_wider_universe()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            self.assertEqual(side.cell.universe_generation(), 2)
            self.assertIsNone(side.cell.last_rediscovery_failure())
            self.assertEqual(side.cell.route_id(), ROUTE_ID)

    def test_a_failed_rediscovery_without_any_previous_universe_stays_closed(self) -> None:
        self.tick()
        self.accept()
        self.follower_entries = [_entry("NZDUSD")]
        self.feed_catalogs()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertIsNone(result.universe_generation)
        self.assertFalse(result.ready)
        self.assertIn("no discovered universe", result.ready_reason)


class OneSidedReconnectTests(PairCellTestCase):
    """Only one Worker observes the relay transition; the other must still re-seed."""

    def test_a_one_sided_reconnect_asks_the_peer_to_republish_once(self) -> None:
        self.prime()
        marker = self.net.mark()
        # Only the leader sees the outage, so only its caches are cleared.
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.leader.cell.handle_event(PeerSessionEvent(connected=True, observed_at=self.now))
        self.tick()
        self.net.pump()
        self.tick()
        self.assertIn("reseed_request", self.net.kinds_since(marker, LEADER))
        follower_kinds = self.net.kinds_since(marker, FOLLOWER)
        for kind in ("remaining_allowance", "readiness", "catalog_summary", "sizing_plans"):
            self.assertIn(kind, follower_kinds, f"the follower did not re-publish {kind}")
        self.assertIsNotNone(self.leader.cell.peer_remaining_allowance())
        self.feed_quotes()
        self.assertEqual(len(self.attempt_payloads()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_a_reseed_request_is_never_answered_with_another_request(self) -> None:
        self.prime()
        marker = self.net.mark()
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.leader.cell.handle_event(PeerSessionEvent(connected=True, observed_at=self.now))
        for _ in range(5):
            self.tick()
            self.net.pump()
        self.assertEqual(
            self.net.kinds_since(marker, FOLLOWER).count("reseed_request"),
            0,
            "answering a re-seed with a re-seed would be a message storm",
        )
        self.assertEqual(self.net.kinds_since(marker, LEADER).count("reseed_request"), 1)

    def test_a_duplicated_reseed_request_is_served_only_once(self) -> None:
        self.prime()
        self.leader.cell.handle_event(PeerSessionEvent(connected=False, observed_at=self.now))
        self.leader.cell.handle_event(PeerSessionEvent(connected=True, observed_at=self.now))
        self.tick()
        envelope = next(e for e in self.net.sent if e["kind"] == "reseed_request")
        self.net.pump()
        self.tick()
        marker = self.net.mark()
        for _ in range(3):
            self.follower.cell.handle_event(RelayEnvelopeReceived(dict(envelope)))
        self.assertEqual(
            self.net.kinds_since(marker, FOLLOWER),
            [],
            "a repeated nonce re-seeds nothing at all",
        )


class RediscoveryDeliveryTests(PairCellTestCase):
    """A requested rediscovery is a bounded round trip, not a one-shot hope."""

    def _request_from_follower(self) -> None:
        self.follower.cell.request_rediscovery(actor="follower-operator", reason="new symbol")

    def test_a_dropped_rediscovery_request_is_retried_and_then_times_out(self) -> None:
        self.prime()
        self.net.drop_kinds.add("rediscovery_request")
        self._request_from_follower()
        self.assertFalse(self.follower.cell.handle_event(ClockTickEvent(self.now)).ready)
        sends = self.net.kinds(FOLLOWER).count("rediscovery_request")
        self.assertEqual(sends, 1)
        # The retry cadence resends the same idempotent request.
        self.tick(seconds=pair_cell.REDISCOVERY_RETRY_SECONDS)
        self.assertGreater(self.net.kinds(FOLLOWER).count("rediscovery_request"), sends)
        request_ids = {
            str(p.get("request_id")) for p in self.net.payloads("rediscovery_request", FOLLOWER)
        }
        self.assertEqual(len(request_ids), 1, "the retry reuses one idempotent request ID")
        # The attempt is bounded: it gives up and releases entry readiness.
        self.tick(seconds=pair_cell.REDISCOVERY_TIMEOUT_SECONDS)
        result = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.universe_generation, 1)
        self.assertIn("before its deadline", cast(str, result.rediscovery_failure))
        self.assertNotEqual(result.ready_reason, "an explicit rediscovery is in progress")

    def test_a_retried_request_recovers_a_leader_restarted_mid_attempt(self) -> None:
        self.prime()
        self.net.drop_kinds.add("rediscovery_request")
        self._request_from_follower()
        # The leader is rebuilt without ever answering the one-shot request.
        self.leader.restart().recover()
        self.leader.cell.handle_event(self.leader.facts(self.now))
        self.leader_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.follower_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.feed_catalogs()
        self.net.drop_kinds.discard("rediscovery_request")
        self.tick(seconds=pair_cell.REDISCOVERY_RETRY_SECONDS)
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            self.assertEqual(side.cell.universe_generation(), 2)
            self.assertIsNone(side.cell.last_rediscovery_failure())
            self.assertEqual(side.cell.route_id(), ROUTE_ID)

    def test_a_replayed_request_id_is_answered_without_a_second_generation(self) -> None:
        self.prime()
        self.leader_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.follower_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.feed_catalogs()
        self._request_from_follower()
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 2)
        replay = next(e for e in self.net.sent if e["kind"] == "rediscovery_request")
        for _ in range(3):
            self.leader.cell.handle_event(RelayEnvelopeReceived(dict(replay)))
        self.net.pump()
        self.assertEqual(
            self.leader.cell.universe_generation(), 2, "a replay never installs a new generation"
        )
        self.assertIn(
            "rediscovery_answer_replayed",
            [row["event"] for row in self.leader.cell.transition_history()],
        )

    def _split_the_catalog_views(self) -> None:
        """The leader's view of the follower's catalog goes stale mid-flight.

        Both Workers gain a symbol locally, but the follower's summary never
        reaches the leader, so the candidate the leader computes cannot be
        reproduced by the follower that must verify it.
        """

        self.net.drop_from.add((FOLLOWER, "catalog_summary"))
        self.leader_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.follower_entries = [_entry(), _entry(OTHER_SYMBOL)]
        self.feed_catalogs()

    def test_a_follower_verification_refusal_clears_pending_on_both_sides(self) -> None:
        self.prime()
        self._split_the_catalog_views()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        follower = self.follower.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(follower.universe_generation, 1, "the previous universe stays in force")
        self.assertIsNotNone(follower.rediscovery_failure)
        self.assertNotEqual(follower.ready_reason, "an explicit rediscovery is in progress")
        self.assertIn(
            "universe_refused",
            [row["event"] for row in self.follower.cell.transition_history()],
        )
        leader = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertIn("refused the rediscovered universe", cast(str, leader.rediscovery_failure))

    def test_a_refused_rediscovery_leaves_both_workers_on_the_old_generation(self) -> None:
        self.prime()
        self._split_the_catalog_views()
        self.leader.cell.request_rediscovery(actor="operator")
        # Staged, never committed: the leader is still on the previous universe.
        self.assertEqual(self.leader.cell.universe_generation(), 1)
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            result = side.cell.handle_event(ClockTickEvent(self.now))
            self.assertEqual(
                result.universe_generation, 1, f"{side.role} left the previous generation"
            )
            self.assertTrue(result.ready, result.ready_reason)
        self.assertIn(
            "staged_universe_discarded",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        # Convergence: the pair still originates and enters on generation 1.
        self.feed_quotes(sequence=7)
        attempt = self.last_attempt()
        self.assertEqual(attempt.universe_generation, 1)
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_a_refused_rediscovery_can_be_retried_once_the_views_agree(self) -> None:
        self.prime()
        self._split_the_catalog_views()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 1)
        # The relay recovers and the reconnect re-seeds the follower's catalog.
        self.net.drop_from.clear()
        self.peer_session(False)
        self.peer_session(True)
        self.tick()
        self.net.pump()
        self.tick()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        self.tick()
        for side in (self.leader, self.follower):
            self.assertEqual(side.cell.universe_generation(), 2)
            self.assertIsNone(side.cell.last_rediscovery_failure())

    def test_a_staged_candidate_is_never_used_before_the_follower_agrees(self) -> None:
        self.prime()
        self.net.hold_kinds.add("rediscovery_result")
        self._split_the_catalog_views()
        self.leader.cell.request_rediscovery(actor="operator")
        self.net.pump()
        result = self.leader.cell.handle_event(ClockTickEvent(self.now))
        self.assertEqual(result.universe_generation, 1)
        self.assertFalse(result.ready, "a staged rediscovery still blocks entry")
        self.feed_quotes(sequence=8)
        self.assertEqual(self.attempt_payloads(), [])
        # Releasing the refusal rolls the candidate back rather than committing.
        self.net.hold_kinds.discard("rediscovery_result")
        self.net.release_held()
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.universe_generation(), 1)


class ForeignRouteAttemptTests(PairCellTestCase):
    """Durable state reused under a new route must never act on the old one."""

    def _rebuild_on_a_new_route(self, side: _Side, route_id: str) -> PairExecutionCell:
        side.cell.close()
        side.journal.close()
        side.route = RouteAssignment(
            route_id=route_id,
            role=cast(pair_cell.Role, side.role),
            leader_worker_id=LEADER,
            follower_worker_id=FOLLOWER,
        )
        side.journal = FaultyJournal(self.tmp / f"{side.worker_id}-journal.db", self.trace, side.worker_id)
        side.scheduler = DeadlineAwareTraderRpcScheduler()
        side.cell = side._build()
        self.net.register(side.worker_id, side.cell)
        return side.cell

    def test_an_unresolved_attempt_from_another_route_is_never_adopted(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        attempt_id = self.last_attempt().attempt_id
        self.assertEqual(len(self.leader.mt5.positions), 1)

        self.net.route_id = "route-0002"
        cell = self._rebuild_on_a_new_route(self.leader, "route-0002")
        result = cell.recover()
        self.assertEqual(result.route_id, "route-0002")
        self.assertIsNone(result.attempt_id, "the foreign attempt is not adopted")
        self.assertEqual(cell.foreign_route_attempt(), attempt_id)
        self.assertTrue(result.needs_human)
        self.assertIn("route-0001", cast(str, result.needs_human_reason))
        self.assertFalse(result.ready)

    def test_a_foreign_route_attempt_never_relays_containment_on_the_new_route(self) -> None:
        self.prime()
        self.net.drop_kinds.add("leg_status")
        self.feed_quotes()
        self.assertEqual(len(self.leader.mt5.positions), 1)
        closes_before = len(self.leader.close_requests())

        self.net.route_id = "route-0002"
        marker = self.net.mark()
        cell = self._rebuild_on_a_new_route(self.leader, "route-0002")
        cell.recover()
        cell.handle_event(self.leader.facts(self.now))
        cell.handle_event(ClockTickEvent(self.now))
        self.net.pump()
        self.assertEqual(
            len(self.leader.close_requests()), closes_before, "foreign exposure is not guessed at"
        )
        self.assertEqual(len(self.leader.mt5.positions), 1)
        self.assertEqual(
            self.net.kinds_since(marker, LEADER),
            [],
            "no leg status or containment is relayed onto the new route",
        )

    def test_the_new_route_never_inherits_the_old_desired_or_active_state(self) -> None:
        self.run_entry()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "ACTIVE")
        self.net.route_id = "route-0002"
        cell = self._rebuild_on_a_new_route(self.leader, "route-0002")
        result = cell.recover()
        self.assertEqual(result.desired_state, "NONE")
        self.assertNotEqual(result.state, "ACTIVE")
        self.assertIsNone(result.attempt_id)
        self.assertFalse(result.pair_confirmed)

    def test_a_terminal_attempt_from_another_route_is_harmless_history(self) -> None:
        self.prime()
        self.leader.mt5.reject = True
        self.follower.mt5.reject = True
        self.feed_quotes()
        self.net.pump()
        self.tick()
        self.assertEqual(self.leader.cell.handle_event(ClockTickEvent(self.now)).state, "EMPTY")

        self.net.route_id = "route-0002"
        cell = self._rebuild_on_a_new_route(self.leader, "route-0002")
        result = cell.recover()
        self.assertIsNone(cell.foreign_route_attempt())
        self.assertFalse(result.needs_human)
        self.assertIsNone(result.attempt_id)

    def test_an_attempt_is_stored_under_the_route_that_created_it(self) -> None:
        self.run_entry()
        attempt = self.last_attempt()
        self.assertEqual(attempt.route_id, ROUTE_ID)
        cell = self.leader.restart()
        result = cell.recover()
        self.assertEqual(result.attempt_id, attempt.attempt_id, "the same route still adopts it")
        self.assertIsNone(cell.foreign_route_attempt())


class LegacySchemaMigrationTests(PairCellTestCase):
    """A database written by the shipped HEAD revision must open and migrate."""

    #: Verbatim from ``git show HEAD:abt/pair_cell.py``.
    HEAD_SCHEMA = """
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

    LEGACY_ATTEMPT = "legacy-attempt-1"
    LEGACY_SYMBOL = "EURUSD"

    def legacy_database(self, name: str = "legacy-cell.db") -> Path:
        """One database exactly as the shipped revision would have left it."""

        import sqlite3

        path = self.tmp / name
        connection = sqlite3.connect(path)
        connection.executescript(self.HEAD_SCHEMA)
        # An unresolved attempt in the *old* payload shape: no route_id, and
        # per-leg symbols instead of one shared symbol name.
        connection.execute(
            "INSERT INTO cell_attempts (attempt_id, payload_hash, payload, created_at, terminal)"
            " VALUES (?, ?, ?, ?, 0)",
            (
                self.LEGACY_ATTEMPT,
                "0" * 64,
                '{"attempt_id": "legacy-attempt-1", "leader_symbol": "EURUSD",'
                ' "follower_symbol": "EURUSD.r", "lots": "2"}',
                "2026-03-04T13:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO cell_leg_state (attempt_id, payload, updated_at) VALUES (?, ?, ?)",
            (self.LEGACY_ATTEMPT, '{"attempt_id": "legacy-attempt-1", "role": "leader"}', "2026-03-04T13:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO cell_desired_state (id, desired_state, attempt_id, updated_at)"
            " VALUES (1, 'ACTIVE', ?, ?)",
            (self.LEGACY_ATTEMPT, "2026-03-04T13:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO cell_product_quarantine (symbol, offending_worker_id, attempt_id,"
            " receipt, quarantined_at) VALUES (?, ?, ?, ?, ?)",
            (
                self.LEGACY_SYMBOL,
                LEADER,
                self.LEGACY_ATTEMPT,
                '{"retcode": 10021}',
                "2026-03-04T12:00:00+00:00",
            ),
        )
        connection.execute(
            "INSERT INTO cell_product_quarantine_release (symbol, offending_worker_id,"
            " attempt_id, released_at) VALUES (?, ?, ?, ?)",
            ("GBPUSD", LEADER, "legacy-attempt-0", "2026-03-04T11:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO cell_product_release_marker (symbol, marker) VALUES (?, ?)",
            ("GBPUSD", "2026-03-04T11:00:00+00:00"),
        )
        connection.execute(
            "INSERT INTO cell_transitions (attempt_id, event, detail, at) VALUES (?, ?, ?, ?)",
            (self.LEGACY_ATTEMPT, "entry_sent", "legacy", "2026-03-04T13:00:00+00:00"),
        )
        connection.commit()
        connection.close()
        return path

    def open_on_legacy(self, path: Path) -> PairExecutionCell:
        cell = PairExecutionCell(
            worker_id=LEADER,
            route=_route("leader"),
            db_path=path,
            relay=self.net.adapter(LEADER),
            mt5=self.leader.mt5,
            local_risk_limits=LEADER_RISK,
            effect_journal=self.leader.journal,
            scheduler=self.leader.scheduler,
            monotonic=self.clock,
        )
        self.addCleanup(cell.close)
        return cell

    def test_a_head_schema_database_opens_and_migrates_in_place(self) -> None:
        import sqlite3

        path = self.legacy_database()
        applied = pair_cell.migrate_pair_cell_database(path)
        self.assertEqual(
            set(applied),
            {
                "cell_attempts.route_id",
                "cell_desired_state.pair_confirmed",
                "cell_product_quarantine.product_id",
                "cell_product_quarantine_release.product_id",
                "cell_product_release_marker.product_id",
                "cell_product_quarantine.universe_generation",
            },
        )
        self.assertEqual(
            pair_cell.migrate_pair_cell_database(path), [], "migration is idempotent"
        )
        connection = sqlite3.connect(path)
        self.addCleanup(connection.close)
        self.assertEqual(
            connection.execute(
                "SELECT product_id, offending_worker_id, receipt, universe_generation"
                " FROM cell_product_quarantine"
            ).fetchall(),
            [(self.LEGACY_SYMBOL, LEADER, '{"retcode": 10021}', None)],
        )
        self.assertEqual(
            connection.execute(
                "SELECT product_id FROM cell_product_quarantine_release"
            ).fetchall(),
            [("GBPUSD",)],
        )
        self.assertEqual(
            connection.execute("SELECT product_id, marker FROM cell_product_release_marker").fetchall(),
            [("GBPUSD", "2026-03-04T11:00:00+00:00")],
        )
        self.assertEqual(
            connection.execute("SELECT pair_confirmed FROM cell_desired_state").fetchall(), [(0,)]
        )
        self.assertEqual(
            connection.execute("SELECT route_id FROM cell_attempts").fetchall(), [(None,)]
        )

    def test_constructing_a_cell_on_a_head_schema_database_succeeds(self) -> None:
        cell = self.open_on_legacy(self.legacy_database())
        result = cell.recover()
        self.assertEqual(result.route_id, ROUTE_ID)
        self.assertEqual(result.role, "leader")
        # The legacy attempt names no route this Worker owns, so it is isolated
        # rather than adopted, parsed, or contained on the new route.
        self.assertIsNone(result.attempt_id)
        self.assertEqual(cell.foreign_route_attempt(), self.LEGACY_ATTEMPT)
        self.assertTrue(result.needs_human)
        self.assertIn("no recorded route", cast(str, result.needs_human_reason))
        self.assertEqual(result.desired_state, "NONE")
        self.assertEqual(self.leader.close_requests(), [])
        self.assertEqual(self.net.kinds(LEADER), [])

    def test_legacy_quarantine_and_audit_rows_survive_the_migration(self) -> None:
        cell = self.open_on_legacy(self.legacy_database())
        self.assertEqual(cell.quarantined_products(), (self.LEGACY_SYMBOL,))
        self.assertIsNone(cell.quarantine_generation(self.LEGACY_SYMBOL))
        self.assertEqual(cell.quarantine_release_marker("GBPUSD"), "2026-03-04T11:00:00+00:00")
        self.assertIn(
            "entry_sent", [row["event"] for row in cell.transition_history()]
        )

    def test_a_legacy_quarantine_can_still_be_released_by_an_operator(self) -> None:
        cell = self.open_on_legacy(self.legacy_database())
        cell.recover()
        cell.handle_event(
            QuarantineReleaseEvent(
                product_id=self.LEGACY_SYMBOL,
                actor="operator",
                reason="legacy entry cleared",
                observed_at=self.now,
            )
        )
        self.assertEqual(cell.quarantined_products(), ())

    def test_durable_quarantine_reads_migrate_before_answering(self) -> None:
        path = self.legacy_database("unpaired-cell.db")
        # An unpaired Worker reads durable state without constructing a cell;
        # a legacy column name must never look like "nothing is quarantined".
        self.assertEqual(
            pair_cell.durable_quarantined_products(path), (self.LEGACY_SYMBOL,)
        )
        self.assertEqual(pair_cell.durable_quarantined_products(self.tmp / "absent.db"), ())

    def test_a_fresh_database_needs_no_migration_at_all(self) -> None:
        self.prime()
        self.assertEqual(
            pair_cell.migrate_pair_cell_database(self.tmp / f"{LEADER}-cell.db"), []
        )
        self.assertEqual(
            pair_cell.durable_quarantined_products(self.tmp / f"{LEADER}-cell.db"), ()
        )


class LegacyQuarantineScopeTests(PairCellTestCase):
    """A migrated bare-symbol quarantine covers every product on that symbol.

    Migration renames the column but must never invent a derived identity, so
    the surviving key is a bare symbol.  Gating therefore treats it as
    symbol-wide until an authenticated operator releases it.
    """

    QUARANTINED_SYMBOL = SYMBOL

    def setUp(self) -> None:
        super().setUp()
        self.add_second_product()
        self._reopen_leader_on_a_legacy_quarantine()

    def _reopen_leader_on_a_legacy_quarantine(self) -> None:
        import sqlite3

        self.leader.cell.close()
        path = self.tmp / f"{LEADER}-cell.db"
        for suffix in ("", "-wal", "-shm"):
            candidate = Path(str(path) + suffix)
            if candidate.exists():
                candidate.unlink()
        connection = sqlite3.connect(path)
        connection.executescript(LegacySchemaMigrationTests.HEAD_SCHEMA)
        connection.execute(
            "INSERT INTO cell_product_quarantine (symbol, offending_worker_id, attempt_id,"
            " receipt, quarantined_at) VALUES (?, ?, ?, ?, ?)",
            (
                self.QUARANTINED_SYMBOL,
                LEADER,
                "legacy-attempt-9",
                '{"retcode": 10021}',
                "2026-03-04T12:00:00+00:00",
            ),
        )
        connection.commit()
        connection.close()
        self.leader.cell = self.leader._build()
        self.net.register(LEADER, self.leader.cell)

    def test_the_migrated_key_is_reported_as_symbol_wide(self) -> None:
        self.assertEqual(
            self.leader.cell.symbol_wide_quarantines(), (self.QUARANTINED_SYMBOL,)
        )
        self.assertEqual(self.leader.cell.quarantined_products(), (self.QUARANTINED_SYMBOL,))

    def test_a_legacy_symbol_quarantine_blocks_its_derived_product(self) -> None:
        self.prime()
        product = self.product_id()
        self.assertTrue(is_derived_product_identity(product))
        self.assertNotEqual(product, self.QUARANTINED_SYMBOL)
        self.assertTrue(self.leader.cell.is_quarantined(product))
        self.assertIn(
            "symbol-wide quarantine on EURUSD",
            cast(str, self.leader.cell.quarantine_reason(product)),
        )
        self.feed_quotes(symbol=SYMBOL)
        self.assertEqual(self.leader.cell.entry_candidates(), [])
        self.assertEqual(self.attempt_payloads(), [], "the derived product must stay blocked")
        self.assertEqual(self.leader.entry_requests(), [])

    def test_the_follower_also_refuses_an_attempt_covered_symbol_wide(self) -> None:
        from dataclasses import replace

        self.prime()
        self.warm(OTHER_SYMBOL)
        self.net.hold_kinds = {"attempt"}
        self.feed_quotes(symbol=OTHER_SYMBOL)
        other = self.last_attempt()
        # The pair-wide propagation put the legacy key on the follower too.
        self.assertIn(self.QUARANTINED_SYMBOL, self.follower.cell.quarantined_products())
        self.deliver_attempt(
            replace(other, product_id=self.product_id(SYMBOL), symbol=SYMBOL)
        )
        self.assertEqual(self.follower.entry_requests(), [])
        self.assertIn(
            "symbol-wide quarantine on EURUSD",
            " ".join(str(row["detail"]) for row in self.follower.cell.transition_history()),
        )

    def test_a_legacy_symbol_quarantine_never_blocks_another_symbol(self) -> None:
        self.prime()
        self.warm(OTHER_SYMBOL)
        other = self.product_id(OTHER_SYMBOL)
        self.assertFalse(self.leader.cell.is_quarantined(other))
        self.assertIsNone(self.leader.cell.quarantine_reason(other))
        self.feed_quotes(symbol=OTHER_SYMBOL)
        attempt = self.last_attempt()
        self.assertEqual(attempt.product_id, other)
        self.assertEqual(attempt.symbol, OTHER_SYMBOL)
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_an_authenticated_release_restores_the_whole_symbol(self) -> None:
        self.prime()
        product = self.product_id()
        self.assertTrue(self.leader.cell.is_quarantined(product))
        for side in (self.leader, self.follower):
            side.cell.handle_event(
                QuarantineReleaseEvent(
                    product_id=self.QUARANTINED_SYMBOL,
                    actor="operator",
                    reason="broker restored prices",
                    observed_at=self.now,
                )
            )
        self.assertEqual(self.leader.cell.quarantined_products(), ())
        self.assertEqual(self.leader.cell.symbol_wide_quarantines(), ())
        self.assertFalse(self.leader.cell.is_quarantined(product))
        # The release keeps its marker and its audit record.
        self.assertIsNotNone(
            self.leader.cell.quarantine_release_marker(self.QUARANTINED_SYMBOL)
        )
        self.assertIn(
            "product_quarantine_released",
            [row["event"] for row in self.leader.cell.transition_history()],
        )
        self.feed_quotes(symbol=SYMBOL)
        attempt = self.last_attempt()
        self.assertEqual(attempt.product_id, product)
        self.assertEqual(len(self.leader.entry_requests()), 1)
        self.assertEqual(len(self.follower.entry_requests()), 1)

    def test_a_released_symbol_quarantine_is_not_reintroduced_by_the_peer(self) -> None:
        self.prime()
        product = self.product_id()
        self.assertIn(self.QUARANTINED_SYMBOL, self.follower.cell.quarantined_products())
        # Only the leader releases; the follower keeps advertising the record.
        self.leader.cell.handle_event(
            QuarantineReleaseEvent(
                product_id=self.QUARANTINED_SYMBOL,
                actor="operator",
                reason="broker restored prices",
                observed_at=self.now,
            )
        )
        self.assertEqual(self.leader.cell.quarantined_products(), ())
        for _ in range(3):
            self.follower.cell.handle_event(ClockTickEvent(self.now))
            self.net.pump()
            self.tick()
        self.assertEqual(
            self.leader.cell.quarantined_products(),
            (),
            "a peer advertisement must not resurrect a released quarantine",
        )
        self.assertFalse(self.leader.cell.is_quarantined(product))

    def test_an_exact_derived_quarantine_stays_exact(self) -> None:
        self.prime()
        self.warm(OTHER_SYMBOL)
        other = self.product_id(OTHER_SYMBOL)
        self.leader.mt5.reject_next_entry_retcode = _PRICE_OFF
        self.feed_quotes(symbol=OTHER_SYMBOL)
        self.net.pump()
        self.tick()
        self.assertIn(other, self.leader.cell.quarantined_products())
        self.assertNotIn(other, self.leader.cell.symbol_wide_quarantines())
        # An exact identity never covers a different product on that symbol.
        self.assertIsNone(
            self.leader.cell.quarantine_reason(f"{OTHER_SYMBOL}:0000000000000000")
        )
        self.assertTrue(self.leader.cell.is_quarantined(other))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
