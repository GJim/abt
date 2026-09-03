"""Unit tests for the Pair Execution Cell <-> Worker session production wiring
(``abt.worker.pair_cell_adapter``).

They cover the revised Worker-facing model end to end without a live
controller: the optional tunables-only configuration file and its
role-specific authority, the frozen startup **balance**, Worker-initiated
pairing (role declaration, available-follower listing, interactive and
non-interactive selection, non-TTY behaviour, automatic follower acceptance
and reasoned refusal), the durable route's precedence over startup flags,
Initial Compatible Product Discovery through the *production* catalog reader,
explicit rediscovery, the ``UNPAIRING`` state with state-version bound
assertions, quarantine release keyed on derived product identity, realized
per-leg loss attribution, and one complete pairing -> discovery -> immediate
entry -> ``ACTIVE`` -> close -> safe unpair lifecycle.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
import json
from pathlib import Path
import shutil
import sqlite3
import tempfile
from typing import cast
import unittest
from uuid import uuid4

from abt.pair_cell import PROTOCOL_VERSION as CELL_ENVELOPE_VERSION
from abt.pair_cell import (
    BrokerClockCalibration,
    CatalogSummary,
    RealizedPnLEvent,
    RouteAssignment,
    discover_compatible_products,
)
from abt.trader_protocol import (
    pair_cell_control_message_adapter,
    pair_cell_relay_envelope_adapter,
)
from abt.worker.effect_journal import WorkerEffectJournal
from abt.worker.pair_cell_adapter import (
    ROUTE_SCOPED_TABLES,
    ATTEMPT_SCOPED_TABLES,
    PRESERVED_SAFETY_TABLES,
    DurableRouteRecord,
    FrozenAcceptance,
    FrozenBudget,
    PairCellConfig,
    PairCellPollingConfig,
    PairCellRuntime,
    PairCellStartupOptions,
    StartupBalance,
    WorkerPairCellMT5,
    WorkerPairCellRelay,
    _pair_cell_broker_request,
    _payload_hash,
    default_pair_cell_config,
    evaluate_follower_acceptance,
    load_durable_route,
    load_frozen_acceptance,
    load_frozen_budget,
    load_pair_cell_config,
    parse_pair_cell_config,
    read_catalog_entries,
    read_local_quote,
    read_readiness_facts,
    read_startup_balance,
    sample_broker_clock_calibration,
    save_durable_route,
    save_frozen_acceptance,
    save_frozen_budget,
    unwrap_pair_relay_envelope,
    worker_risk_limits,
)
from abt.worker.reconciliation import WorkerEnrollmentError
from abt.worker.scheduler import DeadlineAwareTraderRpcScheduler


LEADER = "worker-leader"
FOLLOWER = "worker-follower"
SYMBOL = "EURUSD"
# 2024-01-02 14:00Z is 09:00 in New York: comfortably before the 16:00 flatten
# cutoff, so no test accidentally depends on the cutoff path.
START = datetime(2024, 1, 2, 14, 0, tzinfo=UTC)


class Clock:
    """A deterministic UTC clock shared by a Worker and its MT5 fake."""

    def __init__(self, start: datetime = START) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float = 0.05) -> datetime:
        self.now = self.now + timedelta(seconds=max(seconds, 0.001))
        return self.now


def symbol_spec(**overrides: object) -> dict[str, object]:
    spec: dict[str, object] = {
        "digits": 5,
        "point": 0.00001,
        "trade_tick_size": 0.00001,
        "trade_contract_size": 100000.0,
        "volume_min": 0.01,
        "volume_step": 0.01,
        "volume_max": 50.0,
        "currency_base": "EUR",
        "currency_profit": "USD",
        "trade_calc_mode": 0,
        "trade_mode": 4,
        "filling_mode": 1,
        "trade_tick_value": 1.0,
        "trade_stops_level": 5,
        "trade_freeze_level": 0,
        "visible": True,
    }
    spec.update(overrides)
    return spec


class FakeMT5:
    """A deterministic MT5-shaped fake using the exact request conventions
    ``abt.worker.reconciliation`` already relies on."""

    ORDER_TYPE_BUY = 0
    ORDER_TYPE_SELL = 1
    POSITION_TYPE_BUY = 0
    POSITION_TYPE_SELL = 1
    TRADE_ACTION_DEAL = 1
    TRADE_ACTION_SLTP = 2
    TRADE_ACTION_REMOVE = 3
    ORDER_FILLING_FOK = 0
    ORDER_FILLING_IOC = 1
    TRADE_RETCODE_DONE = 10009
    TRADE_RETCODE_PLACED = 10008

    def __init__(
        self,
        *,
        clock: Clock,
        login: int = 555111,
        server: str = "Broker-Demo",
        bid: float = 1.10000,
        ask: float = 1.10010,
        balance: float = 10_000.0,
        currency: str = "USD",
        symbols: dict[str, dict[str, object]] | None = None,
    ) -> None:
        self.clock = clock
        self.account: dict[str, object] = {
            "login": login,
            "server": server,
            "margin_free": 100_000.0,
            "balance": balance,
            "equity": balance + 5_000.0,
            "currency": currency,
        }
        self.terminal: dict[str, object] = {
            "connected": True, "trade_allowed": True, "tradeapi_disabled": False
        }
        self.orders: list[dict[str, object]] = []
        self.positions: list[dict[str, object]] = []
        self.symbols: dict[str, dict[str, object]] = symbols or {SYMBOL: symbol_spec()}
        self.prices: dict[str, tuple[float, float]] = {
            name: (bid, ask) for name in self.symbols
        }
        self.margin_per_lot = 1000.0
        self.margin_failures: set[str] = set()
        self.catalog_available = True
        self.frozen_symbols: set[str] = set()
        self.deals_by_position: dict[int, list[dict[str, object]]] = {}
        self.sent_requests: list[dict[str, object]] = []
        self.symbol_info_calls: list[str] = []
        self.reject = False
        #: MT5 ``10021 TRADE_RETCODE_PRICE_OFF``, the durable quarantine trigger.
        self.price_off = False
        self._next_ticket = 5000

    # -- reads ------------------------------------------------------------- #

    def account_info(self) -> object:
        return dict(self.account)

    def terminal_info(self) -> object:
        return dict(self.terminal)

    def orders_get(self) -> object:
        return [dict(order) for order in self.orders]

    def positions_get(self) -> object:
        return [dict(position) for position in self.positions]

    def symbols_get(self) -> object:
        if not self.catalog_available:
            return None
        return [{"name": name} for name in sorted(self.symbols)]

    def symbol_info(self, symbol: str) -> object:
        self.symbol_info_calls.append(symbol)
        spec = self.symbols.get(symbol)
        return None if spec is None else dict(spec)

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return True

    def symbol_info_tick(self, symbol: str) -> object:
        prices = self.prices.get(symbol)
        if prices is None or symbol in self.frozen_symbols:
            return None
        bid, ask = prices
        return {
            "bid": bid,
            "ask": ask,
            "last": (bid + ask) / 2,
            "time_msc": int(self.clock().timestamp() * 1000),
        }

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> object:
        if symbol in self.margin_failures:
            return None
        return self.margin_per_lot * volume

    def history_deals_get(self, *, position: int) -> object:
        return [dict(deal) for deal in self.deals_by_position.get(position, [])] or None

    # -- writes ------------------------------------------------------------ #

    def _realized(self, position: dict[str, object]) -> float:
        volume = float(cast(float, position["volume"]))
        opened = float(cast(float, position["price_open"]))
        bid, ask = self.prices[str(position["symbol"])]
        if position["type"] == self.ORDER_TYPE_BUY:
            return round((bid - opened) * volume * 100_000, 2)
        return round((opened - ask) * volume * 100_000, 2)

    def order_send(self, request: dict[str, object]) -> object:
        self.sent_requests.append(dict(request))
        if self.reject:
            return {"retcode": 10013, "comment": "rejected"}
        action = request.get("action")
        if self.price_off and action == self.TRADE_ACTION_DEAL and "position" not in request:
            return {"retcode": 10021, "comment": "No prices"}
        if action == self.TRADE_ACTION_DEAL and "position" not in request:
            ticket = self._next_ticket
            self._next_ticket += 1
            self.positions.append(
                {
                    "ticket": ticket,
                    "symbol": request["symbol"],
                    "type": request["type"],
                    "volume": request["volume"],
                    "price_open": request["price"],
                    "sl": request.get("sl", 0.0),
                    "tp": request.get("tp", 0.0),
                }
            )
            self.deals_by_position[ticket] = [{"entry": 0, "profit": 0.0}]
            return {"retcode": self.TRADE_RETCODE_DONE, "position": ticket, "price": request["price"]}
        if action == self.TRADE_ACTION_SLTP:
            for position in self.positions:
                if position["ticket"] == request["position"]:
                    position["sl"], position["tp"] = request["sl"], request["tp"]
            return {"retcode": self.TRADE_RETCODE_DONE}
        if action == self.TRADE_ACTION_DEAL and "position" in request:
            ticket = cast(int, request["position"])
            closed = [p for p in self.positions if p["ticket"] == ticket]
            self.positions = [p for p in self.positions if p["ticket"] != ticket]
            for position in closed:
                self.deals_by_position.setdefault(ticket, []).append(
                    {"entry": 1, "profit": self._realized(position)}
                )
            return {"retcode": self.TRADE_RETCODE_DONE}
        return {"retcode": self.TRADE_RETCODE_DONE}


# --------------------------------------------------------------------------- #
# An in-process stand-in for the controller's Worker-facing pairing surface
# --------------------------------------------------------------------------- #


class FakeController:
    """Mirrors the controller's pairing control plane and opaque relay.

    It deliberately implements exactly the message shapes
    ``abt.controlplane.service`` produces, so these tests exercise the real
    adapter protocol without a live server; ``tests/test_pair_cell_integration``
    proves the same wiring against the genuine FastAPI controller.
    """

    def __init__(self) -> None:
        self.sessions: dict[str, FakeSession] = {}
        self.declarations: dict[str, str | None] = {}
        self.logins: dict[str, tuple[int, str]] = {}
        self.reservations: dict[str, dict[str, str]] = {}
        self.routes: dict[str, dict[str, object]] = {}
        self.state_versions: dict[tuple[str, str], int] = {}
        self.assertions: dict[tuple[str, str], int] = {}
        self.refuse_next_proposal: str | None = None
        #: Message-loss controls, so a test can reproduce exactly the one-shot
        #: deliveries that a disconnect around pairing would swallow.  Each
        #: maps a Worker to the message types dropped for it; an empty set
        #: drops every message of that kind.
        self.drop_pushes_to: dict[str, set[str]] = {}
        self.drop_results_to: dict[str, set[str]] = {}
        self.strip_acceptance = False
        #: Relay payload kinds dropped per sending Worker; an empty set drops
        #: every kind.  Used to lose exactly one opaque publication.
        self.drop_relay_kinds_from: dict[str, set[str]] = {}
        self.counter = 0

    @staticmethod
    def _dropped(rules: dict[str, set[str]], worker_id: str, message_type: object) -> bool:
        types = rules.get(worker_id)
        return types is not None and (not types or message_type in types)

    # -- registry ---------------------------------------------------------- #

    def register(self, session: FakeSession, *, login: int = 1, server: str = "srv") -> None:
        self.sessions[session.worker_id] = session
        self.logins[session.worker_id] = (login, server)

    def disconnect(self, worker_id: str) -> None:
        self.sessions.pop(worker_id, None)

    def route_for(self, worker_id: str) -> dict[str, object] | None:
        for route in self.routes.values():
            if worker_id in (route["leader_worker_id"], route["follower_worker_id"]):
                return route
        return None

    def _reserved(self, worker_id: str) -> bool:
        return any(
            worker_id in (reservation["leader"], reservation["follower"])
            for reservation in self.reservations.values()
        )

    def _view(self, route: dict[str, object], worker_id: str | None = None) -> dict[str, object]:
        view: dict[str, object] = {
            "route_id": route["route_id"],
            "leader_worker_id": route["leader_worker_id"],
            "follower_worker_id": route["follower_worker_id"],
            "state": route["state"],
            "created_at": "2024-01-02T14:00:00+00:00",
            "unpairing_at": route.get("unpairing_at"),
        }
        if worker_id is not None:
            view["role"] = "leader" if worker_id == route["leader_worker_id"] else "follower"
        return view

    def _push(self, worker_id: str, message: dict[str, object]) -> None:
        if self._dropped(self.drop_pushes_to, worker_id, message.get("type")):
            return
        if self.strip_acceptance and message.get("type") == "pair_cell_route_established":
            message = {key: value for key, value in message.items() if key != "acceptance"}
        session = self.sessions.get(worker_id)
        if session is not None:
            session.pushes.append(message)

    def _reply(self, worker_id: str, message: dict[str, object]) -> None:
        if self._dropped(self.drop_results_to, worker_id, message.get("type")):
            return
        session = self.sessions.get(worker_id)
        if session is not None:
            session.results.append(message)

    # -- control messages --------------------------------------------------- #

    def handle(self, worker_id: str, message: dict[str, object]) -> None:
        kind = str(message["type"])
        reply: dict[str, object] = {
            "type": f"{kind}_result", "request_id": message["request_id"], "accepted": True
        }
        handler = getattr(self, f"_on_{kind}")
        handler(worker_id, message, reply)

    def _on_pair_cell_role(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        role = message.get("role")
        self.declarations[worker_id] = cast(str | None, role)
        route = self.route_for(worker_id)
        reply["role"] = role or "follower"
        reply["declared_role"] = role
        reply["route"] = None if route is None else self._view(route, worker_id)
        self._reply(worker_id, reply)

    def _on_pair_cell_available_followers(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        reply["followers"] = [
            {
                "worker_id": candidate,
                "login": self.logins.get(candidate, (0, ""))[0],
                "server": self.logins.get(candidate, (0, ""))[1],
                "declared_role": self.declarations.get(candidate),
                "declared_at": None,
            }
            for candidate in sorted(self.sessions)
            if candidate != worker_id
            and self.declarations.get(candidate) in (None, "follower")
            and self.route_for(candidate) is None
            and not self._reserved(candidate)
        ]
        self._reply(worker_id, reply)

    def _on_pair_cell_pairing_proposal(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        follower = str(message["follower_worker_id"])
        refusal = self.refuse_next_proposal
        if refusal is not None:
            self.refuse_next_proposal = None
            self._reply(worker_id, {**reply, "accepted": False, "reason": refusal})
            return
        if (
            follower not in self.sessions
            or self.route_for(follower) is not None
            or self.route_for(worker_id) is not None
            or self._reserved(follower)
            or self._reserved(worker_id)
        ):
            self._reply(
                worker_id,
                {**reply, "accepted": False, "reason": "The selected follower Worker is unavailable."},
            )
            return
        self.counter += 1
        proposal_id = f"proposal-{self.counter}"
        self.reservations[proposal_id] = {"leader": worker_id, "follower": follower}
        reply["proposal_id"] = proposal_id
        reply["follower_worker_id"] = follower
        reply["expires_at"] = "2024-01-02T14:00:30+00:00"
        reply["timeout_seconds"] = 30
        self._reply(worker_id, reply)
        self._push(
            follower,
            {
                "type": "pair_cell_pairing_proposed",
                "proposal_id": proposal_id,
                "leader_worker_id": worker_id,
                "follower_worker_id": follower,
                "expires_at": "2024-01-02T14:00:30+00:00",
                "timeout_seconds": 30,
            },
        )

    def _on_pair_cell_pairing_decision(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        proposal_id = str(message["proposal_id"])
        reservation = self.reservations.get(proposal_id)
        if reservation is None or reservation["follower"] != worker_id:
            self._reply(
                worker_id,
                {**reply, "accepted": False, "reason": "No live pairing reservation names this Worker."},
            )
            return
        leader = reservation["leader"]
        del self.reservations[proposal_id]
        if not message.get("accepted"):
            reply["outcome"] = "refused"
            reply["route"] = None
            self._reply(worker_id, reply)
            self._push(
                leader,
                {
                    "type": "pair_cell_pairing_refused",
                    "proposal_id": proposal_id,
                    "follower_worker_id": worker_id,
                    "reason": message.get("reason"),
                },
            )
            return
        self.counter += 1
        route_id = f"route-{self.counter}"
        route: dict[str, object] = {
            "route_id": route_id,
            "leader_worker_id": leader,
            "follower_worker_id": worker_id,
            "state": "ACTIVE",
            "unpairing_at": None,
        }
        self.routes[route_id] = route
        view = self._view(route)
        reply["outcome"] = "paired"
        reply["route"] = view
        self._reply(worker_id, reply)
        # The controller forwards the follower's opaque acceptance payload to
        # the reserved leader unchanged, and never parses it.
        self._push(
            leader,
            {
                "type": "pair_cell_route_established",
                "route": view,
                "acceptance": {
                    "proposal_id": proposal_id,
                    "from_worker_id": worker_id,
                    "payload_hash": message.get("payload_hash"),
                    "payload": message.get("acceptance_payload"),
                },
            },
        )
        self._push(worker_id, {"type": "pair_cell_route_established", "route": view})

    def _on_pair_cell_route_sync(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        route = self.route_for(worker_id)
        reply["route"] = None if route is None else self._view(route, worker_id)
        self._reply(worker_id, reply)

    def _on_pair_cell_unpair(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        route = self.routes.get(str(message["route_id"]))
        if route is None or worker_id not in (route["leader_worker_id"], route["follower_worker_id"]):
            self._reply(
                worker_id, {**reply, "accepted": False, "reason": "This Worker is not on that route."}
            )
            return
        action = str(message["action"])
        if action == "enter":
            route["state"] = "UNPAIRING"
            route["unpairing_at"] = "2024-01-02T14:00:10+00:00"
        else:
            if route["state"] != "UNPAIRING":
                self._reply(
                    worker_id, {**reply, "accepted": False, "reason": "This route is not unpairing."}
                )
                return
            route["state"] = "ACTIVE"
            route["unpairing_at"] = None
        for key in [key for key in self.assertions if key[0] == route["route_id"]]:
            del self.assertions[key]
        view = self._view(route)
        reply["action"] = action
        reply["route"] = view
        self._reply(worker_id, reply)
        for target in (route["leader_worker_id"], route["follower_worker_id"]):
            self._push(
                cast(str, target),
                {"type": "pair_cell_route_state_changed", "route": view, "requested_by": worker_id},
            )

    def _on_pair_cell_state_version(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        route_id = str(message["route_id"])
        route = self.routes.get(route_id)
        if route is None or worker_id not in (route["leader_worker_id"], route["follower_worker_id"]):
            self._reply(
                worker_id, {**reply, "accepted": False, "reason": "This Worker is not on that route."}
            )
            return
        version = cast(int, message["state_version"])
        self.state_versions[(route_id, worker_id)] = version
        invalidated = self.assertions.pop((route_id, worker_id), None) not in (None, version)
        reply["route_id"] = route_id
        reply["state_version"] = version
        reply["assertion_invalidated"] = invalidated
        self._reply(worker_id, reply)

    def _on_pair_cell_unpair_assertion(
        self, worker_id: str, message: dict[str, object], reply: dict[str, object]
    ) -> None:
        route_id = str(message["route_id"])
        route = self.routes.get(route_id)
        if route is None or worker_id not in (route["leader_worker_id"], route["follower_worker_id"]):
            self._reply(
                worker_id, {**reply, "accepted": False, "reason": "This Worker is not on that route."}
            )
            return
        if route["state"] != "UNPAIRING":
            self._reply(
                worker_id,
                {**reply, "accepted": False, "reason": "The route is not UNPAIRING."},
            )
            return
        version = cast(int, message["state_version"])
        self.state_versions[(route_id, worker_id)] = version
        self.assertions[(route_id, worker_id)] = version
        fresh = {
            asserted_worker
            for (asserted_route, asserted_worker), asserted in self.assertions.items()
            if asserted_route == route_id
            and self.state_versions.get((route_id, asserted_worker)) == asserted
        }
        removed = fresh == {route["leader_worker_id"], route["follower_worker_id"]}
        reply["route_id"] = route_id
        reply["state_version"] = version
        reply["asserted_worker_ids"] = sorted(fresh)
        reply["route_removed"] = removed
        self._reply(worker_id, reply)
        if removed:
            del self.routes[route_id]
            for target in (route["leader_worker_id"], route["follower_worker_id"]):
                self._push(
                    cast(str, target),
                    {"type": "pair_cell_route_removed", "route_id": route_id, "reason": "safe_unpair"},
                )

    # -- opaque relay -------------------------------------------------------- #

    def relay(self, from_worker_id: str, envelope: dict[str, object], request_id: str) -> None:
        session = self.sessions.get(from_worker_id)
        # The wire envelope must be exactly what the controller validates.
        parsed = pair_cell_relay_envelope_adapter.validate_python(envelope)
        kind = cast(dict, envelope["payload"]).get("kind")
        if self._dropped(self.drop_relay_kinds_from, from_worker_id, kind):
            if session is not None:
                session.relay_acks.append({"request_id": request_id, "accepted": True})
            return
        route = self.routes.get(parsed.route.route_id)
        if (
            route is None
            or route["leader_worker_id"] != parsed.route.leader_worker_id
            or route["follower_worker_id"] != parsed.route.follower_worker_id
        ):
            if session is not None:
                session.relay_acks.append(
                    {"request_id": request_id, "accepted": False, "reason": "no live route"}
                )
            return
        peer = self.sessions.get(parsed.to_worker_id)
        if peer is None:
            if session is not None:
                session.relay_acks.append(
                    {"request_id": request_id, "accepted": False, "reason": "Peer Worker is disconnected."}
                )
            return
        peer.relay_inbox.append(json.loads(json.dumps(envelope)))
        if session is not None:
            session.relay_acks.append({"request_id": request_id, "accepted": True})


class FakeSession:
    """The subset of ``AuthenticatedWorkerSession`` the adapter drives."""

    def __init__(self, worker_id: str, controller: FakeController, *, login: int = 1, server: str = "srv") -> None:
        self.worker_id = worker_id
        self._controller = controller
        self._scheduler = DeadlineAwareTraderRpcScheduler()
        self.results: list[dict[str, object]] = []
        self.pushes: list[dict[str, object]] = []
        self.relay_inbox: list[dict[str, object]] = []
        self.relay_acks: list[dict[str, object]] = []
        self.quarantine_releases: list[dict[str, object]] = []
        self.quarantine_results: list[dict[str, object]] = []
        self.control_messages: list[dict[str, object]] = []
        self.sent_envelopes: list[dict[str, object]] = []
        controller.register(self, login=login, server=server)

    @property
    def trader_rpc_scheduler(self) -> DeadlineAwareTraderRpcScheduler:
        return self._scheduler

    def dispatch_scheduler_outcome(self, outcome: object) -> None:
        return None

    # -- relay -------------------------------------------------------------- #

    def send_pair_relay(self, envelope: dict[str, object], *, request_id: str) -> None:
        self.sent_envelopes.append(envelope)
        self._controller.relay(self.worker_id, envelope, request_id)

    def drain_pair_relay_envelopes(self) -> list[dict[str, object]]:
        envelopes, self.relay_inbox = self.relay_inbox, []
        return envelopes

    def drain_pair_relay_acks(self) -> list[dict[str, object]]:
        acks, self.relay_acks = self.relay_acks, []
        return acks

    def drain_pair_cell_quarantine_releases(self) -> list[dict[str, object]]:
        releases, self.quarantine_releases = self.quarantine_releases, []
        return releases

    def send_pair_cell_quarantine_release_result(self, *, request_id: str, symbol: str, outcome: str) -> None:
        self.quarantine_results.append(
            {"request_id": request_id, "symbol": symbol, "outcome": outcome}
        )

    # -- pairing control plane ---------------------------------------------- #

    def drain_pair_cell_results(self) -> list[dict[str, object]]:
        results, self.results = self.results, []
        return results

    def drain_pair_cell_pushes(self) -> list[dict[str, object]]:
        pushes, self.pushes = self.pushes, []
        return pushes

    def _control(self, message: dict[str, object]) -> str:
        request_id = str(uuid4())
        outgoing = {**message, "request_id": request_id, "protocol_version": 1}
        # Every outbound control message must be exactly what the controller
        # validates, so a shape drift fails here rather than silently in a
        # fake that is more permissive than production.
        pair_cell_control_message_adapter.validate_python(outgoing)
        self.control_messages.append(outgoing)
        self._controller.handle(self.worker_id, outgoing)
        return request_id

    def declare_pair_cell_role(self, role: str | None) -> str:
        return self._control({"type": "pair_cell_role", "role": role})

    def request_available_pair_followers(self) -> str:
        return self._control({"type": "pair_cell_available_followers"})

    def propose_pair_cell_pairing(self, follower_worker_id: str) -> str:
        return self._control(
            {"type": "pair_cell_pairing_proposal", "follower_worker_id": follower_worker_id}
        )

    def send_pair_cell_pairing_decision(
        self,
        *,
        proposal_id: str,
        accepted: bool,
        acceptance_payload: dict[str, object] | None = None,
        payload_hash: str | None = None,
        reason: str | None = None,
    ) -> str:
        message: dict[str, object] = {
            "type": "pair_cell_pairing_decision",
            "proposal_id": proposal_id,
            "accepted": accepted,
        }
        if accepted:
            message["acceptance_payload"] = acceptance_payload
            message["payload_hash"] = payload_hash
        elif reason is not None:
            message["reason"] = reason
        return self._control(message)

    def request_pair_cell_route_sync(self) -> str:
        return self._control({"type": "pair_cell_route_sync"})

    def send_pair_cell_unpair(self, *, route_id: str, action: str) -> str:
        return self._control({"type": "pair_cell_unpair", "route_id": route_id, "action": action})

    def send_pair_cell_state_version(self, *, route_id: str, state_version: int) -> str:
        return self._control(
            {"type": "pair_cell_state_version", "route_id": route_id, "state_version": state_version}
        )

    def send_pair_cell_unpair_assertion(self, *, route_id: str, state_version: int) -> str:
        return self._control(
            {"type": "pair_cell_unpair_assertion", "route_id": route_id, "state_version": state_version}
        )


# --------------------------------------------------------------------------- #
# Worker harness
# --------------------------------------------------------------------------- #


def polling(**overrides: object) -> PairCellPollingConfig:
    values: dict[str, object] = {
        "quote_interval": timedelta(milliseconds=1),
        "readiness_interval": timedelta(milliseconds=1),
        "broker_snapshot_interval": timedelta(milliseconds=1),
        "sizing_refresh_interval": timedelta(hours=1),
        "clock_sample_interval": timedelta(milliseconds=1),
    }
    values.update(overrides)
    return PairCellPollingConfig(**values)  # type: ignore[arg-type]


class Worker:
    """One Worker: its MT5 fake, effect journal, session and runtime."""

    def __init__(
        self,
        test: unittest.TestCase,
        *,
        worker_id: str,
        controller: FakeController,
        directory: Path,
        clock: Clock,
        mt5: FakeMT5,
        config: PairCellConfig | None = None,
        options: PairCellStartupOptions = PairCellStartupOptions(),
        polling_config: PairCellPollingConfig | None = None,
    ) -> None:
        self.worker_id = worker_id
        self.clock = clock
        self.mt5 = mt5
        self.directory = directory
        self.session = FakeSession(worker_id, controller)
        self.journal = WorkerEffectJournal(directory / f"{worker_id}.effects.sqlite")
        test.addCleanup(self.journal.close)
        self.db_path = directory / f"{worker_id}.paircell.sqlite"
        self.options = options
        self.config = config if config is not None else default_pair_cell_config()
        self.polling = polling_config or polling()
        self.results: list[object] = []
        self.runtime = self._build()
        test.addCleanup(self.close)

    @property
    def state(self) -> str | None:
        return None if not self.results else cast(object, self.results[-1]).state  # type: ignore[attr-defined]

    def _build(self) -> PairCellRuntime:
        return PairCellRuntime(
            worker_id=self.worker_id,
            db_path=self.db_path,
            session=self.session,  # type: ignore[arg-type]
            mt5=self.mt5,
            effect_journal=self.journal,
            recovery_epoch=f"{self.worker_id}-epoch",
            login=cast(int, self.mt5.account["login"]),
            server=cast(str, self.mt5.account["server"]),
            config=self.config,
            options=self.options,
            polling=self.polling,
            now=self.clock,
            sleep=lambda seconds: self.clock.advance(seconds),
        )

    def restart(self, *, options: PairCellStartupOptions | None = None) -> PairCellRuntime:
        self.runtime.close()
        if options is not None:
            self.options = options
        self.runtime = self._build()
        return self.runtime

    def close(self) -> None:
        try:
            self.runtime.close()
        except Exception:
            pass


def pump(*workers: Worker, rounds: int = 8, seconds: float = 0.05) -> None:
    """Advance every Worker's loop a bounded, deterministic number of times."""

    for _ in range(rounds):
        for worker in workers:
            worker.clock.advance(seconds)
            worker.runtime.pump(worker.clock())


def pair(*workers: Worker, rounds: int = 40) -> None:
    pump(*workers, rounds=rounds)


# --------------------------------------------------------------------------- #
# Configuration: optional, tunables-only, role-specific authority
# --------------------------------------------------------------------------- #


class PairCellConfigurationTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tmp, True)
        self.directory = Path(self._tmp)

    def test_absent_configuration_file_synthesizes_the_documented_defaults(self) -> None:
        """No file at all is the ordinary deployment: defaults, not "disabled"."""

        config = load_pair_cell_config(self.directory / "missing.json")
        self.assertEqual({}, dict(config.risk_overrides))
        self.assertEqual({}, dict(config.shared_policy))
        self.assertTrue(config.allow_live)
        self.assertEqual("20", config.daily_loss_warning_threshold_usd)
        limits = worker_risk_limits(StartupBalance(amount_usd="2500"), config)
        self.assertEqual("2500", limits.strategy_budget_usd)
        self.assertEqual("0.10", limits.maximum_margin_fraction)
        self.assertEqual("0.03", limits.daily_loss_fraction)
        self.assertEqual("0.02", limits.trade_loss_fraction)
        self.assertEqual("40", limits.maximum_loss_per_trade_usd)

    def test_a_route_or_trader_or_built_product_key_is_a_startup_error(self) -> None:
        for key, value in (
            ("route", {"leader_worker_id": "a"}),
            ("route_id", "route-1"),
            ("trader_id", "trader-1"),
            ("built_products", []),
            ("eligible_products", ["fx:EURUSD"]),
        ):
            with self.subTest(key=key):
                with self.assertRaises(WorkerEnrollmentError) as raised:
                    parse_pair_cell_config({key: value})
                self.assertIn(key, str(raised.exception))

    def test_unknown_settings_are_a_startup_error(self) -> None:
        with self.assertRaises(WorkerEnrollmentError):
            parse_pair_cell_config({"leader_volume": "1"})

    def test_allow_live_must_be_a_boolean(self) -> None:
        with self.assertRaises(WorkerEnrollmentError):
            parse_pair_cell_config({"allow_live": "false"})

    def test_daily_loss_warning_threshold_is_a_local_non_negative_usd_setting(self) -> None:
        config = parse_pair_cell_config({"daily_loss_warning_threshold_usd": "12.50"})
        self.assertEqual("12.5", config.daily_loss_warning_threshold_usd)
        self.assertIsNone(config.role_authority_error("follower"))
        for value in ("-1", "NaN", True):
            with self.subTest(value=value):
                with self.assertRaises(WorkerEnrollmentError):
                    parse_pair_cell_config({"daily_loss_warning_threshold_usd": value})

    def test_a_follower_permissible_file_is_valid_for_either_role(self) -> None:
        config = parse_pair_cell_config(
            {"maximum_margin_fraction": "0.2", "daily_loss_fraction": "0.01", "allow_live": False}
        )
        self.assertIsNone(config.role_authority_error("follower"))
        self.assertIsNone(config.role_authority_error("leader"))
        self.assertFalse(config.allow_live)
        self.assertFalse(config.declares_shared_policy)

    def test_a_follower_file_may_not_set_shared_policy(self) -> None:
        for key, value in (
            ("mode", "shadow"),
            ("entry_edge_points", "9"),
            ("quote_max_age_seconds", 2.0),
            ("quote_max_skew_seconds", 2.0),
            ("follower_confirmation_timeout_seconds", 9.0),
            ("trading_blackout_start_ny", "16:30"),
            ("trading_blackout_end_ny", "18:30"),
            ("maximum_holding_seconds", 60.0),
        ):
            with self.subTest(key=key):
                config = parse_pair_cell_config({key: value})
                self.assertTrue(config.declares_shared_policy)
                reason = config.role_authority_error("follower")
                self.assertIsNotNone(reason)
                self.assertIn(key, cast(str, reason))
                self.assertIsNone(config.role_authority_error("leader"))

    def test_removed_flatten_setting_is_rejected(self) -> None:
        with self.assertRaises(WorkerEnrollmentError) as raised:
            parse_pair_cell_config({"flatten_at_ny": "16:00"})
        self.assertIn("unknown settings flatten_at_ny", str(raised.exception))

    def test_unusable_risk_numbers_are_a_startup_error(self) -> None:
        with self.assertRaises(WorkerEnrollmentError):
            parse_pair_cell_config({"maximum_margin_fraction": "-1"})

    def test_a_malformed_file_is_a_startup_error(self) -> None:
        path = self.directory / "pair.json"
        path.write_text("{not json", encoding="utf-8")
        with self.assertRaises(WorkerEnrollmentError):
            load_pair_cell_config(path)

    def test_a_present_file_is_loaded_from_disk(self) -> None:
        path = self.directory / "pair.json"
        path.write_text(json.dumps({"entry_edge_points": "7", "allow_live": False}), encoding="utf-8")
        config = load_pair_cell_config(path)
        self.assertEqual({"entry_edge_points": "7"}, dict(config.shared_policy))
        self.assertFalse(config.allow_live)


class StartupBalanceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()

    def test_the_default_budget_is_the_startup_balance_not_the_equity(self) -> None:
        mt5 = FakeMT5(clock=self.clock, balance=4_000.0)
        mt5.account["equity"] = 9_999.0
        balance = read_startup_balance(mt5)
        self.assertEqual("4000", balance.amount_usd)
        self.assertEqual(
            "4000", worker_risk_limits(balance, default_pair_cell_config()).strategy_budget_usd
        )

    def test_a_non_usd_currency_fails_closed(self) -> None:
        mt5 = FakeMT5(clock=self.clock, currency="EUR")
        with self.assertRaises(WorkerEnrollmentError):
            read_startup_balance(mt5)

    def test_a_non_positive_balance_fails_closed(self) -> None:
        for balance in (0.0, -25.0):
            with self.subTest(balance=balance):
                mt5 = FakeMT5(clock=self.clock, balance=balance)
                with self.assertRaises(WorkerEnrollmentError):
                    read_startup_balance(mt5)

    def test_an_unreadable_or_malformed_account_fails_closed(self) -> None:
        mt5 = FakeMT5(clock=self.clock)
        mt5.account_info = lambda: None  # type: ignore[method-assign]
        with self.assertRaises(WorkerEnrollmentError):
            read_startup_balance(mt5)
        broken = FakeMT5(clock=self.clock)
        broken.account.pop("balance")
        with self.assertRaises(WorkerEnrollmentError):
            read_startup_balance(broken)

    def test_no_margin_headroom_beyond_the_margin_fraction(self) -> None:
        limits = worker_risk_limits(StartupBalance(amount_usd="10000"), default_pair_cell_config())
        self.assertEqual(Decimal("1000.00"), limits.margin_budget_usd)


# --------------------------------------------------------------------------- #
# Initial Compatible Product Discovery, read through the production adapter
# --------------------------------------------------------------------------- #


def _summary(worker_id: str, mt5: FakeMT5, *, epoch: int = 1, version: int = 1) -> CatalogSummary:
    return CatalogSummary(
        worker_id=worker_id,
        publisher_epoch=epoch,
        version=version,
        entries=read_catalog_entries(mt5),
    )


class DiscoveryCatalogTests(unittest.TestCase):
    """Known literal catalogs, read by the production ``read_catalog_entries``."""

    def setUp(self) -> None:
        self.clock = Clock()

    def _pair(
        self,
        leader_symbols: dict[str, dict[str, object]],
        follower_symbols: dict[str, dict[str, object]],
    ) -> tuple[CatalogSummary, CatalogSummary]:
        leader = FakeMT5(clock=self.clock, symbols=leader_symbols)
        follower = FakeMT5(clock=self.clock, symbols=follower_symbols)
        return _summary(LEADER, leader), _summary(FOLLOWER, follower)

    def test_trade_mode_four_is_applied_to_each_catalog_before_the_intersection(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(), "GBPUSD": symbol_spec()},
            {SYMBOL: symbol_spec(trade_mode=3), "GBPUSD": symbol_spec()},
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertTrue(outcome.admitted)
        self.assertEqual(("GBPUSD",), tuple(product.symbol for product in outcome.products))

    def test_a_missing_or_non_integer_or_boolean_trade_mode_invalidates_the_catalog(self) -> None:
        for value in (None, "4", True):
            with self.subTest(trade_mode=value):
                spec = symbol_spec()
                if value is None:
                    spec.pop("trade_mode")
                else:
                    spec["trade_mode"] = value
                leader, follower = self._pair({SYMBOL: spec}, {SYMBOL: symbol_spec()})
                outcome = discover_compatible_products(leader, follower)
                self.assertFalse(outcome.admitted)
                self.assertIn("invalid catalog", outcome.reason)

    def test_symbol_intersection_is_exact_with_no_suffix_normalization(self) -> None:
        leader, follower = self._pair({SYMBOL: symbol_spec()}, {"EURUSD.pro": symbol_spec()})
        outcome = discover_compatible_products(leader, follower)
        self.assertFalse(outcome.admitted)
        self.assertEqual("no compatible symbol survived discovery", outcome.reason)

    def test_hard_specification_inequality_excludes_the_candidate(self) -> None:
        for field, value in (
            ("trade_calc_mode", 1),
            ("trade_contract_size", 50_000.0),
            ("volume_min", 0.1),
            ("volume_step", 0.1),
        ):
            with self.subTest(field=field):
                leader, follower = self._pair(
                    {SYMBOL: symbol_spec()}, {SYMBOL: symbol_spec(**{field: value})}
                )
                outcome = discover_compatible_products(leader, follower)
                self.assertFalse(outcome.admitted)

    def test_a_candidate_needs_both_long_and_short(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(order_mode=1)}, {SYMBOL: symbol_spec(order_mode=1)}
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertFalse(outcome.admitted)
        self.assertIn(
            "bidirectional trading is unavailable", dict(outcome.excluded).get(SYMBOL, "")
        )

    def test_shared_fok_is_preferred_over_shared_ioc(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(filling_mode=3)}, {SYMBOL: symbol_spec(filling_mode=3)}
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertEqual("FOK", outcome.products[0].filling_mode)

    def test_shared_ioc_is_used_when_fok_is_not_shared(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(filling_mode=3)}, {SYMBOL: symbol_spec(filling_mode=2)}
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertEqual("IOC", outcome.products[0].filling_mode)

    def test_a_candidate_without_a_shared_filling_mode_is_excluded(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(filling_mode=1)}, {SYMBOL: symbol_spec(filling_mode=2)}
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertFalse(outcome.admitted)
        self.assertIn("no shared FOK or IOC filling mode", dict(outcome.excluded).get(SYMBOL, ""))

    def test_currency_digits_point_and_tick_size_may_differ(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec()},
            {
                SYMBOL: symbol_spec(
                    currency_base="XXX",
                    currency_profit="EUR",
                    digits=3,
                    point=0.001,
                    trade_tick_size=0.0005,
                    volume_max=20.0,
                )
            },
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertTrue(outcome.admitted)
        product = outcome.products[0]
        # Canonical execution point is the coarser broker's; common volume max
        # is the smaller of the two.
        self.assertEqual("0.001", product.canonical_point)
        self.assertEqual("20", product.volume_max)

    def test_product_identity_is_deterministic_across_repeated_runs(self) -> None:
        leader, follower = self._pair({SYMBOL: symbol_spec()}, {SYMBOL: symbol_spec()})
        first = discover_compatible_products(leader, follower).products[0]
        second = discover_compatible_products(leader, follower).products[0]
        self.assertEqual(first.product_id, second.product_id)
        self.assertTrue(first.product_id.startswith(f"{SYMBOL}:"))

    def test_an_unavailable_catalog_fails_closed(self) -> None:
        mt5 = FakeMT5(clock=self.clock)
        mt5.catalog_available = False
        with self.assertRaises(WorkerEnrollmentError):
            read_catalog_entries(mt5)

    def test_unusable_catalog_economics_exclude_only_that_candidate(self) -> None:
        leader, follower = self._pair(
            {SYMBOL: symbol_spec(), "GBPUSD": symbol_spec()},
            {SYMBOL: symbol_spec(point="not-a-number"), "GBPUSD": symbol_spec()},
        )
        outcome = discover_compatible_products(leader, follower)
        self.assertEqual(("GBPUSD",), tuple(product.symbol for product in outcome.products))

    def test_a_restricted_read_is_not_used_to_hide_the_broker_catalog(self) -> None:
        """The whole catalog is always read, so both summaries stay symmetric."""

        mt5 = FakeMT5(clock=self.clock, symbols={SYMBOL: symbol_spec(), "GBPUSD": symbol_spec()})
        entries = read_catalog_entries(mt5)
        self.assertEqual(("EURUSD", "GBPUSD"), tuple(sorted(entry.name for entry in entries)))


# --------------------------------------------------------------------------- #
# Relay wire format and MT5 request translation
# --------------------------------------------------------------------------- #


class RelayAndBrokerTranslationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.controller = FakeController()
        self.session = FakeSession(LEADER, self.controller)
        self.route = RouteAssignment(
            route_id="route-9", role="leader", leader_worker_id=LEADER, follower_worker_id=FOLLOWER
        )

    def test_the_wire_envelope_is_route_scoped_and_carries_no_trader_identity(self) -> None:
        relay = WorkerPairCellRelay(self.session, route=self.route)
        relay.send(
            {
                "protocol_version": CELL_ENVELOPE_VERSION,
                "kind": "quote",
                "route_id": "route-9",
                "from_worker_id": LEADER,
                "from_role": "leader",
                "to_worker_id": FOLLOWER,
                "payload": {"symbol": SYMBOL},
            }
        )
        envelope = self.session.sent_envelopes[0]
        parsed = pair_cell_relay_envelope_adapter.validate_python(envelope)
        self.assertEqual("route-9", parsed.route.route_id)
        self.assertNotIn("trader_id", json.dumps(envelope))
        inner = unwrap_pair_relay_envelope(envelope)
        assert inner is not None
        self.assertEqual("quote", inner["kind"])
        self.assertEqual("route-9", inner["route_id"])
        self.assertEqual({"symbol": SYMBOL}, inner["payload"])

    def test_a_malformed_delivery_is_ignored_rather_than_raising(self) -> None:
        self.assertIsNone(unwrap_pair_relay_envelope({"payload": "nope"}))
        self.assertIsNone(unwrap_pair_relay_envelope({"payload": {"kind": 1, "payload": {}}}))

    def test_a_relay_send_failure_never_propagates_into_the_cell(self) -> None:
        def explode(envelope: dict[str, object], *, request_id: str) -> None:
            raise WorkerEnrollmentError("socket is gone")

        self.session.send_pair_relay = explode  # type: ignore[method-assign]
        relay = WorkerPairCellRelay(self.session, route=self.route)
        relay.send(
            {
                "kind": "quote",
                "route_id": "route-9",
                "from_worker_id": LEADER,
                "to_worker_id": FOLLOWER,
                "payload": {},
            }
        )
        self.assertTrue(relay.last_send_failed)

    def test_order_calc_margin_uses_the_proven_trader_translation(self) -> None:
        mt5 = FakeMT5(clock=self.clock)
        adapter = WorkerPairCellMT5(mt5)
        self.assertEqual(
            2000.0,
            adapter.order_calc_margin(
                {"direction": "LONG", "symbol": SYMBOL, "volume": "2", "price": "1.1"}
            ),
        )
        mt5.margin_failures.add(SYMBOL)
        with self.assertRaises(WorkerEnrollmentError):
            adapter.order_calc_margin(
                {"direction": "LONG", "symbol": SYMBOL, "volume": "1", "price": "1.1"}
            )

    def test_the_abstract_broker_payload_becomes_a_genuine_mt5_request(self) -> None:
        mt5 = FakeMT5(clock=self.clock)
        market = _pair_cell_broker_request(
            mt5,
            {
                "type": "market",
                "symbol": SYMBOL,
                "volume": "0.4",
                "direction": "LONG",
                "filling_mode": "FOK",
                "sl": "1.0990",
                "tp": "1.1010",
            },
        )
        self.assertEqual(FakeMT5.TRADE_ACTION_DEAL, market["action"])
        self.assertEqual(FakeMT5.ORDER_TYPE_BUY, market["type"])
        self.assertEqual(0.4, market["volume"])
        protect = _pair_cell_broker_request(
            mt5,
            {"type": "modify_sl_tp", "symbol": SYMBOL, "position": "77", "sl": "1.0", "tp": "1.2"},
        )
        self.assertEqual(FakeMT5.TRADE_ACTION_SLTP, protect["action"])
        self.assertEqual(77, protect["position"])
        cancel = _pair_cell_broker_request(mt5, {"type": "cancel", "ticket": "42"})
        self.assertEqual({"action": FakeMT5.TRADE_ACTION_REMOVE, "order": 42}, cancel)
        with self.assertRaises(WorkerEnrollmentError):
            _pair_cell_broker_request(mt5, {"type": "teleport"})

    def test_a_disabled_trading_terminal_refuses_the_broker_write(self) -> None:
        mt5 = FakeMT5(clock=self.clock)
        mt5.terminal["trade_allowed"] = False
        with self.assertRaises(WorkerEnrollmentError):
            WorkerPairCellMT5(mt5).order_send(
                {"type": "market", "symbol": SYMBOL, "volume": "1", "direction": "LONG", "filling_mode": "FOK"}
            )


class LocalEvidenceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.clock = Clock()
        self.mt5 = FakeMT5(clock=self.clock)

    def test_three_strictly_advancing_samples_calibrate_the_broker_clock(self) -> None:
        sample = sample_broker_clock_calibration(
            self.mt5,
            SYMBOL,
            now=self.clock,
            sleep=lambda seconds: self.clock.advance(seconds),
            interval=timedelta(milliseconds=1),
        )
        assert sample is not None
        self.assertTrue(sample.calibration.usable)

    def test_a_frozen_feed_never_calibrates(self) -> None:
        self.assertIsNone(
            sample_broker_clock_calibration(
                self.mt5,
                SYMBOL,
                now=self.clock,
                sleep=lambda seconds: None,
                interval=timedelta(milliseconds=1),
            )
        )

    def test_a_quote_uses_millisecond_broker_time(self) -> None:
        quote = read_local_quote(
            self.mt5,
            product_id="p",
            symbol=SYMBOL,
            recovery_epoch="e",
            sequence=1,
            now=self.clock,
            calibration=BrokerClockCalibration(status="calibrated"),
        )
        self.assertEqual(
            int(self.clock().timestamp() * 1000), int(quote.broker_time.timestamp() * 1000)
        )

    def test_readiness_facts_never_invent_an_empty_account(self) -> None:
        self.mt5.positions_get = lambda: None  # type: ignore[method-assign]
        facts = read_readiness_facts(
            cast(object, self.mt5), login=555111, server="Broker-Demo", now=self.clock()  # type: ignore[arg-type]
        )
        self.assertIsNone(facts.positions)
        self.assertTrue(facts.terminal_connected)


# --------------------------------------------------------------------------- #
# Worker-initiated pairing
# --------------------------------------------------------------------------- #


def pump_until(
    workers: Sequence[Worker], predicate, *, rounds: int = 240, seconds: float = 0.05
) -> bool:
    for _ in range(rounds):
        for worker in workers:
            worker.clock.advance(seconds)
            result = worker.runtime.pump(worker.clock())
            if result is not None:
                worker.results.append(result)
        if predicate():
            return True
    return False


class PairingTestCase(unittest.TestCase):
    """Two Workers, one in-process controller, one shared deterministic clock."""

    def setUp(self) -> None:
        self._tmp = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self._tmp, True)
        self.directory = Path(self._tmp)
        self.controller = FakeController()
        self.clock = Clock()

    def worker(
        self,
        worker_id: str,
        *,
        bid: float = 1.10000,
        ask: float = 1.10010,
        balance: float = 10_000.0,
        config: PairCellConfig | None = None,
        options: PairCellStartupOptions = PairCellStartupOptions(),
        symbols: dict[str, dict[str, object]] | None = None,
        login: int | None = None,
    ) -> Worker:
        mt5 = FakeMT5(
            clock=self.clock,
            login=login if login is not None else abs(hash(worker_id)) % 900000 + 1000,
            server=f"Broker-{worker_id}",
            bid=bid,
            ask=ask,
            balance=balance,
            symbols=symbols,
        )
        return Worker(
            self,
            worker_id=worker_id,
            controller=self.controller,
            directory=self.directory,
            clock=self.clock,
            mt5=mt5,
            config=config,
            options=options,
        )

    def paired(
        self,
        *,
        leader_options: PairCellStartupOptions | None = None,
        leader_config: PairCellConfig | None = None,
        follower_config: PairCellConfig | None = None,
        follower_balance: float = 4_000.0,
    ) -> tuple[Worker, Worker]:
        follower = self.worker(
            FOLLOWER, bid=1.10100, ask=1.10110, balance=follower_balance, config=follower_config
        )
        leader = self.worker(
            LEADER,
            bid=1.10000,
            ask=1.10010,
            balance=10_000.0,
            config=leader_config,
            options=leader_options
            or PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER),
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=30,
            ),
            f"pairing did not complete (leader={leader.runtime.pairing_diagnostic!r},"
            f" follower={follower.runtime.pairing_diagnostic!r})",
        )
        return leader, follower

    def test_unpair_without_a_route_exits_after_the_controller_confirms_none_exists(self) -> None:
        worker = self.worker(
            LEADER,
            options=PairCellStartupOptions(unpair=True),
        )

        self.assertTrue(
            pump_until([worker], lambda: worker.runtime.should_exit, rounds=12),
            "the idempotent safe-unpair did not exit after the controller confirmed no route",
        )
        self.assertFalse(worker.runtime.enabled)
        self.assertEqual("no Pair Execution Cell route exists to unpair", worker.runtime.pairing_diagnostic)


class WorkerInitiatedPairingTests(PairingTestCase):
    def test_a_worker_started_with_no_role_becomes_an_available_follower(self) -> None:
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=5)
        self.assertEqual("available", worker.runtime.pairing_state)
        self.assertFalse(worker.runtime.enabled)
        declaration = worker.session.control_messages[0]
        self.assertEqual("pair_cell_role", declaration["type"])
        self.assertIsNone(declaration["role"])

    def test_a_leader_with_follower_worker_id_pairs_without_any_prompt(self) -> None:
        def never(_followers: Sequence[Mapping[str, object]]) -> str | None:
            raise AssertionError("an unattended leader must never prompt")

        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        leader = self.worker(
            LEADER,
            options=PairCellStartupOptions(
                role="leader", follower_worker_id=FOLLOWER, interactive=True, select_follower=never
            ),
        )
        self.assertTrue(
            pump_until([leader, follower], lambda: leader.runtime.enabled and follower.runtime.enabled, rounds=30)
        )
        self.assertEqual("leader", leader.runtime.role)
        self.assertEqual("follower", follower.runtime.role)
        self.assertEqual(leader.runtime.route_id, follower.runtime.route_id)
        self.assertNotIn(
            "pair_cell_available_followers",
            [message["type"] for message in leader.session.control_messages],
        )

    def test_a_leader_pairs_from_an_interactive_selection(self) -> None:
        shown: list[Sequence[Mapping[str, object]]] = []

        def select(followers: Sequence[Mapping[str, object]]) -> str | None:
            shown.append(followers)
            return str(followers[0]["worker_id"])

        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        leader = self.worker(
            LEADER,
            options=PairCellStartupOptions(role="leader", interactive=True, select_follower=select),
        )
        self.assertTrue(
            pump_until([leader, follower], lambda: leader.runtime.enabled and follower.runtime.enabled, rounds=30)
        )
        self.assertEqual([FOLLOWER], [str(entry["worker_id"]) for entry in shown[0]])
        self.assertEqual("leader", leader.runtime.role)

    def test_a_non_tty_leader_without_the_flag_stays_authenticated_and_unpaired(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        leader = self.worker(LEADER, options=PairCellStartupOptions(role="leader", interactive=False))
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=10)
        self.assertEqual("unpaired", leader.runtime.pairing_state)
        self.assertFalse(leader.runtime.enabled)
        self.assertIsNone(leader.runtime.role)
        self.assertIn("--follower-worker-id", leader.runtime.pairing_diagnostic)
        # It never selects implicitly and never downgrades itself to follower.
        self.assertNotIn(
            "pair_cell_pairing_proposal",
            [message["type"] for message in leader.session.control_messages],
        )
        self.assertEqual({}, self.controller.routes)

    def test_an_interactive_leader_shown_an_empty_list_stays_unpaired_and_may_relist(self) -> None:
        selected: list[str] = []

        def select(followers: Sequence[Mapping[str, object]]) -> str | None:
            selected.append(str(followers[0]["worker_id"]))
            return selected[-1]

        leader = self.worker(
            LEADER,
            options=PairCellStartupOptions(role="leader", interactive=True, select_follower=select),
        )
        pump_until([leader], lambda: leader.runtime.pairing_state == "unpaired", rounds=6)
        self.assertEqual("unpaired", leader.runtime.pairing_state)
        self.assertIn("listed no connected", leader.runtime.pairing_diagnostic)
        self.assertEqual([], selected)

        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        self.assertTrue(leader.runtime.relist_available_followers())
        self.assertTrue(
            pump_until([leader, follower], lambda: leader.runtime.enabled and follower.runtime.enabled, rounds=30)
        )
        self.assertEqual([FOLLOWER], selected)

    def test_a_follower_refuses_when_it_is_not_broker_verified_empty(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        follower.mt5.positions.append({"ticket": 4242, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=12)
        self.assertEqual({}, self.controller.routes)
        self.assertEqual({}, self.controller.reservations)
        self.assertFalse(leader.runtime.enabled)
        self.assertFalse(follower.runtime.enabled)
        self.assertIn("not broker-verified empty", follower.runtime.pairing_diagnostic)
        # A refusal never becomes an implied acceptance and leaves no durable
        # local pairing state on either side.
        self.assertFalse((self.directory / f"{FOLLOWER}.paircell.route.json").exists())
        self.assertFalse((self.directory / f"{LEADER}.paircell.route.json").exists())

    def test_follower_safety_conditions_are_each_an_explicit_refusal(self) -> None:
        facts = read_readiness_facts(
            cast(object, FakeMT5(clock=self.clock)),  # type: ignore[arg-type]
            login=555111,
            server="Broker-Demo",
            now=self.clock(),
        )
        usable = BrokerClockCalibration(status="calibrated")
        balance = StartupBalance(amount_usd="1000")

        def refusal(**overrides: object) -> str | None:
            arguments: dict[str, object] = {
                "unpaired": True,
                "facts": facts,
                "calibration": usable,
                "effect_journal": None,
                "balance": balance,
            }
            arguments.update(overrides)
            return evaluate_follower_acceptance(**arguments)  # type: ignore[arg-type]

        self.assertIsNone(refusal())
        self.assertIn("already appears", cast(str, refusal(unpaired=False)))
        self.assertIn("no local readiness facts", cast(str, refusal(facts=None)))
        self.assertIn(
            "terminal is not usable",
            cast(str, refusal(facts=replace(facts, terminal_connected=False))),
        )
        self.assertIn(
            "read is unavailable", cast(str, refusal(facts=replace(facts, positions=None)))
        )
        self.assertIn(
            "not broker-verified empty",
            cast(str, refusal(facts=replace(facts, positions=[{"ticket": 1}]))),
        )
        self.assertIn(
            "clock calibration",
            cast(str, refusal(calibration=BrokerClockCalibration(status="stale"))),
        )
        self.assertIn("unreadable", cast(str, refusal(balance=None)))

    def test_the_pairing_acceptance_payload_travels_with_the_decision(self) -> None:
        leader, follower = self.paired(
            follower_config=parse_pair_cell_config({"trade_loss_fraction": "0.01"})
        )
        decision = next(
            message
            for message in follower.session.control_messages
            if message["type"] == "pair_cell_pairing_decision"
        )
        self.assertTrue(decision["accepted"])
        payload = cast(dict, decision["acceptance_payload"])
        self.assertEqual(FOLLOWER, payload["worker_id"])
        self.assertEqual("4000", payload["startup_balance_usd"])
        self.assertEqual("USD", payload["account_currency"])
        self.assertEqual("0.01", payload["trade_loss_fraction"])
        self.assertEqual(_payload_hash(payload), decision["payload_hash"])
        # The leader holds the follower's own block verbatim, without waiting
        # for the relayed copy.
        assert leader.runtime.cell is not None
        acceptance = leader.runtime.cell.peer_pairing_acceptance()
        assert acceptance is not None
        self.assertEqual(payload, acceptance.canonical())

    def test_the_leader_route_notice_carries_the_acceptance_and_the_followers_does_not(self) -> None:
        pushes: dict[str, list[dict[str, object]]] = {LEADER: [], FOLLOWER: []}
        original = FakeController._push

        def record(controller: FakeController, worker_id: str, message: dict[str, object]) -> None:
            if message.get("type") == "pair_cell_route_established":
                pushes.setdefault(worker_id, []).append(message)
            original(controller, worker_id, message)

        FakeController._push = record  # type: ignore[method-assign]
        self.addCleanup(setattr, FakeController, "_push", original)
        leader, follower = self.paired()

        leader_notice = pushes[LEADER][0]
        acceptance = cast(dict, leader_notice["acceptance"])
        self.assertEqual(
            {"proposal_id", "from_worker_id", "payload_hash", "payload"}, set(acceptance)
        )
        self.assertEqual(FOLLOWER, acceptance["from_worker_id"])
        self.assertEqual(
            _payload_hash(cast(dict, acceptance["payload"])), acceptance["payload_hash"]
        )
        # The follower's own notice is the route alone.
        self.assertEqual({"type", "route"}, set(pushes[FOLLOWER][0]))
        _ = (leader, follower)

    def test_the_leader_never_copies_a_forwarded_acceptance_it_cannot_verify(self) -> None:
        leader, follower = self.paired()
        cell = leader.runtime.cell
        assert cell is not None
        held = cell.peer_pairing_acceptance()
        assert held is not None
        self.assertEqual("4000", held.startup_balance_usd)

        tampered = dict(held.canonical())
        tampered["startup_balance_usd"] = "999999"
        unverifiable = (
            # its hash does not match its own content
            {
                "proposal_id": held.proposal_id,
                "from_worker_id": FOLLOWER,
                "payload_hash": "not-the-hash",
                "payload": tampered,
            },
            # it names a Worker the controller did not route as the follower
            {
                "proposal_id": held.proposal_id,
                "from_worker_id": LEADER,
                "payload_hash": _payload_hash(tampered),
                "payload": tampered,
            },
            # it is bound to a proposal this leader never made
            {
                "proposal_id": "some-other-proposal",
                "from_worker_id": FOLLOWER,
                "payload_hash": _payload_hash(tampered),
                "payload": tampered,
            },
            {"proposal_id": held.proposal_id, "from_worker_id": FOLLOWER, "payload": "not a mapping"},
        )
        for record in unverifiable:
            with self.subTest(record=record.get("payload_hash")):
                leader.runtime._accept_forwarded_acceptance(record)
                current = cell.peer_pairing_acceptance()
                assert current is not None
                self.assertEqual("4000", current.startup_balance_usd)
        _ = follower

    def test_a_refusal_carries_a_reason_and_no_acceptance_payload(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        follower.mt5.positions.append({"ticket": 5151, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=12)
        decision = next(
            message
            for message in follower.session.control_messages
            if message["type"] == "pair_cell_pairing_decision"
        )
        self.assertFalse(decision["accepted"])
        self.assertNotIn("acceptance_payload", decision)
        self.assertNotIn("payload_hash", decision)
        self.assertIn("not broker-verified empty", cast(str, decision["reason"]))

    def test_a_reservation_conflict_is_deterministic_and_never_waited_out(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        self.controller.refuse_next_proposal = "The selected follower Worker is unavailable."
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=10)
        self.assertEqual("unpaired", leader.runtime.pairing_state)
        self.assertIn("unavailable", leader.runtime.pairing_diagnostic)
        self.assertEqual({}, self.controller.routes)

    def test_a_leader_whose_configuration_exceeds_its_assigned_role_fails_closed(self) -> None:
        """A follower that was handed a leader-authored file never pairs partially."""

        shared = parse_pair_cell_config({"entry_edge_points": "9"})
        follower = self.worker(
            FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0, config=shared
        )
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: follower.runtime.pairing_state == "unpaired", rounds=20)
        self.assertFalse(follower.runtime.enabled)
        self.assertIn("entry_edge_points", follower.runtime.pairing_diagnostic)

    def test_a_worker_without_a_positive_usd_balance_never_pairs(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=0.0)
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=12)
        self.assertEqual({}, self.controller.routes)
        self.assertIn("balance", follower.runtime.pairing_diagnostic)


class DurableRouteAndAuthorityTests(PairingTestCase):
    def test_the_durable_route_copy_records_the_frozen_startup_balance(self) -> None:
        leader, follower = self.paired()
        record = load_durable_route(self.directory / f"{FOLLOWER}.paircell.route.json")
        self.assertIsInstance(record, DurableRouteRecord)
        assert record is not None
        self.assertEqual("follower", record.role)
        self.assertEqual(follower.runtime.route_id, record.route_id)
        self.assertEqual("4000", record.frozen_balance_usd)
        self.assertEqual("USD", record.account_currency)
        self.assertEqual(LEADER, record.leader_worker_id)
        self.assertEqual(FOLLOWER, record.follower_worker_id)
        _ = leader

    def test_an_intraday_balance_change_never_resizes_a_live_pairing(self) -> None:
        leader, follower = self.paired()
        follower.mt5.account["balance"] = 9_999.0
        runtime = follower.restart()
        limits = runtime.declared_risk_limits()
        assert limits is not None
        self.assertEqual("4000", limits.strategy_budget_usd)
        _ = leader

    def test_the_durable_route_wins_over_a_contradicting_role_flag(self) -> None:
        leader, follower = self.paired()
        route_id = follower.runtime.route_id
        sent_before = len(follower.session.control_messages)
        runtime = follower.restart(options=PairCellStartupOptions(role="leader"))
        self.assertTrue(runtime.enabled)
        self.assertEqual("follower", runtime.role)
        self.assertEqual(route_id, runtime.route_id)
        types = [
            message["type"] for message in follower.session.control_messages[sent_before:]
        ]
        # It resumes its assigned role and never re-enters listing or proposal.
        self.assertNotIn("pair_cell_role", types)
        self.assertNotIn("pair_cell_available_followers", types)
        self.assertNotIn("pair_cell_pairing_proposal", types)
        _ = leader

    def test_a_reconnecting_worker_synchronises_the_authoritative_route_immediately(self) -> None:
        leader, follower = self.paired()
        sent_before = len(follower.session.control_messages)
        follower.restart()
        types = [message["type"] for message in follower.session.control_messages[sent_before:]]
        self.assertEqual("pair_cell_route_sync", types[0])
        _ = leader

    def test_a_route_metadata_disagreement_removes_readiness_without_touching_exposure(self) -> None:
        leader, follower = self.paired()
        follower.mt5.positions.append({"ticket": 7007, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        # The controller now names a different route for this Worker.
        route = cast(dict, self.controller.route_for(FOLLOWER))
        del self.controller.routes[cast(str, route["route_id"])]
        renamed = {**route, "route_id": "route-renamed"}
        self.controller.routes["route-renamed"] = renamed
        runtime = follower.restart()
        pump_until([follower], lambda: runtime.cell is not None
                   and runtime.cell.metadata_reconciliation() is not None, rounds=6)
        assert runtime.cell is not None
        self.assertIsNotNone(runtime.cell.metadata_reconciliation())
        self.assertIn("route_id", cast(str, runtime.cell.metadata_reconciliation()))
        # Local exposure is never discarded, closed, hidden or inferred away.
        self.assertEqual(1, len(follower.mt5.positions))
        self.assertTrue(runtime.enabled)
        _ = leader

    def test_the_leader_copies_the_pairing_acceptance_payload_verbatim(self) -> None:
        follower_config = parse_pair_cell_config(
            {"maximum_margin_fraction": "0.25", "maximum_loss_per_trade_usd": "25"}
        )
        leader, follower = self.paired(follower_config=follower_config)
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: follower.runtime.enforced_risk_limits() is not None,
                rounds=120,
            ),
            f"the follower never accepted a policy ({follower.runtime.pairing_diagnostic!r})",
        )
        assert leader.runtime.cell is not None
        acceptance = leader.runtime.cell.peer_pairing_acceptance()
        assert acceptance is not None
        self.assertEqual(FOLLOWER, acceptance.worker_id)
        self.assertEqual("4000", acceptance.startup_balance_usd)
        self.assertEqual("USD", acceptance.account_currency)
        self.assertEqual("0.25", acceptance.maximum_margin_fraction)
        follower_risk = follower.runtime.enforced_risk_limits()
        assert follower_risk is not None
        self.assertTrue(follower_risk.is_verbatim_copy_of(acceptance.risk_limits()))
        leader_risk = leader.runtime.enforced_risk_limits()
        assert leader_risk is not None
        self.assertEqual("10000", leader_risk.strategy_budget_usd)
        self.assertEqual("0.10", leader_risk.maximum_margin_fraction)


class DiscoveryLifecycleTests(PairingTestCase):
    def _discovered(self) -> tuple[Worker, Worker]:
        leader, follower = self.paired(
            leader_config=parse_pair_cell_config({"entry_edge_points": "100000"})
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.universe_generation() is not None
                and follower.runtime.universe_generation() is not None,
                rounds=60,
            ),
            "discovery never installed a universe",
        )
        return leader, follower

    def test_discovery_runs_after_the_route_and_installs_the_first_generation(self) -> None:
        leader, follower = self._discovered()
        self.assertEqual(1, leader.runtime.universe_generation())
        self.assertEqual(1, follower.runtime.universe_generation())
        assert leader.runtime.cell is not None and follower.runtime.cell is not None
        leader_universe = leader.runtime.cell.discovered_universe()
        follower_universe = follower.runtime.cell.discovered_universe()
        assert leader_universe is not None and follower_universe is not None
        self.assertEqual(
            [product.product_id for product in leader_universe.products],
            [product.product_id for product in follower_universe.products],
        )
        self.assertEqual([SYMBOL], [product.symbol for product in leader_universe.products])

    def test_sizing_plans_are_built_from_the_discovered_universe(self) -> None:
        leader, follower = self._discovered()
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.runtime.cell and leader.runtime.cell.sizing_plans()),
                rounds=120,
            )
        )
        assert leader.runtime.cell is not None
        plans = leader.runtime.cell.sizing_plans()
        self.assertEqual({("LONG"), ("SHORT")}, {direction for _, direction in plans})
        # 10 000 * 0.10 / 1 000 = 1 lot for the leader.
        self.assertEqual({"1"}, {plan.local_max_lots for plan in plans.values()})
        _ = follower

    def test_the_hourly_refresh_never_admits_a_newly_appeared_symbol(self) -> None:
        leader, follower = self._discovered()
        for worker in (leader, follower):
            worker.mt5.symbols["GBPUSD"] = symbol_spec()
            worker.mt5.prices["GBPUSD"] = (1.2500, 1.2502)
        self.clock.advance(3_700)
        pump_until([leader, follower], lambda: False, rounds=10)
        assert leader.runtime.cell is not None
        universe = leader.runtime.cell.discovered_universe()
        assert universe is not None
        self.assertEqual(1, universe.universe_generation)
        self.assertEqual([SYMBOL], [product.symbol for product in universe.products])

    def test_an_explicit_rediscovery_installs_a_new_generation_on_the_same_route(self) -> None:
        leader, follower = self._discovered()
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.runtime.cell and leader.runtime.cell.sizing_plans()),
                rounds=120,
            )
        )
        route_id = leader.runtime.route_id
        for worker in (leader, follower):
            worker.mt5.symbols["GBPUSD"] = symbol_spec()
            worker.mt5.prices["GBPUSD"] = (1.2500, 1.2502)
        # Both Workers exchange their current catalogs on the ordinary hourly
        # cycle; the frozen universe is unchanged until a rediscovery says so.
        self.clock.advance(3_700)
        pump_until([leader, follower], lambda: False, rounds=10)
        self.assertEqual(1, leader.runtime.universe_generation())

        leader.runtime.request_rediscovery(actor="operator", reason="new symbol")
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.universe_generation() == 2
                and follower.runtime.universe_generation() == 2,
                rounds=60,
            ),
            "the explicit rediscovery never installed a new universe generation",
        )
        # Rediscovery is not a re-pairing: same route, same roles.
        self.assertEqual(route_id, leader.runtime.route_id)
        self.assertEqual(route_id, follower.runtime.route_id)
        self.assertEqual("leader", leader.runtime.role)
        assert leader.runtime.cell is not None
        universe = leader.runtime.cell.discovered_universe()
        assert universe is not None
        self.assertEqual(
            [SYMBOL, "GBPUSD"], sorted(p.symbol for p in universe.products)
        )

    def test_a_refused_rediscovery_leaves_the_previous_generation_in_place(self) -> None:
        leader, follower = self._discovered()
        leader.mt5.positions.append({"ticket": 3131, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        leader.runtime.request_rediscovery(actor="operator", reason="while not empty")
        pump_until([leader, follower], lambda: False, rounds=10)
        assert leader.runtime.cell is not None
        # The previous generation stays installed rather than being replaced
        # by an empty universe, and the refusal is reported explicitly.
        self.assertEqual(1, leader.runtime.universe_generation())
        self.assertIn(
            "rediscovery refused", cast(str, leader.runtime.cell.last_rediscovery_failure())
        )

    def test_quarantine_release_is_keyed_on_the_derived_product_identity(self) -> None:
        leader, follower = self._discovered()
        assert leader.runtime.cell is not None
        universe = leader.runtime.cell.discovered_universe()
        assert universe is not None
        product_id = universe.products[0].product_id
        self.assertEqual((product_id,), leader.runtime._release_targets(SYMBOL))
        leader.session.quarantine_releases.append(
            {
                "request_id": "release-1",
                "symbol": SYMBOL,
                "actor": "admin",
                "reason": "manual",
                "observed_at": self.clock().isoformat(),
            }
        )
        leader.runtime.drain_relay()
        self.assertEqual(
            [{"request_id": "release-1", "symbol": SYMBOL, "outcome": "applied"}],
            leader.session.quarantine_results,
        )
        _ = follower

    def test_superseded_sizing_plan_versions_are_retained_for_in_flight_attempts(self) -> None:
        leader, follower = self._discovered()
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.runtime.cell and leader.runtime.cell.sizing_plans()),
                rounds=120,
            )
        )
        assert leader.runtime.cell is not None
        before = {plan.version for plan in leader.runtime.cell.sizing_plans().values()}
        leader.mt5.margin_per_lot = 500.0
        self.clock.advance(3_700)
        pump_until([leader, follower], lambda: False, rounds=6)
        after = {plan.version for plan in leader.runtime.cell.sizing_plans().values()}
        self.assertNotEqual(before, after)
        self.assertTrue(set(leader.runtime.cell.retained_plan_versions()) & before)


# --------------------------------------------------------------------------- #
# Safe unpair through the explicit UNPAIRING state
# --------------------------------------------------------------------------- #


class SafeUnpairTests(PairingTestCase):
    def _idle_pair(self) -> tuple[Worker, Worker]:
        """A paired, entry-ready pair whose edge threshold admits nothing."""

        leader, follower = self.paired(
            leader_config=parse_pair_cell_config({"entry_edge_points": "100000"})
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.results and follower.results)
                and cast(object, leader.results[-1]).ready  # type: ignore[attr-defined]
                and cast(object, follower.results[-1]).ready,  # type: ignore[attr-defined]
                rounds=200,
            ),
            "the pair never became entry-ready",
        )
        return leader, follower

    def test_each_worker_publishes_its_local_state_version_to_the_controller(self) -> None:
        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        self.assertIn((route_id, LEADER), self.controller.state_versions)
        self.assertIn((route_id, FOLLOWER), self.controller.state_versions)

    def test_either_worker_may_enter_unpairing_and_both_lose_entry_readiness(self) -> None:
        leader, follower = self._idle_pair()
        # There is no proposer privilege: the follower initiates here.
        self.assertTrue(follower.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.runtime.cell and leader.runtime.cell.route_state() == "UNPAIRING")
                and bool(follower.runtime.cell and follower.runtime.cell.route_state() == "UNPAIRING"),
                rounds=20,
            ),
            "entering UNPAIRING did not take effect on both sides",
        )
        self.assertFalse(cast(object, leader.results[-1]).ready)  # type: ignore[attr-defined]
        self.assertFalse(cast(object, follower.results[-1]).ready)  # type: ignore[attr-defined]
        self.assertIn(
            "UNPAIRING", cast(object, leader.results[-1]).ready_reason  # type: ignore[attr-defined]
        )

    def test_cancelling_unpairing_returns_the_route_to_normal_operation(self) -> None:
        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        # A route leaves UNPAIRING only by an explicit cancel, so one side is
        # held non-terminal to stop the unpair completing on its own.
        follower.mt5.positions.append({"ticket": 9191, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        leader.runtime.request_safe_unpair()
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(follower.runtime.cell and follower.runtime.cell.route_state() == "UNPAIRING"),
                rounds=20,
            )
        )
        self.assertTrue(follower.runtime.cancel_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(leader.runtime.cell and leader.runtime.cell.route_state() == "ACTIVE")
                and bool(follower.runtime.cell and follower.runtime.cell.route_state() == "ACTIVE")
                and cast(object, leader.results[-1]).ready,  # type: ignore[attr-defined]
                rounds=60,
            ),
            "cancelling UNPAIRING did not restore ordinary operation",
        )
        self.assertIn(route_id, self.controller.routes)

    def test_an_assertion_is_sent_only_when_the_cell_proves_it_is_locally_safe(self) -> None:
        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        follower.mt5.positions.append({"ticket": 8181, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        leader.runtime.request_safe_unpair()
        pump_until([leader, follower], lambda: False, rounds=20)
        # The leader is provably empty and asserts; the follower is not and
        # never does, so a one-sided assertion cannot unpair.
        self.assertIn((route_id, LEADER), self.controller.assertions)
        self.assertNotIn((route_id, FOLLOWER), self.controller.assertions)
        self.assertIn(route_id, self.controller.routes)
        self.assertTrue(leader.runtime.enabled)
        # The follower's exposure is never discarded to make an unpair succeed.
        self.assertEqual(1, len(follower.mt5.positions))

    def test_two_fresh_assertions_remove_the_route_and_a_new_pairing_gets_a_new_route_id(self) -> None:
        leader, follower = self._idle_pair()
        original = cast(str, leader.runtime.route_id)
        self.assertTrue(leader.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=120
            ),
            "two fresh assertions never removed the route",
        )
        self.assertNotIn(original, self.controller.routes)
        # Both Workers become unpaired, unreserved and pairable again, and the
        # next pairing produces a new, never-reused route_id.
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=80,
            ),
            f"the Workers never re-paired ({leader.runtime.pairing_diagnostic!r})",
        )
        self.assertNotEqual(original, leader.runtime.route_id)
        self.assertEqual(leader.runtime.route_id, follower.runtime.route_id)


# --------------------------------------------------------------------------- #
# Mode authority and the follower's fail-closed allow_live guard
# --------------------------------------------------------------------------- #


class ModeAuthorityTests(PairingTestCase):
    def test_a_follower_with_allow_live_false_refuses_the_default_live_policy(self) -> None:
        leader, follower = self.paired(
            follower_config=parse_pair_cell_config({"allow_live": False})
        )
        pump_until([leader, follower], lambda: False, rounds=120)
        # It stays paired, safe and idle: it never rewrites the policy and
        # never silently runs shadow against a live leader.
        self.assertTrue(follower.runtime.enabled)
        self.assertIsNone(follower.runtime.enforced_risk_limits())
        self.assertFalse(cast(object, follower.results[-1]).ready)  # type: ignore[attr-defined]
        self.assertIn(
            "allow_live", cast(object, follower.results[-1]).ready_reason  # type: ignore[attr-defined]
        )
        # And the leader originates nothing against a follower that has not
        # accepted.
        self.assertEqual([], leader.mt5.positions)
        self.assertEqual([], follower.mt5.positions)

    def test_a_leader_authored_shadow_policy_resolves_the_disagreement(self) -> None:
        leader, follower = self.paired(
            leader_config=parse_pair_cell_config({"mode": "shadow"}),
            follower_config=parse_pair_cell_config({"allow_live": False}),
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: follower.runtime.enforced_risk_limits() is not None,
                rounds=160,
            ),
            f"the follower never accepted the shadow policy "
            f"({follower.results[-1] if follower.results else None})",
        )
        self.assertIsNotNone(follower.runtime.enforced_risk_limits())


# --------------------------------------------------------------------------- #
# One complete lifecycle
# --------------------------------------------------------------------------- #


# --------------------------------------------------------------------------- #
# Durable recovery: one-shot pairing messages, offline unpairs, safety history
# --------------------------------------------------------------------------- #


class DurablePersistenceTests(PairingTestCase):
    """Regressions for the Worker-side durable recovery defects."""

    #: Comfortably past the default ``safe_unpair_probe_interval`` of 1s.
    polling_interval_seconds = 1.5

    def _idle_pair(self) -> tuple[Worker, Worker]:
        return self.paired(
            leader_config=parse_pair_cell_config({"entry_edge_points": "100000"})
        )

    def _tables(self, worker: Worker) -> set[str]:
        connection = sqlite3.connect(worker.db_path)
        try:
            return {
                str(row[0])
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            } - {"sqlite_sequence"}
        finally:
            connection.close()

    # -- (4) atomic durable writes ------------------------------------------ #

    def test_durable_route_and_acceptance_writes_are_atomic(self) -> None:
        route_path = self.directory / "atomic.route.json"
        record = DurableRouteRecord(
            route_id="route-atomic",
            role="follower",
            leader_worker_id=LEADER,
            follower_worker_id=FOLLOWER,
            frozen_balance_usd="4000",
            state="UNPAIRING",
            proposal_id="proposal-1",
        )
        # A leftover temporary from an interrupted write is never read and
        # never survives the next successful one.
        temporary = route_path.with_name(f"{route_path.name}.tmp")
        temporary.write_text("{truncated", encoding="utf-8")
        save_durable_route(route_path, record)
        self.assertFalse(temporary.exists())
        self.assertEqual(record, load_durable_route(route_path))
        save_durable_route(route_path, replace(record, state="ACTIVE"))
        self.assertEqual("ACTIVE", cast(DurableRouteRecord, load_durable_route(route_path)).state)

        acceptance_path = self.directory / "atomic.acceptance.json"
        payload = {"proposal_id": "proposal-1", "worker_id": FOLLOWER, "startup_balance_usd": "4000"}
        frozen = FrozenAcceptance(
            proposal_id="proposal-1", payload=payload, payload_hash=_payload_hash(payload)
        )
        save_frozen_acceptance(acceptance_path, frozen)
        self.assertFalse(acceptance_path.with_name(f"{acceptance_path.name}.tmp").exists())
        self.assertEqual(frozen, load_frozen_acceptance(acceptance_path))

    def test_a_frozen_acceptance_that_fails_its_own_hash_is_refused(self) -> None:
        path = self.directory / "tampered.acceptance.json"
        path.write_text(
            json.dumps(
                {
                    "proposal_id": "proposal-1",
                    "payload": {"startup_balance_usd": "999999"},
                    "payload_hash": "not-the-hash",
                }
            ),
            encoding="utf-8",
        )
        self.assertIsNone(load_frozen_acceptance(path))

    # -- (1) safety history survives route removal --------------------------- #

    def test_every_durable_cell_table_is_explicitly_classified(self) -> None:
        """A new durable cell table must be a deliberate keep-or-clear decision."""

        leader, follower = self._idle_pair()
        classified = (
            set(ROUTE_SCOPED_TABLES) | set(ATTEMPT_SCOPED_TABLES) | set(PRESERVED_SAFETY_TABLES)
        )
        for worker in (leader, follower):
            with self.subTest(worker=worker.worker_id):
                unclassified = self._tables(worker) - classified
                self.assertEqual(
                    set(),
                    unclassified,
                    f"{sorted(unclassified)} must be classified as route-scoped state or as"
                    " account/product-scoped safety history that outlives a route",
                )

    def test_a_safe_unpair_keeps_the_new_york_daily_realized_loss(self) -> None:
        leader, follower = self._idle_pair()
        cell = follower.runtime.cell
        assert cell is not None
        cell.handle_event(
            RealizedPnLEvent(
                attempt_id="attempt-1",
                realized_usd="-37.50",
                closed_at=self.clock(),
                event_id="position:4242",
            )
        )
        self.assertEqual(Decimal("37.50"), follower.runtime.accumulated_realized_loss_usd())

        original = cast(str, follower.runtime.route_id)
        self.assertTrue(follower.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=120
            ),
            "the safe unpair never completed",
        )
        # The route-scoped state is gone but this account's own realized-loss
        # accounting is not: a same-day re-pair must not hand the pair a fresh
        # daily allowance it has already spent.
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=100,
            ),
            f"the Workers never re-paired ({follower.runtime.pairing_diagnostic!r})",
        )
        self.assertNotEqual(original, follower.runtime.route_id)
        self.assertEqual(Decimal("37.50"), follower.runtime.accumulated_realized_loss_usd())

    def test_a_safe_unpair_keeps_a_price_off_quarantine_and_its_release_requirement(self) -> None:
        leader, follower = self.paired()
        # MT5 ``10021 TRADE_RETCODE_PRICE_OFF`` on the leader's own entry
        # durably quarantines the derived product identity, pair-wide.
        leader.mt5.price_off = True
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: bool(leader.runtime.quarantined_products()), rounds=400
            ),
            "no product was quarantined by the PRICE_OFF entry",
        )
        quarantined = leader.runtime.quarantined_products()
        self.assertEqual(1, len(quarantined))
        product_id = quarantined[0]
        self.assertTrue(product_id.startswith(f"{SYMBOL}:"))

        leader.mt5.price_off = False
        follower.mt5.prices[SYMBOL] = leader.mt5.prices[SYMBOL]
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: not leader.mt5.positions and not follower.mt5.positions,
                rounds=200,
            )
        )
        original = cast(str, leader.runtime.route_id)
        self.assertTrue(leader.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=200
            ),
            "the safe unpair never completed",
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=120,
            ),
            f"the Workers never re-paired ({leader.runtime.pairing_diagnostic!r})",
        )
        # The quarantine has no automatic expiry and is keyed by the derived
        # product identity, so re-pairing does not clear it ...
        self.assertNotEqual(original, leader.runtime.route_id)
        self.assertIn(product_id, leader.runtime.quarantined_products())
        # ... and only an explicit authenticated operator release does.
        leader.session.quarantine_releases.append(
            {
                "request_id": "release-1",
                "symbol": SYMBOL,
                "actor": "admin",
                "reason": "operator release",
                "observed_at": self.clock().isoformat(),
            }
        )
        leader.runtime.drain_relay()
        self.assertEqual(
            [{"request_id": "release-1", "symbol": SYMBOL, "outcome": "applied"}],
            leader.session.quarantine_results,
        )
        self.assertNotIn(product_id, leader.runtime.quarantined_products())

    def test_route_removal_clears_route_scoped_state_but_not_safety_history(self) -> None:
        leader, follower = self._idle_pair()
        follower_db = follower.db_path
        original = cast(str, follower.runtime.route_id)
        cell = follower.runtime.cell
        assert cell is not None
        cell.handle_event(
            RealizedPnLEvent(
                attempt_id="attempt-1",
                realized_usd="-5",
                closed_at=self.clock(),
                event_id="position:1",
            )
        )
        with sqlite3.connect(follower_db) as connection:
            connection.execute(
                "INSERT INTO cell_operator_stop (id, reason, updated_at) VALUES (1, ?, ?)",
                ("containment effect old-route:cancel:123 has non-success replay evidence",
                 self.clock().isoformat()),
            )
        self.assertTrue(follower.runtime.request_safe_unpair())
        # Keep the pair from immediately re-pairing and repopulating the very
        # tables this test is inspecting.
        self.controller.refuse_next_proposal = "not now"
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=120
            )
        )
        pump_until([leader, follower], lambda: False, rounds=10)
        connection = sqlite3.connect(follower_db)
        try:
            counts = {
                table: int(
                    cast(tuple, connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone())[0]
                )
                for table in ROUTE_SCOPED_TABLES + ("cell_realized_pnl",)
            }
        finally:
            connection.close()
        for table in ROUTE_SCOPED_TABLES:
            with self.subTest(table=table):
                self.assertEqual(0, counts[table])
        self.assertEqual(1, counts["cell_realized_pnl"])

    # -- (2) the Pairing Acceptance payload is reconstructable ---------------- #

    def test_a_follower_that_missed_its_route_notice_rebuilds_the_acceptance(self) -> None:
        """The follower's decision reply *and* route notice are both lost."""

        self.controller.drop_pushes_to = {FOLLOWER: {"pair_cell_route_established"}}
        self.controller.drop_results_to = {
            FOLLOWER: {"pair_cell_pairing_decision_result", "pair_cell_route_sync_result"}
        }
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        leader = self.worker(
            LEADER,
            options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER),
            config=parse_pair_cell_config({"entry_edge_points": "100000"}),
        )
        self.assertTrue(
            pump_until([leader, follower], lambda: leader.runtime.enabled, rounds=40),
            "the leader never adopted the route",
        )
        # The follower accepted and is routed by the controller, but knows
        # nothing about it yet.
        self.assertFalse(follower.runtime.enabled)
        self.assertEqual("deciding", follower.runtime.pairing_state)
        self.assertTrue((self.directory / f"{FOLLOWER}.paircell.acceptance.json").exists())
        # The leader is routed and says, actionably, why it cannot proceed.
        self.assertIsNone(leader.runtime.enforced_risk_limits())

        # The follower's own balance moves before it recovers; the frozen one
        # must win, or the leader's verbatim copy would no longer match.
        follower.mt5.account["balance"] = 12_345.0
        self.controller.drop_pushes_to.clear()
        self.controller.drop_results_to.clear()
        self.clock.advance(120)
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: follower.runtime.enabled
                and follower.runtime.enforced_risk_limits() is not None,
                rounds=200,
            ),
            f"the follower never recovered its route ({follower.runtime.pairing_diagnostic!r})",
        )
        self.assertEqual(leader.runtime.route_id, follower.runtime.route_id)
        limits = follower.runtime.declared_risk_limits()
        assert limits is not None
        self.assertEqual("4000", limits.strategy_budget_usd)
        enforced = follower.runtime.enforced_risk_limits()
        assert enforced is not None
        self.assertEqual("4000", enforced.strategy_budget_usd)
        assert leader.runtime.cell is not None
        acceptance = leader.runtime.cell.peer_pairing_acceptance()
        assert acceptance is not None
        self.assertEqual("4000", acceptance.startup_balance_usd)

    def test_a_leader_that_missed_the_acceptance_gets_it_from_republication(self) -> None:
        """Both the forwarded copy and the first relayed copy are lost."""

        self.controller.strip_acceptance = True
        self.controller.drop_relay_kinds_from = {FOLLOWER: {"pairing_acceptance"}}
        leader, follower = self.paired(
            leader_config=parse_pair_cell_config({"entry_edge_points": "100000"})
        )
        pump_until([leader, follower], lambda: False, rounds=20)
        assert leader.runtime.cell is not None
        self.assertIsNone(leader.runtime.cell.peer_pairing_acceptance())
        self.assertIsNone(leader.runtime.enforced_risk_limits())
        # The operator is told exactly what the pair is waiting for.
        diagnostic = cast(str, leader.runtime.acceptance_diagnostic)
        self.assertIn("Pairing Acceptance payload", diagnostic)
        self.assertIn(FOLLOWER, diagnostic)

        self.controller.drop_relay_kinds_from.clear()
        self.clock.advance(60)
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enforced_risk_limits() is not None,
                rounds=200,
            ),
            "the follower never republished its Pairing Acceptance payload",
        )
        self.assertIsNone(leader.runtime.acceptance_diagnostic)
        acceptance = leader.runtime.cell.peer_pairing_acceptance()
        assert acceptance is not None
        self.assertEqual("4000", acceptance.startup_balance_usd)

    # -- (2b) the pairing's frozen startup balance is never re-read ---------- #

    def test_a_leader_that_missed_its_route_write_retains_the_frozen_balance(self) -> None:
        """The leader proposes on balance A, the route commits, and the route
        notice (and this Worker's own durable route write) is lost.  The
        balance then moves to B before it recovers: the pairing must keep A."""

        self.controller.drop_pushes_to = {LEADER: {"pair_cell_route_established"}}
        self.controller.drop_results_to = {LEADER: {"pair_cell_route_sync_result"}}
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        leader = self.worker(
            LEADER,
            balance=10_000.0,
            options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER),
        )
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: self.controller.route_for(LEADER) is not None, rounds=40
            ),
            "the controller never created the route",
        )
        pump_until([leader, follower], lambda: False, rounds=5)
        # The controller has the route; this leader knows nothing about it and
        # never wrote a durable copy -- but it did freeze its budget.
        self.assertFalse(leader.runtime.enabled)
        self.assertFalse((self.directory / f"{LEADER}.paircell.route.json").exists())
        budget = load_frozen_budget(self.directory / f"{LEADER}.paircell.budget.json")
        assert budget is not None
        self.assertEqual("leader", budget.role)
        self.assertEqual(FOLLOWER, budget.counterparty_worker_id)
        self.assertEqual("10000", budget.startup_balance_usd)

        # The broker balance moves, and the process is rebuilt from disk.
        leader.mt5.account["balance"] = 55_000.0
        self.controller.drop_pushes_to.clear()
        self.controller.drop_results_to.clear()
        leader.restart(
            options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        self.clock.advance(120)
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and leader.runtime.enforced_risk_limits() is not None,
                rounds=250,
            ),
            f"the leader never recovered its route ({leader.runtime.pairing_diagnostic!r})",
        )
        route_id = cast(str, self.controller.route_for(LEADER))["route_id"]
        self.assertEqual(route_id, leader.runtime.route_id)
        # The frozen budget wins over the broker's current balance, in the
        # durable route copy, in the declared limits, and in the canonical
        # policy's ``leader_risk``.
        record = load_durable_route(self.directory / f"{LEADER}.paircell.route.json")
        assert record is not None
        self.assertEqual("10000", record.frozen_balance_usd)
        declared = leader.runtime.declared_risk_limits()
        enforced = leader.runtime.enforced_risk_limits()
        assert declared is not None and enforced is not None
        self.assertEqual("10000", declared.strategy_budget_usd)
        self.assertEqual("10000", enforced.strategy_budget_usd)

    def test_adopting_a_route_never_re_reads_the_broker_balance(self) -> None:
        """Neither a durable route copy nor a bound record means fail closed."""

        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        leader.mt5.account["balance"] = 77_000.0
        # Both durable records are lost; only the controller still knows.
        (self.directory / f"{LEADER}.paircell.route.json").unlink()
        (self.directory / f"{LEADER}.paircell.budget.json").unlink()
        leader.restart(
            options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: False, rounds=20)

        self.assertFalse(leader.runtime.enabled)
        self.assertIsNone(leader.runtime.declared_risk_limits())
        diagnostic = leader.runtime.pairing_diagnostic
        self.assertIn(route_id, diagnostic)
        self.assertIn("frozen startup balance", diagnostic)
        self.assertIn("Safe-unpair this route", diagnostic)

    def test_a_frozen_budget_bound_to_another_pairing_is_never_reused(self) -> None:
        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        budget_path = self.directory / f"{LEADER}.paircell.budget.json"
        genuine = load_frozen_budget(budget_path)
        self.assertIsInstance(genuine, FrozenBudget)
        assert genuine is not None
        (self.directory / f"{LEADER}.paircell.route.json").unlink()

        stale = (
            # a record from a proposal to a different counterparty
            replace(genuine, counterparty_worker_id="worker-somebody-else"),
            # a record this Worker wrote in the opposite role
            replace(genuine, role="follower"),
        )
        for record in stale:
            with self.subTest(record=record.canonical()):
                save_frozen_budget(budget_path, record)
                leader.restart(
                    options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
                )
                pump_until([leader, follower], lambda: False, rounds=15)
                self.assertFalse(leader.runtime.enabled)
                self.assertIn(route_id, leader.runtime.pairing_diagnostic)

        # A record whose integrity hash does not cover its content is refused
        # outright rather than silently trusted.
        budget_path.write_text(
            json.dumps(
                {
                    "record": {**genuine.canonical(), "startup_balance_usd": "999999"},
                    "record_hash": genuine.record_hash,
                }
            ),
            encoding="utf-8",
        )
        self.assertIsNone(load_frozen_budget(budget_path))

    def test_an_abandoned_proposal_leaves_no_frozen_budget_behind(self) -> None:
        follower = self.worker(FOLLOWER, bid=1.10100, ask=1.10110, balance=4_000.0)
        self.controller.refuse_next_proposal = "The selected follower Worker is unavailable."
        leader = self.worker(
            LEADER, options=PairCellStartupOptions(role="leader", follower_worker_id=FOLLOWER)
        )
        pump_until([leader, follower], lambda: leader.runtime.pairing_state == "unpaired", rounds=12)
        self.assertFalse((self.directory / f"{LEADER}.paircell.budget.json").exists())
        self.assertFalse((self.directory / f"{FOLLOWER}.paircell.budget.json").exists())

    def test_a_safe_unpair_clears_the_frozen_budget_so_the_next_pairing_re_reads(self) -> None:
        leader, follower = self._idle_pair()
        original = cast(str, leader.runtime.route_id)
        # A later pairing re-reads the startup balance, which is what makes a
        # changed balance produce a different canonical policy hash.
        leader.mt5.account["balance"] = 20_000.0
        self.assertTrue(leader.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=120
            )
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and leader.runtime.route_id != original,
                rounds=120,
            ),
            f"the Workers never re-paired ({leader.runtime.pairing_diagnostic!r})",
        )
        record = load_durable_route(self.directory / f"{LEADER}.paircell.route.json")
        assert record is not None
        self.assertEqual("20000", record.frozen_balance_usd)

    # -- (1) unsafe assertions must not be derived on every loop ------------- #

    def test_an_unsafe_safe_unpair_assertion_is_probed_on_a_bounded_cadence(self) -> None:
        leader, follower = self._idle_pair()
        cell = follower.runtime.cell
        assert cell is not None
        # Hold this account non-terminal so every probe is *unsafe*: that is
        # the state a Worker can sit in indefinitely.
        follower.mt5.positions.append({"ticket": 6161, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        self.assertTrue(follower.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: bool(cell.route_state() == "UNPAIRING"),
                rounds=20,
            )
        )

        probes: list[int] = []
        genuine = cell.safe_unpair_assertion

        def counted() -> object:
            probes.append(1)
            return genuine()

        cell.safe_unpair_assertion = counted  # type: ignore[method-assign]
        self.addCleanup(setattr, cell, "safe_unpair_assertion", genuine)

        # 100 pumps inside one probe interval must derive at most one probe,
        # even though every one of them is unsafe and never records an
        # assertion to compare against.
        before = self._assertion_transitions(follower)
        for _ in range(100):
            follower.runtime.pump(self.clock())
        self.assertLessEqual(len(probes), 1)
        self.assertLessEqual(self._assertion_transitions(follower) - before, 1)

        # The cadence is genuinely time-based, not a one-shot latch.
        for _ in range(4):
            self.clock.advance(self.polling_interval_seconds)
            follower.runtime.pump(self.clock())
        self.assertLessEqual(len(probes), 5)
        self.assertGreaterEqual(len(probes), 2)

    def test_a_changed_local_state_version_probes_immediately(self) -> None:
        leader, follower = self._idle_pair()
        cell = follower.runtime.cell
        assert cell is not None
        follower.mt5.positions.append({"ticket": 6262, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        self.assertTrue(follower.runtime.request_safe_unpair())
        pump_until([leader, follower], lambda: cell.route_state() == "UNPAIRING", rounds=20)

        probes: list[int] = []
        genuine = cell.safe_unpair_assertion

        def counted() -> object:
            probes.append(1)
            return genuine()

        cell.safe_unpair_assertion = counted  # type: ignore[method-assign]
        self.addCleanup(setattr, cell, "safe_unpair_assertion", genuine)
        follower.runtime.pump(self.clock())
        probes.clear()
        # No local movement inside the interval: no further probe.
        follower.runtime.pump(self.clock())
        self.assertEqual([], probes)
        # A new or changed local attempt/effect advances the state version,
        # which must be probed at once rather than waiting out the cadence.
        version = cell.state_version()
        original_state_version = cell.state_version
        cell.state_version = lambda: version + 1  # type: ignore[method-assign]
        self.addCleanup(setattr, cell, "state_version", original_state_version)
        follower.runtime.pump(self.clock())
        self.assertGreaterEqual(len(probes), 1)
        _ = leader

    def _assertion_transitions(self, worker: Worker) -> int:
        connection = sqlite3.connect(worker.db_path)
        try:
            row = connection.execute(
                "SELECT COUNT(*) FROM cell_transitions WHERE event = 'safe_unpair_assertion'"
            ).fetchone()
        finally:
            connection.close()
        return 0 if row is None else int(row[0])

    # -- (2) an unpaired Worker never falsely reports a release applied ------- #

    def test_an_unpaired_worker_refuses_a_release_it_cannot_actually_apply(self) -> None:
        leader, follower = self.paired()
        leader.mt5.price_off = True
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: bool(leader.runtime.quarantined_products()), rounds=400
            ),
            "no product was quarantined by the PRICE_OFF entry",
        )
        product_id = leader.runtime.quarantined_products()[0]
        leader.mt5.price_off = False
        follower.mt5.prices[SYMBOL] = leader.mt5.prices[SYMBOL]
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: not leader.mt5.positions and not follower.mt5.positions,
                rounds=200,
            )
        )
        original = cast(str, leader.runtime.route_id)
        self.controller.refuse_next_proposal = "not now"
        self.assertTrue(leader.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=200
            )
        )
        pump_until([leader, follower], lambda: False, rounds=10)
        self.assertFalse(leader.runtime.enabled)

        leader.session.quarantine_releases.append(
            {
                "request_id": "release-unpaired",
                "symbol": SYMBOL,
                "actor": "admin",
                "reason": "operator release",
                "observed_at": self.clock().isoformat(),
            }
        )
        leader.runtime.drain_relay()
        # The quarantine is still in force, so the release is reported
        # rejected and the operator can retry rather than being told it worked.
        self.assertEqual(
            [{"request_id": "release-unpaired", "symbol": SYMBOL, "outcome": "rejected"}],
            leader.session.quarantine_results,
        )
        self.assertIn(product_id, self._durable_quarantine(leader))

    def test_an_unpaired_worker_with_nothing_quarantined_reports_applied(self) -> None:
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=6)
        worker.session.quarantine_releases.append(
            {
                "request_id": "release-noop",
                "symbol": SYMBOL,
                "actor": "admin",
                "reason": "operator release",
                "observed_at": self.clock().isoformat(),
            }
        )
        worker.runtime.drain_relay()
        self.assertEqual(
            [{"request_id": "release-noop", "symbol": SYMBOL, "outcome": "applied"}],
            worker.session.quarantine_results,
        )

    def _durable_quarantine(self, worker: Worker) -> tuple[str, ...]:
        connection = sqlite3.connect(worker.db_path)
        try:
            return tuple(
                str(row[0])
                for row in connection.execute("SELECT product_id FROM cell_product_quarantine")
            )
        finally:
            connection.close()

    # -- legacy durable state must fail closed, never falsely succeed -------- #

    def _legacy_quarantine_database(
        self, worker_id: str, *, columns: str, row: tuple
    ) -> Path:
        """A quarantine table as an earlier build wrote it, keyed by symbol."""

        db_path = self.directory / f"{worker_id}.paircell.sqlite"
        connection = sqlite3.connect(db_path)
        try:
            connection.execute(f"CREATE TABLE cell_product_quarantine ({columns})")
            connection.execute(
                "INSERT INTO cell_product_quarantine VALUES (" + ",".join("?" * len(row)) + ")",
                row,
            )
            connection.commit()
        finally:
            connection.close()
        return db_path

    def _release_outcome(self, worker: Worker) -> str:
        worker.session.quarantine_releases.append(
            {
                "request_id": "release-legacy",
                "symbol": SYMBOL,
                "actor": "admin",
                "reason": "operator release",
                "observed_at": self.clock().isoformat(),
            }
        )
        worker.runtime.drain_relay()
        return cast(str, worker.session.quarantine_results[-1]["outcome"])

    def test_a_legacy_symbol_keyed_quarantine_is_still_found_while_unpaired(self) -> None:
        """A database from an earlier build keys the quarantine by symbol."""

        self._legacy_quarantine_database(
            FOLLOWER,
            columns="symbol TEXT PRIMARY KEY, offending_worker_id TEXT, attempt_id TEXT,"
            " receipt TEXT, quarantined_at TEXT",
            row=(SYMBOL, FOLLOWER, "attempt-1", "{}", "2024-01-02T14:00:00+00:00"),
        )
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=6)
        self.assertFalse(worker.runtime.enabled)
        # The new ``product_id`` column is simply absent; that must never be
        # read as "nothing is quarantined".
        self.assertEqual("rejected", self._release_outcome(worker))

    def test_a_legacy_quarantine_for_another_symbol_still_releases(self) -> None:
        self._legacy_quarantine_database(
            FOLLOWER,
            columns="symbol TEXT PRIMARY KEY, offending_worker_id TEXT, attempt_id TEXT,"
            " receipt TEXT, quarantined_at TEXT",
            row=("GBPUSD", FOLLOWER, "attempt-1", "{}", "2024-01-02T14:00:00+00:00"),
        )
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=6)
        self.assertEqual("applied", self._release_outcome(worker))

    def test_an_unrecognised_quarantine_shape_never_reports_applied(self) -> None:
        self._legacy_quarantine_database(
            FOLLOWER,
            columns="instrument TEXT PRIMARY KEY, quarantined_at TEXT",
            row=(SYMBOL, "2024-01-02T14:00:00+00:00"),
        )
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=6)
        self.assertEqual("rejected", self._release_outcome(worker))

    def test_an_unreadable_database_never_reports_a_release_applied(self) -> None:
        (self.directory / f"{FOLLOWER}.paircell.sqlite").write_bytes(b"this is not a database")
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: worker.runtime.pairing_state == "available", rounds=6)
        self.assertEqual("rejected", self._release_outcome(worker))

    def test_an_unopenable_durable_database_fails_the_cell_closed(self) -> None:
        """Schema creation or migration failing must not kill the Worker."""

        db_path = self.directory / f"{FOLLOWER}.paircell.sqlite"
        db_path.write_bytes(b"not a sqlite database at all")
        save_durable_route(
            self.directory / f"{FOLLOWER}.paircell.route.json",
            DurableRouteRecord(
                route_id="route-legacy",
                role="follower",
                leader_worker_id=LEADER,
                follower_worker_id=FOLLOWER,
                frozen_balance_usd="4000",
            ),
        )
        # Constructing the runtime must not raise: the Worker's reconciliation
        # loop keeps running and this cell simply refuses to trade.
        worker = self.worker(FOLLOWER)
        pump_until([worker], lambda: False, rounds=6)

        self.assertFalse(worker.runtime.enabled)
        self.assertIsNone(worker.runtime.cell)
        diagnostic = worker.runtime.pairing_diagnostic
        self.assertIn("durable database could not be opened or migrated", diagnostic)
        self.assertIn(str(db_path), diagnostic)
        self.assertIn("will not trade", diagnostic)

    # -- (3) attempt evidence and its desired state are cleared as one set ---- #

    def test_retained_attempt_evidence_keeps_its_desired_and_active_state(self) -> None:
        leader, follower = self._idle_pair()
        db_path = follower.db_path
        # Route removal is only ever arbitrated on two fresh assertions that
        # every attempt is terminal, so unresolved evidence at reset time is a
        # contradiction.  It is exercised directly here because it cannot be
        # reached through the ordinary flow -- and if it ever does happen, the
        # attempt, its legs and the desired state it converges through must
        # survive together rather than leaving a recovered attempt with its
        # desired state amputated.
        self._seed_unresolved_attempt(db_path)
        with self.assertLogs("abt.worker.pair_cell_adapter", level="ERROR") as logs:
            follower.runtime._route_removed("contradiction")
        self.assertIn("unresolved attempt", "\n".join(logs.output))

        counts = self._counts(db_path, ATTEMPT_SCOPED_TABLES + ROUTE_SCOPED_TABLES)
        for table in ATTEMPT_SCOPED_TABLES:
            with self.subTest(retained=table):
                self.assertGreater(counts[table], 0)
        for table in ROUTE_SCOPED_TABLES:
            with self.subTest(cleared=table):
                self.assertEqual(0, counts[table])
        _ = leader

    def test_a_new_route_never_inherits_desired_or_active_state(self) -> None:
        leader, follower = self._idle_pair()
        db_path = follower.db_path
        original = cast(str, follower.runtime.route_id)
        # Terminal attempt evidence, exactly as a completed lifecycle leaves it.
        self._seed_unresolved_attempt(db_path, terminal=True)
        self.assertTrue(follower.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=200
            )
        )
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=120,
            ),
            f"the Workers never re-paired ({follower.runtime.pairing_diagnostic!r})",
        )
        self.assertNotEqual(original, follower.runtime.route_id)
        counts = self._counts(db_path, ("cell_desired_state", "cell_active_state", "cell_attempts"))
        for table, count in counts.items():
            with self.subTest(table=table):
                self.assertEqual(0, count)

    def _seed_unresolved_attempt(self, db_path: Path, *, terminal: bool = False) -> None:
        """Write one attempt's evidence set directly, as a crash would leave it."""

        connection = sqlite3.connect(db_path)
        try:
            connection.execute(
                "INSERT OR REPLACE INTO cell_attempts (attempt_id, payload_hash, payload,"
                " created_at, terminal, terminal_reason) VALUES ('seeded', 'h', '{}', ?, ?, NULL)",
                ("2024-01-02T14:00:00+00:00", 1 if terminal else 0),
            )
            connection.execute(
                "INSERT OR REPLACE INTO cell_leg_state (attempt_id, payload, updated_at)"
                " VALUES ('seeded', '{}', ?)",
                ("2024-01-02T14:00:00+00:00",),
            )
            connection.execute(
                "INSERT OR REPLACE INTO cell_peer_leg (attempt_id, payload, updated_at)"
                " VALUES ('seeded', '{}', ?)",
                ("2024-01-02T14:00:00+00:00",),
            )
            connection.execute(
                "INSERT OR REPLACE INTO cell_desired_state (id, desired_state, attempt_id,"
                " pair_confirmed, updated_at) VALUES (1, 'EMPTY', 'seeded', 0, ?)",
                ("2024-01-02T14:00:00+00:00",),
            )
            connection.execute(
                "INSERT OR REPLACE INTO cell_active_state (attempt_id, active_since)"
                " VALUES ('seeded', ?)",
                ("2024-01-02T14:00:00+00:00",),
            )
            connection.commit()
        finally:
            connection.close()

    def _counts(self, db_path: Path, tables: Sequence[str]) -> dict[str, int]:
        connection = sqlite3.connect(db_path)
        try:
            return {
                table: int(
                    cast(tuple, connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone())[0]
                )
                for table in tables
            }
        finally:
            connection.close()

    # -- (3) an offline safe unpair must not strand a Worker ------------------ #

    def _unpair_while_follower_is_offline(self) -> tuple[Worker, Worker, str]:
        leader, follower = self._idle_pair()
        route_id = cast(str, leader.runtime.route_id)
        # Hold the leader non-terminal so the follower asserts first.
        leader.mt5.positions.append({"ticket": 1212, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        pump_until([leader, follower], lambda: False, rounds=6)
        self.assertTrue(follower.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: (route_id, FOLLOWER) in self.controller.assertions,
                rounds=60,
            ),
            "the follower never asserted",
        )
        self.assertIn(route_id, self.controller.routes)
        # The follower goes offline; the leader becomes empty and completes it.
        self.controller.disconnect(FOLLOWER)
        leader.mt5.positions.clear()
        self.assertTrue(
            pump_until([leader], lambda: route_id not in self.controller.routes, rounds=120),
            "the leader never completed the safe unpair",
        )
        self.controller.register(follower.session)
        return leader, follower, route_id

    def test_a_worker_offline_for_the_completed_safe_unpair_adopts_it_on_reconnect(self) -> None:
        leader, follower, route_id = self._unpair_while_follower_is_offline()
        record = load_durable_route(self.directory / f"{FOLLOWER}.paircell.route.json")
        assert record is not None
        self.assertEqual("UNPAIRING", record.state)

        runtime = follower.restart(options=PairCellStartupOptions())
        self.assertTrue(runtime.enabled)
        self.assertTrue(
            pump_until(
                [follower],
                lambda: not follower.runtime.enabled,
                rounds=120,
            ),
            f"the follower stayed stranded on a removed route"
            f" ({follower.runtime.pairing_diagnostic!r})",
        )
        self.assertIsNone(
            load_durable_route(self.directory / f"{FOLLOWER}.paircell.route.json")
        )
        self.assertIsNone(follower.runtime.route_id)
        # And it is pairable again.
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.runtime.enabled and follower.runtime.enabled,
                rounds=120,
            ),
            f"the follower never became pairable again ({follower.runtime.pairing_diagnostic!r})",
        )
        self.assertNotEqual(route_id, follower.runtime.route_id)

    def test_an_absent_route_is_not_adopted_while_local_evidence_is_unsafe(self) -> None:
        leader, follower, route_id = self._unpair_while_follower_is_offline()
        # This account is *not* provably empty when it comes back, so the
        # removal is never inferred and its exposure is never touched.
        follower.mt5.positions.append({"ticket": 9999, "symbol": SYMBOL, "type": 0, "volume": 1.0})
        follower.restart(options=PairCellStartupOptions())
        pump_until([follower], lambda: False, rounds=60)
        self.assertTrue(follower.runtime.enabled)
        self.assertEqual(route_id, follower.runtime.route_id)
        assert follower.runtime.cell is not None
        self.assertIsNotNone(follower.runtime.cell.metadata_reconciliation())
        self.assertEqual(1, len(follower.mt5.positions))
        self.assertIsNotNone(
            load_durable_route(self.directory / f"{FOLLOWER}.paircell.route.json")
        )
        _ = leader

    def test_an_absent_route_that_was_never_unpairing_reconciles_fail_closed(self) -> None:
        leader, follower = self._idle_pair()
        route_id = cast(str, follower.runtime.route_id)
        # The controller's route vanishes while it was still ACTIVE.
        del self.controller.routes[route_id]
        follower.restart(options=PairCellStartupOptions())
        pump_until([follower], lambda: False, rounds=60)
        self.assertTrue(follower.runtime.enabled)
        self.assertEqual(route_id, follower.runtime.route_id)
        assert follower.runtime.cell is not None
        self.assertIsNotNone(follower.runtime.cell.metadata_reconciliation())
        self.assertIsNotNone(
            load_durable_route(self.directory / f"{FOLLOWER}.paircell.route.json")
        )
        _ = leader


# --------------------------------------------------------------------------- #
# One complete lifecycle
# --------------------------------------------------------------------------- #


class FullLifecycleTests(PairingTestCase):
    def test_pairing_then_discovery_then_immediate_entry_active_close_and_safe_unpair(self) -> None:
        leader, follower = self.paired()

        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: leader.state == "ACTIVE" and follower.state == "ACTIVE",
                rounds=400,
            ),
            "the pair never reached ACTIVE "
            f"(leader={leader.results[-1] if leader.results else None}, "
            f"follower={follower.results[-1] if follower.results else None})",
        )

        # Discovery ran before any entry, on the same route, under generation 1.
        self.assertEqual(1, leader.runtime.universe_generation())
        self.assertEqual(1, follower.runtime.universe_generation())
        self.assertEqual(leader.runtime.route_id, follower.runtime.route_id)

        # Both legs use exactly the common lots -- the lower Worker capacity,
        # 4 000 * 0.10 / 1 000 = 0.4 -- never a fixed per-Worker volume.
        self.assertEqual(1, len(leader.mt5.positions))
        self.assertEqual(1, len(follower.mt5.positions))
        for mt5 in (leader.mt5, follower.mt5):
            self.assertEqual(0.4, float(cast(float, mt5.positions[0]["volume"])))
            self.assertGreater(float(cast(float, mt5.positions[0]["sl"])), 0.0)
            self.assertGreater(float(cast(float, mt5.positions[0]["tp"])), 0.0)
        # Mirror directions: the leader is LONG against its ask, the follower
        # SHORT against its bid.
        self.assertEqual(FakeMT5.ORDER_TYPE_BUY, leader.mt5.positions[0]["type"])
        self.assertEqual(FakeMT5.ORDER_TYPE_SELL, follower.mt5.positions[0]["type"])

        # Flatten the edge and let the flat quotes propagate, so the exit path
        # converges instead of immediately originating the next attempt.
        follower.mt5.prices[SYMBOL] = leader.mt5.prices[SYMBOL]
        pump_until([leader, follower], lambda: False, rounds=40)
        leader.runtime.request_close("timed_exit")
        self.assertTrue(
            pump_until(
                [leader, follower],
                lambda: not leader.mt5.positions and not follower.mt5.positions,
                rounds=200,
            ),
            "the pair never converged to a broker-verified empty account",
        )
        # Realized per-leg P&L is attributed from broker deal history, so the
        # New York daily allowance genuinely accounts for the closed legs.
        pump_until([leader, follower], lambda: False, rounds=20)
        self.assertGreater(leader.runtime.accumulated_realized_loss_usd(), Decimal(0))

        original = cast(str, leader.runtime.route_id)
        self.assertTrue(leader.runtime.request_safe_unpair())
        self.assertTrue(
            pump_until(
                [leader, follower], lambda: original not in self.controller.routes, rounds=200
            ),
            "the safe unpair never removed the route",
        )
        self.assertNotIn(original, self.controller.routes)


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    unittest.main()
