"""End-to-end integration tests across a live Worker session <-> controller
websocket <-> peer Worker session <-> :class:`~abt.pair_cell.PairExecutionCell`
adapter.

These run a real ``uvicorn`` server (the actual FastAPI app), connect two real
``AuthenticatedWorkerSession`` objects over real websockets, and drive two
real :class:`~abt.worker.pair_cell_adapter.PairCellRuntime` instances -- one
declaring ``leader`` and one that declares nothing at all -- with
deterministic MT5 fakes and **no** pre-existing route, no configuration file
carrying a route, and no administrator action of any kind.

They fail if the production wiring (Worker-initiated pairing over the
authenticated control plane, the two-phase reservation, the opaque relay keyed
on ``route_id``, Initial Compatible Product Discovery, leader-owned canonical
policy built from the follower's Pairing Acceptance payload, scheduler
sharing, MT5 request translation, or safe unpair) is removed or
short-circuited.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
import http.cookies
import json
from pathlib import Path
import socket
import sqlite3
import tempfile
import threading
import time
from typing import cast
import unittest
from uuid import uuid4

import httpx
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
import uvicorn
from websockets.sync.client import connect as websocket_connect

from abt.controlplane.crypto import (
    device_certificate_payload,
    enrollment_payload,
    worker_proof_payload,
)
from abt.controlplane.service import create_app
from abt.worker.effect_journal import WorkerEffectJournal
from abt.worker.pair_cell_adapter import (
    PRESERVED_SAFETY_TABLES,
    PairCellConfig,
    PairCellPollingConfig,
    PairCellRuntime,
    PairCellStartupOptions,
    load_durable_route,
    parse_pair_cell_config,
)
from abt.worker.session import AuthenticatedWorkerSession


SYMBOL = "EURUSD"

#: The leader authors the shared policy; a wide quote-age/skew budget keeps a
#: real-websocket round trip from being mistaken for stale market evidence.
LEADER_CONFIG = parse_pair_cell_config(
    {
        "entry_edge_points": "1",
        "quote_max_age_seconds": 30.0,
        "quote_max_skew_seconds": 30.0,
    }
)


class MemorySecretStore:
    """A minimal in-memory secret store; ``create_app`` requires one."""

    def __init__(self) -> None:
        self.passwords: dict[str, str] = {}

    def write_password(self, reference: str, password: str) -> None:
        self.passwords[reference] = password

    def read_password(self, reference: str) -> str:
        return self.passwords[reference]

    def delete_password(self, reference: str) -> None:
        del self.passwords[reference]


class MemoryCertificateIssuer:
    """A minimal in-memory device-certificate issuer/verifier for enrollment."""

    def __init__(self) -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())

    def issue(self, *, worker_id: str, login: int, server: str, public_key_pem: str) -> str:
        issued_at = datetime.now(UTC)
        payload = device_certificate_payload(
            worker_id=worker_id, login=login, server=server, public_key_pem=public_key_pem,
            issued_at=issued_at, expires_at=issued_at + timedelta(days=30),
        )
        return json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "signature": base64.b64encode(self._key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
            },
            separators=(",", ":"), sort_keys=True,
        )

    def verify(self, certificate: str) -> None:
        envelope = json.loads(certificate)
        payload = base64.b64decode(envelope["payload"], validate=True)
        signature = base64.b64decode(envelope["signature"], validate=True)
        self._key.public_key().verify(signature, payload, ec.ECDSA(hashes.SHA256()))


class FakeMT5:
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

    def __init__(self, *, login: int, server: str, bid: float, ask: float, balance: float) -> None:
        self.account: dict[str, object] = {
            "login": login,
            "server": server,
            "margin_free": 100_000.0,
            "balance": balance,
            "equity": balance + 1_000.0,
            "currency": "USD",
        }
        self.terminal = {"connected": True, "trade_allowed": True, "tradeapi_disabled": False}
        self.orders: list[dict[str, object]] = []
        self.positions: list[dict[str, object]] = []
        self.symbol: dict[str, object] = {
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
        self.tick = {"bid": bid, "ask": ask, "last": (bid + ask) / 2, "time_msc": int(time.time() * 1000)}
        self._next_ticket = 5000
        self.deals_by_position: dict[int, list[dict[str, object]]] = {}
        self.lock = threading.Lock()

    def account_info(self) -> object:
        return dict(self.account)

    def terminal_info(self) -> object:
        return dict(self.terminal)

    def orders_get(self) -> object:
        with self.lock:
            return [dict(order) for order in self.orders]

    def positions_get(self) -> object:
        with self.lock:
            return [dict(position) for position in self.positions]

    def symbols_get(self) -> object:
        return [{"name": SYMBOL}]

    def symbol_info(self, symbol: str) -> object:
        return None if symbol != SYMBOL else dict(self.symbol)

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return True

    def symbol_info_tick(self, symbol: str) -> object:
        if symbol != SYMBOL:
            return None
        with self.lock:
            self.tick["time_msc"] = int(time.time() * 1000)
            return dict(self.tick)

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> object:
        return 1000.0 * volume

    def history_deals_get(self, *, position: int) -> object:
        with self.lock:
            return [dict(deal) for deal in self.deals_by_position.get(position, [])] or None

    def _realized(self, position: dict[str, object]) -> float:
        volume = float(cast(float, position["volume"]))
        opened = float(cast(float, position["price_open"]))
        if position["type"] == self.ORDER_TYPE_BUY:
            return round((float(cast(float, self.tick["bid"])) - opened) * volume * 100_000, 2)
        return round((opened - float(cast(float, self.tick["ask"]))) * volume * 100_000, 2)

    def order_send(self, request: dict[str, object]) -> object:
        with self.lock:
            action = request.get("action")
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


def _free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


class _LiveServer:
    def __init__(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.secret_store = MemorySecretStore()
        self.certificate_issuer = MemoryCertificateIssuer()
        self.app = create_app(
            Path(self.directory.name) / "ledger.duckdb",
            secret_store=self.secret_store,
            certificate_issuer=self.certificate_issuer,
        )
        self.app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
        self.port = _free_port()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.server = uvicorn.Server(uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)
        self.thread.start()
        for _ in range(100):
            try:
                if httpx.get(f"{self.base_url}/health", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.05)
        else:
            raise AssertionError("live controller did not start")

    def stop(self) -> None:
        self.server.should_exit = True
        try:
            httpx.get(f"{self.base_url}/health", timeout=0.2)
        except httpx.HTTPError:
            pass
        self.thread.join(timeout=5)
        self.directory.cleanup()

    def admin_session(self) -> tuple[str, str]:
        response = httpx.post(
            f"{self.base_url}/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
            timeout=5,
        )
        assert response.status_code == 200, response.text
        cookie = http.cookies.SimpleCookie()
        cookie.load(response.headers["set-cookie"])
        return f"abt_admin_session={cookie['abt_admin_session'].value}", response.json()["csrf_token"]

    def approved_worker(self, login: int, server: str) -> tuple[ec.EllipticCurvePrivateKey, str, str]:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key_pem = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        challenge = httpx.get(f"{self.base_url}/api/enrollment-challenge", timeout=5).json()["challenge"]
        account_info = {"login": login, "server": server}
        terminal_info = {"name": "MetaTrader 5"}
        password = f"worker-{login}-memory-only-password"
        signature = private_key.sign(
            enrollment_payload(login, server, account_info, terminal_info, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        response = httpx.post(
            f"{self.base_url}/api/enrollments",
            json={
                "registration_invite": self.app.state.ledger.create_registration_invite("ABCDEF", "worker"),
                "login": login, "server": server,
                "account_info": account_info, "terminal_info": terminal_info, "mt5_password": password,
                "enrollment_challenge": challenge, "public_key_pem": public_key_pem,
                "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
            timeout=5,
        )
        assert response.status_code == 201, response.text
        cookie, csrf = self.admin_session()
        approval = httpx.post(
            f"{self.base_url}/api/admin/enrollments/{response.json()['enrollment_id']}/approve",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            timeout=5,
        )
        assert approval.status_code == 200, approval.text
        worker_id = approval.json()["worker_id"]
        certificate = self.app.state.ledger.active_worker(worker_id).certificate
        return private_key, worker_id, certificate

    def connect_worker_session(
        self, private_key: ec.EllipticCurvePrivateKey, worker_id: str, certificate: str
    ) -> AuthenticatedWorkerSession:
        raw_socket = websocket_connect(f"ws://127.0.0.1:{self.port}/api/worker/session")
        raw_socket.__enter__()
        raw_socket.send(json.dumps({"worker_id": worker_id, "certificate": certificate}))
        challenge = json.loads(raw_socket.recv())
        signature = private_key.sign(
            worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
            ec.ECDSA(hashes.SHA256()),
        )
        raw_socket.send(json.dumps({"signature": base64.b64encode(signature).decode("ascii")}))
        authenticated = json.loads(raw_socket.recv())
        assert authenticated["type"] == "authenticated", authenticated
        return AuthenticatedWorkerSession(
            raw_socket, reconciliation_cursor=authenticated["cursor"], worker_id=worker_id, certificate=certificate
        )

    def claim_execution_mode(self, *, leader_worker_id: str, follower_worker_id: str, mode: str) -> httpx.Response:
        cookie, csrf = self.admin_session()
        return httpx.post(
            f"{self.base_url}/api/admin/pairs/execution-mode",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={
                "leader_worker_id": leader_worker_id,
                "follower_worker_id": follower_worker_id,
                "trader_id": "trader-1",
                "mode": mode,
            },
            timeout=5,
        )

    def release_pair_quarantine(self, *, route_id: str, symbol: str, reason: str) -> httpx.Response:
        cookie, csrf = self.admin_session()
        return httpx.post(
            f"{self.base_url}/api/admin/pairs/quarantine/release",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={"route_id": route_id, "symbol": symbol, "reason": reason},
            # Release is correlated/acknowledged: the server awaits a
            # ``pair_cell_quarantine_release_result`` from each connected
            # Worker before replying.
            timeout=20,
        )


def _drive(
    *,
    worker_id: str,
    session: AuthenticatedWorkerSession,
    mt5: FakeMT5,
    login: int,
    server: str,
    directory: Path,
    config: PairCellConfig,
    options: PairCellStartupOptions,
    stop: threading.Event,
    results: list,
    holder: list,
) -> None:
    """Construct this Worker's durable adapters and pump loop entirely
    within this thread: sqlite connections are thread-affine, matching the
    single-threaded-per-Worker design of ``abt.worker.reconciliation``'s real
    receive loop (two independent Workers, not two threads of one Worker)."""

    journal = WorkerEffectJournal(directory / f"{worker_id}.effects.sqlite")
    runtime = PairCellRuntime(
        worker_id=worker_id,
        db_path=directory / f"{worker_id}.paircell.sqlite",
        session=session,  # type: ignore[arg-type]
        mt5=mt5,
        effect_journal=journal,
        recovery_epoch=str(uuid4()),
        login=login,
        server=server,
        config=config,
        options=options,
        polling=PairCellPollingConfig(
            quote_interval=timedelta(milliseconds=100),
            readiness_interval=timedelta(milliseconds=100),
            broker_snapshot_interval=timedelta(milliseconds=100),
            # Real spaced samples, just compressed: this fake's tick epoch is
            # real wall-clock time, so 5ms apart is still strictly advancing.
            clock_sample_interval=timedelta(milliseconds=5),
        ),
    )
    holder.append(runtime)
    try:
        while not stop.is_set():
            try:
                for _ in range(10):
                    if not session.receive_worker_relay(timeout=0.02):
                        break
            except Exception:
                return
            try:
                result = runtime.pump(datetime.now(UTC))
            except Exception:
                return
            if result is not None:
                results.append(result)
    finally:
        runtime.close()
        journal.close()


class _Pair:
    """Two live Workers that pair themselves, plus their pump threads."""

    def __init__(
        self,
        test: unittest.TestCase,
        server: _LiveServer,
        directory: Path,
        *,
        follower_worker_id_known: bool = True,
        leader_config: PairCellConfig = LEADER_CONFIG,
        follower_config: PairCellConfig | None = None,
        named_follower: str | None = None,
    ) -> None:
        leader_key, self.leader_id, leader_cert = server.approved_worker(910001, "Broker-A")
        follower_key, self.follower_id, follower_cert = server.approved_worker(910002, "Broker-B")

        # Leader is LONG (prices from its ask); follower is SHORT (prices from
        # its bid). A coherent edge needs follower.bid - leader.ask to clear
        # ``entry_edge_points``.
        self.leader_mt5 = FakeMT5(login=910001, server="Broker-A", bid=1.10000, ask=1.10010, balance=10_000.0)
        self.follower_mt5 = FakeMT5(login=910002, server="Broker-B", bid=1.10100, ask=1.10110, balance=4_000.0)

        self.leader_session = server.connect_worker_session(leader_key, self.leader_id, leader_cert)
        test.addCleanup(self.leader_session.socket.__exit__, None, None, None)
        self.follower_session = server.connect_worker_session(follower_key, self.follower_id, follower_cert)
        test.addCleanup(self.follower_session.socket.__exit__, None, None, None)

        self.stop = threading.Event()
        test.addCleanup(self.stop.set)
        self.leader_results: list = []
        self.follower_results: list = []
        self.leader_holder: list = []
        self.follower_holder: list = []
        selected = named_follower if named_follower is not None else (
            self.follower_id if follower_worker_id_known else None
        )
        self.threads = [
            threading.Thread(
                target=_drive,
                kwargs=dict(
                    worker_id=self.follower_id, session=self.follower_session, mt5=self.follower_mt5,
                    login=910002, server="Broker-B", directory=directory,
                    config=follower_config or parse_pair_cell_config({}),
                    # No role at all: an omitted role means available follower.
                    options=PairCellStartupOptions(),
                    stop=self.stop, results=self.follower_results, holder=self.follower_holder,
                ),
                daemon=True,
            ),
            threading.Thread(
                target=_drive,
                kwargs=dict(
                    worker_id=self.leader_id, session=self.leader_session, mt5=self.leader_mt5,
                    login=910001, server="Broker-A", directory=directory,
                    config=leader_config,
                    options=PairCellStartupOptions(role="leader", follower_worker_id=selected),
                    stop=self.stop, results=self.leader_results, holder=self.leader_holder,
                ),
                daemon=True,
            ),
        ]
        # Start the follower first so it is a connected, available, unpaired
        # candidate by the time the leader proposes.
        for thread in self.threads:
            thread.start()
            time.sleep(0.2)

    @property
    def leader(self) -> PairCellRuntime | None:
        return self.leader_holder[0] if self.leader_holder else None

    @property
    def follower(self) -> PairCellRuntime | None:
        return self.follower_holder[0] if self.follower_holder else None

    def wait_for(self, predicate, *, seconds: float = 30) -> bool:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate():
                return True
            time.sleep(0.05)
        return False

    def both_states(self, state: str) -> bool:
        return (
            bool(self.leader_results)
            and bool(self.follower_results)
            and self.leader_results[-1].state == state
            and self.follower_results[-1].state == state
        )

    def shutdown(self) -> None:
        self.stop.set()
        for thread in self.threads:
            thread.join(timeout=5)


class PairExecutionCellEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _LiveServer()
        self.addCleanup(self.server.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_two_fresh_workers_pair_discover_and_complete_one_entry(self) -> None:
        """No route, no administrator action, no configuration file naming a
        route: the two Workers pair themselves over the authenticated control
        plane, discover their compatible universe, and complete one protected
        two-leg entry."""

        pair = _Pair(self, self.server, Path(self._tmp.name))

        self.assertTrue(
            pair.wait_for(lambda: pair.both_states("ACTIVE")),
            "Pair did not reach ACTIVE on both Workers "
            f"(leader last={pair.leader_results[-1] if pair.leader_results else None}, "
            f"follower last={pair.follower_results[-1] if pair.follower_results else None}, "
            f"leader diag={pair.leader.pairing_diagnostic if pair.leader else None}, "
            f"follower diag={pair.follower.pairing_diagnostic if pair.follower else None}).",
        )

        leader_runtime, follower_runtime = pair.leader, pair.follower
        assert leader_runtime is not None and follower_runtime is not None
        route_id = leader_runtime.route_id
        self.assertIsNotNone(route_id)
        self.assertEqual(route_id, follower_runtime.route_id)
        self.assertEqual("leader", leader_runtime.role)
        self.assertEqual("follower", follower_runtime.role)
        # Discovery ran on this route before any quote became a candidate.
        self.assertEqual(1, leader_runtime.universe_generation())
        self.assertEqual(1, follower_runtime.universe_generation())

        pair.shutdown()

        self.assertEqual(1, len(pair.leader_mt5.positions))
        self.assertEqual(1, len(pair.follower_mt5.positions))
        # Both legs use exactly the common lots: the lower Worker capacity,
        # 4 000 * 0.10 / 1 000 = 0.4 lots, never a fixed volume.
        self.assertEqual(0.4, float(cast(float, pair.leader_mt5.positions[0]["volume"])))
        self.assertEqual(0.4, float(cast(float, pair.follower_mt5.positions[0]["volume"])))
        for mt5 in (pair.leader_mt5, pair.follower_mt5):
            self.assertGreater(float(cast(float, mt5.positions[0]["sl"])), 0.0)
            self.assertGreater(float(cast(float, mt5.positions[0]["tp"])), 0.0)

    def test_the_controller_route_record_carries_no_trader_identity(self) -> None:
        pair = _Pair(self, self.server, Path(self._tmp.name))
        self.assertTrue(
            pair.wait_for(lambda: pair.leader is not None and pair.leader.enabled),
            f"the leader never paired ({pair.leader.pairing_diagnostic if pair.leader else None}).",
        )
        route_id = cast(str, cast(PairCellRuntime, pair.leader).route_id)
        pair.shutdown()

        ledger = self.server.app.state.ledger
        route = ledger.pair_route(route_id)
        assert route is not None
        self.assertNotIn("trader_id", route)
        self.assertEqual(pair.leader_id, route["leader_worker_id"])
        self.assertEqual(pair.follower_id, route["follower_worker_id"])
        self.assertEqual("ACTIVE", route["state"])
        owner = ledger.pair_execution_owner(pair.leader_id, pair.follower_id, "pair_execution_cell")
        assert owner is not None
        self.assertEqual("pair_execution_cell", owner["owner_kind"])
        self.assertIsNone(owner["trader_id"])

    def test_there_is_no_administrator_route_creation_surface(self) -> None:
        cookie, csrf = self.server.admin_session()
        for path in ("/api/admin/pairs/route", "/api/admin/pairs/route/revoke"):
            with self.subTest(path=path):
                response = httpx.post(
                    f"{self.server.base_url}{path}",
                    headers={"Cookie": cookie, "X-CSRF-Token": csrf},
                    json={"leader_worker_id": "a", "follower_worker_id": "b"},
                    timeout=5,
                )
                self.assertEqual(404, response.status_code)

    def test_a_leader_naming_an_unavailable_follower_stays_unpaired(self) -> None:
        pair = _Pair(
            self, self.server, Path(self._tmp.name), named_follower="worker-that-does-not-exist"
        )
        self.assertTrue(
            pair.wait_for(
                lambda: pair.leader is not None and pair.leader.pairing_state == "unpaired",
                seconds=15,
            ),
            "the leader never failed closed on an unavailable follower",
        )
        leader_runtime = cast(PairCellRuntime, pair.leader)
        self.assertFalse(leader_runtime.enabled)
        self.assertIsNone(leader_runtime.route_id)
        pair.shutdown()
        # No route, no partial role assignment, no leaked reservation.
        self.assertIsNone(self.server.app.state.ledger.pair_route_for_worker(pair.leader_id))
        self.assertEqual([], self.server.app.state.ledger.pair_route_reservations_for_worker(pair.leader_id))

    def test_the_legacy_strategy_runtime_blocks_a_pairing_for_the_same_workers(self) -> None:
        leader_key, leader_id, leader_cert = self.server.approved_worker(910011, "Broker-A")
        follower_key, follower_id, follower_cert = self.server.approved_worker(910012, "Broker-B")
        claimed = self.server.claim_execution_mode(
            leader_worker_id=leader_id, follower_worker_id=follower_id, mode="strategy_runtime"
        )
        self.assertEqual(200, claimed.status_code, claimed.text)

        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)
        follower_session = self.server.connect_worker_session(follower_key, follower_id, follower_cert)
        self.addCleanup(follower_session.socket.__exit__, None, None, None)

        follower_session.declare_pair_cell_role(None)
        self.assertTrue(_await_result(follower_session, "pair_cell_role_result")["accepted"])
        leader_session.declare_pair_cell_role("leader")
        self.assertTrue(_await_result(leader_session, "pair_cell_role_result")["accepted"])
        leader_session.propose_pair_cell_pairing(follower_id)
        reply = _await_result(leader_session, "pair_cell_pairing_proposal_result")

        self.assertFalse(reply["accepted"])
        self.assertIsNone(self.server.app.state.ledger.pair_route_for_worker(leader_id))

    def test_the_leader_builds_the_canonical_policy_from_the_forwarded_acceptance(self) -> None:
        """The follower's own authored block reaches the leader through the
        controller and is copied verbatim into ``follower_risk``."""

        pair = _Pair(
            self,
            self.server,
            Path(self._tmp.name),
            follower_config=parse_pair_cell_config(
                {"trade_loss_fraction": "0.01", "maximum_loss_per_trade_usd": "25"}
            ),
        )
        self.assertTrue(
            pair.wait_for(
                lambda: pair.follower is not None
                and pair.follower.enforced_risk_limits() is not None
            ),
            "the follower never accepted a canonical policy "
            f"({pair.follower_results[-1] if pair.follower_results else None}).",
        )
        leader_runtime = cast(PairCellRuntime, pair.leader)
        follower_runtime = cast(PairCellRuntime, pair.follower)
        cell = leader_runtime.cell
        assert cell is not None
        acceptance = cell.peer_pairing_acceptance()
        assert acceptance is not None
        follower_risk = follower_runtime.enforced_risk_limits()
        leader_risk = leader_runtime.enforced_risk_limits()
        pair.shutdown()

        self.assertEqual(pair.follower_id, acceptance.worker_id)
        assert follower_risk is not None and leader_risk is not None
        self.assertTrue(follower_risk.is_verbatim_copy_of(acceptance.risk_limits()))
        # The follower authored its own budget and its own tunables ...
        self.assertEqual("4000", follower_risk.strategy_budget_usd)
        self.assertEqual("0.01", follower_risk.trade_loss_fraction)
        self.assertEqual("25", follower_risk.maximum_loss_per_trade_usd)
        # ... and the leader authored only its own.
        self.assertEqual("10000", leader_risk.strategy_budget_usd)
        self.assertEqual("40", leader_risk.maximum_loss_per_trade_usd)

    def test_an_operator_quarantine_release_is_addressed_by_route_id(self) -> None:
        pair = _Pair(self, self.server, Path(self._tmp.name), leader_config=LEADER_CONFIG)
        self.assertTrue(
            pair.wait_for(
                lambda: pair.leader is not None
                and pair.follower is not None
                and pair.leader.enabled
                and pair.follower.enabled
            ),
            "the two Workers never paired",
        )
        route_id = cast(str, cast(PairCellRuntime, pair.leader).route_id)
        response = self.server.release_pair_quarantine(
            route_id=route_id, symbol=SYMBOL, reason="operator release"
        )
        pair.shutdown()
        self.assertEqual(200, response.status_code, response.text)
        body = response.json()
        self.assertEqual("released", body["status"])
        self.assertEqual(route_id, body["route_id"])
        self.assertEqual("applied", body["leader_outcome"])
        self.assertEqual("applied", body["follower_outcome"])
        self.assertNotIn("trader_id", body)

    def test_a_safe_unpair_removes_the_route_over_the_live_controller(self) -> None:
        """Both Workers prove safety from their own fresh local evidence and
        the controller removes the route on two fresh, currently-bound
        assertions."""

        pair = _Pair(
            self,
            self.server,
            Path(self._tmp.name),
            # An edge nothing can clear keeps this test on the unpair path.
            leader_config=parse_pair_cell_config(
                {
                    "entry_edge_points": "100000",
                    "quote_max_age_seconds": 30.0,
                    "quote_max_skew_seconds": 30.0,
                }
            ),
        )
        self.assertTrue(
            pair.wait_for(
                lambda: pair.leader is not None
                and pair.follower is not None
                and pair.leader.enabled
                and pair.follower.enabled
            ),
            "the two Workers never paired",
        )
        leader_runtime = cast(PairCellRuntime, pair.leader)
        route_id = cast(str, leader_runtime.route_id)
        self.assertTrue(
            pair.wait_for(
                lambda: bool(pair.leader_results) and pair.leader_results[-1].ready, seconds=30
            ),
            f"the leader never became entry-ready "
            f"({pair.leader_results[-1] if pair.leader_results else None}).",
        )

        self.assertTrue(leader_runtime.request_safe_unpair())
        self.assertTrue(
            pair.wait_for(
                lambda: self.server.app.state.ledger.pair_route(route_id) is None, seconds=30
            ),
            "the controller never removed the route on two fresh assertions",
        )
        # Let the Worker observe the removal before its loop is stopped, so
        # this asserts the adopted end state rather than a shutdown race.
        self.assertTrue(
            pair.wait_for(
                lambda: pair.leader is not None and pair.leader.route_id != route_id, seconds=30
            ),
            "the leader never observed its own route removal",
        )
        pair.shutdown()
        self.assertIsNone(self.server.app.state.ledger.pair_route(route_id))

        # The route-scoped durable state is cleared and this Worker's own
        # account/product-scoped safety history survives.  (The exact per-table
        # accounting is asserted deterministically in
        # ``test_pair_cell_adapter``; here the pair is free to re-pair itself
        # immediately, which is precisely what must not resurrect the old
        # route.)
        directory = Path(self._tmp.name)
        record = load_durable_route(directory / f"{pair.leader_id}.paircell.route.json")
        self.assertNotEqual(route_id, None if record is None else record.route_id)
        connection = sqlite3.connect(directory / f"{pair.leader_id}.paircell.sqlite")
        try:
            tables = {
                str(row[0])
                for row in connection.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
            }
        finally:
            connection.close()
        for table in PRESERVED_SAFETY_TABLES:
            with self.subTest(preserved=table):
                self.assertIn(table, tables)


def _await_result(session: AuthenticatedWorkerSession, message_type: str, *, seconds: float = 10) -> dict:
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        session.receive_worker_relay(timeout=0.2)
        for reply in session.drain_pair_cell_results():
            if reply.get("type") == message_type:
                return cast(dict, reply)
    raise AssertionError(f"no {message_type} arrived")


if __name__ == "__main__":  # pragma: no cover - convenience entry point
    unittest.main()
