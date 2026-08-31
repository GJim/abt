"""End-to-end integration tests across a live Worker session <-> controller
websocket <-> peer Worker session <-> :class:`~abt.pair_cell.PairExecutionCell`
adapter, plus execution-mode exclusivity and restart-construction coverage.

These run a real ``uvicorn`` server (the actual FastAPI app), connect two real
``AuthenticatedWorkerSession`` objects over real websockets, and drive two
real :class:`~abt.worker.pair_cell_adapter.PairCellRuntime` instances -- one
leader, one follower -- with deterministic MT5 fakes. They fail if the
production wiring (controller contract/lease distribution, opaque relay,
scheduler sharing, MT5 request translation) is removed or short-circuited.
"""

from __future__ import annotations

import base64
from datetime import UTC, datetime, timedelta
import http.cookies
import json
from pathlib import Path
import socket
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
from abt.worker.pair_cell_adapter import PairCellPollingConfig, PairCellRuntime
from abt.worker.session import AuthenticatedWorkerSession


class MemorySecretStore:
    """A minimal in-memory secret store; the worker password path is unused
    by these tests (Pair Execution Cell activation/relay does not touch it),
    but ``create_app`` requires a store."""

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

    def __init__(self, *, login: int, server: str, bid: float, ask: float) -> None:
        self.account = {"login": login, "server": server, "margin_free": 100_000.0}
        self.terminal = {"connected": True, "trade_allowed": True, "tradeapi_disabled": False}
        self.orders: list[dict[str, object]] = []
        self.positions: list[dict[str, object]] = []
        self.symbol = {
            "trade_tick_size": 0.00001, "point": 0.00001,
            "trade_stops_level": 5, "trade_freeze_level": 0, "trade_tick_value": 1.0,
        }
        self.tick = {"bid": bid, "ask": ask, "last": (bid + ask) / 2, "time_msc": int(time.time() * 1000)}
        self._next_ticket = 5000
        self.lock = threading.Lock()

    def account_info(self) -> object:
        return dict(self.account)

    def terminal_info(self) -> object:
        return dict(self.terminal)

    def orders_get(self) -> object:
        with self.lock:
            return [dict(o) for o in self.orders]

    def positions_get(self) -> object:
        with self.lock:
            return [dict(p) for p in self.positions]

    def symbol_info(self, symbol: str) -> object:
        return dict(self.symbol)

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return True

    def symbol_info_tick(self, symbol: str) -> object:
        with self.lock:
            self.tick["time_msc"] = int(time.time() * 1000)
            return dict(self.tick)

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
                return {"retcode": self.TRADE_RETCODE_DONE, "position": ticket, "price": request["price"]}
            if action == self.TRADE_ACTION_SLTP:
                for position in self.positions:
                    if position["ticket"] == request["position"]:
                        position["sl"], position["tp"] = request["sl"], request["tp"]
                return {"retcode": self.TRADE_RETCODE_DONE}
            if action == self.TRADE_ACTION_DEAL and "position" in request:
                self.positions = [p for p in self.positions if p["ticket"] != request["position"]]
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

    def activate_pair_contract(self, *, leader_worker_id: str, follower_worker_id: str) -> dict[str, object]:
        cookie, csrf = self.admin_session()
        response = httpx.post(
            f"{self.base_url}/api/admin/pairs/activate",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={
                "leader_worker_id": leader_worker_id,
                "follower_worker_id": follower_worker_id,
                "trader_id": "trader-1",
                "symbol": "EURUSD",
                "leader_direction": "LONG",
                "follower_direction": "SHORT",
                "leader_volume": "0.10",
                "follower_volume": "0.10",
                "filling_mode": "FOK",
                "edge_threshold": "0.0005",
                "risk_budget_usd": "50",
                "quote_max_age_seconds": 5.0,
                "quote_max_skew_seconds": 5.0,
                "arm_timeout_seconds": 5.0,
                "commit_lead_seconds": 0.3,
                "execution_expiry_seconds": 5.0,
                "mode": "live",
                "ttl_seconds": 300,
            },
            timeout=5,
        )
        assert response.status_code == 200, response.text
        return response.json()

    def claim_execution_mode(self, *, leader_worker_id: str, follower_worker_id: str, mode: str) -> httpx.Response:
        cookie, csrf = self.admin_session()
        return httpx.post(
            f"{self.base_url}/api/admin/pairs/execution-mode",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={
                "leader_worker_id": leader_worker_id, "follower_worker_id": follower_worker_id,
                "trader_id": "trader-1", "mode": mode,
            },
            timeout=5,
        )

    def release_pair_quarantine(
        self, *, leader_worker_id: str, follower_worker_id: str, symbol: str, reason: str
    ) -> httpx.Response:
        cookie, csrf = self.admin_session()
        return httpx.post(
            f"{self.base_url}/api/admin/pairs/quarantine/release",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={
                "leader_worker_id": leader_worker_id, "follower_worker_id": follower_worker_id,
                "symbol": symbol, "reason": reason,
            },
            # Release is now correlated/acknowledged: the server awaits a
            # ``pair_cell_quarantine_release_result`` from each connected
            # Worker (up to 15s per Worker, run concurrently) before
            # replying, so the client-side timeout must comfortably exceed
            # that.
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
        polling=PairCellPollingConfig(
            quote_interval=timedelta(milliseconds=50),
            readiness_interval=timedelta(milliseconds=50),
            broker_snapshot_interval=timedelta(milliseconds=100),
            calibration_interval=timedelta(seconds=1),
        ),
    )
    holder.append(runtime)
    try:
        while not stop.is_set():
            try:
                session.receive_worker_relay(timeout=0.05)
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


class PairExecutionCellEndToEndTests(unittest.TestCase):
    def setUp(self) -> None:
        self.server = _LiveServer()
        self.addCleanup(self.server.stop)
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def test_full_pair_lifecycle_over_a_live_controller_between_two_workers(self) -> None:
        leader_key, leader_id, leader_cert = self.server.approved_worker(910001, "Broker-A")
        follower_key, follower_id, follower_cert = self.server.approved_worker(910002, "Broker-B")

        # Leader is LONG (prices from the ask); follower is SHORT (prices
        # from the bid). A coherent edge needs follower.bid - leader.ask to
        # clear the configured edge_threshold.
        leader_mt5 = FakeMT5(login=910001, server="Broker-A", bid=1.10000, ask=1.10010)
        follower_mt5 = FakeMT5(login=910002, server="Broker-B", bid=1.10100, ask=1.10110)

        self.server.activate_pair_contract(leader_worker_id=leader_id, follower_worker_id=follower_id)

        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)
        follower_session = self.server.connect_worker_session(follower_key, follower_id, follower_cert)
        self.addCleanup(follower_session.socket.__exit__, None, None, None)

        directory = Path(self._tmp.name)
        stop = threading.Event()
        self.addCleanup(stop.set)
        leader_results: list = []
        follower_results: list = []
        leader_holder: list = []
        follower_holder: list = []
        leader_thread = threading.Thread(
            target=_drive,
            kwargs=dict(
                worker_id=leader_id, session=leader_session, mt5=leader_mt5, login=910001, server="Broker-A",
                directory=directory, stop=stop, results=leader_results, holder=leader_holder,
            ),
            daemon=True,
        )
        follower_thread = threading.Thread(
            target=_drive,
            kwargs=dict(
                worker_id=follower_id, session=follower_session, mt5=follower_mt5, login=910002, server="Broker-B",
                directory=directory, stop=stop, results=follower_results, holder=follower_holder,
            ),
            daemon=True,
        )
        leader_thread.start()
        follower_thread.start()

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if leader_holder and leader_holder[0].activated and follower_holder and follower_holder[0].activated:
                break
            time.sleep(0.05)
        else:
            self.fail("Pair Execution Cell adapters did not activate within the deadline.")

        deadline = time.monotonic() + 15
        while time.monotonic() < deadline:
            if (
                leader_results and follower_results
                and leader_results[-1].state == "ACTIVE"
                and follower_results[-1].state == "ACTIVE"
            ):
                break
            time.sleep(0.05)
        else:
            self.fail(
                "Pair did not reach ACTIVE on both Workers within the deadline "
                f"(leader last={leader_results[-1] if leader_results else None}, "
                f"follower last={follower_results[-1] if follower_results else None})."
            )

        stop.set()
        leader_thread.join(timeout=5)
        follower_thread.join(timeout=5)

        self.assertEqual(1, len(leader_mt5.positions))
        self.assertEqual(1, len(follower_mt5.positions))
        self.assertNotEqual(0.0, leader_mt5.positions[0]["sl"])
        self.assertNotEqual(0.0, follower_mt5.positions[0]["sl"])

    def test_pair_contract_activation_is_blocked_while_a_live_strategy_runtime_owns_the_pair(self) -> None:
        _leader_key, leader_id, _leader_cert = self.server.approved_worker(910101, "Broker-A")
        _follower_key, follower_id, _follower_cert = self.server.approved_worker(910102, "Broker-B")

        claim = self.server.claim_execution_mode(
            leader_worker_id=leader_id, follower_worker_id=follower_id, mode="strategy_runtime"
        )
        self.assertEqual(200, claim.status_code)

        cookie, csrf = self.server.admin_session()
        response = httpx.post(
            f"{self.server.base_url}/api/admin/pairs/activate",
            headers={"Cookie": cookie, "X-CSRF-Token": csrf},
            json={
                "leader_worker_id": leader_id, "follower_worker_id": follower_id, "trader_id": "trader-1",
                "symbol": "EURUSD", "leader_direction": "LONG", "follower_direction": "SHORT",
                "leader_volume": "0.10", "follower_volume": "0.10", "filling_mode": "FOK",
                "edge_threshold": "0.0005", "risk_budget_usd": "50",
                "quote_max_age_seconds": 5.0, "quote_max_skew_seconds": 5.0,
                "arm_timeout_seconds": 5.0, "commit_lead_seconds": 0.3, "execution_expiry_seconds": 5.0,
            },
            timeout=5,
        )

        self.assertEqual(409, response.status_code)

    def test_pair_cell_sync_delivers_activation_to_a_worker_that_connects_after_activation(self) -> None:
        leader_key, leader_id, leader_cert = self.server.approved_worker(910201, "Broker-A")
        _follower_key, follower_id, _follower_cert = self.server.approved_worker(910202, "Broker-B")

        self.server.activate_pair_contract(leader_worker_id=leader_id, follower_worker_id=follower_id)

        # The leader connects only *after* activation already happened; it
        # must still receive its contract/lease via pair_cell_sync_request,
        # not only via the (missed) push at activation time.
        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)

        activation = leader_session.request_pair_cell_activation()

        assert activation is not None
        self.assertEqual(leader_id, activation["contract"]["leader_worker_id"])

    def test_quarantine_release_is_pushed_to_both_connected_worker_sessions_with_actor_and_reason(self) -> None:
        leader_key, leader_id, leader_cert = self.server.approved_worker(910301, "Broker-A")
        follower_key, follower_id, follower_cert = self.server.approved_worker(910302, "Broker-B")

        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)
        follower_session = self.server.connect_worker_session(follower_key, follower_id, follower_cert)
        self.addCleanup(follower_session.socket.__exit__, None, None, None)

        responses: list[httpx.Response] = []

        def _call() -> None:
            responses.append(
                self.server.release_pair_quarantine(
                    leader_worker_id=leader_id,
                    follower_worker_id=follower_id,
                    symbol="EURUSD",
                    reason="broker confirmed the symbol is tradable again",
                )
            )

        caller = threading.Thread(target=_call, daemon=True)
        caller.start()

        # The route now awaits a genuine per-Worker outcome before replying,
        # so this test must play the Worker side of the correlated
        # request/response protocol on the main thread for both connections
        # before the background HTTP call can return.
        for session in (leader_session, follower_session):
            self.assertTrue(session.receive_worker_relay(timeout=5))
            releases = session.drain_pair_cell_quarantine_releases()
            self.assertEqual(1, len(releases))
            self.assertEqual("EURUSD", releases[0]["symbol"])
            self.assertEqual("ABCDEF", releases[0]["actor"])
            self.assertEqual("broker confirmed the symbol is tradable again", releases[0]["reason"])
            session.send_pair_cell_quarantine_release_result(
                request_id=cast(str, releases[0]["request_id"]), symbol="EURUSD", outcome="applied"
            )

        caller.join(timeout=10)
        self.assertFalse(caller.is_alive())
        response = responses[0]
        self.assertEqual(200, response.status_code, response.text)
        body = response.json()
        self.assertEqual("EURUSD", body["symbol"])
        self.assertEqual("ABCDEF", body["actor"])
        self.assertEqual("broker confirmed the symbol is tradable again", body["reason"])
        self.assertEqual("applied", body["leader_outcome"])
        self.assertEqual("applied", body["follower_outcome"])

    def test_quarantine_release_reports_409_and_never_a_false_success_when_a_worker_rejects_it(self) -> None:
        """Live-safety requirement: if either Worker genuinely rejects the
        release (e.g. an unresolved attempt still holds the quarantine),
        the route must never report overall success -- and it must say so
        per-Worker, so an operator can tell which side still needs a retry."""

        leader_key, leader_id, leader_cert = self.server.approved_worker(910303, "Broker-A")
        follower_key, follower_id, follower_cert = self.server.approved_worker(910304, "Broker-B")

        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)
        follower_session = self.server.connect_worker_session(follower_key, follower_id, follower_cert)
        self.addCleanup(follower_session.socket.__exit__, None, None, None)

        responses: list[httpx.Response] = []

        def _call() -> None:
            responses.append(
                self.server.release_pair_quarantine(
                    leader_worker_id=leader_id,
                    follower_worker_id=follower_id,
                    symbol="EURUSD",
                    reason="operator review",
                )
            )

        caller = threading.Thread(target=_call, daemon=True)
        caller.start()

        outcomes = [(leader_session, "applied"), (follower_session, "rejected")]
        for session, outcome in outcomes:
            self.assertTrue(session.receive_worker_relay(timeout=5))
            releases = session.drain_pair_cell_quarantine_releases()
            self.assertEqual(1, len(releases))
            session.send_pair_cell_quarantine_release_result(
                request_id=cast(str, releases[0]["request_id"]), symbol="EURUSD", outcome=outcome
            )

        caller.join(timeout=10)
        self.assertFalse(caller.is_alive())
        response = responses[0]
        self.assertEqual(409, response.status_code)
        body = response.json()["detail"]
        self.assertEqual("applied", body["leader_outcome"])
        self.assertEqual("rejected", body["follower_outcome"])

    def test_quarantine_release_is_rejected_while_either_worker_is_disconnected(self) -> None:
        leader_key, leader_id, leader_cert = self.server.approved_worker(910401, "Broker-A")
        _follower_key, follower_id, _follower_cert = self.server.approved_worker(910402, "Broker-B")

        # Only the leader is connected; the follower is offline, and unlike
        # contract activation (idempotent latest-state a Worker can fetch via
        # pair_cell_sync_request on reconnect), a quarantine release is a
        # one-time action, so it must never be silently queued/dropped for a
        # Worker that happens to be offline right now.
        leader_session = self.server.connect_worker_session(leader_key, leader_id, leader_cert)
        self.addCleanup(leader_session.socket.__exit__, None, None, None)

        response = self.server.release_pair_quarantine(
            leader_worker_id=leader_id, follower_worker_id=follower_id, symbol="EURUSD", reason="operator review",
        )

        self.assertEqual(409, response.status_code)
        self.assertFalse(leader_session.receive_worker_relay(timeout=0.2))


if __name__ == "__main__":
    unittest.main()
