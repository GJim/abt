from __future__ import annotations

import base64
import http.cookies
import io
import json
import re
import socket
import tempfile
import threading
import time
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import httpx
import uvicorn
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient
from websockets.sync.client import connect as websocket_connect

import abt.controlplane.service as controlplane_service
from abt.controlplane.crypto import (
    device_certificate_payload,
    enrollment_payload,
    worker_proof_payload,
    worker_rotation_payload,
)
from abt.controlplane.secrets import SecretStoreError
from abt.controlplane.service import create_app
from abt.worker.cli import HTTPEnrollmentTransport
from abt.worker.credentials import retrieve_mt5_password
from abt.worker.enrollment import (
    EnrollmentResult,
    register_worker,
)
from abt.worker.identity import (
    WorkerIdentity,
    load_identity,
    pending_identity_path,
    save_identity,
)
from abt.worker.keystore import (
    create_key_store,
    enroll_key_store,
    open_key_store,
)
from abt.worker.pkcs11_keystore import LinuxPKCS11KeyStoreFactory
from abt.worker.rotation import (
    WorkerCertificateRotated,
    maintain_worker_certificate,
)
from abt.worker.wine_mt5 import WineMetaTrader5Adapter
from tests.test_worker_pkcs11_keystore import MODULE_PATH


class MemorySecretStore:
    def __init__(self) -> None:
        self.passwords: dict[str, str] = {}

    def write_password(self, reference: str, password: str) -> None:
        self.passwords[reference] = password

    def read_password(self, reference: str) -> str:
        if reference not in self.passwords:
            raise SecretStoreError(f"unknown secret reference {reference}")
        return self.passwords[reference]

    def delete_password(self, reference: str) -> None:
        self.passwords.pop(reference, None)


class MemoryCertificateIssuer:
    def __init__(self) -> None:
        self._key = ec.generate_private_key(ec.SECP256R1())

    def issue(self, *, worker_id: str, login: int, server: str, public_key_pem: str) -> str:
        issued_at = datetime.now(UTC)
        payload = device_certificate_payload(
            worker_id=worker_id,
            login=login,
            server=server,
            public_key_pem=public_key_pem,
            issued_at=issued_at,
            expires_at=issued_at + timedelta(days=30),
        )
        return json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "signature": base64.b64encode(self._key.sign(payload, ec.ECDSA(hashes.SHA256()))).decode("ascii"),
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def verify(self, certificate: str) -> None:
        envelope = json.loads(certificate)
        payload = base64.b64decode(envelope["payload"], validate=True)
        signature = base64.b64decode(envelope["signature"], validate=True)
        self._key.public_key().verify(signature, payload, ec.ECDSA(hashes.SHA256()))


class FakeBridgeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.closed = False
        self.results: dict[str, object] = {
            "constants": {"TIMEFRAME_M1": 1, "ORDER_TYPE_BUY": 0, "TRADE_RETCODE_DONE": 10009},
            "initialize": {"initialized": True, "last_error": [1, "Success"]},
            "login": {"logged_in": True, "last_error": [1, "Success"]},
            "account_info": {"login": 123456, "server": "Broker-Demo"},
            "terminal_info": {"connected": True},
            "orders_get": [],
            "positions_get": [],
            "symbols_get": [{"name": "EURUSD"}],
            "symbol_info": {"name": "EURUSD", "point": 0.00001, "volume_min": 0.01},
            "symbol_info_tick": {"bid": 1.10000, "ask": 1.10010},
            "symbol_select": {"selected": True},
            "copy_rates_range": [{"time": 1}],
            "copy_rates_from_pos": [{"time": 1}],
            "copy_ticks_range": [{"time": 1}],
            "order_calc_margin": 10.0,
            "order_calc_profit": 2.0,
            "order_check": {"retcode": 0, "comment": "Done"},
            "last_error": [1, "Success"],
        }

    def request(self, operation: str, params: dict[str, object] | None = None) -> object:
        self.calls.append((operation, {} if params is None else params))
        return self.results[operation]

    def close(self) -> None:
        self.closed = True


class FakeOneShotMutationClient:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    def order_send(
        self,
        request: dict[str, object],
        *,
        expected_login: int,
        expected_server: str,
    ) -> object:
        self.calls.append(
            {
                "request": request,
                "expected_login": expected_login,
                "expected_server": expected_server,
            }
        )
        return {"retcode": 10009, "deal": 101, "order": 201}


@unittest.skipUnless(MODULE_PATH.is_file(), "requires SoftHSM2")
class LinuxWorkerLifecycleE2ETests(unittest.TestCase):
    def setUp(self) -> None:
        self.directory = tempfile.TemporaryDirectory()
        self.root = Path(self.directory.name)
        self.identity_path = self.root / "worker.json"
        self.keys_dir = self.root / "worker.keys"
        self.provider = LinuxPKCS11KeyStoreFactory(self.keys_dir, module_path=MODULE_PATH)

        self.secret_store = MemorySecretStore()
        self.certificate_issuer = MemoryCertificateIssuer()
        self.app = create_app(
            self.root / "ledger.duckdb",
            secret_store=self.secret_store,
            certificate_issuer=self.certificate_issuer,
        )
        self.app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")

        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            self.port = probe.getsockname()[1]
        server = uvicorn.Server(uvicorn.Config(self.app, host="127.0.0.1", port=self.port, log_level="warning"))
        self.server = server
        self.server_thread = threading.Thread(target=server.run, daemon=True)
        self.server_thread.start()
        self.base_url = f"http://127.0.0.1:{self.port}"
        self.controller_https = "https://testserver"

        # Wait for server
        for _ in range(50):
            try:
                if httpx.get(f"{self.base_url}/health", timeout=0.2).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.05)
        else:
            self.fail("uvicorn server did not start")

    def tearDown(self) -> None:
        self.server.should_exit = True
        try:
            httpx.get(f"{self.base_url}/health", timeout=0.2)
        except httpx.HTTPError:
            pass
        self.server_thread.join(timeout=3)
        self.directory.cleanup()

    def _login_admin(self) -> dict[str, str]:
        response = httpx.post(
            f"{self.base_url}/api/admin/login",
            json={"username": "ABCDEF", "password": "A-secure-admin-password!"},
        )
        self.assertEqual(200, response.status_code)
        cookie = http.cookies.SimpleCookie()
        cookie.load(response.headers["set-cookie"])
        csrf_token = response.json()["csrf_token"]
        return {"Cookie": f"abt_admin_session={cookie['abt_admin_session'].value}", "X-CSRF-Token": csrf_token}

    def test_full_linux_worker_lifecycle_e2e(self) -> None:
        # Step 0: create invite
        admin_headers = self._login_admin()
        invite_response = httpx.post(
            f"{self.base_url}/api/admin/registration-invites",
            headers=admin_headers,
            json={"role": "worker"},
        )
        self.assertEqual(201, invite_response.status_code)
        invite = invite_response.json()["invite"]

        # Step 1: Enroll using Linux PKCS#11 key store and Wine MT5 adapter
        bridge = FakeBridgeClient()
        mutation = FakeOneShotMutationClient()
        mt5 = WineMetaTrader5Adapter(
            client=bridge,
            mutation_client_factory=lambda: mutation,
        )

        transport = HTTPEnrollmentTransport(
            client=TestClient(self.app, base_url=self.controller_https)
        )
        key = enroll_key_store(self.provider, "device-key")
        original_public_key_pem = key.public_key_pem()

        result = register_worker(
            controller_url=self.controller_https,
            login=123456,
            server="Broker-Demo",
            registration_invite=invite,
            key_store=key,
            mt5=mt5,
            transport=transport,
            password_prompt=lambda _: "broker-secret-pw",
        )
        self.assertIsNotNone(result.registration_id)
        enrollment_id = result.registration_id

        # Verify pending enrollment on controller
        status = transport.enrollment_status(self.controller_https, enrollment_id)
        self.assertEqual("pending", status["status"])

        # Save pending identity
        pending_id = WorkerIdentity(
            controller_url=self.controller_https,
            enrollment_id=enrollment_id,
            login=123456,
            server="Broker-Demo",
            key_name="device-key",
        )
        save_identity(pending_identity_path(self.identity_path), pending_id, replace=True)

        # Step 2: Admin Approval
        approve_resp = httpx.post(
            f"{self.base_url}/api/admin/enrollments/{enrollment_id}/approve",
            headers=admin_headers,
        )
        self.assertEqual(200, approve_resp.status_code)
        status = transport.enrollment_status(self.controller_https, enrollment_id)
        self.assertEqual("approved", status["status"])

        # Promote pending to active identity
        save_identity(self.identity_path, pending_id, replace=True)
        pending_identity_path(self.identity_path).unlink()

        # Step 3: Reconcile (fetching certificate & credentials with key proof)
        key.close()
        active_key = open_key_store(self.provider, "device-key")
        self.assertIsNotNone(active_key)

        # Fresh Wine MT5 adapter for reconciliation runtime
        reconcile_bridge = FakeBridgeClient()
        mutation = FakeOneShotMutationClient()
        mt5 = WineMetaTrader5Adapter(
            client=reconcile_bridge,
            mutation_client_factory=lambda: mutation,
        )

        # Fetch password via WebSocket using key proof
        ws_url = f"ws://127.0.0.1:{self.port}"
        password = retrieve_mt5_password(
            controller_url=self.controller_https,
            enrollment_id=enrollment_id,
            key_store=active_key,
            connect=lambda url: websocket_connect(re.sub(r"^wss?://[^/]+", f"ws://127.0.0.1:{self.port}", url)),
        )
        self.assertEqual("broker-secret-pw", password)

        # MT5 login & reconciliation checks
        self.assertTrue(mt5.login(123456, password=password, server="Broker-Demo"))
        self.assertEqual({"login": 123456, "server": "Broker-Demo"}, mt5.account_info())
        self.assertEqual({"connected": True}, mt5.terminal_info())
        self.assertEqual([], mt5.orders_get())
        self.assertEqual([], mt5.positions_get())

        # Step 4: Trade (one-shot mutation boundary checks expected_login & expected_server)
        check_result = mt5.order_check({"action": 1, "symbol": "EURUSD", "volume": 0.01})
        self.assertEqual(0, check_result["retcode"])

        open_result = mt5.order_send({"action": 1, "symbol": "EURUSD", "volume": 0.01})
        self.assertEqual(10009, open_result["retcode"])
        self.assertEqual(1, len(mutation.calls))
        self.assertEqual(123456, mutation.calls[0]["expected_login"])
        self.assertEqual("Broker-Demo", mutation.calls[0]["expected_server"])

        # Step 5: Restart (fail-closed reopen of existing PKCS#11 key)
        mt5.close()
        active_key.close()

        reopened_key = open_key_store(self.provider, "device-key")
        self.assertEqual(original_public_key_pem, reopened_key.public_key_pem())

        # Step 6: Certificate rotation (atomic key creation & retired key cleanup)
        # Fetch certificate first
        cert_ws_url = f"ws://127.0.0.1:{self.port}/api/worker/certificate"
        with websocket_connect(cert_ws_url) as ws:
            ws.send(json.dumps({"enrollment_id": enrollment_id}))
            challenge = json.loads(ws.recv())
            worker_id = challenge["worker_id"]
            nonce = challenge["nonce"]
            sig = reopened_key.sign(worker_proof_payload(purpose="certificate_delivery", worker_id=worker_id, nonce=nonce))
            ws.send(json.dumps({"signature": base64.b64encode(sig).decode("ascii")}))
            delivery = json.loads(ws.recv())
            certificate = delivery["certificate"]

        # Advance simulated time past half-life (16 days)
        issued_date = datetime.now(UTC)
        future_date = issued_date + timedelta(days=16)

        errors_io = io.StringIO()
        with self.assertRaises(WorkerCertificateRotated):
            maintain_worker_certificate(
                identity_path=self.identity_path,
                identity=pending_id,
                worker_id=worker_id,
                certificate=certificate,
                current_key=reopened_key,
                key_store_factory=self.provider,
                transport=transport,
                now=lambda: future_date,
                error_output=errors_io,
            )

        # Verify replacement key is in rotated identity
        rotated_id = load_identity(self.identity_path)
        self.assertNotEqual("device-key", rotated_id.key_name)
        self.assertTrue(rotated_id.key_name.startswith("device-key-rotation-"))

        # Verify replacement key can be opened
        replacement_key = open_key_store(self.provider, rotated_id.key_name)
        self.assertIsNotNone(replacement_key)
        self.assertNotEqual(original_public_key_pem, replacement_key.public_key_pem())

        # Clean up keys
        replacement_key.close()
        reopened_key.close()
        transport.close()


if __name__ == "__main__":
    unittest.main()
