from __future__ import annotations

import io
import json
from pathlib import Path
from tempfile import TemporaryDirectory
import unittest
from types import SimpleNamespace
from unittest.mock import patch

import httpx
from websockets.exceptions import ConnectionClosedError
from websockets.frames import Close

from abt.controlplane.crypto import enrollment_payload
from abt.worker.cli import HTTPEnrollmentTransport, MetaTrader5Adapter, _print_diagnostic, main
from abt.worker.credentials import retrieve_mt5_password
from abt.worker.enrollment import WorkerEnrollmentError, WorkerSessionDisconnected, register_worker
from abt.worker.session import AuthenticatedWorkerSession, open_authenticated_worker_session


class FakeMT5:
    def __init__(self) -> None:
        self.login_calls: list[tuple[int, str, str]] = []
        self.shutdown_calls = 0

    def initialize(self) -> bool:
        return True

    def login(self, login: int, *, password: str, server: str) -> bool:
        self.login_calls.append((login, password, server))
        return True

    def account_info(self) -> object:
        return {"login": 123456, "server": "Broker-Demo", "trade_mode": 0}

    def terminal_info(self) -> object:
        return {"build": 5000, "company": "MetaQuotes", "name": "MetaTrader 5"}

    def shutdown(self) -> None:
        self.shutdown_calls += 1


class FakeKeyStore:
    def __init__(self) -> None:
        self.signed_payload: bytes | None = None
        self.signed_payloads: list[bytes] = []
        self.closed = False

    def public_key_pem(self) -> str:
        return "fake-p256-public-key"

    def sign(self, payload: bytes) -> bytes:
        self.signed_payload = payload
        self.signed_payloads.append(payload)
        return b"fake-cng-signature"

    def close(self) -> None:
        self.closed = True


class FakeTransport:
    def __init__(self) -> None:
        self.controller_url: str | None = None
        self.request: dict[str, object] | None = None
        self.closed = False

    def enrollment_challenge(self, controller_url: str) -> dict[str, object]:
        self.controller_url = controller_url
        return {"challenge": "controller-one-time-challenge"}

    def enroll(self, controller_url: str, request: dict[str, object]) -> dict[str, object]:
        self.controller_url = controller_url
        self.request = dict(request)
        return {"enrollment_id": "registration-123", "expires_at": "2026-08-16T06:00:00+00:00"}

    def enrollment_status(self, controller_url: str, enrollment_id: str) -> dict[str, object]:
        return {"status": "approved"}

    def close(self) -> None:
        self.closed = True


class WorkerEnrollmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self._config_directory = TemporaryDirectory()
        self.config_path = Path(self._config_directory.name) / "worker-identity.json"

    def tearDown(self) -> None:
        self._config_directory.cleanup()

    def test_registration_sends_password_but_success_output_never_prints_it(self) -> None:
        password = "never-display-this-password"
        mt5 = FakeMT5()
        key_store = FakeKeyStore()
        transport = FakeTransport()
        output = io.StringIO()

        result = register_worker(
            controller_url="https://controller.example",
            login=123456,
            server="Broker-Demo",
            registration_invite="worker-invite",
            key_store=key_store,
            mt5=mt5,
            transport=transport,
            password_prompt=lambda _: password,
        )
        print(result.display(), file=output)

        self.assertEqual([(123456, password, "Broker-Demo")], mt5.login_calls)
        self.assertEqual(password, transport.request["mt5_password"])
        self.assertEqual("worker-invite", transport.request["registration_invite"])
        self.assertNotIn(password, output.getvalue())
        self.assertEqual(
            enrollment_payload(
                123456,
                "Broker-Demo",
                {"login": 123456, "server": "Broker-Demo", "trade_mode": 0},
                {"build": 5000, "company": "MetaQuotes", "name": "MetaTrader 5"},
                password,
                "controller-one-time-challenge",
            ),
            key_store.signed_payload,
        )
        self.assertEqual(1, mt5.shutdown_calls)
        self.assertEqual("registration-123", result.registration_id)

    def test_rejects_evidence_for_a_different_locally_logged_in_account(self) -> None:
        mt5 = FakeMT5()
        key_store = FakeKeyStore()
        transport = FakeTransport()
        mt5.account_info = lambda: {"login": 654321, "server": "Broker-Demo"}  # type: ignore[method-assign]

        with self.assertRaisesRegex(WorkerEnrollmentError, "does not match"):
            register_worker(
                controller_url="https://controller.example",
                login=123456,
                server="Broker-Demo",
                registration_invite="worker-invite",
                key_store=key_store,
                mt5=mt5,
                transport=transport,
                password_prompt=lambda _: "password",
            )

        self.assertIsNone(transport.request)
        self.assertEqual(1, mt5.shutdown_calls)

    def test_cli_uses_only_safe_success_output(self) -> None:
        password = "never-display-this-password"
        mt5 = FakeMT5()
        key_store = FakeKeyStore()
        transport = FakeTransport()
        output = io.StringIO()
        errors = io.StringIO()

        exit_code = main(
            [
                "enroll",
                "--config",
                str(self.config_path),
                "--controller-url",
                "https://controller.example",
                "--login",
                "123456",
                "--server",
                "Broker-Demo",
                "--registration-invite",
                "worker-invite",
            ],
            mt5_factory=lambda: mt5,
            transport_factory=lambda: transport,
            key_store_factory=lambda _: key_store,
            password_prompt=lambda _: password,
            output=output,
            error_output=errors,
        )

        self.assertEqual(0, exit_code)
        self.assertEqual("", errors.getvalue())
        displayed = json.loads(output.getvalue())
        self.assertEqual(
            {"account_info", "expires_at", "registration_id", "status", "terminal_info"},
            displayed.keys(),
        )
        self.assertNotIn(password, output.getvalue())
        self.assertEqual(password, transport.request["mt5_password"])
        self.assertTrue(key_store.closed)
        self.assertTrue(transport.closed)

    def test_cli_fails_closed_outside_windows(self) -> None:
        errors = io.StringIO()
        with patch("abt.worker.cli.sys.platform", "linux"):
            exit_code = main(["enroll"], error_output=errors)

        self.assertEqual(1, exit_code)
        self.assertIn("only supported on native Windows", errors.getvalue())

    def test_cli_reconcile_stops_cleanly_on_keyboard_interrupt(self) -> None:
        errors = io.StringIO()
        session = InterruptibleSession()
        self.config_path.write_text(
            '{"version":1,"controller_url":"https://controller.example","enrollment_id":"enrollment-123","login":123456,"server":"Broker-Demo","key_name":"abt-worker-device-key"}\n',
            encoding="utf-8",
        )

        with (
            patch("abt.worker.cli.sys.platform", "win32"),
            patch("abt.worker.cli.reconnect_worker_session", side_effect=KeyboardInterrupt),
        ):
            exit_code = main(
                [
                    "reconcile",
                    "--config",
                    str(self.config_path),
                ],
                mt5_factory=FakeMT5,
                transport_factory=FakeTransport,
                key_store_factory=lambda _: FakeKeyStore(),
                error_output=errors,
            )

        self.assertEqual(130, exit_code)
        self.assertIn("Worker reconciliation stopped.", errors.getvalue())

    def test_cli_does_not_print_a_password_when_submission_fails(self) -> None:
        password = "never-display-this-password"
        mt5 = FakeMT5()
        key_store = FakeKeyStore()
        errors = io.StringIO()

        exit_code = main(
            [
                "enroll",
                "--config",
                str(self.config_path),
                "--controller-url",
                "https://controller.example",
                "--login",
                "123456",
                "--server",
                "Broker-Demo",
                "--registration-invite",
                "worker-invite",
            ],
            mt5_factory=lambda: mt5,
            transport_factory=lambda: FailingTransport(password),
            key_store_factory=lambda _: key_store,
            password_prompt=lambda _: password,
            error_output=errors,
        )

        self.assertEqual(1, exit_code)
        self.assertNotIn(password, errors.getvalue())

    def test_cli_reports_success_when_local_cleanup_fails_after_enrollment(self) -> None:
        output = io.StringIO()
        errors = io.StringIO()
        transport = FakeTransport()
        transport.close = lambda: (_ for _ in ()).throw(RuntimeError("never-display-this-error"))  # type: ignore[method-assign]

        exit_code = main(
            [
                "enroll",
                "--config",
                str(self.config_path),
                "--controller-url",
                "https://controller.example",
                "--login",
                "123456",
                "--server",
                "Broker-Demo",
                "--registration-invite",
                "worker-invite",
            ],
            mt5_factory=FakeMT5,
            transport_factory=lambda: transport,
            key_store_factory=lambda _: FakeKeyStore(),
            password_prompt=lambda _: "never-display-this-password",
            output=output,
            error_output=errors,
        )

        self.assertEqual(0, exit_code)
        self.assertIn("registration-123", output.getvalue())
        self.assertIn("local cleanup failed: RuntimeError", errors.getvalue())
        self.assertNotIn("never-display-this-error", errors.getvalue())
        self.assertNotIn("never-display-this-password", errors.getvalue())

    def test_cli_verbose_mode_reports_safe_registration_diagnostics(self) -> None:
        password = "never-display-this-password"
        errors = io.StringIO()

        exit_code = main(
            [
                "enroll",
                "--config",
                str(self.config_path),
                "-v",
                "--controller-url",
                "https://controller.example",
                "--login",
                "123456",
                "--server",
                "Broker-Demo",
                "--registration-invite",
                "worker-invite",
            ],
            mt5_factory=FakeMT5,
            transport_factory=lambda: FailingTransport(password),
            key_store_factory=lambda _: FakeKeyStore(),
            password_prompt=lambda _: password,
            error_output=errors,
        )

        self.assertEqual(1, exit_code)
        self.assertIn("Worker registration failed.", errors.getvalue())
        self.assertIn("RuntimeError", errors.getvalue())
        self.assertNotIn(password, errors.getvalue())

    def test_cli_verbose_mode_reports_websocket_close_code(self) -> None:
        errors = io.StringIO()

        exit_code = main(
            [
                "enroll",
                "--config",
                str(self.config_path),
                "-v",
                "--controller-url",
                "https://controller.example",
                "--login",
                "123456",
                "--server",
                "Broker-Demo",
                "--registration-invite",
                "worker-invite",
            ],
            mt5_factory=FakeMT5,
            transport_factory=lambda: ClosingTransport(),
            key_store_factory=lambda _: FakeKeyStore(),
            password_prompt=lambda _: "never-display-this-password",
            error_output=errors,
        )

        self.assertEqual(1, exit_code)
        self.assertIn("WebSocket closed by controller with code 1008.", errors.getvalue())
        self.assertNotIn("never-display-this-password", errors.getvalue())

    def test_analysis_helper_accepts_m1_verification_requests(self) -> None:
        session = AuthenticatedWorkerSession(
            socket=FakeWebSocket(
                [
                    {
                        "type": "product_catalog_analysis_request",
                        "analysis_id": "analysis-123",
                        "request_id": "request-123",
                        "stage": "m1_verification",
                        "policy": {"label": "FX catalog"},
                        "timeframe": "M1",
                        "period_start_utc": "2026-08-10T00:00:00Z",
                        "period_end_utc": "2026-08-17T00:00:00Z",
                        "symbols": ["EURUSD"],
                    }
                ]
            ),
            reconciliation_cursor=0,
        )

        request = session.receive_product_catalog_analysis()

        self.assertEqual("m1_verification", request["stage"])
        self.assertEqual("M1", request["timeframe"])
        self.assertEqual("2026-08-10T00:00:00Z", request["period_start_utc"])
        self.assertEqual("2026-08-17T00:00:00Z", request["period_end_utc"])
        self.assertEqual(["EURUSD"], request["symbols"])

    def test_analysis_helper_buffers_order_check_requests(self) -> None:
        session = AuthenticatedWorkerSession(
            socket=FakeWebSocket(
                [
                    {
                        "type": "order_check_request",
                        "analysis_id": "order_check",
                        "request_id": "order-check-123",
                        "order": {"symbol": "EURUSD"},
                    },
                    {
                        "type": "product_catalog_analysis_request",
                        "analysis_id": "analysis-123",
                        "request_id": "request-123",
                        "stage": "catalog",
                        "policy": {},
                    },
                ]
            ),
            reconciliation_cursor=0,
        )

        analysis = session.receive_product_catalog_analysis()
        order_check = session.receive_order_check(timeout=0)

        self.assertEqual("analysis-123", analysis["analysis_id"])
        self.assertEqual({"request_id": "order-check-123", "order": {"symbol": "EURUSD"}}, order_check)

    def test_execution_recovery_helper_accepts_worker_cleanup_requests(self) -> None:
        session = AuthenticatedWorkerSession(
            socket=FakeWebSocket([{"type": "worker_cleanup_reconcile_request", "request_id": "cleanup-123"}]),
            reconciliation_cursor=0,
        )

        request = session.receive_execution_recovery()

        self.assertEqual(
            {"type": "worker_cleanup_reconcile_request", "request_id": "cleanup-123"},
            request,
        )

    def test_order_check_response_includes_broker_diagnostics(self) -> None:
        socket = FakeWebSocket([])
        session = AuthenticatedWorkerSession(socket=socket, reconciliation_cursor=0)

        session.send_order_check(
            request_id="order-check-123",
            order={"symbol": "EURUSD"},
            accepted=False,
            diagnostics={"retcode": 10016, "comment": "Invalid stops", "quote": {"bid": 1.2345, "ask": 1.2347}},
        )

        self.assertEqual(
            {
                "type": "order_check_response",
                "analysis_id": "order_check",
                "request_id": "order-check-123",
                "accepted": False,
                "order": {"symbol": "EURUSD"},
                "diagnostics": {"retcode": 10016, "comment": "Invalid stops", "quote": {"bid": 1.2345, "ask": 1.2347}},
            },
            json.loads(socket.sent[-1]),
        )

    def test_analysis_helper_includes_period_fields_in_market_data_responses(self) -> None:
        socket = FakeWebSocket([])
        session = AuthenticatedWorkerSession(socket=socket, reconciliation_cursor=0)

        session.send_product_catalog_analysis(
            analysis_id="analysis-123",
            request_id="request-123",
            collected_at="2026-08-17T07:05:00Z",
            symbols=[{"symbol": "EURUSD", "bars": [], "time_metadata": {}}],
            stage="m1_verification",
            timeframe="M1",
            period_start_utc="2026-08-10T00:00:00Z",
            period_end_utc="2026-08-17T00:00:00Z",
        )

        payload = json.loads(socket.sent[-1])
        self.assertEqual("m1_verification", payload["stage"])
        self.assertEqual("M1", payload["timeframe"])
        self.assertEqual("2026-08-10T00:00:00Z", payload["period_start_utc"])
        self.assertEqual("2026-08-17T00:00:00Z", payload["period_end_utc"])


class FailingTransport:
    def __init__(self, password: str) -> None:
        self._password = password

    def enrollment_challenge(self, controller_url: str) -> dict[str, object]:
        return {"challenge": "controller-one-time-challenge"}

    def enroll(self, controller_url: str, request: dict[str, object]) -> dict[str, object]:
        raise RuntimeError(self._password)


class InterruptibleSession:
    def __init__(self) -> None:
        self.closed = False

    def __enter__(self) -> InterruptibleSession:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.closed = True


class ClosingTransport:
    def enrollment_challenge(self, controller_url: str) -> dict[str, object]:
        return {"challenge": "controller-one-time-challenge"}

    def enroll(self, controller_url: str, request: dict[str, object]) -> dict[str, object]:
        raise ConnectionClosedError(Close(1008, ""), None)


class HTTPEnrollmentTransportTests(unittest.TestCase):
    def test_gets_the_enrollment_challenge_only_from_the_tls_endpoint(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            self.assertEqual("https://controller.example/api/enrollment-challenge", str(request.url))
            return httpx.Response(200, json={"challenge": "one-time"}, request=request)

        transport = HTTPEnrollmentTransport(httpx.Client(transport=httpx.MockTransport(handler)))
        try:
            self.assertEqual("one-time", transport.enrollment_challenge("https://controller.example")["challenge"])
        finally:
            transport.close()

    def test_posts_only_to_the_tls_enrollment_endpoint(self) -> None:
        captured: dict[str, object] = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["body"] = json.loads(request.content)
            return httpx.Response(
                201,
                json={"enrollment_id": "registration-123", "expires_at": "2026-08-16T06:00:00+00:00"},
                request=request,
            )

        transport = HTTPEnrollmentTransport(httpx.Client(transport=httpx.MockTransport(handler)))
        try:
            response = transport.enroll("https://controller.example", {"mt5_password": "password"})
        finally:
            transport.close()
        self.assertEqual("https://controller.example/api/enrollments", captured["url"])
        self.assertEqual("password", captured["body"]["mt5_password"])
        self.assertEqual("registration-123", response["enrollment_id"])

    def test_rejects_non_tls_controller_urls(self) -> None:
        transport = HTTPEnrollmentTransport(httpx.Client(transport=httpx.MockTransport(lambda _: httpx.Response(201))))
        try:
            with self.assertRaisesRegex(WorkerEnrollmentError, "HTTPS origin"):
                transport.enroll("http://controller.example", {})
        finally:
            transport.close()

    def test_includes_safe_http_status_in_enrollment_failure(self) -> None:
        transport = HTTPEnrollmentTransport(
            httpx.Client(transport=httpx.MockTransport(lambda request: httpx.Response(422, request=request)))
        )
        try:
            with self.assertRaisesRegex(WorkerEnrollmentError, "HTTP 422"):
                transport.enroll("https://controller.example", {})
        finally:
            transport.close()


class WorkerCredentialTests(unittest.TestCase):
    def test_retrieves_password_only_after_two_key_proofs(self) -> None:
        key_store = FakeKeyStore()
        certificate_socket = FakeWebSocket(
            [
                {"purpose": "certificate_delivery", "worker_id": "worker-123", "nonce": "certificate-nonce"},
                {"worker_id": "worker-123", "certificate": "certificate"},
            ]
        )
        password_socket = FakeWebSocket(
            [
                {"purpose": "password_request", "worker_id": "worker-123", "nonce": "password-nonce"},
                {"password": "memory-only-password"},
            ]
        )
        urls: list[str] = []

        password = retrieve_mt5_password(
            controller_url="https://controller.example",
            enrollment_id="enrollment-123",
            key_store=key_store,
            connect=lambda url: _connect(url, urls, certificate_socket, password_socket),
        )

        self.assertEqual("memory-only-password", password)
        self.assertEqual(
            ["wss://controller.example/api/worker/certificate", "wss://controller.example/api/worker/credentials"],
            urls,
        )
        self.assertEqual(2, len([message for message in key_store.signed_payloads if message is not None]))
        self.assertNotIn("memory-only-password", "".join(certificate_socket.sent + password_socket.sent))

    def test_rejects_non_tls_controller_url(self) -> None:
        with self.assertRaisesRegex(WorkerEnrollmentError, "HTTPS origin"):
            retrieve_mt5_password(
                controller_url="http://controller.example",
                enrollment_id="enrollment-123",
                key_store=FakeKeyStore(),
                connect=lambda _: FakeWebSocket([]),
            )

    def test_native_adapter_exposes_read_only_reconciliation_getters(self) -> None:
        api = SimpleNamespace(
            initialize=lambda: True,
            login=lambda *_args, **_kwargs: True,
            account_info=lambda: {},
            terminal_info=lambda: {},
            orders_get=lambda: ("order",),
            positions_get=lambda: ("position",),
            symbols_get=lambda: ("symbol",),
            copy_rates_range=lambda *_: ("rate",),
            symbol_info_tick=lambda _: {"time": 1},
            last_error=lambda: (10030, "Unsupported filling mode"),
            TIMEFRAME_M15=15,
            shutdown=lambda: None,
        )
        with patch.dict("sys.modules", {"MetaTrader5": api}):
            adapter = MetaTrader5Adapter()

        self.assertEqual(("order",), adapter.orders_get())
        self.assertEqual(("position",), adapter.positions_get())
        self.assertEqual(("symbol",), adapter.symbols_get())
        self.assertEqual(("rate",), adapter.copy_rates_range("EURUSD", 15, object(), object()))
        self.assertEqual({"time": 1}, adapter.symbol_info_tick("EURUSD"))
        self.assertEqual((10030, "Unsupported filling mode"), adapter.last_error())
        self.assertEqual(15, adapter.TIMEFRAME_M15)

    def test_authenticated_session_uses_one_wss_channel_for_password_and_reconciliation(self) -> None:
        key_store = FakeKeyStore()
        certificate_socket = FakeWebSocket(
            [
                {"purpose": "certificate_delivery", "worker_id": "worker-123", "nonce": "certificate-nonce"},
                {"worker_id": "worker-123", "certificate": "certificate"},
            ]
        )
        session_socket = FakeWebSocket(
            [
                {"purpose": "worker_session", "worker_id": "worker-123", "nonce": "session-nonce"},
                {"type": "authenticated", "worker_id": "worker-123", "cursor": 0},
                {"type": "password", "password": "memory-only-password"},
                {"type": "accepted", "cursor": 0},
            ]
        )
        urls: list[str] = []

        with open_authenticated_worker_session(
            controller_url="https://controller.example",
            enrollment_id="enrollment-123",
            key_store=key_store,
            connect=lambda url: _connect(url, urls, certificate_socket, session_socket),
        ) as session:
            self.assertEqual(0, session.reconciliation_cursor)
            self.assertEqual("memory-only-password", session.request_password())
            session.send_reconciliation(
                {"type": "snapshot", "cursor": 0, "observed_at": "2026-08-16T00:00:00+00:00",
                 "account": {}, "terminal": {}, "orders": [], "positions": []}
            )

        self.assertEqual(
            ["wss://controller.example/api/worker/certificate", "wss://controller.example/api/worker/session"], urls
        )
        self.assertEqual(2, len(key_store.signed_payloads))
        self.assertNotIn("memory-only-password", "".join(certificate_socket.sent + session_socket.sent))

    def test_buffers_analysis_request_while_waiting_for_reconciliation_acknowledgement(self) -> None:
        socket = FakeWebSocket(
            [
                {
                    "type": "product_catalog_analysis_request",
                    "analysis_id": "analysis-123",
                    "request_id": "request-123",
                    "stage": "catalog",
                    "policy": {},
                },
                {"type": "accepted", "cursor": 0},
            ]
        )
        session = AuthenticatedWorkerSession(socket=socket, reconciliation_cursor=0)

        session.send_reconciliation(
            {"type": "snapshot", "cursor": 0, "observed_at": "2026-08-16T00:00:00+00:00",
             "account": {}, "terminal": {}, "orders": [], "positions": []}
        )
        request = session.receive_product_catalog_analysis(timeout=0)

        self.assertEqual("analysis-123", request["analysis_id"])
        self.assertEqual("request-123", request["request_id"])

    def test_names_session_phase_when_controller_closes_websocket(self) -> None:
        key_store = FakeKeyStore()
        certificate_socket = FakeWebSocket(
            [
                {"purpose": "certificate_delivery", "worker_id": "worker-123", "nonce": "certificate-nonce"},
                {"worker_id": "worker-123", "certificate": "certificate"},
            ]
        )
        session_socket = ClosingWebSocket()

        with self.assertRaisesRegex(WorkerEnrollmentError, "authenticated worker session") as raised:
            open_authenticated_worker_session(
                controller_url="https://controller.example",
                enrollment_id="enrollment-123",
                key_store=key_store,
                connect=lambda url: certificate_socket if url.endswith("/certificate") else session_socket,
            )

        errors = io.StringIO()
        _print_diagnostic(raised.exception, errors)
        self.assertIn("authenticated worker session", errors.getvalue())
        self.assertIn("code 1011", errors.getvalue())

    def test_names_password_request_when_controller_closes_websocket(self) -> None:
        session = AuthenticatedWorkerSession(socket=ClosingWebSocket(), reconciliation_cursor=0)

        with self.assertRaisesRegex(WorkerEnrollmentError, "password request") as raised:
            session.request_password()

        errors = io.StringIO()
        _print_diagnostic(raised.exception, errors)
        self.assertIn("password request", errors.getvalue())
        self.assertIn("code 1011", errors.getvalue())

    def test_marks_reconciliation_send_disconnects_for_session_reconnect(self) -> None:
        session = AuthenticatedWorkerSession(socket=ClosingWebSocket(), reconciliation_cursor=0)

        with self.assertRaisesRegex(WorkerSessionDisconnected, "reconciliation"):
            session.send_reconciliation({"type": "snapshot", "cursor": 0})

    def test_marks_abrupt_reconciliation_send_disconnects_for_session_reconnect(self) -> None:
        session = AuthenticatedWorkerSession(socket=AbruptlyClosingWebSocket(), reconciliation_cursor=0)

        with self.assertRaisesRegex(WorkerSessionDisconnected, "reconciliation"):
            session.send_reconciliation({"type": "snapshot", "cursor": 0})


class FakeWebSocket:
    def __init__(self, responses: list[dict[str, str]]) -> None:
        self._responses = [json.dumps(response) for response in responses]
        self.sent: list[str] = []

    def __enter__(self) -> FakeWebSocket:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        pass

    def send(self, message: str) -> None:
        self.sent.append(message)

    def recv(self, timeout: float | None = None) -> str:
        return self._responses.pop(0)


class ClosingWebSocket(FakeWebSocket):
    def __init__(self) -> None:
        super().__init__([])

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        raise ConnectionClosedError(Close(1011, ""), None)

    def recv(self, timeout: float | None = None) -> str:
        raise ConnectionClosedError(Close(1011, ""), None)

    def send(self, message: str) -> None:
        raise ConnectionClosedError(None, Close(1011, ""))


class AbruptlyClosingWebSocket(FakeWebSocket):
    def __init__(self) -> None:
        super().__init__([])

    def send(self, message: str) -> None:
        raise ConnectionClosedError(None, None)


def _connect(
    url: str, urls: list[str], certificate_socket: FakeWebSocket, password_socket: FakeWebSocket
) -> FakeWebSocket:
    urls.append(url)
    return certificate_socket if url.endswith("/certificate") else password_socket
