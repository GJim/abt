from __future__ import annotations

import json
import tempfile
import unittest
import base64
from contextlib import redirect_stdout
from datetime import UTC, datetime
from io import StringIO
from pathlib import Path

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from fastapi.testclient import TestClient

from abt.controlplane.backup import BackupError, BackupManager
from abt.controlplane.console import main
from abt.controlplane.crypto import enrollment_payload, worker_proof_payload
from abt.controlplane.ledger import ControlLedger
from abt.controlplane.service import (
    _product_pair_compatibility_differences,
    _product_pair_retest_candidate,
    _screen_m15_candidates,
    _verify_m1_candidates,
    _previous_complete_utc_week,
    create_app,
)
from abt.worker.reconciliation import MT5ReconciliationAdapter
from abt.worker.session import collect_market_data_evidence, collect_product_catalog_evidence
from tests.test_controlplane_service import MemoryCertificateIssuer, MemorySecretStore


class ReleaseGateBackupTests(unittest.TestCase):
    def test_console_backup_and_restore_verification_are_explicit_local_operations(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            raft = root / "raft"
            tokens = root / "tokens"
            raft.mkdir()
            tokens.mkdir()
            (raft / "state").write_text("raft", encoding="utf-8")
            (tokens / "state").write_text("tokens", encoding="utf-8")
            ledger = root / "ledger.duckdb"
            backups = root / "backups"
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(0, main([
                    "--ledger", str(ledger), "backup", "--backup-directory", str(backups),
                    "--openbao-raft", str(raft), "--softhsm-tokens", str(tokens),
                ]))
            backup_set = Path(output.getvalue().strip().split("=", 1)[1])
            with redirect_stdout(StringIO()):
                self.assertEqual(0, main([
                    "--ledger", str(ledger), "verify-restore-set", "--backup-directory", str(backups), str(backup_set),
                ]))

    def test_hourly_backup_keeps_24_complete_sets_and_verifies_only_allowed_restore_set(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger_path = root / "ledger.duckdb"
            raft = root / "openbao-raft"
            tokens = root / "softhsm-tokens"
            raft.mkdir()
            tokens.mkdir()
            (raft / "raft.db").write_text("raft", encoding="utf-8")
            (tokens / "token").write_text("token", encoding="utf-8")
            ledger = ControlLedger(ledger_path)
            ledger.create_admin("ABCDEF", "A-secure-admin-password!")
            manager = BackupManager(ledger, root / "backups", raft, tokens)
            try:
                for sequence in range(25):
                    manager.create(f"hourly-{sequence}")
                backups = sorted((root / "backups").iterdir())
                self.assertEqual(24, len(backups))
                manifest = manager.verify_restore_set(backups[-1])
                self.assertEqual(
                    {"ledger-export.tar.gz", "openbao-raft.tar.gz", "softhsm-tokens.tar.gz"},
                    set(manifest["artifacts"]),
                )
                manifest_path = backups[-1] / "manifest.json"
                data = json.loads(manifest_path.read_text(encoding="utf-8"))
                data["artifacts"]["ledger-export.tar.gz"]["sha256"] = "invalid"
                manifest_path.write_text(json.dumps(data), encoding="utf-8")
                with self.assertRaises(BackupError):
                    manager.verify_restore_set(backups[-1])
            finally:
                ledger.close()


class TwoWorkerReleaseExerciseTests(unittest.TestCase):
    def test_two_workers_complete_read_only_control_plane_exercise_without_broker_writes(self) -> None:
        broker = _ReadOnlyBroker()
        MT5ReconciliationAdapter(broker, emit=lambda _: None).poll(datetime(2026, 8, 16, tzinfo=UTC))
        self.assertEqual(0, broker.write_calls)
        with tempfile.TemporaryDirectory() as directory:
            secret_store = MemorySecretStore()
            app = create_app(
                Path(directory) / "ledger.duckdb",
                secret_store=secret_store,
                certificate_issuer=MemoryCertificateIssuer(),
            )
            with TestClient(app, base_url="https://console.example") as client:
                app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
                first_key, first_enrollment = self._enroll(client, 123456)
                second_key, second_enrollment = self._enroll(client, 654321)
                login = client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                )
                csrf = login.json()["csrf_token"]
                first_worker = client.post(
                    f"/api/admin/enrollments/{first_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                second_worker = client.post(
                    f"/api/admin/enrollments/{second_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]

                first_certificate = app.state.ledger.active_worker(first_worker).certificate
                second_certificate = app.state.ledger.active_worker(second_worker).certificate
                self._exercise_session(client, first_key, first_worker, first_certificate, cursor=0, external_change=True)
                self._exercise_session(client, second_key, second_worker, second_certificate, cursor=0, external_change=False)

                alerts = client.get("/api/admin/alerts").json()
                self.assertEqual("external_broker_change", alerts[-1]["alert_type"])
                self.assertEqual(
                    204,
                    client.post(f"/api/admin/workers/{first_worker}/revoke", headers={"X-CSRF-Token": csrf}).status_code,
                )
                with client.websocket_connect("/api/worker/session") as websocket:
                    websocket.send_json({"worker_id": first_worker, "certificate": first_certificate})
                    with self.assertRaises(Exception):
                        websocket.receive_json()
                self._exercise_session(client, second_key, second_worker, second_certificate, cursor=0, external_change=False)
                workers = {worker["worker_id"]: worker for worker in client.get("/api/admin/workers").json()}
                self.assertEqual("revoked", workers[first_worker]["connectivity"])
                self.assertEqual("connected", workers[second_worker]["connectivity"])

    def _enroll(self, client: TestClient, login: int) -> tuple[ec.EllipticCurvePrivateKey, str]:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account = {"login": login, "server": "Broker-Demo"}
        terminal = {"name": "MetaTrader 5", "trade_allowed": False}
        password = f"worker-{login}-memory-only-password"
        challenge = client.get("/api/enrollment-challenge").json()["challenge"]
        signature = private_key.sign(
            enrollment_payload(login, "Broker-Demo", account, terminal, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        response = client.post(
            "/api/enrollments",
            json={
                "login": login, "server": "Broker-Demo", "account_info": account,
                "terminal_info": terminal, "mt5_password": password, "enrollment_challenge": challenge,
                "public_key_pem": public_key, "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )
        self.assertEqual(201, response.status_code)
        return private_key, response.json()["enrollment_id"]

    def _exercise_session(
        self, client: TestClient, key: ec.EllipticCurvePrivateKey, worker_id: str, certificate: str, *, cursor: int,
        external_change: bool,
    ) -> None:
        with client.websocket_connect("/api/worker/session") as websocket:
            websocket.send_json({"worker_id": worker_id, "certificate": certificate})
            challenge = websocket.receive_json()
            signature = key.sign(
                worker_proof_payload(purpose="worker_session", worker_id=worker_id, nonce=challenge["nonce"]),
                ec.ECDSA(hashes.SHA256()),
            )
            websocket.send_json({"signature": base64.b64encode(signature).decode("ascii")})
            self.assertEqual({"type": "authenticated", "worker_id": worker_id, "cursor": cursor}, websocket.receive_json())
            websocket.send_json({"type": "password_request"})
            self.assertTrue(websocket.receive_json()["password"])
            websocket.send_json(
                {"type": "snapshot", "cursor": cursor, "observed_at": "2026-08-16T00:00:00+00:00",
                 "account": {"login": worker_id}, "terminal": {"trade_allowed": False}, "orders": [], "positions": []}
            )
            self.assertEqual({"type": "accepted", "cursor": cursor}, websocket.receive_json())
            if external_change:
                websocket.send_json(
                    {"type": "delta", "cursor": cursor + 1, "observed_at": "2026-08-16T00:01:00+00:00",
                     "entity": "position", "ticket": "51", "change": "created", "record": {"ticket": 51, "volume": 1}}
                )
                self.assertEqual({"type": "accepted", "cursor": cursor + 1}, websocket.receive_json())


class _ReadOnlyBroker:
    def __init__(self) -> None:
        self.write_calls = 0

    def account_info(self) -> dict[str, object]:
        return {"login": 123456, "server": "Broker-Demo"}

    def terminal_info(self) -> dict[str, object]:
        return {"trade_allowed": False}

    def orders_get(self) -> list[object]:
        return []

    def positions_get(self) -> list[object]:
        return []

    def order_send(self, *_: object, **__: object) -> None:
        self.write_calls += 1
        raise AssertionError("The release gate must not write to MT5.")


class ProductCatalogReleaseGateTests(unittest.TestCase):
    def test_catalog_collection_reads_symbols_without_broker_writes(self) -> None:
        broker = _ReadOnlyCatalogBroker()

        evidence = collect_product_catalog_evidence(broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC))

        self.assertEqual(0, broker.write_calls)
        self.assertEqual("EURUSD", evidence["symbols"][0]["symbol"])
        self.assertEqual("FOREX", evidence["symbols"][0]["trade_calc_mode"])
        self.assertEqual(
            {
                "trade_tick_size",
                "contract_size",
                "volume_min",
                "volume_step",
                "filling_modes",
                "allowed_directions",
                "volume_max",
                "trade_stops_level",
                "trade_freeze_level",
                "trade_tick_value",
                "currency_margin",
                "swap_long",
                "swap_short",
                "swap_rollover3days",
            },
            {
                field
                for field in evidence["symbols"][0]
                if field
                in {
                    "trade_tick_size",
                    "contract_size",
                    "volume_min",
                    "volume_step",
                    "filling_modes",
                    "allowed_directions",
                    "volume_max",
                    "trade_stops_level",
                    "trade_freeze_level",
                    "trade_tick_value",
                    "currency_margin",
                    "swap_long",
                    "swap_short",
                    "swap_rollover3days",
                }
            },
        )

    def test_m15_collection_reads_rates_without_broker_writes(self) -> None:
        broker = _ReadOnlyMarketDataBroker()

        evidence = collect_market_data_evidence(
            broker,
            symbols=["EURUSD"],
            timeframe="M15",
            period_start_utc="2026-08-10T00:00:00Z",
            period_end_utc="2026-08-17T00:00:00Z",
            collected_at=datetime(2026, 8, 17, tzinfo=UTC),
        )

        self.assertEqual(0, broker.write_calls)
        self.assertEqual("EURUSD", evidence["symbols"][0]["symbol"])
        self.assertEqual(1000, evidence["symbols"][0]["bars"][0]["time"])
        self.assertEqual("2026-08-10T00:00:00Z", evidence["period_start_utc"])
        self.assertEqual("market_data", evidence["symbols"][0]["time_metadata"]["source_family"])

    def test_build_replace_and_retire_product_pairs_without_broker_writes(self) -> None:
        first_broker = _ReadOnlyCatalogBroker()
        second_broker = _ReadOnlyCatalogBroker()
        with tempfile.TemporaryDirectory() as directory:
            app = create_app(
                Path(directory) / "ledger.duckdb",
                secret_store=MemorySecretStore(),
                certificate_issuer=MemoryCertificateIssuer(),
            )
            with TestClient(app, base_url="https://console.example") as client:
                app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
                first_enrollment = self._enroll_server(client, login=123456, server="Broker-B")
                second_enrollment = self._enroll_server(client, login=654321, server="Broker-A")
                login = client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                )
                csrf = login.json()["csrf_token"]
                first_worker = client.post(
                    f"/api/admin/enrollments/{first_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                second_worker = client.post(
                    f"/api/admin/enrollments/{second_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                app.state.ledger.record_worker_session(first_worker)
                app.state.ledger.record_worker_session(second_worker)

                first_analysis = self._seed_buildable_analysis(
                    app,
                    first_worker_id=first_worker,
                    second_worker_id=second_worker,
                    policy={"label": "FX catalog v1"},
                    first_evidence=collect_product_catalog_evidence(first_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                    second_evidence=collect_product_catalog_evidence(second_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                )
                confirmation = client.post(
                    f"/api/admin/product-catalog-analyses/{first_analysis}/product-pair-build-confirmations",
                    headers={"X-CSRF-Token": csrf},
                    json={"first_symbol": "EURUSD", "second_symbol": "EURUSD"},
                )
                self.assertEqual(201, confirmation.status_code)
                built = client.post(
                    "/api/admin/product-pairs",
                    headers={"X-CSRF-Token": csrf},
                    json={"confirmation_id": confirmation.json()["confirmation_id"]},
                )
                self.assertEqual(201, built.status_code)

                replacement_analysis = self._seed_buildable_analysis(
                    app,
                    first_worker_id=first_worker,
                    second_worker_id=second_worker,
                    policy={"label": "FX catalog v2", "maximum_m1_p99_price_difference_points": 20.0},
                    first_evidence=collect_product_catalog_evidence(first_broker, collected_at=datetime(2026, 8, 18, tzinfo=UTC)),
                    second_evidence=collect_product_catalog_evidence(second_broker, collected_at=datetime(2026, 8, 18, tzinfo=UTC)),
                )
                replacement_confirmation = client.post(
                    f"/api/admin/product-catalog-analyses/{replacement_analysis}/product-pair-build-confirmations",
                    headers={"X-CSRF-Token": csrf},
                    json={"first_symbol": "EURUSD", "second_symbol": "EURUSD"},
                )
                self.assertEqual(201, replacement_confirmation.status_code)
                replaced = client.post(
                    f"/api/admin/product-pairs/{built.json()['product_pair_id']}/replace",
                    headers={"X-CSRF-Token": csrf},
                    json={"confirmation_id": replacement_confirmation.json()["confirmation_id"]},
                )
                self.assertEqual(201, replaced.status_code)
                retired = client.post(
                    f"/api/admin/product-pairs/{replaced.json()['product_pair_id']}/retire",
                    headers={"X-CSRF-Token": csrf},
                )
                self.assertEqual(200, retired.status_code)

        self.assertEqual(0, first_broker.write_calls)
        self.assertEqual(0, second_broker.write_calls)

    def test_product_pair_compatibility_check_and_exclusion_do_not_write_to_broker(self) -> None:
        reference_broker = _ReadOnlyCatalogBroker()
        live_broker = _ReadOnlyCatalogBroker(digits=4)
        with tempfile.TemporaryDirectory() as directory:
            app = create_app(
                Path(directory) / "ledger.duckdb",
                secret_store=MemorySecretStore(),
                certificate_issuer=MemoryCertificateIssuer(),
            )
            with TestClient(app, base_url="https://console.example") as client:
                app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
                first_enrollment = self._enroll_server(client, login=123456, server="Broker-B")
                second_enrollment = self._enroll_server(client, login=654321, server="Broker-A")
                third_enrollment = self._enroll_server(client, login=777777, server="Broker-B")
                login = client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                )
                csrf = login.json()["csrf_token"]
                first_worker = client.post(
                    f"/api/admin/enrollments/{first_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                second_worker = client.post(
                    f"/api/admin/enrollments/{second_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                third_worker = client.post(
                    f"/api/admin/enrollments/{third_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                app.state.ledger.record_worker_session(first_worker)
                app.state.ledger.record_worker_session(second_worker)

                analysis_id = self._seed_buildable_analysis(
                    app,
                    first_worker_id=first_worker,
                    second_worker_id=second_worker,
                    policy={"label": "FX catalog v1"},
                    first_evidence=collect_product_catalog_evidence(reference_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                    second_evidence=collect_product_catalog_evidence(reference_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                )
                confirmation = client.post(
                    f"/api/admin/product-catalog-analyses/{analysis_id}/product-pair-build-confirmations",
                    headers={"X-CSRF-Token": csrf},
                    json={"first_symbol": "EURUSD", "second_symbol": "EURUSD"},
                )
                self.assertEqual(201, confirmation.status_code)
                product_pair = client.post(
                    "/api/admin/product-pairs",
                    headers={"X-CSRF-Token": csrf},
                    json={"confirmation_id": confirmation.json()["confirmation_id"]},
                ).json()
                reference_specification = next(
                    item["specification"]
                    for item in product_pair["reference_specifications"]
                    if item["server"] == "Broker-B"
                )
                live_specification = collect_product_catalog_evidence(
                    live_broker,
                    collected_at=datetime(2026, 8, 18, tzinfo=UTC),
                )["symbols"][0]
                hard_block_differences, warning_differences = _product_pair_compatibility_differences(
                    reference_specification,
                    live_specification,
                )
                compatibility = app.state.ledger.record_product_pair_worker_compatibility_check(
                    product_pair["product_pair_id"],
                    third_worker,
                    checked_by="ABCDEF",
                    reference_symbol="EURUSD",
                    reference_specification=reference_specification,
                    live_specification=live_specification,
                    hard_block_differences=hard_block_differences,
                    warning_differences=warning_differences,
                )
                exclusion = app.state.ledger.exclude_product_pair_worker(
                    product_pair["product_pair_id"],
                    third_worker,
                    excluded_by="ABCDEF",
                )

        self.assertEqual(["digits"], [difference["field"] for difference in compatibility["hard_block_differences"]])
        self.assertEqual([], compatibility["warning_differences"])
        self.assertEqual("excluded", exclusion["applicability_status"])
        self.assertEqual(0, reference_broker.write_calls)
        self.assertEqual(0, live_broker.write_calls)

    def test_manual_retest_of_an_active_product_pair_does_not_write_to_broker(self) -> None:
        reference_broker = _ReadOnlyCatalogBroker()
        first_retest_broker = _ReadOnlyCatalogBroker()
        second_retest_broker = _ReadOnlyCatalogBroker()
        first_market_broker = _ReadOnlyRetestMarketDataBroker()
        second_market_broker = _ReadOnlyRetestMarketDataBroker(m15_shift=0.0001, m1_shift=0.00001)
        with tempfile.TemporaryDirectory() as directory:
            app = create_app(
                Path(directory) / "ledger.duckdb",
                secret_store=MemorySecretStore(),
                certificate_issuer=MemoryCertificateIssuer(),
            )
            with TestClient(app, base_url="https://console.example") as client:
                app.state.ledger.create_admin("ABCDEF", "A-secure-admin-password!")
                first_enrollment = self._enroll_server(client, login=123456, server="Broker-B")
                second_enrollment = self._enroll_server(client, login=654321, server="Broker-A")
                third_enrollment = self._enroll_server(client, login=777777, server="Broker-A")
                fourth_enrollment = self._enroll_server(client, login=888888, server="Broker-B")
                login = client.post(
                    "/api/admin/login", json={"username": "ABCDEF", "password": "A-secure-admin-password!"}
                )
                csrf = login.json()["csrf_token"]
                first_worker = client.post(
                    f"/api/admin/enrollments/{first_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                second_worker = client.post(
                    f"/api/admin/enrollments/{second_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                third_worker = client.post(
                    f"/api/admin/enrollments/{third_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                fourth_worker = client.post(
                    f"/api/admin/enrollments/{fourth_enrollment}/approve", headers={"X-CSRF-Token": csrf}
                ).json()["worker_id"]
                for worker_id in (first_worker, second_worker, third_worker, fourth_worker):
                    app.state.ledger.record_worker_session(worker_id)

                analysis_id = self._seed_buildable_analysis(
                    app,
                    first_worker_id=first_worker,
                    second_worker_id=second_worker,
                    policy={"label": "FX catalog v1"},
                    first_evidence=collect_product_catalog_evidence(reference_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                    second_evidence=collect_product_catalog_evidence(reference_broker, collected_at=datetime(2026, 8, 17, tzinfo=UTC)),
                )
                confirmation = client.post(
                    f"/api/admin/product-catalog-analyses/{analysis_id}/product-pair-build-confirmations",
                    headers={"X-CSRF-Token": csrf},
                    json={"first_symbol": "EURUSD", "second_symbol": "EURUSD"},
                )
                self.assertEqual(201, confirmation.status_code)
                product_pair = client.post(
                    "/api/admin/product-pairs",
                    headers={"X-CSRF-Token": csrf},
                    json={"confirmation_id": confirmation.json()["confirmation_id"]},
                ).json()

                retest = app.state.ledger.create_product_pair_retest(
                    product_pair["product_pair_id"],
                    first_worker_id=third_worker,
                    second_worker_id=fourth_worker,
                    requested_by="ABCDEF",
                    analysis_period=_previous_complete_utc_week(datetime(2026, 8, 24, tzinfo=UTC)),
                )
                first_catalog_evidence = collect_product_catalog_evidence(
                    first_retest_broker,
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                second_catalog_evidence = collect_product_catalog_evidence(
                    second_retest_broker,
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                app.state.ledger.record_product_pair_retest_catalog(
                    retest["retest_id"],
                    first_evidence=first_catalog_evidence,
                    second_evidence=second_catalog_evidence,
                )
                candidate = _product_pair_retest_candidate(
                    retest["reference_specifications"],
                    first_catalog_evidence,
                    second_catalog_evidence,
                )
                analysis_period = retest["analysis_period"]
                first_m15 = collect_market_data_evidence(
                    first_market_broker,
                    symbols=[candidate["first_symbol"]],
                    timeframe="M15",
                    period_start_utc=analysis_period["started_at_utc"],
                    period_end_utc=analysis_period["ended_at_utc"],
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                second_m15 = collect_market_data_evidence(
                    second_market_broker,
                    symbols=[candidate["second_symbol"]],
                    timeframe="M15",
                    period_start_utc=analysis_period["started_at_utc"],
                    period_end_utc=analysis_period["ended_at_utc"],
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                m15_results = _screen_m15_candidates(
                    [candidate],
                    {"symbols": {candidate["first_symbol"]: first_m15["symbols"][0]}},
                    {"symbols": {candidate["second_symbol"]: second_m15["symbols"][0]}},
                    retest["policy_snapshot"],
                )
                first_m1 = collect_market_data_evidence(
                    first_market_broker,
                    symbols=[candidate["first_symbol"]],
                    timeframe="M1",
                    period_start_utc=analysis_period["started_at_utc"],
                    period_end_utc=analysis_period["ended_at_utc"],
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                second_m1 = collect_market_data_evidence(
                    second_market_broker,
                    symbols=[candidate["second_symbol"]],
                    timeframe="M1",
                    period_start_utc=analysis_period["started_at_utc"],
                    period_end_utc=analysis_period["ended_at_utc"],
                    collected_at=datetime(2026, 8, 24, tzinfo=UTC),
                )
                m1_results = _verify_m1_candidates(
                    [candidate],
                    {"symbols": {candidate["first_symbol"]: first_m1["symbols"][0]}},
                    {"symbols": {candidate["second_symbol"]: second_m1["symbols"][0]}},
                    first_catalog_evidence["symbols"],
                    second_catalog_evidence["symbols"],
                    retest["policy_snapshot"],
                )
                completed = app.state.ledger.complete_product_pair_retest(
                    retest["retest_id"],
                    m15_screening_results=m15_results,
                    m1_verification_results=m1_results,
                    passed=True,
                )

        self.assertEqual("passed", completed["status"])
        self.assertEqual(0, reference_broker.write_calls)
        self.assertEqual(0, first_retest_broker.write_calls)
        self.assertEqual(0, second_retest_broker.write_calls)
        self.assertEqual(0, first_market_broker.write_calls)
        self.assertEqual(0, second_market_broker.write_calls)

    def _enroll_server(self, client: TestClient, *, login: int, server: str) -> str:
        private_key = ec.generate_private_key(ec.SECP256R1())
        public_key = private_key.public_key().public_bytes(
            serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
        ).decode("utf-8")
        account = {"login": login, "server": server}
        terminal = {"name": "MetaTrader 5", "trade_allowed": False}
        password = f"worker-{login}-memory-only-password"
        challenge = client.get("/api/enrollment-challenge").json()["challenge"]
        signature = private_key.sign(
            enrollment_payload(login, server, account, terminal, password, challenge),
            ec.ECDSA(hashes.SHA256()),
        )
        response = client.post(
            "/api/enrollments",
            json={
                "login": login, "server": server, "account_info": account,
                "terminal_info": terminal, "mt5_password": password, "enrollment_challenge": challenge,
                "public_key_pem": public_key, "proof_signature": base64.b64encode(signature).decode("ascii"),
            },
        )
        self.assertEqual(201, response.status_code)
        return response.json()["enrollment_id"]

    def _seed_buildable_analysis(
        self,
        app: object,
        *,
        first_worker_id: str,
        second_worker_id: str,
        policy: dict[str, object],
        first_evidence: dict[str, object],
        second_evidence: dict[str, object],
    ) -> str:
        analysis_id = app.state.ledger.create_product_catalog_analysis(
            first_worker_id=first_worker_id,
            second_worker_id=second_worker_id,
            requested_by="ABCDEF",
            policy=policy,
            analysis_period={
                "timeframe": "M15",
                "started_at_utc": "2026-08-10T00:00:00Z",
                "ended_at_utc": "2026-08-17T00:00:00Z",
            },
        )
        app.state.ledger.record_product_catalog_analysis_catalog(
            analysis_id,
            first_evidence=first_evidence,
            second_evidence=second_evidence,
            eligible_candidates=[{
                "first_symbol": "EURUSD",
                "second_symbol": "EURUSD",
                "currency_base": "EUR",
                "currency_profit": "USD",
                "first_point": 0.00001,
                "second_point": 0.00001,
            }],
        )
        app.state.ledger.complete_product_catalog_analysis(
            analysis_id,
            m15_screening_results=[{
                "first_symbol": "EURUSD",
                "second_symbol": "EURUSD",
                "currency_base": "EUR",
                "currency_profit": "USD",
                "first_point": 0.00001,
                "second_point": 0.00001,
                "screening_status": "passed",
                "statistics": {
                    "aligned_bar_count": 3,
                    "first_bar_count": 3,
                    "second_bar_count": 3,
                    "coverage_ratio": 1.0,
                    "return_correlation": 1.0,
                    "median_price_difference_points": 1.0,
                    "p99_price_difference_points": 1.0,
                    "target_point": 0.00001,
                },
                "policy_evaluation": {
                    "minimum_common_coverage": 0.99,
                    "minimum_m15_return_correlation": 0.98,
                    "coverage_passed": True,
                    "return_correlation_passed": True,
                },
                "first_market_data": {"symbol": "EURUSD", "bar_count": 3, "content_hash": "m15-first", "first_raw_epoch": 1000, "first_utc": "1970-01-01T00:16:40Z", "last_raw_epoch": 2800, "last_utc": "1970-01-01T00:46:40Z", "time_metadata": {"source_family": "market_data"}},
                "second_market_data": {"symbol": "EURUSD", "bar_count": 3, "content_hash": "m15-second", "first_raw_epoch": 1000, "first_utc": "1970-01-01T00:16:40Z", "last_raw_epoch": 2800, "last_utc": "1970-01-01T00:46:40Z", "time_metadata": {"source_family": "market_data"}},
            }],
            m1_verification_results=[{
                "first_symbol": "EURUSD",
                "second_symbol": "EURUSD",
                "currency_base": "EUR",
                "currency_profit": "USD",
                "first_point": 0.00001,
                "second_point": 0.00001,
                "screening_status": "passed",
                "verification_status": "passed",
                "statistics": {
                    "aligned_bar_count": 4,
                    "first_bar_count": 4,
                    "second_bar_count": 4,
                    "coverage_ratio": 1.0,
                    "return_correlation": 1.0,
                    "median_price_difference_points": 1.0,
                    "p99_price_difference_points": 1.0,
                    "target_point": 0.00001,
                },
                "policy_evaluation": {
                    "minimum_common_coverage": 0.99,
                    "minimum_m1_return_correlation": 0.97,
                    "maximum_m1_median_price_difference_points": 2.0,
                    "maximum_m1_p99_price_difference_points": float(policy.get("maximum_m1_p99_price_difference_points", 15.0)),
                    "coverage_passed": True,
                    "return_correlation_passed": True,
                    "median_price_difference_passed": True,
                    "p99_price_difference_passed": True,
                    "hard_block_differences_passed": True,
                },
                "hard_block_differences": [],
                "warning_differences": [],
                "first_market_data": {"symbol": "EURUSD", "bar_count": 4, "content_hash": "m1-first", "first_raw_epoch": 1000, "first_utc": "1970-01-01T00:16:40Z", "last_raw_epoch": 1180, "last_utc": "1970-01-01T00:19:40Z", "time_metadata": {"source_family": "market_data"}},
                "second_market_data": {"symbol": "EURUSD", "bar_count": 4, "content_hash": "m1-second", "first_raw_epoch": 1000, "first_utc": "1970-01-01T00:16:40Z", "last_raw_epoch": 1180, "last_utc": "1970-01-01T00:19:40Z", "time_metadata": {"source_family": "market_data"}},
            }],
            m1_verified=True,
        )
        return analysis_id


class _ReadOnlyCatalogBroker:
    def __init__(self, *, digits: int = 5) -> None:
        self.write_calls = 0
        self._digits = digits

    def symbols_get(self) -> list[object]:
        return [
            _SymbolSpec("EURUSD", "FOREX", "EUR", "USD", self._digits, 0.00001),
            _SymbolSpec("XAUUSD", "CFD", "XAU", "USD", 2, 0.01),
        ]

    def order_send(self, *_: object, **__: object) -> None:
        self.write_calls += 1
        raise AssertionError("The release gate must not write to MT5.")


class _ReadOnlyMarketDataBroker:
    TIMEFRAME_M15 = "M15"

    def __init__(self) -> None:
        self.write_calls = 0

    def symbol_info_tick(self, symbol: str) -> object:
        return {"symbol": symbol, "time": 1000}

    def copy_rates_range(self, symbol: str, timeframe: str, _from: datetime, _to: datetime) -> list[dict[str, object]]:
        assert timeframe == "M15"
        return [
            {"time": 1000, "open": 1.1000, "high": 1.1020, "low": 1.0990, "close": 1.1010},
            {"time": 1900, "open": 1.1010, "high": 1.1030, "low": 1.1000, "close": 1.1020},
        ]

    def order_send(self, *_: object, **__: object) -> None:
        self.write_calls += 1
        raise AssertionError("The release gate must not write to MT5.")


class _ReadOnlyRetestMarketDataBroker:
    TIMEFRAME_M15 = "M15"
    TIMEFRAME_M1 = "M1"

    def __init__(self, *, m15_shift: float = 0.0, m1_shift: float = 0.0) -> None:
        self.write_calls = 0
        self._m15_shift = m15_shift
        self._m1_shift = m1_shift

    def symbol_info_tick(self, symbol: str) -> object:
        return {"symbol": symbol, "time": 1000}

    def copy_rates_range(self, symbol: str, timeframe: str, _from: datetime, _to: datetime) -> list[dict[str, object]]:
        if timeframe == "M15":
            shift = self._m15_shift
            return [
                {"time": 1000, "open": 1.1000 + shift, "high": 1.1020 + shift, "low": 1.0990 + shift, "close": 1.1010 + shift},
                {"time": 1900, "open": 1.1010 + shift, "high": 1.1030 + shift, "low": 1.1000 + shift, "close": 1.1022 + shift},
                {"time": 2800, "open": 1.1020 + shift, "high": 1.1040 + shift, "low": 1.1010 + shift, "close": 1.1030 + shift},
            ]
        if timeframe == "M1":
            shift = self._m1_shift
            return [
                {"time": 1000, "open": 1.10000 + shift, "high": 1.10010 + shift, "low": 1.09990 + shift, "close": 1.10000 + shift},
                {"time": 1060, "open": 1.10000 + shift, "high": 1.10060 + shift, "low": 1.09995 + shift, "close": 1.10050 + shift},
                {"time": 1120, "open": 1.10050 + shift, "high": 1.10130 + shift, "low": 1.10040 + shift, "close": 1.10120 + shift},
                {"time": 1180, "open": 1.10120 + shift, "high": 1.10190 + shift, "low": 1.10110 + shift, "close": 1.10180 + shift},
            ]
        raise AssertionError(f"Unexpected timeframe: {timeframe}")

    def order_send(self, *_: object, **__: object) -> None:
        self.write_calls += 1
        raise AssertionError("The release gate must not write to MT5.")


class _SymbolSpec:
    def __init__(
        self,
        name: str,
        trade_calc_mode: str,
        currency_base: str,
        currency_profit: str,
        digits: int,
        point: float,
    ) -> None:
        self.name = name
        self.trade_calc_mode = trade_calc_mode
        self.currency_base = currency_base
        self.currency_profit = currency_profit
        self.digits = digits
        self.point = point
        self.trade_tick_size = point
        self.trade_contract_size = 100000.0
        self.volume_min = 0.01
        self.volume_step = 0.01
        self.volume_max = 100.0
        self.trade_stops_level = 10
        self.trade_freeze_level = 0
        self.trade_tick_value = 1.0
        self.currency_margin = currency_profit
        self.swap_long = -1.5
        self.swap_short = 0.5
        self.swap_rollover3days = 3
        self.filling_modes = ["FOK", "IOC"]
        self.allowed_directions = ["LONG", "SHORT"]
