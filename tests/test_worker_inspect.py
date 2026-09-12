from __future__ import annotations

import io
import json
import sqlite3
import unittest
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from abt.worker.cli import (
    _QuarantineReleaseCompletion,
    _RediscoverCompletion,
    _UnpairCompletion,
    _one_shot_run,
    main,
)
from abt.worker.enrollment import WorkerEnrollmentError
from abt.worker.symbols_inspect import (
    allowed_products,
    edge_searchable_products,
    excluded_products,
    snapshot_symbols,
)


def _write_db(db_path: Path) -> None:
    connection = sqlite3.connect(db_path)
    try:
        connection.execute(
            "CREATE TABLE cell_universe (universe_generation INTEGER PRIMARY KEY,"
            " route_id TEXT NOT NULL, payload TEXT NOT NULL, installed_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE cell_policy (id INTEGER PRIMARY KEY CHECK (id = 1),"
            " policy_hash TEXT NOT NULL, payload TEXT NOT NULL, accepted_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE cell_sizing_plans (plan_version TEXT PRIMARY KEY,"
            " universe_generation INTEGER NOT NULL, product_id TEXT NOT NULL,"
            " direction TEXT NOT NULL, payload TEXT NOT NULL, installed_at TEXT NOT NULL,"
            " superseded_at TEXT)"
        )
        connection.execute(
            "CREATE TABLE cell_product_quarantine (product_id TEXT PRIMARY KEY,"
            " offending_worker_id TEXT NOT NULL, attempt_id TEXT NOT NULL,"
            " universe_generation INTEGER, receipt TEXT NOT NULL, quarantined_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE cell_product_suspend (product_id TEXT PRIMARY KEY,"
            " symbol TEXT NOT NULL, universe_generation INTEGER NOT NULL,"
            " reason TEXT NOT NULL, recorded_at TEXT NOT NULL)"
        )
        connection.execute(
            "CREATE TABLE cell_discovery_excluded (universe_generation INTEGER NOT NULL,"
            " symbol TEXT NOT NULL, reason TEXT NOT NULL, recorded_at TEXT NOT NULL,"
            " PRIMARY KEY (universe_generation, symbol))"
        )
        universe = {
            "route_id": "route-1",
            "universe_generation": 1,
            "products": [
                {"symbol": "EURUSD", "product_id": "EURUSD:9f2c4b1a7d3e5f01"},
                {"symbol": "GBPUSD", "product_id": "GBPUSD:1a2b3c4d5e6f7081"},
                {"symbol": "USDJPY", "product_id": "USDJPY:abcdef0123456789"},
            ],
            "leader_catalog_hash": "l",
            "follower_catalog_hash": "f",
        }
        connection.execute(
            "INSERT INTO cell_universe VALUES (1, 'route-1', ?, '2026-09-12T00:00:00+00:00')",
            (json.dumps(universe),),
        )
        policy = {
            "policy_version": "route-1",
            "mode": "live",
            "strategy_budget_usd": "0",
            "leader_risk": {"strategy_budget_usd": "10000"},
            "follower_risk": {"strategy_budget_usd": "4000"},
        }
        connection.execute(
            "INSERT INTO cell_policy VALUES (1, 'hash-1', ?, '2026-09-12T00:00:00+00:00')",
            (json.dumps(policy),),
        )
        for product_id, symbol, direction in (
            ("EURUSD:9f2c4b1a7d3e5f01", "EURUSD", "LONG"),
            ("EURUSD:9f2c4b1a7d3e5f01", "EURUSD", "SHORT"),
            ("GBPUSD:1a2b3c4d5e6f7081", "GBPUSD", "LONG"),
        ):
            payload = {"symbol": symbol, "local_max_lots": "0.4"}
            connection.execute(
                "INSERT INTO cell_sizing_plans VALUES (?, 1, ?, ?, ?,"
                " '2026-09-12T00:00:00+00:00', NULL)",
                (f"v-{product_id}-{direction}", product_id, direction, json.dumps(payload)),
            )
        connection.execute(
            "INSERT INTO cell_product_quarantine VALUES ('GBPUSD:1a2b3c4d5e6f7081', 'worker-x', 'att-1', 1,"
            " ?, '2026-09-12T00:00:00+00:00')",
            (json.dumps({"retcode": 10021, "comment": "off quotes"}),),
        )
        connection.execute(
            "INSERT INTO cell_product_suspend VALUES ('USDJPY:abcdef0123456789', 'USDJPY', 1,"
            " 'no current local quote exists for this product', '2026-09-12T01:00:00+00:00')"
        )
        connection.execute(
            "INSERT INTO cell_discovery_excluded VALUES (1, 'XAUUSD',"
            " 'hard specification mismatch: contract_size', '2026-09-12T00:00:00+00:00')"
        )
        connection.commit()
    finally:
        connection.close()


class InspectTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.db_path = self.directory / "worker.paircell.sqlite"
        _write_db(self.db_path)
        (self.directory / "worker.paircell.route.json").write_text(
            json.dumps(
                {
                    "route_id": "route-1",
                    "role": "leader",
                    "leader_worker_id": "worker-a",
                    "follower_worker_id": "worker-b",
                    "state": "ACTIVE",
                }
            ),
            encoding="utf-8",
        )
        (self.directory / "worker.paircell.budget.json").write_text(
            json.dumps({"startup_balance_usd": "10000", "account_currency": "USD", "proposal_id": "p-1"}),
            encoding="utf-8",
        )

    def tearDown(self) -> None:
        self._directory.cleanup()

    def test_allowed_is_the_universe_minus_quarantine(self) -> None:
        snapshot = snapshot_symbols(self.db_path)
        allowed = allowed_products(snapshot)
        self.assertEqual(["EURUSD", "USDJPY"], [p.symbol for p in allowed])

    def test_edge_is_current_plans_on_non_quarantined_products(self) -> None:
        snapshot = snapshot_symbols(self.db_path)
        edge = edge_searchable_products(snapshot)
        self.assertEqual(["EURUSD"], [p.symbol for p in edge])
        self.assertEqual(("LONG", "SHORT"), edge[0].directions)

    def test_excluded_names_quarantine_and_planless_products_with_reasons(self) -> None:
        snapshot = snapshot_symbols(self.db_path)
        rows = {row["symbol"]: str(row["reason"]) for row in excluded_products(snapshot)}
        self.assertIn("USDJPY", rows)
        self.assertIn("suspended at the last sizing refresh", rows["USDJPY"])
        self.assertIn("no current local quote", rows["USDJPY"])
        self.assertIn("GBPUSD", rows)
        self.assertIn("quarantined by worker-x", rows["GBPUSD"])
        self.assertIn("10021", rows["GBPUSD"])
        self.assertIn("XAUUSD", rows)
        self.assertIn("never admitted by the installed discovery run", rows["XAUUSD"])
        self.assertIn("hard specification mismatch", rows["XAUUSD"])

    def test_frozen_carries_route_budget_policy_and_universe(self) -> None:
        snapshot = snapshot_symbols(self.db_path)
        assert snapshot.frozen is not None
        self.assertEqual("route-1", snapshot.frozen.route["route_id"])
        self.assertEqual("10000", snapshot.frozen.budget["startup_balance_usd"])
        assert snapshot.policy is not None
        self.assertEqual("live", snapshot.policy.mode)
        assert snapshot.universe is not None
        self.assertEqual(3, len(snapshot.universe.products))

    def test_a_legacy_database_without_status_tables_still_inspects(self) -> None:
        legacy = self.directory / "legacy.paircell.sqlite"
        connection = sqlite3.connect(legacy)
        try:
            connection.execute(
                "CREATE TABLE cell_universe (universe_generation INTEGER PRIMARY KEY,"
                " route_id TEXT NOT NULL, payload TEXT NOT NULL, installed_at TEXT NOT NULL)"
            )
            connection.execute(
                "INSERT INTO cell_universe VALUES (1, 'route-1', ?, '2026-09-12T00:00:00+00:00')",
                (
                    json.dumps(
                        {
                            "route_id": "route-1",
                            "universe_generation": 1,
                            "products": [{"symbol": "EURUSD", "product_id": "EURUSD:9f2c4b1a7d3e5f01"}],
                            "leader_catalog_hash": "l",
                            "follower_catalog_hash": "f",
                        }
                    ),
                ),
            )
            connection.commit()
        finally:
            connection.close()
        snapshot = snapshot_symbols(legacy)
        self.assertEqual((), snapshot.suspended)
        self.assertEqual((), snapshot.discovery_excluded)
        rows = {row["symbol"]: str(row["reason"]) for row in excluded_products(snapshot)}
        self.assertIn("EURUSD", rows)
        self.assertIn("no reason recorded yet", rows["EURUSD"])


class SymbolsCommandTests(unittest.TestCase):
    def setUp(self) -> None:
        self._directory = TemporaryDirectory()
        self.directory = Path(self._directory.name)
        self.config_path = self.directory / "worker.json"
        self.config_path.write_text("{}", encoding="utf-8")
        _write_db(self.directory / "worker.paircell.sqlite")

    def tearDown(self) -> None:
        self._directory.cleanup()

    def _run(self, *argv: str) -> tuple[int, str, str]:
        output, errors = io.StringIO(), io.StringIO()
        with patch("abt.worker.cli.sys.platform", "win32"):
            code = main(
                ["symbols", *argv, "--config", str(self.config_path)],
                output=output,
                error_output=errors,
            )
        return code, output.getvalue(), errors.getvalue()

    def test_allowed_view(self) -> None:
        code, out, _ = self._run("allowed")
        self.assertEqual(0, code)
        self.assertIn("EURUSD", out)
        self.assertIn("USDJPY", out)
        self.assertNotIn("GBPUSD", out)

    def test_frozen_view(self) -> None:
        code, out, _ = self._run("frozen")
        self.assertEqual(0, code)
        self.assertIn("route-1", out)
        self.assertIn("Frozen budget", out)

    def test_edge_view(self) -> None:
        code, out, _ = self._run("edge")
        self.assertEqual(0, code)
        self.assertIn("EURUSD", out)
        self.assertNotIn("USDJPY", out)

    def test_excluded_view(self) -> None:
        code, out, _ = self._run("excluded")
        self.assertEqual(0, code)
        self.assertIn("USDJPY", out)
        self.assertIn("GBPUSD", out)

    def test_missing_database_is_a_clean_failure(self) -> None:
        (self.directory / "worker.paircell.sqlite").unlink()
        code, _, errors = self._run("allowed")
        self.assertEqual(1, code)
        self.assertIn("failed", errors)

    def test_a_missing_view_is_a_clean_failure(self) -> None:
        output, errors = io.StringIO(), io.StringIO()
        with patch("abt.worker.cli.sys.platform", "win32"):
            code = main(["symbols", "--config", str(self.config_path)], output=output, error_output=errors)
        self.assertEqual(1, code)
        self.assertIn("unknown view", errors.getvalue())


class FakeRuntime:
    def __init__(self) -> None:
        self.route_id: str | None = None
        self.cell: object | None = None
        self.generation: int | None = None
        self.diagnostic = ""
        self.unpair_calls = 0
        self.unpair_ok = True
        self.rediscover_result: object = "started"
        self.release_proposal: object = None
        self.release_statuses: dict[str, dict[str, object]] = {}
        self.closed = False

    @property
    def pairing_diagnostic(self) -> str:
        return self.diagnostic

    def universe_generation(self) -> int | None:
        return self.generation

    def request_safe_unpair(self) -> bool:
        self.unpair_calls += 1
        return self.unpair_ok

    def request_rediscovery(self, *, actor: str = "", reason: str = "") -> object:
        return self.rediscover_result

    def request_quarantine_release(self, symbol: str, *, reason: str = "") -> object:
        return self.release_proposal

    def quarantine_release_status(self, proposal_id: str) -> dict[str, object] | None:
        return self.release_statuses.get(proposal_id)

    def pump(self, observed_at: object) -> object:
        return None

    def close(self) -> None:
        self.closed = True


class WatcherTests(unittest.TestCase):
    def test_unpair_reports_nothing_to_do_off_route(self) -> None:
        watcher = _UnpairCompletion()
        self.assertIn("nothing to unpair", watcher(FakeRuntime(), None) or "")

    def test_unpair_sends_once_then_reports_removal(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        watcher = _UnpairCompletion()
        self.assertIsNone(watcher(runtime, None))
        self.assertIsNone(watcher(runtime, None))
        self.assertEqual(1, runtime.unpair_calls)
        runtime.route_id = None
        message = watcher(runtime, None)
        self.assertIn("route-1", message or "")
        self.assertIn("complete", message or "")

    def test_unpair_send_failure_raises(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        runtime.unpair_ok = False
        with self.assertRaises(WorkerEnrollmentError):
            _UnpairCompletion()(runtime, None)

    def test_rediscover_waits_for_the_cell_then_for_a_new_generation(self) -> None:
        runtime = FakeRuntime()
        watcher = _RediscoverCompletion(reason="review")
        self.assertIsNone(watcher(runtime, None))
        runtime.cell, runtime.generation = object(), 1
        self.assertIsNone(watcher(runtime, None))
        runtime.generation = 2
        message = watcher(runtime, SimpleNamespace(rediscovery_failure=None))
        self.assertIn("1 -> 2", message or "")

    def test_rediscover_failure_raises(self) -> None:
        runtime = FakeRuntime()
        runtime.cell, runtime.generation = object(), 1
        watcher = _RediscoverCompletion()
        self.assertIsNone(watcher(runtime, None))
        with self.assertRaises(WorkerEnrollmentError):
            watcher(runtime, SimpleNamespace(rediscovery_failure="peer went away"))

    def test_release_waits_for_the_cell_then_for_the_outcome(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        watcher = _QuarantineReleaseCompletion(symbol="EURUSD", reason="broker fixed")
        self.assertIsNone(watcher(runtime, None))
        runtime.cell = object()
        runtime.release_proposal = {"proposal_id": "p-1"}
        self.assertIsNone(watcher(runtime, None))
        runtime.release_statuses["p-1"] = {
            "state": "applied",
            "applied": ["EURUSD:abc"],
            "peer_applied": ["EURUSD:abc"],
        }
        message = watcher(runtime, None)
        self.assertIn("applied", message or "")
        self.assertIn("route-1", message or "")

    def test_release_rejection_raises(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        runtime.cell = object()
        runtime.release_proposal = {"proposal_id": "p-1"}
        watcher = _QuarantineReleaseCompletion(symbol="EURUSD")
        self.assertIsNone(watcher(runtime, None))
        runtime.release_statuses["p-1"] = {"state": "rejected", "reason": "unresolved attempt"}
        with self.assertRaises(WorkerEnrollmentError) as raised:
            watcher(runtime, None)
        self.assertIn("unresolved attempt", str(raised.exception))

    def test_release_without_a_route_raises(self) -> None:
        runtime = FakeRuntime()
        runtime.cell = object()
        runtime.release_proposal = None
        watcher = _QuarantineReleaseCompletion(symbol="EURUSD")
        with self.assertRaises(WorkerEnrollmentError):
            watcher(runtime, None)

    def test_one_shot_run_prints_the_completion_message(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        pumps = 0

        def pump(_: object) -> object:
            nonlocal pumps
            pumps += 1
            if pumps >= 2:
                runtime.route_id = None
            return None

        runtime.pump = pump  # type: ignore[method-assign]
        mt5 = SimpleNamespace(
            initialize=lambda: True,
            login=lambda login, *, password, server: True,
            account_info=lambda: {"login": 7, "server": "Broker-Demo"},
        )
        session = SimpleNamespace(request_password=lambda: "secret")
        output = io.StringIO()
        clock = iter([datetime(2026, 9, 12, tzinfo=UTC)] * 100).__next__
        run = _one_shot_run(
            done=_UnpairCompletion(), timeout_seconds=30.0, output=output, now=clock  # type: ignore[arg-type]
        )
        run(
            mt5=mt5,
            session=session,
            login=7,
            server="Broker-Demo",
            sleep=lambda _: None,
            maintenance=None,
            effect_journal=None,
            pair_cell_factory=lambda *args: runtime,
        )
        self.assertIn("complete", output.getvalue())
        self.assertTrue(runtime.closed)

    def test_one_shot_run_times_out_with_the_diagnostic(self) -> None:
        runtime = FakeRuntime()
        runtime.route_id = "route-1"
        runtime.diagnostic = "waiting for the peer assertion"
        mt5 = SimpleNamespace(
            initialize=lambda: True,
            login=lambda login, *, password, server: True,
            account_info=lambda: {"login": 7, "server": "Broker-Demo"},
        )
        session = SimpleNamespace(request_password=lambda: "secret")
        clock = iter([datetime(2026, 9, 12, tzinfo=UTC)] * 100).__next__
        run = _one_shot_run(
            done=_UnpairCompletion(), timeout_seconds=0.0, output=io.StringIO(), now=clock  # type: ignore[arg-type]
        )
        with self.assertRaises(WorkerEnrollmentError) as raised:
            run(
                mt5=mt5,
                session=session,
                login=7,
                server="Broker-Demo",
                sleep=lambda _: None,
                maintenance=None,
                effect_journal=None,
                pair_cell_factory=lambda *args: runtime,
            )
        self.assertIn("timed out", str(raised.exception))
        self.assertTrue(runtime.closed)
