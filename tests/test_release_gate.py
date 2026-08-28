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
from abt.controlplane.service import create_app
from abt.worker.reconciliation import MT5ReconciliationAdapter
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
        self.swap_mode = 0
        self.swap_rollover3days = 3
        self.filling_modes = ["FOK", "IOC"]
        self.allowed_directions = ["LONG", "SHORT"]
