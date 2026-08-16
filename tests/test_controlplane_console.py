from __future__ import annotations

import re
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path
from unittest.mock import patch

from abt.controlplane.console import main
from abt.controlplane.ledger import ControlLedger


class ControlPlaneConsoleTests(unittest.TestCase):
    def test_create_admin_uses_the_configured_default_ledger(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = Path(directory) / "ledger.duckdb"
            output = StringIO()
            with patch.dict("os.environ", {"ABT_LEDGER_PATH": str(ledger_path)}), redirect_stdout(output):
                self.assertEqual(0, main(["create-admin"]))

            self.assertTrue(ledger_path.exists())
            self.assertIn("username=", output.getvalue())

    def test_create_admin_generates_login_credentials_once(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            ledger_path = Path(directory) / "ledger.duckdb"
            output = StringIO()
            with redirect_stdout(output):
                self.assertEqual(0, main(["--ledger", str(ledger_path), "create-admin"]))

            match = re.fullmatch(r"username=([A-Z]{6})\npassword=(.{20})\n", output.getvalue())
            self.assertIsNotNone(match)
            assert match is not None
            ledger = ControlLedger(ledger_path)
            try:
                session = ledger.authenticate_admin(match.group(1), match.group(2), "203.0.113.10")
                self.assertTrue(session.token)
            finally:
                ledger.close()
