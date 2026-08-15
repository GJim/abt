from __future__ import annotations

import re
import tempfile
import unittest
from contextlib import redirect_stdout
from io import StringIO
from pathlib import Path

from abt.controlplane.console import main
from abt.controlplane.ledger import ControlLedger


class ControlPlaneConsoleTests(unittest.TestCase):
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
