from __future__ import annotations

import os
import unittest
from pathlib import Path
from unittest.mock import patch

from client import BridgeClient, BridgeClientError
from protocol import write_frame


class FakeProcess:
    def __init__(self, stdin: object, stdout: object) -> None:
        self.stdin = stdin
        self.stdout = stdout


class BridgeClientFailureTests(unittest.TestCase):
    def _client_with_pipe(self, *, response: dict[str, object] | None, keep_open: bool = False) -> tuple[BridgeClient, object]:
        request_read_fd, request_write_fd = os.pipe()
        request_reader = os.fdopen(request_read_fd, "rb", buffering=0)
        request_writer = os.fdopen(request_write_fd, "wb", buffering=0)
        read_fd, write_fd = os.pipe()
        reader = os.fdopen(read_fd, "rb", buffering=0)
        writer = os.fdopen(write_fd, "wb", buffering=0)
        self.addCleanup(request_reader.close)
        self.addCleanup(request_writer.close)
        self.addCleanup(reader.close)
        self.addCleanup(lambda: None if writer.closed else writer.close())
        if response is not None:
            write_frame(writer, response)
        if not keep_open:
            writer.close()
        client = BridgeClient.__new__(BridgeClient)
        client.process = FakeProcess(request_writer, reader)
        client.timeout_seconds = 0.02
        return client, writer

    def test_mismatched_response_id_is_rejected(self) -> None:
        client, _ = self._client_with_pipe(
            response={"version": 1, "id": "wrong", "ok": True, "result": {}}
        )
        with self.assertRaisesRegex(BridgeClientError, "mismatched response id"):
            client.request("health")

    def test_bridge_eof_is_explicit_failure(self) -> None:
        client, _ = self._client_with_pipe(response=None)
        with self.assertRaisesRegex(BridgeClientError, "unexpected EOF"):
            client.request("health")

    def test_bridge_timeout_is_explicit_failure(self) -> None:
        client, writer = self._client_with_pipe(response=None, keep_open=True)
        try:
            with self.assertRaisesRegex(BridgeClientError, "timed out"):
                client.request("health")
        finally:
            writer.close()

    def test_blocked_request_write_times_out(self) -> None:
        client, writer = self._client_with_pipe(response=None, keep_open=True)
        request_fd = client.process.stdin.fileno()
        os.set_blocking(request_fd, False)
        try:
            while True:
                os.write(request_fd, b"x" * 4096)
        except BlockingIOError:
            pass
        finally:
            os.set_blocking(request_fd, True)
        try:
            with self.assertRaisesRegex(BridgeClientError, "timed out while writing"):
                client.request("health")
        finally:
            writer.close()

    def test_invalid_timeout_is_rejected_before_process_start(self) -> None:
        with patch("client.subprocess.Popen") as popen:
            for invalid in (0, -1, float("inf"), float("nan"), True, 301):
                with self.subTest(invalid=invalid):
                    with self.assertRaisesRegex(BridgeClientError, "timeout_seconds"):
                        BridgeClient(
                            wine_prefix=Path("/tmp/wine"),
                            windows_python=r"C:\python.exe",
                            timeout_seconds=invalid,
                        )
            popen.assert_not_called()

    def test_partial_response_header_times_out(self) -> None:
        client, writer = self._client_with_pipe(response=None, keep_open=True)
        writer.write(b"\x00")
        writer.flush()
        try:
            with self.assertRaisesRegex(BridgeClientError, "timed out"):
                client.request("health")
        finally:
            writer.close()

    def test_success_response_requires_exact_schema_and_result(self) -> None:
        client, _ = self._client_with_pipe(
            response={"version": 1, "id": "fixed", "ok": True, "extra": True}
        )
        with patch("client.uuid.uuid4", return_value="fixed"):
            with self.assertRaisesRegex(BridgeClientError, "malformed bridge response"):
                client.request("health")

    def test_boolean_response_version_is_rejected(self) -> None:
        client, _ = self._client_with_pipe(
            response={"version": True, "id": "fixed", "ok": True, "result": {}}
        )
        with patch("client.uuid.uuid4", return_value="fixed"):
            with self.assertRaisesRegex(BridgeClientError, "unsupported protocol version"):
                client.request("health")


if __name__ == "__main__":
    unittest.main()
