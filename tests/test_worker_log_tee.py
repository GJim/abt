"""Tests for in-process log tee (--log-file) and termination signal handling."""

from __future__ import annotations

import io
import logging
import signal
import tempfile
import threading
import unittest
from datetime import UTC, datetime
from pathlib import Path

from abt.worker.cli import _ConsoleLogHandler, _configure_logging, _parser
from abt.worker.reconciliation import (
    _GracefulShutdown,
    _run_reconciliation_with_relay,
    _termination_to_keyboard_interrupt,
    install_graceful_signal_handlers,
)


class LogTeeTests(unittest.TestCase):
    def test_reconcile_parser_accepts_log_file(self) -> None:
        arguments = _parser().parse_args(["reconcile", "--log-file", "worker.log"])
        self.assertEqual(Path("worker.log"), arguments.log_file)

    def test_reconcile_parser_defaults_log_file_to_none(self) -> None:
        arguments = _parser().parse_args(["reconcile"])
        self.assertIsNone(arguments.log_file)

    def _emit_through_tee(self, *, verbose: bool) -> tuple[str, str]:
        console = io.StringIO()
        root = logging.getLogger()
        saved_handlers = list(root.handlers)
        saved_level = root.level
        for handler in saved_handlers:
            root.removeHandler(handler)
        try:
            with tempfile.TemporaryDirectory() as directory:
                log_file = Path(directory) / "worker.log"
                _configure_logging(verbose=verbose, log_file=log_file, error_output=console)
                added = list(root.handlers)
                logging.getLogger("abt.worker.cli").info("hello-tee-marker")
                for handler in added:
                    handler.flush()
                # Close before leaving the TemporaryDirectory: on Windows an
                # open FileHandler blocks cleanup with PermissionError.
                for handler in added:
                    root.removeHandler(handler)
                    handler.close()
                file_content = log_file.read_text(encoding="utf-8")
        finally:
            for handler in list(root.handlers):
                root.removeHandler(handler)
                try:
                    handler.close()
                except Exception:
                    pass
            for handler in saved_handlers:
                root.addHandler(handler)
            root.setLevel(saved_level)
        return console.getvalue(), file_content

    def test_log_file_tees_to_console_and_file(self) -> None:
        console, file_content = self._emit_through_tee(verbose=True)
        self.assertIn("hello-tee-marker", console)
        self.assertIn("hello-tee-marker", file_content)
        # Both sides share the UTC "Z" format.
        self.assertRegex(console, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z INFO abt\.worker\.cli: ")
        self.assertRegex(file_content, r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+Z INFO abt\.worker\.cli: ")

    def test_log_file_implies_verbose_without_flag(self) -> None:
        console, file_content = self._emit_through_tee(verbose=False)
        self.assertIn("hello-tee-marker", console)
        self.assertIn("hello-tee-marker", file_content)

    def test_no_flags_adds_no_handlers(self) -> None:
        root = logging.getLogger()
        before = len(root.handlers)
        _configure_logging(verbose=False, log_file=None, error_output=io.StringIO())
        # Under pytest the root logger may already carry capture handlers; the
        # tee must not add more when neither -v nor --log-file is given.
        self.assertEqual(before, len(root.handlers))

    def test_console_handler_survives_a_broken_pipe(self) -> None:
        class BrokenStream(io.StringIO):
            def write(self, _message: str) -> int:
                raise BrokenPipeError("pipe closed by Tee-Object")

        handler = _ConsoleLogHandler(BrokenStream())
        record = logging.LogRecord("tee", logging.INFO, __file__, 1, "dropped", (), None)
        saved, logging.raiseExceptions = logging.raiseExceptions, False
        try:
            handler.emit(record)
        finally:
            logging.raiseExceptions = saved


class GracefulSignalTests(unittest.TestCase):
    def test_termination_signals_raise_keyboard_interrupt(self) -> None:
        previous = install_graceful_signal_handlers()
        try:
            seen = []
            for name in ("SIGBREAK", "SIGTERM"):
                signum = getattr(signal, name, None)
                if signum is None:
                    continue
                seen.append(name)
                self.assertIs(_termination_to_keyboard_interrupt, signal.getsignal(signum))
                with self.assertRaises(KeyboardInterrupt):
                    _termination_to_keyboard_interrupt(signum, None)
            self.assertTrue(seen, "expected at least one termination signal on this platform")
        finally:
            for signum, old in previous.items():
                signal.signal(signum, old)

    def test_install_outside_main_thread_keeps_defaults(self) -> None:
        outcomes: list[dict[int, object]] = []

        def install() -> None:
            outcomes.append(install_graceful_signal_handlers())

        worker = threading.Thread(target=install)
        worker.start()
        worker.join()
        self.assertEqual([{}], outcomes)


class FakePairCell:
    def __init__(self, *, completes: bool) -> None:
        self._completes = completes
        self._complete = False
        self.requested_reasons: list[str] = []

    @property
    def should_exit(self) -> bool:
        return False

    @property
    def shutdown_complete(self) -> bool:
        return self._complete

    def pump(self, _observed_at: object) -> None:
        return None

    def drain_relay(self) -> None:
        return None

    def request_close(self, reason: str) -> None:
        self.requested_reasons.append(reason)
        if self._completes:
            self._complete = True
        return None


class InterruptOnceSession:
    def __init__(self, *, always: bool = False) -> None:
        self._always = always
        self.calls = 0

    def receive_worker_relay(self, timeout: float | None = None) -> bool:
        del timeout
        self.calls += 1
        if self._always or self.calls == 1:
            raise KeyboardInterrupt
        return False


class FakeReconciliation:
    def __init__(self) -> None:
        self.polls = 0

    def poll(self, _observed_at: object) -> None:
        self.polls += 1


class RelayInterruptTests(unittest.TestCase):
    def _run(self, *, completes: bool, always: bool) -> FakePairCell:
        cell = FakePairCell(completes=completes)
        _run_reconciliation_with_relay(
            FakeReconciliation(),
            mt5=object(),
            session=InterruptOnceSession(always=always),  # type: ignore[arg-type]
            now=lambda: datetime.now(UTC),
            safety=None,
            maintenance=None,
            live_state=None,
            effect_journal=None,
            pair_cell=cell,  # type: ignore[arg-type]
            graceful_shutdown=_GracefulShutdown(),
        )
        return cell

    def test_first_interrupt_requests_close_and_completes(self) -> None:
        cell = self._run(completes=True, always=False)
        self.assertEqual(["operator interrupt"], cell.requested_reasons)

    def test_second_interrupt_forces_exit(self) -> None:
        with self.assertRaises(KeyboardInterrupt):
            self._run(completes=False, always=True)


if __name__ == "__main__":
    unittest.main()
