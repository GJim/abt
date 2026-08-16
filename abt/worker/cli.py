from __future__ import annotations

import argparse
import getpass
import sys
from collections.abc import Callable, Mapping
from typing import TextIO

import httpx

from .enrollment import EnrollmentTransport, MT5Client, WorkerEnrollmentError, register_worker
from .keystore import HardwareKeyStore, WindowsCNGKeyStore


class HTTPEnrollmentTransport:
    """Submit enrollment only to an HTTPS controller origin."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=15.0, follow_redirects=False)
        self._owns_client = client is None

    def enroll(self, controller_url: str, request: dict[str, object]) -> Mapping[str, object]:
        endpoint = _enrollment_endpoint(controller_url)
        try:
            response = self._client.post(endpoint, json=request, follow_redirects=False)
            if response.status_code != 201:
                raise httpx.HTTPStatusError(
                    "Unexpected enrollment response status.",
                    request=response.request,
                    response=response,
                )
            body = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise WorkerEnrollmentError("The controller enrollment request failed.") from error
        if not isinstance(body, Mapping):
            raise WorkerEnrollmentError("The controller returned an invalid enrollment response.")
        return body

    def enrollment_challenge(self, controller_url: str) -> Mapping[str, object]:
        endpoint = _enrollment_challenge_endpoint(controller_url)
        try:
            response = self._client.get(endpoint, follow_redirects=False)
            if response.status_code != 200:
                raise httpx.HTTPStatusError(
                    "Unexpected enrollment challenge response status.",
                    request=response.request,
                    response=response,
                )
            body = response.json()
        except (httpx.HTTPError, ValueError) as error:
            raise WorkerEnrollmentError("The controller enrollment challenge request failed.") from error
        if not isinstance(body, Mapping):
            raise WorkerEnrollmentError("The controller returned an invalid enrollment challenge.")
        return body

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class MetaTrader5Adapter:
    """Lazy adapter so tests and unsupported hosts never import MetaTrader5."""

    def __init__(self) -> None:
        try:
            import MetaTrader5 as mt5
        except ImportError as error:
            raise WorkerEnrollmentError("The native MetaTrader5 package is unavailable.") from error
        self._mt5 = mt5

    def initialize(self) -> bool:
        return bool(self._mt5.initialize())

    def login(self, login: int, *, password: str, server: str) -> bool:
        return bool(self._mt5.login(login, password=password, server=server))

    def account_info(self) -> object:
        return self._mt5.account_info()

    def terminal_info(self) -> object:
        return self._mt5.terminal_info()

    def shutdown(self) -> None:
        self._mt5.shutdown()


def main(
    argv: list[str] | None = None,
    *,
    mt5_factory: Callable[[], MT5Client] = MetaTrader5Adapter,
    transport_factory: Callable[[], EnrollmentTransport] = HTTPEnrollmentTransport,
    key_store_factory: Callable[[str], HardwareKeyStore] = WindowsCNGKeyStore,
    password_prompt: Callable[[str], str] = getpass.getpass,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
) -> int:
    """Run the native Windows worker registration command."""

    output = output or sys.stdout
    error_output = error_output or sys.stderr
    if sys.platform != "win32":
        print("abt-worker is only supported on native Windows.", file=error_output)
        return 1

    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.command != "enroll":
        parser.error("a command is required")
    try:
        mt5 = mt5_factory()
        transport = transport_factory()
        try:
            key_store = key_store_factory(arguments.key_name)
            try:
                result = register_worker(
                    controller_url=arguments.controller_url,
                    login=arguments.login,
                    server=arguments.server,
                    key_store=key_store,
                    mt5=mt5,
                    transport=transport,
                    password_prompt=password_prompt,
                )
            finally:
                _close(key_store)
        finally:
            _close(transport)
    except Exception:
        print("Worker registration failed.", file=error_output)
        return 1

    print(result.display(), file=output)
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt-worker")
    commands = parser.add_subparsers(dest="command")
    enroll = commands.add_parser("enroll", help="enroll this Windows MT5 worker")
    enroll.add_argument("--controller-url", required=True, help="HTTPS controller origin")
    enroll.add_argument("--login", required=True, type=_positive_login, help="MT5 account login")
    enroll.add_argument("--server", required=True, help="MT5 server")
    enroll.add_argument(
        "--key-name",
        default="abt-worker-device-key",
        help="persistent Windows CNG key name",
    )
    return parser


def _positive_login(value: str) -> int:
    try:
        login = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if login <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return login


def _enrollment_endpoint(controller_url: str) -> str:
    return _controller_endpoint(controller_url, "/api/enrollments")


def _enrollment_challenge_endpoint(controller_url: str) -> str:
    return _controller_endpoint(controller_url, "/api/enrollment-challenge")


def _controller_endpoint(controller_url: str, path: str) -> str:
    try:
        url = httpx.URL(controller_url)
    except (TypeError, httpx.InvalidURL) as error:
        raise WorkerEnrollmentError("The controller URL must be an HTTPS origin.") from error
    if (
        url.scheme != "https"
        or not url.host
        or url.username
        or url.password
        or url.path not in ("", "/")
        or url.query
        or url.fragment
    ):
        raise WorkerEnrollmentError("The controller URL must be an HTTPS origin.")
    return str(url.copy_with(path=path))


def _close(value: object) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()
