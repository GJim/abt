from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import TextIO

import httpx

from .enrollment import EnrollmentTransport, MT5Client, WorkerEnrollmentError, register_worker
from .effect_journal import WorkerEffectJournal
from .identity import (
    WorkerIdentity,
    default_identity_path,
    ensure_identity_can_be_saved,
    load_identity,
    pending_identity_path,
    save_identity,
)
from .keystore import HardwareKeyStore, WindowsCNGKeyStore
from .pair_cell_adapter import PairCellRuntime, PairCellStartupOptions
from .reconciliation import reconnect_worker_session
from .rotation import (
    WorkerCertificateExpired,
    WorkerCertificateRotated,
    WorkerRotationError,
    ensure_worker_certificate_current,
    maintain_worker_certificate,
)
from .session import open_authenticated_worker_session


class _UtcLogFormatter(logging.Formatter):
    """Render CLI logs in a host-independent timestamp format."""

    def formatTime(self, record: logging.LogRecord, datefmt: str | None = None) -> str:
        return datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds").replace(
            "+00:00", "Z"
        )


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
                raise WorkerEnrollmentError(f"The controller enrollment request returned HTTP {response.status_code}.")
            body = response.json()
        except WorkerEnrollmentError:
            raise
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
                raise WorkerEnrollmentError(f"The controller enrollment challenge request returned HTTP {response.status_code}.")
            body = response.json()
        except WorkerEnrollmentError:
            raise
        except (httpx.HTTPError, ValueError) as error:
            raise WorkerEnrollmentError("The controller enrollment challenge request failed.") from error
        if not isinstance(body, Mapping):
            raise WorkerEnrollmentError("The controller returned an invalid enrollment challenge.")
        return body

    def enrollment_status(self, controller_url: str, enrollment_id: str) -> Mapping[str, object]:
        endpoint = _controller_endpoint(controller_url, f"/api/enrollments/{enrollment_id}/status")
        try:
            response = self._client.get(endpoint, follow_redirects=False)
            if response.status_code != 200:
                raise WorkerEnrollmentError(f"The controller enrollment status request returned HTTP {response.status_code}.")
            body = response.json()
        except WorkerEnrollmentError:
            raise
        except (httpx.HTTPError, ValueError) as error:
            raise WorkerEnrollmentError("The controller enrollment status request failed.") from error
        if not isinstance(body, Mapping):
            raise WorkerEnrollmentError("The controller returned an invalid enrollment status.")
        return body

    def rotation_challenge(self, controller_url: str, worker_id: str, public_key_pem: str) -> Mapping[str, object]:
        return self._rotation_request(
            "POST", controller_url, "/api/workers/certificates/rotation-challenge",
            {"worker_id": worker_id, "public_key_pem": public_key_pem},
        )

    def rotate(
        self, controller_url: str, worker_id: str, public_key_pem: str, old_signature: str, replacement_signature: str
    ) -> Mapping[str, object]:
        return self._rotation_request(
            "POST", controller_url, "/api/workers/certificates/rotate",
            {
                "worker_id": worker_id,
                "public_key_pem": public_key_pem,
                "old_key_signature": old_signature,
                "replacement_key_signature": replacement_signature,
            },
        )

    def _rotation_request(
        self, method: str, controller_url: str, path: str, request: dict[str, object]
    ) -> Mapping[str, object]:
        try:
            response = self._client.request(method, _controller_endpoint(controller_url, path), json=request, follow_redirects=False)
            if response.status_code != 200:
                raise WorkerEnrollmentError(f"The controller certificate rotation request returned HTTP {response.status_code}.")
            body = response.json()
        except WorkerEnrollmentError:
            raise
        except (httpx.HTTPError, ValueError) as error:
            raise WorkerEnrollmentError("The controller certificate rotation request failed.") from error
        if not isinstance(body, Mapping):
            raise WorkerEnrollmentError("The controller returned an invalid certificate rotation response.")
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

    def orders_get(self) -> object:
        return self._mt5.orders_get()

    def positions_get(self) -> object:
        return self._mt5.positions_get()

    def symbols_get(self) -> object:
        return self._mt5.symbols_get()

    def copy_rates_range(self, symbol: str, timeframe: object, from_time: object, to_time: object) -> object:
        return self._mt5.copy_rates_range(symbol, timeframe, from_time, to_time)

    def symbol_info_tick(self, symbol: str) -> object:
        return self._mt5.symbol_info_tick(symbol)

    def symbol_info(self, symbol: str) -> object:
        return self._mt5.symbol_info(symbol)

    def symbol_select(self, symbol: str, enable: bool) -> bool:
        return bool(self._mt5.symbol_select(symbol, enable))

    def order_calc_margin(self, action: int, symbol: str, volume: float, price: float) -> object:
        return self._mt5.order_calc_margin(action, symbol, volume, price)

    def order_calc_profit(self, action: int, symbol: str, volume: float, open_price: float, close_price: float) -> object:
        return self._mt5.order_calc_profit(action, symbol, volume, open_price, close_price)

    def copy_ticks_range(self, symbol: str, from_time: object, to_time: object, flags: int) -> object:
        return self._mt5.copy_ticks_range(symbol, from_time, to_time, flags)

    def order_check(self, request: dict[str, object]) -> object:
        return self._mt5.order_check(request)

    def order_send(self, request: dict[str, object]) -> object:
        return self._mt5.order_send(request)

    def last_error(self) -> object:
        return self._mt5.last_error()

    def __getattr__(self, name: str) -> object:
        if name.startswith(("COPY_TICKS_", "TIMEFRAME_", "TRADE_", "ORDER_", "POSITION_")):
            return getattr(self._mt5, name)
        raise AttributeError(name)

    def shutdown(self) -> None:
        self._mt5.shutdown()


def main(
    argv: list[str] | None = None,
    *,
    mt5_factory: Callable[[], MT5Client] = MetaTrader5Adapter,
    transport_factory: Callable[[], EnrollmentTransport] = HTTPEnrollmentTransport,
    key_store_factory: Callable[[str], HardwareKeyStore] = WindowsCNGKeyStore,
    password_prompt: Callable[[str], str] | None = None,
    input_prompt: Callable[[str], str] = input,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
    interactive: bool | None = None,
) -> int:
    """Run the native Windows worker registration command."""

    output = output or sys.stdout
    error_output = error_output or sys.stderr
    if sys.platform != "win32":
        print("abt-worker is only supported on native Windows.", file=error_output)
        return 1

    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.command not in {"enroll", "reconcile"}:
        parser.error("a command is required")
    if arguments.command == "reconcile":
        try:
            pair_cell_options = _pair_cell_startup_options(
                arguments,
                interactive=_interactive(interactive),
                input_prompt=input_prompt,
                output=output,
            )
        except WorkerEnrollmentError as error:
            print(f"Worker reconciliation failed: {error}", file=error_output)
            return 1
    else:
        pair_cell_options = PairCellStartupOptions()
    if arguments.verbose:
        handler = logging.StreamHandler(error_output)
        handler.setFormatter(
            _UtcLogFormatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        )
        logging.basicConfig(
            level=logging.INFO,
            handlers=[handler],
        )
        logging.getLogger("abt").setLevel(logging.DEBUG)
    cleanup_errors: list[Exception] = []
    try:
        if arguments.command == "reconcile":
            identity = load_identity(arguments.config)
            transport = transport_factory()
            try:
                pending_path = pending_identity_path(arguments.config)
                pending_identity = load_identity(pending_path) if pending_path.exists() else None
                candidate = pending_identity or identity
                status = _required_enrollment_status(transport.enrollment_status(candidate.controller_url, candidate.enrollment_id))
                if status == "approved" and pending_identity is not None:
                    save_identity(arguments.config, pending_identity, replace=True)
                    pending_path.unlink()
                    identity = pending_identity
            finally:
                _close_safely(transport, cleanup_errors)
            if status == "pending":
                print("Worker enrollment is pending administrator approval.", file=output)
                return 0
            if status != "approved":
                raise WorkerEnrollmentError("Worker enrollment is no longer active.")
        else:
            existing_identity = load_identity(arguments.config) if arguments.config.exists() else None
            ensure_identity_can_be_saved(arguments.config, replace=arguments.replace_config)
            identity = WorkerIdentity(
                controller_url=_required_prompted(arguments.controller_url, "Controller URL: ", input_prompt),
                login=_prompted_login(arguments.login, input_prompt),
                server=_required_prompted(arguments.server, "MT5 server: ", input_prompt),
                enrollment_id="",
                key_name=arguments.key_name or (
                    existing_identity.key_name if existing_identity is not None else "abt-worker-device-key"
                ),
            )
            registration_invite = _required_prompted(
                arguments.registration_invite, "Registration invite: ", input_prompt
            )
        mt5 = mt5_factory()
        key_store = key_store_factory(identity.key_name)
        try:
            if arguments.command == "reconcile":
                _reconcile_with_certificate_maintenance(
                    identity_path=arguments.config,
                    identity=identity,
                    mt5=mt5,
                    key_store=key_store,
                    key_store_factory=key_store_factory,
                    transport_factory=transport_factory,
                    error_output=error_output,
                    pair_cell_config_path=getattr(arguments, "pair_cell_config", None),
                    pair_cell_options=pair_cell_options,
                )
                return 0
            transport = transport_factory()
            try:
                result = register_worker(
                    controller_url=identity.controller_url,
                    login=identity.login,
                    server=identity.server,
                    registration_invite=registration_invite,
                    key_store=key_store,
                    mt5=mt5,
                    transport=transport,
                    password_prompt=password_prompt or input_prompt,
                )
                saved_identity = WorkerIdentity(
                    controller_url=identity.controller_url,
                    enrollment_id=result.registration_id,
                    login=identity.login,
                    server=identity.server,
                    key_name=identity.key_name,
                )
                save_identity(
                    pending_identity_path(arguments.config) if existing_identity is not None else arguments.config,
                    saved_identity,
                    replace=True if existing_identity is not None else arguments.replace_config,
                )
            finally:
                _close_safely(transport, cleanup_errors)
        finally:
            _close_safely(key_store, cleanup_errors)
    except KeyboardInterrupt:
        print("Worker reconciliation stopped.", file=error_output)
        return 130
    except WorkerEnrollmentError as error:
        operation = "reconciliation" if arguments.command == "reconcile" else "registration"
        print(f"Worker {operation} failed: {error}", file=error_output)
        if arguments.verbose:
            _print_diagnostic(error, error_output)
        return 1
    except Exception as error:
        operation = "reconciliation" if arguments.command == "reconcile" else "registration"
        print(f"Worker {operation} failed.", file=error_output)
        if arguments.verbose:
            _print_diagnostic(error, error_output)
        return 1

    print(result.display(), file=output)
    for error in cleanup_errors:
        print(f"Worker registration completed, but local cleanup failed: {type(error).__name__}.", file=error_output)
    return 0


def _reconcile_with_certificate_maintenance(
    *,
    identity_path: Path,
    identity: WorkerIdentity,
    mt5: MT5Client,
    key_store: HardwareKeyStore,
    key_store_factory: Callable[[str], HardwareKeyStore],
    transport_factory: Callable[[], EnrollmentTransport],
    error_output: TextIO,
    pair_cell_config_path: Path | None = None,
    pair_cell_options: PairCellStartupOptions = PairCellStartupOptions(),
) -> None:
    current_identity = identity
    current_key = key_store
    try:
        while True:
            def maintain(session: object) -> None:
                worker_id = getattr(session, "worker_id", None)
                certificate = getattr(session, "certificate", None)
                if not isinstance(worker_id, str) or not worker_id or not isinstance(certificate, str) or not certificate:
                    return
                transport: EnrollmentTransport | None = None
                try:
                    transport = transport_factory()
                    maintain_worker_certificate(
                        identity_path=identity_path,
                        identity=current_identity,
                        worker_id=worker_id,
                        certificate=certificate,
                        current_key=current_key,
                        key_store_factory=key_store_factory,
                        transport=transport,  # type: ignore[arg-type]
                        now=lambda: datetime.now(UTC),
                        error_output=error_output,
                    )
                except WorkerCertificateExpired:
                    raise
                except WorkerCertificateRotated:
                    raise
                except WorkerRotationError as error:
                    print(f"Worker certificate rotation failed; continuing with the current identity: {error}", file=error_output)
                finally:
                    if transport is not None:
                        _close(transport)

            try:
                effect_journal = WorkerEffectJournal(identity_path.with_suffix(".effects.sqlite"))
                pair_cell_db_path = identity_path.with_suffix(".paircell.sqlite")
                pair_cell_config_path = pair_cell_config_path or identity_path.with_suffix(".paircell.json")
                reconnect_worker_session(
                    open_session=lambda: open_authenticated_worker_session(
                        controller_url=current_identity.controller_url,
                        enrollment_id=current_identity.enrollment_id,
                        key_store=current_key,
                        certificate_received=lambda certificate: ensure_worker_certificate_current(
                            certificate, now=lambda: datetime.now(UTC)
                        ),
                    ),
                    mt5=mt5,
                    login=current_identity.login,
                    server=current_identity.server,
                    maintenance=maintain,
                    effect_journal=effect_journal,
                    pair_cell_factory=lambda mt5_client, session, journal, login, server: PairCellRuntime(
                        worker_id=getattr(session, "worker_id", ""),
                        db_path=pair_cell_db_path,
                        session=session,  # type: ignore[arg-type]
                        mt5=mt5_client,
                        effect_journal=journal if journal is not None else WorkerEffectJournal(pair_cell_db_path.with_suffix(".fallback-effects.sqlite")),
                        recovery_epoch=getattr(session, "recovery_epoch", ""),
                        login=login,
                        server=server,
                        config_path=pair_cell_config_path,
                        options=pair_cell_options,
                    ),
                )
                return
            except WorkerCertificateRotated:
                _close(current_key)
                current_identity = load_identity(identity_path)
                current_key = key_store_factory(current_identity.key_name)
            finally:
                if "effect_journal" in locals():
                    effect_journal.close()
    finally:
        _close(current_key)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt-worker")
    parser.add_argument("-v", "--verbose", action="store_true", help="show safe failure diagnostics")
    commands = parser.add_subparsers(dest="command")
    enroll = commands.add_parser("enroll", help="enroll this Windows MT5 worker")
    enroll.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics")
    enroll.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    enroll.add_argument(
        "--replace-config", action="store_true", help="replace an existing worker identity configuration"
    )
    enroll.add_argument("--controller-url", help="HTTPS controller origin")
    enroll.add_argument("--login", type=_positive_login, help="MT5 account login")
    enroll.add_argument("--server", help="MT5 server")
    enroll.add_argument("--registration-invite", help="one-time worker enrollment invite")
    enroll.add_argument(
        "--key-name",
        help="persistent Windows CNG key name",
    )
    reconcile = commands.add_parser("reconcile", help="run read-only MT5 reconciliation for an approved worker")
    reconcile.add_argument(
        "-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics"
    )
    reconcile.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    reconcile.add_argument(
        "--pair-cell-config",
        type=Path,
        default=None,
        help=(
            "optional Pair Execution Cell tunables file (risk tunables, allow_live, and -- for a"
            " leader -- shared policy); absent means the documented defaults are synthesized"
        ),
    )
    reconcile.add_argument(
        "--pair-cell-role",
        choices=("leader", "follower"),
        default=None,
        help=(
            "desired Pair Execution Cell role on this Worker's first unpaired connection;"
            " omitted means available follower. A durable route always wins over this."
        ),
    )
    reconcile.add_argument(
        "--follower-worker-id",
        default=None,
        help=(
            "leader only: select the follower non-interactively for unattended deployment"
            " instead of choosing from the controller's available-follower list"
        ),
    )
    reconcile.add_argument(
        "--pair-cell-unpair",
        action="store_true",
        help="request a safe unpair of this Worker's current Pair Execution Cell route",
    )
    reconcile.add_argument(
        "--pair-cell-cancel-unpair",
        action="store_true",
        help="cancel an in-progress safe unpair and return this route to normal operation",
    )
    reconcile.add_argument(
        "--pair-cell-rediscover",
        action="store_true",
        help="request an explicit rediscovery on this Worker's current route",
    )
    return parser


def _interactive(override: bool | None) -> bool:
    if override is not None:
        return override
    stream = getattr(sys, "stdin", None)
    isatty = getattr(stream, "isatty", None)
    if not callable(isatty):
        return False
    try:
        return bool(isatty())
    except ValueError:  # pragma: no cover - a closed stream is not interactive
        return False


def _pair_cell_startup_options(
    arguments: argparse.Namespace,
    *,
    interactive: bool,
    input_prompt: Callable[[str], str],
    output: TextIO,
) -> PairCellStartupOptions:
    """Translate the Pair Execution Cell command line into runtime options.

    A leader without ``--follower-worker-id`` gets an interactive selector.
    On a non-TTY process it deliberately gets none: the runtime then stays
    authenticated and unpaired with an actionable diagnostic rather than
    blocking on input, selecting implicitly, or exiting.
    """

    role = getattr(arguments, "pair_cell_role", None)
    follower_worker_id = getattr(arguments, "follower_worker_id", None)
    if follower_worker_id is not None and role != "leader":
        raise WorkerEnrollmentError(
            "--follower-worker-id is leader only; pass --pair-cell-role leader to use it."
        )
    if getattr(arguments, "pair_cell_unpair", False) and getattr(
        arguments, "pair_cell_cancel_unpair", False
    ):
        raise WorkerEnrollmentError(
            "--pair-cell-unpair and --pair-cell-cancel-unpair cannot be requested together."
        )
    selector = None
    if role == "leader" and follower_worker_id is None and interactive:
        selector = lambda followers: _select_follower(  # noqa: E731 - a tiny bound closure
            followers, input_prompt=input_prompt, output=output
        )
    return PairCellStartupOptions(
        role=role,
        follower_worker_id=follower_worker_id,
        interactive=interactive,
        select_follower=selector,
        unpair=bool(getattr(arguments, "pair_cell_unpair", False)),
        cancel_unpair=bool(getattr(arguments, "pair_cell_cancel_unpair", False)),
        rediscover=bool(getattr(arguments, "pair_cell_rediscover", False)),
    )


def _select_follower(
    followers: Sequence[Mapping[str, object]],
    *,
    input_prompt: Callable[[str], str],
    output: TextIO,
) -> str | None:
    """Show the controller's available-follower list and pick exactly one."""

    print("Available followers:", file=output)
    for index, follower in enumerate(followers, start=1):
        print(
            f"  {index}. {follower.get('worker_id')}"
            f" (login {follower.get('login')} on {follower.get('server')})",
            file=output,
        )
    try:
        answer = input_prompt("Select a follower by number (or press Enter to skip): ").strip()
    except (EOFError, KeyboardInterrupt):
        return None
    if not answer:
        return None
    try:
        choice = int(answer)
    except ValueError:
        return next(
            (
                str(follower["worker_id"])
                for follower in followers
                if str(follower.get("worker_id")) == answer
            ),
            None,
        )
    if 1 <= choice <= len(followers):
        return str(followers[choice - 1]["worker_id"])
    return None


def _positive_login(value: str) -> int:
    try:
        login = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError("must be a positive integer") from error
    if login <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return login


def _required_prompted(value: str | None, prompt: str, input_prompt: Callable[[str], str]) -> str:
    if value is None:
        value = input_prompt(prompt)
    if not isinstance(value, str) or not value.strip():
        raise WorkerEnrollmentError(f"{prompt.rstrip(': ')} is required.")
    return value.strip()


def _prompted_login(value: int | None, input_prompt: Callable[[str], str]) -> int:
    if value is not None:
        return value
    prompted = _required_prompted(None, "MT5 account login: ", input_prompt)
    try:
        return _positive_login(prompted)
    except argparse.ArgumentTypeError as error:
        raise WorkerEnrollmentError("MT5 account login must be a positive integer.") from error


def _required_enrollment_status(response: Mapping[str, object]) -> str:
    status = response.get("status")
    if not isinstance(status, str) or status not in {"pending", "approved"}:
        raise WorkerEnrollmentError("The controller returned an invalid enrollment status.")
    return status


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


def _close_safely(value: object, errors: list[Exception]) -> None:
    try:
        _close(value)
    except Exception as error:
        errors.append(error)


def _print_diagnostic(error: Exception, output: TextIO) -> None:
    if isinstance(error, WorkerEnrollmentError):
        print(f"Diagnostic: {error}", file=output)
        cause = error.__cause__
        if not isinstance(cause, Exception):
            return
        error = cause
    received_close = getattr(error, "rcvd", None)
    sent_close = getattr(error, "sent", None)
    received_code = getattr(received_close, "code", None)
    sent_code = getattr(sent_close, "code", None)
    if isinstance(received_code, int):
        print(f"Diagnostic: WebSocket closed by controller with code {received_code}.", file=output)
        return
    if isinstance(sent_code, int):
        print(f"Diagnostic: WebSocket closed locally with code {sent_code}.", file=output)
        return
    print(f"Diagnostic: {type(error).__name__}", file=output)
