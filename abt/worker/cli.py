from __future__ import annotations

import argparse
import logging
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TextIO

import httpx

from .enrollment import EnrollmentTransport, MT5Client, WorkerEnrollmentError, register_worker
from .effect_journal import WorkerEffectJournal
from .symbols_inspect import (
    InspectError,
    allowed_products,
    edge_searchable_products,
    excluded_products,
    snapshot_symbols,
)
from .identity import (
    WorkerIdentity,
    default_identity_path,
    ensure_identity_can_be_saved,
    load_identity,
    pending_identity_path,
    save_identity,
)
from .keystore import (
    HardwareKeyStore,
    KeyStoreFactory,
    default_key_store_provider,
    enroll_key_store,
    open_key_store,
)
from .pair_cell_adapter import PairCellRuntime, PairCellStartupOptions, PairExecutionCellError
from .reconciliation import AccountMismatchError, reconnect_worker_session
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

    def history_deals_get(self, *args: object, **kwargs: object) -> object:
        history = getattr(self._mt5, "history_deals_get", None)
        if callable(history):
            return history(*args, **kwargs)
        return []

    def last_error(self) -> object:
        return self._mt5.last_error()

    def __getattr__(self, name: str) -> object:
        if name.startswith(("COPY_TICKS_", "TIMEFRAME_", "TRADE_", "ORDER_", "POSITION_")):
            return getattr(self._mt5, name)
        raise AttributeError(name)

    def shutdown(self) -> None:
        self._mt5.shutdown()

    def close(self) -> None:
        self.shutdown()


def main(
    argv: list[str] | None = None,
    *,
    mt5_factory: Callable[[], MT5Client] | None = None,
    transport_factory: Callable[[], EnrollmentTransport] = HTTPEnrollmentTransport,
    key_store_factory: KeyStoreFactory | None = None,
    password_prompt: Callable[[str], str] | None = None,
    input_prompt: Callable[[str], str] = input,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
    interactive: bool | None = None,
) -> int:
    """Run the native Worker registration command."""

    output = output or sys.stdout
    error_output = error_output or sys.stderr
    if sys.platform not in {"win32", "linux"}:
        print(f"abt-worker is unsupported on {sys.platform}.", file=error_output)
        return 1

    parser = _parser()
    arguments = parser.parse_args(argv)
    if arguments.command == "symbols":
        return _run_symbols(arguments, output=output, error_output=error_output)
    if arguments.command == "quarantine" and getattr(arguments, "quarantine_action", None) != "release":
        print("Worker quarantine inspection failed: unknown action.", file=error_output)
        return 1
    if arguments.command not in {"enroll", "reconcile", "unpair", "rediscover", "quarantine"}:
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
        provider = key_store_factory or default_key_store_provider(arguments.config)
        if arguments.command in {"reconcile", "unpair", "rediscover", "quarantine"}:
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
        if mt5_factory is not None:
            mt5 = mt5_factory()
        elif sys.platform == "linux":
            from .wine_mt5 import WineMetaTrader5Adapter

            mt5 = WineMetaTrader5Adapter(
                wine_prefix=getattr(arguments, "wine_prefix", Path.home() / ".mt5"),
                windows_python=getattr(arguments, "windows_python", r"C:\abt-python313\python.exe"),
                timeout_seconds=float(getattr(arguments, "bridge_timeout_seconds", 15.0)),
            )
        else:
            mt5 = MetaTrader5Adapter()
        key_store = (
            open_key_store(provider, identity.key_name)
            if arguments.command in {"reconcile", "unpair", "rediscover", "quarantine"}
            else enroll_key_store(provider, identity.key_name)
        )
        try:
            if arguments.command == "reconcile":
                _reconcile_with_certificate_maintenance(
                    identity_path=arguments.config,
                    identity=identity,
                    mt5=mt5,
                    key_store=key_store,
                    key_store_factory=provider,
                    transport_factory=transport_factory,
                    error_output=error_output,
                    pair_cell_config_path=getattr(arguments, "pair_cell_config", None),
                    pair_cell_options=pair_cell_options,
                )
                return 0
            if arguments.command == "unpair":
                _reconcile_with_certificate_maintenance(
                    identity_path=arguments.config,
                    identity=identity,
                    mt5=mt5,
                    key_store=key_store,
                    key_store_factory=provider,
                    transport_factory=transport_factory,
                    error_output=error_output,
                    pair_cell_config_path=getattr(arguments, "pair_cell_config", None),
                    pair_cell_options=pair_cell_options,
                    run_reconciliation=_one_shot_run(
                        done=_UnpairCompletion(),
                        timeout_seconds=float(arguments.timeout),
                        output=output,
                    ),
                )
                return 0
            if arguments.command == "rediscover":
                _reconcile_with_certificate_maintenance(
                    identity_path=arguments.config,
                    identity=identity,
                    mt5=mt5,
                    key_store=key_store,
                    key_store_factory=provider,
                    transport_factory=transport_factory,
                    error_output=error_output,
                    pair_cell_config_path=getattr(arguments, "pair_cell_config", None),
                    pair_cell_options=pair_cell_options,
                    run_reconciliation=_one_shot_run(
                        done=_RediscoverCompletion(reason=arguments.reason or ""),
                        timeout_seconds=float(arguments.timeout),
                        output=output,
                    ),
                )
                return 0
            if arguments.command == "quarantine":
                _reconcile_with_certificate_maintenance(
                    identity_path=arguments.config,
                    identity=identity,
                    mt5=mt5,
                    key_store=key_store,
                    key_store_factory=provider,
                    transport_factory=transport_factory,
                    error_output=error_output,
                    pair_cell_config_path=getattr(arguments, "pair_cell_config", None),
                    pair_cell_options=pair_cell_options,
                    run_reconciliation=_one_shot_run(
                        done=_QuarantineReleaseCompletion(
                            symbol=arguments.symbol, reason=arguments.reason or ""
                        ),
                        timeout_seconds=float(arguments.timeout),
                        output=output,
                    ),
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
            _close_safely(mt5, cleanup_errors)
            _close_safely(key_store, cleanup_errors)
    except KeyboardInterrupt:
        print("Worker reconciliation stopped.", file=error_output)
        return 130
    except WorkerEnrollmentError as error:
        operation = _operation_name(arguments.command)
        print(f"Worker {operation} failed: {error}", file=error_output)
        if arguments.verbose:
            _print_diagnostic(error, error_output)
        return 1
    except Exception as error:
        operation = _operation_name(arguments.command)
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
    key_store_factory: KeyStoreFactory,
    transport_factory: Callable[[], EnrollmentTransport],
    error_output: TextIO,
    pair_cell_config_path: Path | None = None,
    pair_cell_options: PairCellStartupOptions = PairCellStartupOptions(),
    run_reconciliation: Callable[..., None] | None = None,
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
                reconnect_kwargs: dict[str, object] = {
                    "open_session": lambda: open_authenticated_worker_session(
                        controller_url=current_identity.controller_url,
                        enrollment_id=current_identity.enrollment_id,
                        key_store=current_key,
                        certificate_received=lambda certificate: ensure_worker_certificate_current(
                            certificate, now=lambda: datetime.now(UTC)
                        ),
                    ),
                    "mt5": mt5,
                    "login": current_identity.login,
                    "server": current_identity.server,
                    "maintenance": maintain,
                    "effect_journal": effect_journal,
                    "pair_cell_factory": lambda mt5_client, session, journal, login, server: PairCellRuntime(
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
                }
                if run_reconciliation is not None:
                    reconnect_kwargs["run_reconciliation"] = run_reconciliation
                reconnect_worker_session(**reconnect_kwargs)  # type: ignore[arg-type]
                return
            except WorkerCertificateRotated:
                _close(current_key)
                current_identity = load_identity(identity_path)
                current_key = open_key_store(key_store_factory, current_identity.key_name)
            finally:
                if "effect_journal" in locals():
                    effect_journal.close()
    finally:
        _close(current_key)


def _operation_name(command: str | None) -> str:
    return {
        "enroll": "registration",
        "reconcile": "reconciliation",
        "unpair": "unpair request",
        "rediscover": "policy rediscovery",
        "quarantine": "quarantine release",
    }.get(command or "", "worker")


class _UnpairCompletion:
    """One-shot completion: send the unpair request once, then wait for removal."""

    def __init__(self) -> None:
        self._sent = False
        self._ever_routed = False
        self._route_id: str | None = None

    def __call__(self, runtime: PairCellRuntime, result: object) -> str | None:
        route_id = runtime.route_id
        if route_id is None:
            if self._ever_routed:
                return f"Route {self._route_id} was removed; the safe unpair is complete."
            return "This Worker is not on a Pair Execution Cell route; nothing to unpair."
        self._ever_routed = True
        self._route_id = route_id
        if not self._sent:
            if not runtime.request_safe_unpair():
                raise WorkerEnrollmentError("The Pair Execution Cell unpair command could not be sent.")
            self._sent = True
        return None


class _RediscoverCompletion:
    """One-shot completion: request one rediscovery, then wait for a new generation."""

    def __init__(self, *, reason: str = "") -> None:
        self._reason = reason
        self._requested = False
        self._generation: int | None = None

    def __call__(self, runtime: PairCellRuntime, result: object) -> str | None:
        if runtime.cell is None:
            if runtime.pairing_diagnostic:
                raise WorkerEnrollmentError(
                    f"Policy rediscovery cannot start: {runtime.pairing_diagnostic}"
                )
            return None
        generation = runtime.universe_generation()
        if not self._requested:
            if runtime.request_rediscovery(actor="cli", reason=self._reason) is None:
                raise WorkerEnrollmentError(
                    runtime.pairing_diagnostic or "Policy rediscovery cannot start on this route."
                )
            self._requested = True
            self._generation = generation
            return None
        failure = getattr(result, "rediscovery_failure", None)
        if isinstance(failure, str) and failure:
            raise WorkerEnrollmentError(f"Policy rediscovery failed: {failure}")
        if generation is not None and generation != self._generation:
            return (
                f"Rediscovery complete on route {runtime.route_id}:"
                f" universe generation {self._generation} -> {generation}."
            )
        return None


class _QuarantineReleaseCompletion:
    """One-shot completion: propose one symbol's release, then wait for the outcome."""

    def __init__(self, *, symbol: str, reason: str = "") -> None:
        self._symbol = symbol
        self._reason = reason
        self._proposal_id: str | None = None

    def __call__(self, runtime: PairCellRuntime, result: object) -> str | None:
        if runtime.cell is None:
            if runtime.pairing_diagnostic:
                raise WorkerEnrollmentError(
                    f"Quarantine release cannot start: {runtime.pairing_diagnostic}"
                )
            return None
        if self._proposal_id is None:
            try:
                proposal = runtime.request_quarantine_release(self._symbol, reason=self._reason)
            except PairExecutionCellError as error:
                raise WorkerEnrollmentError(f"Quarantine release is refused: {error}") from error
            if proposal is None:
                raise WorkerEnrollmentError(
                    runtime.pairing_diagnostic or "Quarantine release cannot start on this route."
                )
            proposal_id = proposal.get("proposal_id")
            if not isinstance(proposal_id, str) or not proposal_id:
                raise WorkerEnrollmentError("Quarantine release cannot start on this route.")
            self._proposal_id = proposal_id
            return None
        status = runtime.quarantine_release_status(self._proposal_id)
        if status is None:
            raise WorkerEnrollmentError("Quarantine release cannot start on this route.")
        state = status.get("state")
        if state == "pending":
            return None
        if state == "applied":
            applied = status.get("applied") or []
            peer_applied = status.get("peer_applied") or []
            return (
                f"Quarantine release applied for {self._symbol} on route {runtime.route_id}:"
                f" local={list(applied)} peer={list(peer_applied)}."
            )
        raise WorkerEnrollmentError(
            f"Quarantine release rejected: {status.get('reason') or 'the release did not take effect'}."
        )


def _one_shot_run(
    *,
    done: Callable[[PairCellRuntime, object], str | None],
    timeout_seconds: float,
    output: TextIO,
    poll_seconds: float = 0.5,
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Callable[..., None]:
    """Build a bounded ``run_reconciliation`` that performs one action and returns."""

    def run(
        *,
        mt5: MT5Client,
        session: object,
        login: int,
        server: str,
        sleep: Callable[[float], None],
        maintenance: Callable[[], None] | None,
        effect_journal: WorkerEffectJournal | None,
        pair_cell_factory: Callable[..., PairCellRuntime],
        graceful_shutdown: object = None,
    ) -> None:
        request_password = getattr(session, "request_password", None)
        initialize = getattr(mt5, "initialize", None)
        login_mt5 = getattr(mt5, "login", None)
        if not callable(request_password) or not callable(initialize) or not callable(login_mt5):
            raise WorkerEnrollmentError("The one-shot command requires an authenticated MT5 session.")
        if not initialize():
            raise WorkerEnrollmentError("Unable to initialize the local MT5 terminal.")
        if not login_mt5(login, password=request_password(), server=server):
            raise WorkerEnrollmentError("The local MT5 login was not accepted.")
        raw_account = mt5.account_info()
        if isinstance(raw_account, Mapping):
            account = dict(raw_account)
        else:
            as_dict = getattr(raw_account, "_asdict", None)
            account = dict(as_dict()) if callable(as_dict) else {}
        if account.get("login") != login or account.get("server") != server:
            raise AccountMismatchError("The local MT5 account does not match the approved worker binding.")
        runtime = pair_cell_factory(mt5, session, effect_journal, login, server)
        deadline = now() + timedelta(seconds=timeout_seconds)
        last_result: object = None
        try:
            while True:
                if maintenance is not None:
                    maintenance()
                last_result = runtime.pump(now())
                message = done(runtime, last_result)
                if message is not None:
                    print(message, file=output)
                    return
                if now() >= deadline:
                    raise WorkerEnrollmentError(
                        f"The command timed out after {timeout_seconds:g} seconds:"
                        f" {runtime.pairing_diagnostic or 'the route is not converging'}."
                    )
                sleep(poll_seconds)
        finally:
            runtime.close()

    return run


def _run_symbols(arguments: argparse.Namespace, *, output: TextIO, error_output: TextIO) -> int:
    """Inspect durable symbols state offline, without any session or broker."""

    view = getattr(arguments, "symbols_view", None)
    db_path = arguments.config.with_suffix(".paircell.sqlite")
    try:
        snapshot = snapshot_symbols(db_path)
    except InspectError as error:
        print(f"Worker symbols inspection failed: {error}", file=error_output)
        return 1
    if view == "allowed":
        products = allowed_products(snapshot)
        if not products:
            print("No allowed symbols: no frozen universe or everything is quarantined.", file=output)
            return 0
        generation = snapshot.universe.universe_generation if snapshot.universe else "?"
        print(f"Allowed symbols (universe generation {generation}, quarantine excluded):", file=output)
        for product in products:
            print(f"  {product.symbol} product_id={product.product_id}", file=output)
        return 0
    if view == "frozen":
        frozen = snapshot.frozen
        route = frozen.route if frozen is not None else {}
        budget = frozen.budget if frozen is not None else {}
        acceptance = frozen.acceptance if frozen is not None else {}
        policy = snapshot.policy
        universe = snapshot.universe
        print(f"Route: {route.get('route_id', '-')} role={route.get('role', '-')}"
              f" leader={route.get('leader_worker_id', '-')}"
              f" follower={route.get('follower_worker_id', '-')}"
              f" state={route.get('state', '-')}", file=output)
        print(f"Frozen budget: {budget.get('startup_balance_usd', '-')}"
              f" {budget.get('account_currency', '')}"
              f" proposal={budget.get('proposal_id', '-')}"
              f" role={budget.get('role', '-')}", file=output)
        print(f"Frozen acceptance: proposal={acceptance.get('proposal_id', '-')}"
              f" payload_hash={acceptance.get('payload_hash', '-')}", file=output)
        if policy is not None:
            print(f"Policy: hash={policy.policy_hash} mode={policy.mode}"
                  f" strategy_budget_usd={policy.strategy_budget_usd}"
                  f" leader_budget={policy.leader_budget_usd}"
                  f" follower_budget={policy.follower_budget_usd}", file=output)
        else:
            print("Policy: none accepted yet", file=output)
        if universe is not None:
            print(f"Universe: generation={universe.universe_generation}"
                  f" products={len(universe.products)} route={universe.route_id}", file=output)
        else:
            print("Universe: none discovered yet", file=output)
        return 0
    if view == "edge":
        plans = edge_searchable_products(snapshot)
        if not plans:
            print("No edge-searchable symbols in the last persisted state.", file=output)
            return 0
        print("Edge-searchable symbols (last persisted current plans, quarantine excluded):", file=output)
        for plan in plans:
            directions = ",".join(plan.directions)
            lots = ",".join(plan.local_max_lots)
            print(f"  {plan.symbol} product_id={plan.product_id}"
                  f" directions={directions} local_max_lots={lots}", file=output)
        return 0
    if view == "excluded":
        rows = excluded_products(snapshot)
        if not rows:
            print("No excluded symbols: every universe product has a current plan.", file=output)
            return 0
        print("Excluded symbols (last persisted state):", file=output)
        for row in rows:
            print(f"  {row['symbol']} product_id={row['product_id']}: {row['reason']}", file=output)
        return 0
    print(f"Worker symbols inspection failed: unknown view {view!r}.", file=error_output)
    return 1


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt-worker")
    parser.add_argument("-v", "--verbose", action="store_true", help="show safe failure diagnostics")
    commands = parser.add_subparsers(dest="command")
    enroll = commands.add_parser("enroll", help="enroll this MT5 worker")
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
        help="persistent device-key name",
    )
    enroll.add_argument("--wine-prefix", type=Path, default=Path.home() / ".mt5", help="Wine prefix directory")
    enroll.add_argument("--windows-python", default=r"C:\abt-python313\python.exe", help="Windows python path in Wine")
    enroll.add_argument("--bridge-timeout-seconds", type=float, default=15.0, help="Wine bridge request timeout")
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
            "optional Pair Execution Cell tunables file (risk tunables and -- for a"
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
    reconcile.add_argument("--wine-prefix", type=Path, default=Path.home() / ".mt5", help="Wine prefix directory")
    reconcile.add_argument("--windows-python", default=r"C:\abt-python313\python.exe", help="Windows python path in Wine")
    reconcile.add_argument("--bridge-timeout-seconds", type=float, default=15.0, help="Wine bridge request timeout")
    unpair = commands.add_parser(
        "unpair",
        help="request a safe unpair of this Worker's current Pair Execution Cell route and wait until it is removed",
    )
    unpair.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics")
    unpair.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    unpair.add_argument(
        "--pair-cell-config",
        type=Path,
        default=None,
        help="optional Pair Execution Cell tunables file; absent means the documented defaults are synthesized",
    )
    unpair.add_argument(
        "--timeout", type=float, default=300.0, help="seconds to wait for the route to be removed"
    )
    unpair.add_argument("--wine-prefix", type=Path, default=Path.home() / ".mt5", help="Wine prefix directory")
    unpair.add_argument("--windows-python", default=r"C:\abt-python313\python.exe", help="Windows python path in Wine")
    unpair.add_argument("--bridge-timeout-seconds", type=float, default=15.0, help="Wine bridge request timeout")
    rediscover = commands.add_parser(
        "rediscover",
        help="request an explicit product rediscovery on this Worker's current route and wait for a new generation",
    )
    rediscover.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics")
    rediscover.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    rediscover.add_argument(
        "--pair-cell-config",
        type=Path,
        default=None,
        help="optional Pair Execution Cell tunables file; absent means the documented defaults are synthesized",
    )
    rediscover.add_argument("--reason", default="", help="operator reason recorded with the rediscovery request")
    rediscover.add_argument(
        "--timeout", type=float, default=120.0, help="seconds to wait for the new universe generation"
    )
    rediscover.add_argument("--wine-prefix", type=Path, default=Path.home() / ".mt5", help="Wine prefix directory")
    rediscover.add_argument("--windows-python", default=r"C:\abt-python313\python.exe", help="Windows python path in Wine")
    rediscover.add_argument("--bridge-timeout-seconds", type=float, default=15.0, help="Wine bridge request timeout")
    quarantine = commands.add_parser(
        "quarantine",
        help="worker-owned product quarantine actions, coordinated peer-to-peer over the opaque relay",
    )
    quarantine.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics")
    quarantine.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    quarantine_actions = quarantine.add_subparsers(dest="quarantine_action")
    release = quarantine_actions.add_parser(
        "release",
        help="propose releasing one symbol's quarantine to the peer with one shared marker and wait for the outcome",
    )
    release.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    release.add_argument(
        "--pair-cell-config",
        type=Path,
        default=None,
        help="optional Pair Execution Cell tunables file; absent means the documented defaults are synthesized",
    )
    release.add_argument("--symbol", required=True, help="quarantined symbol to release (or a derived product identity)")
    release.add_argument("--reason", default="", help="operator reason recorded with the release proposal")
    release.add_argument(
        "--timeout", type=float, default=60.0, help="seconds to wait for the peer acknowledgement"
    )
    release.add_argument("--wine-prefix", type=Path, default=Path.home() / ".mt5", help="Wine prefix directory")
    release.add_argument("--windows-python", default=r"C:\abt-python313\python.exe", help="Windows python path in Wine")
    release.add_argument("--bridge-timeout-seconds", type=float, default=15.0, help="Wine bridge request timeout")
    symbols = commands.add_parser(
        "symbols",
        help="inspect durable Pair Execution Cell symbols state offline, without any session or broker",
    )
    symbols.add_argument("-v", "--verbose", action="store_true", default=argparse.SUPPRESS, help="show safe failure diagnostics")
    symbols.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
    symbols_views = symbols.add_subparsers(dest="symbols_view")
    for view, view_help in (
        ("allowed", "frozen universe products that are not quarantined"),
        ("frozen", "the frozen route, budget, acceptance, policy and universe records"),
        ("edge", "last persisted current sizing plans on non-quarantined products"),
        ("excluded", "quarantined products and universe products without a current plan"),
    ):
        view_parser = symbols_views.add_parser(view, help=view_help)
        view_parser.add_argument("--config", type=Path, default=default_identity_path(), help="worker identity configuration path")
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
