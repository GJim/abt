from __future__ import annotations

import argparse
import json
import sys
import time
from base64 import b64encode
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Protocol, TextIO

import httpx
from websockets.exceptions import ConnectionClosed

from abt.controlplane.crypto import trader_proof_payload
from abt.worker.keystore import HardwareKeyStore, WindowsCNGKeyStore

from .identity import (
    TraderEnrollmentError,
    TraderIdentity,
    atomic_write_json,
    default_identity_path,
    ensure_identity_can_be_saved,
    load_identity,
    pending_identity_path,
    save_identity,
    state_path,
)
from .session import open_authenticated_trader_session, run_trader_session


class TraderTransport(Protocol):
    def enroll(self, controller_url: str, request: dict[str, object]) -> Mapping[str, object]: ...
    def enrollment_status(self, controller_url: str, enrollment_id: str) -> Mapping[str, object]: ...
    def certificate_challenge(self, controller_url: str, enrollment_id: str) -> Mapping[str, object]: ...
    def certificate(self, controller_url: str, enrollment_id: str, signature: str) -> Mapping[str, object]: ...


class PublicIpDiscovery(Protocol):
    def public_ip(self) -> str: ...


class HTTPTraderTransport:
    """Use only the Trader's HTTPS enrollment and certificate endpoints."""

    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=15.0, follow_redirects=False)
        self._owns_client = client is None

    def enroll(self, controller_url: str, request: dict[str, object]) -> Mapping[str, object]:
        return self._request("POST", controller_url, "/api/traders/enrollments", request, expected=201)

    def enrollment_status(self, controller_url: str, enrollment_id: str) -> Mapping[str, object]:
        return self._request("GET", controller_url, f"/api/traders/enrollments/{enrollment_id}/status", None, expected=200)

    def certificate_challenge(self, controller_url: str, enrollment_id: str) -> Mapping[str, object]:
        return self._request(
            "POST", controller_url, "/api/traders/certificates/challenge", {"registration_id": enrollment_id}, expected=200
        )

    def certificate(self, controller_url: str, enrollment_id: str, signature: str) -> Mapping[str, object]:
        return self._request(
            "POST", controller_url, "/api/traders/certificates",
            {"registration_id": enrollment_id, "signature": signature}, expected=200,
        )

    def _request(
        self, method: str, controller_url: str, path: str, body: dict[str, object] | None, *, expected: int
    ) -> Mapping[str, object]:
        try:
            response = self._client.request(method, _controller_endpoint(controller_url, path), json=body)
            if response.status_code != expected:
                raise TraderEnrollmentError(f"The controller request returned HTTP {response.status_code}.")
            payload = response.json()
        except TraderEnrollmentError:
            raise
        except (httpx.HTTPError, ValueError) as error:
            raise TraderEnrollmentError("The controller request failed.") from error
        if not isinstance(payload, Mapping):
            raise TraderEnrollmentError("The controller returned an invalid Trader response.")
        return payload

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


class IpifyDiscovery:
    def __init__(self, client: httpx.Client | None = None) -> None:
        self._client = client or httpx.Client(timeout=10.0, follow_redirects=False)
        self._owns_client = client is None

    def public_ip(self) -> str:
        try:
            response = self._client.get("https://api.ipify.org")
            response.raise_for_status()
            value = response.text.strip()
        except httpx.HTTPError as error:
            raise TraderEnrollmentError("Public-IP discovery failed.") from error
        return _valid_public_ip(value)

    def close(self) -> None:
        if self._owns_client:
            self._client.close()


def main(
    argv: list[str] | None = None,
    *,
    transport_factory: Callable[[], TraderTransport] = HTTPTraderTransport,
    key_store_factory: Callable[[str], HardwareKeyStore] = WindowsCNGKeyStore,
    public_ip_discovery: PublicIpDiscovery | Callable[[], str] = IpifyDiscovery(),
    input_prompt: Callable[[str], str] = input,
    output: TextIO | None = None,
    error_output: TextIO | None = None,
) -> int:
    """Run the native Windows Trader enrollment and event connection command."""

    output = output or sys.stdout
    error_output = error_output or sys.stderr
    if sys.platform != "win32":
        print("abt-trader is only supported on native Windows.", file=error_output)
        return 1
    arguments = _parser().parse_args(argv)
    try:
        if arguments.command == "enroll":
            existing = load_identity(arguments.config) if arguments.config.exists() else None
            ensure_identity_can_be_saved(arguments.config, replace=arguments.replace_config)
            controller_url = _required(arguments.controller_url, "Controller URL: ", input_prompt)
            invite = _required(arguments.registration_invite, "Registration invite: ", input_prompt)
            strategy_name = _required(arguments.strategy_name, "Strategy name: ", input_prompt)
            identity = TraderIdentity(
                controller_url=controller_url,
                enrollment_id="",
                strategy_name=strategy_name,
                public_ip=_public_ip(arguments.public_ip, public_ip_discovery, input_prompt),
                key_name=arguments.key_name or (existing.key_name if existing is not None else "abt-trader-device-key"),
            )
            transport = transport_factory()
            key_store = key_store_factory(identity.key_name)
            try:
                signature = key_store.sign(_enrollment_payload(invite, identity.strategy_name, identity.public_ip))
                if not isinstance(signature, bytes):
                    raise TraderEnrollmentError("The Windows CNG key returned an invalid signature.")
                response = transport.enroll(
                    identity.controller_url,
                    {
                        "registration_invite": invite,
                        "strategy_name": identity.strategy_name,
                        "claimed_public_ip": identity.public_ip,
                        "public_key_pem": key_store.public_key_pem(),
                        "proof_signature": b64encode(signature).decode("ascii"),
                    },
                )
                enrollment_id = _required_response(response, "registration_id")
                saved = TraderIdentity(
                    controller_url=identity.controller_url, enrollment_id=enrollment_id, strategy_name=identity.strategy_name,
                    public_ip=identity.public_ip, key_name=identity.key_name,
                )
                save_identity(
                    pending_identity_path(arguments.config) if existing is not None else arguments.config,
                    saved, replace=existing is not None or arguments.replace_config,
                )
            finally:
                _close(key_store)
                _close(transport)
            print(json.dumps({"registration_id": enrollment_id, "status": "pending_approval"}, sort_keys=True), file=output)
            return 0

        identity = load_identity(arguments.config)
        transport = transport_factory()
        try:
            pending = pending_identity_path(arguments.config)
            if pending.exists():
                candidate = load_identity(pending)
                pending_status = _required_response(
                    transport.enrollment_status(candidate.controller_url, candidate.enrollment_id), "status"
                )
                if pending_status == "approved":
                    save_identity(arguments.config, candidate, replace=True)
                    pending.unlink()
                    identity = candidate
                else:
                    print(
                        "Trader replacement enrollment is pending or inactive; continuing with the saved identity.",
                        file=error_output,
                    )
            status = _required_response(transport.enrollment_status(identity.controller_url, identity.enrollment_id), "status")
            if status == "pending":
                print("Trader enrollment is pending administrator approval.", file=error_output)
                return 0
            if status != "approved":
                raise TraderEnrollmentError("Trader enrollment is no longer active.")
            connect_trader(
                identity,
                transport=transport,
                key_store_factory=key_store_factory,
                output=output,
                error_output=error_output,
                state_file=state_path(arguments.config),
            )
            return 0
        finally:
            _close(transport)
    except KeyboardInterrupt:
        print("Trader connection stopped.", file=error_output)
        return 130
    except TraderEnrollmentError as error:
        operation = "connection" if arguments.command == "connect" else "enrollment"
        print(f"Trader {operation} failed: {error}", file=error_output)
        return 1


def connect_trader(
    identity: TraderIdentity, *, transport: TraderTransport, key_store_factory: Callable[[str], HardwareKeyStore],
    output: TextIO, error_output: TextIO, state_file: Path,
) -> None:
    """Retrieve a certificate per connection and safely reconnect after interruption."""

    key_store = key_store_factory(identity.key_name)
    try:
        _warn_if_state_unavailable(state_file, error_output)
        while True:
            try:
                challenge = transport.certificate_challenge(identity.controller_url, identity.enrollment_id)
                trader_id = _required_response(challenge, "trader_id")
                nonce = _required_response(challenge, "nonce")
                signature = key_store.sign(
                    trader_proof_payload(purpose="certificate_delivery", trader_id=trader_id, nonce=nonce)
                )
                if not isinstance(signature, bytes):
                    raise TraderEnrollmentError("The Windows CNG key returned an invalid signature.")
                delivery = transport.certificate(
                    identity.controller_url, identity.enrollment_id, b64encode(signature).decode("ascii")
                )
                if _required_response(delivery, "trader_id") != trader_id:
                    raise TraderEnrollmentError("The controller returned an invalid Trader certificate.")
                socket, _ = open_authenticated_trader_session(
                    controller_url=identity.controller_url, enrollment_id=identity.enrollment_id, key_store=key_store,
                    certificate=(trader_id, _required_response(delivery, "certificate")),
                )
                try:
                    run_trader_session(socket, output=output, save_cursor=lambda cursor: _save_cursor(state_file, cursor))
                finally:
                    socket.__exit__(None, None, None)
            except (ConnectionClosed, OSError, TimeoutError) as error:
                print(f"Trader connection interrupted ({type(error).__name__}); reconnecting.", file=error_output)
                time.sleep(5)
    finally:
        _close(key_store)


def _save_cursor(path: Path, cursor: int) -> None:
    atomic_write_json(path, {"version": 1, "acknowledged_cursor": cursor}, "Trader state")


def _warn_if_state_unavailable(path: Path, error_output: TextIO) -> None:
    if not path.exists():
        print("Trader cursor state is unavailable; the controller will replay from its acknowledged cursor.", file=error_output)
        return
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if (
            not isinstance(value, dict)
            or set(value) != {"version", "acknowledged_cursor"}
            or value.get("version") != 1
            or not isinstance(value.get("acknowledged_cursor"), int)
        ):
            raise ValueError
    except (OSError, ValueError, json.JSONDecodeError):
        print("Trader cursor state is invalid; the controller will replay from its acknowledged cursor.", file=error_output)


def _public_ip(
    value: str | None, discovery: PublicIpDiscovery | Callable[[], str], prompt: Callable[[str], str]
) -> str:
    if value is not None:
        return _valid_public_ip(value)
    try:
        candidate = discovery() if callable(discovery) else discovery.public_ip()
        candidate = _valid_public_ip(candidate)
    except Exception:
        return _valid_public_ip(prompt("Public IP: "))
    confirmation = prompt(f"Use detected public IP {candidate}? [yes/no]: ").strip().lower()
    return candidate if confirmation in {"y", "yes"} else _valid_public_ip(prompt("Public IP: "))


def _valid_public_ip(value: object) -> str:
    import ipaddress
    if not isinstance(value, str):
        raise TraderEnrollmentError("A valid public IP address is required.")
    try:
        return str(ipaddress.ip_address(value.strip()))
    except ValueError as error:
        raise TraderEnrollmentError("A valid public IP address is required.") from error


def _required(value: str | None, prompt: str, input_prompt: Callable[[str], str]) -> str:
    value = input_prompt(prompt) if value is None else value
    if not isinstance(value, str) or not value.strip():
        raise TraderEnrollmentError(f"{prompt.rstrip(': ')} is required.")
    return value.strip()


def _required_response(response: Mapping[str, object], field: str) -> str:
    value = response.get(field)
    if not isinstance(value, str) or not value:
        raise TraderEnrollmentError("The controller returned an invalid Trader response.")
    return value


def _enrollment_payload(invite: str, strategy_name: str, public_ip: str) -> bytes:
    return json.dumps(
        {"claimed_public_ip": public_ip, "registration_invite": invite, "strategy_name": strategy_name},
        separators=(",", ":"), sort_keys=True,
    ).encode("utf-8")


def _controller_endpoint(controller_url: str, path: str) -> str:
    url = httpx.URL(controller_url)
    if url.scheme != "https" or not url.host or url.username or url.password or url.path not in ("", "/") or url.query or url.fragment:
        raise TraderEnrollmentError("The controller URL must be an HTTPS origin.")
    return str(url.copy_with(path=path))


def _close(value: object) -> None:
    close = getattr(value, "close", None)
    if callable(close):
        close()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="abt-trader")
    commands = parser.add_subparsers(dest="command", required=True)
    enroll = commands.add_parser("enroll", help="enroll this Windows Trader")
    enroll.add_argument("--config", type=Path, default=default_identity_path())
    enroll.add_argument("--replace-config", action="store_true")
    enroll.add_argument("--controller-url")
    enroll.add_argument("--registration-invite")
    enroll.add_argument("--strategy-name")
    enroll.add_argument("--public-ip")
    enroll.add_argument("--key-name")
    connect = commands.add_parser("connect", help="connect the approved Trader event stream")
    connect.add_argument("--config", type=Path, default=default_identity_path())
    return parser
