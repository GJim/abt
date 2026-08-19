from __future__ import annotations

import json
from base64 import b64encode
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Protocol

from abt.controlplane.crypto import enrollment_payload

from .keystore import HardwareKeyStore


class WorkerEnrollmentError(RuntimeError):
    """Raised when local worker enrollment cannot safely complete."""


class WorkerSessionDisconnected(WorkerEnrollmentError):
    """Raised when an established controller WebSocket disconnects."""


class MT5Client(Protocol):
    """The read-only MT5 calls needed to prove a local account login."""

    def initialize(self) -> bool: ...

    def login(self, login: int, *, password: str, server: str) -> bool: ...

    def account_info(self) -> object: ...

    def terminal_info(self) -> object: ...

    def shutdown(self) -> None: ...


class EnrollmentTransport(Protocol):
    """A TLS transport for submitting a signed enrollment request."""

    def enrollment_challenge(self, controller_url: str) -> Mapping[str, object]: ...

    def enroll(self, controller_url: str, request: dict[str, object]) -> Mapping[str, object]: ...


@dataclass(frozen=True)
class EnrollmentResult:
    registration_id: str
    account_info: dict[str, object]
    terminal_info: dict[str, object]
    expires_at: str

    def display(self) -> str:
        """Return the complete successful-registration console output."""

        return json.dumps(
            {
                "account_info": self.account_info,
                "expires_at": self.expires_at,
                "registration_id": self.registration_id,
                "terminal_info": self.terminal_info,
            },
            indent=2,
            sort_keys=True,
        )



def register_worker(
    *,
    controller_url: str,
    login: int,
    server: str,
    registration_invite: str,
    key_store: HardwareKeyStore,
    mt5: MT5Client,
    transport: EnrollmentTransport,
    password_prompt: Callable[[str], str],
) -> EnrollmentResult:
    """Log in locally, prove that evidence, and submit a signed enrollment."""

    password = password_prompt("MT5 password: ")
    if not isinstance(password, str) or not password:
        raise WorkerEnrollmentError("An MT5 password is required.")
    if not registration_invite:
        raise WorkerEnrollmentError("A registration invite is required.")

    initialized = False
    request: dict[str, object] | None = None
    try:
        initialized = bool(mt5.initialize())
        if not initialized:
            raise WorkerEnrollmentError("Unable to initialize the local MT5 terminal.")
        if not mt5.login(login, password=password, server=server):
            raise WorkerEnrollmentError("The local MT5 login was not accepted.")

        account_info = _evidence(mt5.account_info(), "account")
        terminal_info = _evidence(mt5.terminal_info(), "terminal")
        _verify_account_evidence(account_info, login, server)

        challenge = _required_response_text(transport.enrollment_challenge(controller_url), "challenge")

        signed_payload = enrollment_payload(
            login,
            server,
            account_info,
            terminal_info,
            password,
            challenge,
        )
        signature = key_store.sign(signed_payload)
        if not isinstance(signature, bytes):
            raise WorkerEnrollmentError("The Windows CNG key returned an invalid signature.")
        public_key_pem = key_store.public_key_pem()
        if not isinstance(public_key_pem, str) or not public_key_pem:
            raise WorkerEnrollmentError("The Windows CNG key returned an invalid public key.")

        request = {
            "registration_invite": registration_invite,
            "login": login,
            "server": server,
            "account_info": account_info,
            "terminal_info": terminal_info,
            "mt5_password": password,
            "enrollment_challenge": challenge,
            "public_key_pem": public_key_pem,
            "proof_signature": b64encode(signature).decode("ascii"),
        }
        response = transport.enroll(controller_url, request)
        return EnrollmentResult(
            registration_id=_required_response_text(response, "enrollment_id"),
            account_info=account_info,
            terminal_info=terminal_info,
            expires_at=_required_response_text(response, "expires_at"),
        )
    finally:
        if request is not None:
            request.pop("mt5_password", None)
        password = ""
        if initialized:
            mt5.shutdown()


def _evidence(value: object, kind: str) -> dict[str, object]:
    if value is None:
        raise WorkerEnrollmentError(f"The local MT5 terminal did not return {kind} evidence.")
    if isinstance(value, Mapping):
        evidence = dict(value)
    else:
        as_dict = getattr(value, "_asdict", None)
        if not callable(as_dict):
            raise WorkerEnrollmentError(f"The local MT5 {kind} evidence is invalid.")
        evidence = dict(as_dict())
    if not evidence:
        raise WorkerEnrollmentError(f"The local MT5 terminal returned empty {kind} evidence.")
    try:
        json.dumps(evidence, allow_nan=False)
    except (TypeError, ValueError) as error:
        raise WorkerEnrollmentError(f"The local MT5 {kind} evidence is not JSON-safe.") from error
    return evidence


def _verify_account_evidence(account_info: dict[str, object], login: int, server: str) -> None:
    evidence_login = account_info.get("login")
    if isinstance(evidence_login, bool) or not isinstance(evidence_login, int) or evidence_login != login:
        raise WorkerEnrollmentError("The local MT5 account evidence does not match the requested login.")
    evidence_server = account_info.get("server")
    if not isinstance(evidence_server, str) or evidence_server != server:
        raise WorkerEnrollmentError("The local MT5 account evidence does not match the requested server.")


def _required_response_text(response: Mapping[str, object], field: str) -> str:
    value = response.get(field)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The controller returned an invalid enrollment response.")
    return value
