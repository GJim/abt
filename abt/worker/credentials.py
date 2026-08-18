from __future__ import annotations

import json
from base64 import b64encode
from collections.abc import Callable
from typing import Protocol
from urllib.parse import urlsplit, urlunsplit

from abt.controlplane.crypto import worker_proof_payload

from .enrollment import WorkerEnrollmentError
from .keystore import HardwareKeyStore


class WorkerWebSocket(Protocol):
    def send(self, message: str) -> None: ...

    def recv(self) -> str: ...

    def __enter__(self) -> WorkerWebSocket: ...

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None: ...


WebSocketConnector = Callable[[str], WorkerWebSocket]


def retrieve_mt5_password(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    connect: WebSocketConnector | None = None,
) -> str:
    """Retrieve an approved worker's password without persisting or displaying it."""

    if connect is None:
        from websockets.sync.client import connect as websocket_connect

        connect = websocket_connect
    certificate_url = _worker_endpoint(controller_url, "/api/worker/certificate")
    with connect(certificate_url) as socket:
        _send(socket, {"enrollment_id": enrollment_id})
        challenge = _message(socket)
        worker_id = _required_text(challenge, "worker_id")
        _send_proof(socket, key_store, challenge, "certificate_delivery", worker_id)
        delivery = _message(socket)
        if _required_text(delivery, "worker_id") != worker_id:
            raise WorkerEnrollmentError("The controller returned an invalid device certificate.")
        certificate = _required_text(delivery, "certificate")

    credentials_url = _worker_endpoint(controller_url, "/api/worker/credentials")
    with connect(credentials_url) as socket:
        _send(socket, {"worker_id": worker_id, "certificate": certificate})
        challenge = _message(socket)
        _send_proof(socket, key_store, challenge, "password_request", worker_id)
        return _required_text(_message(socket), "password")


def _worker_endpoint(controller_url: str, endpoint: str) -> str:
    parsed = urlsplit(controller_url)
    if parsed.scheme != "https" or not parsed.netloc or parsed.query or parsed.fragment:
        raise WorkerEnrollmentError("The controller URL must be an HTTPS origin.")
    return urlunsplit(("wss", parsed.netloc, endpoint, "", ""))


def _send_proof(
    socket: WorkerWebSocket,
    key_store: HardwareKeyStore,
    challenge: dict[str, object],
    purpose: str,
    worker_id: str,
) -> None:
    if _required_text(challenge, "purpose") != purpose or _required_text(challenge, "worker_id") != worker_id:
        raise WorkerEnrollmentError("The controller returned an invalid worker challenge.")
    nonce = _required_text(challenge, "nonce")
    signature = key_store.sign(worker_proof_payload(purpose=purpose, worker_id=worker_id, nonce=nonce))
    if not isinstance(signature, bytes):
        raise WorkerEnrollmentError("The Windows CNG key returned an invalid signature.")
    _send(socket, {"signature": b64encode(signature).decode("ascii")})


def _send(socket: WorkerWebSocket, message: dict[str, object]) -> None:
    socket.send(json.dumps(message, separators=(",", ":"), sort_keys=True))


def _message(socket: WorkerWebSocket) -> dict[str, object]:
    try:
        message = json.loads(socket.recv())
    except (TypeError, ValueError) as error:
        raise WorkerEnrollmentError("The controller returned an invalid worker response.") from error
    if not isinstance(message, dict):
        raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    return message


def _required_text(message: dict[str, object], field: str) -> str:
    value = message.get(field)
    if not isinstance(value, str) or not value:
        raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    return value
