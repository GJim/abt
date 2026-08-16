from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Self

from .credentials import (
    WebSocketConnector,
    WorkerWebSocket,
    _message,
    _required_text,
    _send,
    _send_proof,
    _worker_endpoint,
)
from .enrollment import WorkerEnrollmentError
from .keystore import HardwareKeyStore


@dataclass
class AuthenticatedWorkerSession:
    """A proved WSS channel for one approved 帳戶工作者."""

    socket: WorkerWebSocket

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_value: object, traceback: object) -> None:
        self.socket.__exit__(exc_type, exc_value, traceback)

    def request_password(self) -> str:
        _send(self.socket, {"type": "password_request"})
        response = _message(self.socket)
        if response.get("type") != "password":
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
        return _required_text(response, "password")

    def send_reconciliation(self, message: dict[str, object]) -> None:
        _send(self.socket, message)
        response = _message(self.socket)
        if response.get("type") != "accepted" or response.get("cursor") != message.get("cursor"):
            raise WorkerEnrollmentError("The controller rejected worker reconciliation.")


def open_authenticated_worker_session(
    *,
    controller_url: str,
    enrollment_id: str,
    key_store: HardwareKeyStore,
    connect: WebSocketConnector | None = None,
) -> AuthenticatedWorkerSession:
    """Deliver the approved certificate, then prove the device key on one persistent WSS channel."""

    if connect is None:
        from websockets.sync.client import connect as websocket_connect

        connect = websocket_connect
    with connect(_worker_endpoint(controller_url, "/api/worker/certificate")) as certificate_socket:
        _send(certificate_socket, {"enrollment_id": enrollment_id})
        challenge = _message(certificate_socket)
        worker_id = _required_text(challenge, "worker_id")
        _send_proof(certificate_socket, key_store, challenge, "certificate_delivery", worker_id)
        delivery = _message(certificate_socket)
        if _required_text(delivery, "worker_id") != worker_id:
            raise WorkerEnrollmentError("The controller returned an invalid device certificate.")
        certificate = _required_text(delivery, "certificate")

    socket = connect(_worker_endpoint(controller_url, "/api/worker/session"))
    try:
        socket.__enter__()
        _send(socket, {"worker_id": worker_id, "certificate": certificate})
        challenge = _message(socket)
        _send_proof(socket, key_store, challenge, "worker_session", worker_id)
        authenticated = _message(socket)
        if authenticated != {"type": "authenticated", "worker_id": worker_id}:
            raise WorkerEnrollmentError("The controller returned an invalid worker response.")
    except BaseException as error:
        socket.__exit__(type(error), error, error.__traceback__)
        raise
    return AuthenticatedWorkerSession(socket)
