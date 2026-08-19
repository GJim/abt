from __future__ import annotations

from typing import Protocol
from datetime import UTC, datetime, timedelta
import base64
import json

import httpx

from .crypto import ProofError, device_certificate_payload, trader_certificate_payload

class SecretStoreError(RuntimeError):
    """Raised when the control plane cannot read or write an OpenBao secret."""


class SecretStore(Protocol):
    def write_password(self, reference: str, password: str) -> None: ...

    def read_password(self, reference: str) -> str: ...

    def delete_password(self, reference: str) -> None: ...


class DeviceCertificateIssuer(Protocol):
    def issue(self, *, worker_id: str, login: int, server: str, public_key_pem: str) -> str: ...

    def issue_trader(self, *, trader_id: str, strategy_name: str, public_key_pem: str) -> str: ...


class DeviceCertificateVerifier(Protocol):
    def verify(self, certificate: str) -> None: ...


class OpenBaoSecretStore:
    def __init__(self, address: str, token: str) -> None:
        self._address = address.rstrip("/")
        self._token = token

    def write_password(self, reference: str, password: str) -> None:
        self._request("POST", reference, json={"data": {"password": password}})

    def read_password(self, reference: str) -> str:
        response = self._request("GET", reference)
        try:
            return response.json()["data"]["data"]["password"]
        except (KeyError, TypeError, ValueError) as error:
            raise SecretStoreError("OpenBao password secret has an invalid response shape.") from error

    def delete_password(self, reference: str) -> None:
        self._request("DELETE", reference)

    def _request(self, method: str, reference: str, **kwargs: object) -> httpx.Response:
        try:
            response = httpx.request(
                method,
                f"{self._address}/v1/{reference.lstrip('/')}",
                headers={"X-Vault-Token": self._token},
                timeout=5.0,
                **kwargs,
            )
            response.raise_for_status()
            return response
        except httpx.HTTPError as error:
            raise SecretStoreError("OpenBao password operation failed.") from error


class OpenBaoDeviceCertificateIssuer:
    def __init__(self, address: str, token: str, transit_key: str = "abt-device-certificates") -> None:
        self._address = address.rstrip("/")
        self._token = token
        self._transit_key = transit_key

    def issue(self, *, worker_id: str, login: int, server: str, public_key_pem: str) -> str:
        issued_at = datetime.now(UTC)
        expires_at = issued_at + timedelta(days=30)
        payload = device_certificate_payload(
            worker_id=worker_id,
            login=login,
            server=server,
            public_key_pem=public_key_pem,
            issued_at=issued_at,
            expires_at=expires_at,
        )
        return self._sign(payload)

    def issue_trader(self, *, trader_id: str, strategy_name: str, public_key_pem: str) -> str:
        issued_at = datetime.now(UTC)
        return self._sign(
            trader_certificate_payload(
                trader_id=trader_id,
                strategy_name=strategy_name,
                public_key_pem=public_key_pem,
                issued_at=issued_at,
                expires_at=issued_at + timedelta(days=30),
            )
        )

    def _sign(self, payload: bytes) -> str:
        try:
            response = httpx.post(
                f"{self._address}/v1/transit/sign/{self._transit_key}",
                headers={"X-Vault-Token": self._token},
                json={"input": base64.b64encode(payload).decode("ascii")},
                timeout=5.0,
            )
            response.raise_for_status()
            signature = response.json()["data"]["signature"]
        except httpx.HTTPStatusError as error:
            raise SecretStoreError(f"OpenBao device certificate signing failed (HTTP {error.response.status_code}).") from error
        except httpx.TimeoutException as error:
            raise SecretStoreError("OpenBao device certificate signing timed out.") from error
        except (httpx.HTTPError, KeyError, TypeError, ValueError) as error:
            raise SecretStoreError("OpenBao device certificate signing transport failed.") from error
        return json.dumps(
            {
                "payload": base64.b64encode(payload).decode("ascii"),
                "signature": signature,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

    def verify(self, certificate: str) -> None:
        try:
            envelope = json.loads(certificate)
            payload = envelope["payload"]
            signature = envelope["signature"]
            if not isinstance(payload, str) or not isinstance(signature, str):
                raise ValueError
            response = httpx.post(
                f"{self._address}/v1/transit/verify/{self._transit_key}",
                headers={"X-Vault-Token": self._token},
                json={"input": payload, "signature": signature},
                timeout=5.0,
            )
            response.raise_for_status()
            valid = response.json()["data"]["valid"]
        except (httpx.HTTPError, KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
            raise SecretStoreError("OpenBao device certificate verification failed.") from error
        if valid is not True:
            raise ProofError("The device certificate signature is invalid.")
