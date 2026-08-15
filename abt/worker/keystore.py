from __future__ import annotations

from typing import Protocol


class HardwareKeyStore(Protocol):
    """A non-exportable ECDSA P-256 device identity key."""

    def public_key_pem(self) -> str:
        """Return the public key that is safe to submit during enrollment."""

    def sign(self, payload: bytes) -> bytes:
        """Sign payload without exposing the device private key."""


class UnsupportedKeyStore(RuntimeError):
    """Raised when the host lacks the required CNG/TPM/PKCS#11 provider."""
