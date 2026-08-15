from __future__ import annotations

import base64
import json

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec


class ProofError(ValueError):
    """Raised when a worker enrollment proof is invalid."""


def enrollment_payload(login: int, server: str, pairing_code: str) -> bytes:
    return json.dumps(
        {"login": login, "pairing_code": pairing_code, "server": server},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def verify_enrollment_proof(
    public_key_pem: str,
    signature_base64: str,
    *,
    login: int,
    server: str,
    pairing_code: str,
) -> None:
    try:
        public_key = serialization.load_pem_public_key(public_key_pem.encode("utf-8"))
        signature = base64.b64decode(signature_base64, validate=True)
    except (TypeError, ValueError) as error:
        raise ProofError("The enrollment proof is malformed.") from error
    if not isinstance(public_key, ec.EllipticCurvePublicKey) or not isinstance(public_key.curve, ec.SECP256R1):
        raise ProofError("The worker public key must use ECDSA P-256.")
    try:
        public_key.verify(
            signature,
            enrollment_payload(login, server, pairing_code),
            ec.ECDSA(hashes.SHA256()),
        )
    except InvalidSignature as error:
        raise ProofError("The enrollment proof signature is invalid.") from error
