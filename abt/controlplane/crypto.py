from __future__ import annotations

import base64
import json
from datetime import UTC, datetime, timedelta

from cryptography.exceptions import InvalidSignature
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.asymmetric.utils import encode_dss_signature


class ProofError(ValueError):
    """Raised when a worker enrollment proof is invalid."""


def enrollment_payload(
    login: int,
    server: str,
    account_info: dict[str, object] | None = None,
    terminal_info: dict[str, object] | None = None,
    mt5_password: str = "",
    enrollment_challenge: str = "",
) -> bytes:
    return json.dumps(
        {
            "account_info": account_info or {},
            "login": login,
            "mt5_password": mt5_password,
            "enrollment_challenge": enrollment_challenge,
            "server": server,
            "terminal_info": terminal_info or {},
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def device_certificate_payload(
    *,
    worker_id: str,
    login: int,
    server: str,
    public_key_pem: str,
    issued_at: datetime,
    expires_at: datetime,
) -> bytes:
    return json.dumps(
        {
            "expires_at": expires_at.astimezone(UTC).isoformat(),
            "issued_at": issued_at.astimezone(UTC).isoformat(),
            "login": login,
            "public_key_pem": public_key_pem,
            "server": server,
            "worker_id": worker_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def worker_proof_payload(*, purpose: str, worker_id: str, nonce: str) -> bytes:
    """Return the canonical, replay-resistant payload a device key must sign."""

    return json.dumps(
        {"nonce": nonce, "purpose": purpose, "worker_id": worker_id},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def trader_certificate_payload(
    *,
    trader_id: str,
    strategy_name: str,
    public_key_pem: str,
    issued_at: datetime,
    expires_at: datetime,
) -> bytes:
    return json.dumps(
        {
            "expires_at": expires_at.astimezone(UTC).isoformat(),
            "issued_at": issued_at.astimezone(UTC).isoformat(),
            "public_key_pem": public_key_pem,
            "strategy_name": strategy_name,
            "trader_id": trader_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def parse_trader_certificate(certificate: str) -> dict[str, object]:
    """Decode a Trader certificate without trusting its signature."""

    try:
        envelope = json.loads(certificate)
        payload = base64.b64decode(envelope["payload"], validate=True)
        claims = json.loads(payload)
    except (TypeError, KeyError, ValueError, json.JSONDecodeError) as error:
        raise ProofError("The Trader certificate is malformed.") from error
    if not isinstance(claims, dict):
        raise ProofError("The Trader certificate is malformed.")
    required = {"trader_id", "strategy_name", "public_key_pem", "issued_at", "expires_at"}
    if set(claims) != required or not all(isinstance(claims[name], str) for name in required):
        raise ProofError("The Trader certificate has invalid claims.")
    try:
        expires_at = datetime.fromisoformat(claims["expires_at"])
        issued_at = datetime.fromisoformat(claims["issued_at"])
    except (TypeError, ValueError) as error:
        raise ProofError("The Trader certificate has invalid timestamps.") from error
    if issued_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= issued_at or datetime.now(UTC) >= expires_at:
        raise ProofError("The Trader certificate is expired or invalid.")
    return claims


def trader_proof_payload(*, purpose: str, trader_id: str, nonce: str) -> bytes:
    return json.dumps(
        {"nonce": nonce, "purpose": purpose, "trader_id": trader_id},
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def verify_trader_proof(public_key_pem: str, signature_base64: str, *, purpose: str, trader_id: str, nonce: str) -> None:
    _verify_p256_signature(
        public_key_pem,
        signature_base64,
        trader_proof_payload(purpose=purpose, trader_id=trader_id, nonce=nonce),
        malformed_message="The Trader proof is malformed.",
        invalid_message="The Trader proof signature is invalid.",
    )


def parse_device_certificate(certificate: str) -> dict[str, object]:
    """Decode a device certificate without trusting its signature."""

    try:
        envelope = json.loads(certificate)
        payload = base64.b64decode(envelope["payload"], validate=True)
        claims = json.loads(payload)
    except (TypeError, KeyError, ValueError, json.JSONDecodeError) as error:
        raise ProofError("The device certificate is malformed.") from error
    if not isinstance(claims, dict):
        raise ProofError("The device certificate is malformed.")
    required = {"worker_id", "login", "server", "public_key_pem", "issued_at", "expires_at"}
    if set(claims) != required:
        raise ProofError("The device certificate has invalid claims.")
    if (
        not isinstance(claims["worker_id"], str)
        or not isinstance(claims["login"], int)
        or isinstance(claims["login"], bool)
        or not isinstance(claims["server"], str)
        or not isinstance(claims["public_key_pem"], str)
    ):
        raise ProofError("The device certificate has invalid claims.")
    try:
        expires_at = datetime.fromisoformat(claims["expires_at"])
        issued_at = datetime.fromisoformat(claims["issued_at"])
    except (TypeError, ValueError) as error:
        raise ProofError("The device certificate has invalid timestamps.") from error
    if issued_at.tzinfo is None or expires_at.tzinfo is None or expires_at <= issued_at or datetime.now(UTC) >= expires_at:
        raise ProofError("The device certificate is expired or invalid.")
    return claims


def verify_worker_proof(public_key_pem: str, signature_base64: str, *, purpose: str, worker_id: str, nonce: str) -> None:
    """Verify a P-256 signature over a server-issued worker challenge."""

    _verify_p256_signature(
        public_key_pem,
        signature_base64,
        worker_proof_payload(purpose=purpose, worker_id=worker_id, nonce=nonce),
        malformed_message="The worker proof is malformed.",
        invalid_message="The worker proof signature is invalid.",
    )


def verify_enrollment_proof(
    public_key_pem: str,
    signature_base64: str,
    *,
    login: int,
    server: str,
    account_info: dict[str, object] | None = None,
    terminal_info: dict[str, object] | None = None,
    mt5_password: str = "",
    enrollment_challenge: str = "",
) -> None:
    _verify_p256_signature(
        public_key_pem,
        signature_base64,
        enrollment_payload(
            login, server, account_info, terminal_info, mt5_password, enrollment_challenge
        ),
        malformed_message="The enrollment proof is malformed.",
        invalid_message="The enrollment proof signature is invalid.",
    )


def trader_enrollment_payload(*, registration_invite: str, strategy_name: str, claimed_public_ip: str) -> bytes:
    return json.dumps(
        {
            "claimed_public_ip": claimed_public_ip,
            "registration_invite": registration_invite,
            "strategy_name": strategy_name,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def trader_rotation_payload(*, trader_id: str, replacement_public_key_pem: str, nonce: str) -> bytes:
    """Return the payload signed by both keys during a Trader key rotation."""

    return json.dumps(
        {
            "nonce": nonce,
            "purpose": "trader_certificate_rotation",
            "replacement_public_key_pem": replacement_public_key_pem,
            "trader_id": trader_id,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def verify_trader_attestation(
    attestation_jwt: str, *, public_key_pem: str, trust_root_public_key_pem: str | None
) -> str:
    """Verify a signed hardware-key attestation bound to the enrollment key.

    The trust root is deliberately explicit: a supplied JWT is never useful
    unless its ES256 signature chains to the configured platform root.
    """

    if trust_root_public_key_pem is None:
        raise ProofError("Trader attestation trust root is not configured.")
    try:
        header_part, claims_part, signature_part = attestation_jwt.split(".")
        header = json.loads(_base64url_decode(header_part))
        claims = json.loads(_base64url_decode(claims_part))
        raw_signature = _base64url_decode(signature_part)
        trust_root = serialization.load_pem_public_key(trust_root_public_key_pem.encode("utf-8"))
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise ProofError("The Trader attestation JWT is malformed.") from error
    if (
        not isinstance(header, dict)
        or header.get("alg") != "ES256"
        or set(header) - {"alg", "typ", "kid"}
        or not isinstance(claims, dict)
        or len(raw_signature) != 64
        or not isinstance(trust_root, ec.EllipticCurvePublicKey)
        or not isinstance(trust_root.curve, ec.SECP256R1)
    ):
        raise ProofError("The Trader attestation JWT is invalid.")
    provider = claims.get("provider")
    exp = claims.get("exp")
    issued_at = claims.get("iat")
    if (
        provider not in {"CNG", "TPM", "PKCS11"}
        or claims.get("public_key_pem") != public_key_pem
        or claims.get("non_exportable") is not True
        or not isinstance(exp, (int, float))
        or isinstance(exp, bool)
        or not isinstance(issued_at, (int, float))
        or isinstance(issued_at, bool)
        or issued_at > datetime.now(UTC).timestamp() + 300
        or exp <= datetime.now(UTC).timestamp()
    ):
        raise ProofError("The Trader attestation claims are invalid.")
    try:
        trust_root.verify(
            encode_dss_signature(int.from_bytes(raw_signature[:32]), int.from_bytes(raw_signature[32:])),
            f"{header_part}.{claims_part}".encode("ascii"),
            ec.ECDSA(hashes.SHA256()),
        )
    except InvalidSignature as error:
        raise ProofError("The Trader attestation signature is invalid.") from error
    return provider


def verify_trader_enrollment_proof(
    public_key_pem: str, signature_base64: str, *, registration_invite: str, strategy_name: str, claimed_public_ip: str
) -> None:
    _verify_p256_signature(
        public_key_pem,
        signature_base64,
        trader_enrollment_payload(
            registration_invite=registration_invite, strategy_name=strategy_name, claimed_public_ip=claimed_public_ip
        ),
        malformed_message="The trader enrollment proof is malformed.",
        invalid_message="The trader enrollment proof signature is invalid.",
    )


def verify_trader_rotation_proof(
    public_key_pem: str,
    signature_base64: str,
    *,
    trader_id: str,
    replacement_public_key_pem: str,
    nonce: str,
) -> None:
    _verify_p256_signature(
        public_key_pem,
        signature_base64,
        trader_rotation_payload(
            trader_id=trader_id, replacement_public_key_pem=replacement_public_key_pem, nonce=nonce
        ),
        malformed_message="The Trader rotation proof is malformed.",
        invalid_message="The Trader rotation proof signature is invalid.",
    )


def _base64url_decode(value: str) -> bytes:
    if not isinstance(value, str) or not value:
        raise ValueError
    return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))


def _verify_p256_signature(
    public_key_pem: str,
    signature_base64: str,
    payload: bytes,
    *,
    malformed_message: str,
    invalid_message: str,
) -> None:
    try:
        public_key = serialization.load_pem_public_key(public_key_pem.encode("utf-8"))
        signature = base64.b64decode(signature_base64, validate=True)
    except (TypeError, ValueError) as error:
        raise ProofError(malformed_message) from error
    if not isinstance(public_key, ec.EllipticCurvePublicKey) or not isinstance(public_key.curve, ec.SECP256R1):
        raise ProofError("The worker public key must use ECDSA P-256.")
    try:
        public_key.verify(signature, payload, ec.ECDSA(hashes.SHA256()))
    except InvalidSignature as error:
        raise ProofError(invalid_message) from error
