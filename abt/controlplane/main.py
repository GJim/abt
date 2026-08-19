from __future__ import annotations

import os
from pathlib import Path

import httpx

from .service import create_app
from .secrets import OpenBaoDeviceCertificateIssuer, OpenBaoSecretStore


def _openbao_is_healthy() -> bool:
    health_url = os.environ.get("ABT_OPENBAO_HEALTH_URL", "http://openbao:8200/v1/abt/data/health")
    health_token = os.environ.get("ABT_OPENBAO_HEALTH_TOKEN")
    if health_token is None:
        return False
    try:
        response = httpx.get(health_url, headers={"X-Vault-Token": health_token}, timeout=2.0)
    except httpx.HTTPError:
        return False
    return response.status_code == 200


def _openbao_secret_store() -> OpenBaoSecretStore | None:
    token = os.environ.get("ABT_OPENBAO_CONTROLLER_TOKEN")
    if token is None:
        return None
    return OpenBaoSecretStore(os.environ.get("ABT_OPENBAO_URL", "http://openbao:8200"), token)


def _openbao_certificate_issuer() -> OpenBaoDeviceCertificateIssuer | None:
    token = os.environ.get("ABT_OPENBAO_CONTROLLER_TOKEN")
    if token is None:
        return None
    return OpenBaoDeviceCertificateIssuer(os.environ.get("ABT_OPENBAO_URL", "http://openbao:8200"), token)


_ledger_path = Path(os.environ.get("ABT_LEDGER_PATH", "/var/lib/abt/ledger.duckdb"))
app = create_app(
    _ledger_path,
    spa_directory=Path(os.environ["ABT_SPA_DIRECTORY"]) if "ABT_SPA_DIRECTORY" in os.environ else None,
    secret_control_healthy=_openbao_is_healthy,
    secret_store=_openbao_secret_store(),
    certificate_issuer=_openbao_certificate_issuer(),
    backup_directory=Path(os.environ["ABT_BACKUP_DIRECTORY"]) if "ABT_BACKUP_DIRECTORY" in os.environ else None,
    openbao_raft_directory=(
        Path(os.environ["ABT_OPENBAO_RAFT_DIRECTORY"]) if "ABT_OPENBAO_RAFT_DIRECTORY" in os.environ else None
    ),
    softhsm_tokens_directory=(
        Path(os.environ["ABT_SOFTHSM_TOKENS_DIRECTORY"]) if "ABT_SOFTHSM_TOKENS_DIRECTORY" in os.environ else None
    ),
    trader_attestation_trust_root=os.environ.get("ABT_TRADER_ATTESTATION_TRUST_ROOT"),
)
