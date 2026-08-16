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


app = create_app(
    Path(os.environ.get("ABT_LEDGER_PATH", "/var/lib/abt/ledger.duckdb")),
    trusted_proxy_ips=frozenset(filter(None, os.environ.get("ABT_TRUSTED_PROXY_IPS", "").split(","))),
    spa_directory=Path(os.environ["ABT_SPA_DIRECTORY"]) if "ABT_SPA_DIRECTORY" in os.environ else None,
    secret_control_healthy=_openbao_is_healthy,
    secret_store=_openbao_secret_store(),
    certificate_issuer=_openbao_certificate_issuer(),
)
