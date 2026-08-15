from __future__ import annotations

import os
from pathlib import Path

from .service import create_app


app = create_app(
    Path(os.environ.get("ABT_LEDGER_PATH", "/var/lib/abt/ledger.duckdb")),
    trusted_proxy_ips=frozenset(filter(None, os.environ.get("ABT_TRUSTED_PROXY_IPS", "").split(","))),
    spa_directory=Path(os.environ["ABT_SPA_DIRECTORY"]) if "ABT_SPA_DIRECTORY" in os.environ else None,
)
