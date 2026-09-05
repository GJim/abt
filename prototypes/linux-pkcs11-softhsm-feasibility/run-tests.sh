#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
prototype_dir="/workspace/prototypes/linux-pkcs11-softhsm-feasibility"

printf '%s\n' '== Ubuntu 24.04 distribution matrix =='
docker run --rm \
  -v "${repo_root}:/workspace:ro" \
  -w "${prototype_dir}" \
  ubuntu:24.04 \
  bash -lc '
    set -euo pipefail
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      softhsm2 python3-pkcs11 python3-cryptography >/dev/null
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/workspace python3 -m unittest test_pkcs11_keystore.py -v
    python3 --version
    dpkg-query -W -f="\${Package}=\${Version}\\n" \
      softhsm2 libsofthsm2 python3-pkcs11 python3-cryptography
  '

printf '%s\n' '== Python 3.13 application matrix =='
docker run --rm \
  -v "${repo_root}:/workspace:ro" \
  -w "${prototype_dir}" \
  python:3.13-slim-bookworm \
  sh -lc '
    set -eu
    apt-get update -qq
    DEBIAN_FRONTEND=noninteractive apt-get install -y -qq \
      softhsm2 gcc libffi-dev >/dev/null
    pip install --no-cache-dir -q python-pkcs11==0.9.5 cryptography==50.0.1
    PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/workspace python -m unittest test_pkcs11_keystore.py -v
    python - <<"PY"
import importlib.metadata as metadata
import sys
print(f"python={sys.version.split()[0]}")
for package in ("python-pkcs11", "cryptography"):
    print(f"{package}={metadata.version(package)}")
PY
    dpkg-query -W -f="\${Package}=\${Version}\\n" softhsm2 libsofthsm2
  '
