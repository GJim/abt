#!/usr/bin/env bash
set -euo pipefail

readonly deploy_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${deploy_directory}/docker-compose.yml"
readonly env_file="${ABT_ENV_FILE:-${deploy_directory}/.env}"
readonly init_file="$(mktemp)"

cleanup() {
  rm -f "${init_file}"
}
trap cleanup EXIT

if [[ ! -f "${env_file}" ]]; then
  printf 'Create %s with OPENBAO_PKCS11_PIN and SOFTHSM_SO_PIN first.\n' "${env_file}" >&2
  exit 1
fi

compose=(docker compose --env-file "${env_file}" -f "${compose_file}")

"${compose[@]}" up -d --build softhsm openbao

for _ in {1..24}; do
  if "${compose[@]}" exec -T softhsm sh -c \
    'pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --login --pin "$SOFTHSM_USER_PIN" --list-objects --type secrkey | grep -Fq "label:      abt-root"'; then
    break
  fi
  sleep 5
done

if ! "${compose[@]}" exec -T softhsm sh -c \
  'pkcs11-tool --module /usr/lib/softhsm/libsofthsm2.so --login --pin "$SOFTHSM_USER_PIN" --list-objects --type secrkey | grep -Fq "label:      abt-root"'; then
  printf 'SoftHSM did not create the abt-root seal key.\n' >&2
  exit 1
fi

status=""
for _ in {1..30}; do
  candidate="$("${compose[@]}" exec -T openbao bao status -format=json 2>/dev/null || true)"
  if python3 -c 'import json, sys; json.load(sys.stdin)' <<<"${candidate}" >/dev/null 2>&1; then
    status="${candidate}"
    break
  fi
  sleep 2
done

if [[ -z "${status}" ]]; then
  printf 'OpenBao API did not become ready; inspect its logs before retrying.\n' >&2
  exit 1
fi

initialized="$(python3 -c 'import json, sys; print(json.load(sys.stdin)["initialized"])' <<<"${status}")"
if [[ "${initialized}" != "False" ]]; then
  printf 'OpenBao is already initialized; refusing to replace existing credentials.\n' >&2
  exit 1
fi

"${compose[@]}" exec -T openbao bao operator init \
  -format=json -recovery-shares=5 -recovery-threshold=3 >"${init_file}"

root_token="$(python3 -c 'import json, sys; print(json.load(sys.stdin)["root_token"])' <"${init_file}")"

printf 'Save these recovery keys and the initial root token in offline secure storage now:\n\n' >&2
python3 -c '
import json
import sys

values = json.load(sys.stdin)
for index, key in enumerate(values["recovery_keys_b64"], 1):
    print(f"Recovery Key {index}: {key}")
print(f"\nInitial Root Token: {values['root_token']}")
' <"${init_file}" >&2

bao() {
  "${compose[@]}" exec -T -e "BAO_TOKEN=${root_token}" openbao bao "$@"
}

bao secrets enable -path=abt kv-v2
bao kv put abt/health status=ok
bao secrets enable transit
bao write -f transit/keys/abt-device-certificates

printf 'path "abt/data/health" {\n  capabilities = ["read"]\n}\n' |
  bao policy write abt-health -

cat <<'EOF' | bao policy write abt-controller -
path "abt/data/mt5/*" {
  capabilities = ["create", "read", "update", "delete"]
}

path "transit/sign/abt-device-certificates" {
  capabilities = ["update"]
}

path "transit/verify/abt-device-certificates" {
  capabilities = ["update"]
}
EOF

health_token="$(bao token create -format=json -display-name=abt-health -policy=abt-health |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["auth"]["client_token"])')"
controller_token="$(bao token create -format=json -display-name=abt-controller -policy=abt-controller |
  python3 -c 'import json, sys; print(json.load(sys.stdin)["auth"]["client_token"])')"

python3 - "${env_file}" "${health_token}" "${controller_token}" <<'PYTHON'
import os
import sys

path, health_token, controller_token = sys.argv[1:]
replacements = {
    "ABT_OPENBAO_HEALTH_TOKEN": health_token,
    "ABT_OPENBAO_CONTROLLER_TOKEN": controller_token,
}
lines = open(path, encoding="utf-8").read().splitlines()
updated = []
for line in lines:
    key = line.split("=", 1)[0]
    if key in replacements:
        updated.append(f"{key}={replacements.pop(key)}")
    else:
        updated.append(line)
updated.extend(f"{key}={value}" for key, value in replacements.items())
with open(path, "w", encoding="utf-8") as destination:
    destination.write("\n".join(updated) + "\n")
os.chmod(path, 0o600)
PYTHON

unset root_token health_token controller_token
printf '\nBootstrap complete. Start the full stack with:\n  docker compose --env-file deploy/.env -f deploy/docker-compose.yml up -d --build\n'
