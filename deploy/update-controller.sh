#!/usr/bin/env bash
set -euo pipefail

# Rebuild and restart only the controller service.
#
# Usage: sudo ./deploy/update-controller.sh
#
# This picks up local changes to the controller image (abt/ source, web/
# frontend, deploy/controller.Dockerfile) without touching the sealed
# secrets topology: openbao, softhsm, and cloudflared are neither rebuilt
# nor restarted, so Raft state, HSM tokens, and tunnel ingress keep running.
# Full first-time bootstrap remains deploy/bootstrap-deployment.sh.

readonly deploy_directory="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly compose_file="${deploy_directory}/docker-compose.yml"
readonly env_file="${ABT_ENV_FILE:-${deploy_directory}/.env}"

if [[ ! -f "${env_file}" ]]; then
  printf 'Missing %s: it must exist so Compose can interpolate service secrets.\n' "${env_file}" >&2
  exit 1
fi

compose=(docker compose --env-file "${env_file}" -f "${compose_file}")

"${compose[@]}" up -d --build controller

for _ in {1..30}; do
  if "${compose[@]}" exec -T controller python -c \
    'from urllib.request import urlopen; urlopen("http://localhost:8000/health", timeout=2)' >/dev/null 2>&1; then
    printf 'Controller updated and healthy.\n'
    exit 0
  fi
  sleep 2
done

printf 'Controller did not become healthy; inspect its logs before retrying.\n' >&2
"${compose[@]}" ps controller
"${compose[@]}" logs --tail=50 controller
exit 1
