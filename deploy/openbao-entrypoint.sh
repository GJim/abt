#!/bin/sh
set -eu

chown -R openbao:openbao /var/lib/openbao /var/lib/softhsm/tokens
exec su-exec openbao docker-entrypoint.sh "$@"
