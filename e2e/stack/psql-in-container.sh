#!/usr/bin/env bash
# psql inside the e2e Postgres container, for scripts that take a PSQL binary
# (scripts/check_definer_search_path.sh, scripts/check_provisioning_privileges.sh).
# The runner needs no psql of its own; URLs are the container's view
# (127.0.0.1:5432). E2E_PG_CONTAINER names the container (stack/config.mjs).
set -euo pipefail
: "${E2E_PG_CONTAINER:?set E2E_PG_CONTAINER (stack/config.mjs CONTAINERS.pg)}"
exec docker exec -i "$E2E_PG_CONTAINER" psql "$@"
