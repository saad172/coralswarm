#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo 'usage: prepare_ci_pg_roles.sh --container POSTGRES_SERVICE_ID | --localhost' >&2
  exit 2
}

case "${1:-}" in
  --container)
    if [ "$#" -ne 2 ]; then
      usage
    fi
    if [[ ! "$2" =~ ^[a-f0-9]{12,64}$ ]]; then
      usage
    fi
    driver=(docker exec -i "$2" psql -U postgres -d postgres -X -v ON_ERROR_STOP=1)
    ;;
  --localhost)
    [ "$#" -eq 1 ] || usage
    connection_pattern='^postgres(ql)?://[^/@[:space:]]+@(localhost|127\.0\.0\.1):([0-9]{1,5})/[A-Za-z0-9_]+$'
    if [[ ! "${DATABASE_URL:-}" =~ $connection_pattern ]]; then
      echo 'CI role preparation requires an explicit localhost PostgreSQL URL without query parameters.' >&2
      exit 2
    fi
    port="${BASH_REMATCH[3]}"
    (( 10#$port >= 1 && 10#$port <= 65535 )) || usage
    driver=(psql "$DATABASE_URL" -X -v ON_ERROR_STOP=1)
    ;;
  *) usage ;;
esac

"${driver[@]}" <<'SQL'
DO $fixture$
DECLARE
    role_name text;
    existing pg_catalog.pg_roles%ROWTYPE;
BEGIN
    FOREACH role_name IN ARRAY ARRAY['coralswarm_app', 'coralswarm_tenant_app'] LOOP
        SELECT * INTO existing FROM pg_catalog.pg_roles WHERE rolname = role_name;
        IF NOT FOUND THEN
            EXECUTE format('CREATE ROLE %I LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEDB NOCREATEROLE NOREPLICATION', role_name);
        ELSIF NOT existing.rolcanlogin OR existing.rolsuper OR existing.rolbypassrls
                OR existing.rolcreatedb OR existing.rolcreaterole OR existing.rolreplication
                OR EXISTS (SELECT 1 FROM pg_catalog.pg_auth_members WHERE member = existing.oid) THEN
            RAISE EXCEPTION 'incompatible existing CI role %', role_name;
        END IF;
    END LOOP;
END
$fixture$;
SQL
