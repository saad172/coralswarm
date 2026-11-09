#!/usr/bin/env bash
#
# check_definer_search_path.sh — every SECURITY DEFINER routine must end its
# search_path with pg_temp.
#
# When pg_temp is not named in a search_path, PostgreSQL searches the session's
# temporary schema FIRST for relation and type names. A session holding
# app-role SQL can create temporary tables, views and domains, so a definer
# whose path does not end in pg_temp can be made to resolve a name to an object
# the session controls while it runs with its owner's privileges (a temporary
# DOMAIN `text` whose CHECK calls a pg_temp function runs that function as the
# owner). Control 0376 / tenant 0198 pinned every definer that existed then; a
# later CREATE [OR REPLACE] FUNCTION replaces the SET clause, so this guard is
# what keeps new and redefined definers honest.
#
# What it does: applies the control tree and the tenant tree to two fresh
# scratch databases with sqlx (the same migrator the backend runs), then lists
# every SECURITY DEFINER routine outside the system schemas (extension members
# excluded) whose proconfig has no search_path, or whose search_path's last
# entry is not pg_temp. Any hit fails the run.
#
# Fix for a hit: write `SET search_path = pg_catalog, public, pg_temp` (keep any
# extra schema the function needs, pg_temp last) on the CREATE FUNCTION.
#
# Usage:
#   scripts/check_definer_search_path.sh              # apply both trees, check
#   scripts/check_definer_search_path.sh --self-test  # prove the check query
#   scripts/check_definer_search_path.sh --database URL [LABEL]
#                     # check one ALREADY-MIGRATED database (no scratch DBs,
#                     # no sqlx); the e2e stack runs this at boot against the
#                     # control and tenant databases it built with the real
#                     # migrators (e2e/stack/assert-db.mjs)
#
# Env:
#   DATABASE_URL  localhost base connection with CREATE DATABASE and CREATE ROLE
#                 rights (e.g. postgres://postgres:postgres@localhost:5432/postgres).
#                 REQUIRED except with --database. The app roles are prepared
#                 on it first, as CI does.
#   SQLX          sqlx binary (default: sqlx on PATH, else ~/.cargo/bin/sqlx).
#   PSQL          psql binary (default: psql on PATH).
#
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ "${1:-}" != "--database" ]; then
  : "${DATABASE_URL:?set DATABASE_URL to a localhost base connection with createdb rights}"
fi
PSQL="${PSQL:-psql}"
SQLX="${SQLX:-$(command -v sqlx || echo "$HOME/.cargo/bin/sqlx")}"

# One row per offending routine: signature | owner | search_path or <unset>.
# The last list entry is compared after trimming spaces and quotes, so
# `public, pg_temp` and `"pg_temp"` both pass and `pg_temp, public` fails.
read -r -d '' CHECK_SQL <<'SQL' || true
WITH d AS (
  SELECT p.oid::pg_catalog.regprocedure::text AS fn,
         pg_catalog.pg_get_userbyid(p.proowner) AS owner,
         (SELECT pg_catalog.substr(c, 13) FROM pg_catalog.unnest(p.proconfig) c
           WHERE c LIKE 'search_path=%') AS sp
    FROM pg_catalog.pg_proc p
    JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
   WHERE p.prosecdef
     AND n.nspname NOT IN ('pg_catalog', 'information_schema')
     AND n.nspname NOT LIKE 'pg\_toast%'
     AND n.nspname NOT LIKE 'pg\_temp\_%'
     AND NOT EXISTS (SELECT 1 FROM pg_catalog.pg_depend dep
                      WHERE dep.classid = 'pg_catalog.pg_proc'::pg_catalog.regclass
                        AND dep.objid = p.oid AND dep.deptype = 'e')
)
SELECT fn || ' | owner=' || owner || ' | search_path=' || COALESCE(sp, '<unset>')
  FROM d
 WHERE sp IS NULL
    OR pg_catalog.btrim((pg_catalog.string_to_array(sp, ','))[pg_catalog.cardinality(pg_catalog.string_to_array(sp, ','))], ' "') <> 'pg_temp'
 ORDER BY 1;
SQL

scratch_url() { echo "$DATABASE_URL" | sed -E "s#(://[^/]+/)[^?]*#\1$1#"; }
q() { "$PSQL" "$1" -X -v ON_ERROR_STOP=1 -qAt -c "$2"; }

SCRATCH_DBS=()
cleanup() {
  local db
  for db in ${SCRATCH_DBS[@]+"${SCRATCH_DBS[@]}"}; do
    "$PSQL" "$DATABASE_URL" -X -qc "DROP DATABASE IF EXISTS $db WITH (FORCE);" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

new_db() {
  local db="$1"
  SCRATCH_DBS+=("$db")
  PGOPTIONS='-c client_min_messages=warning' q "$DATABASE_URL" "DROP DATABASE IF EXISTS $db;" >/dev/null
  q "$DATABASE_URL" "CREATE DATABASE $db;" >/dev/null
}

self_test() {
  local db="definer_sp_selftest_$$" url got want
  new_db "$db"
  url="$(scratch_url "$db")"
  "$PSQL" "$url" -X -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
CREATE SCHEMA probe_ext;
CREATE FUNCTION public.ok_public_temp() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = public, pg_temp AS 'SELECT 1';
CREATE FUNCTION public.ok_extra_schema() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, probe_ext, public, pg_temp AS 'SELECT 1';
CREATE FUNCTION public.ok_invoker() RETURNS int LANGUAGE sql SET search_path = public AS 'SELECT 1';
CREATE FUNCTION public.bad_no_temp() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public AS 'SELECT 1';
CREATE FUNCTION public.bad_catalog_only() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS 'SELECT 1';
CREATE FUNCTION public.bad_temp_first() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_temp, public AS 'SELECT 1';
CREATE FUNCTION public.bad_unset() RETURNS int LANGUAGE sql SECURITY DEFINER AS 'SELECT 1';
CREATE FUNCTION probe_ext.bad_other_schema() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = probe_ext AS 'SELECT 1';
CREATE PROCEDURE public.bad_procedure() LANGUAGE sql SECURITY DEFINER SET search_path = public AS 'SELECT 1';
SQL
  got="$("$PSQL" "$url" -X -v ON_ERROR_STOP=1 -qAt -c "$CHECK_SQL" | sed -E 's/ \|.*//' | sort | tr '\n' ' ')"
  want="bad_catalog_only() bad_no_temp() bad_procedure() bad_temp_first() bad_unset() probe_ext.bad_other_schema() "
  if [ "$got" != "$want" ]; then
    echo "definer search_path guard self-test: FAILED" >&2
    echo "  want: $want" >&2
    echo "  got:  $got" >&2
    exit 1
  fi
  echo "definer search_path guard self-test: OK (flags 6 bad definers, passes 2 pinned definers and 1 invoker)"
}

check_tree() {
  local tree="$1" db="$2" url hits
  new_db "$db"
  url="$(scratch_url "$db")"
  echo "definer search_path: applying $tree tree -> $db"
  DATABASE_URL="$url" "$SQLX" migrate run --source "backend/migrations_${tree}" >/dev/null
  local total
  total="$(q "$url" "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND n.nspname NOT IN ('pg_catalog','information_schema')")"
  hits="$("$PSQL" "$url" -X -v ON_ERROR_STOP=1 -qAt -c "$CHECK_SQL")"
  if [ -n "$hits" ]; then
    echo "FAIL: $tree: SECURITY DEFINER routines whose search_path does not end in pg_temp:" >&2
    while IFS= read -r line; do echo "  $line" >&2; done <<< "$hits"
    fail=1
  else
    echo "definer search_path: $tree OK ($total SECURITY DEFINER routines, all end in pg_temp)"
  fi
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit 0
fi

if [ "${1:-}" = "--database" ]; then
  [ -n "${2:-}" ] || { echo "usage: $0 --database URL [LABEL]" >&2; exit 2; }
  label="${3:-database}"
  total="$(q "$2" "SELECT count(*) FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace WHERE p.prosecdef AND n.nspname NOT IN ('pg_catalog','information_schema')")"
  hits="$("$PSQL" "$2" -X -v ON_ERROR_STOP=1 -qAt -c "$CHECK_SQL")"
  if [ -n "$hits" ]; then
    echo "FAIL: $label: SECURITY DEFINER routines whose search_path does not end in pg_temp:" >&2
    while IFS= read -r line; do echo "  $line" >&2; done <<< "$hits"
    exit 1
  fi
  # A migrated tree has dozens of definers; none means the wrong database.
  if [ "$total" -lt 1 ]; then
    echo "FAIL: $label has no SECURITY DEFINER routines at all; is it migrated?" >&2
    exit 1
  fi
  echo "definer search_path: $label OK ($total SECURITY DEFINER routines, all end in pg_temp)"
  exit 0
fi

bash scripts/prepare_ci_pg_roles.sh --localhost

fail=0
check_tree control "definer_sp_ctl_$$"
check_tree tenant "definer_sp_tnt_$$"
if [ "$fail" -ne 0 ]; then
  echo "Pin every SECURITY DEFINER function with SET search_path = pg_catalog, public, pg_temp (pg_temp last)." >&2
  echo "definer search_path guard: FAILED" >&2
  exit 1
fi
echo "definer search_path guard: OK"
