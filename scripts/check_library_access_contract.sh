#!/usr/bin/env bash
#
# check_library_access_contract.sh: the Library access-contract digest gate
# (#1524 blocker 1). Sibling in spirit of check_tenant_rls_coverage.sh: that
# gate proves every tenant query path sets the RLS GUCs; this one proves the
# RLS contract the cached reader's equivalence argument was reviewed against
# has not moved.
#
# The accelerated Library reader admits a cached graph to any reader of the
# same access class (backend/src/read_access). That is exact only while the
# policies, RLS flags and visibility/floor/permission function bodies hashed
# by `library_access_contract_digest()` (tenant 0206 / control 0391) are the
# reviewed ones. At run time a changed digest already fails closed (every
# read falls back to Live as `access_contract_changed`); this gate makes the
# change visible at review time instead of as a silent fast-path outage.
#
# What it does: creates two scratch databases on the given cluster, applies
# backend/migrations_tenant and backend/migrations_control with sqlx, computes
# each plane's digest, and compares it with TENANT_CONTRACT_DIGEST /
# CONTROL_CONTRACT_DIGEST in backend/src/read_access/mod.rs. The scratch
# databases are always dropped.
#
# A failure means a migration changed a covered object. Do NOT just paste the
# new value: re-review the class equivalence (design
# docs/plans/library-members-busy-design/design.md sections 1.3, 2.1, 2.4)
# for that migration, then update the constant in the same PR. Updating the
# constant IS the review marker.
#
# Usage (CI is currently off, so this is how it runs today):
#   LIBRARY_ACCESS_CONTRACT_PG_URL=postgres://postgres:postgres@localhost:15544/postgres \
#     scripts/check_library_access_contract.sh
# ORGT_TEST_BASE_URL is accepted as a fallback. The cluster needs pgvector and
# the coralswarm_tenant_app / coralswarm_app roles (scripts/prepare_ci_pg_roles.sh).
# Requires: sqlx (sqlx-cli), psql.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

base="${LIBRARY_ACCESS_CONTRACT_PG_URL:-${ORGT_TEST_BASE_URL:-}}"
if [ -z "$base" ]; then
  echo "check_library_access_contract: set LIBRARY_ACCESS_CONTRACT_PG_URL (or ORGT_TEST_BASE_URL) to an admin URL of a local pgvector cluster" >&2
  exit 2
fi
case "$base" in
  postgres://*@localhost:*/*|postgres://*@127.0.0.1:*/*|postgresql://*@localhost:*/*|postgresql://*@127.0.0.1:*/*) ;;
  *) echo "check_library_access_contract: refusing a non-local cluster URL; this gate creates and drops databases" >&2; exit 2 ;;
esac
for tool in sqlx psql; do
  command -v "$tool" >/dev/null 2>&1 || { echo "check_library_access_contract: $tool is required" >&2; exit 2; }
done

source_file="backend/src/read_access/mod.rs"
expected() {
  sed -n "s/^pub const $1: &str = \"\([0-9a-f]\{64\}\)\";$/\1/p" "$source_file"
}
tenant_expected="$(expected TENANT_CONTRACT_DIGEST)"
control_expected="$(expected CONTROL_CONTRACT_DIGEST)"
if [ -z "$tenant_expected" ] || [ -z "$control_expected" ]; then
  echo "check_library_access_contract: could not read both digest constants from $source_file" >&2
  exit 1
fi

server="${base%/*}"
suffix="$(date +%s)_$$"
tenant_db="library_contract_tenant_${suffix}"
control_db="library_contract_control_${suffix}"
cleanup() {
  psql -X -q "$base" -c "DROP DATABASE IF EXISTS ${tenant_db} WITH (FORCE)" -c "DROP DATABASE IF EXISTS ${control_db} WITH (FORCE)" >/dev/null 2>&1 || true
}
trap cleanup EXIT
psql -X -q -v ON_ERROR_STOP=1 "$base" -c "CREATE DATABASE ${tenant_db}" -c "CREATE DATABASE ${control_db}" >/dev/null

status=0
check() {
  local plane="$1" db="$2" tree="$3" want="$4" got
  sqlx migrate run --source "backend/${tree}" --database-url "${server}/${db}" >/dev/null
  got="$(psql -X -A -t -v ON_ERROR_STOP=1 "${server}/${db}" -c "SELECT public.library_access_contract_digest()" | tr -d '[:space:]')"
  if [ "$got" = "$want" ]; then
    echo "ok   ${plane}: ${got}"
  else
    echo "FAIL ${plane}: migrated digest ${got} != reviewed ${want} (${source_file})" >&2
    status=1
  fi
}
check tenant "$tenant_db" migrations_tenant "$tenant_expected"
check control "$control_db" migrations_control "$control_expected"
if [ "$status" -ne 0 ]; then
  echo "A migration changed the Library access contract. Re-review the class equivalence before updating the constant." >&2
fi
exit "$status"
