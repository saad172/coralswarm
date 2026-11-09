#!/usr/bin/env bash
#
# check_ci_guards_complete.sh — fail if ci.yml or deploy.yml runs a
# database-free guard that scripts/ci_guards.sh (the PR gate's `guards` job)
# does not.
#
# Every single-line `run: bash scripts/…` / `run: python3 scripts/…` step in
# the two workflows is either a guard (its exact command line must appear in
# ci_guards.sh) or listed in NOT_GUARDS below with the reason it cannot run in
# a database-free, build-free job.
#
# Usage: scripts/check_ci_guards_complete.sh [--self-test]

set -euo pipefail
cd "$(dirname "$0")/.."

# Command prefixes that are not database-free guards, and why.
NOT_GUARDS=(
  'SQLX='                             # needs Postgres + sqlx (definer/provisioning/schema-parity jobs)
  'bash scripts/prepare_ci_pg_roles.sh' # a Postgres fixture step, not a check
  'bash scripts/mcp-smoke.sh'          # needs a running backend
  'bash scripts/migrate_shared_db_org.sh --self-test' # refuses to run without a throwaway Postgres (ORGT_SELF_TEST_DSN)
  'python3 scripts/deployment.py'      # deploy bookkeeping
  'scripts/check_glibc_floor.sh'       # needs the built release dist/
)

workflow_commands() {
  grep -hoE '^[[:space:]]+(- )?run: (SQLX=[^ ]+ )?(bash |python3 )?scripts/[A-Za-z0-9_./-]+([^|&;]*)?$' "$@" \
    | sed -E 's/^[[:space:]]+(- )?run: //; s/[[:space:]]+$//' | sort -u
}

missing_from() {
  local guards="$1"; shift
  local cmd skip prefix
  while IFS= read -r cmd; do
    [ -n "$cmd" ] || continue
    skip=
    for prefix in "${NOT_GUARDS[@]}"; do
      [[ "$cmd" == "$prefix"* ]] && skip=1
    done
    [ -n "$skip" ] && continue
    grep -qF -- " $cmd" "$guards" || echo "$cmd"
  done < <(workflow_commands "$@")
}

if [ "${1:-}" = "--self-test" ]; then
  tmp=$(mktemp -d); trap 'rm -rf "$tmp"' EXIT
  printf '    steps:\n      - run: bash scripts/check_a.sh\n      - run: bash scripts/check_b.sh --self-test\n      - run: SQLX=x bash scripts/check_db.sh\n' > "$tmp/wf.yml"
  printf 'run "a" bash scripts/check_a.sh\n' > "$tmp/guards.sh"
  out=$(missing_from "$tmp/guards.sh" "$tmp/wf.yml")
  [ "$out" = "bash scripts/check_b.sh --self-test" ] || { echo "self-test FAILED: got '$out'" >&2; exit 1; }
  printf 'run "b" bash scripts/check_b.sh --self-test\n' >> "$tmp/guards.sh"
  [ -z "$(missing_from "$tmp/guards.sh" "$tmp/wf.yml")" ] || { echo "self-test FAILED: complete list reported missing" >&2; exit 1; }
  echo "check_ci_guards_complete self-test: ok"
  exit 0
fi

missing=$(missing_from scripts/ci_guards.sh .github/workflows/ci.yml .github/workflows/deploy.yml)
if [ -n "$missing" ]; then
  echo "FAIL: ci.yml/deploy.yml run these guards but scripts/ci_guards.sh does not:" >&2
  printf '  %s\n' "$missing" >&2
  echo "Add each to scripts/ci_guards.sh, or to NOT_GUARDS in $0 with the reason it needs a database or a build." >&2
  exit 1
fi
echo "check_ci_guards_complete: every workflow guard is in scripts/ci_guards.sh"
