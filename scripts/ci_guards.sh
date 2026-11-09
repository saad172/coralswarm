#!/usr/bin/env bash
#
# ci_guards.sh — every seconds-long, database-free invariant guard, in one
# place, for the PR gate (`.github/workflows/pr.yml`, job `guards`).
#
# This script owns no checking logic. Each line runs the SAME script that
# ci.yml's guard jobs and deploy.yml's `pre-deploy-invariant-guard` already
# call, with the same arguments; the union of those two lists is what runs
# here. Guards that need a database or `sqlx` (definer search_path,
# provisioning privileges, schema parity) are NOT here: migrations-from-zero
# runs in the e2e stack boot instead (docs: e2e/README.md).
#
# When a guard is added to ci.yml or deploy.yml, add it here too:
# scripts/check_ci_guards_complete.sh (run first, below) fails until you do.
# The CI redesign's cleanup PR points both workflows at this script so the
# list exists once.
#
# Usage: scripts/ci_guards.sh
#   BASE_REF  optional; base commit for check_migrations.sh's append-only
#             diff (pr.yml passes the PR / merge-group base). Unset, that one
#             sub-check no-ops exactly as it does in ci.yml today.
#
# Runs every guard even after one fails, then exits 1 if any failed, so one
# CI run reports every broken invariant instead of the first.

set -uo pipefail

cd "$(dirname "$0")/.."

failed=()

run() {
  local name="$1"
  shift
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::group::$name"; else echo "==> $name"; fi
  local started=$SECONDS
  if "$@"; then
    status=ok
  else
    status=FAILED
    failed+=("$name")
  fi
  if [ -n "${GITHUB_ACTIONS:-}" ]; then echo "::endgroup::"; fi
  echo "    $status ($((SECONDS - started))s) $name"
}

# This list is complete: every database-free guard ci.yml or deploy.yml
# invokes is here, or the run fails (scripts/check_ci_guards_complete.sh).
run "ci_guards completeness self-test" bash scripts/check_ci_guards_complete.sh --self-test
run "ci_guards.sh covers every workflow guard" bash scripts/check_ci_guards_complete.sh

# release.yml plan job: the release path is tested on every PR, not first at release time
run "financial image admission tests" python3 scripts/test_deploy_financial_backend.py
run "release evidence store and verifier tests" python3 scripts/test_release_evidence.py
run "release plan tests" python3 scripts/test_release_plan.py

# deploy.yml pre-deploy-invariant-guard
run "deployment admission tests" python3 -m unittest discover -s scripts/tests -p test_deployment.py
run "no ambient pool under routes/" bash scripts/check_no_ambient_pool.sh
run "promotion never crosses a database" bash scripts/check_no_cross_plane_promotion.sh
run "no unsupported Postgres startup parameter" bash scripts/check_startup_parameters.sh
run "embedding-gateway guard self-test" bash scripts/check_embedding_gateway.sh --self-test
run "every embedding goes through the gateway" bash scripts/check_embedding_gateway.sh
run "managed front-door guard self-test" bash scripts/check_managed_front_door.sh --self-test
run "one plane resolver for managed admission and billing" bash scripts/check_managed_front_door.sh
run "silent-drop guard self-test" bash scripts/check_silent_drop_is_alertable.sh --self-test
run "a silently-dropping state must be alertable" bash scripts/check_silent_drop_is_alertable.sh
run "no O(rows) request fan-out" bash scripts/check_no_unbounded_fanout.sh
run "verdict-once guard self-test" python3 scripts/check_verdict_once.py --self-test
run "database health parser self-test" python3 scripts/check_fly_db_health.py --self-test
run "page-read health parser self-test" python3 scripts/check_page_read_health.py --self-test
run "page-load fixture and timing evidence tests" node --test e2e/perf/helpers.test.mjs
run "page-load lawful rate cadence tests" node --test e2e/perf/rate-cadence.test.mjs
run "page-load healthy setup tests" node --test e2e/perf/healthy-setup.test.mjs
run "each per-row verdict runs once" python3 scripts/check_verdict_once.py
run "_scoped shape ratchet self-test" bash scripts/check_scoped_shape_ratchet.sh --self-test
run "_scoped shape ratchet" bash scripts/check_scoped_shape_ratchet.sh
run "handler transaction ratchet self-test" python3 scripts/check_handler_transactions.py --self-test
run "handler transaction ratchet" python3 scripts/check_handler_transactions.py
run "RLS scope coverage gate" bash scripts/check_rls_coverage.sh
run "migration numbering + checksum manifest" bash scripts/check_migrations.sh
run "migration contract guard self-test" bash scripts/check_migration_contract.sh --self-test
run "migration contract (expand/contract)" bash scripts/check_migration_contract.sh

# ci.yml guard jobs not already listed above
run "operator-script self-tests: backup_control_plane" bash scripts/backup_control_plane.sh --self-test
run "operator-script self-tests: backup_tenants" bash scripts/backup_tenants.sh --self-test
run "operator-script self-tests: rehearse_tenant_restore" bash scripts/rehearse_tenant_restore.sh --self-test
run "operator-script self-tests: tenant_restore_drill" bash scripts/tenant_restore_drill.sh --self-test
# Run shellcheck over the file list ci.yml's migration-guard job names, read from
# ci.yml itself so the list exists once.
shellcheck_ci_list() {
  local files
  files=$(grep -m1 -E '^[[:space:]]+run: shellcheck ' .github/workflows/ci.yml | sed -E 's/^[[:space:]]+run: shellcheck //')
  [ -n "$files" ] || { echo "ci.yml has no shellcheck step to mirror" >&2; return 1; }
  # shellcheck disable=SC2086 # the list is whitespace-separated paths
  shellcheck $files
}
run "shellcheck (ci.yml's list)" shellcheck_ci_list
run "CI APT source and failure policy fixtures" python3 scripts/test_ci_apt_setup.py
run "CI PostgreSQL role fixture wiring" python3 scripts/test_prepare_ci_pg_roles.py
run "connectors-do-not-answer guard self-test" bash scripts/check_connectors_do_not_answer.sh --self-test
run "connectors do not answer" bash scripts/check_connectors_do_not_answer.sh
run "membership-writer guard self-test" bash scripts/check_ensure_org_membership_callers.sh --self-test
run "membership writers" bash scripts/check_ensure_org_membership_callers.sh
run "history-floor boundary guard self-test" bash scripts/check_history_floor_boundary.sh --self-test
run "history-floor boundary" bash scripts/check_history_floor_boundary.sh
run "tenant RLS coverage gate" bash scripts/check_tenant_rls_coverage.sh
run "org-arm scope guard self-test" bash scripts/check_org_arm_scope.sh --self-test
run "org-arm scope" bash scripts/check_org_arm_scope.sh

echo
run "connector legacy routes" python3 scripts/check_connector_legacy_routes.py
run "connector legacy guard tests" python3 scripts/test_connector_legacy_routes.py

run "page-read declaration guard self-test" bash scripts/check_page_read_routes.sh --self-test
run "page-read declaration coverage" bash scripts/check_page_read_routes.sh

run "read-model triggers and compute boundary" bash scripts/check_read_model_triggers.sh
run "read-model guard self-test" bash scripts/check_read_model_triggers.sh --self-test

if [ "${#failed[@]}" -gt 0 ]; then
  echo "ci_guards: ${#failed[@]} guard(s) failed:" >&2
  printf '  - %s\n' "${failed[@]}" >&2
  exit 1
fi
echo "ci_guards: all guards passed"
