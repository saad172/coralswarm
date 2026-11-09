#!/bin/sh
#
# release.sh — the Fly deploy `release_command` (see fly.toml / fly.staging.toml).
#
# Fly boots ONE throwaway machine from the newly built image, runs this to
# completion, and only then rolls traffic onto the new version; a non-zero exit
# aborts the deploy with nothing having been routed. Both migration trees are
# applied here, in order:
#
#   1. CONTROL tree  — /app/migrate_control  (the shared consumer plane)
#   2. TENANT tree   — /app/migrate_tenants  (every active/provisioning org DB)
#
# WHY THE TENANT TREE BELONGS HERE (OPS-5). `backend/src/workers/tenant_harness.rs`
# refuses to poll — and the request path 503s — any tenant whose
# `tenant_databases.schema_version` is below `tenancy::migrate::head_version()`.
# While the tenant tree was applied only by hand from an operator checkout
# (onboarding step 5), EVERY tenant-tree migration silently broke every existing
# org tenant from the moment the new image served until a human remembered to
# run the orchestrator. Chaining it into the release closes that window by
# construction: the code that assumes tenant schema vN cannot take traffic
# before vN is applied.
#
# WHY A SCRIPT AND NOT `sh -c '... && ...'` IN fly.toml. The runtime image is
# debian:trixie-slim, so a shell IS available and the inline form would work —
# but a file keeps the quoting out of TOML, gives the failure modes somewhere to
# be documented, and is covered by CI shellcheck.
#
# FAILURE SEMANTICS — FAIL-CLOSED BY DEFAULT.
# A tenant that cannot be migrated ABORTS THE DEPLOY. Shipping an image whose
# tenant tree could not be applied to a live tenant means shipping a guaranteed
# 503 for that org, so it is better caught before traffic routes. This is not a
# trap: the unblock path is a control-plane UPDATE, not a code change — set that
# org's `tenant_databases.status` to 'suspended' (the migrator selects only
# 'active'/'provisioning', see `routing::list_migratable`) and deploys flow
# again while you repair it. `TENANT_MIGRATE_RELEASE` below is the escape hatch
# for when even that is not possible.
#
# Env:
#   TENANT_MIGRATE_RELEASE  enforce (default) — a tenant failure aborts the deploy
#                           warn              — run the tenant tree, log loudly,
#                                               but let the deploy proceed. For
#                                               the "must ship the fix NOW and a
#                                               tenant is down" case. Affected
#                                               orgs stay 503 until repaired.
#                           skip              — do not touch the tenant tree at
#                                               all (control tree still runs).
#                           Set out-of-band and REMOVE it again:
#                             fly secrets set TENANT_MIGRATE_RELEASE=warn -a coralswarm-backend
#                             fly secrets unset TENANT_MIGRATE_RELEASE -a coralswarm-backend
#                           Deliberately not defaulted in fly.toml's [env] so
#                           there is exactly one place it can come from.
#
# Everything both binaries need (MIGRATOR_DATABASE_URL, and one TENANT_DB_<SLUG>
# per tenant) arrives as app-wide Fly secrets, which release machines receive.
# A routing row whose `db_secret_ref` secret was never set is therefore the one
# way to brick releases — see the fail-closed note above for the remedy.

set -eu

MODE="${TENANT_MIGRATE_RELEASE:-enforce}"
case "$MODE" in
    enforce | warn | skip) ;;
    *)
        echo "release: TENANT_MIGRATE_RELEASE='$MODE' is not one of enforce|warn|skip" >&2
        exit 2
        ;;
esac

# 1. CONTROL tree. Always fail-closed — `set -e` aborts here on non-zero, which
#    is byte-for-byte the behaviour this release command had before the tenant
#    tree was chained on.
echo "release: applying CONTROL tree (/app/migrate_control)"
/app/migrate_control

# 2. TENANT tree.
if [ "$MODE" = skip ]; then
    echo "release: TENANT tree SKIPPED (TENANT_MIGRATE_RELEASE=skip) — any org whose" \
        "schema_version is behind this build will 503 until migrate_tenants is run" >&2
    exit 0
fi

echo "release: applying TENANT tree (/app/migrate_tenants, mode=$MODE)"
rc=0
/app/migrate_tenants || rc=$?

if [ "$rc" -eq 0 ]; then
    echo "release: tenant tree applied — all selected tenants at head"
    exit 0
fi

# rc 1 = a tenant failed/drifted, 2 = usage/no DSN, 3 = a tenant was skipped
# because another orchestrator held its advisory lock (see migrate_tenants'
# exit-code table). All are non-zero for the same reason: this run did not
# establish `schema_version >= head_version()` for every selected tenant.
if [ "$MODE" = warn ]; then
    echo "release: migrate_tenants exited $rc — DEGRADED RELEASE, proceeding anyway because" \
        "TENANT_MIGRATE_RELEASE=warn. Affected orgs will 503 until their tenant tree is" \
        "applied. Unset TENANT_MIGRATE_RELEASE once repaired." >&2
    exit 0
fi

echo "release: migrate_tenants exited $rc — ABORTING the deploy (fail-closed). No traffic has" \
    "been routed to the new version. Fix the tenant, or set its tenant_databases.status to" \
    "'suspended' to take it out of the migrator's selection, then redeploy. Emergency" \
    "override: TENANT_MIGRATE_RELEASE=warn." >&2
exit "$rc"
