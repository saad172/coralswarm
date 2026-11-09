#!/usr/bin/env bash
#
# check_provisioning_privileges.sh — nobody but the table owner may write the
# self-serve provisioning tables directly, and the serving roles may not call
# the definers that write routing or advance a job.
#
# Control 0083 revokes table write on `tenant_databases` from `coralswarm_app`
# (an app-layer compromise must not be able to repoint an org's database), and
# control 0381 extends that to `tenant_provisioning_jobs` and
# `tenant_db_credentials`, and splits the routing-writing SECURITY DEFINER
# functions off to `coralswarm_provisioner` (the provisioning worker's own
# login, PROVISIONER_DATABASE_URL). One later `GRANT ... TO coralswarm_app`,
# a column-level `GRANT UPDATE (db_secret_ref)`, a grant to PUBLIC, or a
# GRANT of the provisioner role to an app role would quietly undo that. The
# migration asserts some of it once, when it runs; this guard asserts all of
# it on every deploy, against the tree about to ship.
#
# WHAT IT COVERS, AND WHAT IT DOES NOT: it builds a FRESH database from the
# control tree (in CI), so it catches a migration in the tree that loosens any
# of this. It never sees prod: a GRANT or `ALTER ROLE` run by hand on the
# prod control plane is invisible to it. The live `coralswarm_provisioner`
# role's attributes, and the worker login's (not the serving role, no
# UPDATE on tenant_databases, can call the claim definer), are checked by
# the worker's boot self-check (`provisioning::provisioner_self_check`),
# which refuses to start on a failure. Function bodies are matched as text:
# a write or call made through dynamic SQL (`EXECUTE format(...)`) is not
# detected by the definer-write or definer-call checks.
#
# What it does: applies the control tree to a fresh scratch database with
# sqlx (the same migrator the release runs), then lists every violation:
#   write <table> <role>       effective table-level INSERT/UPDATE/DELETE/
#                              TRUNCATE (has_table_privilege, so membership
#                              and PUBLIC count) for coralswarm_app,
#                              coralswarm_tenant_app, coralswarm_provisioner
#                              and PUBLIC
#   write <table>.<col> <priv> <role>
#                              effective column-level INSERT/UPDATE
#                              (has_column_privilege), same roles
#   acl <table>[.<col>] <priv> <grantee>
#                              any explicit write grant, table ACL or column
#                              ACL (pg_class.relacl / pg_attribute.attacl), to
#                              anyone but the table owner
#
# ONE ALLOWLISTED GRANT: control 0088 gives coralswarm_app column UPDATE on
# tenant_databases (slug, clerk_slug_updated_at, updated_at), so the Clerk
# organization.updated handler can reconcile a rename. Deliberately not
# db_secret_ref, account_id, clerk_org_id or status (0088's own comment).
# Exactly those three columns, that privilege, that grantee; anything else on
# those columns is still flagged.
#   execute <function> <role>  EXECUTE on a provisioner-only definer for
#                              coralswarm_app, coralswarm_tenant_app or PUBLIC
#   member <role> coralswarm_provisioner
#                              an app role can assume the provisioner role
#   definer-write <function> <role>
#                              ANY SECURITY DEFINER function in any schema
#                              (not just the ones listed below; named
#                              <schema>.<function> outside public) whose body
#                              INSERTs, UPDATEs or DELETEs one of the three
#                              tables and that coralswarm_app or PUBLIC can
#                              EXECUTE, except the six app-callable
#                              definers (enqueue, cancel, retry,
#                              mark_clerk_org_cleanup_done, and control
#                              0384's claim_clerk_org_cleanup and
#                              release_clerk_org_cleanup, in public). A
#                              new definer that writes routing and is granted
#                              to the app role is a way round the whole split,
#                              whatever it is called.
#   definer-call <function> <role>
#                              ANY SECURITY DEFINER function in any schema,
#                              other than the provisioner-only definers
#                              themselves, whose body names one of them and
#                              that coralswarm_app or PUBLIC can EXECUTE: a
#                              wrapper runs as its owner, so it would hand the
#                              app role a provisioner-only definer.
#   attr coralswarm_provisioner <attribute>
#                              the group role can log in, or holds
#                              superuser, createdb, createrole, replication or
#                              bypassrls (0381 asserts this once, when it
#                              runs; this re-checks it on the fresh CI
#                              database, so it catches a migration that sets
#                              one, not a prod `ALTER ROLE`; the boot
#                              self-check covers the live role)
#   missing-table / missing-function / missing-role
#                              the thing checked is gone (fails closed, so a
#                              rename cannot turn this guard into a no-op)
# Any line fails the run.
#
# Usage:
#   scripts/check_provisioning_privileges.sh              # apply control tree, check
#   scripts/check_provisioning_privileges.sh --self-test  # prove the check query
#   scripts/check_provisioning_privileges.sh --database URL
#                     # check one ALREADY-MIGRATED control database (no
#                     # scratch DB, no sqlx); the e2e stack runs this at boot
#                     # (e2e/stack/assert-db.mjs). DATABASE_URL is not needed.
#
# Env:
#   DATABASE_URL  localhost base connection with CREATE DATABASE and CREATE ROLE
#                 rights (e.g. postgres://postgres:postgres@localhost:5432/postgres).
#                 REQUIRED. The app roles are prepared on it first, as CI does.
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

# The worker's definers (control 0381): EXECUTE for coralswarm_provisioner
# only. Keep in sync with the migration's GRANT list. Control 0387 adds
# `org_deletion_delete_routing` (in-app org deletion deletes an ACTIVE routing
# row, which 0381's cleanup definer never does): the org-deletion worker's
# provisioner pool only, never the serving role.
PROVISIONER_ONLY="tenant_provisioning_store_credential tenant_provisioning_delete_credential \
tenant_provisioning_insert_routing tenant_provisioning_record_schema_version \
tenant_provisioning_activate tenant_provisioning_delete_routing tenant_provisioning_claim \
tenant_provisioning_set_step tenant_provisioning_defer tenant_provisioning_record_create_intent \
tenant_provisioning_clear_create_intent tenant_provisioning_record_database \
tenant_provisioning_record_credential_stored tenant_provisioning_fail \
org_deletion_delete_routing"

# The only definers that write the three tables and that the serving role may
# call (control 0381's app-role GRANTs, plus 0384's two Clerk-org cleanup
# claim definers for the A2 sweep; none writes routing or credentials, and
# none moves a job past `verify_org`). Any other writing definer it can
# EXECUTE is flagged, by body, not by name.
APP_DEFINERS="tenant_provisioning_enqueue tenant_provisioning_cancel \
tenant_provisioning_retry tenant_provisioning_mark_clerk_org_cleanup_done \
tenant_provisioning_claim_clerk_org_cleanup tenant_provisioning_release_clerk_org_cleanup"

# A body that writes one of the three tables: INSERT INTO / UPDATE / DELETE
# FROM, optionally ONLY, optionally (quoted) public-qualified, the name
# optionally quoted. Case-insensitive.
WRITES_RE='(insert[[:space:]]+into|update|delete[[:space:]]+from)[[:space:]]+(only[[:space:]]+)?("?public"?[[:space:]]*\.[[:space:]]*)?"?(tenant_databases|tenant_provisioning_jobs|tenant_db_credentials)"?([^a-z0-9_]|$)'

# The group role whose attributes are checked. Only the self-test overrides
# it (with a throwaway role), since the real one is cluster-wide and shared.
PROVISIONER_ROLE="${PROVISIONER_ROLE_UNDER_TEST:-coralswarm_provisioner}"

read -r -d '' CHECK_SQL <<'SQL' || true
WITH tbl(name) AS (
  VALUES ('tenant_databases'), ('tenant_provisioning_jobs'), ('tenant_db_credentials')
), rol(name) AS (
  VALUES ('coralswarm_app'), ('coralswarm_tenant_app'), ('coralswarm_provisioner'), ('public')
), serving(name) AS (
  VALUES ('coralswarm_app'), ('coralswarm_tenant_app'), ('public')
), allowed(tbl, col, priv, grantee) AS (
  VALUES ('tenant_databases', 'slug', 'UPDATE', 'coralswarm_app'),
         ('tenant_databases', 'clerk_slug_updated_at', 'UPDATE', 'coralswarm_app'),
         ('tenant_databases', 'updated_at', 'UPDATE', 'coralswarm_app')
), fn(name) AS (
  SELECT pg_catalog.unnest(pg_catalog.string_to_array(:'provisioner_only', ' '))
), app_fn(name) AS (
  SELECT pg_catalog.unnest(pg_catalog.string_to_array(:'app_definers', ' '))
), app_callers(name) AS (
  VALUES ('coralswarm_app'), ('public')
), definer AS (
  -- Every SECURITY DEFINER function or procedure in any schema, with its full
  -- definition (pg_get_functiondef covers plpgsql, sql and BEGIN ATOMIC
  -- bodies). `label` is the bare name in public, schema-qualified elsewhere.
  SELECT pr.oid, pr.proname, n.nspname,
         CASE n.nspname WHEN 'public' THEN pr.proname::text ELSE n.nspname || '.' || pr.proname END AS label,
         pg_catalog.pg_get_functiondef(pr.oid) AS def
    FROM pg_catalog.pg_proc pr
    JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace
   WHERE pr.prosecdef AND pr.prokind IN ('f', 'p')
), writer AS (
  SELECT d.oid, d.proname, d.nspname, d.label FROM definer d
   WHERE d.def ~* :'writes_re'
), calls_re(re) AS (
  -- Any provisioner-only definer's name as a whole identifier.
  SELECT '(^|[^a-z0-9_$])(' || pg_catalog.string_agg(f.name, '|') || ')([^a-z0-9_$]|$)' FROM fn f
), caller AS (
  SELECT d.oid, d.label FROM definer d CROSS JOIN calls_re c
   WHERE d.def ~* c.re
     AND NOT (d.nspname = 'public' AND d.proname IN (SELECT f.name FROM fn f))
), rel AS (
  SELECT t.name, pg_catalog.to_regclass('public.' || t.name) AS oid FROM tbl t
), present AS (
  SELECT r.name, r.oid, c.relowner, c.relacl FROM rel r JOIN pg_catalog.pg_class c ON c.oid = r.oid
), col AS (
  SELECT p.name, p.oid, p.relowner, att.attname, att.attnum, att.attacl
    FROM present p
    JOIN pg_catalog.pg_attribute att ON att.attrelid = p.oid AND att.attnum > 0 AND NOT att.attisdropped
), priv(name) AS (
  VALUES ('INSERT'), ('UPDATE')
), live_rol AS (
  SELECT ro.name FROM rol ro WHERE ro.name = 'public' OR pg_catalog.to_regrole(ro.name) IS NOT NULL
), v(line) AS (
  SELECT 'missing-table ' || r.name FROM rel r WHERE r.oid IS NULL
  UNION ALL
  SELECT 'missing-role ' || ro.name FROM rol ro
   WHERE ro.name <> 'public' AND pg_catalog.to_regrole(ro.name) IS NULL
  UNION ALL
  SELECT 'write ' || p.name || ' ' || CASE ro.name WHEN 'public' THEN 'PUBLIC' ELSE ro.name END
    FROM present p CROSS JOIN live_rol ro
   WHERE pg_catalog.has_table_privilege(ro.name, p.oid, 'INSERT,UPDATE,DELETE,TRUNCATE')
  UNION ALL
  SELECT 'write ' || c.name || '.' || c.attname || ' ' || pv.name || ' '
         || CASE ro.name WHEN 'public' THEN 'PUBLIC' ELSE ro.name END
    FROM col c CROSS JOIN live_rol ro CROSS JOIN priv pv
   WHERE pg_catalog.has_column_privilege(ro.name, c.oid, c.attnum, pv.name)
     AND NOT pg_catalog.has_table_privilege(ro.name, c.oid, pv.name)
     AND NOT EXISTS (SELECT 1 FROM allowed al
                      WHERE al.tbl = c.name AND al.col = c.attname AND al.priv = pv.name AND al.grantee = ro.name)
  UNION ALL
  SELECT 'acl ' || p.name || ' ' || a.privilege_type || ' '
         || CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(a.grantee) END
    FROM present p, pg_catalog.aclexplode(p.relacl) a
   WHERE a.privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE') AND a.grantee <> p.relowner
  UNION ALL
  SELECT 'acl ' || c.name || '.' || c.attname || ' ' || a.privilege_type || ' '
         || CASE a.grantee WHEN 0 THEN 'PUBLIC' ELSE pg_catalog.pg_get_userbyid(a.grantee) END
    FROM col c
    CROSS JOIN LATERAL pg_catalog.aclexplode(c.attacl) a
   WHERE a.privilege_type IN ('INSERT', 'UPDATE') AND a.grantee <> c.relowner
     AND NOT EXISTS (SELECT 1 FROM allowed al
                      WHERE al.tbl = c.name AND al.col = c.attname AND al.priv = a.privilege_type
                        AND a.grantee <> 0 AND al.grantee = pg_catalog.pg_get_userbyid(a.grantee))
  UNION ALL
  SELECT 'missing-function ' || f.name FROM fn f
   WHERE NOT EXISTS (SELECT 1 FROM pg_catalog.pg_proc pr JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace
                      WHERE n.nspname = 'public' AND pr.proname = f.name)
  UNION ALL
  SELECT 'execute ' || pr.proname || ' ' || CASE s.name WHEN 'public' THEN 'PUBLIC' ELSE s.name END
    FROM fn f
    JOIN pg_catalog.pg_proc pr ON pr.proname = f.name
    JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace AND n.nspname = 'public'
    CROSS JOIN serving s
   WHERE (s.name = 'public' OR pg_catalog.to_regrole(s.name) IS NOT NULL)
     AND pg_catalog.has_function_privilege(s.name, pr.oid, 'EXECUTE')
  UNION ALL
  SELECT 'member ' || s.name || ' coralswarm_provisioner' FROM serving s
   WHERE s.name <> 'public' AND pg_catalog.to_regrole(s.name) IS NOT NULL
     AND pg_catalog.to_regrole('coralswarm_provisioner') IS NOT NULL
     AND pg_catalog.pg_has_role(s.name, 'coralswarm_provisioner', 'MEMBER')
  UNION ALL
  SELECT 'definer-write ' || w.label || ' ' || CASE c.name WHEN 'public' THEN 'PUBLIC' ELSE c.name END
    FROM writer w CROSS JOIN app_callers c
   WHERE (c.name = 'public' OR pg_catalog.to_regrole(c.name) IS NOT NULL)
     AND pg_catalog.has_function_privilege(c.name, w.oid, 'EXECUTE')
     AND NOT (w.nspname = 'public' AND EXISTS (SELECT 1 FROM app_fn a WHERE a.name = w.proname))
  UNION ALL
  SELECT 'definer-call ' || k.label || ' ' || CASE c.name WHEN 'public' THEN 'PUBLIC' ELSE c.name END
    FROM caller k CROSS JOIN app_callers c
   WHERE (c.name = 'public' OR pg_catalog.to_regrole(c.name) IS NOT NULL)
     AND pg_catalog.has_function_privilege(c.name, k.oid, 'EXECUTE')
  UNION ALL
  SELECT 'attr ' || r.rolname || ' ' || a.attr
    FROM pg_catalog.pg_roles r
    CROSS JOIN LATERAL (VALUES ('login', r.rolcanlogin), ('superuser', r.rolsuper),
                               ('createdb', r.rolcreatedb), ('createrole', r.rolcreaterole),
                               ('replication', r.rolreplication), ('bypassrls', r.rolbypassrls)) a(attr, held)
   WHERE r.rolname = :'provisioner_role' AND a.held
)
SELECT DISTINCT line FROM v ORDER BY 1;
SQL

scratch_url() { echo "$DATABASE_URL" | sed -E "s#(://[^/]+/)[^?]*#\1$1#"; }
q() { "$PSQL" "$1" -X -v ON_ERROR_STOP=1 -qAt -c "$2"; }
run_check() {
  "$PSQL" "$1" -X -v ON_ERROR_STOP=1 -qAt -v provisioner_only="$PROVISIONER_ONLY" \
    -v app_definers="$APP_DEFINERS" -v provisioner_role="${2:-$PROVISIONER_ROLE}" \
    -v writes_re="$WRITES_RE" <<< "$CHECK_SQL"
}
# How many SECURITY DEFINER functions in public write the three tables: the
# body match must find the real tree's definers, or the check is vacuous.
count_writers() {
  "$PSQL" "$1" -X -v ON_ERROR_STOP=1 -qAt -v writes_re="$WRITES_RE" <<'SQL'
SELECT count(*) FROM pg_catalog.pg_proc pr
  JOIN pg_catalog.pg_namespace n ON n.oid = pr.pronamespace AND n.nspname = 'public'
 WHERE pr.prosecdef AND pr.prokind IN ('f', 'p') AND pg_catalog.pg_get_functiondef(pr.oid) ~* :'writes_re';
SQL
}

SCRATCH_DBS=()
SCRATCH_ROLES=()
cleanup() {
  local db role
  for db in ${SCRATCH_DBS[@]+"${SCRATCH_DBS[@]}"}; do
    "$PSQL" "$DATABASE_URL" -X -qc "DROP DATABASE IF EXISTS $db WITH (FORCE);" >/dev/null 2>&1 || true
  done
  for role in ${SCRATCH_ROLES[@]+"${SCRATCH_ROLES[@]}"}; do
    "$PSQL" "$DATABASE_URL" -X -qc "DROP ROLE IF EXISTS $role;" >/dev/null 2>&1 || true
  done
}
trap cleanup EXIT

new_db() {
  local db="$1"
  SCRATCH_DBS+=("$db")
  PGOPTIONS='-c client_min_messages=warning' q "$DATABASE_URL" "DROP DATABASE IF EXISTS $db;" >/dev/null
  q "$DATABASE_URL" "CREATE DATABASE $db;" >/dev/null
}

# The provisioner role is cluster-wide and created by control 0381; the
# self-test needs it before any migration ran, with the same attributes.
ensure_provisioner_role() {
  q "$DATABASE_URL" "DO \$\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = 'coralswarm_provisioner') THEN CREATE ROLE coralswarm_provisioner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS; END IF; END \$\$;" >/dev/null
}

self_test() {
  local db="prov_priv_selftest_$$" url got want fns=""
  bash scripts/prepare_ci_pg_roles.sh --localhost
  ensure_provisioner_role
  new_db "$db"
  url="$(scratch_url "$db")"
  for f in $PROVISIONER_ONLY; do
    fns+="CREATE FUNCTION public.$f() RETURNS int LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS 'SELECT 1';
REVOKE ALL ON FUNCTION public.$f() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.$f() TO coralswarm_provisioner;
"
  done
  "$PSQL" "$url" -X -v ON_ERROR_STOP=1 -q >/dev/null <<SQL
CREATE TABLE public.tenant_databases (id int, slug text, clerk_slug_updated_at timestamptz,
                                      updated_at timestamptz, db_secret_ref text, status text);
CREATE TABLE public.tenant_provisioning_jobs (id int, step text);
CREATE TABLE public.tenant_db_credentials (id int, secret_ref text);
GRANT SELECT ON public.tenant_databases, public.tenant_provisioning_jobs, public.tenant_db_credentials
  TO coralswarm_app, coralswarm_provisioner;
-- 0088's one allowlisted grant: must pass.
GRANT UPDATE (slug, clerk_slug_updated_at, updated_at) ON public.tenant_databases TO coralswarm_app;
$fns
-- The four app-callable definers write the job table and are the app's: must pass.
CREATE FUNCTION public.tenant_provisioning_enqueue(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'INSERT INTO public.tenant_provisioning_jobs (step) VALUES (p)';
CREATE FUNCTION public.tenant_provisioning_cancel(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE public.tenant_provisioning_jobs SET step = p';
CREATE FUNCTION public.tenant_provisioning_retry(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE tenant_provisioning_jobs SET step = p';
CREATE FUNCTION public.tenant_provisioning_mark_clerk_org_cleanup_done(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE public.tenant_provisioning_jobs SET step = p';
REVOKE ALL ON FUNCTION public.tenant_provisioning_enqueue(text), public.tenant_provisioning_cancel(text),
  public.tenant_provisioning_retry(text), public.tenant_provisioning_mark_clerk_org_cleanup_done(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.tenant_provisioning_enqueue(text), public.tenant_provisioning_cancel(text),
  public.tenant_provisioning_retry(text), public.tenant_provisioning_mark_clerk_org_cleanup_done(text) TO coralswarm_app;
-- A writing definer only the provisioner may call, and a writing INVOKER
-- function the app may call (it runs with the app's own, absent, write
-- privilege): both must pass.
CREATE FUNCTION public.tenant_routing_worker_only(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE public.tenant_databases SET status = p';
REVOKE ALL ON FUNCTION public.tenant_routing_worker_only(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.tenant_routing_worker_only(text) TO coralswarm_provisioner;
CREATE FUNCTION public.tenant_routing_invoker(p text) RETURNS void LANGUAGE sql
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE public.tenant_databases SET status = p';
GRANT EXECUTE ON FUNCTION public.tenant_routing_invoker(text) TO coralswarm_app;
-- A definer wrapping a provisioner-only definer, callable only by the
-- provisioner: must pass.
CREATE FUNCTION public.tenant_claim_wrapper_worker_only() RETURNS int LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'SELECT public.tenant_provisioning_claim()';
REVOKE ALL ON FUNCTION public.tenant_claim_wrapper_worker_only() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.tenant_claim_wrapper_worker_only() TO coralswarm_provisioner;
-- A definer that only READS routing, callable by the app: must pass.
CREATE FUNCTION public.tenant_routing_peek(p text) RETURNS bigint LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'SELECT count(*) FROM public.tenant_databases WHERE slug = p';
GRANT EXECUTE ON FUNCTION public.tenant_routing_peek(text) TO coralswarm_app;
SQL
  got="$(run_check "$url")"
  if [ -n "$got" ]; then
    echo "provisioning privilege guard self-test: FAILED (a clean fixture was flagged)" >&2
    printf '  %s\n' "$got" >&2
    exit 1
  fi
  "$PSQL" "$url" -X -v ON_ERROR_STOP=1 -q >/dev/null <<'SQL'
GRANT UPDATE (db_secret_ref) ON public.tenant_databases TO coralswarm_app;
GRANT UPDATE (slug) ON public.tenant_databases TO coralswarm_tenant_app;
GRANT INSERT (slug) ON public.tenant_databases TO coralswarm_app;
GRANT INSERT ON public.tenant_db_credentials TO PUBLIC;
GRANT DELETE ON public.tenant_provisioning_jobs TO coralswarm_provisioner;
GRANT EXECUTE ON FUNCTION public.tenant_provisioning_insert_routing() TO coralswarm_app;
GRANT EXECUTE ON FUNCTION public.tenant_provisioning_claim() TO PUBLIC;
DROP FUNCTION public.tenant_provisioning_activate();
-- A definer that is in no list, named nothing like the others, that forces a
-- routing row live and is granted to the serving role.
CREATE FUNCTION public.tenant_routing_force(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS $f$UPDATE tenant_databases SET status='active' WHERE slug = p$f$;
REVOKE ALL ON FUNCTION public.tenant_routing_force(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.tenant_routing_force(text) TO coralswarm_app;
-- plpgsql, a quoted, schema-qualified name, and PUBLIC's default EXECUTE.
CREATE FUNCTION public.credential_purge(p text) RETURNS void LANGUAGE plpgsql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS $f$
BEGIN
  DELETE FROM "public"."tenant_db_credentials" WHERE secret_ref = p;
END $f$;
-- Wrappers: a definer outside public that calls a provisioner-only definer
-- and is granted to the serving role (it runs as its owner, so the app
-- could claim jobs through it), and a plpgsql one in public left on
-- PUBLIC's default EXECUTE that calls one by its quoted name.
CREATE SCHEMA ops;
CREATE FUNCTION ops.kick_claim() RETURNS int LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'SELECT public.tenant_provisioning_claim()';
REVOKE ALL ON FUNCTION ops.kick_claim() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.kick_claim() TO coralswarm_app;
CREATE FUNCTION public.routing_nudge() RETURNS void LANGUAGE plpgsql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS $f$
BEGIN
  PERFORM "tenant_provisioning_insert_routing"();
END $f$;
-- A routing-writing definer outside public, granted to the serving role.
CREATE FUNCTION ops.reroute(p text) RETURNS void LANGUAGE sql SECURITY DEFINER
  SET search_path = pg_catalog, public, pg_temp AS 'UPDATE public.tenant_databases SET status = p';
REVOKE ALL ON FUNCTION ops.reroute(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION ops.reroute(text) TO coralswarm_app;
SQL
  got="$(run_check "$url" | tr '\n' ';')"
  want="acl tenant_databases.db_secret_ref UPDATE coralswarm_app;acl tenant_db_credentials INSERT PUBLIC;"
  want+="acl tenant_provisioning_jobs DELETE coralswarm_provisioner;"
  want+="execute tenant_provisioning_claim PUBLIC;execute tenant_provisioning_claim coralswarm_app;"
  want+="execute tenant_provisioning_claim coralswarm_tenant_app;execute tenant_provisioning_insert_routing coralswarm_app;"
  want+="missing-function tenant_provisioning_activate;"
  want+="acl tenant_databases.slug UPDATE coralswarm_tenant_app;acl tenant_databases.slug INSERT coralswarm_app;"
  want+="write tenant_databases.db_secret_ref UPDATE coralswarm_app;write tenant_databases.slug UPDATE coralswarm_tenant_app;"
  want+="write tenant_databases.slug INSERT coralswarm_app;write tenant_db_credentials PUBLIC;"
  want+="write tenant_db_credentials coralswarm_app;write tenant_db_credentials coralswarm_provisioner;"
  want+="write tenant_db_credentials coralswarm_tenant_app;write tenant_provisioning_jobs coralswarm_provisioner;"
  want+="definer-write tenant_routing_force coralswarm_app;"
  want+="definer-write credential_purge PUBLIC;definer-write credential_purge coralswarm_app;"
  want+="definer-write ops.reroute coralswarm_app;"
  want+="definer-call ops.kick_claim coralswarm_app;"
  want+="definer-call routing_nudge PUBLIC;definer-call routing_nudge coralswarm_app;"
  # Compare as sorted sets (collation-independent).
  got="$(printf '%s' "$got" | tr ';' '\n' | LC_ALL=C sort | tr '\n' ';')"
  want="$(printf '%s' "$want" | tr ';' '\n' | LC_ALL=C sort | tr '\n' ';')"
  if [ "$got" != "$want" ]; then
    echo "provisioning privilege guard self-test: FAILED" >&2
    echo "  want: $want" >&2
    echo "  got:  $got" >&2
    exit 1
  fi
  # Role attributes: checked against a throwaway LOGIN role (never the
  # shared, cluster-wide coralswarm_provisioner).
  local role="prov_priv_selftest_role_$$"
  SCRATCH_ROLES+=("$role")
  PGOPTIONS='-c client_min_messages=warning' q "$DATABASE_URL" "DROP ROLE IF EXISTS $role; CREATE ROLE $role LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS;" >/dev/null
  got="$(run_check "$url" "$role" | grep '^attr ' || true)"
  if [ "$got" != "attr $role login" ]; then
    echo "provisioning privilege guard self-test: FAILED (a LOGIN group role was not flagged)" >&2
    echo "  got: $got" >&2
    exit 1
  fi
  echo "provisioning privilege guard self-test: OK (clean fixture with 0088's allowlisted grant and the four app definers passes; column, PUBLIC, provisioner and EXECUTE grants, a missing definer, unlisted writing definers callable by the app or PUBLIC in any schema, wrapper definers calling a provisioner-only definer, and a LOGIN group role are all flagged)"
}

if [ "${1:-}" = "--self-test" ]; then
  self_test
  exit 0
fi

# Both modes end in the same two checks.
verdict() {
  local url="$1" hits writers
  hits="$(run_check "$url")"
  if [ -n "$hits" ]; then
    echo "FAIL: provisioning tables / definers grant more than control 0381 allows:" >&2
    while IFS= read -r line; do echo "  $line" >&2; done <<< "$hits"
    echo "Only the table owner writes tenant_databases, tenant_provisioning_jobs and tenant_db_credentials;" >&2
    echo "the worker's definers are coralswarm_provisioner's alone. Revoke the grant (see control 0381)." >&2
    echo "provisioning privilege guard: FAILED" >&2
    exit 1
  fi
  writers="$(count_writers "$url")"
  # The 15 worker definers (14 from 0381, 0387's org_deletion_delete_routing)
  # and 4 app definers all write: far fewer matches means the body pattern
  # stopped matching (fail closed).
  if [ "$writers" -lt 19 ]; then
    echo "FAIL: only $writers SECURITY DEFINER functions in public matched the table-write pattern (expected at least 19); the definer-write check would be vacuous" >&2
    echo "provisioning privilege guard: FAILED" >&2
    exit 1
  fi
  echo "provisioning privilege guard: OK (3 tables, $(wc -w <<< "$PROVISIONER_ONLY" | tr -d ' ') provisioner-only definers, $writers table-writing definers checked)"
}

if [ "${1:-}" = "--database" ]; then
  [ -n "${2:-}" ] || { echo "usage: $0 --database URL" >&2; exit 2; }
  verdict "$2"
  exit 0
fi

bash scripts/prepare_ci_pg_roles.sh --localhost

db="prov_priv_ctl_$$"
new_db "$db"
url="$(scratch_url "$db")"
echo "provisioning privileges: applying control tree -> $db"
DATABASE_URL="$url" "$SQLX" migrate run --source backend/migrations_control >/dev/null
verdict "$url"
