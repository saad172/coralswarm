#!/usr/bin/env node
// Boot-time structural assertions: the tenant-isolation invariants that live
// in the database's shape and in how the binaries treat it, which no HTTP
// request can observe. `up.mjs` runs these once per boot, after the release
// migrations and before the backend starts; any failure fails the boot.
//
// Each check names the ci.yml job it replaces (CI redesign decision 8). The
// HTTP half of the same invariants is in tests/isolation-*.spec.ts, and that
// half is only meaningful because of check 1: the backend really serves as
// a NOBYPASSRLS role.
//
//   1 serving-role        consumer-rls / rls-probe-harness / migrator-dsn-split
//                         (requests are served as coralswarm_app; see the check)
//   2 rls-strict          consumer-rls (no `missing_ok`: unscoped is an error)
//   3 rls-coverage        consumer-rls (roster) / check_rls_coverage.sh's intent
//   4 definer-search-path definer-search-path (control + tenant)
//   5 provisioning-privs  definer-search-path job's provisioning guard
//   6 migrator-split      migrator-dsn-split (legacy/broken/stale)
//   7 tenant-owner        tenant-role-provisioning
//
// Usage: node stack/assert-db.mjs   (against a stack `up.mjs` already booted)

import { spawn, spawnSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { BINS, CONTAINERS, CONTROL_DB, FAKE, PORTS, REPO, STATE_DIR, backendEnv } from "./config.mjs";

// Container-side URLs: the guard scripts run psql inside the container.
const inContainer = (user, db) => `postgres://${user}@127.0.0.1:5432/${db}`;
const hostUrl = (user, db) => `postgres://${user}@127.0.0.1:${PORTS.pg}/${db}`;

const PROBE = {
  control: "cs_e2e_probe_ctl",
  tenant: "cs_e2e_probe_tenant",
  stale: "cs_e2e_probe_stale",
  empty: "cs_e2e_probe_empty",
  // A Neon project owner: LOGIN CREATEDB CREATEROLE, never SUPERUSER,
  // BYPASSRLS or REPLICATION (tests/tenant_rls_role_provisioning_pg.rs).
  owner: "cs_e2e_neon_owner",
  org: "org_e2e_tenant_probe",
  slug: "e2e-tenant-probe",
  secretRef: "TENANT_DB_E2E_PROBE",
};

/**
 * Tables on the control plane that carry an `account_id` or `graph_id` column
 * and have NO row-level security, at the time this check was written
 * (2026-10-28). A ratchet, not an endorsement: each is either control-plane
 * bookkeeping the serving path reads by a key it already proved, or a gap
 * nobody has closed. The check fails when this set changes in EITHER
 * direction: a new account- or graph-keyed table without RLS has to be added
 * here in review (and reviewed as a gap), and a table that gains RLS has to
 * leave the list, so the list never overstates what is unprotected.
 */
export const RLS_EXEMPT_SCOPED_TABLES = [
  "account_members", "account_prefs", "addin_microsoft_links", "addin_microsoft_sessions", "addin_pairing_codes",
  "addin_tokens", "calendar_prefs", "cold_start_notices", "connector_installs",
  "escrow_recovery_log", "escrow_recovery_pubkey", "escrow_shares", "escrow_wrapped_keys", "github_installations",
  "graph_mail_routes", "jobs", "member_ocean_desired_state", "member_ocean_lifecycle_log", "microsoft_admin_consents",
  "node_case_fold_log", "org_selfserve_creations", "org_trial_claims", "slack_member_channels", "teams_container_watchers",
  "tenant_databases", "tenant_provisioning_jobs", "user_account_prefs", "user_active_graph",
];

function psql(sql, { db = "postgres", user = "postgres" } = {}) {
  const r = spawnSync("docker", ["exec", "-i", CONTAINERS.pg, "psql", "-U", user, "-d", db, "-X", "-q", "-t", "-A", "-v", "ON_ERROR_STOP=1"], {
    input: sql,
    encoding: "utf8",
  });
  return { ok: r.status === 0, out: (r.stdout ?? "").trim(), err: (r.stderr ?? "").trim() };
}

function must(sql, opts) {
  const r = psql(sql, opts);
  if (!r.ok) throw new Error(`psql (${opts?.user ?? "postgres"}@${opts?.db ?? "postgres"}) failed: ${r.err}\n${sql}`);
  return r.out;
}

function run(bin, argv, env, timeoutMs = 60_000) {
  const clean = { ...process.env };
  for (const k of ["DATABASE_URL", "MIGRATOR_DATABASE_URL"]) delete clean[k];
  const r = spawnSync(bin, argv, { env: { ...clean, ...env }, encoding: "utf8", timeout: timeoutMs, cwd: REPO });
  return { status: r.status, out: `${r.stdout ?? ""}${r.stderr ?? ""}`, timedOut: r.error?.code === "ETIMEDOUT" };
}

function bashGuard(script, argv, env = {}) {
  const r = spawnSync("bash", [path.join("scripts", script), ...argv], {
    cwd: REPO,
    encoding: "utf8",
    env: { ...process.env, PSQL: "e2e/stack/psql-in-container.sh", E2E_PG_CONTAINER: CONTAINERS.pg, ...env },
  });
  if (r.status !== 0) throw new Error(`${script} ${argv.join(" ")} failed (exit ${r.status}):\n${r.stdout ?? ""}${r.stderr ?? ""}`);
  // The verdict line; psql NOTICEs go to stderr.
  return (r.stdout ?? "").trim().split("\n").pop();
}

/**
 * Boot the (new) backend against `db` and require that it REFUSES to serve:
 * exits non-zero, within the timeout, with a diagnosis naming `expect` and
 * the step to run. A backend that is still up when the timeout hits served a
 * database it should not have.
 */
async function expectBootRefused(backendExe, db, expect, timeoutMs = 60_000) {
  const env = {
    ...backendEnv(),
    DATABASE_URL: hostUrl("postgres", db),
    // Never the running stack's ports, in case it gets as far as binding.
    PORT: String(PORTS.api + 50),
    INTERNAL_PORT: String(PORTS.internal + 50),
  };
  const inherited = { ...process.env };
  delete inherited.MIGRATOR_DATABASE_URL;
  const child = spawn(backendExe, [], { env: { ...inherited, ...env }, cwd: REPO, windowsHide: true });
  let out = "";
  child.stdout.on("data", (d) => (out += d));
  child.stderr.on("data", (d) => (out += d));
  const code = await new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve("timeout");
    }, timeoutMs);
    child.on("exit", (c) => {
      clearTimeout(timer);
      resolve(c);
    });
  });
  if (code === "timeout") throw new Error(`backend kept running against ${db} (${expect}) for ${timeoutMs / 1000}s; it must refuse to serve\n${out.slice(-2000)}`);
  if (process.env.E2E_ASSERT_DEBUG) console.log(`--- ${db} (exit ${code}) ---\n${out.slice(-600)}`);
  if (code === 0) throw new Error(`backend exited 0 against ${db}; it must refuse (${expect})\n${out.slice(-2000)}`);
  for (const needle of [expect, "migrate_control"]) {
    if (!out.includes(needle)) throw new Error(`backend refused ${db} but its diagnosis does not name "${needle}":\n${out.slice(-2000)}`);
  }
  return code;
}

function dropProbes() {
  for (const db of Object.values(PROBE).filter((v) => v.startsWith("cs_e2e_probe_"))) must(`DROP DATABASE IF EXISTS ${db} WITH (FORCE)`);
}

export async function assertDatabases({ template, backendExe = BINS.backend, log = (m) => console.log(`[e2e assert-db] ${m}`) }) {
  const failures = [];
  const check = async (name, fn) => {
    const t = Date.now();
    try {
      const detail = await fn();
      log(`ok   ${name} (${((Date.now() - t) / 1000).toFixed(1)}s)${detail ? `: ${detail}` : ""}`);
    } catch (e) {
      failures.push(`${name}: ${e.message}`);
      log(`FAIL ${name}: ${e.message.split("\n")[0]}`);
    }
  };
  dropProbes();

  // 1. The serving DSN is the least-privilege app role, so every HTTP
  //    isolation scenario runs with RLS enforced (the #700 lesson: as the
  //    owner, every policy passes vacuously), and the server holds no
  //    migrator DSN (migrations are out-of-band, OPS-4).
  //
  //    The claim is "every request is served as coralswarm_app", not "the
  //    process holds no other credential": the in-process provisioning
  //    workers hold two more, as in production. PROVISIONER_DATABASE_URL is
  //    coralswarm_provisioner's login (control 0381), and
  //    TENANT_PROVISIONER_ADMIN_DSN is the local provisioner's stand-in for
  //    Neon's project API (CREATE DATABASE). Checked here: no other DSN is
  //    present, the provisioner login is not privileged and is not the app
  //    role, and only those two workers (plus the module that defines the
  //    provisioner variable's name) read either variable.
  await check("serving-role", () => {
    const env = backendEnv();
    const dsnRole = (k) => new URL(env[k]).username;
    const dsns = Object.keys(env).filter((k) => /^postgres(ql)?:\/\//.test(env[k] ?? ""));
    const expected = { DATABASE_URL: "coralswarm_app", PROVISIONER_DATABASE_URL: "cs_e2e_provisioner", TENANT_PROVISIONER_ADMIN_DSN: "postgres" };
    const unexpected = dsns.filter((k) => !(k in expected));
    if (unexpected.length) throw new Error(`backend env carries DSNs beyond the serving role and the provisioning workers': ${unexpected.join(", ")}`);
    for (const [k, role] of Object.entries(expected)) {
      if (!dsns.includes(k) || dsnRole(k) !== role) throw new Error(`backend ${k} must connect as ${role} (got ${env[k] ? dsnRole(k) : "unset"})`);
    }
    // Who reads the two worker-only variables. A route or the serving pool
    // reading either would void the claim above.
    const readers = [];
    const walk = (dir) => {
      for (const e of fs.readdirSync(dir, { withFileTypes: true })) {
        const p = path.join(dir, e.name);
        if (e.isDirectory()) walk(p);
        else if (e.name.endsWith(".rs") && /TENANT_PROVISIONER_ADMIN_DSN|\bPROVISIONER_DATABASE_URL\b/.test(fs.readFileSync(p, "utf8"))) readers.push(path.relative(path.join(REPO, "backend", "src"), p).split(path.sep).join("/"));
      }
    };
    walk(path.join(REPO, "backend", "src"));
    const allowedReaders = ["workers/tenant_provisioning.rs", "workers/org_deletion.rs", "tenancy/provisioning.rs", "services/org_deletion.rs"];
    const strays = readers.filter((r) => !allowedReaders.includes(r));
    if (strays.length) throw new Error(`files outside the provisioning workers reference the provisioning DSNs: ${strays.join(", ")}`);

    // Role attributes, read by the app role in its own session (so
    // row_security is the value its connections actually get).
    const attrs = must(
      `SELECT current_user, r.rolcanlogin, r.rolsuper, r.rolbypassrls, r.rolcreaterole, r.rolcreatedb, r.rolreplication,
              (SELECT count(*) FROM pg_class WHERE relowner = r.oid),
              pg_has_role(r.oid, (SELECT relowner FROM pg_class WHERE oid = 'public.graphs'::regclass), 'MEMBER'),
              current_setting('row_security')
         FROM pg_roles r WHERE r.rolname = current_user`,
      { db: CONTROL_DB, user: "coralswarm_app" },
    );
    if (attrs !== "coralswarm_app|t|f|f|f|f|f|0|f|on") {
      throw new Error(`coralswarm_app must be LOGIN NOSUPERUSER NOBYPASSRLS NOCREATEROLE NOCREATEDB NOREPLICATION, own nothing, not be a member of the table owner, and run with row_security=on (got ${attrs})`);
    }
    const prov = must(
      `SELECT r.rolsuper, r.rolbypassrls, pg_has_role(r.oid, 'coralswarm_app', 'MEMBER'), pg_has_role('coralswarm_app', r.oid, 'MEMBER')
         FROM pg_roles r WHERE r.rolname = 'cs_e2e_provisioner'`,
      { db: CONTROL_DB },
    );
    if (prov !== "f|f|f|f") throw new Error(`the provisioner login must be NOSUPERUSER NOBYPASSRLS and share no membership with coralswarm_app (got ${prov})`);
    // It cannot repoint an org's database or migrate the schema.
    const denied = psql("UPDATE tenant_databases SET status = status WHERE false", { db: CONTROL_DB, user: "coralswarm_app" });
    if (denied.ok) throw new Error("coralswarm_app can UPDATE tenant_databases (control 0083 revokes it)");
    const ddl = psql("CREATE TABLE e2e_probe_ddl (x int)", { db: CONTROL_DB, user: "coralswarm_app" });
    if (ddl.ok) throw new Error("coralswarm_app can CREATE TABLE in public");
    return `serving pool coralswarm_app (NOBYPASSRLS, row_security on, owns 0 relations), no migrator DSN; provisioner DSNs read only by ${allowedReaders.slice(0, 2).join(", ")}`;
  });

  // 2. The consumer policies use `current_setting('app.account_id')` with no
  //    `missing_ok`: a query that forgot to scope itself is a hard error, not
  //    a quietly empty (or quietly complete) result.
  await check("rls-strict", () => {
    const unscoped = psql("SELECT count(*) FROM graphs", { db: CONTROL_DB, user: "coralswarm_app" });
    if (unscoped.ok || !/app\.account_id/.test(unscoped.err)) {
      throw new Error(`an unscoped read of graphs as coralswarm_app must error on app.account_id (got ${unscoped.ok ? `rows: ${unscoped.out}` : unscoped.err})`);
    }
    const scoped = must(
      "BEGIN; SELECT set_config('app.account_id', gen_random_uuid()::text, true) IS NOT NULL; SELECT count(*) FROM graphs; SELECT count(*) FROM atoms; COMMIT;",
      { db: CONTROL_DB, user: "coralswarm_app" },
    );
    if (scoped.split("\n").slice(1).join(",") !== "0,0") throw new Error(`a scope naming no account saw rows: ${scoped}`);
    return "unscoped read errors; a foreign scope sees nothing";
  });

  // 3. Every account- or graph-keyed table has RLS, except the reviewed list.
  await check("rls-coverage", () => {
    const unprotected = must(
      `SELECT c.relname FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = 'public' AND c.relkind IN ('r', 'p') AND NOT c.relrowsecurity
          AND EXISTS (SELECT 1 FROM pg_attribute a WHERE a.attrelid = c.oid AND NOT a.attisdropped AND a.attname IN ('account_id', 'graph_id'))
        ORDER BY 1`,
      { db: CONTROL_DB },
    ).split("\n").filter(Boolean);
    const allowed = new Set(RLS_EXEMPT_SCOPED_TABLES);
    const added = unprotected.filter((t) => !allowed.has(t));
    const gained = RLS_EXEMPT_SCOPED_TABLES.filter((t) => !unprotected.includes(t));
    if (added.length) throw new Error(`account/graph-keyed tables without row-level security: ${added.join(", ")} (enable RLS with an account_isolation policy, or add them to RLS_EXEMPT_SCOPED_TABLES in e2e/stack/assert-db.mjs with a reviewer's sign-off)`);
    if (gained.length) throw new Error(`these now have RLS (or are gone); remove them from RLS_EXEMPT_SCOPED_TABLES: ${gained.join(", ")}`);
    const enabled = must("SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relrowsecurity", { db: CONTROL_DB });
    return `${enabled} tables under RLS; ${unprotected.length} reviewed exemptions unchanged`;
  });

  // 7 (first: 4 needs its tenant database). The tenant tree applied by the
  //    REAL migrate_tenants under a Neon-shaped owner that is not a superuser
  //    (the v184 abort: migration 9's ALTER ROLE needed superuser).
  await check("tenant-owner", () => {
    must(`DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${PROBE.owner}') THEN CREATE ROLE ${PROBE.owner} LOGIN; END IF; END $$;
          ALTER ROLE ${PROBE.owner} WITH LOGIN NOSUPERUSER CREATEDB CREATEROLE NOBYPASSRLS NOREPLICATION;
          -- The cluster-wide clock role may already exist from another owner;
          -- mirror the creator's ADMIN-only edge, as the pg suite does.
          DO $$ BEGIN IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'coralswarm_managed_clock') THEN
            GRANT coralswarm_managed_clock TO ${PROBE.owner} WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; END IF; END $$;`);
    must(`CREATE DATABASE ${PROBE.control} TEMPLATE ${template}`);
    must(`CREATE DATABASE ${PROBE.tenant} OWNER ${PROBE.owner}`);
    // `vector` is untrusted: Neon projects are created with it enabled and
    // the tenant tree never installs it as the owner (onboard_tenant.sh).
    must("CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA public; CREATE EXTENSION IF NOT EXISTS vector WITH SCHEMA public;", { db: PROBE.tenant });
    // The routing row, written the way the financial guard requires
    // (billing_enter_account in the account's scope).
    must(
      `BEGIN;
       WITH u AS (INSERT INTO users (id, email) VALUES (gen_random_uuid(), 'tenant-probe@e2e.invalid') RETURNING id)
       INSERT INTO accounts (owner_user_id, clerk_org_id) SELECT id, '${PROBE.org}' FROM u;
       SELECT set_config('app.account_id', (SELECT id::text FROM accounts WHERE clerk_org_id = '${PROBE.org}'), true) IS NOT NULL;
       SELECT public.billing_enter_account((SELECT id FROM accounts WHERE clerk_org_id = '${PROBE.org}')) IS NULL;
       INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status)
         SELECT '${PROBE.org}', id, '${PROBE.slug}', '${PROBE.secretRef}', 'active' FROM accounts WHERE clerk_org_id = '${PROBE.org}';
       COMMIT;`,
      { db: PROBE.control },
    );
    const owner = must(`SELECT rolsuper, rolbypassrls, rolreplication, rolcreaterole FROM pg_roles WHERE rolname = '${PROBE.owner}'`);
    if (owner !== "f|f|f|t") throw new Error(`the probe owner is not Neon-shaped (got ${owner}); this check would prove nothing`);
    const r = run(BINS.migrateTenants, ["--only", PROBE.slug], {
      MIGRATOR_DATABASE_URL: hostUrl("postgres", PROBE.control),
      [PROBE.secretRef]: hostUrl(PROBE.owner, PROBE.tenant),
      MASTER_KEK_HEX: FAKE.masterKek,
      RUST_LOG: "warn",
    });
    if (r.status !== 0 || !/1 migrated/.test(r.out)) throw new Error(`migrate_tenants under a non-superuser owner failed (exit ${r.status}):\n${r.out.slice(-3000)}`);
    const shape = must(
      `SELECT (SELECT count(*) FROM pg_class WHERE relnamespace = 'public'::regnamespace AND relowner <> '${PROBE.owner}'::regrole AND relkind IN ('r','p','v','S')),
              (SELECT string_agg(concat_ws(',', rolsuper, rolbypassrls, rolreplication, rolcreatedb, rolcreaterole), '') FROM pg_roles WHERE rolname = 'coralswarm_tenant_app'),
              (SELECT string_agg(concat_ws(',', rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls), '') FROM pg_roles WHERE rolname = 'coralswarm_managed_clock'),
              EXISTS (SELECT 1 FROM pg_roles r WHERE r.rolname IN ('coralswarm_app', 'coralswarm_tenant_app')
                       AND (pg_has_role(r.oid, 'coralswarm_managed_clock', 'MEMBER') OR pg_has_role(r.oid, 'coralswarm_managed_clock', 'SET'))),
              EXISTS (SELECT 1 FROM unnest(ARRAY['ever_held', 'hold_basis', 'processed_at']) c
                       WHERE has_column_privilege('coralswarm_tenant_app', 'public.corpus_capture_admissions', c, 'UPDATE')
                          OR has_column_privilege('coralswarm_tenant_app', 'public.corpus_capture_admissions', c, 'INSERT')),
              (SELECT has_function_privilege('${PROBE.owner}', oid, 'EXECUTE') AND NOT EXISTS (SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee = 0)
                 FROM pg_proc WHERE oid = 'public.corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure)`,
      { db: PROBE.tenant },
    );
    // relations not owned by the owner | tenant app role attrs | clock role
    // attrs | app can assume clock | app writes protected receipt columns |
    // owner executes the clock, PUBLIC does not
    if (shape !== "0|f,f,f,f,f|f,f,f,f,f,f,f|f|f|t") {
      throw new Error(`tenant database shape after migrating as a non-superuser owner is wrong (got ${shape}; want 0|f,f,f,f,f|f,f,f,f,f,f,f|f|f|t)`);
    }
    return `migrate_tenants -> head as ${PROBE.owner} (NOSUPERUSER); coralswarm_tenant_app NOSUPERUSER NOBYPASSRLS; managed clock sealed`;
  });

  // 4 + 5. The deploy-time guards' own SQL, run against the databases the real
  //    migrators built instead of scratch ones they build with sqlx-cli. The
  //    definer guard's --self-test runs first: it proves the query still flags
  //    the bad shapes, so a green run below is not a query matching nothing.
  const base = inContainer("postgres", "postgres");
  await check("definer-search-path", () =>
    [
      bashGuard("check_definer_search_path.sh", ["--self-test"], { DATABASE_URL: base }),
      bashGuard("check_definer_search_path.sh", ["--database", inContainer("postgres", CONTROL_DB), "control"]),
      bashGuard("check_definer_search_path.sh", ["--database", inContainer("postgres", PROBE.tenant), "tenant"]),
    ].join("; "),
  );
  // (Its --self-test prepares roles with a host psql, so it stays in the
  // guard's own CI job; `--database` keeps the "at least 19 writing definers
  // matched" vacuity floor.)
  await check("provisioning-privileges", () => bashGuard("check_provisioning_privileges.sh", ["--database", inContainer("postgres", CONTROL_DB)]));

  // 6. Migrations are out-of-band and the server checks, never applies.
  await check("migrator-split", async () => {
    // migrate_control refuses the serving role (it cannot read _sqlx_migrations)...
    const asApp = run(BINS.migrateControl, [], { MIGRATOR_DATABASE_URL: hostUrl("coralswarm_app", CONTROL_DB), RUST_LOG: "warn" });
    if (asApp.status === 0 || !/permission denied|migration failed/i.test(asApp.out)) {
      throw new Error(`migrate_control as coralswarm_app must fail loudly (exit ${asApp.status}):\n${asApp.out.slice(-1500)}`);
    }
    // ...and a missing DSN is a usage error, not a default.
    const noDsn = run(BINS.migrateControl, [], { RUST_LOG: "warn" });
    if (noDsn.status !== 2) throw new Error(`migrate_control with no DSN must exit 2 (got ${noDsn.status}):\n${noDsn.out.slice(-1500)}`);
    // A database one migration behind this build: --check reports it, and
    // the server refuses it rather than serving or migrating it.
    must(`CREATE DATABASE ${PROBE.stale} TEMPLATE ${template}`);
    const head = must("DELETE FROM _sqlx_migrations WHERE version = (SELECT max(version) FROM _sqlx_migrations) RETURNING version", { db: PROBE.stale });
    const check = run(BINS.migrateControl, ["--check"], { MIGRATOR_DATABASE_URL: hostUrl("postgres", PROBE.stale), RUST_LOG: "warn" });
    if (check.status === 0) throw new Error(`migrate_control --check passed a database missing migration ${head}`);
    await expectBootRefused(backendExe, PROBE.stale, "BEHIND");
    // A database nobody migrated.
    must(`CREATE DATABASE ${PROBE.empty}`);
    await expectBootRefused(backendExe, PROBE.empty, "MISSING");
    return `app-role migrate refused, no-DSN exit 2, --check flags v${head} pending, server refuses behind/unmigrated`;
  });

  dropProbes();
  if (failures.length) throw new Error(`database assertions failed:\n  - ${failures.join("\n  - ")}`);
}

if (process.argv[1]?.endsWith("assert-db.mjs")) {
  const state = JSON.parse(fs.readFileSync(path.join(STATE_DIR, "state.json"), "utf8"));
  const widened = path.join(STATE_DIR, path.basename(BINS.backend));
  const backendExe = process.platform === "win32" && fs.existsSync(widened) ? widened : BINS.backend;
  assertDatabases({ template: state.template, backendExe }).catch((e) => {
    console.error(e.message);
    process.exit(1);
  });
}
