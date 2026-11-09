//! The tenant tree must apply under a NON-SUPERUSER database owner.
//!
//! # Why this suite exists
//!
//! Migration `0009_tenant_rls` aborted the v184 production deploy:
//!
//! ```text
//! release: applying TENANT tree (/app/migrate_tenants, mode=enforce)
//!   [testorg] org_… — FAIL migrate: while executing migration 9:
//!   error returned from database: permission denied to alter role
//! release: ABORTING the deploy (fail-closed).
//! ```
//!
//! Every other tenant-tree suite — `tenant_rls_pg`, `tenant_rls_phase2_pg`,
//! `tenant_migrate_pg`, and the ~20 others — applies the tree from
//! `ORGT_TEST_BASE_URL`, which in CI is the `postgres` SUPERUSER. Under a
//! superuser, `ALTER ROLE … WITH NOSUPERUSER NOBYPASSRLS NOREPLICATION` is
//! free. Under the owner role of a **Neon** project it is refused, and no
//! amount of green from those suites could ever have said so. That blind spot,
//! not the SQL, is what shipped the outage: the property under test was
//! "the tree applies", and it was only ever tested against the one privilege
//! level production does not use.
//!
//! So this suite migrates as a deliberately **de-privileged owner** —
//! `NOSUPERUSER`, `CREATEDB`, `CREATEROLE`, no `BYPASSRLS`, no `REPLICATION`
//! — which is the shape of a Neon tenant owner. It is the only suite in the
//! repo that does.
//!
//! # The two independent gates, both covered here
//!
//! Postgres 16 ("Allow CREATEROLE to manage roles", unchanged in 17) gates
//! `ALTER ROLE` twice over:
//!
//! 1. **Attribute gate** — naming `SUPERUSER`/`REPLICATION`/`BYPASSRLS` in an
//!    ALTER requires holding that same attribute, and the check is on whether
//!    the CLAUSE WAS WRITTEN, not on whether the value would change. This is
//!    what actually failed in production: the migrator had just created the
//!    role and held `ADMIN OPTION` on it.
//! 2. **`ADMIN OPTION` gate** — independently, *any* `ALTER ROLE` needs
//!    `CREATEROLE` plus `ADMIN OPTION` on the target. The creator gets it
//!    automatically; a role created by a different principal leaves the
//!    migrator without it.
//!
//! [`tenant_tree_applies_under_a_non_superuser_neon_like_owner`] covers gate 1
//! and [`migration_needs_no_role_privilege_when_the_role_is_already_correct`]
//! covers gate 2.
//!
//! # Cluster-wide state — read before adding a test here
//!
//! `coralswarm_tenant_app` lives in `pg_authid`, which is **cluster-wide**, not
//! per-database. Two tests on two scratch databases share one role, and the
//! advisory lock 0009 takes is per-database and serialises nothing between
//! them. Three consequences, all load-bearing:
//!
//! * Every test here takes [`ROLE_LOCK_KEY`] on the SHARED base database
//!   (`ORGT_TEST_BASE_URL`'s own db, normally `postgres`), which is the one
//!   connection point all of them have in common, so the lock actually
//!   serialises — including across separate test binaries.
//! * The suite runs `--test-threads=1` in CI anyway, belt and braces.
//! * It gets its **own CI job with its own Postgres service**. Two tests here
//!   deliberately tamper with the role's attributes; sharing a cluster with
//!   `tenant_rls_pg` (which asserts the role is untampered) would make both
//!   flaky in a way that looks like a real RLS failure.
//!
//! Gated on `ORGT_TEST_BASE_URL`, a superuser DSN with `CREATE DATABASE` and
//! `CREATE ROLE` rights — the suite *drops down* to a de-privileged role
//! itself, so it needs the privilege in order to give it away:
//!
//! ```text
//! ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//!   cargo test -p coralswarm-backend --test tenant_rls_role_provisioning_pg \
//!   -- --test-threads=1
//! ```

use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};

/// The low-privilege role `0009_tenant_rls` provisions. Cluster-wide.
const APP_ROLE: &str = "coralswarm_tenant_app";

/// A stand-in for Neon's tenant owner: NOT a superuser, but holding the
/// `CREATEDB`/`CREATEROLE` a Neon project owner has. The absence of `BYPASSRLS`
/// and `REPLICATION` is the whole point — those are the attributes whose
/// mere mention in an `ALTER ROLE` is refused.
const OWNER_ROLE: &str = "orgt_neon_like_owner";
/// Set only on the ephemeral test cluster, only for the duration of the run.
/// Never used against any deploy.
const OWNER_PASSWORD: &str = "orgt_neon_like_owner_test_only_pw";

/// Serialises every test in this file against the cluster-wide role catalog.
/// Taken on the BASE database so that two tests using two different scratch
/// databases still contend for the same lock (advisory locks are scoped to the
/// database the session is connected to — the exact trap 0009's own
/// per-database lock falls into, documented in its role-block header).
/// Arbitrary but stable, and deliberately in the same shape as
/// `support/rls_probe.rs`'s `0x0083_5C09`: the high half names the migration
/// whose cluster-wide `CREATE ROLE` it serialises.
const ROLE_LOCK_KEY: i64 = 0x0009_5C09;

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

/// A DSN for the same scratch database, but authenticating as the de-privileged
/// owner rather than as the superuser the base URL names.
fn as_owner_dsn(db_url: &str) -> String {
    let opts = PgConnectOptions::from_str(db_url).expect("parse scratch db url");
    format!(
        "postgres://{OWNER_ROLE}:{OWNER_PASSWORD}@{}:{}/{}",
        opts.get_host(),
        opts.get_port(),
        opts.get_database().expect("scratch db name in url"),
    )
}

/// Held for the body of each test; releases the cluster-wide role lock on drop
/// by closing the connection that owns it.
struct RoleLock(PgConnection);

impl RoleLock {
    async fn acquire(base: &str) -> Self {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        sqlx::query("SELECT pg_advisory_lock($1)")
            .bind(ROLE_LOCK_KEY)
            .execute(&mut conn)
            .await
            .expect("take cluster-wide role lock");
        RoleLock(conn)
    }

    fn conn(&mut self) -> &mut PgConnection {
        &mut self.0
    }
}

/// Every scratch database this suite creates.
///
/// Listed in one place, and dropped in full by [`fresh_tenant_db`] before each
/// test, because BOTH shared roles are cluster-wide and a role cannot be
/// dropped while any database still depends on it: [`OWNER_ROLE`] owns these
/// databases, and [`APP_ROLE`] holds 0009's grants inside them. Dropping only
/// the database the current test is about to use would leave the previous
/// test's database pinning both roles, which makes every test after the first
/// fail on fixture setup — and makes the suite order-dependent, which for a
/// suite whose entire subject is shared catalog state would be its own kind of
/// lie.
const SCRATCH_DBS: &[&str] = &[
    "orgt_role_prov_fresh",
    "orgt_role_prov_preexisting",
    "orgt_role_prov_tampered",
    "orgt_role_prov_selfheal",
    "orgt_role_prov_construction",
];

/// Drop `coralswarm_tenant_app` so a test starts from a known catalog state.
///
/// A role cannot be dropped while anything still depends on it, and 0009 grants
/// it privileges on ~45 objects — so a leftover scratch database from ANOTHER
/// tenant-tree suite blocks this, cluster-wide, with an error that names no
/// remedy. Rather than surface that as an inscrutable sqlx failure, name the
/// databases that are in the way. (In CI this cannot happen: this suite gets
/// its own Postgres service. Locally it happens the first time you run it on a
/// cluster where `tenant_rls_pg` has already been.)
async fn drop_app_role(conn: &mut PgConnection) {
    if let Err(e) = conn
        .execute(format!("DROP ROLE IF EXISTS {APP_ROLE}").as_str())
        .await
    {
        let blockers: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT d.datname
               FROM pg_shdepend s
               JOIN pg_database d ON d.oid = s.dbid
              WHERE s.refobjid = to_regrole($1)::oid
              ORDER BY 1",
        )
        .bind(APP_ROLE)
        .fetch_all(&mut *conn)
        .await
        .unwrap_or_default();
        panic!(
            "cannot drop the cluster-wide role {APP_ROLE}: {e}\n\
             Databases still holding privileges granted to it: {}\n\
             Drop those scratch databases and re-run. This suite needs a clean role catalog \
             because it asserts on the role's attributes; in CI it gets a dedicated Postgres \
             service for exactly this reason.",
            if blockers.is_empty() {
                "<none found — the dependency is outside pg_shdepend>".to_string()
            } else {
                blockers.join(", ")
            }
        );
    }
}

/// A scratch tenant database owned by the de-privileged [`OWNER_ROLE`], with
/// the extensions the baseline needs already installed.
///
/// `vector` is an UNTRUSTED extension: `CREATE EXTENSION vector` requires a
/// superuser and a Neon project owner is not one. That is not a gap in the
/// fidelity of this fixture — it mirrors production exactly, where the Neon
/// project is created with pgvector already enabled (`onboard_tenant.sh`
/// step 2) and the tenant tree never installs it under the owner's own
/// privileges. `pg_trgm` is trusted and would work either way; it is installed
/// here too so the two are handled the same way.
async fn fresh_tenant_db(base: &str, db: &str) -> String {
    assert!(
        SCRATCH_DBS.contains(&db),
        "{db} is not in SCRATCH_DBS — add it, or the next test will fail to drop the shared roles"
    );
    let mut admin = PgConnection::connect(base).await.expect("connect admin");

    for scratch in SCRATCH_DBS {
        admin
            .execute(format!("DROP DATABASE IF EXISTS {scratch} WITH (FORCE)").as_str())
            .await
            .unwrap_or_else(|e| panic!("drop scratch db {scratch}: {e}"));
    }
    drop_app_role(&mut admin).await;

    // The owner role is created once and then re-asserted, never dropped: it
    // owns whichever scratch databases exist, and as a superuser we can simply
    // set its attributes back to the de-privileged shape the fixture promises.
    // `assert_owner_is_deprivileged` re-checks the outcome on every test rather
    // than trusting this statement.
    admin
        .execute(
            format!(
                "DO $$ BEGIN
                   IF NOT EXISTS (SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = '{OWNER_ROLE}')
                   THEN CREATE ROLE {OWNER_ROLE} LOGIN; END IF;
                 END $$;"
            )
            .as_str(),
        )
        .await
        .expect("ensure neon-like owner exists");
    admin
        .execute(
            format!(
                "ALTER ROLE {OWNER_ROLE} WITH LOGIN PASSWORD '{OWNER_PASSWORD}' \
                 NOSUPERUSER CREATEDB CREATEROLE NOBYPASSRLS NOREPLICATION"
            )
            .as_str(),
        )
        .await
        .expect("de-privilege the neon-like owner");

    // The clock owner is cluster-wide. A dedicated fresh CI service lets this
    // migrator CREATE it and receive its ADMIN-only creator edge naturally.
    // A reused local fixture cluster may already have it from another owner;
    // reproduce only that trusted migration-owner edge, as standing_execution
    // does. Do not grant either app role a clock/owner membership, and keep the
    // preexisting APP_ROLE case free of app-role ADMIN authority.
    admin
        .execute(
            format!(
                "DO $$ BEGIN \
                 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname='coralswarm_managed_clock') THEN \
                   GRANT coralswarm_managed_clock TO {OWNER_ROLE} \
                     WITH ADMIN TRUE, INHERIT FALSE, SET FALSE; \
                 END IF; END $$;"
            )
            .as_str(),
        )
        .await
        .expect("mirror the trusted migration owner's existing-clock ADMIN-only edge");
    admin
        .execute(format!("CREATE DATABASE {db} OWNER {OWNER_ROLE}").as_str())
        .await
        .expect("create scratch db");

    let db_url = with_db(base, db);
    let mut as_super = PgConnection::connect(&db_url)
        .await
        .expect("connect to scratch db as superuser");
    for ext in ["pg_trgm", "vector"] {
        as_super
            .execute(format!("CREATE EXTENSION IF NOT EXISTS {ext} WITH SCHEMA public").as_str())
            .await
            .unwrap_or_else(|e| panic!("install {ext}: {e}"));
    }
    as_super.close().await.ok();

    db_url
}

/// A pool authenticated AS the de-privileged owner — a real separate login, not
/// `SET ROLE`, because `SET ROLE` would keep the superuser's `rolsuper` and
/// silently restore exactly the privilege this suite exists to remove.
async fn owner_pool(db_url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(2)
        .connect(&as_owner_dsn(db_url))
        .await
        .expect("connect AS the neon-like owner")
}

/// Assert the fixture really is de-privileged before trusting anything it says.
async fn assert_owner_is_deprivileged(pool: &PgPool) {
    let (who, is_super, bypass, repl, createrole): (String, bool, bool, bool, bool) =
        sqlx::query_as(
            "SELECT current_user,
                    r.rolsuper, r.rolbypassrls, r.rolreplication, r.rolcreaterole
               FROM pg_roles r WHERE r.rolname = current_user",
        )
        .fetch_one(pool)
        .await
        .expect("read own role attributes");
    assert_eq!(who, OWNER_ROLE, "migrating as the wrong role");
    assert!(!is_super, "fixture owner is a SUPERUSER — this suite proves nothing");
    assert!(!bypass, "fixture owner holds BYPASSRLS — it could run the ALTER after all");
    assert!(!repl, "fixture owner holds REPLICATION — it could run the ALTER after all");
    assert!(createrole, "a Neon tenant owner holds CREATEROLE; the fixture must too");
}

async fn app_role_attrs(conn: &mut PgConnection) -> Option<(bool, bool, bool, bool, bool)> {
    sqlx::query_as(
        "SELECT rolsuper, rolbypassrls, rolreplication, rolcreatedb, rolcreaterole
           FROM pg_roles WHERE rolname = $1",
    )
    .bind(APP_ROLE)
    .fetch_optional(conn)
    .await
    .expect("read app role attributes")
}

async fn assert_managed_clock_boundary(pool: &PgPool) {
    let role_shape: (bool, bool, bool, bool, bool, bool, bool) = sqlx::query_as(
        "SELECT rolcanlogin, rolinherit, rolsuper, rolcreatedb, rolcreaterole,
                rolreplication, rolbypassrls
           FROM pg_roles WHERE rolname='coralswarm_managed_clock'",
    ).fetch_one(pool).await.expect("managed clock role exists");
    assert_eq!(role_shape, (false, false, false, false, false, false, false));
    let temporary_authority: (bool, bool) = sqlx::query_as(
        "SELECT pg_has_role(current_user,'coralswarm_managed_clock','SET'),
                has_schema_privilege('coralswarm_managed_clock','public','CREATE')",
    ).fetch_one(pool).await.expect("read temporary clock authority");
    assert_eq!(temporary_authority, (false, false), "ownership transfer must restore temporary grants");
    let owner_memberships: Vec<(bool, bool, bool)> = sqlx::query_as(
        "SELECT admin_option, inherit_option, set_option FROM pg_auth_members
           WHERE roleid='coralswarm_managed_clock'::regrole
             AND member=(SELECT oid FROM pg_roles WHERE rolname=current_user)",
    ).fetch_all(pool).await.expect("read restored migration-owner role grants");
    assert_eq!(owner_memberships, vec![(true, false, false)],
        "preserve the creator's ADMIN-only grant without a new self-grant");
    let function_shape: (String, bool, Vec<String>, bool) = sqlx::query_as(
        "SELECT pg_get_userbyid(proowner), prosecdef, proconfig,
                has_function_privilege('coralswarm_tenant_app',oid,'EXECUTE')
           FROM pg_proc WHERE oid='public.corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure",
    ).fetch_one(pool).await.expect("read managed clock function owner");
    assert_eq!(function_shape, ("coralswarm_managed_clock".into(), true,
        vec!["search_path=pg_catalog, public".into()], true));
    // Production tenant DSNs connect as this owner (tenancy::registry, until
    // OPS-4-T). Tenant 0155 alone left it without EXECUTE, which failed every
    // standing adoption in production; 0159 makes the owner an INHERIT (never
    // SET) member of coralswarm_tenant_app, and PUBLIC stays closed.
    let owner_execute: (bool, bool) = sqlx::query_as(
        "SELECT has_function_privilege(current_user,oid,'EXECUTE'),
                EXISTS(SELECT 1 FROM aclexplode(proacl) a WHERE a.grantee=0)
           FROM pg_proc WHERE oid='public.corpus_standing_receipt_event(uuid,uuid,text,text)'::regprocedure",
    ).fetch_one(pool).await.expect("read owner runtime clock privilege");
    assert_eq!(owner_execute, (true, false),
        "the owner runtime must execute the managed clock, and PUBLIC must not");
    let owner_call: bool = sqlx::query_scalar(
        "SELECT corpus_standing_receipt_event(gen_random_uuid(),gen_random_uuid(),'adopt',NULL)",
    ).fetch_one(pool).await.expect("owner runtime calls the managed clock without 42501");
    assert!(!owner_call, "an unknown unit is not clocked");
    let app_can_assume: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM pg_roles r
           WHERE r.rolname IN ('coralswarm_app','coralswarm_tenant_app')
             AND (pg_has_role(r.oid,'coralswarm_managed_clock','MEMBER')
               OR pg_has_role(r.oid,'coralswarm_managed_clock','SET')))",
    ).fetch_one(pool).await.expect("read app clock membership paths");
    assert!(!app_can_assume, "an app role must not assume the managed clock owner");
    let protected_writes: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM unnest(ARRAY['ever_held','hold_basis','processed_at']) c
           WHERE has_column_privilege('coralswarm_tenant_app','public.corpus_capture_admissions',c,'UPDATE')
              OR has_column_privilege('coralswarm_tenant_app','public.corpus_capture_admissions',c,'INSERT'))",
    ).fetch_one(pool).await.expect("read protected receipt column grants");
    assert!(!protected_writes, "app receipt writes must not bypass managed clock ownership");
}

/// THE regression test for the v184 abort.
///
/// Before the fix this fails at migration 9 with `permission denied to alter
/// role`; the assertion below is the exact production symptom, inverted.
#[tokio::test]
async fn tenant_tree_applies_under_a_non_superuser_neon_like_owner() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let mut lock = RoleLock::acquire(&base).await;
    let db_url = fresh_tenant_db(&base, "orgt_role_prov_fresh").await;
    let owner = owner_pool(&db_url).await;
    assert_owner_is_deprivileged(&owner).await;

    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect(
            "the tenant tree must apply under a NON-SUPERUSER owner — this is the v184 \
             production abort (`permission denied to alter role` at migration 9)",
        );

    assert_managed_clock_boundary(&owner).await;

    // 0159: the owner that created coralswarm_tenant_app keeps its ADMIN-only
    // creator edge and gains one INHERIT-only edge. Inheritance never changes
    // current_user, so the table owner (NOBYPASSRLS here) stays outside RLS.
    let app_edges: Vec<(bool, bool, bool)> = sqlx::query_as(
        "SELECT admin_option, inherit_option, set_option FROM pg_auth_members
          WHERE roleid='coralswarm_tenant_app'::regrole
            AND member=(SELECT oid FROM pg_roles WHERE rolname=current_user)
          ORDER BY admin_option",
    ).fetch_all(&owner).await.expect("read owner runtime app-role edges");
    assert_eq!(app_edges, vec![(false, true, false), (true, false, false)]);
    let identity: (String, bool, bool) = sqlx::query_as(
        "SELECT current_user::text, row_security_active('public.graphs'),
                pg_has_role('coralswarm_tenant_app',current_user,'MEMBER')",
    ).fetch_one(&owner).await.expect("read owner runtime identity");
    assert_eq!(identity, (OWNER_ROLE.to_string(), false, false),
        "inheriting the app role must not change the owner's identity or RLS exemption");

    // The role exists and holds none of the three attributes that would make
    // every policy in 0009 a no-op — created that way by a connection that is
    // incapable of creating it any other way.
    let (is_super, bypass, repl, createdb, createrole) = app_role_attrs(lock.conn())
        .await
        .expect("0009 must provision coralswarm_tenant_app");
    assert!(!bypass, "{APP_ROLE} has BYPASSRLS — every policy in 0009 is a no-op");
    assert!(!is_super, "{APP_ROLE} is a SUPERUSER");
    assert!(!repl, "{APP_ROLE} has REPLICATION — it can read every ocean off the WAL");
    assert!(!createdb, "{APP_ROLE} has CREATEDB");
    assert!(!createrole, "{APP_ROLE} has CREATEROLE");

    // Idempotent: the tree re-applies cleanly, which is what every subsequent
    // deploy does.
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect("re-applying the tenant tree must be a no-op");

    owner.close().await;
}

/// Gate 2: the `ADMIN OPTION` failure mode, and the reason the fix is
/// *verify-first* rather than *reconcile-the-safe-attributes-unconditionally*.
///
/// The role is created here by a DIFFERENT principal (the superuser), so the
/// migrating owner has no `ADMIN OPTION` on it and **every** `ALTER ROLE`
/// against it is refused — including `NOCREATEDB`, which needs no attribute at
/// all. The migration must therefore issue no `ALTER ROLE` whatsoever when the
/// role is already correct. That is not an optimisation; it is what makes the
/// tree re-runnable on a tenant whose role was provisioned out-of-band.
#[tokio::test]
async fn migration_needs_no_role_privilege_when_the_role_is_already_correct() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let mut lock = RoleLock::acquire(&base).await;
    let db_url = fresh_tenant_db(&base, "orgt_role_prov_preexisting").await;

    // Created by the superuser, NOT by the migrator: no ADMIN OPTION is granted
    // to the owner role, so the migrator cannot ALTER it at all.
    lock.conn()
        .execute(
            format!(
                "CREATE ROLE {APP_ROLE} WITH LOGIN NOSUPERUSER NOCREATEDB \
                 NOCREATEROLE NOREPLICATION NOBYPASSRLS"
            )
            .as_str(),
        )
        .await
        .expect("pre-create the app role as a third party");

    // Prove the subject exists before counting grants against it. `count(*)`
    // over an absent role is also 0, so without this the assertion below would
    // pass just as happily if the CREATE above had silently not taken — the
    // same "empty set reads as success" shape the migration's post-condition
    // had to be rewritten to avoid.
    app_role_attrs(lock.conn())
        .await
        .expect("fixture is wrong: the pre-created app role is missing");
    let admin_option_holders: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_auth_members m
          WHERE m.roleid = to_regrole($1)::oid AND m.member = to_regrole($2)::oid",
    )
    .bind(APP_ROLE)
    .bind(OWNER_ROLE)
    .fetch_one(lock.conn())
    .await
    .expect("count admin-option grants");
    assert_eq!(
        admin_option_holders, 0,
        "fixture is wrong: the owner must NOT hold ADMIN OPTION for this test to mean anything"
    );

    let owner = owner_pool(&db_url).await;
    assert_owner_is_deprivileged(&owner).await;
    sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect(
            "with a correct, third-party-created role the migration must issue no ALTER ROLE \
             and therefore need no ADMIN OPTION",
        );
    assert_managed_clock_boundary(&owner).await;
    owner.close().await;
}

/// A tampered role must fail the deploy CLOSED, and say what to do about it.
///
/// `BYPASSRLS` on the app role makes all 34 policies no-ops. A non-superuser
/// owner cannot clear it (that is gate 1 again, in the direction that matters),
/// so the only correct behaviour is to refuse — and to say so in the MESSAGE
/// field, because sqlx renders only that field and `USING HINT` would be
/// invisible in the deploy log this text exists to be read in.
#[tokio::test]
async fn tampered_bypassrls_role_fails_closed_with_an_actionable_message() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let mut lock = RoleLock::acquire(&base).await;
    let db_url = fresh_tenant_db(&base, "orgt_role_prov_tampered").await;
    lock.conn()
        .execute(format!("CREATE ROLE {APP_ROLE} WITH LOGIN BYPASSRLS").as_str())
        .await
        .expect("seed a tampered app role");

    let owner = owner_pool(&db_url).await;
    assert_owner_is_deprivileged(&owner).await;
    let err = sqlx::migrate!("./migrations_tenant")
        .run(&owner)
        .await
        .expect_err("a BYPASSRLS app role must fail the migration closed");
    let msg = err.to_string();

    // Everything the next responder needs, in the field sqlx actually prints.
    assert!(msg.contains(APP_ROLE), "message must name the role: {msg}");
    assert!(msg.contains("BYPASSRLS"), "message must name the attribute: {msg}");
    assert!(
        msg.contains("tenant-rls-cutover-runbook.md"),
        "message must point at the runbook: {msg}"
    );
    assert!(
        msg.contains("DROP"),
        "message must state the remedy, not just the diagnosis: {msg}"
    );
    assert!(
        !msg.contains("permission denied to alter role"),
        "the bare Postgres error is what cost v184 — it must be replaced, not wrapped: {msg}"
    );

    // Failed closed: the tamper is still there, untouched, and nothing was
    // installed on top of it.
    let (_, bypass, ..) = app_role_attrs(lock.conn())
        .await
        .expect("role still present");
    assert!(bypass, "the migration must not silently clear the tamper");

    // …and the failure left NO ledger row and NO partial schema behind.
    //
    // This is the assertion that says testorg needs no manual repair after the
    // v184 abort. 0009 carries no `-- no-transaction` marker, so sqlx wraps it
    // in one transaction and inserts its `_sqlx_migrations` row inside that
    // same transaction — a mid-migration failure therefore rolls back the row
    // and the DDL together. Asserted rather than reasoned about, because the
    // alternative (a half-applied migration 9 and a ledger that claims
    // otherwise) is the difference between "re-run the deploy" and "hand-repair
    // every tenant", and the whole recovery plan rests on which one it is.
    let ledger_v9: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM _sqlx_migrations WHERE version = 9",
    )
    .fetch_one(&owner)
    .await
    .expect("read the sqlx ledger");
    assert_eq!(
        ledger_v9, 0,
        "a failed migration 9 must leave no ledger row — otherwise every tenant that hit the \
         v184 abort would need hand-repair before it could be re-run"
    );
    let policies: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_policies WHERE schemaname = 'public'",
    )
    .fetch_one(&owner)
    .await
    .expect("count policies");
    assert_eq!(policies, 0, "a failed migration 9 must leave no policies behind");

    owner.close().await;
}

/// 0083 parity: where the platform CAN self-heal a tampered role, it still
/// does. The verify-first shape must not have traded self-healing away — it
/// only stopped issuing DDL that was both unnecessary and impossible.
#[tokio::test]
async fn superuser_owner_still_self_heals_a_tampered_role() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let mut lock = RoleLock::acquire(&base).await;
    let db_url = fresh_tenant_db(&base, "orgt_role_prov_selfheal").await;
    lock.conn()
        .execute(format!("CREATE ROLE {APP_ROLE} WITH LOGIN BYPASSRLS CREATEDB").as_str())
        .await
        .expect("seed a tampered app role");

    // Migrate as the SUPERUSER base connection this time — the consumer plane's
    // shape, and dev/CI's.
    let superuser = PgPoolOptions::new()
        .max_connections(2)
        .connect(&db_url)
        .await
        .expect("connect as superuser");
    sqlx::migrate!("./migrations_tenant")
        .run(&superuser)
        .await
        .expect("a superuser CAN clear the tamper, so the migration must succeed");

    let (is_super, bypass, repl, createdb, _) = app_role_attrs(lock.conn())
        .await
        .expect("role present");
    assert!(!bypass, "a superuser migration must clear BYPASSRLS");
    assert!(!createdb, "…and the non-critical drift alongside it");
    assert!(!is_super);
    assert!(!repl);
    superuser.close().await;
}

/// The property that makes the whole fix safe: on the platform where the
/// migration cannot RE-ASSERT the critical attributes, it also cannot CREATE
/// them. PG 16 refuses to turn `SUPERUSER`/`BYPASSRLS`/`REPLICATION` on for a
/// caller that does not already hold them, while accepting the `NO-` forms from
/// any `CREATEROLE` holder — so a role provisioned by 0009 under a Neon owner
/// is correct by construction, not by assertion.
///
/// If a future Postgres relaxed this, the verify-first block would still fail
/// closed (it re-reads the catalog), but the reasoning in 0009's header would
/// no longer hold — so it is asserted rather than assumed.
#[tokio::test]
async fn a_non_superuser_owner_cannot_create_a_privileged_role() {
    let Some(base) = base_url() else {
        eprintln!("skipping: set ORGT_TEST_BASE_URL");
        return;
    };
    let _lock = RoleLock::acquire(&base).await;
    let db_url = fresh_tenant_db(&base, "orgt_role_prov_construction").await;
    let owner = owner_pool(&db_url).await;
    assert_owner_is_deprivileged(&owner).await;

    for attr in ["SUPERUSER", "BYPASSRLS", "REPLICATION"] {
        let probe = format!("orgt_probe_{}", attr.to_lowercase());
        match sqlx::query(&format!("CREATE ROLE {probe} WITH LOGIN {attr}"))
            .execute(&owner)
            .await
        {
            Ok(_) => panic!(
                "a de-privileged owner created a {attr} role — 0009's guarantee that the app \
                 role is correct BY CONSTRUCTION on Neon no longer holds, and the verify-first \
                 block's reasoning needs revisiting"
            ),
            Err(e) => assert!(
                e.to_string().contains("permission denied to create role"),
                "expected a privilege refusal for {attr}, got: {e}"
            ),
        }
    }
    owner.close().await;
}
