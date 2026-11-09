//! Integration test for the OUT-OF-BAND control-tree migrator
//! (`backend/src/bin/migrate_control.rs`) — the binary itself.
//!
//! Background (OPS-4): migration `0083_consumer_rls` REVOKEs ALL on
//! `_sqlx_migrations` from the least-privilege `coralswarm_app` role, so
//! applying the control tree needs the elevated owner role. PR #283's first
//! fix kept migrating at boot but on a separate `MIGRATOR_DATABASE_URL`
//! connection — which left that elevated DSN in the SERVING process's
//! environment for its whole lifetime, readable by any post-boot compromise.
//! The serving process now never migrates at all: `migrate_control` applies
//! the tree out-of-band (Fly deploy `release_command`; `cargo run --bin
//! migrate_control` in dev/CI), exactly as `migrate_tenants` has always done
//! for the tenant tree.
//!
//! This file (formerly the "backward compatibility, migrate-on-boot" case)
//! now proves the replacement mechanism actually works, end to end, by
//! SPAWNING THE REAL BINARY (`CARGO_BIN_EXE_migrate_control`) — not by
//! re-implementing its logic:
//!   * fresh database → applies the whole tree, exit 0;
//!   * second run → idempotent, "up-to-date", exit 0;
//!   * `--check` → clean after a full apply, DRIFT + exit 1 when a migration
//!     is pending (this is what makes a skipped release step detectable);
//!   * no DSN configured → exit 2 (usage), never a connection attempt;
//!   * unreachable DSN → exit 1 (a Fly `release_command` non-zero exit aborts
//!     the deploy, which is the whole point);
//!   * run AS the least-privilege `coralswarm_app` role → exit 1 with a clear
//!     error, rather than silently reporting success without migrating.
//!
//! The companion files: `migrator_dsn_fixed_pg.rs` (the server boots as the
//! app role with `MIGRATOR_DATABASE_URL` unset ENTIRELY),
//! `migrator_dsn_broken_pg.rs` (server vs. an unmigrated DB → fail fast) and
//! `migrator_dsn_stale_pg.rs` (server vs. a DB behind this build → fail fast).
//! They stay in separate files/binaries because `coralswarm_backend::run()`
//! calls `init_tracing()`, which sets the process-global `tracing` subscriber
//! exactly once; a second in-process `run()` panics on it.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights AND permission to `ALTER ROLE`/`CREATE ROLE`), same
//! convention as `consumer_rls_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test migrator_dsn_legacy_pg

#[cfg(not(feature = "binaries"))]
compile_error!("This suite runs utilities. Enable defaults or --features binaries.");

use std::borrow::Cow;
use std::process::{Command, Output};

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};

/// Test-only password for `coralswarm_app`, set on the scratch cluster only —
/// the role itself is created with NO password by migration 0083; never a
/// real credential.
const APP_ROLE_TEST_PASSWORD: &str = "orgt_migrate_control_test_only_pw";

/// Serializes the cases that APPLY the control tree.
///
/// Postgres ROLES are cluster-wide, not per-database: migration 0083 creates
/// `coralswarm_app` and grants/revokes on it, and the app-role case below sets
/// a password on it. Cargo runs `#[tokio::test]`s in one binary concurrently,
/// so without this lock two cases on two different scratch DATABASES would
/// still contend on that one shared role — observed as a flaky "failed to
/// connect" (password reset out from under the authenticating case) instead of
/// the expected permission-denied. 0083's own `XX000` retry loop exists for
/// the sibling version of this race on shared catalog DDL.
///
/// Only the DB-touching cases take it; the two pure exit-code cases don't need
/// it (and shouldn't wait on a 15s migration to run).
static CONTROL_TREE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
/// Rewrite a `postgres://user[:pass]@host.../db` DSN to authenticate as a
/// different role, keeping the same host/port/db/params. Plain string surgery
/// — good enough for the well-formed scratch-DB DSNs this test constructs.
fn as_role(url: &str, user: &str, pass: &str) -> String {
    let scheme_end = url.find("://").expect("url has a scheme") + 3;
    let (scheme, rest) = url.split_at(scheme_end);
    let host_and_rest = rest.split_once('@').map(|(_, h)| h).unwrap_or(rest);
    format!("{scheme}{user}:{pass}@{host_and_rest}")
}
async fn admin(base: &str) -> PgConnection {
    PgConnection::connect(base).await.expect("connect admin")
}
async fn recreate(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}
async fn drop_db(conn: &mut PgConnection, db: &str) {
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Spawn the REAL `migrate_control` binary cargo just built. Both DSN env
/// vars are cleared first so an ambient `DATABASE_URL` in the developer's (or
/// CI's) shell can never silently satisfy a case meant to run without one.
fn run_migrate_control(args: &[&str], migrator: Option<&str>, database: Option<&str>) -> Output {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_migrate_control"));
    cmd.args(args)
        .env_remove("MIGRATOR_DATABASE_URL")
        .env_remove("DATABASE_URL");
    if let Some(u) = migrator {
        cmd.env("MIGRATOR_DATABASE_URL", u);
    }
    if let Some(u) = database {
        cmd.env("DATABASE_URL", u);
    }
    cmd.output().expect("spawn migrate_control")
}

fn stdout_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stdout).to_string()
}
fn stderr_of(o: &Output) -> String {
    String::from_utf8_lossy(&o.stderr).to_string()
}

/// Apply the control tree only UP TO (excluding) `before_version`, through the
/// real sqlx migrator (so `-- no-transaction` migrations run correctly and the
/// ledger stays valid for a follow-up full run). Same helper shape as
/// `mcp_governance_pg.rs`.
async fn migrate_control_until(pool: &PgPool, before_version: i64) {
    let full = sqlx::migrate!("./migrations_control");
    let subset: Vec<sqlx::migrate::Migration> = full
        .iter()
        .filter(|m| m.version < before_version)
        .cloned()
        .collect();
    let truncated = sqlx::migrate::Migrator {
        migrations: Cow::Owned(subset),
        ..full
    };
    truncated.run(pool).await.expect("run partial control tree");
}

#[tokio::test]
async fn migrate_control_applies_the_tree_to_a_fresh_database_and_is_idempotent() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping migrate_control_applies_the_tree_to_a_fresh_database_and_is_idempotent: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Cluster-wide `coralswarm_app` role: see CONTROL_TREE_LOCK.
    let _serialized = CONTROL_TREE_LOCK.lock().await;
    let db = "orgt_migrate_control_fresh";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);
    let head = coralswarm_backend::control_head_version();

    // ── First run: a genuinely fresh database, elevated DSN supplied the way
    // the Fly release_command supplies it (MIGRATOR_DATABASE_URL only —
    // DATABASE_URL is not even set). ──────────────────────────────────────
    let out = run_migrate_control(&[], Some(&owner_url), None);
    assert!(
        out.status.success(),
        "migrate_control must exit 0 on a fresh DB\nstdout: {}\nstderr: {}",
        stdout_of(&out),
        stderr_of(&out)
    );
    assert!(
        stdout_of(&out).contains("applied"),
        "expected an 'applied N migration(s)' summary, got: {}",
        stdout_of(&out)
    );

    // The ledger really is at this build's head — i.e. the binary applied the
    // whole embedded tree, not a prefix of it.
    let owner: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&owner_url)
        .await
        .expect("connect owner");
    let applied: Option<i64> = sqlx::query_scalar("SELECT max(version) FROM _sqlx_migrations")
        .fetch_one(&owner)
        .await
        .expect("read ledger");
    assert_eq!(
        applied,
        Some(head),
        "the database must be at the embedded tree's head version"
    );

    // ── Second run: idempotent, and reports so. This is what makes the
    // release_command safe to re-run on every deploy. ─────────────────────
    let out = run_migrate_control(&[], Some(&owner_url), None);
    assert!(
        out.status.success(),
        "a re-run against an up-to-date DB must exit 0\nstderr: {}",
        stderr_of(&out)
    );
    assert!(
        stdout_of(&out).contains("up-to-date"),
        "expected an 'up-to-date' summary on the second run, got: {}",
        stdout_of(&out)
    );

    // ── --check is clean after a full apply. ──────────────────────────────
    let out = run_migrate_control(&["--check"], Some(&owner_url), None);
    assert!(
        out.status.success(),
        "--check must exit 0 against a fully-migrated DB\nstderr: {}",
        stderr_of(&out)
    );

    owner.close().await;
    drop_db(&mut admin_conn, db).await;
}

#[tokio::test]
async fn migrate_control_check_reports_pending_migrations_and_then_applies_them() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping migrate_control_check_reports_pending_migrations_and_then_applies_them: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Cluster-wide `coralswarm_app` role: see CONTROL_TREE_LOCK.
    let _serialized = CONTROL_TREE_LOCK.lock().await;
    let db = "orgt_migrate_control_check";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);
    let head = coralswarm_backend::control_head_version();

    // Everything except the head migration — the shape of a database that a
    // skipped/failed release step left behind.
    let owner: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&owner_url)
        .await
        .expect("connect owner");
    migrate_control_until(&owner, head).await;

    let out = run_migrate_control(&["--check"], Some(&owner_url), None);
    assert!(
        !out.status.success(),
        "--check must exit non-zero while a migration is pending"
    );
    let err = stderr_of(&out);
    assert!(
        err.contains("DRIFT") && err.contains(&format!("pending {head}")),
        "expected a DRIFT report naming the pending version {head}, got: {err}"
    );

    // Applying it clears the drift.
    let out = run_migrate_control(&[], Some(&owner_url), None);
    assert!(
        out.status.success(),
        "migrate must exit 0\nstderr: {}",
        stderr_of(&out)
    );
    assert!(
        stdout_of(&out).contains(&head.to_string()),
        "expected the applied summary to name v{head}, got: {}",
        stdout_of(&out)
    );
    let out = run_migrate_control(&["--check"], Some(&owner_url), None);
    assert!(
        out.status.success(),
        "--check must be clean once the pending migration is applied\nstderr: {}",
        stderr_of(&out)
    );

    owner.close().await;
    drop_db(&mut admin_conn, db).await;
}

#[tokio::test]
async fn migrate_control_exits_2_when_no_dsn_is_configured() {
    // No database needed — this is a pure configuration-error path, so it runs
    // even without ORGT_TEST_BASE_URL.
    let out = run_migrate_control(&[], None, None);
    assert_eq!(
        out.status.code(),
        Some(2),
        "expected exit 2 (usage/config error) with neither DSN set, got {:?}\nstderr: {}",
        out.status.code(),
        stderr_of(&out)
    );
    assert!(
        stderr_of(&out).contains("MIGRATOR_DATABASE_URL"),
        "the error must name the variable an operator has to set, got: {}",
        stderr_of(&out)
    );
}

#[tokio::test]
async fn migrate_control_exits_nonzero_on_an_unreachable_database() {
    // Port 1 is never a Postgres: connect fails immediately (no 15s wait).
    // A non-zero exit here is exactly what aborts a Fly deploy before any
    // traffic is routed to the new image.
    let out = run_migrate_control(
        &[],
        Some("postgres://nobody@127.0.0.1:1/definitely_not_a_database"),
        None,
    );
    assert!(
        !out.status.success(),
        "an unreachable DSN must exit non-zero"
    );
    assert!(
        stderr_of(&out).contains("failed to connect"),
        "expected a connect-failure message, got: {}",
        stderr_of(&out)
    );
}

#[tokio::test]
async fn migrate_control_fails_loudly_when_pointed_at_the_least_privilege_app_role() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping migrate_control_fails_loudly_when_pointed_at_the_least_privilege_app_role: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    // Cluster-wide `coralswarm_app` role: see CONTROL_TREE_LOCK.
    let _serialized = CONTROL_TREE_LOCK.lock().await;
    let db = "orgt_migrate_control_app_role";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&owner_url)
        .await
        .expect("connect owner");
    sqlx::migrate!("./migrations_control")
        .run(&owner)
        .await
        .expect("apply control tree incl. 0083");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");
    owner.close().await;

    // The operator mistake this guards: pointing the release command at the
    // SERVING role's DSN. 0083 revoked `_sqlx_migrations` from it, so the run
    // must fail loudly — never report "up-to-date" while having verified
    // nothing.
    let app_url = as_role(&owner_url, "coralswarm_app", APP_ROLE_TEST_PASSWORD);
    let out = run_migrate_control(&[], Some(&app_url), None);
    assert!(
        !out.status.success(),
        "migrate_control must fail when run as coralswarm_app\nstdout: {}\nstderr: {}",
        stdout_of(&out),
        stderr_of(&out)
    );
    let err = stderr_of(&out).to_lowercase();
    assert!(
        err.contains("permission denied") || err.contains("migration failed"),
        "expected a permission/migration failure, got: {}",
        stderr_of(&out)
    );

    drop_db(&mut admin_conn, db).await;
}
