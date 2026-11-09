//! Integration test for the OUT-OF-BAND control-tree migrator — the SERVING
//! process's side of it.
//!
//! This is the headline guarantee of the change: the server boots and serves
//! as the least-privilege `coralswarm_app` role with `MIGRATOR_DATABASE_URL`
//! **removed from the environment entirely**. The elevated owner DSN is never
//! handed to the serving container at all, so a post-boot compromise of that
//! process has nothing to read — which was the residual risk CodeRabbit
//! flagged on PR #283 (there, the boot path only *used* the elevated DSN
//! briefly, but the variable stayed in the process env for its lifetime).
//!
//! Sequence under test (the real production one):
//!   1. `migrate_control` applies the control tree out-of-band under the
//!      elevated owner DSN — on Fly, the deploy `release_command`, which
//!      completes before any traffic is routed to the new image.
//!   2. `run()` boots with `DATABASE_URL` = `coralswarm_app` and NO
//!      `MIGRATOR_DATABASE_URL`, verifies the control schema read-only, and
//!      becomes ready.
//!
//! It also re-confirms (belt-and-suspenders with `consumer_rls_pg.rs`) that
//! the long-lived connection really cannot touch `_sqlx_migrations` — i.e.
//! the boot preflight took its documented `42501 → presence check` fallback
//! rather than accidentally running with elevated rights.
//!
//! Companion files: `migrator_dsn_legacy_pg.rs` (the migrator binary itself),
//! `migrator_dsn_broken_pg.rs` (server vs. an unmigrated DB) and
//! `migrator_dsn_stale_pg.rs` (server vs. a DB behind this build). Split into
//! separate files/binaries rather than separate `#[tokio::test]`s because
//! `coralswarm_backend::run()` calls `init_tracing()`, which sets the
//! process-global `tracing` subscriber exactly once; a second in-process
//! `run()` call panics on it (`SetGlobalDefaultError`). Each `tests/*.rs`
//! file compiles to its own separate binary/process, so this is a clean
//! split, not a workaround.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights AND permission to `ALTER ROLE`/`CREATE ROLE`), same
//! convention as `consumer_rls_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test migrator_dsn_fixed_pg

#[cfg(not(feature = "binaries"))]
compile_error!("This suite runs utilities. Enable defaults or --features binaries.");

use std::process::Command;
use std::str::FromStr;

use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::{Connection, Executor, PgConnection, PgPool};

/// Test-only password for `coralswarm_app`, set on the scratch cluster only —
/// the role itself is created with NO password by migration 0083; never a
/// real credential.
const APP_ROLE_TEST_PASSWORD: &str = "orgt_migrator_dsn_test_only_pw";

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
/// Rewrite a `postgres://user[:pass]@host.../db` DSN to authenticate as a
/// different role, keeping the same host/port/db/params. Plain string
/// surgery (no `url` crate direct dependency in this crate) — good enough for
/// the well-formed scratch-DB DSNs this test constructs itself.
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

/// Step 1 of the real deploy sequence: apply the control tree by running the
/// ACTUAL `migrate_control` binary under the elevated owner DSN (this is what
/// Fly's `release_command` executes), then set a test-only password on the
/// `coralswarm_app` role migration 0083 creates so the server can authenticate
/// as it.
async fn migrate_out_of_band_and_password_the_app_role(owner_url: &str) {
    let out = Command::new(env!("CARGO_BIN_EXE_migrate_control"))
        .env_remove("DATABASE_URL")
        .env("MIGRATOR_DATABASE_URL", owner_url)
        .output()
        .expect("spawn migrate_control");
    assert!(
        out.status.success(),
        "out-of-band migrate_control must succeed\nstdout: {}\nstderr: {}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );

    let owner: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(owner_url)
        .await
        .expect("connect owner");
    sqlx::query(&format!(
        "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
    ))
    .execute(&owner)
    .await
    .expect("set test-only password on coralswarm_app");
    owner.close().await;
}

/// Poll `/readyz` (bounded retry, not a fixed sleep) and return whether it
/// ever returned 2xx.
async fn wait_for_ready(base: &str) -> bool {
    let http = reqwest::Client::new();
    for _ in 0..50 {
        if let Ok(resp) = http.get(format!("{base}/readyz")).send().await {
            if resp.status().is_success() {
                return true;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    false
}

#[tokio::test]
async fn run_boots_as_the_app_role_with_migrator_database_url_absent_from_the_environment() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping run_boots_as_the_app_role_with_migrator_database_url_absent_from_the_environment: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_migrator_dsn_fixed";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);
    migrate_out_of_band_and_password_the_app_role(&owner_url).await;
    let app_url = as_role(&owner_url, "coralswarm_app", APP_ROLE_TEST_PASSWORD);

    // ── The production config after OPS-4: DATABASE_URL is the low-privilege
    // app role and the serving process has NO elevated credential at all. ──
    let port: u16 = 18190;
    let internal_port: u16 = 18191;
    unsafe {
        std::env::set_var("DATABASE_URL", &app_url);
        std::env::remove_var("MIGRATOR_DATABASE_URL");
        std::env::set_var("PORT", port.to_string());
        std::env::set_var("INTERNAL_PORT", internal_port.to_string());
    }
    assert!(
        std::env::var("MIGRATOR_DATABASE_URL").is_err(),
        "the serving process must not have an elevated DSN in its environment"
    );

    let server = tokio::spawn(coralswarm_backend::run());
    let ready = wait_for_ready(&format!("http://127.0.0.1:{port}")).await;
    assert!(
        ready,
        "server must boot and become ready as coralswarm_app with MIGRATOR_DATABASE_URL unset — \
         the control tree was already applied out-of-band by migrate_control, and the boot \
         preflight is read-only"
    );
    server.abort();

    unsafe {
        std::env::remove_var("DATABASE_URL");
        std::env::remove_var("PORT");
        std::env::remove_var("INTERNAL_PORT");
    }

    // ── Belt-and-suspenders: the long-lived connection really is locked out
    // of `_sqlx_migrations`, exactly as migration 0083 intends — so the boot
    // preflight above genuinely took its `42501 → presence check` fallback
    // and did not somehow run with elevated rights. ───────────────────────
    let opts = PgConnectOptions::from_str(&owner_url)
        .expect("parse scratch db url")
        .username("coralswarm_app")
        .password(APP_ROLE_TEST_PASSWORD);
    let app_pool = PgPoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .expect("connect AS coralswarm_app");
    let result: Result<i64, sqlx::Error> =
        sqlx::query_scalar("SELECT count(*) FROM _sqlx_migrations")
            .fetch_one(&app_pool)
            .await;
    assert!(
        result.is_err(),
        "coralswarm_app (what DATABASE_URL now points at) must NOT be able to read \
         _sqlx_migrations — got {result:?}"
    );
    app_pool.close().await;

    drop_db(&mut admin_conn, db).await;
}
