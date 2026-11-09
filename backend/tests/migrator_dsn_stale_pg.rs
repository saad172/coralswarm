//! Integration test for the boot-time CONTROL-schema preflight — the
//! STALE-database case (schema present, but behind this build).
//!
//! See `migrator_dsn_fixed_pg.rs`'s module doc for full background, and for
//! why each `run()` case lives in its own file/binary (`init_tracing()` sets
//! the process-global `tracing` subscriber exactly once).
//!
//! The unmigrated case (`migrator_dsn_broken_pg.rs`) is the loud one. This is
//! the quiet one, and the reason the preflight compares VERSIONS rather than
//! just checking that some tables exist: a database migrated by an older
//! build has every table a smoke test would look for, so the failure mode is
//! scattered "column does not exist" 500s at request time, hours later,
//! instead of a refusal at boot. Concretely this is what an operator sees if
//! they deploy with the release command skipped or disabled
//! (`fly deploy --no-release-command`) — the deploy's own abort-on-failure
//! guarantee is gone, and this preflight is the last line.
//!
//! Note the honest limitation this test also documents: the version
//! comparison is only possible while the boot role can READ
//! `_sqlx_migrations`. Post-OPS-4, `DATABASE_URL` is `coralswarm_app`, which
//! migration 0083 revokes that read from, and the preflight degrades to a
//! presence check (see `verify_control_schema`). So this test deliberately
//! boots with the OWNER DSN — it covers the strict path, and the deploy
//! pipeline's ordering is what covers the app-role path.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same convention as `consumer_rls_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test migrator_dsn_stale_pg

use std::borrow::Cow;

use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
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

/// Apply the control tree only UP TO (excluding) `before_version`, through the
/// real sqlx migrator (so `-- no-transaction` migrations run correctly and the
/// ledger stays valid). Same helper shape as `mcp_governance_pg.rs`.
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
async fn run_refuses_to_serve_a_database_that_is_behind_this_build() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping run_refuses_to_serve_a_database_that_is_behind_this_build: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_migrator_dsn_stale";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);
    let head = coralswarm_backend::control_head_version();

    // Everything except the head migration — a database one deploy behind.
    let owner: PgPool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&owner_url)
        .await
        .expect("connect owner");
    migrate_control_until(&owner, head).await;
    owner.close().await;

    unsafe {
        std::env::set_var("DATABASE_URL", &owner_url);
        std::env::remove_var("MIGRATOR_DATABASE_URL");
    }

    // Not spawned: the preflight runs before any listener bind.
    let result = coralswarm_backend::run().await;

    unsafe {
        std::env::remove_var("DATABASE_URL");
    }

    let err = result.expect_err(
        "run() must refuse to serve a database whose applied control version is behind the \
         version this binary's queries assume",
    );
    assert!(
        err.contains("BEHIND"),
        "expected an explicit behind-this-build diagnosis, got: {err}"
    );
    assert!(
        err.contains(&format!("v{head}")),
        "the error must state the version this build expects (v{head}), got: {err}"
    );
    assert!(
        err.contains("migrate_control"),
        "the error must name the step the operator has to run, got: {err}"
    );

    drop_db(&mut admin_conn, db).await;
}
