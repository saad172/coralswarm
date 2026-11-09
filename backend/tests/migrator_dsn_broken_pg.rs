//! Integration test for the boot-time CONTROL-schema preflight —
//! the UNMIGRATED-database case.
//!
//! See `migrator_dsn_fixed_pg.rs`'s module doc for full background, and for
//! why this negative case lives in its own file/binary (each in-process
//! `coralswarm_backend::run()` call sets the global `tracing` subscriber
//! exactly once via `init_tracing()`; a second call in the same process
//! panics on it).
//!
//! Why this test exists: the serving process no longer applies migrations at
//! boot (that moved to `bin/migrate_control.rs`, the Fly deploy
//! `release_command`). Removing migrate-on-boot removes an accidental safety
//! net — a server pointed at a database nobody migrated used to fix itself.
//! It must NOT instead boot happily and then 500 every request against a
//! schema-less database. `run()`'s read-only preflight
//! (`verify_control_schema`) is what replaces that net: it fails FAST, with a
//! message that names the missing step, before any listener is bound.
//!
//! This is also the most likely DEV failure ("I created a database and ran
//! `cargo run`"), so the assertion checks the error actually points at the
//! remedy rather than just being non-empty.
//!
//! Gated on `ORGT_TEST_BASE_URL`, same convention as `consumer_rls_pg.rs`:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test migrator_dsn_broken_pg

use sqlx::{Connection, Executor, PgConnection};

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

#[tokio::test]
async fn run_fails_fast_with_a_diagnosable_error_against_a_never_migrated_database() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping run_fails_fast_with_a_diagnosable_error_against_a_never_migrated_database: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_migrator_dsn_broken";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
    let mut admin_conn = admin(&base).await;
    // Deliberately NOT migrated: an empty database, exactly what an operator
    // who skipped the release command (or a dev who skipped
    // `cargo run --bin migrate_control`) would be pointing at.
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    recreate(&mut admin_conn, db).await;
    let owner_url = with_db(&base, db);

    unsafe {
        std::env::set_var("DATABASE_URL", &owner_url);
        std::env::remove_var("MIGRATOR_DATABASE_URL");
    }

    // Not spawned: the preflight runs before any listener bind, so a direct
    // `.await` returns `Err` without ever leaving a socket open.
    let result = coralswarm_backend::run().await;

    unsafe {
        std::env::remove_var("DATABASE_URL");
    }

    let err = result.expect_err(
        "run() must return Err, not hang and not boot into a broken serving state, when the \
         database has never been migrated",
    );
    assert!(
        err.contains("control schema is MISSING"),
        "expected an explicit missing-schema diagnosis, got: {err}"
    );
    assert!(
        err.contains("migrate_control"),
        "the error must name the step the operator/dev has to run, got: {err}"
    );
    assert!(
        err.contains(&coralswarm_backend::control_head_version().to_string()),
        "the error must state the schema version this build expects, got: {err}"
    );

    drop_db(&mut admin_conn, db).await;
}
