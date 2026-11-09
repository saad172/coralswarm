//! Shared, retry-safe way to set the `coralswarm_tenant_app` role's test-only
//! password on a fresh `migrations_tenant`-built scratch database.
//!
//! **Why this is the ONE copy, not a fourth.** `ALTER ROLE ... WITH PASSWORD`
//! targets `pg_authid`, a CLUSTER-WIDE shared catalog — not per-database — so
//! any two callers racing it (two `#[tokio::test]` functions in one binary,
//! which Rust's default test harness runs concurrently by default; or two
//! DIFFERENT `*_pg.rs` test binaries running in parallel `cargo test` jobs)
//! can hit the exact same shared-catalog race Postgres's own concurrent-DDL
//! behavior produces. This is not theoretical: `migrations_tenant/
//! 0009_tenant_rls.sql`'s own `CREATE ROLE` step already needed a retry loop
//! for it (hit in CI on concurrent `CREATE ROLE`, `23505`), `0083_consumer_rls`
//! was found to carry the identical latent risk, `tenant_rls_phase2_pg.rs`
//! needed it for its own two tests, and `tenant_rls_connectors_pg.rs` hit it
//! independently before this module existed (CodeRabbit Major — "the fourth
//! time today"). Four independent copies of the same workaround is how a
//! fifth gets forgotten; every `*_pg.rs` suite that sets this password calls
//! THIS function.
//!
//! `pg_advisory_xact_lock` is per-database (its lock tag includes the database
//! OID — the same caveat 0009's migration header calls out about its own
//! advisory lock), so it cannot serialize two DIFFERENT scratch databases
//! against each other here either; the retry below is what actually protects
//! this, exactly as 0009's own header says about its migration-time
//! equivalent.

use sqlx::PgPool;

/// The shared test-only password every `*_pg.rs` suite in this tree sets on
/// `coralswarm_tenant_app` before connecting as it. Not a secret (test-only,
/// scratch databases dropped at teardown) — shared so every caller of
/// [`set_app_role_password_with_retry`] and every DSN built with it agree on
/// the same value without re-declaring it.
pub const APP_ROLE_TEST_PASSWORD: &str = "orgt_tenant_rls_test_only_pw";

/// `ALTER ROLE coralswarm_tenant_app WITH PASSWORD ...`, retried against the
/// exact shared-catalog race `migrations_tenant/0009_tenant_rls.sql`'s own
/// `CREATE ROLE` loop documents and catches. Catches the identical three
/// SQLSTATEs for the identical reason: `23505` unique_violation and `42710`
/// duplicate_object (two sessions racing the same catalog write, reported
/// under either code depending on which index/path catches it first) and
/// `XX000` internal_error ("tuple concurrently updated" — Postgres's
/// shared-catalog concurrent-update signature). Deliberately not
/// `WHEN OTHERS`, for the same reason 0009's loop isn't: that would also
/// swallow a real configuration/typo error behind silent retries.
pub async fn set_app_role_password_with_retry(pool: &PgPool) {
    let mut attempt: u32 = 0;
    loop {
        let result = sqlx::query(&format!(
            "ALTER ROLE coralswarm_tenant_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
        ))
        .execute(pool)
        .await;

        match result {
            Ok(_) => return,
            Err(sqlx::Error::Database(e))
                if matches!(
                    e.code().as_deref(),
                    Some("23505") | Some("42710") | Some("XX000")
                ) =>
            {
                attempt += 1;
                if attempt >= 10 {
                    panic!(
                        "set test-only password: exhausted retries on a shared-catalog race \
                         ({}): {e}",
                        e.code().as_deref().unwrap_or("?")
                    );
                }
                // Jittered backoff, same shape as the migration's own loop —
                // the random component keeps two racing attempts from
                // retrying in lockstep and colliding again on the same pass.
                let jitter_ms = (attempt as u64 * 20) + (rand_jitter_ms());
                tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
            }
            Err(e) => panic!("set test-only password failed: {e}"),
        }
    }
}

/// Cheap, dependency-free jitter (0-19ms) — this module has no `rand` crate
/// dependency and doesn't need cryptographic randomness, just enough spread
/// that two concurrently-retrying callers don't resonate on the same delay.
fn rand_jitter_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64 % 20)
        .unwrap_or(0)
}
