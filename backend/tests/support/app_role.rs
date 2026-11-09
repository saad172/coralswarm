//! Shared, retry-safe way to set the `coralswarm_app` role's test-only
//! password on a fresh `migrations_control`-built scratch database.
//!
//! **Why this is the ONE copy, not another.** `ALTER ROLE ... WITH
//! PASSWORD` targets `pg_authid`, a CLUSTER-WIDE shared catalog — not
//! per-database — so any two callers racing it (two `#[tokio::test]`
//! functions in one binary, which Rust's default test harness runs
//! concurrently unless `--test-threads=1`; or two DIFFERENT `*_pg.rs` test
//! binaries whose runs overlap) can hit Postgres's own concurrent-DDL
//! signature on a shared catalog (`XX000` "tuple concurrently updated").
//! `journal_rollups_pg.rs`'s own copy of this ALTER ROLE call was raw and
//! unretried (CodeRabbit Major on PR #454's first review) — this module is
//! this tree's `coralswarm_app` analogue of `support::tenant_role`'s
//! existing `coralswarm_tenant_app` helper (see that module's doc for the
//! fuller "four independent copies" history this is deliberately not
//! repeating for the `coralswarm_app` role too).
//!
//! Every `*_pg.rs` suite in this tree that sets `coralswarm_app`'s password
//! is a SEPARATE pre-existing copy of the same raw statement (see
//! `consumer_rls_pg.rs`, `store_scoped_chunk2_pg.rs`,
//! `store_scoped_chunk3_pg.rs`, `bootstrap_scope_pg.rs`,
//! `admin_backfill_rls_pg.rs`, `migrator_dsn_fixed_pg.rs`,
//! `migrator_dsn_legacy_pg.rs`, `org_slug_rename_pg.rs`,
//! `support::rls_probe`) — consolidating ALL of those onto this helper is a
//! separate follow-up, out of scope for the fix this module exists for
//! (`journal_rollups_pg.rs`'s own copy).

use sqlx::PgPool;

/// The shared test-only password [`set_app_role_password_with_retry`] sets
/// on `coralswarm_app`. Not a secret (test-only, scratch databases dropped
/// at teardown) — a fixed, well-known value so every caller of this helper
/// agrees on it without re-declaring it.
pub const APP_ROLE_TEST_PASSWORD: &str = "orgt_app_role_shared_test_only_pw";

/// `ALTER ROLE coralswarm_app WITH PASSWORD ...`, retried against the exact
/// shared-catalog race `support::tenant_role::set_app_role_password_with_retry`
/// documents and catches for `coralswarm_tenant_app` — same three SQLSTATEs,
/// same reasoning: `23505` unique_violation and `42710` duplicate_object
/// (two sessions racing the same catalog write, reported under either code
/// depending on which index/path catches it first) and `XX000`
/// internal_error ("tuple concurrently updated" — Postgres's shared-catalog
/// concurrent-update signature). Deliberately not `WHEN OTHERS`, for the
/// same reason: that would also swallow a real configuration/typo error
/// behind silent retries.
pub async fn set_app_role_password_with_retry(pool: &PgPool) {
    let mut attempt: u32 = 0;
    loop {
        let result = sqlx::query(&format!(
            "ALTER ROLE coralswarm_app WITH PASSWORD '{APP_ROLE_TEST_PASSWORD}'"
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
                        "set test-only password on coralswarm_app: exhausted retries on a \
                         shared-catalog race ({}): {e}",
                        e.code().as_deref().unwrap_or("?")
                    );
                }
                // Jittered backoff, same shape as `tenant_role`'s loop — the
                // random component keeps two racing attempts from retrying
                // in lockstep and colliding again on the same pass.
                let jitter_ms = (attempt as u64 * 20) + (rand_jitter_ms());
                tokio::time::sleep(std::time::Duration::from_millis(jitter_ms)).await;
            }
            Err(e) => panic!("set test-only password on coralswarm_app failed: {e}"),
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
