//! Consumed-`jti` replay store for signed OAuth `state` tokens (CS-110
//! follow-up, foundation train PR 1.4, `.scratch/m365-enterprise/plan.md`).
//!
//! # Why this exists
//!
//! [`super::state::verify_state`] proves a `state` token is authentic and
//! unexpired, but a signed, still-valid token can be replayed verbatim any
//! number of times within its whole `STATE_TTL_SECS` (600s) validity window
//! — `state.rs`'s own `jti` field used to be "defence-in-depth; not yet
//! checked against a store". This module IS that store: [`consume`] claims a
//! `jti` exactly once via `oauth_state_jti` (migration `0173`), so a second
//! presentation of the same signed token is rejected even though its
//! signature and expiry still check out fine.
//!
//! # Atomicity: one `INSERT ... ON CONFLICT DO NOTHING`, never check-then-insert
//!
//! [`consume`] is a single statement — no separate `SELECT` followed by an
//! `INSERT`. A check-then-insert shape would leave a TOCTOU gap: two
//! concurrent callback requests racing the SAME jti could both observe
//! "not yet consumed" before either one's insert commits, and both would
//! proceed. `ON CONFLICT (jti) DO NOTHING` closes that gap at the database
//! level — Postgres itself serializes the two inserts, so exactly one of
//! them affects a row. Same idiom `routes::stripe_webhook` already uses for
//! `stripe_webhook_events` (see that module's doc, "claimed == 0 -> replay"
//! and "a concurrent duplicate delivery blocks on the uncommitted insert and
//! then reads zero rows, so the serialization is Postgres's rather than
//! ours").
//!
//! # Fail-closed
//!
//! [`consume`] returns `Result<bool, sqlx::Error>`, not a bare `bool` that
//! collapses "already consumed" and "store unreachable" into the same
//! answer. Callers MUST treat `Err` as a rejection, exactly like "already
//! consumed" — never as "unclaimed, so let it through". Failing open here
//! would mean a flaky replay store silently disables replay protection
//! entirely for as long as it stays unreachable, which is a worse outcome
//! than the callback returning `503` for legitimate connect attempts during
//! that window.
//!
//! # TTL sweep
//!
//! `backend/src/workers/oauth_state_jti_sweep.rs` purges rows older than
//! `STATE_TTL_SECS` plus a clock-skew buffer — see that module's own doc for
//! why that is always safe (a consumed row that old can never again match a
//! still-verifiable token).

use sqlx::PgPool;

/// Atomically claim `jti` against the consumed-state-jti store.
///
/// Returns:
/// - `Ok(true)` — this call claimed the jti for the first time; the caller
///   may proceed.
/// - `Ok(false)` — `jti` was already claimed (replay); the caller MUST
///   reject.
/// - `Err(_)` — the store is unreachable. The caller MUST fail closed
///   (reject), never treat this the same as `Ok(true)` — see this module's
///   own "Fail-closed" doc section.
pub async fn consume(pool: &PgPool, jti: &str) -> Result<bool, sqlx::Error> {
    let result =
        sqlx::query("INSERT INTO oauth_state_jti (jti) VALUES ($1) ON CONFLICT (jti) DO NOTHING")
            .bind(jti)
            .execute(pool)
            .await?;
    Ok(result.rows_affected() == 1)
}

// ── Tests ─────────────────────────────────────────────────────────────────────
//
// Gated on `RELINFER_TEST_DATABASE_URL`, same convention as
// `workers::usage_pending_sweep`'s own live tests — a plain `cargo test` with
// no DB configured skips these (prints and returns rather than failing).
#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    async fn test_pool_or_skip(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run {test_name}");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// **Headline test (PR 1.4 task spec): the second consume of the same
    /// jti rejects.** Proves the atomic check-and-consume contract directly,
    /// not just via the callback's HTTP shell.
    #[tokio::test]
    async fn second_consume_of_same_jti_rejects() {
        let Some(pool) = test_pool_or_skip("second_consume_of_same_jti_rejects").await else {
            return;
        };
        let jti = Uuid::new_v4().to_string();

        assert!(
            consume(&pool, &jti).await.expect("first consume"),
            "the first consume of a fresh jti must claim it"
        );
        assert!(
            !consume(&pool, &jti).await.expect("second consume"),
            "a second consume of the SAME jti must be rejected as a replay"
        );

        sqlx::query("DELETE FROM oauth_state_jti WHERE jti = $1")
            .bind(&jti)
            .execute(&pool)
            .await
            .ok();
    }

    /// Negative control paired with the test above: two DIFFERENT jtis each
    /// claim independently. Without this, the replay rejection above could
    /// pass simply because `consume` always rejects — this proves it only
    /// rejects the exact jti already claimed.
    #[tokio::test]
    async fn distinct_jtis_each_claim_independently() {
        let Some(pool) = test_pool_or_skip("distinct_jtis_each_claim_independently").await else {
            return;
        };
        let jti_a = Uuid::new_v4().to_string();
        let jti_b = Uuid::new_v4().to_string();

        assert!(consume(&pool, &jti_a).await.expect("consume a"));
        assert!(
            consume(&pool, &jti_b).await.expect("consume b"),
            "an unrelated jti must not be blocked by a different jti already consumed"
        );

        sqlx::query("DELETE FROM oauth_state_jti WHERE jti = $1 OR jti = $2")
            .bind(&jti_a)
            .bind(&jti_b)
            .execute(&pool)
            .await
            .ok();
    }

    /// **Concurrency proof (PR 1.4 task spec: atomic, not check-then-insert).**
    /// Fires many concurrent `consume` calls at the SAME jti and asserts
    /// exactly one succeeds — a check-then-insert implementation would let
    /// more than one through under real concurrency; a single `ON CONFLICT`
    /// statement cannot.
    #[tokio::test]
    async fn concurrent_consumes_of_the_same_jti_only_one_wins() {
        let Some(pool) =
            test_pool_or_skip("concurrent_consumes_of_the_same_jti_only_one_wins").await
        else {
            return;
        };
        let jti = Uuid::new_v4().to_string();

        let mut handles = Vec::new();
        for _ in 0..10 {
            let pool = pool.clone();
            let jti = jti.clone();
            handles.push(tokio::spawn(async move { consume(&pool, &jti).await }));
        }

        let mut claimed_count = 0;
        for h in handles {
            if h.await.expect("task join").expect("consume call") {
                claimed_count += 1;
            }
        }

        assert_eq!(
            claimed_count, 1,
            "exactly one of the concurrent consumes must claim the jti"
        );

        sqlx::query("DELETE FROM oauth_state_jti WHERE jti = $1")
            .bind(&jti)
            .execute(&pool)
            .await
            .ok();
    }
}
