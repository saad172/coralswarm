//! Live test for `Store::meeting_occurred_at` (cs-candidate-meeting-labels)
//! against a REAL Postgres, which runs the `MIN(atoms.timestamp)` aggregate
//! query.
//!
//! Proves, against the database:
//!   * the earliest **event** timestamp among a meeting's chunk atoms
//!     (`source_id` prefix) is returned, not the latest / insertion order;
//!   * a `source_id` with no matching atoms returns `None`.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with all migrations applied);
//! a normal `cargo test` with no DB configured skips it.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:55432/coralswarm \
//!     cargo test -p coralswarm-backend --test meeting_occurred_at_pg -- --nocapture

use chrono::{Duration, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use sqlx::postgres::PgPoolOptions;
use uuid::Uuid;

#[tokio::test]
async fn meeting_occurred_at_returns_earliest_chunk_timestamp_and_none_when_unmatched() {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping: set RELINFER_TEST_DATABASE_URL to run the live meeting_occurred_at e2e");
            return;
        }
    };

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&url)
        .await
        .expect("connect to test Postgres");

    // ── Seed tenancy chain: user → account → graph ────────────────────────────
    let email = format!("{}@test.local", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(&pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(&pool)
            .await
            .expect("insert account");

    let store = PostgresStore::new(pool.clone());
    let graph = store
        .create_graph(account_id, "meeting-occurred-at-e2e", "personal")
        .await
        .expect("create graph");
    let g = graph.id;

    // ── Seed two chunk atoms sharing a source_id prefix, distinct timestamps,
    // deliberately inserted latest-first so a naive "first row" read would fail. ─
    let base = format!("meeting-{}", Uuid::new_v4());
    let earliest = Utc::now() - Duration::hours(2);
    let latest = Utc::now() - Duration::hours(1);

    for (chunk_ix, ts) in [(1, latest), (0, earliest)] {
        sqlx::query(
            "INSERT INTO atoms (graph_id, source_type, source_id, content_blob_key, timestamp) \
             VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(g)
        .bind("teams_transcript")
        .bind(format!("{base}:chunk:{chunk_ix}"))
        .bind("test-blob-key")
        .bind(ts)
        .execute(&pool)
        .await
        .expect("insert chunk atom");
    }

    // ── MIN timestamp across the meeting's chunk atoms is returned ───────────
    let occurred_at = store
        .meeting_occurred_at(g, &base)
        .await
        .expect("meeting_occurred_at query");
    assert_eq!(
        occurred_at.map(|t| t.timestamp_millis()),
        Some(earliest.timestamp_millis()),
        "must return the EARLIEST chunk timestamp, not the latest or insertion order"
    );

    // ── A source_id with no matching atoms returns None ──────────────────────
    let miss = store
        .meeting_occurred_at(g, &format!("unmatched-{}", Uuid::new_v4()))
        .await
        .expect("meeting_occurred_at query (miss)");
    assert_eq!(miss, None, "no matching atoms must return None");

    // ── Cleanup (cascades through graph → atoms) ──────────────────────────────
    sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id).execute(&pool).await.ok();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await.ok();

    println!("PG meeting_occurred_at e2e passed: earliest chunk timestamp returned, miss returns None");
}
