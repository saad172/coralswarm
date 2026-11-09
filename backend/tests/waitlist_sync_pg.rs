//! `workers::waitlist_sync` — live-Postgres proof of the `waitlist_snapshot`
//! table + its bulk upsert (platform-admin-metrics PR 5 of 7, migration
//! `0096_waitlist_snapshot`).
//!
//! This suite deliberately does NOT stand up a mock Cloudflare D1 HTTP
//! endpoint — the D1-fetch/response-parsing half of `workers::waitlist_sync`
//! (`fetch_rows`/`parse_row`/`parse_signed_up_at`/`D1Config::from_env`) is
//! exhaustively unit-tested in that module's own `#[cfg(test)]` block without
//! a live server (no wiremock-style dependency in this crate). What genuinely
//! needs a real Postgres — and is what this file proves — is the OTHER half:
//! `workers::waitlist_sync::upsert_snapshot`, exercised directly against the
//! `waitlist_snapshot` table this PR's migration creates:
//!
//!   1. **Insert** — a fresh batch of rows lands correctly, including NULL
//!      `signed_up_at`/`utm_source`/`country` (pre-PR-#357 D1 rows / a
//!      `created_at` that failed to parse).
//!   2. **Upsert on re-sync** — `email_normalized` is the PK; re-running the
//!      upsert with a CHANGED `email_raw`/attribution for the same normalized
//!      email UPDATES the row in place (not a duplicate), and `synced_at`
//!      advances.
//!   3. **Case/whitespace-insensitive identity** — two rows differing only in
//!      raw casing/whitespace collapse to the SAME `email_normalized` PK
//!      (this worker's own normalization, proven at the DB layer here rather
//!      than only in the pure `parse_row` unit test).
//!
//! Gated on `ORGT_TEST_BASE_URL` — same convention as every other `*_pg.rs`
//! file here.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test waitlist_sync_pg -- --nocapture

use chrono::{TimeZone, Utc};
use coralswarm_backend::workers::waitlist_sync::{upsert_snapshot, SnapshotRow};
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
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
}
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new().max_connections(4).connect(&with_db(base, db)).await.expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

async fn snapshot_row(pool: &PgPool, email_normalized: &str) -> (String, Option<chrono::DateTime<Utc>>, Option<String>, Option<String>) {
    sqlx::query_as(
        "SELECT email_raw, signed_up_at, utm_source, country FROM waitlist_snapshot WHERE email_normalized = $1",
    )
    .bind(email_normalized)
    .fetch_one(pool)
    .await
    .expect("fetch waitlist_snapshot row")
}
async fn row_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM waitlist_snapshot").fetch_one(pool).await.expect("count")
}
async fn synced_at(pool: &PgPool, email_normalized: &str) -> chrono::DateTime<Utc> {
    sqlx::query_scalar("SELECT synced_at FROM waitlist_snapshot WHERE email_normalized = $1")
        .bind(email_normalized)
        .fetch_one(pool)
        .await
        .expect("fetch synced_at")
}

#[tokio::test]
async fn upsert_inserts_fresh_rows_including_nulls() {
    let Some(base) = base_url() else {
        eprintln!("skipping upsert_inserts_fresh_rows_including_nulls: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_ws1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let signed_up = Utc.with_ymd_and_hms(2026, 7, 20, 10, 15, 30).unwrap();
    let rows = vec![
        SnapshotRow {
            email_normalized: "full@test.dev".to_string(),
            email_raw: "Full@Test.dev".to_string(),
            signed_up_at: Some(signed_up),
            utm_source: Some("twitter".to_string()),
            country: Some("US".to_string()),
        },
        SnapshotRow {
            // Pre-PR-#357 D1 row: no attribution columns existed yet, and
            // `created_at` failed to parse (defensively None, never an error).
            email_normalized: "bare@test.dev".to_string(),
            email_raw: "bare@test.dev".to_string(),
            signed_up_at: None,
            utm_source: None,
            country: None,
        },
    ];

    let n = upsert_snapshot(&pool, &rows).await.expect("upsert succeeds");
    assert_eq!(n, 2);
    assert_eq!(row_count(&pool).await, 2);

    let (raw, sa, utm, country) = snapshot_row(&pool, "full@test.dev").await;
    assert_eq!(raw, "Full@Test.dev", "email_raw preserved verbatim");
    assert_eq!(sa, Some(signed_up));
    assert_eq!(utm.as_deref(), Some("twitter"));
    assert_eq!(country.as_deref(), Some("US"));

    let (raw2, sa2, utm2, country2) = snapshot_row(&pool, "bare@test.dev").await;
    assert_eq!(raw2, "bare@test.dev");
    assert_eq!(sa2, None, "unparseable/absent created_at stores NULL, never errors");
    assert_eq!(utm2, None);
    assert_eq!(country2, None);

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[tokio::test]
async fn resyncing_the_same_normalized_email_updates_in_place() {
    let Some(base) = base_url() else {
        eprintln!("skipping resyncing_the_same_normalized_email_updates_in_place: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_ws2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    upsert_snapshot(
        &pool,
        &[SnapshotRow {
            email_normalized: "person@test.dev".to_string(),
            email_raw: "person@test.dev".to_string(),
            signed_up_at: Some(Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap()),
            utm_source: None,
            country: None,
        }],
    )
    .await
    .expect("first sync");
    assert_eq!(row_count(&pool).await, 1);
    let synced_1 = synced_at(&pool, "person@test.dev").await;

    tokio::time::sleep(std::time::Duration::from_millis(20)).await;

    // Re-sync: D1 now has attribution columns populated for this same row
    // (utm_source arrived after PR #357 merged, say) — must UPDATE, not
    // insert a second row.
    upsert_snapshot(
        &pool,
        &[SnapshotRow {
            email_normalized: "person@test.dev".to_string(),
            email_raw: "person@test.dev".to_string(),
            signed_up_at: Some(Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap()),
            utm_source: Some("newsletter".to_string()),
            country: Some("CA".to_string()),
        }],
    )
    .await
    .expect("second sync");

    assert_eq!(row_count(&pool).await, 1, "re-sync of the same normalized email must not duplicate");
    let (_, _, utm, country) = snapshot_row(&pool, "person@test.dev").await;
    assert_eq!(utm.as_deref(), Some("newsletter"), "attribution added on re-sync is reflected");
    assert_eq!(country.as_deref(), Some("CA"));
    let synced_2 = synced_at(&pool, "person@test.dev").await;
    assert!(synced_2 > synced_1, "synced_at must advance on every successful sync pass");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

#[tokio::test]
async fn casing_and_whitespace_variants_collapse_to_one_normalized_row() {
    let Some(base) = base_url() else {
        eprintln!("skipping casing_and_whitespace_variants_collapse_to_one_normalized_row: set ORGT_TEST_BASE_URL");
        return;
    };
    let db = "orgt_ws3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    // Two raw D1 rows a real sync would normalize to the SAME
    // `email_normalized` (matching `parse_row`'s
    // `email.trim().to_lowercase()` — proven at the unit level; this proves
    // the PK actually enforces the collapse at the DB layer too).
    let rows = vec![
        SnapshotRow {
            email_normalized: "same@test.dev".to_string(),
            email_raw: "Same@Test.dev".to_string(),
            signed_up_at: None,
            utm_source: None,
            country: None,
        },
        SnapshotRow {
            email_normalized: "same@test.dev".to_string(),
            email_raw: " same@test.dev ".to_string(),
            signed_up_at: None,
            utm_source: Some("google".to_string()),
            country: None,
        },
    ];

    upsert_snapshot(&pool, &rows).await.expect("upsert both rows in one batch — must not hard-error on the shared conflict target");
    assert_eq!(row_count(&pool).await, 1, "both normalize to the same PK — exactly one row");
    // `upsert_snapshot` dedupes by `email_normalized` BEFORE building the
    // batch, last-wins (see its own doc comment) — deterministically the
    // SECOND row in the input slice, regardless of HashMap iteration order.
    let (raw, _, utm, _) = snapshot_row(&pool, "same@test.dev").await;
    assert_eq!(raw, " same@test.dev ", "last-wins: the second input row's raw email survives");
    assert_eq!(utm.as_deref(), Some("google"), "last-wins: the second input row's attribution survives");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}
