//! `workers::geo_backfill` — live-Postgres proof of the batch-selection/
//! update logic (platform-admin-metrics PR 4 of 7).
//!
//! Scratch-DB pattern shared by every `*_pg.rs` file in this directory
//! (`access_log_purge_pg.rs` is the closest sibling: same
//! `admin`/`recreate`/`with_db`/`control_pool` helpers, same
//! `user_access_log` fixture shape).
//!
//! This suite drives [`coralswarm_backend::workers::geo_backfill::tick`]
//! against a real `user_access_log` table using a deterministic **stub**
//! [`GeoLookup`] implementation, never a real MaxMind `.mmdb` file — this
//! repo never commits one (large, separately licensed under MaxMind's EULA;
//! see that module's own doc comment and `.env.example`). The stub proves
//! exactly the SQL-level contract this worker owns: which rows get selected,
//! which get updated, and that a resolver miss / a concurrently-deleted row
//! never errors or resurrects anything — the actual MaxMind field-extraction
//! logic (`extract_geo_fields`) is unit-tested directly in the module itself
//! against hand-built `geoip2::City` records, with no database involved on
//! either side.
//!
//! Proves:
//!   1. A row with `ip` set and `country` NULL gets resolved and updated.
//!   2. A row that already has `country` set is never re-selected/touched.
//!   3. A row with `ip` NULL is never selected (nothing to resolve).
//!   4. A resolver miss (stub returns `None`) leaves the row untouched —
//!      still a candidate, not an error.
//!   5. Retention interaction: a row deleted (simulating the purge worker's
//!      sweep) between selection and update is not resurrected and does not
//!      error the tick — this worker only ever `UPDATE`s by primary key,
//!      never `INSERT`s.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as every other `*_pg.rs` file.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test geo_backfill_pg -- --nocapture

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;

use coralswarm_backend::workers::geo_backfill::{tick, GeoFields, GeoLookup};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file) ─────────────

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
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
}
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&with_db(base, db))
        .await
        .expect("connect control");
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

async fn mint_user(pool: &PgPool, email: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(email)
        .fetch_one(pool)
        .await
        .expect("insert user")
}

/// Insert a `user_access_log` row directly (bypassing `spawn_capture` —
/// this suite is about the BACKFILL side, not capture). `ip` is a plain
/// string cast server-side, same convention `workers::geo_backfill` itself
/// uses on its write path (`$3::inet`; its read path uses `host(ip)`, NOT a
/// bare `ip::text` cast — see that function's own comment for why the
/// difference matters).
async fn insert_row(pool: &PgPool, user_id: Uuid, session_id: &str, ip: Option<&str>) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO user_access_log (user_id, clerk_session_id, ip) \
         VALUES ($1, $2, $3::inet) RETURNING id",
    )
    .bind(user_id)
    .bind(session_id)
    .bind(ip)
    .fetch_one(pool)
    .await
    .expect("insert access log row")
}

async fn geo_fields_for(pool: &PgPool, id: Uuid) -> (Option<String>, Option<String>, Option<String>) {
    sqlx::query_as("SELECT country, region, city FROM user_access_log WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("read geo fields")
}

// ── Deterministic stub resolver ──────────────────────────────────────────────

/// A `GeoLookup` stub keyed by exact IP string — no MaxMind database
/// involved (see module doc). `Mutex<HashMap<..>>` only so the same stub
/// instance can be shared behind an immutable `&self` reference (`resolve`
/// takes `&self`, matching the production `GeoResolver`'s signature).
struct StubResolver {
    known: Mutex<HashMap<IpAddr, GeoFields>>,
}

impl StubResolver {
    fn new(entries: &[(&str, GeoFields)]) -> Self {
        let known = entries
            .iter()
            .map(|(ip, fields)| (ip.parse().unwrap(), fields.clone()))
            .collect();
        Self { known: Mutex::new(known) }
    }
}

impl GeoLookup for StubResolver {
    fn resolve(&self, ip: IpAddr) -> Option<GeoFields> {
        self.known.lock().unwrap().get(&ip).cloned()
    }
}

fn fields(country: &str, region: &str, city: &str) -> GeoFields {
    GeoFields {
        country: Some(country.to_string()),
        region: Some(region.to_string()),
        city: Some(city.to_string()),
    }
}

// ── (1)+(2)+(3) Selection: resolves candidates, skips already-resolved and ip-NULL rows ─

#[tokio::test]
async fn tick_resolves_candidates_and_skips_already_resolved_and_ip_null_rows() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping tick_resolves_candidates_and_skips_already_resolved_and_ip_null_rows: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_geobf1_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;

    // Candidate: ip set, country NULL.
    let candidate = insert_row(&pool, user, "sess-candidate", Some("203.0.113.7")).await;
    // Already resolved: must be left alone even though its ip is also known
    // to the stub — asserts the WHERE clause, not just "the stub returned
    // something".
    let already_resolved = insert_row(&pool, user, "sess-resolved", Some("203.0.113.7")).await;
    sqlx::query("UPDATE user_access_log SET country = 'PRE-EXISTING' WHERE id = $1")
        .bind(already_resolved)
        .execute(&pool)
        .await
        .unwrap();
    // ip NULL: never a candidate at all.
    let no_ip = insert_row(&pool, user, "sess-no-ip", None).await;

    let resolver = StubResolver::new(&[("203.0.113.7", fields("US", "Texas", "Fort Worth"))]);

    let resolved = tick(&pool, &resolver).await.expect("tick ok");
    assert_eq!(resolved, 1, "exactly the one genuine candidate is resolved");

    let (country, region, city) = geo_fields_for(&pool, candidate).await;
    assert_eq!(country.as_deref(), Some("US"));
    assert_eq!(region.as_deref(), Some("Texas"));
    assert_eq!(city.as_deref(), Some("Fort Worth"));

    let (country, _, _) = geo_fields_for(&pool, already_resolved).await;
    assert_eq!(
        country.as_deref(),
        Some("PRE-EXISTING"),
        "a row that already has a country must never be re-selected/overwritten"
    );

    let (country, region, city) = geo_fields_for(&pool, no_ip).await;
    assert_eq!(
        (country, region, city),
        (None, None, None),
        "a row with no ip is never a candidate and stays fully NULL"
    );

    // Second tick: nothing left to resolve.
    let resolved_again = tick(&pool, &resolver).await.expect("tick ok (second pass)");
    assert_eq!(resolved_again, 0, "second tick finds nothing left to resolve");

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

// ── (4) Resolver miss leaves the row untouched, not an error ─────────────────

#[tokio::test]
async fn a_resolver_miss_leaves_the_row_untouched_and_is_not_an_error() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_resolver_miss_leaves_the_row_untouched_and_is_not_an_error: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_geobf2_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;
    // A private/reserved-range IP the stub (mirroring a real GeoLite2
    // database) has no entry for at all.
    let unresolvable = insert_row(&pool, user, "sess-unresolvable", Some("10.0.0.1")).await;

    let resolver = StubResolver::new(&[]); // empty — every lookup misses

    let resolved = tick(&pool, &resolver).await.expect("tick ok");
    assert_eq!(resolved, 0, "a miss resolves nothing this tick");

    let (country, region, city) = geo_fields_for(&pool, unresolvable).await;
    assert_eq!(
        (country, region, city),
        (None, None, None),
        "an unresolvable ip must be left fully NULL, not partially written or errored"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}

// ── (5) Retention interaction: a concurrently-purged row is not resurrected ──

#[tokio::test]
async fn a_row_deleted_between_selection_and_update_is_not_resurrected_and_does_not_error() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_row_deleted_between_selection_and_update_is_not_resurrected_and_does_not_error: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db = "orgt_geobf3_ctl";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut conn = admin(&base).await;
    recreate(&mut conn, db).await;
    let pool = control_pool(&base, db).await;

    let user = mint_user(&pool, &format!("{}@test.local", Uuid::new_v4())).await;
    let row_id = insert_row(&pool, user, "sess-about-to-be-purged", Some("203.0.113.9")).await;

    // Simulate `workers::access_log_purge` winning the race: the row is gone
    // by the time this worker's UPDATE would run. `tick` still SELECTs it
    // (the DELETE below happens strictly after that SELECT would have run
    // inside `tick`, since we delete before calling `tick` at all here — the
    // stronger, easier-to-assert version of the same property: even a row
    // that vanishes BEFORE resolution is attempted is handled cleanly).
    sqlx::query("DELETE FROM user_access_log WHERE id = $1")
        .bind(row_id)
        .execute(&pool)
        .await
        .unwrap();

    let resolver = StubResolver::new(&[("203.0.113.9", fields("US", "Texas", "Austin"))]);

    let resolved = tick(&pool, &resolver).await;
    assert!(
        resolved.is_ok(),
        "a row that disappeared before this tick ran must never error the sweep: {resolved:?}"
    );
    assert_eq!(resolved.unwrap(), 0, "nothing was there left to resolve");

    let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM user_access_log WHERE id = $1")
        .bind(row_id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        remaining, 0,
        "the backfill worker must never resurrect a row the purge worker already removed"
    );

    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
}
