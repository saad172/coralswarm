//! Live regression suite for migration `0155_billing_plans_pricing.sql`
//! (real-pricing-and-limits PR) — proves the three new `plans` columns
//! (`price_usd_cents`, `is_seat_priced`, `min_seats`) and the seeded
//! registered tier ladder (`pro`/`team_standard`/`team_premium`) are exactly
//! what the migration's own header claims.
//!
//! Proven here against REAL Postgres:
//!
//!   1. `plans_seat_minimum_requires_seat_pricing` actually REJECTS a
//!      `min_seats` value on a flat-rate (`is_seat_priced = false`) plan —
//!      the CHECK this migration adds specifically to make that combination
//!      structurally impossible.
//!   2. The three seeded rows (`pro`, `team_standard`, `team_premium`) exist,
//!      satisfy `plans_period_length_matches_refresh_kind` (they would not
//!      have been insertable by the migration itself if they didn't — this
//!      test re-reads them back to prove the seed committed with the exact
//!      values the header documents, not merely that the migration ran
//!      without erroring), and `price_usd_cents` round-trips losslessly.
//!   3. `free`/`founder_comp` (seeded in `0153`, untouched by `0155`) still
//!      have `price_usd_cents IS NULL` — the migration must not have
//!      accidentally priced the two non-purchasable plans.
//!
//! Negative control (re-run by hand, fails as designed): drop
//! `plans_seat_minimum_requires_seat_pricing` (`ALTER TABLE plans DROP
//! CONSTRAINT plans_seat_minimum_requires_seat_pricing;`) and re-run test 1 —
//! the INSERT that test expects to fail instead SUCCEEDS, and the test's own
//! `assert!(insert.is_err(), ...)` fails with "a min_seats value on a
//! flat-rate plan was accepted". Reproduced live while authoring this file
//! (see the PR body / session report for the exact `psql` transcript) before
//! the CHECK was added to the migration.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with `migrations_control`
//! applied through at least `0155`) — same convention as
//! `candidates_keyset_pg.rs`; a normal `cargo test` with no DB configured
//! SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/billing_real_pricing_test \
//!     cargo test -p coralswarm-backend --test billing_plans_seed_pg -- --nocapture

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live \
     billing-plans-seed suite (a Postgres with migrations_control applied through 0155)";

async fn pool(url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .expect("connect to test Postgres")
}

/// **Test 1 — the load-bearing negative control.** A `min_seats` value on a
/// flat-rate (`is_seat_priced = false`) plan must be REJECTED by
/// `plans_seat_minimum_requires_seat_pricing`, not silently accepted.
#[tokio::test]
async fn seat_minimum_on_a_flat_rate_plan_is_rejected() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let code = format!("test_bad_flat_seats_{}", uuid::Uuid::new_v4().simple());

    let insert = sqlx::query(
        "INSERT INTO plans (code, name, credits, price_usd_cents, is_seat_priced, min_seats, \
         refresh_kind, period_length) \
         VALUES ($1, 'Bad flat+seats', 100, 500, false, 3, 'anniversary', INTERVAL '1 month')",
    )
    .bind(&code)
    .execute(&pool)
    .await;

    assert!(
        insert.is_err(),
        "a min_seats value on a flat-rate (is_seat_priced = false) plan was accepted — \
         plans_seat_minimum_requires_seat_pricing did not fire"
    );
    let msg = insert.unwrap_err().to_string();
    assert!(
        msg.contains("plans_seat_minimum_requires_seat_pricing"),
        "insert was rejected, but not by the expected constraint: {msg}"
    );

    // Sanity: the same row IS accepted once is_seat_priced flips to true —
    // proves the CHECK is scoped to the flat/seat-priced distinction, not
    // rejecting every min_seats value outright.
    let ok = sqlx::query(
        "INSERT INTO plans (code, name, credits, price_usd_cents, is_seat_priced, min_seats, \
         refresh_kind, period_length) \
         VALUES ($1, 'Good seat+min', 100, 500, true, 3, 'anniversary', INTERVAL '1 month')",
    )
    .bind(&code)
    .execute(&pool)
    .await;
    assert!(ok.is_ok(), "min_seats on a genuinely seat-priced plan must be accepted: {ok:?}");

    sqlx::query("DELETE FROM plans WHERE code = $1")
        .bind(&code)
        .execute(&pool)
        .await
        .expect("cleanup");
}

/// **Test 2.** The three registered tiers exist with exactly the values the
/// migration header documents, and `price_usd_cents` round-trips losslessly.
#[tokio::test]
async fn registered_tiers_seeded_with_exact_values() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    struct Expected {
        code: &'static str,
        credits: i64,
        price_usd_cents: i32,
        is_seat_priced: bool,
        min_seats: Option<i32>,
    }
    let expected = [
        Expected { code: "pro", credits: 1500, price_usd_cents: 2000, is_seat_priced: false, min_seats: None },
        Expected {
            code: "team_standard",
            credits: 3000,
            price_usd_cents: 4000,
            is_seat_priced: true,
            min_seats: Some(3),
        },
        Expected {
            code: "team_premium",
            credits: 15000,
            price_usd_cents: 17500,
            is_seat_priced: true,
            min_seats: None,
        },
    ];

    for e in expected {
        let row = sqlx::query(
            "SELECT credits::bigint AS credits, price_usd_cents, is_seat_priced, min_seats, \
             refresh_kind, period_length FROM plans WHERE code = $1",
        )
        .bind(e.code)
        .fetch_one(&pool)
        .await
        .unwrap_or_else(|err| panic!("plan {:?} not found: {err}", e.code));

        assert_eq!(row.get::<i64, _>("credits"), e.credits, "{}.credits", e.code);
        assert_eq!(
            row.get::<i32, _>("price_usd_cents"),
            e.price_usd_cents,
            "{}.price_usd_cents did not round-trip",
            e.code
        );
        assert_eq!(row.get::<bool, _>("is_seat_priced"), e.is_seat_priced, "{}.is_seat_priced", e.code);
        assert_eq!(
            row.get::<Option<i32>, _>("min_seats"),
            e.min_seats,
            "{}.min_seats",
            e.code
        );
        assert_eq!(row.get::<String, _>("refresh_kind"), "anniversary", "{}.refresh_kind", e.code);
        assert!(
            row.get::<Option<sqlx::postgres::types::PgInterval>, _>("period_length").is_some(),
            "{}.period_length must be set (anniversary requires it, per \
             plans_period_length_matches_refresh_kind — the row would not exist at all if this \
             failed at insert time, but re-reading it here proves the migration's own INSERT \
             actually satisfied that pre-existing CHECK rather than the migration silently \
             no-op'ing on ON CONFLICT)",
            e.code
        );
    }
}

/// **Test 3 — negative control on the non-purchasable plans.** `free`/
/// `founder_comp` (seeded in `0153`, untouched by `0155`) must still have
/// `price_usd_cents IS NULL` after this migration runs.
#[tokio::test]
async fn free_and_founder_comp_stay_unpriced() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    for code in ["free", "founder_comp"] {
        let price: Option<i32> = sqlx::query_scalar("SELECT price_usd_cents FROM plans WHERE code = $1")
            .bind(code)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|err| panic!("plan {code:?} not found: {err}"));
        assert!(
            price.is_none(),
            "{code}.price_usd_cents must stay NULL (not self-serve purchasable), got {price:?}"
        );
    }
}
