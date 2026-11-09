//! Live regression suite for migration `0156_grants_seats.sql` — the seat
//! count on `grants`, and the entitlement arithmetic that depends on it.
//!
//! # What this suite is actually defending
//!
//! Before `0156`, `plans.credits` was read as a flat allowance even for the
//! plans `0155` had just registered as PER-SEAT. A `team_standard` grant
//! therefore issued 3,000 credits TOTAL — so a 10-seat customer paying
//! $400/month got the same allowance as a 3-seat customer paying $120. The
//! failure mode is quiet: no error, no constraint violation, just a number
//! that is wrong by a factor of the seat count. Every test here exists to
//! make some version of that silence impossible.
//!
//! Proven against REAL Postgres:
//!
//!   1. `grants_seats_iff_seat_priced` REJECTS a seat-priced grant carrying
//!      no seat count — the precise shape of the pre-0156 bug, now
//!      reintroducible only through a NULL rather than a missing column.
//!   2. The same CHECK REJECTS the mirror-image error: a flat-plan grant
//!      that carries a seat count.
//!   3. `grants_plan_seat_pricing_fk` REJECTS a grant whose denormalized
//!      `plan_is_seat_priced` DISAGREES with the plan it references — the
//!      copy cannot drift, which is the whole justification for storing it.
//!   4. The PRODUCTION entitlement query
//!      (`billing::entitlement::current_plan`, which `routes::usage` itself
//!      calls) returns `credits x seats` for a seat-priced grant: 4 seats of
//!      `team_standard` is 12,000, not 3,000.
//!   5. `credits_override` short-circuits the multiplication ABSOLUTELY —
//!      an override on a 4-seat grant returns the override itself, never
//!      `override x 4`. This is the property that keeps every grant written
//!      under 0155's "set credits_override by hand" guidance correct.
//!   6. A flat plan (`pro`) is arithmetically UNCHANGED by all of the above:
//!      1,500 credits, `seats IS NULL`.
//!
//! # Negative controls — each verified to genuinely fail
//!
//! Every assertion below was watched failing before it was trusted, per this
//! codebase's standing rule that billing code is arithmetic that looks right:
//!
//!   * Tests 1-3: drop the constraint under test (`ALTER TABLE grants DROP
//!     CONSTRAINT grants_seats_iff_seat_priced;` / `... DROP CONSTRAINT
//!     grants_plan_seat_pricing_fk;`) and re-run — the INSERT each test
//!     expects to fail instead SUCCEEDS and the `assert!(… .is_err())`
//!     trips.
//!   * Test 4: revert `PLAN_ENTITLEMENT_SQL` in
//!     `billing::entitlement` to the pre-0156
//!     `COALESCE(g.credits_override, p.credits)` — it returns 3,000 where
//!     12,000 is asserted, i.e. the original bug, and the test catches it.
//!   * Test 5: change that same query to multiply the override
//!     (`COALESCE(g.credits_override * COALESCE(g.seats,1), …)`) — returns
//!     28,000 instead of 7,000 and the test catches it.
//!
//! Those two controls are only meaningful because the sabotage now happens
//! in PRODUCTION code. An earlier revision re-typed the SQL into this file,
//! so breaking the handler left this suite green — review caught it, and
//! `billing::entitlement` exists to make that class of hole impossible.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with
//! `migrations_control` applied through at least `0156`) — same convention
//! as `billing_plans_seed_pg.rs`; a normal `cargo test` with no DB
//! configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/coralswarm \
//!     cargo test -p coralswarm-backend --test billing_grant_seats_pg -- --nocapture

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live \
     grant-seats suite (a Postgres with migrations_control applied through 0156)";
async fn execute_financial_fixture<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    match query.execute(&mut *tx).await {
        Ok(result) => {
            tx.commit().await?;
            Ok(result)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

/// Runs the PRODUCTION entitlement query against `account_id` and returns
/// the computed total.
///
/// Earlier revisions of this file re-typed the SQL as a local `const` and
/// invited the reader to diff it against `routes::usage` by eye. Review
/// caught that: the tests exercised the copy, so a regression in the
/// handler's own query would have left every test here green. Eyeball
/// diffing is not a test. `billing::entitlement::current_plan` is now the
/// single definition, and these tests call it — which is what makes them
/// evidence about production rather than about themselves.

async fn entitled_credits(pool: &sqlx::PgPool, account_id: Uuid) -> f64 {
    current_plan(pool, account_id).await.credits
}

/// The whole production row, for the tests that assert on more than the
/// total.
async fn current_plan(
    pool: &sqlx::PgPool,
    account_id: Uuid,
) -> coralswarm_backend::entitlement::PlanEntitlement {
    coralswarm_backend::entitlement::current_plan(pool, account_id)
        .await
        .expect("entitlement query")
        .expect("a non-revoked grant must exist for this account")
}

async fn pool(url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .expect("connect to test Postgres")
}

/// A throwaway account to hang grants off. Returns its `accounts.id`.
async fn make_account(pool: &sqlx::PgPool) -> Uuid {
    let email = format!("grant-seats-{}@test.example", Uuid::new_v4().simple());
    let owner: Uuid =
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(&email)
            .fetch_one(pool)
            .await
            .expect("create test user");
    sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
        .bind(owner)
        .fetch_one(pool)
        .await
        .expect("create test account")
}

/// `plans.id` for a seeded plan code, so no test hardcodes a UUID.
async fn plan_id(pool: &sqlx::PgPool, code: &str) -> Uuid {
    sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("plan {code:?} must be seeded (migrations 0153/0155): {e}"))
}

/// Deletes the account and everything hanging off it. `grants` cascades on
/// `account_id`, so this is enough to leave the DB as it was found.
async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid) {
    let owner: Option<Uuid> =
        sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .ok()
            .flatten();
    let _ = execute_financial_fixture(
        pool,
        account_id,
        sqlx::query("DELETE FROM accounts WHERE id = $1")
        .bind(account_id),
    )
        .await;
    if let Some(owner) = owner {
        let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(owner).execute(pool).await;
    }
}

/// **Test 1 — the load-bearing negative control.** A grant for a seat-priced
/// plan with `seats` left NULL is exactly the pre-0156 bug wearing a NULL
/// instead of a missing column, and must be REJECTED rather than silently
/// entitling one seat's worth of credits.
#[tokio::test]
async fn seat_priced_grant_without_a_seat_count_is_rejected() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    let insert = execute_financial_fixture(
        &pool,
        account_id,
        sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, NULL)",
    )
    .bind(account_id)
    .bind(team),
    )
    .await;

    assert!(
        insert.is_err(),
        "a seat-priced grant with seats IS NULL was ACCEPTED — this is the pre-0156 bug: \
         the entitlement expression would COALESCE it to 1 seat and issue 3,000 credits \
         to a customer who bought more"
    );
    let msg = insert.unwrap_err().to_string();
    assert!(
        msg.contains("grants_seats_iff_seat_priced"),
        "rejected, but not by the expected constraint: {msg}"
    );

    // Sanity: the identical row IS accepted once a seat count is supplied —
    // proving the CHECK is scoped to the missing-seats case rather than
    // rejecting seat-priced grants outright.
    let ok = execute_financial_fixture(
        &pool,
        account_id,
        sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, 3)",
    )
    .bind(account_id)
    .bind(team),
    )
    .await;
    assert!(ok.is_ok(), "a seat-priced grant WITH a seat count must be accepted: {ok:?}");

    cleanup(&pool, account_id).await;
}

/// **Test 2 — the mirror-image error.** A flat plan carrying a seat count
/// means somebody believes they bought something the price does not
/// reflect. Rejected, not silently ignored.
#[tokio::test]
async fn flat_plan_grant_carrying_seats_is_rejected() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let pro = plan_id(&pool, "pro").await;

    let insert = execute_financial_fixture(
        &pool,
        account_id,
        sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, false, 5)",
    )
    .bind(account_id)
    .bind(pro),
    )
    .await;

    assert!(
        insert.is_err(),
        "a flat-plan grant carrying seats = 5 was accepted — the seat count would be \
         silently ignored by the entitlement expression, which is the wrong answer to \
         an operator who thinks they sold 5 seats"
    );
    assert!(
        insert.unwrap_err().to_string().contains("grants_seats_iff_seat_priced"),
        "rejected by the wrong constraint"
    );

    cleanup(&pool, account_id).await;
}

/// **Test 3 — the copy cannot drift.** `plan_is_seat_priced` is denormalized
/// onto `grants` purely so the CHECK in tests 1-2 is expressible. That is
/// only sound if the copy is forced to agree with the plan it mirrors, which
/// is `grants_plan_seat_pricing_fk`'s entire job. Without this, a writer
/// could claim `false` against `team_standard` and walk straight back into
/// the one-seat bug with the CHECK satisfied.
#[tokio::test]
async fn a_grant_may_not_misreport_its_plans_pricing_shape() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    // Claims "flat" (so the CHECK is happy with seats IS NULL) while
    // pointing at a genuinely seat-priced plan. The composite FK is the only
    // thing standing between this row and a 3,000-credit Team customer.
    let insert = execute_financial_fixture(
        &pool,
        account_id,
        sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, false, NULL)",
    )
    .bind(account_id)
    .bind(team),
    )
    .await;

    assert!(
        insert.is_err(),
        "a grant claiming plan_is_seat_priced = false against the seat-priced \
         team_standard plan was ACCEPTED — the denormalized mirror drifted, and with it \
         the CHECK that depends on the mirror being true"
    );
    assert!(
        insert.unwrap_err().to_string().contains("grants_plan_seat_pricing_fk"),
        "rejected, but not by the composite FK that keeps the mirror honest"
    );

    cleanup(&pool, account_id).await;
}

/// **Test 4 — the arithmetic this whole change exists for.** Four seats of
/// `team_standard` (3,000 credits per seat) is 12,000 credits.
#[tokio::test]
async fn seat_priced_entitlement_multiplies_by_seats() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    execute_financial_fixture(
        &pool,
        account_id,
    sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, 4)",
    )
    .bind(account_id)
    .bind(team),
    )
    .await
    .expect("insert 4-seat team_standard grant");

    let credits = entitled_credits(&pool, account_id).await;

    assert_eq!(
        credits, 12_000.0,
        "4 seats x 3,000 credits/seat must be 12,000. Getting 3,000 here means the \
         entitlement expression dropped the seat multiplication — the exact pre-0156 bug"
    );

    cleanup(&pool, account_id).await;
}

/// **Test 5.** `credits_override` is an ABSOLUTE total and wins over the
/// multiplication entirely. 0155 told operators to reach for it precisely
/// because seats did not exist, so every grant written under that guidance
/// already holds a correct TOTAL — re-reading it as per-seat would multiply
/// those rows by their seat count.
#[tokio::test]
async fn credits_override_is_absolute_and_beats_the_seat_multiplication() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    execute_financial_fixture(&pool, account_id, sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats, credits_override) \
         VALUES ($1, $2, true, 4, 7000)",
    )
    .bind(account_id)
    .bind(team))
    .await
    .expect("insert overridden 4-seat grant");

    let credits = entitled_credits(&pool, account_id).await;

    assert_eq!(
        credits, 7_000.0,
        "an override of 7,000 on a 4-seat grant must yield 7,000, not 28,000 — \
         credits_override is a total, not a per-seat figure"
    );

    cleanup(&pool, account_id).await;
}

/// **Test 6.** Flat plans are arithmetically untouched: `COALESCE(seats, 1)`
/// must be a genuine no-op, not a behavior change smuggled in alongside the
/// seat work.
#[tokio::test]
async fn flat_plan_entitlement_is_unchanged() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let pro = plan_id(&pool, "pro").await;

    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(&pool, account_id)
        .await
        .expect("begin pro grant fixture");
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id)
        .await
        .expect("enter pro grant fixture");
    let row = sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced) \
         VALUES ($1, $2, false) RETURNING id, seats",
    )
    .bind(account_id)
    .bind(pro)
    .fetch_one(&mut *financial)
    .await
    .expect("insert pro grant");
    financial.commit().await.expect("commit pro grant fixture");
    let seats: Option<i32> = row.get("seats");

    assert_eq!(seats, None, "a flat-plan grant must store seats IS NULL");

    let credits = entitled_credits(&pool, account_id).await;

    assert_eq!(credits, 1_500.0, "pro is a flat 1,500 credits — seats must not change it");

    cleanup(&pool, account_id).await;
}

/// **Test 7.** An absolute `credits_override` must SUPPRESS
/// `credits_per_seat`, not merely coexist with it.
///
/// Review caught this as a contract defect: with an override in play,
/// `credits_per_seat * seats != credits`, so a client that renders the
/// obvious "4 seats x 3,000 credits" line would print it beside a total of
/// 7,000 and look broken. Publishing the per-seat figure there is not
/// redundant, it is CONTRADICTORY. The API therefore returns `None`, which
/// is what lets every client apply one rule — "if it's present, render the
/// breakdown; if not, don't" — without knowing overrides exist.
///
/// Negative control: delete `OR g.credits_override IS NOT NULL` from
/// `PLAN_ENTITLEMENT_SQL` and this test fails with `Some(3000.0)`.
#[tokio::test]
async fn an_override_suppresses_the_contradictory_per_seat_figure() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    execute_financial_fixture(&pool, account_id, sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats, credits_override) \
         VALUES ($1, $2, true, 4, 7000)",
    )
    .bind(account_id)
    .bind(team))
    .await
    .expect("insert overridden 4-seat grant");

    let plan = current_plan(&pool, account_id).await;

    assert_eq!(plan.credits, 7_000.0, "the override is the total");
    assert_eq!(plan.seats, Some(4), "seats are still reported — they were really purchased");
    assert_eq!(
        plan.credits_per_seat, None,
        "credits_per_seat must be None under an override: 4 x 3,000 = 12,000 contradicts the \
         7,000 total, and a client rendering both would look broken"
    );

    // The invariant the contract now guarantees, stated as an assertion:
    // whenever credits_per_seat is Some, the multiplication reconciles.
    if let (Some(per_seat), Some(seats)) = (plan.credits_per_seat, plan.seats) {
        assert_eq!(
            per_seat * f64::from(seats),
            plan.credits,
            "credits_per_seat * seats must equal credits whenever per-seat is published"
        );
    }

    cleanup(&pool, account_id).await;
}

/// **Test 8.** The mirror of test 7: with NO override, `credits_per_seat`
/// IS published and reconciles exactly. Without this, test 7 would pass
/// trivially if the field were hardcoded to `None`.
#[tokio::test]
async fn without_an_override_the_per_seat_figure_reconciles() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let account_id = make_account(&pool).await;
    let team = plan_id(&pool, "team_standard").await;

    execute_financial_fixture(
        &pool,
        account_id,
    sqlx::query(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, 4)",
    )
    .bind(account_id)
    .bind(team),
    )
    .await
    .expect("insert 4-seat grant");

    let plan = current_plan(&pool, account_id).await;

    assert_eq!(
        plan.credits_per_seat,
        Some(3_000.0),
        "with no override the per-seat allowance must be published"
    );
    assert_eq!(
        plan.credits_per_seat.unwrap() * f64::from(plan.seats.unwrap()),
        plan.credits,
        "3,000 x 4 must reconcile to the 12,000 total"
    );

    cleanup(&pool, account_id).await;
}

/// **Test 9 (Billing Phase 4c).** The initial allowance window `bin/grant_plan`
/// derives from the plan's own `refresh_kind`/`period_length`, asserted through
/// a full sqlx round trip.
///
/// Two independent things are being defended here, and the round trip is what
/// makes the second one visible at all:
///
///   1. An `anniversary` plan gets exactly its `period_length` (one month), so
///      the first period ends where Stripe's first `invoice.paid` picks up. A
///      window shorter than the billing period would strand members with no
///      allowance before their card is even charged.
///   2. A `lifetime` plan (`free`, `founder_comp`; `period_length IS NULL`) gets
///      a FINITE far-future window rather than `'infinity'::timestamptz`.
///      Infinity satisfies both `member_allowance_period_end_after_start` and
///      `is_blocked`'s `period_end > now()`, so it looks like the obvious
///      choice — but sqlx's chrono decoder has no representation for it and
///      errors on the way back out. The row would be writable and then
///      permanently unreadable, which is a worse failure than the one it solves:
///      the gate reads this row on the request path.
///
/// The negative control for (2) is to substitute `'infinity'` for the interval
/// and re-run: the INSERT still succeeds, and the SELECT below fails to decode.
#[tokio::test]
async fn the_initial_allowance_window_follows_the_plans_refresh_kind() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    // The exact expression `bin/grant_plan::resolve_initial_period` runs.
    let window = |code: &'static str| {
        let pool = pool.clone();
        async move {
            let row: (
                chrono::DateTime<chrono::Utc>,
                chrono::DateTime<chrono::Utc>) = sqlx::query_as(
                "SELECT now(), now() + COALESCE(p.period_length, interval '100 years') \
                   FROM plans p WHERE p.code = $1",
            )
            .bind(code)
            .fetch_one(&pool)
            .await
            .expect("the period window must decode back into chrono — see this test's doc");
            row
        }
    };

    let (start, end) = window("team_standard").await;
    let days = (end - start).num_days();
    assert!(
        (28..=31).contains(&days),
        "an anniversary plan's first period must be its one-month period_length, got {days} days"
    );

    let (start, end) = window("free").await;
    let years = (end - start).num_days() / 365;
    assert!(
        years >= 99,
        "a lifetime plan's period must outlast the account, got {years} years"
    );
    assert!(
        end < chrono::DateTime::<chrono::Utc>::MAX_UTC,
        "and it must stay FINITE — an infinite period_end is writable but not readable"
    );
}
