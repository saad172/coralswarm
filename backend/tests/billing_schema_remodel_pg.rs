//! Live regression suite for billing self-serve launch PR B (schema remodel)
//! — migrations `0165_seat_tiers.sql` through `0169_account_members_seat_
//! tier.sql`. See `.scratch/billing-self-serve/plan.md` §1.3-§1.5/§1.7/§1.10
//! and §2.B for the registered decisions this schema implements; this suite
//! does not re-litigate any of it, only proves the schema holds it.
//!
//! # What this suite is actually defending
//!
//!   1. The catalog seeds (`seat_tiers`, `credit_packs`, the `team` plan, and
//!      `team_standard`/`team_premium` deactivation) landed with the exact
//!      registered numbers, not a typo'd one — the same class of bug 0155's
//!      own header warns about ("derived numbers superseded by the
//!      register").
//!   2. `account_members_seat_tier_iff_org` — the NOT-NULL-for-team-orgs
//!      invariant — REJECTS both directions of the wrong shape (a personal
//!      member carrying a seat tier; a team-org member with none). The
//!      BEFORE INSERT trigger (`account_members_default_seat_tier`) used to
//!      transparently satisfy it for every caller by defaulting a bare
//!      insert to `standard` (`0169`); **billing self-serve PR F's migration
//!      `0176` retired that default in favor of `RAISE EXCEPTION`**, now that
//!      every writer of `account_members` for an org member
//!      (`ensure_org_membership`/`refresh_org_membership_from_claims`/
//!      `Store::insert_account_member`) resolves and passes `seat_tier_id`
//!      explicitly — see test 6 for the direct proof of the CURRENT
//!      (rejecting) behavior.
//!   3. `account_members_account_org_fk` (the composite-FK mirror of
//!      `accounts.is_org_account`) rejects a row whose denormalized copy
//!      disagrees with the row it mirrors — the same "copy cannot drift"
//!      proof `billing_grant_seats_pg.rs` already runs for `0156`'s
//!      `grants_plan_seat_pricing_fk`, applied to this PR's one new instance
//!      of the identical technique. `grant_seat_tiers` does NOT use this
//!      technique (review-round fix — an earlier version of `0168` did, on a
//!      false claim about RLS `USING` policies that has since been retracted;
//!      see that migration's header) — its account attribution is proven as
//!      a query-level join property instead (test 11).
//!   4. The mixed-seat entitlement arithmetic plan §1.4 introduces
//!      (`Σ(seats × tier.credits)`, computed by joining `grant_seat_tiers` to
//!      `seat_tiers`) is correct for a grant spanning BOTH tiers at once —
//!      the exact shape a single-tier `plans.credits × seats` formula (0156)
//!      cannot express, which is the whole reason this schema exists.
//!
//! # Negative controls — each verified to genuinely fail
//!
//! Every constraint-rejection assertion below was watched failing before
//! being trusted, per this codebase's standing rule that billing code is
//! arithmetic that looks right: dropping `account_members_seat_tier_iff_org`
//! (or `..._account_org_fk`) and re-running turns the corresponding
//! `assert!(...is_err())` into a failure, and reverting
//! `account_members_default_seat_tier` to its pre-PR-F (0169) default-to-
//! standard body turns test 6's `assert!(insert.is_err())` into a failure
//! too (the bare insert would succeed again instead of raising).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (a Postgres with
//! `migrations_control` applied through at least `0169`) — same convention
//! as `billing_grant_seats_pg.rs`; a normal `cargo test` with no DB
//! configured SKIPS LOUDLY.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/coralswarm \
//!     cargo test -p coralswarm-backend --test billing_schema_remodel_pg -- --test-threads=1

use sqlx::postgres::PgPoolOptions;
use sqlx::Row;
use uuid::Uuid;

const SKIP_REASON: &str = "skipping: set RELINFER_TEST_DATABASE_URL to run the live \
     billing-schema-remodel suite (a Postgres with migrations_control applied through 0169)";

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

async fn fetch_financial_fixture_id<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Uuid, sqlx::postgres::PgArguments>,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = coralswarm_backend::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
    match query.fetch_one(&mut *tx).await {
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

async fn pool(url: &str) -> sqlx::PgPool {
    PgPoolOptions::new()
        .max_connections(4)
        .connect(url)
        .await
        .expect("connect to test Postgres")
}

/// A personal account (no `clerk_org_id`), with its owner as the sole
/// `account_members` row. Returns `(account_id, owner_user_id)`.
async fn make_personal_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
    let email = format!("schema-remodel-personal-{}@test.example", Uuid::new_v4().simple());
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("create test user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .expect("create test personal account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(owner)
        .execute(pool)
        .await
        .expect("create account_members row");
    (account_id, owner)
}

/// A team-org account (`clerk_org_id` set), with its owner as the sole
/// `account_members` row. Returns `(account_id, owner_user_id)`.
///
/// **Passes `seat_tier_id` explicitly (billing self-serve PR F).** Migration
/// `0176` retired `account_members_default_seat_tier`'s silent
/// default-to-standard arm in favor of `RAISE EXCEPTION` (see this file's
/// `a_bare_insert_for_a_team_org_account_is_rejected_without_an_explicit_tier`
/// test for the direct proof) — every OTHER test in this suite uses this
/// helper only to get a valid org-account fixture to build on, not to
/// exercise the default itself, so it must not rely on a default that no
/// longer exists.
async fn make_org_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
    let email = format!("schema-remodel-org-{}@test.example", Uuid::new_v4().simple());
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(&email)
        .fetch_one(pool)
        .await
        .expect("create test user");
    let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(&clerk_org_id)
    .fetch_one(pool)
    .await
    .expect("create test org account");
    let standard_id: Uuid = sqlx::query_scalar(
        "SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true",
    )
    .fetch_one(pool)
    .await
    .expect("active standard seat tier must be seeded (0165)");
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'owner', $3)",
    )
    .bind(account_id)
    .bind(owner)
    .bind(standard_id)
    .execute(pool)
    .await
    .expect("create account_members row");
    (account_id, owner)
}

async fn cleanup(pool: &sqlx::PgPool, account_id: Uuid, owner: Uuid) {
    let _ = execute_financial_fixture(
        pool,
        account_id,
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id),
    ).await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(owner).execute(pool).await;
}

// ── Catalog seed tests ───────────────────────────────────────────────────────

/// **Test 1.** `seat_tiers` holds exactly the two registered rows (plan
/// §1.3), with the exact figures — 3,000 credits/$40 standard, 15,000
/// credits/$175 premium — not a plausible-looking typo of them.
#[tokio::test]
async fn seat_tiers_are_seeded_with_the_registered_figures() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    let rows: Vec<(String, sqlx::types::Decimal, i32, bool)> = sqlx::query_as(
        "SELECT code, credits, price_usd_cents, is_active FROM seat_tiers ORDER BY code",
    )
    .fetch_all(&pool)
    .await
    .expect("select seat_tiers");

    assert_eq!(rows.len(), 2, "exactly two seat tiers, got {rows:?}");
    let (code, credits, price, active) = &rows[0];
    assert_eq!((code.as_str(), *price, *active), ("premium", 17500, true));
    assert_eq!(*credits, sqlx::types::Decimal::from(15000));
    let (code, credits, price, active) = &rows[1];
    assert_eq!((code.as_str(), *price, *active), ("standard", 4000, true));
    assert_eq!(*credits, sqlx::types::Decimal::from(3000));
}

/// **Test 2.** `credit_packs` holds exactly the three registered top-up
/// packs (plan §1.10) — $0.022/credit for all three.
#[tokio::test]
async fn credit_packs_are_seeded_with_the_registered_figures() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    let rows: Vec<(String, sqlx::types::Decimal, i32)> = sqlx::query_as(
        "SELECT code, credits, price_usd_cents FROM credit_packs ORDER BY code")
    .fetch_all(&pool)
    .await
    .expect("select credit_packs");

    assert_eq!(rows.len(), 3, "exactly three credit packs, got {rows:?}");
    let expected = [
        ("pack_2000", 2000, 4400),
        ("pack_500", 500, 1100),
        ("pack_5000", 5000, 11000),
    ];
    for ((code, credits, price), (exp_code, exp_credits, exp_price)) in rows.iter().zip(expected) {
        assert_eq!(code.as_str(), exp_code);
        assert_eq!(*credits, sqlx::types::Decimal::from(exp_credits));
        assert_eq!(*price, exp_price);
        // Every pack must land at exactly $0.022/credit (plan §1.10).
        let per_credit = f64::from(*price) / f64::try_from(*credits).unwrap() / 100.0;
        assert!(
            (per_credit - 0.022).abs() < 1e-9,
            "{code} must price at $0.022/credit, got {per_credit}"
        );
    }
}

/// **Test 3.** The ladder remodel itself: `team_standard`/`team_premium` are
/// deactivated (kept, not deleted), and one active `team` umbrella plan
/// exists with no seat minimum (plan §1.5, supersedes the old 3-seat floor)
/// and no flat/per-seat price of its own (priced entirely via `seat_tiers`).
/// `pro` is untouched.
#[tokio::test]
async fn the_team_ladder_is_remodeled_and_pro_is_untouched() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    let row = |code: &'static str| {
        let pool = pool.clone();
        async move {
            sqlx::query(
                "SELECT is_active, is_seat_priced, min_seats, credits, price_usd_cents \
                 FROM plans WHERE code = $1",
            )
            .bind(code)
            .fetch_one(&pool)
            .await
            .unwrap_or_else(|e| panic!("plan {code:?} must exist: {e}"))
        }
    };

    let team_standard = row("team_standard").await;
    assert!(!team_standard.get::<bool, _>("is_active"), "team_standard must be deactivated");

    let team_premium = row("team_premium").await;
    assert!(!team_premium.get::<bool, _>("is_active"), "team_premium must be deactivated");

    let team = row("team").await;
    // ACTIVE as of migration 0170 (billing self-serve launch PR E). 0167
    // seeded this row INACTIVE deliberately — "must stay unpurchasable until
    // PR E teaches renewal to read `grant_seat_tiers` instead of
    // `plans.credits`" (0167's header, "Why `team.is_active` is seeded
    // `false`") — and PR E is exactly that renewal rewrite:
    // `billing::renewal::roll_period`/`seed_member_period` and
    // `billing::entitlement::PLAN_ENTITLEMENT_SQL` now detect `plans.code =
    // 'team'` and read `grant_seat_tiers`/`seat_tiers`/`account_members.
    // seat_tier_id` instead of `plans.credits`, closing the "0-credit grant
    // would win as the account's authoritative entitlement" trap 0167's
    // header warned about. 0170's own header records this same reasoning.
    // `bin/grant_plan.rs` separately refuses `--plan team` by name (its own
    // updated doc comment, this same PR) since it still cannot express a
    // MIXED tier composition through a single `--seats <n>` — so the
    // zero-credit trap 0167 feared is closed on that path a different way,
    // not by leaving `team` inactive forever.
    assert!(team.get::<bool, _>("is_active"), "the team umbrella plan must be ACTIVE as of PR E (migration 0170)");
    assert!(team.get::<bool, _>("is_seat_priced"), "team grants still participate in the seats invariant");
    assert_eq!(
        team.get::<Option<i32>, _>("min_seats"),
        None,
        "no seat minimum survives the remodel (plan §1.5)"
    );
    // Finding 4: both load-bearing placeholders per 0167's own header — a
    // real (nonzero, non-NULL) value here would look meaningful to a future
    // reader and risks being multiplied by the pre-existing (0156)
    // entitlement formula instead of the real `grant_seat_tiers` join.
    assert_eq!(
        team.get::<sqlx::types::Decimal, _>("credits"),
        sqlx::types::Decimal::from(0),
        "team.credits must be exactly 0 — the loud 'read from grant_seat_tiers instead' \
         placeholder 0167's header documents, not a plausible-looking nonzero figure"
    );
    assert_eq!(
        team.get::<Option<i32>, _>("price_usd_cents"),
        None,
        "team.price_usd_cents must be NULL — team has no single flat/per-seat price of its \
         own; its real prices live on seat_tiers.price_usd_cents, one row per tier (0167's \
         header)"
    );

    let pro = row("pro").await;
    assert!(pro.get::<bool, _>("is_active"), "pro must be untouched by the team remodel");
}

/// **Test 4.** `stripe_price_id` exists (nullable) on `plans`, `seat_tiers`,
/// and `credit_packs`, and TWO FRESHLY-INSERTED NULL rows in EACH of
/// `seat_tiers`/`credit_packs` do not collide under the UNIQUE constraint
/// (Postgres UNIQUE permits any number of NULLs) — a naive implementation
/// might reach for a partial unique index instead and this proves the plain
/// `UNIQUE` column is sufficient.
///
/// Review-round fix: the original version of this test asserted
/// `null_count == 2` against the SEED rows and never touched `credit_packs`
/// at all — a brittle proxy for the thing the test's own name claims to
/// prove (multiple NULLs coexisting under UNIQUE), since that count breaks
/// the moment PR C fills in a seed row's `stripe_price_id` even though the
/// UNIQUE-permits-NULLs behavior itself would still hold. This version
/// INSERTS two more NULL rows into each catalog table itself (inside a
/// transaction that is always rolled back, never committed, so the catalog
/// tables are untouched by this test) and asserts the inserts succeed and
/// both freshly-inserted ids are visible — a direct proof, not a seed-count
/// proxy, and one that survives PR C populating the seed rows.
#[tokio::test]
async fn stripe_price_id_is_nullable_and_multiple_nulls_do_not_collide() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;

    let plans_col_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'plans' AND column_name = 'stripe_price_id')",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(plans_col_exists, "plans.stripe_price_id must exist (0167)");

    let mut tx = pool.begin().await.expect("begin rolled-back tx");

    let seat_tier_ids: Vec<Uuid> = sqlx::query_scalar(
        "INSERT INTO seat_tiers (code, name, credits, price_usd_cents, stripe_price_id) \
         VALUES ('t4_probe_a', 'T4 Probe A', 100, 100, NULL), \
                ('t4_probe_b', 'T4 Probe B', 100, 100, NULL) \
         RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("two independently-NULL stripe_price_id rows in seat_tiers must not collide");
    assert_eq!(seat_tier_ids.len(), 2, "both probe seat_tiers rows must insert");

    let pack_ids: Vec<Uuid> = sqlx::query_scalar(
        "INSERT INTO credit_packs (code, name, credits, price_usd_cents, stripe_price_id) \
         VALUES ('t4_probe_a', 'T4 Probe A', 100, 100, NULL), \
                ('t4_probe_b', 'T4 Probe B', 100, 100, NULL) \
         RETURNING id",
    )
    .fetch_all(&mut *tx)
    .await
    .expect("two independently-NULL stripe_price_id rows in credit_packs must not collide");
    assert_eq!(pack_ids.len(), 2, "both probe credit_packs rows must insert");

    // Never committed — the catalog tables are untouched once this test ends.
    tx.rollback().await.expect("roll back the probe tx");
}

// ── NOT-NULL-for-team-orgs enforcement ──────────────────────────────────────

/// **Test 5.** A bare `INSERT INTO account_members (account_id, user_id,
/// role)` — the exact shape every existing route/worker call site uses —
/// against a PERSONAL account must leave `seat_tier_id` NULL and
/// `account_is_org_account` false, entirely via the BEFORE INSERT trigger,
/// with no caller-side change.
#[tokio::test]
async fn a_bare_insert_for_a_personal_account_defaults_to_no_seat_tier() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_personal_account(&pool).await;

    let row = sqlx::query(
        "SELECT account_is_org_account, seat_tier_id FROM account_members \
         WHERE account_id = $1 AND user_id = $2",
    )
    .bind(account_id)
    .bind(owner)
    .fetch_one(&pool)
    .await
    .expect("read the seeded row");

    assert!(!row.get::<bool, _>("account_is_org_account"));
    assert_eq!(row.get::<Option<Uuid>, _>("seat_tier_id"), None);

    cleanup(&pool, account_id, owner).await;
}

/// **Test 6 — the load-bearing proof (billing self-serve PR F).** The
/// identical bare INSERT shape against a TEAM-ORG account must now be
/// REJECTED — migration `0176` retired `account_members_default_seat_tier`'s
/// silent default-to-standard arm (`RAISE WARNING` + a `standard` fallback,
/// `0169`) in favor of `RAISE EXCEPTION`, because every known writer of
/// `account_members` for an org member (`clerk_provision::
/// ensure_org_membership`/`refresh_org_membership_from_claims`,
/// `Store::insert_account_member`) now resolves and passes `seat_tier_id`
/// explicitly — see `0176`'s own header for the full inventory and the
/// "SUNSET" note `0169` shipped promising exactly this flip.
///
/// This test SUPERSEDES the pre-PR-F version of this suite's own "load-
/// bearing proof" (`a_bare_insert_for_a_team_org_account_defaults_to_standard`),
/// which asserted the now-retired default — restated here rather than kept
/// alongside it, because the two are mutually exclusive: the trigger can only
/// default OR reject a bare org-member insert, never both.
#[tokio::test]
async fn a_bare_insert_for_a_team_org_account_is_rejected_without_an_explicit_tier() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    // `make_org_account` already passes `seat_tier_id` explicitly (PR F) —
    // used here only to get a valid ORG ACCOUNT to attempt a bare (no
    // explicit tier) SECOND member insert against, which is what this test
    // actually exercises.
    let (account_id, owner) = make_org_account(&pool).await;

    let second_email = format!("schema-remodel-org-second-{}@test.example", Uuid::new_v4().simple());
    let second_user: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(&second_email)
        .fetch_one(&pool)
        .await
        .expect("create second test user");

    let insert = sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')",
    )
    .bind(account_id)
    .bind(second_user)
    .execute(&pool)
    .await;

    assert!(
        insert.is_err(),
        "a bare org-member insert with no explicit seat_tier_id was ACCEPTED — the 0176 \
         trigger hardening did not take effect"
    );
    let err = insert.unwrap_err();
    // Billing self-serve PR F review, trivial finding: assert the trigger's
    // dedicated SQLSTATE (`USING ERRCODE = 'check_violation'`, 23514 — the
    // same convention `0082`'s own `account_members_block_revoked` trigger
    // uses), not the exception message TEXT — a future wording tweak to
    // 0176's own message must not fail this test as long as the rejection
    // itself is unchanged.
    assert_eq!(
        err.as_database_error().and_then(|e| e.code()).as_deref(),
        Some("23514"),
        "expected the 0176 trigger's check_violation rejection (23514), got {err}"
    );

    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(second_user).execute(&pool).await;
    cleanup(&pool, account_id, owner).await;
}

/// **Test 7.** The mirror-image error: an explicit seat tier on a PERSONAL
/// account is rejected by `account_members_seat_tier_iff_org`, not silently
/// accepted or silently dropped.
#[tokio::test]
async fn an_explicit_seat_tier_on_a_personal_account_is_rejected() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_personal_account(&pool).await;

    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
        .fetch_one(&pool)
        .await
        .expect("standard seat tier must be seeded");

    let another_user = format!("second-{}@test.example", Uuid::new_v4().simple());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(&another_user)
        .fetch_one(&pool)
        .await
        .expect("create second user");

    let insert = sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, 'member', $3)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(standard_id)
    .execute(&pool)
    .await;

    assert!(
        insert.is_err(),
        "a personal-account member explicitly carrying a seat tier was ACCEPTED"
    );
    assert!(
        insert.unwrap_err().to_string().contains("account_members_seat_tier_iff_org"),
        "rejected, but not by the expected constraint"
    );

    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await;
    cleanup(&pool, account_id, owner).await;
}

/// **Test 8.** The denormalized `account_is_org_account` mirror cannot
/// drift: claiming `false` for a member of a genuinely org account is
/// rejected by `account_members_account_org_fk`, the same "copy cannot
/// drift" proof `billing_grant_seats_pg.rs` runs for `grants_plan_seat_
/// pricing_fk`.
#[tokio::test]
async fn account_is_org_account_may_not_misreport_its_accounts_shape() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_org_account(&pool).await;

    let another_user = format!("second-{}@test.example", Uuid::new_v4().simple());
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(&another_user)
        .fetch_one(&pool)
        .await
        .expect("create second user");

    let insert = sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, account_is_org_account) \
         VALUES ($1, $2, 'member', false)",
    )
    .bind(account_id)
    .bind(user_id)
    .execute(&pool)
    .await;

    assert!(
        insert.is_err(),
        "a member row claiming account_is_org_account = false against a genuinely org \
         account was ACCEPTED — the denormalized mirror drifted"
    );
    assert!(
        insert.unwrap_err().to_string().contains("account_members_account_org_fk"),
        "rejected, but not by the composite FK that keeps the mirror honest"
    );

    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(&pool).await;
    cleanup(&pool, account_id, owner).await;
}

/// **Test 9.** `pending_seat_tier_id` requires a current `seat_tier_id` —
/// a deferred downgrade only makes sense for a member who currently has a
/// tier to downgrade FROM.
#[tokio::test]
async fn a_pending_tier_without_a_current_tier_is_rejected() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_personal_account(&pool).await;

    let premium_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium'")
        .fetch_one(&pool)
        .await
        .expect("premium seat tier must be seeded");

    let insert = sqlx::query(
        "UPDATE account_members SET pending_seat_tier_id = $1 WHERE account_id = $2")
    .bind(premium_id)
    .bind(account_id)
    .execute(&pool)
    .await;

    assert!(
        insert.is_err(),
        "a pending seat tier on a member with no CURRENT seat tier was ACCEPTED"
    );
    assert!(
        insert
            .unwrap_err()
            .to_string()
            .contains("account_members_pending_requires_current_tier"),
        "rejected, but not by the expected constraint"
    );

    cleanup(&pool, account_id, owner).await;
}

/// **Test 9b — the accepting direction (finding 5).** The mirror image of
/// test 9: an org member who ALREADY holds a `seat_tier_id` MAY take a
/// `pending_seat_tier_id` (plan §1.7's deferred downgrade). Without this
/// proof, an over-strict `CHECK (pending_seat_tier_id IS NULL)` — i.e. one
/// that forbids ANY pending tier at all, not just an unearned one — would
/// still pass test 9's rejection case, since test 9 only ever supplies a
/// pending tier to a member with NO current tier at all.
#[tokio::test]
async fn a_pending_tier_with_a_current_tier_is_accepted() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_org_account(&pool).await;

    let premium_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium'")
        .fetch_one(&pool)
        .await
        .expect("premium seat tier must be seeded");

    // `owner`'s row already carries `seat_tier_id = standard`, stamped by
    // the BEFORE INSERT default-filler (the same mechanism test 6 proves) —
    // a genuine "already has a current tier" member, not a hand-crafted one.
    let update = sqlx::query(
        "UPDATE account_members SET pending_seat_tier_id = $1 \
         WHERE account_id = $2 AND user_id = $3",
    )
    .bind(premium_id)
    .bind(account_id)
    .bind(owner)
    .execute(&pool)
    .await;

    assert!(
        update.is_ok(),
        "an org member who already holds a seat_tier_id must be able to take a \
         pending_seat_tier_id (plan §1.7's deferred downgrade) — got {update:?}"
    );
    assert_eq!(
        update.unwrap().rows_affected(),
        1,
        "the UPDATE must have actually touched the seeded member row"
    );

    cleanup(&pool, account_id, owner).await;
}

// ── grant_seat_tiers ─────────────────────────────────────────────────────────

/// **Test 10 — the arithmetic this whole PR exists for.** A single `team`
/// grant spanning BOTH tiers at once (3 standard seats + 2 premium seats)
/// entitles `3×3,000 + 2×15,000 = 39,000` credits, computed by joining
/// `grant_seat_tiers` to `seat_tiers` — the shape a single-tier `plans.
/// credits × grants.seats` formula (0156) cannot express at all.
#[tokio::test]
async fn a_mixed_tier_grant_sums_correctly_across_tiers() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_org_account(&pool).await;

    let team_plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
        .fetch_one(&pool)
        .await
        .expect("team plan must be seeded (migration 0167)");
    let grant_id: Uuid = fetch_financial_fixture_id(
        &pool,
        account_id,
        sqlx::query_scalar(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, 5) RETURNING id",
    )
    .bind(account_id)
    .bind(team_plan_id),
    )
    .await
    .expect("insert team grant");

    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let premium_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium'")
        .fetch_one(&pool)
        .await
        .unwrap();

    execute_financial_fixture(
        &pool,
        account_id,
    sqlx::query(
        "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
         VALUES ($1, $2, 3), ($1, $3, 2)",
    )
    .bind(grant_id)
    .bind(standard_id)
    .bind(premium_id),
    )
    .await
    .expect("insert mixed-tier grant_seat_tiers rows");

    let total: sqlx::types::Decimal = sqlx::query_scalar(
        "SELECT SUM(gst.seats * st.credits) FROM grant_seat_tiers gst \
         JOIN seat_tiers st ON st.id = gst.seat_tier_id WHERE gst.grant_id = $1",
    )
    .bind(grant_id)
    .fetch_one(&pool)
    .await
    .expect("sum mixed-tier entitlement");

    assert_eq!(
        total,
        sqlx::types::Decimal::from(39_000),
        "3 standard (3,000 each) + 2 premium (15,000 each) must total 39,000"
    );

    cleanup(&pool, account_id, owner).await;
}

/// **Test 11.** `grant_seat_tiers` has no `account_id` column of its own
/// (review-round fix — an earlier version of this migration/test pair
/// denormalized one, plus a composite FK proving the copy "cannot drift";
/// both were removed once `0168`'s false "a `USING` policy cannot join to
/// another table" claim was retracted — see that migration's header). This
/// test proves the one-hop join this suite now relies on instead actually
/// attributes each `grant_seat_tiers` row to the RIGHT account when read
/// back through `grants` — i.e. that the join `grant_seat_tiers JOIN grants
/// ON grants.id = grant_seat_tiers.grant_id` never misattributes account_a's
/// row to account_b, which is the same fact the removed composite FK used to
/// guarantee, now proven as a query-level property instead of a constraint.
/// `consumer_rls_pg.rs`'s
/// `grant_seat_tiers_rls_isolates_via_the_one_hop_grants_subquery` covers the
/// same join under the actual RLS policy and the `coralswarm_app` role; this
/// test covers it as plain arithmetic, independent of RLS being enabled.
#[tokio::test]
async fn grant_seat_tiers_attributes_correctly_through_the_grants_join() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_a, owner_a) = make_org_account(&pool).await;
    let (account_b, owner_b) = make_org_account(&pool).await;

    let team_plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
        .fetch_one(&pool)
        .await
        .unwrap();

    async fn seed(pool: &sqlx::PgPool, account_id: Uuid, team_plan_id: Uuid, tier_id: Uuid, seats: i32) -> Uuid {
        let grant_id: Uuid = fetch_financial_fixture_id(
            pool,
            account_id,
            sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
             VALUES ($1, $2, true, $3) RETURNING id",
        )
        .bind(account_id)
        .bind(team_plan_id)
        .bind(seats),
        )
        .await
        .expect("insert grant");
        execute_financial_fixture(
            pool,
            account_id,
        sqlx::query(
            "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)",
        )
        .bind(grant_id)
        .bind(tier_id)
        .bind(seats),
        )
        .await
        .expect("insert grant_seat_tiers row");
        grant_id
    }

    let grant_a = seed(&pool, account_a, team_plan_id, standard_id, 3).await;
    let grant_b = seed(&pool, account_b, team_plan_id, standard_id, 7).await;

    let attributed: Vec<(Uuid, i32)> = sqlx::query_as(
        "SELECT g.account_id, gst.seats FROM grant_seat_tiers gst \
         JOIN grants g ON g.id = gst.grant_id \
         WHERE gst.grant_id IN ($1, $2) ORDER BY gst.seats",
    )
    .bind(grant_a)
    .bind(grant_b)
    .fetch_all(&pool)
    .await
    .expect("join grant_seat_tiers to grants");

    assert_eq!(
        attributed,
        vec![(account_a, 3), (account_b, 7)],
        "each grant_seat_tiers row must attribute to its OWN grant's account through the join, \
         never the other account's"
    );

    cleanup(&pool, account_a, owner_a).await;
    cleanup(&pool, account_b, owner_b).await;
}

/// **Test 12.** `seats >= 1` on `grant_seat_tiers` — no seat minimum (plan
/// §1.5) does not mean no floor at all; a tier with zero seats simply has no
/// row (0168's header), never a `seats = 0` row.
#[tokio::test]
async fn grant_seat_tiers_rejects_a_zero_seat_row() {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("{SKIP_REASON}");
        return;
    };
    let pool = pool(&url).await;
    let (account_id, owner) = make_org_account(&pool).await;

    let team_plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
        .fetch_one(&pool)
        .await
        .unwrap();
    let grant_id: Uuid = fetch_financial_fixture_id(
        &pool,
        account_id,
        sqlx::query_scalar(
        "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
         VALUES ($1, $2, true, 1) RETURNING id",
    )
    .bind(account_id)
    .bind(team_plan_id),
    )
    .await
    .expect("insert grant");
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard'")
        .fetch_one(&pool)
        .await
        .unwrap();

    let insert = execute_financial_fixture(
        &pool,
        account_id,
        sqlx::query(
        "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
         VALUES ($1, $2, 0)",
    )
    .bind(grant_id)
    .bind(standard_id),
    )
    .await;

    assert!(insert.is_err(), "a zero-seat grant_seat_tiers row was ACCEPTED");
    assert!(
        insert.unwrap_err().to_string().contains("grant_seat_tiers_seats_check"),
        "rejected, but not by the named seats CHECK (0168 now names it \
         grant_seat_tiers_seats_check explicitly so this assertion doesn't depend on a \
         Postgres-generated constraint name)"
    );

    cleanup(&pool, account_id, owner).await;
}
