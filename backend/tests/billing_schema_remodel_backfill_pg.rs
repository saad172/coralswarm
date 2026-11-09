//! Upgrade-path regression for billing self-serve launch PR B's `0169`
//! backfill (review-round finding 1): proves the backfill maps each
//! team-org member's `seat_tier_id` from THEIR ACCOUNT's currently active
//! grant, not `standard` unconditionally.
//!
//! # Why this needs a real upgrade path, not the shared `RELINFER_TEST_DATABASE_URL`
//! fixture `billing_schema_remodel_pg.rs` uses
//!
//! `billing_schema_remodel_pg.rs` runs against a Postgres already migrated
//! through `0169` — perfect for proving CONSTRAINTS and TRIGGERS hold going
//! forward, useless for proving a BACKFILL did the right thing, because by
//! the time that fixture's database exists, `account_members` never held a
//! pre-`0169` row to backfill in the first place. This test instead:
//!
//!   1. Migrates a fresh scratch DB to (and including) `0164` — the last
//!      migration before this PR's schema remodel.
//!   2. Seeds, in that PRE-remodel schema, six team-org member shapes that
//!      can only be told apart by their ACCOUNT's grant history: a personal
//!      account (control — must never get a seat tier), a team-org account
//!      with NO grant at all, one with an active `team_standard` grant, one
//!      with an active `team_premium` grant, one whose ONLY `team_premium`
//!      grant is revoked (finding 3a — CodeRabbit round 1: the backfill's
//!      grant-selection semantics, `revoked_at IS NULL ORDER BY granted_at
//!      DESC`, were otherwise unexercised by this suite), and one with an
//!      older `team_premium` grant superseded by a newer `team_standard`
//!      grant (finding 3b — proves the NEWEST unrevoked grant wins, not
//!      merely any unrevoked team_premium grant that exists).
//!   3. Applies `0165`-`0169` (the rest of this PR's migrations, including
//!      the backfill under test) on top.
//!   4. Asserts every member's backfilled `seat_tier_id` matches what
//!      `billing::renewal::CURRENT_GRANT_SQL` would already consider that
//!      account's active entitlement — `team_premium` -> `premium`,
//!      everything else (including no grant) -> `standard`.
//!
//! Uses `sqlx::migrate::Migrator`'s public (if `#[doc(hidden)]`) `migrations`
//! field to run a partial subset of the tree at runtime — the same technique
//! `member_ocean_probe_pg.rs`'s `subset` closure already establishes for
//! exactly this "migrate to X, seed, migrate the rest" shape (see that
//! file's own comment on why raw file execution is not equivalent to running
//! the real `Migrator`).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as `member_ocean_probe_pg.rs`/
//! `consumer_rls_pg.rs`.
//!
//! Run with:
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test billing_schema_remodel_backfill_pg

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

// ── Scratch-DB plumbing (duplicated per this directory's documented
//    "no shared test-support module across files" convention) ───────────────

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
        .expect("drop scratch db");
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .expect("create scratch db");
}

#[tokio::test]
async fn backfill_maps_seat_tier_from_the_accounts_active_grant() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping backfill_maps_seat_tier_from_the_accounts_active_grant: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let db = "orgt_billing_schema_remodel_backfill";
    let _cleanup_db = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, db).await;
    let pool: PgPool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(&base, db))
        .await
        .expect("connect scratch db");

    // Runtime-loaded (not the compile-time `migrate!()` macro this crate
    // uses elsewhere) so the resolved migration list can be filtered by
    // version — same technique `member_ocean_probe_pg.rs`'s `subset`
    // closure uses for the identical "migrate to X, seed, migrate the rest"
    // shape.
    let migrator = sqlx::migrate::Migrator::new(std::path::Path::new(
        concat!(env!("CARGO_MANIFEST_DIR"), "/migrations_control"),
    ))
    .await
    .expect("resolve migrations_control");
    let subset = |keep: &dyn Fn(i64) -> bool| sqlx::migrate::Migrator {
        migrations: migrator
            .iter()
            .filter(|m| keep(m.version))
            .cloned()
            .collect::<Vec<_>>()
            .into(),
        // The second call's resolved set omits every version <= 164, which
        // the ledger already holds applied from the first call — the default
        // validation would reject that as "previously applied but missing
        // from the resolved set". Relaxed deliberately, inert on the first
        // call (empty ledger).
        ignore_missing: true,
        locking: migrator.locking,
        no_tx: migrator.no_tx,
    };

    // ── 1. Migrate to (and including) 0164 — the pre-remodel schema ────────
    subset(&|v| v <= 164)
        .run(&pool)
        .await
        .expect("apply migrations_control through 0164");

    // Sanity: the columns under test must NOT exist yet, or this test would
    // silently be proving something about the wrong schema state.
    let seat_tier_col_exists: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM information_schema.columns \
         WHERE table_name = 'account_members' AND column_name = 'seat_tier_id')",
    )
    .fetch_one(&pool)
    .await
    .expect("check seat_tier_id absence");
    assert!(
        !seat_tier_col_exists,
        "account_members.seat_tier_id must not exist yet at migration 0164 — \
         this test is running against the wrong schema state"
    );

    // ── 2. Seed, in the PRE-remodel schema, the four account shapes ────────
    async fn make_user(pool: &PgPool, label: &str) -> Uuid {
        let email = format!("backfill-{label}-{}@test.example", Uuid::new_v4().simple());
        sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
            .bind(&email)
            .fetch_one(pool)
            .await
            .expect("create test user")
    }
    async fn make_org_account(pool: &PgPool, label: &str) -> (Uuid, Uuid) {
        let owner = make_user(pool, label).await;
        let clerk_org_id = format!("org_{label}_{}", Uuid::new_v4().simple());
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&clerk_org_id)
        .fetch_one(pool)
        .await
        .expect("create org account");
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .expect("seed owner member row");
        let second = make_user(pool, &format!("{label}-second")).await;
        sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'member')")
            .bind(account_id)
            .bind(second)
            .execute(pool)
            .await
            .expect("seed second member row");
        (account_id, owner)
    }
    async fn grant_plan(pool: &PgPool, account_id: Uuid, plan_code: &str, seats: i32) {
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(plan_code)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("plan {plan_code:?} must be seeded (0153/0164): {e}"));
        sqlx::query(
            "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats) \
             VALUES ($1, $2, true, $3)",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(seats)
        .execute(pool)
        .await
        .expect("insert grant");
    }

    /// Like [`grant_plan`], but with an explicit `granted_at`/`revoked_at`
    /// (finding 3) — needed to exercise the backfill's `revoked_at IS NULL
    /// ORDER BY granted_at DESC` selection semantics, which `grant_plan`
    /// above (one grant per account, DB-default `granted_at = now()`) never
    /// exercises at all. Explicit whole-second epoch literals, not
    /// `Utc::now()` +/- `Duration` arithmetic: chrono carries nanosecond
    /// precision while Postgres `TIMESTAMPTZ` stores microseconds, and this
    /// codebase has already hit that exact platform-dependent flake once
    /// (see `70fdc4c`'s `journal_axis_round_trips_through_finalize` fix) —
    /// fixed whole-second literals sidestep the mismatch entirely rather
    /// than risking it a second time.
    async fn grant_plan_at(
        pool: &PgPool,
        account_id: Uuid,
        plan_code: &str,
        seats: i32,
        granted_at: DateTime<Utc>,
        revoked_at: Option<DateTime<Utc>>,
    ) {
        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(plan_code)
            .fetch_one(pool)
            .await
            .unwrap_or_else(|e| panic!("plan {plan_code:?} must be seeded (0153/0164): {e}"));
        sqlx::query(
            "INSERT INTO grants (account_id, plan_id, plan_is_seat_priced, seats, granted_at, revoked_at) \
             VALUES ($1, $2, true, $3, $4, $5)",
        )
        .bind(account_id)
        .bind(plan_id)
        .bind(seats)
        .bind(granted_at)
        .bind(revoked_at)
        .execute(pool)
        .await
        .expect("insert grant with explicit granted_at/revoked_at");
    }

    // Personal account — control. Must never get a seat tier regardless of
    // the backfill logic.
    let personal_owner = make_user(&pool, "personal").await;
    let personal_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(personal_owner)
            .fetch_one(&pool)
            .await
            .expect("create personal account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(personal_account)
        .bind(personal_owner)
        .execute(&pool)
        .await
        .expect("seed personal member row");

    // Team org, NO active grant at all — must still backfill to `standard`
    // (the correct floor for a grant-less account, per 0169's header).
    let (no_grant_account, _) = make_org_account(&pool, "nogrant").await;

    // Team org with an active `team_standard` grant — must backfill to
    // `standard`.
    let (standard_account, _) = make_org_account(&pool, "standard").await;
    grant_plan(&pool, standard_account, "team_standard", 2).await;

    // Team org with an active `team_premium` grant — the load-bearing case:
    // must backfill to `premium`, NOT `standard`.
    let (premium_account, _) = make_org_account(&pool, "premium").await;
    grant_plan(&pool, premium_account, "team_premium", 2).await;

    // Team org whose ONLY `team_premium` grant is REVOKED (finding 3a) —
    // must still backfill to `standard`. `revoked_at IS NULL` excludes it
    // from `CURRENT_GRANT_SQL`'s selection entirely, so this account has NO
    // active entitlement at all — the same floor as `no_grant_account`
    // above, exercised via a grant that exists but doesn't count, rather
    // than via no grant existing at all.
    let (revoked_premium_account, _) = make_org_account(&pool, "revokedpremium").await;
    let t0 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    grant_plan_at(
        &pool,
        revoked_premium_account,
        "team_premium",
        2,
        t0,
        Some(t0 + chrono::Duration::seconds(60)),
    )
    .await;

    // Team org with an OLDER `team_premium` grant SUPERSEDED by a NEWER,
    // still-unrevoked `team_standard` grant (finding 3b) — must backfill to
    // `standard`, not `premium`: `CURRENT_GRANT_SQL`'s `ORDER BY granted_at
    // DESC LIMIT 1` picks the NEWEST unrevoked grant, not merely "any
    // unrevoked team_premium grant it happens to find" — this is the case
    // that would catch a backfill that used `EXISTS (... team_premium ...)`
    // instead of the real newest-wins selection.
    let (superseded_account, _) = make_org_account(&pool, "superseded").await;
    let t1 = DateTime::<Utc>::from_timestamp(1_700_000_000, 0).unwrap();
    let t2 = DateTime::<Utc>::from_timestamp(1_700_000_200, 0).unwrap();
    grant_plan_at(&pool, superseded_account, "team_premium", 2, t1, None).await;
    grant_plan_at(&pool, superseded_account, "team_standard", 2, t2, None).await;

    // ── 3. Apply the rest of this PR's migrations (0165-0169) ──────────────
    subset(&|v| v > 164 && v <= 169)
        .run(&pool)
        .await
        .expect("apply migrations_control 0165 through 0169");

    // ── 4. Assert the backfilled tier matches the account's real grant ─────
    async fn tiers_for(pool: &PgPool, account_id: Uuid) -> Vec<Option<String>> {
        let mut rows: Vec<Option<String>> = sqlx::query_scalar(
            "SELECT st.code FROM account_members m \
             LEFT JOIN seat_tiers st ON st.id = m.seat_tier_id \
             WHERE m.account_id = $1 ORDER BY m.user_id",
        )
        .bind(account_id)
        .fetch_all(pool)
        .await
        .expect("read backfilled seat tiers");
        rows.sort();
        rows
    }

    assert_eq!(
        tiers_for(&pool, personal_account).await,
        vec![None],
        "a personal account's member must never get a seat tier"
    );
    assert_eq!(
        tiers_for(&pool, no_grant_account).await,
        vec![Some("standard".to_string()), Some("standard".to_string())],
        "a team-org account with NO active grant must backfill its members to standard \
         (the correct floor, per 0169's header)"
    );
    assert_eq!(
        tiers_for(&pool, standard_account).await,
        vec![Some("standard".to_string()), Some("standard".to_string())],
        "a team-org account with an active team_standard grant must backfill its members \
         to standard"
    );
    assert_eq!(
        tiers_for(&pool, premium_account).await,
        vec![Some("premium".to_string()), Some("premium".to_string())],
        "a team-org account with an active team_premium grant must backfill its members to \
         PREMIUM, not standard — this is the exact bug review finding 1 exists to catch: \
         CURRENT_GRANT_SQL already entitles these members to the premium per-seat allowance, \
         and an unconditional `standard` backfill would silently understate it"
    );
    assert_eq!(
        tiers_for(&pool, revoked_premium_account).await,
        vec![Some("standard".to_string()), Some("standard".to_string())],
        "a team-org account whose ONLY team_premium grant is REVOKED must backfill its \
         members to standard, not premium — a revoked grant is not an active entitlement \
         (finding 3a)"
    );
    assert_eq!(
        tiers_for(&pool, superseded_account).await,
        vec![Some("standard".to_string()), Some("standard".to_string())],
        "a team-org account whose older team_premium grant is superseded by a newer, still- \
         unrevoked team_standard grant must backfill its members to standard — the NEWEST \
         unrevoked grant wins (CURRENT_GRANT_SQL's ORDER BY granted_at DESC), not merely any \
         unrevoked team_premium grant that exists (finding 3b)"
    );

    pool.close().await;
    admin(&base)
        .await
        .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}
