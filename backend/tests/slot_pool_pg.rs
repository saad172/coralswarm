//! Live regression suite for CS-435 (org-onboarding plan §3 items 7/15,
//! `docs/plans/org-onboarding-from-the-invite.md`, D1/D7/D9/D15) —
//! `billing::slot_pool`'s derived purchased/used/reserved/available
//! arithmetic, the `slot_reservations` reader/writer pair, and the expiry
//! sweep (`workers::slot_reservations::tick`). See `billing::slot_pool`'s
//! own module doc for the full framing this suite proves.
//!
//! `acquire_seat`'s consume-before-buy branch, `routes::org_slots`' HTTP
//! surface, and `billing::renewal::RenewalOutcome::seat_mismatch`'s
//! pool-aware drift are proven IN-CRATE instead
//! (`billing::seat_lifecycle::pg_tests` / `routes::org_slots::pg_tests` /
//! `billing::renewal::pg_tests` / `routes::stripe_webhook::pg_tests`) — see
//! this crate's own `check_pg_suite_coverage.sh` header for why: `routes`
//! is a crate-private module (`mod routes;` in `lib.rs`), so `get_slots`/
//! `purchase_slots` cannot be named from an external `backend/tests/*.rs`
//! binary at all, `pub` or not, and `acquire_seat`'s consume-before-buy path
//! needs the same mock-Stripe fixture kit `billing::seat_lifecycle::
//! pg_tests` already carries. This suite covers exactly the surface that
//! genuinely is reachable from outside the crate: `billing::slot_pool`
//! itself and the sweep worker.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB-per-test fixture pattern as every
//! other `*_pg.rs` file in this directory (`base_url`/`with_db`/`admin`/
//! `recreate`/`control_pool`, copied from
//! `backend/tests/member_ocean_lifecycle_pg.rs:119-150`).
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test slot_pool_pg -- \
//!     --test-threads=1 --nocapture

use chrono::Duration as ChronoDuration;
use coralswarm_backend::billing::slot_pool::{
    expire_reservations, read_slot_pool, read_slot_pool_for_tier, release_reservation, reserve_slot,
    ReleaseReason,
};
use coralswarm_backend::tenancy::scope::begin_account_scoped;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::slot_reservations::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
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
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect(&with_db(base, db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

/// Fresh scratch DB, uniquely named per test — avoids any cross-test
/// interference under `--test-threads=1` (the discipline every `*_pg.rs`
/// file in this directory already follows) and lets tests run with more
/// threads later without becoming flaky.
async fn fresh_pool(test_name: &str) -> Option<(coralswarm_backend::test_db::TestDatabase, PgPool)> {
    let Some(base) = base_url() else {
        eprintln!("skipping {test_name}: set ORGT_TEST_BASE_URL to run the live slot_pool suite");
        return None;
    };
    let db = coralswarm_backend::test_db::TestDatabase::from_url(&base, "cs435_slot_pool");
    let mut admin_conn = admin(&base).await;
    recreate(&mut admin_conn, &db).await;
    let pool = control_pool(&base, &db).await;
    Some((db, pool))
}

// ── Fixtures ──────────────────────────────────────────────────────────────

/// A `team`-plan org account with `purchased` standard slots and `used` of
/// them consumed by `used` distinct members (`seat_purchased_at` stamped).
/// Returns `(account_id, grant_id, member_user_ids)` — `member_user_ids` has
/// exactly `used` entries, one per consumed slot.
async fn seed_team_pool(pool: &PgPool, purchased: i64, used: i64) -> (Uuid, Uuid, Vec<Uuid>) {
    assert!(used <= purchased, "fixture bug: cannot consume more than purchased");
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("slot-pool-owner-{}@test.example", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert owner");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(format!("org_{}", Uuid::new_v4().simple()))
    .fetch_one(pool)
    .await
    .expect("insert org account");
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(pool)
        .await
        .expect("standard tier seeded by migration 0165");
    let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = 'team'")
        .fetch_one(pool)
        .await
        .expect("team plan seeded by migration 0167");
    let grant_id: Uuid = fetch_financial_fixture_id(
        pool,
        account_id,
        sqlx::query_scalar(
        "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
         VALUES ($1, $2, $3, true) RETURNING id",
    )
    .bind(account_id)
    .bind(plan_id)
    .bind(purchased as i32),
    )
    .await
    .expect("insert team grant");
    if purchased > 0 {
        execute_financial_fixture(
            pool,
            account_id,
            sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
            .bind(grant_id)
            .bind(standard_id)
            .bind(purchased as i32),
        )
            .await
            .expect("seed the purchased ledger");
    }

    let mut member_user_ids = Vec::with_capacity(used as usize);
    for i in 0..used {
        let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
            .bind(format!("slot-pool-member-{i}-{}@test.example", Uuid::new_v4().simple()))
            .fetch_one(pool)
            .await
            .expect("insert member user");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, now())",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(standard_id)
        .execute(pool)
        .await
        .expect("insert purchased member");
        member_user_ids.push(user_id);
    }
    (account_id, grant_id, member_user_ids)
}

/// A bare, unattached user — for a `reserve_slot` caller not yet counted in
/// `used`.
async fn seed_bare_user(pool: &PgPool) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("slot-pool-reserver-{}@test.example", Uuid::new_v4().simple()))
        .fetch_one(pool)
        .await
        .expect("insert bare user")
}

/// CS-442 (item 40, D8): add `purchased` seats at a SECOND tier
/// (`code`, e.g. `"premium"`) to an already-seeded `grant_id` — the
/// per-tier sibling of `seed_team_pool`'s own single-tier `standard` seed,
/// so a suite can prove `reserve_slot`/`read_slot_pool_for_tier` tell two
/// tiers' pools apart on the SAME grant. Returns the tier's `seat_tiers.id`.
async fn seed_second_tier_ledger(pool: &PgPool,
    account_id: Uuid,
    grant_id: Uuid, code: &str, purchased: i64) -> Uuid {
    let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active = true")
        .bind(code)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|e| panic!("seat_tiers {code:?} must be seeded (0165): {e}"));
    execute_financial_fixture(
        pool,
        account_id,
        sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
        .bind(grant_id)
        .bind(tier_id)
        .bind(purchased as i32),
    )
        .await
        .expect("seed the second tier's purchased ledger");
    tier_id
}

fn worker_tenant(pool: PgPool) -> WorkerTenant {
    // `store`/`tenant` — this suite only exercises `t.pool`/`t.tenant`
    // (`workers::slot_reservations::tick` never touches `t.store`);
    // `PostgresStore::new` wraps the
    // SAME control pool this suite already built, which is the one thing
    // `WorkerTenant` requires to be a valid value at all.
    WorkerTenant {
        tenant: TenantId::Consumer,
        store: std::sync::Arc::new(coralswarm_backend::store::PostgresStore::new(pool.clone())),
        pool,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────

/// `read_slot_pool`'s three numbers, independently: `purchased` from the
/// live `team` grant, `used` from purchased members, `reserved` from live
/// reservations — and `available()` is exactly `purchased - used - reserved`
/// when non-negative.
#[tokio::test]
async fn read_slot_pool_reports_purchased_used_reserved_available() {
    let Some((_db, pool)) = fresh_pool("read_slot_pool_reports_purchased_used_reserved_available").await
    else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 5, 2).await;
    let reserver = seed_bare_user(&pool).await;
    let reservation = reserve_slot(&pool, account_id, reserver, None, ChronoDuration::minutes(5))
        .await
        .expect("reserve_slot must succeed")
        .expect("pool has room — must return Some");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let snapshot = read_slot_pool(&mut tx, account_id).await.expect("read pool");
    tx.commit().await.unwrap();

    assert_eq!(snapshot.purchased, 5);
    assert_eq!(snapshot.used, 2);
    assert_eq!(snapshot.reserved, 1);
    assert_eq!(snapshot.available(), 2, "5 purchased - 2 used - 1 reserved = 2");

    release_reservation(&pool, reservation, ReleaseReason::Released)
        .await
        .expect("release must succeed");
}

/// `available() == 0` (`purchased == used`) ⇒ `reserve_slot` returns `None`
/// — the pool-full fallback path, not an error.
#[tokio::test]
async fn reserve_slot_returns_none_when_the_pool_is_full() {
    let Some((_db, pool)) = fresh_pool("reserve_slot_returns_none_when_the_pool_is_full").await else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 3, 3).await;
    let reserver = seed_bare_user(&pool).await;

    let result = reserve_slot(&pool, account_id, reserver, None, ChronoDuration::minutes(5))
        .await
        .expect("reserve_slot must succeed even when full");
    assert_eq!(result, None, "a full pool must refuse, not error");
}

/// **The double-spend D9 exists to close.** With exactly ONE slot left, two
/// concurrent `reserve_slot` calls for two DIFFERENT members race under the
/// SAME advisory lock — exactly one must win.
#[tokio::test]
async fn reserve_slot_is_race_safe_for_the_last_slot() {
    let Some((_db, pool)) = fresh_pool("reserve_slot_is_race_safe_for_the_last_slot").await else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 1, 0).await;
    let user_a = seed_bare_user(&pool).await;
    let user_b = seed_bare_user(&pool).await;

    let pool_a = pool.clone();
    let pool_b = pool.clone();
    let (result_a, result_b) = tokio::join!(
        tokio::spawn(async move {
            reserve_slot(&pool_a, account_id, user_a, None, ChronoDuration::minutes(5)).await
        }),
        tokio::spawn(async move {
            reserve_slot(&pool_b, account_id, user_b, None, ChronoDuration::minutes(5)).await
        }),
    );
    let result_a = result_a.expect("task a must not panic").expect("reserve_slot must not error");
    let result_b = result_b.expect("task b must not panic").expect("reserve_slot must not error");

    let winners = [result_a.is_some(), result_b.is_some()].into_iter().filter(|w| *w).count();
    assert_eq!(winners, 1, "exactly one of two concurrent callers must win the last slot");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let snapshot = read_slot_pool(&mut tx, account_id).await.expect("read pool");
    tx.commit().await.unwrap();
    assert_eq!(snapshot.reserved, 1, "the pool must never over-reserve past its own capacity");
}

/// A reservation for a member who ALREADY holds a live one is an upsert
/// (refreshed `expires_at`), not a second row — `reserved` stays at 1, never
/// double-charging the pool for one person (the partial UNIQUE index's own
/// job, `0233`'s own header).
#[tokio::test]
async fn reserve_slot_is_an_upsert_for_the_same_member() {
    let Some((_db, pool)) = fresh_pool("reserve_slot_is_an_upsert_for_the_same_member").await else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 2, 0).await;
    let user_id = seed_bare_user(&pool).await;

    let first = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::seconds(30))
        .await
        .expect("first reserve must succeed")
        .expect("pool has room");
    let second = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::minutes(10))
        .await
        .expect("second reserve (retry) must succeed")
        .expect("the SAME member retrying must still get Some, not None");
    assert_eq!(first, second, "a retry for the same member must refresh the SAME row, not insert a second");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let snapshot = read_slot_pool(&mut tx, account_id).await.expect("read pool");
    tx.commit().await.unwrap();
    assert_eq!(snapshot.reserved, 1, "one member holding a live reservation must count as ONE reserved slot");
}

/// `release_reservation` is idempotent under redelivery: a second call for
/// an already-resolved row affects nothing and returns `false`, never
/// overwriting the FIRST `release_reason`.
#[tokio::test]
async fn release_reservation_is_idempotent() {
    let Some((_db, pool)) = fresh_pool("release_reservation_is_idempotent").await else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 1, 0).await;
    let user_id = seed_bare_user(&pool).await;
    let id = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::minutes(5))
        .await
        .unwrap()
        .unwrap();

    let first = release_reservation(&pool, id, ReleaseReason::Fulfilled).await.unwrap();
    let second = release_reservation(&pool, id, ReleaseReason::Released).await.unwrap();
    assert!(first, "the first release must actually resolve the row");
    assert!(!second, "a second release of an already-resolved row must affect nothing");

    let reason: Option<String> = sqlx::query_scalar("SELECT release_reason FROM slot_reservations WHERE id = $1")
        .bind(id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(reason.as_deref(), Some("fulfilled"), "the FIRST release's reason must survive the second, no-op call");
}

/// An expired-but-unresolved reservation is excluded from `reserved`
/// (`read_slot_pool`'s own `expires_at > now()` predicate) even before any
/// sweep runs, and `expire_reservations` — called inside an account-scoped
/// transaction, matching `workers::slot_reservations::tick`'s own per-account
/// loop shape — marks it `expired`.
#[tokio::test]
async fn expired_reservation_is_excluded_and_the_sweep_marks_it_expired() {
    let Some((_db, pool)) = fresh_pool("expired_reservation_is_excluded_and_the_sweep_marks_it_expired").await
    else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 1, 0).await;
    let user_id = seed_bare_user(&pool).await;

    // Reserve, then force it into the past directly — `reserve_slot` itself
    // never accepts a negative TTL, so backdating `expires_at` after the
    // fact is the only way to build an already-expired fixture.
    let id = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE slot_reservations SET expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let snapshot = read_slot_pool(&mut tx, account_id).await.expect("read pool");
    tx.commit().await.unwrap();
    assert_eq!(snapshot.reserved, 0, "an expired-but-unswept reservation must not count as reserved");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let expired = expire_reservations(&mut tx).await.expect("sweep must succeed");
    tx.commit().await.unwrap();
    assert_eq!(expired, 1);

    let (released_at, reason): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) = sqlx::query_as(
        "SELECT released_at, release_reason FROM slot_reservations WHERE id = $1")
    .bind(id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(released_at.is_some());
    assert_eq!(reason.as_deref(), Some("expired"));
}

/// `workers::slot_reservations::tick` — the FULL registered worker, not just
/// `expire_reservations` directly — expires a past-due reservation on the
/// `TenantId::Consumer` pass, and is a clean no-op for `TenantId::Org`
/// (control-only table, no tenant-tree twin — see that module's own doc
/// comment).
#[tokio::test]
async fn tick_expires_on_the_consumer_pass_and_is_a_noop_for_org() {
    let Some((_db, pool)) = fresh_pool("tick_expires_on_the_consumer_pass_and_is_a_noop_for_org").await else {
        return;
    };
    let (account_id, _grant_id, _members) = seed_team_pool(&pool, 1, 0).await;
    let user_id = seed_bare_user(&pool).await;
    let id = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::minutes(5))
        .await
        .unwrap()
        .unwrap();
    sqlx::query("UPDATE slot_reservations SET expires_at = now() - interval '1 minute' WHERE id = $1")
        .bind(id)
        .execute(&pool)
        .await
        .unwrap();

    let mut org_tenant = worker_tenant(pool.clone());
    org_tenant.tenant = TenantId::Org("org_slot_pool_test".to_string());
    let org_result = tick(&org_tenant).await.expect("org pass must not error");
    assert_eq!(org_result.processed, 0, "an Org pass must never touch the control-only slot_reservations table");

    let (released_before,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT released_at FROM slot_reservations WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(released_before.is_none(), "the Org pass must not have expired anything");

    let consumer_tenant = worker_tenant(pool.clone());
    let consumer_result = tick(&consumer_tenant).await.expect("consumer pass must succeed");
    assert!(consumer_result.processed >= 1, "the Consumer pass must expire the past-due reservation");

    let (released_after,): (Option<chrono::DateTime<chrono::Utc>>,) =
        sqlx::query_as("SELECT released_at FROM slot_reservations WHERE id = $1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(released_after.is_some());
}

// ── CS-442 (item 40, D8): per-tier accounting ───────────────────────────────

/// `read_slot_pool_for_tier` tells two tiers on the SAME grant apart:
/// `standard` fully used, `premium` fully free — each tier's own
/// `purchased`/`used`/`available()` must reflect ONLY that tier's ledger,
/// never the account-wide total `read_slot_pool` reports.
#[tokio::test]
async fn read_slot_pool_for_tier_is_scoped_to_one_tier() {
    let Some((_db, pool)) = fresh_pool("read_slot_pool_for_tier_is_scoped_to_one_tier").await else {
        return;
    };
    // standard: purchased 1, used 1 (full). premium: purchased 1, used 0 (free).
    let (account_id, grant_id, _members) = seed_team_pool(&pool, 1, 1).await;
    let premium_id = seed_second_tier_ledger(&pool, account_id, grant_id, "premium", 1).await;
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let standard_pool = read_slot_pool_for_tier(&mut tx, account_id, standard_id).await.expect("read standard pool");
    let premium_pool = read_slot_pool_for_tier(&mut tx, account_id, premium_id).await.expect("read premium pool");
    let total = read_slot_pool(&mut tx, account_id).await.expect("read total pool");
    tx.commit().await.unwrap();

    assert_eq!(standard_pool.purchased, 1);
    assert_eq!(standard_pool.used, 1);
    assert_eq!(standard_pool.available(), 0, "standard is full");
    assert_eq!(premium_pool.purchased, 1);
    assert_eq!(premium_pool.used, 0);
    assert_eq!(premium_pool.available(), 1, "premium is untouched and free");
    assert_eq!(total.purchased, 2, "the account-wide total must still sum BOTH tiers");
    assert_eq!(total.used, 1);
}

/// `reserve_slot(.., Some(tier))` reserves against THAT tier's own
/// availability, not the account's total — a `standard`-full,
/// `premium`-free account refuses a `standard` reservation but grants a
/// `premium` one, and the resulting `reserved` count is scoped to the tier
/// it was taken against.
#[tokio::test]
async fn reserve_slot_is_scoped_to_the_requested_tier() {
    let Some((_db, pool)) = fresh_pool("reserve_slot_is_scoped_to_the_requested_tier").await else {
        return;
    };
    let (account_id, grant_id, _members) = seed_team_pool(&pool, 1, 1).await;
    let premium_id = seed_second_tier_ledger(&pool, account_id, grant_id, "premium", 1).await;
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(&pool)
        .await
        .unwrap();

    let standard_claimant = seed_bare_user(&pool).await;
    let refused = reserve_slot(&pool, account_id, standard_claimant, Some(standard_id), ChronoDuration::minutes(5))
        .await
        .expect("reserve_slot must succeed even when the requested tier is full");
    assert_eq!(refused, None, "standard has zero availability — must refuse");

    let premium_claimant = seed_bare_user(&pool).await;
    let granted = reserve_slot(&pool, account_id, premium_claimant, Some(premium_id), ChronoDuration::minutes(5))
        .await
        .expect("reserve_slot must succeed")
        .expect("premium has one free slot — must return Some");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let standard_pool = read_slot_pool_for_tier(&mut tx, account_id, standard_id).await.expect("read standard pool");
    let premium_pool = read_slot_pool_for_tier(&mut tx, account_id, premium_id).await.expect("read premium pool");
    tx.commit().await.unwrap();
    assert_eq!(standard_pool.reserved, 0, "the refused standard attempt must not have written a row");
    assert_eq!(premium_pool.reserved, 1, "the granted premium reservation must count against premium only");

    release_reservation(&pool, granted, ReleaseReason::Released).await.expect("release must succeed");
}

/// Migration 0241's own NULL semantics: a reservation written with
/// `seat_tier_id = NULL` (the pre-CS-442 shape — `reserve_slot(.., None,
/// ..)`) counts against `standard` on read, never against `premium` and
/// never as "unscoped".
#[tokio::test]
async fn a_null_seat_tier_id_reservation_counts_against_standard() {
    let Some((_db, pool)) = fresh_pool("a_null_seat_tier_id_reservation_counts_against_standard").await else {
        return;
    };
    let (account_id, grant_id, _members) = seed_team_pool(&pool, 1, 0).await;
    let premium_id = seed_second_tier_ledger(&pool, account_id, grant_id, "premium", 1).await;
    let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(&pool)
        .await
        .unwrap();
    let user_id = seed_bare_user(&pool).await;

    let id = reserve_slot(&pool, account_id, user_id, None, ChronoDuration::minutes(5))
        .await
        .expect("reserve_slot must succeed")
        .expect("standard has one free slot");
    let stamped_tier: Option<Uuid> =
        sqlx::query_scalar("SELECT seat_tier_id FROM slot_reservations WHERE id = $1").bind(id).fetch_one(&pool).await.unwrap();
    assert_eq!(stamped_tier, None, "a `None` caller writes a NULL column, unchanged from pre-CS-442 rows");

    let mut tx = begin_account_scoped(&pool, account_id).await.expect("scope");
    let standard_pool = read_slot_pool_for_tier(&mut tx, account_id, standard_id).await.expect("read standard pool");
    let premium_pool = read_slot_pool_for_tier(&mut tx, account_id, premium_id).await.expect("read premium pool");
    tx.commit().await.unwrap();
    assert_eq!(standard_pool.reserved, 1, "a NULL seat_tier_id reservation counts against standard");
    assert_eq!(premium_pool.reserved, 0, "and never against a different tier");

    release_reservation(&pool, id, ReleaseReason::Released).await.expect("release must succeed");
}
