//! Live-Postgres tests for the funding policy: pre-flight reasons, the
//! allowance/extra split at settlement, idempotent replay, concurrency, period
//! rollover, and the managed reservation path.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` like the rest of `billing::`. Without
//! it each test prints a skip line and returns, which is a vacuous pass: CI
//! runs this module through the `billing::funding_policy::` filter, which
//! fails if it matches zero tests.
//!
//! The pool connects as the database owner, so RLS is bypassed, but the
//! 0321 financial entry fence is a trigger and fires for every role: a write
//! without `billing_enter_account` fails here exactly as in production.

use chrono::{DateTime, Duration, Utc};
use sqlx::{types::Decimal, PgPool};
use uuid::Uuid;

use super::db::{evaluate, write_extra_debit, ExtraDebit, Mode, PeriodSel, SettlementPlanner, Split};
use super::{ExtraFunding, FundingOutcome, Intent, RefusalReason};
use crate::billing::enforcement::{check_funding, managed_funding_tx, ManagedAllocation, Spend};
use crate::workers::billing_derivation::native_job_debit;

pub(super) const RATE: i64 = 22_000;

pub(super) fn d(n: i64) -> Decimal {
    Decimal::from(n)
}

pub(super) async fn pg(test: &str) -> Option<PgPool> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping {test}: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(24)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

#[derive(Clone, Copy, PartialEq)]
pub(super) enum Plan {
    Free,
    Pro,
    Team,
}

pub(super) struct Acct {
    pub(super) id: Uuid,
    pub(super) user: Uuid,
    pub(super) start: DateTime<Utc>,
    pub(super) end: DateTime<Utc>,
}

/// A scoped transaction that has entered the account's financial fence.
pub(super) async fn fenced(pool: &PgPool, account: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("enter");
    tx
}

pub(super) async fn new_user(pool: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(id)
        .bind(format!("{id}@funding-policy.test"))
        .execute(pool)
        .await
        .expect("user");
    id
}

/// An account on `plan` with one member holding a `granted`/`consumed`
/// allowance for the current window and an empty plan pool.
pub(super) async fn seed(pool: &PgPool, plan: Plan, granted: i64, consumed: i64) -> Acct {
    let user = new_user(pool).await;
    let id = Uuid::new_v4();
    if plan == Plan::Team {
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
            .bind(id)
            .bind(user)
            .bind(format!("org_{}", id.simple()))
            .execute(pool)
            .await
            .expect("org account");
    } else {
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(id)
            .bind(user)
            .execute(pool)
            .await
            .expect("account");
    }
    let code = match plan {
        Plan::Free => "free",
        Plan::Pro => "pro",
        Plan::Team => "team_standard",
    };
    let (plan_id, seat_priced): (Uuid, bool) =
        sqlx::query_as("SELECT id, is_seat_priced FROM plans WHERE code=$1")
            .bind(code)
            .fetch_one(pool)
            .await
            .expect("plan seeded");
    let start = Utc::now() - Duration::days(1);
    let end = Utc::now() + Duration::days(29);
    let mut tx = fenced(pool, id).await;
    sqlx::query(
        "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1,$2,$3,$4)",
    )
    .bind(id)
    .bind(plan_id)
    .bind(seat_priced.then_some(1))
    .bind(seat_priced)
    .execute(&mut *tx)
    .await
    .expect("grant");
    sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 0)")
        .bind(id)
        .execute(&mut *tx)
        .await
        .expect("pool");
    tx.commit().await.expect("commit");
    let acct = Acct { id, user, start, end };
    add_period(pool, &acct, user, granted, consumed).await;
    acct
}

pub(super) async fn add_period(pool: &PgPool, a: &Acct, user: Uuid, granted: i64, consumed: i64) {
    let mut tx = fenced(pool, a.id).await;
    sqlx::query(
        "INSERT INTO member_allowance_periods \
            (account_id, user_id, period_start, period_end, granted_credits, consumed_credits) \
         VALUES ($1,$2,$3,$4,$5,$6)",
    )
    .bind(a.id)
    .bind(user)
    .bind(a.start)
    .bind(a.end)
    .bind(d(granted))
    .bind(d(consumed))
    .execute(&mut *tx)
    .await
    .expect("period");
    tx.commit().await.expect("commit");
}

pub(super) async fn add_member(pool: &PgPool, a: &Acct, granted: i64, consumed: i64) -> Uuid {
    let user = new_user(pool).await;
    add_period(pool, a, user, granted, consumed).await;
    user
}

pub(super) async fn set_pool(pool: &PgPool, a: &Acct, balance: i64) {
    let mut tx = fenced(pool, a.id).await;
    sqlx::query("UPDATE credit_balances SET balance=$2 WHERE account_id=$1")
        .bind(a.id)
        .bind(d(balance))
        .execute(&mut *tx)
        .await
        .expect("pool");
    tx.commit().await.expect("commit");
}

pub(super) async fn set_extra(pool: &PgPool, a: &Acct, enabled: bool, limit: Option<i64>, multiple: Option<i64>) {
    sqlx::query(
        "INSERT INTO extra_usage_settings (account_id, enabled, monthly_limit_micro_usd, member_cap_seat_multiple) \
         VALUES ($1,$2,$3,$4) ON CONFLICT (account_id) DO UPDATE SET enabled=EXCLUDED.enabled, \
            monthly_limit_micro_usd=EXCLUDED.monthly_limit_micro_usd, \
            member_cap_seat_multiple=EXCLUDED.member_cap_seat_multiple",
    )
    .bind(a.id)
    .bind(enabled)
    .bind(limit)
    .bind(multiple.map(d))
    .execute(pool)
    .await
    .expect("settings");
}

pub(super) async fn set_extra_balance(pool: &PgPool, a: &Acct, micro: i64) {
    let mut tx = fenced(pool, a.id).await;
    sqlx::query(
        "INSERT INTO extra_usage_balances (account_id, balance_micro_usd) VALUES ($1,$2) \
         ON CONFLICT (account_id) DO UPDATE SET balance_micro_usd=EXCLUDED.balance_micro_usd",
    )
    .bind(a.id)
    .bind(micro)
    .execute(&mut *tx)
    .await
    .expect("extra balance");
    tx.commit().await.expect("commit");
}

pub(super) async fn set_subscription(pool: &PgPool, a: &Acct, status: &str) {
    let mut tx = fenced(pool, a.id).await;
    sqlx::query(
        "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) VALUES ($1,$2,$3) \
         ON CONFLICT (account_id) DO UPDATE SET status=EXCLUDED.status",
    )
    .bind(a.id)
    .bind(format!("cus_{}", Uuid::new_v4().simple()))
    .bind(status)
    .execute(&mut *tx)
    .await
    .expect("subscription");
    tx.commit().await.expect("commit");
}

/// A Pro account with its 100-credit allowance spent and extra usage on:
/// $50 limit, `balance` micro-USD.
pub(super) async fn pro_with_extra(pool: &PgPool, balance: i64) -> Acct {
    let a = seed(pool, Plan::Pro, 100, 100).await;
    set_extra(pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(pool, &a, balance).await;
    a
}

pub(super) async fn preflight(pool: &PgPool, a: &Acct, user: Uuid) -> Option<RefusalReason> {
    check_funding(pool, a.id, user, Spend::Background).await.expect("check_funding")
}

pub(super) async fn debit(pool: &PgPool, a: &Acct, payer: Option<Uuid>, credits: i64) -> Uuid {
    let job = Uuid::new_v4();
    native_job_debit(pool, a.id, payer, job, d(credits)).await.expect("settle");
    job
}

pub(super) async fn extra_balance(pool: &PgPool, a: &Acct) -> i64 {
    sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(pool)
        .await
        .expect("extra balance")
}

/// `(rows, sum(micro))` of this account's extra debits.
pub(super) async fn extra_debits(pool: &PgPool, a: &Acct) -> (i64, i64) {
    sqlx::query_as(
        "SELECT count(*), COALESCE(sum(-amount_micro_usd),0)::bigint FROM extra_usage_ledger \
          WHERE account_id=$1 AND entry_kind='debit'",
    )
    .bind(a.id)
    .fetch_one(pool)
    .await
    .expect("extra debits")
}

pub(super) async fn plan_ledger_sum(pool: &PgPool, a: &Acct) -> Decimal {
    sqlx::query_scalar("SELECT COALESCE(sum(credits),0) FROM credit_ledger WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(pool)
        .await
        .expect("plan ledger")
}

pub(super) async fn consumed(pool: &PgPool, a: &Acct, user: Uuid) -> Decimal {
    sqlx::query_scalar(
        "SELECT consumed_credits FROM member_allowance_periods WHERE account_id=$1 AND user_id=$2",
    )
    .bind(a.id)
    .bind(user)
    .fetch_one(pool)
    .await
    .expect("consumed")
}

pub(super) async fn pool_balance(pool: &PgPool, a: &Acct) -> Decimal {
    sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(pool)
        .await
        .expect("pool balance")
}

// ── pre-flight: every reason, from real rows ───────────────────────────────

#[tokio::test]
async fn preflight_reports_the_reason_for_each_gate() {
    let Some(pool) = pg("preflight_reports_the_reason_for_each_gate").await else { return };

    // Free, allowance spent.
    let free = seed(&pool, Plan::Free, 100, 100).await;
    assert_eq!(preflight(&pool, &free, free.user).await, Some(RefusalReason::FreePlan));
    // ... a Free account with settings that somehow say enabled is still Free.
    set_extra(&pool, &free, true, Some(1_000_000), None).await;
    set_extra_balance(&pool, &free, 1_000_000).await;
    assert_eq!(preflight(&pool, &free, free.user).await, Some(RefusalReason::FreePlan));

    // Pro, no settings row: extra usage is off.
    let pro = seed(&pool, Plan::Pro, 100, 100).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, Some(RefusalReason::ExtraUsageOff));
    // Disabled row: still off.
    set_extra(&pool, &pro, false, None, None).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, Some(RefusalReason::ExtraUsageOff));
    // Enabled, no balance row yet.
    set_extra(&pool, &pro, true, Some(50_000_000), None).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, Some(RefusalReason::ExtraBalanceExhausted));
    // Funded.
    set_extra_balance(&pool, &pro, 1_000).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, None);
    // Frozen by a cancellation beats everything else.
    set_subscription(&pool, &pro, "canceled").await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, Some(RefusalReason::CreditsFrozen));
    set_subscription(&pool, &pro, "active").await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, None);
}

#[tokio::test]
async fn preflight_inside_the_allowance_never_looks_at_extra_usage() {
    let Some(pool) = pg("preflight_inside_the_allowance_never_looks_at_extra_usage").await else { return };
    let a = seed(&pool, Plan::Free, 100, 99).await;
    assert_eq!(preflight(&pool, &a, a.user).await, None);
    let a = seed(&pool, Plan::Free, 100, 100).await;
    set_pool(&pool, &a, 1).await;
    assert_eq!(preflight(&pool, &a, a.user).await, None, "pool credits are plan allowance");
}

#[tokio::test]
async fn preflight_reports_the_monthly_limit_and_the_member_cap() {
    let Some(pool) = pg("preflight_reports_the_monthly_limit_and_the_member_cap").await else { return };

    let pro = pro_with_extra(&pool, 60_000_000).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, None);
    debit(&pool, &pro, Some(pro.user), 2272).await; // 2272 * 22_000 = 49_984_000
    assert_eq!(preflight(&pool, &pro, pro.user).await, None, "16_000 micro-USD of room is left");
    set_extra(&pool, &pro, true, Some(49_984_000), None).await;
    assert_eq!(preflight(&pool, &pro, pro.user).await, Some(RefusalReason::MonthlyLimitReached));

    // A team org: 1x the seat allowance (100 credits) per member.
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    let second = add_member(&pool, &org, 100, 100).await;
    debit(&pool, &org, Some(org.user), 100).await;
    assert_eq!(preflight(&pool, &org, org.user).await, Some(RefusalReason::MemberCapReached));
    assert_eq!(preflight(&pool, &org, second).await, None, "the cap is per member");
}

// ── settlement: the split ──────────────────────────────────────────────────

#[tokio::test]
async fn a_charge_straddling_the_allowance_splits_between_the_plan_and_extra_usage() {
    let Some(pool) = pg("a_charge_straddling_the_allowance_splits_between_the_plan_and_extra_usage").await else {
        return;
    };
    let a = seed(&pool, Plan::Pro, 100, 95).await;
    set_pool(&pool, &a, 5).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;

    let job = debit(&pool, &a, Some(a.user), 8).await;

    // Plan ledger: only the 5 credits the allowance bore.
    let plan: Decimal = sqlx::query_scalar("SELECT credits FROM credit_ledger WHERE source_job_id=$1")
        .bind(job)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(plan, d(-5));
    assert_eq!(pool_balance(&pool, &a).await, d(0));
    assert_eq!(consumed(&pool, &a, a.user).await, d(100));

    // Extra ledger: 3 credits at the snapshotted rate.
    let row: (i64, Decimal, i64, Option<Uuid>, DateTime<Utc>, String, String) = sqlx::query_as(
        "SELECT amount_micro_usd, credits, rate_micro_usd_per_credit, user_id, period_start, \
                idempotency_key, entry_kind FROM extra_usage_ledger WHERE account_id=$1",
    )
    .bind(a.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(row.0, -66_000);
    assert_eq!(row.1, d(3));
    assert_eq!(row.2, RATE);
    assert_eq!(row.3, Some(a.user));
    let month_start: DateTime<Utc> =
        sqlx::query_scalar("SELECT date_trunc('month', now() AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(row.4, month_start, "counts in the UTC calendar month");
    assert_eq!(row.5, format!("debit:job:{}:{job}", a.id));
    assert_eq!(row.6, "debit");
    assert_eq!(extra_balance(&pool, &a).await, 10_000_000 - 66_000);
    let (spent, spent_credits): (i64, Decimal) = sqlx::query_as(
        "SELECT spent_micro_usd, spent_credits FROM extra_usage_periods WHERE account_id=$1",
    )
    .bind(a.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!((spent, spent_credits), (66_000, d(3)));
}

#[tokio::test]
async fn a_charge_inside_the_allowance_writes_nothing_to_extra_usage() {
    let Some(pool) = pg("a_charge_inside_the_allowance_writes_nothing_to_extra_usage").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 10).await;
    set_pool(&pool, &a, 90).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 1_000_000).await;
    debit(&pool, &a, Some(a.user), 30).await;
    assert_eq!(plan_ledger_sum(&pool, &a).await, d(-30));
    assert_eq!(extra_debits(&pool, &a).await, (0, 0));
    assert_eq!(extra_balance(&pool, &a).await, 1_000_000);
}

#[tokio::test]
async fn extra_usage_off_leaves_the_whole_overshoot_on_the_plan_ledger_as_before() {
    let Some(pool) = pg("extra_usage_off_leaves_the_whole_overshoot_on_the_plan_ledger_as_before").await else {
        return;
    };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    debit(&pool, &a, Some(a.user), 7).await;
    assert_eq!(plan_ledger_sum(&pool, &a).await, d(-7));
    assert_eq!(pool_balance(&pool, &a).await, d(-7));
    assert_eq!(consumed(&pool, &a, a.user).await, d(107));
    assert_eq!(extra_debits(&pool, &a).await, (0, 0));
}

#[tokio::test]
async fn a_debit_larger_than_the_balance_is_clamped_and_the_rest_is_unfunded() {
    let Some(pool) = pg("a_debit_larger_than_the_balance_is_clamped_and_the_rest_is_unfunded").await else {
        return;
    };
    let a = pro_with_extra(&pool, 30_000).await;
    debit(&pool, &a, Some(a.user), 3).await; // wants 66_000

    assert_eq!(extra_balance(&pool, &a).await, 0, "spent exactly what was there");
    assert_eq!(extra_debits(&pool, &a).await, (1, 30_000));
    // The remainder is unfunded: it stays on the plan ledger, so the plan pool
    // goes below zero exactly as any overshoot always did.
    let funded_credits = Decimal::from(30_000) / Decimal::from(RATE);
    let unfunded = d(3) - funded_credits;
    assert_eq!(plan_ledger_sum(&pool, &a).await, -unfunded);
    assert_eq!(pool_balance(&pool, &a).await, -unfunded);
    // And now the gate says why.
    assert_eq!(preflight(&pool, &a, a.user).await, Some(RefusalReason::ExtraBalanceExhausted));
}

#[tokio::test]
async fn a_debit_larger_than_the_limit_room_is_clamped_to_the_limit() {
    let Some(pool) = pg("a_debit_larger_than_the_limit_room_is_clamped_to_the_limit").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(44_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    debit(&pool, &a, Some(a.user), 5).await; // wants 110_000, room 44_000
    assert_eq!(extra_debits(&pool, &a).await, (1, 44_000));
    let reached: Option<DateTime<Utc>> =
        sqlx::query_scalar("SELECT limit_reached_at FROM extra_usage_periods WHERE account_id=$1")
            .bind(a.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert!(reached.is_some(), "the first time spend reaches the limit is recorded");
    assert_eq!(preflight(&pool, &a, a.user).await, Some(RefusalReason::MonthlyLimitReached));
    // Further debits cannot add to extra usage at all.
    debit(&pool, &a, Some(a.user), 5).await;
    assert_eq!(extra_debits(&pool, &a).await, (1, 44_000));
}

#[tokio::test]
async fn a_member_debit_is_clamped_to_the_member_cap() {
    let Some(pool) = pg("a_member_debit_is_clamped_to_the_member_cap").await else { return };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    debit(&pool, &org, Some(org.user), 60).await;
    debit(&pool, &org, Some(org.user), 60).await; // only 40 credits of cap room
    assert_eq!(extra_debits(&pool, &org).await, (2, (60 + 40) * RATE));
    let capped: Decimal = sqlx::query_scalar(
        "SELECT COALESCE(sum(credits),0) FROM extra_usage_ledger WHERE account_id=$1 AND user_id=$2",
    )
    .bind(org.id)
    .bind(org.user)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(capped, d(100), "exactly 1x the seat allowance");
    assert_eq!(plan_ledger_sum(&pool, &org).await, d(-20), "the 20 over the cap are unfunded");
}

#[tokio::test]
async fn unattributed_team_spend_is_charged_to_the_org_limit_and_balance() {
    let Some(pool) = pg("unattributed_team_spend_is_charged_to_the_org_limit_and_balance").await else { return };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(8 * RATE), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    // No member to cap: the org limit is what bounds it.
    debit(&pool, &org, None, 5).await;
    assert_eq!(extra_debits(&pool, &org).await, (1, 5 * RATE));
    let user_id: Option<Uuid> =
        sqlx::query_scalar("SELECT user_id FROM extra_usage_ledger WHERE account_id=$1")
            .bind(org.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(user_id, None, "charged to the org, not to a member");
    debit(&pool, &org, None, 5).await; // only 3 credits of limit room
    assert_eq!(extra_debits(&pool, &org).await, (2, 8 * RATE));
    // The gate closes for members, so unattributed spend can never be free overshoot.
    assert_eq!(preflight(&pool, &org, org.user).await, Some(RefusalReason::MonthlyLimitReached));
    // A personal account has no cap, so it behaves the same.
    let pro = pro_with_extra(&pool, 10_000_000).await;
    debit(&pool, &pro, None, 5).await;
    assert_eq!(extra_debits(&pool, &pro).await, (1, 5 * RATE));
}

#[tokio::test]
async fn unattributed_spend_can_be_attributed_to_the_account_owner_instead() {
    let Some(pool) = pg("unattributed_spend_can_be_attributed_to_the_account_owner_instead").await else {
        return;
    };
    use super::db::UnattributedSpend;
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    let mut tx = fenced(&pool, org.id).await;
    let mut planner = SettlementPlanner::with_policy(org.id, UnattributedSpend::Owner);
    let split = planner.plan(&mut tx, None, PeriodSel::Current, d(5)).await.unwrap();
    assert_eq!(split.extra_payer, Some(org.user), "the owner pays");
    assert_eq!(split.extra.micro_usd, 5 * RATE);
    planner.record(&split);
    // The owner's cap (1x 100 credits) now applies: 95 more fit, not 100.
    let more = planner.plan(&mut tx, None, PeriodSel::Current, d(100)).await.unwrap();
    assert_eq!(more.extra.credits, d(95));
    tx.rollback().await.unwrap();
}

// ── idempotency ────────────────────────────────────────────────────────────

#[tokio::test]
async fn replaying_the_same_source_job_never_double_debits() {
    let Some(pool) = pg("replaying_the_same_source_job_never_double_debits").await else { return };
    let a = pro_with_extra(&pool, 10_000_000).await;
    let job = Uuid::new_v4();
    native_job_debit(&pool, a.id, Some(a.user), job, d(4)).await.unwrap();
    let after_first = (
        extra_debits(&pool, &a).await,
        extra_balance(&pool, &a).await,
        plan_ledger_sum(&pool, &a).await,
        consumed(&pool, &a, a.user).await,
    );
    assert_eq!(after_first.0, (1, 4 * RATE));
    for _ in 0..3 {
        let inserted = native_job_debit(&pool, a.id, Some(a.user), job, d(4)).await.unwrap();
        assert_eq!(inserted, 0, "the plan ledger already has this job");
    }
    // The state moved between the attempts (the allowance grew back), which
    // would change the split if the replay were decided again. It is not.
    let mut tx = fenced(&pool, a.id).await;
    sqlx::query("UPDATE member_allowance_periods SET consumed_credits=0 WHERE account_id=$1")
        .bind(a.id)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    native_job_debit(&pool, a.id, Some(a.user), job, d(4)).await.unwrap();
    assert_eq!(extra_debits(&pool, &a).await, after_first.0);
    assert_eq!(extra_balance(&pool, &a).await, after_first.1);
    assert_eq!(plan_ledger_sum(&pool, &a).await, after_first.2);
}

#[tokio::test]
async fn concurrent_replays_of_one_job_debit_once() {
    let Some(pool) = pg("concurrent_replays_of_one_job_debit_once").await else { return };
    let a = pro_with_extra(&pool, 10_000_000).await;
    let job = Uuid::new_v4();
    let mut tasks = Vec::new();
    for _ in 0..8 {
        let (pool, id, user) = (pool.clone(), a.id, a.user);
        tasks.push(tokio::spawn(async move {
            native_job_debit(&pool, id, Some(user), job, d(2)).await.unwrap()
        }));
    }
    let mut inserted = 0;
    for t in tasks {
        inserted += t.await.unwrap();
    }
    assert_eq!(inserted, 1);
    assert_eq!(extra_debits(&pool, &a).await, (1, 2 * RATE));
    assert_eq!(extra_balance(&pool, &a).await, 10_000_000 - 2 * RATE);
}

// ── concurrency ────────────────────────────────────────────────────────────

#[tokio::test]
async fn concurrent_debits_cannot_overshoot_the_monthly_limit() {
    let Some(pool) = pg("concurrent_debits_cannot_overshoot_the_monthly_limit").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(3 * RATE), None).await; // room for exactly 3 credits
    set_extra_balance(&pool, &a, 10_000_000).await;
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let (pool, id, user) = (pool.clone(), a.id, a.user);
        tasks.push(tokio::spawn(async move {
            native_job_debit(&pool, id, Some(user), Uuid::new_v4(), d(1)).await.unwrap()
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    assert_eq!(extra_debits(&pool, &a).await, (3, 3 * RATE), "never past the limit");
    assert_eq!(extra_balance(&pool, &a).await, 10_000_000 - 3 * RATE);
    assert_eq!(plan_ledger_sum(&pool, &a).await, d(-7), "the other 7 are unfunded overshoot");
    let spent: i64 =
        sqlx::query_scalar("SELECT spent_micro_usd FROM extra_usage_periods WHERE account_id=$1")
            .bind(a.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(spent, 3 * RATE);
}

#[tokio::test]
async fn concurrent_debits_cannot_drive_the_balance_negative() {
    let Some(pool) = pg("concurrent_debits_cannot_drive_the_balance_negative").await else { return };
    let a = pro_with_extra(&pool, 2 * RATE + 1_000).await; // two credits and change
    let mut tasks = Vec::new();
    for _ in 0..10 {
        let (pool, id, user) = (pool.clone(), a.id, a.user);
        tasks.push(tokio::spawn(async move {
            native_job_debit(&pool, id, Some(user), Uuid::new_v4(), d(1)).await.unwrap()
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let balance = extra_balance(&pool, &a).await;
    assert_eq!(balance, 0, "spent to exactly zero, never below");
    let (rows, spent) = extra_debits(&pool, &a).await;
    assert_eq!(spent, 2 * RATE + 1_000);
    assert_eq!(rows, 3, "two whole credits and the clamped remainder");
    // Conservation: every credit charged is on exactly one ledger.
    let extra_credits: Decimal = sqlx::query_scalar(
        "SELECT COALESCE(sum(credits),0) FROM extra_usage_ledger WHERE account_id=$1 AND entry_kind='debit'",
    )
    .bind(a.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(extra_credits + (-plan_ledger_sum(&pool, &a).await), d(10));
}

// ── allowance rollover and the calendar month ─────────────────────────────

/// Renewal cuts the open allowance period at a boundary and starts a new one
/// with a fresh allowance (the shape `roll_period` produces).
async fn roll_allowance(pool: &PgPool, a: &Acct, user: Uuid, granted: i64) {
    let boundary = Utc::now() - Duration::seconds(1);
    let mut tx = fenced(pool, a.id).await;
    sqlx::query("UPDATE member_allowance_periods SET period_end=$2 WHERE account_id=$1")
        .bind(a.id)
        .bind(boundary)
        .execute(&mut *tx)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO member_allowance_periods (account_id,user_id,period_start,period_end,granted_credits) \
         VALUES ($1,$2,$3,$4,$5)",
    )
    .bind(a.id)
    .bind(user)
    .bind(boundary)
    .bind(boundary + Duration::days(30))
    .bind(d(granted))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn an_allowance_rollover_gives_fresh_allowance_but_not_a_fresh_extra_limit_or_cap() {
    let Some(pool) = pg("an_allowance_rollover_gives_fresh_allowance_but_not_a_fresh_extra_limit_or_cap").await
    else {
        return;
    };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(100 * RATE), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    debit(&pool, &org, Some(org.user), 100).await;
    assert_eq!(preflight(&pool, &org, org.user).await, Some(RefusalReason::MemberCapReached));

    roll_allowance(&pool, &org, org.user, 100).await;

    // The new allowance is plan allowance again, so the member is admitted ...
    assert_eq!(preflight(&pool, &org, org.user).await, None);
    debit(&pool, &org, Some(org.user), 100).await;
    assert_eq!(extra_debits(&pool, &org).await.0, 1, "100 credits fit the new allowance");
    // ... but extra usage is counted per calendar month, so the cap and the
    // limit already used this month are not reset by a mid-month rollover.
    assert_eq!(preflight(&pool, &org, org.user).await, Some(RefusalReason::MemberCapReached));
    debit(&pool, &org, Some(org.user), 10).await;
    assert_eq!(extra_debits(&pool, &org).await, (1, 100 * RATE));
    let periods: i64 = sqlx::query_scalar("SELECT count(*) FROM extra_usage_periods WHERE account_id=$1")
        .bind(org.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(periods, 1, "one extra period row per calendar month, whatever the allowance did");
}

#[tokio::test]
async fn a_new_calendar_month_starts_a_fresh_limit_and_cap() {
    let Some(pool) = pg("a_new_calendar_month_starts_a_fresh_limit_and_cap").await else { return };
    let org = seed(&pool, Plan::Team, 100, 100).await;
    set_extra(&pool, &org, true, Some(100 * RATE), Some(1)).await;
    set_extra_balance(&pool, &org, 10_000_000).await;
    // Last month: the member used the whole cap and the org the whole limit.
    let last_month_start: DateTime<Utc> = sqlx::query_scalar(
        "SELECT (date_trunc('month', now() AT TIME ZONE 'UTC') - interval '1 month') AT TIME ZONE 'UTC'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let mut tx = fenced(&pool, org.id).await;
    sqlx::query(
        "INSERT INTO extra_usage_periods (account_id, period_start, period_end, spent_micro_usd, spent_credits) \
         VALUES ($1, $2, $2 + interval '1 month', $3, 100)",
    )
    .bind(org.id)
    .bind(last_month_start)
    .bind(100 * RATE)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO extra_usage_ledger (account_id, entry_kind, amount_micro_usd, credits, \
            rate_micro_usd_per_credit, user_id, period_start, idempotency_key) \
         VALUES ($1,'debit',$2,100,$3,$4,$5,$6)",
    )
    .bind(org.id)
    .bind(-100 * RATE)
    .bind(RATE)
    .bind(org.user)
    .bind(last_month_start)
    .bind(format!("debit:job:{}:{}", org.id, Uuid::new_v4()))
    .execute(&mut *tx)
    .await
    .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(preflight(&pool, &org, org.user).await, None, "last month does not count");
    debit(&pool, &org, Some(org.user), 100).await;
    let this_month: (i64,) = sqlx::query_as(
        "SELECT COALESCE(sum(-amount_micro_usd),0)::bigint FROM extra_usage_ledger \
          WHERE account_id=$1 AND period_start > $2",
    )
    .bind(org.id)
    .bind(last_month_start)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(this_month.0, 100 * RATE);
}

// ── fence and shape ────────────────────────────────────────────────────────

#[tokio::test]
async fn an_extra_usage_write_outside_the_entry_fence_is_refused() {
    let Some(pool) = pg("an_extra_usage_write_outside_the_entry_fence_is_refused").await else { return };
    let a = pro_with_extra(&pool, 1_000_000).await;
    let split = Split {
        allowance_credits: d(0),
        extra: ExtraFunding { credits: d(1), micro_usd: RATE },
        unfunded_credits: d(0),
        reason: None,
        rate_micro_usd_per_credit: RATE,
        window: (a.start, a.end),
        payer: Some(a.user),
        sel: PeriodSel::Current,
        extra_payer: Some(a.user),
    };
    // Scoped but NOT entered: no `lock_period_rotation`.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    let result = write_extra_debit(
        &mut tx,
        &ExtraDebit {
            account: a.id,
            split: &split,
            idempotency_key: format!("debit:job:{}:{}", a.id, Uuid::new_v4()),
            source_job_id: None,
            note: None,
        },
    )
    .await;
    assert!(result.is_err(), "the 0321 guard must refuse an unentered financial write");
    drop(tx);
    assert_eq!(extra_debits(&pool, &a).await, (0, 0));
}

#[tokio::test]
async fn extra_usage_check_constraints_still_refuse_a_negative_balance() {
    let Some(pool) = pg("extra_usage_check_constraints_still_refuse_a_negative_balance").await else { return };
    let a = pro_with_extra(&pool, 1_000).await;
    let mut tx = fenced(&pool, a.id).await;
    let err = sqlx::query("UPDATE extra_usage_balances SET balance_micro_usd = -1 WHERE account_id=$1")
        .bind(a.id)
        .execute(&mut *tx)
        .await;
    assert!(err.is_err(), "balance_micro_usd >= 0 is the backstop under the clamp");
}

// ── managed reservations ───────────────────────────────────────────────────

pub(super) fn request(a: &Acct, bound_micro: i64) -> crate::billing::managed_reservation::FundingRequest {
    crate::billing::managed_reservation::FundingRequest {
        plane: "consumer".into(),
        receipt: Uuid::new_v4(),
        account: a.id,
        payer: a.user,
        graph: Uuid::new_v4(),
        logical_call: Uuid::new_v4(),
        request_hash: "0".repeat(64),
        bound_micro_usd: bound_micro,
    }
}

/// Insert a held reservation directly (the fixture for hold accounting).
pub(super) async fn hold(
    pool: &PgPool,
    a: &Acct,
    request: &crate::billing::managed_reservation::FundingRequest,
    bound_credits: Decimal,
    basis: &str,
    extra_credits: Option<Decimal>,
) {
    let period: Uuid = sqlx::query_scalar(
        "SELECT id FROM member_allowance_periods WHERE account_id=$1 AND user_id=$2",
    )
    .bind(a.id)
    .bind(a.user)
    .fetch_one(pool)
    .await
    .unwrap();
    let mut tx = fenced(pool, a.id).await;
    sqlx::query(
        "INSERT INTO managed_credit_reservations \
           (plane,receipt_id,account_id,payer_id,graph_id,logical_call,request_hash,bound_micro_usd, \
            bound_credits,member_period_id,conversion_effective_from,reference_cost_per_credit,funding_basis, \
            extra_credits) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,now(),1,$11,$12)",
    )
    .bind(&request.plane)
    .bind(request.receipt)
    .bind(a.id)
    .bind(a.user)
    .bind(request.graph)
    .bind(request.logical_call)
    .bind(&request.request_hash)
    .bind(request.bound_micro_usd)
    .bind(bound_credits)
    .bind(period)
    .bind(basis)
    .bind(extra_credits)
    .execute(&mut *tx)
    .await
    .expect("reservation");
    tx.commit().await.unwrap();
}

pub(super) async fn admit(pool: &PgPool, a: &Acct, bound: Decimal) -> Option<(Option<Uuid>, String)> {
    let mut tx = fenced(pool, a.id).await;
    let funding = managed_funding_tx(&mut tx, a.id, a.user, bound, ManagedAllocation::Current)
        .await
        .expect("managed_funding_tx");
    tx.commit().await.unwrap();
    funding.map(|f| (f.member_period, f.basis))
}

#[tokio::test]
async fn admission_picks_allowance_then_pool_then_extra_and_refuses_the_rest() {
    let Some(pool) = pg("admission_picks_allowance_then_pool_then_extra_and_refuses_the_rest").await else {
        return;
    };
    // Inside the allowance.
    let a = seed(&pool, Plan::Pro, 100, 10).await;
    assert_eq!(admit(&pool, &a, d(5)).await.map(|f| f.1).as_deref(), Some("allowance"));
    // Allowance spent, pool has credits.
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_pool(&pool, &a, 50).await;
    assert_eq!(admit(&pool, &a, d(5)).await.map(|f| f.1).as_deref(), Some("pool"));
    // Allowance and pool spent, extra usage off.
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    assert_eq!(admit(&pool, &a, d(5)).await, None);
    // ... on, with balance.
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 5 * RATE).await;
    let funded = admit(&pool, &a, d(5)).await.expect("extra usage funds it");
    assert_eq!(funded.1, "extra");
    assert!(funded.0.is_some(), "the member period is still recorded for the allowance part");
    // One micro-USD short.
    set_extra_balance(&pool, &a, 5 * RATE - 1).await;
    assert_eq!(admit(&pool, &a, d(5)).await, None);
}

#[tokio::test]
async fn extra_reservations_hold_the_balance_against_each_other() {
    let Some(pool) = pg("extra_reservations_hold_the_balance_against_each_other").await else { return };
    let a = pro_with_extra(&pool, 100_000).await; // ~4.5 credits
    let r1 = request(&a, 2_000_000);
    assert_eq!(admit(&pool, &a, d(2)).await.map(|f| f.1).as_deref(), Some("extra"));
    hold(&pool, &a, &r1, d(2), "extra", Some(d(2))).await;
    let r2 = request(&a, 2_000_000);
    assert_eq!(admit(&pool, &a, d(2)).await.map(|f| f.1).as_deref(), Some("extra"));
    hold(&pool, &a, &r2, d(2), "extra", Some(d(2))).await;
    // 4 credits are held (88_000 of 100_000): one more credit does not fit.
    assert_eq!(admit(&pool, &a, d(1)).await, None);
    // Releasing one reservation frees its share.
    let mut tx = fenced(&pool, a.id).await;
    crate::billing::managed_reservation::release_unsent_tx(&mut tx, &r2).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(admit(&pool, &a, d(1)).await.map(|f| f.1).as_deref(), Some("extra"));
}

#[tokio::test]
async fn settling_an_extra_reservation_splits_the_actual_cost() {
    let Some(pool) = pg("settling_an_extra_reservation_splits_the_actual_cost").await else { return };
    let a = pro_with_extra(&pool, 1_000_000).await;
    let r = request(&a, 5_000_000);
    hold(&pool, &a, &r, d(5), "extra", Some(d(5))).await;

    // Actual cost 3 credits (reference_cost_per_credit is 1 USD).
    let mut tx = fenced(&pool, a.id).await;
    let changed = crate::billing::managed_reservation::settle_tx(&mut tx, &r, 3_000_000).await.unwrap();
    tx.commit().await.unwrap();
    assert!(changed);

    let plan: Decimal =
        sqlx::query_scalar("SELECT credits FROM credit_ledger WHERE source_receipt_id=$1")
            .bind(r.receipt)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(plan, d(0), "the allowance was spent, so the plan ledger bears nothing");
    assert_eq!(extra_debits(&pool, &a).await, (1, 3 * RATE));
    assert_eq!(extra_balance(&pool, &a).await, 1_000_000 - 3 * RATE);
    let key: String = sqlx::query_scalar("SELECT idempotency_key FROM extra_usage_ledger WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(key, format!("debit:receipt:{}:{}", a.id, r.receipt));

    // Replaying the same settlement is a no-op.
    let mut tx = fenced(&pool, a.id).await;
    let again = crate::billing::managed_reservation::settle_tx(&mut tx, &r, 3_000_000).await.unwrap();
    tx.commit().await.unwrap();
    assert!(!again);
    assert_eq!(extra_debits(&pool, &a).await, (1, 3 * RATE));
}

#[tokio::test]
async fn settling_beyond_the_balance_clamps_a_managed_charge_too() {
    let Some(pool) = pg("settling_beyond_the_balance_clamps_a_managed_charge_too").await else { return };
    let a = pro_with_extra(&pool, 1_000_000).await;
    let r = request(&a, 5_000_000);
    hold(&pool, &a, &r, d(5), "extra", Some(d(5))).await;
    // Another settlement spends most of the balance first (reservations are
    // admitted against holds, but a settled cost can exceed its bound).
    set_extra_balance(&pool, &a, 10_000).await;
    let mut tx = fenced(&pool, a.id).await;
    crate::billing::managed_reservation::settle_tx(&mut tx, &r, 3_000_000).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(extra_balance(&pool, &a).await, 0);
    assert_eq!(extra_debits(&pool, &a).await, (1, 10_000));
    let unfunded = d(3) - Decimal::from(10_000) / Decimal::from(RATE);
    assert_eq!(plan_ledger_sum(&pool, &a).await, -unfunded);
}

#[tokio::test]
async fn evaluate_returns_the_outcome_without_touching_extra_usage_when_the_allowance_covers() {
    let Some(pool) = pg("evaluate_returns_the_outcome_without_touching_extra_usage_when_the_allowance_covers").await
    else {
        return;
    };
    let a = seed(&pool, Plan::Pro, 100, 10).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    let ev = evaluate(&mut tx, a.id, a.user, Mode::Settlement, PeriodSel::Current, Intent::Settle(d(20)))
        .await
        .unwrap();
    assert!(matches!(
        ev.outcome,
        FundingOutcome::Funded { allowance_credits, extra } if allowance_credits == d(20) && extra.micro_usd == 0
    ));
}

#[tokio::test]
async fn one_planner_keeps_one_running_total_per_period_row() {
    let Some(pool) = pg("one_planner_keeps_one_running_total_per_period_row").await else { return };
    // 5 credits of allowance left, then a settlement pinned to the SAME period
    // row: the batch's earlier (unflushed) consumption must already count.
    let a = seed(&pool, Plan::Pro, 100, 95).await;
    set_pool(&pool, &a, 5).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    let period: Uuid = sqlx::query_scalar("SELECT id FROM member_allowance_periods WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(&pool)
        .await
        .unwrap();

    let mut tx = fenced(&pool, a.id).await;
    let mut planner = SettlementPlanner::new(a.id);
    let first = planner.plan(&mut tx, Some(a.user), PeriodSel::Current, d(5)).await.unwrap();
    assert_eq!((first.allowance_credits, first.extra.micro_usd), (d(5), 0));
    planner.record(&first);

    let second = planner.plan(&mut tx, Some(a.user), PeriodSel::Pinned(Some(period)), d(1)).await.unwrap();
    assert_eq!(second.allowance_credits, d(0), "the allowance is already spent in this batch");
    assert_eq!((second.extra.credits, second.extra.micro_usd), (d(1), RATE));
    planner.record(&second);

    // The pool is shared by every lookup too: nothing is left for a third.
    let third = planner.plan(&mut tx, None, PeriodSel::Current, d(1)).await.unwrap();
    assert_eq!(third.allowance_credits, d(0));
    tx.rollback().await.unwrap();
}
