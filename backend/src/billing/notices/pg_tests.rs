//! Live-Postgres tests for billing notices: claim-then-send exactly once
//! (concurrently and on replay), retry after a failed send, recipients per
//! kind, and the refusal/settlement detectors.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`; without it each test prints a skip
//! line and returns (a vacuous pass, which the CI step guards against by
//! failing on any `skipping ` line). Resend is a local axum mock: the sender
//! takes its base URL as a value, so no process env is touched.
//!
//! The pool connects as the database owner, so RLS is bypassed, but every
//! write here goes through the same account-scoped transactions production
//! uses and the 0321 financial entry fence is a trigger that fires for every
//! role.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use sqlx::{types::Decimal, PgPool};
use uuid::Uuid;

use super::email::Mailer;
use super::*;
use crate::billing::enforcement::{check_blocked, Spend};
use crate::workers::billing_derivation::native_job_debit;

/// Seeding shared with `billing::me`'s tests.
pub(crate) mod kit {
    use super::*;

    pub(crate) const RATE: i64 = 22_000;

    pub(crate) fn d(n: i64) -> Decimal {
        Decimal::from(n)
    }

    pub(crate) async fn pg(test: &str) -> Option<PgPool> {
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
    pub(crate) enum Plan {
        Free,
        Pro,
        Team,
    }

    pub(crate) struct Acct {
        pub id: Uuid,
        /// The owner.
        pub user: Uuid,
        pub start: DateTime<Utc>,
        pub end: DateTime<Utc>,
    }

    pub(crate) async fn fenced(pool: &PgPool, account: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
        crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("enter");
        tx
    }

    pub(crate) async fn new_user(pool: &PgPool, tag: &str) -> (Uuid, String) {
        let id = Uuid::new_v4();
        let email = format!("{tag}-{}@notices.test", id.simple());
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(id)
            .bind(&email)
            .execute(pool)
            .await
            .expect("user");
        (id, email)
    }

    /// An account on `plan` whose owner holds a `granted`/`consumed`
    /// allowance for the current window, with an empty plan pool. A team
    /// account also gets the owner's `account_members` row.
    pub(crate) async fn seed(pool: &PgPool, plan: Plan, granted: i64, consumed: i64) -> Acct {
        let (user, _) = new_user(pool, "owner").await;
        let id = Uuid::new_v4();
        if plan == Plan::Team {
            sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
                .bind(id)
                .bind(user)
                .bind(format!("org_{}", id.simple()))
                .execute(pool)
                .await
                .expect("org account");
            sqlx::query(
                "INSERT INTO account_members (account_id, user_id, role, account_is_org_account, seat_tier_id) \
                 VALUES ($1, $2, 'owner', true, (SELECT id FROM seat_tiers WHERE code='standard'))",
            )
            .bind(id)
            .bind(user)
            .execute(pool)
            .await
            .expect("owner member");
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
        let (plan_id, seat_priced): (Uuid, bool) = sqlx::query_as("SELECT id, is_seat_priced FROM plans WHERE code=$1")
            .bind(code)
            .fetch_one(pool)
            .await
            .expect("plan seeded");
        let start = Utc::now() - ChronoDuration::days(1);
        let end = Utc::now() + ChronoDuration::days(29);
        let mut tx = fenced(pool, id).await;
        sqlx::query("INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1,$2,$3,$4)")
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
        // The interactive gate (and so `billing/me`'s `blocked`) only refuses
        // accounts with enforcement on.
        sqlx::query("UPDATE accounts SET billing_enforcement_enabled = true WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await
            .expect("enforcement");
        let acct = Acct { id, user, start, end };
        add_period(pool, &acct, user, granted, consumed).await;
        acct
    }

    pub(crate) async fn add_period(pool: &PgPool, a: &Acct, user: Uuid, granted: i64, consumed: i64) {
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

    /// A member of a team account with an allowance period. Returns
    /// `(user id, email)`.
    pub(crate) async fn add_member(
        pool: &PgPool,
        a: &Acct,
        role: &str,
        granted: i64,
        consumed: i64,
    ) -> (Uuid, String) {
        let (user, email) = new_user(pool, role).await;
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, account_is_org_account, seat_tier_id) \
             VALUES ($1, $2, $3, true, (SELECT id FROM seat_tiers WHERE code='standard'))",
        )
        .bind(a.id)
        .bind(user)
        .bind(role)
        .execute(pool)
        .await
        .expect("member");
        add_period(pool, a, user, granted, consumed).await;
        (user, email)
    }

    pub(crate) async fn set_extra(
        pool: &PgPool,
        a: &Acct,
        enabled: bool,
        limit: Option<i64>,
        multiple: Option<i64>,
    ) {
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

    pub(crate) async fn set_extra_balance(pool: &PgPool, a: &Acct, micro: i64) {
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

    pub(crate) async fn set_subscription(pool: &PgPool, a: &Acct, status: &str) {
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

    /// Settle a real native job debit of `credits` for `payer`.
    pub(crate) async fn debit(pool: &PgPool, a: &Acct, payer: Option<Uuid>, credits: i64) {
        native_job_debit(pool, a.id, payer, Uuid::new_v4(), d(credits)).await.expect("settle");
    }
}

use kit::*;

// ── mock Resend ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Sent {
    to: String,
    subject: String,
    html: String,
    idempotency_key: String,
}

#[derive(Default)]
struct MockState {
    sent: Vec<Sent>,
    /// Addresses the mock answers 500 for.
    failing: HashSet<String>,
    /// Every request, failed or not, by address.
    attempts: Vec<String>,
}

#[derive(Clone)]
struct Resend {
    base: String,
    state: Arc<Mutex<MockState>>,
}

impl Resend {
    async fn start() -> Self {
        use axum::{extract::State, http::HeaderMap, http::StatusCode, routing::post, Json, Router};
        let state = Arc::new(Mutex::new(MockState::default()));
        async fn handler(
            State(state): State<Arc<Mutex<MockState>>>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> Result<Json<serde_json::Value>, StatusCode> {
            let to = body["to"][0].as_str().unwrap_or_default().to_string();
            let mut s = state.lock().unwrap();
            s.attempts.push(to.clone());
            if s.failing.contains(&to) {
                return Err(StatusCode::INTERNAL_SERVER_ERROR);
            }
            s.sent.push(Sent {
                to,
                subject: body["subject"].as_str().unwrap_or_default().to_string(),
                html: body["html"].as_str().unwrap_or_default().to_string(),
                idempotency_key: headers
                    .get("Idempotency-Key")
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or_default()
                    .to_string(),
            });
            Ok(Json(serde_json::json!({"id": format!("msg_{}", s.sent.len())})))
        }
        let app = Router::new().route("/emails", post(handler)).with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        Resend { base: format!("http://{addr}"), state }
    }

    fn mailer(&self) -> Mailer {
        Mailer::new(reqwest::Client::new(), "re_test".into(), self.base.clone())
    }

    fn sent(&self) -> Vec<Sent> {
        self.state.lock().unwrap().sent.clone()
    }

    fn sent_to(&self, email: &str) -> Vec<Sent> {
        self.sent().into_iter().filter(|s| s.to == email).collect()
    }

    fn fail(&self, email: &str) {
        self.state.lock().unwrap().failing.insert(email.to_string());
    }

    fn heal(&self, email: &str) {
        self.state.lock().unwrap().failing.remove(email);
    }
}

// ── helpers ────────────────────────────────────────────────────────────────

const WEB: &str = "https://app.coralswarm.test";

/// One worker pass for one account, as the worker runs it.
async fn pass(pool: &PgPool, mailer: Option<&Mailer>, account: Uuid) -> PassStats {
    run_account(pool, mailer, account, || async { (WEB.to_string(), Some("Acme".to_string())) })
        .await
        .expect("pass")
}

#[derive(Debug, sqlx::FromRow)]
struct Row {
    notice_kind: String,
    dedupe_key: String,
    state: String,
    attempts: i32,
    delivered_to: Vec<Uuid>,
    error: Option<String>,
    sent_at: Option<DateTime<Utc>>,
}

async fn rows(pool: &PgPool, account: Uuid) -> Vec<Row> {
    sqlx::query_as(
        "SELECT notice_kind, dedupe_key, state, attempts, delivered_to, error, sent_at \
           FROM billing_notices WHERE account_id=$1 ORDER BY notice_kind, dedupe_key",
    )
    .bind(account)
    .fetch_all(pool)
    .await
    .expect("rows")
}

async fn only(pool: &PgPool, account: Uuid, kind: NoticeKind) -> Row {
    let mut r: Vec<Row> = rows(pool, account).await.into_iter().filter(|r| r.notice_kind == kind.as_str()).collect();
    assert_eq!(r.len(), 1, "exactly one {kind:?} notice, got {r:?}");
    r.remove(0)
}

async fn make_due(pool: &PgPool, account: Uuid) {
    sqlx::query("UPDATE billing_notices SET next_attempt_at = now() - interval '1 second' WHERE account_id=$1")
        .bind(account)
        .execute(pool)
        .await
        .unwrap();
}

/// A team org with an owner, one admin and two members (each with a
/// 100-credit allowance, all spent), extra usage on with a $50 limit and a
/// 1x member cap.
struct Org {
    a: Acct,
    owner_email: String,
    admin: (Uuid, String),
    member: (Uuid, String),
    other: (Uuid, String),
}

async fn org(pool: &PgPool) -> Org {
    let a = seed(pool, Plan::Team, 100, 100).await;
    let owner_email: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1")
        .bind(a.user)
        .fetch_one(pool)
        .await
        .unwrap();
    let admin = add_member(pool, &a, "admin", 100, 100).await;
    let member = add_member(pool, &a, "member", 100, 100).await;
    let other = add_member(pool, &a, "member", 100, 100).await;
    set_extra(pool, &a, true, Some(50_000_000), Some(1)).await;
    set_extra_balance(pool, &a, 10_000_000).await;
    Org { a, owner_email, admin, member, other }
}

fn emails(sent: &[Sent]) -> Vec<String> {
    let mut v: Vec<String> = sent.iter().map(|s| s.to.clone()).collect();
    v.sort();
    v
}

fn sorted(mut v: Vec<String>) -> Vec<String> {
    v.sort();
    v
}

// ── exactly once ───────────────────────────────────────────────────────────

#[tokio::test]
async fn sixteen_concurrent_claims_of_one_notice_have_one_winner() {
    let Some(pool) = pg("sixteen_concurrent_claims_of_one_notice_have_one_winner").await else { return };
    let o = org(&pool).await;
    let start = o.a.start;
    let mut handles = Vec::new();
    for _ in 0..16 {
        let pool = pool.clone();
        let id = o.a.id;
        handles.push(tokio::spawn(async move {
            let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, id).await.unwrap();
            let won = claim_for_period(&mut tx, id, NoticeKind::ExtraUsageLimitReached, None, start).await.unwrap();
            tx.commit().await.unwrap();
            won
        }));
    }
    let mut wins = 0;
    for h in handles {
        if h.await.unwrap() {
            wins += 1;
        }
    }
    assert_eq!(wins, 1, "exactly one claim wins");
    assert_eq!(rows(&pool, o.a.id).await.len(), 1);
}

#[tokio::test]
async fn a_claimed_notice_is_sent_once_even_with_workers_racing_and_replaying() {
    let Some(pool) = pg("a_claimed_notice_is_sent_once_even_with_workers_racing_and_replaying").await else { return };
    let resend = Resend::start().await;
    let mailer = resend.mailer();
    let o = org(&pool).await;
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
        assert!(claim_for_period(&mut tx, o.a.id, NoticeKind::ExtraUsageLimitReached, None, o.a.start).await.unwrap());
        assert!(claim_for_period(&mut tx, o.a.id, NoticeKind::BalanceDepleted, None, o.a.start).await.unwrap());
        tx.commit().await.unwrap();
    }
    // Eight workers race for the same account.
    let mut handles = Vec::new();
    for _ in 0..8 {
        let (pool, mailer, id) = (pool.clone(), mailer.clone(), o.a.id);
        handles.push(tokio::spawn(async move { pass(&pool, Some(&mailer), id).await }));
    }
    for h in handles {
        h.await.unwrap();
    }
    // Two notices x two admins (owner + admin).
    let sent = resend.sent();
    assert_eq!(sent.len(), 4, "each notice reaches each admin exactly once: {sent:?}");
    let keys: HashSet<_> = sent.iter().map(|s| s.idempotency_key.clone()).collect();
    assert_eq!(keys.len(), 4, "each send carries its own provider idempotency key");

    // Replay: more passes, and re-detecting the same events, send nothing new.
    for _ in 0..3 {
        pass(&pool, Some(&mailer), o.a.id).await;
    }
    assert_eq!(resend.sent().len(), 4);
    for r in rows(&pool, o.a.id).await {
        assert_eq!(r.state, "sent");
        assert!(r.sent_at.is_some());
        assert_eq!(r.delivered_to.len(), 2);
    }
}

#[tokio::test]
async fn re_detecting_a_sent_event_does_not_claim_it_again() {
    let Some(pool) = pg("re_detecting_a_sent_event_does_not_claim_it_again").await else { return };
    let resend = Resend::start().await;
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(44_000), None).await;
    set_extra_balance(&pool, &a, 10_000_000).await;
    debit(&pool, &a, Some(a.user), 5).await; // wants 110_000, limit room 44_000

    let first = pass(&pool, Some(&resend.mailer()), a.id).await;
    assert_eq!(first.claimed, 1);
    assert_eq!(resend.sent().len(), 1);
    // The same limit_reached_at is still there on every later scan.
    let second = pass(&pool, Some(&resend.mailer()), a.id).await;
    assert_eq!(second.claimed, 0);
    assert_eq!(resend.sent().len(), 1);
    assert_eq!(rows(&pool, a.id).await.len(), 1);
}

// ── retry ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn a_failed_send_is_retried_later_and_is_not_lost() {
    let Some(pool) = pg("a_failed_send_is_retried_later_and_is_not_lost").await else { return };
    let resend = Resend::start().await;
    let mailer = resend.mailer();
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    let owner: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1").bind(a.user).fetch_one(&pool).await.unwrap();
    resend.fail(&owner);
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        claim_for_period(&mut tx, a.id, NoticeKind::BalanceDepleted, None, a.start).await.unwrap();
        tx.commit().await.unwrap();
    }

    let stats = pass(&pool, Some(&mailer), a.id).await;
    assert_eq!((stats.leased, stats.sent, stats.retrying), (1, 0, 1));
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!(r.state, "failed");
    assert_eq!(r.attempts, 1);
    assert!(r.sent_at.is_none());
    assert!(r.error.as_deref().unwrap_or_default().contains("500"));

    // Backing off: an immediate pass does not hammer the provider.
    let before = resend.state.lock().unwrap().attempts.len();
    pass(&pool, Some(&mailer), a.id).await;
    assert_eq!(resend.state.lock().unwrap().attempts.len(), before, "no retry before the backoff");
    let wait: f64 = sqlx::query_scalar(
        "SELECT extract(epoch FROM next_attempt_at - now())::float8 FROM billing_notices WHERE account_id=$1",
    )
    .bind(a.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(wait > 30.0 && wait <= 61.0, "first backoff is a minute, got {wait}");

    // The provider recovers and the backoff elapses: it goes out, once.
    resend.heal(&owner);
    make_due(&pool, a.id).await;
    let stats = pass(&pool, Some(&mailer), a.id).await;
    assert_eq!((stats.leased, stats.sent), (1, 1));
    assert_eq!(resend.sent_to(&owner).len(), 1);
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!((r.state.as_str(), r.attempts), ("sent", 2));
    assert!(r.sent_at.is_some() && r.error.is_none());

    make_due(&pool, a.id).await;
    pass(&pool, Some(&mailer), a.id).await;
    assert_eq!(resend.sent_to(&owner).len(), 1, "a sent notice is never sent again");
}

#[tokio::test]
async fn a_partial_failure_retries_only_the_people_who_did_not_get_it() {
    let Some(pool) = pg("a_partial_failure_retries_only_the_people_who_did_not_get_it").await else { return };
    let resend = Resend::start().await;
    let mailer = resend.mailer();
    let o = org(&pool).await;
    resend.fail(&o.admin.1);
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
        claim_for_period(&mut tx, o.a.id, NoticeKind::ExtraUsageLimitReached, None, o.a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    pass(&pool, Some(&mailer), o.a.id).await;
    assert_eq!(emails(&resend.sent()), vec![o.owner_email.clone()]);
    let r = only(&pool, o.a.id, NoticeKind::ExtraUsageLimitReached).await;
    assert_eq!(r.state, "failed");
    assert_eq!(r.delivered_to, vec![o.a.user], "the owner is recorded as done");

    resend.heal(&o.admin.1);
    make_due(&pool, o.a.id).await;
    pass(&pool, Some(&mailer), o.a.id).await;
    assert_eq!(resend.sent_to(&o.owner_email).len(), 1, "the owner is not emailed again");
    assert_eq!(resend.sent_to(&o.admin.1).len(), 1);
    assert_eq!(only(&pool, o.a.id, NoticeKind::ExtraUsageLimitReached).await.state, "sent");
}

#[tokio::test]
async fn a_dead_workers_lease_expires_and_the_notice_goes_out() {
    let Some(pool) = pg("a_dead_workers_lease_expires_and_the_notice_goes_out").await else { return };
    let resend = Resend::start().await;
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        claim_for_period(&mut tx, a.id, NoticeKind::BalanceDepleted, None, a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    // A worker leases it and dies before sending.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    assert_eq!(lease_due(&mut tx, a.id, 10).await.unwrap().len(), 1);
    tx.commit().await.unwrap();
    // While the lease holds, nobody else takes it.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    assert!(lease_due(&mut tx, a.id, 10).await.unwrap().is_empty());
    tx.commit().await.unwrap();
    assert!(resend.sent().is_empty());
    // The lease runs out.
    make_due(&pool, a.id).await;
    let stats = pass(&pool, Some(&resend.mailer()), a.id).await;
    assert_eq!(stats.sent, 1);
    assert_eq!(resend.sent().len(), 1);
    assert_eq!(only(&pool, a.id, NoticeKind::BalanceDepleted).await.attempts, 2);
}

#[tokio::test]
async fn a_notice_stops_retrying_after_its_last_attempt() {
    let Some(pool) = pg("a_notice_stops_retrying_after_its_last_attempt").await else { return };
    let resend = Resend::start().await;
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    let owner: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1").bind(a.user).fetch_one(&pool).await.unwrap();
    resend.fail(&owner);
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        claim_for_period(&mut tx, a.id, NoticeKind::BalanceDepleted, None, a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    for _ in 0..MAX_ATTEMPTS {
        pass(&pool, Some(&resend.mailer()), a.id).await;
        make_due(&pool, a.id).await;
    }
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!((r.state.as_str(), r.attempts), ("failed", MAX_ATTEMPTS));
    let attempts = resend.state.lock().unwrap().attempts.len();
    assert_eq!(attempts as i32, MAX_ATTEMPTS);
    pass(&pool, Some(&resend.mailer()), a.id).await;
    assert_eq!(resend.state.lock().unwrap().attempts.len(), attempts, "no attempt past the cap");
}

#[tokio::test]
async fn without_mail_configured_notices_stay_claimed_and_untouched() {
    let Some(pool) = pg("without_mail_configured_notices_stay_claimed_and_untouched").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        claim_for_period(&mut tx, a.id, NoticeKind::BalanceDepleted, None, a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    let stats = pass(&pool, None, a.id).await;
    assert_eq!(stats.leased, 0);
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!((r.state.as_str(), r.attempts), ("claimed", 1));
    // Once mail is configured it goes out.
    let resend = Resend::start().await;
    pass(&pool, Some(&resend.mailer()), a.id).await;
    assert_eq!(resend.sent().len(), 1);
}

// ── recipients ─────────────────────────────────────────────────────────────

async fn recipients_of(pool: &PgPool, a: &Acct, kind: NoticeKind, key: &str) -> (Audience, Vec<String>) {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, a.id).await.unwrap();
    let (aud, people) = recipients(&mut tx, a.id, kind, key).await.unwrap();
    tx.rollback().await.ok();
    (aud, sorted(people.into_iter().map(|p| p.email).collect()))
}

#[tokio::test]
async fn limit_and_depleted_go_to_every_admin_and_the_owner_but_not_members() {
    let Some(pool) = pg("limit_and_depleted_go_to_every_admin_and_the_owner_but_not_members").await else { return };
    let o = org(&pool).await;
    let want = sorted(vec![o.owner_email.clone(), o.admin.1.clone()]);
    for kind in [NoticeKind::ExtraUsageLimitReached, NoticeKind::BalanceDepleted, NoticeKind::AutoReloadFailed] {
        let (aud, got) = recipients_of(&pool, &o.a, kind, "2026-10-01T00:00:00.000000Z").await;
        assert_eq!(aud, Audience::OrgAdmin);
        assert_eq!(got, want, "{kind:?}");
    }
}

#[tokio::test]
async fn a_member_cap_goes_to_that_member_only() {
    let Some(pool) = pg("a_member_cap_goes_to_that_member_only").await else { return };
    let o = org(&pool).await;
    let key = format!("user:{}:2026-10-01T00:00:00.000000Z", o.member.0);
    let (aud, got) = recipients_of(&pool, &o.a, NoticeKind::MemberCapReached, &key).await;
    assert_eq!(aud, Audience::OrgMember);
    assert_eq!(got, vec![o.member.1.clone()], "not the admins, not the other member");

    // A member who has since left has nobody to tell.
    sqlx::query("DELETE FROM account_members WHERE account_id=$1 AND user_id=$2")
        .bind(o.a.id)
        .bind(o.member.0)
        .execute(&pool)
        .await
        .unwrap();
    let (_, got) = recipients_of(&pool, &o.a, NoticeKind::MemberCapReached, &key).await;
    assert!(got.is_empty());
}

#[tokio::test]
async fn a_pro_account_notifies_its_owner() {
    let Some(pool) = pg("a_pro_account_notifies_its_owner").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    let owner: String = sqlx::query_scalar("SELECT email FROM users WHERE id=$1").bind(a.user).fetch_one(&pool).await.unwrap();
    for kind in [NoticeKind::ExtraUsageLimitReached, NoticeKind::BalanceDepleted] {
        let (aud, got) = recipients_of(&pool, &a, kind, "2026-10-01T00:00:00.000000Z").await;
        assert_eq!((aud, got), (Audience::PersonalOwner, vec![owner.clone()]), "{kind:?}");
    }
}

#[tokio::test]
async fn delivered_emails_have_the_right_voice_and_link_for_each_audience() {
    let Some(pool) = pg("delivered_emails_have_the_right_voice_and_link_for_each_audience").await else { return };
    let resend = Resend::start().await;
    let o = org(&pool).await;
    let pro = seed(&pool, Plan::Pro, 100, 100).await;
    for (acct, kind, user) in [
        (o.a.id, NoticeKind::ExtraUsageLimitReached, None),
        (o.a.id, NoticeKind::MemberCapReached, Some(o.member.0)),
        (pro.id, NoticeKind::BalanceDepleted, None),
    ] {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, acct).await.unwrap();
        claim_for_period(&mut tx, acct, kind, user, o.a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    pass(&pool, Some(&resend.mailer()), o.a.id).await;
    pass(&pool, Some(&resend.mailer()), pro.id).await;

    let admin_mail = &resend.sent_to(&o.admin.1)[0];
    assert!(admin_mail.subject.contains("Acme has reached its monthly extra usage limit"), "{}", admin_mail.subject);
    assert!(admin_mail.html.contains(&format!("{WEB}/settings/organization/billing")));
    assert!(admin_mail.html.contains("$50.00"), "admins see their own limit in dollars");

    let member_mail = &resend.sent_to(&o.member.1)[0];
    assert!(member_mail.html.contains(&format!("{WEB}/settings\"")), "members link to their own page, not Billing");
    assert!(!member_mail.html.contains('$'), "no dollars in a member's email");
    assert!(resend.sent_to(&o.other.1).is_empty());

    let pro_mail = resend.sent().into_iter().find(|s| s.subject == "Your extra usage balance is empty").unwrap();
    assert!(pro_mail.html.contains(&format!("{WEB}/settings/billing")));
}

// ── detectors ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn the_settlement_scan_claims_the_limit_the_depletion_and_the_member_cap() {
    let Some(pool) = pg("the_settlement_scan_claims_the_limit_the_depletion_and_the_member_cap").await else { return };
    // Limit: the debit that reaches the limit stamps limit_reached_at.
    let pro = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &pro, true, Some(44_000), None).await;
    set_extra_balance(&pool, &pro, 10_000_000).await;
    debit(&pool, &pro, Some(pro.user), 5).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, pro.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, pro.id).await.unwrap(), 1);
    assert_eq!(scan_account(&mut tx, pro.id).await.unwrap(), 0, "replay claims nothing");
    tx.commit().await.unwrap();
    let r = only(&pool, pro.id, NoticeKind::ExtraUsageLimitReached).await;
    let expected: String = sqlx::query_scalar(&format!(
        "SELECT {} FROM extra_usage_periods WHERE account_id=$1",
        period_key_sql("period_start")
    ))
    .bind(pro.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(r.dedupe_key, expected, "dedupe key is the period start");

    // Depletion: spend drains the balance to exactly zero.
    let drained = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &drained, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &drained, 3 * RATE).await;
    debit(&pool, &drained, Some(drained.user), 3).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, drained.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, drained.id).await.unwrap(), 1);
    tx.commit().await.unwrap();
    only(&pool, drained.id, NoticeKind::BalanceDepleted).await;

    // Member cap: a clamped debit records the code in `note`.
    let o = org(&pool).await;
    debit(&pool, &o.a, Some(o.member.0), 60).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, o.a.id).await.unwrap(), 0, "not capped yet");
    tx.commit().await.unwrap();
    debit(&pool, &o.a, Some(o.member.0), 60).await; // only 40 of cap room
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, o.a.id).await.unwrap(), 1);
    assert_eq!(scan_account(&mut tx, o.a.id).await.unwrap(), 0);
    tx.commit().await.unwrap();
    let r = only(&pool, o.a.id, NoticeKind::MemberCapReached).await;
    assert!(r.dedupe_key.starts_with(&format!("user:{}:", o.member.0)), "{}", r.dedupe_key);
    assert_eq!(member_of_key(&r.dedupe_key), Some(o.member.0));
}

#[tokio::test]
async fn a_refused_request_claims_the_notice_even_though_no_debit_row_exists() {
    let Some(pool) = pg("a_refused_request_claims_the_notice_even_though_no_debit_row_exists").await else { return };
    // The member is already at their cap, so a debit would fund nothing and
    // write no ledger row: only the 402 side can see it.
    let o = org(&pool).await;
    debit(&pool, &o.a, Some(o.member.0), 100).await; // exactly the cap, nothing clamped
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, o.a.id).await.unwrap(), 0);
    tx.commit().await.unwrap();

    // The gate refuses; the claim happens on a spawned task.
    assert!(check_blocked(&pool, o.a.id, o.member.0, Spend::Background).await.unwrap());
    let mut found = Vec::new();
    for _ in 0..100 {
        found = rows(&pool, o.a.id).await;
        if !found.is_empty() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert_eq!(found.len(), 1, "{found:?}");
    assert_eq!(found[0].notice_kind, "member_cap_reached");
    assert!(found[0].dedupe_key.starts_with(&format!("user:{}:", o.member.0)));

    // The settlement scan sees the same event under the same key: no second
    // claim for the same period.
    debit(&pool, &o.a, Some(o.member.0), 5).await; // clamped to 0 extra, note set only on partial rows with extra > 0
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
    scan_account(&mut tx, o.a.id).await.unwrap();
    tx.commit().await.unwrap();
    assert_eq!(rows(&pool, o.a.id).await.len(), 1);
}

#[tokio::test]
async fn refusal_reasons_map_to_notice_kinds() {
    use crate::billing::funding_policy::RefusalReason as R;
    assert_eq!(NoticeKind::for_refusal(R::MemberCapReached), Some(NoticeKind::MemberCapReached));
    assert_eq!(NoticeKind::for_refusal(R::MonthlyLimitReached), Some(NoticeKind::ExtraUsageLimitReached));
    assert_eq!(NoticeKind::for_refusal(R::ExtraBalanceExhausted), Some(NoticeKind::BalanceDepleted));
    for r in [R::FreePlan, R::CreditsFrozen, R::ExtraUsageOff] {
        assert_eq!(NoticeKind::for_refusal(r), None, "{r:?} reached nothing, so there is nothing to email");
    }
    for k in [
        NoticeKind::ExtraUsageLimitReached,
        NoticeKind::MemberCapReached,
        NoticeKind::BalanceDepleted,
        NoticeKind::AutoReloadSucceeded,
        NoticeKind::AutoReloadFailed,
    ] {
        assert_eq!(NoticeKind::parse(k.as_str()), Some(k));
    }
}

#[tokio::test]
async fn refusal_claims_and_the_scan_share_one_key_for_the_limit() {
    let Some(pool) = pg("refusal_claims_and_the_scan_share_one_key_for_the_limit").await else { return };
    let pro = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &pro, true, Some(44_000), None).await;
    set_extra_balance(&pool, &pro, 10_000_000).await;
    debit(&pool, &pro, Some(pro.user), 5).await;

    // Refusal side first, with the window the gate would pass.
    let won = claim_refusal(&pool, pro.id, pro.user, NoticeKind::ExtraUsageLimitReached).await.unwrap();
    assert!(won);
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, pro.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, pro.id).await.unwrap(), 0, "the scan finds the notice already claimed");
    tx.commit().await.unwrap();
    assert!(!claim_refusal(&pool, pro.id, pro.user, NoticeKind::ExtraUsageLimitReached).await.unwrap());
    assert_eq!(rows(&pool, pro.id).await.len(), 1);
}

// ── reload kinds (claimed by 3.5) ──────────────────────────────────────────

#[tokio::test]
async fn reload_notices_claim_by_reload_id_and_say_the_amount() {
    let Some(pool) = pg("reload_notices_claim_by_reload_id_and_say_the_amount").await else { return };
    let resend = Resend::start().await;
    let o = org(&pool).await;
    let reload = Uuid::new_v4();
    {
        let mut tx = fenced(&pool, o.a.id).await;
        sqlx::query("INSERT INTO extra_usage_reloads (id, account_id, amount_micro_usd, period_start) VALUES ($1,$2,$3,$4)")
            .bind(reload)
            .bind(o.a.id)
            .bind(25_000_000i64)
            .bind(o.a.start)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
    }
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, o.a.id).await.unwrap();
    assert!(claim(&mut tx, o.a.id, NoticeKind::AutoReloadFailed, &reload.to_string(), None).await.unwrap());
    assert!(!claim(&mut tx, o.a.id, NoticeKind::AutoReloadFailed, &reload.to_string(), None).await.unwrap());
    tx.commit().await.unwrap();

    pass(&pool, Some(&resend.mailer()), o.a.id).await;
    let mail = &resend.sent_to(&o.admin.1)[0];
    assert!(mail.html.contains("$25.00"), "{}", mail.html);
    assert!(mail.subject.contains("couldn't reload"));
}

// ── balance depleted vs auto-reload ────────────────────────────────────────

async fn set_auto_reload(pool: &PgPool, a: &Acct, on: bool) {
    sqlx::query(
        "UPDATE extra_usage_settings SET auto_reload_enabled = $2, \
                auto_reload_threshold_micro_usd = 5000000, auto_reload_amount_micro_usd = 20000000 \
          WHERE account_id = $1",
    )
    .bind(a.id)
    .bind(on)
    .execute(pool)
    .await
    .expect("auto reload");
}

#[tokio::test]
async fn a_depleted_balance_is_not_announced_while_auto_reload_is_on() {
    let Some(pool) = pg("a_depleted_balance_is_not_announced_while_auto_reload_is_on").await else { return };
    let resend = Resend::start().await;
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    set_extra(&pool, &a, true, Some(50_000_000), None).await;
    set_extra_balance(&pool, &a, 3 * RATE).await;
    debit(&pool, &a, Some(a.user), 3).await; // drains the balance to zero
    set_auto_reload(&pool, &a, true).await;

    // The settlement scan and the refusal side both stay quiet.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, a.id).await.unwrap(), 0, "scan must not claim while a reload is coming");
    tx.commit().await.unwrap();
    assert!(!claim_refusal(&pool, a.id, a.user, NoticeKind::BalanceDepleted).await.unwrap());
    assert!(rows(&pool, a.id).await.is_empty());

    // Other notices are unaffected by auto-reload.
    assert!(claim_refusal(&pool, a.id, a.user, NoticeKind::ExtraUsageLimitReached).await.unwrap());

    // Auto-reload off again (for instance switched off after failures): the
    // depletion is now news.
    set_auto_reload(&pool, &a, false).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    assert_eq!(scan_account(&mut tx, a.id).await.unwrap(), 1);
    tx.commit().await.unwrap();
    only(&pool, a.id, NoticeKind::BalanceDepleted).await;

    // Claimed while off, then auto-reload is switched on before delivery: the
    // unsent notice is dropped, not emailed.
    set_auto_reload(&pool, &a, true).await;
    make_due(&pool, a.id).await;
    pass(&pool, Some(&resend.mailer()), a.id).await;
    assert!(resend.sent_to(&owner_email(&pool, &a).await).iter().all(|s| !s.subject.to_lowercase().contains("balance")),
        "no depleted email while auto-reload is on");
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!((r.state.as_str(), r.error.as_deref()), ("sent", Some("suppressed_auto_reload")));
}

async fn owner_email(pool: &PgPool, a: &Acct) -> String {
    sqlx::query_scalar("SELECT email FROM users WHERE id=$1").bind(a.user).fetch_one(pool).await.unwrap()
}

// ── a delivery that dies on a database error still counts its attempts ─────

#[tokio::test]
async fn database_errors_during_delivery_count_toward_the_attempt_ceiling() {
    let Some(pool) = pg("database_errors_during_delivery_count_toward_the_attempt_ceiling").await else { return };
    let a = seed(&pool, Plan::Pro, 100, 100).await;
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        claim_for_period(&mut tx, a.id, NoticeKind::BalanceDepleted, None, a.start).await.unwrap();
        tx.commit().await.unwrap();
    }
    // Every delivery attempt dies in the database; each lease counts.
    for expected in 1..=MAX_ATTEMPTS {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
        let leased = lease_due(&mut tx, a.id, 5).await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(leased.len(), 1, "attempt {expected} must still be leased");
        assert_eq!(leased[0].attempt, expected);
        record_delivery_error(&pool, &leased[0], "connection reset").await;
        make_due(&pool, a.id).await;
    }
    let r = only(&pool, a.id, NoticeKind::BalanceDepleted).await;
    assert_eq!((r.state.as_str(), r.attempts), ("failed", MAX_ATTEMPTS));
    assert!(r.error.as_deref().is_some_and(|e| e.starts_with("database error")), "{:?}", r.error);

    // The ceiling holds: no further lease.
    let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, a.id).await.unwrap();
    assert!(lease_due(&mut tx, a.id, 5).await.unwrap().is_empty());
    tx.commit().await.unwrap();
}
