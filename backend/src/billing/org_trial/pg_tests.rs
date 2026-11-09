//! The trial lifecycle against real Postgres, driven through the production
//! entry points (the Checkout-completed webhook, `invoice.paid`, the
//! worker's per-account pass, the funding planner) and a local axum Stripe
//! double. Every handler runs as `coralswarm_app`, the production role, with
//! RLS and the 0321 financial fence enforced; only fixtures and read-backs
//! use the owner pool.
//!
//! `RELINFER_TEST_DATABASE_URL`-gated like every billing pg suite. With
//! `REQUIRE_PG_SUITE` set, an unset URL FAILS instead of skipping, so a CI
//! step cannot go green without running these.
//!
//! Run with:
//!   RELINFER_TEST_DATABASE_URL=postgres://localhost:5432/<migrated db> REQUIRE_PG_SUITE=1 \
//!     cargo test -p coralswarm-backend --lib "billing::org_trial::" -- --test-threads=1

use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use axum::response::IntoResponse;
use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::types::Decimal;
use sqlx::PgPool;
use uuid::Uuid;

use super::db::{self as trial_db, TrialRow};
use super::{lifecycle, TrialState};
use crate::billing::stripe_client::{SessionLineItem, StripeClient};
use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};

const TRIAL_START: i64 = 1_800_000_000;
const TRIAL_END: i64 = TRIAL_START + 90 * 86_400;
const PAID_START: i64 = TRIAL_START + 20 * 86_400;
const PAID_END: i64 = PAID_START + 30 * 86_400;
/// `seat_tiers.credits` for `standard` (0165): the paid per-seat allowance.
const STANDARD_SEAT_CREDITS: i64 = 3_000;

fn ts(secs: i64) -> DateTime<Utc> {
    DateTime::from_timestamp(secs, 0).unwrap()
}

fn d(n: i64) -> Decimal {
    Decimal::from(n)
}

// ── Pools ────────────────────────────────────────────────────────────────

struct Pools {
    owner: PgPool,
    app: PgPool,
}

async fn pools(test: &str) -> Option<Pools> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        assert!(
            std::env::var_os("REQUIRE_PG_SUITE").is_none(),
            "{test}: REQUIRE_PG_SUITE is set but RELINFER_TEST_DATABASE_URL is not"
        );
        eprintln!("skipping {test}: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    let owner = sqlx::postgres::PgPoolOptions::new()
        .max_connections(6)
        .connect(&url)
        .await
        .expect("connect to test Postgres");
    let app = crate::app_role_test_support::app_role_pool(&owner, &url).await;
    Some(Pools { owner, app })
}

/// Run one statement inside the financial entry fence, as every billing
/// writer must.
async fn fin(pool: &PgPool, account: Uuid, q: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>) {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("lock");
    q.execute(&mut *tx).await.expect("fenced fixture write");
    tx.commit().await.expect("commit");
}

// ── Stripe double ────────────────────────────────────────────────────────

#[derive(Clone)]
struct MockCfg {
    sub_id: String,
    price: String,
    qty: i64,
    fingerprint: Option<String>,
    /// What the subscription becomes after `trial_end=now`.
    status_after_end: &'static str,
    cancel_scheduled: bool,
    /// `false`: the subscription carries no `default_payment_method`, so the
    /// fingerprint must come from the customer (or the session).
    sub_has_default_pm: bool,
}

#[derive(Clone, Debug)]
struct Seen {
    method: String,
    path: String,
    body: String,
    idem: Option<String>,
}

/// The conversion invoice (`in_conversion_mock`, every subscription's
/// `latest_invoice`) and how the double answers a void or a refund.
#[derive(Clone, Debug)]
struct InvoiceMock {
    status: String,
    amount_paid: i64,
    /// A void answers 400 and changes nothing (a transient failure).
    void_fails: bool,
    /// The customer pays just before the void lands: the invoice becomes
    /// `paid`, the subscription `active`, and the void answers 400.
    pay_on_void: bool,
    /// `POST /v1/refunds` answers 400.
    refund_fails: bool,
    /// The void succeeds and Stripe moves the `past_due` subscription back
    /// to `active` (#1528 S1): the subscription looks paid, the invoice is
    /// `void`.
    activate_on_void: bool,
    /// `POST /v1/refunds` answers 400 `charge_already_refunded`: a replay
    /// after the idempotency key expired (#1528 N3).
    refund_already_done: bool,
}

impl Default for InvoiceMock {
    fn default() -> Self {
        InvoiceMock {
            status: "open".into(),
            amount_paid: 0,
            void_fails: false,
            pay_on_void: false,
            refund_fails: false,
            activate_on_void: false,
            refund_already_done: false,
        }
    }
}

/// How the double answers for a Checkout session (`/v1/checkout/sessions/{id}`).
#[derive(Clone, Debug)]
struct CheckoutMock {
    /// `POST .../expire` answers 400 and changes nothing.
    expire_fails: bool,
    /// What `GET` reports for a session not expired through this double:
    /// `open`, `expired` or `complete`.
    status: String,
    /// Sessions this double has expired (each session has its own state).
    expired: std::collections::HashSet<String>,
    /// `line_items.data` of a `GET` session, as Stripe expands it.
    line_items: Vec<Value>,
}

#[derive(Clone)]
struct StripeMock {
    cfg: MockCfg,
    status: Arc<Mutex<String>>,
    checkout: Arc<Mutex<CheckoutMock>>,
    cancel_scheduled: Arc<Mutex<bool>>,
    invoice: Arc<Mutex<InvoiceMock>>,
    seen: Arc<Mutex<Vec<Seen>>>,
    /// Applied right after the next `GET` of the subscription answers: the
    /// state changes at Stripe between two of our reads (Stripe ending the
    /// trial itself on day 90, say).
    status_after_next_get: Arc<Mutex<Option<String>>>,
}

impl StripeMock {
    fn sub_json(&self, status: &str) -> Value {
        self.sub_json_for(&self.cfg.sub_id, status)
    }

    fn sub_json_for(&self, id: &str, status: &str) -> Value {
        let (ps, pe) = if status == "trialing" { (TRIAL_START, TRIAL_END) } else { (PAID_START, PAID_END) };
        json!({
            "id": id, "object": "subscription", "status": status,
            "customer": "cus_trial_mock",
            "default_payment_method": if self.cfg.sub_has_default_pm { Value::from("pm_trial") } else { Value::Null },
            "latest_invoice": "in_conversion_mock",
            "trial_end": if status == "trialing" { Value::from(TRIAL_END) } else { Value::Null },
            "cancel_at_period_end": *self.cancel_scheduled.lock().unwrap(),
            "items": {"object": "list", "data": [{
                "id": "si_trial", "quantity": self.cfg.qty,
                "current_period_start": ps, "current_period_end": pe,
                "price": {"id": self.cfg.price},
            }]},
        })
    }

    fn calls(&self, method: &str, path_prefix: &str) -> Vec<Seen> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|s| s.method == method && s.path.starts_with(path_prefix))
            .cloned()
            .collect()
    }

    fn set_status(&self, s: &str) {
        *self.status.lock().unwrap() = s.to_string();
    }

    fn set_cancel_scheduled(&self, v: bool) {
        *self.cancel_scheduled.lock().unwrap() = v;
    }

    fn flip_status_after_next_get(&self, s: &str) {
        *self.status_after_next_get.lock().unwrap() = Some(s.to_string());
    }

    fn invoice(&self, f: impl FnOnce(&mut InvoiceMock)) {
        f(&mut self.invoice.lock().unwrap());
    }

    fn checkout(&self, f: impl FnOnce(&mut CheckoutMock)) {
        f(&mut self.checkout.lock().unwrap());
    }
}

fn stripe_400(message: &str) -> (StatusCode, Value) {
    (StatusCode::BAD_REQUEST, json!({"error": {"type": "invalid_request_error", "message": message}}))
}

async fn spawn_stripe(cfg: MockCfg, initial_status: &str) -> (Arc<StripeClient>, StripeMock) {
    let mock = StripeMock {
        cancel_scheduled: Arc::new(Mutex::new(cfg.cancel_scheduled)),
        invoice: Arc::new(Mutex::new(InvoiceMock::default())),
        checkout: Arc::new(Mutex::new(CheckoutMock { expire_fails: false, status: "open".into(), expired: Default::default(), line_items: Vec::new() })),
        cfg,
        status: Arc::new(Mutex::new(initial_status.to_string())),
        seen: Arc::new(Mutex::new(Vec::new())),
        status_after_next_get: Arc::new(Mutex::new(None)),
    };
    let m = mock.clone();
    let counter = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::http::Request<axum::body::Body>| {
        let m = m.clone();
        let counter = counter.clone();
        async move {
            let (parts, body) = req.into_parts();
            let bytes = axum::body::to_bytes(body, 1 << 20).await.unwrap();
            let body = String::from_utf8_lossy(&bytes).into_owned();
            let path = parts.uri.path().to_string();
            let method = parts.method.to_string();
            let idem = parts.headers.get("idempotency-key").and_then(|v| v.to_str().ok()).map(str::to_string);
            m.seen.lock().unwrap().push(Seen { method: method.clone(), path: path.clone(), body: body.clone(), idem });
            let sub_path = format!("/v1/subscriptions/{}", m.cfg.sub_id);
            let ok = |v: Value| (StatusCode::OK, v);
            let (code, reply) = match (method.as_str(), path.as_str()) {
                ("GET", p) if p == sub_path => {
                    let reply = m.sub_json(&m.status.lock().unwrap().clone());
                    if let Some(next) = m.status_after_next_get.lock().unwrap().take() {
                        m.set_status(&next);
                    }
                    ok(reply)
                }
                ("POST", p) if p == sub_path && body.contains("trial_end=now") => {
                    m.set_status(m.cfg.status_after_end);
                    if m.cfg.status_after_end == "active" {
                        // Stripe charged the saved card: the conversion
                        // invoice is paid.
                        let mut inv = m.invoice.lock().unwrap();
                        inv.status = "paid".into();
                        inv.amount_paid = inv.amount_paid.max(4000);
                    }
                    ok(m.sub_json(m.cfg.status_after_end))
                }
                ("POST", p) if p == sub_path => {
                    // cancel_at_period_end=true (the owner's cancel, kept on
                    // a paid conversion).
                    if body.contains("cancel_at_period_end=true") {
                        m.set_cancel_scheduled(true);
                    }
                    ok(m.sub_json(&m.status.lock().unwrap().clone()))
                }
                ("DELETE", p) if p == sub_path => {
                    m.set_status("canceled");
                    ok(m.sub_json("canceled"))
                }
                // Any other subscription (a duplicate Checkout's): it shares
                // the status but is never ended or cancelled by the trial.
                ("GET", p) if p.starts_with("/v1/subscriptions/") => {
                    let id = p.trim_start_matches("/v1/subscriptions/").to_string();
                    ok(m.sub_json_for(&id, &m.status.lock().unwrap().clone()))
                }
                ("DELETE", p) if p.starts_with("/v1/subscriptions/") => {
                    let id = p.trim_start_matches("/v1/subscriptions/").to_string();
                    ok(m.sub_json_for(&id, "canceled"))
                }
                ("POST", p) if p.starts_with("/v1/invoices/") && p.ends_with("/void") => {
                    let mut inv = m.invoice.lock().unwrap();
                    if inv.pay_on_void {
                        inv.status = "paid".into();
                        inv.pay_on_void = false;
                        m.set_status("active");
                        stripe_400("You can only pass in open invoices.")
                    } else if inv.void_fails || inv.status != "open" {
                        stripe_400("This invoice cannot be voided.")
                    } else {
                        inv.status = "void".into();
                        if inv.activate_on_void {
                            m.set_status("active");
                        }
                        ok(json!({"id": "in_conversion_mock", "status": "void"}))
                    }
                }
                ("GET", p) if p.starts_with("/v1/invoices/") => {
                    let inv = m.invoice.lock().unwrap();
                    ok(json!({"id": p.trim_start_matches("/v1/invoices/"), "object": "invoice",
                              "status": inv.status, "amount_paid": inv.amount_paid,
                              "payments": {"data": [{"status": "paid",
                                  "payment": {"type": "payment_intent", "payment_intent": "pi_conversion_mock"}}]}}))
                }
                ("POST", "/v1/refunds") => {
                    if m.invoice.lock().unwrap().refund_already_done {
                        (
                            StatusCode::BAD_REQUEST,
                            json!({"error": {"type": "invalid_request_error", "code": "charge_already_refunded",
                                             "message": "Charge ch_mock has already been refunded."}}),
                        )
                    } else if m.invoice.lock().unwrap().refund_fails {
                        stripe_400("Refund failed.")
                    } else {
                        ok(json!({"id": "re_mock", "object": "refund", "status": "succeeded"}))
                    }
                }
                ("GET", "/v1/payment_methods/pm_trial") => ok(json!({
                    "id": "pm_trial", "type": "card",
                    "card": {"brand": "visa", "last4": "4242", "exp_month": 1, "exp_year": 2030,
                             "fingerprint": m.cfg.fingerprint},
                })),
                ("GET", p) if p.starts_with("/v1/customers/") => {
                    ok(json!({"id": "cus_trial_mock", "invoice_settings": {"default_payment_method": "pm_trial"}}))
                }
                ("POST", "/v1/customers") => {
                    let account = body
                        .split('&')
                        .find_map(|kv| kv.strip_prefix("metadata%5Baccount_id%5D="))
                        .unwrap_or_default()
                        .to_string();
                    ok(json!({"id": format!("cus_mock_{account}")}))
                }
                ("POST", p) if p.starts_with("/v1/checkout/sessions/") && p.ends_with("/expire") => {
                    let mut c = m.checkout.lock().unwrap();
                    let id = p.trim_start_matches("/v1/checkout/sessions/").trim_end_matches("/expire").to_string();
                    if c.expire_fails || c.status != "open" || c.expired.contains(&id) {
                        stripe_400("Only Checkout Sessions with a status of `open` can be expired.")
                    } else {
                        c.expired.insert(id.clone());
                        ok(json!({"id": id, "status": "expired"}))
                    }
                }
                ("GET", p) if p.starts_with("/v1/checkout/sessions/") => {
                    let c = m.checkout.lock().unwrap();
                    let id = p.trim_start_matches("/v1/checkout/sessions/").to_string();
                    let status = if c.expired.contains(&id) { "expired".to_string() } else { c.status.clone() };
                    ok(json!({"id": id, "object": "checkout.session",
                              "status": status, "line_items": {"object": "list", "data": c.line_items.clone()}}))
                }
                ("POST", "/v1/checkout/sessions") => {
                    let n = counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
                    ok(json!({"id": format!("cs_trial_{n}_{}", Uuid::new_v4().simple()),
                           "url": format!("https://checkout.stripe.com/mock/{n}")}))
                }
                _ => ok(json!({"id": "x", "status": "expired"})),
            };
            (code, [("content-type", "application/json")], reply.to_string()).into_response()
        }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (Arc::new(StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"))), mock)
}

fn cfg(price: &str, qty: i64, fingerprint: &str) -> MockCfg {
    MockCfg {
        sub_id: format!("sub_trial_{}", Uuid::new_v4().simple()),
        price: price.to_string(),
        qty,
        fingerprint: Some(fingerprint.to_string()),
        status_after_end: "active",
        cancel_scheduled: false,
        sub_has_default_pm: true,
    }
}

fn app_state(app: &PgPool, stripe: Arc<StripeClient>) -> Arc<crate::AppState> {
    let mut s = crate::test_app_state_owned_with_db_and_store(app.clone(), None);
    s.stripe = Some(stripe);
    Arc::new(s)
}

// ── Fixtures ─────────────────────────────────────────────────────────────

struct Org {
    account: Uuid,
    users: Vec<Uuid>,
}

async fn new_user(owner: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(format!("trial-{}@test.example", id.simple()))
        .bind(format!("user_{}", id.simple()))
        .execute(owner)
        .await
        .expect("insert user");
    id
}

/// A self-serve team org as `POST /v1/orgs` leaves it: Clerk-linked
/// `accounts` row, the creator as owner plus `members - 1` members on the
/// standard tier, a Stripe customer mapping (`pending`, no subscription).
async fn seed_org(owner: &PgPool, creator: Option<Uuid>, members: usize) -> Org {
    let creator = match creator {
        Some(c) => c,
        None => new_user(owner).await,
    };
    let mut users = vec![creator];
    for _ in 1..members {
        users.push(new_user(owner).await);
    }
    let account = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(creator)
        .bind(format!("org_{}", account.simple()))
        .execute(owner)
        .await
        .expect("insert org account");
    let standard: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
        .fetch_one(owner)
        .await
        .expect("standard tier");
    for (i, u) in users.iter().enumerate() {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, $4)")
            .bind(account)
            .bind(u)
            .bind(if i == 0 { "owner" } else { "member" })
            .bind(standard)
            .execute(owner)
            .await
            .expect("insert member");
    }
    fin(
        owner,
        account,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, status) VALUES ($1, $2, 'pending')",
        )
        .bind(account)
        .bind(format!("cus_trial_{}", account.simple())),
    )
    .await;
    Org { account, users }
}

/// The `checkout_open` row `start_org_trial_checkout` writes.
async fn open_trial(app: &PgPool, org: &Org, seats: i32, eligible: bool) {
    let mut tx = crate::tenancy::scope::begin_account_scoped(app, org.account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, org.account).await.expect("lock");
    let out = trial_db::open_checkout(&mut tx, org.account, org.users[0], seats, eligible, "cs_seed")
        .await
        .expect("open checkout");
    assert_eq!(out, trial_db::OpenOutcome::Open);
    tx.commit().await.expect("commit");
}

async fn complete(state: &Arc<crate::AppState>, app: &PgPool, org: &Org, mock: &StripeMock, event: &str) -> StatusCode {
    crate::routes::stripe_webhook::apply_checkout_completed_subscription(
        state,
        app,
        org.account,
        event,
        "cs_completed",
        &mock.cfg.sub_id,
        &SessionLineItem { price_id: mock.cfg.price.clone(), quantity: mock.cfg.qty },
    )
    .await
    .status()
}

/// [`complete`] for a subscription other than the mock's own (a second
/// Checkout's).
async fn complete_sub(
    state: &Arc<crate::AppState>,
    app: &PgPool,
    org: &Org,
    mock: &StripeMock,
    event: &str,
    sub_id: &str,
) -> StatusCode {
    crate::routes::stripe_webhook::apply_checkout_completed_subscription(
        state,
        app,
        org.account,
        event,
        "cs_completed_second",
        sub_id,
        &SessionLineItem { price_id: mock.cfg.price.clone(), quantity: mock.cfg.qty },
    )
    .await
    .status()
}

async fn invoice_paid(app: &PgPool, stripe: &StripeClient, account: Uuid, event: &str, invoice: &Value) -> StatusCode {
    crate::routes::stripe_webhook::handle_invoice_paid(app, Some(stripe), account, event, "invoice.paid", "cus_trial_mock", invoice)
        .await
        .status()
}

fn invoice_json(sub: &str, reason: &str, start: i64, end: i64, amount: i64) -> Value {
    json!({
        "id": format!("in_{}", Uuid::new_v4().simple()), "object": "invoice", "customer": "cus_trial_mock",
        "billing_reason": reason, "amount_paid": amount, "subscription": sub,
        "parent": {"subscription_details": {"subscription": sub}},
        "lines": {"data": [{"period": {"start": start, "end": end}}]},
    })
}

async fn extra_refused(app: &PgPool, account: Uuid) -> bool {
    let mut tx = crate::tenancy::scope::begin_account_scoped(app, account).await.unwrap();
    let r = trial_db::extra_usage_refused(&mut tx, account).await.unwrap();
    tx.commit().await.unwrap();
    r
}

async fn mirror(owner: &PgPool, account: Uuid) -> (Option<String>, String) {
    sqlx::query_as("SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1")
        .bind(account)
        .fetch_one(owner)
        .await
        .unwrap()
}

async fn trial(app: &PgPool, account: Uuid) -> TrialRow {
    let mut tx = crate::tenancy::scope::begin_account_scoped(app, account).await.expect("scope");
    let row = trial_db::load(&mut tx, account).await.expect("load").expect("org_trials row");
    tx.commit().await.expect("commit");
    row
}

async fn pool_balance(owner: &PgPool, account: Uuid) -> Option<Decimal> {
    sqlx::query_scalar("SELECT balance FROM credit_balances WHERE account_id = $1")
        .bind(account)
        .fetch_optional(owner)
        .await
        .expect("read pool")
}

/// What the billing derivation worker does for each debit: the pool drops
/// by the credits spent.
async fn spend(owner: &PgPool, account: Uuid, credits: i64) {
    fin(
        owner,
        account,
        sqlx::query("UPDATE credit_balances SET balance = balance - $2 WHERE account_id = $1").bind(account).bind(d(credits)),
    )
    .await;
}

async fn team_grants(owner: &PgPool, account: Uuid) -> Vec<(Option<Decimal>, Option<i32>)> {
    sqlx::query_as(
        "SELECT g.credits_override, g.seats FROM grants g JOIN plans p ON p.id = g.plan_id \
          WHERE g.account_id = $1 AND p.code = 'team' AND g.revoked_at IS NULL",
    )
    .bind(account)
    .fetch_all(owner)
    .await
    .expect("read team grants")
}

async fn ledger_seats(owner: &PgPool, account: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(SUM(gst.seats), 0)::bigint FROM grant_seat_tiers gst JOIN grants g ON g.id = gst.grant_id \
          WHERE g.account_id = $1 AND g.revoked_at IS NULL",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .expect("read seat ledger")
}

async fn periods_at(owner: &PgPool, account: Uuid, start: i64) -> Vec<Decimal> {
    sqlx::query_scalar(
        "SELECT granted_credits FROM member_allowance_periods WHERE account_id = $1 AND period_start = $2 \
          ORDER BY user_id",
    )
    .bind(account)
    .bind(ts(start))
    .fetch_all(owner)
    .await
    .expect("read periods")
}

async fn notices(owner: &PgPool, account: Uuid) -> Vec<String> {
    sqlx::query_scalar("SELECT notice_kind FROM billing_notices WHERE account_id = $1 ORDER BY notice_kind")
        .bind(account)
        .fetch_all(owner)
        .await
        .expect("read notices")
}

async fn ledger_kinds(owner: &PgPool, account: Uuid) -> Vec<(String, Decimal)> {
    sqlx::query_as("SELECT entry_kind, credits FROM credit_ledger WHERE account_id = $1 ORDER BY created_at, id")
        .bind(account)
        .fetch_all(owner)
        .await
        .expect("read ledger")
}

async fn provisioning_jobs(owner: &PgPool, account: Uuid) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM tenant_provisioning_jobs WHERE account_id = $1")
        .bind(account)
        .fetch_one(owner)
        .await
        .expect("read provisioning jobs")
}

/// Every allowance period of the account, `(user, start, end, granted)`.
async fn all_periods(owner: &PgPool, account: Uuid) -> Vec<(Uuid, DateTime<Utc>, DateTime<Utc>, Decimal)> {
    sqlx::query_as(
        "SELECT user_id, period_start, period_end, granted_credits FROM member_allowance_periods \
          WHERE account_id = $1 ORDER BY user_id, period_start",
    )
    .bind(account)
    .fetch_all(owner)
    .await
    .expect("read all periods")
}

/// The mirror with its period.
async fn mirror_full(owner: &PgPool, account: Uuid) -> (Option<String>, String, Option<DateTime<Utc>>, Option<DateTime<Utc>>) {
    sqlx::query_as(
        "SELECT stripe_subscription_id, status, current_period_start, current_period_end \
           FROM stripe_subscriptions WHERE account_id = $1",
    )
    .bind(account)
    .fetch_one(owner)
    .await
    .unwrap()
}

async fn event_outcome(owner: &PgPool, event: &str) -> Option<String> {
    sqlx::query_scalar("SELECT outcome FROM stripe_webhook_events WHERE event_id = $1")
        .bind(event)
        .fetch_optional(owner)
        .await
        .expect("read event")
        .flatten()
}

async fn cleanup(owner: &PgPool, org: &Org) {
    sqlx::query("DELETE FROM org_trial_claims WHERE account_id = $1")
        .bind(org.account)
        .execute(owner)
        .await
        .expect("cleanup claims");
    fin(owner, org.account, sqlx::query("DELETE FROM accounts WHERE id = $1").bind(org.account)).await;
}

async fn cleanup_users(owner: &PgPool, users: &[Uuid]) {
    for u in users {
        sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(owner).await.expect("cleanup user");
    }
}

fn evt() -> String {
    format!("evt_{}", Uuid::new_v4().simple())
}

fn fingerprint() -> String {
    format!("fp_{}", Uuid::new_v4().simple())
}

/// Price the `standard` seat for this test; returns the previous price to
/// restore. Callers hold `CATALOG_PRICE_LOCK`.
async fn price_standard(owner: &PgPool) -> (String, Option<String>) {
    let price = format!("price_std_trial_{}", Uuid::new_v4().simple());
    let previous = set_seat_tier_price(owner, "standard", Some(&price)).await;
    (price, previous)
}

// ── Checkout completion ──────────────────────────────────────────────────

/// Decision #1: Checkout completed grants 3,000 credits as ONE flat org
/// pool, whatever the seat count; the team grant is real (seat ledger at
/// the bought quantity) but pays 0 credits per seat until conversion; the
/// trial and its claims are recorded; provisioning is asked for in the same
/// transaction (the flag is off here, which must not fail the event).
#[tokio::test]
async fn checkout_completion_starts_a_trial_with_one_flat_pool_and_a_zero_credit_team_grant() {
    const NAME: &str = "checkout_completion_starts_a_trial_with_one_flat_pool_and_a_zero_credit_team_grant";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 3).await;
    open_trial(&p.app, &org, 3, true).await;
    let fp = fingerprint();
    let (stripe, mock) = spawn_stripe(cfg(&price, 3, &fp), "trialing").await;
    let state = app_state(&p.app, stripe);
    let event = evt();

    let status = complete(&state, &p.app, &org, &mock, &event).await;
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    let granted: Vec<Decimal> = sqlx::query_scalar(
        "SELECT granted_credits FROM member_allowance_periods WHERE account_id = $1",
    )
    .bind(org.account)
    .fetch_all(&p.owner)
    .await
    .unwrap();
    let outcome = event_outcome(&p.owner, &event).await;
    let mirror: (Option<String>, String) =
        sqlx::query_as("SELECT stripe_subscription_id, status FROM stripe_subscriptions WHERE account_id = $1")
            .bind(org.account)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let claimed = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        let user = trial_db::already_claimed(&mut tx, Some(org.users[0]), None).await.unwrap();
        let card = trial_db::already_claimed(&mut tx, None, Some(&fp)).await.unwrap();
        tx.commit().await.unwrap();
        (user, card)
    };
    let ledger = ledger_kinds(&p.owner, org.account).await;
    spend(&p.owner, org.account, 120).await;
    let summary = {
        let user = crate::middleware::auth::UserContext {
            user_id: org.users[0],
            account_id: org.account,
            role: "owner".into(),
            device_id: "trial-test".into(),
        };
        let resp = crate::routes::billing_manage::get_billing_summary(
            axum::extract::State(state.clone()),
            axum::Extension(user),
        )
        .await;
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()
    };
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(summary["trial"]["state"], "trialing", "{summary}");
    assert_eq!(summary["trial"]["seats"], 3);
    assert_eq!(summary["trial"]["credits_used"], 120.0, "{summary}");
    assert_eq!(summary["trial"]["credits_total"], 3000.0);
    assert_eq!(summary["trial"]["payment_failed"], false);
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(row.trial_ends_at, Some(ts(TRIAL_END)), "Stripe's trial_end is the backstop");
    assert_eq!(row.pool_baseline, Some(d(3000)));
    assert_eq!(balance, Some(d(3000)), "one flat pool of 3,000 whatever the seat count");
    assert_eq!(ledger, vec![("trial_credit".to_string(), d(3000))], "the pool's journal moves with it");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(3))], "team grant, 3 seats, 0 credits per seat");
    assert_eq!(seats, 3, "the seat ledger holds every seat bought at Checkout");
    assert!(granted.iter().all(|g| g.is_zero()), "no per-member allowance during the trial: {granted:?}");
    assert_eq!(mirror, (Some(mock.cfg.sub_id.clone()), "active".to_string()), "trialing mirrors as live");
    assert_eq!(claimed, (true, true), "the creator and the card are both claimed");
    let outcome = outcome.unwrap_or_default();
    assert!(outcome.contains("trial=trialing"), "{outcome}");
    assert!(outcome.contains("provisioning=skipped(signup flag off)"), "{outcome}");
}

/// Stripe redelivers, and can send a second distinct event for the same
/// session. Neither may add a second pool, grant or seat.
#[tokio::test]
async fn a_redelivered_or_second_completion_event_adds_nothing() {
    const NAME: &str = "a_redelivered_or_second_completion_event_adds_nothing";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe);
    let event = evt();
    let mut statuses = Vec::new();
    for e in [event.clone(), event.clone(), evt()] {
        statuses.push(complete(&state, &p.app, &org, &mock, &e).await);
    }
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    let ledger = ledger_kinds(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(statuses, vec![StatusCode::OK; 3]);
    assert_eq!(balance, Some(d(3000)));
    assert_eq!(ledger.len(), 1, "{ledger:?}");
    assert_eq!(grants.len(), 1);
    assert_eq!(seats, 2);
}

/// Decision #9, card half: the second org paying with a card that already
/// had a trial gets no free credits; its trial is ended at Stripe at once
/// (charging the seats), and only then does it get the paid allowance.
#[tokio::test]
async fn a_repeat_card_gets_no_pool_and_is_charged_at_once() {
    const NAME: &str = "a_repeat_card_gets_no_pool_and_is_charged_at_once";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let fp = fingerprint();

    let first = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &first, 2, true).await;
    let (stripe_a, mock_a) = spawn_stripe(cfg(&price, 2, &fp), "trialing").await;
    let first_status = complete(&app_state(&p.app, stripe_a), &p.app, &first, &mock_a, &evt()).await;

    let second = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &second, 2, true).await;
    let (stripe_b, mock_b) = spawn_stripe(cfg(&price, 2, &fp), "trialing").await;
    let state_b = app_state(&p.app, stripe_b.clone());
    let second_status = complete(&state_b, &p.app, &second, &mock_b, &evt()).await;
    let after_checkout = trial(&p.app, second.account).await;
    let pool_after_checkout = pool_balance(&p.owner, second.account).await;
    let user_claimed_by_second = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, second.account).await.unwrap();
        let c = trial_db::already_claimed(&mut tx, Some(second.users[0]), None).await.unwrap();
        tx.commit().await.unwrap();
        c
    };

    // The webhook also starts this on a spawned task; driving it here makes
    // the test deterministic (a second run is a no-op either way).
    let mut report = lifecycle::PassReport::default();
    lifecycle::drive_conversion(&p.app, Some(stripe_b.as_ref()), second.account, &mut report).await.expect("drive");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let converted = trial(&p.app, second.account).await;
    let paid = periods_at(&p.owner, second.account, PAID_START).await;
    let grants = team_grants(&p.owner, second.account).await;
    let end_calls = mock_b.calls("POST", &format!("/v1/subscriptions/{}", mock_b.cfg.sub_id));

    cleanup(&p.owner, &first).await;
    cleanup(&p.owner, &second).await;
    cleanup_users(&p.owner, &first.users).await;
    cleanup_users(&p.owner, &second.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((first_status, second_status), (StatusCode::OK, StatusCode::OK));
    assert_eq!(after_checkout.state, TrialState::Converting);
    assert_eq!(after_checkout.convert_reason.as_deref(), Some("repeat_card"));
    assert_eq!(pool_after_checkout, None, "no trial pool for a repeat card");
    assert!(!user_claimed_by_second, "a refused trial claims nobody");
    assert_eq!(converted.state, TrialState::Converted);
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "the paid allowance, per standard seat");
    assert_eq!(grants, vec![(None, Some(2))]);
    assert!(!end_calls.is_empty(), "the trial was ended at Stripe");
    assert!(end_calls.iter().all(|c| c.body.contains("trial_end=now")));
    let keys: std::collections::HashSet<_> = end_calls.iter().map(|c| c.idem.clone()).collect();
    assert_eq!(keys.len(), 1, "every attempt carries the one per-subscription key: {keys:?}");
}

/// Decision #9, user half: a creator who already had a trial is sent to a
/// Checkout with no trial (pay the seats on day 1); a fresh creator gets the
/// 90-day trial on every seat they chose.
#[tokio::test]
async fn a_repeat_creator_gets_a_checkout_that_charges_now() {
    const NAME: &str = "a_repeat_creator_gets_a_checkout_that_charges_now";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let first = seed_org(&p.owner, None, 1).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe);
    let url = "http://localhost:3000/onboarding/org";

    let fresh = crate::routes::org_trial_checkout::start_org_trial_checkout(
        &state, first.account, first.users[0], 4, url, url,
    )
    .await
    .expect("fresh creator checkout");
    let fresh_body = mock.calls("POST", "/v1/checkout/sessions").last().unwrap().body.clone();
    let opened = trial(&p.app, first.account).await;
    complete(&state, &p.app, &first, &mock, &evt()).await;

    let second = seed_org(&p.owner, Some(first.users[0]), 1).await;
    let repeat = crate::routes::org_trial_checkout::start_org_trial_checkout(
        &state, second.account, first.users[0], 2, url, url,
    )
    .await
    .expect("repeat creator checkout");
    let repeat_body = mock.calls("POST", "/v1/checkout/sessions").last().unwrap().body.clone();
    let repeat_row = trial(&p.app, second.account).await;
    let one_seat = crate::routes::org_trial_checkout::start_org_trial_checkout(
        &state, second.account, first.users[0], 1, url, url,
    )
    .await
    .err()
    .map(|r| r.status());

    cleanup(&p.owner, &first).await;
    cleanup(&p.owner, &second).await;
    cleanup_users(&p.owner, &first.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(fresh.trial);
    assert_eq!((fresh.seats, fresh.trial_credits, fresh.trial_days), (4, 3000, 90));
    assert!(fresh_body.contains("subscription_data%5Btrial_period_days%5D=90"), "{fresh_body}");
    assert!(fresh_body.contains("line_items%5B0%5D%5Bquantity%5D=4"), "{fresh_body}");
    assert!(fresh_body.contains("payment_method_collection=always"), "{fresh_body}");
    assert_eq!((opened.state, opened.seats, opened.trial_eligible), (TrialState::CheckoutOpen, 4, true));
    assert!(!repeat.trial, "the creator already had a trial");
    assert!(!repeat_body.contains("trial_period_days"), "{repeat_body}");
    assert!(!repeat_row.trial_eligible);
    assert_eq!(one_seat, Some(StatusCode::BAD_REQUEST), "team orgs start at two seats");
}

/// A creator who already trialed pays at Checkout: the subscription is
/// active, so completion grants the normal allowance right away.
#[tokio::test]
async fn a_no_trial_checkout_is_paid_at_completion() {
    const NAME: &str = "a_no_trial_checkout_is_paid_at_completion";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, false).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "active").await;
    let status = complete(&app_state(&p.app, stripe), &p.app, &org, &mock, &evt()).await;
    let row = trial(&p.app, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(row.convert_reason.as_deref(), Some("no_trial"));
    assert_eq!(grants, vec![(None, Some(2))], "the normal allowance, no override");
    assert_eq!(balance, None, "no trial pool");
}

// ── Spending the pool ────────────────────────────────────────────────────

/// Any member may spend any of the trial pool (no per-member cap while
/// trialing), and the pool is the whole limit: a charge past it is partly
/// unfunded, never funded by extra usage.
#[tokio::test]
async fn any_member_can_spend_the_whole_pool_and_no_more() {
    const NAME: &str = "any_member_can_spend_the_whole_pool_and_no_more";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe), &p.app, &org, &mock, &evt()).await;

    let member = org.users[1];
    let (whole, past, blocked_full, cap) = {
        use crate::billing::funding_policy::db::{load_period, Mode, PeriodSel, SettlementPlanner};
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, org.account).await.unwrap();
        let cap = load_period(&mut tx, org.account, member, Mode::Settlement, PeriodSel::Current)
            .await
            .unwrap()
            .map(|p| p.member.pool_cap);
        let whole = SettlementPlanner::new(org.account).plan(&mut tx, Some(member), PeriodSel::Current, d(3000)).await.unwrap();
        let past = SettlementPlanner::new(org.account).plan(&mut tx, Some(member), PeriodSel::Current, d(3500)).await.unwrap();
        tx.commit().await.unwrap();
        let blocked_full =
            crate::billing::enforcement::is_blocked(&p.app, org.account, member, crate::billing::enforcement::Spend::Background).await;
        (whole, past, blocked_full, cap)
    };
    spend(&p.owner, org.account, 3000).await;
    let blocked_empty =
        crate::billing::enforcement::is_blocked(&p.app, org.account, member, crate::billing::enforcement::Spend::Background).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(cap, Some(None), "uncapped per member while trialing");
    assert_eq!((whole.allowance_credits, whole.unfunded_credits), (d(3000), Decimal::ZERO));
    assert_eq!((past.allowance_credits, past.unfunded_credits), (d(3000), d(500)));
    assert_eq!(past.extra.micro_usd, 0, "extra usage never funds a trial");
    assert!(!blocked_full);
    assert!(blocked_empty, "an empty pool blocks spend until the trial converts");
}

/// Decision #6: extra usage, top-ups and packs stay refused while the trial
/// runs; turning extra usage off is still allowed.
#[tokio::test]
async fn extra_usage_is_refused_until_the_first_charge() {
    const NAME: &str = "extra_usage_is_refused_until_the_first_charge";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe);
    complete(&state, &p.app, &org, &mock, &evt()).await;

    let user = crate::middleware::auth::UserContext {
        user_id: org.users[0],
        account_id: org.account,
        role: "owner".into(),
        device_id: "trial-test".into(),
    };
    let put = |body: Value| {
        let state = state.clone();
        let user = user.clone();
        async move {
            let body: crate::billing::extra_usage::UpdateRequest = serde_json::from_value(body).unwrap();
            let resp = crate::routes::extra_usage::put_extra_usage(
                axum::extract::State(state),
                axum::Extension(user),
                axum::Json(body),
            )
            .await;
            let status = resp.status();
            let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
            (status, serde_json::from_slice::<Value>(&bytes).unwrap_or(Value::Null))
        }
    };
    let turn_on = put(json!({"enabled": true, "monthly_limit_micro_usd": 50_000_000})).await;
    let turn_off = put(json!({"enabled": false})).await;
    let topup = crate::routes::extra_usage::refuse_if_balance_unspendable(&state, org.account).await.err().map(|r| r.status());
    let shared = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        let s = crate::billing::funding_policy::db::load_extra_shared(&mut tx, org.account).await.unwrap();
        tx.commit().await.unwrap();
        s
    };
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(turn_on.0, StatusCode::CONFLICT, "{:?}", turn_on.1);
    assert_eq!(turn_on.1["code"], "trial_active");
    assert_eq!(turn_off.0, StatusCode::OK, "{:?}", turn_off.1);
    assert_eq!(topup, Some(StatusCode::CONFLICT));
    assert!(shared.trial_refuses_extra);
    assert!(!shared.settings.is_some_and(|s| s.enabled));
}

// ── Notices and conversion ───────────────────────────────────────────────

/// 80% used claims the owner's warning; 100% claims the "used up" email and
/// converts: the trial is ended at Stripe (charging the seats), and the
/// paid per-seat allowance is granted for the period Stripe opened.
#[tokio::test]
async fn eighty_percent_warns_and_a_used_up_pool_converts_with_the_paid_allowance() {
    const NAME: &str = "eighty_percent_warns_and_a_used_up_pool_converts_with_the_paid_allowance";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    let now = ts(TRIAL_START + 86_400);

    spend(&p.owner, org.account, 2399).await;
    let quiet = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    spend(&p.owner, org.account, 1).await;
    let at_80 = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let again_80 = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let state_80 = trial(&p.app, org.account).await.state;
    spend(&p.owner, org.account, 650).await; // 3,050 used: a little overshoot past the pool
    let at_100 = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let trial_period_ends: Vec<DateTime<Utc>> = sqlx::query_scalar(
        "SELECT period_end FROM member_allowance_periods WHERE account_id = $1 AND period_start < $2",
    )
    .bind(org.account)
    .bind(ts(PAID_START))
    .fetch_all(&p.owner)
    .await
    .unwrap();
    let kinds = notices(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let after = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let mirror: (String, Option<DateTime<Utc>>) =
        sqlx::query_as("SELECT status, current_period_start FROM stripe_subscriptions WHERE account_id = $1")
            .bind(org.account)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(!quiet.worked(), "2,399 of 3,000 is under 80%: {:?}", quiet.actions);
    assert_eq!(at_80.actions, vec!["trial_credits_80"]);
    assert!(!again_80.worked(), "a notice is claimed once: {:?}", again_80.actions);
    assert_eq!(state_80, TrialState::Trialing);
    assert_eq!(at_100.actions, vec!["trial_credits_used", "converting", "trial_ended_at_stripe", "converted"]);
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(row.convert_reason.as_deref(), Some("credits_used"));
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "each member's first paid month, at once");
    assert!(trial_period_ends.iter().all(|e| *e <= ts(PAID_START)), "trial periods closed at the paid start");
    assert_eq!(kinds, vec!["trial_credits_80", "trial_credits_used"]);
    assert_eq!(grants, vec![(None, Some(2))], "override cleared: the per-tier allowance applies");
    assert_eq!(balance, Some(d(-50)), "the overshoot stays on the pool; nothing left to expire");
    assert!(!after.worked(), "a converted trial is done: {:?}", after.actions);
    assert_eq!(mirror, ("active".to_string(), Some(ts(PAID_START))));
}

/// Conversion happens once even when two workers race each other and usage
/// keeps landing: one paid period per member, one override clear, one
/// Stripe key.
#[tokio::test]
async fn conversion_is_once_under_concurrent_workers_and_debits() {
    const NAME: &str = "conversion_is_once_under_concurrent_workers_and_debits";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 3).await;
    open_trial(&p.app, &org, 3, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 3, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let now = ts(TRIAL_START + 86_400);

    let (a, b, c, _, _) = tokio::join!(
        lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now),
        lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now),
        lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now),
        spend(&p.owner, org.account, 7),
        spend(&p.owner, org.account, 11),
    );
    let reports = [a.unwrap(), b.unwrap(), c.unwrap()];
    let converted_by = reports.iter().filter(|r| r.actions.contains(&"converted")).count();
    let began = reports.iter().filter(|r| r.actions.contains(&"converting")).count();
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let keys: std::collections::HashSet<_> = mock
        .calls("POST", &format!("/v1/subscriptions/{}", mock.cfg.sub_id))
        .into_iter()
        .map(|c| c.idem)
        .collect();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(began, 1, "trialing -> converting happens once: {reports:?}");
    assert_eq!(converted_by, 1, "finalize happens once: {reports:?}");
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 3], "one paid period per member, not one per worker");
    assert_eq!(grants, vec![(None, Some(3))]);
    assert_eq!(balance, Some(d(-18)), "debits that landed during conversion still count");
    assert_eq!(keys.len(), 1, "{keys:?}");
}

/// Day 90 with credits left: the 7-day warning goes out first, then the
/// trial converts anyway and the unused credits are expired (not carried
/// into the paid plan's pool).
#[tokio::test]
async fn the_ninety_day_backstop_warns_then_converts_and_expires_what_is_left() {
    const NAME: &str = "the_ninety_day_backstop_warns_then_converts_and_expires_what_is_left";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 400).await;

    let week_out = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_END - 6 * 86_400)).await.unwrap();
    let state_week_out = trial(&p.app, org.account).await.state;
    let due = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_END)).await.unwrap();
    let row = trial(&p.app, org.account).await;
    let ledger = ledger_kinds(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let kinds = notices(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(week_out.actions, vec!["trial_ending_soon"]);
    assert_eq!(state_week_out, TrialState::Trialing);
    assert!(due.actions.contains(&"converted"), "{:?}", due.actions);
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(row.convert_reason.as_deref(), Some("backstop"));
    assert_eq!(
        ledger,
        vec![("trial_credit".to_string(), d(3000)), ("trial_credit_expired".to_string(), d(-2600))]
    );
    assert_eq!(balance, Some(Decimal::ZERO));
    assert_eq!(kinds, vec!["trial_ending_soon"]);
}

/// A cancel scheduled in the portal during the trial is honoured: running
/// out of credits ends the trial without charging the card.
#[tokio::test]
async fn a_cancel_scheduled_during_the_trial_is_never_charged() {
    const NAME: &str = "a_cancel_scheduled_during_the_trial_is_never_charged";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.cancel_scheduled = true;
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let report = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();
    let row = trial(&p.app, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let posts = mock.calls("POST", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(report.actions.contains(&"canceled"), "{:?}", report.actions);
    assert_eq!(row.state, TrialState::Canceled);
    assert!(posts.is_empty(), "the trial was never ended at Stripe (no charge)");
    assert_eq!(deletes.len(), 1, "the subscription is canceled at Stripe now, so day 90 cannot charge");
    assert!(paid.is_empty(), "no paid allowance: {paid:?}");
    // Review finding 3: the trial's 0-credit override does not outlive the
    // trial (a later subscription on this grant gets the normal allowance).
    assert_eq!(grants, vec![(None, Some(2))]);
}

/// The conversion charge fails (card declined): no paid allowance, the
/// trial waits in `converting` while Stripe retries, and the `invoice.paid`
/// that eventually arrives converts it.
#[tokio::test]
async fn a_failed_conversion_charge_waits_and_invoice_paid_converts_it() {
    const NAME: &str = "a_failed_conversion_charge_waits_and_invoice_paid_converts_it";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let failed = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();
    let waiting = trial(&p.app, org.account).await;
    let paid_before = periods_at(&p.owner, org.account, PAID_START).await;

    let invoice = json!({
        "id": "in_conversion", "object": "invoice", "customer": "cus_trial_mock",
        "billing_reason": "subscription_update", "amount_paid": 4000,
        "parent": {"subscription_details": {"subscription": mock.cfg.sub_id}},
        "subscription": mock.cfg.sub_id,
        "lines": {"data": [{"period": {"start": PAID_START, "end": PAID_END}}]},
    });
    let event = evt();
    let status = crate::routes::stripe_webhook::handle_invoice_paid(
        &p.app, Some(stripe.as_ref()), org.account, &event, "invoice.paid", "cus_trial_mock", &invoice,
    )
    .await
    .status();
    let replay = crate::routes::stripe_webhook::handle_invoice_paid(
        &p.app, Some(stripe.as_ref()), org.account, &event, "invoice.paid", "cus_trial_mock", &invoice,
    )
    .await
    .status();
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let outcome = event_outcome(&p.owner, &event).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(failed.actions.contains(&"awaiting_payment"), "{:?}", failed.actions);
    assert_eq!(waiting.state, TrialState::Converting);
    assert!(paid_before.is_empty(), "no paid allowance before the payment: {paid_before:?}");
    assert_eq!((status, replay), (StatusCode::OK, StatusCode::OK));
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2]);
    assert!(outcome.unwrap_or_default().starts_with("trial_converted"));
}

/// Stripe ends the trial by itself on day 90 (our worker never ran): the
/// `subscription_cycle` invoice it pays converts the trial. The opening $0
/// invoice, by contrast, rolls nothing.
#[tokio::test]
async fn stripe_ending_the_trial_itself_converts_and_the_opening_invoice_rolls_nothing() {
    const NAME: &str = "stripe_ending_the_trial_itself_converts_and_the_opening_invoice_rolls_nothing";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    let periods_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
        .bind(org.account)
        .fetch_one(&p.owner)
        .await
        .unwrap();

    let invoice = |reason: &str, start: i64, end: i64, amount: i64| {
        json!({
            "id": format!("in_{}", Uuid::new_v4().simple()), "object": "invoice", "customer": "cus_trial_mock",
            "billing_reason": reason, "amount_paid": amount, "subscription": mock.cfg.sub_id,
            "lines": {"data": [{"period": {"start": start, "end": end}}]},
        })
    };
    let opening_event = evt();
    let opening = crate::routes::stripe_webhook::handle_invoice_paid(
        &p.app, Some(stripe.as_ref()), org.account, &opening_event, "invoice.paid", "cus_trial_mock",
        &invoice("subscription_create", TRIAL_START, TRIAL_END, 0),
    )
    .await
    .status();
    let periods_after_opening: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
            .bind(org.account)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let state_after_opening = trial(&p.app, org.account).await.state;
    let natural = crate::routes::stripe_webhook::handle_invoice_paid(
        &p.app, Some(stripe.as_ref()), org.account, &evt(), "invoice.paid", "cus_trial_mock",
        &invoice("subscription_cycle", TRIAL_END, TRIAL_END + 30 * 86_400, 4000),
    )
    .await
    .status();
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, TRIAL_END).await;
    let opening_outcome = event_outcome(&p.owner, &opening_event).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((opening, natural), (StatusCode::OK, StatusCode::OK));
    assert_eq!(periods_after_opening, periods_before, "the $0 opening invoice rolls nothing");
    assert_eq!(state_after_opening, TrialState::Trialing);
    assert!(opening_outcome.unwrap_or_default().starts_with("trial_opening_invoice"));
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(row.convert_reason.as_deref(), Some("backstop"));
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2]);
}

/// An unpaid Checkout keeps its org for 24 hours ("resume payment"), then
/// the worker marks it abandoned and a resume is refused.
#[tokio::test]
async fn an_unpaid_checkout_is_abandoned_after_24_hours_and_cannot_be_resumed() {
    const NAME: &str = "an_unpaid_checkout_is_abandoned_after_24_hours_and_cannot_be_resumed";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 1).await;
    let (stripe, _mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    let url = "http://localhost:3000/onboarding/org";
    crate::routes::org_trial_checkout::start_org_trial_checkout(&state, org.account, org.users[0], 2, url, url)
        .await
        .expect("open");
    let opened_at = trial(&p.app, org.account).await.checkout_opened_at;
    let resumed =
        crate::routes::org_trial_checkout::start_org_trial_checkout(&state, org.account, org.users[0], 3, url, url)
            .await
            .expect("resume within the window");
    let reopened = trial(&p.app, org.account).await;
    let early = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, opened_at + chrono::Duration::hours(23))
        .await
        .unwrap();
    let late = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, opened_at + chrono::Duration::hours(24))
        .await
        .unwrap();
    let row = trial(&p.app, org.account).await;
    let after = crate::routes::org_trial_checkout::start_org_trial_checkout(&state, org.account, org.users[0], 2, url, url)
        .await
        .err()
        .map(|r| r.status());
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(resumed.seats, 3);
    assert_eq!(reopened.seats, 3, "a resume may change the seat count");
    assert_eq!(reopened.checkout_opened_at, opened_at, "a resume never extends the window");
    assert!(!early.worked());
    assert_eq!(late.actions, vec!["abandoned", "checkout_expired"], "the abandoned session is expired at Stripe");
    assert_eq!(row.state, TrialState::Abandoned);
    assert_eq!(after, Some(StatusCode::GONE));
}

/// The claims table is reachable only through the definers: the app role
/// cannot list who trialed.
#[tokio::test]
async fn the_app_role_cannot_read_trial_claims_directly() {
    const NAME: &str = "the_app_role_cannot_read_trial_claims_directly";
    let Some(p) = pools(NAME).await else { return };
    let err = sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM org_trial_claims")
        .fetch_one(&p.app)
        .await
        .expect_err("coralswarm_app must not read org_trial_claims");
    assert!(err.to_string().contains("permission denied"), "{err}");
    // And the claim definer refuses an account the transaction is not scoped to.
    let other = Uuid::new_v4();
    let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, Uuid::new_v4()).await.unwrap();
    let refused = trial_db::claim(&mut tx, other, Some(Uuid::new_v4()), None).await;
    tx.rollback().await.ok();
    assert!(refused.is_err(), "claiming for a different account must fail");
}

// ── Review fixes (round 1) ───────────────────────────────────────────────

/// Finding 1a: the conversion charge failed, then the owner scheduled a
/// cancel at period end, then the charge went through (Stripe now reports
/// `active` with `cancel_at_period_end`). The customer paid for the period,
/// so the worker finalizes: the allowance is granted and the cancel takes
/// effect at period end. Cancelling here charged the card and granted
/// nothing. The `invoice.paid` that follows writes nothing more.
#[tokio::test]
async fn a_paid_conversion_with_a_cancel_at_period_end_grants_the_allowance() {
    const NAME: &str = "a_paid_conversion_with_a_cancel_at_period_end_grants_the_allowance";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let now = ts(TRIAL_START + 86_400);
    let failed = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();

    // The owner schedules the cancel; the dunning retry then succeeds.
    mock.set_cancel_scheduled(true);
    mock.set_status("active");
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 4000;
    });
    let paid_pass = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let event = evt();
    let late_invoice = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 4000),
    )
    .await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let deletes = mock.calls("DELETE", "/v1/subscriptions/");
    let outcome = event_outcome(&p.owner, &event).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(failed.actions.contains(&"awaiting_payment"), "{:?}", failed.actions);
    assert!(paid_pass.actions.contains(&"converted"), "{:?}", paid_pass.actions);
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "the paid period's allowance");
    assert_eq!(grants, vec![(None, Some(2))]);
    assert!(deletes.is_empty(), "a paid subscription is never canceled early by the trial: {deletes:?}");
    assert_eq!(late_invoice, StatusCode::OK);
    assert!(outcome.is_none(), "a converted trial's invoice is not the trial's to handle: {outcome:?}");
}

/// Finding 1b + 1c: a cancel scheduled while the conversion charge is in
/// dunning cancels the trial, and on the way voids the open conversion
/// invoice and cancels the subscription at Stripe (keyed), so no retry can
/// charge. If the customer pays that invoice anyway (before the void
/// landed), `invoice.paid` grants the allowance: they paid.
#[tokio::test]
async fn a_conversion_paid_after_a_cancel_from_converting_still_grants_the_allowance() {
    const NAME: &str = "a_conversion_paid_after_a_cancel_from_converting_still_grants_the_allowance";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let now = ts(TRIAL_START + 86_400);
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    mock.set_cancel_scheduled(true);
    let cancel_pass = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let canceled = trial(&p.app, org.account).await;
    let voids = mock.calls("POST", "/v1/invoices/in_conversion_mock/void");
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));

    let event = evt();
    // Round 2 nit: with the signup flag ON, a late-paid finalize still
    // queues no tenant database (the subscription is canceled).
    let status = {
        let _flag = SignupFlagOn::set();
        invoice_paid(
            &p.app, &stripe, org.account, &event,
            &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 4000),
        )
        .await
    };
    let jobs = provisioning_jobs(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    sqlx::query("DELETE FROM tenant_provisioning_jobs WHERE account_id = $1").bind(org.account).execute(&p.owner).await.unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(cancel_pass.actions.contains(&"stopped_at_stripe"), "{:?}", cancel_pass.actions);
    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!(voids.len(), 1, "the open conversion invoice is voided");
    assert!(voids[0].idem.is_some(), "the void is keyed");
    assert_eq!(deletes.len(), 1, "the subscription is canceled at Stripe");
    assert_eq!(deletes[0].idem.as_deref(), Some(lifecycle::cancel_trial_key(org.account, &mock.cfg.sub_id).as_str()));
    assert!(deletes[0].body.contains("prorate=false"), "{}", deletes[0].body);
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Converted, "the customer paid, so the trial converts");
    assert!(row.first_charge_at.is_none(), "#1528 review: a payment owed back is not a first charge");
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2]);
    assert_eq!(grants, vec![(None, Some(2))]);
    assert!(outcome.contains("after_cancel=true"), "{outcome}");
    assert!(outcome.contains("refund_required"), "#1528 N4: the subscription was canceled at Stripe: {outcome}");
    assert_eq!(jobs, 0, "no tenant database is queued for a canceled subscription's late payment");
}

/// Finding 2: a second trial Checkout completing for an org already
/// trialing (an old tab, a resumed session that was not expired in time)
/// made a second subscription that would bill on day 90. It is canceled at
/// Stripe now, keyed on its id, and the mirror, the pool and the trial stay
/// on the first subscription.
#[tokio::test]
async fn a_second_completed_trial_checkout_is_canceled_and_does_not_touch_the_mirror() {
    const NAME: &str = "a_second_completed_trial_checkout_is_canceled_and_does_not_touch_the_mirror";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    let first = complete(&state, &p.app, &org, &mock, &evt()).await;
    let second_sub = format!("sub_dup_{}", Uuid::new_v4().simple());
    let event = evt();
    let second = complete_sub(&state, &p.app, &org, &mock, &event, &second_sub).await;
    let replay = complete_sub(&state, &p.app, &org, &mock, &event, &second_sub).await;
    let linked = mirror(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{second_sub}"));
    let first_deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((first, second, replay), (StatusCode::OK, StatusCode::OK, StatusCode::OK));
    assert!(!deletes.is_empty(), "the duplicate subscription is canceled at Stripe");
    let key = crate::routes::stripe_webhook::duplicate_trial_cancel_key(&second_sub);
    assert!(deletes.iter().all(|c| c.idem.as_deref() == Some(key.as_str())), "{deletes:?}");
    assert!(deletes[0].body.contains("prorate=false") && deletes[0].body.contains("invoice_now=false"));
    assert!(first_deletes.is_empty(), "the trial's own subscription is left alone");
    assert_eq!(linked, (Some(mock.cfg.sub_id.clone()), "active".to_string()), "mirror untouched");
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(row.stripe_subscription_id.as_deref(), Some(mock.cfg.sub_id.as_str()));
    assert_eq!(balance, Some(d(3000)), "no second pool");
    assert_eq!(seats, 2);
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))]);
    assert!(outcome.starts_with("duplicate_trial_subscription"), "{outcome}");
}

/// Finding 3 (and the decision #6 nit): a trial canceled before it paid
/// clears its 0-credit override and expires the pool. Extra usage stays
/// refused until a first successful charge. A later ordinary resubscribe
/// then gives every member the normal allowance, and its paid (`active`)
/// Checkout unlocks extra usage (#1528 S2: at completion).
#[tokio::test]
async fn a_canceled_trial_then_a_normal_resubscribe_gets_the_normal_allowance() {
    const NAME: &str = "a_canceled_trial_then_a_normal_resubscribe_gets_the_normal_allowance";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 500).await;
    // customer.subscription.deleted during the trial.
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;
    let cancel = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400))
        .await
        .unwrap();
    let canceled = trial(&p.app, org.account).await;
    let grants_after_cancel = team_grants(&p.owner, org.account).await;
    let ledger = ledger_kinds(&p.owner, org.account).await;
    let refused_after_cancel = extra_refused(&p.app, org.account).await;

    // An ordinary (non-trial) subscription Checkout for 2 seats, paid now.
    mock.set_status("active");
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 4000;
    });
    let resub = format!("sub_resub_{}", Uuid::new_v4().simple());
    let resubscribed = complete_sub(&state, &p.app, &org, &mock, &evt(), &resub).await;
    let refused_before_invoice = extra_refused(&p.app, org.account).await;
    let paid_invoice = invoice_paid(
        &p.app, &stripe, org.account, &evt(),
        &invoice_json(&resub, "subscription_create", PAID_START, PAID_END, 4000),
    )
    .await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let refused_after_invoice = extra_refused(&p.app, org.account).await;
    let linked = mirror(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(cancel.actions.contains(&"canceled"), "{:?}", cancel.actions);
    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!(grants_after_cancel, vec![(None, Some(2))], "the trial's 0-credit override is cleared on cancel");
    assert_eq!(
        ledger,
        vec![("trial_credit".to_string(), d(3000)), ("trial_credit_expired".to_string(), d(-2500))],
        "the unused trial credits are expired on cancel"
    );
    assert!(refused_after_cancel, "decision #6: no extra usage before a first successful charge");
    assert_eq!(resubscribed, StatusCode::OK);
    // #1528 S2: the completed Checkout's subscription is `active`, so it
    // was paid; the completion stamps the first charge without waiting for
    // an `invoice.paid` that may already have come and gone.
    assert!(!refused_before_invoice, "an active resubscription unlocks extra usage at completion");
    assert_eq!(paid_invoice, StatusCode::OK);
    assert_eq!(linked, (Some(resub.clone()), "active".to_string()));
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "every member gets the normal allowance");
    assert!(!refused_after_invoice, "the first paid invoice unlocks extra usage");
}

/// Finding 4: when no source yields a card fingerprint (subscription,
/// customer, Checkout session), the one-trial-per-card rule cannot be
/// checked. Fail closed: no pool, the trial is ended at once. Round 2: the
/// creator's USER claim is still recorded, so they cannot trial a later org.
#[tokio::test]
async fn an_unreadable_card_fingerprint_fails_closed_and_charges_now() {
    const NAME: &str = "an_unreadable_card_fingerprint_fails_closed_and_charges_now";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.fingerprint = None;
    c.sub_has_default_pm = false;
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    let status = complete(&state, &p.app, &org, &mock, &evt()).await;
    let row = trial(&p.app, org.account).await;
    let ledger = ledger_kinds(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let user_claimed = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        let c = trial_db::already_claimed(&mut tx, Some(org.users[0]), None).await.unwrap();
        tx.commit().await.unwrap();
        c
    };
    let customer_reads = mock.calls("GET", "/v1/customers/");
    let session_reads = mock.calls("GET", "/v1/checkout/sessions/");
    let mut report = lifecycle::PassReport::default();
    lifecycle::drive_conversion(&p.app, Some(stripe.as_ref()), org.account, &mut report).await.expect("drive");
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    let converted = trial(&p.app, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert!(matches!(row.state, TrialState::Converting | TrialState::Converted), "{:?}", row.state);
    assert_eq!(row.convert_reason.as_deref(), Some("card_unverified"));
    assert!(ledger.iter().all(|(k, _)| k != "trial_credit"), "no free credits: {ledger:?}");
    assert!(user_claimed, "round 2: the creator's user claim is recorded even when the card is unverified");
    assert!(grants.iter().all(|(o, _)| o.is_none() || *o == Some(Decimal::ZERO)));
    assert!(!customer_reads.is_empty(), "the customer's default card was tried");
    assert!(!session_reads.is_empty(), "the Checkout session was tried");
    assert_eq!(converted.state, TrialState::Converted, "charged now, then the paid allowance");
}

/// Finding 5: a $0 `subscription_update` invoice while the subscription is
/// still trialing (a seat change, say) is not the conversion charge.
#[tokio::test]
async fn a_zero_dollar_update_invoice_during_the_trial_does_not_convert() {
    const NAME: &str = "a_zero_dollar_update_invoice_during_the_trial_does_not_convert";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    let event = evt();
    let status = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", TRIAL_START, TRIAL_END, 0),
    )
    .await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, TRIAL_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Trialing);
    assert!(paid.iter().all(|g| g.is_zero()), "no paid allowance: {paid:?}");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))]);
    assert_eq!(balance, Some(d(3000)), "the pool is not expired");
    assert!(outcome.starts_with("trial_zero_invoice"), "{outcome}");
}

/// Sets the self-serve signup flag for one test and unsets it on drop.
/// Callers hold `CATALOG_PRICE_LOCK`, which every trial test that reads the
/// flag's effect also holds, so none of them runs while it is set.
struct SignupFlagOn;

impl SignupFlagOn {
    fn set() -> Self {
        std::env::set_var(crate::tenancy::provisioning::SELFSERVE_SIGNUP_FLAG, "1");
        SignupFlagOn
    }
}

impl Drop for SignupFlagOn {
    fn drop(&mut self) {
        std::env::remove_var(crate::tenancy::provisioning::SELFSERVE_SIGNUP_FLAG);
    }
}

/// Finding 6: A1's `enqueue_in` runs its insert under a SAVEPOINT, so a
/// unique conflict it hits inside the Checkout webhook's transaction comes
/// back as `Conflict` (alerted) and the event still commits: the grant, the
/// pool and the trial are all recorded.
#[tokio::test]
async fn an_enqueue_conflict_inside_the_webhook_transaction_still_commits_the_checkout() {
    const NAME: &str = "an_enqueue_conflict_inside_the_webhook_transaction_still_commits_the_checkout";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    // Another org's job already holds this account (UNIQUE account_id): the
    // pre-checks cannot see it (they look at this org's id), the insert does.
    let other_org = format!("org_other_{}", Uuid::new_v4().simple());
    let slug = crate::tenancy::provisioning::opaque_slug_for_account(org.account);
    sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref) VALUES ($1, $2, $3, $4)",
    )
    .bind(&other_org)
    .bind(org.account)
    .bind(&slug)
    .bind(format!("TENANT_DB_{}", slug.to_uppercase().replace('-', "_")))
    .execute(&p.owner)
    .await
    .expect("seed the conflicting job");
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe);
    let event = evt();
    let status = {
        let _flag = SignupFlagOn::set();
        complete(&state, &p.app, &org, &mock, &event).await
    };
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    let own_job: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(format!("org_{}", org.account.simple()))
        .fetch_one(&p.owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(&other_org)
        .execute(&p.owner)
        .await
        .unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK, "the conflict must not abort the webhook's transaction");
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(balance, Some(d(3000)));
    assert!(outcome.contains("provisioning=skipped(conflict"), "{outcome}");
    assert_eq!(own_job, 0, "no job row for this org was written");
}

/// Finding 7: Stripe does not order `invoice.paid` (the $0 opening invoice)
/// and `checkout.session.completed`. Arriving first, the invoice rolls
/// nothing and grants nothing; the completion that follows proceeds
/// normally.
#[tokio::test]
async fn the_opening_invoice_before_checkout_completion_rolls_nothing() {
    const NAME: &str = "the_opening_invoice_before_checkout_completion_rolls_nothing";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let event = evt();
    let early = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_create", TRIAL_START, TRIAL_END, 0),
    )
    .await;
    let periods_early: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
        .bind(org.account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let grants_early = team_grants(&p.owner, org.account).await;
    let any_grants_early: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM grants WHERE account_id = $1")
        .bind(org.account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let mirror_early = mirror(&p.owner, org.account).await;
    let state_early = trial(&p.app, org.account).await.state;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();

    let completed = complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(early, StatusCode::OK);
    assert_eq!(periods_early, 0, "no period rolled before the Checkout completed");
    assert!(grants_early.is_empty() && any_grants_early == 0, "nothing granted");
    assert_eq!(mirror_early, (None, "pending".to_string()), "the mirror is the completion's to write");
    assert_eq!(state_early, TrialState::CheckoutOpen);
    assert!(outcome.starts_with("trial_invoice_before_checkout"), "{outcome}");
    assert_eq!(completed, StatusCode::OK);
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(balance, Some(d(3000)));
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))]);
    assert_eq!(seats, 2);
}

/// Nit: the app role holds exactly SELECT, INSERT, UPDATE on `org_trials`.
#[tokio::test]
async fn the_app_role_cannot_delete_trial_rows() {
    const NAME: &str = "the_app_role_cannot_delete_trial_rows";
    let Some(p) = pools(NAME).await else { return };
    let privs: Vec<(String, bool)> = sqlx::query_as(
        "SELECT p, has_table_privilege('coralswarm_app', 'public.org_trials', p) \
           FROM unnest(ARRAY['SELECT','INSERT','UPDATE','DELETE','TRUNCATE','REFERENCES','TRIGGER']) AS p",
    )
    .fetch_all(&p.owner)
    .await
    .unwrap();
    let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, Uuid::new_v4()).await.unwrap();
    let delete = sqlx::query("DELETE FROM org_trials").execute(&mut *tx).await;
    tx.rollback().await.ok();
    let expected: Vec<(String, bool)> = [
        ("SELECT", true),
        ("INSERT", true),
        ("UPDATE", true),
        ("DELETE", false),
        ("TRUNCATE", false),
        ("REFERENCES", false),
        ("TRIGGER", false),
    ]
    .into_iter()
    .map(|(p, b)| (p.to_string(), b))
    .collect();
    assert_eq!(privs, expected);
    let err = delete.expect_err("coralswarm_app must not delete org_trials rows");
    assert!(err.to_string().contains("permission denied"), "{err}");
}

// ── Review round 2 ───────────────────────────────────────────────────────

/// Round 2, finding 5: a $0 non-create invoice converts only when Stripe
/// reports the subscription `active`. `past_due` (the conversion charge
/// failed) is not a payment.
#[tokio::test]
async fn a_zero_dollar_invoice_while_past_due_does_not_convert() {
    const NAME: &str = "a_zero_dollar_invoice_while_past_due_does_not_convert";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    mock.set_status("past_due");
    let event = evt();
    let status = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 0),
    )
    .await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Trialing, "past_due is not paid");
    assert!(paid.is_empty(), "no paid allowance: {paid:?}");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))]);
    assert_eq!(balance, Some(d(3000)), "the pool is not expired");
    assert!(outcome.starts_with("trial_zero_invoice"), "{outcome}");
}

/// Round 2, finding A: cancelling from `converting` never cancels a
/// customer who has just paid.
///
/// 1. The void fails and the invoice is still `open`: nothing is canceled
///    (the row stays `converting`, the next pass decides again).
/// 2. The customer pays just before the void lands (the void answers 400,
///    the invoice is `paid`, the subscription `active`): the conversion is
///    finalized, the allowance granted, the subscription is NOT canceled,
///    and the owner's cancel stays scheduled for period end.
#[tokio::test]
async fn a_conversion_paid_while_being_canceled_is_finalized_not_canceled() {
    const NAME: &str = "a_conversion_paid_while_being_canceled_is_finalized_not_canceled";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let now = ts(TRIAL_START + 86_400);
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();

    mock.set_cancel_scheduled(true);
    mock.invoice(|i| i.void_fails = true);
    let void_failed = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await;
    let after_void_failed = trial(&p.app, org.account).await;
    let deletes_after_void_failed = mock.calls("DELETE", "/v1/subscriptions/").len();

    mock.invoice(|i| {
        i.void_fails = false;
        i.pay_on_void = true;
        i.amount_paid = 8000;
    });
    let paid_pass = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let deletes = mock.calls("DELETE", "/v1/subscriptions/");
    let invoice_reads = mock.calls("GET", "/v1/invoices/in_conversion_mock");
    let cancel_kept = *mock.cancel_scheduled.lock().unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(void_failed.is_err(), "an unconfirmed void is retried, not followed by a cancel: {void_failed:?}");
    assert_eq!(after_void_failed.state, TrialState::Converting);
    assert_eq!(deletes_after_void_failed, 0, "no cancel while the invoice is still open");
    let paid_pass = paid_pass.expect("paid pass");
    assert!(paid_pass.actions.contains(&"paid_before_cancel"), "{:?}", paid_pass.actions);
    assert!(paid_pass.actions.contains(&"converted"), "{:?}", paid_pass.actions);
    assert_eq!(row.state, TrialState::Converted);
    assert!(row.first_charge_at.is_some());
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "the paid allowance");
    assert_eq!(grants, vec![(None, Some(2))]);
    assert!(deletes.is_empty(), "a customer who paid is never canceled on the spot: {deletes:?}");
    assert!(invoice_reads.len() >= 2, "the invoice is re-read after every void attempt");
    assert!(cancel_kept, "the owner's cancel stays scheduled for period end");
}

/// Round 2, finding B: the trial is canceled from `converting`, the org
/// resubscribes on sub_B, and only then is sub_A's conversion invoice paid.
/// sub_B's mirror, every allowance period and the grant are untouched; the
/// payment is recorded for a manual refund.
#[tokio::test]
async fn a_late_paid_conversion_after_a_resubscribe_leaves_the_live_subscription_alone() {
    const NAME: &str = "a_late_paid_conversion_after_a_resubscribe_leaves_the_live_subscription_alone";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(&price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let now = ts(TRIAL_START + 86_400);
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    mock.set_cancel_scheduled(true);
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, now).await.unwrap();
    let canceled = trial(&p.app, org.account).await;
    // customer.subscription.deleted for sub_A.
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;

    // The org resubscribes on sub_B (an ordinary paid Checkout) and its
    // first invoice rolls a period well after sub_A's conversion period.
    mock.set_status("active");
    mock.set_cancel_scheduled(false);
    let sub_b = format!("sub_resub_{}", Uuid::new_v4().simple());
    let resubscribed = complete_sub(&state, &p.app, &org, &mock, &evt(), &sub_b).await;
    let resub_start = PAID_END + 5 * 86_400;
    let resub_paid = invoice_paid(
        &p.app, &stripe, org.account, &evt(),
        &invoice_json(&sub_b, "subscription_create", resub_start, resub_start + 30 * 86_400, 8000),
    )
    .await;
    let mirror_before = mirror_full(&p.owner, org.account).await;
    let periods_before = all_periods(&p.owner, org.account).await;
    let grants_before = team_grants(&p.owner, org.account).await;
    let seats_before = ledger_seats(&p.owner, org.account).await;

    // sub_A's conversion invoice is paid late.
    let event = evt();
    let late = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 8000),
    )
    .await;
    let mirror_after = mirror_full(&p.owner, org.account).await;
    let periods_after = all_periods(&p.owner, org.account).await;
    let grants_after = team_grants(&p.owner, org.account).await;
    let seats_after = ledger_seats(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!((resubscribed, resub_paid, late), (StatusCode::OK, StatusCode::OK, StatusCode::OK));
    assert_eq!(mirror_before.0.as_deref(), Some(sub_b.as_str()), "the org runs on sub_B");
    assert_eq!(mirror_before.1, "active");
    assert!(!periods_before.is_empty(), "sub_B's first invoice rolled a period");
    assert_eq!(mirror_after, mirror_before, "sub_B's mirror is untouched");
    assert_eq!(periods_after, periods_before, "no period closed or rolled by sub_A's payment");
    assert_eq!(grants_after, grants_before, "the grant (and its override) is untouched");
    assert_eq!(seats_after, seats_before);
    assert_eq!(row.state, TrialState::Canceled, "the trial does not convert onto a replaced subscription");
    assert!(outcome.starts_with("trial_late_paid_superseded refund_required"), "{outcome}");
    assert!(outcome.contains(&format!("live={sub_b}")), "{outcome}");
}

/// Round 2, finding C: a duplicate Checkout that PAID is refunded in full,
/// keyed on its invoice; a $0 duplicate is not refunded. A failed refund
/// answers 500 and leaves the event unclaimed, so Stripe redelivers.
#[tokio::test]
async fn a_paid_duplicate_checkout_is_refunded_in_full_and_a_zero_one_is_not() {
    const NAME: &str = "a_paid_duplicate_checkout_is_refunded_in_full_and_a_zero_one_is_not";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;

    // A $0 duplicate (another trial Checkout): canceled, nothing refunded.
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 0;
    });
    let zero_sub = format!("sub_dup0_{}", Uuid::new_v4().simple());
    let zero_event = evt();
    let zero = complete_sub(&state, &p.app, &org, &mock, &zero_event, &zero_sub).await;
    let refunds_after_zero = mock.calls("POST", "/v1/refunds").len();
    let zero_outcome = event_outcome(&p.owner, &zero_event).await.unwrap_or_default();

    // A paid duplicate (a no-trial Checkout charged at completion). The
    // first refund attempt fails: 500, event not claimed.
    mock.invoice(|i| {
        i.amount_paid = 8000;
        i.refund_fails = true;
    });
    let paid_sub = format!("sub_dup1_{}", Uuid::new_v4().simple());
    let paid_event = evt();
    let failed = complete_sub(&state, &p.app, &org, &mock, &paid_event, &paid_sub).await;
    let claimed_after_failure = event_outcome(&p.owner, &paid_event).await;
    let claimed_row: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM stripe_webhook_events WHERE event_id = $1")
        .bind(&paid_event)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    // Stripe redelivers; the refund now succeeds.
    mock.invoice(|i| i.refund_fails = false);
    let redelivered = complete_sub(&state, &p.app, &org, &mock, &paid_event, &paid_sub).await;
    let refunds = mock.calls("POST", "/v1/refunds");
    let paid_outcome = event_outcome(&p.owner, &paid_event).await.unwrap_or_default();
    let linked = mirror(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(zero, StatusCode::OK);
    assert_eq!(refunds_after_zero, 0, "a $0 duplicate is not refunded");
    assert!(zero_outcome.contains("refund=none"), "{zero_outcome}");
    assert_eq!(failed, StatusCode::INTERNAL_SERVER_ERROR, "a failed refund asks Stripe to redeliver");
    assert_eq!((claimed_after_failure, claimed_row), (None, 0), "the event is not claimed");
    assert_eq!(redelivered, StatusCode::OK);
    assert_eq!(refunds.len(), 2, "one failed attempt, one success");
    let key = crate::routes::stripe_webhook::duplicate_trial_refund_key("in_conversion_mock");
    assert!(refunds.iter().all(|r| r.idem.as_deref() == Some(key.as_str())), "{refunds:?}");
    assert!(refunds.iter().all(|r| r.body.contains("payment_intent=pi_conversion_mock")), "{refunds:?}");
    assert!(refunds.iter().all(|r| !r.body.contains("amount=")), "a full refund names no amount: {refunds:?}");
    assert!(paid_outcome.contains("refunded=in_conversion_mock amount=8000"), "{paid_outcome}");
    assert_eq!(linked, (Some(mock.cfg.sub_id.clone()), "active".to_string()), "mirror untouched");
    assert_eq!(row.state, TrialState::Trialing);
}

/// Round 2, finding E: ending the trial expires only what is left of the
/// trial grant. A promo credit that was in the pool before the trial, and
/// an operator credit added during it, both survive.
#[tokio::test]
async fn pool_expiry_keeps_credits_that_did_not_come_from_the_trial() {
    const NAME: &str = "pool_expiry_keeps_credits_that_did_not_come_from_the_trial";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    // 700 promo credits in the pool before the trial.
    fin(
        &p.owner,
        org.account,
        sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, 'promo_grant', 700)")
            .bind(org.account),
    )
    .await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 700)").bind(org.account),
    )
    .await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    // 1,000 spent, then an operator adds 500 during the trial.
    spend(&p.owner, org.account, 1000).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, 'operator_grant', 500)")
            .bind(org.account),
    )
    .await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE credit_balances SET balance = balance + 500 WHERE account_id = $1").bind(org.account),
    )
    .await;
    let used_mid_trial = {
        let row = trial(&p.app, org.account).await;
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        let pool = trial_db::trial_pool(&mut tx, &row).await.unwrap();
        tx.commit().await.unwrap();
        pool
    };
    // Day 90: the backstop converts and expires what is left of the trial.
    let report = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_END + 60)).await.unwrap();
    let row = trial(&p.app, org.account).await;
    let ledger = ledger_kinds(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(used_mid_trial.granted, d(3000));
    assert_eq!(used_mid_trial.used, d(1000), "the operator credit does not hide trial spending");
    assert_eq!(used_mid_trial.remaining, d(2000));
    assert!(report.actions.contains(&"converted"), "{:?}", report.actions);
    assert_eq!(row.state, TrialState::Converted);
    let expired: Vec<Decimal> =
        ledger.iter().filter(|(k, _)| k == "trial_credit_expired").map(|(_, c)| *c).collect();
    assert_eq!(expired, vec![d(-2000)], "only the trial's unspent 2,000 expire: {ledger:?}");
    assert_eq!(balance, Some(d(1200)), "the 700 promo and 500 operator credits survive");
}

// ── #1528 follow-ups ─────────────────────────────────────────────────────

/// A trial in `converting` with its conversion charge in dunning
/// (`past_due`), as the worker leaves it after ending the trial.
async fn converting_in_dunning(p: &Pools, price: &str) -> (Org, Arc<StripeClient>, StripeMock) {
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let mut c = cfg(price, 2, &fingerprint());
    c.status_after_end = "past_due";
    let (stripe, mock) = spawn_stripe(c, "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 3000).await;
    let first = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400))
        .await
        .unwrap();
    assert!(first.actions.contains(&"awaiting_payment"), "{:?}", first.actions);
    (org, stripe, mock)
}

/// #1528 S1: the owner cancels while the conversion charge is in dunning.
/// Voiding the open conversion invoice moves Stripe's `past_due`
/// subscription back to `active`. The invoice was read and is `void`, so
/// nothing was paid: the trial is canceled (at Stripe too) and no
/// first-month allowance is granted on the unpaid invoice.
#[tokio::test]
async fn a_void_invoice_on_an_active_subscription_cancels_and_grants_nothing() {
    const NAME: &str = "a_void_invoice_on_an_active_subscription_cancels_and_grants_nothing";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_in_dunning(&p, &price).await;

    mock.set_cancel_scheduled(true);
    mock.invoice(|i| i.activate_on_void = true);
    let pass = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let voids = mock.calls("POST", "/v1/invoices/in_conversion_mock/void");
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let status_after_void = mock.status.lock().unwrap().clone();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let pass = pass.expect("cancel pass");
    assert_eq!(voids.len(), 1, "the open conversion invoice is voided");
    assert!(!pass.actions.contains(&"converted"), "a void invoice is not a payment: {:?}", pass.actions);
    assert!(pass.actions.contains(&"stopped_at_stripe"), "{:?}", pass.actions);
    assert_eq!(row.state, TrialState::Canceled);
    assert!(row.first_charge_at.is_none(), "nothing was charged");
    assert!(paid.is_empty(), "no paid allowance on a void invoice: {paid:?}");
    assert_eq!(grants, vec![(None, Some(2))], "the trial override is cleared by the cancel");
    assert_eq!(deletes.len(), 1, "the subscription is canceled at Stripe");
    assert_eq!(status_after_void, "canceled");
}

/// #1528 S1, the next pass: Stripe already shows the subscription `active`
/// with its conversion invoice `void` (voided earlier, or from the
/// dashboard). `active` alone used to finalize; the invoice decides, so
/// the trial is canceled with no allowance.
#[tokio::test]
async fn an_active_subscription_whose_conversion_invoice_is_void_is_not_finalized() {
    const NAME: &str = "an_active_subscription_whose_conversion_invoice_is_void_is_not_finalized";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_in_dunning(&p, &price).await;

    mock.set_status("active");
    mock.invoice(|i| i.status = "void".into());
    let pass = lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let pass = pass.expect("pass");
    assert!(!pass.actions.contains(&"converted"), "{:?}", pass.actions);
    assert_eq!(row.state, TrialState::Canceled);
    assert!(paid.is_empty(), "no paid allowance on a void invoice: {paid:?}");
    assert_eq!(deletes.len(), 1, "the subscription is canceled at Stripe");
}

/// #1528 S2: a canceled trial's org resubscribes through Checkout, and the
/// new subscription's `invoice.paid` arrives BEFORE
/// `checkout.session.completed` (the mirror still names the old, canceled
/// subscription, so `invoice.paid` cannot stamp the first charge). The
/// completion stamps it: extra usage unlocks now, not a month later.
#[tokio::test]
async fn a_resubscribe_whose_invoice_paid_arrives_first_still_unlocks_extra_usage() {
    const NAME: &str = "a_resubscribe_whose_invoice_paid_arrives_first_still_unlocks_extra_usage";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();
    let canceled = trial(&p.app, org.account).await;

    mock.set_status("active");
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 4000;
    });
    let resub = format!("sub_resub_{}", Uuid::new_v4().simple());
    let early_invoice = invoice_paid(
        &p.app, &stripe, org.account, &evt(),
        &invoice_json(&resub, "subscription_create", PAID_START, PAID_END, 4000),
    )
    .await;
    let refused_after_early_invoice = extra_refused(&p.app, org.account).await;
    let event = evt();
    let resubscribed = complete_sub(&state, &p.app, &org, &mock, &event, &resub).await;
    let refused_after_completion = extra_refused(&p.app, org.account).await;
    let row = trial(&p.app, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(canceled.state, TrialState::Canceled);
    assert!(canceled.first_charge_at.is_none());
    let _ = early_invoice;
    assert!(refused_after_early_invoice, "the early invoice.paid cannot stamp: the mirror names the old subscription");
    assert_eq!(resubscribed, StatusCode::OK);
    assert!(row.first_charge_at.is_some(), "the completion of a paid resubscribe stamps the first charge");
    assert!(!refused_after_completion, "extra usage unlocks at the resubscribe");
    assert!(outcome.contains("first_charge=stamped"), "{outcome}");
}

/// #1528 S2's other half: a resubscribe whose subscription is NOT active
/// (still in dunning) has not paid, and stamps nothing.
#[tokio::test]
async fn a_resubscribe_that_is_not_active_does_not_unlock_extra_usage() {
    const NAME: &str = "a_resubscribe_that_is_not_active_does_not_unlock_extra_usage";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();

    mock.set_status("past_due");
    let resub = format!("sub_resub_{}", Uuid::new_v4().simple());
    let resubscribed = complete_sub(&state, &p.app, &org, &mock, &evt(), &resub).await;
    let refused = extra_refused(&p.app, org.account).await;
    let row = trial(&p.app, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(resubscribed, StatusCode::OK);
    assert!(row.first_charge_at.is_none(), "nothing was paid");
    assert!(refused, "extra usage stays locked until a charge");
}

#[derive(Clone, Default)]
struct LogBuf(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for LogBuf {
    fn write(&mut self, b: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(b);
        Ok(b.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for LogBuf {
    type Writer = LogBuf;
    fn make_writer(&'a self) -> LogBuf {
        self.clone()
    }
}

/// #1528 S3: a worker pass that fails for a trial stuck in `converting`
/// for more than 24 hours raises an operator alert (error, `alert=true`).
/// Within the first 24 hours it is only a warning.
#[tokio::test]
async fn a_trial_stuck_in_converting_for_a_day_raises_an_alert() {
    const NAME: &str = "a_trial_stuck_in_converting_for_a_day_raises_an_alert";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, _stripe, _mock) = converting_in_dunning(&p, &price).await;
    let requested = trial_converting_since(&p.app, org.account).await;

    let buf = LogBuf::default();
    let subscriber = tracing_subscriber::fmt().with_writer(buf.clone()).with_ansi(false).finish();
    let (fresh, stuck, not_converting) = {
        let _guard = tracing::subscriber::set_default(subscriber);
        let fresh = lifecycle::report_pass_failure(&p.app, org.account, requested + chrono::Duration::hours(23), "boom fresh").await;
        let stuck = lifecycle::report_pass_failure(&p.app, org.account, requested + chrono::Duration::hours(25), "boom stuck").await;
        let other = Uuid::new_v4();
        let not_converting = lifecycle::report_pass_failure(&p.app, other, requested + chrono::Duration::hours(25), "boom other").await;
        (fresh, stuck, not_converting)
    };
    let out = String::from_utf8(buf.0.lock().unwrap().clone()).unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(fresh, lifecycle::FailureReport::Warned);
    assert_eq!(stuck, lifecycle::FailureReport::Alerted);
    assert_eq!(not_converting, lifecycle::FailureReport::Warned);
    let stuck_line = out.lines().find(|l| l.contains("boom stuck")).unwrap_or_default();
    assert!(stuck_line.contains("ERROR") && stuck_line.contains("alert=true"), "{out}");
    let fresh_line = out.lines().find(|l| l.contains("boom fresh")).unwrap_or_default();
    assert!(fresh_line.contains("WARN") && !fresh_line.contains("alert=true"), "{out}");
}

async fn trial_converting_since(app: &PgPool, account: Uuid) -> DateTime<Utc> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(app, account).await.unwrap();
    let at = trial_db::converting_since(&mut tx, account).await.unwrap().expect("converting");
    tx.commit().await.unwrap();
    at
}

/// #1528 N1: an operator clawback (a negative ledger row that is not
/// usage) lowers the pool without anyone spending. It is not trial spend.
#[tokio::test]
async fn an_operator_clawback_is_not_counted_as_trial_spend() {
    const NAME: &str = "an_operator_clawback_is_not_counted_as_trial_spend";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    spend(&p.owner, org.account, 1000).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("INSERT INTO credit_ledger (account_id, entry_kind, credits) VALUES ($1, 'operator_adjustment', -400)")
            .bind(org.account),
    )
    .await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE credit_balances SET balance = balance - 400 WHERE account_id = $1").bind(org.account),
    )
    .await;
    let pool = {
        let row = trial(&p.app, org.account).await;
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.unwrap();
        let pool = trial_db::trial_pool(&mut tx, &row).await.unwrap();
        tx.commit().await.unwrap();
        pool
    };
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(pool.granted, d(3000));
    assert_eq!(pool.used, d(1000), "the clawback is not spending: {pool:?}");
    assert_eq!(pool.balance, d(1600));
    assert_eq!(pool.remaining, d(1600), "never more than the balance");
}

/// #1528 N3: a paid duplicate Checkout redelivered after the refund's
/// idempotency key expired sends a new refund, which Stripe answers with
/// `charge_already_refunded`. The refund is done: 200, recorded as refunded.
#[tokio::test]
async fn an_already_refunded_duplicate_counts_as_refunded() {
    const NAME: &str = "an_already_refunded_duplicate_counts_as_refunded";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 8000;
        i.refund_already_done = true;
    });
    let dup = format!("sub_dup_{}", Uuid::new_v4().simple());
    let event = evt();
    let status = complete_sub(&state, &p.app, &org, &mock, &event, &dup).await;
    let refunds = mock.calls("POST", "/v1/refunds").len();
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK, "already refunded is success, not a redelivery loop");
    assert_eq!(refunds, 1);
    assert!(outcome.contains("refunded=in_conversion_mock amount=8000"), "{outcome}");
}

/// #1528 N4: the trial was canceled from `converting` (and its
/// subscription with it; `customer.subscription.deleted` has landed), then
/// the conversion invoice is paid anyway. The outcome says
/// `refund_required`: the org cannot use what it paid for.
#[tokio::test]
async fn a_late_paid_conversion_on_a_canceled_subscription_says_refund_required() {
    const NAME: &str = "a_late_paid_conversion_on_a_canceled_subscription_says_refund_required";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_in_dunning(&p, &price).await;
    mock.set_cancel_scheduled(true);
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();
    let canceled = trial(&p.app, org.account).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;
    let event = evt();
    let status = invoice_paid(
        &p.app, &stripe, org.account, &event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 4000),
    )
    .await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    let linked = mirror(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!(status, StatusCode::OK);
    assert!(outcome.starts_with("trial_converted"), "{outcome}");
    assert!(outcome.contains("refund_required"), "{outcome}");
    assert!(outcome.contains("amount_paid=4000"), "{outcome}");
    assert_eq!(linked.1, "canceled", "a late payment does not revive the subscription");
    assert_eq!(row.state, TrialState::Converted);
    assert!(row.first_charge_at.is_none(), "#1528 review: refund_required is not a first charge");
}

/// #1528 N5: a $0 non-opening invoice converts only on the subscription's
/// `active` status. A stale one names an old period in its lines; the
/// conversion uses the period of the subscription that was read.
#[tokio::test]
async fn a_stale_zero_dollar_invoice_finalizes_on_the_subscriptions_period() {
    const NAME: &str = "a_stale_zero_dollar_invoice_finalizes_on_the_subscriptions_period";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    // Stripe's subscription is active on [PAID_START, PAID_END); the $0
    // invoice delivered now names a stale period inside the trial.
    // It is the subscription's latest invoice, and paid ($0: a 100%-off
    // coupon, say), so it converts (#1528 review: only then).
    mock.set_status("active");
    mock.invoice(|i| i.status = "paid".into());
    let stale_start = TRIAL_START + 5 * 86_400;
    let event = evt();
    let mut zero = invoice_json(&mock.cfg.sub_id, "subscription_update", stale_start, stale_start + 30 * 86_400, 0);
    zero["id"] = json!("in_conversion_mock");
    let status = invoice_paid(&p.app, &stripe, org.account, &event, &zero).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let stale = periods_at(&p.owner, org.account, stale_start).await;
    let linked = mirror_full(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Converted);
    assert_eq!(paid, vec![d(STANDARD_SEAT_CREDITS); 2], "rolled on the subscription's period");
    assert!(stale.is_empty(), "nothing rolled on the stale invoice's period: {stale:?}");
    assert_eq!((linked.2, linked.3), (Some(ts(PAID_START)), Some(ts(PAID_END))));
}

// ── #1528 review fixes ───────────────────────────────────────────────────

/// A trial moved to `converting` in the database while Stripe still shows
/// the subscription `trialing`, with no worker pass yet (so no
/// `payment_failed_at`).
async fn converting_untouched(p: &Pools, price: &str) -> (Org, Arc<StripeClient>, StripeMock) {
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(price, 2, &fingerprint()), "trialing").await;
    complete(&app_state(&p.app, stripe.clone()), &p.app, &org, &mock, &evt()).await;
    let mut tx = crate::tenancy::scope::begin_account_scoped(&p.app, org.account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, org.account).await.expect("lock");
    assert!(trial_db::begin_conversion(&mut tx, org.account, super::ConvertReason::Backstop).await.expect("begin"));
    tx.commit().await.expect("commit");
    (org, stripe, mock)
}

async fn payment_failed_at(owner: &PgPool, account: Uuid) -> Option<DateTime<Utc>> {
    sqlx::query_scalar("SELECT payment_failed_at FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_one(owner)
        .await
        .unwrap()
}

/// #1528 review, fix 1: the webhook's $0 path applies S1's rule too. The
/// conversion invoice was voided and Stripe flipped the subscription back
/// to `active`; a $0 `subscription_update` invoice.paid then arrives. It is
/// not the subscription's latest invoice (and the latest is `void`), so it
/// does not convert. Nor does a $0 invoice.paid naming the void invoice.
#[tokio::test]
async fn a_zero_dollar_invoice_after_a_void_flipped_the_subscription_active_does_not_convert() {
    const NAME: &str = "a_zero_dollar_invoice_after_a_void_flipped_the_subscription_active_does_not_convert";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_in_dunning(&p, &price).await;
    mock.invoice(|i| i.status = "void".into());
    mock.set_status("active");

    let other_event = evt();
    let other = invoice_paid(
        &p.app, &stripe, org.account, &other_event,
        &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 0),
    )
    .await;
    let after_other = trial(&p.app, org.account).await;
    let latest_event = evt();
    let mut latest = invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 0);
    latest["id"] = json!("in_conversion_mock");
    let on_latest = invoice_paid(&p.app, &stripe, org.account, &latest_event, &latest).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let other_outcome = event_outcome(&p.owner, &other_event).await.unwrap_or_default();
    let latest_outcome = event_outcome(&p.owner, &latest_event).await.unwrap_or_default();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((other, on_latest), (StatusCode::OK, StatusCode::OK));
    assert_eq!(after_other.state, TrialState::Converting, "`active` alone is not a payment");
    assert_eq!(row.state, TrialState::Converting, "a void latest invoice is not a payment");
    assert!(row.first_charge_at.is_none());
    assert!(paid.is_empty(), "no paid allowance: {paid:?}");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))], "the trial override stays");
    assert!(other_outcome.starts_with("trial_zero_invoice"), "{other_outcome}");
    assert!(latest_outcome.starts_with("trial_zero_invoice"), "{latest_outcome}");
}

/// #1528 review, fix 2: a resubscribe whose subscription is `active` but
/// took no money (a 100%-off promotion code, or a customer balance that
/// zeroed the invoice) is not a first charge, and extra usage stays locked.
#[tokio::test]
async fn a_zero_amount_resubscribe_does_not_unlock_extra_usage() {
    const NAME: &str = "a_zero_amount_resubscribe_does_not_unlock_extra_usage";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET status = 'canceled' WHERE account_id = $1").bind(org.account),
    )
    .await;
    lifecycle::process_account(&p.app, Some(stripe.as_ref()), org.account, ts(TRIAL_START + 86_400)).await.unwrap();
    let canceled = trial(&p.app, org.account).await;

    mock.set_status("active");
    mock.invoice(|i| {
        i.status = "paid".into();
        i.amount_paid = 0;
    });
    let resub = format!("sub_resub_{}", Uuid::new_v4().simple());
    let event = evt();
    let resubscribed = complete_sub(&state, &p.app, &org, &mock, &event, &resub).await;
    let refused = extra_refused(&p.app, org.account).await;
    let row = trial(&p.app, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    let invoice_reads = mock.calls("GET", "/v1/invoices/in_conversion_mock").len();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!(resubscribed, StatusCode::OK);
    assert!(outcome.contains("resubscribed plan=team"), "{outcome}");
    assert!(invoice_reads >= 1, "the latest invoice was read");
    assert!(row.first_charge_at.is_none(), "nothing was collected");
    assert!(refused, "extra usage stays locked until money is taken");
    assert!(!outcome.contains("first_charge=stamped"), "{outcome}");
}

/// #1528 review, fix 3: the owner cancels while the trial is `converting`
/// and Stripe still shows it `trialing`; Stripe ends the trial itself (day
/// 90) between the cancel path's first read and its re-read. The re-read
/// subscription is `active` and its conversion invoice is still a `draft`.
/// The re-read's own invoice decides: pending, so nothing is finalized and
/// nothing is canceled this pass.
#[tokio::test]
async fn a_trial_ended_by_stripe_between_the_cancel_reads_is_decided_on_the_re_read_invoice() {
    const NAME: &str = "a_trial_ended_by_stripe_between_the_cancel_reads_is_decided_on_the_re_read_invoice";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_untouched(&p, &price).await;
    mock.set_cancel_scheduled(true);
    mock.invoice(|i| i.status = "draft".into());
    mock.flip_status_after_next_get("active");

    let mut report = lifecycle::PassReport::default();
    let pass = lifecycle::drive_conversion(&p.app, Some(stripe.as_ref()), org.account, &mut report).await;
    let row = trial(&p.app, org.account).await;
    let paid = periods_at(&p.owner, org.account, PAID_START).await;
    let grants = team_grants(&p.owner, org.account).await;
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let invoice_reads = mock.calls("GET", "/v1/invoices/in_conversion_mock").len();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let err = pass.expect_err("a draft conversion invoice is pending: no decision this pass");
    assert!(err.contains("draft"), "{err}");
    assert_eq!(invoice_reads, 1, "the re-read subscription's conversion invoice is read");
    assert!(!report.actions.contains(&"converted"), "{:?}", report.actions);
    assert_eq!(row.state, TrialState::Converting);
    assert!(paid.is_empty(), "no paid allowance on a draft invoice: {paid:?}");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))], "the trial override stays");
    assert!(deletes.is_empty(), "nothing is canceled while the invoice is pending");
}

/// #1528 review, fix 4: Stripe holds the conversion invoice as a `draft`
/// for about an hour after the trial ends. Nothing has failed: the worker
/// waits without stamping `payment_failed_at` (billing would show "payment
/// failed").
#[tokio::test]
async fn a_draft_conversion_invoice_waits_without_marking_payment_failed() {
    const NAME: &str = "a_draft_conversion_invoice_waits_without_marking_payment_failed";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_untouched(&p, &price).await;
    mock.set_status("active");
    mock.invoice(|i| i.status = "draft".into());

    let mut report = lifecycle::PassReport::default();
    let pass = lifecycle::drive_conversion(&p.app, Some(stripe.as_ref()), org.account, &mut report).await;
    let failed_at_draft = payment_failed_at(&p.owner, org.account).await;
    let row = trial(&p.app, org.account).await;
    // Control: the same invoice once finalized and unpaid (`open`) does mark it.
    mock.invoice(|i| i.status = "open".into());
    let mut open_report = lifecycle::PassReport::default();
    lifecycle::drive_conversion(&p.app, Some(stripe.as_ref()), org.account, &mut open_report).await.expect("open pass");
    let failed_at_open = payment_failed_at(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    pass.expect("draft pass");
    assert!(report.actions.contains(&"awaiting_invoice"), "{:?}", report.actions);
    assert!(!report.actions.contains(&"awaiting_payment"), "{:?}", report.actions);
    assert!(failed_at_draft.is_none(), "a draft invoice has not failed");
    assert_eq!(row.state, TrialState::Converting);
    assert!(open_report.actions.contains(&"awaiting_payment"), "{:?}", open_report.actions);
    assert!(failed_at_open.is_some(), "an open (failed) invoice is a payment failure");
}

// ── Clerk organization.deleted (#1396) ───────────────────────────────────
//
// Driven through the webhook arm itself
// (`routes::clerk_webhook::handle_org_deleted`) as `coralswarm_app`, each
// delivered twice: Svix redelivers, and a 5xx retry replays every step.

fn clerk_org_of(org: &Org) -> String {
    format!("org_{}", org.account.simple())
}

async fn deliver_org_deleted(state: &Arc<crate::AppState>, app: &PgPool, org: &Org) {
    crate::routes::clerk_webhook::handle_org_deleted(state, app, &json!({"id": clerk_org_of(org), "deleted": true}))
        .await
        .expect("organization.deleted handled");
}

/// An active routing row for the org, as the provisioner leaves it.
async fn seed_active_tenant(owner: &PgPool, org: &Org) {
    fin(
        owner,
        org.account,
        sqlx::query(
            "INSERT INTO tenant_databases (clerk_org_id, account_id, slug, db_secret_ref, status, schema_version) \
             VALUES ($1, $2, $3, $4, 'active', 100000)",
        )
        .bind(clerk_org_of(org))
        .bind(org.account)
        .bind(format!("del-{}", &org.account.simple().to_string()[..12]))
        .bind(format!("ORG_DELETED_TEST_DSN_{}", org.account.simple())),
    )
    .await;
}

async fn active_orgs(app: &PgPool) -> Vec<String> {
    crate::tenancy::routing::list_active(app)
        .await
        .expect("list active tenants")
        .into_iter()
        .map(|t| t.clerk_org_id)
        .collect()
}

async fn trial_end_reason(owner: &PgPool, account: Uuid) -> Option<String> {
    sqlx::query_scalar("SELECT end_reason FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_one(owner)
        .await
        .expect("read end_reason")
}

/// What `organization.deleted` writes keyed on the Clerk org id (no FK to
/// `accounts`, so the account cleanup does not reach it).
async fn cleanup_org_deleted(owner: &PgPool, org: &Org) {
    let clerk = clerk_org_of(org);
    fin(owner, org.account, sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id = $1").bind(&clerk)).await;
    for table in ["org_revocations", "org_membership_tombstones", "member_ocean_desired_state"] {
        sqlx::query(&format!("DELETE FROM {table} WHERE clerk_org_id = $1"))
            .bind(&clerk)
            .execute(owner)
            .await
            .expect("cleanup org deletion rows");
    }
}

/// #1396: deleting a trialing org ends the trial through `org_trial`'s end
/// path and cancels the subscription at Stripe exactly once, across a
/// redelivery of the same event.
#[tokio::test]
async fn org_deleted_cancels_the_subscription_once_and_ends_the_trial_across_redelivery() {
    const NAME: &str = "org_deleted_cancels_the_subscription_once_and_ends_the_trial_across_redelivery";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    let before = trial(&p.app, org.account).await.state;

    deliver_org_deleted(&state, &p.app, &org).await;
    deliver_org_deleted(&state, &p.app, &org).await;

    let row = trial(&p.app, org.account).await;
    let reason = trial_end_reason(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let posts = mock.calls("POST", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(before, TrialState::Trialing);
    assert_eq!(row.state, TrialState::Canceled, "the trial ends through org_trial::db::end");
    assert_eq!(reason.as_deref(), Some("organization deleted in Clerk"));
    assert_eq!(grants, vec![(None, Some(2))], "db::end cleared the trial's 0-credit override");
    assert_eq!(balance, Some(Decimal::ZERO), "db::end expired the pool");
    assert_eq!(deletes.len(), 1, "one Stripe cancel across two deliveries");
    assert_eq!(
        deletes[0].idem.as_deref(),
        Some(crate::billing::cancel::deleted_org_cancel_key(&mock.cfg.sub_id).as_str()),
        "keyed on the subscription id"
    );
    assert!(posts.is_empty(), "the trial was never ended at Stripe, so nothing was charged");
}

/// #1396 / #1533: a subscription already cancelled at Stripe (the A2 sweep
/// cancels before it deletes the Clerk org; our mirror has not caught up)
/// is a no-op. The trial still ends.
#[tokio::test]
async fn org_deleted_with_an_already_cancelled_subscription_makes_no_stripe_write() {
    const NAME: &str = "org_deleted_with_an_already_cancelled_subscription_makes_no_stripe_write";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    mock.set_status("canceled");

    deliver_org_deleted(&state, &p.app, &org).await;
    deliver_org_deleted(&state, &p.app, &org).await;

    let row = trial(&p.app, org.account).await;
    let deletes = mock.calls("DELETE", "/v1/subscriptions/");
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert!(deletes.is_empty(), "an already-cancelled subscription is never written to: {deletes:?}");
    assert_eq!(row.state, TrialState::Canceled);
}

/// #1396: once the org is deleted, the tenant harness's discovery read
/// (`routing::list_active`) no longer returns it, so every tenant-loop
/// worker stops scheduling it. Other tenants are untouched, and the
/// provisioner-only `tenant_databases.status` is left alone.
#[tokio::test]
async fn org_deleted_stops_the_tenant_workers_picking_the_org() {
    const NAME: &str = "org_deleted_stops_the_tenant_workers_picking_the_org";
    let Some(p) = pools(NAME).await else { return };
    let org = seed_org(&p.owner, None, 1).await;
    let other = seed_org(&p.owner, None, 1).await;
    seed_active_tenant(&p.owner, &org).await;
    seed_active_tenant(&p.owner, &other).await;
    let (stripe, _mock) = spawn_stripe(cfg("price_unused", 1, &fingerprint()), "active").await;
    let state = app_state(&p.app, stripe);
    let before = active_orgs(&p.app).await;

    deliver_org_deleted(&state, &p.app, &org).await;
    deliver_org_deleted(&state, &p.app, &org).await;

    let after = active_orgs(&p.app).await;
    let status: String = sqlx::query_scalar("SELECT status FROM tenant_databases WHERE clerk_org_id = $1")
        .bind(clerk_org_of(&org))
        .fetch_one(&p.owner)
        .await
        .unwrap();
    for o in [&org, &other] {
        cleanup_org_deleted(&p.owner, o).await;
        cleanup(&p.owner, o).await;
        cleanup_users(&p.owner, &o.users).await;
    }

    assert!(before.contains(&clerk_org_of(&org)) && before.contains(&clerk_org_of(&other)), "{before:?}");
    assert!(!after.contains(&clerk_org_of(&org)), "the deleted org is still scheduled: {after:?}");
    assert!(after.contains(&clerk_org_of(&other)), "another org's tenant must keep running: {after:?}");
    assert_eq!(status, "active", "routing status is provisioner-only and not written here");
}

/// #1396: an org deleted while its trial Checkout is still open ends as
/// `abandoned` (the only end `checkout_open` allows) through the same
/// `db::end` path, and the Checkout session is expired so it cannot be
/// completed later.
#[tokio::test]
async fn org_deleted_with_an_open_checkout_abandons_the_trial_and_expires_the_session() {
    const NAME: &str = "org_deleted_with_an_open_checkout_abandons_the_trial_and_expires_the_session";
    let Some(p) = pools(NAME).await else { return };
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg("price_unused", 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe);

    deliver_org_deleted(&state, &p.app, &org).await;
    deliver_org_deleted(&state, &p.app, &org).await;

    let row = trial(&p.app, org.account).await;
    let expires = mock.calls("POST", "/v1/checkout/sessions/cs_seed/expire");
    let deletes = mock.calls("DELETE", "/v1/subscriptions/");
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;

    assert_eq!(row.state, TrialState::Abandoned);
    assert_eq!(expires.len(), 1, "the open Checkout is expired once");
    assert!(deletes.is_empty(), "no subscription exists yet");
}

// ── organization.deleted: the #1396 review blockers ──────────────────────

/// Deliver `organization.deleted` through the real route
/// (`routes::clerk_webhook::clerk_webhook`), Svix-signed, and return its
/// status. `CLERK_WEBHOOK_SECRET` is set for the call only (these tests run
/// with `--test-threads=1`).
async fn deliver_org_deleted_http(state: &Arc<crate::AppState>, clerk_org_id: &str) -> StatusCode {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};
    struct Unset;
    impl Drop for Unset {
        fn drop(&mut self) {
            // SAFETY: single-threaded test binary run (`--test-threads=1`).
            unsafe { std::env::remove_var("CLERK_WEBHOOK_SECRET") };
        }
    }
    let secret_b64 = STANDARD.encode(b"org-deleted-1396-review-test-secret");
    let body = json!({"type": "organization.deleted", "data": {"id": clerk_org_id, "deleted": true}}).to_string();
    let (id, ts) = (format!("msg_{}", Uuid::new_v4().simple()), Utc::now().timestamp().to_string());
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&STANDARD.decode(&secret_b64).unwrap()).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    let sig = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("svix-id", id.parse().unwrap());
    headers.insert("svix-timestamp", ts.parse().unwrap());
    headers.insert("svix-signature", sig.parse().unwrap());
    // SAFETY: see `Unset`.
    unsafe { std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{secret_b64}")) };
    let _unset = Unset;
    let (status, _) =
        crate::routes::clerk_webhook::clerk_webhook(axum::extract::State(state.clone()), headers, axum::body::Bytes::from(body))
            .await;
    status
}

async fn count_by_org(owner: &PgPool, table: &str, clerk_org_id: &str) -> i64 {
    sqlx::query_scalar(&format!("SELECT COUNT(*) FROM {table} WHERE clerk_org_id = $1"))
        .bind(clerk_org_id)
        .fetch_one(owner)
        .await
        .expect("count rows")
}

/// Review blocker 2: a converting trial whose conversion invoice is still
/// open. The deletion voids the invoice BEFORE it cancels the subscription,
/// and a conversion payment that arrives afterwards anyway (`invoice.paid`,
/// the late-paid edge) does not revive the org: no conversion, no
/// allowance, no mirror change, no tenant database queued.
#[tokio::test]
async fn org_deleted_voids_the_open_conversion_invoice_and_a_late_payment_does_not_revive_the_org() {
    const NAME: &str = "org_deleted_voids_the_open_conversion_invoice_and_a_late_payment_does_not_revive_the_org";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let (org, stripe, mock) = converting_in_dunning(&p, &price).await;
    let state = app_state(&p.app, stripe.clone());

    deliver_org_deleted(&state, &p.app, &org).await;
    deliver_org_deleted(&state, &p.app, &org).await;
    let canceled = trial(&p.app, org.account).await;
    let seen: Vec<(String, String)> = mock.seen.lock().unwrap().iter().map(|c| (c.method.clone(), c.path.clone())).collect();
    let sub_path = format!("/v1/subscriptions/{}", mock.cfg.sub_id);
    let void_at = seen.iter().position(|(m, p)| m == "POST" && p == "/v1/invoices/in_conversion_mock/void");
    let delete_at = seen.iter().position(|(m, p)| m == "DELETE" && p == &sub_path);
    let voids = mock.calls("POST", "/v1/invoices/in_conversion_mock/void");
    let grants_before = team_grants(&p.owner, org.account).await;
    let mirror_before = mirror_full(&p.owner, org.account).await;
    let periods_before = all_periods(&p.owner, org.account).await;

    let event = evt();
    let status = {
        let _flag = SignupFlagOn::set();
        invoice_paid(
            &p.app, &stripe, org.account, &event,
            &invoice_json(&mock.cfg.sub_id, "subscription_update", PAID_START, PAID_END, 4000),
        )
        .await
    };
    let row = trial(&p.app, org.account).await;
    let jobs = provisioning_jobs(&p.owner, org.account).await;
    let grants_after = team_grants(&p.owner, org.account).await;
    let mirror_after = mirror_full(&p.owner, org.account).await;
    let periods_after = all_periods(&p.owner, org.account).await;
    let outcome = event_outcome(&p.owner, &event).await.unwrap_or_default();
    sqlx::query("DELETE FROM tenant_provisioning_jobs WHERE account_id = $1").bind(org.account).execute(&p.owner).await.unwrap();
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(canceled.state, TrialState::Canceled);
    assert_eq!(voids.len(), 1, "the open conversion invoice is voided once: {seen:?}");
    assert_eq!(
        voids[0].idem.as_deref(),
        Some(crate::billing::cancel::deleted_org_void_key("in_conversion_mock").as_str())
    );
    assert!(void_at.is_some() && delete_at.is_some() && void_at < delete_at, "void before the cancel: {seen:?}");
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row.state, TrialState::Canceled, "a late payment does not convert a deleted org");
    assert!(outcome.starts_with("trial_org_deleted refund_required"), "{outcome}");
    assert_eq!(jobs, 0, "no tenant database is queued for a deleted org");
    assert_eq!(grants_after, grants_before, "no allowance granted");
    assert_eq!(mirror_after, mirror_before, "the mirror is not revived");
    assert_eq!(periods_after, periods_before, "no paid period rolled");
}

/// Review blocker 3, first half: a trial Checkout that cannot be expired and
/// is still `open` is a 5xx, and the row stays `checkout_open` so Clerk's
/// retry tries again. Once Stripe reports it expired, the retry ends it.
#[tokio::test]
async fn org_deleted_with_a_checkout_that_will_not_expire_is_retried_until_it_is_closed() {
    const NAME: &str = "org_deleted_with_a_checkout_that_will_not_expire_is_retried_until_it_is_closed";
    let Some(p) = pools(NAME).await else { return };
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg("price_unused", 2, &fingerprint()), "trialing").await;
    mock.checkout(|c| c.expire_fails = true);
    let state = app_state(&p.app, stripe);

    let first = crate::routes::clerk_webhook::handle_org_deleted(&state, &p.app, &json!({"id": clerk_org_of(&org)})).await;
    let after_first = trial(&p.app, org.account).await.state;
    mock.checkout(|c| c.status = "expired".into());
    let second = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let after_second = trial(&p.app, org.account).await.state;
    let expires = mock.calls("POST", "/v1/checkout/sessions/cs_seed/expire").len();
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;

    assert!(first.is_err(), "an open Checkout that would not expire must fail the event so Clerk retries");
    assert_eq!(after_first, TrialState::CheckoutOpen, "the row is not ended while the session can still complete");
    assert_eq!(second, StatusCode::OK);
    assert_eq!(after_second, TrialState::Abandoned);
    assert_eq!(expires, 2, "the retry tries the expire again");
}

/// Deliver one Stripe event through the real route
/// (`routes::stripe_webhook::stripe_webhook`), signed, and return its status.
/// `STRIPE_WEBHOOK_SECRET` is set for the call only (`--test-threads=1`).
async fn deliver_stripe_event(state: &Arc<crate::AppState>, event_type: &str, object: Value) -> StatusCode {
    use hmac::{Hmac, Mac};
    struct Unset;
    impl Drop for Unset {
        fn drop(&mut self) {
            // SAFETY: single-threaded test binary run (`--test-threads=1`).
            unsafe { std::env::remove_var("STRIPE_WEBHOOK_SECRET") };
        }
    }
    const SECRET: &str = "whsec_org_deleted_1396_review";
    let body = json!({"id": evt(), "object": "event", "type": event_type, "data": {"object": object}}).to_string();
    let ts = Utc::now().timestamp();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(format!("{ts}.{body}").as_bytes());
    let mut headers = axum::http::HeaderMap::new();
    headers.insert("stripe-signature", format!("t={ts},v1={}", hex::encode(mac.finalize().into_bytes())).parse().unwrap());
    // SAFETY: see `Unset`.
    unsafe { std::env::set_var("STRIPE_WEBHOOK_SECRET", SECRET) };
    let _unset = Unset;
    crate::routes::stripe_webhook::stripe_webhook(axum::extract::State(state.clone()), headers, axum::body::Bytes::from(body))
        .await
        .into_response()
        .status()
}

/// Review blocker 3, second half: the Checkout completes after the org was
/// deleted (the expire lost the race), so `organization.deleted` found no
/// subscription to cancel. The real `checkout.session.completed` route then
/// refuses it (`refuse_for_deleting_org`, #1543, keyed on the
/// `org_revocations` row): the new subscription is ended at Stripe with the
/// shared deleted-org key, and nothing is activated.
#[tokio::test]
async fn a_checkout_completed_after_the_org_was_deleted_is_cancelled_not_activated() {
    const NAME: &str = "a_checkout_completed_after_the_org_was_deleted_is_cancelled_not_activated";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    mock.checkout(|c| {
        c.expire_fails = true;
        c.status = "complete".into();
    });
    let state = app_state(&p.app, stripe.clone());
    let session = json!({
        "id": "cs_seed", "object": "checkout.session", "mode": "subscription",
        "customer": format!("cus_trial_{}", org.account.simple()),
        "subscription": mock.cfg.sub_id, "payment_intent": null,
    });

    let deleted = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let completed = deliver_stripe_event(&state, "checkout.session.completed", session.clone()).await;
    let replay = deliver_stripe_event(&state, "checkout.session.completed", session).await;
    let row = trial(&p.app, org.account).await;
    let deletes = mock.calls("DELETE", &format!("/v1/subscriptions/{}", mock.cfg.sub_id));
    let linked = mirror(&p.owner, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    let key = crate::billing::cancel::deleted_org_cancel_key(&mock.cfg.sub_id);
    assert_eq!(deleted, StatusCode::OK, "a completed session is closed; its subscription is the completion's to end");
    assert_eq!((completed, replay), (StatusCode::OK, StatusCode::OK));
    assert!(!deletes.is_empty(), "the new subscription is ended at Stripe");
    assert!(deletes.iter().all(|d| d.idem.as_deref() == Some(key.as_str())), "one shared key, so a replay is a no-op: {deletes:?}");
    assert!(deletes[0].body.contains("prorate=false"), "{}", deletes[0].body);
    assert_eq!(row.state, TrialState::Abandoned, "the trial is not started for a deleted org");
    assert_eq!(linked, (None, "pending".to_string()), "the mirror is not linked to the subscription");
    assert!(balance.is_none() || balance == Some(Decimal::ZERO), "no trial pool: {balance:?}");
    assert!(grants.is_empty(), "no team grant: {grants:?}");
}

/// coralswarm-84, item 4: the self-serve cleanup sweep (#1533) cancels at
/// Stripe, deletes the Clerk org, then deletes the `accounts` and
/// `account_members` rows. `organization.deleted` then arrives for an org
/// with no `accounts` row: 200 every time, no Stripe call, and nothing
/// written but the org-level `org_revocations` row (TEXT key, no FK).
#[tokio::test]
async fn org_deleted_with_no_accounts_row_answers_200_and_writes_nothing_else() {
    const NAME: &str = "org_deleted_with_no_accounts_row_answers_200_and_writes_nothing_else";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, mock) = spawn_stripe(cfg("price_unused", 1, &fingerprint()), "active").await;
    let state = app_state(&p.app, stripe);
    let clerk = format!("org_gone_{}", Uuid::new_v4().simple());

    let first = deliver_org_deleted_http(&state, &clerk).await;
    let second = deliver_org_deleted_http(&state, &clerk).await;
    let revocations = count_by_org(&p.owner, "org_revocations", &clerk).await;
    let tombstones = count_by_org(&p.owner, "org_membership_tombstones", &clerk).await;
    let intents = count_by_org(&p.owner, "member_ocean_desired_state", &clerk).await;
    let accounts = count_by_org(&p.owner, "accounts", &clerk).await;
    let calls = mock.seen.lock().unwrap().len();
    sqlx::query("DELETE FROM org_revocations WHERE clerk_org_id = $1").bind(&clerk).execute(&p.owner).await.unwrap();

    assert_eq!((first, second), (StatusCode::OK, StatusCode::OK));
    assert_eq!(calls, 0, "no Stripe call for an org with no account");
    assert_eq!(revocations, 1, "only the org-level revocation is written");
    assert_eq!((tombstones, intents, accounts), (0, 0, 0), "nothing else is written");
}

/// coralswarm-84, item 5: in-app org deletion cancels Stripe itself and
/// deletes the routing row before it deletes the Clerk org. A subscription
/// already `canceled` in the mirror, with no `tenant_databases` row, is a
/// 200 no-op with no Stripe call at all.
#[tokio::test]
async fn org_deleted_after_in_app_deletion_cancelled_and_dropped_the_routing_row_is_a_noop() {
    const NAME: &str = "org_deleted_after_in_app_deletion_cancelled_and_dropped_the_routing_row_is_a_noop";
    let Some(p) = pools(NAME).await else { return };
    let org = seed_org(&p.owner, None, 1).await;
    let sub = format!("sub_gone_{}", Uuid::new_v4().simple());
    fin(
        &p.owner,
        org.account,
        sqlx::query("UPDATE stripe_subscriptions SET stripe_subscription_id = $2, status = 'canceled' WHERE account_id = $1")
            .bind(org.account)
            .bind(&sub),
    )
    .await;
    let (stripe, mock) = spawn_stripe(cfg("price_unused", 1, &fingerprint()), "canceled").await;
    let state = app_state(&p.app, stripe);

    let first = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let second = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let routing = count_by_org(&p.owner, "tenant_databases", &clerk_org_of(&org)).await;
    let calls = mock.seen.lock().unwrap().len();
    let linked = mirror(&p.owner, org.account).await;
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;

    assert_eq!((first, second), (StatusCode::OK, StatusCode::OK));
    assert_eq!(routing, 0, "fixture: no routing row");
    assert_eq!(calls, 0, "an already-cancelled subscription needs no Stripe call");
    assert_eq!(linked, (Some(sub), "canceled".to_string()));
}

/// coralswarm-84, item 5: while in-app org deletion owns the org (a live
/// `org_deletions` row, #1543), `organization.deleted` leaves the trial and
/// the subscription to that flow: 200, no Stripe call, the trial untouched.
/// `org_deletions` is not on main yet; when it is absent this test creates a
/// stand-in with the same columns and definer, and drops it afterwards.
#[tokio::test]
async fn org_deleted_leaves_billing_to_a_live_in_app_deletion() {
    const NAME: &str = "org_deleted_leaves_billing_to_a_live_in_app_deletion";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let stand_in: bool = sqlx::query_scalar("SELECT to_regclass('public.org_deletions') IS NULL")
        .fetch_one(&p.owner)
        .await
        .unwrap();
    if stand_in {
        for ddl in [
            "CREATE TABLE public.org_deletions (account_id UUID NOT NULL, clerk_org_id TEXT NOT NULL, \
             initiated_by TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'requested')",
            "CREATE FUNCTION public.org_deletion_state_for_org(p_clerk_org_id TEXT) RETURNS TEXT \
             LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public, pg_temp AS \
             $$ SELECT state FROM public.org_deletions WHERE clerk_org_id = p_clerk_org_id AND state <> 'canceled' $$",
        ] {
            sqlx::query(ddl).execute(&p.owner).await.expect("org_deletions stand-in");
        }
    }
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    let calls_before = mock.seen.lock().unwrap().len();
    sqlx::query("INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by, state) VALUES ($1, $2, 'owner', 'grace')")
        .bind(org.account)
        .bind(clerk_org_of(&org))
        .execute(&p.owner)
        .await
        .expect("org deletion row");

    let status = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let calls_after = mock.seen.lock().unwrap().len();
    let row = trial(&p.app, org.account).await;
    let revoked = count_by_org(&p.owner, "org_revocations", &clerk_org_of(&org)).await;
    sqlx::query("DELETE FROM org_deletions WHERE account_id = $1").bind(org.account).execute(&p.owner).await.unwrap();
    if stand_in {
        sqlx::query("DROP FUNCTION public.org_deletion_state_for_org(TEXT)").execute(&p.owner).await.unwrap();
        sqlx::query("DROP TABLE public.org_deletions").execute(&p.owner).await.unwrap();
    }
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(calls_after, calls_before, "in-app deletion owns Stripe: no call from this webhook");
    assert_eq!(row.state, TrialState::Trialing, "the trial is left to the in-app deletion");
    assert_eq!(revoked, 1, "access is still revoked");
}

/// Review item 6: after `adopt_personal_org_account` a person's own account
/// carries their personal org's Clerk id. Deleting that Clerk org must not
/// end the person's subscription or trial.
#[tokio::test]
async fn org_deleted_for_a_personal_org_leaves_the_subscription_alone() {
    const NAME: &str = "org_deleted_for_a_personal_org_leaves_the_subscription_alone";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    complete(&state, &p.app, &org, &mock, &evt()).await;
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(clerk_org_of(&org))
        .bind(format!("user_personal_{}", Uuid::new_v4().simple()))
        .execute(&p.owner)
        .await
        .expect("personal org row");

    let status = deliver_org_deleted_http(&state, &clerk_org_of(&org)).await;
    let deletes = mock.calls("DELETE", "/v1/subscriptions/");
    let row = trial(&p.app, org.account).await;
    let refused = crate::billing::cancel::org_deleted_in_clerk(&p.app, org.account).await.unwrap();
    sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1").bind(clerk_org_of(&org)).execute(&p.owner).await.unwrap();
    cleanup_org_deleted(&p.owner, &org).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK);
    assert!(deletes.is_empty(), "a personal subscription is never cancelled: {deletes:?}");
    assert_eq!(row.state, TrialState::Trialing);
    assert!(!refused, "a personal org is not treated as a deleted org by billing");
}

// ── The $0 welcome-offer line on the trial Checkout ──────────────────────

/// The expanded session lines as Stripe returned them for a sandbox trial
/// Checkout with the welcome-offer line: the seat line is recurring, the
/// welcome line is one_time at $0 and is NOT a subscription item.
fn welcome_offer_session_lines(price: &str, seats: i64) -> Vec<Value> {
    vec![
        json!({"quantity": seats, "amount_total": 0, "price": {"id": price, "type": "recurring", "unit_amount": 4000}}),
        json!({"quantity": 1, "amount_total": 0,
               "description": "Welcome offer: 3,000 free credits",
               "price": {"id": "price_welcome_adhoc", "type": "one_time", "unit_amount": 0}}),
    ]
}

fn trial_session_json(org: &Org, mock: &StripeMock) -> Value {
    json!({
        "id": "cs_seed", "object": "checkout.session", "mode": "subscription",
        "customer": format!("cus_trial_{}", org.account.simple()),
        "subscription": mock.cfg.sub_id, "payment_intent": null,
    })
}

/// The whole production route (signed `checkout.session.completed` -> session
/// re-fetch -> dispatcher), with the welcome-offer line on the session. The
/// trial still starts with the bought seats, one flat 3,000 pool, a
/// zero-credit team grant, and the mirror linked. A redelivery adds nothing.
#[tokio::test]
async fn a_trial_checkout_with_the_welcome_offer_line_still_yields_seats_trial_and_mirror() {
    const NAME: &str = "a_trial_checkout_with_the_welcome_offer_line_still_yields_seats_trial_and_mirror";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 3).await;
    open_trial(&p.app, &org, 3, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 3, &fingerprint()), "trialing").await;
    mock.checkout(|c| {
        c.status = "complete".into();
        c.line_items = welcome_offer_session_lines(&price, 3);
    });
    let state = app_state(&p.app, stripe.clone());
    let session = trial_session_json(&org, &mock);

    let first = deliver_stripe_event(&state, "checkout.session.completed", session.clone()).await;
    let replay = deliver_stripe_event(&state, "checkout.session.completed", session).await;
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    let linked = mirror(&p.owner, org.account).await;
    let unprocessable: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM stripe_webhook_events WHERE account_id = $1 AND outcome LIKE 'unprocessable%'",
    )
    .bind(org.account)
    .fetch_one(&p.owner)
    .await
    .unwrap();
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((first, replay), (StatusCode::OK, StatusCode::OK));
    assert_eq!(unprocessable, 0, "the $0 welcome line must not make the session unpriceable");
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(row.trial_ends_at, Some(ts(TRIAL_END)));
    assert_eq!(balance, Some(d(3000)), "one flat pool, granted once");
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(3))], "3 seats, not 4 and not 1");
    assert_eq!(seats, 3, "the welcome line (quantity 1) is not a seat");
    assert_eq!(linked.0.as_deref(), Some(mock.cfg.sub_id.as_str()));
}

/// Only FREE one-time lines are skipped: a PAID extra line on a subscription
/// session is still refused as unpriceable, with nothing granted.
#[tokio::test]
async fn a_paid_extra_line_on_a_trial_session_is_still_refused() {
    const NAME: &str = "a_paid_extra_line_on_a_trial_session_is_still_refused";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    mock.checkout(|c| {
        c.status = "complete".into();
        let mut lines = welcome_offer_session_lines(&price, 2);
        lines[1] = json!({"quantity": 1, "amount_total": 500, "price": {"id": "price_fee", "type": "one_time", "unit_amount": 500}});
        c.line_items = lines;
    });
    let state = app_state(&p.app, stripe.clone());
    let session = trial_session_json(&org, &mock);

    let status = deliver_stripe_event(&state, "checkout.session.completed", session).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(status, StatusCode::OK, "accept-and-record");
    assert!(balance.is_none() || balance == Some(Decimal::ZERO), "{balance:?}");
    assert!(grants.is_empty(), "{grants:?}");
}

/// The first invoice of a welcome-offer trial carries the $0 welcome line
/// FIRST, then the trial seat line, both with the trial's own period (the
/// shape Stripe returned in the sandbox). After the completion it rolls no
/// allowance period, grants nothing, and leaves the trial alone.
#[tokio::test]
async fn the_first_invoice_with_a_welcome_line_is_inert_after_completion() {
    const NAME: &str = "the_first_invoice_with_a_welcome_line_is_inert_after_completion";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let (price, previous) = price_standard(&p.owner).await;
    let org = seed_org(&p.owner, None, 2).await;
    open_trial(&p.app, &org, 2, true).await;
    let (stripe, mock) = spawn_stripe(cfg(&price, 2, &fingerprint()), "trialing").await;
    let state = app_state(&p.app, stripe.clone());
    let completed = complete(&state, &p.app, &org, &mock, &evt()).await;
    let periods_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
        .bind(org.account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let mut inv = invoice_json(&mock.cfg.sub_id, "subscription_create", TRIAL_START, TRIAL_START, 0);
    inv["lines"] = json!({"data": [
        {"description": "Welcome offer: 3,000 free credits", "quantity": 1, "amount": 0,
         "period": {"start": TRIAL_START, "end": TRIAL_END}},
        {"description": "Free trial for 2 x seat", "quantity": 2, "amount": 0,
         "period": {"start": TRIAL_START, "end": TRIAL_END}},
    ]});
    let paid = invoice_paid(&p.app, &stripe, org.account, &evt(), &inv).await;
    let periods_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM member_allowance_periods WHERE account_id = $1")
        .bind(org.account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let row = trial(&p.app, org.account).await;
    let balance = pool_balance(&p.owner, org.account).await;
    let grants = team_grants(&p.owner, org.account).await;
    let seats = ledger_seats(&p.owner, org.account).await;
    cleanup(&p.owner, &org).await;
    cleanup_users(&p.owner, &org.users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((completed, paid), (StatusCode::OK, StatusCode::OK));
    assert_eq!(periods_after, periods_before, "a $0 trial invoice rolls nothing");
    assert_eq!(row.state, TrialState::Trialing);
    assert_eq!(balance, Some(d(3000)));
    assert_eq!(grants, vec![(Some(Decimal::ZERO), Some(2))]);
    assert_eq!(seats, 2);
}
