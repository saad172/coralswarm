//! Live-Postgres tests for `PUT /v1/billing/extra-usage` and
//! `POST /v1/billing/extra-usage/topup`, with a local axum server standing in
//! for Stripe (this repo's mock-HTTP convention, see `routes::checkout`).
//!
//! Gated on `RELINFER_TEST_DATABASE_URL`; the CI step that runs this module
//! fails on a zero-test match or any `skipping ` line.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::extract::{Form, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use sqlx::{types::Decimal, PgPool};
use uuid::Uuid;

use super::*;
use crate::billing::extra_usage::{AutoReloadUpdate, UpdateRequest, MICRO_PER_USD as M, TOPUP_PURPOSE};

async fn pg(test: &str) -> Option<PgPool> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping {test}: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(16)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

// ── Stripe mock ────────────────────────────────────────────────────────────

#[derive(Clone, Debug)]
struct Seen {
    path: String,
    idempotency_key: Option<String>,
    form: HashMap<String, String>,
}

#[derive(Clone, Default)]
struct Calls(Arc<Mutex<Vec<Seen>>>);

impl Calls {
    fn sessions(&self) -> Vec<Seen> {
        self.0.lock().unwrap().iter().filter(|s| s.path == "/v1/checkout/sessions").cloned().collect()
    }
}

async fn mock_stripe(fail_sessions: bool) -> (crate::billing::stripe_client::StripeClient, Calls) {
    let calls = Calls::default();
    let c1 = calls.clone();
    let c2 = calls.clone();
    let app = axum::Router::new()
        .route(
            "/v1/customers",
            axum::routing::post(move |headers: HeaderMap, Form(form): Form<HashMap<String, String>>| {
                let calls = c1.clone();
                async move {
                    let acct = form.get("metadata[account_id]").cloned().unwrap_or_default();
                    calls.0.lock().unwrap().push(Seen {
                        path: "/v1/customers".into(),
                        idempotency_key: headers.get("idempotency-key").map(|v| v.to_str().unwrap().to_string()),
                        form,
                    });
                    (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        json!({"id": format!("cus_mock_{acct}")}).to_string(),
                    )
                        .into_response()
                }
            }),
        )
        .route(
            "/v1/checkout/sessions",
            axum::routing::post(move |headers: HeaderMap, Form(form): Form<HashMap<String, String>>| {
                let calls = c2.clone();
                async move {
                    calls.0.lock().unwrap().push(Seen {
                        path: "/v1/checkout/sessions".into(),
                        idempotency_key: headers.get("idempotency-key").map(|v| v.to_str().unwrap().to_string()),
                        form,
                    });
                    if fail_sessions {
                        return (
                            StatusCode::BAD_REQUEST,
                            [("content-type", "application/json")],
                            json!({"error": {"type": "invalid_request_error", "message": "boom"}}).to_string(),
                        )
                            .into_response();
                    }
                    (
                        StatusCode::OK,
                        [("content-type", "application/json")],
                        json!({"id": "cs_topup_mock", "url": "https://checkout.stripe.com/topup"}).to_string(),
                    )
                        .into_response()
                }
            }),
        );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    (
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}")),
        calls,
    )
}

fn state_with(pool: &PgPool, stripe: Option<crate::billing::stripe_client::StripeClient>) -> Arc<AppState> {
    let mut s = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
    s.stripe = stripe.map(Arc::new);
    Arc::new(s)
}

// ── Fixtures ───────────────────────────────────────────────────────────────

#[derive(Clone, Copy, PartialEq, Debug)]
enum Plan {
    None,
    Free,
    Pro,
    Team,
}

struct Acct {
    id: Uuid,
    /// The caller: owner for personal accounts, the requested role for orgs.
    user: Uuid,
}

fn ctx(a: &Acct) -> UserContext {
    UserContext { user_id: a.user, account_id: a.id, role: "owner".into(), device_id: "extra-usage-test".into() }
}

async fn fenced(pool: &PgPool, account: Uuid) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("enter");
    tx
}

async fn new_user(pool: &PgPool) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
        .bind(id)
        .bind(format!("{id}@extra-usage-routes.test"))
        .execute(pool)
        .await
        .expect("user");
    id
}

async fn grant(pool: &PgPool, account: Uuid, plan: Plan) {
    let code = match plan {
        Plan::None => return,
        Plan::Free => "free",
        Plan::Pro => "pro",
        Plan::Team => "team_standard",
    };
    let (plan_id, seat_priced): (Uuid, bool) = sqlx::query_as("SELECT id, is_seat_priced FROM plans WHERE code=$1")
        .bind(code)
        .fetch_one(pool)
        .await
        .expect("plan");
    let mut tx = fenced(pool, account).await;
    sqlx::query("INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1,$2,$3,$4)")
        .bind(account)
        .bind(plan_id)
        .bind(seat_priced.then_some(3))
        .bind(seat_priced)
        .execute(&mut *tx)
        .await
        .expect("grant");
    tx.commit().await.expect("commit");
}

/// A personal account owned by its only user.
async fn personal(pool: &PgPool, plan: Plan) -> Acct {
    let user = new_user(pool).await;
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
        .bind(id)
        .bind(user)
        .execute(pool)
        .await
        .expect("account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1,$2,'owner')")
        .bind(id)
        .bind(user)
        .execute(pool)
        .await
        .expect("member");
    grant(pool, id, plan).await;
    Acct { id, user }
}

/// An org account (owner + one more member at `role`); returns the member.
async fn org(pool: &PgPool, plan: Plan, role: &str) -> Acct {
    let owner = new_user(pool).await;
    let id = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1,$2,$3)")
        .bind(id)
        .bind(owner)
        .bind(format!("org_{}", id.simple()))
        .execute(pool)
        .await
        .expect("org account");
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code='standard' AND is_active")
        .fetch_one(pool)
        .await
        .expect("standard tier");
    let user = new_user(pool).await;
    for (u, r) in [(owner, "owner"), (user, role)] {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1,$2,$3,$4)")
            .bind(id)
            .bind(u)
            .bind(r)
            .bind(tier)
            .execute(pool)
            .await
            .expect("org member");
    }
    grant(pool, id, plan).await;
    Acct { id, user }
}

/// A personal org: an org-shaped account that is really one person's Pro account.
async fn personal_org(pool: &PgPool) -> Acct {
    let clerk_user = format!("user_{}", Uuid::new_v4().simple());
    let clerk_org = format!("org_{}", Uuid::new_v4().simple());
    let user: Uuid = sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1,$2) RETURNING id")
        .bind(format!("{}@extra-usage-routes.test", Uuid::new_v4()))
        .bind(&clerk_user)
        .fetch_one(pool)
        .await
        .expect("user");
    let id: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1,$2) RETURNING id")
        .bind(user)
        .bind(&clerk_org)
        .fetch_one(pool)
        .await
        .expect("account");
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1,$2)")
        .bind(&clerk_org)
        .bind(&clerk_user)
        .execute(pool)
        .await
        .expect("personal org");
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code='standard' AND is_active")
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1,$2,'owner',$3)")
        .bind(id)
        .bind(user)
        .bind(tier)
        .execute(pool)
        .await
        .expect("member");
    grant(pool, id, Plan::Pro).await;
    Acct { id, user }
}

async fn set_subscription(pool: &PgPool, a: &Acct, status: &str) {
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

async fn body(resp: Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("body");
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn enable() -> UpdateRequest {
    UpdateRequest { enabled: true, ..Default::default() }
}

async fn put(state: &Arc<AppState>, a: &Acct, req: UpdateRequest) -> (StatusCode, Value) {
    body(put_extra_usage(State(state.clone()), Extension(ctx(a)), Json(req)).await).await
}

async fn topup(state: &Arc<AppState>, a: &Acct, amount: i64) -> (StatusCode, Value) {
    topup_to(state, a, amount, "http://localhost:3000/ok").await
}

async fn topup_to(state: &Arc<AppState>, a: &Acct, amount: i64, success: &str) -> (StatusCode, Value) {
    body(
        create_topup(
            State(state.clone()),
            Extension(ctx(a)),
            Json(TopupRequest {
                amount_micro_usd: amount,
                success_url: success.into(),
                cancel_url: "http://localhost:3000/no".into(),
            }),
        )
        .await,
    )
    .await
}

#[derive(Debug, PartialEq)]
struct Row {
    enabled: bool,
    limit: Option<i64>,
    cap: Option<Decimal>,
    reload: bool,
    threshold: Option<i64>,
    amount: Option<i64>,
    updated_by: Option<Uuid>,
}

async fn row(pool: &PgPool, a: &Acct) -> Option<Row> {
    sqlx::query_as::<_, (bool, Option<i64>, Option<Decimal>, bool, Option<i64>, Option<i64>, Option<Uuid>)>(
        "SELECT enabled, monthly_limit_micro_usd, member_cap_seat_multiple, auto_reload_enabled, \
                auto_reload_threshold_micro_usd, auto_reload_amount_micro_usd, updated_by_user_id \
           FROM extra_usage_settings WHERE account_id=$1",
    )
    .bind(a.id)
    .fetch_optional(pool)
    .await
    .expect("settings")
    .map(|(enabled, limit, cap, reload, threshold, amount, updated_by)| Row {
        enabled,
        limit,
        cap,
        reload,
        threshold,
        amount,
        updated_by,
    })
}

fn reload(enabled: bool, threshold: Option<i64>, amount: Option<i64>) -> Option<AutoReloadUpdate> {
    Some(AutoReloadUpdate { enabled, threshold_micro_usd: threshold, amount_micro_usd: amount })
}

// ── PUT /v1/billing/extra-usage ────────────────────────────────────────────

#[tokio::test]
async fn put_is_refused_on_free_and_on_no_plan_with_a_clear_code() {
    let Some(pool) = pg("put_is_refused_on_free_and_on_no_plan_with_a_clear_code").await else { return };
    let state = state_with(&pool, None);
    for plan in [Plan::Free, Plan::None] {
        let a = personal(&pool, plan).await;
        let (status, b) = put(&state, &a, enable()).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{plan:?}");
        assert_eq!(b["code"], "plan_required", "{plan:?}");
        assert!(b["error"].as_str().is_some_and(|m| !m.is_empty()));
        assert_eq!(row(&pool, &a).await, None, "nothing is written for a refused request");
    }
}

#[tokio::test]
async fn put_is_admin_only_for_an_org() {
    let Some(pool) = pg("put_is_admin_only_for_an_org").await else { return };
    let state = state_with(&pool, None);
    let member = org(&pool, Plan::Team, "member").await;
    let (status, b) = put(&state, &member, enable()).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(b["code"], "admin_required");
    assert_eq!(row(&pool, &member).await, None);

    for role in ["admin", "owner"] {
        let who = org(&pool, Plan::Team, role).await;
        let (status, b) = put(&state, &who, enable()).await;
        assert_eq!(status, StatusCode::OK, "{role}: {b}");
    }
}

#[tokio::test]
async fn an_org_admin_enabling_for_the_first_time_gets_the_org_defaults() {
    let Some(pool) = pg("an_org_admin_enabling_for_the_first_time_gets_the_org_defaults").await else { return };
    let state = state_with(&pool, None);
    let admin = org(&pool, Plan::Team, "admin").await;
    let (status, b) = put(&state, &admin, enable()).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    assert_eq!(
        row(&pool, &admin).await,
        Some(Row {
            enabled: true,
            limit: Some(100 * M),
            cap: Some(Decimal::ONE),
            reload: false,
            threshold: None,
            amount: None,
            updated_by: Some(admin.user),
        })
    );
    let e = &b["extra_usage"];
    assert_eq!(e["enabled"], true);
    assert_eq!(e["monthly_limit_micro_usd"], 100 * M);
    assert_eq!(e["member_cap_seat_multiple"], 1.0);
    assert_eq!(e["balance_micro_usd"], 0);
    assert_eq!(e["auto_reload"]["enabled"], false);
    assert_eq!(b["limits"]["topup_min_micro_usd"], 5 * M);
    assert_eq!(b["limits"]["topup_max_micro_usd"], 1_000 * M);
}

#[tokio::test]
async fn a_pro_owner_enabling_gets_the_pro_default_and_no_member_cap() {
    let Some(pool) = pg("a_pro_owner_enabling_gets_the_pro_default_and_no_member_cap").await else { return };
    let state = state_with(&pool, None);
    let pro = personal(&pool, Plan::Pro).await;
    let (status, b) = put(&state, &pro, enable()).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    let r = row(&pool, &pro).await.unwrap();
    assert_eq!((r.enabled, r.limit, r.cap), (true, Some(50 * M), None));
    assert!(b["extra_usage"]["member_cap_seat_multiple"].is_null());

    // A personal org is one person's Pro account: same defaults, no cap.
    let po = personal_org(&pool).await;
    let (status, b) = put(&state, &po, enable()).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    let r = row(&pool, &po).await.unwrap();
    assert_eq!((r.limit, r.cap), (Some(50 * M), None));
    let (status, b) = put(&state, &po, UpdateRequest { member_cap_seat_multiple: Some(1.0), ..enable() }).await;
    assert_eq!((status, b["code"].as_str()), (StatusCode::BAD_REQUEST, Some("member_cap_not_applicable")));
}

#[tokio::test]
async fn invalid_requests_are_400_with_a_code_and_write_nothing() {
    let Some(pool) = pg("invalid_requests_are_400_with_a_code_and_write_nothing").await else { return };
    let state = state_with(&pool, None);
    let pro = personal(&pool, Plan::Pro).await;
    let cases: Vec<(&str, UpdateRequest)> = vec![
        ("limit_out_of_range", UpdateRequest { monthly_limit_micro_usd: Some(0), ..enable() }),
        ("limit_out_of_range", UpdateRequest { monthly_limit_micro_usd: Some(-50 * M), ..enable() }),
        ("limit_out_of_range", UpdateRequest { monthly_limit_micro_usd: Some(100_001 * M), ..enable() }),
        ("amount_not_whole_cents", UpdateRequest { monthly_limit_micro_usd: Some(10 * M + 7), ..enable() }),
        ("member_cap_not_applicable", UpdateRequest { member_cap_seat_multiple: Some(2.0), ..enable() }),
        (
            "auto_reload_exceeds_limit",
            UpdateRequest {
                monthly_limit_micro_usd: Some(20 * M),
                auto_reload: reload(true, Some(5 * M), Some(25 * M)),
                ..enable()
            },
        ),
        ("auto_reload_incomplete", UpdateRequest { auto_reload: reload(true, None, None), ..enable() }),
        (
            "auto_reload_requires_extra_usage",
            UpdateRequest { enabled: false, auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..Default::default() },
        ),
    ];
    for (code, req) in cases {
        let (status, b) = put(&state, &pro, req).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{code}: {b}");
        assert_eq!(b["code"], code);
    }
    assert_eq!(row(&pool, &pro).await, None);
}

#[tokio::test]
async fn auto_reload_is_stored_updated_and_switched_off_with_extra_usage() {
    let Some(pool) = pg("auto_reload_is_stored_updated_and_switched_off_with_extra_usage").await else { return };
    let state = state_with(&pool, None);
    let admin = org(&pool, Plan::Team, "admin").await;

    let (status, b) = put(
        &state,
        &admin,
        UpdateRequest {
            monthly_limit_micro_usd: Some(200 * M),
            member_cap_seat_multiple: Some(2.5),
            auto_reload: reload(true, Some(5 * M), Some(50 * M)),
            ..enable()
        },
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{b}");
    let r = row(&pool, &admin).await.unwrap();
    assert_eq!((r.reload, r.threshold, r.amount), (true, Some(5 * M), Some(50 * M)));
    assert_eq!(r.cap, Some(Decimal::new(250, 2)));
    assert_eq!(b["extra_usage"]["auto_reload"]["amount_micro_usd"], 50 * M);

    // Lowering the limit under the stored reload amount is refused, row untouched.
    let before = row(&pool, &admin).await;
    let (status, b) = put(&state, &admin, UpdateRequest { monthly_limit_micro_usd: Some(40 * M), ..enable() }).await;
    assert_eq!((status, b["code"].as_str()), (StatusCode::BAD_REQUEST, Some("auto_reload_exceeds_limit")));
    assert_eq!(row(&pool, &admin).await, before);

    // Replaying the same request is a no-op success.
    let (status, _) = put(&state, &admin, enable()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(row(&pool, &admin).await, before);

    // Disabling extra usage switches reload off but keeps its values.
    let (status, b) = put(&state, &admin, UpdateRequest { enabled: false, ..Default::default() }).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    let r = row(&pool, &admin).await.unwrap();
    assert_eq!((r.enabled, r.reload, r.amount, r.limit), (false, false, Some(50 * M), Some(200 * M)));
    assert_eq!(b["extra_usage"]["auto_reload"]["enabled"], false);
}

#[tokio::test]
async fn concurrent_puts_never_violate_the_schema_checks() {
    let Some(pool) = pg("concurrent_puts_never_violate_the_schema_checks").await else { return };
    let state = state_with(&pool, None);
    let pro = Arc::new(personal(&pool, Plan::Pro).await);
    let mut tasks = Vec::new();
    for i in 0..12 {
        let (state, pro) = (state.clone(), pro.clone());
        tasks.push(tokio::spawn(async move {
            let req = if i % 3 == 0 {
                UpdateRequest { enabled: false, ..Default::default() }
            } else if i % 3 == 1 {
                UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() }
            } else {
                UpdateRequest { monthly_limit_micro_usd: Some((20 + i) * M), ..enable() }
            };
            put(&state, &pro, req).await.0
        }));
    }
    for t in tasks {
        let s = t.await.unwrap();
        assert!(s == StatusCode::OK || s == StatusCode::BAD_REQUEST, "never a 500: {s}");
    }
    let r = row(&pool, &pro).await.unwrap();
    assert!(!r.reload || r.enabled, "reload implies enabled");
    if let (Some(l), Some(a)) = (r.limit, r.amount) {
        assert!(a <= l, "reload amount within limit");
    }
}

// ── POST /v1/billing/extra-usage/topup ─────────────────────────────────────

async fn pro_enabled(pool: &PgPool, state: &Arc<AppState>) -> Acct {
    let pro = personal(pool, Plan::Pro).await;
    let (s, b) = put(state, &pro, enable()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    pro
}

#[tokio::test]
async fn topup_is_refused_when_it_could_not_be_spent() {
    let Some(pool) = pg("topup_is_refused_when_it_could_not_be_spent").await else { return };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));

    // Free (403 plan_required).
    let free = personal(&pool, Plan::Free).await;
    let (s, b) = topup(&state, &free, 10 * M).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::FORBIDDEN, Some("plan_required")));

    // Paid but extra usage never configured, then explicitly disabled (409).
    let pro = personal(&pool, Plan::Pro).await;
    let (s, b) = topup(&state, &pro, 10 * M).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("extra_usage_off")));
    put(&state, &pro, enable()).await;
    put(&state, &pro, UpdateRequest { enabled: false, ..Default::default() }).await;
    let (s, b) = topup(&state, &pro, 10 * M).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("extra_usage_off")));

    // Frozen by a cancellation (decision 19): refused rather than taking money
    // for a balance that cannot be spent until the account resubscribes.
    let frozen = pro_enabled(&pool, &state).await;
    set_subscription(&pool, &frozen, "canceled").await;
    let (s, b) = topup(&state, &frozen, 10 * M).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("credits_frozen")));

    // A plain org member (403 admin_required).
    let member = org(&pool, Plan::Team, "member").await;
    let (s, b) = topup(&state, &member, 10 * M).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::FORBIDDEN, Some("admin_required")));

    assert!(calls.sessions().is_empty(), "no Stripe session for any refused top-up");
}

#[tokio::test]
async fn topup_amount_bounds_are_enforced_server_side() {
    let Some(pool) = pg("topup_amount_bounds_are_enforced_server_side").await else { return };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));
    let pro = pro_enabled(&pool, &state).await;
    for bad in [i64::MIN, -10 * M, 0, 1, 5 * M - 10_000, 1_000 * M + 10_000, i64::MAX] {
        let (s, b) = topup(&state, &pro, bad).await;
        assert_eq!((s, b["code"].as_str()), (StatusCode::BAD_REQUEST, Some("topup_amount_out_of_range")), "{bad}");
    }
    let (s, b) = topup(&state, &pro, 10 * M + 1).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::BAD_REQUEST, Some("amount_not_whole_cents")));
    assert!(calls.sessions().is_empty());
}

#[tokio::test]
async fn topup_refuses_a_return_url_this_deployment_does_not_own() {
    let Some(pool) = pg("topup_refuses_a_return_url_this_deployment_does_not_own").await else { return };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));
    let pro = pro_enabled(&pool, &state).await;
    let (s, _) = topup_to(&state, &pro, 10 * M, "https://evil.example/ok").await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert!(calls.sessions().is_empty());
}

#[tokio::test]
async fn topup_creates_a_checkout_session_that_saves_the_card_and_links_the_account() {
    let Some(pool) = pg("topup_creates_a_checkout_session_that_saves_the_card_and_links_the_account").await else {
        return;
    };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));
    let pro = pro_enabled(&pool, &state).await;

    let (s, b) = topup(&state, &pro, 25 * M + 500_000).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["checkout_url"], "https://checkout.stripe.com/topup");
    assert_eq!(b["session_id"], "cs_topup_mock");

    let seen = calls.sessions();
    assert_eq!(seen.len(), 1);
    let f = &seen[0].form;
    let acct = pro.id.to_string();
    assert_eq!(f["mode"], "payment");
    assert_eq!(f["customer"], format!("cus_mock_{acct}"));
    assert_eq!(f["line_items[0][price_data][unit_amount]"], "2550");
    assert_eq!(f["payment_intent_data[setup_future_usage]"], "off_session");
    assert_eq!(f["metadata[account_id]"], acct);
    assert_eq!(f["metadata[purpose]"], TOPUP_PURPOSE);
    assert_eq!(f["payment_intent_data[metadata][account_id]"], acct);
    assert!(seen[0].idempotency_key.as_deref().is_some_and(|k| !k.is_empty()), "idempotency key is sent");

    // The customer mapping exists before any webhook could arrive.
    let stored: Option<String> =
        sqlx::query_scalar("SELECT stripe_customer_id FROM stripe_subscriptions WHERE account_id=$1")
            .bind(pro.id)
            .fetch_optional(&pool)
            .await
            .unwrap();
    assert_eq!(stored, Some(format!("cus_mock_{acct}")));

    // A double click inside the window sends the same key, so Stripe returns one session.
    let (s, _) = topup(&state, &pro, 25 * M + 500_000).await;
    assert_eq!(s, StatusCode::OK);
    let seen = calls.sessions();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].idempotency_key, seen[1].idempotency_key);
    // Creating a session never credits anything.
    let bal: Option<i64> = sqlx::query_scalar("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
        .bind(pro.id)
        .fetch_optional(&pool)
        .await
        .unwrap();
    assert_eq!(bal, None);
}

#[tokio::test]
async fn an_org_admin_can_top_up_for_the_org() {
    let Some(pool) = pg("an_org_admin_can_top_up_for_the_org").await else { return };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));
    let admin = org(&pool, Plan::Team, "admin").await;
    assert_eq!(put(&state, &admin, enable()).await.0, StatusCode::OK);
    let (s, b) = topup(&state, &admin, 100 * M).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(calls.sessions()[0].form["metadata[account_id]"], admin.id.to_string());
}

#[tokio::test]
async fn topup_without_a_stripe_client_is_503_and_a_stripe_error_is_500() {
    let Some(pool) = pg("topup_without_a_stripe_client_is_503_and_a_stripe_error_is_500").await else { return };
    let state = state_with(&pool, None);
    let pro = pro_enabled(&pool, &state).await;
    assert_eq!(topup(&state, &pro, 10 * M).await.0, StatusCode::SERVICE_UNAVAILABLE);

    let (stripe, _) = mock_stripe(true).await;
    let state = state_with(&pool, Some(stripe));
    let (s, b) = topup(&state, &pro, 10 * M).await;
    assert_eq!(s, StatusCode::INTERNAL_SERVER_ERROR);
    assert!(!b.to_string().contains("boom"), "Stripe's message is not leaked to the caller");
}

// ── end to end ─────────────────────────────────────────────────────────────

#[tokio::test]
async fn enable_top_up_spend_past_the_allowance_and_the_balance_clamps_at_zero() {
    use crate::billing::enforcement::{check_funding, Spend};
    use crate::billing::extra_usage::webhook::{apply_topup, PaymentFacts, PaymentOutcome};
    use crate::billing::funding_policy::db::{evaluate, Mode, PeriodSel};
    use crate::billing::funding_policy::{ExtraFunding, FundingOutcome, Intent, RefusalReason};
    use crate::workers::billing_derivation::native_job_debit;

    let Some(pool) = pg("enable_top_up_spend_past_the_allowance_and_the_balance_clamps_at_zero").await else {
        return;
    };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));
    let d = Decimal::from;

    // A Pro account whose 100-credit allowance is spent.
    let pro = personal(&pool, Plan::Pro).await;
    {
        let mut tx = fenced(&pool, pro.id).await;
        sqlx::query("INSERT INTO credit_balances (account_id, balance) VALUES ($1, 0)")
            .bind(pro.id)
            .execute(&mut *tx)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO member_allowance_periods (account_id,user_id,period_start,period_end,granted_credits,consumed_credits) \
             VALUES ($1,$2, now() - interval '1 day', now() + interval '29 days', 100, 100)",
        )
        .bind(pro.id)
        .bind(pro.user)
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }
    let preflight = || async { check_funding(&pool, pro.id, pro.user, Spend::Background).await.unwrap() };
    assert_eq!(preflight().await, Some(RefusalReason::ExtraUsageOff), "paywall before extra usage is on");

    // 1. Enable (Pro default $50 limit). On but empty: still a paywall, for a new reason.
    assert_eq!(put(&state, &pro, enable()).await.0, StatusCode::OK);
    assert_eq!(preflight().await, Some(RefusalReason::ExtraBalanceExhausted));

    // 2. Top up $11 (500 credits): session created, then Stripe's webhook credits it.
    let (s, b) = topup(&state, &pro, 11 * M).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(calls.sessions()[0].form["line_items[0][price_data][unit_amount]"], "1100");
    let facts = PaymentFacts {
        payment_intent: Some(format!("pi_{}", Uuid::new_v4().simple())),
        amount_total_cents: Some(1100),
        amount_subtotal_cents: Some(1100),
        currency: Some("usd".into()),
        payment_status: Some("paid".into()),
        purpose: Some(TOPUP_PURPOSE.into()),
        metadata_account_id: Some(pro.id.to_string()),
    };
    let out = apply_topup(&pool, pro.id, &format!("evt_{}", Uuid::new_v4().simple()), "checkout.session.completed", &format!("cs_{}", Uuid::new_v4().simple()), &facts)
        .await
        .unwrap();
    assert_eq!(out, PaymentOutcome::Credited { micro_usd: 11 * M });
    assert_eq!(preflight().await, None, "funded: the paywall is gone");

    // 3. FundingPolicy::decide funds a 450-credit charge entirely from extra usage.
    let mut conn = pool.acquire().await.unwrap();
    let decided = evaluate(&mut conn, pro.id, pro.user, Mode::Settlement, PeriodSel::Current, Intent::Settle(d(450)))
        .await
        .unwrap()
        .outcome;
    drop(conn);
    assert_eq!(
        decided,
        FundingOutcome::Funded {
            allowance_credits: d(0),
            extra: ExtraFunding { credits: d(450), micro_usd: 9_900_000 }
        }
    );

    // 4. Spend it: the balance drops by exactly the money, and a debit row exists.
    native_job_debit(&pool, pro.id, Some(pro.user), Uuid::new_v4(), d(450)).await.unwrap();
    let bal = |pool: PgPool, id: Uuid| async move {
        sqlx::query_scalar::<_, i64>("SELECT balance_micro_usd FROM extra_usage_balances WHERE account_id=$1")
            .bind(id)
            .fetch_one(&pool)
            .await
            .unwrap()
    };
    assert_eq!(bal(pool.clone(), pro.id).await, 1_100_000);

    // 5. Spend past what is left (80 credits wanted, 50 affordable): clamps at zero.
    native_job_debit(&pool, pro.id, Some(pro.user), Uuid::new_v4(), d(80)).await.unwrap();
    assert_eq!(bal(pool.clone(), pro.id).await, 0, "never negative");
    let spent: i64 = sqlx::query_scalar(
        "SELECT sum(-amount_micro_usd)::bigint FROM extra_usage_ledger WHERE account_id=$1 AND entry_kind='debit'",
    )
    .bind(pro.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(spent, 11 * M, "debits equal exactly the money that was paid in");
    let unfunded: Decimal = sqlx::query_scalar("SELECT COALESCE(sum(credits),0) FROM credit_ledger WHERE account_id=$1")
        .bind(pro.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(unfunded, d(-30), "the 30 credits extra usage could not fund stay unfunded");

    // 6. And the gate says why.
    assert_eq!(preflight().await, Some(RefusalReason::ExtraBalanceExhausted));
}

async fn pack(state: &Arc<AppState>, a: &Acct) -> (StatusCode, Value) {
    body(
        crate::routes::checkout::create_pack_checkout(
            State(state.clone()),
            Extension(ctx(a)),
            Json(crate::routes::checkout::PackCheckoutRequest {
                pack: "pack_500".into(),
                quantity: 1,
                success_url: "http://localhost:3000/ok".into(),
                cancel_url: "http://localhost:3000/no".into(),
            }),
        )
        .await,
    )
    .await
}

#[tokio::test]
async fn pack_checkout_is_refused_when_the_balance_could_not_be_spent() {
    let Some(pool) = pg("pack_checkout_is_refused_when_the_balance_could_not_be_spent").await else { return };
    let (stripe, calls) = mock_stripe(false).await;
    let state = state_with(&pool, Some(stripe));

    // Free (403 plan_required): a pack would land in a balance Free can never spend.
    let free = personal(&pool, Plan::Free).await;
    let (s, b) = pack(&state, &free).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::FORBIDDEN, Some("plan_required")));

    // Paid but extra usage never configured, then explicitly disabled (409).
    let pro = personal(&pool, Plan::Pro).await;
    let (s, b) = pack(&state, &pro).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("extra_usage_off")));
    put(&state, &pro, enable()).await;
    put(&state, &pro, UpdateRequest { enabled: false, ..Default::default() }).await;
    let (s, b) = pack(&state, &pro).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("extra_usage_off")));

    // Frozen by a cancellation (409 credits_frozen).
    let frozen = pro_enabled(&pool, &state).await;
    set_subscription(&pool, &frozen, "canceled").await;
    let (s, b) = pack(&state, &frozen).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("credits_frozen")));

    assert!(calls.sessions().is_empty(), "no Stripe session for any refused pack");
}

#[tokio::test]
async fn a_put_that_changes_nothing_does_not_bump_updated_at() {
    let Some(pool) = pg("a_put_that_changes_nothing_does_not_bump_updated_at").await else { return };
    let state = state_with(&pool, None);
    let pro = personal(&pool, Plan::Pro).await;
    let stamp = |pool: PgPool, id: Uuid| async move {
        sqlx::query_scalar::<_, chrono::DateTime<chrono::Utc>>(
            "SELECT updated_at FROM extra_usage_settings WHERE account_id=$1",
        )
        .bind(id)
        .fetch_one(&pool)
        .await
        .expect("updated_at")
    };
    let (s, b) = put(&state, &pro, enable()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let first = stamp(pool.clone(), pro.id).await;
    let (s, _) = put(&state, &pro, enable()).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(stamp(pool.clone(), pro.id).await, first, "a no-op save must not move updated_at");
    // A real change does.
    let (s, _) = put(&state, &pro, UpdateRequest { monthly_limit_micro_usd: Some(60 * M), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    assert!(stamp(pool.clone(), pro.id).await > first);
}

/// Packs land in the extra-usage balance, so the pack route only serves an
/// account that could spend it: a paid plan with extra usage on. Shared with
/// `routes::checkout`'s pg tests, which seed accounts of their own.
pub(crate) async fn make_pack_buyer(pool: &PgPool, account: Uuid) {
    grant(pool, account, Plan::Pro).await;
    sqlx::query(
        "INSERT INTO extra_usage_settings (account_id, enabled, monthly_limit_micro_usd) VALUES ($1, true, 50000000)",
    )
    .bind(account)
    .execute(pool)
    .await
    .expect("extra usage settings");
}

async fn reset_at(pool: &PgPool, a: &Acct) -> Option<chrono::DateTime<chrono::Utc>> {
    sqlx::query_scalar("SELECT auto_reload_reset_at FROM extra_usage_settings WHERE account_id=$1")
        .bind(a.id)
        .fetch_one(pool)
        .await
        .expect("reset_at")
}

#[tokio::test]
async fn auto_reload_reset_at_moves_only_when_auto_reload_goes_from_off_to_on() {
    let Some(pool) = pg("auto_reload_reset_at_moves_only_when_auto_reload_goes_from_off_to_on").await else { return };
    let state = state_with(&pool, None);
    let pro = personal(&pool, Plan::Pro).await;

    let (s, b) = put(&state, &pro, enable()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(reset_at(&pool, &pro).await, None, "auto-reload never turned on");

    let (s, b) = put(&state, &pro, UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() }).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    let on_at = reset_at(&pool, &pro).await.expect("set when switched on");

    // Unrelated saves (limit, threshold) leave it alone.
    let (s, _) = put(&state, &pro, UpdateRequest { monthly_limit_micro_usd: Some(80 * M), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    let (s, _) = put(&state, &pro, UpdateRequest { auto_reload: reload(true, Some(6 * M), Some(20 * M)), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(reset_at(&pool, &pro).await, Some(on_at), "saves while on do not reset the count");

    // Off, then on again: a fresh count.
    let (s, _) = put(&state, &pro, UpdateRequest { auto_reload: reload(false, None, None), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(reset_at(&pool, &pro).await, Some(on_at), "turning it off does not move it");
    let (s, _) = put(&state, &pro, UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    assert!(reset_at(&pool, &pro).await.unwrap() > on_at, "off -> on starts a new count");

    // First ever save with auto-reload already on (insert path) sets it too.
    let fresh = personal(&pool, Plan::Pro).await;
    let (s, b) = put(&state, &fresh, UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() }).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert!(reset_at(&pool, &fresh).await.is_some());
}

// ── auto-reload needs a card as the default payment method ─────────────────

fn state_with_arc(pool: &PgPool, stripe: Arc<crate::billing::stripe_client::StripeClient>) -> Arc<AppState> {
    let mut s = crate::test_app_state_owned_with_db_and_store(pool.clone(), None);
    s.stripe = Some(stripe);
    Arc::new(s)
}

/// A Pro account whose Stripe customer's default payment method is `default`
/// (expanded object), and the mock answering for it.
async fn pro_with_default_pm(
    pool: &PgPool,
    default: Option<Value>,
) -> (Acct, Arc<AppState>, crate::billing::test_support::MockStripe) {
    let pro = personal(pool, Plan::Pro).await;
    let customer = format!("cus_pm_{}", Uuid::new_v4().simple());
    crate::billing::test_support::seed_subscription(pool, pro.id, &customer, Some(&format!("sub_{}", Uuid::new_v4().simple())), "active").await;
    let mock = crate::billing::test_support::MockStripe::spawn().await;
    mock.respond(
        "GET",
        &format!("/v1/customers/{customer}"),
        200,
        json!({"id": customer, "invoice_settings": {"default_payment_method": default}}),
    );
    mock.respond("GET", "/v1/payment_methods", 200, json!({"data": [], "has_more": false}));
    let state = state_with_arc(pool, mock.client.clone());
    (pro, state, mock)
}

fn bank_pm() -> Value {
    json!({"id": "pm_bank", "type": "us_bank_account", "us_bank_account": {"bank_name": "Example Bank", "last4": "4242"}})
}

fn card_pm() -> Value {
    json!({"id": "pm_card", "type": "card", "card": {"brand": "visa", "last4": "4242", "exp_month": 1, "exp_year": 2031}})
}

fn reload_on() -> UpdateRequest {
    UpdateRequest { auto_reload: reload(true, Some(5 * M), Some(20 * M)), ..enable() }
}

#[tokio::test]
async fn turning_auto_reload_on_is_refused_when_the_default_payment_method_is_a_bank_account() {
    let Some(pool) = pg("turning_auto_reload_on_is_refused_when_the_default_payment_method_is_a_bank_account").await
    else {
        return;
    };
    let (pro, state, _mock) = pro_with_default_pm(&pool, Some(bank_pm())).await;
    let (s, b) = put(&state, &pro, reload_on()).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("auto_reload_requires_card")), "{b}");
    assert!(b["error"].as_str().unwrap().contains("Manage billing"), "{b}");
    assert_eq!(row(&pool, &pro).await, None, "a refused save writes nothing");

    // Extra usage itself, without auto-reload, is unaffected by the bank method.
    let (s, b) = put(&state, &pro, enable()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    // Explicitly sending auto-reload off is fine too.
    let (s, b) = put(&state, &pro, UpdateRequest { auto_reload: reload(false, None, None), ..enable() }).await;
    assert_eq!(s, StatusCode::OK, "{b}");
}

#[tokio::test]
async fn turning_auto_reload_on_is_allowed_with_a_card_default() {
    let Some(pool) = pg("turning_auto_reload_on_is_allowed_with_a_card_default").await else { return };
    let (pro, state, _mock) = pro_with_default_pm(&pool, Some(card_pm())).await;
    let (s, b) = put(&state, &pro, reload_on()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    assert_eq!(b["extra_usage"]["auto_reload"]["enabled"], true);
}

#[tokio::test]
async fn a_subscriber_with_auto_reload_already_on_keeps_editing_with_a_bank_default() {
    let Some(pool) = pg("a_subscriber_with_auto_reload_already_on_keeps_editing_with_a_bank_default").await else {
        return;
    };
    let (pro, state, mock) = pro_with_default_pm(&pool, Some(card_pm())).await;
    let (s, b) = put(&state, &pro, reload_on()).await;
    assert_eq!(s, StatusCode::OK, "{b}");
    // The default later becomes a bank account (the subscription renewed on one).
    let customer = mock.requests().iter().find_map(|r| r.path.strip_prefix("/v1/customers/").map(str::to_string)).unwrap();
    mock.respond(
        "GET",
        &format!("/v1/customers/{customer}"),
        200,
        json!({"id": customer, "invoice_settings": {"default_payment_method": bank_pm()}}),
    );
    let (s, b) = put(&state, &pro, UpdateRequest { monthly_limit_micro_usd: Some(80 * M), ..reload_on() }).await;
    assert_eq!(s, StatusCode::OK, "an already-on setting is not refused: {b}");
    // Off and back on is a real off -> on change: refused until a card is added.
    let (s, _) = put(&state, &pro, UpdateRequest { auto_reload: reload(false, None, None), ..enable() }).await;
    assert_eq!(s, StatusCode::OK);
    let (s, b) = put(&state, &pro, reload_on()).await;
    assert_eq!((s, b["code"].as_str()), (StatusCode::CONFLICT, Some("auto_reload_requires_card")), "{b}");
}

#[tokio::test]
async fn a_stripe_failure_while_checking_the_payment_method_is_a_503_not_a_guess() {
    let Some(pool) = pg("a_stripe_failure_while_checking_the_payment_method_is_a_503_not_a_guess").await else {
        return;
    };
    let pro = personal(&pool, Plan::Pro).await;
    crate::billing::test_support::seed_subscription(&pool, pro.id, &format!("cus_{}", Uuid::new_v4().simple()), Some(&format!("sub_{}", Uuid::new_v4().simple())), "active").await;
    let mock = crate::billing::test_support::MockStripe::spawn().await; // no routes: every call 404s
    let state = state_with_arc(&pool, mock.client.clone());
    let (s, _) = put(&state, &pro, reload_on()).await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE);
}
