//! Shared fixtures for the settings-launch billing pg tests
//! (`routes::billing_manage::pg_tests`, `billing::cancel::pg_tests`, and the
//! PR 3.3 additions to `routes::stripe_webhook::pg_tests`).
//!
//! `RELINFER_TEST_DATABASE_URL`-gated like every billing pg suite: without
//! it [`pg_test_pool`] returns `None` and the test prints a skip line. A
//! skip is NOT a pass, see `scripts/check_pg_suite_coverage.sh`.
//!
//! [`MockStripe`] is the repo's established Stripe substitute (a local axum
//! server, no mock-HTTP crate) generalised to record every request, so a
//! test can assert on the exact wire call (path, form body, idempotency key)
//! instead of only on the handler's response.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::{HeaderMap, Method, StatusCode, Uri};
use axum::response::IntoResponse;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::billing::stripe_client::StripeClient;
use crate::middleware::auth::UserContext;

pub async fn pg_test_pool(test_name: &str) -> Option<sqlx::PgPool> {
    let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
        Ok(u) => u,
        Err(_) => {
            eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
            return None;
        }
    };
    Some(
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&url)
            .await
            .expect("connect to test Postgres"),
    )
}

/// Run one fixture statement the way every billing writer does: account
/// scope, then `lock_period_rotation` (which enters the 0321 financial
/// fence), then the statement.
pub async fn exec_fixture<'q>(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
    crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
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

pub async fn seed_personal_account(pool: &sqlx::PgPool) -> (Uuid, Uuid) {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@billing-manage.test", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("insert account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("insert account_members row");
    (account_id, user_id)
}

/// A real org account whose first member holds `role`.
pub async fn seed_org_account(pool: &sqlx::PgPool, role: &str) -> (Uuid, Uuid) {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@billing-manage.test", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(format!("org_{}", Uuid::new_v4().simple()))
    .fetch_one(pool)
    .await
    .expect("insert org account");
    insert_org_member(pool, account_id, user_id, role).await;
    (account_id, user_id)
}

/// Add another member to an org account; returns the new user id.
pub async fn add_org_member(pool: &sqlx::PgPool, account_id: Uuid, role: &str) -> Uuid {
    let user_id: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@billing-manage.test", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .expect("insert user");
    insert_org_member(pool, account_id, user_id, role).await;
    user_id
}

async fn insert_org_member(pool: &sqlx::PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    let standard_tier_id: Uuid =
        sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .fetch_one(pool)
            .await
            .expect("active standard seat tier must exist (0165's seed)");
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, $4)",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .bind(standard_tier_id)
    .execute(pool)
    .await
    .expect("insert account_members row");
}

/// A personal org's own account (`clerk_org_id` set, `personal_orgs` row).
/// Returns `(account_id, user_id, clerk_org_id)`.
pub async fn seed_personal_org_account(pool: &sqlx::PgPool) -> (Uuid, Uuid, String) {
    let clerk_user_id = format!("user_{}", Uuid::new_v4().simple());
    let clerk_org_id = format!("org_{}", Uuid::new_v4().simple());
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
            .bind(format!("{}@billing-manage.test", Uuid::new_v4()))
            .bind(&clerk_user_id)
            .fetch_one(pool)
            .await
            .expect("insert user");
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(&clerk_org_id)
    .fetch_one(pool)
    .await
    .expect("insert personal-org account");
    sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
        .bind(&clerk_org_id)
        .bind(&clerk_user_id)
        .execute(pool)
        .await
        .expect("insert personal_orgs row");
    insert_org_member(pool, account_id, user_id, "owner").await;
    (account_id, user_id, clerk_org_id)
}

/// Insert (or replace) the account's `stripe_subscriptions` row.
pub async fn seed_subscription(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    customer_id: &str,
    subscription_id: Option<&str>,
    status: &str,
) {
    exec_fixture(
        pool,
        account_id,
        sqlx::query(
            "INSERT INTO stripe_subscriptions \
                 (account_id, stripe_customer_id, stripe_subscription_id, status, \
                  current_period_start, current_period_end) \
             VALUES ($1, $2, $3, $4, now() - interval '5 days', now() + interval '25 days') \
             ON CONFLICT (account_id) DO UPDATE SET \
                 stripe_customer_id = EXCLUDED.stripe_customer_id, \
                 stripe_subscription_id = EXCLUDED.stripe_subscription_id, \
                 status = EXCLUDED.status, \
                 current_period_start = EXCLUDED.current_period_start, \
                 current_period_end = EXCLUDED.current_period_end",
        )
        .bind(account_id)
        .bind(customer_id)
        .bind(subscription_id)
        .bind(status),
    )
    .await
    .expect("seed stripe_subscriptions");
}

pub async fn cleanup_account(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    users: &[Uuid],
) -> Result<(), sqlx::Error> {
    exec_fixture(pool, account_id, sqlx::query("DELETE FROM accounts WHERE id = $1").bind(account_id))
        .await?;
    for user_id in users {
        sqlx::query("DELETE FROM users WHERE id = $1").bind(user_id).execute(pool).await?;
    }
    Ok(())
}

pub async fn cleanup_personal_org(pool: &sqlx::PgPool, clerk_org_id: &str) {
    sqlx::query("DELETE FROM personal_orgs WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .execute(pool)
        .await
        .expect("cleanup personal_orgs row");
}

pub fn user_context(account_id: Uuid, user_id: Uuid) -> UserContext {
    UserContext {
        user_id,
        account_id,
        role: "owner".to_string(),
        device_id: "billing-manage-test".to_string(),
    }
}

pub async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.expect("read body");
    serde_json::from_slice(&bytes).expect("valid JSON body")
}

// ── Recording Stripe mock ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct Recorded {
    pub method: String,
    pub path: String,
    #[allow(dead_code)]
    pub query: HashMap<String, String>,
    pub form: HashMap<String, String>,
    pub idempotency_key: Option<String>,
}

type RouteKey = (String, String);

type Responder = Arc<dyn Fn(&Recorded) -> (u16, Value) + Send + Sync>;

#[derive(Default)]
struct MockState {
    responses: HashMap<RouteKey, Responder>,
    log: Vec<Recorded>,
}

pub struct MockStripe {
    pub client: Arc<StripeClient>,
    state: Arc<Mutex<MockState>>,
}

fn parse_pairs(raw: &str) -> HashMap<String, String> {
    raw.split('&')
        .filter(|kv| !kv.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            let dec = |s: &str| {
                urlencoding::decode(&s.replace('+', " "))
                    .map(|c| c.into_owned())
                    .unwrap_or_else(|_| s.to_string())
            };
            (dec(k), dec(v))
        })
        .collect()
}

impl MockStripe {
    pub async fn spawn() -> Self {
        let state = Arc::new(Mutex::new(MockState::default()));
        let handler_state = state.clone();
        let app = axum::Router::new().fallback(axum::routing::any(
            move |method: Method, uri: Uri, headers: HeaderMap, body: axum::body::Bytes| {
                let st = handler_state.clone();
                async move {
                    let path = uri.path().to_string();
                    let rec = Recorded {
                        method: method.to_string(),
                        path: path.clone(),
                        query: parse_pairs(uri.query().unwrap_or("")),
                        form: parse_pairs(&String::from_utf8_lossy(&body)),
                        idempotency_key: headers
                            .get("idempotency-key")
                            .and_then(|v| v.to_str().ok())
                            .map(str::to_string),
                    };
                    let mut g = st.lock().unwrap();
                    g.log.push(rec.clone());
                    match g.responses.get(&(method.to_string(), path)) {
                        Some(f) => {
                            let (status, body) = f(&rec);
                            (
                                StatusCode::from_u16(status).unwrap(),
                                [("content-type", "application/json")],
                                body.to_string(),
                            )
                                .into_response()
                        }
                        None => (
                            StatusCode::NOT_FOUND,
                            [("content-type", "application/json")],
                            json!({"error": {"type": "invalid_request_error", "message": "no mock for this route"}})
                                .to_string(),
                        )
                            .into_response(),
                    }
                }
            },
        ));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let client = Arc::new(
            StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")),
        );
        Self { client, state }
    }

    /// Canned response for `METHOD path` (path without query string).
    pub fn respond(&self, method: &str, path: &str, status: u16, body: Value) -> &Self {
        self.respond_with(method, path, move |_| (status, body.clone()))
    }

    /// Like [`Self::respond`], but the answer may depend on the request.
    pub fn respond_with(
        &self,
        method: &str,
        path: &str,
        f: impl Fn(&Recorded) -> (u16, Value) + Send + Sync + 'static,
    ) -> &Self {
        self.state
            .lock()
            .unwrap()
            .responses
            .insert((method.to_string(), path.to_string()), Arc::new(f));
        self
    }

    pub fn requests(&self) -> Vec<Recorded> {
        self.state.lock().unwrap().log.clone()
    }

    /// Requests recorded for `METHOD path`.
    pub fn calls(&self, method: &str, path: &str) -> Vec<Recorded> {
        self.requests().into_iter().filter(|r| r.method == method && r.path == path).collect()
    }
}

/// A minimal live subscription object in the `basil` shape (period on the
/// item, not the subscription).
pub fn subscription_json(
    id: &str,
    status: &str,
    items: &[(&str, &str, i64)],
    cancel_at_period_end: bool,
    cancel_at: Option<i64>,
    default_payment_method: Option<&str>,
) -> Value {
    let data: Vec<Value> = items
        .iter()
        .map(|(item_id, price_id, qty)| {
            json!({
                "id": item_id,
                "price": {"id": price_id},
                "quantity": qty,
                "current_period_start": 1_700_000_000,
                "current_period_end": 1_702_592_000,
            })
        })
        .collect();
    json!({
        "id": id,
        "status": status,
        "items": {"data": data},
        "cancel_at_period_end": cancel_at_period_end,
        "cancel_at": cancel_at,
        "default_payment_method": default_payment_method,
    })
}
