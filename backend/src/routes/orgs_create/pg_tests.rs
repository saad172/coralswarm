//! `POST /v1/orgs`, the status and retry routes, and the cleanup sweep,
//! against real Postgres with local Clerk and Stripe doubles. Every handler
//! and the sweep run as `coralswarm_app` (prod runs with RLS on); only
//! fixtures and read-backs use the owner pool.
//!
//! `RELINFER_TEST_DATABASE_URL`-gated like every billing pg suite; with
//! `REQUIRE_PG_SUITE` set, an unset URL FAILS instead of skipping.
//!
//!   RELINFER_TEST_DATABASE_URL=postgres://localhost:5432/<migrated db> REQUIRE_PG_SUITE=1 \
//!     cargo test -p coralswarm-backend --lib "routes::orgs_create::" -- --test-threads=2

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use serde_json::{json, Value};
use sqlx::PgPool;
use uuid::Uuid;

use super::*;
use crate::billing::stripe_client::StripeClient;
use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};
use crate::tenancy::org_selfserve_cleanup::{CleanupPolicy, Sweeper};
use crate::tenancy::provisioning::CleanupRelease;

/// The sweep scans every row in the database, so the two sweep tests (the
/// only ones that make sweepable rows) take turns.
static SWEEP_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

const URL: &str = "http://localhost:3000/onboarding/org";

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
    let owner = sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect");
    let app = crate::app_role_test_support::app_role_pool(&owner, &url).await;
    Some(Pools { owner, app })
}

async fn fin(pool: &PgPool, account: Uuid, q: sqlx::query::Query<'_, sqlx::Postgres, sqlx::postgres::PgArguments>) {
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account).await.expect("scope");
    crate::billing::renewal::lock_period_rotation(&mut tx, account).await.expect("lock");
    q.execute(&mut *tx).await.expect("fenced fixture write");
    tx.commit().await.expect("commit");
}

async fn body(resp: Response) -> (StatusCode, Value) {
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

fn ok(v: Value) -> Response {
    (StatusCode::OK, [("content-type", "application/json")], v.to_string()).into_response()
}

fn err(code: StatusCode, msg: &str) -> Response {
    (code, [("content-type", "application/json")], json!({"errors": [{"code": "x", "message": msg}], "error": {"message": msg}}).to_string())
        .into_response()
}

async fn serve(app: axum::Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}")
}

// ── Clerk double ─────────────────────────────────────────────────────────

#[derive(Default)]
struct ClerkState {
    /// clerk user id → (primary email verified?, email override)
    users: HashMap<String, (bool, Option<String>)>,
    /// Every org this double knows: id → org object.
    orgs: HashMap<String, Value>,
    /// Bodies of `POST /v1/organizations`.
    creates: Vec<Value>,
    deletes: Vec<String>,
    /// `PATCH /v1/organizations/{id}` bodies.
    patches: Vec<(String, Value)>,
    /// Delay before answering a create (ms), to keep requests in flight.
    create_delay_ms: u64,
    /// `GET /v1/users/{id}` calls per user (#1540 N6).
    user_reads: HashMap<String, usize>,
    /// #1540 SF4: the next create succeeds in Clerk but answers 500 (a
    /// response lost to a timeout).
    lose_next_create_response: bool,
    /// #1540 SF4: `GET /v1/organizations?user_id=` lags and shows nothing.
    hide_from_list: bool,
    /// #1556: creates answer this Clerk refusal (status, code).
    refuse_create: Option<(u16, &'static str)>,
}

#[derive(Clone)]
struct ClerkMock(Arc<Mutex<ClerkState>>);

impl ClerkMock {
    fn creates(&self) -> Vec<Value> {
        self.0.lock().unwrap().creates.clone()
    }
    fn deletes(&self) -> Vec<String> {
        self.0.lock().unwrap().deletes.clone()
    }
    fn patches(&self) -> Vec<(String, Value)> {
        self.0.lock().unwrap().patches.clone()
    }
    fn add_user(&self, clerk_id: &str, verified: bool) {
        self.0.lock().unwrap().users.insert(clerk_id.into(), (verified, None));
    }
    fn add_user_email(&self, clerk_id: &str, email: &str) {
        self.0.lock().unwrap().users.insert(clerk_id.into(), (true, Some(email.into())));
    }
    fn add_org(&self, org: Value) {
        let id = org["id"].as_str().unwrap().to_string();
        self.0.lock().unwrap().orgs.insert(id, org);
    }
    fn set_create_delay(&self, ms: u64) {
        self.0.lock().unwrap().create_delay_ms = ms;
    }
    fn user_reads(&self, clerk_id: &str) -> usize {
        self.0.lock().unwrap().user_reads.get(clerk_id).copied().unwrap_or(0)
    }
    fn lose_next_create_response(&self) {
        self.0.lock().unwrap().lose_next_create_response = true;
    }
    fn refuse_creates_with(&self, status: u16, code: &'static str) {
        self.0.lock().unwrap().refuse_create = Some((status, code));
    }
    fn hide_from_list(&self, hide: bool) {
        self.0.lock().unwrap().hide_from_list = hide;
    }
}

async fn spawn_clerk() -> (ClerkAdminClient, ClerkMock) {
    let mock = ClerkMock(Arc::new(Mutex::new(ClerkState::default())));
    let m = mock.clone();
    let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::http::Request<axum::body::Body>| {
        let m = m.clone();
        async move {
            let (parts, b) = req.into_parts();
            let bytes = axum::body::to_bytes(b, 1 << 20).await.unwrap();
            let path = parts.uri.path().to_string();
            let method = parts.method.to_string();
            if method == "POST" && path == "/v1/organizations" {
                let delay = m.0.lock().unwrap().create_delay_ms;
                if delay > 0 {
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
            let mut s = m.0.lock().unwrap();
            match (method.as_str(), path.as_str()) {
                ("GET", p) if p.starts_with("/v1/users/") => {
                    let id = p.trim_start_matches("/v1/users/");
                    *s.user_reads.entry(id.to_string()).or_default() += 1;
                    match s.users.get(id) {
                        Some((v, email)) => ok(json!({
                            "id": id, "last_sign_in_at": null, "primary_email_address_id": "idn_1",
                            "email_addresses": [{"id": "idn_1",
                                "email_address": email.clone().unwrap_or_else(|| format!("{id}@test.example")),
                                "verification": {"status": if *v { "verified" } else { "unverified" }}}]
                        })),
                        None => err(StatusCode::NOT_FOUND, "no such user"),
                    }
                }
                ("POST", "/v1/organizations") => {
                    let v: Value = serde_json::from_slice(&bytes).unwrap();
                    if let Some((status, code)) = s.refuse_create {
                        s.creates.push(v);
                        return (StatusCode::from_u16(status).unwrap(), [("content-type", "application/json")],
                            json!({"errors": [{"code": code, "message": "refused", "long_message": "refused"}]}).to_string())
                            .into_response();
                    }
                    // Clerk refuses a slug another org holds (#1540 SF4).
                    if !v["slug"].is_null() && s.orgs.values().any(|o| o["slug"] == v["slug"]) {
                        s.creates.push(v);
                        return (StatusCode::UNPROCESSABLE_ENTITY, [("content-type", "application/json")],
                            json!({"errors": [{"code": "form_identifier_exists", "message": "slug taken"}]}).to_string())
                            .into_response();
                    }
                    let id = format!("org_{}", Uuid::new_v4().simple());
                    let org = json!({"id": id, "name": v["name"], "slug": v["slug"], "private_metadata": v["private_metadata"],
                                     "created_by": v["created_by"]});
                    s.creates.push(v);
                    s.orgs.insert(id, org.clone());
                    if std::mem::take(&mut s.lose_next_create_response) {
                        return err(StatusCode::INTERNAL_SERVER_ERROR, "response lost");
                    }
                    ok(org)
                }
                ("GET", p) if p.starts_with("/v1/organizations/") => {
                    // By id or by slug, as Clerk's endpoint accepts either.
                    let key = p.trim_start_matches("/v1/organizations/");
                    match s.orgs.values().find(|o| o["id"].as_str() == Some(key) || o["slug"].as_str() == Some(key)) {
                        Some(o) => ok(o.clone()),
                        None => err(StatusCode::NOT_FOUND, "not found"),
                    }
                }
                ("GET", "/v1/organizations") => {
                    if s.hide_from_list {
                        return ok(json!({"data": [], "total_count": 0}));
                    }
                    let q = parts.uri.query().unwrap_or("");
                    let user = q.split('&').find_map(|kv| kv.strip_prefix("user_id=")).unwrap_or("").to_string();
                    let data: Vec<Value> =
                        s.orgs.values().filter(|o| o["created_by"].as_str() == Some(user.as_str())).cloned().collect();
                    let n = data.len();
                    ok(json!({"data": data, "total_count": n}))
                }
                ("PATCH", p) if p.starts_with("/v1/organizations/") => {
                    let id = p.trim_start_matches("/v1/organizations/").to_string();
                    let v: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                    s.patches.push((id.clone(), v));
                    ok(json!({"id": id}))
                }
                ("DELETE", p) if p.starts_with("/v1/organizations/") => {
                    let id = p.trim_start_matches("/v1/organizations/").to_string();
                    s.deletes.push(id.clone());
                    if s.orgs.remove(&id).is_some() {
                        ok(json!({"id": id, "deleted": true}))
                    } else {
                        err(StatusCode::NOT_FOUND, "not found")
                    }
                }
                _ => err(StatusCode::NOT_FOUND, "unhandled"),
            }
        }
    }));
    let base = serve(app).await;
    (ClerkAdminClient::new(reqwest::Client::new(), "sk_test_clerk").with_base_url(base), mock)
}

// ── Stripe double ────────────────────────────────────────────────────────

type CancelGate = (tokio::sync::mpsc::UnboundedSender<String>, Arc<tokio::sync::Notify>);

#[derive(Default)]
struct StripeState {
    sessions: HashMap<String, String>,
    /// Each subscription's status per read (the last one sticks).
    subs: HashMap<String, Vec<String>>,
    reads: HashMap<String, usize>,
    customer_subs: HashMap<String, Vec<String>>,
    latest_invoice: HashMap<String, String>,
    invoice_pi: HashMap<String, String>,
    /// #1540 SF1: invoice id → (status, created unix seconds).
    invoices: HashMap<String, (String, i64)>,
    /// #1540 review SF-b: invoice id → `status_transitions.voided_at`.
    invoice_voided_at: HashMap<String, i64>,
    /// #1540 review SF-c: extra fields merged into a subscription's body
    /// (`cancel_at_period_end`, `cancel_at`).
    sub_extra: HashMap<String, Value>,
    pi_status: HashMap<String, String>,
    /// Page size of `GET /v1/subscriptions` (default: everything at once).
    page_size: Option<usize>,
    /// (method, path, idempotency key)
    calls: Vec<(String, String, Option<String>)>,
    /// `POST /v1/subscriptions/{id}` bodies: (path, form body).
    posts: Vec<(String, String, Option<String>)>,
    /// Held once: the next subscription DELETE reports here and waits.
    cancel_gate: Option<CancelGate>,
    n: u32,
}

impl StripeState {
    fn read_status(&mut self, id: &str) -> Option<String> {
        let seq = self.subs.get(id)?.clone();
        let n = self.reads.entry(id.to_string()).or_default();
        let st = seq[(*n).min(seq.len() - 1)].clone();
        *n += 1;
        Some(st)
    }
    fn sub_value(&mut self, id: &str) -> Option<Value> {
        let st = self.read_status(id)?;
        let mut v = sub_json(id, &st);
        if let Some(inv) = self.latest_invoice.get(id) {
            v["latest_invoice"] = json!(inv);
        }
        if let Some(Value::Object(extra)) = self.sub_extra.get(id) {
            for (k, x) in extra {
                v[k.as_str()] = x.clone();
            }
        }
        Some(v)
    }
}

#[derive(Clone)]
struct StripeMock(Arc<Mutex<StripeState>>);

impl StripeMock {
    fn calls(&self, method: &str, prefix: &str) -> Vec<(String, String, Option<String>)> {
        self.0.lock().unwrap().calls.iter().filter(|(m, p, _)| m == method && p.starts_with(prefix)).cloned().collect()
    }
    fn posts(&self, prefix: &str) -> Vec<(String, String, Option<String>)> {
        self.0.lock().unwrap().posts.iter().filter(|(p, _, _)| p.starts_with(prefix)).cloned().collect()
    }
    fn set_session(&self, id: &str, status: &str) {
        self.0.lock().unwrap().sessions.insert(id.into(), status.into());
    }
    fn set_sub(&self, customer: &str, id: &str, status: &str) {
        let mut s = self.0.lock().unwrap();
        s.subs.insert(id.into(), vec![status.into()]);
        s.customer_subs.entry(customer.into()).or_default().push(id.into());
    }
    fn set_sub_sequence(&self, id: &str, statuses: &[&str]) {
        let mut s = self.0.lock().unwrap();
        s.subs.insert(id.into(), statuses.iter().map(|x| x.to_string()).collect());
        s.reads.remove(id);
    }
    fn set_processing_payment(&self, sub: &str, invoice: &str, pi: &str, pi_status: &str) {
        let mut s = self.0.lock().unwrap();
        s.latest_invoice.insert(sub.into(), invoice.into());
        s.invoice_pi.insert(invoice.into(), pi.into());
        s.pi_status.insert(pi.into(), pi_status.into());
    }
    fn set_invoice(&self, sub: &str, invoice: &str, status: &str, created: i64) {
        let mut s = self.0.lock().unwrap();
        s.latest_invoice.insert(sub.into(), invoice.into());
        s.invoices.insert(invoice.into(), (status.into(), created));
    }
    fn set_invoice_voided_at(&self, invoice: &str, voided_at: i64) {
        self.0.lock().unwrap().invoice_voided_at.insert(invoice.into(), voided_at);
    }
    fn set_sub_extra(&self, sub: &str, extra: Value) {
        self.0.lock().unwrap().sub_extra.insert(sub.into(), extra);
    }
    fn set_page_size(&self, n: usize) {
        self.0.lock().unwrap().page_size = Some(n);
    }
    fn gate_next_cancel(&self) -> (tokio::sync::mpsc::UnboundedReceiver<String>, Arc<tokio::sync::Notify>) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let notify = Arc::new(tokio::sync::Notify::new());
        self.0.lock().unwrap().cancel_gate = Some((tx, notify.clone()));
        (rx, notify)
    }
    fn sub_status(&self, id: &str) -> Option<String> {
        self.0.lock().unwrap().subs.get(id).and_then(|v| v.last().cloned())
    }
    fn session_status(&self, id: &str) -> Option<String> {
        self.0.lock().unwrap().sessions.get(id).cloned()
    }
}

fn sub_json(id: &str, status: &str) -> Value {
    json!({"id": id, "status": status, "items": {"data": [{"id": "si_1", "quantity": 2,
        "current_period_start": 1, "current_period_end": 2, "price": {"id": "price_x"}}]}})
}

async fn spawn_stripe() -> (Arc<StripeClient>, StripeMock) {
    let mock = StripeMock(Arc::new(Mutex::new(StripeState::default())));
    let m = mock.clone();
    let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::http::Request<axum::body::Body>| {
        let m = m.clone();
        async move {
            let (parts, b) = req.into_parts();
            let bytes = axum::body::to_bytes(b, 1 << 20).await.unwrap_or_default();
            let form = String::from_utf8_lossy(&bytes).to_string();
            let path = parts.uri.path().to_string();
            let method = parts.method.to_string();
            let idem = parts.headers.get("idempotency-key").and_then(|v| v.to_str().ok()).map(str::to_string);
            let gate = {
                let mut s = m.0.lock().unwrap();
                s.calls.push((method.clone(), path.clone(), idem.clone()));
                if method == "DELETE" && path.starts_with("/v1/subscriptions/") {
                    s.cancel_gate.take()
                } else {
                    None
                }
            };
            if let Some((tx, notify)) = gate {
                let _ = tx.send(path.trim_start_matches("/v1/subscriptions/").to_string());
                notify.notified().await;
            }
            let mut s = m.0.lock().unwrap();
            match (method.as_str(), path.as_str()) {
                ("POST", "/v1/customers") => {
                    s.n += 1;
                    ok(json!({"id": format!("cus_a2_{}_{}", s.n, Uuid::new_v4().simple())}))
                }
                ("POST", "/v1/checkout/sessions") => {
                    s.n += 1;
                    let id = format!("cs_a2_{}_{}", s.n, Uuid::new_v4().simple());
                    s.sessions.insert(id.clone(), "open".into());
                    ok(json!({"id": id, "url": format!("https://checkout.stripe.com/a2/{}", s.n)}))
                }
                ("POST", p) if p.starts_with("/v1/checkout/sessions/") && p.ends_with("/expire") => {
                    let id = p.trim_start_matches("/v1/checkout/sessions/").trim_end_matches("/expire").to_string();
                    match s.sessions.get(&id).map(String::as_str) {
                        Some("open") => {
                            s.sessions.insert(id.clone(), "expired".into());
                            ok(json!({"id": id, "status": "expired"}))
                        }
                        _ => err(StatusCode::BAD_REQUEST, "session is not open"),
                    }
                }
                ("GET", p) if p.starts_with("/v1/checkout/sessions/") => {
                    let id = p.trim_start_matches("/v1/checkout/sessions/").to_string();
                    let st = s.sessions.get(&id).cloned().unwrap_or_else(|| "expired".into());
                    ok(json!({"id": id, "status": st}))
                }
                ("GET", "/v1/subscriptions") => {
                    let q = parts.uri.query().unwrap_or("");
                    let get = |k: &str| q.split('&').find_map(|kv| kv.strip_prefix(&format!("{k}="))).map(str::to_string);
                    let cus = get("customer").unwrap_or_default();
                    let after = get("starting_after");
                    let all = s.customer_subs.get(&cus).cloned().unwrap_or_default();
                    let start = match &after {
                        Some(a) => all.iter().position(|x| x == a).map(|i| i + 1).unwrap_or(all.len()),
                        None => 0,
                    };
                    let size = s.page_size.unwrap_or(usize::MAX);
                    let page: Vec<String> = all.iter().skip(start).take(size).cloned().collect();
                    let has_more = start + page.len() < all.len();
                    let data: Vec<Value> =
                        page.iter().map(|id| s.sub_value(id).unwrap_or_else(|| sub_json(id, "canceled"))).collect();
                    ok(json!({"data": data, "has_more": has_more}))
                }
                ("GET", p) if p.starts_with("/v1/subscriptions/") => {
                    let id = p.trim_start_matches("/v1/subscriptions/").to_string();
                    match s.sub_value(&id) {
                        Some(v) => ok(v),
                        None => err(StatusCode::NOT_FOUND, "no such subscription"),
                    }
                }
                ("POST", p) if p.starts_with("/v1/subscriptions/") => {
                    let id = p.trim_start_matches("/v1/subscriptions/").to_string();
                    s.posts.push((path.clone(), form, idem));
                    let st = s.subs.get(&id).and_then(|v| v.last().cloned()).unwrap_or_else(|| "active".into());
                    ok(sub_json(&id, &st))
                }
                ("DELETE", p) if p.starts_with("/v1/subscriptions/") => {
                    let id = p.trim_start_matches("/v1/subscriptions/").to_string();
                    s.subs.insert(id.clone(), vec!["canceled".into()]);
                    ok(sub_json(&id, "canceled"))
                }
                ("GET", p) if p.starts_with("/v1/invoices/") => {
                    let id = p.trim_start_matches("/v1/invoices/").to_string();
                    let pi = s.invoice_pi.get(&id).cloned();
                    let (status, created) = s.invoices.get(&id).cloned().unwrap_or_else(|| ("open".into(), 0));
                    let voided_at = s.invoice_voided_at.get(&id).copied();
                    ok(json!({"id": id, "status": status, "created": created, "amount_paid": 0, "payment_intent": pi,
                              "status_transitions": {"voided_at": voided_at}}))
                }
                ("GET", p) if p.starts_with("/v1/payment_intents/") => {
                    let id = p.trim_start_matches("/v1/payment_intents/").to_string();
                    let st = s.pi_status.get(&id).cloned().unwrap_or_else(|| "requires_payment_method".into());
                    ok(json!({"id": id, "status": st}))
                }
                _ => err(StatusCode::NOT_FOUND, "unhandled"),
            }
        }
    }));
    let base = serve(app).await;
    (Arc::new(StripeClient::new(reqwest::Client::new(), "sk_test_a2").with_base_url(base)), mock)
}

fn app_state(app: &PgPool, stripe: Arc<StripeClient>) -> crate::AppState {
    let mut s = crate::test_app_state_owned_with_db_and_store(app.clone(), None);
    s.stripe = Some(stripe);
    s
}

// ── Fixtures ─────────────────────────────────────────────────────────────

async fn new_user(owner: &PgPool) -> (Uuid, String) {
    let id = Uuid::new_v4();
    let clerk = format!("user_{}", id.simple());
    sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
        .bind(id)
        .bind(format!("a2-{}@test.example", id.simple()))
        .bind(&clerk)
        .execute(owner)
        .await
        .expect("insert user");
    (id, clerk)
}

fn ctx(user: Uuid) -> UserContext {
    UserContext { user_id: user, account_id: Uuid::nil(), role: "owner".into(), device_id: "a2-test".into() }
}

fn req(name: &str, seats: i64) -> CreateOrgRequest {
    CreateOrgRequest { name: name.into(), seats, success_url: URL.into(), cancel_url: URL.into() }
}

const CAPS: Caps = Caps {
    max_pending_per_user: 2,
    max_per_user_per_day: 3,
    max_per_hour: 100_000,
    max_per_domain_per_hour: 100_000,
    max_freemail_per_hour: 100_000,
};

async fn create(
    state: &crate::AppState,
    clerk: &ClerkAdminClient,
    user: Uuid,
    key: &str,
    r: CreateOrgRequest,
    caps: &Caps,
) -> (StatusCode, Value) {
    let u = ctx(user);
    body(crate::middleware::auth::with_request_authority_for_test(true, create_org_with(state, clerk, &u, Some(key), r, caps)).await).await
}

/// A self-serve org as `POST /v1/orgs` leaves it, written by fixture: Clerk
/// id, `accounts` row, intent row, members (`(user, role)`), a customer
/// mapping. Returns the account id.
async fn seed_org(owner: &PgPool, org: &str, creator: Uuid, members: &[(Uuid, &str)], created_hours_ago: i64) -> Uuid {
    let account = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(creator)
        .bind(org)
        .execute(owner)
        .await
        .expect("account");
    let standard: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
        .fetch_one(owner)
        .await
        .unwrap();
    for (u, role) in members {
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, $4)")
            .bind(account)
            .bind(u)
            .bind(role)
            .bind(standard)
            .execute(owner)
            .await
            .expect("member");
    }
    sqlx::query(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, clerk_org_id, account_id, created_at) \
         VALUES ($1, $2, 'Seeded', 2, 'created', $3, $4, now() - make_interval(hours => $5))",
    )
    .bind(creator)
    .bind(format!("seed-{}", Uuid::new_v4().simple()))
    .bind(org)
    .bind(account)
    .bind(created_hours_ago as i32)
    .execute(owner)
    .await
    .expect("intent");
    account
}

async fn mirror(owner: &PgPool, account: Uuid, customer: &str, sub: Option<&str>, status: &str) {
    fin(
        owner,
        account,
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, $4)",
        )
        .bind(account)
        .bind(customer)
        .bind(sub)
        .bind(status),
    )
    .await;
}

/// A failed job as A1 leaves it (`clerk_org_cleanup = 'pending'`), written
/// by the owner (the app role cannot write the job table).
async fn failed_job(owner: &PgPool, org: &str, account: Uuid, failed_hours_ago: i64) {
    let slug = crate::tenancy::provisioning::opaque_slug_for_account(account);
    sqlx::query(
        "INSERT INTO tenant_provisioning_jobs (clerk_org_id, account_id, slug, db_secret_ref, step, status, \
             last_error, clerk_org_cleanup, failed_at) \
         VALUES ($1, $2, $3, $4, 'cleanup', 'failed', 'neon: 500 at https://console.neon.tech dsn=postgres://secret', \
                 'pending', now() - make_interval(hours => $5))",
    )
    .bind(org)
    .bind(account)
    .bind(&slug)
    .bind(crate::tenancy::provisioning::secret_ref_for_slug(&slug))
    .bind(failed_hours_ago as i32)
    .execute(owner)
    .await
    .expect("failed job");
}

async fn job_row(owner: &PgPool, org: &str) -> (String, String, i32) {
    sqlx::query_as("SELECT status, clerk_org_cleanup, clerk_org_cleanup_requeues FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(org)
        .fetch_one(owner)
        .await
        .expect("job row")
}

async fn drop_org(owner: &PgPool, org: &str) {
    let account: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(org)
        .fetch_optional(owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM org_selfserve_creations WHERE clerk_org_id = $1").bind(org).execute(owner).await.ok();
    sqlx::query("DELETE FROM tenant_provisioning_jobs WHERE clerk_org_id = $1").bind(org).execute(owner).await.ok();
    if let Some(a) = account {
        sqlx::query("DELETE FROM org_trial_claims WHERE account_id = $1").bind(a).execute(owner).await.ok();
        let mut tx = crate::tenancy::scope::begin_account_scoped(owner, a).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, a).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id = $1").bind(a).execute(&mut *tx).await.ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(a).execute(&mut *tx).await.expect("drop account");
        tx.commit().await.unwrap();
    }
}

async fn drop_users(owner: &PgPool, users: &[Uuid]) {
    for u in users {
        sqlx::query("DELETE FROM users WHERE id = $1").bind(u).execute(owner).await.ok();
    }
}

async fn price_standard(owner: &PgPool) -> Option<String> {
    set_seat_tier_price(owner, "standard", Some(&format!("price_std_a2_{}", Uuid::new_v4().simple()))).await
}

// ── POST /v1/orgs ────────────────────────────────────────────────────────

/// A PAT, add-in or MCP/OAuth client is refused before
/// anything is read; so is an unverified email; the seat floor is 2.
#[tokio::test]
async fn create_refuses_non_interactive_credentials_unverified_email_and_one_seat() {
    const NAME: &str = "create_refuses_non_interactive_credentials_unverified_email_and_one_seat";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, false);
    let key = Uuid::new_v4().to_string();

    let r = crate::middleware::auth::with_request_authority_for_test(
        false,
        create_org_with(&state, &clerk, &ctx(u), Some(&key), req("Acme", 3), &CAPS),
    )
    .await;
    let (s1, b1) = body(r).await;
    let (s2, b2) = create(&state, &clerk, u, &key, req("Acme", 3), &CAPS).await;
    cmock.add_user(&clerk_id, true);
    let (s3, b3) = create(&state, &clerk, u, &key, req("Acme", 1), &CAPS).await;
    let (s4, b4) = {
        let r = crate::middleware::auth::with_request_authority_for_test(
            true,
            create_org_with(&state, &clerk, &ctx(u), None, req("Acme", 3), &CAPS),
        )
        .await;
        body(r).await
    };
    let intents: i64 = sqlx::query_scalar("SELECT count(*) FROM org_selfserve_creations WHERE user_id = $1")
        .bind(u)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    drop_users(&p.owner, &[u]).await;

    assert_eq!((s1, b1["code"].as_str()), (StatusCode::FORBIDDEN, Some("interactive_session_required")), "{b1}");
    assert_eq!((s2, b2["code"].as_str()), (StatusCode::FORBIDDEN, Some("email_unverified")), "{b2}");
    assert_eq!((s3, b3["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_seats")), "{b3}");
    assert_eq!((s4, b4["code"].as_str()), (StatusCode::BAD_REQUEST, Some("idempotency_key_required")), "{b4}");
    assert_eq!(intents, 0, "nothing was reserved");
    assert!(cmock.creates().is_empty(), "no Clerk org was created");
    assert!(smock.calls("POST", "/v1/checkout/sessions").is_empty());
}

/// Two submits with one key, concurrently: one org, one Checkout, the same
/// answer twice. The org gets no `free` grant (not even when Clerk's
/// webhook path reaches `ensure_org_account` afterwards), the creator is its
/// admin, Clerk's cap is lifted and its slug is opaque. Reusing the key for
/// a different org is refused.
#[tokio::test]
async fn a_double_submit_returns_the_same_org_and_checkout_and_grants_nothing() {
    const NAME: &str = "a_double_submit_returns_the_same_org_and_checkout_and_grants_nothing";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    let key = Uuid::new_v4().to_string();

    let (a, b) = tokio::join!(
        create(&state, &clerk, u, &key, req("Acme Corp", 3), &CAPS),
        create(&state, &clerk, u, &key, req("Acme Corp", 3), &CAPS),
    );
    let (s3, b3) = create(&state, &clerk, u, &key, req("Acme Corp", 3), &CAPS).await;
    let (s4, b4) = create(&state, &clerk, u, &key, req("Acme Corp", 4), &CAPS).await;
    let org = [&a, &b]
        .iter()
        .find_map(|r| r.1["org_id"].as_str())
        .unwrap_or_default()
        .to_string();
    // Clerk's webhook (or a first org JWT) reaching ensure_org_account now.
    let via_webhook = crate::clerk_provision::ensure_org_account(&state, &p.app, &org, u, Some("Acme Corp")).await;
    let account: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(&org)
        .fetch_optional(&p.owner)
        .await
        .unwrap();
    let account = account.unwrap_or_default();
    let grants: i64 = sqlx::query_scalar("SELECT count(*) FROM grants WHERE account_id = $1")
        .bind(account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let role: Option<String> = sqlx::query_scalar("SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account)
        .bind(u)
        .fetch_optional(&p.owner)
        .await
        .unwrap();
    let trial: Option<(String, i32)> = sqlx::query_as("SELECT state, seats FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_optional(&p.owner)
        .await
        .unwrap();
    let intent_id: Uuid = sqlx::query_scalar("SELECT id FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let jobs: i64 = sqlx::query_scalar("SELECT count(*) FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
        .bind(&org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let status = body(
        crate::middleware::auth::with_request_authority_for_test(true, get_provisioning_with(&state, &ctx(u), &org)).await,
    )
    .await;
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    // S1: one creates; the other either waited out the lease and replays,
    // or is told the sign-up is in progress (409 create_in_progress).
    let (first, second) = if a.0 == StatusCode::CREATED { (&a, &b) } else { (&b, &a) };
    assert_eq!(first.0, StatusCode::CREATED, "{} / {}", a.1, b.1);
    match second.0 {
        StatusCode::OK => assert_eq!(second.1["checkout_url"], first.1["checkout_url"]),
        StatusCode::CONFLICT => {
            assert_eq!(second.1["code"], "create_in_progress", "{}", second.1);
            assert!(second.1["retry_after_s"].as_i64().unwrap() >= 1);
        }
        other => panic!("unexpected {other}: {}", second.1),
    }
    let a = first.clone();
    assert!(a.1["checkout_url"].as_str().unwrap().starts_with("https://checkout.stripe.com/"), "{}", a.1);
    assert_eq!(a.1["status"], "awaiting_payment");
    assert_eq!((a.1["trial"].as_bool(), a.1["seats"].as_i64()), (Some(true), Some(3)));
    assert_eq!((s3, &b3["checkout_url"], b3["replayed"].as_bool()), (StatusCode::OK, &a.1["checkout_url"], Some(true)));
    assert_eq!((s4, b4["code"].as_str()), (StatusCode::CONFLICT, Some("idempotency_key_reused")), "{b4}");
    assert_eq!(cmock.creates().len(), 1, "one Clerk org");
    assert_eq!(smock.calls("POST", "/v1/checkout/sessions").len(), 1, "one Checkout");
    let created = &cmock.creates()[0];
    assert_eq!(created["max_allowed_memberships"], 3, "S4: Clerk's member cap is the seats being bought, not unlimited");
    assert_eq!(created["slug"], json!(clerk_slug_for_intent(intent_id)), "opaque, server-picked slug");
    assert_eq!(created["private_metadata"][SELFSERVE_ORG_MARKER], true);
    assert_eq!(created["private_metadata"][SELFSERVE_INTENT_MARKER], json!(intent_id.to_string()));
    assert_eq!(via_webhook.ok(), Some(account), "the webhook path finds the same account");
    assert_eq!(grants, 0, "no free plan before the card");
    assert_eq!(role.as_deref(), Some("admin"));
    assert_eq!(trial, Some(("checkout_open".into(), 3)));
    assert_eq!(jobs, 0, "provisioning is the Checkout webhook's to enqueue");
    assert_eq!((status.0, status.1["status"].as_str()), (StatusCode::OK, Some("awaiting_payment")), "{}", status.1);
    assert_eq!(status.1["seats"], 3, "the status body carries the seats (web PR B's resume payment)");
}

/// An attempt that died after Clerk created the org is adopted by its
/// marker, never created a second time.
#[tokio::test]
async fn a_crashed_attempt_is_adopted_by_its_marker_not_created_twice() {
    const NAME: &str = "a_crashed_attempt_is_adopted_by_its_marker_not_created_twice";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    let key = Uuid::new_v4().to_string();
    let intent: Uuid = sqlx::query_scalar(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats) VALUES ($1, $2, 'Beta Co', 2) RETURNING id",
    )
    .bind(u)
    .bind(&key)
    .fetch_one(&p.owner)
    .await
    .unwrap();
    let orphan = format!("org_{}", Uuid::new_v4().simple());
    cmock.add_org(json!({"id": orphan, "name": "Beta Co", "created_by": clerk_id,
        "private_metadata": {SELFSERVE_ORG_MARKER: true, SELFSERVE_INTENT_MARKER: intent.to_string()}}));

    let (s, b) = create(&state, &clerk, u, &key, req("Beta Co", 2), &CAPS).await;
    drop_org(&p.owner, &orphan).await;
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(s, StatusCode::CREATED, "{b}");
    assert_eq!(b["org_id"], json!(orphan), "the orphan was adopted");
    assert!(cmock.creates().is_empty(), "no second Clerk org");
}

/// #1556: a Clerk refusal that is not about the name (staging's
/// `organization_slugs_disabled` 403, a plan limit 422) is a retry-later 503,
/// never "try a different name"; a genuine slug/name refusal still is.
#[tokio::test]
async fn a_clerk_config_refusal_is_a_503_and_only_a_name_refusal_says_try_a_different_name() {
    const NAME: &str = "a_clerk_config_refusal_is_a_503_and_only_a_name_refusal_says_try_a_different_name";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);

    let mut seen = Vec::new();
    for (status, code) in [
        (403u16, "organization_slugs_disabled"),
        (422, "plan_limit_exceeded"),
        (401, "authentication_invalid"),
        (422, "form_param_value_invalid"),
    ] {
        cmock.refuse_creates_with(status, code);
        let key = Uuid::new_v4().to_string();
        let (s, b) = create(&state, &clerk, u, &key, req("Gamma Co", 2), &CAPS).await;
        let intent: String =
            sqlx::query_scalar("SELECT status FROM org_selfserve_creations WHERE user_id = $1 AND idempotency_key = $2")
                .bind(u)
                .bind(&key)
                .fetch_one(&p.owner)
                .await
                .unwrap();
        seen.push((status, code, s, b, intent));
    }
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    for (status, code, s, b, intent) in &seen[..3] {
        assert_eq!(*s, StatusCode::SERVICE_UNAVAILABLE, "{status} {code}: {b}");
        assert_eq!(b["code"], json!("org_create_unavailable"), "{status} {code}");
        assert!(!b["error"].as_str().unwrap().contains("different name"), "{b}");
        assert_eq!(intent, "refused", "stops counting against the caps");
    }
    let (_, _, s, b, _) = &seen[3];
    assert_eq!(*s, StatusCode::UNPROCESSABLE_ENTITY, "{b}");
    assert_eq!(b["code"], json!("org_create_refused"));
}

/// At most 2 unpaid orgs per creator at once, and 3 per day.
#[tokio::test]
async fn creation_caps_refuse_past_the_pending_and_daily_limits() {
    const NAME: &str = "creation_caps_refuse_past_the_pending_and_daily_limits";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    let mut orgs = Vec::new();
    let mut results = Vec::new();
    for i in 0..3 {
        let (s, b) = create(&state, &clerk, u, &Uuid::new_v4().to_string(), req(&format!("Org {i}"), 2), &CAPS).await;
        if let Some(o) = b["org_id"].as_str() {
            orgs.push(o.to_string());
        }
        results.push((s, b));
    }
    // One of them is paid: the sweep marks it kept.
    sqlx::query("UPDATE org_selfserve_creations SET sweep_state = 'kept' WHERE clerk_org_id = $1")
        .bind(&orgs[0])
        .execute(&p.owner)
        .await
        .unwrap();
    let (s4, b4) = create(&state, &clerk, u, &Uuid::new_v4().to_string(), req("Org 3", 2), &CAPS).await;
    if let Some(o) = b4["org_id"].as_str() {
        orgs.push(o.to_string());
    }
    sqlx::query("UPDATE org_selfserve_creations SET sweep_state = 'kept' WHERE user_id = $1")
        .bind(u)
        .execute(&p.owner)
        .await
        .unwrap();
    let (s5, b5) = create(&state, &clerk, u, &Uuid::new_v4().to_string(), req("Org 4", 2), &CAPS).await;
    for o in &orgs {
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(results[0].0, StatusCode::CREATED);
    assert_eq!(results[1].0, StatusCode::CREATED);
    assert_eq!((results[2].0, results[2].1["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("too_many_pending_orgs")));
    assert_eq!(s4, StatusCode::CREATED, "one paid frees a pending slot: {b4}");
    assert_eq!((s5, b5["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("org_create_rate_limited")), "{b5}");
    assert!(b5["retry_after_s"].as_i64().unwrap() > 0);
    assert_eq!(cmock.creates().len(), 3, "a refused attempt creates nothing in Clerk");
}

// ── Status and retry ─────────────────────────────────────────────────────

/// Only an owner/admin of THAT org may read its status or retry it; another
/// org's admin and a plain member get the same 404 as a stranger. A retry
/// is rate limited per org, counted only when it is accepted, and refused
/// while the sweep holds the org's cleanup claim.
#[tokio::test]
async fn retry_and_status_are_for_this_orgs_admins_only_and_rate_limited() {
    const NAME: &str = "retry_and_status_are_for_this_orgs_admins_only_and_rate_limited";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (owner_a, _) = new_user(&p.owner).await;
    let (member_a, _) = new_user(&p.owner).await;
    let (owner_b, _) = new_user(&p.owner).await;
    let org_a = format!("org_{}", Uuid::new_v4().simple());
    let org_b = format!("org_{}", Uuid::new_v4().simple());
    let acc_a = seed_org(&p.owner, &org_a, owner_a, &[(owner_a, "owner"), (member_a, "member")], 0).await;
    let acc_b = seed_org(&p.owner, &org_b, owner_b, &[(owner_b, "admin")], 0).await;
    failed_job(&p.owner, &org_a, acc_a, 0).await;
    failed_job(&p.owner, &org_b, acc_b, 0).await;
    let limits = RetryLimits { min_interval_secs: 600, max_retries: 2 };
    let as_user = |u: Uuid, interactive: bool| {
        let state = &state;
        let org_a = org_a.clone();
        async move {
            body(
                crate::middleware::auth::with_request_authority_for_test(
                    interactive,
                    retry_provisioning_with(state, &ctx(u), &org_a, &limits),
                )
                .await,
            )
            .await
        }
    };
    let status_for = |u: Uuid| {
        let state = &state;
        let org_a = org_a.clone();
        async move {
            body(crate::middleware::auth::with_request_authority_for_test(true, get_provisioning_with(state, &ctx(u), &org_a)).await)
                .await
        }
    };

    let other_admin_status = status_for(owner_b).await;
    let member_status = status_for(member_a).await;
    let failed_status = status_for(owner_a).await;
    let other_admin = as_user(owner_b, true).await;
    let member = as_user(member_a, true).await;
    let not_interactive = as_user(owner_a, false).await;
    let first = as_user(owner_a, true).await;
    let after_first = job_row(&p.owner, &org_a).await;
    // Fail it again; a retry right away is rate limited.
    let refail = "UPDATE tenant_provisioning_jobs SET status = 'failed', step = 'cleanup', clerk_org_cleanup = 'pending', \
                  failed_at = now() WHERE clerk_org_id = $1";
    sqlx::query(refail).bind(&org_a).execute(&p.owner).await.unwrap();
    let too_soon = as_user(owner_a, true).await;
    sqlx::query("UPDATE org_selfserve_creations SET last_retry_at = now() - interval '1 hour' WHERE clerk_org_id = $1")
        .bind(&org_a)
        .execute(&p.owner)
        .await
        .unwrap();
    // The sweep holds the org: refused, and the refusal spends nothing.
    let sweep_claim = crate::tenancy::provisioning::claim_clerk_org_cleanup(&p.app, &org_a, std::time::Duration::from_secs(600))
        .await
        .unwrap()
        .expect("claimed");
    let while_sweeping = as_user(owner_a, true).await;
    let count_after_refusal: i32 = sqlx::query_scalar("SELECT retry_count FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&org_a)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    assert_eq!(
        crate::tenancy::provisioning::release_clerk_org_cleanup(&p.app, &org_a, CleanupRelease::Pending, sweep_claim, 2).await.unwrap(),
        crate::tenancy::provisioning::ReleaseOutcome::Pending
    );
    let second = as_user(owner_a, true).await;
    sqlx::query(refail).bind(&org_a).execute(&p.owner).await.unwrap();
    sqlx::query("UPDATE org_selfserve_creations SET last_retry_at = now() - interval '1 hour' WHERE clerk_org_id = $1")
        .bind(&org_a)
        .execute(&p.owner)
        .await
        .unwrap();
    let over_limit = as_user(owner_a, true).await;
    let b_untouched = job_row(&p.owner, &org_b).await;
    drop_org(&p.owner, &org_a).await;
    drop_org(&p.owner, &org_b).await;
    drop_users(&p.owner, &[owner_a, member_a, owner_b]).await;

    assert_eq!((other_admin_status.0, other_admin_status.1["code"].as_str()), (StatusCode::NOT_FOUND, Some("org_not_found")));
    assert_eq!(member_status.0, StatusCode::NOT_FOUND);
    assert_eq!(failed_status.0, StatusCode::OK);
    assert_eq!(failed_status.1["status"], "failed");
    assert_eq!(failed_status.1["retryable"], true);
    let text = failed_status.1.to_string();
    assert!(!text.contains("neon") && !text.contains("postgres") && !text.contains("TENANT_DB"), "no secrets: {text}");
    assert_eq!((other_admin.0, other_admin.1["code"].as_str()), (StatusCode::NOT_FOUND, Some("org_not_found")), "another org's admin");
    assert_eq!(member.0, StatusCode::NOT_FOUND, "a plain member");
    assert_eq!((not_interactive.0, not_interactive.1["code"].as_str()), (StatusCode::FORBIDDEN, Some("interactive_session_required")));
    assert_eq!((first.0, first.1["status"].as_str()), (StatusCode::ACCEPTED, Some("provisioning")), "{}", first.1);
    assert_eq!(after_first.0, "provisioning");
    assert_eq!((too_soon.0, too_soon.1["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("retry_rate_limited")));
    assert_eq!((while_sweeping.0, while_sweeping.1["code"].as_str()), (StatusCode::CONFLICT, Some("not_retryable")));
    assert_eq!(count_after_refusal, 1, "a refused retry is not counted");
    assert_eq!(second.0, StatusCode::ACCEPTED, "{}", second.1);
    assert_eq!((over_limit.0, over_limit.1["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("retry_limit_reached")));
    assert_eq!(b_untouched, ("failed".to_string(), "pending".to_string(), 0), "the other org's job was never touched");
}

// ── The cleanup sweep ────────────────────────────────────────────────────

/// Failed jobs, decided by the subscription AT STRIPE (#1517 S9):
/// live → requeued (then held once the budget is spent), unpaid → canceled
/// at Stripe then the Clerk org deleted, none → the Clerk org deleted. A job
/// inside the grace is left alone.
#[tokio::test]
async fn the_sweep_requeues_a_paid_failed_org_and_cancels_or_deletes_an_unpaid_one() {
    const NAME: &str = "the_sweep_requeues_a_paid_failed_org_and_cancels_or_deletes_an_unpaid_one";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let org = |tag: &str| format!("org_{tag}{}", Uuid::new_v4().simple());
    let (paid, unpaid, none, fresh) = (org("paid"), org("unpaid"), org("none"), org("fresh"));
    let mut accounts = HashMap::new();
    for o in [&paid, &unpaid, &none, &fresh] {
        let a = seed_org(&p.owner, o, u, &[(u, "admin")], 48).await;
        cmock.add_org(json!({"id": o, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
        failed_job(&p.owner, o, a, if *o == fresh { 0 } else { 2 }).await;
        accounts.insert(o.clone(), a);
    }
    mirror(&p.owner, accounts[&paid], "cus_paid", Some("sub_paid"), "past_due").await; // mirror lags Stripe
    smock.set_sub("cus_paid", "sub_paid", "active");
    mirror(&p.owner, accounts[&unpaid], "cus_unpaid", Some("sub_unpaid"), "past_due").await;
    smock.set_sub("cus_unpaid", "sub_unpaid", "past_due");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let r1 = sweeper.sweep_once(chrono::Utc::now()).await;
    let after = [job_row(&p.owner, &paid).await, job_row(&p.owner, &unpaid).await, job_row(&p.owner, &none).await, job_row(&p.owner, &fresh).await];
    // The paid org fails again, twice more: requeued once more, then held.
    let refail = "UPDATE tenant_provisioning_jobs SET status = 'failed', step = 'cleanup', clerk_org_cleanup = 'pending', \
                  failed_at = now() - interval '2 hours' WHERE clerk_org_id = $1";
    sqlx::query(refail).bind(&paid).execute(&p.owner).await.unwrap();
    let r2 = sweeper.sweep_once(chrono::Utc::now()).await;
    sqlx::query(refail).bind(&paid).execute(&p.owner).await.unwrap();
    let r3 = sweeper.sweep_once(chrono::Utc::now()).await;
    let held = job_row(&p.owner, &paid).await;
    // A held org is still the owner's to retry.
    let owner_retry = crate::tenancy::provisioning::retry(&p.app, &paid).await;
    let deletes = cmock.deletes();
    let cancels = smock.calls("DELETE", "/v1/subscriptions/");
    for o in [&paid, &unpaid, &none, &fresh] {
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;

    assert!(r1.errors.is_empty(), "{r1:?}");
    assert!(r1.requeued.contains(&paid), "{r1:?}");
    assert_eq!(after[0], ("provisioning".to_string(), "none".to_string(), 1), "paid at Stripe: requeued, not deleted");
    assert_eq!(after[1].1, "done");
    assert_eq!(after[2].1, "done");
    assert_eq!(after[3], ("failed".to_string(), "pending".to_string(), 0), "inside the grace: untouched");
    assert!(!deletes.contains(&paid) && !deletes.contains(&fresh), "{deletes:?}");
    assert!(deletes.contains(&unpaid) && deletes.contains(&none), "{deletes:?}");
    assert_eq!(cancels.len(), 1, "only the unpaid subscription is canceled: {cancels:?}");
    assert_eq!(cancels[0].1, "/v1/subscriptions/sub_unpaid");
    assert_eq!(
        cancels[0].2.as_deref(),
        Some(crate::tenancy::org_selfserve_cleanup::cancel_key(accounts[&unpaid], "sub_unpaid").as_str()),
        "keyed on the subscription"
    );
    assert_eq!(smock.sub_status("sub_paid").as_deref(), Some("active"), "a paying org is never canceled");
    assert!(r2.requeued.contains(&paid), "{r2:?}");
    assert!(r3.held.contains(&paid), "{r3:?}");
    assert_eq!(held, ("failed".to_string(), "held".to_string(), 2));
    assert!(owner_retry.is_ok(), "{owner_retry:?}");
}

/// Abandoned sign-ups: an expired-or-expirable Checkout with nothing live is
/// deleted (Clerk org, accounts row); one whose Checkout completed is kept;
/// an unpaid subscription on the customer is canceled first; a trialing org
/// is marked kept and never looked at again.
#[tokio::test]
async fn the_sweep_deletes_an_abandoned_signup_and_keeps_a_completed_one() {
    const NAME: &str = "the_sweep_deletes_an_abandoned_signup_and_keeps_a_completed_one";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let org = |tag: &str| format!("org_{tag}{}", Uuid::new_v4().simple());
    let (gone, completed, unpaid, trialing, recent) = (org("gone"), org("completed"), org("unpaid"), org("trialing"), org("recent"));
    let mut accounts = HashMap::new();
    for o in [&gone, &completed, &unpaid, &trialing, &recent] {
        let a = seed_org(&p.owner, o, u, &[(u, "admin")], 30).await;
        cmock.add_org(json!({"id": o, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
        let cus = format!("cus_{o}");
        mirror(&p.owner, a, &cus, None, "pending").await;
        let session = format!("cs_{}", Uuid::new_v4().simple());
        let (state, ended): (&str, Option<i64>) = match o {
            o if *o == trialing => ("trialing", None),
            o if *o == recent => ("abandoned", Some(0)),
            _ => ("abandoned", Some(3)),
        };
        let q = if state == "trialing" {
            "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, \
                 checkout_opened_at, stripe_subscription_id, pool_baseline, trial_started_at, trial_ends_at) \
             VALUES ($1, 'trialing', $2, 2, true, $3, now() - interval '30 hours', 'sub_t', 3000, now(), now() + interval '90 days')"
        } else {
            "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, \
                 checkout_opened_at, ended_at, end_reason) \
             VALUES ($1, 'abandoned', $2, 2, true, $3, now() - interval '30 hours', now() - make_interval(hours => $4), \
                     'checkout not completed within 24 hours')"
        };
        fin(&p.owner, a, sqlx::query(q).bind(a).bind(u).bind(&session).bind(ended.unwrap_or(0) as i32)).await;
        smock.set_session(&session, if *o == completed { "complete" } else if *o == gone { "open" } else { "expired" });
        accounts.insert(o.clone(), (a, session));
    }
    smock.set_sub(&format!("cus_{unpaid}"), "sub_abandoned_unpaid", "incomplete");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let report = sweeper.sweep_once(chrono::Utc::now()).await;
    let state_of = |o: &String| {
        let o = o.clone();
        let owner = p.owner.clone();
        async move {
            let row: (String, Option<Uuid>) =
                sqlx::query_as("SELECT sweep_state, account_id FROM org_selfserve_creations WHERE clerk_org_id = $1")
                    .bind(&o)
                    .fetch_one(&owner)
                    .await
                    .unwrap();
            let account: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM accounts WHERE clerk_org_id = $1)")
                .bind(&o)
                .fetch_one(&owner)
                .await
                .unwrap();
            (row.0, account)
        }
    };
    let gone_s = state_of(&gone).await;
    let completed_s = state_of(&completed).await;
    let unpaid_s = state_of(&unpaid).await;
    let trialing_s = state_of(&trialing).await;
    let recent_s = state_of(&recent).await;
    let gone_trial: i64 = sqlx::query_scalar("SELECT count(*) FROM org_trials WHERE account_id = $1")
        .bind(accounts[&gone].0)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let report2 = sweeper.sweep_once(chrono::Utc::now()).await;
    let deletes = cmock.deletes();
    let gone_session = smock.session_status(&accounts[&gone].1);
    let unpaid_sub = smock.sub_status("sub_abandoned_unpaid");
    for o in [&gone, &completed, &unpaid, &trialing, &recent] {
        sqlx::query("DELETE FROM org_selfserve_creations WHERE clerk_org_id = $1").bind(o).execute(&p.owner).await.ok();
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;

    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(gone_s, ("deleted".to_string(), false), "Clerk org and account deleted");
    assert_eq!(gone_trial, 0, "the trial row went with the account");
    assert_eq!(gone_session.as_deref(), Some("expired"), "its open Checkout was expired first");
    assert_eq!(completed_s, ("none".to_string(), true), "a completed Checkout keeps the org (released)");
    assert!(report.released.contains(&completed), "{report:?}");
    assert_eq!(unpaid_s, ("deleted".to_string(), false));
    assert_eq!(unpaid_sub.as_deref(), Some("canceled"), "the unpaid subscription was canceled, no charge");
    assert_eq!(trialing_s, ("kept".to_string(), true));
    assert_eq!(recent_s, ("none".to_string(), true), "inside the grace");
    assert!(deletes.contains(&gone) && deletes.contains(&unpaid), "{deletes:?}");
    assert!(!deletes.contains(&completed) && !deletes.contains(&trialing) && !deletes.contains(&recent), "{deletes:?}");
    assert!(report2.abandoned_deleted.is_empty() && !report2.kept.contains(&trialing), "settled rows are not revisited: {report2:?}");
}

/// The claims are single conditional writes: of many concurrent claimers
/// exactly one wins; a claim and an owner retry never both succeed; a stale
/// claim is taken over and the old holder can no longer write; "done" needs
/// the claim.
#[tokio::test]
async fn the_cleanup_claims_are_atomic_under_concurrency() {
    const NAME: &str = "the_cleanup_claims_are_atomic_under_concurrency";
    let Some(p) = pools(NAME).await else { return };
    let (u, _) = new_user(&p.owner).await;
    let org = format!("org_claim{}", Uuid::new_v4().simple());
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 0).await;
    failed_job(&p.owner, &org, account, 0).await;
    let stale = std::time::Duration::from_secs(600);
    use crate::tenancy::provisioning as prov;

    let done_without_claim = prov::mark_clerk_org_cleanup_done(&p.app, &org, chrono::Utc::now()).await.unwrap();
    let claims = futures::future::join_all((0..8).map(|_| prov::claim_clerk_org_cleanup(&p.app, &org, stale))).await;
    let job_winners = claims.iter().filter(|r| matches!(r, Ok(Some(_)))).count();
    // S6: a stale job claim taken over; the old holder can neither release
    // nor mark done.
    let old_token = claims.iter().find_map(|r| r.as_ref().ok().copied().flatten()).unwrap();
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_claimed_at = now() - interval '1 hour' WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    let backdated: chrono::DateTime<chrono::Utc> =
        sqlx::query_scalar("SELECT clerk_org_cleanup_claimed_at FROM tenant_provisioning_jobs WHERE clerk_org_id = $1")
            .bind(&org)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let new_token = prov::claim_clerk_org_cleanup(&p.app, &org, stale).await.unwrap().expect("stale takeover");
    let old_release = prov::release_clerk_org_cleanup(&p.app, &org, CleanupRelease::Pending, backdated, 2).await.unwrap();
    let old_done = prov::mark_clerk_org_cleanup_done(&p.app, &org, old_token).await.unwrap();
    let new_holds = prov::holds_clerk_org_cleanup(&p.app, &org, new_token).await.unwrap();
    let all_ok = claims.iter().all(|r| r.is_ok());

    // Claim vs the owner's retry, ten rounds.
    let mut both_won = 0;
    let mut neither = 0;
    for _ in 0..10 {
        sqlx::query(
            "UPDATE tenant_provisioning_jobs SET status = 'failed', step = 'cleanup', clerk_org_cleanup = 'pending', \
             clerk_org_cleanup_claimed_at = NULL, failed_at = now() WHERE clerk_org_id = $1",
        )
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
        let (c, r) = tokio::join!(prov::claim_clerk_org_cleanup(&p.app, &org, stale), prov::retry(&p.app, &org));
        match (c.unwrap().is_some(), r.is_ok()) {
            (true, true) => both_won += 1,
            (false, false) => neither += 1,
            _ => {}
        }
    }

    // Stale takeover on the intent claim.
    let intent: Uuid = sqlx::query_scalar("SELECT id FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let intent_claims =
        futures::future::join_all((0..8).map(|_| crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, intent, stale))).await;
    let intent_winners: Vec<_> = intent_claims.into_iter().filter_map(|r| r.unwrap()).collect();
    let fresh_retake = crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, intent, stale).await.unwrap();
    sqlx::query("UPDATE org_selfserve_creations SET sweep_claimed_at = now() - interval '1 hour' WHERE id = $1")
        .bind(intent)
        .execute(&p.owner)
        .await
        .unwrap();
    let takeover = crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, intent, stale).await.unwrap();
    let old_holder_release: u64 = sqlx::query(
        "UPDATE org_selfserve_creations SET sweep_state = 'none', sweep_claimed_at = NULL \
          WHERE id = $1 AND sweep_state = 'deleting' AND sweep_claimed_at = $2",
    )
    .bind(intent)
    .bind(intent_winners.first().copied())
    .execute(&p.app)
    .await
    .unwrap()
    .rows_affected();
    let bad_stale = prov::claim_clerk_org_cleanup(&p.app, &org, std::time::Duration::from_secs(1)).await;
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(!done_without_claim, "done needs the claim");
    assert_eq!(old_release, crate::tenancy::provisioning::ReleaseOutcome::Lost, "the taken-over holder cannot release");
    assert!(!old_done, "the taken-over holder cannot mark done");
    assert!(new_holds, "the new holder holds it");
    assert!(all_ok);
    assert_eq!(job_winners, 1, "exactly one concurrent claimer wins");
    assert_eq!((both_won, neither), (0, 0), "claim and retry: exactly one wins every round");
    assert_eq!(intent_winners.len(), 1, "exactly one intent claimer wins");
    assert!(fresh_retake.is_none(), "a fresh claim is not taken over");
    assert!(takeover.is_some(), "a stale claim is");
    assert_eq!(old_holder_release, 0, "the old holder can no longer write");
    assert!(bad_stale.is_err(), "a stale window under a minute is refused");
}

/// Clerk's `organization.created` webhook (or a first org JWT) racing the
/// route's own `accounts` insert: it blocks on the route's uncommitted row,
/// loses the `ON CONFLICT`, and must then see the intent the route recorded
/// in the same transaction, so it grants no `free` plan.
#[tokio::test]
async fn a_webhook_that_loses_the_account_insert_race_grants_nothing() {
    const NAME: &str = "a_webhook_that_loses_the_account_insert_race_grants_nothing";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, _smock) = spawn_stripe().await;
    let state = Arc::new(app_state(&p.app, stripe));
    let (u, _) = new_user(&p.owner).await;
    let org = format!("org_race{}", Uuid::new_v4().simple());
    let intent: Uuid = sqlx::query_scalar(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats) VALUES ($1, $2, 'Race', 2) RETURNING id",
    )
    .bind(u)
    .bind(Uuid::new_v4().to_string())
    .fetch_one(&p.app)
    .await
    .unwrap();

    // The route's transaction (`record_created`), held open.
    let mut tx = p.app.begin().await.unwrap();
    sqlx::query("UPDATE org_selfserve_creations SET status = 'created', clerk_org_id = $2 WHERE id = $1")
        .bind(intent)
        .bind(&org)
        .execute(&mut *tx)
        .await
        .unwrap();
    let account = crate::clerk_provision::ensure_selfserve_org_account(&mut tx, &org, u).await.unwrap();
    // The webhook, concurrently: blocks on the uncommitted account row.
    let webhook = {
        let (state, app, org) = (state.clone(), p.app.clone(), org.clone());
        tokio::spawn(async move { crate::clerk_provision::ensure_org_account(&state, &app, &org, u, Some("Race")).await })
    };
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    sqlx::query("UPDATE org_selfserve_creations SET account_id = $2 WHERE id = $1")
        .bind(intent)
        .bind(account)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    let via_webhook = webhook.await.unwrap();
    let grants: i64 = sqlx::query_scalar("SELECT count(*) FROM grants WHERE account_id = $1")
        .bind(account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert_eq!(via_webhook.ok(), Some(account), "the webhook reads the route's account");
    assert_eq!(grants, 0, "no free plan, even for the losing webhook");
}

// ── Security review fixes (round 2) ──────────────────────────────────────

/// An app-role pool with `max` connections and a short acquire timeout: a
/// request that holds one connection while waiting for another shows up as
/// a pool timeout here instead of a hang.
async fn small_app_pool(owner: &PgPool, max: u32) -> PgPool {
    use std::str::FromStr;
    crate::app_role_test_support::set_app_role_password_with_retry(owner).await;
    let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
    let opts = sqlx::postgres::PgConnectOptions::from_str(&url)
        .unwrap()
        .username("coralswarm_app")
        .password(crate::app_role_test_support::APP_ROLE_TEST_PASSWORD);
    sqlx::postgres::PgPoolOptions::new()
        .max_connections(max)
        .acquire_timeout(std::time::Duration::from_secs(5))
        .connect_with(opts)
        .await
        .expect("small app pool")
}

/// S1: 25 concurrent creates on a 5-connection pool all finish (201, 409
/// or 429), none with a pool timeout. Before the lease, each request held a
/// connection for its advisory lock across the Clerk and Stripe calls while
/// `create_locked` asked the pool for more: five requests starved it.
#[tokio::test]
async fn twenty_five_concurrent_creates_on_a_five_connection_pool_never_starve_it() {
    const NAME: &str = "twenty_five_concurrent_creates_on_a_five_connection_pool_never_starve_it";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let small = small_app_pool(&p.owner, 5).await;
    let state = app_state(&small, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    cmock.set_create_delay(150);
    let mut users = Vec::new();
    for _ in 0..20 {
        let (u, clerk_id) = new_user(&p.owner).await;
        cmock.add_user(&clerk_id, true);
        users.push(u);
    }
    // 20 distinct creators, plus 5 extra submits of the first creator's key.
    let shared_key = Uuid::new_v4().to_string();
    let mut calls = Vec::new();
    for (i, u) in users.iter().enumerate() {
        let key = if i == 0 { shared_key.clone() } else { Uuid::new_v4().to_string() };
        calls.push((*u, key, format!("Starve {i}")));
    }
    for _ in 0..5 {
        calls.push((users[0], shared_key.clone(), "Starve 0".to_string()));
    }
    let started = std::time::Instant::now();
    let results = futures::future::join_all(
        calls.iter().map(|(u, key, name)| create(&state, &clerk, *u, key, req(name, 2), &CAPS)),
    )
    .await;
    let elapsed = started.elapsed();
    let orgs: Vec<String> = results.iter().filter_map(|(_, b)| b["org_id"].as_str().map(str::to_string)).collect();
    for o in orgs.iter().collect::<std::collections::BTreeSet<_>>() {
        drop_org(&p.owner, o).await;
    }
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = ANY($1)").bind(&users).execute(&p.owner).await.ok();
    drop_users(&p.owner, &users).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    for (s, b) in &results {
        assert!(
            matches!(*s, StatusCode::CREATED | StatusCode::OK | StatusCode::CONFLICT | StatusCode::TOO_MANY_REQUESTS),
            "every request finishes with 201/200/409/429, never a pool timeout: {s} {b}"
        );
        if *s == StatusCode::CONFLICT {
            assert_eq!(b["code"], "create_in_progress", "{b}");
        }
    }
    let created = results.iter().filter(|(s, _)| *s == StatusCode::CREATED).count();
    assert_eq!(created, 20, "every creator got one org: {results:?}");
    assert_eq!(cmock.creates().len(), 20, "the shared key made one org, not six");
    assert!(elapsed < std::time::Duration::from_secs(30), "no request waited out the pool: {elapsed:?}");
}

/// S5: a repeat create is refused once the sweep has touched the intent or
/// it is more than 24 h old; the sweep cannot claim an intent a request
/// holds; a held lease answers 409 create_in_progress.
#[tokio::test]
async fn a_replay_is_refused_once_the_sweep_owns_the_intent_and_the_sweep_waits_for_the_lease() {
    const NAME: &str = "a_replay_is_refused_once_the_sweep_owns_the_intent_and_the_sweep_waits_for_the_lease";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let (u, clerk_id) = new_user(&p.owner).await;
    cmock.add_user(&clerk_id, true);
    let caps = Caps { max_pending_per_user: 10, max_per_user_per_day: 10, ..CAPS };
    let stale = std::time::Duration::from_secs(600);
    let intent_of = |org: String| {
        let owner = p.owner.clone();
        async move {
            sqlx::query_scalar::<_, Uuid>("SELECT id FROM org_selfserve_creations WHERE clerk_org_id = $1")
                .bind(org)
                .fetch_one(&owner)
                .await
                .unwrap()
        }
    };
    let mut orgs = Vec::new();
    let make = |i: usize| {
        let key = format!("s5-key-{i}-{}", Uuid::new_v4().simple());
        let (state, clerk) = (&state, &clerk);
        async move {
            let (s, b) = create(state, clerk, u, &key, req(&format!("Replay {i}"), 2), &caps).await;
            assert_eq!(s, StatusCode::CREATED, "{b}");
            (key, b["org_id"].as_str().unwrap().to_string())
        }
    };
    let (k_kept, o_kept) = make(0).await;
    let (k_claimed, o_claimed) = make(1).await;
    let (k_old, o_old) = make(2).await;
    let (k_leased, o_leased) = make(3).await;
    orgs.extend([o_kept.clone(), o_claimed.clone(), o_old.clone(), o_leased.clone()]);

    sqlx::query("UPDATE org_selfserve_creations SET sweep_state = 'kept' WHERE clerk_org_id = $1")
        .bind(&o_kept)
        .execute(&p.owner)
        .await
        .unwrap();
    let kept = create(&state, &clerk, u, &k_kept, req("Replay 0", 2), &caps).await;
    let claimed_intent = intent_of(o_claimed.clone()).await;
    let claim = crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, claimed_intent, stale).await.unwrap();
    let claimed = create(&state, &clerk, u, &k_claimed, req("Replay 1", 2), &caps).await;
    sqlx::query("UPDATE org_selfserve_creations SET created_at = now() - interval '25 hours' WHERE clerk_org_id = $1")
        .bind(&o_old)
        .execute(&p.owner)
        .await
        .unwrap();
    let old = create(&state, &clerk, u, &k_old, req("Replay 2", 2), &caps).await;
    // A request holds the lease: the sweep cannot claim, a replay is 409.
    let leased_intent = intent_of(o_leased.clone()).await;
    sqlx::query(
        "UPDATE org_selfserve_creations SET lease_token = gen_random_uuid(), lease_until = now() + interval '1 minute' \
          WHERE id = $1",
    )
    .bind(leased_intent)
    .execute(&p.owner)
    .await
    .unwrap();
    let sweep_vs_lease = crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, leased_intent, stale).await.unwrap();
    let leased = create(&state, &clerk, u, &k_leased, req("Replay 3", 2), &caps).await;
    for o in &orgs {
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!((kept.0, kept.1["code"].as_str()), (StatusCode::CONFLICT, Some("signup_complete")), "{}", kept.1);
    assert_eq!(kept.1["org_id"], json!(o_kept));
    assert!(claim.is_some());
    assert_eq!((claimed.0, claimed.1["code"].as_str()), (StatusCode::GONE, Some("checkout_expired")), "{}", claimed.1);
    assert_eq!((old.0, old.1["code"].as_str()), (StatusCode::GONE, Some("checkout_expired")), "{}", old.1);
    assert!(sweep_vs_lease.is_none(), "the sweep never claims an intent a request holds");
    assert_eq!((leased.0, leased.1["code"].as_str()), (StatusCode::CONFLICT, Some("create_in_progress")), "{}", leased.1);
}

/// S8 + N3: a per-domain cap (free-mail domains share one bucket); the
/// global hourly threshold throttles but never refuses on its own; a
/// refused intent tried again is re-checked against the caps.
#[tokio::test]
async fn per_domain_caps_refuse_while_the_global_threshold_only_throttles() {
    const NAME: &str = "per_domain_caps_refuse_while_the_global_threshold_only_throttles";
    let Some(p) = pools(NAME).await else { return };
    let _catalog = CATALOG_PRICE_LOCK.lock().await;
    let previous = price_standard(&p.owner).await;
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (clerk, cmock) = spawn_clerk().await;
    let domain = format!("acme-{}.test", Uuid::new_v4().simple());
    let mut users = Vec::new();
    let add = |email: String| {
        let owner = p.owner.clone();
        let cmock = cmock.clone();
        async move {
            let (u, clerk_id) = new_user(&owner).await;
            cmock.add_user_email(&clerk_id, &email);
            u
        }
    };
    for i in 0..3 {
        users.push(add(format!("p{i}@{domain}")).await);
    }
    let gmail = [add(format!("a-{}@gmail.com", Uuid::new_v4().simple())).await, add(format!("b-{}@GMail.com", Uuid::new_v4().simple())).await];
    let lone = add(format!("solo@solo-{}.test", Uuid::new_v4().simple())).await;
    // The freemail bucket is shared by every test in the database; count
    // what is already there in the last hour so the cap lands right.
    let freemail_now: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM org_selfserve_creations WHERE cap_bucket LIKE '*freemail%' AND status <> 'refused' \
           AND created_at > now() - interval '1 hour'",
    )
    .fetch_one(&p.owner)
    .await
    .unwrap();
    let caps = Caps { max_per_domain_per_hour: 2, max_freemail_per_hour: freemail_now + 1, ..CAPS };
    let mut results = Vec::new();
    for (i, u) in users.iter().enumerate() {
        results.push(create(&state, &clerk, *u, &Uuid::new_v4().to_string(), req(&format!("Dom {i}"), 2), &caps).await);
    }
    let g1 = create(&state, &clerk, gmail[0], &Uuid::new_v4().to_string(), req("Free 1", 2), &caps).await;
    let g2 = create(&state, &clerk, gmail[1], &Uuid::new_v4().to_string(), req("Free 2", 2), &caps).await;
    // Global threshold 0: every create is past it. A first-time creator on a
    // fresh domain is still let in (throttled, alerted), not refused.
    let throttled = create(&state, &clerk, lone, &Uuid::new_v4().to_string(), req("Solo", 2), &Caps { max_per_hour: 0, ..caps }).await;

    // N3: a refused intent revived over the creator's daily cap is refused.
    let (n3, n3_clerk) = new_user(&p.owner).await;
    cmock.add_user(&n3_clerk, true);
    for i in 0..3 {
        sqlx::query(
            "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, clerk_org_id, sweep_state) \
             VALUES ($1, $2, 'Day', 2, 'created', $3, 'kept')",
        )
        .bind(n3)
        .bind(format!("n3-day-{i}-{}", Uuid::new_v4().simple()))
        .bind(format!("org_n3{}", Uuid::new_v4().simple()))
        .execute(&p.owner)
        .await
        .unwrap();
    }
    let refused_key = format!("n3-refused-{}", Uuid::new_v4().simple());
    sqlx::query("INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status) VALUES ($1, $2, 'Revive', 2, 'refused')")
        .bind(n3)
        .bind(&refused_key)
        .execute(&p.owner)
        .await
        .unwrap();
    let creates_before = cmock.creates().len();
    let revived = create(&state, &clerk, n3, &refused_key, req("Revive", 2), &CAPS).await;
    let creates_after = cmock.creates().len();

    let mut orgs: Vec<String> = results.iter().chain([&g1, &g2, &throttled]).filter_map(|(_, b)| b["org_id"].as_str().map(str::to_string)).collect();
    orgs.dedup();
    for o in &orgs {
        drop_org(&p.owner, o).await;
    }
    let all: Vec<Uuid> = users.iter().copied().chain(gmail).chain([lone, n3]).collect();
    sqlx::query("DELETE FROM org_selfserve_creations WHERE user_id = ANY($1)").bind(&all).execute(&p.owner).await.ok();
    drop_users(&p.owner, &all).await;
    set_seat_tier_price(&p.owner, "standard", previous.as_deref()).await;

    assert_eq!(results[0].0, StatusCode::CREATED, "{}", results[0].1);
    assert_eq!(results[1].0, StatusCode::CREATED, "{}", results[1].1);
    assert_eq!(
        (results[2].0, results[2].1["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("org_create_rate_limited")),
        "the third creator on one domain within the hour: {}",
        results[2].1
    );
    assert_eq!(g1.0, StatusCode::CREATED, "{}", g1.1);
    assert_eq!(
        (g2.0, g2.1["code"].as_str()),
        (StatusCode::TOO_MANY_REQUESTS, Some("org_create_rate_limited")),
        "every free-mail identity also counts toward the free-mail aggregate: {}",
        g2.1
    );
    assert_eq!(throttled.0, StatusCode::CREATED, "the global threshold alone never refuses: {}", throttled.1);
    assert_eq!(revived.0, StatusCode::TOO_MANY_REQUESTS, "N3: a revived refused intent is re-checked: {}", revived.1);
    assert_eq!(creates_after, creates_before, "nothing was created in Clerk for it");
}

/// S4: the Clerk member cap follows the purchased seats (here: the seats
/// being bought, or the members already there, plus an invite's headroom),
/// and only for an org `POST /v1/orgs` created.
#[tokio::test]
async fn the_clerk_member_cap_follows_the_seats_and_only_for_selfserve_orgs() {
    const NAME: &str = "the_clerk_member_cap_follows_the_seats_and_only_for_selfserve_orgs";
    let Some(p) = pools(NAME).await else { return };
    let (clerk, cmock) = spawn_clerk().await;
    let mut users = Vec::new();
    for _ in 0..5 {
        users.push(new_user(&p.owner).await.0);
    }
    let org = format!("org_cap{}", Uuid::new_v4().simple());
    let members: Vec<(Uuid, &str)> = users.iter().map(|u| (*u, "member")).collect();
    let account = seed_org(&p.owner, &org, users[0], &members, 0).await;
    sqlx::query("UPDATE org_selfserve_creations SET seats = 3 WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    // A concierge org (no intent row).
    let concierge = format!("org_conc{}", Uuid::new_v4().simple());
    let concierge_account = seed_org(&p.owner, &concierge, users[0], &[(users[0], "admin")], 0).await;
    sqlx::query("DELETE FROM org_selfserve_creations WHERE clerk_org_id = $1").bind(&concierge).execute(&p.owner).await.unwrap();

    let synced = sync_clerk_membership_cap(&p.app, &clerk, account, 0).await;
    let with_headroom = sync_clerk_membership_cap(&p.app, &clerk, account, 1).await;
    let untouched = sync_clerk_membership_cap(&p.app, &clerk, concierge_account, 1).await;
    let patches = cmock.patches();
    drop_org(&p.owner, &org).await;
    drop_org(&p.owner, &concierge).await;
    drop_users(&p.owner, &users).await;

    assert_eq!(synced, Ok(Some(5)), "never below the 5 members it has (3 seats being bought)");
    assert_eq!(with_headroom, Ok(Some(6)), "an invite that buys its own seat gets one place");
    assert_eq!(untouched, Ok(None), "a concierge org is never touched");
    assert_eq!(patches.len(), 2, "{patches:?}");
    assert!(patches.iter().all(|(id, _)| *id == org), "{patches:?}");
    assert_eq!(patches[0].1["max_allowed_memberships"], 5);
    assert_eq!(patches[1].1["max_allowed_memberships"], 6);
}

/// N1: Clerk's `organization.created` and membership webhooks for an org
/// carrying the self-serve marker never mint its `accounts` row (nor a
/// `free` grant), even before the create route has recorded the org.
#[tokio::test]
async fn a_webhook_for_a_selfserve_org_never_mints_its_account() {
    const NAME: &str = "a_webhook_for_a_selfserve_org_never_mints_its_account";
    let Some(p) = pools(NAME).await else { return };
    let (stripe, _smock) = spawn_stripe().await;
    let state = app_state(&p.app, stripe);
    let (u, clerk_id) = new_user(&p.owner).await;
    let org = format!("org_n1{}", Uuid::new_v4().simple());
    let payload = json!({"id": org, "name": "Marked", "created_by": clerk_id,
        "private_metadata": {SELFSERVE_ORG_MARKER: true, SELFSERVE_INTENT_MARKER: Uuid::new_v4().to_string()}});
    let created = crate::routes::clerk_webhook::handle_org_created(&state, &p.app, &payload).await;
    let membership = crate::routes::clerk_webhook::handle_membership_upsert(
        &state,
        &p.app,
        &json!({"organization": payload, "public_user_data": {"user_id": clerk_id, "identifier": "x@test.example"}, "role": "org:admin"}),
        true,
    )
    .await;
    let accounts: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE clerk_org_id = $1")
        .bind(&org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(created.is_ok(), "{created:?}");
    assert!(membership.is_ok(), "{membership:?}");
    assert_eq!(accounts, 0, "no accounts row (and so no free grant) from the webhook");
}

/// S2 + S3 + N9: the failed-job sweep reads the customer's whole list (a
/// trialing subscription the mirror does not name keeps the org, on any
/// page), and an unpaid subscription with a processing payment is live.
#[tokio::test]
async fn the_sweep_reads_every_subscription_on_the_customer_and_a_processing_payment_is_live() {
    const NAME: &str = "the_sweep_reads_every_subscription_on_the_customer_and_a_processing_payment_is_live";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let org = |tag: &str| format!("org_{tag}{}", Uuid::new_v4().simple());
    let (unmirrored, paged, processing) = (org("unmirrored"), org("paged"), org("processing"));
    let mut accounts = HashMap::new();
    for o in [&unmirrored, &paged, &processing] {
        let a = seed_org(&p.owner, o, u, &[(u, "admin")], 48).await;
        cmock.add_org(json!({"id": o, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
        failed_job(&p.owner, o, a, 2).await;
        accounts.insert(o.clone(), a);
    }
    // S2: the mirror has the customer but no subscription id.
    let tag = Uuid::new_v4().simple().to_string();
    let cus = |o: &str| format!("cus_{o}");
    mirror(&p.owner, accounts[&unmirrored], &cus(&unmirrored), None, "pending").await;
    smock.set_sub(&cus(&unmirrored), &format!("sub_t{tag}"), "trialing");
    // N9: the live one is on the third page.
    smock.set_page_size(1);
    mirror(&p.owner, accounts[&paged], &cus(&paged), None, "pending").await;
    smock.set_sub(&cus(&paged), &format!("sub_c1{tag}"), "canceled");
    smock.set_sub(&cus(&paged), &format!("sub_c2{tag}"), "canceled");
    smock.set_sub(&cus(&paged), &format!("sub_l{tag}"), "active");
    // S3: past_due, but its latest invoice's PaymentIntent is processing.
    let sub_p = format!("sub_p{tag}");
    mirror(&p.owner, accounts[&processing], &cus(&processing), Some(&sub_p), "past_due").await;
    smock.set_sub(&cus(&processing), &sub_p, "past_due");
    smock.set_processing_payment(&sub_p, &format!("in_{tag}"), &format!("pi_{tag}"), "processing");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let rows = [job_row(&p.owner, &unmirrored).await, job_row(&p.owner, &paged).await, job_row(&p.owner, &processing).await];
    let deletes = cmock.deletes();
    let cancels = smock.calls("DELETE", "/v1/subscriptions/");
    for o in [&unmirrored, &paged, &processing] {
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;

    for (o, row) in [&unmirrored, &paged, &processing].iter().zip(&rows) {
        assert_eq!(row.0, "provisioning", "{o}: live at Stripe, requeued, not deleted: {r:?}");
        assert!(!deletes.contains(o), "{o} was deleted: {deletes:?}");
    }
    assert!(cancels.iter().all(|(_, path, _)| !path.ends_with(&sub_p)), "a processing payment is never canceled: {cancels:?}");
}

/// S6: the stale-takeover race. A pass whose claim is taken over while its
/// Stripe cancel is in flight stops: it neither deletes the Clerk org nor
/// marks anything; the new holder owns the org. And a fresh Stripe read
/// immediately before the cancel releases an org that turned live.
#[tokio::test]
async fn a_pass_whose_claim_is_taken_over_mid_flight_stops_before_the_clerk_delete() {
    const NAME: &str = "a_pass_whose_claim_is_taken_over_mid_flight_stops_before_the_clerk_delete";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let (job_org, intent_org, flip_org) = (format!("org_race{tag}"), format!("org_irace{tag}"), format!("org_flip{tag}"));
    // The failed-job path.
    let job_account = seed_org(&p.owner, &job_org, u, &[(u, "admin")], 48).await;
    cmock.add_org(json!({"id": job_org, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    failed_job(&p.owner, &job_org, job_account, 2).await;
    let job_sub = format!("sub_jr{tag}");
    mirror(&p.owner, job_account, &format!("cus_jr{tag}"), Some(&job_sub), "past_due").await;
    smock.set_sub(&format!("cus_jr{tag}"), &job_sub, "past_due");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };
    let stale = std::time::Duration::from_secs(600);

    let (mut rx, go) = smock.gate_next_cancel();
    let (report, taken) = tokio::join!(sweeper.sweep_once(chrono::Utc::now()), async {
        // Bounded: should the pass never reach a cancel, the test fails on
        // its assertions instead of hanging.
        if tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv()).await.is_err() {
            return None;
        }
        sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_claimed_at = now() - interval '1 hour' WHERE clerk_org_id = $1")
            .bind(&job_org)
            .execute(&p.owner)
            .await
            .unwrap();
        let t = crate::tenancy::provisioning::claim_clerk_org_cleanup(&p.app, &job_org, stale).await.unwrap();
        go.notify_one();
        t
    });
    let job_after: (String, Option<chrono::DateTime<chrono::Utc>>) = sqlx::query_as(
        "SELECT clerk_org_cleanup, clerk_org_cleanup_claimed_at FROM tenant_provisioning_jobs WHERE clerk_org_id = $1",
    )
    .bind(&job_org)
    .fetch_one(&p.owner)
    .await
    .unwrap();
    let job_deleted = cmock.deletes().contains(&job_org);

    // The intent (abandoned) path, the same race.
    let intent_account = seed_org(&p.owner, &intent_org, u, &[(u, "admin")], 30).await;
    cmock.add_org(json!({"id": intent_org, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    let intent_sub = format!("sub_ir{tag}");
    mirror(&p.owner, intent_account, &format!("cus_ir{tag}"), None, "pending").await;
    smock.set_sub(&format!("cus_ir{tag}"), &intent_sub, "incomplete");
    let intent_id: Uuid = sqlx::query_scalar("SELECT id FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&intent_org)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let (mut rx, go) = smock.gate_next_cancel();
    let (report2, taken2) = tokio::join!(sweeper.sweep_once(chrono::Utc::now()), async {
        if tokio::time::timeout(std::time::Duration::from_secs(10), rx.recv()).await.is_err() {
            return None;
        }
        sqlx::query("UPDATE org_selfserve_creations SET sweep_claimed_at = now() - interval '1 hour' WHERE id = $1")
            .bind(intent_id)
            .execute(&p.owner)
            .await
            .unwrap();
        let t = crate::tenancy::org_selfserve_cleanup::claim_intent(&p.app, intent_id, stale).await.unwrap();
        go.notify_one();
        t
    });
    let intent_after: (String, Option<chrono::DateTime<chrono::Utc>>) =
        sqlx::query_as("SELECT sweep_state, sweep_claimed_at FROM org_selfserve_creations WHERE id = $1")
            .bind(intent_id)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let intent_deleted = cmock.deletes().contains(&intent_org);
    let intent_account_left: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1)")
        .bind(intent_account)
        .fetch_one(&p.owner)
        .await
        .unwrap();

    // The re-read before the cancel: unpaid on the first read, live by the
    // re-check (reads: retrieve, list; then the re-check's retrieve, list).
    let flip_account = seed_org(&p.owner, &flip_org, u, &[(u, "admin")], 48).await;
    cmock.add_org(json!({"id": flip_org, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    failed_job(&p.owner, &flip_org, flip_account, 2).await;
    let flip_sub = format!("sub_flip{tag}");
    mirror(&p.owner, flip_account, &format!("cus_flip{tag}"), Some(&flip_sub), "past_due").await;
    smock.set_sub(&format!("cus_flip{tag}"), &flip_sub, "past_due");
    smock.set_sub_sequence(&flip_sub, &["past_due", "past_due", "active"]);
    let cancels_before = smock.calls("DELETE", &format!("/v1/subscriptions/{flip_sub}")).len();
    let report3 = sweeper.sweep_once(chrono::Utc::now()).await;
    let flip_cancels = smock.calls("DELETE", &format!("/v1/subscriptions/{flip_sub}")).len() - cancels_before;
    let flip_row = job_row(&p.owner, &flip_org).await;
    let flip_deleted = cmock.deletes().contains(&flip_org);

    for o in [&job_org, &intent_org, &flip_org] {
        drop_org(&p.owner, o).await;
    }
    drop_users(&p.owner, &[u]).await;

    let taken = taken.expect("the second pass took the stale job claim");
    assert!(report.lost_claims.contains(&job_org), "{report:?}");
    assert!(!job_deleted, "the overtaken pass did not delete the Clerk org");
    assert_eq!(job_after, ("deleting".to_string(), Some(taken)), "the new holder owns it; nothing marked done");
    let taken2 = taken2.expect("the second pass took the stale intent claim");
    assert!(report2.lost_claims.contains(&intent_org), "{report2:?}");
    assert!(!intent_deleted, "the overtaken intent pass did not delete the Clerk org");
    assert_eq!(intent_after, ("deleting".to_string(), Some(taken2)));
    assert!(intent_account_left, "nor the account");
    assert_eq!(flip_cancels, 0, "turned live before the cancel: not canceled: {report3:?}");
    assert!(!flip_deleted);
    assert_eq!(flip_row.1, "pending", "released for the next pass to decide (it will requeue)");
}

/// S7: a held paid org keeps no bill running. The sweep pauses collection
/// on its subscription (keyed per subscription and failure), the trial
/// worker does not convert it at the 90-day backstop, and once its job is
/// ready the sweep resumes collection.
#[tokio::test]
async fn a_held_paid_org_has_collection_paused_and_is_not_converted() {
    const NAME: &str = "a_held_paid_org_has_collection_paused_and_is_not_converted";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let org = format!("org_held{tag}");
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
    cmock.add_org(json!({"id": org, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    failed_job(&p.owner, &org, account, 2).await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_requeues = 2 WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    let sub = format!("sub_held{tag}");
    mirror(&p.owner, account, &format!("cus_held{tag}"), Some(&sub), "active").await;
    smock.set_sub(&format!("cus_held{tag}"), &sub, "trialing");
    // Trialing at the 90-day backstop: the worker would convert it now.
    fin(
        &p.owner,
        account,
        sqlx::query(
            "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, \
                 checkout_opened_at, stripe_subscription_id, pool_baseline, trial_started_at, trial_ends_at) \
             VALUES ($1, 'trialing', $2, 2, true, $3, now() - interval '91 days', $4, 3000, now() - interval '91 days', \
                     now() - interval '1 day')",
        )
        .bind(account)
        .bind(u)
        .bind(format!("cs_held{tag}"))
        .bind(&sub),
    )
    .await;
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let r1 = sweeper.sweep_once(chrono::Utc::now()).await;
    let held = job_row(&p.owner, &org).await;
    let paused_at: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT collection_paused_at FROM org_selfserve_creations WHERE clerk_org_id = $1")
            .bind(&org)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let pass = crate::billing::org_trial::lifecycle::process_account(&p.app, None, account, chrono::Utc::now()).await;
    let trial_state: String = sqlx::query_scalar("SELECT state FROM org_trials WHERE account_id = $1")
        .bind(account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let r2 = sweeper.sweep_once(chrono::Utc::now()).await;
    // An operator gets it built.
    sqlx::query("UPDATE tenant_provisioning_jobs SET status = 'ready', step = 'done', clerk_org_cleanup = 'none' WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    let r3 = sweeper.sweep_once(chrono::Utc::now()).await;
    let resumed: Option<chrono::DateTime<chrono::Utc>> =
        sqlx::query_scalar("SELECT collection_paused_at FROM org_selfserve_creations WHERE clerk_org_id = $1")
            .bind(&org)
            .fetch_one(&p.owner)
            .await
            .unwrap();
    let posts = smock.posts(&format!("/v1/subscriptions/{sub}"));
    sqlx::query("DELETE FROM org_trials WHERE account_id = $1").bind(account).execute(&p.owner).await.ok();
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r1.held.contains(&org), "{r1:?}");
    assert_eq!(held.1, "held");
    assert!(r1.collection_paused.contains(&sub), "{r1:?}");
    assert!(paused_at.is_some(), "recorded");
    assert!(pass.is_ok(), "{pass:?}");
    assert!(!pass.unwrap().actions.contains(&"converting"), "a held org is not converted");
    assert_eq!(trial_state, "trialing", "no conversion while held");
    assert!(r2.collection_paused.is_empty(), "paused once, not every pass: {r2:?}");
    assert!(r3.collection_resumed.contains(&sub), "{r3:?}");
    assert!(resumed.is_none(), "collection resumed once the org is ready");
    assert_eq!(posts.len(), 2, "one pause and one resume: {posts:?}");
    assert!(posts[0].1.contains("pause_collection%5Bbehavior%5D=void") || posts[0].1.contains("pause_collection[behavior]=void"), "{posts:?}");
    assert!(posts[0].2.as_deref().is_some_and(|k| k.starts_with(&format!("org-selfserve-held-pause:{account}:{sub}:"))), "keyed: {posts:?}");
    assert!(posts[1].2.as_deref().is_some_and(|k| k.starts_with(&format!("org-selfserve-held-resume:{account}:{sub}:"))), "keyed: {posts:?}");
}

/// #1540 N7 (pre-merge): the Hold arm pauses collection only for an org
/// `POST /v1/orgs` recorded creating, the same record check as the
/// cancel/delete path. A held paid org with no `created` record is held for
/// an operator, but nothing is paused at Stripe.
#[tokio::test]
async fn n7_a_held_org_without_a_selfserve_record_is_not_paused() {
    const NAME: &str = "n7_a_held_org_without_a_selfserve_record_is_not_paused";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (u, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();
    let org = format!("org_n7held{tag}");
    let account = seed_org(&p.owner, &org, u, &[(u, "admin")], 48).await;
    cmock.add_org(json!({"id": org, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    failed_job(&p.owner, &org, account, 2).await;
    sqlx::query("UPDATE tenant_provisioning_jobs SET clerk_org_cleanup_requeues = 2 WHERE clerk_org_id = $1")
        .bind(&org)
        .execute(&p.owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM org_selfserve_creations WHERE clerk_org_id = $1").bind(&org).execute(&p.owner).await.unwrap();
    let sub = format!("sub_n7held{tag}");
    mirror(&p.owner, account, &format!("cus_n7held{tag}"), Some(&sub), "active").await;
    smock.set_sub(&format!("cus_n7held{tag}"), &sub, "trialing");
    let sweeper = Sweeper { control: &p.app, clerk: &clerk, stripe: Some(stripe.as_ref()), policy: CleanupPolicy::default() };

    let r = sweeper.sweep_once(chrono::Utc::now()).await;
    let job = job_row(&p.owner, &org).await;
    let posts = smock.posts(&format!("/v1/subscriptions/{sub}"));
    drop_org(&p.owner, &org).await;
    drop_users(&p.owner, &[u]).await;

    assert!(r.held.contains(&org), "{r:?}");
    assert_eq!(job.1, "held");
    assert!(!r.collection_paused.contains(&sub), "no self-serve record: nothing paused: {r:?}");
    assert!(posts.is_empty(), "no Stripe write for an org without the record: {posts:?}");
    assert!(!r.errors.iter().any(|e| e.contains(&org)), "{r:?}");
}

/// S9 + N4 + N5 + N2. The intent outlives its creator and the sweep still
/// cleans up the org; a row the sweep leaves alone cannot starve a due one;
/// the account delete must delete exactly the org's row or nothing is
/// marked; a dead `creating` attempt is adopted by its marker or refused.
#[tokio::test]
async fn the_sweep_survives_a_deleted_creator_never_starves_and_settles_dead_attempts() {
    const NAME: &str = "the_sweep_survives_a_deleted_creator_never_starves_and_settles_dead_attempts";
    let Some(p) = pools(NAME).await else { return };
    let _sweep = SWEEP_LOCK.lock().await;
    let (stripe, _smock) = spawn_stripe().await;
    let (clerk, cmock) = spawn_clerk().await;
    let (owner_u, _) = new_user(&p.owner).await;
    let (creator, _) = new_user(&p.owner).await;
    let tag = Uuid::new_v4().simple().to_string();

    // S9: an abandoned org (no Checkout, 30 h old) whose creator is deleted.
    let orphan = format!("org_orph{tag}");
    let orphan_account = seed_org(&p.owner, &orphan, owner_u, &[(owner_u, "admin")], 30).await;
    cmock.add_org(json!({"id": orphan, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    sqlx::query("UPDATE org_selfserve_creations SET user_id = $2 WHERE clerk_org_id = $1")
        .bind(&orphan)
        .bind(creator)
        .execute(&p.owner)
        .await
        .unwrap();
    sqlx::query("DELETE FROM users WHERE id = $1").bind(creator).execute(&p.owner).await.expect("delete creator");
    let orphan_creator: Option<Option<Uuid>> =
        sqlx::query_scalar("SELECT user_id FROM org_selfserve_creations WHERE clerk_org_id = $1")
            .bind(&orphan)
            .fetch_optional(&p.owner)
            .await
            .unwrap();

    // N4: the oldest row is not due (its Checkout is open) and comes first
    // by creation time; the due row behind it must still be reached.
    let blocker = format!("org_blk{tag}");
    let blocker_account = seed_org(&p.owner, &blocker, owner_u, &[(owner_u, "admin")], 1000).await;
    fin(
        &p.owner,
        blocker_account,
        sqlx::query(
            "INSERT INTO org_trials (account_id, state, creator_user_id, seats, trial_eligible, checkout_session_id, checkout_opened_at) \
             VALUES ($1, 'checkout_open', $2, 2, true, $3, now())",
        )
        .bind(blocker_account)
        .bind(owner_u)
        .bind(format!("cs_blk{tag}")),
    )
    .await;
    sqlx::query("UPDATE org_selfserve_creations SET sweep_next_check_at = now() - interval '1 day' WHERE clerk_org_id = $1")
        .bind(&blocker)
        .execute(&p.owner)
        .await
        .unwrap();

    // N5: an intent pointing at an account that is not an org account.
    let personal = Uuid::new_v4();
    sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)").bind(personal).bind(owner_u).execute(&p.owner).await.unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(personal)
        .bind(owner_u)
        .execute(&p.owner)
        .await
        .unwrap();
    let odd = format!("org_odd{tag}");
    cmock.add_org(json!({"id": odd, "name": "x", "created_by": "user_x", "private_metadata": {SELFSERVE_ORG_MARKER: true}}));
    sqlx::query(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, clerk_org_id, account_id, created_at) \
         VALUES ($1, $2, 'Odd', 2, 'created', $3, $4, now() - interval '30 hours')",
    )
    .bind(owner_u)
    .bind(format!("odd-{tag}"))
    .bind(&odd)
    .bind(personal)
    .execute(&p.owner)
    .await
    .unwrap();

    // N2: two dead attempts, 3 h in `creating`: one whose org Clerk has (by
    // its marker), one with none.
    let (dead_user, dead_clerk) = new_user(&p.owner).await;
    let dead_found: Uuid = sqlx::query_scalar(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, created_at) \
         VALUES ($1, $2, 'Dead found', 2, now() - interval '3 hours') RETURNING id",
    )
    .bind(dead_user)
    .bind(format!("dead-a-{tag}"))
    .fetch_one(&p.owner)
    .await
    .unwrap();
    let dead_none: Uuid = sqlx::query_scalar(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, created_at) \
         VALUES ($1, $2, 'Dead none', 2, now() - interval '3 hours') RETURNING id",
    )
    .bind(dead_user)
    .bind(format!("dead-b-{tag}"))
    .fetch_one(&p.owner)
    .await
    .unwrap();
    let adopted_org = format!("org_dead{tag}");
    cmock.add_org(json!({"id": adopted_org, "name": "Dead found", "created_by": dead_clerk,
        "private_metadata": {SELFSERVE_ORG_MARKER: true, SELFSERVE_INTENT_MARKER: dead_found.to_string()}}));

    let sweeper = Sweeper {
        control: &p.app,
        clerk: &clerk,
        stripe: Some(stripe.as_ref()),
        policy: CleanupPolicy { batch: 1, ..CleanupPolicy::default() },
    };
    let mut reports = Vec::new();
    for _ in 0..12 {
        let r = sweeper.sweep_once(chrono::Utc::now()).await;
        let done: bool = sqlx::query_scalar("SELECT sweep_state = 'deleted' FROM org_selfserve_creations WHERE clerk_org_id = $1")
            .bind(&orphan)
            .fetch_one(&p.owner)
            .await
            .unwrap();
        reports.push(r);
        if done {
            break;
        }
    }
    let orphan_state: String = sqlx::query_scalar("SELECT sweep_state FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&orphan)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let orphan_account_left: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM accounts WHERE id = $1)")
        .bind(orphan_account)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    // Let N5's row and the dead attempts be reached (batch 1).
    for _ in 0..12 {
        reports.push(sweeper.sweep_once(chrono::Utc::now()).await);
    }
    let odd_state: String = sqlx::query_scalar("SELECT sweep_state FROM org_selfserve_creations WHERE clerk_org_id = $1")
        .bind(&odd)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let personal_member: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM account_members WHERE account_id = $1)")
        .bind(personal)
        .fetch_one(&p.owner)
        .await
        .unwrap();
    let dead: Vec<(Uuid, String, Option<String>)> =
        sqlx::query_as("SELECT id, status, clerk_org_id FROM org_selfserve_creations WHERE id = ANY($1)")
            .bind([dead_found, dead_none])
            .fetch_all(&p.owner)
            .await
            .unwrap();
    let adopted_account: bool = sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM accounts WHERE clerk_org_id = $1)")
        .bind(&adopted_org)
        .fetch_one(&p.owner)
        .await
        .unwrap();

    sqlx::query("DELETE FROM org_selfserve_creations WHERE clerk_org_id = ANY($1) OR id = ANY($2)")
        .bind([&orphan, &odd, &blocker, &adopted_org])
        .bind([dead_found, dead_none])
        .execute(&p.owner)
        .await
        .ok();
    for o in [&orphan, &blocker, &adopted_org] {
        drop_org(&p.owner, o).await;
    }
    {
        let mut tx = crate::tenancy::scope::begin_account_scoped(&p.owner, personal).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, personal).await.unwrap();
        sqlx::query("DELETE FROM account_members WHERE account_id = $1").bind(personal).execute(&mut *tx).await.ok();
        sqlx::query("DELETE FROM accounts WHERE id = $1").bind(personal).execute(&mut *tx).await.ok();
        tx.commit().await.unwrap();
    }
    drop_users(&p.owner, &[owner_u, dead_user]).await;

    assert_eq!(orphan_creator, Some(None), "S9: the intent outlives its creator (SET NULL)");
    assert_eq!(orphan_state, "deleted", "S9 + N4: reached past the not-due row and deleted: {reports:?}");
    assert!(!orphan_account_left);
    assert!(cmock.deletes().contains(&orphan));
    assert_eq!(odd_state, "none", "N5: the account delete hit no org row, so nothing is marked deleted");
    assert!(personal_member, "N5: and the membership delete was rolled back");
    let by_id: HashMap<Uuid, (String, Option<String>)> = dead.into_iter().map(|(i, s, o)| (i, (s, o))).collect();
    assert_eq!(by_id[&dead_found], ("created".to_string(), Some(adopted_org.clone())), "N2: adopted by its marker");
    assert!(adopted_account, "N2: the adopted org got its account row");
    assert_eq!(by_id[&dead_none].0, "refused", "N2: no org in Clerk: refused");
}

#[path = "pg_tests/followups.rs"]
mod followups;
