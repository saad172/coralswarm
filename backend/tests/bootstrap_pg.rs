//! `GET /v1/bootstrap` (routes/bootstrap.rs) against real Postgres, through
//! the REAL public router and the REAL binding gate.
//!
//! What this proves:
//!
//! 1. Each part is BYTE-IDENTICAL to its standalone endpoint's body
//!    (`/v1/me`, `/v1/graphs`, `/v1/orgs/join-requests/count`) for an org
//!    admin on a real `TenancyDecision::Org` (control DB + tenant DB), an org
//!    member, and a personal user.
//! 2. `join_requests_count` is `null` exactly where the standalone count is
//!    not 200 (a member, a personal user) and the response is still 200.
//! 3. A member removed through the real signed Clerk webhook gets the same
//!    403 status AND body from `/v1/bootstrap` as from `/v1/me`.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Without it every test prints a skip line and passes
//! vacuously, so run it with the variable set:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test bootstrap_pg -- --test-threads=1 --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;
#[path = "support/bootstrap_trace.rs"]
mod bootstrap_trace;

use axum::body::Body;
#[cfg(feature = "e2e")]
use axum::Json;
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes the process-global env vars every test here sets.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

const APEX: &str = "coralswarm.example";
const ORG: &str = "org_bootstrap";
const TEST_MASTER_KEK_HEX: &str =
    "3333333333333333333333333333333333333333333333333333333333333333";
const WEBHOOK_RAW_KEY: &[u8] = b"bootstrap-pg-webhook-test-key";
const ENV_KEYS: [&str; 7] = ["CLERK_ISSUER", "CLERK_JWKS_URL", "MASTER_KEK_HEX", "CLERK_WEBHOOK_SECRET", "TODAY_ACTIVITIES_ENABLED", "TODAY_GIST_ENABLED", "ORG_SELFSERVE_SIGNUP_ENABLED"];

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}
async fn recreate(base: &str, db: &str) {
    let mut conn = PgConnection::connect(base).await.expect("connect admin");
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

/// Holds `ENV_LOCK` and restores every var it set on drop (panics included).
struct Env {
    _lock: tokio::sync::MutexGuard<'static, ()>,
    previous: Vec<(&'static str, Option<std::ffi::OsString>)>,
}
impl Env {
    async fn enable() -> Self {
        let lock = ENV_LOCK.lock().await;
        let previous = ENV_KEYS.into_iter().map(|key| (key, std::env::var_os(key))).collect();
        let jwks_url = spawn_local_jwks().await;
        use base64::{engine::general_purpose::STANDARD, Engine};
        // SAFETY: every env mutation in this binary is serialized by ENV_LOCK,
        // held for this guard's whole lifetime.
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            std::env::set_var(
                "CLERK_WEBHOOK_SECRET",
                format!("whsec_{}", STANDARD.encode(WEBHOOK_RAW_KEY)),
            );
        }
        Self { _lock: lock, previous }
    }
}
impl Drop for Env {
    fn drop(&mut self) {
        // SAFETY: ENV_LOCK is still held (dropped after this body).
        unsafe {
            for (key, value) in &self.previous {
                if let Some(value) = value { std::env::set_var(key, value); }
                else { std::env::remove_var(key); }
            }
        }
    }
}

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}
fn org_token(sub: &str, role: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": format!("sess_{sub}"), "org_id": ORG, "org_role": role,
    }))
}
fn personal_token(sub: &str) -> String {
    mint(json!({ "iss": ISSUER, "sub": sub, "exp": future_exp(), "sid": format!("sess_{sub}") }))
}

/// Raw response: status + body BYTES (equality is checked on bytes).
async fn get_raw(app: &axum::Router, path: &str, token: &str) -> (StatusCode, Vec<u8>) {
    let req = HttpRequest::builder()
        .method("GET")
        .uri(path)
        .header("host", APEX)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, bytes.to_vec())
}

async fn post_clerk_webhook(app: &axum::Router, event: Value) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};
    let body = event.to_string();
    let id = "msg_bootstrap_test";
    let ts = Utc::now().timestamp().to_string();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(WEBHOOK_RAW_KEY).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    let sig = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("host", APEX)
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "clerk webhook must be acked");
}

struct Fixture {
    app: axum::Router,
    control: PgPool,
    org_account: Uuid,
}

/// Control DB + one org tenant DB, wired through a real enabled
/// `TenantRuntime` — the same shape as `route_migration_org_pg.rs`.
async fn fixture(base: &str, tag: &str) -> Fixture {
    fixture_with_provider(base, tag, None).await
}

async fn fixture_with_provider(base: &str, tag: &str, stripe: Option<Arc<coralswarm_backend::billing::stripe_client::StripeClient>>) -> Fixture {
    let (ctl, tenant_db) = (format!("bootstrap_{tag}_ctl"), format!("bootstrap_{tag}_tenant"));
    recreate(base, &ctl).await;
    recreate(base, &tenant_db).await;

    let control: PgPool = PgPoolOptions::new().connect(&with_db(base, &ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'owner@bootstrap.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(ORG)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, 'bootacme', 'SECRET_BOOT', 'active')",
        )
        .bind(ORG)
        .bind(org_account),
    )
    .await
    .unwrap();

    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(base, &tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(ORG)
        .execute(&tenant_pool)
        .await
        .unwrap();

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.stripe = stripe;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_BOOT", with_db(base, &tenant_db))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    Fixture {
        app: coralswarm_backend::test_support_app(Arc::new(state)),
        control,
        org_account,
    }
}

/// The whole contract for one caller: bootstrap is 200, `me`/`graphs` are
/// byte-equal to their endpoints, and `join_requests_count` is byte-equal to
/// the count endpoint when that answers 200, else `null`. Returns the parsed
/// bootstrap body for caller-specific assertions.
async fn assert_parts_equal_endpoints(app: &axum::Router, token: &str, who: &str) -> Value {
    // Warm: the first request for an identity provisions it (first-sight
    // membership, org-ocean self-heal). Equality is about steady state.
    let _ = get_raw(app, "/v1/me", token).await;
    let _ = get_raw(app, "/v1/graphs", token).await;

    let (me_status, me_bytes) = get_raw(app, "/v1/me", token).await;
    let (graphs_status, graphs_bytes) = get_raw(app, "/v1/graphs", token).await;
    let (count_status, count_bytes) = get_raw(app, "/v1/orgs/join-requests/count", token).await;
    let (status, boot_bytes) = get_raw(app, "/v1/bootstrap", token).await;
    assert_eq!(me_status, StatusCode::OK, "{who}: /v1/me");
    assert_eq!(graphs_status, StatusCode::OK, "{who}: /v1/graphs");
    assert_eq!(status, StatusCode::OK, "{who}: /v1/bootstrap {}", String::from_utf8_lossy(&boot_bytes));

    let boot: Value = serde_json::from_slice(&boot_bytes).unwrap();
    let keys: Vec<&str> = boot.as_object().unwrap().keys().map(String::as_str).collect();
    assert_eq!(
        keys.len(),
        10,
        "{who}: existing bootstrap fields plus activities_enabled and gist_enabled: {keys:?}"
    );
    // #1540: these callers have no self-serve org being set up.
    assert_eq!(boot["pending_orgs"], Value::Array(Vec::new()), "{who}: no pending self-serve orgs");
    // #1540 review SF-a: none of these accounts was made by POST /v1/orgs,
    // so the web keeps Clerk's own org profile for them.
    assert_eq!(boot["selfserve_org"], Value::Bool(false), "{who}: not a self-serve org");
    assert_eq!(
        boot["selfserve_signup_enabled"],
        Value::Bool(coralswarm_backend::tenancy::provisioning::selfserve_signup_enabled()),
        "{who}: the self-serve flag"
    );

    assert_eq!(
        serde_json::to_vec(&boot["me"]).unwrap(),
        me_bytes,
        "{who}: `me` must be byte-identical to GET /v1/me"
    );
    assert_eq!(
        serde_json::to_vec(&boot["graphs"]).unwrap(),
        graphs_bytes,
        "{who}: `graphs` must be byte-identical to GET /v1/graphs"
    );
    if count_status == StatusCode::OK {
        assert_eq!(
            serde_json::to_vec(&boot["join_requests_count"]).unwrap(),
            count_bytes,
            "{who}: `join_requests_count` must be byte-identical to GET /v1/orgs/join-requests/count"
        );
    } else {
        assert!(count_status.is_client_error(), "{who}: count answered {count_status}");
        assert_eq!(boot["join_requests_count"], Value::Null, "{who}: count {count_status} => null");
    }
    let (mine_status,mine_bytes)=get_raw(app,"/v1/me/join-requests",token).await;
    assert_eq!(mine_status,StatusCode::OK);
    assert_eq!(boot["my_join_requests"],serde_json::from_slice::<Value>(&mine_bytes).unwrap());
    let (billing_status,billing_bytes)=get_raw(app,"/v1/billing/summary",token).await;
    if who=="org admin" && billing_status==StatusCode::OK {
        assert_eq!(boot["billing_banner"],serde_json::from_slice::<Value>(&billing_bytes).unwrap());
    } else {assert_eq!(boot["billing_banner"],Value::Null);}
    assert_eq!(boot["activities_enabled"], json!(coralswarm_backend::services::journal_activities::enabled()));
    assert_eq!(boot["gist_enabled"], json!(coralswarm_backend::services::journal_gist::enabled()));
    boot
}

#[tokio::test]
async fn bootstrap_parts_are_byte_identical_to_their_endpoints() {
    let Some(base) = base_url() else {
        eprintln!("skipping bootstrap_parts_are_byte_identical_to_their_endpoints: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = Env::enable().await;
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let f = fixture(&base, "eq").await;

    // ── Org admin on a real Org decision. Seed one open join request so the
    //    count part is a real non-zero 200, not a default. ─────────────────
    let admin_sub = format!("boot_admin_{}", Uuid::new_v4().simple());
    let admin_token = org_token(&admin_sub, "org:admin");
    let _ = get_raw(&f.app, "/v1/me", &admin_token).await;
    let requester: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'wants-in@bootstrap.example') RETURNING id",
    )
    .fetch_one(&f.control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO org_join_requests(account_id, user_id, source) VALUES ($1, $2, 'self_request')")
        .bind(f.org_account)
        .bind(requester)
        .execute(&f.control)
        .await
        .unwrap();

    let boot = assert_parts_equal_endpoints(&f.app, &admin_token, "org admin").await;
    let records = capture.take();
    assert_eq!(records.len(), 1, "one completion for bootstrap, none for standalone endpoints");
    assert_eq!(records[0]["billing"]["provider"]["outcome"], "skipped");
    assert_eq!(records[0]["billing"]["db"]["outcome"], "ok");
    assert_eq!(boot["join_requests_count"], json!({"open": 1}));
    assert_eq!(boot["me"]["account_id"], json!(f.org_account.to_string()));
    let graphs = boot["graphs"].as_array().expect("graphs is the bare array");
    assert!(
        graphs.iter().any(|g| g["kind"] == json!("org")),
        "the org admin sees the tenant's org ocean: {graphs:?}"
    );

    // ── Org member (not admin): count endpoint 403s, bootstrap part is null.
    let member_token = org_token(&format!("boot_member_{}", Uuid::new_v4().simple()), "org:member");
    let boot = assert_parts_equal_endpoints(&f.app, &member_token, "org member").await;
    assert_eq!(boot["join_requests_count"], Value::Null);
    let records = capture.take();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["branches"]["billing_banner"]["outcome"], "skipped");
    assert_eq!(records[0]["billing"]["provider"]["outcome"], "not_reached");

    // ── Personal user on the personal plane (same app, no org claim).
    let personal = personal_token(&format!("boot_personal_{}", Uuid::new_v4().simple()));
    let boot = assert_parts_equal_endpoints(&f.app, &personal, "personal user").await;
    assert_eq!(boot["join_requests_count"], Value::Null);
    let records = capture.take();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0]["branches"]["billing_banner"]["outcome"], "skipped");
    assert_eq!(records[0]["billing"]["provider"]["outcome"], "not_reached");
    assert_eq!(boot["me"]["is_personal_org"], json!(false));
    assert_ne!(boot["me"]["account_id"], json!(f.org_account.to_string()));
    assert_eq!(boot["selfserve_org"], Value::Bool(false), "a personal account is never a self-serve org");

    // The real shell flags retain their actual authority on both planes.
    // ENV_LOCK is held through this matrix and restores the prior environment.
    for (activities, gist) in [(false, false), (true, false), (false, true), (true, true)] {
        unsafe {
            std::env::set_var("TODAY_ACTIVITIES_ENABLED", if activities { "true" } else { "false" });
            std::env::set_var("TODAY_GIST_ENABLED", if gist { "true" } else { "false" });
            std::env::set_var("ORG_SELFSERVE_SIGNUP_ENABLED", if activities { "true" } else { "false" });
        }
        for token in [&admin_token, &personal] {
            let (status, bytes) = get_raw(&f.app, "/v1/bootstrap", token).await;
            assert_eq!(status, StatusCode::OK);
            let body: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(body["activities_enabled"], activities);
            assert_eq!(body["gist_enabled"], gist);
            assert_eq!(body["selfserve_signup_enabled"], activities);
            assert_eq!(body.as_object().unwrap().len(), 10);
            assert_eq!(capture.take().len(), 1);
        }
    }

    // ── #1540 review SF-a: the same org, had POST /v1/orgs made it, is a
    //    self-serve org for its members (the web drops Clerk's org profile).
    sqlx::query(
        "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, clerk_org_id, account_id) \
         VALUES ($1, 'boot-selfserve-key', 'Acme', 2, 'created', $2, $3)",
    )
    .bind(requester)
    .bind(ORG)
    .bind(f.org_account)
    .execute(&f.control)
    .await
    .unwrap();
    let (status, bytes) = get_raw(&f.app, "/v1/bootstrap", &admin_token).await;
    assert_eq!(status, StatusCode::OK);
    let boot: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(boot["selfserve_org"], Value::Bool(true), "the active org was made by POST /v1/orgs");
}

#[tokio::test]
async fn removed_member_gets_the_same_403_as_v1_me() {
    let Some(base) = base_url() else {
        eprintln!("skipping removed_member_gets_the_same_403_as_v1_me: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = Env::enable().await;
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let f = fixture(&base, "removed").await;

    let victim_sub = format!("boot_victim_{}", Uuid::new_v4().simple());
    let token = org_token(&victim_sub, "org:member");
    let (status, _) = get_raw(&f.app, "/v1/bootstrap", &token).await;
    assert_eq!(status, StatusCode::OK, "a live member boots");

    post_clerk_webhook(
        &f.app,
        json!({
            "type": "organizationMembership.deleted",
            "data": { "organization": { "id": ORG }, "public_user_data": { "user_id": victim_sub } }
        }),
    )
    .await;

    // The same still-valid token, replayed.
    let (me_status, me_body) = get_raw(&f.app, "/v1/me", &token).await;
    let (boot_status, boot_body) = get_raw(&f.app, "/v1/bootstrap", &token).await;
    assert_eq!(me_status, StatusCode::FORBIDDEN, "{}", String::from_utf8_lossy(&me_body));
    assert_eq!(boot_status, me_status, "bootstrap must reject exactly like /v1/me");
    assert_eq!(boot_body, me_body, "and with the same body");
    let (opted_status, opted_body) = get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", &token).await;
    assert_eq!((opted_status, opted_body), (me_status, me_body));
    assert_eq!(capture.take().len(), 1, "removed-member request is refused by auth before handler entry");
}

/// Real router/auth/scoped PostgreSQL reads + the real Stripe client against
/// a loopback-only provider. The e2e feature permits that HTTP test boundary;
/// the production client and timeout policies are unchanged.
#[cfg(feature = "e2e")]
#[tokio::test]
async fn billing_provider_attribution_uses_only_the_already_executed_lookup() {
    use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
    use axum::response::IntoResponse;
    let base = base_url().expect("provider regression requires ORGT_TEST_BASE_URL");
    let _env = Env::enable().await;
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let mode = Arc::new(AtomicU8::new(0));
    let calls = Arc::new(AtomicUsize::new(0));
    let provider = axum::Router::new().route("/v1/customers/{customer}", axum::routing::get({
        let mode = mode.clone(); let calls = calls.clone();
        move || { let mode = mode.clone(); let calls = calls.clone(); async move {
            calls.fetch_add(1, Ordering::SeqCst);
            match mode.load(Ordering::SeqCst) {
                1 => std::future::pending::<axum::response::Response>().await,
                2 => (StatusCode::BAD_REQUEST, Json(json!({"error": {"type": "invalid_request_error", "message": "private-provider-error"}}))).into_response(),
                _ => Json(json!({"invoice_settings": {"default_payment_method": {"id": "pm_local_only", "type": "card", "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030}}}})).into_response(),
            }
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, provider).await.unwrap() });
    let stripe = coralswarm_backend::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_local_not_a_secret")
        .with_base_url(format!("http://{address}"));
    let f = fixture_with_provider(&base, "provider", Some(Arc::new(stripe))).await;
    let admin = org_token(&format!("provider_admin_{}", Uuid::new_v4().simple()), "org:admin");
    get_raw(&f.app, "/v1/me", &admin).await;
    // Enabled client, no existing customer: no lookup is added to classify it.
    let (status, _) = get_raw(&f.app, "/v1/bootstrap", &admin).await;
    assert_eq!(status, StatusCode::OK);
    let records = capture.take(); assert_eq!(records.len(), 1);
    assert_eq!(records[0]["billing"]["provider"]["outcome"], "skipped");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    financial_fixture::execute(&f.control, f.org_account,
        sqlx::query("INSERT INTO stripe_subscriptions(account_id, stripe_customer_id) VALUES ($1, 'cus_local_only')").bind(f.org_account)
    ).await.unwrap();

    for (mode_value, outcome) in [(0, "ok"), (2, "error"), (1, "cancelled")] {
        mode.store(mode_value, Ordering::SeqCst);
        let before = calls.load(Ordering::SeqCst);
        let (status, bytes) = get_raw(&f.app, "/v1/bootstrap", &admin).await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let records = capture.take(); assert_eq!(records.len(), 1);
        let r = &records[0];
        assert_eq!(r["billing"]["db"]["outcome"], "ok");
        assert_eq!(r["billing"]["provider"]["outcome"], outcome);
        assert_eq!(r["billing"]["provider"]["executed"], true);
        assert_eq!(calls.load(Ordering::SeqCst), before + 1, "no observational provider call");
        let db_end = r["billing"]["db"]["end_us"].as_u64().unwrap();
        let provider_start = r["billing"]["provider"]["start_us"].as_u64().unwrap();
        let provider_end = r["billing"]["provider"]["end_us"].as_u64().unwrap();
        assert!(db_end <= provider_start);
        assert!(provider_end <= r["complete_elapsed_us"].as_u64().unwrap());
        if mode_value == 1 {
            assert_eq!(r["branches"]["billing_banner"]["outcome"], "timeout");
            assert!(body["billing_banner"].is_null());
        } else {
            let (status, summary) = get_raw(&f.app, "/v1/billing/summary", &admin).await;
            assert_eq!(status, StatusCode::OK);
            assert_eq!(body["billing_banner"], serde_json::from_slice::<Value>(&summary).unwrap());
            assert!(capture.take().is_empty(), "standalone summary emits no bootstrap record");
        }
        let serialized = serde_json::to_string(r).unwrap();
        for private in ["cus_local_only", "pm_local_only", "last4", "private-provider-error", "sk_test", "account_id", "user_id"] {
            assert!(!serialized.contains(private));
        }
    }
    mode.store(0, Ordering::SeqCst);
    let member = org_token(&format!("provider_member_{}", Uuid::new_v4().simple()), "org:member");
    let before = calls.load(Ordering::SeqCst);
    let (status, _) = get_raw(&f.app, "/v1/billing/summary", &member).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "purchase authorization unchanged");
    let (status, bytes) = get_raw(&f.app, "/v1/bootstrap", &member).await;
    assert_eq!(status, StatusCode::OK);
    assert!(serde_json::from_slice::<Value>(&bytes).unwrap()["billing_banner"].is_null());
    assert_eq!(calls.load(Ordering::SeqCst), before);
    let records = capture.take(); assert_eq!(records.len(), 1);
    assert_eq!(records[0]["branches"]["billing_banner"]["outcome"], "skipped");
    assert_eq!(records[0]["billing"]["provider"]["outcome"], "not_reached");
    server.abort(); let _ = server.await;
}

/// Red on released main: the unknown opt-in is ignored and boot calls the
/// held provider. Green must complete with real core data without that call.
#[cfg(feature = "e2e")]
#[tokio::test]
async fn trial_banner_boot_is_independent_of_payment_provider() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let base = base_url().expect("provider barrier requires ORGT_TEST_BASE_URL");
    let _env = Env::enable().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let provider = axum::Router::new().route("/v1/customers/{customer}", axum::routing::get({
        let calls = calls.clone(); let release = release.clone();
        move || { let calls = calls.clone(); let release = release.clone(); async move {
            calls.fetch_add(1, Ordering::SeqCst);
            release.acquire().await.unwrap().forget();
            Json(json!({"invoice_settings": {"default_payment_method": {"id": "pm_barrier", "type": "card", "card": {"brand": "visa", "last4": "4242", "exp_month": 12, "exp_year": 2030}}}}))
        }}
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, provider).await.unwrap() });
    let stripe = coralswarm_backend::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_local_not_a_secret")
        .with_base_url(format!("http://{address}"));
    let f = fixture_with_provider(&base, "banner_barrier", Some(Arc::new(stripe))).await;
    let admin = org_token(&format!("barrier_admin_{}", Uuid::new_v4().simple()), "org:admin");
    let (me_status, me) = get_raw(&f.app, "/v1/me", &admin).await;
    let (graphs_status, graphs) = get_raw(&f.app, "/v1/graphs", &admin).await;
    assert_eq!(me_status, StatusCode::OK); assert_eq!(graphs_status, StatusCode::OK);
    financial_fixture::execute(&f.control, f.org_account,
        sqlx::query("INSERT INTO stripe_subscriptions(account_id, stripe_customer_id) VALUES ($1, 'cus_barrier')").bind(f.org_account)
    ).await.unwrap();
    // The barrier, rather than a fast fake response, is the causal assertion.
    // No implementation is patched to create this red.
    let start = std::time::Instant::now();
    let result = tokio::time::timeout(std::time::Duration::from_millis(400),
        get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", &admin)).await;
    let calls_before_release = calls.load(Ordering::SeqCst);
    if result.is_err() || calls_before_release != 0 {
        release.add_permits(4); server.abort();
        panic!("banner boot must complete before provider release, without invoking it: completed={}, provider_calls={calls_before_release}", result.is_ok());
    }
    let (status, bytes) = result.unwrap();
    let boot: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(status, StatusCode::OK);
    assert_eq!(boot["me"], serde_json::from_slice::<Value>(&me).unwrap());
    assert_eq!(boot["graphs"], serde_json::from_slice::<Value>(&graphs).unwrap());
    assert_eq!(boot["billing_banner_mode"], "trial_v1");
    assert_eq!(boot["trial_banner"], json!({"kind":"trial_banner_v1", "trial":null}));
    assert!(boot.get("billing_banner").is_none(), "trial data cannot impersonate a full summary");
    eprintln!("banner barrier: core and banner completed in {:?}; provider calls=0", start.elapsed());

    // Both legacy callers still wait for payment decoration and receive the
    // complete summary after the same controlled barrier is released.
    for path in ["/v1/bootstrap", "/v1/billing/summary"] {
        let before = calls.load(Ordering::SeqCst);
        let request = get_raw(&f.app, path, &admin);
        tokio::pin!(request);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(150), &mut request).await.is_err(), "{path} must wait for provider");
        assert_eq!(calls.load(Ordering::SeqCst), before + 1);
        release.add_permits(1);
        let (status, bytes) = request.await;
        assert_eq!(status, StatusCode::OK);
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        let summary = if path == "/v1/bootstrap" { &body["billing_banner"] } else { &body };
        assert_eq!(summary["payment_method"]["last4"], "4242");
        assert_eq!(summary["payment_method"]["type"], "card");
        for field in ["account_kind", "plan", "seats", "credits", "extra_usage", "subscription", "payment_method", "trial"] {
            assert!(summary.get(field).is_some(), "complete summary: {field}");
        }
    }
    server.abort(); let _ = server.await;
}

#[tokio::test]
async fn trial_banner_contract_preserves_authority_states_and_optional_unknown() {
    let Some(base) = base_url() else { return; };
    let _env = Env::enable().await;
    let capture = bootstrap_trace::Capture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let f = fixture(&base, "banner_contract").await;
    let admin = org_token(&format!("banner_admin_{}", Uuid::new_v4().simple()), "org:admin");
    let member = org_token(&format!("banner_member_{}", Uuid::new_v4().simple()), "org:member");
    let personal = personal_token(&format!("banner_personal_{}", Uuid::new_v4().simple()));
    for (token, eligible) in [(&admin, true), (&member, false), (&personal, false)] {
        get_raw(&f.app, "/v1/me", token).await;
        get_raw(&f.app, "/v1/graphs", token).await;
        let (_, legacy) = get_raw(&f.app, "/v1/bootstrap", token).await;
        let legacy: Value = serde_json::from_slice(&legacy).unwrap();
        let (status, opted) = get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", token).await;
        assert_eq!(status, StatusCode::OK);
        let opted: Value = serde_json::from_slice(&opted).unwrap();
        let mut expected = legacy.clone();
        expected.as_object_mut().unwrap().remove("billing_banner");
        expected["billing_banner_mode"] = json!("trial_v1");
        expected["trial_banner"] = if eligible { json!({"kind":"trial_banner_v1", "trial":null}) } else { Value::Null };
        assert_eq!(opted, expected, "only the opted-in billing contract changes");
        let records = capture.take(); assert_eq!(records.len(), 2);
        assert_eq!(records[1]["billing"]["provider"]["executed"], false);
        assert_eq!(records[1]["billing"]["provider"]["outcome"], if eligible { "skipped" } else { "not_reached" });
        let (status, _) = get_raw(&f.app, "/v1/billing/trial-banner", token).await;
        assert_eq!(status, if token == &member { StatusCode::FORBIDDEN } else { StatusCode::OK });
        let (_, unknown) = get_raw(&f.app, "/v1/bootstrap?billing_banner=unknown", token).await;
        assert_eq!(serde_json::from_slice::<Value>(&unknown).unwrap(), legacy);
        capture.take();
    }

    financial_fixture::execute(&f.control, f.org_account, sqlx::query(
        "INSERT INTO org_trials(account_id,state,seats,trial_eligible) VALUES ($1,'checkout_open',2,true)"
    ).bind(f.org_account)).await.unwrap();
    for state in ["checkout_open", "trialing", "converting", "converted", "abandoned", "canceled"] {
        financial_fixture::execute(&f.control, f.org_account, sqlx::query(
            "UPDATE org_trials SET state=$2, pool_baseline=3000, trial_started_at=now(), trial_ends_at=now()+interval '90 days', \
             stripe_subscription_id='sub_banner_local', convert_reason='credits_used', convert_requested_at=now(), \
             payment_failed_at=now(), converted_at=CASE WHEN $2='converted' THEN now() END, \
             ended_at=CASE WHEN $2 IN ('abandoned','canceled') THEN now() END, \
             end_reason=CASE WHEN $2 IN ('abandoned','canceled') THEN 'fixture' END WHERE account_id=$1"
        ).bind(f.org_account).bind(state)).await.unwrap();
        let (_, full) = get_raw(&f.app, "/v1/billing/summary", &admin).await;
        let full: Value = serde_json::from_slice(&full).unwrap();
        let (status, banner) = get_raw(&f.app, "/v1/billing/trial-banner", &admin).await;
        assert_eq!(status, StatusCode::OK);
        let banner: Value = serde_json::from_slice(&banner).unwrap();
        assert_eq!(banner, json!({"kind":"trial_banner_v1", "trial":full["trial"]}));
        assert_eq!(banner["trial"]["state"], state);
        assert_eq!(banner["trial"]["payment_failed"], state == "converting");
        let (status, boot) = get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", &admin).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(serde_json::from_slice::<Value>(&boot).unwrap()["trial_banner"], banner);
        capture.take();
    }
    // A real DB lock holds only the optional trial branch. It times out as
    // unknown, retaining usable required data and the original deadline.
    let mut lock = f.control.begin().await.unwrap();
    sqlx::query("LOCK TABLE org_trials IN ACCESS EXCLUSIVE MODE").execute(&mut *lock).await.unwrap();
    let (status, boot) = get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", &admin).await;
    assert_eq!(status, StatusCode::OK);
    let boot: Value = serde_json::from_slice(&boot).unwrap();
    assert!(boot["trial_banner"].is_null());
    assert!(boot["me"].is_object()); assert!(boot["graphs"].is_array());
    let records = capture.take(); assert_eq!(records.len(), 1);
    assert_eq!(records[0]["branches"]["billing_banner"]["outcome"], "timeout");
    assert_eq!(records[0]["billing"]["db"]["outcome"], "cancelled");
    assert_eq!(records[0]["billing"]["provider"]["executed"], false);
    lock.rollback().await.unwrap();
    sqlx::query("ALTER TABLE org_trials RENAME TO banner_fixture_hidden_trials").execute(&f.control).await.unwrap();
    let (status, boot) = get_raw(&f.app, "/v1/bootstrap?billing_banner=trial_v1", &admin).await;
    sqlx::query("ALTER TABLE banner_fixture_hidden_trials RENAME TO org_trials").execute(&f.control).await.unwrap();
    assert_eq!(status, StatusCode::OK);
    assert!(serde_json::from_slice::<Value>(&boot).unwrap()["trial_banner"].is_null());
    let records = capture.take(); assert_eq!(records.len(), 1);
    assert_eq!(records[0]["branches"]["billing_banner"]["outcome"], "error");
}
