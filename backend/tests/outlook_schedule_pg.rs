//! Outlook compose capture v2 over real HTTP (`routes::outlook_schedule`): an ORG
//! `csms_` Microsoft SSO credential reaches the new routes through the actual router
//! (`test_support_app`), org-only, per-member isolation, caps, Stop.
//!
//! Auth is the same path as the existing Outlook/Teams meeting-capture routes: the
//! Microsoft SSO session minted by `/v1/addin/microsoft/session` after a pairing
//! approval (see `addin_microsoft_pg.rs`, whose harness this file reuses).
//!
//! Gated on `ORGT_TEST_BASE_URL`; without it every test prints `skipping ...` and
//! passes vacuously. Run locally:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test outlook_schedule_pg -- --test-threads=1 --nocapture

#![allow(dead_code)]

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const APP_ID: &str = "00000000-0000-0000-0000-000000000000";
const TID: &str = "11111111-2222-3333-4444-555555555555";
const OID: &str = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";

/// Serializes the whole file: process-global env (flag, JWKS URL, Clerk issuer)
/// and migration 0083's cluster-wide `CREATE ROLE`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers ───────────────────────────────────────────────────────

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
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
async fn drop_db(base: &str, db: &str) {
    let mut conn = admin(base).await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// Serves the repo's test JWKS at `/keys` — used as BOTH Microsoft's key
/// endpoint and Clerk's (one throwaway key signs both token families).
async fn spawn_jwks() -> String {
    let body = test_jwks();
    let app = axum::Router::new().route(
        "/keys",
        axum::routing::get(move || {
            let body = body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind jwks");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/keys")
}

/// One test's world: scratch control DB, router, env, env-lock guard.
struct World {
    _database: coralswarm_backend::test_db::TestDatabase,
    base: String,
    db: String,
    pool: PgPool,
    app: axum::Router,
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl World {
    async fn new(base: &str, db: &str) -> Self {
        Self::build(base, db, true, None).await
    }

    /// `sso_enabled=false` leaves the flag unset (the default).
    async fn build(
        base: &str,
        db: &str,
        sso_enabled: bool,
        runtime: Option<Arc<TenantRuntime>>,
    ) -> Self {
        let database = coralswarm_backend::test_db::TestDatabase::adopt_from_url(base, db);
        let guard = ENV_LOCK.lock().await;
        let jwks = spawn_jwks().await;
        // SAFETY: serialized by `ENV_LOCK`, held for this struct's lifetime.
        unsafe {
            if sso_enabled {
                std::env::set_var("ADDIN_MICROSOFT_SSO_ENABLED", "1");
            } else {
                std::env::remove_var("ADDIN_MICROSOFT_SSO_ENABLED");
            }
            std::env::set_var("MICROSOFT_ADDIN_JWKS_URL", &jwks);
            std::env::remove_var("MICROSOFT_ADDIN_APP_ID");
            std::env::remove_var("MICROSOFT_ADDIN_APP_ID_URI");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", &jwks);
        }
        let mut conn = admin(base).await;
        recreate(&mut conn, db).await;
        let pool = PgPoolOptions::new()
            .max_connections(6)
            .connect(&with_db(base, db))
            .await
            .expect("connect scratch DB");
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .expect("migrate control tree");
        let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
        state.db = Some(pool.clone());
        state.store = Some(Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>);
        state.tenant_runtime = runtime;
        let app = coralswarm_backend::test_support_app(Arc::new(state));
        Self {
            _database: database,
            base: base.to_string(),
            db: db.to_string(),
            pool,
            app,
            _guard: guard,
        }
    }

    async fn finish(self) {
        self.pool.close().await;
        drop_db(&self.base, &self.db).await;
    }

    async fn call(&self, req: Request<Body>) -> (StatusCode, Value) {
        let resp = self.app.clone().oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        let body = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        (status, body)
    }
}

impl Drop for World {
    fn drop(&mut self) {
        unsafe {
            for k in [
                "ADDIN_MICROSOFT_SSO_ENABLED",
                "MICROSOFT_ADDIN_JWKS_URL",
                "CLERK_ISSUER",
                "CLERK_JWKS_URL",
                "ORG_MICROSOFT_TEAMS_ENABLED",
            ] {
                std::env::remove_var(k);
            }
        }
    }
}

// ── Request / token builders ─────────────────────────────────────────────────

fn req(method: &str, uri: &str, bearer: Option<&str>, body: Option<Value>, host: Option<&str>) -> Request<Body> {
    let mut b = Request::builder().method(method).uri(uri);
    if let Some(t) = bearer {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    if let Some(h) = host {
        b = b.header("host", h);
    }
    match body {
        Some(v) => b
            .header("content-type", "application/json")
            .body(Body::from(v.to_string()))
            .unwrap(),
        None => b.body(Body::empty()).unwrap(),
    }
}

fn ms_claims(tid: &str, oid: &str) -> Value {
    json!({
        "iss": format!("https://login.microsoftonline.com/{tid}/v2.0"),
        "aud": APP_ID,
        "ver": "2.0",
        "tid": tid,
        "oid": oid,
        "scp": "access_as_user",
        "azp": "00000000-0000-4000-8000-e34a138d45f9",
        "exp": (Utc::now().timestamp() + 3600) as usize,
        "nbf": (Utc::now().timestamp() - 60) as usize,
    })
}

fn ms_token(tid: &str, oid: &str) -> String {
    mint(ms_claims(tid, oid))
}

fn clerk_jwt(sub: &str) -> String {
    mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": (Utc::now().timestamp() + 3600) as usize,
        "sid": format!("sess_{}", Uuid::new_v4()),
    }))
}

async fn session(w: &World, token: &str) -> (StatusCode, Value) {
    w.call(req("POST", "/v1/addin/microsoft/session", None, Some(json!({"ms_token": token})), None))
        .await
}

async fn poll(w: &World, code: &str, secret: &str) -> (StatusCode, Value) {
    w.call(req(
        "POST",
        "/v1/addin/pair/poll",
        None,
        Some(json!({"code": code, "poll_secret": secret})),
        None,
    ))
    .await
}

async fn approve(w: &World, clerk_sub: &str, body: Value) -> (StatusCode, Value) {
    // `Host: APEX` so the same helper works when the binding gate is enforcing.
    w.call(req("POST", "/v1/addin/pair/approve", Some(&clerk_jwt(clerk_sub)), Some(body), Some(APEX)))
        .await
}

// ── Seeding ──────────────────────────────────────────────────────────────────

/// A user with a Clerk subject (so a Clerk JWT for `sub` resolves to them) and
/// their personal account. Returns `(user_id, personal_account_id)`.
async fn seed_user(pool: &PgPool, sub: &str) -> (Uuid, Uuid) {
    let user: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
    )
    .bind(format!("{sub}@example.test"))
    .bind(sub)
    .fetch_one(pool)
    .await
    .unwrap();
    let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
        .bind(user)
        .fetch_one(pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
    (user, account)
}

async fn seed_org(pool: &PgPool, clerk_org_id: &str, owner: Uuid) -> Uuid {
    sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

async fn add_member(pool: &PgPool, account: Uuid, user: Uuid) {
    // Org members must carry a seat tier (migration 0169's trigger refuses a bare insert).
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
}

async fn remove_member(pool: &PgPool, account: Uuid, user: Uuid) {
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(account)
        .bind(user)
        .execute(pool)
        .await
        .unwrap();
}

/// A "Connect Microsoft" row — exactly what the connector stores: tenant id in
/// `provider_workspace_id`, object id in `provider_user_id`.
async fn seed_ms_connection(pool: &PgPool, account: Uuid, owner: Option<Uuid>, tid: &str, oid: &str) {
    sqlx::query(
        "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id, \
         provider_user_id, owner_user_id) VALUES ($1, 'microsoft', 'x', '\\x00', $2, $3, $4)",
    )
    .bind(account)
    .bind(tid)
    .bind(oid)
    .bind(owner)
    .execute(pool)
    .await
    .unwrap();
}


macro_rules! need_db {
    ($name:literal) => {
        match base_url() {
            Some(b) => b,
            None => {
                eprintln!(concat!("skipping ", $name, ": set ORGT_TEST_BASE_URL"));
                return;
            }
        }
    };
}

/// An org with two members, a real tenant database, and a `csms_` credential per member.
struct Org {
    w: World,
    tenant_db: String,
    base: String,
    alice_token: String,
    bob_token: String,
    personal_token: String,
}

async fn org_world(base: &str, name: &str) -> Org {
    let tenant_db = format!("{name}_tenant");
    let mut conn = admin(base).await;
    recreate(&mut conn, &tenant_db).await;
    let tpool = PgPoolOptions::new().connect(&with_db(base, &tenant_db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tpool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta(id, org_id) VALUES (true, 'org_acme')")
        .execute(&tpool)
        .await
        .unwrap();
    tpool.close().await;

    let resolver = StaticMapResolver::from_pairs([("SECRET_ACME", with_db(base, &tenant_db))]);
    let runtime = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let w = World::build(base, name, true, Some(runtime)).await;
    unsafe { std::env::set_var("ORG_MICROSOFT_TEAMS_ENABLED", "1") };

    let (alice, alice_personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;
    add_member(&w.pool, org, bob).await;
    financial_fixture::execute(&w.pool, org, sqlx::query("INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) VALUES ('org_acme', $1, 'acme', 'SECRET_ACME', 'active')")
        .bind(org))
        .await
        .unwrap();
    // Each member's primary private ocean in the tenant database.
    for user in [alice, bob] {
        let t = PgPoolOptions::new().connect(&with_db(base, &tenant_db)).await.unwrap();
        sqlx::query("INSERT INTO graphs(id,account_id,owner_user_id,kind,name,is_primary) VALUES(gen_random_uuid(),$1,$2,'member','Private',true)")
            .bind(org)
            .bind(user)
            .execute(&t)
            .await
            .unwrap();
        t.close().await;
    }
    // Each member's own connected Microsoft account (what `identity()` reads).
    seed_ms_connection(&w.pool, org, Some(alice), "aaaaaaaa-0000-0000-0000-00000000000a", "aaaaaaaa-0000-0000-0000-0000000000a1").await;
    seed_ms_connection(&w.pool, org, Some(bob), "aaaaaaaa-0000-0000-0000-00000000000a", "bbbbbbbb-0000-0000-0000-0000000000b1").await;

    // Pair each member's Microsoft identity to the ORG workspace -> csms_ credentials.
    let mut tokens = Vec::new();
    for (clerk, tid, oid) in [
        ("user_alice", "66666666-6666-6666-6666-666666666666", "99999999-9999-9999-9999-999999999991"),
        ("user_bob", "66666666-6666-6666-6666-666666666666", "99999999-9999-9999-9999-999999999992"),
    ] {
        let (_, s) = session(&w, &ms_token(tid, oid)).await;
        let code = s["pairing"]["code"].as_str().unwrap().to_string();
        let (st, ab) = approve(&w, clerk, json!({"code": code, "account_id": org})).await;
        assert_eq!(st, StatusCode::OK, "{clerk}: {ab}");
        let (_, c) = w
            .call(req("POST", "/v1/addin/microsoft/session", None, Some(json!({"ms_token": ms_token(tid, oid)})), Some(APEX)))
            .await;
        assert_eq!(c["workspace"]["kind"], "org", "{c}");
        tokens.push(c["token"].as_str().unwrap().to_string());
    }
    // Alice also has a PERSONAL-workspace link under another identity.
    seed_ms_connection(&w.pool, alice_personal, Some(alice), TID, OID).await;
    let (_, p) = w
        .call(req("POST", "/v1/addin/microsoft/session", None, Some(json!({"ms_token": ms_token(TID, OID)})), Some(APEX)))
        .await;
    Org {
        w,
        tenant_db,
        base: base.to_string(),
        alice_token: tokens[0].clone(),
        bob_token: tokens[1].clone(),
        personal_token: p["token"].as_str().unwrap().to_string(),
    }
}

impl Org {
    async fn call(&self, method: &str, uri: &str, token: Option<&str>) -> (StatusCode, Value) {
        self.w.call(req(method, uri, token, None, Some(APEX))).await
    }
    async fn finish(self) {
        self.w.finish().await;
        drop_db(&self.base, &self.tenant_db).await;
    }
}

#[tokio::test]
async fn org_sso_credential_drives_create_status_lookup_and_stop_pg() {
    let base = need_db!("org_sso_credential_drives_create_status_lookup_and_stop_pg");
    let o = org_world(&base, "outlook_sched_http").await;
    let base_path = "/v1/outlook-capture-schedule";

    // Unauthenticated: refused before anything else.
    let (status, _) = o.call("POST", &format!("{base_path}/intents"), None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // Create.
    let (status, created) = o.call("POST", &format!("{base_path}/intents"), Some(&o.alice_token)).await;
    assert_eq!(status, StatusCode::CREATED, "{created}");
    let id = created["intent_id"].as_str().unwrap().to_string();
    assert!(Uuid::parse_str(&id).is_ok());
    let nonce = created["nonce"].as_str().unwrap().to_string();
    assert!(Uuid::parse_str(&nonce).is_ok());
    let expires = chrono::DateTime::parse_from_rfc3339(created["expires_at"].as_str().unwrap()).unwrap();
    let hours = (expires.with_timezone(&Utc) - Utc::now()).num_minutes();
    assert!((23 * 60..=24 * 60 + 1).contains(&hours), "24h lifetime, got {hours} min");

    // Status: exactly the contract's keys.
    let (status, body) = o.call("GET", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    // expires_at is the same 24h deadline the create call returned.
    assert_eq!(body["expires_at"], created["expires_at"], "{body}");
    let mut body = body;
    body.as_object_mut().unwrap().remove("expires_at");
    assert_eq!(
        body,
        json!({"status":"pending_save","error_code":null,"recurring":false,"next_occurrence_start":null,"auto_transcription":null,"manual_reason":null})
    );

    // Lookup by nonce carries the id. (by-event is not exercised here: an id unknown to the DB
    // goes on to Graph, which this suite does not fake.)
    let (status, body) = o.call("GET", &format!("{base_path}/by-nonce/{nonce}"), Some(&o.alice_token)).await;
    assert_eq!((status, body["intent_id"].as_str()), (StatusCode::OK, Some(id.as_str())), "{body}");

    // Another member of the same org cannot see or stop it.
    let (status, _) = o.call("GET", &format!("{base_path}/intents/{id}"), Some(&o.bob_token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = o.call("GET", &format!("{base_path}/by-nonce/{nonce}"), Some(&o.bob_token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (status, _) = o.call("DELETE", &format!("{base_path}/intents/{id}"), Some(&o.bob_token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    let (_, body) = o.call("GET", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!(body["status"], "pending_save", "bob's DELETE must not have stopped it");

    // Unknown id.
    let (status, _) = o.call("GET", &format!("{base_path}/intents/{}", Uuid::new_v4()), Some(&o.alice_token)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    // Stop: 204, idempotent, and the status reflects it.
    let (status, _) = o.call("DELETE", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (status, _) = o.call("DELETE", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    let (_, body) = o.call("GET", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!((body["status"].as_str(), body["error_code"].is_null()), (Some("stopped"), true), "{body}");

    // Scope layer: the credential can use the documented shapes and nothing near them.
    let (status, body) = o.call("PUT", &format!("{base_path}/intents/{id}"), Some(&o.alice_token)).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("addin_session_scope")));
    o.finish().await;
}

#[tokio::test]
async fn pending_cap_and_personal_workspace_refusal_pg() {
    let base = need_db!("pending_cap_and_personal_workspace_refusal_pg");
    let o = org_world(&base, "outlook_sched_caps").await;
    let uri = "/v1/outlook-capture-schedule/intents";
    for n in 0..10 {
        let (status, body) = o.call("POST", uri, Some(&o.alice_token)).await;
        assert_eq!(status, StatusCode::CREATED, "intent {n}: {body}");
    }
    let (status, body) = o.call("POST", uri, Some(&o.alice_token)).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::TOO_MANY_REQUESTS, Some("too_many_pending")), "{body}");
    // The cap is per member.
    let (status, _) = o.call("POST", uri, Some(&o.bob_token)).await;
    assert_eq!(status, StatusCode::CREATED);

    // A PERSONAL-workspace credential is refused: org workspace only.
    let (status, body) = o.call("POST", uri, Some(&o.personal_token)).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::FORBIDDEN, Some("organization_required")), "{body}");
    o.finish().await;
}
