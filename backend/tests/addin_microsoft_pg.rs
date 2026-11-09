//! Microsoft SSO sign-in for the Teams / Outlook panes (`routes::addin_microsoft`,
//! migration 0264) — live-Postgres proof driving REAL HTTP through the actual
//! public router (`test_support_app`).
//!
//! The Microsoft side is real RS256: tokens are signed with the repo's throwaway
//! test key (`clerk::test_support`) and verified against a LOCAL JWKS listener
//! (`MICROSOFT_ADDIN_JWKS_URL`), so signature / issuer / audience / scope /
//! lifetime checks run end to end — only Microsoft's actual endpoint is
//! substituted.
//!
//! What this file proves (each is one test, named for the property):
//!   * flag off → `404 microsoft_sso_disabled`; bad tokens → `401
//!     invalid_microsoft_token` through the router (the exhaustive claim matrix
//!     is the unit suite in `microsoft_sso.rs`);
//!   * `link_required` creates a pairing BOUND to the verified `(tid, oid)`; the
//!     approve step links THAT identity (a client-supplied identity is ignored),
//!     verifies workspace membership server-side, and never mints a PAT;
//!   * `pair/poll` for a Microsoft pairing: `pending` → `approved` with no token;
//!   * auto-link: exactly one prior "Connect Microsoft" match links; 0 / 2 users /
//!     1 user in 2 workspaces do not; a Disconnect is never silently undone;
//!   * membership loss: the link dies, the live credential 401s;
//!   * change workspace re-binds for Teams AND Outlook (old credentials revoked);
//!     disconnect revokes them all;
//!   * the credential's scope allowlist, and — with the binding gate enforcing —
//!     that a personal link is served the personal plane and an ORG link is bound
//!     to the org's tenant plane, never the consumer plane (and is refused, not
//!     downgraded, when the org is not servable).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a Postgres connection with CREATE DATABASE
//! rights); every test prints `skipping ...: set ORGT_TEST_BASE_URL` and returns
//! otherwise — which is a VACUOUS pass. CI greps for that line (see the step in
//! `.github/workflows/ci.yml`).
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test addin_microsoft_pg -- --test-threads=1 --nocapture

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
const ADDIN_CLIENT: &str = "example_18";
const GMAIL_CLIENT: &str = "GmailAddinClient0001";
const MEET_CLIENT: &str = "MeetAddinClient00001";
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
            std::env::set_var("ADDIN_CLERK_OAUTH_CLIENT_ID", ADDIN_CLIENT);
            std::env::set_var("GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID", GMAIL_CLIENT);
            std::env::set_var("MEET_ADDIN_CLERK_OAUTH_CLIENT_ID", MEET_CLIENT);
            std::env::remove_var("REST_FOREIGN_OAUTH");
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
                "ADDIN_CLERK_OAUTH_CLIENT_ID",
                "GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID",
                "MEET_ADDIN_CLERK_OAUTH_CLIENT_ID",
                "REST_FOREIGN_OAUTH",
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

/// A Clerk OAuth access token: NO `sid`, a `client_id`, optionally an `org_id`.
fn oauth_jwt(sub: &str, client_id: &str, org_id: Option<&str>) -> String {
    let mut c = json!({
        "iss": ISSUER,
        "sub": sub,
        "client_id": client_id,
        "scope": "openid profile email offline_access user:org:read",
        "exp": (Utc::now().timestamp() + 3600) as usize,
    });
    if let Some(o) = org_id {
        c["org_id"] = json!(o);
    }
    mint(c)
}

fn addin_jwt(sub: &str, org_id: Option<&str>) -> String {
    oauth_jwt(sub, ADDIN_CLIENT, org_id)
}

async fn link(w: &World, bearer: &str, ms: &str) -> (StatusCode, Value) {
    w.call(req("POST", "/v1/addin/microsoft/link", Some(bearer), Some(json!({"ms_token": ms})), Some(APEX)))
        .await
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

async fn link_row(pool: &PgPool, tid: &str, oid: &str) -> Option<(Uuid, Uuid, String, bool)> {
    sqlx::query_as(
        "SELECT user_id, account_id, link_method, disconnected_at IS NOT NULL \
         FROM addin_microsoft_links WHERE tenant_id = $1 AND object_id = $2",
    )
    .bind(tid)
    .bind(oid)
    .fetch_optional(pool)
    .await
    .unwrap()
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

// ── Tests ────────────────────────────────────────────────────────────────────

#[tokio::test]
async fn flag_off_answers_404_and_never_touches_the_token_pg() {
    let base = need_db!("flag_off_answers_404_and_never_touches_the_token_pg");
    let w = World::build(&base, "addin_ms_pg_flag", false, None).await;
    let (status, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body, json!({"error": "microsoft_sso_disabled"}));
    // Not even a bad token distinguishes: the flag is the first gate.
    let (status, body) = session(&w, "garbage").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert_eq!(body["error"], "microsoft_sso_disabled");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_pairing_codes")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "a disabled flag must write nothing");
    w.finish().await;
}

#[tokio::test]
async fn invalid_microsoft_tokens_are_401_through_the_router_pg() {
    let base = need_db!("invalid_microsoft_tokens_are_401_through_the_router_pg");
    let w = World::new(&base, "addin_ms_pg_invalid").await;
    let mut wrong_aud = ms_claims(TID, OID);
    wrong_aud["aud"] = json!("00000000-0000-0000-0000-000000000000");
    let mut no_scope = ms_claims(TID, OID);
    no_scope.as_object_mut().unwrap().remove("scp");
    let mut expired = ms_claims(TID, OID);
    expired["exp"] = json!((Utc::now().timestamp() - 3600) as usize);
    let mut wrong_iss = ms_claims(TID, OID);
    wrong_iss["iss"] = json!("https://login.microsoftonline.com/99999999-2222-3333-4444-555555555555/v2.0");
    let mut other_client = ms_claims(TID, OID);
    other_client["azp"] = json!("00000000-1111-2222-3333-444444444444");
    let mut no_client = ms_claims(TID, OID);
    no_client.as_object_mut().unwrap().remove("azp");
    let good = ms_token(TID, OID);
    let (head, sig) = good.rsplit_once('.').unwrap();
    let bad_sig = format!("{head}.{}{}", if sig.starts_with('A') { 'B' } else { 'A' }, &sig[1..]);

    for (label, token) in [
        ("garbage", "not-a-jwt".to_string()),
        ("empty", String::new()),
        ("wrong audience", mint(wrong_aud)),
        ("missing scope", mint(no_scope)),
        ("expired", mint(expired)),
        ("wrong issuer", mint(wrong_iss)),
        ("unlisted client application", mint(other_client)),
        ("missing azp", mint(no_client)),
        ("bad signature", bad_sig),
    ] {
        let (status, body) = session(&w, &token).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{label}");
        assert_eq!(body, json!({"error": "invalid_microsoft_token"}), "{label}");
    }
    let n: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM addin_pairing_codes) + (SELECT count(*) FROM addin_microsoft_links)")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "a refused token must create no pairing and no link");
    w.finish().await;
}

#[tokio::test]
async fn session_endpoint_is_rate_limited_before_signature_work_pg() {
    let base = need_db!("session_endpoint_is_rate_limited_before_signature_work_pg");
    let w = World::new(&base, "addin_ms_pg_rate").await;
    let mut last = StatusCode::OK;
    for _ in 0..125 {
        last = session(&w, "garbage").await.0;
    }
    assert_eq!(last, StatusCode::TOO_MANY_REQUESTS, "125 calls in a minute must trip the per-IP ceiling");
    w.finish().await;
}

/// The core happy path, and the identity-binding property: pairing → approve →
/// poll → session → credential.
#[tokio::test]
async fn pairing_is_bound_to_the_verified_identity_and_links_only_that_identity_pg() {
    let base = need_db!("pairing_is_bound_to_the_verified_identity_and_links_only_that_identity_pg");
    let w = World::new(&base, "addin_ms_pg_pairing").await;
    let (user, personal) = seed_user(&w.pool, "user_alice").await;

    // 1. No link, no prior Connect Microsoft → link_required + a bound pairing.
    let (status, body) = session(&w, &ms_token(TID, &OID.to_uppercase())).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "link_required");
    let code = body["pairing"]["code"].as_str().unwrap().to_string();
    let secret = body["pairing"]["poll_secret"].as_str().unwrap().to_string();
    assert_eq!(code.len(), 8);
    assert!(
        body["pairing"]["approve_url"].as_str().unwrap().ends_with(&format!("/addin/approve?code={code}&kind=microsoft")),
        "approve_url shape: {}",
        body["pairing"]["approve_url"]
    );
    assert!(body["pairing"]["expires_at"].is_string());
    // The row is bound, at creation, to the VERIFIED identity (oid lower-cased).
    let (kind, tid, oid): (String, Option<String>, Option<String>) = sqlx::query_as(
        "SELECT kind, ms_tenant_id, ms_object_id FROM addin_pairing_codes WHERE code = $1",
    )
    .bind(&code)
    .fetch_one(&w.pool)
    .await
    .unwrap();
    assert_eq!((kind.as_str(), tid.as_deref(), oid.as_deref()), ("microsoft", Some(TID), Some(OID)));
    // The secret is stored only as a hash.
    let stored: String = sqlx::query_scalar("SELECT poll_secret_hash FROM addin_pairing_codes WHERE code = $1")
        .bind(&code)
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_ne!(stored, secret);

    // 2. Polling: pending; a wrong secret is indistinguishable from expired.
    assert_eq!(poll(&w, &code, &secret).await.1, json!({"status": "pending"}));
    assert_eq!(poll(&w, &code, "wrong-secret").await.1, json!({"status": "expired"}));

    // 3. Re-opening the pane replaces the pairing: ONE live row per identity.
    let (_, again) = session(&w, &ms_token(TID, OID)).await;
    let code2 = again["pairing"]["code"].as_str().unwrap().to_string();
    assert_ne!(code, code2);
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_pairing_codes WHERE kind = 'microsoft'")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(live, 1, "the identity's previous pairing must be replaced, not accumulated");
    let secret2 = again["pairing"]["poll_secret"].as_str().unwrap().to_string();
    assert_eq!(poll(&w, &code, &secret).await.1, json!({"status": "expired"}), "the old code is dead");

    // 4. Approve — with a CLIENT-SUPPLIED identity that must be ignored.
    let (status, body) = approve(
        &w,
        "user_alice",
        json!({"code": code2, "account_id": personal, "tid": "ffffffff-ffff-ffff-ffff-ffffffffffff",
               "tenant_id": "ffffffff-ffff-ffff-ffff-ffffffffffff", "oid": "evil", "object_id": "evil"}),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["approved"], true);
    assert_eq!(body["workspace"]["kind"], "personal");
    let link = link_row(&w.pool, TID, OID).await.expect("link keyed by the PAIRING's identity");
    assert_eq!((link.0, link.1, link.2.as_str(), link.3), (user, personal, "pairing", false));
    assert!(link_row(&w.pool, "ffffffff-ffff-ffff-ffff-ffffffffffff", "evil").await.is_none());
    let pats: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_tokens").fetch_one(&w.pool).await.unwrap();
    assert_eq!(pats, 0, "a Microsoft pairing must never mint a csat_ PAT");

    // 5. Poll: approved, NO token, and the row is spent.
    let (_, polled) = poll(&w, &code2, &secret2).await;
    assert_eq!(polled, json!({"status": "approved"}));
    assert_eq!(poll(&w, &code2, &secret2).await.1, json!({"status": "expired"}));

    // 6. The pane calls /session again → connected, and the credential works.
    let (status, conn) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(conn["status"], "connected");
    assert!(conn.get("auto_linked").is_none());
    assert_eq!(conn["account"]["email"], "user_alice@example.test");
    assert_eq!(conn["workspace"], json!({"account_id": personal, "name": "Personal", "kind": "personal"}));
    let token = conn["token"].as_str().unwrap();
    assert!(token.starts_with("csms_"));
    assert!(conn["expires_at"].is_string());
    let stored_hash: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_microsoft_sessions WHERE token_hash = $1")
        .bind(token)
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(stored_hash, 0, "the plaintext credential must never be stored");

    let (status, linkbody) = w.call(req("GET", "/v1/addin/microsoft/link", Some(token), None, None)).await;
    assert_eq!(status, StatusCode::OK, "{linkbody}");
    assert_eq!(linkbody["account"]["email"], "user_alice@example.test");
    assert_eq!(linkbody["workspace"]["account_id"], json!(personal));
    assert_eq!(linkbody["workspaces"][0]["kind"], "personal");
    w.finish().await;
}

#[tokio::test]
async fn approve_verifies_membership_server_side_and_keeps_gmail_unchanged_pg() {
    let base = need_db!("approve_verifies_membership_server_side_and_keeps_gmail_unchanged_pg");
    let w = World::new(&base, "addin_ms_pg_approve").await;
    let (alice, alice_personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, bob_personal) = seed_user(&w.pool, "user_bob").await;
    let org_member = seed_org(&w.pool, "org_member", bob).await;
    add_member(&w.pool, org_member, alice).await;
    let org_other = seed_org(&w.pool, "org_other", bob).await; // alice is NOT a member

    let (_, s) = session(&w, &ms_token(TID, OID)).await;
    let code = s["pairing"]["code"].as_str().unwrap().to_string();

    // Not a member / someone else's personal account / nonexistent → 403, no link.
    for (label, account) in [
        ("org she is not in", org_other),
        ("another user's personal account", bob_personal),
        ("nonexistent account", Uuid::new_v4()),
    ] {
        let (status, body) = approve(&w, "user_alice", json!({"code": code, "account_id": account})).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {body}");
    }
    assert!(link_row(&w.pool, TID, OID).await.is_none(), "a refused approval must not link");
    // The pairing is still approvable afterwards (a refusal does not burn it).
    let (status, _) = approve(&w, "user_alice", json!({"code": code, "account_id": org_member})).await;
    assert_eq!(status, StatusCode::OK, "an org she IS a live member of is pickable");
    let link = link_row(&w.pool, TID, OID).await.unwrap();
    assert_eq!((link.0, link.1), (alice, org_member));
    // Approving twice is a conflict, not a re-link.
    let (status, _) = approve(&w, "user_alice", json!({"code": code, "account_id": alice_personal})).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // A Microsoft pairing without a workspace must not fall into the Gmail path.
    let (_, s2) = session(&w, &ms_token(TID, "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")).await;
    let code_b = s2["pairing"]["code"].as_str().unwrap().to_string();
    let (status, _) = approve(&w, "user_alice", json!({"code": code_b})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let pats: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_tokens").fetch_one(&w.pool).await.unwrap();
    assert_eq!(pats, 0, "approving a Microsoft pairing the Gmail way must mint nothing");

    // A GMAIL pairing refuses a workspace choice (personal-only, unchanged)...
    let (_, g) = w.call(req("POST", "/v1/addin/pair/start", None, None, None)).await;
    let gcode = g["code"].as_str().unwrap().to_string();
    let (status, _) = approve(&w, "user_alice", json!({"code": gcode, "account_id": org_member})).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    // ...and still approves exactly as before without one.
    let (status, body) = approve(&w, "user_alice", json!({"code": gcode})).await;
    assert_eq!((status, body), (StatusCode::OK, json!({"approved": true})));
    let (_, polled) = poll(&w, &gcode, g["poll_secret"].as_str().unwrap()).await;
    assert_eq!(polled["status"], "approved");
    assert!(polled["token"].as_str().unwrap().starts_with("csat_"), "Gmail still gets its PAT");
    w.finish().await;
}

#[tokio::test]
async fn pair_info_lists_the_workspaces_a_user_may_pick_pg() {
    let base = need_db!("pair_info_lists_the_workspaces_a_user_may_pick_pg");
    let w = World::new(&base, "addin_ms_pg_info").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org_in = seed_org(&w.pool, "org_in", bob).await;
    add_member(&w.pool, org_in, alice).await;
    let _org_out = seed_org(&w.pool, "org_out", bob).await;
    financial_fixture::execute(&w.pool, org_in, sqlx::query("INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) VALUES ('org_in', $1, 'acme-co', 'S_IN', 'active')")
        .bind(org_in))
        .await
        .unwrap();

    let (_, s) = session(&w, &ms_token(TID, OID)).await;
    let code = s["pairing"]["code"].as_str().unwrap();

    // Unauthenticated → 401.
    let (status, _) = w.call(req("GET", &format!("/v1/addin/pair/info?code={code}"), None, None, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    let (status, body) = w
        .call(req("GET", &format!("/v1/addin/pair/info?code={code}"), Some(&clerk_jwt("user_alice")), None, None))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["kind"], "microsoft");
    let ws = body["workspaces"].as_array().unwrap();
    assert_eq!(ws.len(), 2, "personal + the ONE org she belongs to: {body}");
    assert_eq!(ws[0], json!({"account_id": personal, "name": "Personal", "kind": "personal"}));
    assert_eq!(ws[1]["account_id"], json!(org_in));
    assert_eq!(ws[1]["kind"], "org");
    assert_eq!(ws[1]["name"], "acme-co", "falls back to the routing slug when Clerk cannot name it");

    // A Gmail pairing offers only the personal workspace.
    let (_, g) = w.call(req("POST", "/v1/addin/pair/start", None, None, None)).await;
    let (_, body) = w
        .call(req(
            "GET",
            &format!("/v1/addin/pair/info?code={}", g["code"].as_str().unwrap()),
            Some(&clerk_jwt("user_alice")),
            None,
            None,
        ))
        .await;
    assert_eq!(body["kind"], "gmail");
    assert_eq!(body["workspaces"].as_array().unwrap().len(), 1);
    assert_eq!(body["workspaces"][0]["kind"], "personal");

    // Unknown code → the one indistinguishable 404.
    let (status, _) = w
        .call(req("GET", "/v1/addin/pair/info?code=ZZZZZZZZ", Some(&clerk_jwt("user_alice")), None, None))
        .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    w.finish().await;
}

#[tokio::test]
async fn auto_link_requires_exactly_one_prior_microsoft_connection_pg() {
    let base = need_db!("auto_link_requires_exactly_one_prior_microsoft_connection_pg");
    let w = World::new(&base, "addin_ms_pg_auto").await;
    let (alice, alice_personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _bob_personal) = seed_user(&w.pool, "user_bob").await;

    // 0 matches → link_required.
    let (_, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(body["status"], "link_required");
    assert!(link_row(&w.pool, TID, OID).await.is_none());

    // Exactly 1 (a member row, in her personal workspace) → auto-linked.
    seed_ms_connection(&w.pool, alice_personal, Some(alice), TID, &OID.to_uppercase()).await;
    let (status, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "connected");
    assert_eq!(body["auto_linked"], true);
    assert_eq!(body["workspace"]["account_id"], json!(alice_personal));
    let link = link_row(&w.pool, TID, OID).await.unwrap();
    assert_eq!((link.0, link.1, link.2.as_str()), (alice, alice_personal, "auto"));
    // Next time it is an ordinary link, no longer flagged.
    let (_, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(body["status"], "connected");
    assert!(body.get("auto_linked").is_none());

    // 2 USERS connected the same Microsoft account → ambiguous → link_required.
    let (t2, o2) = ("22222222-2222-2222-2222-222222222222", "cccccccc-cccc-cccc-cccc-cccccccccccc");
    seed_ms_connection(&w.pool, alice_personal, Some(alice), t2, o2).await;
    let bob_acct: Uuid = sqlx::query_scalar("SELECT id FROM accounts WHERE owner_user_id = $1")
        .bind(bob)
        .fetch_one(&w.pool)
        .await
        .unwrap();
    seed_ms_connection(&w.pool, bob_acct, Some(bob), t2, o2).await;
    let (_, body) = session(&w, &ms_token(t2, o2)).await;
    assert_eq!(body["status"], "link_required", "two users share the match: never guess");
    assert!(link_row(&w.pool, t2, o2).await.is_none());

    // 1 user in 2 WORKSPACES → ambiguous which workspace → link_required.
    let (t3, o3) = ("33333333-3333-3333-3333-333333333333", "dddddddd-dddd-dddd-dddd-dddddddddddd");
    let org = seed_org(&w.pool, "org_two", bob).await;
    add_member(&w.pool, org, alice).await;
    seed_ms_connection(&w.pool, alice_personal, Some(alice), t3, o3).await;
    seed_ms_connection(&w.pool, org, Some(alice), t3, o3).await;
    let (_, body) = session(&w, &ms_token(t3, o3)).await;
    assert_eq!(body["status"], "link_required");

    // A single match whose user is NOT a member of that org → no auto-link.
    let (t4, o4) = ("44444444-4444-4444-4444-444444444444", "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee");
    let org_gone = seed_org(&w.pool, "org_gone", bob).await;
    seed_ms_connection(&w.pool, org_gone, Some(alice), t4, o4).await; // alice has no membership row
    let (_, body) = session(&w, &ms_token(t4, o4)).await;
    assert_eq!(body["status"], "link_required", "auto-link must re-check membership");

    // An ACCOUNT-WIDE row on an org account belongs to nobody → not a match.
    let (t5, o5) = ("55555555-5555-5555-5555-555555555555", "ffffffff-0000-0000-0000-000000000005");
    seed_ms_connection(&w.pool, org, None, t5, o5).await;
    let (_, body) = session(&w, &ms_token(t5, o5)).await;
    assert_eq!(body["status"], "link_required");
    w.finish().await;
}

#[tokio::test]
async fn disconnect_revokes_every_credential_and_is_never_silently_undone_pg() {
    let base = need_db!("disconnect_revokes_every_credential_and_is_never_silently_undone_pg");
    let w = World::new(&base, "addin_ms_pg_disconnect").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;

    // Teams and Outlook each exchange their own Microsoft token → two
    // credentials on ONE link.
    let (_, a) = session(&w, &ms_token(TID, OID)).await;
    let (_, b) = session(&w, &ms_token(TID, OID)).await;
    let (teams, outlook) = (a["token"].as_str().unwrap(), b["token"].as_str().unwrap());
    assert_ne!(teams, outlook);
    for t in [teams, outlook] {
        let (status, _) = w.call(req("GET", "/v1/addin/microsoft/link", Some(t), None, None)).await;
        assert_eq!(status, StatusCode::OK);
    }

    // Disconnect from "Teams" → 204 → BOTH are dead.
    let (status, _) = w.call(req("DELETE", "/v1/addin/microsoft/link", Some(teams), None, None)).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
    for t in [teams, outlook] {
        let (status, _) = w.call(req("GET", "/v1/addin/microsoft/link", Some(t), None, None)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "a disconnected link's credentials must all 401");
    }
    let live: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_microsoft_sessions WHERE revoked_at IS NULL")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(live, 0);

    // The user already did Connect Microsoft, so an auto-link WOULD match — and
    // must not: Disconnect is not a no-op.
    let (_, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(body["status"], "link_required", "Disconnect must not be silently auto-undone");
    assert!(link_row(&w.pool, TID, OID).await.unwrap().3, "the link is tombstoned");

    // An explicit approval lifts it.
    let code = body["pairing"]["code"].as_str().unwrap();
    let (status, _) = approve(&w, "user_alice", json!({"code": code, "account_id": personal})).await;
    assert_eq!(status, StatusCode::OK);
    let (_, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(body["status"], "connected");
    w.finish().await;
}

#[tokio::test]
async fn membership_loss_kills_the_link_and_the_live_credential_pg() {
    let base = need_db!("membership_loss_kills_the_link_and_the_live_credential_pg");
    let w = World::new(&base, "addin_ms_pg_membership").await;
    let (alice, _personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;

    // Link alice's Microsoft identity to the ORG workspace by pairing.
    let (_, s) = session(&w, &ms_token(TID, OID)).await;
    let code = s["pairing"]["code"].as_str().unwrap();
    assert_eq!(approve(&w, "user_alice", json!({"code": code, "account_id": org})).await.0, StatusCode::OK);
    let (_, c) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(c["status"], "connected");
    assert_eq!(c["workspace"]["kind"], "org");
    let token = c["token"].as_str().unwrap().to_string();
    assert_eq!(
        w.call(req("GET", "/v1/addin/microsoft/link", Some(&token), None, None)).await.0,
        StatusCode::OK
    );

    // She is removed from the org.
    remove_member(&w.pool, org, alice).await;

    // (a) The credential minted BEFORE the removal stops working immediately.
    let (status, _) = w.call(req("GET", "/v1/addin/microsoft/link", Some(&token), None, None)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "lost membership must 401 an already-issued credential");
    // (b) A new exchange is link_required (never `connected` to the org), and
    //     the dead link is removed.
    let (status, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "link_required");
    assert!(link_row(&w.pool, TID, OID).await.is_none(), "the dead link is removed");
    w.finish().await;
}

/// In-app org deletion (control 0387): a Microsoft add-in session never passes
/// through the Clerk door, so the org-deletion lockout must live in the shared
/// membership predicate. A credential minted before the deletion stops working
/// the moment the deletion is requested (members AND the owner), a new
/// exchange is `link_required`, and an undo gives the workspace back.
#[tokio::test]
async fn org_deletion_locks_out_a_live_addin_session_and_an_undo_restores_it_pg() {
    let base = need_db!("org_deletion_locks_out_a_live_addin_session_and_an_undo_restores_it_pg");
    let w = World::new(&base, "addin_ms_pg_org_deletion").await;
    let (alice, _personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_deleting", bob).await;
    add_member(&w.pool, org, alice).await;

    let (_, s) = session(&w, &ms_token(TID, OID)).await;
    let code = s["pairing"]["code"].as_str().unwrap();
    assert_eq!(approve(&w, "user_alice", json!({"code": code, "account_id": org})).await.0, StatusCode::OK);
    let (_, c) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(c["workspace"]["kind"], "org");
    let token = c["token"].as_str().unwrap().to_string();
    let link = || req("GET", "/v1/addin/microsoft/link", Some(&token), None, None);
    assert_eq!(w.call(link()).await.0, StatusCode::OK);

    // The owner asks for the org to be deleted.
    let deletion: Uuid = sqlx::query_scalar(
        "INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, 'org_deleting', 'owner') RETURNING id",
    )
    .bind(org)
    .fetch_one(&w.pool)
    .await
    .unwrap();
    let (locked, _) = w.call(link()).await;
    assert_eq!(locked, StatusCode::UNAUTHORIZED, "an add-in session dies with the deletion request");
    for live in ["grace", "awaiting_approval"] {
        // (operator-level writes: the lockout must hold in every live state)
        sqlx::query("UPDATE org_deletions SET state = $2 WHERE id = $1")
            .bind(deletion)
            .bind(live)
            .execute(&w.pool)
            .await
            .unwrap();
        assert_eq!(w.call(link()).await.0, StatusCode::UNAUTHORIZED, "{live}");
    }
    let members: i64 = sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
        .bind(org)
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(members, 1, "the lockout is the predicate; no membership row was removed");

    // Undo: the very same credential works again.
    sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now() WHERE id = $1")
        .bind(deletion)
        .execute(&w.pool)
        .await
        .unwrap();
    assert_eq!(w.call(link()).await.0, StatusCode::OK, "undo restores the workspace");

    // A fresh deletion: a new exchange is never `connected` to the org.
    sqlx::query("INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, 'org_deleting', 'owner')")
        .bind(org)
        .execute(&w.pool)
        .await
        .unwrap();
    let (_, ex) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(ex["status"], "link_required", "no credential for a deleting org: {ex}");
    w.finish().await;
}

#[tokio::test]
async fn change_workspace_rebinds_for_both_apps_and_revokes_old_credentials_pg() {
    let base = need_db!("change_workspace_rebinds_for_both_apps_and_revokes_old_credentials_pg");
    let w = World::new(&base, "addin_ms_pg_change").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;
    let foreign_org = seed_org(&w.pool, "org_foreign", bob).await;
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;

    let (_, a) = session(&w, &ms_token(TID, OID)).await; // auto-linked → personal ("Teams")
    let (_, b) = session(&w, &ms_token(TID, OID)).await; // ("Outlook")
    let (teams, outlook) = (a["token"].as_str().unwrap().to_string(), b["token"].as_str().unwrap().to_string());

    // The workspaces list is what the ⋮ menu renders.
    let (_, link) = w.call(req("GET", "/v1/addin/microsoft/link", Some(&teams), None, None)).await;
    let kinds: Vec<_> = link["workspaces"].as_array().unwrap().iter().map(|x| x["kind"].as_str().unwrap().to_string()).collect();
    assert_eq!(kinds, ["personal", "org"]);

    // A workspace she is NOT in → 403, and nothing changed: old credential lives.
    let (status, _) = w
        .call(req("PUT", "/v1/addin/microsoft/link/workspace", Some(&teams), Some(json!({"account_id": foreign_org})), None))
        .await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert_eq!(w.call(req("GET", "/v1/addin/microsoft/link", Some(&teams), None, None)).await.0, StatusCode::OK);

    // Change to the org.
    let (status, body) = w
        .call(req("PUT", "/v1/addin/microsoft/link/workspace", Some(&teams), Some(json!({"account_id": org})), None))
        .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "connected");
    assert_eq!(body["workspace"]["account_id"], json!(org));
    assert_eq!(body["workspace"]["kind"], "org");
    assert_eq!(body["account"]["email"], "user_alice@example.test");
    let fresh = body["token"].as_str().unwrap().to_string();
    assert_ne!(fresh, teams);

    // BOTH old credentials (Teams and Outlook) are revoked; the new one works
    // and reports the new workspace.
    for old in [&teams, &outlook] {
        assert_eq!(
            w.call(req("GET", "/v1/addin/microsoft/link", Some(old), None, None)).await.0,
            StatusCode::UNAUTHORIZED,
            "a re-bind must revoke every previous credential for the link"
        );
    }
    let (status, link) = w.call(req("GET", "/v1/addin/microsoft/link", Some(&fresh), None, None)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(link["workspace"]["account_id"], json!(org));
    // The link row itself moved — so the NEXT exchange (either app) lands in the org.
    let row = link_row(&w.pool, TID, OID).await.unwrap();
    assert_eq!(row.1, org);
    let (_, again) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(again["workspace"]["account_id"], json!(org));
    w.finish().await;
}

#[tokio::test]
async fn session_credential_scope_is_an_allowlist_and_link_routes_need_it_pg() {
    let base = need_db!("session_credential_scope_is_an_allowlist_and_link_routes_need_it_pg");
    let w = World::new(&base, "addin_ms_pg_scope").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;
    let (_, c) = session(&w, &ms_token(TID, OID)).await;
    let token = c["token"].as_str().unwrap();

    for (method, uri) in [
        ("GET", "/v1/me"),
        ("GET", "/v1/graphs"),
        ("POST", "/v1/addin/pair/approve"),
        ("GET", "/v1/addin/tokens"),
        ("POST", "/v1/connectors/google/gmail/everything-mode"),
        ("POST", "/v1/connectors/microsoft/teams/transcripts/sync"),
        ("DELETE", "/v1/orgs/members/00000000-0000-0000-0000-000000000001"),
        ("PATCH", "/v1/orgs/members/00000000-0000-0000-0000-000000000001"),
    ] {
        let (status, body) = w.call(req(method, uri, Some(token), Some(json!({})), None)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {body}");
        assert_eq!(body["code"], "addin_session_scope", "{method} {uri}");
    }

    // The link routes refuse a Clerk bearer (they act on the CREDENTIAL's link).
    let clerk = clerk_jwt("user_alice");
    for (method, uri) in [("GET", "/v1/addin/microsoft/link"), ("DELETE", "/v1/addin/microsoft/link")] {
        let (status, body) = w.call(req(method, uri, Some(&clerk), None, None)).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {body}");
    }
    // And a Clerk session is NOT restricted by the add-in scope layer.
    assert_eq!(w.call(req("GET", "/v1/me", Some(&clerk), None, None)).await.0, StatusCode::OK);
    // Expired credential → 401 (clock the row, not the code).
    sqlx::query("UPDATE addin_microsoft_sessions SET expires_at = now() - interval '1 minute'")
        .execute(&w.pool)
        .await
        .unwrap();
    assert_eq!(
        w.call(req("GET", "/v1/addin/microsoft/link", Some(token), None, None)).await.0,
        StatusCode::UNAUTHORIZED
    );
    w.finish().await;
}

/// The tenancy binding, with the binding gate ENFORCING: a personal link is
/// served the personal plane; an ORG link is bound to the org's tenant plane and
/// is REFUSED — never quietly served the consumer plane — when that is not
/// possible.
#[tokio::test]
async fn org_credential_binds_the_tenant_plane_and_never_the_consumer_plane_pg() {
    let base = need_db!("org_credential_binds_the_tenant_plane_and_never_the_consumer_plane_pg");
    let tenant_db = "addin_ms_pg_tenant";
    let _cleanup_tenant_db = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
    let mut conn = admin(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, tenant_db);
    recreate(&mut conn, tenant_db).await;
    let tpool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tpool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta(id, org_id) VALUES (true, 'org_acme')")
        .execute(&tpool)
        .await
        .unwrap();
    tpool.close().await;

    let resolver = StaticMapResolver::from_pairs([("SECRET_ACME", with_db(&base, tenant_db))]);
    let runtime = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let w = World::build(&base, "addin_ms_pg_plane", true, Some(runtime)).await;
    // The meeting routes' own org gate (per-org flag) — on, so an org request
    // gets PAST it and the only remaining discriminator is the plane.
    unsafe { std::env::set_var("ORG_MICROSOFT_TEAMS_ENABLED", "1") };

    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;
    financial_fixture::execute(&w.pool, org, sqlx::query("INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) VALUES ('org_acme', $1, 'acme', 'SECRET_ACME', 'active')")
        .bind(org))
        .await
        .unwrap();

    let probe = |token: &str| {
        req(
            "POST",
            "/v1/connectors/microsoft/teams/transcripts/meeting-status",
            Some(token),
            Some(json!({"capture_id": Uuid::new_v4()})),
            Some(APEX),
        )
    };

    // Personal link → consumer plane. Meeting capture is available to a personal
    // workspace now, so the probe reaches the owner's own (empty) ledger and answers
    // `capture_not_found` — the credential really IS personal, and is not refused.
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;
    let (_, p) = w
        .call(req("POST", "/v1/addin/microsoft/session", None, Some(json!({"ms_token": ms_token(TID, OID)})), Some(APEX)))
        .await;
    let personal_token = p["token"].as_str().unwrap().to_string();
    let (status, body) = w.call(probe(&personal_token)).await;
    assert_eq!((status, body["code"].as_str()), (StatusCode::NOT_FOUND, Some("capture_not_found")), "{body}");
    // ...while email save works in the personal workspace (it is not refused by
    // the scope layer or by plane).
    let (status, body) = w
        .call(req("GET", "/v1/connectors/microsoft/watched", Some(&personal_token), None, Some(APEX)))
        .await;
    assert!(status.is_success(), "personal email-save surface must work: {status} {body}");

    // Org link (a DIFFERENT Microsoft identity linked by pairing to the org).
    let (t2, o2) = ("66666666-6666-6666-6666-666666666666", "99999999-9999-9999-9999-999999999999");
    let (_, s) = session(&w, &ms_token(t2, o2)).await;
    let code = s["pairing"]["code"].as_str().unwrap();
    assert_eq!(approve(&w, "user_alice", json!({"code": code, "account_id": org})).await.0, StatusCode::OK);
    let (_, c) = w
        .call(req("POST", "/v1/addin/microsoft/session", None, Some(json!({"ms_token": ms_token(t2, o2)})), Some(APEX)))
        .await;
    assert_eq!(c["workspace"]["kind"], "org");
    let org_token = c["token"].as_str().unwrap().to_string();

    // The org credential is bound to the TENANT plane: it passes the meeting
    // routes' org check (organization_required would mean it was served the
    // consumer plane).
    let (status, body) = w.call(probe(&org_token)).await;
    assert_ne!(status, StatusCode::UNAUTHORIZED, "{body}");
    assert_ne!(body["code"].as_str(), Some("organization_required"), "an ORG credential was served the CONSUMER plane: {body}");

    // Org not servable → REFUSED by the gate, never downgraded to personal.
    sqlx::query("UPDATE tenant_databases SET status = 'suspended' WHERE clerk_org_id = 'org_acme'")
        .execute(&w.pool)
        .await
        .unwrap();
    let (status, body) = w.call(probe(&org_token)).await;
    assert!(
        !status.is_success() && body["code"].as_str() != Some("organization_required"),
        "a suspended org must be refused by the gate, not served another plane: {status} {body}"
    );
    financial_fixture::execute(
        &w.pool,
        org,
        sqlx::query("DELETE FROM tenant_databases WHERE clerk_org_id = 'org_acme'"),
    )
    .await
    .unwrap();
    let (status, body) = w.call(probe(&org_token)).await;
    assert!(
        !status.is_success() && body["code"].as_str() != Some("organization_required"),
        "an org with no routing row must be refused, not served the consumer plane: {status} {body}"
    );

    w.finish().await;
    drop_db(&base, tenant_db).await;
}

/// The Outlook pane's whole-mailbox switch: a `csms_` credential may GET/POST
/// Microsoft's everything-mode (own mailbox only), personal link.
#[tokio::test]
async fn session_credential_can_get_and_set_everything_mode_pg() {
    let base = need_db!("session_credential_can_get_and_set_everything_mode_pg");
    let w = World::new(&base, "addin_ms_pg_everything").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;
    let (_, c) = session(&w, &ms_token(TID, OID)).await;
    let token = c["token"].as_str().unwrap();
    let uri = "/v1/connectors/microsoft/everything-mode";

    let (status, body) = w.call(req("GET", uri, Some(token), None, None)).await;
    assert_eq!((status, &body["enabled"]), (StatusCode::OK, &json!(false)), "{body}");
    let (status, body) = w.call(req("POST", uri, Some(token), Some(json!({"enabled": true})), None)).await;
    assert_ne!(status, StatusCode::FORBIDDEN, "scope layer must allow it: {body}");
    assert_ne!(body["code"], "addin_session_scope");
    assert!(status.is_success(), "{status} {body}");
    let (_, body) = w.call(req("GET", uri, Some(token), None, None)).await;
    assert_eq!(body["enabled"], true);
    w.finish().await;
}

/// CodeRabbit round 1: a personal account holding BOTH a legacy account-wide
/// Microsoft row and a per-member row for the same identity is ONE candidate.
#[tokio::test]
async fn auto_link_counts_account_wide_and_member_rows_of_one_user_once_pg() {
    let base = need_db!("auto_link_counts_account_wide_and_member_rows_of_one_user_once_pg");
    let w = World::new(&base, "addin_ms_pg_dedupe").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, None, TID, OID).await; // legacy account-wide
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await; // per-member
    let (_, body) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(body["status"], "connected", "{body}");
    assert_eq!(body["auto_linked"], true);
    w.finish().await;
}

/// CodeRabbit round 1: Teams and Outlook opening together for an identity with
/// one prior Connect-Microsoft match — the pane that loses the link-insert race
/// must be served the winning link, never a pairing it does not need.
#[tokio::test]
async fn concurrent_auto_link_never_sends_the_loser_to_a_pairing_pg() {
    let base = need_db!("concurrent_auto_link_never_sends_the_loser_to_a_pairing_pg");
    let w = World::new(&base, "addin_ms_pg_race").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, Some(alice), TID, OID).await;
    let token = ms_token(TID, OID);
    let results = futures_util::future::join_all((0..6).map(|_| session(&w, &token))).await;
    for (status, body) in &results {
        assert_eq!(*status, StatusCode::OK);
        assert_eq!(body["status"], "connected", "a lost insert race must still be connected: {body}");
    }
    let pairings: i64 = sqlx::query_scalar("SELECT count(*) FROM addin_pairing_codes")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(pairings, 0, "no pairing may be created for an identity that has a link");
    w.finish().await;
}

// ── Clerk OAuth sign-in: POST /v1/addin/microsoft/link ───────────────────────

#[tokio::test]
async fn link_personal_mints_a_working_csms_and_is_silent_afterwards_pg() {
    let base = need_db!("link_personal_mints_a_working_csms_and_is_silent_afterwards_pg");
    let w = World::new(&base, "addin_ms_pg_lk_personal").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;

    let (status, body) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["status"], "connected");
    assert_eq!(body["workspace"]["kind"], "personal");
    assert_eq!(body["workspace"]["account_id"], json!(personal));
    assert_eq!(body["account"]["email"], "user_alice@example.test");
    let token = body["token"].as_str().unwrap().to_string();
    assert!(token.starts_with("csms_"));
    let row = link_row(&w.pool, TID, OID).await.unwrap();
    assert_eq!(row, (alice, personal, "clerk_oauth".to_string(), false));

    // The minted credential works, and the next load is silent via /session.
    assert_eq!(w.call(req("GET", "/v1/addin/microsoft/link", Some(&token), None, None)).await.0, StatusCode::OK);
    let (status, again) = session(&w, &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(again["status"], "connected");
    w.finish().await;
}

#[tokio::test]
async fn link_response_is_no_store_pg() {
    let base = need_db!("link_response_is_no_store_pg");
    let w = World::new(&base, "addin_ms_pg_lk_nostore").await;
    seed_user(&w.pool, "user_alice").await;
    let resp = w
        .app
        .clone()
        .oneshot(req(
            "POST",
            "/v1/addin/microsoft/link",
            Some(&addin_jwt("user_alice", None)),
            Some(json!({"ms_token": ms_token(TID, OID)})),
            Some(APEX),
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("cache-control").unwrap(), "no-store");
    w.finish().await;
}

#[tokio::test]
async fn link_org_binds_the_org_workspace_pg() {
    let base = need_db!("link_org_binds_the_org_workspace_pg");
    let w = World::new(&base, "addin_ms_pg_lk_org").await;
    let (alice, _) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;

    let (status, body) = link(&w, &addin_jwt("user_alice", Some("org_acme")), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["workspace"]["kind"], "org");
    assert_eq!(body["workspace"]["account_id"], json!(org));
    assert_eq!(link_row(&w.pool, TID, OID).await.unwrap(), (alice, org, "clerk_oauth".to_string(), false));
    let token = body["token"].as_str().unwrap();
    let (_, l) = w.call(req("GET", "/v1/addin/microsoft/link", Some(token), None, None)).await;
    assert_eq!(l["workspace"]["account_id"], json!(org));
    w.finish().await;
}

#[tokio::test]
async fn link_for_a_personal_org_claim_binds_the_personal_account_pg() {
    let base = need_db!("link_for_a_personal_org_claim_binds_the_personal_account_pg");
    let w = World::new(&base, "addin_ms_pg_lk_porg").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    let porg = seed_org(&w.pool, "org_personal_alice", alice).await;
    add_member(&w.pool, porg, alice).await;
    sqlx::query("INSERT INTO personal_orgs(clerk_org_id, clerk_user_id) VALUES ('org_personal_alice', 'user_alice')")
        .execute(&w.pool)
        .await
        .unwrap();

    let (status, body) = link(&w, &addin_jwt("user_alice", Some("org_personal_alice")), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert_eq!(body["workspace"]["kind"], "personal");
    assert_eq!(body["workspace"]["account_id"], json!(personal));
    assert_ne!(body["workspace"]["account_id"], json!(porg));

    // Another user's personal org is refused outright (403), as on MCP.
    let (_bob, _) = seed_user(&w.pool, "user_bob").await;
    let (status, _) = link(&w, &addin_jwt("user_bob", Some("org_personal_alice")), &ms_token(TID, "bob-oid")).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    w.finish().await;
}

#[tokio::test]
async fn link_with_an_org_claim_for_a_non_member_is_403_and_writes_nothing_pg() {
    let base = need_db!("link_with_an_org_claim_for_a_non_member_is_403_and_writes_nothing_pg");
    let w = World::new(&base, "addin_ms_pg_lk_nonmember").await;
    seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    seed_org(&w.pool, "org_acme", bob).await; // alice is NOT a member
    let (status, _) = link(&w, &addin_jwt("user_alice", Some("org_acme")), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    assert!(link_row(&w.pool, TID, OID).await.is_none());
    // An org the control plane has never heard of: also refused, nothing provisioned.
    let (status, _) = link(&w, &addin_jwt("user_alice", Some("org_unknown")), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::FORBIDDEN);
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE clerk_org_id = 'org_unknown'")
        .fetch_one(&w.pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "an OAuth org claim must never provision an account");
    w.finish().await;
}

#[tokio::test]
async fn relinking_to_a_different_workspace_revokes_old_csms_but_the_same_binding_keeps_them_pg() {
    let base = need_db!("relinking_to_a_different_workspace_revokes_old_csms_but_the_same_binding_keeps_them_pg");
    let w = World::new(&base, "addin_ms_pg_lk_repoint").await;
    let (alice, _) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;

    let (_, a) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    let personal_tok = a["token"].as_str().unwrap().to_string();
    // Same binding again: the earlier credential (the OTHER pane's) survives.
    let (_, a2) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    assert_eq!(
        w.call(req("GET", "/v1/addin/microsoft/link", Some(&personal_tok), None, None)).await.0,
        StatusCode::OK
    );
    let personal_tok2 = a2["token"].as_str().unwrap().to_string();

    // Re-point to the org: every old csms_ dies, the new one reports the org.
    let (status, b) = link(&w, &addin_jwt("user_alice", Some("org_acme")), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    for old in [&personal_tok, &personal_tok2] {
        assert_eq!(
            w.call(req("GET", "/v1/addin/microsoft/link", Some(old), None, None)).await.0,
            StatusCode::UNAUTHORIZED
        );
    }
    let fresh = b["token"].as_str().unwrap();
    let (st, l) = w.call(req("GET", "/v1/addin/microsoft/link", Some(fresh), None, None)).await;
    assert_eq!(st, StatusCode::OK);
    assert_eq!(l["workspace"]["account_id"], json!(org));
    assert_eq!(link_row(&w.pool, TID, OID).await.unwrap().1, org);
    w.finish().await;
}

#[tokio::test]
async fn relinking_as_a_different_user_revokes_the_previous_users_csms_pg() {
    let base = need_db!("relinking_as_a_different_user_revokes_the_previous_users_csms_pg");
    let w = World::new(&base, "addin_ms_pg_lk_user").await;
    let (alice, _) = seed_user(&w.pool, "user_alice").await;
    let (bob, bob_personal) = seed_user(&w.pool, "user_bob").await;
    let (_, a) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    let alice_tok = a["token"].as_str().unwrap().to_string();
    let (status, b) = link(&w, &addin_jwt("user_bob", None), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{b}");
    assert_eq!(
        w.call(req("GET", "/v1/addin/microsoft/link", Some(&alice_tok), None, None)).await.0,
        StatusCode::UNAUTHORIZED
    );
    let row = link_row(&w.pool, TID, OID).await.unwrap();
    assert_eq!((row.0, row.1), (bob, bob_personal));
    assert_ne!(row.0, alice);
    w.finish().await;
}

#[tokio::test]
async fn link_lifts_a_disconnect_tombstone_pg() {
    let base = need_db!("link_lifts_a_disconnect_tombstone_pg");
    let w = World::new(&base, "addin_ms_pg_lk_tomb").await;
    let (_, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, None, TID, OID).await;
    let (_, first) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    let tok = first["token"].as_str().unwrap().to_string();
    assert_eq!(
        w.call(req("DELETE", "/v1/addin/microsoft/link", Some(&tok), None, None)).await.0,
        StatusCode::NO_CONTENT
    );
    assert!(link_row(&w.pool, TID, OID).await.unwrap().3, "tombstone set");
    // /session never silently re-links a tombstone...
    let (_, s) = session(&w, &ms_token(TID, OID)).await;
    assert_ne!(s["status"], "connected");
    // ...but an explicit Clerk sign-in does.
    let (status, body) = link(&w, &addin_jwt("user_alice", None), &ms_token(TID, OID)).await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(!link_row(&w.pool, TID, OID).await.unwrap().3, "tombstone lifted");
    assert_eq!(
        w.call(req("GET", "/v1/addin/microsoft/link", Some(body["token"].as_str().unwrap()), None, None))
            .await
            .0,
        StatusCode::OK
    );
    w.finish().await;
}

#[tokio::test]
async fn link_rejects_other_bearers_and_bad_microsoft_tokens_pg() {
    let base = need_db!("link_rejects_other_bearers_and_bad_microsoft_tokens_pg");
    let w = World::new(&base, "addin_ms_pg_lk_reject").await;
    let (_, personal) = seed_user(&w.pool, "user_alice").await;
    seed_ms_connection(&w.pool, personal, None, TID, OID).await;
    let (_, c) = session(&w, &ms_token(TID, OID)).await;
    let csms = c["token"].as_str().unwrap().to_string();

    // A Clerk SESSION jwt, a csms_ credential and a FOREIGN OAuth client may not link.
    for bearer in [clerk_jwt("user_alice"), csms, oauth_jwt("user_alice", "other_client", None)] {
        let (status, body) = link(&w, &bearer, &ms_token(TID, "someone-else")).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{body}");
    }
    assert!(link_row(&w.pool, TID, "someone-else").await.is_none());

    // Bad / wrong-key Microsoft token -> 401, nothing written.
    for bad in ["", "not-a-jwt"] {
        let (status, body) = link(&w, &addin_jwt("user_alice", None), bad).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED, "{body}");
        assert_eq!(body["error"], "invalid_microsoft_token");
    }
    // No bearer -> 401.
    let (status, _) = w
        .call(req("POST", "/v1/addin/microsoft/link", None, Some(json!({"ms_token": "x"})), Some(APEX)))
        .await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    w.finish().await;
}

#[tokio::test]
async fn addin_oauth_token_is_scoped_like_a_csms_session_pg() {
    let base = need_db!("addin_oauth_token_is_scoped_like_a_csms_session_pg");
    let w = World::new(&base, "addin_ms_pg_oauth_scope").await;
    seed_user(&w.pool, "user_alice").await;
    let tok = addin_jwt("user_alice", None);
    for (method, uri) in [
        ("GET", "/v1/me"),
        ("GET", "/v1/graphs"),
        ("POST", "/v1/addin/pair/approve"),
        ("GET", "/v1/addin/tokens"),
        // The csms_-only link routes also refuse an OAuth token (handler-level).
        ("GET", "/v1/addin/microsoft/link"),
        ("DELETE", "/v1/addin/microsoft/link"),
    ] {
        let (status, body) = w.call(req(method, uri, Some(&tok), Some(json!({})), Some(APEX))).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {body}");
        assert_eq!(body["code"], "addin_session_scope", "{method} {uri}");
    }
    // An allowlisted route is reachable (not scope-refused).
    let (status, body) = w
        .call(req("GET", "/v1/connectors/microsoft/everything-mode", Some(&tok), None, Some(APEX)))
        .await;
    assert_ne!(body["code"], "addin_session_scope", "{status} {body}");
    assert_ne!(status, StatusCode::UNAUTHORIZED, "{body}");
    w.finish().await;
}

#[tokio::test]
async fn foreign_oauth_modes_off_warn_reject_and_session_tokens_unaffected_pg() {
    let base = need_db!("foreign_oauth_modes_off_warn_reject_and_session_tokens_unaffected_pg");
    let w = World::new(&base, "addin_ms_pg_foreign").await;
    seed_user(&w.pool, "user_alice").await;
    let foreign = oauth_jwt("user_alice", "mcp_client_x", None);
    let session_jwt = clerk_jwt("user_alice");
    let get_me = |t: &str| req("GET", "/v1/me", Some(t), None, Some(APEX));

    for (mode, foreign_status) in [
        (None, StatusCode::OK), // default = warn
        (Some("warn"), StatusCode::OK),
        (Some("off"), StatusCode::OK),
        (Some("reject"), StatusCode::UNAUTHORIZED),
    ] {
        // SAFETY: serialized by `ENV_LOCK`, held by `w`.
        unsafe {
            match mode {
                Some(m) => std::env::set_var("REST_FOREIGN_OAUTH", m),
                None => std::env::remove_var("REST_FOREIGN_OAUTH"),
            }
        }
        assert_eq!(w.call(get_me(&foreign)).await.0, foreign_status, "foreign, mode {mode:?}");
        assert_eq!(w.call(get_me(&session_jwt)).await.0, StatusCode::OK, "session, mode {mode:?}");
    }

    // With the add-in client id unset the feature is off: the add-in's own
    // client is just another foreign client (and is not scope-restricted).
    unsafe {
        std::env::remove_var("ADDIN_CLERK_OAUTH_CLIENT_ID");
        std::env::set_var("REST_FOREIGN_OAUTH", "reject");
    }
    assert_eq!(w.call(get_me(&addin_jwt("user_alice", None))).await.0, StatusCode::UNAUTHORIZED);
    unsafe {
        std::env::set_var("REST_FOREIGN_OAUTH", "warn");
    }
    assert_eq!(w.call(get_me(&addin_jwt("user_alice", None))).await.0, StatusCode::OK);
    w.finish().await;
}

#[tokio::test]
async fn gmail_oauth_token_reaches_only_the_gmail_surface_and_cannot_link_pg() {
    let base = need_db!("gmail_oauth_token_reaches_only_the_gmail_surface_and_cannot_link_pg");
    let w = World::new(&base, "addin_ms_pg_gmail").await;
    let (_, personal) = seed_user(&w.pool, "user_alice").await;
    let gmail = oauth_jwt("user_alice", GMAIL_CLIENT, None);
    let outlook = addin_jwt("user_alice", None);

    let scope_refused = |status: StatusCode, body: &Value| {
        status == StatusCode::FORBIDDEN && body["code"] == "addin_session_scope"
    };
    // Gmail client: Gmail routes pass the scope layer...
    for (m, u) in [
        ("GET", "/v1/connectors/google/gmail/everything-mode"),
        ("GET", "/v1/connectors/google/watched/thread1"),
    ] {
        let (st, body) = w.call(req(m, u, Some(&gmail), None, Some(APEX))).await;
        assert!(!scope_refused(st, &body), "{m} {u}: {st} {body}");
        assert_ne!(st, StatusCode::UNAUTHORIZED, "{m} {u}: {body}");
    }
    // ...and Outlook-only / everything else is refused, including the link.
    for (m, u) in [
        ("GET", "/v1/connectors/microsoft/everything-mode"),
        ("POST", "/v1/connectors/microsoft/ingest"),
        ("POST", "/v1/addin/microsoft/link"),
        ("GET", "/v1/me"),
    ] {
        let (st, body) = w.call(req(m, u, Some(&gmail), Some(json!({"ms_token": ms_token(TID, OID)})), Some(APEX))).await;
        assert!(scope_refused(st, &body), "{m} {u}: {st} {body}");
    }
    assert!(link_row(&w.pool, TID, OID).await.is_none(), "a Gmail token must not create a link");

    // Outlook client: Gmail-only routes are refused.
    for (m, u) in [
        ("GET", "/v1/connectors/google/gmail/everything-mode"),
        ("POST", "/v1/connectors/google/ingest"),
        ("GET", "/v1/connectors/google/watched/thread1"),
    ] {
        let (st, body) = w.call(req(m, u, Some(&outlook), Some(json!({})), Some(APEX))).await;
        assert!(scope_refused(st, &body), "{m} {u}: {st} {body}");
    }
    // Same membership handling: a non-member org claim is 403 (not scope), no provisioning.
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    seed_org(&w.pool, "org_acme", bob).await;
    let (st, body) = w
        .call(req("GET", "/v1/connectors/google/gmail/everything-mode", Some(&oauth_jwt("user_alice", GMAIL_CLIENT, Some("org_acme"))), None, Some(APEX)))
        .await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_ne!(body["code"], "addin_session_scope");
    let _ = personal;
    w.finish().await;
}

#[tokio::test]
async fn meet_oauth_token_reaches_only_the_meet_panel_surface_pg() {
    let base = need_db!("meet_oauth_token_reaches_only_the_meet_panel_surface_pg");
    let w = World::new(&base, "addin_ms_pg_meet").await;
    seed_user(&w.pool, "user_alice").await;
    let meet = oauth_jwt("user_alice", MEET_CLIENT, None);
    let gmail = oauth_jwt("user_alice", GMAIL_CLIENT, None);
    let outlook = addin_jwt("user_alice", None);

    let scope_refused = |status: StatusCode, body: &Value| {
        status == StatusCode::FORBIDDEN && body["code"] == "addin_session_scope"
    };
    // Meet client: the workspace label and the capture routes pass the scope layer.
    for (m, u) in [
        ("GET", "/v1/addin/workspace"),
        ("GET", "/v1/connectors/google/meet/captures"),
        ("DELETE", "/v1/connectors/google/meet/captures/7b0e5c1e-0000-4000-8000-000000000001"),
    ] {
        let (st, body) = w.call(req(m, u, Some(&meet), None, Some(APEX))).await;
        assert!(!scope_refused(st, &body), "{m} {u}: {st} {body}");
        assert_ne!(st, StatusCode::UNAUTHORIZED, "{m} {u}: {body}");
    }
    // ...and the Gmail-only, Outlook-only and everything-else routes are refused.
    for (m, u) in [
        ("POST", "/v1/connectors/google/ingest"),
        ("GET", "/v1/connectors/google/gmail/everything-mode"),
        ("GET", "/v1/connectors/google/meet/meetings"),
        ("GET", "/v1/connectors/microsoft/everything-mode"),
        ("POST", "/v1/connectors/microsoft/ingest"),
        ("POST", "/v1/addin/microsoft/link"),
        ("GET", "/v1/me"),
    ] {
        let (st, body) = w.call(req(m, u, Some(&meet), Some(json!({"ms_token": ms_token(TID, OID)})), Some(APEX))).await;
        assert!(scope_refused(st, &body), "{m} {u}: {st} {body}");
    }
    assert!(link_row(&w.pool, TID, OID).await.is_none(), "a Meet token must not create a link");
    // Outlook client: the Meet capture routes are refused. Gmail keeps them (Calendar card).
    let (st, body) = w.call(req("GET", "/v1/connectors/google/meet/captures", Some(&outlook), None, Some(APEX))).await;
    assert!(scope_refused(st, &body), "outlook: {st} {body}");
    let (st, body) = w.call(req("GET", "/v1/connectors/google/meet/captures", Some(&gmail), None, Some(APEX))).await;
    assert!(!scope_refused(st, &body), "gmail: {st} {body}");
    w.finish().await;
}

#[tokio::test]
async fn both_addin_client_ids_unset_means_foreign_for_both_clients_pg() {
    let base = need_db!("both_addin_client_ids_unset_means_foreign_for_both_clients_pg");
    let w = World::new(&base, "addin_ms_pg_gmail_off").await;
    seed_user(&w.pool, "user_alice").await;
    unsafe {
        std::env::remove_var("ADDIN_CLERK_OAUTH_CLIENT_ID");
        std::env::remove_var("GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID");
    }
    let gmail = oauth_jwt("user_alice", GMAIL_CLIENT, None);
    let outlook = addin_jwt("user_alice", None);
    for (mode, want) in [("warn", StatusCode::OK), ("reject", StatusCode::UNAUTHORIZED)] {
        unsafe { std::env::set_var("REST_FOREIGN_OAUTH", mode) };
        for t in [&gmail, &outlook] {
            // Foreign: not scope-restricted under warn, 401 under reject.
            assert_eq!(w.call(req("GET", "/v1/me", Some(t), None, Some(APEX))).await.0, want, "mode {mode}");
        }
    }
    w.finish().await;
}

// ── GET /v1/addin/workspace ──────────────────────────────────────────────────

async fn workspace(w: &World, bearer: &str) -> (StatusCode, Value) {
    w.call(req("GET", "/v1/addin/workspace", Some(bearer), None, Some(APEX))).await
}

#[tokio::test]
async fn workspace_endpoint_names_personal_org_and_personal_org_claims_pg() {
    let base = need_db!("workspace_endpoint_names_personal_org_and_personal_org_claims_pg");
    let w = World::new(&base, "addin_ms_pg_workspace").await;
    let (alice, personal) = seed_user(&w.pool, "user_alice").await;
    let (bob, _) = seed_user(&w.pool, "user_bob").await;
    let org = seed_org(&w.pool, "org_acme", bob).await;
    add_member(&w.pool, org, alice).await;
    let porg = seed_org(&w.pool, "org_personal_alice", alice).await;
    add_member(&w.pool, porg, alice).await;
    sqlx::query("INSERT INTO personal_orgs(clerk_org_id, clerk_user_id) VALUES ('org_personal_alice', 'user_alice')")
        .execute(&w.pool)
        .await
        .unwrap();
    // No Clerk secret in tests, so the name falls to the tenant slug.
    financial_fixture::execute(
        &w.pool,
        org,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_acme', $1, 'acme-slug', 'ref-acme', 'active')",
        )
        .bind(org),
    )
    .await
    .unwrap();

    // Personal: Gmail OAuth, Outlook OAuth, and a Clerk session all agree.
    for bearer in [
        oauth_jwt("user_alice", GMAIL_CLIENT, None),
        addin_jwt("user_alice", None),
        clerk_jwt("user_alice"),
    ] {
        let (st, b) = workspace(&w, &bearer).await;
        assert_eq!(st, StatusCode::OK, "{b}");
        assert_eq!(b["kind"], "personal", "{b}");
        assert_eq!(b["name"], "Personal");
        assert_eq!(b["org_id"], Value::Null);
        // Alice has a personal org, so a claim-less token may act in that
        // org-labelled account; it is still reported as personal.
        assert!(b["account_id"] == json!(personal) || b["account_id"] == json!(porg), "{b}");
    }

    // A user with a plain personal account and no personal org: exact contract.
    let (_dave, dave_personal) = seed_user(&w.pool, "user_dave").await;
    let (st, b) = workspace(&w, &oauth_jwt("user_dave", GMAIL_CLIENT, None)).await;
    assert_eq!(st, StatusCode::OK, "{b}");
    assert_eq!(b, json!({"kind": "personal", "name": "Personal", "account_id": dave_personal, "org_id": null}));

    // Personal-org claim normalizes to personal.
    for bearer in [
        oauth_jwt("user_alice", GMAIL_CLIENT, Some("org_personal_alice")),
        addin_jwt("user_alice", Some("org_personal_alice")),
    ] {
        let (st, b) = workspace(&w, &bearer).await;
        assert_eq!(st, StatusCode::OK, "{b}");
        assert_eq!(b["kind"], "personal");
        assert_eq!(b["name"], "Personal");
        assert!(b["account_id"] == json!(personal) || b["account_id"] == json!(porg), "{b}");
        assert_eq!(b["org_id"], Value::Null);
    }

    // Org claim: org kind, account, clerk org id, a real (non-id) name.
    for bearer in [
        oauth_jwt("user_alice", GMAIL_CLIENT, Some("org_acme")),
        addin_jwt("user_alice", Some("org_acme")),
    ] {
        let resp = w
            .app
            .clone()
            .oneshot(req("GET", "/v1/addin/workspace", Some(&bearer), None, Some(APEX)))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(resp.headers().get("cache-control").unwrap(), "no-store");
        let b: Value = serde_json::from_slice(&to_bytes(resp.into_body(), 1 << 20).await.unwrap()).unwrap();
        assert_eq!(b["kind"], "org", "{b}");
        assert_eq!(b["account_id"], json!(org));
        assert_eq!(b["org_id"], "org_acme");
        assert!(!b["name"].as_str().unwrap().is_empty());
        assert_eq!(b["name"], "acme-slug", "no Clerk in tests: falls back to the tenant slug");
    }

    // A csms_ session (linked via Outlook OAuth) reaches it too.
    let (_, l) = link(&w, &addin_jwt("user_alice", Some("org_acme")), &ms_token(TID, OID)).await;
    let csms = l["token"].as_str().unwrap().to_string();
    let (st, b) = workspace(&w, &csms).await;
    assert_eq!(st, StatusCode::OK, "{b}");
    assert_eq!(b["kind"], "org");
    assert_eq!(b["org_id"], "org_acme");

    // No credential: 401. Non-member org claim: 403 (membership, not scope).
    assert_eq!(w.call(req("GET", "/v1/addin/workspace", None, None, Some(APEX))).await.0, StatusCode::UNAUTHORIZED);
    let (_carol, _) = seed_user(&w.pool, "user_carol").await;
    let (st, b) = workspace(&w, &addin_jwt("user_carol", Some("org_acme"))).await;
    assert_eq!(st, StatusCode::FORBIDDEN);
    assert_ne!(b["code"], "addin_session_scope");
    w.finish().await;
}
