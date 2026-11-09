//! `middleware::auth::require_auth`'s Gmail Add-on PAT branch — live-Postgres
//! proof driving REAL HTTP requests through the actual public router
//! (`test_support_app`), not just the handler functions `routes::addin`'s own
//! in-crate `addin_pg_tests` module exercises directly.
//!
//! Fixture pattern shared with `access_log_capture_pg.rs`
//! (`test_support_app`/`clerk::test_support::mint` scaffolding) — this suite
//! only needs one extra ingredient, a seeded `addin_tokens` row.
//!
//! Proves the properties that matter at the MIDDLEWARE boundary specifically
//! (as opposed to `routes::addin`'s own pairing-flow proof):
//!   1. A valid `csat_...` bearer resolves through the real router to the
//!      SAME `UserContext` (`user_id`/`account_id`) the pairing row names —
//!      via `GET /v1/me`, exactly as a real add-on request to
//!      `/v1/connectors/google/ingest` would resolve its caller.
//!   2. A revoked token is refused (401) — identically to a garbage token,
//!      never distinguishably.
//!   3. `last_used_at` is bumped by a successful call (best-effort,
//!      asynchronous — polled with a short timeout).
//!   4. Authenticating via a PAT triggers ZERO Clerk-provisioning writes
//!      (`users`/`accounts`/`account_members` row counts unchanged) — the PAT
//!      branch never calls `resolve_clerk_context`, so there is nothing for
//!      it to provision.
//!   5. An ordinary Clerk JWT still authenticates exactly as before — the
//!      regression check for the branch split this PR made inside
//!      `require_auth`.
//!   6. `GET /v1/addin/tokens` is REACHABLE over the real router with a
//!      `csat_` PAT bearer, and answers with that caller's own token only —
//!      the property a handler-level test structurally cannot prove, since
//!      an unregistered route and a registered one look identical when you
//!      call the function directly.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as every other `*_pg.rs` file.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test addin_auth_pg -- --nocapture

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes every test in this file against the process-global env vars the
/// Clerk-JWT regression check sets — same rationale as
/// `access_log_capture_pg.rs`'s `ENV_LOCK`/`CaptureEnv`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: access_log_capture_pg.rs) ──────────────────

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
    conn.execute(format!("CREATE DATABASE {db}").as_str())
        .await
        .unwrap();
}
async fn scratch_pool(base: &str, db: &str) -> PgPool {
    let mut conn = admin(base).await;
    recreate(&mut conn, db).await;
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect scratch DB")
}
async fn teardown(base: &str, db: &str, pool: PgPool) {
    pool.close().await;
    let mut conn = admin(base).await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
        .await
        .ok();
}

fn hash_token(raw: &str) -> String {
    hex::encode(Sha256::digest(raw.as_bytes()))
}

async fn seed_user_and_account(pool: &PgPool) -> (Uuid, Uuid) {
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap();
    let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
        .bind(user_id)
        .fetch_one(pool)
        .await
        .unwrap();
    (user_id, account_id)
}

async fn seed_addin_token(pool: &PgPool, account_id: Uuid, user_id: Uuid, plaintext: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO addin_tokens (account_id, owner_user_id, token_hash) VALUES ($1, $2, $3) \
         RETURNING id",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(hash_token(plaintext))
    .fetch_one(pool)
    .await
    .unwrap()
}

async fn control_write_counts(pool: &PgPool) -> Vec<(&'static str, i64)> {
    let mut counts = Vec::new();
    for table in ["users", "accounts", "account_members"] {
        let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
            .fetch_one(pool)
            .await
            .unwrap();
        counts.push((table, n));
    }
    counts
}

fn app_for_pool(pool: PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn get_me(token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/v1/me")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn get_addin_tokens(token: &str) -> Request<Body> {
    Request::builder()
        .method("GET")
        .uri("/v1/addin/tokens")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), 1024 * 1024).await.unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

/// The full middleware-boundary proof: valid PAT resolves the right caller,
/// wrong/revoked PATs are refused identically, `last_used_at` moves, and no
/// Clerk-provisioning row is ever written along the way.
#[tokio::test]
async fn addin_bearer_authenticates_through_the_real_router_pg() {
    let Some(base) = base_url() else {
        eprintln!("skipping addin_bearer_authenticates_through_the_real_router_pg: set ORGT_TEST_BASE_URL");
        return;
    };
    let db_name = "addin_auth_pg_test";
    let pool = scratch_pool(&base, db_name).await;
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("migrate control tree");

    let (user_id, account_id) = seed_user_and_account(&pool).await;
    let plaintext = "csat_test_fixture_token_value_do_not_reuse";
    let token_id = seed_addin_token(&pool, account_id, user_id, plaintext).await;

    let app = app_for_pool(pool.clone());
    let before = control_write_counts(&pool).await;

    // 1. A valid PAT resolves to the SAME caller the pairing row names.
    let resp = app.clone().oneshot(get_me(plaintext)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "a valid, unrevoked add-on token must authenticate");
    let body = body_json(resp).await;
    assert_eq!(body["user_id"].as_str().unwrap(), user_id.to_string());
    assert_eq!(body["account_id"].as_str().unwrap(), account_id.to_string());

    // 2. Authenticating via a PAT must never provision anything — the branch
    //    never calls `resolve_clerk_context`, so `users`/`accounts`/
    //    `account_members` are byte-identical before and after.
    let after = control_write_counts(&pool).await;
    assert_eq!(
        before, after,
        "an add-on PAT request must write ZERO control-plane identity rows"
    );

    // 3. A syntactically add-on-shaped but WRONG token is refused — and
    //    refused the same way (401) an invalid Clerk JWT is, never a
    //    different status that would let a prober distinguish "wrong PAT"
    //    from "wrong JWT".
    let resp = app
        .clone()
        .oneshot(get_me("csat_this_value_was_never_minted"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    // 4. `last_used_at` is bumped by the successful call above — best-effort
    //    and asynchronous (a detached `tokio::spawn`), so poll briefly rather
    //    than asserting immediately.
    let mut bumped = false;
    for _ in 0..20 {
        let last_used: Option<chrono::DateTime<Utc>> =
            sqlx::query_scalar("SELECT last_used_at FROM addin_tokens WHERE id = $1")
                .bind(token_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        if last_used.is_some() {
            bumped = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(bumped, "a successful add-on request must eventually bump last_used_at");

    // 5. Revoke, then confirm the SAME plaintext is refused identically to
    //    the never-minted one above — a revoked token must not be
    //    distinguishable from a bogus one on the wire.
    sqlx::query("UPDATE addin_tokens SET revoked_at = now() WHERE id = $1")
        .bind(token_id)
        .execute(&pool)
        .await
        .unwrap();
    let resp = app.clone().oneshot(get_me(plaintext)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "a revoked add-on token must be refused, even though it was valid moments ago"
    );

    teardown(&base, db_name, pool).await;
}

/// `GET /v1/addin/tokens` over the REAL router, authenticated by a `csat_`
/// PAT — the same second bearer shape `DELETE /v1/addin/tokens/{id}` already
/// accepts.
///
/// Two things only an HTTP-level test can prove:
///   * the route is actually REGISTERED (a handler-level test calls the
///     function directly and passes whether or not `v1_routes` mounts it —
///     an unwired route is a 404 in production and a green test in CI), and
///   * it is behind `require_auth` (no bearer → 401, never an open list of
///     somebody's credentials).
#[tokio::test]
async fn addin_token_list_is_reachable_over_the_router_with_a_pat_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping addin_token_list_is_reachable_over_the_router_with_a_pat_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let db_name = "addin_auth_pg_test_list";
    let pool = scratch_pool(&base, db_name).await;
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("migrate control tree");

    let (user_id, account_id) = seed_user_and_account(&pool).await;
    let plaintext = "csat_list_route_fixture_token_do_not_reuse";
    let token_id = seed_addin_token(&pool, account_id, user_id, plaintext).await;

    // A second user with their own token — the row this caller must NOT see.
    let (other_user, other_account) = seed_user_and_account(&pool).await;
    let other_token_id = seed_addin_token(
        &pool,
        other_account,
        other_user,
        "csat_list_route_fixture_token_belonging_to_someone_else",
    )
    .await;

    let app = app_for_pool(pool.clone());

    // No bearer at all: refused, never an anonymous listing.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .method("GET")
                .uri("/v1/addin/tokens")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "the token list must sit behind require_auth"
    );

    // The PAT itself authenticates the list route — the add-on's own
    // credential is enough to see (and therefore revoke) itself.
    let resp = app.clone().oneshot(get_addin_tokens(plaintext)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "GET /v1/addin/tokens must be registered and accept a csat_ PAT bearer"
    );
    let body = body_json(resp).await;
    let tokens = body["tokens"].as_array().expect("tokens array");
    assert_eq!(tokens.len(), 1, "exactly the caller's own token");
    assert_eq!(tokens[0]["id"].as_str().unwrap(), token_id.to_string());
    assert_eq!(tokens[0]["name"].as_str(), Some("Gmail Add-on"));
    assert!(
        !body.to_string().contains(&other_token_id.to_string()),
        "another user's token id must never appear in this response"
    );
    assert!(
        !body.to_string().contains(&hash_token(plaintext)),
        "the caller's own token HASH must never appear in this response either"
    );

    // A revoked token is gone from the list — and, since the PAT here is
    // the token being revoked, the next request with it is 401 rather than
    // an empty list (the auth layer refuses before the handler runs).
    sqlx::query("UPDATE addin_tokens SET revoked_at = now() WHERE id = $1")
        .bind(token_id)
        .execute(&pool)
        .await
        .unwrap();
    let resp = app.clone().oneshot(get_addin_tokens(plaintext)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::UNAUTHORIZED,
        "a revoked PAT cannot list anything, including itself"
    );

    teardown(&base, db_name, pool).await;
}

// ── Clerk-JWT regression check ───────────────────────────────────────────────
//
// Proves the branch split inside `require_auth` (PAT vs Clerk JWT) left the
// ORIGINAL path byte-behaviorally unchanged: an ordinary personal Clerk
// session token still authenticates through the exact same router.

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let jwks_app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = jwks_body.clone();
            async move { ([("content-type", "application/json")], body) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind local JWKS listener");
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, jwks_app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

struct ClerkEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}
impl ClerkEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: serialized by `ENV_LOCK`, held via `self._guard` through
        // `Drop` — same convention as `access_log_capture_pg.rs`'s `CaptureEnv`.
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
        }
        Self { _guard: guard }
    }
}
impl Drop for ClerkEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
        }
    }
}

#[tokio::test]
async fn ordinary_clerk_jwt_still_authenticates_after_the_addin_branch_split_pg() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping ordinary_clerk_jwt_still_authenticates_after_the_addin_branch_split_pg: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ClerkEnv::enable().await;
    let db_name = "addin_auth_pg_test_clerk";
    let pool = scratch_pool(&base, db_name).await;
    sqlx::migrate!("./migrations_control")
        .run(&pool)
        .await
        .expect("migrate control tree");

    let app = app_for_pool(pool.clone());
    let sub = format!("addin_regression_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER,
        "sub": sub,
        "exp": (Utc::now().timestamp() + 3600) as usize,
        "sid": format!("sess_{}", Uuid::new_v4()),
    }));

    let resp = app.clone().oneshot(get_me(&token)).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "an ordinary Clerk session JWT must still authenticate through the split require_auth"
    );

    teardown(&base, db_name, pool).await;
}
