//! `middleware::access_log::spawn_capture` — live-Postgres proof driving REAL
//! HTTP requests through `require_auth` (platform-admin-metrics PR 4 of 7).
//!
//! Fixture pattern shared with `legacy_token_fence_pg.rs` (same
//! `test_support_app`/`clerk::test_support::mint` scaffolding, minus the
//! webhook half — this suite only needs a Clerk session/OAuth-shaped token
//! and a `user_access_log` row count).
//!
//! Proves the five properties the PR brief calls out explicitly:
//!   1. The first request of a session inserts exactly one row.
//!   2. A second request with the SAME `sid` inserts none (idempotent via
//!      `ON CONFLICT (user_id, clerk_session_id) DO NOTHING`).
//!   3. A disallowed `X-CoralSwarm-Client` value stores `NULL`, never the
//!      raw client-supplied string (a positive control with an allow-listed
//!      value is included alongside it).
//!   4. A token with no `sid` claim (OAuth-access-token-shaped, mirroring
//!      `mcp::auth::has_oauth_claims`'s own distinguishing test) skips
//!      capture entirely — no row at all.
//!   5. A DB failure during capture (simulated here by dropping
//!      `user_access_log` after migrating) never fails the request itself —
//!      `GET /v1/me` still returns 200.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same convention as every other `*_pg.rs` file.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test access_log_capture_pg -- --nocapture

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tower::ServiceExt;

/// Serializes every test in this file against the process-global env vars
/// `CaptureEnv` sets — same rationale as `legacy_token_fence_pg.rs`'s
/// `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: legacy_token_fence_pg.rs / access_log_purge_pg.rs) ─

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
async fn migrate_control_all(pool: &PgPool) {
    sqlx::migrate!("./migrations_control")
        .run(pool)
        .await
        .expect("run control migrations");
}

// ── Env + router fixture (pattern: legacy_token_fence_pg.rs) ─────────────────

const TEST_MASTER_KEK_HEX: &str =
    "4444444444444444444444444444444444444444444444444444444444444444";

async fn spawn_local_jwks() -> String {
    let jwks_body = test_jwks();
    let app = axum::Router::new().route(
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
        axum::serve(listener, app).await.ok();
    });
    format!("http://{addr}/.well-known/jwks.json")
}

struct CaptureEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl CaptureEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation across this test binary is serialized by
        // `ENV_LOCK`, whose guard is held in `self._guard` for the lifetime
        // of this `CaptureEnv` (through `Drop` below) — same convention as
        // `legacy_token_fence_pg.rs`'s `FenceEnv`.
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            std::env::remove_var("MULTITENANT_ENABLED");
        }
        Self { _guard: guard }
    }
}

impl Drop for CaptureEnv {
    fn drop(&mut self) {
        // SAFETY: see `enable`'s SAFETY comment — `self._guard` still holds
        // `ENV_LOCK` here.
        unsafe {
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MASTER_KEK_HEX");
        }
    }
}

fn app_for_pool(pool: PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

/// A Clerk SESSION-shaped JWT (carries `sid`), no org context — the
/// personal-account interactive path `require_auth` serves.
fn session_personal_token(sub: &str, sid: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": sid,
    }))
}

/// An OAuth-access-token-shaped JWT: no `sid` at all, carrying `client_id`/
/// `scope` instead — mirrors `mcp::auth::has_oauth_claims`'s own
/// distinguishing test for "this is not a session token". `require_auth`
/// itself has no OAuth-vs-session branch (that fence is `/mcp`-only), so
/// this token is accepted as an ordinary personal-plane caller; the ONLY
/// thing under test here is that `spawn_capture` sees no `sid` and skips.
fn oauth_shaped_token(sub: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "client_test_oauth", "scope": "coralswarm:read",
    }))
}

async fn api_get(app: &axum::Router, token: &str, client_header: Option<&str>) -> StatusCode {
    let mut builder = Request::builder()
        .method("GET")
        .uri("/v1/me")
        .header("Authorization", format!("Bearer {token}"));
    if let Some(client) = client_header {
        builder = builder.header("X-CoralSwarm-Client", client);
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    resp.status()
}

/// How long a positive wait will keep polling before declaring the row lost.
///
/// Generous on purpose: the cost of a high ceiling is nothing in the happy path
/// (polling exits as soon as the row lands), while the cost of a low one is a
/// flaky failure on a loaded runner.
const CAPTURE_POLL_TIMEOUT: Duration = Duration::from_secs(10);

/// Gap between polls. Short enough that the common case finishes far quicker
/// than the flat 200 ms sleep this replaced.
const CAPTURE_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// Settle window for asserting capture did **not** happen.
const CAPTURE_SETTLE_WINDOW: Duration = Duration::from_millis(300);

/// Wait until `user_access_log` holds at least `expected` rows for `sub`, then
/// return them.
///
/// Capture is spawned fire-and-forget, so the insert races the response. This
/// used to be a flat `sleep(200ms)` followed by an immediate assertion, which is
/// a race rather than synchronization: on a loaded runner the second spawned
/// insert had not landed yet and
/// `disallowed_client_header_stores_null_but_an_allowed_value_is_stored` failed
/// with `[("sess-alc-disallowed", None)]` — one row short, even though the code
/// under test had behaved correctly.
///
/// Polling fixes it in both directions. It cannot give up early, and in the
/// happy path it returns after one 25 ms interval instead of always paying
/// 200 ms.
///
/// Waits for `>= expected` rather than `== expected` deliberately: the caller
/// keeps its own `assert_eq!` on the length, so a run that inserts *too many*
/// rows still fails there instead of being masked by a helper that waits for an
/// exact count and then hangs.
async fn wait_for_access_log_rows(
    pool: &PgPool,
    sub: &str,
    expected: usize,
) -> Vec<(String, Option<String>)> {
    let deadline = std::time::Instant::now() + CAPTURE_POLL_TIMEOUT;

    loop {
        // The read itself is bounded by whatever is left of the deadline. The
        // scratch pool caps out at 4 connections and sets no `acquire_timeout`,
        // so a read can block in pool acquisition rather than in the query —
        // without this, the helper would sail past its own ceiling and hang the
        // test instead of failing it.
        let remaining = deadline.saturating_duration_since(std::time::Instant::now());
        let rows = match tokio::time::timeout(remaining, access_log_rows_for(pool, sub)).await {
            Ok(rows) => rows,
            Err(_) => panic!(
                "timed out after {CAPTURE_POLL_TIMEOUT:?} waiting for {expected} \
                 access-log row(s) for {sub}: the read itself stalled (pool \
                 acquisition or query) rather than returning too few rows"
            ),
        };

        if rows.len() >= expected {
            return rows;
        }
        // Checked after the read, not before, so a deadline that expires while a
        // query is in flight still reports what that query actually saw.
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out after {CAPTURE_POLL_TIMEOUT:?} waiting for {expected} \
                 access-log row(s) for {sub}; saw {}: {rows:?}",
                rows.len()
            );
        }
        tokio::time::sleep(CAPTURE_POLL_INTERVAL).await;
    }
}

/// Bounded wait for the one case with nothing observable to wait *for*.
///
/// Only the DB-failure fixture uses this: it drops `user_access_log` before the
/// request, so no row can ever appear and there is nothing to poll. The
/// assertion that matters there — the request returned 200 — has already been
/// awaited; this just gives the doomed spawned task a moment to run and swallow
/// its own error before the test ends.
///
/// **Not for asserting absence.** A fixed window cannot prove a row will never
/// arrive: under executor pressure the capture task can be scheduled after it,
/// and the test passes falsely. Absence assertions use a positive control
/// instead — see [`capture_pipeline_is_draining`].
async fn settle_spawned_capture() {
    tokio::time::sleep(CAPTURE_SETTLE_WINDOW).await;
}

/// Prove the capture pipeline is actually running in *this* environment, by
/// issuing a request that must capture and waiting until its row lands.
///
/// This is how the absence assertions avoid being vacuous. Sleeping and then
/// finding no row cannot distinguish "capture correctly skipped" from "capture
/// simply hadn't run yet" — the second is a false pass, and a starved executor
/// or slow Postgres produces it. Observing a control row means the pipeline
/// demonstrably drained while the test was watching, so "no row for the subject"
/// is a finding rather than a bet on a timer.
///
/// Returns the control session id so the caller can exclude it.
///
/// Honest limit: `tokio::spawn` does not guarantee that a task spawned earlier
/// completes before one spawned later, so this narrows the false-pass window
/// rather than closing it. Closing it entirely needs capture completion to be
/// observable from the middleware itself (a production change deliberately not
/// made in a test-only fix).
async fn capture_pipeline_is_draining(
    app: &axum::Router,
    pool: &PgPool,
    control_sub: &str,
    expected_control_rows: usize,
) -> String {
    let sid = format!("sess-alc-control-{control_sub}");
    let token = session_personal_token(control_sub, &sid);
    let status = api_get(app, &token, None).await;
    assert_eq!(
        status, StatusCode::OK,
        "the capture control request must succeed, or it proves nothing"
    );
    wait_for_access_log_rows(pool, control_sub, expected_control_rows).await;
    sid
}

async fn access_log_rows_for(pool: &PgPool, sub: &str) -> Vec<(String, Option<String>)> {
    sqlx::query_as::<_, (String, Option<String>)>(
        "SELECT clerk_session_id, client FROM user_access_log ual \
         JOIN users u ON u.id = ual.user_id \
         WHERE u.clerk_user_id = $1 \
         ORDER BY clerk_session_id",
    )
    .bind(sub)
    .fetch_all(pool)
    .await
    .expect("read user_access_log")
}

// ── (1)+(2) First-sight insert, repeat-session no-op ─────────────────────────

#[tokio::test]
async fn first_request_inserts_one_row_and_a_second_request_same_session_inserts_none() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping first_request_inserts_one_row_and_a_second_request_same_session_inserts_none: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = CaptureEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_alc_firstsight").await;
    migrate_control_all(&pool).await;

    let app = app_for_pool(pool.clone());
    let sub = "alc_first_sight_user";
    let token = session_personal_token(sub, "sess-alc-first");

    let status = api_get(&app, &token, None).await;
    assert_eq!(status, StatusCode::OK, "first request must succeed");

    let rows = wait_for_access_log_rows(&pool, sub, 1).await;
    assert_eq!(
        rows.len(),
        1,
        "the first request of a brand-new session must insert exactly one row: {rows:?}"
    );
    assert_eq!(rows[0].0, "sess-alc-first");

    // Same session again — must be a no-op conflict, not a second row.
    let status = api_get(&app, &token, None).await;
    assert_eq!(status, StatusCode::OK, "second request must also succeed");

    // Prove the pipeline drained while we watched, rather than sleeping and
    // hoping: a control request on a DIFFERENT session must produce its row.
    // Without this, "still one row" is indistinguishable from "the second
    // request's capture task simply hadn't run yet".
    let control_sid = capture_pipeline_is_draining(&app, &pool, sub, 2).await;

    let rows = access_log_rows_for(&pool, sub).await;
    let own_rows: Vec<_> = rows.iter().filter(|(sid, _)| *sid != control_sid).collect();
    assert_eq!(
        own_rows.len(),
        1,
        "a second request on the SAME session must insert nothing further \
         (control row excluded): {rows:?}"
    );
    assert_eq!(own_rows[0].0, "sess-alc-first");
}

// ── (3) Client allow-list: disallowed value stores NULL, allowed value stores itself ─

#[tokio::test]
async fn disallowed_client_header_stores_null_but_an_allowed_value_is_stored() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping disallowed_client_header_stores_null_but_an_allowed_value_is_stored: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = CaptureEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_alc_client").await;
    migrate_control_all(&pool).await;

    let app = app_for_pool(pool.clone());
    let sub = "alc_client_user";

    // Disallowed / hostile value → NULL, never the raw string.
    let disallowed_token = session_personal_token(sub, "sess-alc-disallowed");
    let status = api_get(&app, &disallowed_token, Some("cli")).await;
    assert_eq!(status, StatusCode::OK);
    wait_for_access_log_rows(&pool, sub, 1).await;

    // Allow-listed value → stored verbatim (positive control, same user, a
    // different session so both rows exist side by side).
    let allowed_token = session_personal_token(sub, "sess-alc-allowed");
    let status = api_get(&app, &allowed_token, Some("web")).await;
    assert_eq!(status, StatusCode::OK);

    let rows = wait_for_access_log_rows(&pool, sub, 2).await;
    assert_eq!(rows.len(), 2, "both sessions must have captured rows: {rows:?}");
    let disallowed_row = rows
        .iter()
        .find(|(sid, _)| sid == "sess-alc-disallowed")
        .expect("disallowed-session row present");
    assert_eq!(
        disallowed_row.1, None,
        "'cli' (disallowed — not in the allow-list) must store NULL, never the raw string: {rows:?}"
    );
    let allowed_row = rows
        .iter()
        .find(|(sid, _)| sid == "sess-alc-allowed")
        .expect("allowed-session row present");
    assert_eq!(
        allowed_row.1.as_deref(),
        Some("web"),
        "an allow-listed client value must be stored verbatim: {rows:?}"
    );
}

// ── (4) Missing `sid` skips capture entirely ─────────────────────────────────

#[tokio::test]
async fn missing_sid_skips_capture_entirely() {
    let Some(base) = base_url() else {
        eprintln!("skipping missing_sid_skips_capture_entirely: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = CaptureEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_alc_nosid").await;
    migrate_control_all(&pool).await;

    let app = app_for_pool(pool.clone());
    let sub = "alc_no_sid_user";
    let token = oauth_shaped_token(sub);

    let status = api_get(&app, &token, Some("web")).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "require_auth itself has no sid-presence fence (that's /mcp-only); the request must succeed"
    );

    // Prove capture is live in this environment before concluding it skipped.
    // A bare sleep here would pass just as happily if the pipeline were broken
    // or merely slow — the control's row landing is what makes the emptiness
    // below meaningful. The control is a different `sub`, so it cannot
    // contribute a row to the assertion.
    let control_sub = "alc_no_sid_control_user";
    capture_pipeline_is_draining(&app, &pool, control_sub, 1).await;

    let rows = access_log_rows_for(&pool, sub).await;
    assert!(
        rows.is_empty(),
        "a token with no sid claim must skip capture entirely — no row, not a fabricated one: {rows:?}"
    );
}

// ── (5) A DB failure during capture never fails the request ─────────────────

#[tokio::test]
async fn a_db_failure_during_capture_does_not_fail_the_request() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping a_db_failure_during_capture_does_not_fail_the_request: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = CaptureEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_alc_dbfail").await;
    migrate_control_all(&pool).await;

    // Simulate a capture-time DB failure by removing the very table the
    // spawned insert targets. Every other control table `require_auth`'s
    // OWN critical path depends on (users, accounts, account_members) is
    // untouched, so this isolates "the capture insert itself errors" from
    // "the database is generally unavailable" (which would 503 before
    // capture is ever reached, proving nothing about capture's own
    // fire-and-forget contract).
    sqlx::query("DROP TABLE user_access_log")
        .execute(&pool)
        .await
        .expect("drop user_access_log for the fault-injection fixture");

    let app = app_for_pool(pool.clone());
    let sub = "alc_db_failure_user";
    let token = session_personal_token(sub, "sess-alc-dbfail");

    let status = api_get(&app, &token, None).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a capture-insert DB failure (missing table) must never surface as a failed request"
    );
    // Give the doomed spawned task a moment to run and swallow its own error.
    // Nothing to poll for — the table is gone, so there is no row that could
    // ever appear; this test's whole point is that awaiting the request above
    // already returned 200.
    settle_spawned_capture().await;
}
