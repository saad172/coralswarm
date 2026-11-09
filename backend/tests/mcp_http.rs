//! External Axum integration tests for the remote MCP auth spine
//! (docs/plans/remote-mcp-v1.md, Phase 1 step 9 — "Layer B").
//!
//! Unlike the mock-validator tests in `backend/src/mcp/auth.rs` (in-crate,
//! free to inject a fake `TokenValidator`), this file drives the REAL public
//! router (`coralswarm_backend::test_support_app`, gated behind the
//! `test-support` Cargo feature — see `Cargo.toml`) end-to-end, including a
//! genuine RS256 signature check against a local JWKS listener. No live
//! Postgres or Clerk instance is required: identity resolution's DB
//! dependency is exercised only as far as its absence (503), matching the
//! existing `v1_graphs_bearer_no_db_returns_503`-style assertions elsewhere in
//! this crate.
//!
//! All tests that touch process env vars serialize on `ENV_LOCK` — `cargo
//! test` runs tests within one binary concurrently by default, and
//! `McpConfig::from_env()` reads shared process state.

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use coralswarm_backend::clerk::test_support::{
    mint, mint_oauth, mint_with_header, test_jwks, ISSUER, TEST_KID,
};
use tokio::sync::{Mutex, MutexGuard};
use tower::ServiceExt;

// A `tokio::sync::Mutex` (not `std::sync::Mutex`): several tests below hold
// the guard across an `.await` (spawning the local JWKS listener, driving a
// request through the router), which is a real footgun with a std mutex under
// a multi-threaded executor and is correctly clippy-denied
// (`await_holding_lock`) — an async-aware mutex has no such restriction.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Spin up a tiny local JWKS listener serving the shared test keypair, and
/// return its base URL. Kept alive for the test's duration by leaking the
/// spawned task (fine — each test binds an ephemeral port and the process
/// exits at the end of the run).
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

/// Point MCP at a local JWKS listener with a fresh, unique-per-test resource
/// URL (so byte-equality assertions are self-contained) and clear it up.
struct McpEnv {
    _guard: MutexGuard<'static, ()>,
}

impl McpEnv {
    async fn enable(resource_url: &str) -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", resource_url);
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
        }
        Self { _guard: guard }
    }

    async fn disabled() -> Self {
        let guard = ENV_LOCK.lock().await;
        unsafe {
            std::env::remove_var("MCP_ENABLED");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
        }
    }
}

fn router() -> axum::Router {
    coralswarm_backend::test_support_app(coralswarm_backend::test_app_state())
}

// ── flag-off -> 404 ───────────────────────────────────────────────────────────

#[tokio::test]
async fn mcp_and_well_known_routes_404_when_flag_off() {
    let _env = McpEnv::disabled().await;
    let app = router();

    for uri in [
        "/mcp",
        "/.well-known/oauth-protected-resource",
        "/.well-known/oauth-protected-resource/mcp",
        "/.well-known/oauth-authorization-server",
    ] {
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::NOT_FOUND,
            "expected 404 for {uri} with MCP_ENABLED unset"
        );
    }
}

// ── 401 challenge shape ───────────────────────────────────────────────────────

#[tokio::test]
async fn missing_token_401_challenge_shape() {
    let _env = McpEnv::enable("https://mcp-http-test-1.example/mcp").await;
    let app = router();

    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

    let challenge = resp
        .headers()
        .get(axum::http::header::WWW_AUTHENTICATE)
        .and_then(|v| v.to_str().ok())
        .unwrap()
        .to_string();
    assert!(challenge.starts_with("Bearer "));
    assert!(challenge.contains(
        "resource_metadata=\"https://mcp-http-test-1.example/.well-known/oauth-protected-resource/mcp\""
    ));
    // Advertised scopes must be Clerk-grantable; coralswarm:* would break DCR.
    // There is only one resource now (claim-based-tenancy §Phase 3 PR 3.3
    // retired the org MCP resource), and its scopes deliberately EXCLUDE
    // user:org:read — that is what keeps Clerk from minting an org_id claim
    // for ANY MCP connection (see MCP_SCOPES_PERSONAL).
    assert!(challenge.contains("scope=\"openid profile email offline_access user:org:read\""));
    assert!(challenge.contains("user:org:read"));
    assert!(!challenge.contains("coralswarm:"));
}

// ── method pins (GET/DELETE, unauthenticated) ─────────────────────────────────
//
// The authenticated GET/DELETE -> 405 pins (rmcp's own stateless-mode method
// dispatch, verified against the 3.1.2 source in `tower.rs::handle`) live in
// `mcp_dual_era_pg.rs` instead: reaching that code requires a REAL identity
// to clear `mcp_auth`, which needs Postgres. These two need no DB at all.
// `mcp_auth` answers a GET with 405 before the token check, so a client that
// opens the optional SSE stream without a bearer is not told its sign-in
// failed. DELETE still 401s on a missing bearer like POST, before the DB pool
// is touched or rmcp's method dispatch sees the request.

#[tokio::test]
async fn get_unauthenticated_returns_405_not_401() {
    let _env = McpEnv::enable("https://mcp-http-test-10.example/mcp").await;
    let app = router();

    let req = Request::builder()
        .method("GET")
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(
        resp.headers().get(axum::http::header::ALLOW).unwrap(),
        "POST"
    );
    assert!(
        resp.headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .is_none(),
        "a tokenless GET must not look like a failed sign-in"
    );
}

#[tokio::test]
async fn delete_unauthenticated_returns_401_before_reaching_rmcp() {
    let _env = McpEnv::enable("https://mcp-http-test-11.example/mcp").await;
    let app = router();

    let req = Request::builder()
        .method("DELETE")
        .uri("/mcp")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    assert!(
        resp.headers()
            .get(axum::http::header::WWW_AUTHENTICATE)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| {
                value
                    .split_whitespace()
                    .next()
                    .is_some_and(|scheme| scheme.eq_ignore_ascii_case("Bearer"))
            }),
        "mcp_auth's 401 challenge must fire regardless of HTTP method, with a Bearer scheme"
    );
}

// ── token-in-query-string rejection ───────────────────────────────────────────

#[tokio::test]
async fn query_string_token_is_not_authenticated() {
    let _env = McpEnv::enable("https://mcp-http-test-2.example/mcp").await;
    let app = router();

    let token = mint(serde_json::json!({
        "iss": ISSUER, "sub": "user_q", "exp": future_exp(),
        "client_id": "client_q",
    }));
    let req = Request::builder()
        .method("POST")
        .uri(format!("/mcp?access_token={token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── PRM byte-equality + fields ────────────────────────────────────────────────

#[tokio::test]
async fn prm_document_byte_identical_at_both_paths_and_has_expected_fields() {
    let _env = McpEnv::enable("https://mcp-http-test-3.example/mcp").await;
    let app = router();

    let req_a = Request::builder()
        .uri("/.well-known/oauth-protected-resource")
        .body(Body::empty())
        .unwrap();
    let resp_a = app.clone().oneshot(req_a).await.unwrap();
    assert_eq!(resp_a.status(), StatusCode::OK);
    assert_eq!(
        resp_a
            .headers()
            .get(axum::http::header::ACCESS_CONTROL_ALLOW_ORIGIN)
            .unwrap(),
        "*"
    );
    let bytes_a = to_bytes(resp_a.into_body(), 16 * 1024).await.unwrap();

    let req_b = Request::builder()
        .uri("/.well-known/oauth-protected-resource/mcp")
        .body(Body::empty())
        .unwrap();
    let resp_b = app.oneshot(req_b).await.unwrap();
    assert_eq!(resp_b.status(), StatusCode::OK);
    let bytes_b = to_bytes(resp_b.into_body(), 16 * 1024).await.unwrap();

    assert_eq!(bytes_a, bytes_b);

    let body: serde_json::Value = serde_json::from_slice(&bytes_a).unwrap();
    assert_eq!(body["resource"], "https://mcp-http-test-3.example/mcp");
    assert_eq!(body["authorization_servers"][0], ISSUER);
    // Clerk-grantable scopes only, full array — advertising coralswarm:* breaks
    // DCR and dropping offline_access silently kills refresh tokens.
    // `user:org:read` must be ABSENT: it was dropped entirely in claim-based-
    // tenancy §Phase 3 PR 3.3 (see `MCP_SCOPES_PERSONAL`'s doc) — advertising
    // it ever again would force an org_id claim onto a token, which is
    // precisely what made org members' personal oceans unreachable before the
    // (now-retired) org-subdomain resource split existed. Every host,
    // including a former org subdomain, now gets this SAME document —
    // asserted in `mcp::well_known`'s
    // `prm_no_longer_differs_between_apex_and_org_subdomain`.
    assert_eq!(
        body["scopes_supported"],
        serde_json::json!(["openid", "profile", "email", "offline_access", "user:org:read"])
    );
}

// ── real-signature path: expired token ────────────────────────────────────────

#[tokio::test]
async fn real_signed_expired_token_returns_401() {
    let _env = McpEnv::enable("https://mcp-http-test-4.example/mcp").await;
    let app = router();

    let past = (chrono::Utc::now().timestamp() - 7200) as usize;
    let token = mint_oauth(serde_json::json!({
        "aud": "https://mcp-http-test-4.example/mcp",
        "iss": ISSUER, "sub": "user_expired", "exp": past,
        "client_id": "client_expired",
    }));
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── real-signature path: malformed (tampered signature) ───────────────────────

#[tokio::test]
async fn real_signed_tampered_token_returns_401() {
    let _env = McpEnv::enable("https://mcp-http-test-5.example/mcp").await;
    let app = router();

    let token = mint_oauth(serde_json::json!({
        "aud": "https://mcp-http-test-5.example/mcp",
        "iss": ISSUER, "sub": "user_tamper", "exp": future_exp(),
        "client_id": "client_tamper",
    }));
    let mut parts: Vec<&str> = token.split('.').collect();
    let sig: String = parts[2].chars().rev().collect();
    parts[2] = &sig;
    let tampered = parts.join(".");

    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Authorization", format!("Bearer {tampered}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── real-signature path: session JWT (no OAuth-only claims) rejected ─────────

#[tokio::test]
async fn real_signed_session_shaped_token_is_rejected() {
    let _env = McpEnv::enable("https://mcp-http-test-6.example/mcp").await;
    let app = router();

    // Valid signature, valid issuer/expiry, but shaped exactly like a Clerk
    // *session* token (only `sid`, no scope/client_id/azp/jti) — must still
    // be rejected at `/mcp`.
    let token = mint(serde_json::json!({
        "iss": ISSUER, "sub": "user_sess", "exp": future_exp(),
        "sid": "sess_abc",
    }));
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── security review H1 regression: sid + azp together must still reject ─────

/// Clerk's REAL default browser session JWT carries both `sid` and `azp`
/// (`azp` is the standard OIDC authorized-party claim, not OAuth-specific —
/// Clerk sessions carry it too). Before the H1 fix, `has_oauth_claims`
/// accepted a token on `azp.is_some()` alone, so this exact shape bypassed
/// session-JWT rejection with no OAuth consent flow at all. This must 401.
#[tokio::test]
async fn real_signed_session_token_with_sid_and_azp_is_rejected() {
    let _env = McpEnv::enable("https://mcp-http-test-9.example/mcp").await;
    let app = router();

    let token = mint(serde_json::json!({
        "iss": ISSUER, "sub": "user_sess_azp", "exp": future_exp(),
        "sid": "sess_abc", "azp": "https://app.example.com",
    }));
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ── real-signature path: valid OAuth-shaped token clears every gate ──────────

#[tokio::test]
async fn real_signed_oauth_token_clears_verification_and_hits_db_503() {
    let _env = McpEnv::enable("https://mcp-http-test-7.example/mcp").await;
    let app = router();

    // Valid signature, valid issuer/expiry, OAuth-shaped (`client_id` + `jti`).
    // `test_app_state()` has no DB pool, so identity resolution correctly
    // 503s — proving the request got past bearer extraction, JWT-shape,
    // signature/issuer/expiry verification, AND the OAuth-only-claims gate,
    // same depth as `v1_graphs_bearer_no_db_returns_503` elsewhere.
    let token = mint_oauth(serde_json::json!({
        "aud": "https://mcp-http-test-7.example/mcp",
        "iss": ISSUER, "sub": "user_valid", "exp": future_exp(),
        "client_id": "client_valid", "jti": "jti_valid",
    }));
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
}

#[tokio::test]
async fn real_signed_unbound_or_wrong_resource_tokens_get_reauthorization_challenge() {
    let resource = "https://mcp-http-resource-binding.example/mcp";
    let _env = McpEnv::enable(resource).await;
    let app = router();
    let base = serde_json::json!({
        "iss": ISSUER, "sub": "user_resource", "exp": future_exp(),
        "client_id": resource, "azp": resource, "jti": "resource_jti",
    });
    let mut tokens = vec![mint_oauth(base.clone())];
    for aud in [
        serde_json::json!("https://other.example/mcp"),
        serde_json::json!(format!("{resource}/")),
    ] {
        let mut claims = base.clone();
        claims["aud"] = aud;
        tokens.push(mint_oauth(claims));
    }
    for typ in [None, Some("JWT"), Some("id+jwt")] {
        let mut claims = base.clone();
        claims["aud"] = serde_json::json!(resource);
        tokens.push(mint_with_header(claims, typ, TEST_KID));
    }
    for token in tokens {
        let req = Request::builder()
            .method("POST")
            .uri("/mcp")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let response = app.clone().oneshot(req).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        assert!(response.headers().get(axum::http::header::WWW_AUTHENTICATE)
            .unwrap().to_str().unwrap().contains(
                "resource_metadata=\"https://mcp-http-resource-binding.example/.well-known/oauth-protected-resource/mcp\""));
    }
}

// ── origin_guard ──────────────────────────────────────────────────────────────

#[tokio::test]
async fn disallowed_origin_is_forbidden_before_auth() {
    let guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    unsafe {
        std::env::set_var("MCP_ENABLED", "1");
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks_url);
        std::env::set_var(
            "MCP_RESOURCE_URL",
            "https://mcp-http-test-8.example/mcp",
        );
        std::env::set_var("RATE_LIMIT_BACKEND", "pg");
        std::env::set_var("MCP_ALLOWED_ORIGINS", "https://claude.ai");
    }
    let app = router();

    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        .header("Origin", "https://evil.example")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);

    unsafe {
        std::env::remove_var("MCP_ENABLED");
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
        std::env::remove_var("MCP_RESOURCE_URL");
        std::env::remove_var("RATE_LIMIT_BACKEND");
        std::env::remove_var("MCP_ALLOWED_ORIGINS");
    }
    drop(guard);
}
