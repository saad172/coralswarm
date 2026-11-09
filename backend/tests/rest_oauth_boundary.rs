//! REST's configured foreign-OAuth boundary, through the real public router.
//!
//! A deliberately unavailable, lazy database pool distinguishes an auth denial
//! (401, before provisioning) from an admitted token reaching identity lookup
//! (503). This does not claim successful database-backed native add-in login.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use std::{sync::Arc, time::Duration};
use tower::ServiceExt;

struct RestoreEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl RestoreEnv {
    fn set(values: &[(&'static str, &str)]) -> Self {
        let previous = values
            .iter()
            .map(|(key, _)| (*key, std::env::var_os(key)))
            .collect();
        for (key, value) in values {
            std::env::set_var(key, value);
        }
        Self(previous)
    }
}

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        for (key, previous) in &self.0 {
            match previous {
                Some(value) => std::env::set_var(key, value),
                None => std::env::remove_var(key),
            }
        }
    }
}

fn claims(extra: Value) -> Value {
    let mut claims = json!({
        "sub": "user_rest_boundary",
        "iss": ISSUER,
        "exp": chrono::Utc::now().timestamp() + 3600,
    });
    claims
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    claims
}

#[tokio::test]
async fn reject_foreign_oauth_before_identity_lookup_preserving_session_and_native_clients() {
    let jwks = test_jwks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("local JWKS listener");
    let jwks_url = format!("http://{}/jwks", listener.local_addr().unwrap());
    let jwks_task = tokio::spawn(async move {
        axum::serve(
            listener,
            axum::Router::new().route(
                "/jwks",
                axum::routing::get(move || {
                    let jwks = jwks.clone();
                    async move { ([("content-type", "application/json")], jwks) }
                }),
            ),
        )
        .await
        .unwrap();
    });
    // The only test in this integration binary owns its process environment.
    let _env = RestoreEnv::set(&[
        ("CLERK_ISSUER", ISSUER),
        ("CLERK_JWKS_URL", &jwks_url),
        ("REST_FOREIGN_OAUTH", "reject"),
        ("ADDIN_CLERK_OAUTH_CLIENT_ID", "native-outlook"),
        ("GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID", "native-gmail"),
        ("MEET_ADDIN_CLERK_OAUTH_CLIENT_ID", "native-meet"),
    ]);
    let pool = PgPoolOptions::new()
        .acquire_timeout(Duration::from_millis(100))
        .connect_lazy("postgres://fixture:fixture@127.0.0.1:1/unavailable")
        .unwrap();
    let app = coralswarm_backend::test_support_app(Arc::new(
        coralswarm_backend::test_app_state_owned_with_db(pool.clone(), None),
    ));

    let denied = [
        (
            "MCP-bound DCR access token",
            mint_oauth(claims(json!({
                "client_id": "foreign-dcr", "aud": "https://api.coralswarm.example/mcp",
                "scope": "openid profile email offline_access"
            }))),
        ),
        (
            "wrong-resource DCR access token",
            mint_oauth(claims(json!({
                "client_id": "foreign-dcr", "aud": "https://api.coralswarm.example/mcp-reviewer-wrong-resource"
            }))),
        ),
        (
            "legacy unbound DCR access token",
            mint_oauth(claims(json!({"client_id": "foreign-dcr"}))),
        ),
        (
            "ID-shaped JWT without session",
            mint(claims(json!({"aud": "foreign-dcr"}))),
        ),
        (
            "authorized-party ID claim is no native exception",
            mint(claims(json!({
                "aud": "native-outlook", "azp": "native-outlook"
            }))),
        ),
    ];
    let admitted = [
        (
            "interactive session",
            mint(claims(json!({"sid": "sess_rest_boundary"}))),
        ),
        (
            "Outlook native client",
            mint_oauth(claims(json!({"client_id": "native-outlook"}))),
        ),
        (
            "Gmail native client",
            mint_oauth(claims(json!({"client_id": "native-gmail"}))),
        ),
        (
            "Meet native client",
            mint_oauth(claims(json!({"client_id": "native-meet"}))),
        ),
    ];
    for (cases, expected) in [
        (denied.as_slice(), StatusCode::UNAUTHORIZED),
        (admitted.as_slice(), StatusCode::SERVICE_UNAVAILABLE),
    ] {
        for (label, token) in cases {
            for uri in ["/v1/me", "/v1/graphs"] {
                let response = app
                    .clone()
                    .oneshot(
                        Request::builder()
                            .uri(uri)
                            .header("Authorization", format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
                assert_eq!(response.status(), expected, "{label} on {uri}");
                if expected == StatusCode::UNAUTHORIZED {
                    assert!(response.headers().contains_key("www-authenticate"));
                }
            }
        }
    }
    jwks_task.abort();
    pool.close().await;
}
