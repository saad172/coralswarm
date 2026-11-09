//! `POST /v1/me/delete-account` through the REAL public router (JWT
//! verification, the auth door, the binding of `InteractiveClerkSession`) on a
//! real Postgres. Settings launch PR 2.4.
//!
//! The durable job itself is covered in-crate
//! (`services::account_deletion::pg_tests`); this file proves the HTTP edge:
//!
//! 1. only an interactive Clerk session may delete an account: a foreign OAuth
//!    token, the add-in's OAuth token, an add-in PAT (`csat_`) and a Microsoft
//!    add-in session (`csms_`) are all refused;
//! 2. a typed-email mismatch is a 400 and changes nothing;
//! 3. membership of a team organization is a 409 that lists the organizations;
//! 4. a stale first-factor verification (`fva`) is 403 `reverification_required`;
//! 5. a personal account other users are members of is a 409
//!    `shared_personal_account`;
//! 6. once the 202 is returned the user is locked out of everything except the
//!    status and cancel endpoints (403 `account_deletion_pending`), and
//!    cancelling (signed in, or from the signed
//!    email link with no session) restores access;
//! 7. a stale JWT for a deleted Clerk id can never provision a fresh user.
//!
//! Gated on `RELINFER_TEST_DATABASE_URL` (control schema through 0370 applied).
//! Without it every test prints a skip line: a skip is NOT a pass.
//!
//! Run:
//!   RELINFER_TEST_DATABASE_URL=postgres://postgres:postgres@127.0.0.1:5432/ctrl \
//!     cargo test -p coralswarm-backend --test delete_account_pg -- --test-threads=1 --nocapture

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

const LINK_SECRET: &str = "integration-link-secret";

/// The cancel-link token, minted the way the service does (HMAC-SHA256 over a
/// domain-separated message), so the open route is exercised with a real link.
fn cancel_token(id: &str) -> String {
    use hmac::{Hmac, Mac};
    // The per-purpose key, derived by the same KDF the service uses.
    let key = coralswarm_backend::crypto::kdf::derive_key(
        LINK_SECRET.as_bytes(),
        coralswarm_backend::crypto::kdf::purpose::ACCOUNT_DELETION_CANCEL,
    );
    let mut mac = Hmac::<Sha256>::new_from_slice(&key).unwrap();
    mac.update(format!("account-deletion-cancel:v1:{id}").as_bytes());
    format!("{id}.{}", hex::encode(mac.finalize().into_bytes()))
}

struct RestoreEnv(Vec<(&'static str, Option<std::ffi::OsString>)>);

impl RestoreEnv {
    fn set(values: &[(&'static str, &str)]) -> Self {
        let previous = values.iter().map(|(k, _)| (*k, std::env::var_os(k))).collect();
        for (k, v) in values {
            std::env::set_var(k, v);
        }
        Self(previous)
    }
}

impl Drop for RestoreEnv {
    fn drop(&mut self) {
        for (k, prev) in &self.0 {
            match prev {
                Some(v) => std::env::set_var(k, v),
                None => std::env::remove_var(k),
            }
        }
    }
}

fn sha256_hex(s: &str) -> String {
    hex::encode(Sha256::digest(s.as_bytes()))
}

fn claims(sub: &str, extra: Value) -> Value {
    let mut c = json!({"sub": sub, "iss": ISSUER, "exp": chrono::Utc::now().timestamp() + 3600});
    c.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
    c
}

async fn call(app: &axum::Router, method: Method, uri: &str, token: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut req = Request::builder().method(method).uri(uri).header("Authorization", format!("Bearer {token}"));
    let body = match body {
        Some(b) => {
            req = req.header("content-type", "application/json");
            Body::from(b.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(req.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

async fn call_open(app: &axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let req = Request::builder()
        .method(Method::POST)
        .uri(uri)
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

struct Harness {
    app: axum::Router,
    pool: PgPool,
    _env: RestoreEnv,
    jwks_task: tokio::task::JoinHandle<()>,
}

async fn harness() -> Option<Harness> {
    let Ok(url) = std::env::var("RELINFER_TEST_DATABASE_URL") else {
        eprintln!("skipping delete_account_pg: set RELINFER_TEST_DATABASE_URL to run it");
        return None;
    };
    let jwks = test_jwks();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
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
    let env = RestoreEnv::set(&[
        ("CLERK_ISSUER", ISSUER),
        ("CLERK_JWKS_URL", &jwks_url),
        // Foreign OAuth tokens reach the handler (as in prod today), so the
        // route's own session requirement is what has to refuse them.
        ("REST_FOREIGN_OAUTH", "warn"),
        ("ADDIN_CLERK_OAUTH_CLIENT_ID", "native-outlook"),
        ("ACCOUNT_DELETION_LINK_SECRET", LINK_SECRET),
    ]);
    std::env::remove_var("CLERK_SECRET_KEY");
    // Assertions and fixtures use the owner pool; the ROUTER runs on a
    // `coralswarm_app` pool (production's role: not the owner, subject to RLS).
    let pool = PgPoolOptions::new().max_connections(8).connect(&url).await.unwrap();
    sqlx::query("ALTER ROLE coralswarm_app WITH LOGIN PASSWORD 'app_test_pw'").execute(&pool).await.unwrap();
    let app_opts = {
        use std::str::FromStr;
        sqlx::postgres::PgConnectOptions::from_str(&url).unwrap().username("coralswarm_app").password("app_test_pw")
    };
    let app_pool = PgPoolOptions::new().max_connections(8).connect_with(app_opts).await.unwrap();
    let app = coralswarm_backend::test_support_app(Arc::new(
        coralswarm_backend::test_app_state_owned_with_db_and_store(app_pool, None),
    ));
    Some(Harness { app, pool, _env: env, jwks_task })
}

impl Harness {
    /// Provision a user through a real first request and return `(user_id,
    /// account_id, email)`.
    async fn provision(&self, sub: &str) -> (Uuid, Uuid, String) {
        let token = mint(claims(sub, json!({"sid": "sess_a", "fva": [2, -1]})));
        let (status, me) = call(&self.app, Method::GET, "/v1/me", &token, None).await;
        assert_eq!(status, StatusCode::OK, "{me}");
        let user: Uuid = serde_json::from_value(me["user_id"].clone()).unwrap();
        let account: Uuid = serde_json::from_value(me["account_id"].clone()).unwrap();
        let email: String = sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
            .bind(user)
            .fetch_one(&self.pool)
            .await
            .unwrap();
        (user, account, email)
    }

    async fn cleanup(&self, user: Uuid) {
        let _ = sqlx::query("DELETE FROM account_deletions WHERE user_id = $1").bind(user).execute(&self.pool).await;
    }
}

#[tokio::test]
async fn only_an_interactive_session_may_delete_an_account() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, account, email) = h.provision(&sub).await;
    let body = json!({"confirm_email": email});

    // Foreign OAuth (no sid) and the add-in's own OAuth client.
    let foreign = mint_oauth(claims(&sub, json!({"client_id": "some-dcr-client"})));
    let addin = mint_oauth(claims(&sub, json!({"client_id": "native-outlook"})));
    for (label, token) in [("foreign oauth", &foreign), ("add-in oauth", &addin)] {
        let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", token, Some(body.clone())).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {v}");
        // The add-in's own OAuth client is stopped earlier by its scope
        // allowlist (default deny); every other token reaches the handler.
        let code = v["code"].as_str().unwrap_or_default();
        assert!(
            code == "session_required" || (label == "add-in oauth" && code == "addin_session_scope"),
            "{label}: unexpected refusal {v}"
        );
        let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", token, None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label} GET: {v}");
    }

    // Add-in PAT, Microsoft add-in session: real rows, real hashes.
    let addin_pat = format!("csat_{}", Uuid::new_v4().simple());
    sqlx::query("INSERT INTO addin_tokens (account_id, owner_user_id, token_hash) VALUES ($1, $2, $3)")
        .bind(account)
        .bind(user)
        .bind(sha256_hex(&addin_pat))
        .execute(&h.pool)
        .await
        .unwrap();
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &addin_pat, Some(body.clone())).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "add-in PAT: {v}");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1")
        .bind(user)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(n, 0, "no refused credential started a deletion");
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn a_mismatched_confirmation_is_a_400_and_changes_nothing() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, account, email) = h.provision(&sub).await;
    let token = mint(claims(&sub, json!({"sid": "sess_a", "fva": [2, -1]})));

    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &token, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["eligible"], true);
    assert_eq!(v["email"], email.as_str());
    assert_eq!(v["blocking_organizations"], json!([]));
    assert_eq!(v["deletion"], Value::Null);

    for typed in ["someone-else@example.com", "", "  "] {
        let (status, v) =
            call(&h.app, Method::POST, "/v1/me/delete-account", &token, Some(json!({"confirm_email": typed}))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{typed:?}: {v}");
        assert_eq!(v["code"], "confirmation_mismatch");
    }
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1")
        .bind(user)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);
    // The session still works.
    let (status, _) = call(&h.app, Method::GET, "/v1/me", &token, None).await;
    assert_eq!(status, StatusCode::OK);
    let _ = account;
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn a_team_org_member_gets_a_409_listing_the_organizations() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, _account, email) = h.provision(&sub).await;
    let org_owner: Uuid = sqlx::query_scalar("INSERT INTO users (email) VALUES ($1) RETURNING id")
        .bind(format!("{}@org-owner.test", Uuid::new_v4().simple()))
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let org_id = format!("org_{}", Uuid::new_v4().simple());
    let org_account: Uuid = sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
        .bind(org_owner)
        .bind(&org_id)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    let tier: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
        .fetch_one(&h.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'owner', $3), ($1, $4, 'admin', $3)")
        .bind(org_account)
        .bind(org_owner)
        .bind(tier)
        .bind(user)
        .execute(&h.pool)
        .await
        .unwrap();

    let token = mint(claims(&sub, json!({"sid": "sess_a", "fva": [2, -1]})));
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &token, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["eligible"], false);
    assert_eq!(v["blocking_organizations"][0]["clerk_org_id"], org_id.as_str());

    let (status, v) =
        call(&h.app, Method::POST, "/v1/me/delete-account", &token, Some(json!({"confirm_email": email}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["code"], "team_org_membership");
    let orgs = v["organizations"].as_array().unwrap();
    assert_eq!(orgs.len(), 1);
    assert_eq!(orgs[0]["clerk_org_id"], org_id.as_str());
    assert_eq!(orgs[0]["role"], "admin");
    assert_eq!(orgs[0]["account_id"], org_account.to_string());
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1")
        .bind(user)
        .fetch_one(&h.pool)
        .await
        .unwrap();
    assert_eq!(n, 0);

    // Leaving the organization unblocks it.
    sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(org_account)
        .bind(user)
        .execute(&h.pool)
        .await
        .unwrap();
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &token, None).await;
    assert_eq!((status, &v["eligible"]), (StatusCode::OK, &json!(true)));

    let _ = sqlx::query("DELETE FROM account_members WHERE account_id = $1").bind(org_account).execute(&h.pool).await;
    let _ = sqlx::query("DELETE FROM accounts WHERE id = $1").bind(org_account).execute(&h.pool).await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(org_owner).execute(&h.pool).await;
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn re_verification_follows_clerks_second_factor_rule() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, _account, email) = h.provision(&sub).await;
    let body = json!({"confirm_email": email});
    for (label, extra) in [
        ("MFA user, stale second factor", json!({"sid": "s", "fva": [1, 600]})),
        ("both stale", json!({"sid": "s", "fva": [600, 600]})),
        ("neither enrolled", json!({"sid": "s", "fva": [-1, -1]})),
        ("exactly the 10 minute edge", json!({"sid": "s", "fva": [10, -1]})),
        ("single factor, stale", json!({"sid": "s", "fva": [600, -1]})),
        ("a v1 token with no fva claim", json!({"sid": "s"})),
    ] {
        let token = mint(claims(&sub, extra));
        let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &token, Some(body.clone())).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{label}: {v}");
        assert_eq!(v["code"], "reverification_required", "{label}");
        let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &token, None).await;
        assert_eq!((status, &v["reverification_required"]), (StatusCode::OK, &json!(true)), "{label}");
    }
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1").bind(user).fetch_one(&h.pool).await.unwrap();
    assert_eq!(n, 0, "a stale session started nothing");
    // An MFA user whose FIRST factor is old but who just re-verified the second
    // is accepted (this used to loop on reverification_required).
    let mfa = mint(claims(&sub, json!({"sid": "s", "fva": [600, 1]})));
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &mfa, None).await;
    assert_eq!((status, &v["reverification_required"]), (StatusCode::OK, &json!(false)));
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &mfa, Some(body.clone())).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    // Cancel it and prove the single-factor case also passes.
    let (status, _) = call(&h.app, Method::POST, "/v1/me/delete-account/cancel", &mfa, None).await;
    assert_eq!(status, StatusCode::OK);
    let single = mint(claims(&sub, json!({"sid": "s", "fva": [1, -1]})));
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &single, Some(body)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn a_shared_personal_account_is_a_409_listing_the_other_members() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, account, email) = h.provision(&sub).await;
    let other: Uuid = sqlx::query_scalar("INSERT INTO users (email, name) VALUES ($1, 'Grace Hopper') RETURNING id")
        .bind(format!("{}@co-owner.test", Uuid::new_v4().simple()))
        .fetch_one(&h.pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'admin')").bind(account).bind(other).execute(&h.pool).await.unwrap();
    let token = mint(claims(&sub, json!({"sid": "s", "fva": [1, -1]})));
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &token, None).await;
    assert_eq!((status, &v["eligible"]), (StatusCode::OK, &json!(false)));
    assert_eq!(v["shared_account_members"][0]["user_id"], other.to_string());
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &token, Some(json!({"confirm_email": email}))).await;
    assert_eq!(status, StatusCode::CONFLICT, "{v}");
    assert_eq!(v["code"], "shared_personal_account");
    assert_eq!(v["members"][0]["user_id"], other.to_string());
    assert_eq!(v["members"][0]["name"], "Grace Hopper");
    assert_eq!(v["members"][0]["role"], "admin");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1").bind(user).fetch_one(&h.pool).await.unwrap();
    assert_eq!(n, 0);
    let _ = sqlx::query("DELETE FROM account_members WHERE user_id = $1").bind(other).execute(&h.pool).await;
    let _ = sqlx::query("DELETE FROM users WHERE id = $1").bind(other).execute(&h.pool).await;
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn grace_locks_the_user_out_except_status_and_cancel_and_cancelling_restores_access() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, _account, email) = h.provision(&sub).await;
    let session = mint(claims(&sub, json!({"sid": "sess_a", "fva": [2, -1]})));
    let other_session = mint(claims(&sub, json!({"sid": "sess_b", "fva": [2, -1]})));
    let oauth = mint_oauth(claims(&sub, json!({"client_id": "some-dcr-client"})));

    let confirm = json!({"confirm_email": email.to_uppercase()});
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &session, Some(confirm.clone())).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    let id = v["id"].as_str().unwrap().to_string();
    assert_eq!(v["status"], "grace");
    assert_eq!(v["deletion"]["can_cancel"], true);
    assert!(v["deletion"]["purge_after"].is_string());

    // Everything but status/cancel is refused, for every kind of Clerk credential.
    for (label, token) in [("session", &session), ("another session", &other_session), ("oauth", &oauth)] {
        for uri in ["/v1/me", "/v1/graphs", "/v1/billing/summary"] {
            let (status, v) = call(&h.app, Method::GET, uri, token, None).await;
            assert_eq!(status, StatusCode::FORBIDDEN, "{label} {uri}: {v}");
            assert_eq!(v["code"], "account_deletion_pending", "{label} {uri}");
        }
    }

    // ...but the status endpoint answers with the live status .
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &session, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    assert_eq!(v["deletion"]["id"], id.as_str());
    assert_eq!(v["deletion"]["state"], "grace");
    assert_eq!(v["eligible"], false);
    // A second POST is refused at the door like every other route (the service's
    // own start is idempotent, covered in-crate), and creates nothing.
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &session, Some(confirm.clone())).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::FORBIDDEN, Some("account_deletion_pending")), "{v}");
    let n: i64 = sqlx::query_scalar("SELECT count(*) FROM account_deletions WHERE user_id = $1").bind(user).fetch_one(&h.pool).await.unwrap();
    assert_eq!(n, 1);

    // The door's exception is EXACT: right path, right method, nothing else.
    for (method, uri) in [(Method::GET, "/v1/me/delete-account/cancel"), (Method::POST, "/v1/me/delete-account")] {
        let (status, v) = call(&h.app, method.clone(), uri, &session, None).await;
        assert_eq!(status, StatusCode::FORBIDDEN, "{method} {uri}: {v}");
        assert_eq!(v["code"], "account_deletion_pending", "{method} {uri}");
    }
    // Cancel needs a session, not a foreign credential.
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account/cancel", &oauth, None).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::FORBIDDEN, Some("session_required")));
    // The signed link works with NO credentials, only for the deletion it names.
    let bogus = format!("{id}.{}", "ab".repeat(32));
    let (status, v) = call_open(&h.app, "/v1/account-deletion/cancel", json!({"token": bogus})).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::BAD_REQUEST, Some("invalid_token")));
    let (status, v) = call_open(&h.app, "/v1/account-deletion/cancel", json!({"token": cancel_token(&id)})).await;
    assert_eq!((status, v["status"].as_str()), (StatusCode::OK, Some("cancelled")), "{v}");
    let (status, v) = call_open(&h.app, "/v1/account-deletion/cancel", json!({"token": cancel_token(&id)})).await;
    assert_eq!((status, v["status"].as_str()), (StatusCode::OK, Some("cancelled")), "a replayed link is idempotent: {v}");

    // Access is back.
    let (status, v) = call(&h.app, Method::GET, "/v1/me", &session, None).await;
    assert_eq!(status, StatusCode::OK, "{v}");
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &session, None).await;
    assert_eq!((status, &v["eligible"], &v["deletion"]), (StatusCode::OK, &json!(true), &Value::Null));
    // Cancelling again, signed in: nothing pending.
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account/cancel", &session, None).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::NOT_FOUND, Some("no_pending_deletion")));

    // Start again and cancel while signed in.
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &session, Some(confirm)).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account/cancel", &session, None).await;
    assert_eq!((status, v["status"].as_str()), (StatusCode::OK, Some("cancelled")), "{v}");
    h.cleanup(user).await;
    h.jwks_task.abort();
}

#[tokio::test]
async fn after_grace_only_status_is_reachable_and_cancel_is_too_late_then_the_credential_dies() {
    let Some(h) = harness().await else { return };
    let sub = format!("user_{}", Uuid::new_v4().simple());
    let (user, _account, email) = h.provision(&sub).await;
    let session = mint(claims(&sub, json!({"sid": "sess_a", "fva": [2, -1]})));
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account", &session, Some(json!({"confirm_email": email}))).await;
    assert_eq!(status, StatusCode::ACCEPTED, "{v}");
    let id = v["id"].as_str().unwrap().to_string();

    // The destructive phase is running: status still answers, cancel is too late.
    sqlx::query("UPDATE account_deletions SET state = 'running', running_since = now() WHERE user_id = $1").bind(user).execute(&h.pool).await.unwrap();
    let (status, v) = call(&h.app, Method::GET, "/v1/me/delete-account", &session, None).await;
    assert_eq!((status, v["deletion"]["state"].as_str()), (StatusCode::OK, Some("running")), "{v}");
    let (status, v) = call(&h.app, Method::GET, "/v1/me", &session, None).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::FORBIDDEN, Some("account_deletion_in_progress")));
    let (status, v) = call(&h.app, Method::POST, "/v1/me/delete-account/cancel", &session, None).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::CONFLICT, Some("too_late")));
    let (status, v) = call_open(&h.app, "/v1/account-deletion/cancel", json!({"token": cancel_token(&id)})).await;
    assert_eq!((status, v["code"].as_str()), (StatusCode::CONFLICT, Some("too_late")));

    // Completed: 401 everywhere, status included, so the client signs out.
    sqlx::query("UPDATE account_deletions SET state = 'completed', completed_at = now(), completed_steps = ARRAY['tombstone'] WHERE user_id = $1")
        .bind(user).execute(&h.pool).await.unwrap();
    for uri in ["/v1/me", "/v1/me/delete-account"] {
        let (status, v) = call(&h.app, Method::GET, uri, &session, None).await;
        assert_eq!((status, v["code"].as_str()), (StatusCode::UNAUTHORIZED, Some("account_deleted")), "{uri}");
    }

    // The anonymized user no longer carries the Clerk id; the stale JWT still
    // must not create a new user.
    sqlx::query("UPDATE users SET clerk_user_id = NULL, email = 'deleted-' || id::text || '@deleted.invalid' WHERE id = $1").bind(user).execute(&h.pool).await.unwrap();
    let before: i64 = sqlx::query_scalar("SELECT count(*) FROM users").fetch_one(&h.pool).await.unwrap();
    let (status, _) = call(&h.app, Method::GET, "/v1/me", &session, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM users").fetch_one(&h.pool).await.unwrap();
    assert_eq!(before, after, "a deleted Clerk id never provisions a user");
    h.cleanup(user).await;
    h.jwks_task.abort();
}
