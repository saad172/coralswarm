//! Settings launch PR 2.8: admin, purchase and credential-minting actions need
//! an INTERACTIVE Clerk session (a JWT carrying `sid`), through the REAL public
//! router, the REAL `require_auth` and a real org tenant.
//!
//! The org admin `alice` calls every route below three ways:
//!
//! 1. With a Clerk OAuth access token (no `sid`) — what a DCR-registered MCP
//!    client holds. `REST_FOREIGN_OAUTH` is left UNSET (code default `warn`,
//!    i.e. "allow"), so the only thing standing between that token and the
//!    route is the authority check this PR adds. Every route answers 403
//!    `interactive_session_required`.
//! 2. With an add-in Clerk OAuth token: it keeps its own existing scoped 403
//!    (`addin_session_scope`) — this PR does not change what it sees.
//! 3. With her real session JWT: never `interactive_session_required` (the
//!    route's own validation answers instead), proving the gate keys on the
//!    credential, not the route.
//!
//! Gated on `ORGT_TEST_BASE_URL`; without it the test prints a skip line.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::Body;
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
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const ORG: &str = "org_interactive_gate";
const ADDIN_CLIENT: &str = "native-outlook-addin";
const ENV_KEYS: [&str; 6] = [
    "CLERK_ISSUER",
    "CLERK_JWKS_URL",
    "MASTER_KEK_HEX",
    "ADDIN_CLERK_OAUTH_CLIENT_ID",
    "REST_FOREIGN_OAUTH",
    "ORG_DELETION_ENABLED",
];

fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
    let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
    format!("{prefix}/{db}{params}")
}

async fn recreate(base: &str, db: &str) {
    let mut conn = PgConnection::connect(base).await.expect("connect admin");
    conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
    conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
}

async fn spawn_local_jwks() -> String {
    let body = test_jwks();
    let app = axum::Router::new().route(
        "/.well-known/jwks.json",
        axum::routing::get(move || {
            let body = body.clone();
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

struct RestoreEnv;
impl Drop for RestoreEnv {
    fn drop(&mut self) {
        // SAFETY: the only test in this binary owns the process environment.
        unsafe {
            for k in ENV_KEYS {
                std::env::remove_var(k);
            }
        }
    }
}

fn exp() -> i64 {
    Utc::now().timestamp() + 3600
}

async fn call(app: &axum::Router, method: &str, path: &str, token: &str, body: Option<Value>) -> (StatusCode, Value) {
    let mut b = HttpRequest::builder()
        .method(method)
        .uri(path)
        .header("host", APEX)
        .header("authorization", format!("Bearer {token}"));
    let body = match body {
        Some(v) => {
            b = b.header("content-type", "application/json");
            Body::from(v.to_string())
        }
        None => Body::empty(),
    };
    let resp = app.clone().oneshot(b.body(body).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    (status, serde_json::from_slice(&bytes).unwrap_or(Value::Null))
}

#[tokio::test]
async fn admin_and_credential_routes_refuse_every_non_interactive_credential_pg() {
    let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
        eprintln!(
            "skipping admin_and_credential_routes_refuse_every_non_interactive_credential_pg: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _restore = RestoreEnv;
    let jwks = spawn_local_jwks().await;
    // SAFETY: the only test in this binary owns the process environment.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", jwks);
        std::env::set_var("MASTER_KEK_HEX", "4444444444444444444444444444444444444444444444444444444444444444");
        std::env::set_var("ADDIN_CLERK_OAUTH_CLIENT_ID", ADDIN_CLIENT);
        // The org-deletion routes answer 404 while their flag is off.
        std::env::set_var("ORG_DELETION_ENABLED", "1");
        // Deliberately NOT `reject`: the code default (`warn`) lets a foreign
        // OAuth token through `require_auth`, which is the case this guards.
        std::env::remove_var("REST_FOREIGN_OAUTH");
    }

    let (ctl, tnt) = ("interactive_gate_ctl", "interactive_gate_tnt");
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&base, ctl).await;
    let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, tnt);
    recreate(&base, tnt).await;
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
    let founder: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'founder@gate.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account: Uuid =
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(founder)
            .bind(ORG)
            .fetch_one(&control)
            .await
            .unwrap();
    financial_fixture::execute(
        &control,
        org_account,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
             VALUES ($1, $2, 'gate', 'SECRET_GATE', 'active')",
        )
        .bind(ORG)
        .bind(org_account),
    )
    .await
    .unwrap();
    let tenant: PgPool = PgPoolOptions::new().connect(&with_db(&base, tnt)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)").bind(ORG).execute(&tenant).await.unwrap();

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_GATE", with_db(&base, tnt))]);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver)))));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let sub = format!("user_gate_alice_{}", Uuid::new_v4().simple());
    let claims = |extra: Value| {
        let mut c = json!({"iss": ISSUER, "sub": sub, "exp": exp(), "org_id": ORG, "org_role": "org:admin"});
        c.as_object_mut().unwrap().extend(extra.as_object().unwrap().clone());
        c
    };
    let session = mint(claims(json!({"sid": format!("sess_{sub}")})));
    let foreign = mint(claims(json!({"client_id": "dcr-mcp-client", "scope": "openid"})));
    let addin = mint(claims(json!({"client_id": ADDIN_CLIENT})));

    // Provision alice as an org admin through her real session.
    let (status, me) = call(&app, "GET", "/v1/me", &session, None).await;
    assert_eq!(status, StatusCode::OK, "session /v1/me: {me}");
    let alice: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(&sub)
        .fetch_one(&control)
        .await
        .unwrap();
    let role: String = sqlx::query_scalar("SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2")
        .bind(org_account)
        .bind(alice)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(role, "admin", "alice must hold admin authority for this test to mean anything");

    let target = Uuid::new_v4();
    let team = Uuid::new_v4();
    // A real ocean alice can reach, so subject deletion gets past its
    // reachability check to the authority decision.
    let graph: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id) \
         VALUES ($1, 'alice ocean', 'member', true, $2) RETURNING id",
    )
    .bind(org_account)
    .bind(alice)
    .fetch_one(&tenant)
    .await
    .unwrap();
    let routes: Vec<(&str, String, Option<Value>)> = vec![
        // Purchases (require_purchase_authority).
        ("POST", "/v1/billing/checkout/subscription".into(), Some(json!({"tier": "standard", "success_url": "https://coralswarm.example/ok", "cancel_url": "https://coralswarm.example/no"}))),
        ("POST", "/v1/billing/checkout/pack".into(), Some(json!({"pack": "small", "success_url": "https://coralswarm.example/ok", "cancel_url": "https://coralswarm.example/no"}))),
        ("POST", format!("/v1/billing/members/{target}/seat-tier"), Some(json!({"tier": "standard"}))),
        // Escrow admin actions.
        ("PUT", "/v1/escrow/recovery-pubkey".into(), Some(json!({"pubkey": "x", "threshold": 2, "admin_count": 3}))),
        ("PUT", "/v1/escrow/recovered-key".into(), Some(json!({"user_id": target, "blob": "x"}))),
        ("POST", "/v1/escrow/recovery-log".into(), Some(json!({"recovered_user_id": target}))),
        // Org-wide prefs.
        ("PUT", "/v1/prefs".into(), Some(json!({}))),
        // Org admin (require_org_admin / is_admin).
        ("POST", "/v1/orgs/invites".into(), Some(json!({"email": "new@gate.example", "role": "member"}))),
        ("DELETE", format!("/v1/orgs/members/{target}"), None),
        ("PATCH", format!("/v1/orgs/members/{target}"), Some(json!({"role": "admin"}))),
        ("POST", "/v1/orgs/slots".into(), Some(json!({"tier": "standard", "quantity": 1, "idempotency_key": "gate-test"}))),
        // Self-serve signup A2: org creation, its setup status and retry
        // (each can end up costing a Neon project).
        ("POST", "/v1/orgs".into(), Some(json!({"name": "Gate Co", "seats": 2, "success_url": "https://coralswarm.example/ok", "cancel_url": "https://coralswarm.example/no"}))),
        ("GET", format!("/v1/orgs/{ORG}/provisioning"), None),
        ("POST", format!("/v1/orgs/{ORG}/provisioning/retry"), None),
        // In-app org deletion (control 0387): request, undo and the owner's
        // export each need a person at a browser.
        ("POST", format!("/v1/orgs/{ORG}/deletion"), Some(json!({"confirm_name": "x"}))),
        ("POST", format!("/v1/orgs/{ORG}/deletion/cancel"), None),
        ("GET", format!("/v1/orgs/{ORG}/deletion/export"), None),
        // Teams.
        ("POST", "/v1/teams".into(), Some(json!({"name": "Gate team"}))),
        ("PATCH", format!("/v1/teams/{team}"), Some(json!({"name": "Renamed"}))),
        ("DELETE", format!("/v1/teams/{team}"), None),
        ("POST", format!("/v1/teams/{team}/restore"), None),
        ("POST", format!("/v1/teams/{team}/members"), Some(json!({"user_id": target, "role": "member"}))),
        ("PATCH", format!("/v1/teams/{team}/members/{target}"), Some(json!({"role": "admin"}))),
        ("DELETE", format!("/v1/teams/{team}/members/{target}"), None),
        // Content deletion authority.
        ("DELETE", format!("/v1/graphs/{graph}/subjects/{target}"), None),
        // Connect / install / disconnect.
        ("POST", "/v1/connectors/microsoft/teams/install".into(), Some(json!({"kind": "channel", "team_id": "t", "channel_id": "c", "tenant_id": "aad"}))),
        ("POST", "/v1/connectors/composio/notion/connect".into(), None),
        ("GET", "/v1/oauth/state".into(), None),
        ("DELETE", "/v1/oauth/connections/slack".into(), None),
        ("GET", "/v1/integrations/slack/oauth/start".into(), None),
        ("DELETE", "/v1/integrations/slack/T-GATE".into(), None),
        // Credential minting.
        ("POST", "/v1/addin/pair/approve".into(), Some(json!({"code": "ABCD-1234"}))),
    ];

    let mut failures = Vec::new();
    for (method, path, body) in &routes {
        let (status, got) = call(&app, method, path, &foreign, body.clone()).await;
        if status != StatusCode::FORBIDDEN || got["code"] != "interactive_session_required" {
            failures.push(format!("foreign OAuth {method} {path}: {status} {got}"));
        }
        let (status, got) = call(&app, method, path, &addin, body.clone()).await;
        if status != StatusCode::FORBIDDEN || got["code"] != "addin_session_scope" {
            failures.push(format!("add-in {method} {path}: {status} {got}"));
        }
        // Never the session gate for a real session (side effects of the
        // session calls are irrelevant: each answers on its own validation).
        let (status, got) = call(&app, method, path, &session, body.clone()).await;
        if got["code"] == "interactive_session_required" {
            failures.push(format!("SESSION wrongly refused {method} {path}: {status} {got}"));
        }
    }
    assert!(failures.is_empty(), "{} mismatches:\n{}", failures.len(), failures.join("\n"));

    control.close().await;
    tenant.close().await;
    let mut conn = PgConnection::connect(&base).await.unwrap();
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
