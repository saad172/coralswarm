//! Integration tests for the org-creation gate (org-tenancy PR 3.3):
//! `ORG_SELFSERVE_DISABLED`, wired into `clerk_provision::ensure_org_account`
//! and exercised through BOTH triggers that call it — the Clerk
//! `organization.created` webhook (`routes::clerk_webhook`) and the
//! request-path first-sight JWT provisioning (`middleware::auth::
//! resolve_clerk_context`'s `ProvisionFromClaims` policy, reached via
//! `require_auth` on any authenticated `/v1` route).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights — same scratch-DB fixture pattern as `binding_gate_pg.rs` /
//! `route_migration_org_pg.rs` / `mcp_governance_pg.rs`); a plain `cargo test`
//! with no DB configured skips every test in this file.
//!
//! These tests deliberately build a `TenantRuntime` with the binding gate
//! itself DISABLED (`enabled: false`) but `org_selfserve_disabled: true` — the
//! account-creation gate is independent of per-request Host routing (see
//! `TenantRuntime::org_selfserve_disabled`'s doc comment), so isolating it
//! from the gate lets these tests observe ONLY the account-creation decision:
//! with the binding gate off, an absent `TenancyDecision` is the normal
//! flag-off state (`tenancy::extractor`'s module doc) and `/v1` routes serve
//! normally off `state.db`/`state.store`, so a successful request is a clean
//! "provisioning succeeded, and the rest of the API works" signal.

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes the shared-process `CLERK_ISSUER`/`CLERK_JWKS_URL`/
/// `CLERK_WEBHOOK_SECRET` env mutations across this file's tests — same
/// rationale (and pattern) as `route_migration_org_pg.rs`/`mcp_governance_pg.rs`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

const APEX: &str = "coralswarm.example";
const WEBHOOK_SECRET_B64: &str = "b3JnLXRlbmFuY3ktcHItMy0zLXRlc3Qtc2VjcmV0"; // arbitrary fixed key

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
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

fn future_exp() -> usize {
    (chrono::Utc::now().timestamp() + 3600) as usize
}

/// Spin up a local JWKS listener serving the shared test keypair — same
/// fixture `mcp_pg_e2e.rs`/`route_migration_org_pg.rs` use, duplicated here
/// for the same separate-compilation-unit reason.
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

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
        .await
        .unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

/// Build a Svix signature the same way Clerk does, for driving
/// `POST /v1/webhooks/clerk` directly — mirrors `routes::clerk_webhook`'s own
/// (private, so not reusable here) `#[cfg(test)]` `sign()` helper.
fn sign_svix(secret_b64: &str, id: &str, ts: &str, body: &str) -> String {
    let key = STANDARD.decode(secret_b64).unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
}

/// A fresh control-only scratch DB (control tree, no tenant DB needed — every
/// test here runs with the binding gate itself disabled, see module doc) plus
/// an `AppState` wired for the request-path tests. `apex`'s `TenantRuntime`
/// has the gate `enabled: false` but `org_selfserve_disabled` as given.
async fn make_state(base: &str, db_name: &str, org_selfserve_disabled: bool) -> (PgPool, Arc<AppState>) {
    let control: PgPool = PgPoolOptions::new().connect(&with_db(base, db_name)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(TenantRuntime::new_with_org_selfserve_disabled(
        false, // binding gate itself OFF — isolates the account-creation gate
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::new(Default::default())))),
        org_selfserve_disabled,
    ));
    (control, Arc::new(state))
}

async fn accounts_count_for_org(pool: &PgPool, clerk_org_id: &str) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM accounts WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap()
}

#[tokio::test]
async fn self_serve_disabled_rejects_brand_new_org_no_rows_created() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping self_serve_disabled_rejects_brand_new_org_no_rows_created: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation is serialized process-wide by `ENV_LOCK` — this
    // test holds `_env_guard` for its entire body, so no concurrent
    // reader/writer in this binary can observe these vars mid-mutation.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let db_name = "orgt_gate_new_ctl";
    let mut conn = admin(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db_name);
    recreate(&mut conn, db_name).await;
    let (control, state) = make_state(&base, db_name, /* org_selfserve_disabled */ true).await;
    let app = coralswarm_backend::test_support_app(state);

    let org_id = format!("org_new_{}", Uuid::new_v4());
    let sub = format!("selfserve_gate_new_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": org_id, "org_role": "org:member",
    }));

    let req = HttpRequest::builder()
        .uri("/v1/graphs")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "a brand-new org under ORG_SELFSERVE_DISABLED must be a clean rejection, not 200/500"
    );
    let body = body_json(resp).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("not provisioned"),
        "expected the self-serve-disabled rejection message, got {body:?}"
    );

    assert_eq!(
        accounts_count_for_org(&control, &org_id).await,
        0,
        "no shadow accounts row must be created for an un-onboarded org"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)").as_str())
        .await
        .ok();
    // SAFETY: still under `_env_guard` (held for the whole test body) — the
    // clear is serialized against every other test's env mutation the same
    // way the set above was.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

#[tokio::test]
async fn self_serve_disabled_allows_already_provisioned_org() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping self_serve_disabled_allows_already_provisioned_org: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation is serialized process-wide by `ENV_LOCK` — this
    // test holds `_env_guard` for its entire body, so no concurrent
    // reader/writer in this binary can observe these vars mid-mutation.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let db_name = "orgt_gate_existing_ctl";
    let mut conn = admin(&base).await;
    let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db_name);
    recreate(&mut conn, db_name).await;
    let (control, state) = make_state(&base, db_name, /* org_selfserve_disabled */ true).await;

    // Pre-seed the accounts row — as if the concierge/onboarded path (or a
    // sign-in before the flag flipped) already provisioned this org.
    let org_id = format!("org_existing_{}", Uuid::new_v4());
    let owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'person@example.com') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2)")
        .bind(owner)
        .bind(&org_id)
        .execute(&control)
        .await
        .unwrap();

    let app = coralswarm_backend::test_support_app(state);
    let sub = format!("selfserve_gate_existing_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": org_id, "org_role": "org:member",
    }));

    let req = HttpRequest::builder()
        .uri("/v1/graphs")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "an already-provisioned org must keep working under ORG_SELFSERVE_DISABLED"
    );

    assert_eq!(
        accounts_count_for_org(&control, &org_id).await,
        1,
        "the existing accounts row must not be duplicated"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)").as_str())
        .await
        .ok();
    // SAFETY: still under `_env_guard` (held for the whole test body) — the
    // clear is serialized against every other test's env mutation the same
    // way the set above was.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

#[tokio::test]
async fn flag_off_preserves_todays_auto_provisioning() {
    let Some(base) = base_url() else {
        eprintln!("skipping flag_off_preserves_todays_auto_provisioning: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation is serialized process-wide by `ENV_LOCK` — this
    // test holds `_env_guard` for its entire body, so no concurrent
    // reader/writer in this binary can observe these vars mid-mutation.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let db_name = "orgt_gate_flagoff_ctl";
    let mut conn = admin(&base).await;
    let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db_name);
    recreate(&mut conn, db_name).await;
    let (control, state) = make_state(&base, db_name, /* org_selfserve_disabled */ false).await;
    let app = coralswarm_backend::test_support_app(state);

    let org_id = format!("org_flagoff_{}", Uuid::new_v4());
    let sub = format!("selfserve_gate_flagoff_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": org_id, "org_role": "org:member",
    }));

    let req = HttpRequest::builder()
        .uri("/v1/graphs")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "flag-off must be byte-identical to today");

    assert_eq!(
        accounts_count_for_org(&control, &org_id).await,
        1,
        "flag-off must still auto-provision a brand-new org's accounts row, unchanged from today"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)").as_str())
        .await
        .ok();
    // SAFETY: still under `_env_guard` (held for the whole test body) — the
    // clear is serialized against every other test's env mutation the same
    // way the set above was.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

#[tokio::test]
async fn self_serve_disabled_webhook_org_created_is_noop_not_retried() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping self_serve_disabled_webhook_org_created_is_noop_not_retried: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    // SAFETY: env mutation is serialized process-wide by `ENV_LOCK` — this
    // test holds `_env_guard` for its entire body, so no concurrent
    // reader/writer in this binary can observe this var mid-mutation.
    unsafe {
        std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{WEBHOOK_SECRET_B64}"));
    }

    let db_name = "orgt_gate_webhook_ctl";
    let mut conn = admin(&base).await;
    let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db_name);
    recreate(&mut conn, db_name).await;
    let (control, state) = make_state(&base, db_name, /* org_selfserve_disabled */ true).await;
    let app = coralswarm_backend::test_support_app(state);

    let org_id = format!("org_webhook_{}", Uuid::new_v4());
    let body = json!({
        "type": "organization.created",
        "data": {"id": org_id, "name": "Acme", "created_by": "user_webhook_creator"},
    })
    .to_string();
    let (id, ts) = ("msg_orgt_gate_1", chrono::Utc::now().timestamp().to_string());
    let sig = sign_svix(WEBHOOK_SECRET_B64, id, &ts, &body);

    let req = HttpRequest::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", &ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "a self-serve-disabled rejection must be acked (200), never 500 — Svix would otherwise \
         retry a permanent rejection forever"
    );
    let parsed = body_json(resp).await;
    assert_eq!(parsed["skipped"], "self_serve_disabled");

    assert_eq!(
        accounts_count_for_org(&control, &org_id).await,
        0,
        "no shadow accounts row must be created from the organization.created webhook either"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {db_name} WITH (FORCE)").as_str())
        .await
        .ok();
    // SAFETY: still under `_env_guard` (held for the whole test body) — the
    // clear is serialized against every other test's env mutation the same
    // way the set above was.
    unsafe {
        std::env::remove_var("CLERK_WEBHOOK_SECRET");
    }
}
