//! Integration test for the internal admin `rotate-dek` endpoint's tenant
//! selector AND its rotation semantics (org-tenancy PR 1.3, revised after the
//! post-commit security review): drives the REAL `/internal` HTTP router
//! (`coralswarm_backend::test_support_internal_app`) exactly as an operator
//! (or `onboard_tenant.sh`'s `orgt_provision_dek`) would.
//!
//! Deliberately follows `onboard_tenant.sh`'s REAL step order — insert the
//! routing row as `provisioning` (step 4), call rotate-dek (step 7), and only
//! flip to `active` afterwards (step 9) — because the first version of this
//! test inserted the row as `active` up front and so never caught that the
//! handler's original status gate (`is_servable()`, Active-only) rejected the
//! exact sequence onboarding actually uses (P0 in the security review).
//!
//! Gated on `ORGT_TEST_BASE_URL`, following the same scratch-DB pattern as
//! `dek_relocation_pg.rs` / `binding_gate_pg.rs`.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use coralswarm_core::kek::unwrap_dek;
use serde_json::Value;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const INTERNAL_TOKEN: &str = "orgt-rotate-dek-test-token";

/// A fixed, test-only 32-byte (64 hex char) master KEK — `"22".repeat(32)`
/// isn't `const`-evaluable, so this is a plain function.
fn test_kek_hex() -> String {
    "22".repeat(32)
}
fn test_kek_bytes() -> [u8; 32] {
    [0x22u8; 32]
}

/// Serializes every test in this file against the process-global
/// `MASTER_KEK_HEX` / `INTERNAL_TOKEN` env vars — same rationale as
/// `mcp_pg_e2e.rs`'s own `ENV_LOCK` (separate compilation units can't share
/// `#[cfg(test)]`-only guards).
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

struct TestEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}
impl TestEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", test_kek_hex());
            std::env::set_var("INTERNAL_TOKEN", INTERNAL_TOKEN);
        }
        Self { _guard: guard }
    }
}
impl Drop for TestEnv {
    fn drop(&mut self) {
        unsafe {
            std::env::remove_var("MASTER_KEK_HEX");
            std::env::remove_var("INTERNAL_TOKEN");
        }
    }
}

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

async fn call(
    app: &axum::Router,
    account_id: Uuid,
    tenant_qs: Option<&str>,
    force: bool,
    token: Option<&str>,
) -> (StatusCode, Value) {
    let mut uri = format!("/internal/accounts/{account_id}/rotate-dek");
    let mut qs: Vec<String> = Vec::new();
    if let Some(t) = tenant_qs {
        qs.push(format!("tenant={t}"));
    }
    if force {
        qs.push("force=true".to_string());
    }
    if !qs.is_empty() {
        uri.push('?');
        uri.push_str(&qs.join("&"));
    }
    let mut builder = Request::builder().method("POST").uri(uri);
    if let Some(tok) = token {
        builder = builder.header("x-internal-token", tok);
    }
    let req = builder.body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1024 * 1024)
        .await
        .unwrap();
    let body: Value = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap()
    };
    (status, body)
}

#[tokio::test]
async fn rotate_dek_tenant_selector_and_rotation_semantics() {
    let Some(base) = base_url() else {
        eprintln!("skipping rotate_dek_tenant_selector_and_rotation_semantics: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = TestEnv::enable().await;
    let kek = test_kek_bytes();

    let (ctl, tenant_db) = ("orgt_rotate_ctl", "orgt_rotate_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + a personal account + three org accounts
    // (org_a: the happy path, org_b: suspended, org_c: offboarded). ───────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let personal_account_id: Uuid = sqlx::query_scalar(
        "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),'rot-p@x.co') RETURNING id) \
         INSERT INTO accounts(owner_user_id) SELECT id FROM u RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();

    async fn mk_org_account(control: &PgPool, email: &str, org: &str) -> Uuid {
        sqlx::query_scalar(
            "WITH u AS (INSERT INTO users(id,email) VALUES(gen_random_uuid(),$1) RETURNING id) \
             INSERT INTO accounts(owner_user_id, clerk_org_id) SELECT id, $2 FROM u RETURNING id",
        )
        .bind(email)
        .bind(org)
        .fetch_one(control)
        .await
        .unwrap()
    }
    let org_a_account_id = mk_org_account(&control, "rot-org-a@x.co", "org_a").await;
    let org_b_account_id = mk_org_account(&control, "rot-org-b@x.co", "org_b").await;
    let org_c_account_id = mk_org_account(&control, "rot-org-c@x.co", "org_c").await;

    // org_a starts `provisioning` — the REAL state during onboarding step 7.
    // org_b/org_c start in the two write-freeze states.
    for (org, account, slug, secret, status) in [
        ("org_a", org_a_account_id, "acme", "SECRET_A", "provisioning"),
        ("org_b", org_b_account_id, "beta", "SECRET_B", "suspended"),
        ("org_c", org_c_account_id, "gamma", "SECRET_C", "offboarded"),
    ] {
        financial_fixture::execute(
            &control,
            account,
            sqlx::query(
                "INSERT INTO tenant_databases(clerk_org_id,account_id,slug,db_secret_ref,status) \
                 VALUES($1,$2,$3,$4,$5)",
            )
            .bind(org)
            .bind(account)
            .bind(slug)
            .bind(secret)
            .bind(status),
        )
        .await
        .unwrap();
    }

    // ── Tenant DB for org_a: tenant tree + fingerprint, NO account_keys row
    // yet (first rotate-dek call must PROVISION it). ──────────────────────────
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_a')")
        .execute(&tenant_pool)
        .await
        .unwrap();

    let resolver = StaticMapResolver::from_pairs([("SECRET_A", with_db(&base, tenant_db))]);
    let runtime = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.tenant_runtime = Some(runtime);
    let state = Arc::new(state);
    let app = coralswarm_backend::test_support_internal_app(state.clone());

    // ── 1. No token → 401, before anything else. ──────────────────────────────
    let (status, _) = call(&app, personal_account_id, None, false, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);

    // ── 2. Org plane, first call, tenant status STILL "provisioning" (the real
    // onboarding order, step 7 before step 9's flip-to-active): must succeed
    // and PROVISION account_keys — this is the P0 fix. ────────────────────────
    let (status, body) = call(&app, org_a_account_id, Some("org_a"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body["action"], "provisioned");
    let org_wrapped_1: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
        .bind(org_a_account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    // Shadow-provisioning fence holds: no control-plane DEK for the org.
    let org_control_wrapped: Option<Vec<u8>> = sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1")
        .bind(org_a_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(org_control_wrapped, None);

    // Simulate onboarding step 9: flip the routing row to active.
    sqlx::query("UPDATE tenant_databases SET status='active' WHERE clerk_org_id='org_a'")
        .execute(&control)
        .await
        .unwrap();

    // ── 3. Org plane, second call (now active), no force: REWRAP — the
    // wrapped bytes change (fresh nonce) but the underlying DEK is IDENTICAL,
    // so no content is orphaned. This is the P1 fix. ──────────────────────────
    let (status, body) = call(&app, org_a_account_id, Some("org_a"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body["action"], "rewrapped");
    let org_wrapped_2: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
        .bind(org_a_account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert_ne!(org_wrapped_1, org_wrapped_2, "rewrap must change the wrapped bytes (fresh nonce)");
    let dek_1 = unwrap_dek(&kek, &org_wrapped_1).unwrap();
    let dek_2 = unwrap_dek(&kek, &org_wrapped_2).unwrap();
    assert_eq!(dek_1, dek_2, "rewrap must preserve the SAME underlying DEK — content must stay decryptable");

    // ── 4. Org plane, third call with force=true: DESTRUCTIVE — mints a brand
    // new DEK, orphaning the old one. ─────────────────────────────────────────
    let (status, body) = call(&app, org_a_account_id, Some("org_a"), true, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert!(
        body["action"].as_str().unwrap().starts_with("force-rotated"),
        "action was {:?}",
        body["action"]
    );
    let org_wrapped_3: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM account_keys WHERE account_id = $1")
        .bind(org_a_account_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    let dek_3 = unwrap_dek(&kek, &org_wrapped_3).unwrap();
    assert_ne!(dek_2, dek_3, "force=true must mint a genuinely NEW DEK");

    // ── 5. Consumer plane, personal account: same first-provision / rewrap /
    // force-rotate contract applies on the control plane too. ────────────────
    let (status, body) = call(&app, personal_account_id, None, false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK, "body: {body:?}");
    assert_eq!(body["action"], "provisioned");
    let personal_wrapped_1: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1")
        .bind(personal_account_id)
        .fetch_one(&control)
        .await
        .unwrap();

    let (status, body) = call(&app, personal_account_id, None, false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["action"], "rewrapped");
    let personal_wrapped_2: Vec<u8> = sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1")
        .bind(personal_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        unwrap_dek(&kek, &personal_wrapped_1).unwrap(),
        unwrap_dek(&kek, &personal_wrapped_2).unwrap(),
        "personal-plane rewrap must also preserve the same DEK"
    );

    // ── 6. Mismatched account_id/tenant pair → 400, no write anywhere. ────────
    let (status, _) = call(&app, personal_account_id, Some("org_a"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // ── 7. Suspended tenant (org_b) → 409, never touched. ─────────────────────
    let (status, _) = call(&app, org_b_account_id, Some("org_b"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::CONFLICT);
    let org_b_row: Option<i64> = sqlx::query_scalar("SELECT 1 FROM account_keys WHERE account_id = $1")
        .bind(org_b_account_id)
        .fetch_optional(&tenant_pool)
        .await
        .unwrap();
    assert_eq!(org_b_row, None, "a suspended tenant must never get an account_keys row written");

    // ── 8. Offboarded tenant (org_c) → 409. ───────────────────────────────────
    let (status, _) = call(&app, org_c_account_id, Some("org_c"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::CONFLICT);

    // ── 9. Unknown tenant → 404. ───────────────────────────────────────────────
    let (status, _) = call(&app, org_a_account_id, Some("org_nope"), false, Some(INTERNAL_TOKEN)).await;
    assert_eq!(status, StatusCode::NOT_FOUND);

    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
