//! Org-tenancy PR 4.1 — the assembled cross-tenant isolation probe suite.
//!
//! This file is deliberately NOT a wholesale re-run of every per-PR test —
//! `binding_gate_pg.rs`, `reachability_pg.rs`, `route_migration_org_pg.rs`,
//! `mcp_tenant_pg.rs`, `mcp_governance_pg.rs`, `member_ocean_promotion_pg.rs`,
//! `worker_harness_pg.rs`, and `consumer_rls_pg.rs` each already prove their
//! own PR's surface in isolation, exhaustively. What's missing — and what
//! this file adds — is the SEAM between them: scenarios that only exist once
//! multiple PRs are assembled together, which no single-PR test can express.
//!
//! ## Coverage matrix (surface → what's forged → asserted result)
//!
//! | # | Surface | Forged/adversarial input | Asserted result |
//! |---|---|---|---|
//! | 1 | Routes (binding gate) | org-A token + org-B host | 403, zero content read |
//! | 1b | Routes (binding gate) | `X-Forwarded-Host`/`Forwarded`/`X-Original-Host` naming the OTHER live tenant, on every host shape (own tenant, mismatched tenant, non-routing host, apex) | exactly the verdict the same request gets with NO forged header — identical status and body, because the header is never read. (The plan promised this probe twice, lines 53 + 128, and nothing implemented it.) |
//! | 1 | Routes (`Tenant`/reachability) | org-A token+host, org-B's real graph/atom id, with org-B's tenant DB LIVE in the SAME registry at the same time | 404/403, never org B's row |
//! | 2 | Routes (`ReachabilityResolver`, PR 1.7) | a team-X member requesting a team-Y ocean, through the REAL gate→extractor→route→resolver chain (not the direct resolver call `reachability_pg.rs` uses) | 403 for team-Y, 200 for team-X/org oceans |
//! | 3 | R2 read path (routes AND MCP) | a REACHABLE atom (real graph, real org, passes every authz check) whose stored `content_blob_key` carries the WRONG org's prefix | content: null, never fetched — the new PR 4.1 assert (`assert_blob_key_org_scope`) catches what reachability alone cannot |
//! | 4 | Promotion (same-DB invariant) | an org-plane promote whose SOURCE reef/ocean lives on the consumer plane (the caller's own personal ocean), caller a genuine live member of the target org | 403 on both `/promote` and `/promote-source`, zero rows written into the tenant DB |
//!
//! ## Relied upon, not duplicated
//! - **MCP forged cross-org ids / revoked-member-403-pre-pool / claim-only
//!   routing-status gates**: exhaustively covered by `mcp_tenant_pg.rs` and
//!   `mcp_governance_pg.rs` end-to-end through the real `/mcp` router — not
//!   repeated here.
//! - **Workers** ("a job in A's queue processes on A's pool only"):
//!   `worker_harness_pg.rs::jobs_process_per_tenant_and_poisoned_tenant_is_isolated`
//!   already proves this directly against real per-tenant pools; there is no
//!   further seam to add without duplicating that file's harness.
//! - **Consumer-RLS under the `coralswarm_app` role**: `consumer_rls_pg.rs`
//!   already connects AS a real, separately-authenticated `coralswarm_app`
//!   login (not `SET ROLE` inside the owner's session) and asserts real
//!   cross-account denial across the documented 36-table roster, plus the
//!   strict-predicate and inertness proofs the plan calls for. That IS the
//!   "green probe means enforcement is real" probe PR 4.1 requires — it is
//!   verified still green as part of this PR (see the test run below), not
//!   re-implemented here. Tenant DBs skip RLS by design (whole-DB isolation
//!   already covers them), so there is no consumer-RLS × tenant-pool seam to
//!   test.
//! - **Team-ocean gating parity (consumer path vs tenant path, D9 union
//!   semantics)**: the pure two-path comparison lives in `reachability_pg.rs`;
//!   this file's team test (`team_ocean_gating_...`) is the seam version —
//!   same fixture shape, driven through the real HTTP stack instead of a
//!   direct resolver call.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights). Same scratch-DB pattern as every other `*_pg.rs` file in
//! this directory.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test cross_tenant_probe_pg -- --nocapture

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use coralswarm_core::r2::R2Client;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const TEST_MASTER_KEK_HEX: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";

/// Serializes this file's process-global env mutations (`CLERK_ISSUER`,
/// `CLERK_JWKS_URL`, `MCP_ENABLED`, `MASTER_KEK_HEX`, ...) against each other
/// — same pattern as `mcp_tenant_pg.rs`'s `ENV_LOCK` / `route_migration_org_pg.rs`'s.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

// ── Scratch-DB plumbing (pattern shared by every `*_pg.rs` file) ─────────────

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
async fn recreate_all(base: &str, dbs: &[&str]) -> PgConnection {
    let mut conn = admin(base).await;
    for db in dbs {
        recreate(&mut conn, db).await;
    }
    conn
}
async fn drop_all(mut conn: PgConnection, dbs: &[&str]) {
    for db in dbs {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

async fn new_control_pool(base: &str, db: &str) -> PgPool {
    let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}

async fn new_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool: PgPool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

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

fn org_token(sub: &str, org_id: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": org_id, "org_role": "org:member",
    }))
}

const MCP_RESOURCE: &str = "http://localhost/mcp";

/// A resource-bound OAuth access token for the positive MCP security probe.
/// Ordinary REST session fixtures above remain separate.
fn mcp_org_token(sub: &str, org_id: &str) -> String {
    mint_oauth(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "aud": MCP_RESOURCE,
        "client_id": "cross_tenant_probe_client", "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": "org:member",
    }))
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

// ── Identity/routing/content seeding ─────────────────────────────────────────

/// Seed a control-plane org identity (owner user + `accounts` row + an ACTIVE
/// `tenant_databases` routing row pointing `secret_ref` at this org's tenant
/// pool). Returns the control-plane `account_id`.
async fn seed_org_identity(
    control: &PgPool,
    clerk_org_id: &str,
    slug: &str,
    secret_ref: &str,
) -> Uuid {
    let owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("owner-{}@test.local", Uuid::new_v4()))
    .fetch_one(control)
    .await
    .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(clerk_org_id)
    .fetch_one(control)
    .await
    .unwrap();
    let mut financial = coralswarm_backend::tenancy::scope::begin_account_scoped(control, account_id)
        .await
        .unwrap();
    coralswarm_backend::billing::renewal::lock_period_rotation(&mut financial, account_id)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, $3, $4, 'active')",
    )
    .bind(clerk_org_id)
    .bind(account_id)
    .bind(slug)
    .bind(secret_ref)
    .execute(&mut *financial)
    .await
    .unwrap();
    financial.commit().await.unwrap();
    account_id
}

async fn seed_membership(control: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(control)
    .await
    .unwrap();
}

async fn seed_graph(tenant: &PgPool, account_id: Uuid, kind: &str, name: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn seed_inline_atom(tenant: &PgPool, graph_id: Uuid, content: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', $2, 'unused', $3, '{}', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(content)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn new_team(pool: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, 'team', $3) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(pool)
    .await
    .unwrap()
}
async fn add_team_member(pool: &PgPool, team_id: Uuid, user_id: Uuid) {
    sqlx::query(
        "INSERT INTO team_members (team_id, user_id, role, departing_at) \
         VALUES ($1, $2, 'member', NULL)",
    )
    .bind(team_id)
    .bind(user_id)
    .execute(pool)
    .await
    .unwrap();
}

/// Build an `AppState` (control as the ambient consumer plane, an ENABLED
/// tenant runtime resolving `resolvers`) and the real public router over it.
fn build_app(
    control: &PgPool,
    resolvers: impl IntoIterator<Item = (&'static str, String)>,
) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs(resolvers);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn req_get(token: &str, host: &str, uri: &str) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("GET")
        .uri(uri)
        .header("host", host)
        .header("authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap()
}

fn req_post(token: &str, host: &str, uri: &str, body: &Value) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("POST")
        .uri(uri)
        .header("host", host)
        .header("content-type", "application/json")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(body).unwrap()))
        .unwrap()
}

/// One `/mcp` JSON-RPC round trip (pattern: `mcp_tenant_pg.rs`'s own
/// `mcp_rpc`), for the `select_tenant`/`get_atom` two-call sequence
/// claim-based-tenancy §Phase 3 PR 3.3 requires to reach the tenant plane
/// over MCP now (the org-subdomain-Host shortcut this file used before that
/// PR no longer resolves to the tenant plane at all).
async fn mcp_rpc(app: &axum::Router, host: &str, token: &str, method: &str, params: Value) -> Value {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let req = HttpRequest::builder()
        .method("POST")
        .uri("/mcp")
        .header("host", host)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "/mcp transport failure for {method}");
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    if content_type.contains("text/event-stream") {
        let text = String::from_utf8_lossy(&bytes);
        let joined: String = text
            .lines()
            .filter_map(|l| l.strip_prefix("data:"))
            .map(|r| r.strip_prefix(' ').unwrap_or(r))
            .collect::<Vec<_>>()
            .join("\n");
        serde_json::from_str(&joined).expect("parse SSE json-rpc payload")
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1. Routes + binding gate: forged cross-org ids with BOTH tenants live at once
// ═══════════════════════════════════════════════════════════════════════════

/// The genuinely new wrinkle beyond `route_migration_org_pg.rs` (whose tests
/// each wire up exactly ONE live tenant DB): here TWO real tenant databases
/// are simultaneously registered in the SAME `TenantRegistry`/app, so a
/// forged id under org A's token isn't merely "the only other DB that
/// exists" — org B's real row is live, reachable in principle by the same
/// process, at the moment org A's request is served.
#[tokio::test]
async fn routes_seam_forged_cross_org_ids_never_cross_with_both_tenants_live() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping routes_seam_forged_cross_org_ids_never_cross_with_both_tenants_live: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: serialized by `_env_guard` for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ta, tb) = ("orgt_x41_route_ctl", "orgt_x41_route_ta", "orgt_x41_route_tb");
    let conn = recreate_all(&base, &[ctl, ta, tb]).await;

    let control = new_control_pool(&base, ctl).await;
    let org_a_account = seed_org_identity(&control, "org_x41_a", "acme", "SECRET_A").await;
    let org_b_account = seed_org_identity(&control, "org_x41_b", "beta", "SECRET_B").await;

    let tenant_a = new_tenant_pool(&base, ta, "org_x41_a").await;
    let tenant_b = new_tenant_pool(&base, tb, "org_x41_b").await;
    let org_a_graph = seed_graph(&tenant_a, org_a_account, "org", "Acme org ocean").await;
    let org_a_atom = seed_inline_atom(&tenant_a, org_a_graph, "acme secret roadmap").await;
    let org_b_graph = seed_graph(&tenant_b, org_b_account, "org", "Beta org ocean").await;
    let org_b_atom = seed_inline_atom(&tenant_b, org_b_graph, "beta secret roadmap").await;

    let app = build_app(
        &control,
        [("SECRET_A", with_db(&base, ta)), ("SECRET_B", with_db(&base, tb))],
    );

    let sub_a = format!("route_seam_a_{}", Uuid::new_v4());
    let sub_b = format!("route_seam_b_{}", Uuid::new_v4());
    let a_token = org_token(&sub_a, "org_x41_a");
    let b_token = org_token(&sub_b, "org_x41_b");
    // Both orgs' retired API addresses, kept only so the arms below can prove
    // they are dead — nothing in this test routes through them any more.
    let acme_host = "acme.coralswarm.example";
    let beta_host = "beta.coralswarm.example";

    // ── Positive control: each org's own token reads its own content, on the
    //    shared apex. Without this the negative arms below could all be
    //    satisfied by a gate that refuses everything. ──────────────────────
    let resp = app.clone().oneshot(req_get(&a_token, APEX, &format!("/v1/atoms/{org_a_atom}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "org A must read its own atom");
    assert_eq!(body_json(resp).await["content"], json!("acme secret roadmap"));

    let resp = app.clone().oneshot(req_get(&b_token, APEX, &format!("/v1/atoms/{org_b_atom}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "org B must read its own atom");
    assert_eq!(body_json(resp).await["content"], json!("beta secret roadmap"));

    // ── 1. The org API addresses are RETIRED (claim-based tenancy Phase 4
    //    cutover step 3). This arm used to be "org-A token on org-B's host →
    //    403", which was the ONLY thing the mismatched-host check ever
    //    proved. That check is gone with the host, so what is asserted here
    //    now is that the host is gone too — for BOTH orgs, and for the
    //    matching token as well as the mismatched one, since a 410 that
    //    applied only to the "wrong" token would still be a working address.
    //
    //    The isolation property this arm used to carry has not evaporated: it
    //    moved to arm 2, which is the shape the plan names as the survivor
    //    (org-A token, org-B's REAL ids, both tenant DBs live → 404/403), and
    //    which is strictly stronger — it goes through a real pool rather than
    //    stopping at the front door. ────────────────────────────────────────
    for (host, token, who) in [
        (beta_host, &a_token, "org A's token on org B's retired address"),
        (acme_host, &b_token, "org B's token on org A's retired address"),
        (acme_host, &a_token, "org A's token on org A's OWN retired address"),
        (beta_host, &b_token, "org B's token on org B's OWN retired address"),
    ] {
        let resp = app.clone().oneshot(req_get(token, host, "/v1/graphs")).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::GONE,
            "{who} must be 410 — the org API address is retired for every caller, \
             or it is not retired at all"
        );
    }

    // ── 2. org-A token on the shared apex, org-B's REAL atom/graph ids
    //    (guessed/forged), with org B's tenant DB live in the SAME registry:
    //    never org B's data, never a 500 (which would leak "the id shape was
    //    at least valid").
    //
    //    This is the plan's named surviving shape, and the cutover makes it
    //    MORE load-bearing rather than less: with the host gone, the only
    //    things standing between org A and org B's row are the claim, the
    //    routing row it resolves, and the pool that fingerprint-checks it.
    //    Both tenants are live in one process at the moment this runs. ──────
    let resp = app.clone().oneshot(req_get(&a_token, APEX, &format!("/v1/atoms/{org_b_atom}"))).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "org A's own pool must never resolve org B's real atom id"
    );
    let resp = app.clone().oneshot(req_get(&a_token, APEX, &format!("/v1/graphs/{org_b_graph}/stats"))).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "org A's own pool must never resolve org B's real graph id as reachable"
    );

    // Reciprocal: org-B token on the same apex, org-A's real ids.
    let resp = app.clone().oneshot(req_get(&b_token, APEX, &format!("/v1/atoms/{org_a_atom}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "org B must never resolve org A's real atom id");
    let resp = app.clone().oneshot(req_get(&b_token, APEX, &format!("/v1/graphs/{org_a_graph}/stats"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "org B must never resolve org A's real graph id");

    control.close().await;
    tenant_a.close().await;
    tenant_b.close().await;
    drop_all(conn, &[ctl, ta, tb]).await;
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1b. Spoofed `X-Forwarded-Host` never selects a tenant (plan lines 53 + 128)
// ═══════════════════════════════════════════════════════════════════════════

/// The probe the plan lists twice and that nothing implemented:
/// `grep -rn "X-Forwarded-Host" backend/src` returns nothing, so the gate is
/// correct **by omission** — `middleware::auth` reads `Host` and only `Host`.
/// That is exactly the kind of property that decays silently, so this asserts
/// it as behavior.
///
/// The seam this file exists for: BOTH tenants are live in the same registry,
/// so a forged forwarded header has a real, reachable other tenant to try to
/// cross into — not just a nonexistent one.
///
/// Claim-based tenancy (PR 1.2) made this probe matter MORE, not less. The
/// apex used to refuse every org token, so arms 4's forged header had nothing
/// to steer; the apex now binds a tenant from the claim, so there is a live
/// routing decision on every arm here. Arm 4 is therefore asserted
/// differentially — identical response with and without the header — plus a
/// named outcome, so it cannot pass by comparing two refusals.
#[tokio::test]
async fn spoofed_forwarded_host_never_selects_a_tenant_with_both_tenants_live() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping spoofed_forwarded_host_never_selects_a_tenant_with_both_tenants_live: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: serialized by `_env_guard` for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ta, tb) = ("orgt_x41_fwd_ctl", "orgt_x41_fwd_ta", "orgt_x41_fwd_tb");
    let conn = recreate_all(&base, &[ctl, ta, tb]).await;

    let control = new_control_pool(&base, ctl).await;
    let org_a_account = seed_org_identity(&control, "org_x41f_a", "acme", "SECRET_A").await;
    let org_b_account = seed_org_identity(&control, "org_x41f_b", "beta", "SECRET_B").await;

    let tenant_a = new_tenant_pool(&base, ta, "org_x41f_a").await;
    let tenant_b = new_tenant_pool(&base, tb, "org_x41f_b").await;
    let org_a_graph = seed_graph(&tenant_a, org_a_account, "org", "Acme org ocean").await;
    let org_a_atom = seed_inline_atom(&tenant_a, org_a_graph, "acme secret roadmap").await;
    let org_b_graph = seed_graph(&tenant_b, org_b_account, "org", "Beta org ocean").await;
    let org_b_atom = seed_inline_atom(&tenant_b, org_b_graph, "beta secret roadmap").await;

    let app = build_app(
        &control,
        [("SECRET_A", with_db(&base, ta)), ("SECRET_B", with_db(&base, tb))],
    );

    let a_token = org_token(&format!("fwd_a_{}", Uuid::new_v4()), "org_x41f_a");
    let b_token = org_token(&format!("fwd_b_{}", Uuid::new_v4()), "org_x41f_b");
    let acme_host = "acme.coralswarm.example";
    let beta_host = "beta.coralswarm.example";

    // Same as `req_get`, plus the proxy headers a hostile client might inject.
    let forged = |token: &str, host: &str, forwarded: &str, uri: &str| {
        HttpRequest::builder()
            .method("GET")
            .uri(uri)
            .header("host", host)
            .header("x-forwarded-host", forwarded)
            .header("forwarded", format!("host={forwarded}"))
            .header("x-original-host", forwarded)
            .header("authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap()
    };

    // ── 1. org A's token on the APEX, with a forwarded header naming org B's
    //    (now retired) address: the request stays on org A's pool, so org B's
    //    real atom id is still unreachable and org A's own is still served.
    //
    //    This arm moved from `acme_host` to the apex with Phase 4 cutover step
    //    3, and it got sharper rather than weaker: on the old org host, the
    //    Host header itself pinned the tenant, so a forwarded header had a
    //    second, independent signal arguing against it. On the apex there is
    //    only the claim — a forwarded header is now the ONLY other thing in
    //    the request that names a tenant. ────────────────────────────────────
    let resp = app
        .clone()
        .oneshot(forged(&a_token, APEX, beta_host, &format!("/v1/atoms/{org_b_atom}")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "a forwarded header naming org B must not move org A's request onto org B's pool"
    );
    let resp = app
        .clone()
        .oneshot(forged(&a_token, APEX, beta_host, &format!("/v1/atoms/{org_a_atom}")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "org A must still read its own atom");
    assert_eq!(body_json(resp).await["content"], json!("acme secret roadmap"));

    // ── 2. A forwarded header cannot RESURRECT a retired org address.
    //
    //    This arm used to be "org B's token on org A's host + a forwarded
    //    header naming org B → still the 403 the Host-derived binding gives".
    //    There is no Host-derived binding left, so the question it asks now is
    //    the one that matters after the cutover: can a client-supplied header
    //    turn the 410 back into a served request, on the address the founder's
    //    MCP client is being moved off? Every forwarded value — including the
    //    apex itself, the one value that WOULD route if the header were read —
    //    must leave the 410 alone. ──────────────────────────────────────────
    for forwarded in [beta_host, acme_host, APEX, "evil.com"] {
        let resp = app
            .clone()
            .oneshot(forged(&b_token, acme_host, forwarded, "/v1/graphs"))
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::GONE,
            "X-Forwarded-Host: {forwarded} changed how a retired org address is answered"
        );
    }

    // ── 3. A host that routes nowhere, with a forwarded header naming a real
    //    tenant: still the fail-closed 404, in both directions. A retired org
    //    address is deliberately NOT in this list — it answers 410, not 404,
    //    and arm 2 covers it. ────────────────────────────────────────────────
    for (token, forwarded) in [(&a_token, acme_host), (&b_token, beta_host)] {
        for host in ["evil.com", "coralswarm-backend.example"] {
            let resp = app
                .clone()
                .oneshot(forged(token, host, forwarded, "/v1/graphs"))
                .await
                .unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "X-Forwarded-Host: {forwarded} must not promote {host} onto a tenant"
            );
        }
    }

    // ── 4. The apex with a forwarded header naming the OTHER live tenant.
    //
    //    This arm used to assert a flat 403. That assertion was cheap: before
    //    claim-based tenancy (`docs/plans/claim-based-tenancy.md` PR 1.2) an
    //    org token on the apex was refused outright, so no forged header could
    //    steer a binding that never happened. The apex now binds the tenant the
    //    CLAIM names — which is precisely what turns this arm from a formality
    //    into the probe's real question: a routing decision genuinely happens
    //    here now, so can a proxy header redirect it?
    //
    //    Two assertions, because either alone is weak:
    //
    //      (a) DIFFERENTIAL — the response must be identical, status and body,
    //          to the same request with no forged header. A status literal can
    //          be satisfied by a refusal that happens for an unrelated reason;
    //          equality with the un-forged request is what says the header was
    //          never read.
    //      (b) NAMED — and because two identical refusals would satisfy (a)
    //          vacuously, say what the outcome actually is: the apex binds org
    //          A from its claim, so org A's atom is served and org B's — live
    //          in this same registry, and named by the forged header — is not.
    for uri in [
        format!("/v1/atoms/{org_a_atom}"),
        format!("/v1/atoms/{org_b_atom}"),
        "/v1/graphs".to_string(),
    ] {
        let clean = app.clone().oneshot(req_get(&a_token, APEX, &uri)).await.unwrap();
        let clean_status = clean.status();
        let clean_body = body_json(clean).await;
        for spoof in [beta_host, acme_host, "evil.com", "nope.coralswarm.example"] {
            let resp = app.clone().oneshot(forged(&a_token, APEX, spoof, &uri)).await.unwrap();
            let status = resp.status();
            assert_eq!(
                status, clean_status,
                "X-Forwarded-Host: {spoof} changed the status of an apex {uri} \
                 request ({status} vs {clean_status} with no forged header)"
            );
            assert_eq!(
                body_json(resp).await,
                clean_body,
                "X-Forwarded-Host: {spoof} changed the body of an apex {uri} request"
            );
        }
    }
    // (b): the apex binding is org A's, and a header naming org B does not
    // move it. Without this the loop above would pass just as happily if every
    // apex request were refused.
    let resp = app
        .clone()
        .oneshot(forged(&a_token, APEX, beta_host, &format!("/v1/atoms/{org_a_atom}")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the apex must bind the tenant org A's own CLAIM names"
    );
    assert_eq!(body_json(resp).await["content"], json!("acme secret roadmap"));
    let resp = app
        .clone()
        .oneshot(forged(&a_token, APEX, beta_host, &format!("/v1/atoms/{org_b_atom}")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "a forwarded header naming org B must not reach org B's atom from the apex"
    );

    control.close().await;
    tenant_a.close().await;
    tenant_b.close().await;
    drop_all(conn, &[ctl, ta, tb]).await;
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 2. Reachability's team-ocean gating (PR 1.7), end-to-end through the real
//    gate → extractor → route → resolver chain (not the direct resolver call
//    `reachability_pg.rs` exercises).
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn team_ocean_gating_enforced_through_the_real_http_stack() {
    let Some(base) = base_url() else {
        eprintln!("skipping team_ocean_gating_enforced_through_the_real_http_stack: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (ctl, ta) = ("orgt_x41_team_ctl", "orgt_x41_team_ta");
    let conn = recreate_all(&base, &[ctl, ta]).await;

    let control = new_control_pool(&base, ctl).await;
    let org_account = seed_org_identity(&control, "org_x41_team", "acme-team", "SECRET_TEAM").await;

    // Two org members: `owner` (creates both teams, in team1 only) and
    // `member` (in team1 only, NOT team2) — mirrors `reachability_pg.rs`'s
    // fixture shape exactly, but seeded so it can be driven over real HTTP.
    let owner_sub = format!("team_owner_{}", Uuid::new_v4());
    let member_sub = format!("team_member_{}", Uuid::new_v4());
    let owner_user: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("{owner_sub}@users.clerk.local"))
    .fetch_one(&control)
    .await
    .unwrap();
    let member_user: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("{member_sub}@users.clerk.local"))
    .fetch_one(&control)
    .await
    .unwrap();
    seed_membership(&control, org_account, owner_user, "owner").await;
    seed_membership(&control, org_account, member_user, "member").await;

    let tenant = new_tenant_pool(&base, ta, "org_x41_team").await;
    let org_ocean = seed_graph(&tenant, org_account, "org", "Acme org ocean").await;
    let team1_ocean = seed_graph(&tenant, org_account, "team", "Team One ocean").await;
    let team2_ocean = seed_graph(&tenant, org_account, "team", "Team Two ocean").await;
    let team1 = new_team(&tenant, org_account, team1_ocean, owner_user).await;
    let _team2 = new_team(&tenant, org_account, team2_ocean, owner_user).await;
    add_team_member(&tenant, team1, member_user).await;
    // `member_user` is deliberately NOT added to team2.

    let app = build_app(&control, [("SECRET_TEAM", with_db(&base, ta))]);
    let token = org_token(&member_sub, "org_x41_team");
    // Transport only: Phase 4 cutover step 3 retired the org API address, and
    // this probe is about `ReachabilityResolver`, not about hosts.
    let host = APEX;

    // ── list_graphs: sees org_ocean + team1_ocean, never team2_ocean. ───────
    let resp = app.clone().oneshot(req_get(&token, host, "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids: Vec<String> = body
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["id"].as_str().unwrap().to_string())
        .collect();
    assert!(ids.contains(&org_ocean.to_string()), "must see the non-team org ocean: {ids:?}");
    assert!(ids.contains(&team1_ocean.to_string()), "must see team1's ocean (member is in team1): {ids:?}");
    assert!(!ids.contains(&team2_ocean.to_string()), "must NOT see team2's ocean (member is not in team2): {ids:?}");

    // ── get_ocean_stats: 200 for team1, 403 for team2 — through the real
    //    gate + `Tenant` extractor + `ReachabilityResolver`, not a direct
    //    resolver call. ─────────────────────────────────────────────────────
    let resp = app.clone().oneshot(req_get(&token, host, &format!("/v1/graphs/{team1_ocean}/stats"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "member is in team1 -> must reach team1's ocean");
    let resp = app.clone().oneshot(req_get(&token, host, &format!("/v1/graphs/{team2_ocean}/stats"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN, "member is NOT in team2 -> must not reach team2's ocean");

    control.close().await;
    tenant.close().await;
    drop_all(conn, &[ctl, ta]).await;
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. R2 read-path org-segment assert (org-tenancy PR 4.1 Part B): a REACHABLE
//    atom whose stored blob_key carries the WRONG org's prefix must never be
//    fetched, at BOTH the routes surface and the MCP surface.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn blob_key_org_scope_mismatch_seam_route_and_mcp_refuse_a_reachable_forged_blob() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping blob_key_org_scope_mismatch_seam_route_and_mcp_refuse_a_reachable_forged_blob: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: every env var this test touches is mutated only while holding
    // `_env_guard`, which is held for this test's whole body.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
        std::env::set_var("MCP_ENABLED", "1");
        std::env::set_var("MCP_RESOURCE_URL", MCP_RESOURCE);
        std::env::set_var("RATE_LIMIT_BACKEND", "pg");
        std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
        std::env::remove_var("MCP_DEFAULT_SCOPES");
    }

    let (ctl, ta) = ("orgt_x41_blobscope_ctl", "orgt_x41_blobscope_ta");
    let conn = recreate_all(&base, &[ctl, ta]).await;

    let control = new_control_pool(&base, ctl).await;
    let org_account = seed_org_identity(&control, "org_x41_blob", "acme-blob", "SECRET_BLOB").await;
    let sub = format!("blobscope_member_{}", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("{sub}@users.clerk.local"))
    .fetch_one(&control)
    .await
    .unwrap();
    seed_membership(&control, org_account, user_id, "member").await;

    let tenant = new_tenant_pool(&base, ta, "org_x41_blob").await;
    let org_graph = seed_graph(&tenant, org_account, "org", "Acme org ocean").await;

    // Org DEK, so DEK resolution succeeds and the code actually reaches the
    // R2 branch (the assert under test runs BEFORE the network call, so a
    // dummy/never-dialed R2Client is safe here).
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");
    let org_dek = coralswarm_core::kek::generate_dek();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account)
        .bind(coralswarm_core::kek::wrap_dek(&kek, &org_dek).unwrap())
        .execute(&tenant)
        .await
        .unwrap();

    // The forged atom: content_inline is NULL (forces the R2 branch),
    // content_blob_key is SHAPE-VALID (3-segment, real UUIDs) but carries
    // "org_x41_OTHER"'s prefix — not this request's own org
    // ("org_x41_blob"). Simulates a bug/corruption/forgery upstream of the
    // blob read: the row itself is 100% legitimately reachable (real graph,
    // real org, passes every reachability/membership check).
    let forged_key = format!("org_x41_OTHER/{org_graph}/{}", Uuid::new_v4());
    let forged_atom: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', $2, $3, NULL, '{}', now()) RETURNING atom_id",
    )
    .bind(org_graph)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(&forged_key)
    .fetch_one(&tenant)
    .await
    .unwrap();

    // Positive control: a normal inline atom in the SAME graph/org still
    // resolves — proves the mismatch check doesn't reject everything.
    let control_atom = seed_inline_atom(&tenant, org_graph, "a perfectly normal atom").await;

    // ── AppState: r2 IS configured (dummy creds — `R2Client::new` makes no
    //    network call), so `get_atom`/MCP's `get_atom` both reach the branch
    //    that calls `assert_blob_key_org_scope`. If the assert did NOT fire
    //    before the network call, this test would hang/error trying to reach
    //    a nonexistent bucket instead of returning null content promptly. ────
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.r2 = Some(Arc::new(
        R2Client::new("dummy-account", "dummy-key", "dummy-secret", "dummy-bucket").await,
    ));
    let resolver = StaticMapResolver::from_pairs([("SECRET_BLOB", with_db(&base, ta))]);
    state.tenant_runtime = Some(TenantRuntime::new(
        true,
        APEX,
        Arc::new(TenantRegistry::new(Arc::new(resolver))),
    ));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let token = org_token(&sub, "org_x41_blob");
    // Both surfaces go through the apex now: Phase 4 cutover step 3 retired
    // `<slug>.api.<apex>` for REST, and claim-based-tenancy §Phase 3 PR 3.3
    // retires `/mcp`'s own, independent org-host classification the same way
    // — `McpConfig::resolve_resource`'s `Subdomain` arm collapses to
    // `personal_identity`, so there is no more org host for `/mcp` to answer
    // on either. The org claim selects the REST tenant; the MCP tenant is
    // now selected explicitly via `select_tenant` below, regardless of Host.
    let host = APEX;

    // ── Route surface: 200 (the atom itself IS found/reachable), content: null
    //    (the assert refused the mismatched blob_key). ───────────────────────
    let resp = app.clone().oneshot(req_get(&token, host, &format!("/v1/atoms/{forged_atom}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "the atom row itself is legitimately reachable");
    let body = body_json(resp).await;
    assert_eq!(
        body["content"],
        Value::Null,
        "a blob_key whose org segment doesn't match the request's own org context must never \
         be fetched, even for an otherwise-reachable atom: {body:?}"
    );

    // Positive control via the same route.
    let resp = app.clone().oneshot(req_get(&token, host, &format!("/v1/atoms/{control_atom}"))).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(body_json(resp).await["content"], json!("a perfectly normal atom"));

    // ── MCP surface: same story, through the real /mcp router. ─────────────
    //
    // Claim-based-tenancy §Phase 3 PR 3.3 retires the org MCP resource — the
    // Host is no longer the tenancy chooser at all (`McpConfig::
    // resolve_resource`'s `Subdomain` arm now collapses to `Personal`), and
    // decision 5's catalogue gate refuses any non-tenancy tool outright for
    // a caller with ≥1 org membership and no active selection. So reaching
    // the tenant plane over MCP now requires an explicit `select_tenant`
    // FIRST — the only surviving path — rather than simply knocking on the
    // org's connector host with a claim, which is what this probe did before
    // this PR (and what `host` above is otherwise unused for, now that the
    // route surface above is the only consumer of it as a tenancy chooser).
    let mcp_token = mcp_org_token(&sub, "org_x41_blob");
    let select = mcp_rpc(
        &app,
        host,
        &mcp_token,
        "tools/call",
        json!({"name": "select_tenant", "arguments": {"org_id": "org_x41_blob"}}),
    )
    .await;
    let select_result = &select["result"];
    assert_eq!(
        select_result.get("isError").and_then(Value::as_bool),
        Some(false),
        "select_tenant must succeed for a real, live membership: {select:?}"
    );

    let rpc = mcp_rpc(
        &app,
        host,
        &mcp_token,
        "tools/call",
        json!({"name": "get_atom", "arguments": {"atom_id": forged_atom}}),
    )
    .await;
    let result = &rpc["result"];
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    assert!(!is_error, "get_atom tool call itself must not error: {rpc:?}");
    let tool_body: Value = serde_json::from_str(result["content"][0]["text"].as_str().unwrap()).unwrap();
    assert_eq!(
        tool_body["content"],
        Value::Null,
        "MCP get_atom must also refuse the org-scope-mismatched blob_key: {tool_body:?}"
    );

    control.close().await;
    tenant.close().await;
    drop_all(conn, &[ctl, ta]).await;
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
        std::env::remove_var("MCP_ENABLED");
        std::env::remove_var("MCP_RESOURCE_URL");
        std::env::remove_var("RATE_LIMIT_BACKEND");
        std::env::remove_var("MASTER_KEK_HEX");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Promotion never crosses a database (fix/promotion-same-db-only)
// ═══════════════════════════════════════════════════════════════════════════

/// This section used to exercise `services::promotion::promote_node_to_org`,
/// the cross-plane path that read a caller's PERSONAL ocean off the consumer
/// pool and re-encrypted its content into an org's tenant DB. That path is
/// deleted: an org-plane promotion's source must now live in the org's OWN
/// tenant database, and everything else is refused.
///
/// The probe therefore moves UP a level — from the service (which no longer
/// exists) to the real HTTP stack, which is where the refusal has to hold. It
/// asserts the property the deleted test could only assert about targets, and
/// now asserts it about SOURCES too: content on one database never reaches
/// another by promotion, and a refused attempt writes nothing.
#[tokio::test]
async fn promotion_never_crosses_a_database_org_plane_refuses_a_consumer_plane_source() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping promotion_never_crosses_a_database_org_plane_refuses_a_consumer_plane_source: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    // SAFETY: MASTER_KEK_HEX is process-global; serialize against sibling
    // tests in this binary via ENV_LOCK for this test's whole body.
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    unsafe {
        std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }
    let kek = coralswarm_core::kek::load_kek().expect("test KEK loads");

    let (ctl, ta) = ("orgt_x41_promo_ctl", "orgt_x41_promo_ta");
    let conn = recreate_all(&base, &[ctl, ta]).await;
    let control = new_control_pool(&base, ctl).await;

    // The caller's PERSONAL ocean, with a reef and a coral — on the consumer
    // plane, which is exactly where an org-plane promotion may no longer reach.
    let personal_dek = coralswarm_core::kek::generate_dek();
    let caller: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
    )
    .bind(format!("caller-{}@test.local", Uuid::new_v4()))
    .fetch_one(&control)
    .await
    .unwrap();
    let personal_account: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
    )
    .bind(caller)
    .bind(coralswarm_core::kek::wrap_dek(&kek, &personal_dek).unwrap())
    .fetch_one(&control)
    .await
    .unwrap();
    let store = PostgresStore::new(control.clone());
    let personal_graph = store
        .create_graph(personal_account, "Personal Ocean", "personal")
        .await
        .expect("create personal graph")
        .id;
    let node: Uuid = sqlx::query_scalar(
        "INSERT INTO nodes (node_id, graph_id, type, title, body) \
         VALUES (gen_random_uuid(), $1, 'entity', 'Same-DB Promotion Probe', '{}'::jsonb) \
         RETURNING node_id",
    )
    .bind(personal_graph)
    .fetch_one(&control)
    .await
    .unwrap();
    let atom = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO atoms (atom_id, graph_id, source_type, source_id, content_blob_key, \
                            participants, \"timestamp\", scope, content_inline, fts_language, header_len) \
         VALUES ($1, $2, 'note', 'src-1', $3, '{}', now(), 'private', 'promote me', 'english', 0)",
    )
    .bind(atom)
    .bind(personal_graph)
    .bind(format!("{personal_graph}/{atom}"))
    .execute(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO node_atoms (node_id, atom_id) VALUES ($1, $2)")
        .bind(node)
        .bind(atom)
        .execute(&control)
        .await
        .unwrap();

    // A real org identity with its own physically separate tenant DB, and a
    // caller who is a genuine LIVE member of it — so the refusal below is the
    // database boundary talking, never a membership or reachability failure.
    let org_id = "org_x41_promo_a";
    let org_account = seed_org_identity(&control, org_id, "promo-a", "SECRET_PROMO_A").await;
    seed_membership(&control, org_account, caller, "member").await;
    let tenant = new_tenant_pool(&base, ta, org_id).await;
    let org_dek = coralswarm_core::kek::generate_dek();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account)
        .bind(coralswarm_core::kek::wrap_dek(&kek, &org_dek).unwrap())
        .execute(&tenant)
        .await
        .unwrap();
    let org_graph = seed_graph(&tenant, org_account, "org", "Org Commons").await;

    let app = build_app(&control, [("SECRET_PROMO_A", with_db(&base, ta))]);
    let token = org_token(&caller.to_string(), org_id);
    // Transport only: this probe is about the same-DB promotion invariant, and
    // Phase 4 cutover step 3 retired `<slug>.api.<apex>`. The org token's claim
    // is what selects the tenant now.
    let host = APEX.to_string();

    // ── 1. Single-reef promote: source reef lives in the personal ocean on the
    //    consumer plane, target is this org's own commons. Refused. ──────────
    let resp = app
        .clone()
        .oneshot(req_post(
            &token,
            &host,
            &format!("/v1/nodes/{node}/promote"),
            &json!({ "target_graph_id": org_graph }),
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "an org-plane promote whose SOURCE is on the consumer plane must be refused, \
         not served by a cross-database copy"
    );
    // Pin WHICH 403 this is. The caller is a genuine live member of a real org
    // with a real target ocean, so a 403 arriving from a membership or
    // reachability gate would make this test pass for the wrong reason and
    // keep passing if the database boundary itself regressed.
    let body = body_json(resp).await;
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("cannot cross databases"),
        "the 403 must be the cross-database refusal itself, got {body:?}"
    );

    // ── 2. Same for the whole-source route, which took the same fork. ───────
    let resp = app
        .clone()
        .oneshot(req_post(
            &token,
            &host,
            &format!("/v1/graphs/{personal_graph}/promote-source"),
            &json!({ "source_id": "src-1", "target_graph_id": org_graph }),
        ))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "promote-source must refuse a consumer-plane source under an org context too"
    );

    // ── 3. Nothing crossed. The refusal is not a partial write. ─────────────
    let atoms_in_tenant: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
        .fetch_one(&tenant)
        .await
        .unwrap();
    let nodes_in_tenant: i64 = sqlx::query_scalar("SELECT count(*) FROM nodes")
        .fetch_one(&tenant)
        .await
        .unwrap();
    assert_eq!(
        (atoms_in_tenant, nodes_in_tenant),
        (0, 0),
        "a refused cross-database promotion must write ZERO rows into the tenant DB"
    );

    control.close().await;
    tenant.close().await;
    drop_all(conn, &[ctl, ta]).await;
    unsafe {
        std::env::remove_var("MASTER_KEK_HEX");
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}
