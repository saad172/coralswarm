//! Regression test for the 2026-08-05 prod incident: on the ORG/TENANT
//! plane, `POST /v1/teams` (create) and `GET /v1/graphs` (the workspace
//! list) both succeeded, but EVERY by-id team route
//! (`GET /v1/teams/{team_id}/members`, `GET /v1/teams/{team_id}/events`,
//! `PATCH /v1/teams/{team_id}`, …) 404'd for the very team just created, for
//! its own creator, who was also an org admin.
//!
//! Root cause: `GET /v1/graphs` (`routes/graphs.rs::graph_json`) exposes
//! only `graphs.id` for a team-kind ocean — never `teams.id` — so the
//! workspace-management web UI (every call site in
//! `web/components/workspaces/members/*.tsx`) only ever has `graph.id`
//! (== a team's `graph_id` column) to pass as the `{team_id}` path segment.
//! `teams.id` and `teams.graph_id` are two different uuids for the same row,
//! and every by-id handler in `routes/teams.rs` looked the path segment up
//! against `teams.id` only, via `Store::team_exists`/
//! `Store::lock_team_row_scoped` — so a `graph_id` never matched and every
//! one of these routes 404'd, unconditionally, for every caller, the moment
//! `ORG_TEAMS_ENABLED` first let an org actually reach this path (`create` +
//! `list` never touch that lookup at all, which is why only THOSE two
//! appeared to work).
//!
//! The fix (`Store::resolve_team_id`/`_scoped`, `routes::teams::
//! resolve_team_id!`) translates the path segment to the real `teams.id`
//! by matching EITHER `teams.id` OR `teams.graph_id`, as the very first
//! team-touching statement in every affected handler. This test drives the
//! real HTTP router end to end — binding gate → `TenancyDecision::Org` →
//! tenant pool → route handler — exactly reproducing the prod repro: create
//! a team, read the id the workspace list would actually expose
//! (`GET /v1/graphs`'s `id` field), and use THAT id for the roster/events/
//! rename calls, asserting 200 rather than the 404 CoralSwarm shipped.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB convention as every other `*_pg.rs`
//! integration test in this crate (see `route_migration_org_pg.rs`'s header
//! for the fixture this one is modeled on):
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test --test team_by_id_graph_compat_pg
//!
//! Migration 0083's `coralswarm_app` role is cluster-wide, so — same caveat
//! as `binding_gate_pg.rs` — running this concurrently with another
//! `*_pg.rs` binary that also applies `migrations_control` to a fresh
//! scratch DB can race a `CREATE ROLE`. CI pins `--test-threads=1` across the
//! pg suite for this reason; a plain local `cargo test` is not guaranteed
//! race-free against sibling binaries.

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";

/// Serializes this binary's env mutations (`CLERK_ISSUER`/`CLERK_JWKS_URL`/
/// `ORG_TEAMS_ENABLED`/`MEMBER_OCEANS_ENABLED`) — same pattern as
/// `route_migration_org_pg.rs`'s `ENV_LOCK`. This file has exactly one test,
/// so the lock is defensive rather than load-bearing today, but keeps the
/// convention consistent if a second test is ever added here.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

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

/// Same local-JWKS fixture `route_migration_org_pg.rs`/`mcp_pg_e2e.rs` use,
/// duplicated here for the same separate-compilation-unit reason those files
/// document.
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

#[tokio::test]
async fn team_by_id_routes_accept_the_graph_id_the_workspace_list_actually_exposes() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping team_by_id_routes_accept_the_graph_id_the_workspace_list_actually_exposes: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };

    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation is serialized process-wide by `_env_guard`, and
    // every var set here is removed before the guard is dropped.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
        // The exact prod flag combination from the incident report — set
        // TODAY for the first time in prod, which is why this org/tenant-
        // plane create-then-read path had never actually executed before.
        std::env::set_var("ORG_TEAMS_ENABLED", "1");
        std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
        // Fixed test KEK so `create_team`'s DEK wrap/unwrap round-trips
        // deterministically — same fixed value `dek_relocation_pg.rs` uses.
        std::env::set_var("MASTER_KEK_HEX", "ab".repeat(32));
    }

    let (ctl, tenant_db) = ("orgt_team_compat_ctl", "orgt_team_compat_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + a pre-seeded org account + routing row ───
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let dummy_owner: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'org-owner@walktest.example') RETURNING id",
    )
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, 'org_walktest') RETURNING id",
    )
    .bind(dummy_owner)
    .fetch_one(&control)
    .await
    .unwrap();
    financial_fixture::execute(
        &control,
        org_account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ('org_walktest', $1, 'walktest', 'SECRET_WALKTEST', 'active')",
        )
        .bind(org_account_id),
    )
    .await
    .unwrap();

    // ── Tenant DB: tenant tree + fingerprint. No content pre-seeded — the
    //    team ocean below is created entirely through the real API, exactly
    //    like the prod repro. ────────────────────────────────────────────────
    let tenant_pool: PgPool = PgPoolOptions::new()
        .connect(&with_db(&base, tenant_db))
        .await
        .unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_walktest')")
        .execute(&tenant_pool)
        .await
        .unwrap();

    // `create_team`'s org branch resolves the org's account DEK via
    // `account_keys.dek_wrapped` on the TENANT pool (normally provisioned by
    // `onboard_tenant.sh` step 7 / `routes::admin::rotate_dek`) — seed it
    // directly, same fixed-KEK pattern as `dek_relocation_pg.rs`.
    let kek = load_kek().expect("test KEK loads (MASTER_KEK_HEX set above)");
    let org_dek = generate_dek();
    let org_wrapped = wrap_dek(&kek, &org_dek).unwrap();
    sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
        .bind(org_account_id)
        .bind(&org_wrapped)
        .execute(&tenant_pool)
        .await
        .unwrap();

    // ── AppState + app: control pool as the ambient/control plane, real
    //    enabled `TenantRuntime` resolving 'SECRET_WALKTEST' to this test's
    //    tenant Postgres. `test_app_state_owned_with_db` supplies the base
    //    (route-file test convention); `state.store` is set on top of it the
    //    same way `route_migration_org_pg.rs` does, since the org-admin/
    //    control-plane reads in `routes/teams.rs` go through
    //    `state.control_store()`, not `state.db` alone. ───────────────────────
    let resolver = StaticMapResolver::from_pairs([("SECRET_WALKTEST", with_db(&base, tenant_db))]);
    let tenant_runtime = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let mut state = coralswarm_backend::test_app_state_owned_with_db(control.clone(), Some(tenant_runtime));
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // The caller is the team's own creator AND (per the prod repro) an org
    // admin — `org_role: "org:admin"` matches Clerk's shape; `is_org_admin`
    // resolves against `account_members`, which stays empty here, so this
    // test proves the fix on the PLAIN path (creator == active team admin
    // via `team_members`), the one every by-id handler hits first regardless
    // of org-admin status.
    let member_sub = format!("team_compat_creator_{}", Uuid::new_v4());
    let token = mint(json!({
        "iss": ISSUER,
        "sub": member_sub,
        "exp": future_exp(),
        "org_id": "org_walktest",
        "org_role": "org:admin",
    }));
    // Transport only. Claim-based tenancy Phase 4 cutover step 3 retired the
    // org API address `<slug>.api.<apex>` (410); the org token's `org_id`
    // claim selects the tenant on the apex, and nothing this test asserts is
    // about the Host.
    let org_host = APEX;

    macro_rules! req {
        ($method:expr, $uri:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap()
        };
        ($method:expr, $uri:expr, $json:expr) => {
            HttpRequest::builder()
                .method($method)
                .uri($uri)
                .header("host", org_host)
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&$json).unwrap()))
                .unwrap()
        };
    }

    // ── 1. POST /v1/teams — create, exactly like the prod repro. ────────────
    let resp = app
        .clone()
        .oneshot(req!("POST", "/v1/teams", json!({"name": "Walk Test Aug 5"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create_team must succeed under Org decision");
    let created = body_json(resp).await;
    let real_team_id: Uuid = created["team_id"].as_str().unwrap().parse().unwrap();
    let created_graph_id: Uuid = created["graph_id"].as_str().unwrap().parse().unwrap();
    assert_ne!(
        real_team_id, created_graph_id,
        "teams.id and the team's graph_id must be different uuids for this test to mean anything \
         (if they were ever equal by construction, the by-id 404 this test guards against could \
         never have happened)"
    );

    // ── 2. GET /v1/graphs — the workspace list. The ONLY id it exposes for
    //    this team ocean is `graphs.id` — never `teams.id` (routes/graphs.rs
    //    `graph_json`) — which is exactly what
    //    `web/components/workspaces/members/*.tsx` passes to every
    //    `/v1/teams/{team_id}/*` call. This is the id under test below, not
    //    `created["team_id"]`. ─────────────────────────────────────────────
    let resp = app.clone().oneshot(req!("GET", "/v1/graphs")).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "list_graphs must succeed under Org decision");
    let graphs = body_json(resp).await;
    let team_graph = graphs
        .as_array()
        .expect("bare array")
        .iter()
        .find(|g| g["kind"] == json!("team"))
        .expect("the just-created team ocean must be in the reachable list")
        .clone();
    let ui_id: Uuid = team_graph["id"].as_str().unwrap().parse().unwrap();
    assert_eq!(
        ui_id, created_graph_id,
        "GET /v1/graphs's exposed id for this team ocean must be its graph_id"
    );
    assert_ne!(
        ui_id, real_team_id,
        "the id the workspace list exposes must NOT be teams.id — confirming the UI genuinely has \
         no way to obtain the real team_id, which is the root cause under test"
    );

    // ── 3. GET /v1/teams/{ui_id}/members — THE regression. Before the fix,
    //    every by-id route 404'd on this exact id ("team not found"), for
    //    the team's own creator, immediately after creation. ────────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/teams/{ui_id}/members")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list_team_members must accept the graph_id the workspace list actually exposes, not 404 \
         (the 2026-08-05 prod incident)"
    );
    let roster = body_json(resp).await;
    let rows = roster.as_array().expect("bare array");
    assert_eq!(rows.len(), 1, "only the creator so far: {rows:?}");
    assert_eq!(rows[0]["role"], json!("admin"), "the creator is seeded as team admin");

    // ── 4. GET /v1/teams/{ui_id}/events — same regression, the audit trail. ─
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/teams/{ui_id}/events?page=1&page_size=8")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "list_team_events must accept the graph_id, not 404"
    );
    let events = body_json(resp).await;
    let events = events.as_array().expect("bare array");
    assert_eq!(events.len(), 1, "one 'created' audit row so far: {events:?}");
    assert_eq!(events[0]["action"], json!("created"));
    assert_eq!(events[0]["team_id"], json!(real_team_id), "the audit row is still keyed on the REAL team_id internally");

    // ── 5. PATCH /v1/teams/{ui_id} — rename, same regression, a WRITE path. ─
    let resp = app
        .clone()
        .oneshot(req!("PATCH", format!("/v1/teams/{ui_id}"), json!({"name": "Renamed via graph_id"})))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "rename_team must accept the graph_id, not 404");
    let renamed = body_json(resp).await;
    assert_eq!(renamed["changed"], json!(true));
    let stored_name: String = sqlx::query_scalar("SELECT name FROM teams WHERE id = $1")
        .bind(real_team_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert_eq!(stored_name, "Renamed via graph_id", "the rename must have landed on the real teams row");

    // ── 6. Backward compat: the REAL team_id must still resolve too — this
    //    fix only ever WIDENS what the path parameter accepts. ──────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/teams/{real_team_id}/members")))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the real teams.id must still resolve directly — this fix must not have narrowed anything"
    );

    // ── 7. A genuinely unknown id (neither a team's id nor its graph_id)
    //    must still 404, not be silently treated as "found". ────────────────
    let resp = app
        .clone()
        .oneshot(req!("GET", format!("/v1/teams/{}/members", Uuid::new_v4())))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::NOT_FOUND,
        "an id that matches neither teams.id nor teams.graph_id must still 404"
    );

    // Cleanup.
    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
        std::env::remove_var("ORG_TEAMS_ENABLED");
        std::env::remove_var("MEMBER_OCEANS_ENABLED");
        std::env::remove_var("MASTER_KEK_HEX");
    }
}
