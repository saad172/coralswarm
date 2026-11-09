//! Group-B security probe (PR 0 of the member-private-oceans plan,
//! `docs/plans/member-private-oceans.md` §1.4(c)/§7): proves the eight
//! account-scoped-but-not-reachability-scoped paths the plan calls out are
//! actually closed, through the real gate → extractor → route → resolver
//! chain (not a direct resolver/Store call) — same conventions as
//! `cross_tenant_probe_pg.rs` (real scratch DBs, real HTTP, real
//! `coralswarm_app`-adjacent stack), whose fixture helpers this file
//! duplicates per that file's own documented "no shared test-support module
//! across files" convention.
//!
//! ## What each test proves
//!
//! | Test | Item(s) | Surface |
//! |---|---|---|
//! | `group_b_team_ocean_reads_deny_a_non_member` | 1, 2, 3 | `GET /v1/graphs/{id}`, `/atoms`, `/sources` |
//! | `group_b_sync_pull_and_push_deny_a_non_member` | 4, 5 | `GET /v1/sync/pull`, `POST /v1/sync/push` |
//! | `group_b_sync_push_never_auto_provisions_under_org_account` | 5 | `POST /v1/sync/push`, unseen `graph_id` |
//! | `group_b_query_history_and_activity_are_per_user` | 6, 7 | `GET /v1/queries`, `GET /v1/activity` |
//!
//! Item 8 (`resolve_primary_graph` hard-fails on an org account) is proven in
//! an in-crate test instead — see `backend/src/routes/slack.rs`'s
//! `resolve_primary_graph_refuses_to_auto_create_under_an_org_account_pg`
//! (`resolve_primary_graph` is `pub(crate)`, unreachable from this external
//! test crate, and every route that would otherwise exercise it over HTTP
//! already 403s under an org `TenancyDecision` before reaching it — see that
//! route's own `reject_org_*` gate, which is exactly why this file cannot
//! reach it through the router either).
//!
//! Gated on `ORGT_TEST_BASE_URL`, like every other `*_pg.rs` file here:
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test group_b_probe_pg -- --test-threads=1

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
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
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";

/// Serializes this file's process-global env mutations against each other —
/// same pattern as `cross_tenant_probe_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

// ── Scratch-DB plumbing (duplicated from `cross_tenant_probe_pg.rs` per its
//    own documented convention) ───────────────────────────────────────────

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

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

async fn body_text(resp: axum::response::Response) -> String {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    String::from_utf8_lossy(&bytes).into_owned()
}

// ── Identity/routing/content seeding (duplicated from `cross_tenant_probe_pg.rs`) ──

async fn seed_org_identity(control: &PgPool, clerk_org_id: &str, slug: &str, secret_ref: &str) -> Uuid {
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
    financial_fixture::execute(
        control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, $3, $4, 'active')",
        )
        .bind(clerk_org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref),
    )
    .await
    .unwrap();
    account_id
}

async fn seed_user(control: &PgPool, sub: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
        .bind(format!("{sub}@users.clerk.local"))
        .fetch_one(control)
        .await
        .unwrap()
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
    sqlx::query_scalar("INSERT INTO graphs(account_id, name, kind) VALUES ($1, $2, $3) RETURNING id")
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

fn build_app(control: &PgPool, resolvers: impl IntoIterator<Item = (&'static str, String)>) -> axum::Router {
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

fn req_post_json(token: &str, host: &str, uri: &str, json: &Value) -> HttpRequest<Body> {
    HttpRequest::builder()
        .method("POST")
        .uri(uri)
        .header("host", host)
        .header("authorization", format!("Bearer {token}"))
        .header("content-type", "application/json")
        .body(Body::from(serde_json::to_vec(json).unwrap()))
        .unwrap()
}

/// One shared fixture for every test below: an org with a non-team org ocean
/// and a `kind='team'` ocean whose only member is `owner_user` — `member_user`
/// is a full org member but NOT on the team. Mirrors
/// `cross_tenant_probe_pg.rs::team_ocean_gating_enforced_through_the_real_http_stack`'s
/// fixture shape.
struct GroupBFixture {
    _databases: Vec<coralswarm_backend::test_db::TestDatabase>,
    control: PgPool,
    tenant: PgPool,
    org_account: Uuid,
    owner_user: Uuid,
    member_user: Uuid,
    org_ocean: Uuid,
    team_ocean: Uuid,
    owner_token: String,
    member_token: String,
    host: String,
    app: axum::Router,
}

async fn setup_fixture(base: &str, tag: &str) -> (GroupBFixture, PgConnection, Vec<String>) {
    let ctl = format!("orgt_gb_{tag}_ctl");
    let ta = format!("orgt_gb_{tag}_ta");
    let databases = [&ctl, &ta].iter().map(|name| coralswarm_backend::test_db::TestDatabase::adopt_from_url(base, name)).collect();
    let conn = recreate_all(base, &[&ctl, &ta]).await;

    let control = new_control_pool(base, &ctl).await;
    let clerk_org_id = format!("org_gb_{tag}");
    // Leaked to `&'static str`: `build_app`'s resolver map takes a static key
    // (mirrors `cross_tenant_probe_pg.rs`'s literal `"SECRET_A"` keys), and
    // this fixture generates a fresh, per-test-invocation secret name instead
    // of a fixed literal — acceptable, test-only leak.
    let secret: &'static str = Box::leak(format!("SECRET_GB_{tag}").into_boxed_str());
    let org_account = seed_org_identity(&control, &clerk_org_id, &format!("gb-{tag}"), secret).await;

    let owner_sub = format!("gb_owner_{tag}_{}", Uuid::new_v4());
    let member_sub = format!("gb_member_{tag}_{}", Uuid::new_v4());
    let owner_user = seed_user(&control, &owner_sub).await;
    let member_user = seed_user(&control, &member_sub).await;
    seed_membership(&control, org_account, owner_user, "owner").await;
    seed_membership(&control, org_account, member_user, "member").await;

    let tenant = new_tenant_pool(base, &ta, &clerk_org_id).await;
    let org_ocean = seed_graph(&tenant, org_account, "org", "Acme Org Ocean").await;
    let team_ocean = seed_graph(&tenant, org_account, "team", "Leadership Team Ocean").await;
    let team = new_team(&tenant, org_account, team_ocean, owner_user).await;
    add_team_member(&tenant, team, owner_user).await;
    // `member_user` is deliberately NOT added to the team.

    let app = build_app(&control, [(secret, with_db(base, &ta))]);
    let owner_token = org_token(&owner_sub, &clerk_org_id);
    let member_token = org_token(&member_sub, &clerk_org_id);
    // Claim-based tenancy Phase 4 cutover step 3 retired `<slug>.api.<apex>`
    // (410). Every token in this file is an org token, and the apex binds the
    // tenant the CLAIM names, so the door moves and nothing this file asserts
    // does: "a non-member of team X cannot read team X's ocean" is a
    // reachability property, never a Host property. The slug (`gb-{tag}`) is
    // still seeded in `tenant_databases` because `resolve_routing_by_org` reads
    // the same row — it is just no longer reachable as a hostname.
    let host = APEX.to_string();

    let fixture = GroupBFixture {
        _databases: databases,
        control,
        tenant,
        org_account,
        owner_user,
        member_user,
        org_ocean,
        team_ocean,
        owner_token,
        member_token,
        host,
        app,
    };
    (fixture, conn, vec![ctl, ta])
}

async fn teardown(f: GroupBFixture, conn: PgConnection, dbs: Vec<String>) {
    f.control.close().await;
    f.tenant.close().await;
    let db_refs: Vec<&str> = dbs.iter().map(|s| s.as_str()).collect();
    drop_all(conn, &db_refs).await;
}

// ═══════════════════════════════════════════════════════════════════════════
// Items 1-3: GET /v1/graphs/{id}, /atoms, /sources deny a non-team-member
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn group_b_team_ocean_reads_deny_a_non_member() {
    let Some(base) = base_url() else {
        eprintln!("skipping group_b_team_ocean_reads_deny_a_non_member: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (f, conn, dbs) = setup_fixture(&base, "reads").await;
    let secret_content = "quarterly layoffs shortlist — leadership eyes only";
    let atom_id = seed_inline_atom(&f.tenant, f.team_ocean, secret_content).await;

    // ── Item 1: GET /v1/graphs/{team_ocean} ─────────────────────────────────
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, &format!("/v1/graphs/{}", f.team_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "non-team-member get_graph must 404");
    let text = body_text(resp).await;
    assert!(
        !text.contains("Leadership Team Ocean"),
        "the team ocean's name must never appear in a denied response: {text}"
    );

    // ── Item 2: GET /v1/graphs/{team_ocean}/atoms ───────────────────────────
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, &format!("/v1/graphs/{}/atoms", f.team_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "non-team-member list_atoms must 404");
    let text = body_text(resp).await;
    assert!(
        !text.contains(secret_content) && !text.contains(&atom_id.to_string()),
        "the team ocean's coral content must never leak into a denied list_atoms response: {text}"
    );

    // ── Item 3: GET /v1/graphs/{team_ocean}/sources ─────────────────────────
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, &format!("/v1/graphs/{}/sources", f.team_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND, "non-team-member list_sources must 404");
    let text = body_text(resp).await;
    assert!(
        !text.contains(secret_content),
        "the team ocean's corpus must never leak into a denied list_sources response: {text}"
    );

    // ── Positive controls: the fix must not be overly restrictive ──────────
    // The team's own member reads the team ocean fine.
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.owner_token, &f.host, &format!("/v1/graphs/{}", f.team_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "the team's own member must still reach the team ocean");
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.owner_token, &f.host, &format!("/v1/graphs/{}/atoms", f.team_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let text = body_text(resp).await;
    assert!(
        text.contains(secret_content),
        "the team's own member must still see the team ocean's coral content: {text}"
    );
    // The non-member still reads the ordinary (non-team) org ocean fine —
    // this fix must not narrow account-wide oceans.
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, &format!("/v1/graphs/{}", f.org_ocean)))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "a non-team-ocean read must be unaffected by this fix");

    teardown(f, conn, dbs).await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Items 4-5: GET /v1/sync/pull and POST /v1/sync/push deny a non-team-member
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn group_b_sync_pull_and_push_deny_a_non_member() {
    let Some(base) = base_url() else {
        eprintln!("skipping group_b_sync_pull_and_push_deny_a_non_member: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (f, conn, dbs) = setup_fixture(&base, "sync").await;

    // Seed one op_log row against the team ocean directly (as if the team's
    // own device had already pushed it) so item 4 has something real to
    // exclude.
    let secret_marker = "team-ocean-secret-marker-9f3a";
    let team_op_id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO op_log (op_id, entity, entity_id, op_type, payload, version, updated_at, \
         device_id, graph_id) \
         VALUES ($1, 'atom', $2, 'upsert', $3, 0, now(), 'owner-device', $4)",
    )
    .bind(team_op_id)
    .bind(Uuid::new_v4().to_string())
    .bind(json!({ "marker": secret_marker }))
    .bind(f.team_ocean)
    .execute(&f.tenant)
    .await
    .unwrap();

    // ── Item 4: GET /v1/sync/pull must never return the team ocean's op ────
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, "/v1/sync/pull?after=0&limit=200"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "sync pull itself must succeed");
    let text = body_text(resp).await;
    assert!(
        !text.contains(secret_marker) && !text.contains(&team_op_id.to_string()),
        "sync pull must never return an op for a team ocean the caller isn't in: {text}"
    );

    // Positive control: the team's own member DOES pull it.
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.owner_token, &f.host, "/v1/sync/pull?after=0&limit=200"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ops = body["ops"].as_array().expect("ops array");
    assert!(
        ops.iter().any(|o| o["op_id"].as_str() == Some(&team_op_id.to_string())),
        "the team's own member must still pull the team ocean's op: {body:?}"
    );

    // ── Item 5: POST /v1/sync/push must refuse a write naming the team ocean,
    //    and NOTHING must land. ──────────────────────────────────────────────
    let forged_op_id = Uuid::new_v4();
    let forged_entity_id = Uuid::new_v4();
    let push_body = json!({
        "ops": [{
            "seq": 0,
            "op_id": forged_op_id,
            "entity": "atom",
            "entity_id": forged_entity_id.to_string(),
            "op_type": "upsert",
            "payload": {
                "atom_id": forged_entity_id,
                "graph_id": f.team_ocean,
                "source_type": "note",
                "source_id": format!("forged-{forged_entity_id}"),
                "content_blob_key": "unused",
                "content_inline": "forged-write-attempt",
                "timestamp": Utc::now().to_rfc3339(),
            },
            "version": 0,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "attacker-device",
            "graph_id": f.team_ocean,
        }]
    });
    let resp = f
        .app
        .clone()
        .oneshot(req_post_json(&f.member_token, &f.host, "/v1/sync/push", &push_body))
        .await
        .unwrap();
    // Asserting the SPECIFIC status, not merely `!= OK`. A bare `assert_ne!`
    // here is satisfied by 429 (rate limiter), 503 (missing store), or 500
    // (pool timeout / reachability error) -- none of which prove the team gate
    // is what refused the write. If a regression turned this 403 into a 500 the
    // weaker assertion would stay green while the security property was gone.
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "sync push naming an unreachable team ocean must be refused by the team gate (403), \
         not by an incidental failure"
    );

    let landed: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE op_id = $1")
        .bind(forged_op_id)
        .fetch_one(&f.tenant)
        .await
        .unwrap();
    assert_eq!(landed, 0, "the refused push must not land in op_log");
    let landed_atom: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE atom_id = $1")
        .bind(forged_entity_id)
        .fetch_one(&f.tenant)
        .await
        .unwrap();
    assert_eq!(landed_atom, 0, "the refused push must not create the atom row either");

    // Positive control: the team's own member CAN push into the team ocean.
    let legit_op_id = Uuid::new_v4();
    let legit_entity_id = Uuid::new_v4();
    let legit_push = json!({
        "ops": [{
            "seq": 0,
            "op_id": legit_op_id,
            "entity": "atom",
            "entity_id": legit_entity_id.to_string(),
            "op_type": "upsert",
            "payload": {
                "atom_id": legit_entity_id,
                "graph_id": f.team_ocean,
                "source_type": "note",
                "source_id": format!("legit-{legit_entity_id}"),
                "content_blob_key": "unused",
                "content_inline": "legit-write",
                "timestamp": Utc::now().to_rfc3339(),
            },
            "version": 0,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "owner-device-2",
            "graph_id": f.team_ocean,
        }]
    });
    let resp = f
        .app
        .clone()
        .oneshot(req_post_json(&f.owner_token, &f.host, "/v1/sync/push", &legit_push))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "the team's own member must still be able to push into it");

    teardown(f, conn, dbs).await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Item 5 (second half): sync push never auto-provisions a `kind='personal'`
// ocean under an org account for an unseen graph_id.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn group_b_sync_push_never_auto_provisions_under_org_account() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping group_b_sync_push_never_auto_provisions_under_org_account: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (f, conn, dbs) = setup_fixture(&base, "autoprov").await;

    let unseen_graph_id = Uuid::new_v4();
    let op_id = Uuid::new_v4();
    let entity_id = Uuid::new_v4();
    let push_body = json!({
        "ops": [{
            "seq": 0,
            "op_id": op_id,
            "entity": "atom",
            "entity_id": entity_id.to_string(),
            "op_type": "upsert",
            "payload": {"marker": "should-never-land"},
            "version": 0,
            "updated_at": Utc::now().to_rfc3339(),
            "device_id": "desktop-first-contact",
            "graph_id": unseen_graph_id,
        }]
    });
    let resp = f
        .app
        .clone()
        .oneshot(req_post_json(&f.member_token, &f.host, "/v1/sync/push", &push_body))
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "an unseen graph_id under an org account must be refused, never auto-provisioned"
    );

    let created: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE id = $1")
        .bind(unseen_graph_id)
        .fetch_one(&f.tenant)
        .await
        .unwrap();
    assert_eq!(
        created, 0,
        "no graph row — personal or otherwise — may be auto-created under an org account by sync push"
    );
    let landed: i64 = sqlx::query_scalar("SELECT count(*) FROM op_log WHERE op_id = $1")
        .bind(op_id)
        .fetch_one(&f.tenant)
        .await
        .unwrap();
    assert_eq!(landed, 0, "the op itself must not land either");

    teardown(f, conn, dbs).await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Items 6-7: GET /v1/queries and GET /v1/activity are per-user, not
// account-wide — the founder-approved behavior change.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn group_b_query_history_and_activity_are_per_user() {
    let Some(base) = base_url() else {
        eprintln!("skipping group_b_query_history_and_activity_are_per_user: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = ENV_LOCK.lock().await;
    let jwks_url = spawn_local_jwks().await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::set_var("CLERK_ISSUER", ISSUER);
        std::env::set_var("CLERK_JWKS_URL", &jwks_url);
    }

    let (f, conn, dbs) = setup_fixture(&base, "peruser").await;

    // ── query_history: one row per user, same account/graph ────────────────
    let owner_qh: Uuid = sqlx::query_scalar(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text, answer_summary) \
         VALUES ($1, $2, $3, 'what is the layoff plan?', 'owner-only answer summary') RETURNING id",
    )
    .bind(f.org_account)
    .bind(f.org_ocean)
    .bind(f.owner_user)
    .fetch_one(&f.tenant)
    .await
    .unwrap();
    let member_qh: Uuid = sqlx::query_scalar(
        "INSERT INTO query_history (account_id, graph_id, user_id, query_text, answer_summary) \
         VALUES ($1, $2, $3, 'what is my PTO balance?', 'member-only answer summary') RETURNING id",
    )
    .bind(f.org_account)
    .bind(f.org_ocean)
    .bind(f.member_user)
    .fetch_one(&f.tenant)
    .await
    .unwrap();

    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, "/v1/queries"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids: Vec<String> = body["items"]
        .as_array()
        .expect("items array")
        .iter()
        .filter_map(|i| i["id"].as_str().map(str::to_string))
        .collect();
    assert!(ids.contains(&member_qh.to_string()), "member must see their own query history row");
    assert!(
        !ids.contains(&owner_qh.to_string()),
        "member must NOT see the owner's query history row: {ids:?}"
    );
    let text = serde_json::to_string(&body).unwrap();
    assert!(
        !text.contains("owner-only answer summary"),
        "the owner's answer summary text must never leak into the member's response: {text}"
    );

    // Reciprocal.
    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.owner_token, &f.host, "/v1/queries"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids: Vec<String> = body["items"]
        .as_array()
        .expect("items array")
        .iter()
        .filter_map(|i| i["id"].as_str().map(str::to_string))
        .collect();
    assert!(ids.contains(&owner_qh.to_string()));
    assert!(!ids.contains(&member_qh.to_string()), "owner must NOT see the member's query history row");

    // ── activity_events: one row per user, same account/graph ──────────────
    let owner_ev: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, graph_id, event_type, payload) \
         VALUES ($1, $2, $3, 'atom_created', $4) RETURNING id",
    )
    .bind(f.org_account)
    .bind(f.owner_user)
    .bind(f.org_ocean)
    .bind(json!({"marker": "owner-only-activity"}))
    .fetch_one(&f.tenant)
    .await
    .unwrap();
    let member_ev: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, graph_id, event_type, payload) \
         VALUES ($1, $2, $3, 'atom_created', $4) RETURNING id",
    )
    .bind(f.org_account)
    .bind(f.member_user)
    .bind(f.org_ocean)
    .bind(json!({"marker": "member-only-activity"}))
    .fetch_one(&f.tenant)
    .await
    .unwrap();
    // A NULL-user (system/orphaned) row must stay visible to EVERY member —
    // it is not a live member's private action (see `Store::list_activity`'s
    // doc comment).
    let system_ev: Uuid = sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, user_id, graph_id, event_type, payload) \
         VALUES ($1, NULL, $2, 'atom_created', $3) RETURNING id",
    )
    .bind(f.org_account)
    .bind(f.org_ocean)
    .bind(json!({"marker": "system-activity"}))
    .fetch_one(&f.tenant)
    .await
    .unwrap();

    let resp = f
        .app
        .clone()
        .oneshot(req_get(&f.member_token, &f.host, "/v1/activity"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_json(resp).await;
    let ids: Vec<String> = body
        .as_array()
        .expect("activity is a bare array")
        .iter()
        .filter_map(|e| e["id"].as_str().map(str::to_string))
        .collect();
    assert!(ids.contains(&member_ev.to_string()), "member must see their own activity row");
    assert!(ids.contains(&system_ev.to_string()), "member must see the NULL-user system row");
    assert!(
        !ids.contains(&owner_ev.to_string()),
        "member must NOT see the owner's activity row: {ids:?}"
    );

    teardown(f, conn, dbs).await;
    // SAFETY: env mutation serialized process-wide by _env_guard (ENV_LOCK is held for this test body), so no other thread reads or writes these vars concurrently.
    unsafe {
        std::env::remove_var("CLERK_ISSUER");
        std::env::remove_var("CLERK_JWKS_URL");
    }
}
