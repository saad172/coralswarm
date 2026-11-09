//! Postgres integration tests for MCP tool CONTENT tenancy — originally
//! org-tenancy PR 2.3 (host-bound org subdomains); **rewritten for
//! claim-based-tenancy §Phase 3 PR 3.3**, which retires the org MCP resource
//! entirely (`McpConfig::resolve_resource`'s `Subdomain` arm now collapses to
//! `personal_identity` — see that function's doc).
//!
//! Before PR 3.3, this file proved an org-claim token pointed at
//! `<slug>.<apex>` bound the TENANT pool, with a full claim-vs-host gate
//! (routing-row existence/status, claim-matches-host). That entire premise is
//! now false:
//! [`org_host_is_retired_and_an_org_claim_binds_only_a_proven_membership`]
//! proves the NEW truth instead, in three parts, through the REAL
//! public HTTP router (real RS256 tokens against a local JWKS listener, real
//! `mcp_auth` → `resolve_mcp_tenancy` → rmcp dispatch), against real Postgres
//! control + tenant databases:
//!
//! **Part 1 — the (former) org subdomain is 410 Gone, full stop**, one layer
//! OUTSIDE `/mcp` entirely. This PR's own item 3 deletes
//! `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS` (claim-based tenancy Phase 4 cutover
//! step 3, #807) the moment `resolve_resource`'s `Subdomain` arm stops
//! producing `Org` tenancy, so `retired_org_host_guard` now 410s a
//! single-label subdomain host for `/mcp` too, before `/mcp`'s own auth or
//! host classification ever runs — regardless of the named org's routing-row
//! status (active, suspended, or nonexistent: the guard reads none of it),
//! and with zero tenant pools built and zero `mcp_tool_calls` audit rows
//! anywhere (the request never reaches dispatch at all).
//!
//! **Part 2 — an org claim on the apex is FENCED by live membership.** This
//! half was rewritten for #825: it used to assert that the apex "silently
//! downgrades" a legacy org claim (`claims_for_resource`'s strip, "a
//! DOWNGRADE and never an escalation"). That strip is deleted — #825 put
//! `user:org:read` back, so an `org_id` claim on `/mcp` is the organization
//! the user picked on Clerk's consent screen, not a pre-cutover artifact.
//! What stands in its place is the fence that makes honoring it safe: a claim
//! naming an org the caller is not a LIVE, non-revoked member of is 403'd
//! outright (`OrgMembershipPolicy::RequireExisting`), before any tenancy is
//! resolved, any tenant pool is built, or any tool dispatches.
//!
//! **Part 3 — an org claim the caller IS a live member of binds that tenant.**
//! Same token, same apex Host, one `account_members` row later: `list_oceans`
//! and `get_atom` resolve against the ORG's tenant pool, the caller's own
//! consumer-plane ocean is not what they see, and the `mcp_tool_calls` audit
//! rows land on the TENANT pool. This is the regression test for the bug #825
//! left half-fixed: the consent-screen choice reaching the decision instead of
//! being discarded.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB fixture as `route_migration_org_pg.rs`
//! / `mcp_governance_pg.rs`; a plain `cargo test` with no DB configured skips
//! every test in this file.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test mcp_tenant_pg -- --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

const APEX: &str = "coralswarm.example";
const TEST_MASTER_KEK_HEX: &str =
    "1111111111111111111111111111111111111111111111111111111111111111";

/// Serializes tests in this file against the process-global env vars `McpEnv`
/// sets — same rationale as `mcp_governance_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: route_migration_org_pg.rs) ──────────────────

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
    (Utc::now().timestamp() + 3600) as usize
}

// ── Env + JWKS fixture (pattern: mcp_governance_pg.rs) ───────────────────────

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

struct McpEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl McpEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation across this test binary is serialized by
        // `ENV_LOCK`, whose guard is held in `self._guard` for the lifetime of
        // this `McpEnv` (through `Drop` below). No other thread reads or writes
        // these vars concurrently, so the process-global mutation is sound.
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            std::env::remove_var("MCP_DEFAULT_SCOPES");
        }
        Self { _guard: guard }
    }
}

impl Drop for McpEnv {
    fn drop(&mut self) {
        // SAFETY: `self._guard` still holds `ENV_LOCK` here (it is dropped after
        // this body), so these removals are serialized against every other
        // test's env access — no concurrent reader/writer.
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
            std::env::remove_var("MASTER_KEK_HEX");
        }
    }
}

// ── Token minting ────────────────────────────────────────────────────────────

/// OAuth-shaped personal access token (client_id + jti, no sid, no org).
fn personal_token(sub: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_tenant_client", "jti": Uuid::new_v4().to_string(),
    }))
}

/// Same, with an active-org context — `/mcp`'s claim-only org selection.
fn org_token(sub: &str, org_id: &str) -> String {
    mint_oauth(json!({
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "client_id": "mcp_tenant_client", "jti": Uuid::new_v4().to_string(),
        "org_id": org_id, "org_role": "org:member",
    }))
}

// ── JSON-RPC plumbing (pattern: mcp_pg_e2e.rs / mcp_governance_pg.rs) ────────

fn parse_sse_json(bytes: &[u8]) -> Value {
    let text = String::from_utf8_lossy(bytes);
    let mut data_lines = Vec::new();
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data_lines.push(rest.strip_prefix(' ').unwrap_or(rest));
        }
    }
    let joined = data_lines.join("\n");
    serde_json::from_str(&joined)
        .unwrap_or_else(|e| panic!("failed to parse SSE JSON-RPC payload: {e}\nraw body: {text}"))
}

/// The Host that selects an org's plane: `<slug>.<apex>`. Since the URL — not the
/// token — is what selects tenancy at `/mcp`, an org-token call MUST be sent here,
/// exactly as a real client connected to that org's connector URL would.
fn org_host(slug: &str) -> String {
    format!("{slug}.{APEX}")
}

async fn mcp_rpc(
    app: &axum::Router,
    host: &str,
    token: &str,
    method: &str,
    params: Value,
) -> (StatusCode, Value) {
    let body = json!({ "jsonrpc": "2.0", "id": 1, "method": method, "params": params });
    let req = Request::builder()
        .method("POST")
        .uri("/mcp")
        // `tower::oneshot` synthesizes no Host, and `auth::host_guard` requires
        // one it serves — this is also what picks the tenancy plane.
        .header("host", host)
        .header("content-type", "application/json")
        .header("accept", "application/json, text/event-stream")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::from(body.to_string()))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let content_type = resp
        .headers()
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let bytes = to_bytes(resp.into_body(), 16 * 1024 * 1024).await.unwrap();
    let parsed = if content_type.contains("text/event-stream") {
        parse_sse_json(&bytes)
    } else if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, parsed)
}

/// `tools/call` → `(is_error, parsed tool body)`. Panics on transport-level
/// non-200 (auth rejections are asserted via `mcp_rpc` directly).
async fn call_tool(
    app: &axum::Router,
    host: &str,
    token: &str,
    name: &str,
    arguments: Value,
) -> (bool, Value) {
    let (status, rpc) = mcp_rpc(
        app,
        host,
        token,
        "tools/call",
        json!({ "name": name, "arguments": arguments }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "tools/call transport failure for {name}: {rpc:?}");
    let result = rpc
        .get("result")
        .unwrap_or_else(|| panic!("no result in tools/call response for {name}: {rpc:?}"));
    let is_error = result.get("isError").and_then(|v| v.as_bool()).unwrap_or(false);
    let content = result["content"][0]["text"]
        .as_str()
        .unwrap_or_else(|| panic!("tool result has no text content for {name}: {result:?}"));
    let body: Value = serde_json::from_str(content)
        .unwrap_or_else(|e| panic!("tool result text is not JSON for {name}: {e}\n{content}"));
    (is_error, body)
}

// `error_code` used to live here, for the one assertion that a tenant atom id
// was `not_found` from the apex once the org claim had been stripped. Since
// #825 there is no stripped-claim case in this file: the claim is either
// refused at the door (403, an HTTP-level assertion) or honored and the atom
// is FOUND. No tool-level error code is left to read.

// ── Fixture seeding (direct SQL — RequireExisting means no provisioning side
//    effects, so every row the auth path needs is seeded up front) ───────────

async fn seed_user(pool: &PgPool, sub: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
        .bind(format!("{sub}@users.clerk.local"))
        .bind(sub)
        .fetch_one(pool)
        .await
        .expect("seed user")
}

async fn seed_org_account(pool: &PgPool, owner: Uuid, clerk_org_id: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(clerk_org_id)
    .fetch_one(pool)
    .await
    .expect("seed org account")
}

/// A live `account_members` row — what `middleware::auth::live_org_membership`
/// looks for, and therefore the difference between Part 2's 403 and Part 3's
/// tenant binding.
async fn seed_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    // `seat_tier_id` is explicit: `account_members_default_seat_tier` (PR F,
    // seat lifecycle) RAISEs on any writer that omits it. Same shape
    // `mcp_tenant_selection_pg.rs`'s own `seed_membership` uses.
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(pool)
    .await
    .expect("seed account_members row");
}

async fn seed_routing_row(
    pool: &PgPool,
    clerk_org_id: &str,
    account_id: Uuid,
    slug: &str,
    secret_ref: &str,
    status: &str,
) {
    financial_fixture::execute(
        pool,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases(clerk_org_id, account_id, slug, db_secret_ref, status) \
         VALUES ($1, $2, $3, $4, $5)",
        )
        .bind(clerk_org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(status),
    )
    .await
    .expect("seed tenant_databases row");
}

/// Seed one graph + one searchable inline atom on `pool`, returning
/// `(graph_id, atom_id)`.
async fn seed_graph_with_atom(
    pool: &PgPool,
    account_id: Uuid,
    kind: &str,
    is_primary: bool,
    name: &str,
    content: &str,
) -> (Uuid, Uuid) {
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind, is_primary) VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .bind(is_primary)
    .fetch_one(pool)
    .await
    .expect("seed graph");
    let atom_id: Uuid = sqlx::query_scalar(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', $2, 'unused', $3, '{}', now()) RETURNING atom_id",
    )
    .bind(graph_id)
    .bind(format!("seed-{}", Uuid::new_v4()))
    .bind(content)
    .fetch_one(pool)
    .await
    .expect("seed atom");
    sqlx::query(
        "UPDATE atoms SET content_tsv = to_tsvector(fts_language::regconfig, content_inline) \
         WHERE atom_id = $1",
    )
    .bind(atom_id)
    .execute(pool)
    .await
    .expect("content_tsv backfill");
    (graph_id, atom_id)
}

/// Poll (bounded) an `mcp_tool_calls` count for `account_id` on `pool` —
/// audit inserts are fire-and-forget `tokio::spawn`s, so rows can land
/// slightly after the tool response returns.
async fn poll_audit_count(pool: &PgPool, account_id: Uuid, expect_at_least: i64) -> i64 {
    let mut count = 0;
    for _ in 0..100 {
        count = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(pool)
            .await
            .unwrap();
        if count >= expect_at_least {
            return count;
        }
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
    count
}

// ── The org HOST is retired; the org CLAIM is honored, once it is proven ───

/// **Renamed** from `org_mcp_host_is_retired_and_every_claim_downgrades_to_
/// personal` (itself a replacement for PR 2.3's
/// `org_mcp_tool_content_and_audit_land_on_the_tenant_pool_never_consumer`).
/// The host half of that name is still true and still asserted in Part 1; the
/// claim half stopped being true with #825, which is why the name had to move
/// too — see this file's module doc.
#[tokio::test]
async fn org_host_is_retired_and_an_org_claim_binds_only_a_proven_membership() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_host_is_retired_and_an_org_claim_binds_only_a_proven_membership: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = McpEnv::enable().await;

    let (ctl, ta) = ("orgt_mcp33_ctl", "orgt_mcp33_tenant_a");
    let mut conn = admin(&base).await;
    for db in [ctl, ta] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    // ── Control DB: control tree + identity/membership/routing ───────────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    // `sub_a`/`user_a` starts with NO `account_members` row on EITHER org
    // below — a plain personal user with zero live org memberships — and
    // Part 3 adds exactly one, which is the whole experiment.
    //
    // Before #825 this fixture was chosen for the opposite reason: to prove
    // `claims_for_resource`'s strip was UNCONDITIONAL (an `org_id` claim was
    // dropped whether or not the caller belonged to the org it named). With
    // the strip gone, the same fixture proves the fence that replaced it:
    // membership — not the claim, and not the routing row — is what decides
    // whether the claim is honored (Part 3) or refused (Part 2).
    // A HEALTHY (active) routing row for org_mcp33_a, and a deliberately
    // BROKEN (suspended) one for org_mcp33_susp — proves the downgrade below
    // holds regardless of routing-row health, because routing status is
    // never read at all on this path anymore.
    let sub_a = format!("mcp33_member_a_{}", Uuid::new_v4());
    let user_a = seed_user(&control, &sub_a).await;
    let org_a_account = seed_org_account(&control, user_a, "org_mcp33_a").await;
    seed_routing_row(&control, "org_mcp33_a", org_a_account, "acme-a", "SECRET_TA", "active").await;
    let org_susp = seed_org_account(&control, user_a, "org_mcp33_susp").await;
    seed_routing_row(&control, "org_mcp33_susp", org_susp, "susp-a", "SECRET_SUSP", "suspended").await;

    // ── Tenant DB: tenant tree + fingerprint + org graph + atom ──────────────
    let tenant_a: PgPool = PgPoolOptions::new().connect(&with_db(&base, ta)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_a).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_mcp33_a')")
        .execute(&tenant_a)
        .await
        .unwrap();
    let (graph_a, atom_a) = seed_graph_with_atom(
        &tenant_a,
        org_a_account,
        "org",
        false,
        "Acme org ocean",
        "the zebra budget target is forty two",
    )
    .await;

    // ── AppState: control as the ambient consumer plane + an ENABLED tenant
    //    runtime whose registry we keep a handle on (pool-build assertions).
    //    `SECRET_SUSP` deliberately maps to NO real database at all — if
    //    anything on this path ever tried to resolve/connect it, the test
    //    would fail loudly (a panic/connection error) rather than silently
    //    passing, which is a stronger guarantee than merely asserting
    //    `live_pool_count() == 0` on its own. ──────────────────────────────
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_TA",
        with_db(&base, ta),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())) as Arc<dyn Store>);
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let a_token = org_token(&sub_a, "org_mcp33_a");

    // ── Part 1: the (former) org-A subdomain is now 410 Gone, full stop. ─────
    //
    // As of this same PR's rebase onto `main`, `retired_org_host_guard`
    // (claim-based tenancy Phase 4 cutover step 3, #807) ALSO sits in front of
    // `/mcp`: `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`, the constant that used to
    // exempt `/mcp` and its three `/.well-known/oauth-*` documents from that
    // guard, is deleted by this same PR (item 3) the moment
    // `resolve_resource`'s `Subdomain` arm stops producing `Org` tenancy —
    // there is nothing left for the exemption to protect. So a single-label
    // host under the apex is 410'd OUTSIDE the whole app router, before
    // `/mcp`'s own auth or host classification ever runs — not merely
    // downgraded to personal by `/mcp`'s own internal logic. This is a
    // STRONGER retirement than "the claim is stripped": the request never
    // reaches routing at all, so there is nothing left to strip.
    let (status, body) = mcp_rpc(&app, &org_host("acme-a"), &a_token, "tools/list", json!({})).await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "the (former) org-A subdomain must be 410, before /mcp ever runs: {body:?}"
    );

    // The SUSPENDED org's (former) host behaves IDENTICALLY — the guard reads
    // no routing row at all, so a broken/suspended one is exactly as 410 as
    // a healthy one.
    let susp_token = org_token(&sub_a, "org_mcp33_susp");
    let (status, _) = mcp_rpc(&app, &org_host("susp-a"), &susp_token, "tools/list", json!({})).await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "a suspended org's (former) subdomain must be 410 too — the guard never reads routing \
         status, so a broken row and a healthy one are indistinguishable"
    );

    // A subdomain naming NO account/org/routing row whatsoever behaves the
    // same way — no oracle: a slug that never existed and one that did are
    // 410'd identically.
    let (status, _) =
        mcp_rpc(&app, &org_host("never-existed"), &personal_token(&sub_a), "tools/list", json!({})).await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "an unknown subdomain must be 410 exactly like a real one, or the guard is an \
         org-existence oracle"
    );

    // A Host we do not serve at all — not a subdomain of our apex — is
    // rejected differently: `retired_org_host_guard` only fires on
    // `HostTarget::Subdomain`, so a truly foreign host falls through to
    // `/mcp`'s OWN `host_guard`, which 403s it exactly as before. This proves
    // the two guards are doing different jobs, not that one subsumed the
    // other.
    let (status, body) = mcp_rpc(&app, "evil.example", &personal_token(&sub_a), "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "a foreign Host must still be rejected: {body:?}");
    assert_eq!(body["error"], "Forbidden: Host header is not allowed");

    // Zero tenant pools were EVER built, and NOTHING audited at all for the
    // four calls above — they never reached `/mcp`'s own middleware, let
    // alone its tool dispatch, so there is no `mcp_tool_calls` row for any of
    // them anywhere.
    assert_eq!(
        registry.live_pool_count(),
        0,
        "no request through a (retired) org host may ever build a tenant pool"
    );
    let any_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        any_audit, 0,
        "a 410'd request never reaches /mcp's own dispatch, so it must never audit anywhere: {any_audit}"
    );
    let tenant_a_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls")
        .fetch_one(&tenant_a)
        .await
        .unwrap();
    assert_eq!(tenant_a_audit, 0, "nor into a tenant pool");

    // ── Part 2 (#825): an org claim on the apex is FENCED by live membership.
    //    `sub_a` carries org_mcp33_a's claim but has NO `account_members` row
    //    for it, so `OrgMembershipPolicy::RequireExisting` refuses the request
    //    outright — before tenancy resolution, before any pool build, before
    //    any tool dispatch.
    //
    //    This arm REPLACES "the apex silently strips a legacy org claim".
    //    Honoring the claim (Part 3) is only safe because this refusal exists:
    //    naming an organization is not the same as belonging to it. ────────
    let (status, body) = mcp_rpc(
        &app,
        APEX,
        &a_token,
        "tools/call",
        json!({ "name": "list_oceans", "arguments": {} }),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an org claim the caller is not a live member of must be 403'd, never served: {body:?}"
    );
    assert_eq!(body["error"], json!("not a member of this organization"));

    // The suspended org behaves identically, and for the reason that matters:
    // membership — not routing status — is what this fence reads, and `sub_a`
    // has no membership row there either. `SECRET_SUSP` maps to no database at
    // all, so a request that got past this fence would fail loudly.
    let (status, _) = mcp_rpc(
        &app,
        APEX,
        &susp_token,
        "tools/call",
        json!({ "name": "list_oceans", "arguments": {} }),
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "same fence, regardless of routing status");

    assert_eq!(
        registry.live_pool_count(),
        0,
        "a refused org claim must never build a tenant pool"
    );
    let any_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        any_audit, 0,
        "a request refused inside mcp_auth never reaches dispatch, so it must never audit: \
         {any_audit}"
    );

    // ── Part 3 (#825): the SAME claim, once the caller is a live member,
    //    binds that organization's tenant plane — the regression test for the
    //    bug this PR fixes.
    //
    //    Before this PR, `claims_for_resource` stripped this claim on the
    //    personal resource, the decision stayed `TenancyDecision::Personal`,
    //    and the caller was handed a `tools/list` narrowed to
    //    `list_tenants`/`select_tenant` — asked to pick, in-band, the very
    //    organization Clerk's consent screen had already recorded in this
    //    token. Nothing about the request below changes except one
    //    `account_members` row. ──────────────────────────────────────────
    seed_membership(&control, org_a_account, user_a, "member").await;

    // The FULL tool roster, not the two tenancy tools: the catalogue gate
    // (`tool_catalog_restricted_to_tenancy_tools`) only fires on a `Personal`
    // decision, and this request now resolves to `Org`.
    let (status, listed) = mcp_rpc(&app, APEX, &a_token, "tools/list", json!({})).await;
    assert_eq!(status, StatusCode::OK, "tools/list must succeed: {listed:?}");
    let names: Vec<String> = listed["result"]["tools"]
        .as_array()
        .expect("tools array")
        .iter()
        .map(|t| t["name"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        names.len() > 2,
        "a consent-bound org caller must see the FULL roster, not just the tenancy tools: \
         {names:?}"
    );
    assert!(
        names.iter().any(|n| n == "list_oceans"),
        "list_oceans must be in the catalogue: {names:?}"
    );

    let (is_error, body) = call_tool(&app, APEX, &a_token, "list_oceans", json!({})).await;
    assert!(!is_error, "list_oceans must succeed for a consent-bound org caller: {body:?}");
    let oceans = body["oceans"].as_array().expect("oceans array");
    assert!(
        oceans.iter().any(|o| o["id"] == json!(graph_a.to_string())),
        "the consent-screen organization's OWN tenant ocean must be what this caller sees: \
         {body:?}"
    );

    // And its content is reachable — the atom lives only in tenant A's
    // database, so reading it proves the tenant pool, not the consumer plane,
    // served this call.
    let (is_error, body) =
        call_tool(&app, APEX, &a_token, "get_atom", json!({"atom_id": atom_a})).await;
    assert!(!is_error, "a tenant-A atom must be reachable once its org is bound: {body:?}");

    assert_eq!(
        registry.live_pool_count(),
        1,
        "exactly one tenant pool — org A's — must have been built"
    );
    let tenant_audit = poll_audit_count(&tenant_a, org_a_account, 2).await;
    assert!(
        tenant_audit >= 2,
        "an org-bound call's audit rows must land on the TENANT pool: {tenant_audit}"
    );
    let consumer_audit: i64 = sqlx::query_scalar("SELECT count(*) FROM mcp_tool_calls")
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        consumer_audit, 0,
        "and never on the consumer/control plane: {consumer_audit}"
    );

    // Cleanup.
    control.close().await;
    tenant_a.close().await;
    for db in [ctl, ta] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
