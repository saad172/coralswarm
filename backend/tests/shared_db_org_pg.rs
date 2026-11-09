//! Postgres integration tests for org-tenancy PR 4.3: the "shared-DB org"
//! decision — an org that carries a valid Clerk `org_id` claim (and, in the
//! grandfathered case modeled here, a pre-existing control `accounts` row
//! WITH real content still living on the shared/consumer plane from before
//! the tenancy split) but has NO `tenant_databases` routing row.
//!
//! The decision (see `tenancy::gate`'s module doc + `mcp::auth::
//! resolve_mcp_tenancy`'s doc comment, both updated by this PR): **fail
//! closed, on every surface, never a silent fallback to the shared consumer
//! plane for org content.** This file proves that decision holds across all
//! three code paths an org claim with no routing row can reach, and that the
//! three surfaces agree on the SAME rejection message where they overlap
//! (REST apex and `/mcp` both resolve tenancy from the bare claim, with no
//! Host-based subdomain to consult):
//!
//! (a) an org subdomain host (no routing row for ANY slug this org could use)
//!     → 404, fail-closed before auth (org-tenancy PR 1.2's existing "unknown
//!     subdomain" gate — asserted here specifically for a grandfathered org
//!     that DOES have a real `accounts` row, not just a stray claim);
//! (b) the apex host through `require_auth` (a REST route) → 403
//!     "organization is not provisioned for service" — org-tenancy PR 4.3's
//!     new routing-by-`clerk_org_id` lookup in `tenancy::gate::decide`'s Apex
//!     arm (previously this branch ignored routing state entirely and always
//!     said "use your organization's subdomain", which is actively wrong
//!     advice for an org that has no subdomain to go to);
//! (c) `/mcp` on the retired org SUBDOMAIN → 410 Gone, before `/mcp`'s own
//!     tenancy resolution runs at all (claim-based tenancy Phase 4 cutover
//!     step 3, #807/#810 — this arm used to be the 403 below, and moved when
//!     the org API address was retired);
//! (c2) `/mcp` on the APEX, i.e. from the bare claim → the SAME 403 message
//!     as (b) — asserted here to prove (b) and (c2) literally agree.
//!
//!     (c2) read 200 for a stretch, because `/mcp`'s personal resource used
//!     to STRIP an `org_id` claim and serve the caller's own plane. #869
//!     (`968f1f08`) deleted that strip once personal organizations gave the
//!     consent screen a personal row, so the claim now goes through
//!     `tenant_selection::resolve_request_tenancy` -> `tenancy::gate::decide`
//!     like any other and lands on this same fail-closed 403. The assertion
//!     was updated to match on 2026-09-10, with the founder's explicit
//!     sign-off; see the long note on the (c2) block itself for the evidence
//!     that 403 is intended rather than a regression.
//!
//! Every path is also checked for the actual leak this PR exists to rule out:
//! the org's shared-plane graph/atom (seeded directly into the control DB,
//! standing in for content a real grandfathered org would still have there)
//! is never returned, and no tenant pool is ever built (`live_pool_count`
//! stays 0 throughout — there is nowhere for one to be built FOR, since no
//! routing row exists).
//!
//! Two further tests (security review P2-1 on PR #282) exercise the REAL
//! `resolve_routing_by_org → routing_state → decide` wiring for a PRESENT
//! routing row on the apex — the first test above only ever drives the
//! `Missing` branch, which the pure `gate::decide` unit tests already cover
//! for every `RoutingState`, but doesn't prove the wiring reads a real row
//! correctly: `org_with_active_routing_row_still_refused_on_apex_never_served`
//! seeds a REAL tenant database with distinctive content behind an `active`
//! row and proves it never reaches the apex (a wiring regression that let an
//! active row resolve to `GateOutcome::Org` would leak that exact content);
//! `org_with_provisioning_routing_row_apex_returns_503_never_served` does the
//! same for `provisioning` (503, not served, not permanently rejected).
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB fixture pattern as `binding_gate_pg.rs`
//! / `route_migration_org_pg.rs` / `mcp_tenant_pg.rs`.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test shared_db_org_pg -- --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use axum::body::{to_bytes, Body};
use axum::http::{Request as HttpRequest, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, mint_oauth, test_jwks, ISSUER};
use coralswarm_backend::store::PostgresStore;
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

/// Serializes this file's process-global `CLERK_*`/`MCP_*` env mutations —
/// same rationale as `mcp_tenant_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

const APEX: &str = "coralswarm.example";

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

/// Env fixture: enables both `require_auth` (Clerk verification) AND `/mcp`
/// (so a single `test_support_app` router drives all three paths this file
/// tests) — the union of `route_migration_org_pg.rs`'s and
/// `mcp_tenant_pg.rs`'s env needs.
struct Env {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl Env {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: serialized by `ENV_LOCK`, held for this fixture's lifetime.
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", &jwks_url);
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("MCP_RESOURCE_URL", "http://localhost/mcp");
            std::env::set_var("RATE_LIMIT_BACKEND", "pg");
            std::env::remove_var("MCP_DEFAULT_SCOPES");
            std::env::remove_var("ORG_SELFSERVE_DISABLED");
        }
        Self { _guard: guard }
    }
}

impl Drop for Env {
    fn drop(&mut self) {
        // SAFETY: guard still held (dropped after this body runs).
        unsafe {
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("RATE_LIMIT_BACKEND");
        }
    }
}

async fn body_json(resp: axum::response::Response) -> Value {
    let bytes = to_bytes(resp.into_body(), usize::MAX).await.unwrap();
    serde_json::from_slice(&bytes).unwrap_or(Value::Null)
}

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
        .unwrap_or_else(|e| panic!("failed to parse SSE JSON-RPC payload: {e}\nraw: {text}"))
}

/// Drive one `tools/list` call at `/mcp` and return `(status, error-shaped body)`.
/// `mcp_auth`'s rejections use the SAME `{"error": ...}` shape `require_auth`'s
/// binding gate does (`forbidden()`/`service_unavailable()` in both modules),
/// so this can be compared directly against the REST responses below.
/// `tools/call list_oceans` — the strongest available "what content can this
/// caller actually reach" probe. Used here as the non-vacuity control for
/// (c2)'s refusal: the same user's PERSONAL token must still be served, so the
/// 403 is attributable to the org claim and not to a broken fixture. (Before
/// #869 deleted `mcp::auth::claims_for_resource` this probe served a different
/// purpose — proving a STRIPPED-claim caller saw only their own plane. There
/// is no stripped caller any more.)
async fn mcp_list_oceans(app: &axum::Router, token: &str, host: &str) -> (StatusCode, Value) {
    mcp_rpc_raw(
        app,
        token,
        host,
        json!({
            "jsonrpc": "2.0", "id": 1, "method": "tools/call",
            "params": {"name": "list_oceans", "arguments": {}}
        }),
    )
    .await
}

async fn mcp_tools_list(app: &axum::Router, token: &str, host: &str) -> (StatusCode, Value) {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}});
    mcp_rpc_raw(app, token, host, body).await
}

/// POST a JSON-RPC body to the real `/mcp` router under `host` — which is what
/// selects the tenancy plane — returning `(status, parsed envelope)` and handling
/// both the plain-JSON and SSE response shapes.
async fn mcp_rpc_raw(
    app: &axum::Router,
    token: &str,
    host: &str,
    body: Value,
) -> (StatusCode, Value) {
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

#[tokio::test]
async fn org_claim_with_no_routing_row_fails_closed_on_every_surface_and_never_leaks_shared_plane_content(
) {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_claim_with_no_routing_row_fails_closed_on_every_surface_and_never_leaks_shared_plane_content: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = Env::enable().await;

    let ctl = "orgt_sharedorg_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    // ── Control DB: control tree + a GRANDFATHERED org — a real `accounts`
    //    row (as it would have been created pre-split, or by a stray webhook/
    //    JWT before onboarding), a live member, and REAL content still on the
    //    shared/consumer plane — but deliberately NO `tenant_databases` row. ──
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    const LEGACY_ORG: &str = "org_legacy_shared_db";
    let sub = format!("shared_org_member_{}", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
    )
    .bind(format!("{sub}@users.clerk.local"))
    .bind(&sub)
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(LEGACY_ORG)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(org_account_id)
        .bind(user_id)
        .execute(&control)
        .await
        .unwrap();

    // The org's pre-split shared-plane content — this must NEVER be returned
    // or touched by any of the three rejected requests below.
    let shared_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Legacy shared-DB org ocean', 'org') \
         RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', 'legacy-seed-1', 'unused', \
         'grandfathered shared-plane secret content', '{}', now())",
    )
    .bind(shared_graph_id)
    .execute(&control)
    .await
    .unwrap();
    let graphs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_before: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!((graphs_before, atoms_before), (1, 1), "fixture sanity");

    // Confirm no routing row exists for this org — the whole premise.
    let routed: Option<String> =
        sqlx::query_scalar("SELECT status FROM tenant_databases WHERE clerk_org_id = $1")
            .bind(LEGACY_ORG)
            .fetch_optional(&control)
            .await
            .unwrap();
    assert_eq!(routed, None, "fixture sanity: no tenant_databases row for the legacy org");

    // ── AppState: control as the ambient consumer plane, tenant runtime
    //    ENABLED with an otherwise-empty registry (nothing this org could
    //    resolve to even if the gate mistakenly tried). ───────────────────────
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
        Vec::<(String, String)>::new(),
    ))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // REST org token (session-JWT shaped — `require_auth` doesn't require
    // OAuth claims). `/mcp`'s org token below is OAuth-shaped (client_id+jti,
    // no sid) since `mcp_auth` rejects session JWTs outright.
    let rest_token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": LEGACY_ORG, "org_role": "org:member",
    }));
    let mcp_token = mint_oauth(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "client_id": "shared_org_probe_client", "jti": Uuid::new_v4().to_string(),
        "org_id": LEGACY_ORG, "org_role": "org:member",
    }));
    // The SAME user's genuinely personal OAuth token — identical `sub` and
    // `client_id`, no `org_id` claim, which is the ONLY difference. It is the
    // non-vacuity control for (c2): it must still be served where the
    // org-claim token is refused, or that refusal says nothing about the
    // claim.
    let mcp_personal_token = mint_oauth(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "aud": std::env::var("MCP_RESOURCE_URL").expect("configured MCP test resource"),
        "client_id": "shared_org_probe_client", "jti": Uuid::new_v4().to_string(),
    }));

    const EXPECTED_MSG: &str = "organization is not provisioned for service";

    // ── (a) org subdomain — the org API address is RETIRED (claim-based
    //    tenancy Phase 4 cutover step 3), so this is a 410 before auth rather
    //    than the fail-closed 404 it used to be.
    //
    //    ONLY the status literal and the body-substring literal moved, and
    //    only because this PR moved them. The assertion this arm exists for —
    //    the shared/consumer-plane graph id must never appear in a rejected
    //    response — is unchanged, and so is every other arm of this test. See
    //    `docs/plans/claim-based-tenancy.md` decision 10: this test and its two
    //    neighbours are the ones a cutover is most likely to soften by
    //    analogy, and nothing here relaxes what is refused. ──────────────────
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("host", "acme-legacy.coralswarm.example")
                .header("authorization", format!("Bearer {rest_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::GONE,
        "the retired org API address must 410 before auth, even for a grandfathered org with \
         a real accounts row"
    );
    let body = body_json(resp).await;
    assert!(
        body["error"].as_str().unwrap_or("").contains("retired"),
        "unexpected retired-address body: {body:?}"
    );
    assert!(
        !body.to_string().contains(&shared_graph_id.to_string()),
        "the shared-plane org graph id must never appear in a rejected response"
    );

    // ── (b) apex host through `require_auth` (a REST route) — org-tenancy PR
    //    4.3: the Apex arm now resolves THIS org's own routing row (by
    //    clerk_org_id) to pick the rejection, so "no routing row at all" gets
    //    its own distinct, correct message instead of the (actively wrong,
    //    pre-PR-4.3) "use your organization's subdomain" advice. ─────────────
    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("host", APEX)
                .header("authorization", format!("Bearer {rest_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::FORBIDDEN,
        "an org token on the apex must be refused even when the org has a pre-existing \
         accounts row and shared-plane content"
    );
    let body = body_json(resp).await;
    assert_eq!(
        body["error"], EXPECTED_MSG,
        "org-tenancy PR 4.3: the apex-arm rejection for a no-routing-row org must be this exact, \
         support-actionable message, not \"use your organization's subdomain\" (there is no \
         subdomain to use)"
    );
    assert!(
        !body.to_string().contains(&shared_graph_id.to_string()),
        "the shared-plane org graph must never be returned to an apex org-token request"
    );

    // ── (c) `/mcp` on the (FORMER) ORG subdomain — claim-based-tenancy §Phase
    //    3 PR 3.3 retires the org MCP resource: `McpConfig::resolve_resource`'s
    //    `Subdomain` arm no longer shares REST's `gate::decide`/
    //    `TenantRuntime::finish_binding` path at all (that was true through
    //    Phase 1–2, and is what this comment used to say — see git history for
    //    the old fail-closed-404 assertion this replaced).
    //
    //    **Updated again by this same PR's rebase onto `main`.** This test
    //    was briefly written to expect `/mcp` on this Host to collapse to the
    //    SAME personal-resource path as (c2) below (200, claim stripped). That
    //    was true only in the window before `RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`
    //    (claim-based tenancy Phase 4 cutover step 3, #807) was deleted — this
    //    PR's item 3 deletes it the moment `resolve_resource`'s `Subdomain`
    //    arm stops producing `Org` tenancy, which is exactly what just
    //    happened. So `retired_org_host_guard` now 410s this Host for `/mcp`
    //    too, OUTSIDE `/mcp` entirely, before `/mcp`'s own tenancy resolution
    //    ever runs. (c) and (c2) below are therefore NOT the same code path:
    //    (c) never reaches `/mcp` at all; (c2) is `/mcp`'s own claim handling,
    //    on the one Host (`APEX`) the retired-host guard never touches — and
    //    since #869 that handling REFUSES rather than downgrades, so the two
    //    arms now differ in status (410 vs 403) but agree in kind: neither
    //    serves anything. ────────────────────────────────────────────────
    let (status, rpc) = mcp_tools_list(&app, &mcp_token, "acme-legacy.coralswarm.example").await;
    assert_eq!(
        status,
        StatusCode::GONE,
        "the (former) org subdomain must be 410 for /mcp too now — before /mcp's own claim \
         handling ever runs: {rpc:?}"
    );
    assert!(
        !rpc.to_string().contains(&shared_graph_id.to_string()),
        "the shared-plane org graph must never appear in the (former) org-subdomain /mcp response: {rpc:?}"
    );

    // ── (c2) `/mcp` on the APEX — now AGREES with (b): the same 403, with the
    //    same message. `/mcp` no longer diverges here at all. ────────────────
    //
    //    **This assertion was 200 until 2026-09-10 and was changed with the
    //    founder's explicit sign-off, after the code was investigated and found
    //    CORRECT.** Recording that here because the diff on its own reads as
    //    "an agent edited a security assertion", and a future reader has no
    //    other way to tell it was sanctioned.
    //
    //    What this block used to say, and why it was true then: `/mcp`'s
    //    personal resource STRIPPED an `org_id` claim (`mcp::auth::
    //    claims_for_resource`) and served the caller's own personal plane,
    //    because Clerk's consent screen offered no "personal" row and refusing
    //    would have left an org member with no working combination at all.
    //
    //    Both halves of that premise are gone. Personal organizations
    //    (migration 0208) gave the consent screen its personal row, #825 put
    //    `user:org:read` back so the picker renders, and **#869 (`968f1f08`,
    //    "honor the consent screen's organization instead of discarding it")
    //    deleted the strip** — an org claim on `/mcp` now runs through
    //    `tenant_selection::resolve_request_tenancy` -> `tenancy::gate::decide`
    //    on exactly the terms a stored `select_tenant` selection does, and
    //    `RoutingState::Missing` there is `Forbidden(EXPECTED_MSG)`. #869
    //    updated `mcp_tenant_pg` and `mcp_tenant_selection_pg` and did not
    //    update this block, so this was red from `968f1f08` onward — confirmed
    //    by bisect (its parent `03b43d80` passes). Nobody saw it because the CI
    //    runs on the mains in between were cancelled.
    //
    //    The evidence that 403 is the INTENDED answer, not a regression:
    //      * `tenancy::gate`'s own in-crate unit test
    //        `apex_org_token_matrix_binds_only_on_an_active_row` asserts
    //        `decide(Apex, Org, Missing) == Forbidden(EXPECTED_MSG)`, and its
    //        doc says "the SAME message `/mcp` gives this case".
    //      * `mcp_tenant_selection_pg`'s `expected_refusal` maps
    //        `RoutingState::Missing -> (FORBIDDEN, EXPECTED_MSG)`, and
    //        `selected_org_with_a_non_active_routing_row_is_refused_never_personal`
    //        calls a Personal fallback here "exactly the hazard
    //        `tenancy::gate::decide`'s own comment names".
    //      * This file's OWN module doc has said all along that (c) is "the
    //        SAME 403 message ... asserted here to prove (b) and (c) now
    //        literally agree". The module doc tracked the code; only this
    //        assertion did not.
    //
    //    The one legitimate "no routing row => serve as personal" case is a
    //    PERSONAL org (migration 0208), which never has a routing row by
    //    construction — covered by `mcp_tenant_selection_pg`'s personal-org
    //    heal test. `LEGACY_ORG` is a grandfathered shared-DB org with a real
    //    `accounts` row and no `personal_orgs` row, so that carve-out does not
    //    reach this fixture.
    let (status, rpc) = mcp_tools_list(&app, &mcp_token, APEX).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "an org claim with no routing row must be refused on /mcp's personal resource, with the \
         same message (b) gives — this is the surface the module doc calls (c): {rpc:?}"
    );
    assert!(
        rpc.to_string().contains(EXPECTED_MSG),
        "the /mcp refusal must carry the SAME support-actionable message as the REST apex arm \
         (b), not a generic denial — that literal agreement is what this file exists to prove: \
         {rpc:?}"
    );
    assert!(
        !rpc.to_string().contains(&shared_graph_id.to_string()),
        "the org's shared-plane graph must never appear in the refusal: {rpc:?}"
    );

    // The non-vacuity control, and the reason `mcp_personal_token` still
    // exists in this test. A 403 proves nothing on its own: the same 403 would
    // appear if the fixture user were broken, the router misconfigured, or
    // `/mcp` unreachable on this host — and the assertion above would still be
    // green while measuring none of what it claims. So the SAME user's
    // genuinely personal token (identical `sub` and `client_id`, no `org_id`
    // claim — the ONLY difference) must still be served normally on the SAME
    // host and the SAME endpoint. That isolates the refusal to the org claim.
    //
    // This replaces the old "a stripped claim reaches EXACTLY what a personal
    // token reaches" equality control, which is not expressible once there is
    // no stripped caller to compare: the org-claim request no longer reaches
    // anything at all, which is a strictly stronger outcome than reaching the
    // same as personal.
    //
    // Note what this deliberately does NOT assert: that the personal token
    // cannot see the org's ocean. On a deployment where this org has no tenant
    // DB its ocean still lives in the shared consumer database and IS listed
    // to its own members there. That visibility is a property of shared-DB
    // orgs and account membership, not of MCP — asserting against it here
    // would be pinning unrelated behaviour.
    let (status, personal) = mcp_list_oceans(&app, &mcp_personal_token, APEX).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "CONTROL FAILED: the same user's personal token must still be served on the same host \
         and endpoint. If this is not 200, the 403 above is not evidence about the org claim — \
         it is evidence this fixture cannot reach /mcp at all: {personal:?}"
    );

    // ── No leak, no matter how you slice it: zero tenant pools were ever
    //    built (there is no routing row to build one FOR), and the shared
    //    plane's org content is byte-for-byte unchanged — no write slipped
    //    through any of the three rejected requests. ─────────────────────────
    assert_eq!(
        registry.live_pool_count(),
        0,
        "no tenant pool may ever be built for an org with no routing row"
    );
    let graphs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_after: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!(
        (graphs_after, atoms_after),
        (graphs_before, atoms_before),
        "the shared-plane org content must be completely untouched by every rejected request"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}

// ── Apex-present-row wiring ──────────────────────────────────────────────────
//
// The test above only ever exercises `RoutingState::Missing` on the apex arm.
// The two below exercise a PRESENT row, through the REAL wiring —
// `resolve_routing_by_org` reading an actual `tenant_databases` row →
// `routing_state` → `decide` — rather than `decide()` in isolation (the pure
// cross-product invariants in `tenancy::gate::tests` cover the policy itself).
//
// **Claim-based tenancy PR 1.2 inverted the `active` case, and ONLY that one.**
// The apex used to refuse an active org outright; it now binds that org's
// tenant directly — with no redirect — because the claim itself already names
// the tenant. `HostTarget::Subdomain` is NOT retired: it still actively
// serves active-organization requests on their own subdomain during the
// redirect cutover (see `gate::tests::org_subdomain_matrix`), and these two
// tests are only about the apex arm. So the question these tests ask changed
// from "was it served at all?" to "was it served the RIGHT plane?" — which is
// why both still seed a REAL tenant database with distinctive content AND a
// shared/consumer plane with its own: a wrong answer shows up as actual
// content from the wrong plane, not merely a status-code mismatch.
//
// What did NOT change, and must not: `provisioning` (below) still 503s, and
// `org_claim_with_no_routing_row_fails_closed_...` above still refuses. Those
// are separate tests precisely so this inversion cannot be carried across to
// them by analogy — see `docs/plans/claim-based-tenancy.md`, decision 10.

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

#[tokio::test]
async fn org_with_active_routing_row_is_served_its_own_tenant_on_apex_never_the_shared_plane() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_with_active_routing_row_is_served_its_own_tenant_on_apex_never_the_shared_plane: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = Env::enable().await;

    let (ctl, tenant_db) = ("orgt_sharedorg_active_ctl", "orgt_sharedorg_active_tenant");
    let mut conn = admin(&base).await;
    for db in [ctl, tenant_db] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }

    const ORG: &str = "org_legacy_active_row";
    const TENANT_MARKER: &str = "TENANT-PLANE-CONTENT-MUST-NEVER-REACH-THE-APEX-xyz42";

    // ── Control DB: same "grandfathered org with real shared-plane content"
    //    shape as the no-routing-row test above, but this time WITH a routing
    //    row — status 'active'. ────────────────────────────────────────────
    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("shared_org_active_member_{}", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
    )
    .bind(format!("{sub}@users.clerk.local"))
    .bind(&sub)
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(ORG)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(org_account_id)
        .bind(user_id)
        .execute(&control)
        .await
        .unwrap();

    let shared_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Legacy shared-DB org ocean', 'org') \
         RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', 'legacy-seed-1', 'unused', \
         'grandfathered shared-plane secret content', '{}', now())",
    )
    .bind(shared_graph_id)
    .execute(&control)
    .await
    .unwrap();
    let graphs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_before: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();

    // The routing row this test is actually about: ACTIVE (still fully
    // servable on the org's real subdomain via `HostTarget::Subdomain` — the
    // point of this test is that it must NOW ALSO be served on the apex,
    // bound directly to this org's own tenant with no redirect).
    seed_routing_row(
        &control,
        ORG,
        org_account_id,
        "legacy-active-01",
        "SECRET_LEGACY_ACTIVE",
        "active",
    )
    .await;

    // ── A REAL tenant database, fingerprinted and holding distinctive
    //    content — wired into the registry behind the routing row's secret
    //    ref. If a regression let the apex arm resolve an `Org` outcome for
    //    an active row, THIS is what would leak. ────────────────────────────
    let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, tenant_db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(ORG)
        .execute(&tenant_pool)
        .await
        .unwrap();
    let tenant_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, $2, 'org') RETURNING id",
    )
    .bind(org_account_id)
    .bind(TENANT_MARKER)
    .fetch_one(&tenant_pool)
    .await
    .unwrap();

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs([(
        "SECRET_LEGACY_ACTIVE",
        with_db(&base, tenant_db),
    )]))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let rest_token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": ORG, "org_role": "org:member",
    }));

    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("host", APEX)
                .header("authorization", format!("Bearer {rest_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    // ── INVERTED by claim-based tenancy PR 1.2 (was 403 "use your
    //    organization's subdomain"). The apex is now this org's door. ──────
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "an ACTIVE routing row is now SERVED on the apex — the claim names the tenant and \
         there is no subdomain to redirect anyone to"
    );
    // Lift these off before `body_json` consumes the response.
    let tenancy_fields = resp
        .extensions()
        .get::<coralswarm_backend::TenancyLogFields>()
        .cloned();
    let body = body_json(resp).await;
    let body_text = body.to_string();

    // Served the right plane: this org's OWN tenant database.
    assert!(
        body_text.contains(TENANT_MARKER) || body_text.contains(&tenant_graph_id.to_string()),
        "the apex must serve this org's own tenant plane, not an empty result — a 200 that \
         returns nothing would satisfy every negative assertion below while proving \
         the binding never happened: {body:?}"
    );

    // ── NOT inverted, and the whole reason this test exists. ─────────────
    // The shared/consumer plane must never appear. This is the [SEC — HIGH]
    // failure in `docs/plans/claim-based-tenancy.md`: reachability on the
    // consumer plane has no account filter at all, so an org caller who lands
    // there is handed the union of every account they belong to.
    assert!(
        !body_text.contains(&shared_graph_id.to_string()),
        "shared/consumer-plane content reached an org caller on the apex — this is the \
         fall-through the entire plan exists to prevent: {body:?}"
    );

    // A tenant pool WAS built — the positive control. Without this, a
    // regression that refused every request would still pass the negative
    // assertions above.
    assert_eq!(
        registry.live_pool_count(),
        1,
        "the apex must build exactly this org's tenant pool"
    );

    // The access log can SEE which plane served this (claim-based tenancy PR
    // 1.4, decision 12). Asserted end-to-end here rather than only as a unit
    // test, because the lift onto the response is the fragile half: the
    // request has been moved into `next.run` by the time the outermost log
    // layer runs, so if these fields ever stop riding on the response the
    // line silently loses its tenant and nothing else notices.
    //
    // This matters most precisely because decision 9 accepted that a
    // wrong-tenant request is otherwise SILENT — this log is the compensating
    // control, so it has to actually work on the path that serves a tenant.
    let logged = tenancy_fields
        .expect("the served response must carry tenancy fields for the access log");
    assert_eq!(logged.plane, "org", "the log must name the tenant plane");
    assert_eq!(
        logged.org_id.as_deref(),
        Some(ORG),
        "the log must name the org that was actually bound"
    );

    // Zero rows touched anywhere, on either plane.
    let graphs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_after: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!((graphs_after, atoms_after), (graphs_before, atoms_before), "shared plane untouched");
    let tenant_graphs_count: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs")
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
    assert_eq!(
        tenant_graphs_count, 1,
        "the tenant plane was read, not written — still just the seed graph"
    );

    control.close().await;
    tenant_pool.close().await;
    for db in [ctl, tenant_db] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[tokio::test]
async fn org_with_provisioning_routing_row_apex_returns_503_never_served() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping org_with_provisioning_routing_row_apex_returns_503_never_served: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = Env::enable().await;

    let ctl = "orgt_sharedorg_prov_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;

    const ORG: &str = "org_legacy_provisioning_row";

    let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

    let sub = format!("shared_org_prov_member_{}", Uuid::new_v4());
    let user_id: Uuid = sqlx::query_scalar(
        "INSERT INTO users(id, email, clerk_user_id) VALUES (gen_random_uuid(), $1, $2) RETURNING id",
    )
    .bind(format!("{sub}@users.clerk.local"))
    .bind(&sub)
    .fetch_one(&control)
    .await
    .unwrap();
    let org_account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(user_id)
    .bind(ORG)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(org_account_id)
        .bind(user_id)
        .execute(&control)
        .await
        .unwrap();

    let shared_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Legacy shared-DB org ocean', 'org') \
         RETURNING id",
    )
    .bind(org_account_id)
    .fetch_one(&control)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO atoms(graph_id, source_type, source_id, content_blob_key, content_inline, \
         participants, timestamp) \
         VALUES ($1, 'note', 'legacy-seed-1', 'unused', \
         'grandfathered shared-plane secret content', '{}', now())",
    )
    .bind(shared_graph_id)
    .execute(&control)
    .await
    .unwrap();
    let graphs_before: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_before: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();

    // Mid-onboarding: no real tenant DB needs to exist yet for this status —
    // the gate must reject BEFORE ever consulting the registry for a pool.
    seed_routing_row(
        &control,
        ORG,
        org_account_id,
        "legacy-prov-01",
        "SECRET_LEGACY_PROVISIONING_UNUSED",
        "provisioning",
    )
    .await;

    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::from_pairs(
        Vec::<(String, String)>::new(),
    ))));
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let rest_token = mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "org_id": ORG, "org_role": "org:member",
    }));

    let resp = app
        .clone()
        .oneshot(
            HttpRequest::builder()
                .method("GET")
                .uri("/v1/graphs")
                .header("host", APEX)
                .header("authorization", format!("Bearer {rest_token}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::SERVICE_UNAVAILABLE,
        "a mid-onboarding (provisioning) routing row on the apex must 503 (retryable), never \
         serve and never permanently 403"
    );
    let body = body_json(resp).await;
    assert!(
        !body.to_string().contains(&shared_graph_id.to_string()),
        "the shared-plane content must never reach a provisioning-status apex response: {body:?}"
    );

    assert_eq!(
        registry.live_pool_count(),
        0,
        "a provisioning routing row must never cause a tenant pool build attempt on the apex"
    );

    let graphs_after: i64 = sqlx::query_scalar("SELECT count(*) FROM graphs WHERE account_id = $1")
        .bind(org_account_id)
        .fetch_one(&control)
        .await
        .unwrap();
    let atoms_after: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
        .bind(shared_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();
    assert_eq!((graphs_after, atoms_after), (graphs_before, atoms_before), "shared plane untouched");

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}
