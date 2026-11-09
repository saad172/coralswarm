//! Postgres integration tests for org-tenancy PR 4.2 — the legacy-token fence:
//! `require_auth`'s REST API is now closed to stale/removed org members the
//! same way `/mcp` already was (org-tenancy PR 2.2). See
//! `middleware::auth::live_org_membership` and `OrgMembershipPolicy::
//! ProvisionFromClaims`'s doc comment for the mechanism.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights — same scratch-DB fixture pattern as `mcp_governance_pg.rs`
//! / `binding_gate_pg.rs`); a plain `cargo test` with no DB configured skips
//! every test in this file.
//!
//! These tests drive the REAL public HTTP router
//! (`coralswarm_backend::test_support_app`) with genuine RS256-signed tokens
//! against a local JWKS listener, and deliver REAL signed Clerk/Svix webhook
//! events to `/v1/webhooks/clerk` to offboard/revoke — exactly the
//! `mcp_governance_pg.rs` pattern, applied to `require_auth` content routes
//! (`GET /v1/graphs`) instead of `/mcp`.
//!
//! **`MULTITENANT_ENABLED` is never set in this file, deliberately** — same
//! convention as `mcp_governance_pg.rs`. This fence reuses PR 2.2's
//! `account_members`/`org_membership_tombstones`/`org_revocations` machinery,
//! which is control-plane state that exists independently of database-per-org
//! tenant pools, and PR 2.2's own `/mcp` fence has always been flag-
//! independent (see that file's header comment). Gating THIS fence behind
//! `MULTITENANT_ENABLED` would leave the exact gap the PR 2.2 review flagged
//! wide open in prod today (where the flag is unset) until cutover — directly
//! contradicting why this PR is on the T-1 "must be merged/deployed" checklist
//! in the first place. `flag_off_fence_is_still_active` below asserts this
//! property explicitly.
//!
//! ## Running
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test legacy_token_fence_pg -- --nocapture

use axum::body::{to_bytes, Body};
use axum::http::{Request, StatusCode};
use chrono::Utc;
use coralswarm_backend::clerk::test_support::{mint, test_jwks, ISSUER};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::AppState;
use serde_json::{json, Value};
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use std::sync::Arc;
use tokio::sync::Mutex;
use tower::ServiceExt;
use uuid::Uuid;

/// Serializes every test in this file against the process-global env vars
/// `FenceEnv` sets — same rationale as `mcp_governance_pg.rs`'s `ENV_LOCK`.
static ENV_LOCK: Mutex<()> = Mutex::const_new(());

// ── Scratch-DB helpers (pattern: mcp_governance_pg.rs) ───────────────────────

fn base_url() -> Option<String> {
    std::env::var("ORGT_TEST_BASE_URL").ok()
}
fn with_db(base: &str, db: &str) -> String {
    let (prefix, rest) = base.split_at(base.rfind('/').expect("url path /"));
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
async fn scratch_pool(base: &str, db: &str) -> PgPool {
    let mut conn = admin(base).await;
    recreate(&mut conn, db).await;
    PgPoolOptions::new()
        .max_connections(4)
        .connect(&with_db(base, db))
        .await
        .expect("connect scratch DB")
}
async fn migrate_control_all(pool: &PgPool) {
    sqlx::migrate!("./migrations_control")
        .run(pool)
        .await
        .expect("run control migrations");
}

// ── Env + router fixture (pattern: mcp_governance_pg.rs) ─────────────────────

const TEST_MASTER_KEK_HEX: &str =
    "2222222222222222222222222222222222222222222222222222222222222222";
const WEBHOOK_RAW_KEY: &[u8] = b"legacy-fence-webhook-test-key";

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

struct FenceEnv {
    _guard: tokio::sync::MutexGuard<'static, ()>,
}

impl FenceEnv {
    async fn enable() -> Self {
        let guard = ENV_LOCK.lock().await;
        let jwks_url = spawn_local_jwks().await;
        // SAFETY: every env mutation across this test binary is serialized by
        // `ENV_LOCK`, whose guard is held in `self._guard` for the lifetime of
        // this `FenceEnv` (through `Drop` below). No other thread reads or
        // writes these vars concurrently, so the process-global mutation is
        // sound (same convention as `mcp_governance_pg.rs`'s `McpEnv`).
        unsafe {
            std::env::set_var("CLERK_ISSUER", ISSUER);
            std::env::set_var("CLERK_JWKS_URL", jwks_url);
            std::env::set_var("MASTER_KEK_HEX", TEST_MASTER_KEK_HEX);
            {
                use base64::{engine::general_purpose::STANDARD, Engine};
                std::env::set_var(
                    "CLERK_WEBHOOK_SECRET",
                    format!("whsec_{}", STANDARD.encode(WEBHOOK_RAW_KEY)),
                );
            }
            // Flag-off is the condition under test throughout this file (see
            // module doc): explicit even though nothing here sets it.
            std::env::remove_var("MULTITENANT_ENABLED");
        }
        Self { _guard: guard }
    }
}

impl Drop for FenceEnv {
    fn drop(&mut self) {
        // SAFETY: `self._guard` still holds `ENV_LOCK` here (it is dropped
        // after this body), so these removals are serialized against every
        // other test's env access — no concurrent reader/writer.
        unsafe {
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("CLERK_JWKS_URL");
            std::env::remove_var("MASTER_KEK_HEX");
            std::env::remove_var("CLERK_WEBHOOK_SECRET");
        }
    }
}

fn app_for_pool(pool: PgPool) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(pool.clone());
    state.store = Some(Arc::new(PostgresStore::new(pool)) as Arc<dyn Store>);
    coralswarm_backend::test_support_app(Arc::new(state))
}

fn future_exp() -> usize {
    (Utc::now().timestamp() + 3600) as usize
}

/// A Clerk SESSION-shaped JWT (carries `sid`) with an active-org context —
/// what the interactive web path sends to `require_auth`.
fn session_org_token(sub: &str, org_id: &str, org_role: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": format!("sess_{sub}"),
        "org_id": org_id, "org_role": org_role,
    }))
}

/// A Clerk SESSION-shaped JWT with NO org context — the personal-account path.
fn session_personal_token(sub: &str) -> String {
    mint(json!({
        "iss": ISSUER, "sub": sub, "exp": future_exp(),
        "sid": format!("sess_{sub}"),
    }))
}

// ── require_auth + webhook plumbing (pattern: mcp_governance_pg.rs) ─────────

async fn api_get(app: &axum::Router, path: &str, token: &str) -> (StatusCode, Value) {
    let req = Request::builder()
        .method("GET")
        .uri(path)
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    let status = resp.status();
    let bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let parsed = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    };
    (status, parsed)
}

/// Deliver a signed Clerk/Svix webhook event to the REAL
/// `/v1/webhooks/clerk` endpoint — identical construction to
/// `mcp_governance_pg.rs::post_clerk_webhook`.
async fn post_clerk_webhook(app: &axum::Router, event: Value) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    use hmac::{Hmac, Mac};

    let body = event.to_string();
    let id = "msg_fence_test";
    let ts = Utc::now().timestamp().to_string();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(WEBHOOK_RAW_KEY).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    let sig = format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()));

    let req = Request::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "clerk webhook delivery must be acked"
    );
}

// ── Fixture seeding (direct SQL — no provisioning side effects) ─────────────

async fn seed_user(pool: &PgPool, sub: &str) -> Uuid {
    sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
        .bind(format!("{sub}@users.clerk.local"))
        .bind(sub)
        .fetch_one(pool)
        .await
        .expect("seed user")
}

async fn seed_personal_account(pool: &PgPool, user_id: Uuid) -> Uuid {
    let account_id: Uuid =
        sqlx::query_scalar("INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id")
            .bind(user_id)
            .fetch_one(pool)
            .await
            .expect("seed personal account");
    sqlx::query("INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')")
        .bind(account_id)
        .bind(user_id)
        .execute(pool)
        .await
        .expect("seed personal membership");
    account_id
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

async fn seed_membership(pool: &PgPool, account_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query(
        "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
         VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
    )
    .bind(account_id)
    .bind(user_id)
    .bind(role)
    .execute(pool)
    .await
    .expect("seed membership");
}

/// Seed a graph directly onto an account, bypassing the Store trait — the
/// content-plane analogue of the P0 test's "sensitive content" the fence must
/// keep out of the response. Returns the graph's name, used as the "did we
/// leak content" needle in `GET /v1/graphs`'s response body.
async fn seed_graph(pool: &PgPool, account_id: Uuid, name: &str, kind: &str) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, $3) RETURNING id",
    )
    .bind(account_id)
    .bind(name)
    .bind(kind)
    .fetch_one(pool)
    .await
    .expect("seed graph")
}

/// `GET /v1/graphs` returns a bare JSON array (see `routes::graphs::
/// list_graphs`); this asserts the named graph's `name` appears nowhere in it
/// — the "zero content served" requirement, checked on the actual parsed
/// response rather than the raw byte length (200-with-empty-array would also
/// satisfy a byte-length check but is a completely different, wrong,
/// behavior from a 403).
fn response_contains_graph_named(body: &Value, name: &str) -> bool {
    body.as_array()
        .map(|graphs| {
            graphs
                .iter()
                .any(|g| g.get("name").and_then(|n| n.as_str()) == Some(name))
        })
        .unwrap_or(false)
}

// ── (a) Offboarded member: 403, zero content, before-and-after peers unaffected ─

/// THE core PR 4.2 regression: offboard a member via the REAL webhook, then
/// replay their still-valid session token against `GET /v1/graphs`. Before
/// PR 4.2 this request was ACCEPTED (200) and served whatever the account's
/// `list_reachable_graphs` union resolved — the exact gap the PR 2.2 review
/// flagged. A never-removed member of the SAME org must be entirely
/// unaffected.
#[tokio::test]
async fn offboarded_member_403s_on_require_auth_rest_route_with_zero_content() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping offboarded_member_403s_on_require_auth_rest_route_with_zero_content: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_offboard").await;
    migrate_control_all(&pool).await;

    let victim_sub = "fence_victim";
    let survivor_sub = "fence_survivor";
    let victim = seed_user(&pool, victim_sub).await;
    let survivor = seed_user(&pool, survivor_sub).await;
    let _victim_personal = seed_personal_account(&pool, victim).await;
    let org = seed_org_account(&pool, victim, "org_fence_offboard").await;
    seed_membership(&pool, org, victim, "member").await;
    seed_membership(&pool, org, survivor, "member").await;
    let graph_name = "fence-sensitive-ocean";
    seed_graph(&pool, org, graph_name, "org").await;

    let app = app_for_pool(pool.clone());
    // Minted BEFORE offboarding; stays signature-valid for its whole TTL —
    // exactly the artifact the attack replays.
    let victim_token = session_org_token(victim_sub, "org_fence_offboard", "org:member");
    let survivor_token = session_org_token(survivor_sub, "org_fence_offboard", "org:member");

    // Sanity: both members can read the org ocean before offboarding.
    let (status, body) = api_get(&app, "/v1/graphs", &victim_token).await;
    assert_eq!(status, StatusCode::OK, "victim should read before offboarding: {body:?}");
    assert!(
        response_contains_graph_named(&body, graph_name),
        "victim's pre-offboarding read must see the org ocean: {body:?}"
    );

    // Offboard the victim through the REAL signed webhook (tombstone +
    // membership DELETE, one transaction — org-tenancy PR 2.2).
    post_clerk_webhook(
        &app,
        json!({
            "type": "organizationMembership.deleted",
            "data": {
                "organization": { "id": "org_fence_offboard" },
                "public_user_data": { "user_id": victim_sub }
            }
        }),
    )
    .await;

    // THE FENCE: the same still-valid session token, replayed on a
    // require_auth content route, must be rejected — not served, and
    // certainly not served WITH the org's content.
    let (status, body) = api_get(&app, "/v1/graphs", &victim_token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "org-tenancy PR 4.2: offboarded member's still-valid token must 403 on require_auth: {body:?}"
    );
    assert!(
        !response_contains_graph_named(&body, graph_name),
        "zero content served: the 403 body must not leak the org ocean's name: {body:?}"
    );
    assert_eq!(body["error"], "not a member of this organization");

    // A never-removed member of the SAME org is entirely unaffected.
    let (status, body) = api_get(&app, "/v1/graphs", &survivor_token).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a never-removed member of the same org must be unaffected by the fence: {body:?}"
    );
    assert!(
        response_contains_graph_named(&body, graph_name),
        "the surviving member must still read the org ocean: {body:?}"
    );
}

// ── (b) Wholesale-revoked org ─────────────────────────────────────────────────

/// `organization.deleted` revokes every member at once (org-tenancy PR 2.2).
/// A member's still-valid token, replayed on a `require_auth` content route
/// after the revocation, must 403 — the REST analogue of the existing
/// `/mcp` behavior (`mcp_governance_pg::org_deleted_revokes_every_member`).
#[tokio::test]
async fn revoked_org_403s_on_require_auth_rest_route() {
    let Some(base) = base_url() else {
        eprintln!("skipping revoked_org_403s_on_require_auth_rest_route: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_revoked").await;
    migrate_control_all(&pool).await;

    let sub = "fence_revoked_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_fence_revoked").await;
    seed_membership(&pool, org, user, "owner").await;
    let graph_name = "fence-revoked-ocean";
    seed_graph(&pool, org, graph_name, "org").await;

    let app = app_for_pool(pool.clone());
    let token = session_org_token(sub, "org_fence_revoked", "org:admin");

    let (status, _) = api_get(&app, "/v1/graphs", &token).await;
    assert_eq!(status, StatusCode::OK, "member should read before org deletion");

    post_clerk_webhook(
        &app,
        json!({ "type": "organization.deleted", "data": { "id": "org_fence_revoked" } }),
    )
    .await;

    let (status, body) = api_get(&app, "/v1/graphs", &token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a revoked org's member must 403 on require_auth after organization.deleted: {body:?}"
    );
    assert!(!response_contains_graph_named(&body, graph_name));
}

// ── (c) Personal tokens are entirely unaffected ──────────────────────────────

/// The fence only ever engages on a token carrying an `org_id` claim — a
/// personal (no-org) token's access must be byte-identical before and after
/// PR 4.2, including for a user who ALSO happens to be a member of an org
/// that gets offboarded/revoked elsewhere.
#[tokio::test]
async fn personal_token_unaffected_by_the_require_auth_fence() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping personal_token_unaffected_by_the_require_auth_fence: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_personal").await;
    migrate_control_all(&pool).await;

    let sub = "fence_personal_user";
    let user = seed_user(&pool, sub).await;
    let personal = seed_personal_account(&pool, user).await;
    let personal_graph = "fence-personal-ocean";
    seed_graph(&pool, personal, personal_graph, "personal").await;
    // Also a member of an org that gets revoked — must not affect the
    // personal token at all.
    let org = seed_org_account(&pool, user, "org_fence_personal_owner").await;
    seed_membership(&pool, org, user, "owner").await;

    let app = app_for_pool(pool.clone());
    let personal_token = session_personal_token(sub);

    post_clerk_webhook(
        &app,
        json!({ "type": "organization.deleted", "data": { "id": "org_fence_personal_owner" } }),
    )
    .await;

    let (status, body) = api_get(&app, "/v1/graphs", &personal_token).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a personal (no-org) token must be unaffected by the org-scoped fence: {body:?}"
    );
    assert!(
        response_contains_graph_named(&body, personal_graph),
        "personal-account reads must be unaffected: {body:?}"
    );
}

// ── (d) First-sight provisioning for a genuinely-onboarded org still works ──

/// A never-removed member whose `organizationMembership.created` webhook
/// hasn't landed yet (or local dev with no webhook receiver) must still be
/// provisioned on first sign-in — the fence must not regress
/// `OrgMembershipPolicy::ProvisionFromClaims`'s existing first-sight path
/// (already covered at the membership-row level by
/// `mcp_governance_pg::claims_path_still_provisions_a_never_removed_member_but_never_updates_role`;
/// this asserts the REST-visible outcome end to end).
#[tokio::test]
async fn first_sight_member_of_an_onboarded_org_still_gets_in() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping first_sight_member_of_an_onboarded_org_still_gets_in: set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_firstsight").await;
    migrate_control_all(&pool).await;

    let owner_sub = "fence_firstsight_owner";
    let member_sub = "fence_firstsight_member";
    let owner = seed_user(&pool, owner_sub).await;
    let _owner_personal = seed_personal_account(&pool, owner).await;
    let org = seed_org_account(&pool, owner, "org_fence_firstsight").await;
    seed_membership(&pool, org, owner, "owner").await;
    let graph_name = "fence-firstsight-ocean";
    seed_graph(&pool, org, graph_name, "org").await;
    // Deliberately NO account_members row and NO tombstone for member_sub —
    // the webhook-race first-sign-in case.

    let app = app_for_pool(pool.clone());
    let member_token = session_org_token(member_sub, "org_fence_firstsight", "org:member");

    let (status, body) = api_get(&app, "/v1/graphs", &member_token).await;
    assert_eq!(
        status,
        StatusCode::OK,
        "a genuinely new member of an already-onboarded org must still get in on first sight: {body:?}"
    );
    assert!(response_contains_graph_named(&body, graph_name));

    let role: Option<String> = sqlx::query_scalar(
        "SELECT role FROM account_members WHERE account_id = $1 AND user_id = \
         (SELECT id FROM users WHERE clerk_user_id = $2)",
    )
    .bind(org)
    .bind(member_sub)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(role.as_deref(), Some("member"), "first-sight provisioning must have run");
}

// ── (e) Flag-independence, explicit ───────────────────────────────────────────

/// Explicit, dedicated assertion of the module doc's flag-independence claim:
/// `MULTITENANT_ENABLED` is confirmed unset, then the exact offboarding replay
/// from test (a) is repeated and must STILL 403. This is a deliberate
/// deviation from a literal "gate this behind MULTITENANT_ENABLED, flag-off
/// unchanged" reading of the PR spec — see this file's module doc comment
/// for the full rationale: gating the actual revocation-rejection behind the
/// flag would leave today's prod (flag unset) with the exact gap PR 2.2's
/// review flagged, undermining the entire point of landing this PR ahead of
/// cutover. What IS still flag-gated (unchanged by this PR): the subdomain
/// binding gate, tenant pool routing, and `ensure_org_account`'s shared- vs
/// tenant-plane graph/DEK creation branch — none of which this test touches.
#[tokio::test]
async fn flag_off_fence_is_still_active() {
    let Some(base) = base_url() else {
        eprintln!("skipping flag_off_fence_is_still_active: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = FenceEnv::enable().await;
    assert!(
        std::env::var("MULTITENANT_ENABLED").is_err(),
        "this test must run with the flag unset"
    );
    let pool = scratch_pool(&base, "orgt_fence_flagoff").await;
    migrate_control_all(&pool).await;

    let sub = "fence_flagoff_member";
    let user = seed_user(&pool, sub).await;
    let _personal = seed_personal_account(&pool, user).await;
    let org = seed_org_account(&pool, user, "org_fence_flagoff").await;
    seed_membership(&pool, org, user, "member").await;

    let app = app_for_pool(pool.clone());
    let token = session_org_token(sub, "org_fence_flagoff", "org:member");

    let (status, _) = api_get(&app, "/v1/graphs", &token).await;
    assert_eq!(status, StatusCode::OK, "member should read before offboarding");

    post_clerk_webhook(
        &app,
        json!({
            "type": "organizationMembership.deleted",
            "data": {
                "organization": { "id": "org_fence_flagoff" },
                "public_user_data": { "user_id": sub }
            }
        }),
    )
    .await;

    let (status, body) = api_get(&app, "/v1/graphs", &token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "the fence must be active with MULTITENANT_ENABLED unset (flag-independent, matching \
         PR 2.2's /mcp precedent): {body:?}"
    );
}

// ── (f) Atomic revocation guard: no orphan `accounts` row ────────────────────

/// CodeRabbit review (PR #280): `ensure_org_account`'s revocation guard must
/// be atomic with the `accounts` INSERT it gates, not a separate preceding
/// `SELECT EXISTS` — otherwise a concurrent `organization.deleted` commit
/// landing between the check and the insert could still create an orphan
/// shell `accounts` row for an org that is, by the time anyone next looks,
/// already revoked. This drives the CREATE path directly: the org is revoked
/// via the real webhook BEFORE any `accounts` row for it has EVER existed
/// (no owner ever signed in), then a first-sight token for that same org
/// hits `require_auth`. `ensure_org_account` must take the
/// `INSERT ... WHERE NOT EXISTS (org_revocations) ... RETURNING id` branch,
/// find zero rows, re-read and find NO row either (this is the true
/// never-provisioned case, not the concurrent-creator race), and reject —
/// leaving zero `accounts` rows for this org, ever.
///
/// A literal two-thread interleaving isn't meaningfully distinguishable from
/// this deterministic ordering any more: folding the guard into the INSERT's
/// own `WHERE` clause means the revocation predicate and the row creation are
/// now decided by ONE statement against one snapshot, so there is no
/// separate window left to race — this test is the "at minimum" case named
/// in the review (assert the single-statement path rejects when a
/// revocation already exists), and it fails on the pre-fix two-statement
/// version (which would have created the row here, since the plain
/// `SELECT EXISTS` genuinely ran before ANY insert was attempted, not
/// concurrently with one).
#[tokio::test]
async fn revoked_never_provisioned_org_creates_no_orphan_accounts_row() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping revoked_never_provisioned_org_creates_no_orphan_accounts_row: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_orphan").await;
    migrate_control_all(&pool).await;

    // No accounts row, no users row, nothing — this org has never touched
    // the backend in any way before its deletion webhook fires.
    let none: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE clerk_org_id = $1")
        .bind("org_fence_orphan")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(none, 0, "fixture must start with no accounts row for this org");

    let app = app_for_pool(pool.clone());
    post_clerk_webhook(
        &app,
        json!({ "type": "organization.deleted", "data": { "id": "org_fence_orphan" } }),
    )
    .await;
    let revoked: i64 =
        sqlx::query_scalar("SELECT count(*) FROM org_revocations WHERE clerk_org_id = $1")
            .bind("org_fence_orphan")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(revoked, 1, "fixture must record the org-level revocation");

    // First-sight token for the now-revoked, never-provisioned org hits
    // require_auth: this is the ONLY code path that reaches
    // `ensure_org_account`'s INSERT branch (an existing accounts row would
    // short-circuit before ever reaching it).
    let ghost_token = session_org_token("fence_orphan_ghost", "org_fence_orphan", "org:member");
    let (status, body) = api_get(&app, "/v1/graphs", &ghost_token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a revoked, never-provisioned org must reject first-sight provisioning: {body:?}"
    );

    // The atomic guard's whole point: zero accounts rows were ever created,
    // not merely zero memberships.
    let after: i64 = sqlx::query_scalar("SELECT count(*) FROM accounts WHERE clerk_org_id = $1")
        .bind("org_fence_orphan")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        after, 0,
        "org-tenancy PR 4.2 (CodeRabbit atomicity fix): no orphan accounts row may be created \
         for an already-revoked org, even on the first-sight provisioning path"
    );
}

// ── (f) The collapsed hot path (perf-auth-roundtrips) keeps the fence live ──
//
// `normalize_personal_org_claim` now reads the caller's `users` /
// `accounts` / `account_members` rows in ONE control-
// plane statement (`classify_personal_org_claim`), and
// `resolve_clerk_context` serves an already-provisioned member from that
// prefetch instead of re-asking step by step. Nothing is cached across
// requests, so the fence must hold on the VERY NEXT request after any
// membership change — these tests pin that, and pin the statement counts the
// collapse exists to deliver, so a later edit cannot quietly re-grow them.

/// Marker statement for an external `log_statement = 'all'` Postgres log, so
/// one request's statements can be delimited there too. Costs one trivial
/// statement outside the recorded window; never asserted on.
async fn pg_log_mark(pool: &PgPool, label: &str) {
    sqlx::query(&format!("SELECT 'rt-mark: {label}'"))
        .execute(pool)
        .await
        .expect("marker statement");
}

/// A warm org member — membership already present, the overwhelmingly common
/// request — resolves identity, membership and role in exactly ONE
/// control-plane statement, and is still fenced on the
/// very next request after a demotion (role from the DB row, never the
/// token) and after a webhook offboarding (403, zero content).
#[tokio::test]
async fn warm_member_resolves_in_one_statement_and_is_fenced_on_the_next_request() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping warm_member_resolves_in_one_statement_and_is_fenced_on_the_next_request: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_warm").await;
    migrate_control_all(&pool).await;

    let victim_sub = "fence_warm_victim";
    let survivor_sub = "fence_warm_survivor";
    let victim = seed_user(&pool, victim_sub).await;
    let survivor = seed_user(&pool, survivor_sub).await;
    let org = seed_org_account(&pool, victim, "org_fence_warm").await;
    seed_membership(&pool, org, victim, "admin").await;
    seed_membership(&pool, org, survivor, "member").await;
    let graph_name = "fence-warm-ocean";
    seed_graph(&pool, org, graph_name, "org").await;

    let app = app_for_pool(pool.clone());
    // The token claims `org:admin` for its whole TTL; the DB row is what
    // the request is served from.
    let victim_token = session_org_token(victim_sub, "org_fence_warm", "org:admin");
    let survivor_token = session_org_token(survivor_sub, "org_fence_warm", "org:member");

    // Request 1: membership exists.
    let (status, body) = api_get(&app, "/v1/me", &victim_token).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["role"], "admin");
    assert_eq!(body["account_id"], json!(org));

    // Request 2: fully warm. Record what it sends. `/v1/graphs`, not
    // `/v1/me`: the latter's own handler reads `personal_orgs` (naming the
    // same columns the assertions below count), `/v1/graphs`' handler is one
    // SECURITY DEFINER reachability call that names none of them.
    pg_log_mark(&pool, "warm-org-graphs begin").await;
    let ((status, body), log) =
        coralswarm_backend::sqlx_statement_log::record(api_get(&app, "/v1/graphs", &victim_token))
            .await;
    pg_log_mark(&pool, "warm-org-graphs end").await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(response_contains_graph_named(&body, graph_name), "{body:?}");
    eprintln!("warm org GET /v1/graphs issued {} statements:", log.count());
    for s in log.statements() {
        eprintln!("  {s}");
    }
    // Every control-plane identity read on the org path names
    // `clerk_org_id` (personal_orgs map, accounts row, membership presence,
    // the live membership fence) — so exactly ONE
    // statement naming it is the collapse. Before: five.
    assert_eq!(
        log.count_containing("clerk_org_id"),
        1,
        "a warm org request must resolve identity + membership in ONE control-plane statement"
    );
    assert_eq!(
        log.count_containing("user_account_prefs"),
        0,
        "MCP write is always on (settings launch plan decision 6): no request-path statement \
         may read or write the old write-grant table"
    );
    assert_eq!(
        log.count_containing("SELECT id FROM users WHERE clerk_user_id"),
        0,
        "`ensure_user`'s lookup must not run for an already-provisioned user"
    );

    // Demotion: the DB row changes, the token still says `org:admin`. The
    // very next request must serve the DB's answer — no cache in between.
    sqlx::query("UPDATE account_members SET role = 'member' WHERE account_id = $1 AND user_id = $2")
        .bind(org)
        .bind(victim)
        .execute(&pool)
        .await
        .unwrap();
    let (status, body) = api_get(&app, "/v1/me", &victim_token).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(
        body["role"], "member",
        "role must come from the live account_members row on the request right after a demotion, \
         never from the token's org_role claim: {body:?}"
    );

    // Offboarding through the REAL signed webhook (tombstone + membership
    // DELETE), then the same still-valid token on the very next request.
    post_clerk_webhook(
        &app,
        json!({
            "type": "organizationMembership.deleted",
            "data": {
                "organization": { "id": "org_fence_warm" },
                "public_user_data": { "user_id": victim_sub }
            }
        }),
    )
    .await;
    let (status, body) = api_get(&app, "/v1/graphs", &victim_token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "offboarded member must be refused on the request immediately after removal: {body:?}"
    );
    assert!(
        !response_contains_graph_named(&body, graph_name),
        "zero content served after offboarding: {body:?}"
    );
    assert_eq!(body["error"], "not a member of this organization");
    let (status, body) = api_get(&app, "/v1/me", &victim_token).await;
    assert_eq!(status, StatusCode::FORBIDDEN, "{body:?}");

    // A never-removed member of the same org is unaffected.
    let (status, body) = api_get(&app, "/v1/graphs", &survivor_token).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert!(response_contains_graph_named(&body, graph_name), "{body:?}");
}

/// MCP write is always on (settings launch plan decision 6), so the request
/// path no longer provisions the old org write grant. Neither an
/// already-provisioned member's request nor first-sight claims provisioning
/// may create a `user_account_prefs` row or even name the table, and an OLD
/// row (an admin's explicit `false`) is neither rewritten nor read.
#[tokio::test]
async fn request_path_never_touches_the_old_write_grant_table() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping request_path_never_touches_the_old_write_grant_table: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_grant").await;
    migrate_control_all(&pool).await;

    let sub = "fence_grant_member";
    let user = seed_user(&pool, sub).await;
    let org = seed_org_account(&pool, user, "org_fence_grant").await;
    seed_membership(&pool, org, user, "member").await;
    let app = app_for_pool(pool.clone());
    let token = session_org_token(sub, "org_fence_grant", "org:member");

    let prefs_rows = |pool: PgPool| async move {
        sqlx::query_scalar::<_, i64>("SELECT count(*) FROM user_account_prefs")
            .fetch_one(&pool)
            .await
            .unwrap()
    };

    // Provisioned member's first request: one identity statement, no grant.
    let ((status, body), log) =
        coralswarm_backend::sqlx_statement_log::record(api_get(&app, "/v1/graphs", &token)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(log.count_containing("user_account_prefs"), 0, "{:#?}", log.statements());
    assert_eq!(log.count_containing("clerk_org_id"), 1, "{:#?}", log.statements());
    assert_eq!(prefs_rows(pool.clone()).await, 0, "no grant row may be armed");

    // An OLD explicit deny is left exactly as it was and does not change the
    // request's outcome.
    sqlx::query(
        "INSERT INTO user_account_prefs (user_id, account_id, mcp_write_enabled) VALUES ($1, $2, false)",
    )
    .bind(user)
    .bind(org)
    .execute(&pool)
    .await
    .unwrap();
    let ((status, body), log) =
        coralswarm_backend::sqlx_statement_log::record(api_get(&app, "/v1/graphs", &token)).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(log.count_containing("user_account_prefs"), 0);
    let flag: bool = sqlx::query_scalar(
        "SELECT mcp_write_enabled FROM user_account_prefs WHERE user_id = $1 AND account_id = $2",
    )
    .bind(user)
    .bind(org)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert!(!flag, "an old explicit false row is never rewritten");

    // First-sight claims provisioning (no account_members row yet) also arms
    // nothing.
    let sub_new = "fence_grant_first_sight";
    let user_new = seed_user(&pool, sub_new).await;
    let token_new = session_org_token(sub_new, "org_fence_grant", "org:member");
    let (status, body) = api_get(&app, "/v1/graphs", &token_new).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    let new_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM user_account_prefs WHERE user_id = $1")
        .bind(user_new)
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(new_rows, 0, "first-sight provisioning must not arm a write grant");
}

/// A personal (no-org) token resolves user + personal account in ONE
/// control-plane statement (before: the `users` lookup, then the personal
/// account lookup — two), and first-sight provisioning still runs for a user
/// the combined read does not know.
#[tokio::test]
async fn personal_token_resolves_identity_in_one_statement() {
    let Some(base) = base_url() else {
        eprintln!("skipping personal_token_resolves_identity_in_one_statement: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_personal_one").await;
    migrate_control_all(&pool).await;

    let sub = "fence_personal_one";
    let user = seed_user(&pool, sub).await;
    let personal = seed_personal_account(&pool, user).await;
    let app = app_for_pool(pool.clone());
    let token = session_personal_token(sub);

    let (status, body) = api_get(&app, "/v1/me", &token).await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    assert_eq!(body["account_id"], json!(personal));
    assert_eq!(body["user_id"], json!(user));

    // Warm request, recorded on `/v1/graphs` for the same reason as the org
    // test above (`/v1/me`'s handler reads `personal_orgs` itself).
    pg_log_mark(&pool, "warm-personal-graphs begin").await;
    let ((status, body), log) =
        coralswarm_backend::sqlx_statement_log::record(api_get(&app, "/v1/graphs", &token)).await;
    pg_log_mark(&pool, "warm-personal-graphs end").await;
    assert_eq!(status, StatusCode::OK, "{body:?}");
    eprintln!("warm personal GET /v1/graphs issued {} statements:", log.count());
    for s in log.statements() {
        eprintln!("  {s}");
    }
    // `clerk_user_id` appears in the combined read and in nothing else on
    // the personal path once the user exists.
    assert_eq!(
        log.count_containing("clerk_user_id"),
        1,
        "a personal token must resolve user + personal account in ONE control-plane statement"
    );
    assert_eq!(log.count_containing("SELECT id FROM users WHERE clerk_user_id"), 0);

    // First sight of an unknown `sub`: the combined read returns NULLs and
    // the unchanged provisioning path creates the user and their account.
    let fresh_token = session_personal_token("fence_personal_fresh");
    let (status, body) = api_get(&app, "/v1/me", &fresh_token).await;
    assert_eq!(status, StatusCode::OK, "first-sight personal provisioning must still work: {body:?}");
    let fresh_user: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind("fence_personal_fresh")
        .fetch_one(&pool)
        .await
        .expect("first sight must have provisioned the users row");
    assert_eq!(body["user_id"], json!(fresh_user));
    let fresh_account: Option<Uuid> = sqlx::query_scalar(
        "SELECT id FROM accounts WHERE owner_user_id = $1 AND clerk_org_id IS NULL",
    )
    .bind(fresh_user)
    .fetch_optional(&pool)
    .await
    .unwrap();
    assert_eq!(body["account_id"], json!(fresh_account.expect("personal account provisioned")));
}

/// The combined read's `org_revocations` arm, pinned on its own. The
/// webhook revocation test above cannot tell whether that arm exists: the
/// `organization.deleted` handler also deletes every `account_members` row,
/// so the membership join alone already refuses. Here the revocation row is
/// written DIRECTLY and the membership row is left in place — the only
/// thing standing between a still-valid token and a 200 is that the
/// prefetch (like `live_org_membership`) refuses an org with a revocation
/// row. A member who got in on the request before is refused on the next.
#[tokio::test]
async fn revocation_row_alone_fences_a_member_whose_membership_row_survives() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping revocation_row_alone_fences_a_member_whose_membership_row_survives: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env = FenceEnv::enable().await;
    let pool = scratch_pool(&base, "orgt_fence_revocation_row").await;
    migrate_control_all(&pool).await;

    let sub = "fence_revrow_member";
    let user = seed_user(&pool, sub).await;
    let org = seed_org_account(&pool, user, "org_fence_revrow").await;
    seed_membership(&pool, org, user, "member").await;
    seed_graph(&pool, org, "fence-revrow-ocean", "org").await;
    let app = app_for_pool(pool.clone());
    let token = session_org_token(sub, "org_fence_revrow", "org:member");

    // Warm: two requests, the second fully on the prefetch fast path.
    for _ in 0..2 {
        let (status, body) = api_get(&app, "/v1/graphs", &token).await;
        assert_eq!(status, StatusCode::OK, "{body:?}");
    }

    sqlx::query("INSERT INTO org_revocations (clerk_org_id) VALUES ($1)")
        .bind("org_fence_revrow")
        .execute(&pool)
        .await
        .expect("record a wholesale revocation directly");
    let still_member: i64 =
        sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(org)
            .bind(user)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(still_member, 1, "fixture: the membership row must survive, only the org is revoked");

    let (status, body) = api_get(&app, "/v1/graphs", &token).await;
    assert_eq!(
        status,
        StatusCode::FORBIDDEN,
        "a revoked org must be refused on the very next request even while a membership row \
         survives — the org_revocations arm of the fence: {body:?}"
    );
    assert!(!response_contains_graph_named(&body, "fence-revrow-ocean"), "{body:?}");
}
