//! Member-ocean provisioning — the two REQUEST-adjacent triggers of
//! member-private-oceans' three-trigger design (member-private-oceans PR 4,
//! `docs/plans/member-private-oceans.md` §2.4):
//!
//! 1. **The Clerk webhook** (`routes::clerk_webhook::handle_membership_upsert`),
//!    driven here through the real `POST /v1/webhooks/clerk` HTTP endpoint
//!    (Svix-signed, same fixture pattern as `org_creation_gate_pg.rs`).
//! 2. **First-sight on the request path** (`tenancy::runtime::TenantRuntime::
//!    finish_binding`), driven here through `TenantRuntime::resolve_binding`
//!    directly (no HTTP/JWT layer needed — `resolve_binding` takes the
//!    already-"verified" `account_id`/`user_id`/`org_claim` as plain
//!    arguments, same pattern `binding_gate_pg.rs` uses).
//!
//! Trigger 3 (the reconciler sweep, PR 3) already has its own proof in
//! `member_ocean_reconciler_pg.rs`; this file's last test exercises all
//! THREE triggers racing concurrently for one brand-new member, proving the
//! shared `uq_graphs_one_primary_per_owner` `ON CONFLICT` target lets exactly
//! one win regardless of which trigger fires first.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB fixture pattern as every other
//! `*_pg.rs` file in this directory.
//!
//! ## Honest CI coverage
//!
//! Wired into `.github/workflows/ci.yml`'s `member-oceans` job (alongside
//! `member_ocean_reconciler_pg.rs`, PR 3's own suite — which had NOT been
//! wired into any job until this PR; see that job's comment for the full
//! note). Both suites now actually run in CI rather than silently skipping,
//! closing the same class of gap `reachability_pg.rs` had (PR 2's finding).
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_provisioning_pg -- \
//!     --test-threads=1 --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::routing::TenantStatus;
use coralswarm_backend::tenancy::runtime::{TenancyDecision, TenantRuntime};
use coralswarm_backend::tenancy::secrets::StaticMapResolver;
use coralswarm_backend::tenancy::TenantId;
use coralswarm_backend::workers::member_ocean_reconciler::tick;
use coralswarm_backend::workers::tenant_harness::WorkerTenant;
use coralswarm_backend::AppState;
use hmac::{Hmac, Mac};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Connection, Executor, PgConnection, PgPool};
use tokio::sync::Mutex as AsyncMutex;
use tower::ServiceExt;
use uuid::Uuid;
use coralswarm_backend::tenancy::gate::TokenKind;

const APEX: &str = "coralswarm.example";
const WEBHOOK_SECRET_B64: &str = "bWVtYmVyLW9jZWFuLXByLTQtdGVzdC1zZWNyZXQ="; // arbitrary fixed key

/// Serializes every test in this file — same rationale as every other
/// `*_pg.rs` file's `ENV_LOCK`: `MEMBER_OCEANS_ENABLED`/`CLERK_WEBHOOK_SECRET`
/// are process-global, and (per `binding_gate_pg.rs`'s own `ENV_LOCK` doc)
/// migration 0083's `coralswarm_app` role is cluster-wide, so two concurrent
/// control-tree applications race `CREATE ROLE`.
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
async fn control_pool(base: &str, db: &str) -> PgPool {
    let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
    pool
}
async fn tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
    let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
    sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
    sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
        .bind(org_id)
        .execute(&pool)
        .await
        .unwrap();
    pool
}

/// Sets `MEMBER_OCEANS_ENABLED` + `CLERK_WEBHOOK_SECRET` for one test's
/// duration and restores both on drop — including on unwind.
///
/// CodeRabbit review on PR 4: an earlier version of this file set these vars
/// inline at the top of each test and unset them again at the tail of the
/// happy path. `tokio::sync::Mutex` (this file's `ENV_LOCK`) has no
/// poisoning, so a failing `assert!` anywhere between those two points would
/// skip the tail-end cleanup and leave both vars set for the REST of this
/// test binary — the next test acquires `ENV_LOCK` cleanly (the lock itself
/// isn't poisoned) and silently runs with a flag/secret it never asked for,
/// turning one real failure into a cascade of unrelated, confusing ones.
/// Tying cleanup to `Drop` instead makes it unwind-safe: whether a test
/// returns normally or panics, `EnvGuard::drop` still runs before the next
/// test can acquire `ENV_LOCK`.
// The held `MutexGuard` is never read — it exists purely to keep `ENV_LOCK`
// locked for this guard's lifetime, released only when this struct (and thus
// the guard) drops.
#[allow(dead_code)]
struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);

impl EnvGuard {
    /// Acquire `ENV_LOCK` and set both vars. Held for the guard's entire
    /// lifetime — no other test in this binary can be running concurrently
    /// while these vars are set, by the same convention every other
    /// `*_pg.rs` file's `ENV_LOCK` already establishes.
    async fn acquire() -> Self {
        let guard = ENV_LOCK.lock().await;
        // SAFETY: `guard` (held until this `EnvGuard` itself is dropped, see
        // `Drop` below — including on unwind from a panicking assert)
        // serializes every mutation of these two process-global vars in this
        // binary against every other test's, so no concurrent reader can
        // observe them mid-mutation.
        unsafe {
            std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
            std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{WEBHOOK_SECRET_B64}"));
        }
        Self(guard)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: `self.0` (the `ENV_LOCK` guard) is still held at this point
        // — `Drop::drop` runs before `self.0` is itself dropped — so this
        // restore is serialized against every other test the same way the
        // initial `set_var` calls in `acquire` were, whether we got here via
        // normal return or a panicking unwind.
        unsafe {
            std::env::remove_var("MEMBER_OCEANS_ENABLED");
            std::env::remove_var("CLERK_WEBHOOK_SECRET");
        }
    }
}

/// Mint an org's control-plane account row + routing row (`tenant_databases`),
/// with the given lifecycle `status`. Mirrors `member_ocean_reconciler_pg.rs`'s
/// `mint_org`, parameterized on status so the webhook tests can exercise
/// `Missing` (no row at all — just don't call this), `Provisioning`, and
/// `Active`.
async fn mint_org(control: &PgPool, org_id: &str, slug: &str, status: TenantStatus, secret_ref: &str) -> Uuid {
    let owner: Uuid = sqlx::query_scalar("INSERT INTO users(email) VALUES ($1) RETURNING id")
        .bind(format!("{org_id}-owner@wh.test"))
        .fetch_one(control)
        .await
        .unwrap();
    let account_id: Uuid = sqlx::query_scalar(
        "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
    )
    .bind(owner)
    .bind(org_id)
    .fetch_one(control)
    .await
    .unwrap();
    financial_fixture::execute(
        control,
        account_id,
        sqlx::query(
            "INSERT INTO tenant_databases \
         (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
         VALUES ($1, $2, $3, $4, 'neon', 1, $5)",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(status.as_str()),
    )
    .await
    .unwrap();
    account_id
}

async fn add_member(control: &PgPool, account_id: Uuid, email: &str, name: Option<&str>) -> Uuid {
    let user_id: Uuid =
        sqlx::query_scalar("INSERT INTO users(email, name) VALUES ($1, $2) RETURNING id")
            .bind(email)
            .bind(name)
            .fetch_one(control)
            .await
            .unwrap();
    sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
        .bind(account_id)
        .bind(user_id)
        .execute(control)
        .await
        .unwrap();
    user_id
}

async fn primary_member_oceans(tenant: &PgPool, user_id: Uuid) -> Vec<Uuid> {
    sqlx::query_scalar(
        "SELECT id FROM graphs \
         WHERE kind = 'member' AND owner_user_id = $1 AND is_primary AND archived_at IS NULL",
    )
    .bind(user_id)
    .fetch_all(tenant)
    .await
    .unwrap()
}

fn worker_tenant(org_id: &str, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Org(org_id.to_string()),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

/// Svix signature, same helper (and same reasoning for duplicating it — the
/// real one is a private `#[cfg(test)]` fn inside `routes::clerk_webhook`) as
/// `org_creation_gate_pg.rs`'s `sign_svix`.
fn sign_svix(secret_b64: &str, id: &str, ts: &str, body: &str) -> String {
    let key = STANDARD.decode(secret_b64).unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
}

/// Build an `organizationMembership.created` (or `.updated`) Svix-signed
/// request for `POST /v1/webhooks/clerk`.
fn membership_webhook_request(
    event_type: &str,
    org_id: &str,
    org_name: &str,
    clerk_user_id: &str,
    email: &str,
    first_name: &str,
    last_name: &str,
) -> HttpRequest<Body> {
    let body = json!({
        "type": event_type,
        "data": {
            "organization": {"id": org_id, "name": org_name},
            "public_user_data": {
                "user_id": clerk_user_id,
                "identifier": email,
                "first_name": first_name,
                "last_name": last_name,
            },
            "role": "org:member",
        },
    })
    .to_string();
    let (id, ts) = (
        format!("msg_{}", Uuid::new_v4()),
        chrono::Utc::now().timestamp().to_string(),
    );
    let sig = sign_svix(WEBHOOK_SECRET_B64, &id, &ts, &body);
    HttpRequest::builder()
        .method("POST")
        .uri("/v1/webhooks/clerk")
        .header("content-type", "application/json")
        .header("svix-id", id)
        .header("svix-timestamp", ts)
        .header("svix-signature", sig)
        .body(Body::from(body))
        .unwrap()
}

async fn user_id_for_clerk_id(control: &PgPool, clerk_user_id: &str) -> Option<Uuid> {
    sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_optional(control)
        .await
        .unwrap()
}

// ═══════════════════════════════════════════════════════════════════════════
// Trigger 1 — Clerk webhook
// ═══════════════════════════════════════════════════════════════════════════

/// The exact trap plan §1.4b names: `organizationMembership.created` can fire
/// with NO tenant_databases row for the org at all (routing `Missing`). The
/// webhook must 200 (membership itself still provisions) and must not
/// attempt — let alone fail on — a tenant pool build.
#[tokio::test]
async fn webhook_before_tenant_exists_200s_and_does_not_provision() {
    let Some(base) = base_url() else {
        eprintln!("skipping webhook_before_tenant_exists_200s_and_does_not_provision: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let ctl = "orgt_mop1_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control = control_pool(&base, ctl).await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::new(Default::default()))));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    // Note: NO tenant_databases row is ever inserted for this org — routing is
    // genuinely `Missing`, exactly as it is the instant `organization.created`
    // fires in production, long before any tenant DB exists.
    let org_id = format!("org_mop1_{}", Uuid::new_v4());
    let clerk_user_id = format!("user_mop1_{}", Uuid::new_v4());
    let req = membership_webhook_request(
        "organizationMembership.created",
        &org_id,
        "Acme",
        &clerk_user_id,
        "alice@wh.test",
        "Alice",
        "Smith",
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "the webhook must 200 even though this org has no tenant database yet"
    );

    // Membership provisioning itself is unaffected by the ocean trigger.
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id)
        .await
        .expect("ensure_user still ran");
    let member_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM account_members am JOIN accounts a ON a.id = am.account_id \
         WHERE a.clerk_org_id = $1 AND am.user_id = $2",
    )
    .bind(&org_id)
    .bind(user_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(member_count, 1, "control-plane membership must still be upserted");

    // No tenant pool was ever built — the ocean trigger must never even
    // ATTEMPT a build when routing is Missing.
    assert_eq!(
        registry.live_pool_count(),
        0,
        "a Missing routing row must never cause a tenant pool build attempt"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// A `provisioning` (mid-onboarding) routing row — same non-servable
/// treatment as `Missing`: 200, no pool build attempted.
#[tokio::test]
async fn webhook_mid_onboarding_200s_and_does_not_provision() {
    let Some(base) = base_url() else {
        eprintln!("skipping webhook_mid_onboarding_200s_and_does_not_provision: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let ctl = "orgt_mop2_ctl";
    let _cleanup_ctl = coralswarm_backend::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl);
    let mut conn = admin(&base).await;
    let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, ctl);
    recreate(&mut conn, ctl).await;
    let control = control_pool(&base, ctl).await;

    let org_id = format!("org_mop2_{}", Uuid::new_v4());
    mint_org(&control, &org_id, "mop2-org", TenantStatus::Provisioning, "SECRET_MOP2").await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let registry = Arc::new(TenantRegistry::new(Arc::new(StaticMapResolver::new(Default::default()))));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry.clone()));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let clerk_user_id = format!("user_mop2_{}", Uuid::new_v4());
    let req = membership_webhook_request(
        "organizationMembership.created",
        &org_id,
        "Acme",
        &clerk_user_id,
        "bob@wh.test",
        "Bob",
        "Jones",
    );
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "provisioning-status tenant: still 200");
    assert_eq!(
        registry.live_pool_count(),
        0,
        "a provisioning (mid-onboarding) routing row must never cause a pool build attempt"
    );

    control.close().await;
    conn.execute(format!("DROP DATABASE IF EXISTS {ctl} WITH (FORCE)").as_str())
        .await
        .ok();
}

/// The positive control: an ACTIVE tenant with a real, reachable database —
/// the webhook provisions the member's ocean, named from the payload's
/// `first_name`/`last_name` hint, and is idempotent across a second
/// `organizationMembership.updated` delivery for the same member.
#[tokio::test]
async fn webhook_provisions_member_ocean_when_tenant_active_and_is_idempotent() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping webhook_provisions_member_ocean_when_tenant_active_and_is_idempotent: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mop3_ctl", "orgt_mop3_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mop3_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mop3-org", TenantStatus::Active, "SECRET_MOP3").await;

    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    let resolver = StaticMapResolver::from_pairs([("SECRET_MOP3", with_db(&base, tnt))]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry));
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let clerk_user_id = format!("user_mop3_{}", Uuid::new_v4());
    let req1 = membership_webhook_request(
        "organizationMembership.created",
        &org_id,
        "Acme",
        &clerk_user_id,
        "carol@wh.test",
        "Carol",
        "Jones",
    );
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await.expect("user provisioned");
    let oceans = primary_member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1, "exactly one primary member ocean provisioned");
    let name: String = sqlx::query_scalar("SELECT name FROM graphs WHERE id = $1")
        .bind(oceans[0])
        .fetch_one(&tenant)
        .await
        .unwrap();
    assert_eq!(name, "Carol Jones", "named from the webhook payload's first/last name");

    // A second delivery (organizationMembership.updated) for the SAME member
    // must not create a duplicate.
    let req2 = membership_webhook_request(
        "organizationMembership.updated",
        &org_id,
        "Acme",
        &clerk_user_id,
        "carol@wh.test",
        "Carol",
        "Jones",
    );
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    assert_eq!(
        primary_member_oceans(&tenant, user_id).await,
        oceans,
        "a second membership event must not create a second ocean"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Trigger 2 — first-sight on the request path
// ═══════════════════════════════════════════════════════════════════════════

/// Every authenticated `Org` outcome from `finish_binding` provisions the
/// caller's member ocean on first sight, named from the `display_name` hint
/// — driven directly through `TenantRuntime::resolve_binding` (no HTTP/JWT
/// layer needed; `resolve_binding` takes already-resolved
/// `account_id`/`user_id`/`org_claim`, same as `binding_gate_pg.rs`).
#[tokio::test]
async fn first_sight_provisions_member_ocean_on_org_outcome() {
    let Some(base) = base_url() else {
        eprintln!("skipping first_sight_provisions_member_ocean_on_org_outcome: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mop4_ctl", "orgt_mop4_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = "org_mop4";
    let tenant = tenant_pool(&base, tnt, org_id).await;
    let account_id = mint_org(&control, org_id, "mop4-org", TenantStatus::Active, "SECRET_MOP4").await;

    let resolver = StaticMapResolver::from_pairs([("SECRET_MOP4", with_db(&base, tnt))]);
    let rt = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let host = format!("{}.{APEX}", "mop4-org");
    let user_id = Uuid::new_v4();

    let decision = rt
        .resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, user_id, Some("Dana Lee"))
        .await
        .expect("Org outcome");
    assert!(matches!(decision, TenancyDecision::Org(_)));

    let oceans = primary_member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1, "first-sight request-path trigger provisioned the ocean");
    let name: String = sqlx::query_scalar("SELECT name FROM graphs WHERE id = $1")
        .bind(oceans[0])
        .fetch_one(&tenant)
        .await
        .unwrap();
    assert_eq!(name, "Dana Lee");

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// member-private-oceans PR 7: `tenancy::member_ocean::provision_one`'s
/// restore-before-provision fix, exercised directly through first-sight
/// (the trigger most likely to win the race against a rejoining member in
/// production — see that function's own doc comment for why the reconciler
/// sweep, by contrast, essentially never gets there first). An archived
/// `kind='member'` ocean within the 60-day window is RESTORED (same
/// `graphs.id`, `archived_at` cleared, `is_primary` set) rather than shadowed
/// by a fresh insert; the same ocean past day 60 is left alone and a THEN
/// call for that member provisions a genuinely new, distinct graph id.
#[tokio::test]
async fn first_sight_restores_archived_ocean_within_window_but_not_past_it() {
    let Some(base) = base_url() else {
        eprintln!("skipping first_sight_restores_archived_ocean_within_window_but_not_past_it: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mop7_ctl", "orgt_mop7_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = "org_mop7";
    let tenant = tenant_pool(&base, tnt, org_id).await;
    let account_id = mint_org(&control, org_id, "mop7-org", TenantStatus::Active, "SECRET_MOP7").await;
    let resolver = StaticMapResolver::from_pairs([("SECRET_MOP7", with_db(&base, tnt))]);
    let rt = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let host = format!("{}.{APEX}", "mop7-org");

    // ── Within window (day 30): restores the SAME graph id ──────────────────
    let within_window_user = Uuid::new_v4();
    let archived_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary, archived_at) \
         VALUES ($1, 'Gwen (archived)', 'member', $2, false, now() - interval '30 days') \
         RETURNING id",
    )
    .bind(account_id)
    .bind(within_window_user)
    .fetch_one(&tenant)
    .await
    .unwrap();

    let decision = rt
        .resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, within_window_user, Some("Gwen"))
        .await
        .expect("Org outcome");
    assert!(matches!(decision, TenancyDecision::Org(_)));

    let oceans = primary_member_oceans(&tenant, within_window_user).await;
    assert_eq!(oceans, vec![archived_graph_id], "day-30 first-sight RESTORES the archived ocean, same graph id, not a fresh insert");
    let (archived_at, is_primary): (Option<chrono::DateTime<chrono::Utc>>, bool) =
        sqlx::query_as("SELECT archived_at, is_primary FROM graphs WHERE id = $1")
            .bind(archived_graph_id)
            .fetch_one(&tenant)
            .await
            .unwrap();
    assert!(archived_at.is_none() && is_primary, "restored: archived_at cleared, is_primary set");

    // ── Past the window (day 61): left archived, a genuinely NEW ocean ──────
    let past_window_user = Uuid::new_v4();
    let old_graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary, archived_at) \
         VALUES ($1, 'Hank (archived)', 'member', $2, false, now() - interval '61 days') \
         RETURNING id",
    )
    .bind(account_id)
    .bind(past_window_user)
    .fetch_one(&tenant)
    .await
    .unwrap();

    let decision2 = rt
        .resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, past_window_user, Some("Hank"))
        .await
        .expect("Org outcome");
    assert!(matches!(decision2, TenancyDecision::Org(_)));

    let new_oceans = primary_member_oceans(&tenant, past_window_user).await;
    assert_eq!(new_oceans.len(), 1, "day-61: a fresh ocean is provisioned");
    assert_ne!(new_oceans[0], old_graph_id, "day-61: the fresh ocean is NOT the old archived graph id");
    // Read BOTH columns, same as the day-30 arm above: `archived_at` alone
    // would still pass if `provision_one` ever set `is_primary` on the
    // out-of-window row without clearing `archived_at` — leaving the tenant
    // with two `is_primary` member graphs for one owner (the partial unique
    // index does not catch that pair, since it excludes archived rows).
    let (old_archived_at, old_is_primary): (Option<chrono::DateTime<chrono::Utc>>, bool) =
        sqlx::query_as("SELECT archived_at, is_primary FROM graphs WHERE id = $1")
            .bind(old_graph_id)
            .fetch_one(&tenant)
            .await
            .unwrap();
    assert!(
        old_archived_at.is_some() && !old_is_primary,
        "day-61: the old archived ocean is left alone — still archived AND still non-primary"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

/// The TTL cache actually suppresses repeat provisioning attempts: delete the
/// ocean directly (bypassing the app) and re-resolve for the SAME
/// `(org_id, user_id)` — if the cache is doing its job, this does NOT
/// re-insert (the row stays deleted), proving the second `resolve_binding`
/// call issued no provisioning statement at all (an uncached path would have
/// found no conflicting row and happily recreated it). Then evict the cache
/// entry via the test-only hook (simulating the TTL elapsing) and confirm
/// the very next call DOES re-provision.
#[tokio::test]
async fn first_sight_ttl_cache_suppresses_repeat_provisioning() {
    let Some(base) = base_url() else {
        eprintln!("skipping first_sight_ttl_cache_suppresses_repeat_provisioning: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mop5_ctl", "orgt_mop5_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_5 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = "org_mop5";
    let tenant = tenant_pool(&base, tnt, org_id).await;
    let account_id = mint_org(&control, org_id, "mop5-org", TenantStatus::Active, "SECRET_MOP5").await;

    let resolver = StaticMapResolver::from_pairs([("SECRET_MOP5", with_db(&base, tnt))]);
    let rt = TenantRuntime::new(true, APEX, Arc::new(TenantRegistry::new(Arc::new(resolver))));
    let host = format!("{}.{APEX}", "mop5-org");
    let user_id = Uuid::new_v4();

    // ── First call: cache MISS, provisions. ──────────────────────────────────
    rt.resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, user_id, Some("Eve"))
        .await
        .expect("Org outcome");
    let first_oceans = primary_member_oceans(&tenant, user_id).await;
    assert_eq!(first_oceans.len(), 1, "first call provisions");

    // Delete it directly — if the SECOND call still issues a provisioning
    // statement, `ON CONFLICT` would find nothing to conflict with (the row
    // is gone) and would happily insert a fresh one.
    sqlx::query("DELETE FROM graphs WHERE id = $1")
        .bind(first_oceans[0])
        .execute(&tenant)
        .await
        .unwrap();
    assert!(primary_member_oceans(&tenant, user_id).await.is_empty());

    // ── Second call: cache HIT (same process, same TenantRuntime, within
    //    MEMBER_OCEAN_TTL) — must NOT re-provision. ──────────────────────────
    rt.resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, user_id, Some("Eve"))
        .await
        .expect("Org outcome");
    assert!(
        primary_member_oceans(&tenant, user_id).await.is_empty(),
        "a cache HIT must skip the provisioning attempt entirely — the deleted row must stay \
         deleted, proving no INSERT was issued (an uncached path would have recreated it)"
    );

    // ── Evict the cache entry (simulating MEMBER_OCEAN_TTL elapsing) — the
    //    very next call is a cache MISS again and re-provisions. ────────────
    rt.clear_member_ocean_cache_for_test(org_id, user_id);
    rt.resolve_binding(&control, &host, TokenKind::Org(org_id.into()), account_id, user_id, Some("Eve"))
        .await
        .expect("Org outcome");
    assert_eq!(
        primary_member_oceans(&tenant, user_id).await.len(),
        1,
        "after the cache entry expires, the next request re-provisions"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// All three triggers racing concurrently
// ═══════════════════════════════════════════════════════════════════════════

/// The webhook, the request-path first-sight trigger, and a reconciler tick —
/// all three racing to provision the SAME brand-new member concurrently —
/// yield exactly one primary member ocean. This is the convergence property
/// the whole three-trigger design rests on: all three target the identical
/// `uq_graphs_one_primary_per_owner` partial unique index with
/// `ON CONFLICT ... DO NOTHING`, so whichever wins, the other two cleanly
/// no-op.
#[tokio::test]
async fn all_three_triggers_racing_yield_exactly_one_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping all_three_triggers_racing_yield_exactly_one_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mop6_ctl", "orgt_mop6_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_6 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mop6_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mop6-org", TenantStatus::Active, "SECRET_MOP6").await;

    // A brand-new member, known to the control plane (so the reconciler's
    // `account_members` read sees them) but with NO ocean yet.
    let clerk_user_id = format!("user_mop6_{}", Uuid::new_v4());
    let user_id = add_member(&control, account_id, "frank@wh.test", Some("Frank Ocean")).await;
    // The webhook path resolves `user_id` from `clerk_user_id` via
    // `ensure_user` — link them so the webhook's insert targets the SAME
    // control-plane user row the reconciler/first-sight calls already know.
    sqlx::query("UPDATE users SET clerk_user_id = $1 WHERE id = $2")
        .bind(&clerk_user_id)
        .bind(user_id)
        .execute(&control)
        .await
        .unwrap();

    let resolver = StaticMapResolver::from_pairs([("SECRET_MOP6", with_db(&base, tnt))]);
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    let rt = TenantRuntime::new(true, APEX, registry);

    // The SAME `Arc<TenantRuntime>` backs both the webhook path (via
    // `state.tenant_runtime`) and the direct `rt.resolve_binding` call below
    // — sharing one registry/cache is what makes this a genuine race between
    // the two request-adjacent triggers, not two independent processes.
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control.clone())));
    state.tenant_runtime = Some(rt.clone());
    let app = coralswarm_backend::test_support_app(Arc::new(state));

    let host = format!("{}.{APEX}", "mop6-org");
    let webhook_req = membership_webhook_request(
        "organizationMembership.created",
        &org_id,
        "Acme",
        &clerk_user_id,
        "frank@wh.test",
        "Frank",
        "Ocean",
    );
    let wt = worker_tenant(&org_id, tenant.clone());

    let (webhook_resp, first_sight_result, tick_result) = tokio::join!(
        app.oneshot(webhook_req),
        rt.resolve_binding(&control, &host, TokenKind::Org(org_id.clone()), account_id, user_id, Some("Frank Ocean")),
        tick(&wt, &control, None),
    );
    assert_eq!(webhook_resp.unwrap().status(), StatusCode::OK);
    assert!(matches!(first_sight_result, Ok(TenancyDecision::Org(_))));
    tick_result.expect("reconciler tick ok");

    let oceans = primary_member_oceans(&tenant, user_id).await;
    assert_eq!(
        oceans.len(),
        1,
        "all three triggers racing for the same member must yield exactly one primary ocean, \
         got {oceans:?}"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// EnvGuard's own regression proof — no Postgres needed, runs unconditionally
// ═══════════════════════════════════════════════════════════════════════════

/// Proves the exact property `EnvGuard` exists for (CodeRabbit review on
/// PR 4): a panic that unwinds through a live `EnvGuard` still restores the
/// process-global env, so one failing assert in one test can never leak
/// `MEMBER_OCEANS_ENABLED`/`CLERK_WEBHOOK_SECRET` into the next test in this
/// binary. Deliberately not gated on `ORGT_TEST_BASE_URL` — it needs no
/// database, only `EnvGuard` itself, so it always runs (including in a plain
/// `cargo test` with no Postgres configured).
#[tokio::test]
async fn env_guard_restores_env_when_a_panic_unwinds_through_it() {
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    let outcome = AssertUnwindSafe(async {
        let _guard = EnvGuard::acquire().await;
        assert_eq!(
            std::env::var("MEMBER_OCEANS_ENABLED").as_deref(),
            Ok("1"),
            "EnvGuard::acquire must have set the flag before this panics"
        );
        panic!("intentional panic — proving EnvGuard::drop still runs on unwind");
    })
    .catch_unwind()
    .await;

    assert!(
        outcome.is_err(),
        "the inner panic must have propagated as an Err through catch_unwind"
    );
    assert!(
        std::env::var("MEMBER_OCEANS_ENABLED").is_err(),
        "EnvGuard::drop must have restored the env (unset MEMBER_OCEANS_ENABLED) even though \
         its owning future panicked mid-body"
    );
    assert!(
        std::env::var("CLERK_WEBHOOK_SECRET").is_err(),
        "EnvGuard::drop must have restored CLERK_WEBHOOK_SECRET too"
    );
}
