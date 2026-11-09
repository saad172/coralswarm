//! Member-ocean lifecycle — archive + restore (member-private-oceans PR 7,
//! `docs/plans/member-private-oceans.md` §2.6 "Lifecycle"). **Purge is out of
//! scope** (PR 8, its own `MEMBER_OCEAN_PURGE_ENABLED` flag) — nothing in this
//! file ever deletes a row or a blob.
//!
//! Drives the two offboarding writers (`clerk_provision::remove_org_membership`
//! / `revoke_org_access`) and the restore writer (`ensure_org_membership`)
//! through the REAL Svix-signed `POST /v1/webhooks/clerk` endpoint — same
//! pattern as `member_ocean_provisioning_pg.rs` — then calls
//! `workers::member_ocean_reconciler::tick` directly (no HTTP layer needed
//! for the sweep itself, same as `member_ocean_reconciler_pg.rs`) to exercise
//! the archive/restore MATERIALIZATION arms this PR adds.
//!
//! Properties covered:
//!
//! 1. Archive stamps `archived_at` ONCE and clears `is_primary`; a replayed
//!    `organizationMembership.deleted` webhook does not move `offboarded_at`
//!    or `archived_at` (the `COALESCE` replay guard, plan §2.6's central
//!    correctness property).
//! 2. Archive revokes `coding_session_shares` / `meeting_shares` tokens for
//!    the archived ocean.
//! 3. Restore within the 60-day window returns the SAME `graphs.id` with
//!    content intact (an `activity_events` marker row survives).
//! 4. Restore at day 61 does NOT restore — the archived ocean stays archived,
//!    `archived_at` unchanged — and a subsequent rejoin gets a FRESH ocean
//!    (different `graphs.id`), decided purely by the `archived_at` predicate,
//!    never by whether a purge worker has run (there is no purge worker in
//!    this codebase yet).
//! 5. Restore is scoped to the right member AND the right org only — a
//!    sibling member in the same org, and a member of a DIFFERENT org, are
//!    both untouched by another member's archive/restore.
//! 6. A revoked org (`organization.deleted` → `org_revocations`) is never
//!    restored into, and never provisioned into either — enforced by TWO
//!    independent gates, both asserted below. First, BOTH writes in
//!    `ensure_org_membership` are fenced on `NOT EXISTS org_revocations`, so a
//!    rejoin webhook leaves the departed member's
//!    `member_ocean_desired_state` row at `'archived'` (and their
//!    `account_members` row absent). That fence is load-bearing, not
//!    belt-and-braces: `archive_arm` selects work ONLY from
//!    `state = 'archived'`, so flipping the row to `'present'` would silently
//!    DEQUEUE the teardown — the oceans would never be stamped `archived_at`,
//!    `revoke_shares_for_graphs` would never run, and since `org_revocations`
//!    rows are never cleared nothing downstream would ever repair it (live
//!    share tokens outliving the revocation forever). Second, the reconciler's
//!    own `org_revocations` check stops the restore and provision arms — which
//!    cannot substitute for the first gate, because it cannot un-cancel an
//!    archive that was already dequeued. Archiving itself, however, still
//!    proceeds for a revoked org (materializes the intent `revoke_org_access`
//!    already wrote), so the admin-export window remains meaningful right up
//!    to the org's own physical teardown.
//! 7. Share revocation is time-bounded for its RE-check window but not
//!    time-gated for the archive it accompanies — a 90-day-late archive
//!    still revokes shares the moment it finally materializes (pre-existing
//!    coverage, not re-listed above until now).
//! 8. **Org-team-membership PR.** `archive_arm`'s step-6 cascade stamps
//!    `team_members.departing_at` and writes one `membership_events` row per
//!    team for a departed member, exactly once (a second tick is a no-op,
//!    no duplicate rows); a sole-admin cascade is allowed to orphan the team
//!    (never blocked, never wedges the worker); and `restore_arm` — proven
//!    by direct inspection to touch only `graphs` — never resurrects a
//!    `team_members` row on rejoin, mirroring the "DELETE THE GRANT, KEEP
//!    THE DENY" rule `clerk_provision.rs:826` already documents for MCP
//!    write grants.
//!
//! Gated on `ORGT_TEST_BASE_URL` (a base Postgres connection with CREATE
//! DATABASE rights) — same scratch-DB fixture pattern as every other
//! `*_pg.rs` file in this directory.
//!
//! ## Honest CI coverage
//!
//! Wired into `.github/workflows/ci.yml`'s existing `member-oceans` job
//! (alongside `member_ocean_reconciler_pg.rs` / `member_ocean_provisioning_pg.rs`)
//! in THIS PR — see that job's own comment for the pre-existing "an
//! `ORGT_TEST_BASE_URL`-gated suite with no CI job silently `ok`s having done
//! nothing" gap this convention exists to avoid. Verified locally against a
//! real Postgres 17 instance (`ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres`)
//! before this PR was opened — see the PR description for the exact command
//! and result.
//!
//! ## Running locally
//!
//!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//!     cargo test -p coralswarm-backend --test member_ocean_lifecycle_pg -- \
//!     --test-threads=1 --nocapture

#[path = "support/financial_fixture.rs"]
mod financial_fixture;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request as HttpRequest, StatusCode};
use base64::{engine::general_purpose::STANDARD, Engine};
use chrono::{DateTime, Utc};
use coralswarm_backend::store::{PostgresStore, Store};
use coralswarm_backend::tenancy::registry::TenantRegistry;
use coralswarm_backend::tenancy::routing::TenantStatus;
use coralswarm_backend::tenancy::runtime::TenantRuntime;
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

const APEX: &str = "coralswarm.example";
const WEBHOOK_SECRET_B64: &str = "bGlmZWN5Y2xlLXByLTctdGVzdC13ZWJob29rLXNlY3JldA=="; // arbitrary fixed key

/// Serializes every test in this file — same rationale as every other
/// `*_pg.rs` file's `ENV_LOCK` (`MEMBER_OCEANS_ENABLED`/`CLERK_WEBHOOK_SECRET`
/// are process-global; migration 0083's `coralswarm_app` role is cluster-wide).
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
/// duration, restoring both on drop (including on unwind) — same
/// `EnvGuard` pattern as `member_ocean_provisioning_pg.rs`'s, duplicated here
/// per this directory's existing per-file convention rather than shared,
/// since each `*_pg.rs` file already keeps its own copy of this fixture.
#[allow(dead_code)]
struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);

impl EnvGuard {
    async fn acquire() -> Self {
        let guard = ENV_LOCK.lock().await;
        // SAFETY: `guard` serializes every mutation of these two
        // process-global vars in this binary against every other test's, so
        // no concurrent reader can observe them mid-mutation.
        unsafe {
            std::env::set_var("MEMBER_OCEANS_ENABLED", "1");
            std::env::set_var("CLERK_WEBHOOK_SECRET", format!("whsec_{WEBHOOK_SECRET_B64}"));
        }
        Self(guard)
    }
}

impl Drop for EnvGuard {
    fn drop(&mut self) {
        // SAFETY: see `acquire` — `self.0` is still held here.
        unsafe {
            std::env::remove_var("MEMBER_OCEANS_ENABLED");
            std::env::remove_var("CLERK_WEBHOOK_SECRET");
        }
    }
}

/// Temporarily disables `MEMBER_OCEANS_ENABLED` for the duration of `fut`,
/// restoring it afterward. Used to isolate the RECONCILER's restore/provision
/// arms (this PR's subject) from the webhook's own synchronous best-effort
/// trigger (`provision_member_ocean_best_effort`, PR 4 — itself now
/// restore-aware too, PR 7's fix to `tenancy::member_ocean::provision_one`),
/// which would otherwise race ahead and perform the exact same restore/
/// provision INLINE during the webhook call, making it ambiguous which of
/// the two triggers actually did the work. `ensure_org_membership`'s
/// desired-state write is unaffected — it runs unconditionally regardless of
/// this flag (see that function's own doc comment) — only the synchronous
/// provisioning attempt is suppressed. Safe to call only while holding
/// `EnvGuard` (this file's `ENV_LOCK`), same as every other env mutation
/// here.
async fn without_member_oceans_flag<F, T>(fut: F) -> T
where
    F: std::future::Future<Output = T>,
{
    // `Drop`-based restore (CodeRabbit review) — matches `EnvGuard` two
    // functions up: a panicking assert inside `fut` must not leave
    // `MEMBER_OCEANS_ENABLED` unset for the rest of this test (or, worse,
    // for whatever runs next before `EnvGuard::drop` itself fires).
    struct Restore;
    impl Drop for Restore {
        fn drop(&mut self) {
            // SAFETY: the caller still holds `EnvGuard` (`ENV_LOCK`) for the
            // whole of this function's caller's test body, so no other test
            // can observe this var mid-mutation — same invariant as the
            // `remove_var` call just below.
            unsafe { std::env::set_var("MEMBER_OCEANS_ENABLED", "1") };
        }
    }
    // SAFETY: caller holds `EnvGuard` (`ENV_LOCK`) for this test's duration.
    unsafe { std::env::remove_var("MEMBER_OCEANS_ENABLED") };
    let _restore = Restore;
    fut.await
}

/// Mint an org's control-plane account row + `tenant_databases` routing row,
/// `Active` — mirrors `member_ocean_provisioning_pg.rs`'s `mint_org`.
async fn mint_org(control: &PgPool, org_id: &str, slug: &str, secret_ref: &str) -> Uuid {
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
        .bind(TenantStatus::Active.as_str()),
    )
    .await
    .unwrap();
    account_id
}

fn worker_tenant(org_id: &str, pool: PgPool) -> WorkerTenant {
    WorkerTenant {
        tenant: TenantId::Org(org_id.to_string()),
        store: Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn Store>,
        pool,
    }
}

/// Real app wired to a real `TenantRegistry`, so `POST /v1/webhooks/clerk`
/// exercises `handle_membership_upsert`/`handle_membership_deleted`/
/// `handle_org_deleted` end to end, same fixture shape as
/// `member_ocean_provisioning_pg.rs`.
fn webhook_app(control: PgPool, secret_ref_pairs: &[(&str, String)]) -> axum::Router {
    let mut state: AppState = (*coralswarm_backend::test_app_state()).clone();
    // No Clerk key in this suite: membership webhooks use the payload-role
    // fallback, which only `APP_ENV=development` allows (settings PR 2.9).
    state.env.is_dev = true;
    state.db = Some(control.clone());
    state.store = Some(Arc::new(PostgresStore::new(control)));
    let resolver = StaticMapResolver::from_pairs(secret_ref_pairs.iter().cloned());
    let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
    state.tenant_runtime = Some(TenantRuntime::new(true, APEX, registry));
    coralswarm_backend::test_support_app(Arc::new(state))
}

// ── Svix signing + webhook request builders ──────────────────────────────────

fn sign_svix(secret_b64: &str, id: &str, ts: &str, body: &str) -> String {
    let key = STANDARD.decode(secret_b64).unwrap();
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(&key).unwrap();
    mac.update(format!("{id}.{ts}.{body}").as_bytes());
    format!("v1,{}", STANDARD.encode(mac.finalize().into_bytes()))
}

fn signed_request(body_json: serde_json::Value) -> HttpRequest<Body> {
    let body = body_json.to_string();
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

fn membership_upsert_request(event_type: &str, org_id: &str, clerk_user_id: &str, email: &str, name: &str) -> HttpRequest<Body> {
    signed_request(json!({
        "type": event_type,
        "data": {
            "organization": {"id": org_id, "name": "Acme"},
            "public_user_data": {
                "user_id": clerk_user_id,
                "identifier": email,
                "first_name": name,
                "last_name": "",
            },
            "role": "org:member",
        },
    }))
}

fn membership_deleted_request(org_id: &str, clerk_user_id: &str) -> HttpRequest<Body> {
    signed_request(json!({
        "type": "organizationMembership.deleted",
        "data": {
            "organization": {"id": org_id},
            "public_user_data": {"user_id": clerk_user_id},
        },
    }))
}

fn org_deleted_request(org_id: &str) -> HttpRequest<Body> {
    signed_request(json!({ "type": "organization.deleted", "data": { "id": org_id } }))
}

// ── Control/tenant read helpers ───────────────────────────────────────────────

async fn user_id_for_clerk_id(control: &PgPool, clerk_user_id: &str) -> Uuid {
    sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .fetch_one(control)
        .await
        .expect("user provisioned")
}

struct DesiredState {
    state: String,
    offboarded_at: Option<DateTime<Utc>>,
    restored_at: Option<DateTime<Utc>>,
}

async fn desired_state(control: &PgPool, org_id: &str, user_id: Uuid) -> Option<DesiredState> {
    sqlx::query_as::<_, (String, Option<DateTime<Utc>>, Option<DateTime<Utc>>)>(
        "SELECT state, offboarded_at, restored_at FROM member_ocean_desired_state \
         WHERE clerk_org_id = $1 AND user_id = $2",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_optional(control)
    .await
    .unwrap()
    .map(|(state, offboarded_at, restored_at)| DesiredState { state, offboarded_at, restored_at })
}

struct MemberOcean {
    id: Uuid,
    is_primary: bool,
    archived_at: Option<DateTime<Utc>>,
}

/// ALL `kind='member'` graphs owned by `user_id` (not just the primary,
/// non-archived one — `primary_member_oceans` in the sibling test files is
/// deliberately narrower than what this file needs).
async fn member_oceans(tenant: &PgPool, user_id: Uuid) -> Vec<MemberOcean> {
    sqlx::query_as::<_, (Uuid, bool, Option<DateTime<Utc>>)>(
        "SELECT id, is_primary, archived_at FROM graphs \
         WHERE kind = 'member' AND owner_user_id = $1 ORDER BY created_at",
    )
    .bind(user_id)
    .fetch_all(tenant)
    .await
    .unwrap()
    .into_iter()
    .map(|(id, is_primary, archived_at)| MemberOcean { id, is_primary, archived_at })
    .collect()
}

async fn backdate_archived_at(tenant: &PgPool, graph_id: Uuid, days_ago: i64) {
    sqlx::query("UPDATE graphs SET archived_at = now() - make_interval(days => $1::int) WHERE id = $2")
        .bind(days_ago as i32)
        .bind(graph_id)
        .execute(tenant)
        .await
        .unwrap();
}

async fn insert_activity_marker(tenant: &PgPool, account_id: Uuid, graph_id: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO activity_events (account_id, graph_id, event_type) \
         VALUES ($1, $2, 'graph_created') RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn activity_marker_exists(tenant: &PgPool, marker_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>("SELECT EXISTS (SELECT 1 FROM activity_events WHERE id = $1)")
        .bind(marker_id)
        .fetch_one(tenant)
        .await
        .unwrap()
}

/// Plants a `coding_session_shares` row (+ its required `coding_sessions`
/// parent, composite FK `(graph_id, session_id)`) for `graph_id`, returning
/// the share's id.
async fn insert_coding_session_share(tenant: &PgPool, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query(
        "INSERT INTO coding_sessions (graph_id, session_id) VALUES ($1, 'sess-1')",
    )
    .bind(graph_id)
    .execute(tenant)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO coding_session_shares (graph_id, session_id, token, created_by) \
         VALUES ($1, 'sess-1', 'tok-coding-1', $2) RETURNING id",
    )
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn insert_meeting_share(tenant: &PgPool, graph_id: Uuid, created_by: Uuid) -> Uuid {
    sqlx::query_scalar(
        "INSERT INTO meeting_shares (graph_id, source_id, token, created_by) \
         VALUES ($1, 'src-1', 'tok-meeting-1', $2) RETURNING id",
    )
    .bind(graph_id)
    .bind(created_by)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn share_revoked(tenant: &PgPool, table: &str, id: Uuid) -> bool {
    let sql = format!("SELECT revoked_at IS NOT NULL FROM {table} WHERE id = $1");
    sqlx::query_scalar(&sql).bind(id).fetch_one(tenant).await.unwrap()
}

async fn org_revoked(control: &PgPool, org_id: &str) -> bool {
    sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM org_revocations WHERE clerk_org_id = $1)")
        .bind(org_id)
        .fetch_one(control)
        .await
        .unwrap()
}

struct LifecycleLogRow {
    action: String,
    status: String,
    graph_id: Uuid,
    finished_at_is_some: bool,
}

async fn lifecycle_log_rows(control: &PgPool, org_id: &str) -> Vec<LifecycleLogRow> {
    sqlx::query_as::<_, (String, String, Uuid, bool)>(
        "SELECT action, status, graph_id, (finished_at IS NOT NULL) \
         FROM member_ocean_lifecycle_log WHERE clerk_org_id = $1 ORDER BY started_at",
    )
    .bind(org_id)
    .fetch_all(control)
    .await
    .unwrap()
    .into_iter()
    .map(|(action, status, graph_id, finished_at_is_some)| LifecycleLogRow { action, status, graph_id, finished_at_is_some })
    .collect()
}

// ── Team-ocean membership cascade helpers (org-team-membership PR) ───────────
//
// `teams`/`team_members`/`membership_events` are out of this PR's route/store
// scope (owned by a sibling PR) — these helpers write directly to the tenant
// pool, the same "no HTTP layer needed for the sweep itself" approach this
// file already takes for `tick` itself.

/// Creates a `kind='team'` ocean + its `teams` row on the tenant pool,
/// returning the team id. Callers insert their own `team_members` rows
/// explicitly via [`add_team_member`] — `created_by` here is pure provenance
/// (org-team-membership-design.md decision #8: the creator is just the first
/// admin, no durable owner role), not an implicit admin membership.
async fn create_team(tenant: &PgPool, account_id: Uuid, name: &str, created_by: Uuid) -> Uuid {
    let graph_id: Uuid = sqlx::query_scalar(
        "INSERT INTO graphs (account_id, name, kind) VALUES ($1, $2, 'team') RETURNING id",
    )
    .bind(account_id)
    .bind(format!("{name} ocean"))
    .fetch_one(tenant)
    .await
    .unwrap();
    sqlx::query_scalar(
        "INSERT INTO teams (account_id, graph_id, name, created_by) \
         VALUES ($1, $2, $3, $4) RETURNING id",
    )
    .bind(account_id)
    .bind(graph_id)
    .bind(name)
    .bind(created_by)
    .fetch_one(tenant)
    .await
    .unwrap()
}

async fn add_team_member(tenant: &PgPool, team_id: Uuid, user_id: Uuid, role: &str) {
    sqlx::query("INSERT INTO team_members (team_id, user_id, role) VALUES ($1, $2, $3)")
        .bind(team_id)
        .bind(user_id)
        .bind(role)
        .execute(tenant)
        .await
        .unwrap();
}

struct TeamMembership {
    role: String,
    departing_at: Option<DateTime<Utc>>,
}

async fn team_membership(tenant: &PgPool, team_id: Uuid, user_id: Uuid) -> Option<TeamMembership> {
    sqlx::query_as::<_, (String, Option<DateTime<Utc>>)>(
        "SELECT role, departing_at FROM team_members WHERE team_id = $1 AND user_id = $2",
    )
    .bind(team_id)
    .bind(user_id)
    .fetch_optional(tenant)
    .await
    .unwrap()
    .map(|(role, departing_at)| TeamMembership { role, departing_at })
}

/// Non-departed admins remaining on a team — the orphan test's own witness
/// that this worker is willing to drive this count to zero.
async fn live_admin_count(tenant: &PgPool, team_id: Uuid) -> i64 {
    sqlx::query_scalar(
        "SELECT count(*) FROM team_members \
         WHERE team_id = $1 AND role = 'admin' AND departing_at IS NULL",
    )
    .bind(team_id)
    .fetch_one(tenant)
    .await
    .unwrap()
}

struct MembershipEventRow {
    actor_user_id: Option<Uuid>,
    target_user_id: Uuid,
    action: String,
    old_role: Option<String>,
    new_role: Option<String>,
}

async fn membership_events(tenant: &PgPool, team_id: Uuid) -> Vec<MembershipEventRow> {
    sqlx::query_as::<_, (Option<Uuid>, Uuid, String, Option<String>, Option<String>)>(
        "SELECT actor_user_id, target_user_id, action, old_role, new_role \
         FROM membership_events WHERE team_id = $1 ORDER BY created_at",
    )
    .bind(team_id)
    .fetch_all(tenant)
    .await
    .unwrap()
    .into_iter()
    .map(|(actor_user_id, target_user_id, action, old_role, new_role)| MembershipEventRow {
        actor_user_id,
        target_user_id,
        action,
        old_role,
        new_role,
    })
    .collect()
}

// ═══════════════════════════════════════════════════════════════════════════
// 1+2. Archive: stamps once, replay-safe clock, is_primary cleared, shares
//      revoked, two-phase audit rows written.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn archive_stamps_once_replay_safe_and_revokes_shares() {
    let Some(base) = base_url() else {
        eprintln!("skipping archive_stamps_once_replay_safe_and_revokes_shares: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol1_ctl", "orgt_mol1_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_0 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol1_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol1-org", "SECRET_MOL1").await;

    let app = webhook_app(control.clone(), &[("SECRET_MOL1", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol1_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "alice@wh.test", "Alice"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;

    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1, "webhook first-sight already provisioned one member ocean synchronously");
    let graph_id = oceans[0].id;
    assert!(oceans[0].is_primary && oceans[0].archived_at.is_none());

    let share1 = insert_coding_session_share(&tenant, graph_id, user_id).await;
    let share2 = insert_meeting_share(&tenant, graph_id, user_id).await;

    // ── Leave: organizationMembership.deleted ────────────────────────────────
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ds = desired_state(&control, &org_id, user_id).await.expect("desired-state row written");
    assert_eq!(ds.state, "archived");
    let offboarded_at_1 = ds.offboarded_at.expect("offboarded_at set on archive intent");
    assert!(ds.restored_at.is_none());

    // Intent only — the tenant-side graph is NOT archived yet (control-plane
    // write happens even if the tenant DB were unreachable).
    let oceans = member_oceans(&tenant, user_id).await;
    assert!(oceans[0].archived_at.is_none(), "archive is materialized by the reconciler, not the webhook");

    // ── Reconciler materializes the archive ──────────────────────────────────
    let wt = worker_tenant(&org_id, tenant.clone());
    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 1, "exactly one archive this tick");

    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1);
    assert!(oceans[0].archived_at.is_some(), "archived_at stamped");
    assert!(!oceans[0].is_primary, "is_primary cleared on archive");
    let archived_at_1 = oceans[0].archived_at.unwrap();

    assert!(share_revoked(&tenant, "coding_session_shares", share1).await, "coding-session share revoked at archive time");
    assert!(share_revoked(&tenant, "meeting_shares", share2).await, "meeting share revoked at archive time");

    let logs = lifecycle_log_rows(&control, &org_id).await;
    assert_eq!(logs.len(), 1, "one lifecycle-log row for the one archived graph");
    assert_eq!(logs[0].action, "archive");
    assert_eq!(logs[0].status, "completed");
    assert_eq!(logs[0].graph_id, graph_id);
    assert!(logs[0].finished_at_is_some, "completed rows must carry finished_at (0092 CHECK)");

    // ── A second tick is a pure no-op (archived_at IS NULL guards re-entry) ──
    let result2 = tick(&wt, &control, None).await.expect("tick 2 ok");
    assert_eq!(result2.processed, 0);
    let oceans2 = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans2[0].archived_at, Some(archived_at_1), "archived_at unchanged by a second tick");

    // ── Replayed .deleted webhook must NOT move the clock ────────────────────
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ds2 = desired_state(&control, &org_id, user_id).await.unwrap();
    assert_eq!(ds2.state, "archived");
    assert_eq!(ds2.offboarded_at, Some(offboarded_at_1), "COALESCE replay guard: offboarded_at must not move on a replayed .deleted webhook");

    let result3 = tick(&wt, &control, None).await.expect("tick 3 ok");
    assert_eq!(result3.processed, 0, "nothing left to archive after a replay");
    let oceans3 = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans3[0].archived_at, Some(archived_at_1), "archived_at unchanged by the replay + tick");

    control.close().await;
    tenant.close().await;
    let _ = account_id;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 1b. #1396 review: a Clerk-deleted org drops out of every tenant worker's
//     list (`routing::list_active`) EXCEPT the reconciler's, so the archive
//     intents `revoke_org_access` wrote are still carried out by the real
//     scheduling path and the members' share tokens are revoked.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn a_deleted_org_stays_scheduled_for_the_reconciler_only_so_its_shares_are_revoked() {
    use coralswarm_backend::workers::tenant_harness::{list_scheduled_tenants, HarnessConfig};
    let Some(base) = base_url() else {
        eprintln!("skipping a_deleted_org_stays_scheduled_for_the_reconciler_only_so_its_shares_are_revoked: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol1396_ctl", "orgt_mol1396_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_1 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol1396_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mol1396-org", "SECRET_MOL1396").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL1396", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol1396_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "ida@wh.test", "Ida"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let graph_id = member_oceans(&tenant, user_id).await[0].id;
    let share1 = insert_coding_session_share(&tenant, graph_id, user_id).await;
    let share2 = insert_meeting_share(&tenant, graph_id, user_id).await;

    let resp = app.clone().oneshot(org_deleted_request(&org_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let reconciler = coralswarm_backend::workers::member_ocean_reconciler::harness_config();
    let scheduled: Vec<String> = list_scheduled_tenants(&control, &reconciler)
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.clerk_org_id)
        .collect();
    let other: Vec<String> = list_scheduled_tenants(&control, &HarnessConfig::queue_worker("extract", std::time::Duration::from_secs(1)))
        .await
        .unwrap()
        .into_iter()
        .map(|r| r.clerk_org_id)
        .collect();

    // Tick exactly what the reconciler's harness would schedule.
    let wt = worker_tenant(&org_id, tenant.clone());
    let mut processed = 0;
    for org in &scheduled {
        if org == &org_id {
            processed += tick(&wt, &control, None).await.expect("tick ok").processed;
        }
    }
    let oceans = member_oceans(&tenant, user_id).await;
    let coding_revoked = share_revoked(&tenant, "coding_session_shares", share1).await;
    let meeting_revoked = share_revoked(&tenant, "meeting_shares", share2).await;

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    assert!(reconciler.include_revoked_orgs, "the reconciler opts in to revoked orgs");
    assert!(scheduled.contains(&org_id), "the reconciler still schedules the deleted org: {scheduled:?}");
    assert!(!other.contains(&org_id), "every other worker drops the deleted org: {other:?}");
    assert_eq!(processed, 1, "the archive arm ran for the deleted org");
    assert!(oceans[0].archived_at.is_some(), "archived_at stamped for the deleted org's member");
    assert!(coding_revoked, "coding-session share token revoked after organization.deleted");
    assert!(meeting_revoked, "meeting share token revoked after organization.deleted");
}

// ═══════════════════════════════════════════════════════════════════════════
// 3. Restore within the window: same graph id, content intact.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn restore_within_window_returns_same_graph_id_with_content_intact() {
    let Some(base) = base_url() else {
        eprintln!("skipping restore_within_window_returns_same_graph_id_with_content_intact: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol2_ctl", "orgt_mol2_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_2 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol2_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol2-org", "SECRET_MOL2").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL2", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol2_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "bob@wh.test", "Bob"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let original_graph_id = member_oceans(&tenant, user_id).await[0].id;
    let marker = insert_activity_marker(&tenant, account_id, original_graph_id).await;

    // Leave + archive.
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let wt = worker_tenant(&org_id, tenant.clone());
    tick(&wt, &control, None).await.unwrap();
    assert!(member_oceans(&tenant, user_id).await[0].archived_at.is_some());

    // Rejoin within the window (day 30, well inside 60). The webhook's own
    // synchronous trigger is disabled for this call so the RECONCILER's
    // restore arm (this PR's subject) is what's actually being proven here —
    // see `without_member_oceans_flag`'s doc comment.
    backdate_archived_at(&tenant, original_graph_id, 30).await;
    let resp = without_member_oceans_flag(app.clone().oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "bob@wh.test", "Bob")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ds = desired_state(&control, &org_id, user_id).await.unwrap();
    assert_eq!(ds.state, "present");
    assert!(ds.restored_at.is_some());

    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 1, "exactly one restore");

    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1, "no fresh ocean created — the archived one was restored, not replaced");
    assert_eq!(oceans[0].id, original_graph_id, "restore returns the SAME graph id");
    assert!(oceans[0].is_primary && oceans[0].archived_at.is_none());
    assert!(activity_marker_exists(&tenant, marker).await, "content survives the archive/restore round-trip");

    let logs = lifecycle_log_rows(&control, &org_id).await;
    let restore_log = logs.iter().find(|l| l.action == "restore").expect("a restore audit row was written");
    assert_eq!(restore_log.status, "completed");
    assert_eq!(restore_log.graph_id, original_graph_id);

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 4. Day-61: no restore, subsequent rejoin gets a FRESH ocean.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn restore_at_day_61_does_not_restore_and_rejoin_yields_a_fresh_ocean() {
    let Some(base) = base_url() else {
        eprintln!("skipping restore_at_day_61_does_not_restore_and_rejoin_yields_a_fresh_ocean: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol3_ctl", "orgt_mol3_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_3 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol3_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mol3-org", "SECRET_MOL3").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL3", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol3_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "carol@wh.test", "Carol"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let old_graph_id = member_oceans(&tenant, user_id).await[0].id;

    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let wt = worker_tenant(&org_id, tenant.clone());
    tick(&wt, &control, None).await.unwrap();

    // Past the window: 61 days.
    backdate_archived_at(&tenant, old_graph_id, 61).await;

    // Rejoin — webhook's own synchronous trigger disabled, same reasoning as
    // the day-30 test above: this proves the RECONCILER's arms, not whichever
    // trigger got there first.
    let resp = without_member_oceans_flag(app.clone().oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "carol@wh.test", "Carol")))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let result = tick(&wt, &control, None).await.expect("tick ok");
    // Provision arm creates a fresh ocean (1 unit of work); restore arm finds
    // nothing eligible (day-61 boundary), so total processed == 1.
    assert_eq!(result.processed, 1, "one fresh provision, zero restores");

    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 2, "the old archived ocean plus a brand-new one");
    let old = oceans.iter().find(|o| o.id == old_graph_id).expect("old ocean still present");
    assert!(old.archived_at.is_some(), "old ocean stays archived — day-61 boundary, not restored");
    let fresh = oceans.iter().find(|o| o.id != old_graph_id).expect("a fresh ocean was provisioned");
    assert!(fresh.is_primary && fresh.archived_at.is_none());
    assert_ne!(fresh.id, old_graph_id, "day-61 rejoin yields a FRESH graph id, never the old one");

    // The graph state alone proves no restore happened, but a SPURIOUS
    // 'restore' audit row would still pass that check — this file's stated
    // property 4 is specifically that the `archived_at` predicate (not
    // worker timing) decides the day-61 boundary, so the audit trail must
    // agree with the graph state, not just the graph state with itself.
    let logs = lifecycle_log_rows(&control, &org_id).await;
    assert!(
        !logs.iter().any(|l| l.action == "restore"),
        "day-61: no restore was attempted, so no restore audit row may exist"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 5. Scoping: only the right member, only the right org.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn archive_and_restore_are_scoped_to_the_right_member_and_org_only() {
    let Some(base) = base_url() else {
        eprintln!("skipping archive_and_restore_are_scoped_to_the_right_member_and_org_only: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt_a, tnt_b) = ("orgt_mol4_ctl", "orgt_mol4_tnt_a", "orgt_mol4_tnt_b");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt_a, tnt_b] {
        let _database_cleanup_4 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_a = format!("org_mol4a_{}", Uuid::new_v4());
    let org_b = format!("org_mol4b_{}", Uuid::new_v4());
    let tenant_a = tenant_pool(&base, tnt_a, &org_a).await;
    let tenant_b = tenant_pool(&base, tnt_b, &org_b).await;
    mint_org(&control, &org_a, "mol4a-org", "SECRET_MOL4A").await;
    mint_org(&control, &org_b, "mol4b-org", "SECRET_MOL4B").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL4A", with_db(&base, tnt_a)), ("SECRET_MOL4B", with_db(&base, tnt_b))]);

    // Org A: Dave (leaves, then rejoins) and Erin (stays). Org B: Frank
    // (unrelated org, stays) — proves BOTH archive scoping (this test's
    // original property) AND restore scoping (added per CodeRabbit review:
    // without a rejoin/restore step, a restore arm that dropped the
    // `owner_user_id` or `clerk_org_id` predicate — restoring a sibling
    // member's archived ocean, or reaching into org B — would have passed
    // every test in this file).
    let dave_clerk = format!("user_mol4_dave_{}", Uuid::new_v4());
    let erin_clerk = format!("user_mol4_erin_{}", Uuid::new_v4());
    let frank_clerk = format!("user_mol4_frank_{}", Uuid::new_v4());
    for (org, clerk_id, email, name) in [
        (&org_a, &dave_clerk, "dave@wh.test", "Dave"),
        (&org_a, &erin_clerk, "erin@wh.test", "Erin"),
        (&org_b, &frank_clerk, "frank@wh.test", "Frank"),
    ] {
        let resp = app.clone().oneshot(membership_upsert_request("organizationMembership.created", org, clerk_id, email, name)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    let dave = user_id_for_clerk_id(&control, &dave_clerk).await;
    let erin = user_id_for_clerk_id(&control, &erin_clerk).await;
    let frank = user_id_for_clerk_id(&control, &frank_clerk).await;
    let dave_graph = member_oceans(&tenant_a, dave).await[0].id;
    let erin_graph = member_oceans(&tenant_a, erin).await[0].id;
    let frank_graph = member_oceans(&tenant_b, frank).await[0].id;

    // Only Dave leaves.
    let resp = app.clone().oneshot(membership_deleted_request(&org_a, &dave_clerk)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let wt_a = worker_tenant(&org_a, tenant_a.clone());
    let wt_b = worker_tenant(&org_b, tenant_b.clone());
    // Asserting `processed` (not just the end state) proves the sweep did
    // exactly the expected amount of work — org A archives Dave's ocean only,
    // org B has nothing to do — rather than merely leaving the right state
    // behind by coincidence.
    assert_eq!(tick(&wt_a, &control, None).await.unwrap().processed, 1, "org A archives exactly Dave's ocean");
    assert_eq!(tick(&wt_b, &control, None).await.unwrap().processed, 0, "org B has no pending work");

    // Every arm below asserts `.len()` BEFORE indexing `[0]`. `member_oceans`
    // returns EVERY `kind='member'` graph for the owner ordered by
    // `created_at` — and secondary member oceans are legal by design (plan §5;
    // there is deliberately no one-member-ocean-per-owner unique index), so an
    // off-scope arm that inserted an EXTRA ocean for a bystander would leave
    // `[0]` exactly as asserted and pass silently. The expected count here is
    // 1 for each of the three because nothing in this test ever creates a
    // secondary ocean for anyone; it is the exact expected number, not a
    // blanket invariant.
    let dave_oceans = member_oceans(&tenant_a, dave).await;
    assert_eq!(dave_oceans.len(), 1, "Dave's archive stamped his existing ocean, it did not add one");
    assert!(dave_oceans[0].archived_at.is_some(), "Dave's ocean is archived");

    let erin_oceans = member_oceans(&tenant_a, erin).await;
    assert_eq!(erin_oceans.len(), 1, "Dave's archive created no extra ocean for Erin");
    assert_eq!(erin_oceans[0].id, erin_graph);
    assert!(erin_oceans[0].archived_at.is_none(), "Erin (same org, never left) is untouched by Dave's archive");

    let frank_oceans = member_oceans(&tenant_b, frank).await;
    assert_eq!(frank_oceans.len(), 1, "Dave's archive created no extra ocean for Frank");
    assert_eq!(frank_oceans[0].id, frank_graph);
    assert!(frank_oceans[0].archived_at.is_none(), "Frank (different org entirely) is untouched by Dave's archive");

    // ── Restore scoping: Dave rejoins org A; Erin and Frank must be
    //    untouched, and org B's tick must do nothing ──────────────────────
    let resp = without_member_oceans_flag(app.clone().oneshot(membership_upsert_request(
        "organizationMembership.created",
        &org_a,
        &dave_clerk,
        "dave@wh.test",
        "Dave",
    )))
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(tick(&wt_a, &control, None).await.unwrap().processed, 1, "only Dave's restore is pending in org A");
    assert_eq!(tick(&wt_b, &control, None).await.unwrap().processed, 0, "org A's restore must never touch org B");

    let dave_oceans_after = member_oceans(&tenant_a, dave).await;
    assert_eq!(dave_oceans_after.len(), 1, "Dave's restore reused his existing ocean, no fresh one");
    assert_eq!(dave_oceans_after[0].id, dave_graph, "restore returned Dave's SAME graph id");
    assert!(dave_oceans_after[0].is_primary && dave_oceans_after[0].archived_at.is_none());

    let erin_oceans_after = member_oceans(&tenant_a, erin).await;
    assert_eq!(erin_oceans_after.len(), 1, "Dave's restore created no extra ocean for Erin");
    assert_eq!(erin_oceans_after[0].id, erin_graph);
    assert!(erin_oceans_after[0].archived_at.is_none(), "Erin is untouched by Dave's restore");

    let frank_oceans_after = member_oceans(&tenant_b, frank).await;
    assert_eq!(frank_oceans_after.len(), 1, "org A's restore created no extra ocean for Frank");
    assert_eq!(frank_oceans_after[0].id, frank_graph);
    assert!(frank_oceans_after[0].archived_at.is_none(), "Frank (org B) is untouched by org A's restore");

    control.close().await;
    tenant_a.close().await;
    tenant_b.close().await;
    for db in [ctl, tnt_a, tnt_b] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 6. Revoked org: archive still proceeds; restore/provision never do.
// ═══════════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn revoked_org_is_archived_but_never_restored_or_provisioned_into() {
    let Some(base) = base_url() else {
        eprintln!("skipping revoked_org_is_archived_but_never_restored_or_provisioned_into: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol5_ctl", "orgt_mol5_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_5 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol5_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mol5-org", "SECRET_MOL5").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL5", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol5_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "grace@wh.test", "Grace"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let original_graph_id = member_oceans(&tenant, user_id).await[0].id;

    // organization.deleted: whole-org revoke — archives Grace's desired
    // state (revoke_org_access's own intent-write) and tombstones/deletes
    // account_members.
    let resp = app.clone().oneshot(org_deleted_request(&org_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(org_revoked(&control, &org_id).await, "org_revocations row written");
    let ds = desired_state(&control, &org_id, user_id).await.expect("revoke_org_access wrote archive intent");
    assert_eq!(ds.state, "archived");

    // Archive still materializes for a revoked org (admin-export window).
    let wt = worker_tenant(&org_id, tenant.clone());
    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 1, "archive still runs for a revoked org");
    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans[0].id, original_graph_id);
    assert!(oceans[0].archived_at.is_some(), "archived even though the org is revoked");

    // A rejoin webhook for a revoked org. BOTH writes in
    // `ensure_org_membership` are fenced on `NOT EXISTS org_revocations`: the
    // `account_members` INSERT (asserted below) and the
    // `member_ocean_desired_state` upsert (asserted here).
    //
    // This assertion previously expected `'present'`, on the reasoning that
    // the upsert was "documented as unconditional — not the enforcement
    // point" and that the reconciler's own revocation check was the only gate
    // that mattered. That was wrong, and the test was pinning the bug:
    // `archive_arm` selects work ONLY from `state = 'archived'`, so flipping
    // this row back to `'present'` removes the member from the archive queue
    // entirely. Their oceans then never get stamped `archived_at`,
    // `revoke_shares_for_graphs` never runs, and because `org_revocations`
    // rows are never cleared, nothing downstream ever repairs it — live share
    // tokens outlive the revocation permanently. The reconciler's check stops
    // restore and provision, but it cannot un-cancel an archive that was
    // silently dequeued.
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "grace@wh.test", "Grace"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "a replayed webhook for a revoked org must be a clean no-op, not an error");
    let ds2 = desired_state(&control, &org_id, user_id).await.unwrap();
    assert_eq!(
        ds2.state, "archived",
        "a replayed membership webhook must NOT flip a revoked org's member back to 'present' — \
         archive_arm only selects 'archived', so that would silently cancel the teardown"
    );

    let member_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM account_members am JOIN accounts a ON a.id = am.account_id \
         WHERE a.clerk_org_id = $1 AND am.user_id = $2",
    )
    .bind(&org_id)
    .bind(user_id)
    .fetch_one(&control)
    .await
    .unwrap();
    assert_eq!(member_count, 0, "ensure_org_membership must not resurrect a membership in a revoked org");

    let result2 = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result2.processed, 0, "revoked org: reconciler must not restore or provision");

    let oceans2 = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans2.len(), 1, "no fresh ocean provisioned into a revoked org");
    assert_eq!(oceans2[0].id, original_graph_id);
    assert!(oceans2[0].archived_at.is_some(), "the archived ocean was never restored into a revoked org");

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// REGRESSION — a replayed membership webhook must not cancel a teardown.
///
/// The `member_ocean_desired_state` `'present'` upsert in
/// `ensure_org_membership` originally ran UNCONDITIONALLY, while the
/// `account_members` insert twelve lines above it was already fenced on
/// `NOT EXISTS org_revocations`. The gap was exploitable by ordinary Clerk
/// behavior, not by an attacker:
///
///   1. `organization.deleted` → `revoke_org_access` sets every member to
///      `state = 'archived'` and writes the `org_revocations` row.
///   2. Clerk re-delivers (or delivers out of order) an
///      `organizationMembership.created|.updated` for that same org.
///   3. `account_members` correctly stays empty — that path is fenced.
///   4. But the desired-state upsert flipped the row back to `'present'`.
///
/// `archive_arm` selects work ONLY from `state = 'archived'`, so from that
/// moment the member's oceans are never stamped `archived_at` and
/// `revoke_shares_for_graphs` never runs against them. Because
/// `org_revocations` rows are never cleared, nothing downstream repairs it:
/// the archive simply never happens and that member's live share tokens
/// OUTLIVE THE REVOCATION indefinitely.
#[tokio::test]
async fn replayed_membership_webhook_cannot_cancel_a_revoked_orgs_archive_intent() {
    let Some(base) = base_url() else {
        eprintln!("skipping replayed_membership_webhook_cannot_cancel_a_revoked_orgs_archive_intent: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol6_ctl", "orgt_mol6_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_6 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol6_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mol6-org", "SECRET_MOL6").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL6", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol6_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request(
            "organizationMembership.created",
            &org_id,
            &clerk_user_id,
            "heidi@wh.test",
            "Heidi",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    assert_eq!(member_oceans(&tenant, user_id).await.len(), 1);

    // Teardown.
    let resp = app.clone().oneshot(org_deleted_request(&org_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(org_revoked(&control, &org_id).await);
    assert_eq!(
        desired_state(&control, &org_id, user_id).await.expect("archive intent written").state,
        "archived"
    );

    // The replay. Clerk re-delivering a membership event for a deleted org is
    // ordinary at-least-once behavior, not a hostile act — so this must be a
    // clean no-op (200), not a 500 and not a state change.
    for event in ["organizationMembership.created", "organizationMembership.updated"] {
        let resp = app
            .clone()
            .oneshot(membership_upsert_request(event, &org_id, &clerk_user_id, "heidi@wh.test", "Heidi"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "{event} replay must be a clean no-op");

        let ds = desired_state(&control, &org_id, user_id)
            .await
            .expect("desired-state row must still exist after the replay");
        assert_eq!(
            ds.state, "archived",
            "{event} replay flipped a REVOKED org's member back to 'present' — archive_arm only \
             selects 'archived', so this member's oceans would never be archived and their live \
             share tokens would outlive the revocation forever"
        );
        assert!(
            ds.restored_at.is_none(),
            "{event} replay stamped restored_at on a revoked org's member"
        );
    }

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 7. Share revocation is time-BOUNDED but not time-GATED.
// ═══════════════════════════════════════════════════════════════════════════

/// REGRESSION — a late archive still revokes shares, however long ago the
/// member left.
///
/// `archive_arm`'s share-revocation sweep is bounded to members offboarded
/// within `SHARE_REVOKE_RETRY_DAYS` so that its per-tick cost tracks recent
/// churn rather than org age (CodeRabbit: `member_ocean_desired_state`'s
/// `'archived'` rows are never cleared, so an unbounded sweep re-scans every
/// lifetime departure forever). A pure time window, though, would open a hole
/// the plan's §2.6/§A guarantee cannot tolerate: if the tenant DB is
/// unreachable when the member leaves, no tick materializes `archived_at`, and
/// by the time it comes back the offboarding is outside the window — the graph
/// would finally be stamped `archived_at` while its live share tokens were
/// never touched, permanently.
///
/// So the sweep set is a UNION: recently-offboarded members OR the owners of
/// this tick's archive candidates. This test drives the second half — a
/// 90-day-old offboarding whose archive is only materializing now.
#[tokio::test]
async fn a_late_archive_still_revokes_shares_for_a_long_departed_member() {
    let Some(base) = base_url() else {
        eprintln!("skipping a_late_archive_still_revokes_shares_for_a_long_departed_member: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol7_ctl", "orgt_mol7_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_7 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol7_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    mint_org(&control, &org_id, "mol7-org", "SECRET_MOL7").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL7", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol7_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "ivan@wh.test", "Ivan"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let user_id = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let oceans = member_oceans(&tenant, user_id).await;
    assert_eq!(oceans.len(), 1);
    let graph_id = oceans[0].id;

    let share1 = insert_coding_session_share(&tenant, graph_id, user_id).await;
    let share2 = insert_meeting_share(&tenant, graph_id, user_id).await;

    // Ivan leaves. The control-plane intent lands; the tenant-side archive
    // does not, because (in the scenario this pins) the tenant DB is down.
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(member_oceans(&tenant, user_id).await[0].archived_at.is_none(), "not archived yet — intent only");

    // Ninety days pass with no successful tick. This is the ONLY thing the
    // test fakes: `offboarded_at` is the clock the retry window reads.
    sqlx::query("UPDATE member_ocean_desired_state SET offboarded_at = now() - interval '90 days' WHERE clerk_org_id = $1 AND user_id = $2")
        .bind(&org_id)
        .bind(user_id)
        .execute(&control)
        .await
        .unwrap();

    // The tenant comes back and the sweep finally runs.
    let wt = worker_tenant(&org_id, tenant.clone());
    assert_eq!(tick(&wt, &control, None).await.expect("tick ok").processed, 1, "the archive is materialized, 90 days late");
    assert!(member_oceans(&tenant, user_id).await[0].archived_at.is_some(), "archived_at stamped");

    assert!(
        share_revoked(&tenant, "coding_session_shares", share1).await,
        "a 90-day-late archive must still revoke the coding-session share — the retry window bounds \
         RE-checks, it must not gate the revocation that accompanies the archive itself"
    );
    assert!(
        share_revoked(&tenant, "meeting_shares", share2).await,
        "a 90-day-late archive must still revoke the meeting share"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// 8. Team-ocean membership cascade (org-team-membership PR) — archive_arm's
//    "step 3b". `teams`/`team_members`/`membership_events` live in a migration a
//    sibling PR lands in parallel; the helpers above write those tables
//    directly rather than going through any route (out of this PR's scope).
// ═══════════════════════════════════════════════════════════════════════════

/// Archive cascades a departed member out of every team they were on, stamps
/// `departing_at` exactly once, and writes exactly one `membership_events`
/// row per team — and a second tick (a pure replay of the same desired
/// state, no new departure) writes no duplicate rows and moves nothing.
#[tokio::test]
async fn team_membership_cascade_stamps_once_and_writes_one_event_per_team_no_duplicates_on_replay() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping team_membership_cascade_stamps_once_and_writes_one_event_per_team_no_duplicates_on_replay: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol8_ctl", "orgt_mol8_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_8 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol8_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol8-org", "SECRET_MOL8").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL8", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol8_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "jack@wh.test", "Jack"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let jack = user_id_for_clerk_id(&control, &clerk_user_id).await;

    // A co-admin on team2 so team2's cascade does NOT orphan it — this test's
    // subject is the stamp + audit-row properties, not the orphan property
    // (covered separately below).
    let co_admin_clerk = format!("user_mol8_coadmin_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &co_admin_clerk, "kim@wh.test", "Kim"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let kim = user_id_for_clerk_id(&control, &co_admin_clerk).await;

    let team1 = create_team(&tenant, account_id, "Team One", jack).await;
    let team2 = create_team(&tenant, account_id, "Team Two", jack).await;
    add_team_member(&tenant, team1, jack, "member").await;
    add_team_member(&tenant, team2, jack, "admin").await;
    add_team_member(&tenant, team2, kim, "admin").await;

    // ── Jack leaves the org ──────────────────────────────────────────────────
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let wt = worker_tenant(&org_id, tenant.clone());
    let result = tick(&wt, &control, None).await.expect("tick ok");
    assert_eq!(result.processed, 1, "exactly one graph archived (Jack's member ocean) — cascade doesn't affect this count");

    // team1: Jack was the sole membership on this team.
    let m1 = team_membership(&tenant, team1, jack).await.expect("Jack's team1 row still exists");
    assert!(m1.departing_at.is_some(), "team1: departing_at stamped");
    assert_eq!(m1.role, "member", "role column itself is untouched by the cascade — only departing_at moves");

    let events1 = membership_events(&tenant, team1).await;
    assert_eq!(events1.len(), 1, "exactly one membership_events row for team1");
    assert_eq!(events1[0].action, "cascade_removed");
    assert_eq!(events1[0].actor_user_id, None, "no human actor — this is a system-driven cascade");
    assert_eq!(events1[0].target_user_id, jack);
    assert_eq!(events1[0].old_role.as_deref(), Some("member"));
    assert_eq!(events1[0].new_role, None);

    // team2: Jack was an admin alongside Kim.
    let m2 = team_membership(&tenant, team2, jack).await.expect("Jack's team2 row still exists");
    assert!(m2.departing_at.is_some(), "team2: departing_at stamped");

    let events2 = membership_events(&tenant, team2).await;
    assert_eq!(events2.len(), 1, "exactly one membership_events row for team2 (Jack only — Kim is untouched)");
    assert_eq!(events2[0].action, "cascade_removed");
    assert_eq!(events2[0].actor_user_id, None);
    assert_eq!(events2[0].target_user_id, jack);
    assert_eq!(events2[0].old_role.as_deref(), Some("admin"));
    assert_eq!(events2[0].new_role, None);

    // Kim (still a live org member, never departed) is completely untouched.
    let kim_m2 = team_membership(&tenant, team2, kim).await.expect("Kim's team2 row still exists");
    assert!(kim_m2.departing_at.is_none(), "Kim was never offboarded — her membership must be untouched");

    let jack_departing_at_1 = m1.departing_at.unwrap();

    // ── A second tick is a pure no-op: no re-stamping, no duplicate events ──
    let result2 = tick(&wt, &control, None).await.expect("tick 2 ok");
    assert_eq!(result2.processed, 0, "second tick: nothing left to archive");

    let m1_again = team_membership(&tenant, team1, jack).await.unwrap();
    assert_eq!(m1_again.departing_at, Some(jack_departing_at_1), "departing_at unchanged by a second tick");
    assert_eq!(membership_events(&tenant, team1).await.len(), 1, "no duplicate membership_events row on team1 after a second tick");
    assert_eq!(membership_events(&tenant, team2).await.len(), 1, "no duplicate membership_events row on team2 after a second tick");

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// A sole team admin leaving the org is allowed to orphan the team — the
/// cascade must not skip, error, or otherwise wedge on this case
/// (org-team-membership-design.md decision #9: the last-admin invariant is
/// enforced only at the interactive `remove_team_member` API, never here).
#[tokio::test]
async fn team_membership_cascade_allows_orphaning_a_team_with_only_one_admin() {
    let Some(base) = base_url() else {
        eprintln!("skipping team_membership_cascade_allows_orphaning_a_team_with_only_one_admin: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol9_ctl", "orgt_mol9_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_9 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol9_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol9-org", "SECRET_MOL9").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL9", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol9_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "liz@wh.test", "Liz"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let liz = user_id_for_clerk_id(&control, &clerk_user_id).await;

    let solo_team = create_team(&tenant, account_id, "Solo Admin Team", liz).await;
    add_team_member(&tenant, solo_team, liz, "admin").await;
    assert_eq!(live_admin_count(&tenant, solo_team).await, 1, "sanity: Liz starts as the only admin");

    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let wt = worker_tenant(&org_id, tenant.clone());
    // The important assertion here is simply that this returns `Ok` at all —
    // a guard/constraint that blocked the cascade to protect the last-admin
    // invariant would surface as an `Err` (a failed UPDATE) or a stuck tick,
    // neither of which this call tolerates.
    let result = tick(&wt, &control, None).await.expect("tick must succeed even though it orphans a team");
    assert_eq!(result.processed, 1, "Liz's member ocean was archived");

    let m = team_membership(&tenant, solo_team, liz).await.expect("Liz's team row still exists — cascade does not delete it");
    assert!(m.departing_at.is_some(), "Liz is cascaded out despite being the only admin");
    assert_eq!(m.role, "admin", "role value is preserved as historical record — only departing_at moves");

    assert_eq!(
        live_admin_count(&tenant, solo_team).await,
        0,
        "the team is now orphaned (zero non-departed admins) — this is the INTENDED outcome, not a bug"
    );

    let events = membership_events(&tenant, solo_team).await;
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].action, "cascade_removed");
    assert_eq!(events[0].old_role.as_deref(), Some("admin"));
    assert_eq!(events[0].new_role, None);

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// The restore arm rejoining an org must NOT resurrect team membership — the
/// exact "DELETE THE GRANT, KEEP THE DENY" pattern `clerk_provision.rs:826`
/// already documents for MCP write grants (org-team-membership-design.md,
/// "Rejected" list: "Auto-restore team membership on org rejoin").
#[tokio::test]
async fn restore_arm_does_not_resurrect_team_membership() {
    let Some(base) = base_url() else {
        eprintln!("skipping restore_arm_does_not_resurrect_team_membership: set ORGT_TEST_BASE_URL");
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol10_ctl", "orgt_mol10_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_10 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol10_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol10-org", "SECRET_MOL10").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL10", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol10_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &clerk_user_id, "mona@wh.test", "Mona"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let mona = user_id_for_clerk_id(&control, &clerk_user_id).await;
    let original_graph_id = member_oceans(&tenant, mona).await[0].id;

    // A co-admin so the team isn't ALSO exercising the orphan path here —
    // this test's subject is restore-not-resurrecting, not orphaning.
    let co_admin_clerk = format!("user_mol10_coadmin_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request("organizationMembership.created", &org_id, &co_admin_clerk, "nora@wh.test", "Nora"))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let nora = user_id_for_clerk_id(&control, &co_admin_clerk).await;

    let team = create_team(&tenant, account_id, "Mona's Team", mona).await;
    add_team_member(&tenant, team, mona, "admin").await;
    add_team_member(&tenant, team, nora, "admin").await;

    // Leave + archive + cascade.
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let wt = worker_tenant(&org_id, tenant.clone());
    tick(&wt, &control, None).await.expect("tick 1 ok");

    let m_after_archive = team_membership(&tenant, team, mona).await.expect("Mona's team row still exists");
    assert!(m_after_archive.departing_at.is_some(), "cascade stamped departing_at on archive");
    assert_eq!(membership_events(&tenant, team).await.len(), 1, "one cascade event written");

    // Rejoin within the restore window (day 30) — same pattern as
    // `restore_within_window_returns_same_graph_id_with_content_intact`: the
    // webhook's own synchronous trigger is disabled so this exercises the
    // RECONCILER's restore arm specifically.
    backdate_archived_at(&tenant, original_graph_id, 30).await;
    let resp = without_member_oceans_flag(app.clone().oneshot(membership_upsert_request(
        "organizationMembership.created",
        &org_id,
        &clerk_user_id,
        "mona@wh.test",
        "Mona",
    )))
    .await
    .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let result = tick(&wt, &control, None).await.expect("tick 2 ok");
    assert_eq!(result.processed, 1, "exactly one restore — Mona's member ocean");

    // Mona's org access (member ocean) IS restored — same-graph-id property
    // this file already proves elsewhere, re-asserted briefly here as
    // context for the contrast below.
    let oceans_after = member_oceans(&tenant, mona).await;
    assert_eq!(oceans_after.len(), 1);
    assert_eq!(oceans_after[0].id, original_graph_id);
    assert!(oceans_after[0].archived_at.is_none(), "Mona's member ocean is restored");

    // Mona's TEAM membership is NOT restored — this is the property under test.
    let m_after_restore = team_membership(&tenant, team, mona).await.expect("row still exists — restore doesn't delete it either");
    assert_eq!(
        m_after_restore.departing_at, m_after_archive.departing_at,
        "restore_arm must not clear team_members.departing_at — rejoining restores org access, not team oceans"
    );
    assert_eq!(
        membership_events(&tenant, team).await.len(),
        1,
        "restore must not write a new membership_events row ('added' or otherwise) — only an admin re-adding does that"
    );

    // Nora (never departed) is untouched throughout.
    let nora_m = team_membership(&tenant, team, nora).await.expect("Nora's row exists");
    assert!(nora_m.departing_at.is_none());

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}

/// The single most valuable test in this cascade suite (CodeRabbit gap): the
/// step-3b cascade inside `archive_arm` is deliberately placed BEFORE that
/// function's `if candidates.is_empty() { return 0; }` early return, and
/// scoped to the FULL `archived_users` set rather than `candidates`' owners
/// — see `archive_arm`'s own doc comment, "3b. Team-ocean membership
/// cascade", for the full rationale. That placement exists for exactly one
/// scenario: a member whose member-ocean graphs were ALREADY archived on an
/// EARLIER tick — so `candidates` is EMPTY for them on the tick under test —
/// but who still holds an active `team_members` row. That is precisely the
/// shape of everyone who left the org before this cascade feature shipped:
/// a one-time backfill, for free, on the very next tick after this lands. A
/// cascade placed AFTER the guard would never run for such a member on any
/// future tick, silently leaving their team memberships live forever.
///
/// Reproduces that shape directly, in order:
///   1. Priya leaves the org with NO team membership yet — her ocean
///      archives on tick 1, and the cascade correctly has nothing to do.
///   2. A `team_members` row for Priya is inserted directly AFTERWARD —
///      standing in for a membership that existed all along but that the
///      pre-cascade code never swept (the backfill population this test's
///      name refers to). Her graphs are already archived at this point.
///   3. Tick 2 must still stamp `departing_at` and write exactly one
///      `cascade_removed` `membership_events` row for her, even though ZERO
///      graphs are archived on that tick (`candidates` is empty) — the exact
///      case a cascade placed after the early-return guard would silently
///      skip forever.
#[tokio::test]
async fn archive_arm_backfills_the_cascade_for_a_member_already_archived_before_this_feature_shipped() {
    let Some(base) = base_url() else {
        eprintln!(
            "skipping archive_arm_backfills_the_cascade_for_a_member_already_archived_before_this_feature_shipped: \
             set ORGT_TEST_BASE_URL"
        );
        return;
    };
    let _env_guard = EnvGuard::acquire().await;

    let (ctl, tnt) = ("orgt_mol11_ctl", "orgt_mol11_tnt");
    let mut conn = admin(&base).await;
    for db in [ctl, tnt] {
        let _database_cleanup_11 = coralswarm_backend::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&mut conn, db).await;
    }
    let control = control_pool(&base, ctl).await;
    let org_id = format!("org_mol11_{}", Uuid::new_v4());
    let tenant = tenant_pool(&base, tnt, &org_id).await;
    let account_id = mint_org(&control, &org_id, "mol11-org", "SECRET_MOL11").await;
    let app = webhook_app(control.clone(), &[("SECRET_MOL11", with_db(&base, tnt))]);

    let clerk_user_id = format!("user_mol11_{}", Uuid::new_v4());
    let resp = app
        .clone()
        .oneshot(membership_upsert_request(
            "organizationMembership.created",
            &org_id,
            &clerk_user_id,
            "priya@wh.test",
            "Priya",
        ))
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let priya = user_id_for_clerk_id(&control, &clerk_user_id).await;

    // A team Priya will (later) turn out to have been on — created now, but
    // deliberately NOT populated with her membership yet. That ordering is
    // the whole point: on tick 1 she has ZERO `team_members` rows, so this
    // tick's cascade is correctly a no-op for her — there is nothing to
    // backfill yet.
    let team = create_team(&tenant, account_id, "Priya's Team", priya).await;

    // ── Priya leaves the org; her member ocean archives on tick 1 ──────────
    let resp = app.clone().oneshot(membership_deleted_request(&org_id, &clerk_user_id)).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let wt = worker_tenant(&org_id, tenant.clone());
    let result1 = tick(&wt, &control, None).await.expect("tick 1 ok");
    assert_eq!(result1.processed, 1, "tick 1: Priya's member ocean is archived");
    assert_eq!(
        membership_events(&tenant, team).await.len(),
        0,
        "tick 1 sanity check: no team_members row exists yet, so the cascade has nothing to do"
    );

    // ── Backfill scenario: Priya's team membership existed all along, but
    // (in the pre-this-feature world this reproduces) nothing ever swept it
    // when she left. Inserted directly, AFTER the archive already happened —
    // exactly like a membership that predates this cascade shipping at all.
    // Her graphs are already archived, so on the NEXT tick `candidates` will
    // be empty for her; this row is the only witness that the cascade must
    // still run.
    add_team_member(&tenant, team, priya, "admin").await;

    // ── Tick 2: zero graphs to archive (already done on tick 1), but the
    // backfill cascade must still stamp departing_at and write the audit
    // row. If the cascade were ever moved to AFTER
    // `candidates.is_empty() { return 0; }`, this tick would short-circuit
    // before ever reaching it and both assertions below would fail — that is
    // the regression this test exists to catch.
    let result2 = tick(&wt, &control, None).await.expect("tick 2 ok");
    assert_eq!(
        result2.processed, 0,
        "tick 2: no graphs are archived this tick (Priya's ocean was already archived on tick 1) \
         — the cascade below must still fire despite this being zero"
    );

    let m = team_membership(&tenant, team, priya)
        .await
        .expect("Priya's backfilled team_members row still exists");
    assert!(
        m.departing_at.is_some(),
        "the backfill cascade must stamp departing_at even though `candidates` was empty on this \
         tick — this is the property a cascade placed after the early-return guard would silently \
         lose"
    );
    assert_eq!(
        m.role, "admin",
        "role is preserved as historical record — only departing_at moves"
    );

    let events = membership_events(&tenant, team).await;
    assert_eq!(
        events.len(),
        1,
        "the backfill cascade must write exactly one membership_events row for Priya's team"
    );
    assert_eq!(events[0].action, "cascade_removed");
    assert_eq!(events[0].actor_user_id, None, "no human actor — this is a system-driven backfill cascade");
    assert_eq!(events[0].target_user_id, priya);
    assert_eq!(events[0].old_role.as_deref(), Some("admin"));
    assert_eq!(events[0].new_role, None);

    // ── A third tick is a pure no-op — the backfill itself must not
    // re-stamp or write a duplicate event, the same idempotency property as
    // the ordinary (non-backfill) cascade already proven above in this file.
    let result3 = tick(&wt, &control, None).await.expect("tick 3 ok");
    assert_eq!(result3.processed, 0, "tick 3: still nothing to archive");
    let m_again = team_membership(&tenant, team, priya).await.unwrap();
    assert_eq!(
        m_again.departing_at, m.departing_at,
        "departing_at must be unchanged by a third tick"
    );
    assert_eq!(
        membership_events(&tenant, team).await.len(),
        1,
        "no duplicate membership_events row on a third tick"
    );

    control.close().await;
    tenant.close().await;
    for db in [ctl, tnt] {
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }
}
