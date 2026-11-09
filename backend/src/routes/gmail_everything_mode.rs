//! Gmail everything-mode opt-in (gsuite enterprise train PR G2.4).
//!
//! `docs/connectors/CONNECTOR_CONTRACT.md` §4 ("Two rules that fall out of
//! the matrix"), §7 ("Everything-mode carve-out"), §6 (amended conformance
//! test #9): the Mailbox archetype confines Gmail to per-thread opt-in
//! (`routes::connectors::ingest_selected`) by default. Everything mode is
//! the one permitted exception — a whole-mailbox pull — gated on THREE hard
//! requirements, none of them negotiable:
//!
//! 1. **the mailbox owner configures it themselves** — never an admin,
//!    never a default. This file's whole job is enforcing that: see
//!    [`set_everything_mode`]'s actor==target check below.
//! 2. **content lands exclusively in that owner's private ocean** —
//!    `resolve_primary_graph(.., IngestCaller::Attributable(owner))`.
//! 3. **onward sharing only via an explicit I3 promotion** — nothing here
//!    or in `workers::gmail_everything_mode` writes to any ocean but the
//!    owner's own.
//!
//! # This module MIRRORS `routes::outlook_everything_mode` — the precedent
//! # it consumes
//!
//! Founder decision (`.scratch/gsuite-enterprise/plan.md`): "Q2 = NO — G2.4
//! waits for Outlook 3.4." m365 enterprise train PR 3.4 (merged #700) shipped
//! the FIRST implementation of this carve-out and the contract's amended
//! test #9; every structural choice in this file is a port of that one, not
//! an independent design — see [`set_everything_mode`]'s own doc for the one
//! place worth calling out explicitly. Where this file's shape differs from
//! the pre-#700 design sketch (`.scratch/gsuite-enterprise/g2-design.md` §4,
//! which guessed a `POST /v1/connectors/google/gmail/mode` route taking
//! `{"mode": "threads"|"everything"}`), the merged Outlook precedent wins:
//! this file uses Outlook's own boolean `{"enabled": bool}` shape and route
//! name (`/v1/connectors/google/gmail/everything-mode`) instead, for
//! consistency across the two mailbox connectors rather than inventing a
//! second shape for the same concept.
//!
//! Routes (bearer-authed, account+user scoped — same shape as
//! `routes::connectors`/`routes::outlook_everything_mode`):
//! * `POST /v1/connectors/google/gmail/everything-mode` — enable/disable for
//!   the CALLER'S OWN mailbox only (`{"enabled": bool}` body, no target-user
//!   field exists at all).
//! * `GET  /v1/connectors/google/gmail/everything-mode` — current state for
//!   the caller's own mailbox (`{"enabled": bool}`; `false` when no row
//!   exists, which is the fresh-connection default).
//!
//! Org tenancy: reuses the SAME per-org `org_connector_flags` registry flag
//! `ORG_GOOGLE_GMAIL_ENABLED` (`routes::oauth::org_google_gmail_enabled_for`,
//! provider `"google_gmail"`, gsuite Train G2 PR 6) every other Gmail-org
//! route already gates on — one Gmail flag, not a second, everything-mode-
//! specific one. Flag off (the zero-row default): an org member gets the
//! same clean 403 every other Gmail-org-gated route already returns. Flag
//! on: `tenant.pool` resolves to the org's own TENANT pool and
//! `set_everything_mode` writes there, into the org member's own private
//! ocean — never a team ocean (`resolve_primary_graph`'s org arm has no such
//! destination to pick). Personal plane: always allowed, same as every
//! other connector route.
//!
//! The actual whole-mailbox pull driven by this opt-in lives in
//! `workers::gmail_everything_mode` — this file only owns the toggle and its
//! enforcement.
//!
//! # RLS fix applied before merge (adversarial review, 2026-08-25) — the
//! # "Personal plane: always allowed" claim above used to be FALSE
//!
//! Migration `0192`'s (Outlook's) `WITH CHECK` — ported verbatim into this
//! table's own first draft — required `graphs.kind = 'member'`. On the
//! Personal plane, `resolve_primary_graph` never resolves a `kind='member'`
//! graph at all (member oceans are an Org-only concept; Personal's own arm
//! resolves `kind='personal', owner_user_id IS NULL` instead) — so
//! `set_everything_mode`'s enabling INSERT would ALWAYS violate RLS under
//! the real, NOBYPASSRLS `coralswarm_app` role: a Personal-plane `POST
//! .../everything-mode {"enabled":true}` 500s, unconditionally, in
//! production. Confirmed against real Postgres with the real role before
//! merge (this file's own pg tests, run against the table-OWNER role until
//! this fix, could not have caught it — see [`set_everything_mode`]'s own
//! doc for that fix too). Migration `0197`'s `WITH CHECK` now carries a
//! second arm for `kind='personal' AND is_primary=true` — see that
//! migration's own header for the full incident. **This exact defect is
//! inherited, byte-identical, in Outlook's own merged `0192`/#700** — not
//! this PR's responsibility to fix on that connector, but recorded here so
//! a follow-up can fix both together rather than rediscovering it twice.

use std::sync::Arc;

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    middleware::auth::UserContext,
    routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    tenancy::extractor::Tenant,
    AppState,
};

/// Derive [`AccountPlane`] from an already-gated `Tenant` — identical shape
/// to `routes::connectors::plane_of`/`routes::outlook_everything_mode::
/// plane_of`. Every call site here runs AFTER
/// [`reject_org_everything_mode_access`], so an `Org` `TenancyDecision`
/// reaching this point has already had that gate's flag confirmed on for its
/// org.
fn plane_of(tenant: &Tenant) -> AccountPlane {
    match tenant.org_context() {
        Some(_) => AccountPlane::Org,
        None => AccountPlane::Personal,
    }
}

/// Fail-closed gate for these routes under an org `TenancyDecision` — reuses
/// the SAME registry flag every other Gmail-org route already gates on
/// (`routes::oauth::org_google_gmail_enabled_for`, gsuite Train G2 PR 6),
/// not a fourth, everything-mode-specific env var. Flag off: refuse before
/// any pool resolution or DB write. Personal / flag-off requests are
/// byte-identical to before this PR.
async fn reject_org_everything_mode_access(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::routes::oauth::org_google_gmail_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "gmail everything-mode route refused under org context (org_connector_flags resolved \
         off for provider \"google_gmail\" for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "Gmail everything mode is available in your personal workspace only, \
                          or once your organization admin enables Gmail connectors"
            })),
        )
            .into_response(),
    )
}

/// Failure modes for [`set_everything_mode`]/[`everything_mode_status`],
/// mapped to precise HTTP statuses by [`EverythingModeError::into_response`].
/// Byte-identical shape to `routes::outlook_everything_mode::
/// EverythingModeError`.
#[derive(Debug)]
pub(crate) enum EverythingModeError {
    /// The acting user is not the mailbox owner they are trying to
    /// enable/disable everything mode for — contract §7 condition (a). → `403`.
    ActorMismatch,
    /// `resolve_primary_graph` could not resolve the target owner's private
    /// ocean. → `503`.
    OceanUnresolved(String),
    /// A DB-layer failure opening the scoped transaction or running the
    /// upsert/read. → `500`.
    Db(String),
}

impl EverythingModeError {
    fn into_response(self) -> Response {
        match self {
            EverythingModeError::ActorMismatch => (
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": "everything mode can only be enabled or disabled by the mailbox's \
                              own owner"
                })),
            )
                .into_response(),
            EverythingModeError::OceanUnresolved(e) => {
                tracing::warn!(error = %e, "gmail_everything_mode: could not resolve owner's private ocean");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "your private workspace is not ready yet — try again shortly"})),
                )
                    .into_response()
            }
            EverythingModeError::Db(e) => {
                tracing::error!(error = %e, "gmail_everything_mode: db failure");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "failed to update everything-mode state"})),
                )
                    .into_response()
            }
        }
    }
}

/// Enable/disable everything mode for `target_user_id`'s mailbox, acting as
/// `actor_user_id`. **The load-bearing check is the first line**: refuses
/// unconditionally the instant the actor is not the target — an admin, a
/// teammate, or any other non-owner caller gets
/// [`EverythingModeError::ActorMismatch`] before touching the database at
/// all, regardless of what role `actor_user_id` holds. Byte-identical
/// enforcement shape to `routes::outlook_everything_mode::
/// set_everything_mode` — the ONLY genuine deviation from that precedent is
/// that disabling here still opens a scoped transaction unconditionally
/// (Gmail's `gmail_everything_mode` row is anchored on `graph_id` exactly
/// like Outlook's, so the identical "disable must never depend on
/// `resolve_primary_graph`" invariant applies and is preserved below — this
/// is not a deviation from the precedent, just restated so it is obviously
/// present here too).
///
/// On success: resolves `target_user_id`'s own primary private ocean
/// (`resolve_primary_graph`, `IngestCaller::Attributable` — never
/// `NonAttributable`, never a team-ocean destination) and upserts the
/// `gmail_everything_mode` row, returning the persisted `enabled` value.
/// Idempotent: enabling an already-enabled mailbox (or disabling an
/// already-disabled/never-enabled one) is a normal upsert, not an error.
pub(crate) async fn set_everything_mode(
    pool: &PgPool,
    account_id: Uuid,
    plane: AccountPlane,
    actor_user_id: Uuid,
    target_user_id: Uuid,
    enabled: bool,
) -> Result<bool, EverythingModeError> {
    if actor_user_id != target_user_id {
        tracing::warn!(
            %account_id, %actor_user_id, %target_user_id,
            "gmail_everything_mode: refusing to set everything mode for another user's mailbox \
             — owner-self-configured only (contract §7 carve-out, condition 1)"
        );
        return Err(EverythingModeError::ActorMismatch);
    }

    // Disabling must NEVER depend on ocean resolution — same reasoning
    // `routes::outlook_everything_mode::set_everything_mode` documents
    // (CodeRabbit round 1 on PR #700): the off switch is the one control the
    // contract gives the owner, and a deprovisioned/not-yet-reconciled
    // private ocean must not hold it hostage while the worker keeps pulling
    // the mailbox. Only ENABLING needs a destination graph.
    //
    // Also clears `resume_cursor`/`last_error`/`last_error_at` (F3, fixed
    // before merge — adversarial review): nothing else in this table's
    // lifecycle ever clears `resume_cursor` — `mark_failed_no_progress`
    // never touches it, and a failed `record_tick_end` write rewrites the
    // SAME value back. Without clearing it here, enable → walk to some
    // `pageToken` → disable → re-enable a week later would resume from a
    // WEEK-OLD cursor Gmail may well now reject outright (the worker's
    // restart-once self-heal covers THAT case,
    // but there is no reason to make a legitimate re-enable pay for a stale
    // cursor at all when disabling is the natural, cheap place to reset it).
    // `last_error`/`last_error_at` are cleared alongside it for the same
    // "start clean" reason — stale error state from before the disable
    // would otherwise persist across a re-enable with no code path that
    // ever clears it either.
    if !enabled {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
            .await
            .map_err(|e| EverythingModeError::Db(e.to_string()))?;
        sqlx::query(
            "UPDATE gmail_everything_mode \
             SET enabled = FALSE, resume_cursor = NULL, walk_started_at = NULL, walk_after = NULL, \
                 page_failures = 0, page_failing_id = NULL, walk_skip_ids = '{}', last_error = NULL, last_error_at = NULL, updated_at = now() \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(target_user_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| EverythingModeError::Db(e.to_string()))?;
        tx.commit().await.map_err(|e| EverythingModeError::Db(e.to_string()))?;
        return Ok(false);
    }

    let graph_id = resolve_primary_graph(pool, account_id, plane, IngestCaller::Attributable(target_user_id))
        .await
        .map_err(|e| EverythingModeError::OceanUnresolved(e.to_string()))?;

    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    let row: (bool,) = sqlx::query_as(
        r#"
        INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (account_id, owner_user_id) DO UPDATE
            SET enabled = EXCLUDED.enabled, graph_id = EXCLUDED.graph_id, updated_at = now(),
                -- A different destination ocean has none of this mailbox's
                -- corals: drop the walk state so the worker's next walk
                -- covers the whole mailbox into it.
                resume_cursor = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                     THEN gmail_everything_mode.resume_cursor END,
                walk_started_at = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                       THEN gmail_everything_mode.walk_started_at END,
                walk_after = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                  THEN gmail_everything_mode.walk_after END,
                page_failures = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                     THEN gmail_everything_mode.page_failures ELSE 0 END,
                page_failing_id = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                       THEN gmail_everything_mode.page_failing_id END,
                walk_skip_ids = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                     THEN gmail_everything_mode.walk_skip_ids ELSE '{}' END,
                high_water_at = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                     THEN gmail_everything_mode.high_water_at END,
                last_full_walk_at = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                         THEN gmail_everything_mode.last_full_walk_at END,
                -- Messages the walk skips (migration 0280/0118) get another
                -- chance in a new ocean.
                unreadable_ids = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                      THEN gmail_everything_mode.unreadable_ids ELSE '{}' END,
                given_up_ids = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                    THEN gmail_everything_mode.given_up_ids ELSE '{}' END,
                -- A tick already running for the OLD ocean holds the claim
                -- and fences every write on it: drop the claim, so it cannot
                -- write the old ocean's position, mark or skip lists back
                -- over this reset (S4, review of #1168).
                claim_id = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                THEN gmail_everything_mode.claim_id END,
                claimed_until = CASE WHEN gmail_everything_mode.graph_id = EXCLUDED.graph_id
                                     THEN gmail_everything_mode.claimed_until END
        RETURNING enabled
        "#,
    )
    .bind(account_id)
    .bind(target_user_id)
    .bind(graph_id)
    .bind(enabled)
    .fetch_one(&mut *tx)
    .await
    .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    tx.commit().await.map_err(|e| EverythingModeError::Db(e.to_string()))?;
    Ok(row.0)
}

/// Current everything-mode state for `owner_user_id`'s own mailbox. `Ok(false)`
/// with NO row present is the fresh-connection default — this function never
/// creates a row (unlike [`set_everything_mode`]).
pub(crate) async fn everything_mode_status(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<bool, EverythingModeError> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT enabled FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    tx.rollback().await.ok();
    Ok(row.map(|r| r.0).unwrap_or(false))
}

#[derive(Debug, Deserialize)]
pub(crate) struct SetEverythingModeRequest {
    pub enabled: bool,
}

#[derive(Debug, Serialize)]
struct EverythingModeResponse {
    enabled: bool,
}

/// `POST /v1/connectors/google/gmail/everything-mode`
pub async fn set_everything_mode_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<SetEverythingModeRequest>,
) -> Response {
    if let Some(reject) = reject_org_everything_mode_access(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let plane = plane_of(&tenant);
    match set_everything_mode(
        &pool,
        user_ctx.account_id,
        plane,
        user_ctx.user_id,
        user_ctx.user_id,
        body.enabled,
    )
    .await
    {
        Ok(enabled) => (StatusCode::OK, Json(EverythingModeResponse { enabled })).into_response(),
        Err(e) => e.into_response(),
    }
}

/// `GET /v1/connectors/google/gmail/everything-mode`
pub async fn get_everything_mode_route(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_everything_mode_access(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    match everything_mode_status(&pool, user_ctx.account_id, user_ctx.user_id).await {
        Ok(enabled) => (StatusCode::OK, Json(EverythingModeResponse { enabled })).into_response(),
        Err(e) => e.into_response(),
    }
}

/// pg-gated tests implementing `docs/connectors/CONNECTOR_CONTRACT.md` §6's
/// AMENDED conformance test #9 ("Sensitive-path discipline") for Gmail —
/// mirroring `routes::outlook_everything_mode::
/// outlook_everything_mode_route_pg_tests` (the FIRST implementation, m365
/// enterprise train PR 3.4) case for case. Against a REAL Postgres database:
///
/// * **Positive**: the owner enabling their OWN mailbox succeeds
///   (`owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg`) and
///   resolves ONLY that owner's private member ocean — never a team ocean
///   that exists on the very same account
///   (`enabling_never_selects_a_team_ocean_even_when_one_exists_pg`).
/// * **Negative (a)** — admin/non-owner enabling for someone else is
///   refused, and creates NO row at all:
///   `actor_mismatch_is_refused_before_touching_the_database_pg`.
/// * **Negative (b)** — nothing enables it by default: a fresh mailbox reads
///   disabled with no row
///   (`owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg`'s
///   first assertion) — and `workers::gmail_everything_mode`'s own pg tests
///   (`due_accounts_empty_when_nothing_is_enabled_anywhere_pg`,
///   `owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg`)
///   prove the worker SKIPS a mailbox with no row, never lazily creating one.
/// * **Onward sharing / no fan-out**: nothing in [`set_everything_mode`] or
///   in `workers::gmail_everything_mode::sync_owner` ever writes any
///   `graph_id` other than the one `resolve_primary_graph` resolves for the
///   target owner — there is no second destination parameter anywhere in
///   either code path for a fan-out or team-ocean write to come from.
/// * **No other path for correspondence to reach here**: proven by
///   construction — `connectors::google_gmail::list_everything_mode_page`/
///   `get_everything_mode_message` are free functions, never called from
///   `GoogleConnector`'s `ConnectorProvider::sync`/`fetch_items` impls
///   (Gmail's opt-in thread/live-follow path, untouched by this PR); their
///   only caller is `workers::gmail_everything_mode::sync_owner_with_token`.
///
/// Route-layer HTTP wiring is exercised indirectly (the routes are thin
/// wrappers with no branching logic of their own beyond what's tested here
/// plus the org gate, which `routes::connectors`/`routes::oauth`'s own pg
/// tests already cover for the identical `org_google_gmail_enabled_for`
/// flag).
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --lib "routes::gmail_everything_mode::gmail_everything_mode_route_pg_tests" \
///     -- --test-threads=1
///
/// **Runs against the REAL, NOBYPASSRLS `coralswarm_app` role** (`crate::
/// app_role_test_support`), not the migration-owner role — fixed before
/// merge (adversarial review, 2026-08-25): the first draft of every test
/// below called `set_everything_mode`/`everything_mode_status` against a
/// plain owner-role pool, which BYPASSES RLS entirely. That let
/// `owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg` pass
/// while asserting an outcome that is IMPOSSIBLE in production: migration
/// `0197`'s original `WITH CHECK` (ported verbatim from Outlook's 0192)
/// required `graphs.kind = 'member'`, which the Personal plane's own
/// `resolve_primary_graph` arm never produces — so every Personal-plane
/// enable 500'd for real under `coralswarm_app`, while this same test kept
/// reporting green. `migrations/0197`'s header documents the fix (a second
/// `kind='personal'` WITH CHECK arm); this module's tests now run under the
/// role that can actually prove it.
#[cfg(test)]
mod gmail_everything_mode_route_pg_tests {
    use super::*;
    use crate::app_role_test_support::app_role_pool;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }
    async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    /// The migration-OWNER pool — used ONLY for `sqlx::migrate!` and seeding
    /// `users`/`accounts`/`graphs` fixture rows (those inserts are not what
    /// this suite is proving; BYPASSRLS there is standard practice, same as
    /// `tenant_rls_pg.rs`'s own `seed()`). Every call to
    /// `set_everything_mode`/`everything_mode_status` — the functions this
    /// suite actually exists to prove — MUST go through [`app_pool`]
    /// instead.
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
    }
    /// A pool connected AS `coralswarm_app` — the real, NOBYPASSRLS
    /// production role. Every `set_everything_mode`/`everything_mode_status`
    /// call in this module routes through this, not `owner`.
    async fn app_pool(owner: &PgPool, base: &str, db: &str) -> PgPool {
        app_role_pool(owner, &with_db(base, db)).await
    }
    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("user-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap()
    }
    async fn seed_account(pool: &PgPool, owner: Uuid) -> Uuid {
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn disabling_succeeds_even_when_the_owners_ocean_cannot_be_resolved_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping disabling_succeeds_even_when_the_owners_ocean_cannot_be_resolved_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_disable_no_ocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'orphaned destination', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO gmail_everything_mode (account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        let disabled = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, false)
            .await
            .expect("disable must succeed without any ocean resolution");
        assert!(!disabled);
        assert!(
            !everything_mode_status(&app, account_id, owner).await.unwrap(),
            "the row must read disabled — the worker's WHERE enabled = true no longer matches"
        );

        app.close().await;
        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn actor_mismatch_is_refused_before_touching_the_database_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping actor_mismatch_is_refused_before_touching_the_database_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_actor_mismatch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        let other = seed_user(&pool).await;

        let result = set_everything_mode(&app, account_id, AccountPlane::Personal, other, owner, true).await;
        assert!(
            matches!(result, Err(EverythingModeError::ActorMismatch)),
            "a non-owner enabling everything mode for someone else's mailbox must be refused, \
             got {result:?}"
        );

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_everything_mode WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0, "a refused actor-mismatch write must not create any row");

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// Conformance test #9's positive case for the PERSONAL plane — the
    /// case that was PASSING VACUOUSLY before the RLS fix (see this
    /// module's own doc): run under the real `coralswarm_app` role, this is
    /// the test that actually proves a personal-plane owner can enable
    /// everything mode in production, not merely against a schema-owner
    /// connection that bypasses RLS entirely.
    #[tokio::test]
    async fn owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_owner_enable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        let status = everything_mode_status(&app, account_id, owner).await.unwrap();
        assert!(!status, "a fresh mailbox must default to everything mode DISABLED");

        let enabled = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .expect(
                "PERSONAL-plane enable must succeed under the real coralswarm_app role — this is \
                 exactly the case migration 0197's WITH CHECK fix (kind='personal' arm) exists for",
            );
        assert!(enabled);
        assert!(everything_mode_status(&app, account_id, owner).await.unwrap());

        let enabled_again = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled_again);

        let disabled = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, false)
            .await
            .unwrap();
        assert!(!disabled);
        assert!(!everything_mode_status(&app, account_id, owner).await.unwrap());

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 1, "enable/disable/re-enable on the same owner must upsert one row, never duplicate");

        app.close().await;
        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn one_owners_row_never_affects_a_different_owner_on_the_same_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping one_owners_row_never_affects_a_different_owner_on_the_same_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_cross_owner";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner_a = seed_user(&pool).await;
        let owner_b = seed_user(&pool).await;

        set_everything_mode(&app, account_id, AccountPlane::Personal, owner_a, owner_a, true)
            .await
            .unwrap();

        assert!(!everything_mode_status(&app, account_id, owner_b).await.unwrap());
        assert!(everything_mode_status(&app, account_id, owner_a).await.unwrap());

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// Conformance test #9's positive case, sharpened: a TEAM ocean already
    /// exists on the SAME account (the shared destination the Mailbox
    /// archetype forbids everything mode from ever reaching — contract §7
    /// condition (b)). Proves `set_everything_mode` resolves ONLY the
    /// owner's own private member ocean, never the team one sitting right
    /// next to it — on the ORG plane, where a team ocean is a real,
    /// reachable destination for other features and so is the sharpest
    /// place to prove this connector never reaches for it.
    #[tokio::test]
    async fn enabling_never_selects_a_team_ocean_even_when_one_exists_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping enabling_never_selects_a_team_ocean_even_when_one_exists_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_never_team_ocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        let team_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind) VALUES ($1, 'Team ocean', 'team') RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let member_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'owner private ocean', 'member', $2, true) RETURNING id",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();

        let enabled = set_everything_mode(&app, account_id, AccountPlane::Org, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled);

        let graph_id: Uuid = sqlx::query_scalar(
            "SELECT graph_id FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(
            graph_id, member_graph,
            "everything mode must resolve the owner's own private member ocean"
        );
        assert_ne!(
            graph_id, team_graph,
            "everything mode must NEVER select a team/shared ocean, even when one exists on the \
             same account — contract §7 condition (b)"
        );

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// F3 (adversarial review, 2026-08-25): disabling must clear
    /// `resume_cursor`/`last_error`/`last_error_at` so a later re-enable
    /// starts clean, rather than resuming from a possibly-stale (or by then
    /// Gmail-rejected) cursor. Simulates a mid-walk cursor + a recorded
    /// error by writing them directly (the worker's own job, not this
    /// route's), then proves `set_everything_mode(false)` wipes both, and
    /// that a subsequent re-enable does not resurrect them.
    #[tokio::test]
    async fn disabling_clears_the_resume_cursor_and_error_state_so_a_later_re_enable_starts_clean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping disabling_clears_the_resume_cursor_and_error_state_so_a_later_re_enable_starts_clean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_disable_clears_cursor";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        let enabled = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled);

        // Simulate a week-old, mid-walk cursor + a recorded error — exactly
        // what `workers::gmail_everything_mode::record_tick_end`'s error
        // branch would have left behind.
        sqlx::query(
            "UPDATE gmail_everything_mode \
             SET resume_cursor = 'stale-page-token-from-a-week-ago', \
                 last_error = 'boom', last_error_at = now() \
             WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        let disabled = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, false)
            .await
            .unwrap();
        assert!(!disabled);

        let (cursor, last_error, last_error_at): (Option<String>, Option<String>, Option<chrono::DateTime<chrono::Utc>>) =
            sqlx::query_as(
                "SELECT resume_cursor, last_error, last_error_at FROM gmail_everything_mode \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(cursor, None, "disabling must clear resume_cursor");
        assert_eq!(last_error, None, "disabling must clear last_error");
        assert_eq!(last_error_at, None, "disabling must clear last_error_at");

        // Re-enable — must NOT resurrect the old cursor/error (there is no
        // code path that would, but assert it directly rather than trusting
        // that by omission).
        let enabled_again = set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled_again);
        let (cursor_after_reenable,): (Option<String>,) = sqlx::query_as(
            "SELECT resume_cursor FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            cursor_after_reenable, None,
            "a re-enable after a clean disable must still start with no resume_cursor"
        );

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// The worker's high-water mark (migration `0279`) means "this mailbox's
    /// mail up to here is in THIS ocean". A re-enable into the same ocean
    /// keeps it (no whole-mailbox re-walk); a re-enable that resolves a
    /// DIFFERENT ocean drops it, so the next walk covers the whole mailbox
    /// into the new one. Disabling clears the in-progress walk.
    #[tokio::test]
    async fn re_enabling_into_a_different_ocean_drops_the_walk_state_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping re_enabling_into_a_different_ocean_drops_the_walk_state_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_reenable_new_ocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());

        type Walk = (Option<String>, Option<chrono::DateTime<chrono::Utc>>, Option<chrono::DateTime<chrono::Utc>>);
        let walk = |pool: PgPool| async move {
            sqlx::query_as::<_, Walk>(
                "SELECT resume_cursor, walk_started_at, high_water_at FROM gmail_everything_mode \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let given_up = |pool: PgPool| async move {
            sqlx::query_scalar::<_, Vec<String>>(
                "SELECT given_up_ids FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let set_walk = |pool: PgPool| async move {
            sqlx::query(
                "UPDATE gmail_everything_mode SET resume_cursor = 'tok-9', walk_started_at = now(), \
                 high_water_at = now() - interval '1 day', given_up_ids = ARRAY['poison-1'] \
                 WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .execute(&pool)
            .await
            .unwrap();
        };

        // Same ocean: the mark survives a re-enable; the walk survives too.
        set_walk(pool.clone()).await;
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());
        let (cursor, started, mark) = walk(pool.clone()).await;
        assert_eq!(cursor.as_deref(), Some("tok-9"));
        assert!(started.is_some() && mark.is_some(), "same-ocean re-enable keeps the walk state");
        assert_eq!(given_up(pool.clone()).await, vec!["poison-1".to_string()], "and the given-up ids");

        // Disable clears the in-progress walk, keeps the mark.
        assert!(!set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, false).await.unwrap());
        let (cursor, started, mark) = walk(pool.clone()).await;
        assert_eq!((cursor, started), (None, None), "disabling clears the in-progress walk");
        assert!(mark.is_some(), "disabling keeps the high-water mark");
        assert_eq!(given_up(pool.clone()).await, vec!["poison-1".to_string()], "and the given-up ids");

        // The row points at some other ocean (e.g. the primary moved since):
        // the re-enable resolves the current primary, a different graph.
        let other: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) VALUES ($1, 'old', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE gmail_everything_mode SET graph_id = $3 WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(owner)
            .bind(other)
            .execute(&pool)
            .await
            .unwrap();
        set_walk(pool.clone()).await;
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());
        assert_eq!(walk(pool.clone()).await, (None, None, None), "a new ocean starts a whole-mailbox walk");
        assert!(given_up(pool.clone()).await.is_empty(), "a new ocean retries the given-up messages");

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// S4 (review of #1168): a re-enable into a DIFFERENT ocean drops the
    /// claim too. A tick already running for the old ocean holds that claim
    /// and fences every write on it, so it can no longer write the old
    /// ocean's walk position, mark or given-up ids back over the reset. A
    /// same-ocean re-enable leaves a running tick alone.
    #[tokio::test]
    async fn re_enabling_into_a_different_ocean_fences_out_a_running_tick_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping re_enabling_into_a_different_ocean_fences_out_a_running_tick_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "g24_reenable_fences_claim";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());

        let claim = |pool: PgPool| async move {
            sqlx::query_scalar::<_, Option<Uuid>>(
                "UPDATE gmail_everything_mode SET claim_id = gen_random_uuid(), \
                 claimed_until = now() + interval '15 minutes' \
                 WHERE account_id = $1 AND owner_user_id = $2 RETURNING claim_id",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap()
        };
        let current = |pool: PgPool| async move {
            sqlx::query_scalar::<_, Option<Uuid>>(
                "SELECT claim_id FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
            )
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap()
        };

        // Same ocean: the running tick keeps its claim.
        let running = claim(pool.clone()).await;
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());
        assert_eq!(current(pool.clone()).await, running, "same-ocean re-enable leaves the claim");

        // Different ocean: the claim is dropped.
        let other: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) VALUES ($1, 'old', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query("UPDATE gmail_everything_mode SET graph_id = $3 WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(owner)
            .bind(other)
            .execute(&pool)
            .await
            .unwrap();
        let running = claim(pool.clone()).await;
        assert!(running.is_some());
        assert!(set_everything_mode(&app, account_id, AccountPlane::Personal, owner, owner, true).await.unwrap());
        assert_eq!(current(pool.clone()).await, None, "a new ocean drops the running tick's claim");
        let until: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT claimed_until FROM gmail_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(until, None, "and the row is claimable again at once");

        app.close().await;
        teardown(&base, db, pool).await;
    }

    /// R4 (adversarial review, 2026-08-25): an owner whose member ocean has
    /// since been archived (offboarding, `member_ocean_reconciler`'s
    /// archive arm) must still be able to reach their own OFF switch.
    /// Migration `0197`'s `WITH CHECK` used to re-validate `graphs.
    /// archived_at IS NULL` on EVERY write, including a disable that never
    /// even touches `graph_id` — reproduced against real Postgres before
    /// this fix (`archived_at IS NULL` dropped from the policy).
    #[tokio::test]
    async fn disabling_succeeds_even_after_the_owners_graph_is_archived_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping disabling_succeeds_even_after_the_owners_graph_is_archived_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "g24_disable_after_archive";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;
        let app = app_pool(&pool, &base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        let member_graph: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'owner private ocean', 'member', $2, true) RETURNING id",
        )
        .bind(account_id)
        .bind(owner)
        .fetch_one(&pool)
        .await
        .unwrap();

        let enabled = set_everything_mode(&app, account_id, AccountPlane::Org, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled);

        // Archive the graph — same shape `member_ocean_reconciler`'s
        // offboard arm writes.
        sqlx::query("UPDATE graphs SET archived_at = now() WHERE id = $1")
            .bind(member_graph)
            .execute(&pool)
            .await
            .unwrap();

        let disabled = set_everything_mode(&app, account_id, AccountPlane::Org, owner, owner, false)
            .await
            .expect("disabling must succeed even though the owner's graph is now archived");
        assert!(!disabled);

        app.close().await;
        teardown(&base, db, pool).await;
    }
}
