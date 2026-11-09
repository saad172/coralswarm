//! Outlook everything-mode opt-in (m365 enterprise train PR 3.4).
//!
//! `docs/connectors/CONNECTOR_CONTRACT.md` §4 ("Two rules that fall out of
//! the matrix"), §7 ("Everything-mode carve-out"), §6 (amended conformance
//! test #9): the Mailbox archetype (contract §7) confines Outlook to
//! per-thread opt-in (`routes::connectors::ingest_selected`) by default.
//! Everything mode is the one permitted exception — a whole-mailbox
//! `sync()`-shaped pull — gated on THREE hard requirements, none of them
//! negotiable:
//!
//! 1. **the mailbox owner configures it themselves** — never an admin,
//!    never a default. This file's whole job is enforcing that: see
//!    [`set_everything_mode`]'s actor==target check below, which is real
//!    application-layer enforcement, not merely an artifact of the route
//!    never exposing a target-user parameter (defense in depth — the same
//!    "don't rely on the API surface alone" posture `outlook_everything_
//!    mode`'s own RLS `WITH CHECK` takes for internal row consistency).
//! 2. **content lands exclusively in that owner's private ocean** —
//!    `resolve_primary_graph(.., IngestCaller::Attributable(owner))`, the
//!    same member-private-ocean primitive every other per-owner connector
//!    ingest path in this codebase uses. There is no fan-out, no team-ocean
//!    destination option anywhere in this file or in `workers::
//!    outlook_everything_mode`, which actually performs the pull.
//! 3. **onward sharing only via an explicit I3 promotion** — nothing here
//!    or in the worker writes to any ocean but the owner's own; sharing
//!    everything-mode content is whatever the ordinary promotion path
//!    already is, never a side effect of enabling the mode.
//!
//! Routes (bearer-authed, account+user scoped — same shape as
//! `routes::connectors`/`routes::teams_ingest`):
//! * `POST /v1/connectors/microsoft/everything-mode` — enable/disable for
//!   the CALLER'S OWN mailbox only (`{"enabled": bool}` body, no target-user
//!   field exists at all — see [`SetEverythingModeRequest`]).
//! * `GET  /v1/connectors/microsoft/everything-mode` — current state for the
//!   caller's own mailbox (`{"enabled": bool}`; `false` when no row exists,
//!   which is the fresh-connection default — nobody's mailbox starts in
//!   everything mode).
//!
//! Org tenancy: reuses the SAME per-org `org_connector_flags` registry flag
//! (`routes::teams_ingest::org_microsoft_teams_enabled_for`, provider
//! `"microsoft"`) m365 Train 3 PR 3.3 already lifted for the other Microsoft
//! connector routes (`routes::connectors::reject_org_connector_access`,
//! `routes::teams_ingest::reject_org_teams_access`) — one Microsoft
//! provider, one flag, not a fourth env var. Flag off (the zero-row
//! default): an org member gets the same clean 403 every other
//! Microsoft-org-gated route already returns. Flag on: `tenant.pool`
//! resolves to the org's own TENANT pool and `set_everything_mode` writes
//! there, into the org member's own private ocean — never a team ocean
//! (`resolve_primary_graph`'s org arm has no such destination to pick).
//! Personal plane: always allowed, same as every other connector route.
//!
//! The actual whole-mailbox pull driven by this opt-in lives in
//! `workers::outlook_everything_mode` — this file only owns the toggle and
//! its enforcement.

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
/// to `routes::connectors::plane_of`/`routes::teams_ingest::plane_of`. Every
/// call site here runs AFTER [`reject_org_everything_mode_access`], so an
/// `Org` `TenancyDecision` reaching this point has already had that gate's
/// flag confirmed on for its org.
fn plane_of(tenant: &Tenant) -> AccountPlane {
    match tenant.org_context() {
        Some(_) => AccountPlane::Org,
        None => AccountPlane::Personal,
    }
}

/// Fail-closed gate for these routes under an org `TenancyDecision` — same
/// policy shape and same registry flag as `routes::connectors::
/// reject_org_connector_access`'s `provider == "microsoft"` arm and
/// `routes::teams_ingest::reject_org_teams_access` (m365 Train 3 PR 3.3's
/// gate machinery, reused rather than re-invented). Flag off: refuse before
/// any pool resolution or DB write. Personal / flag-off requests are
/// byte-identical to before this PR.
async fn reject_org_everything_mode_access(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "outlook everything-mode route refused under org context (org_connector_flags \
         resolved off for provider \"microsoft\" for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "Outlook everything mode is available in your personal workspace \
                          only, or once your organization admin enables Microsoft connectors"
            })),
        )
            .into_response(),
    )
}

/// Failure modes for [`set_everything_mode`]/[`everything_mode_status`],
/// mapped to precise HTTP statuses by [`EverythingModeError::into_response`].
#[derive(Debug)]
pub(crate) enum EverythingModeError {
    /// The acting user is not the mailbox owner they are trying to
    /// enable/disable everything mode for. This is the contract's "never an
    /// admin, never on someone else's behalf" requirement — see this
    /// module's own doc comment, point 1. → `403`.
    ActorMismatch,
    /// `resolve_primary_graph` could not resolve the target owner's private
    /// ocean (org member ocean not yet provisioned, or an unknown account) —
    /// same refuse-don't-guess posture `workers::microsoft_calendar_sync`'s
    /// `sync_member` already takes for the identical resolution. → `503`.
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
                tracing::warn!(error = %e, "outlook_everything_mode: could not resolve owner's private ocean");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "your private workspace is not ready yet — try again shortly"})),
                )
                    .into_response()
            }
            EverythingModeError::Db(e) => {
                tracing::error!(error = %e, "outlook_everything_mode: db failure");
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
/// teammate, or any other non-owner caller gets [`EverythingModeError::
/// ActorMismatch`] before touching the database at all, regardless of what
/// role `actor_user_id` holds. The HTTP route above always calls this with
/// `actor_user_id == target_user_id == user_ctx.user_id` (there is no
/// request field to name a different target), so this check is defense in
/// depth from the route's perspective — but it is the ONLY enforcement from
/// a direct caller's perspective (a future admin surface, a script, a test),
/// which is exactly why it lives here rather than being implied by the
/// route's own shape. Conformance test #9(a) (`docs/connectors/
/// CONNECTOR_CONTRACT.md` §6) calls this function directly with a mismatched
/// actor/target to prove the enforcement, independent of the route.
///
/// On success: resolves `target_user_id`'s own primary private ocean
/// (`resolve_primary_graph`, `IngestCaller::Attributable` — never `NonAttributable`,
/// never a team-ocean destination) and upserts the `outlook_everything_mode`
/// row, returning the persisted `enabled` value. Idempotent: enabling an
/// already-enabled mailbox (or disabling an already-disabled/never-enabled
/// one) is a normal upsert, not an error.
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
            "outlook_everything_mode: refusing to set everything mode for another user's \
             mailbox — owner-self-configured only (contract §7 carve-out, condition 1)"
        );
        return Err(EverythingModeError::ActorMismatch);
    }

    // Disabling must NEVER depend on ocean resolution (CodeRabbit round 1):
    // the off switch is the one control the contract gives the owner, and a
    // deprovisioned/not-yet-reconciled private ocean must not hold it
    // hostage while the worker keeps pulling the mailbox. Only ENABLING
    // needs a destination graph.
    if !enabled {
        let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, actor_user_id)
            .await
            .map_err(|e| EverythingModeError::Db(e.to_string()))?;
        // No row = already off (the fresh-connection default) — a clean
        // no-op success, never an INSERT (a disabled row for a mailbox that
        // never enabled would be noise, and would need a graph we may not
        // be able to resolve).
        sqlx::query(
            "UPDATE outlook_everything_mode SET enabled = FALSE, updated_at = now() \
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

    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, actor_user_id)
        .await
        .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    let row: (bool,) = sqlx::query_as(
        r#"
        INSERT INTO outlook_everything_mode (account_id, owner_user_id, graph_id, enabled)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (account_id, owner_user_id) DO UPDATE
            SET enabled = EXCLUDED.enabled, graph_id = EXCLUDED.graph_id, updated_at = now()
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
/// with NO row present is the fresh-connection default — see this module's
/// own doc comment: nobody's mailbox starts in everything mode, and this
/// function never creates a row (unlike [`set_everything_mode`]).
pub(crate) async fn everything_mode_status(
    pool: &PgPool,
    account_id: Uuid,
    owner_user_id: Uuid,
) -> Result<bool, EverythingModeError> {
    let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, owner_user_id)
        .await
        .map_err(|e| EverythingModeError::Db(e.to_string()))?;
    let row: Option<(bool,)> = sqlx::query_as(
        "SELECT enabled FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
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

/// `POST /v1/connectors/microsoft/everything-mode`
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

/// `GET /v1/connectors/microsoft/everything-mode`
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
/// AMENDED conformance test #9 ("Sensitive-path discipline") for Outlook —
/// the headline deliverable of m365 enterprise train PR 3.4. Against a REAL
/// Postgres database:
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
///   first assertion) — and `workers::outlook_everything_mode`'s own pg
///   tests (`due_accounts_empty_when_nothing_is_enabled_anywhere_pg`,
///   `owner_state_is_none_for_a_fresh_mailbox_and_some_once_enabled_pg`)
///   prove the worker SKIPS a mailbox with no row, never lazily creating one.
/// * **Onward sharing / no fan-out**: nothing in [`set_everything_mode`] or
///   in `workers::outlook_everything_mode::sync_owner` ever writes any
///   `graph_id` other than the one `resolve_primary_graph` resolves for the
///   target owner — there is no second destination parameter anywhere in
///   either code path for a fan-out or team-ocean write to come from. Onward
///   sharing of everything-mode content therefore can only ever happen
///   through whatever the ordinary I3 promotion path already is, which lives
///   entirely outside this feature (contract §7, condition (c)).
/// * **No other path for `sync()` to return correspondence**: proven by
///   construction, not a runtime assertion — `connectors::microsoft::
///   fetch_everything_mode_mail` is a free function, never called from
///   `MicrosoftConnector`'s `ConnectorProvider::sync` impl (still
///   calendar-only, unmodified by this PR); `grep -rn
///   fetch_everything_mode_mail backend/src` has exactly one caller
///   (`workers::outlook_everything_mode::sync_owner`).
///
/// Route-layer HTTP wiring is exercised indirectly (the routes are thin
/// wrappers with no branching logic of their own beyond what's tested here
/// plus the org gate, which `routes::teams_ingest`/`routes::connectors`' own
/// pg tests already cover for the identical `org_microsoft_teams_enabled_for`
/// flag).
///
///   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
///     cargo test -p coralswarm-backend --lib "routes::outlook_everything_mode::outlook_everything_mode_route_pg_tests" \
///     -- --test-threads=1
#[cfg(test)]
mod outlook_everything_mode_route_pg_tests {
    use super::*;
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
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        pool
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
        // CodeRabbit round 1: the off switch is the ONE control the contract
        // gives the owner — it must not depend on `resolve_primary_graph`
        // succeeding. Seed an ENABLED row whose owner has NO resolvable
        // primary ocean (the FK graph is deliberately non-primary), then
        // prove disable succeeds and persists where the old code 503'd.
        let Some(base) = base_url() else {
            eprintln!(
                "skipping disabling_succeeds_even_when_the_owners_ocean_cannot_be_resolved_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "m365_34_disable_no_ocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        // A graph that satisfies the FK but is NOT the resolvable primary
        // ocean — `resolve_primary_graph` fails for this owner.
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, is_primary) \
             VALUES ($1, 'orphaned destination', 'personal', false) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO outlook_everything_mode (account_id, owner_user_id, graph_id, enabled) \
             VALUES ($1, $2, $3, TRUE)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(graph_id)
        .execute(&pool)
        .await
        .unwrap();

        let disabled = set_everything_mode(&pool, account_id, AccountPlane::Personal, owner, owner, false)
            .await
            .expect("disable must succeed without any ocean resolution");
        assert!(!disabled);
        assert!(
            !everything_mode_status(&pool, account_id, owner).await.unwrap(),
            "the row must read disabled — the worker's WHERE enabled = true no longer matches"
        );

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn actor_mismatch_is_refused_before_touching_the_database_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping actor_mismatch_is_refused_before_touching_the_database_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_actor_mismatch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;
        let other = seed_user(&pool).await;

        // `other` (e.g. an admin, or any non-owner) tries to enable
        // everything mode for `owner`'s mailbox. Must be refused — and
        // MUST NOT create a row at all (contract §6 test #9(a)/(b)).
        let result = set_everything_mode(&pool, account_id, AccountPlane::Personal, other, owner, true).await;
        assert!(
            matches!(result, Err(EverythingModeError::ActorMismatch)),
            "a non-owner enabling everything mode for someone else's mailbox must be refused, \
             got {result:?}"
        );

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM outlook_everything_mode WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 0, "a refused actor-mismatch write must not create any row");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping owner_enabling_their_own_mailbox_succeeds_and_is_idempotent_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_owner_enable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner = seed_user(&pool).await;

        // Fresh connection, never touched: default is disabled with no row.
        let status = everything_mode_status(&pool, account_id, owner).await.unwrap();
        assert!(!status, "a fresh mailbox must default to everything mode DISABLED");

        // The owner enables it for themselves — must succeed.
        let enabled = set_everything_mode(&pool, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled);
        assert!(everything_mode_status(&pool, account_id, owner).await.unwrap());

        // Re-enabling (idempotent upsert) still succeeds and stays enabled.
        let enabled_again = set_everything_mode(&pool, account_id, AccountPlane::Personal, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled_again);

        // The owner disables it — must succeed and persist.
        let disabled = set_everything_mode(&pool, account_id, AccountPlane::Personal, owner, owner, false)
            .await
            .unwrap();
        assert!(!disabled);
        assert!(!everything_mode_status(&pool, account_id, owner).await.unwrap());

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2")
            .bind(account_id)
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 1, "enable/disable/re-enable on the same owner must upsert one row, never duplicate");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn one_owners_row_never_affects_a_different_owner_on_the_same_account_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping one_owners_row_never_affects_a_different_owner_on_the_same_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_cross_owner";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

        let founder = seed_user(&pool).await;
        let account_id = seed_account(&pool, founder).await;
        let owner_a = seed_user(&pool).await;
        let owner_b = seed_user(&pool).await;

        set_everything_mode(&pool, account_id, AccountPlane::Personal, owner_a, owner_a, true)
            .await
            .unwrap();

        // owner_b never enabled anything — must still read as disabled/no row,
        // regardless of owner_a's state on the SAME account.
        assert!(!everything_mode_status(&pool, account_id, owner_b).await.unwrap());
        assert!(everything_mode_status(&pool, account_id, owner_a).await.unwrap());

        teardown(&base, db, pool).await;
    }

    /// Conformance test #9's positive case, sharpened: a TEAM ocean already
    /// exists on the SAME account (the shared destination the Mailbox
    /// archetype forbids everything mode from ever reaching — contract §7
    /// condition (b), §4's "the connector itself never routes it to a team
    /// ocean"). Proves `set_everything_mode` resolves ONLY the owner's own
    /// private member ocean, never the team one sitting right next to it —
    /// on the ORG plane, where a team ocean is a real, reachable destination
    /// for other features (Slack/Teams channel routing) and so is the
    /// sharpest place to prove this connector never reaches for it.
    #[tokio::test]
    async fn enabling_never_selects_a_team_ocean_even_when_one_exists_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping enabling_never_selects_a_team_ocean_even_when_one_exists_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "m365_34_never_team_ocean";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool = control_pool(&base, db).await;

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

        let enabled = set_everything_mode(&pool, account_id, AccountPlane::Org, owner, owner, true)
            .await
            .unwrap();
        assert!(enabled);

        let graph_id: Uuid = sqlx::query_scalar(
            "SELECT graph_id FROM outlook_everything_mode WHERE account_id = $1 AND owner_user_id = $2",
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

        teardown(&base, db, pool).await;
    }
}
