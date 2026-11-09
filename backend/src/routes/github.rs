//! GitHub connector HTTP surface (PR A, sync plumbing —
//! docs/connectors/github.md, docs/connectors/CONNECTOR_CONTRACT.md).
//!
//! * `GET /v1/connectors/github/installs` — [`list_installs`]. Every
//!   installation this account has recorded, with a pending-selection count.
//! * `GET /v1/connectors/github/repos?installation_id=` — [`list_repos`].
//!   Live-merges the installation's granted repos (`GET
//!   /installation/repositories`, under the installation token) against
//!   existing `github_watched_repos` rows.
//! * `PUT /v1/connectors/github/repos` — [`put_repos`]. Upserts selections —
//!   active/paused, destination — and enqueues `JobKind::GithubBackfill` for
//!   every newly-active row.
//! * `DELETE /v1/connectors/github/installs/{installation_id}` —
//!   [`delete_install`]. Pauses every row for that installation — never
//!   deletes (§3.8).
//! * `POST /v1/github/events` — [`github_events`]. The App webhook, open
//!   (unauthenticated — HMAC-verified inside, same posture as
//!   `/v1/slack/events`).
//!
//! `/v1/oauth/github/authorize` and `/v1/oauth/github/callback` are NOT in
//! this file — they are the existing generic `/v1/oauth/{provider}/…` routes
//! (`routes::oauth`), which resolve `github` through `connectors::lookup`
//! like every other provider.

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Extension, Json,
};
use axum::body::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::PgPool;
use uuid::Uuid;

use crate::{
    connectors::{
        github::{self, GithubApiError, InstallationLifecycle},
        github_disconnect::{self, paused_reason},
        github_repos::{self, DestinationError, RequestedDestination},
    },
    jobs::{self, queue as job_queue},
    middleware::auth::UserContext,
    tenancy::extractor::Tenant,
    AppState,
};

// ---------------------------------------------------------------------------
// Rollout gate
// ---------------------------------------------------------------------------

/// Refuse an org-context GitHub route unless the per-org flag (or its env
/// default, `ORG_GITHUB_ENABLED`) is on — same shape as every other
/// connector's org gate (`routes::oauth::reject_org_gmail_management`,
/// `routes::google_chat::reject_org_google_chat_management`). A no-op on the
/// Personal plane (`tenant.org_context()` is `None`).
async fn reject_org_github_management(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if crate::connector_flags::github_enabled(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "github connector route refused under org context (ORG_GITHUB_ENABLED/org_connector_flags \
         resolved off for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "GitHub is not enabled for this organization yet"
            })),
        )
            .into_response(),
    )
}

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

// ---------------------------------------------------------------------------
// Tenant-plane pool resolution for a background/webhook context (no `Tenant`
// extractor available — the webhook carries no session at all).
// ---------------------------------------------------------------------------

/// Resolve a `PgPool` for `account_id`'s content plane — the consumer/control
/// pool for a Personal account, or the org's own tenant pool once one is
/// servable. Mirrors `routes::slack_events::resolve_app_home_tenant_pool`'s
/// contract (that function is private to its own module, so this is a
/// smaller, GitHub-scoped re-derivation rather than a visibility change to
/// unrelated Slack code) — fails to `None` on any lookup/build error or an
/// unservable/revoked org, rather than guessing a plane.
async fn resolve_tenant_pool(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    // Kept for call-site symmetry with `resolve_app_home_tenant_pool` (and
    // for a future log line that needs it) even though the org branch below
    // resolves its own `row.account_id` off the routing lookup — the two
    // are expected to always agree (one installation binds to one account,
    // §3.1), so this is not read on the hot path.
    _account_id: Uuid,
    clerk_org_id: Option<&str>,
) -> Option<PgPool> {
    let Some(org_id) = clerk_org_id else {
        return Some(control_pool.clone());
    };
    if crate::tenancy::routing::org_is_revoked(control_pool, org_id).await.unwrap_or(true) {
        return None;
    }
    let row = match crate::tenancy::routing::lookup_by_org(control_pool, org_id).await {
        Ok(Some(row)) => row,
        // NO `tenant_databases` row: this org was never onboarded onto its
        // own database — a personal org (`personal_orgs`, control migration
        // 0208) by design, or a tenant not built yet — and its content lives
        // on the shared consumer plane. This is `routes::slack_events::
        // resolve_org_plane`'s rule ("Missing: never onboarded onto a tenant
        // database — its content genuinely still lives on the shared
        // consumer plane"), and it is what the request path already does:
        // the selection page's `Tenant` extractor hands `put_repos` the
        // consumer pool for exactly this account shape, so the rows it
        // writes ARE there. Answering `None` here instead (the original
        // shape) made every install-event pause a silent no-op for such an
        // account — observed 2026-09-22: `installation_repositories.added`
        // logged "could not resolve tenant pool" and created no pending
        // row. I8 is untouched: the ACCOUNT was already resolved from
        // `installation.id` via `connector_installs` before this runs; this
        // only picks the pool that account's tables live on.
        Ok(None) => return Some(control_pool.clone()),
        Err(e) => {
            tracing::warn!(org = %org_id, error = %e, connector_health = true, "github webhook: tenant routing lookup failed — refusing rather than guessing a plane");
            return None;
        }
    };
    if !row.is_servable() {
        return None;
    }
    let rt = state.tenant_runtime.as_ref()?;
    rt.registry.get_or_build(org_id, row.account_id, &row.db_secret_ref).await.ok()
}

async fn clerk_org_id_for(control_pool: &PgPool, account_id: Uuid) -> Option<String> {
    sqlx::query_scalar::<_, Option<String>>("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(control_pool)
        .await
        .ok()
        .flatten()
        .flatten()
}

// ---------------------------------------------------------------------------
// GET /v1/connectors/github/installs
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct InstallSummary {
    installation_id: String,
    account_login: String,
    account_type: String,
    connected_at: chrono::DateTime<chrono::Utc>,
    suspended: bool,
    pending_selection: i64,
}

pub async fn list_installs(State(state): State<Arc<AppState>>, tenant: Tenant, Extension(user): Extension<UserContext>) -> Response {
    if let Some(reject) = reject_org_github_management(&state, &tenant).await {
        return reject;
    }
    let Some(control_pool) = state.control_db() else {
        return service_unavailable();
    };

    let rows: Result<Vec<(i64, String, String, chrono::DateTime<chrono::Utc>, Option<chrono::DateTime<chrono::Utc>>)>, sqlx::Error> =
        sqlx::query_as(
            "SELECT installation_id, account_login, account_type, created_at, suspended_at \
             FROM github_installations WHERE account_id = $1 ORDER BY created_at",
        )
        .bind(user.account_id)
        .fetch_all(control_pool)
        .await;

    let rows = match rows {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, account_id = %user.account_id, "github list_installs: query failed");
            return internal_error();
        }
    };

    let mut installs = Vec::with_capacity(rows.len());
    for (installation_id, account_login, account_type, connected_at, suspended_at) in rows {
        let pending_selection: i64 = match &tenant.pool {
            Some(pool) => count_pending(pool, user.account_id, installation_id).await.unwrap_or(0),
            None => 0,
        };
        installs.push(InstallSummary {
            installation_id: installation_id.to_string(),
            account_login,
            account_type,
            connected_at,
            suspended: suspended_at.is_some(),
            pending_selection,
        });
    }

    (StatusCode::OK, Json(json!({"installs": installs}))).into_response()
}

async fn count_pending(pool: &PgPool, account_id: Uuid, installation_id: i64) -> Result<i64, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("count_pending scoped tx: {e}")))?;
    let count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM github_watched_repos \
         WHERE account_id = $1 AND installation_id = $2 AND paused_reason = $3",
    )
    .bind(account_id)
    .bind(installation_id)
    .bind(paused_reason::PENDING_SELECTION)
    .fetch_one(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(count)
}

// ---------------------------------------------------------------------------
// GET /v1/connectors/github/repos
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct ListReposParams {
    installation_id: String,
}

#[derive(Serialize, Clone)]
struct DestinationView {
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    team_id: Option<Uuid>,
    #[serde(skip_serializing_if = "Option::is_none")]
    team_name: Option<String>,
}

/// Per-type narrative backfill progress. The retired code tier's keys
/// (`code`, `code_skipped`, `summaries`) are not surfaced: the connector
/// captures no source code since 2026-10-27, and a row synced before then
/// still carries them in `backfill_progress` only as history.
#[derive(Serialize)]
struct ProgressView {
    prs: Option<u8>,
    issues: Option<u8>,
    releases: Option<u8>,
    commits: Option<u8>,
}

#[derive(Serialize)]
struct RepoView {
    row_id: Option<Uuid>,
    repo_id: i64,
    owner: String,
    name: String,
    full_name: String,
    default_branch: String,
    visibility: String,
    archived: bool,
    fork: bool,
    size_kb: i64,
    active: bool,
    paused_reason: Option<String>,
    destination: Option<DestinationView>,
    progress: Option<ProgressView>,
    #[serde(skip_serializing_if = "Option::is_none")]
    suggested: Option<bool>,
    last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
    sync_stopped_at: Option<chrono::DateTime<chrono::Utc>>,
    /// `github_watched_repos.sync_error_code` (migration `0250`/`0102`) —
    /// the CLASSIFICATION of this repo's most recent failed backfill tick,
    /// `null` when it is not currently failing.
    ///
    /// A closed set (`'destination_unreachable' | 'sync_failed'`, enforced
    /// by that column's own CHECK constraint), never the worker's error
    /// text: this field is serialized to the client, and internal error
    /// detail must not leave through an API response. The verbatim message
    /// lives in `jobs.error_message` and the worker's tracing output. See
    /// `connectors::github_backfill::SyncErrorCode`.
    ///
    /// ALWAYS serialised (no `skip_serializing_if`): the web's status badge
    /// reads this to tell a dead backfill apart from a running one, and a
    /// key that silently disappears when the value is absent is exactly how
    /// "no error shown" became indistinguishable from "no error".
    sync_error_code: Option<String>,
    sync_error_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Serialize)]
struct TeamOption {
    id: Uuid,
    name: String,
    is_admin: bool,
}

#[derive(Serialize)]
struct OrgDestinationView {
    allowed: bool,
    member_count: i64,
    non_collaborator_count: Option<i64>,
}

#[derive(Serialize)]
struct DestinationsView {
    personal: bool,
    teams: Vec<TeamOption>,
    org: OrgDestinationView,
}

pub async fn list_repos(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Query(params): Query<ListReposParams>,
) -> Response {
    if let Some(reject) = reject_org_github_management(&state, &tenant).await {
        return reject;
    }
    let Ok(installation_id) = params.installation_id.parse::<i64>() else {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid installation_id"}))).into_response();
    };
    let Some(control_pool) = state.control_db() else {
        return service_unavailable();
    };
    let Some(tenant_pool) = tenant.pool.clone() else {
        return service_unavailable();
    };

    if !installation_belongs_to_account(control_pool, installation_id, user.account_id).await {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "installation not found"}))).into_response();
    }

    let token = match github::installation_token(&state.http_client, installation_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, installation_id, "github list_repos: could not mint installation token");
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": "could not reach GitHub"}))).into_response();
        }
    };

    let url = format!("{}/installation/repositories?per_page=100", github::github_api_base());
    let live = match github::get_paginated(&state.http_client, &token, &url, 20).await {
        Ok(items) => items,
        Err(GithubApiError::RateLimited { retry_after }) => {
            return (
                StatusCode::TOO_MANY_REQUESTS,
                Json(json!({"error": "rate limited by GitHub", "retry_after_secs": retry_after.as_secs()})),
            )
                .into_response();
        }
        Err(e) => {
            tracing::warn!(error = %e, installation_id, "github list_repos: live repo listing failed");
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": "could not reach GitHub"}))).into_response();
        }
    };
    // `GET /installation/repositories` wraps the array in `{"repositories": [...]}`,
    // not a bare array — `get_paginated`'s generic array-or-object handling
    // pushes the WHOLE first-page object as one item when the top level is
    // an object, so unwrap it here rather than teaching the paginator this
    // one endpoint's shape.
    let live_repos: Vec<serde_json::Value> = live
        .into_iter()
        .flat_map(|page| match page {
            serde_json::Value::Object(mut obj) => obj
                .remove("repositories")
                .and_then(|v| v.as_array().cloned())
                .unwrap_or_default(),
            other => vec![other],
        })
        .collect();

    let existing = match fetch_existing_rows(&tenant_pool, user.account_id, installation_id).await {
        Ok(rows) => rows,
        Err(e) => {
            tracing::error!(error = %e, installation_id, "github list_repos: existing-rows query failed");
            return internal_error();
        }
    };

    let repos = merge_repo_views(&live_repos, &existing);
    let destinations = build_destinations(control_pool, &tenant_pool, user.account_id, user.user_id).await;

    (StatusCode::OK, Json(json!({"repos": repos, "destinations": destinations}))).into_response()
}

async fn installation_belongs_to_account(control_pool: &PgPool, installation_id: i64, account_id: Uuid) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS(SELECT 1 FROM github_installations WHERE installation_id = $1 AND account_id = $2)",
    )
    .bind(installation_id)
    .bind(account_id)
    .fetch_one(control_pool)
    .await
    .unwrap_or(false)
}

/// One `github_watched_repos` row's fields, as needed to merge with a live
/// GitHub listing. Deliberately does NOT carry `default_branch`/
/// `visibility`/`archived`/`fork`/`size_kb` — the merge always prefers the
/// LIVE GitHub values for those (a fresher signal than whatever this row's
/// `last_synced_at` may reflect), so loading a second, potentially-stale
/// copy of them here would be dead weight.
struct ExistingRepoRow {
    row_id: Uuid,
    repo_id: i64,
    active: bool,
    paused_reason: Option<String>,
    graph_id: Option<Uuid>,
    destination_kind: Option<String>,
    last_synced_at: Option<chrono::DateTime<chrono::Utc>>,
    sync_stopped_at: Option<chrono::DateTime<chrono::Utc>>,
    /// Migration `0250`/`0102`. See [`RepoView::sync_error_code`].
    sync_error_code: Option<String>,
    sync_error_at: Option<chrono::DateTime<chrono::Utc>>,
    /// `connectors::github_backfill::BackfillProgress`, written by the real
    /// backfill worker (phase 2) — `null` until the first tick of the
    /// backfill for this row runs. Read here as a raw `Value` rather than
    /// the typed struct so this route (control/selection surface) does not
    /// need a dependency on the worker-internal type; the JSON SHAPE is the
    /// contract between the two.
    backfill_progress: Option<serde_json::Value>,
}

async fn fetch_existing_rows(
    tenant_pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
) -> Result<Vec<ExistingRepoRow>, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("fetch_existing_rows scoped tx: {e}")))?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(
        Uuid,
        i64,
        bool,
        Option<String>,
        Option<Uuid>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<String>,
        Option<chrono::DateTime<chrono::Utc>>,
        Option<serde_json::Value>,
    )> = sqlx::query_as(
        "SELECT id, repo_id, active, paused_reason, graph_id, destination_kind, last_synced_at, sync_stopped_at, \
                sync_error_code, sync_error_at, backfill_progress \
         FROM github_watched_repos WHERE account_id = $1 AND installation_id = $2",
    )
    .bind(account_id)
    .bind(installation_id)
    .fetch_all(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(rows
        .into_iter()
        .map(
            |(row_id, repo_id, active, paused_reason, graph_id, destination_kind, last_synced_at, sync_stopped_at, sync_error_code, sync_error_at, backfill_progress)| {
                ExistingRepoRow {
                    row_id,
                    repo_id,
                    active,
                    paused_reason,
                    graph_id,
                    destination_kind,
                    last_synced_at,
                    sync_stopped_at,
                    sync_error_code,
                    sync_error_at,
                    backfill_progress,
                }
            },
        )
        .collect())
}

/// Shape `github_watched_repos.backfill_progress` into this route's own
/// `ProgressView`. `connectors::github_backfill::BackfillProgress` writes
/// `{prs, issues, releases, commits, updated_at}` by merging into the
/// column, so older rows may also carry the retired code tier's keys, which
/// are ignored here. `None`/malformed input -> `None` (never a fabricated
/// all-`null` `ProgressView` object — the selection UI's own contract is
/// "no `progress` key at all until the first backfill tick has run", not a
/// placeholder).
fn progress_view_from(raw: &Option<serde_json::Value>) -> Option<ProgressView> {
    let v = raw.as_ref()?;
    let pct = |key: &str| -> Option<u8> { v.get(key).and_then(serde_json::Value::as_u64).map(|n| n.min(100) as u8) };
    Some(ProgressView {
        prs: pct("prs"),
        issues: pct("issues"),
        releases: pct("releases"),
        commits: pct("commits"),
    })
}

fn merge_repo_views(live: &[serde_json::Value], existing: &[ExistingRepoRow]) -> Vec<RepoView> {
    live.iter()
        .filter_map(|repo| {
            let repo_id = repo.get("id").and_then(serde_json::Value::as_i64)?;
            let owner = repo.get("owner").and_then(|o| o.get("login")).and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
            let name = repo.get("name").and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
            let full_name = repo.get("full_name").and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
            let default_branch = repo.get("default_branch").and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
            let archived = repo.get("archived").and_then(serde_json::Value::as_bool).unwrap_or(false);
            let fork = repo.get("fork").and_then(serde_json::Value::as_bool).unwrap_or(false);
            let private = repo.get("private").and_then(serde_json::Value::as_bool).unwrap_or(true);
            let visibility = repo
                .get("visibility")
                .and_then(serde_json::Value::as_str)
                .map(str::to_string)
                .unwrap_or_else(|| if private { "private".to_string() } else { "public".to_string() });
            let size_kb = repo.get("size").and_then(serde_json::Value::as_i64).unwrap_or(0);

            let row = existing.iter().find(|r| r.repo_id == repo_id);
            match row {
                Some(r) => Some(RepoView {
                    row_id: Some(r.row_id),
                    repo_id,
                    owner,
                    name,
                    full_name,
                    default_branch,
                    visibility,
                    archived,
                    fork,
                    size_kb,
                    active: r.active,
                    paused_reason: r.paused_reason.clone(),
                    // `team_id`/`team_name` are left unresolved here
                    // (`None`) — doing so would need an extra `teams`
                    // lookup by `graph_id` per row; deferred, see this
                    // file's own module doc.
                    destination: r.graph_id.map(|_graph_id| DestinationView {
                        kind: r.destination_kind.clone().unwrap_or_default(),
                        team_id: None,
                        team_name: None,
                    }),
                    progress: progress_view_from(&r.backfill_progress),
                    suggested: None,
                    last_synced_at: r.last_synced_at,
                    sync_stopped_at: r.sync_stopped_at,
                    sync_error_code: r.sync_error_code.clone(),
                    sync_error_at: r.sync_error_at,
                }),
                None => Some(RepoView {
                    row_id: None,
                    repo_id,
                    owner,
                    name,
                    full_name,
                    default_branch,
                    visibility,
                    archived,
                    fork,
                    size_kb,
                    active: false,
                    paused_reason: None,
                    destination: None,
                    progress: None,
                    suggested: Some(!archived && !fork),
                    last_synced_at: None,
                    sync_stopped_at: None,
                    // No watch row means nothing has ever tried to sync this
                    // repo, so there is no failure to report — distinct from
                    // "a row exists and is not currently failing", which is
                    // also `None` but arrives from a real column read.
                    sync_error_code: None,
                    sync_error_at: None,
                }),
            }
        })
        .collect()
}

async fn build_destinations(control_pool: &PgPool, tenant_pool: &PgPool, account_id: Uuid, user_id: Uuid) -> DestinationsView {
    let member_count: i64 = sqlx::query_scalar("SELECT count(*) FROM account_members WHERE account_id = $1")
        .bind(account_id)
        .fetch_one(control_pool)
        .await
        .unwrap_or(0);

    let allowed = crate::tenancy::scope::verify_org_admin(control_pool, account_id, user_id)
        .await
        .ok()
        .flatten()
        .is_some();

    let teams: Vec<(Uuid, String, bool)> = {
        let scoped = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id).await;
        match scoped {
            Ok(mut tx) => {
                let rows: Vec<(Uuid, String, bool)> = sqlx::query_as(
                    "SELECT t.id, t.name, EXISTS(SELECT 1 FROM team_members m \
                       WHERE m.team_id = t.id AND m.user_id = $2 AND m.role = 'admin' \
                         AND m.departing_at IS NULL) \
                     FROM teams t WHERE t.account_id = $1",
                )
                .bind(account_id)
                .bind(user_id)
                .fetch_all(&mut *tx)
                .await
                .unwrap_or_default();
                tx.rollback().await.ok();
                rows
            }
            Err(_) => Vec::new(),
        }
    };

    let team_options: Vec<TeamOption> = teams
        .into_iter()
        .map(|(id, name, is_admin)| TeamOption { id, name, is_admin })
        .collect();

    DestinationsView {
        personal: true,
        teams: team_options,
        org: OrgDestinationView { allowed, member_count, non_collaborator_count: None },
    }
}

// ---------------------------------------------------------------------------
// PUT /v1/connectors/github/repos
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
pub struct PutReposBody {
    /// Accepts a JSON number OR a numeric string. GitHub installation ids
    /// are integers, `GET .../installs` returns them as numbers, and the web
    /// client sends the number straight back — the first real install round
    /// trip (2026-09-21) hit a bare `HTTP 422` here because this field was
    /// declared `String`, so axum's `Json` extractor rejected the body
    /// before the handler ever ran. Lenient on purpose: an id typed by hand
    /// into a curl still works.
    #[serde(deserialize_with = "installation_id_num_or_string")]
    installation_id: i64,
    selections: Vec<Selection>,
}

/// serde helper for [`PutReposBody::installation_id`]: `163636527` and
/// `"163636527"` both deserialize to `163636527`.
fn installation_id_num_or_string<'de, D: serde::Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum NumOrStr {
        Num(i64),
        Str(String),
    }
    match NumOrStr::deserialize(d)? {
        NumOrStr::Num(n) => Ok(n),
        NumOrStr::Str(s) => s.trim().parse::<i64>().map_err(serde::de::Error::custom),
    }
}

#[derive(Deserialize)]
struct Selection {
    repo_id: i64,
    active: bool,
    #[serde(default)]
    destination: Option<DestinationInput>,
}

#[derive(Deserialize)]
struct DestinationInput {
    kind: String,
    #[serde(default)]
    team_id: Option<Uuid>,
}

pub async fn put_repos(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PutReposBody>,
) -> Response {
    if let Some(reject) = reject_org_github_management(&state, &tenant).await {
        return reject;
    }
    let installation_id = body.installation_id;
    let Some(control_pool) = state.control_db() else {
        return service_unavailable();
    };
    let Some(tenant_pool) = tenant.pool.clone() else {
        return service_unavailable();
    };

    if !installation_belongs_to_account(control_pool, installation_id, user.account_id).await {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "installation not found"}))).into_response();
    }

    let token = match github::installation_token(&state.http_client, installation_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, installation_id, "github put_repos: could not mint installation token");
            return (StatusCode::BAD_GATEWAY, Json(json!({"error": "could not reach GitHub"}))).into_response();
        }
    };

    let mut prepared = Vec::with_capacity(body.selections.len());
    for selection in &body.selections {
        match validate_selection(&state, control_pool, &tenant_pool, &token, user.account_id, user.user_id, selection).await
        {
            Ok(row) => prepared.push(row),
            Err(resp) => return resp,
        }
    }
    for row in &prepared {
        if let Err(resp) = commit_selection(&state, control_pool, &tenant_pool, user.account_id, user.user_id, installation_id, row).await
        {
            return resp;
        }
    }

    // Re-render the merged view (same shape as GET) so the caller sees the
    // result of its own write without a second round trip.
    let url = format!("{}/installation/repositories?per_page=100", github::github_api_base());
    let live = github::get_paginated(&state.http_client, &token, &url, 20).await.unwrap_or_default();
    let live_repos: Vec<serde_json::Value> = live
        .into_iter()
        .flat_map(|page| match page {
            serde_json::Value::Object(mut obj) => obj.remove("repositories").and_then(|v| v.as_array().cloned()).unwrap_or_default(),
            other => vec![other],
        })
        .collect();
    let existing = fetch_existing_rows(&tenant_pool, user.account_id, installation_id).await.unwrap_or_default();
    let repos = merge_repo_views(&live_repos, &existing);
    let destinations = build_destinations(control_pool, &tenant_pool, user.account_id, user.user_id).await;

    (StatusCode::OK, Json(json!({"repos": repos, "destinations": destinations}))).into_response()
}

struct PreparedSelection {
    repo_id: i64,
    active: bool,
    owner: String,
    name: String,
    default_branch: String,
    visibility: String,
    archived: bool,
    fork: bool,
    size_kb: i64,
    requested: Option<RequestedDestination>,
    resolved: Option<github_repos::ResolvedDestination>,
}

/// Permission checks and GitHub metadata lookup only — no watch-row writes.
/// `NewTeam` is always allowed and creates on commit, so it is not resolved here.
async fn validate_selection(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    token: &str,
    account_id: Uuid,
    user_id: Uuid,
    selection: &Selection,
) -> Result<PreparedSelection, Response> {
    let repo_url = format!("{}/repositories/{}", github::github_api_base(), selection.repo_id);
    let repo = github::get_one(&state.http_client, token, &repo_url).await.map_err(|e| {
        tracing::warn!(error = %e, repo_id = selection.repo_id, "github put_repos: repo metadata lookup failed");
        (StatusCode::BAD_GATEWAY, Json(json!({"error": "could not reach GitHub for this repo"}))).into_response()
    })?;
    let owner = repo.get("owner").and_then(|o| o.get("login")).and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
    let name = repo.get("name").and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
    let default_branch = repo.get("default_branch").and_then(serde_json::Value::as_str).unwrap_or_default().to_string();
    let archived = repo.get("archived").and_then(serde_json::Value::as_bool).unwrap_or(false);
    let fork = repo.get("fork").and_then(serde_json::Value::as_bool).unwrap_or(false);
    let private = repo.get("private").and_then(serde_json::Value::as_bool).unwrap_or(true);
    let visibility = if private { "private" } else { "public" }.to_string();
    let size_kb = repo.get("size").and_then(serde_json::Value::as_i64).unwrap_or(0);

    if !selection.active {
        return Ok(PreparedSelection {
            repo_id: selection.repo_id,
            active: false,
            owner,
            name,
            default_branch,
            visibility,
            archived,
            fork,
            size_kb,
            requested: None,
            resolved: None,
        });
    }

    let Some(destination) = &selection.destination else {
        return Err((StatusCode::BAD_REQUEST, Json(json!({"error": "destination is required when active is true"}))).into_response());
    };
    let requested = match destination.kind.as_str() {
        "personal" => RequestedDestination::Personal,
        "team" => match destination.team_id {
            Some(team_id) => RequestedDestination::ExistingTeam { team_id },
            None => RequestedDestination::NewTeam,
        },
        "org" => RequestedDestination::Org,
        other => {
            return Err((StatusCode::BAD_REQUEST, Json(json!({"error": format!("unknown destination kind: {other}")}))).into_response());
        }
    };

    let resolved = if matches!(requested, RequestedDestination::NewTeam) {
        None
    } else {
        Some(
            github_repos::resolve_destination(control_pool, tenant_pool, account_id, user_id, requested.clone(), &owner, &name)
                .await
                .map_err(|e| destination_error_response(control_pool, tenant_pool, account_id, user_id, e))?,
        )
    };

    Ok(PreparedSelection {
        repo_id: selection.repo_id,
        active: true,
        owner,
        name,
        default_branch,
        visibility,
        archived,
        fork,
        size_kb,
        requested: Some(requested),
        resolved,
    })
}

#[allow(clippy::too_many_arguments)]
async fn commit_selection(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    tenant_pool: &PgPool,
    account_id: Uuid,
    user_id: Uuid,
    installation_id: i64,
    row: &PreparedSelection,
) -> Result<(), Response> {
    if !row.active {
        github_disconnect::pause_repos(tenant_pool, account_id, installation_id, Some(row.repo_id), paused_reason::ADMIN_TOGGLED_OFF)
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "github put_repos: pause_repos failed");
                internal_error()
            })?;
        return Ok(());
    }

    let resolved = match row.resolved {
        Some(resolved) => resolved,
        None => {
            let requested = row.requested.clone().ok_or_else(|| {
                (StatusCode::BAD_REQUEST, Json(json!({"error": "destination is required when active is true"}))).into_response()
            })?;
            github_repos::resolve_destination(control_pool, tenant_pool, account_id, user_id, requested, &row.owner, &row.name)
                .await
                .map_err(|e| destination_error_response(control_pool, tenant_pool, account_id, user_id, e))?
        }
    };

    let repo_row_id = upsert_watched_repo(
        tenant_pool,
        account_id,
        installation_id,
        row.repo_id,
        &row.owner,
        &row.name,
        &row.default_branch,
        &row.visibility,
        row.archived,
        row.fork,
        row.size_kb,
        resolved.graph_id,
        resolved.kind.as_str(),
        user_id,
    )
    .await
    .map_err(|e| {
        tracing::error!(error = %e, "github put_repos: upsert_watched_repo failed");
        internal_error()
    })?;

    if let Some(new_team_id) = resolved.newly_created_team {
        let outcome = github_repos::seat_repo_collaborators(
            &state.http_client,
            control_pool,
            tenant_pool,
            account_id,
            new_team_id,
            installation_id,
            row.repo_id,
            &row.owner,
            &row.name,
        )
        .await;
        match outcome {
            Ok(o) => tracing::info!(seated = o.seated, unmapped = o.unmapped, repo_id = row.repo_id, "github: auto-seated repo-mirrored team"),
            Err(e) => tracing::warn!(error = %e, repo_id = row.repo_id, "github: collaborator auto-seat failed (non-fatal — the team ocean still exists)"),
        }
    }

    // Enqueue backfill for a newly-active row. `idempotency_key` scoped to
    // the row id so re-saving the same selection (no state change) does not
    // pile up duplicate pending jobs.
    if let Err(e) = job_queue::enqueue(
        tenant_pool,
        &jobs::JobKind::GithubBackfill { repo_row_id },
        Some(account_id),
        Some(&format!("github_backfill:{repo_row_id}")),
        0,
    )
    .await
    {
        tracing::error!(error = %e, %repo_row_id, "github put_repos: failed to enqueue GithubBackfill job");
    }

    Ok(())
}

fn destination_error_response(control_pool: &PgPool, _tenant_pool: &PgPool, _account_id: Uuid, _user_id: Uuid, e: DestinationError) -> Response {
    match e {
        DestinationError::OrgRequiresAdmin => (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "org_destination_requires_admin",
                "allowed_options": ["personal", "team"],
            })),
        )
            .into_response(),
        DestinationError::TeamRequiresAdmin { team_name } => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": format!("'{team_name}' requires a team admin to route a repo into it")})),
        )
            .into_response(),
        DestinationError::TeamNotFound => (StatusCode::NOT_FOUND, Json(json!({"error": "team not found"}))).into_response(),
        DestinationError::NoOrgOcean => {
            tracing::error!("github put_repos: account has no org ocean — should be unreachable");
            let _ = control_pool;
            internal_error()
        }
        DestinationError::Db(e) => {
            tracing::error!(error = %e, "github put_repos: destination resolution db error");
            internal_error()
        }
    }
}

#[allow(clippy::too_many_arguments)]
/// The routed-row conflict target MUST carry `WHERE graph_id IS NOT NULL`:
/// `uq_github_watched_repos_routed` is a PARTIAL unique index (migration
/// 0235 / tenant 0095 — two disjoint indexes split the pending and routed
/// shapes), and Postgres only infers a partial index as an arbiter when the
/// `ON CONFLICT` predicate matches it. Without the predicate the statement
/// fails with "there is no unique or exclusion constraint matching the ON
/// CONFLICT specification" — which is exactly what the first real save on
/// the selection page hit (2026-09-21), because every earlier test seeded
/// rows directly and never ran this upsert.
async fn upsert_watched_repo(
    tenant_pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    repo_id: i64,
    owner: &str,
    name: &str,
    default_branch: &str,
    visibility: &str,
    archived: bool,
    fork: bool,
    size_kb: i64,
    graph_id: Uuid,
    destination_kind: &str,
    owner_user_id: Uuid,
) -> Result<Uuid, sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("upsert_watched_repo scoped tx: {e}")))?;

    // Claim a pending row (graph_id IS NULL) for this repo if one exists —
    // routing it is not "a second row", it is that same grant finally
    // getting a destination. Otherwise fall through to the per-destination
    // upsert below.
    let claimed: Option<Uuid> = sqlx::query_scalar(
        "UPDATE github_watched_repos \
         SET graph_id = $1, destination_kind = $2, owner_user_id = $3, active = true, \
             paused_reason = NULL, sync_stopped_at = NULL, updated_at = now(), \
             owner = $5, name = $6, default_branch = $7, visibility = $8, archived = $9, fork = $10, size_kb = $11 \
         WHERE account_id = $4 AND installation_id = $12 AND repo_id = $13 AND graph_id IS NULL \
         RETURNING id",
    )
    .bind(graph_id)
    .bind(destination_kind)
    .bind(owner_user_id)
    .bind(account_id)
    .bind(owner)
    .bind(name)
    .bind(default_branch)
    .bind(visibility)
    .bind(archived)
    .bind(fork)
    .bind(size_kb)
    .bind(installation_id)
    .bind(repo_id)
    .fetch_optional(&mut *tx)
    .await?;

    let row_id = if let Some(id) = claimed {
        id
    } else {
        sqlx::query_scalar(
            "INSERT INTO github_watched_repos \
                (account_id, installation_id, repo_id, owner, name, default_branch, visibility, \
                 archived, fork, size_kb, active, graph_id, destination_kind, owner_user_id) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true, $11, $12, $13) \
             ON CONFLICT (account_id, installation_id, repo_id, graph_id) WHERE graph_id IS NOT NULL DO UPDATE SET \
                active = true, paused_reason = NULL, sync_stopped_at = NULL, updated_at = now(), \
                owner = EXCLUDED.owner, name = EXCLUDED.name, default_branch = EXCLUDED.default_branch, \
                visibility = EXCLUDED.visibility, archived = EXCLUDED.archived, fork = EXCLUDED.fork, \
                size_kb = EXCLUDED.size_kb \
             RETURNING id",
        )
        .bind(account_id)
        .bind(installation_id)
        .bind(repo_id)
        .bind(owner)
        .bind(name)
        .bind(default_branch)
        .bind(visibility)
        .bind(archived)
        .bind(fork)
        .bind(size_kb)
        .bind(graph_id)
        .bind(destination_kind)
        .bind(owner_user_id)
        .fetch_one(&mut *tx)
        .await?
    };

    tx.commit().await?;
    Ok(row_id)
}

// ---------------------------------------------------------------------------
// DELETE /v1/connectors/github/installs/{installation_id}
// ---------------------------------------------------------------------------

pub async fn delete_install(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(installation_id): Path<String>,
) -> Response {
    if let Some(reject) = reject_org_github_management(&state, &tenant).await {
        return reject;
    }
    let Ok(installation_id) = installation_id.parse::<i64>() else {
        return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid installation_id"}))).into_response();
    };
    let Some(control_pool) = state.control_db() else {
        return service_unavailable();
    };
    let Some(tenant_pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    if !installation_belongs_to_account(control_pool, installation_id, user.account_id).await {
        return (StatusCode::NOT_FOUND, Json(json!({"error": "installation not found"}))).into_response();
    }

    let paused = github_disconnect::pause_repos(&tenant_pool, user.account_id, installation_id, None, paused_reason::UNINSTALLED)
        .await
        .unwrap_or(0);

    (StatusCode::OK, Json(json!({"status": "disconnected", "paused": paused}))).into_response()
}

// ---------------------------------------------------------------------------
// POST /v1/github/events — the App webhook
// ---------------------------------------------------------------------------

/// Bound on webhook body size read into memory — GitHub's own payload cap is
/// 25MB; this is a defensive backstop against a pathological body, same
/// convention `connectors::slack`'s own response readers use.
pub(crate) const MAX_WEBHOOK_BODY_BYTES: usize = 25 * 1024 * 1024;

pub async fn github_events(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    if body.len() > MAX_WEBHOOK_BODY_BYTES {
        return (StatusCode::PAYLOAD_TOO_LARGE, Json(json!({"error": "payload too large"}))).into_response();
    }

    let secret = match github::webhook_secret() {
        Ok(s) => s,
        Err(_) => {
            tracing::error!("github webhook: GITHUB_WEBHOOK_SECRET not configured — the connector is offline");
            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "GITHUB_WEBHOOK_SECRET not configured"}))).into_response();
        }
    };

    let Some(sig_header) = headers.get("x-hub-signature-256").and_then(|v| v.to_str().ok()) else {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "missing signature"}))).into_response();
    };
    if !github::verify_webhook_signature(&secret, &body, sig_header) {
        return (StatusCode::UNAUTHORIZED, Json(json!({"error": "signature mismatch"}))).into_response();
    }

    let event = headers.get("x-github-event").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();
    let delivery_id = headers.get("x-github-delivery").and_then(|v| v.to_str().ok()).unwrap_or("").to_string();

    if event == "ping" {
        return (StatusCode::OK, Json(json!({"status": "ok"}))).into_response();
    }

    let payload: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return (StatusCode::BAD_REQUEST, Json(json!({"error": "invalid JSON"}))).into_response(),
    };

    let Some(installation_id) = payload.get("installation").and_then(|i| i.get("id")).and_then(serde_json::Value::as_i64) else {
        // No installation context at all — nothing we can route. Ack so
        // GitHub does not retry forever.
        return (StatusCode::ACCEPTED, Json(json!({"status": "ignored"}))).into_response();
    };

    let Some(control_pool) = state.control_db() else {
        return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response();
    };

    // I8: resolve the tenant BEFORE touching any tenant data. Unknown or
    // suspended -> 202, nothing written on either plane.
    let resolved = match github::resolve_installation(control_pool, installation_id).await {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, installation_id, "github webhook: installation lookup failed");
            return (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response();
        }
    };
    let Some((account_id, suspended)) = resolved else {
        tracing::info!(installation_id, event = %event, connector_health = true, "github webhook: unknown installation — refusing (I8), nothing written");
        return (StatusCode::ACCEPTED, Json(json!({"status": "unknown installation"}))).into_response();
    };
    if suspended && event != "installation" {
        tracing::info!(installation_id, event = %event, connector_health = true, "github webhook: suspended installation — refusing (I8), nothing written");
        return (StatusCode::ACCEPTED, Json(json!({"status": "suspended"}))).into_response();
    }

    match event.as_str() {
        "installation" => handle_installation_event(&state, control_pool, account_id, installation_id, &payload).await,
        "installation_repositories" => handle_installation_repositories_event(&state, control_pool, account_id, installation_id, &payload).await,
        _ => {
            // Every other event type: durable hand-off — see
            // `jobs::JobKind::GithubEventIngest`'s own doc comment for why
            // this is a stub consumer for now.
            let clerk_org_id = clerk_org_id_for(control_pool, account_id).await;
            match resolve_tenant_pool(&state, control_pool, account_id, clerk_org_id.as_deref()).await {
                Some(pool) => {
                    if let Err(e) = job_queue::enqueue(
                        &pool,
                        &jobs::JobKind::GithubEventIngest {
                            account_id,
                            installation_id,
                            delivery_id: delivery_id.clone(),
                            event: event.clone(),
                            payload: payload.clone(),
                        },
                        Some(account_id),
                        Some(&format!("github_event:{delivery_id}")),
                        0,
                    )
                    .await
                    {
                        tracing::error!(error = %e, installation_id, event = %event, "github webhook: failed to enqueue GithubEventIngest");
                    }
                }
                None => {
                    tracing::warn!(installation_id, event = %event, connector_health = true, "github webhook: could not resolve a tenant pool — event dropped");
                }
            }
            (StatusCode::OK, Json(json!({"status": "queued"}))).into_response()
        }
    }
}

async fn handle_installation_event(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    payload: &serde_json::Value,
) -> Response {
    let action = payload.get("action").and_then(serde_json::Value::as_str).unwrap_or("");
    match action {
        "deleted" => {
            let removed = github::set_installation_lifecycle(control_pool, installation_id, InstallationLifecycle::Deleted).await;
            if let Err(e) = removed {
                tracing::error!(error = %e, installation_id, "github webhook: installation.deleted lifecycle update failed");
            }
            pause_all_best_effort(state, control_pool, account_id, installation_id, paused_reason::UNINSTALLED).await;
        }
        "suspend" => {
            if let Err(e) = github::set_installation_lifecycle(control_pool, installation_id, InstallationLifecycle::Suspended).await {
                tracing::error!(error = %e, installation_id, "github webhook: installation.suspend lifecycle update failed");
            }
            pause_all_best_effort(state, control_pool, account_id, installation_id, paused_reason::UNINSTALLED).await;
        }
        "unsuspend" => {
            if let Err(e) = github::set_installation_lifecycle(control_pool, installation_id, InstallationLifecycle::Unsuspended).await {
                tracing::error!(error = %e, installation_id, "github webhook: installation.unsuspend lifecycle update failed");
            }
            // Deliberately does NOT reactivate any paused repo — an unsuspend
            // only removes the block on FUTURE selection/backfill; §3.3's
            // "granted, never auto-backfilled" rule still applies to
            // whatever was paused while suspended.
        }
        "created" => {
            // The webhook can race the OAuth callback (which is what
            // actually records the installation — see `record_installation`'s
            // own doc comment). If we don't know this installation yet,
            // `resolve_installation` already returned `None` above and this
            // handler was never reached; nothing further to do here.
        }
        other => {
            tracing::debug!(installation_id, action = other, "github webhook: unhandled installation action");
        }
    }
    (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
}

async fn handle_installation_repositories_event(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    payload: &serde_json::Value,
) -> Response {
    let action = payload.get("action").and_then(serde_json::Value::as_str).unwrap_or("");
    let clerk_org_id = clerk_org_id_for(control_pool, account_id).await;
    let Some(tenant_pool) = resolve_tenant_pool(state, control_pool, account_id, clerk_org_id.as_deref()).await else {
        tracing::warn!(installation_id, connector_health = true, "github webhook: installation_repositories — could not resolve tenant pool");
        return (StatusCode::OK, Json(json!({"status": "ok"}))).into_response();
    };

    match action {
        "removed" => {
            if let Some(repos) = payload.get("repositories_removed").and_then(serde_json::Value::as_array) {
                for repo in repos {
                    if let Some(repo_id) = repo.get("id").and_then(serde_json::Value::as_i64) {
                        if let Err(e) =
                            github_disconnect::pause_repos(&tenant_pool, account_id, installation_id, Some(repo_id), paused_reason::REPO_REMOVED).await
                        {
                            tracing::error!(error = %e, repo_id, "github webhook: pause_repos (repo removed) failed");
                        }
                    }
                }
            }
        }
        "added" => {
            if let Some(repos) = payload.get("repositories_added").and_then(serde_json::Value::as_array) {
                for repo in repos {
                    let (Some(repo_id), Some(full_name)) = (
                        repo.get("id").and_then(serde_json::Value::as_i64),
                        repo.get("full_name").and_then(serde_json::Value::as_str),
                    ) else {
                        continue;
                    };
                    let (owner, name) = full_name.split_once('/').unwrap_or(("", full_name));
                    if let Err(e) = insert_pending_repo(&tenant_pool, account_id, installation_id, repo_id, owner, name).await {
                        tracing::error!(error = %e, repo_id, "github webhook: insert_pending_repo failed");
                    }
                }
            }
        }
        other => {
            tracing::debug!(installation_id, action = other, "github webhook: unhandled installation_repositories action");
        }
    }

    (StatusCode::OK, Json(json!({"status": "ok"}))).into_response()
}

async fn insert_pending_repo(tenant_pool: &PgPool, account_id: Uuid, installation_id: i64, repo_id: i64, owner: &str, name: &str) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(tenant_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("insert_pending_repo scoped tx: {e}")))?;
    sqlx::query(
        "INSERT INTO github_watched_repos (account_id, installation_id, repo_id, owner, name, active, paused_reason) \
         VALUES ($1, $2, $3, $4, $5, false, $6) \
         ON CONFLICT (account_id, installation_id, repo_id) WHERE graph_id IS NULL DO NOTHING",
    )
    .bind(account_id)
    .bind(installation_id)
    .bind(repo_id)
    .bind(owner)
    .bind(name)
    .bind(paused_reason::PENDING_SELECTION)
    .execute(&mut *tx)
    .await?;
    tx.commit().await
}

async fn pause_all_best_effort(state: &Arc<AppState>, control_pool: &PgPool, account_id: Uuid, installation_id: i64, reason: &str) {
    let clerk_org_id = clerk_org_id_for(control_pool, account_id).await;
    match resolve_tenant_pool(state, control_pool, account_id, clerk_org_id.as_deref()).await {
        Some(tenant_pool) => {
            if let Err(e) = github_disconnect::pause_repos(&tenant_pool, account_id, installation_id, None, reason).await {
                tracing::error!(error = %e, installation_id, "github webhook: pause_all (installation lifecycle) failed");
            }
        }
        None => {
            tracing::warn!(installation_id, connector_health = true, "github webhook: could not resolve tenant pool to pause repos");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The first real install round trip failed with a bare 422: the web
    /// client sends `installation_id` as the JSON number GitHub uses, and the
    /// body struct said `String`. Both shapes must deserialize.
    #[test]
    fn put_repos_body_accepts_installation_id_as_number_or_string() {
        let as_number: PutReposBody =
            serde_json::from_str(r#"{"installation_id":163636527,"selections":[]}"#).expect("number form");
        assert_eq!(as_number.installation_id, 163_636_527);
        let as_string: PutReposBody =
            serde_json::from_str(r#"{"installation_id":"163636527","selections":[]}"#).expect("string form");
        assert_eq!(as_string.installation_id, 163_636_527);
        assert!(serde_json::from_str::<PutReposBody>(r#"{"installation_id":"not-an-id","selections":[]}"#).is_err());
    }
    use axum::{body::Body, http::Request as HttpRequest, routing::post, Router};
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn webhook_router(state: Arc<AppState>) -> Router {
        Router::new().route("/v1/github/events", post(github_events)).with_state(state)
    }

    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    #[tokio::test]
    async fn webhook_missing_secret_returns_503() {
        let _guard = env_lock().lock().unwrap();
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };
        let router = webhook_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "ping")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn webhook_missing_signature_header_returns_401() {
        let _guard = env_lock().lock().unwrap();
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", "test-secret") };
        let router = webhook_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "ping")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };
    }

    #[tokio::test]
    async fn webhook_tampered_signature_returns_401() {
        let _guard = env_lock().lock().unwrap();
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", "test-secret") };
        let router = webhook_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "ping")
            .header("x-hub-signature-256", "sha256=0000000000000000000000000000000000000000000000000000000000000000")
            .body(Body::from("{}"))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };
    }

    #[tokio::test]
    async fn webhook_valid_signature_ping_returns_200() {
        let _guard = env_lock().lock().unwrap();
        let secret = "test-secret";
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", secret) };
        let router = webhook_router(test_state());
        let body = b"{\"zen\":\"hi\"}".to_vec();
        use hmac::Mac;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(&body);
        let sig = format!("sha256={}", hex::encode(mac.finalize().into_bytes()));
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "ping")
            .header("x-hub-signature-256", sig)
            .body(Body::from(body))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };
    }
}

/// pg-gated: conformance test 2 (I8) — an inbound webhook for a
/// missing/suspended tenant writes nothing, on either plane. Proves it
/// through the REAL route handler against a real control database, not a
/// hand-composed stand-in.
///
///   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
///     cargo test -p coralswarm-backend --lib routes::github::webhook_installation_resolution_pg_tests
#[cfg(test)]
mod webhook_installation_resolution_pg_tests {
    use super::*;
    use axum::{body::Body, http::Request as HttpRequest, routing::post, Router};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection};
    use tower::ServiceExt;

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
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn drop_db(admin_conn: &mut PgConnection, db: &str) {
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    fn env_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    fn sign(secret: &str, body: &[u8]) -> String {
        use hmac::Mac;
        let mut mac = hmac::Hmac::<sha2::Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn webhook_router(state: Arc<AppState>) -> Router {
        Router::new().route("/v1/github/events", post(github_events)).with_state(state)
    }

    /// Conformance test 2, unknown-installation half: no `github_installations`
    /// row for this `installation.id` at all -> 202, and `github_watched_repos`
    /// (the only tenant-plane table a `push` event could otherwise touch) gets
    /// zero rows.
    /// The first real save on the selection page (2026-09-21) failed with
    /// "there is no unique or exclusion constraint matching the ON CONFLICT
    /// specification": the routed-row upsert named the index columns but not
    /// the partial index's `WHERE graph_id IS NOT NULL` predicate. Every
    /// earlier pg test seeded `github_watched_repos` directly and never ran
    /// this statement. Runs the REAL upsert twice: the second call must land
    /// on the conflict arm (same row id, no duplicate), and a second
    /// destination for the same repo must be a second row (§3.4).
    #[tokio::test]
    async fn upsert_watched_repo_hits_the_partial_unique_index_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping upsert_watched_repo_hits_the_partial_unique_index_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "cs_gh_route_upsert_partial_idx";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = Uuid::new_v4();
        let user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(user_id)
            .bind(format!("{user_id}@example.com"))
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, 'org_upsert_partial')")
            .bind(account_id)
            .bind(user_id)
            .execute(&pool)
            .await
            .unwrap();
        let mut graphs = Vec::new();
        for name in ["ocean-a", "ocean-b"] {
            let g = Uuid::new_v4();
            sqlx::query("INSERT INTO graphs (id, account_id, name, kind, is_primary) VALUES ($1, $2, $3, 'personal', false)")
                .bind(g)
                .bind(account_id)
                .bind(name)
                .execute(&pool)
                .await
                .unwrap();
            graphs.push(g);
        }

        let first = upsert_watched_repo(
            &pool, account_id, 910_100, 5_550_100, "acme", "repo", "main", "private", false, false, 29,
            graphs[0], "personal", user_id,
        )
        .await
        .expect("first upsert inserts");
        let again = upsert_watched_repo(
            &pool, account_id, 910_100, 5_550_100, "acme", "repo", "main", "private", false, false, 31,
            graphs[0], "personal", user_id,
        )
        .await
        .expect("second upsert must take the ON CONFLICT arm, not error");
        assert_eq!(first, again, "re-saving the same repo+destination updates the row in place");

        let other_ocean = upsert_watched_repo(
            &pool, account_id, 910_100, 5_550_100, "acme", "repo", "main", "private", false, false, 31,
            graphs[1], "personal", user_id,
        )
        .await
        .expect("a second destination is a second routed row");
        assert_ne!(first, other_ocean);

        let rows: i64 = sqlx::query_scalar("SELECT count(*) FROM github_watched_repos WHERE repo_id = 5550100")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(rows, 2);
        let size: i64 = sqlx::query_scalar("SELECT size_kb FROM github_watched_repos WHERE id = $1")
            .bind(first)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(size, 31, "the conflict arm refreshed the metadata");

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    #[tokio::test]
    async fn unknown_installation_writes_nothing_pg() {
        let _guard = env_lock().lock().unwrap();
        let Some(base) = base_url() else {
            eprintln!("skipping unknown_installation_writes_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_webhook_unknown_install";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let secret = "test-secret-unknown";
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", secret) };
        let body = serde_json::json!({
            "action": "opened",
            "installation": {"id": 999_999_999_i64},
        })
        .to_string()
        .into_bytes();
        let sig = sign(secret, &body);
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "push")
            .header("x-github-delivery", "delivery-1")
            .header("x-hub-signature-256", sig)
            .body(Body::from(body))
            .unwrap();
        let resp = webhook_router(state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };

        let installs: i64 = sqlx::query_scalar("SELECT count(*) FROM github_installations")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(installs, 0, "nothing may be written for an unknown installation");

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    /// Conformance test 2, suspended-installation half: a KNOWN but
    /// suspended installation also refuses (202), writing nothing further —
    /// proven here by asserting `github_installations`' own row is
    /// unchanged (still suspended, `updated_at` untouched by this delivery).
    /// A personal-org-shaped account — `clerk_org_id` set, NO
    /// `tenant_databases` row — lives on the consumer pool by design
    /// (`personal_orgs`, control 0208), and the selection page's request
    /// path already writes its rows there. The webhook's pool resolver used
    /// to answer `None` for that shape, so every install-event pause was a
    /// silent no-op for such an account (observed 2026-09-22). Drives the
    /// real route with a signed `installation_repositories.added` and
    /// asserts the pending row lands, on the consumer pool, unrouted.
    #[tokio::test]
    async fn repos_added_for_an_org_without_a_tenant_db_creates_pending_rows_on_the_consumer_pool_pg() {
        let _guard = env_lock().lock().unwrap();
        let Some(base) = base_url() else {
            eprintln!("skipping repos_added_for_an_org_without_a_tenant_db_...: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_webhook_personal_org_pending";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = Uuid::new_v4();
        let owner_user_id = Uuid::new_v4();
        let installation_id: i64 = 424_343;
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner_user_id)
            .bind("personal-org-owner@example.com")
            .execute(&pool)
            .await
            .unwrap();
        // The org-shaped account with no tenant_databases row.
        sqlx::query("INSERT INTO accounts (id, owner_user_id, clerk_org_id) VALUES ($1, $2, 'org_personal_shape_424343')")
            .bind(account_id)
            .bind(owner_user_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO github_installations (account_id, installation_id, account_login, account_type) \
             VALUES ($1, $2, 'someone', 'User')",
        )
        .bind(account_id)
        .bind(installation_id)
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO connector_installs (id, provider, external_workspace_id, account_id, owner_user_id) \
             VALUES ($1, 'github', $2, $3, $4)",
        )
        .bind(Uuid::new_v4())
        .bind(installation_id.to_string())
        .bind(account_id)
        .bind(owner_user_id)
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        let secret = "test-secret-personal-org";
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", secret) };
        let body = serde_json::json!({
            "action": "added",
            "installation": {"id": installation_id, "account": {"login": "someone", "type": "User"}},
            "repository_selection": "selected",
            "repositories_added": [
                {"id": 77_001_i64, "name": "later-repo", "full_name": "someone/later-repo", "private": true}
            ],
            "repositories_removed": [],
        })
        .to_string()
        .into_bytes();
        let sig = sign(secret, &body);
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "installation_repositories")
            .header("x-github-delivery", "delivery-personal-org-added")
            .header("x-hub-signature-256", sig)
            .body(Body::from(body))
            .unwrap();
        let resp = webhook_router(state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };

        let row: Option<(bool, Option<String>, Option<Uuid>)> = sqlx::query_as(
            "SELECT active, paused_reason, graph_id FROM github_watched_repos WHERE account_id = $1 AND repo_id = 77001",
        )
        .bind(account_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        let (active, reason, graph_id) = row.expect("the granted-later repo must land as a pending row on the consumer pool");
        assert!(!active, "granted later is paused, never auto-backfilled (§3.3)");
        assert_eq!(reason.as_deref(), Some("pending_selection"));
        assert!(graph_id.is_none(), "pending rows have no destination yet");

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }

    #[tokio::test]
    async fn suspended_installation_writes_nothing_pg() {
        let _guard = env_lock().lock().unwrap();
        let Some(base) = base_url() else {
            eprintln!("skipping suspended_installation_writes_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_gh_webhook_suspended_install";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let account_id = Uuid::new_v4();
        let owner_user_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email) VALUES ($1, $2)")
            .bind(owner_user_id)
            .bind("owner@example.com")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO accounts (id, owner_user_id) VALUES ($1, $2)")
            .bind(account_id)
            .bind(owner_user_id)
            .execute(&pool)
            .await
            .unwrap();
        let installation_id: i64 = 424_242;
        sqlx::query(
            "INSERT INTO github_installations (account_id, installation_id, account_login, account_type, suspended_at) \
             VALUES ($1, $2, 'acme', 'Organization', now())",
        )
        .bind(account_id)
        .bind(installation_id)
        .execute(&pool)
        .await
        .unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let secret = "test-secret-suspended";
        unsafe { std::env::set_var("GITHUB_WEBHOOK_SECRET", secret) };
        let body = serde_json::json!({
            "action": "opened",
            "installation": {"id": installation_id},
        })
        .to_string()
        .into_bytes();
        let sig = sign(secret, &body);
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/github/events")
            .header("x-github-event", "pull_request")
            .header("x-github-delivery", "delivery-2")
            .header("x-hub-signature-256", sig)
            .body(Body::from(body))
            .unwrap();
        let resp = webhook_router(state).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        unsafe { std::env::remove_var("GITHUB_WEBHOOK_SECRET") };

        let still_suspended: bool = sqlx::query_scalar(
            "SELECT suspended_at IS NOT NULL FROM github_installations WHERE installation_id = $1",
        )
        .bind(installation_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(still_suspended, "a suspended installation must remain untouched, never silently unsuspended");

        pool.close().await;
        drop_db(&mut admin_conn, db).await;
    }
}
