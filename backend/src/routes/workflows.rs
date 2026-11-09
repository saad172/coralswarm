//! Outlook ingest workflows (CS-118 / ADR-0008 D6).
//!
//! A workflow is a per-user rule: *when a new message matches this filter,
//! auto-watch its conversation and ingest the whole thread* — the same effect as
//! a manual "ingest this thread" opt-in, triggered by a rule instead of a click.
//!
//! * Live matching runs in the change-notification webhook
//!   (`routes::graph_webhook`): on a notification for a not-yet-watched
//!   conversation, the message is matched against the account's enabled rules.
//! * Creating a rule also ensures the inbox/sent subscriptions exist (so the
//!   webhook fires) and kicks off a **bounded backfill** that scans recent mail
//!   and ingests existing matches.
//!
//! Match semantics: **AND across populated criteria, OR within each** — e.g.
//! `from_domains:[acme.com] + subject_contains:[invoice,po]` means "from acme.com
//! AND subject contains invoice or po". An empty filter matches nothing (and is
//! rejected at creation) so a rule can never silently ingest the whole mailbox.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    connectors::{microsoft, ConnectorError, MicrosoftConnector, TokenOwner},
    middleware::auth::UserContext,
    routes::{
        connectors::load_creds_bg, graph_webhook::ingest_message_stored, oauth::store_token_bundle,
        slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    },
    tenancy::extractor::Tenant,
    AppState,
};

/// Fail-closed gate for the Outlook-workflow routes under an org
/// `TenancyDecision` (org-tenancy PR 1.6) — same policy + rationale as
/// `routes::connectors::reject_org_connector_access`: workflows are rules
/// over a PERSONAL Microsoft connection ingesting into a PERSONAL primary
/// ocean, and the Graph change-notification webhook that fires them resolves
/// state on the CONSUMER plane. Refuse cleanly under Org instead of writing
/// a rule the webhook path could never match. Personal / flag-off requests
/// are byte-identical to before.
fn reject_org_workflow_access(tenant: &Tenant) -> Option<Response> {
    tenant.org_context().map(|ctx| {
        tracing::info!(
            org_id = %ctx.org_id,
            "workflow route refused under org context (personal-workspace-only in v1)"
        );
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "ingest workflows are available in your personal workspace only; \
                          switch out of the organization workspace to manage them"
            })),
        )
            .into_response()
    })
}

// ── Filter + matcher ──────────────────────────────────────────────────────────

/// A workflow's match criteria. Every populated field must match (AND); within a
/// field any entry matches (OR). All string comparisons are case-insensitive.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkflowFilter {
    /// Exact From addresses (e.g. `jane@acme.com`).
    #[serde(default)]
    pub from_addresses: Vec<String>,
    /// From domains (e.g. `acme.com`; a leading `@` is tolerated).
    #[serde(default)]
    pub from_domains: Vec<String>,
    /// Substrings to look for in the subject.
    #[serde(default)]
    pub subject_contains: Vec<String>,
    /// Exact To/Cc addresses.
    #[serde(default)]
    pub recipient_addresses: Vec<String>,
    /// To/Cc domains.
    #[serde(default)]
    pub recipient_domains: Vec<String>,
    /// Outlook category labels (exact, case-insensitive).
    #[serde(default)]
    pub categories: Vec<String>,
    /// Well-known folders the message must be in (e.g. `inbox`, `sentitems`).
    #[serde(default)]
    pub folders: Vec<String>,
    /// If set, the message's attachment state must equal this.
    #[serde(default)]
    pub has_attachments: Option<bool>,
    /// Display label per entry of `folders`, keyed by the raw folder value.
    /// Server-computed (see `folder_labels_for`) — any value sent by a client is
    /// ignored and overwritten on the next read. Well-known keys always resolve;
    /// a raw Graph folder id from an older workflow resolves to nothing (no
    /// Graph round-trip on this hot list path) and the frontend falls back to
    /// its own presentation. Additive/nullable: absent on older backends.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub folder_labels: HashMap<String, String>,
}

impl WorkflowFilter {
    /// True when no criteria are set — such a filter is rejected at creation so a
    /// rule can never match (and ingest) everything.
    pub fn is_empty(&self) -> bool {
        self.from_addresses.is_empty()
            && self.from_domains.is_empty()
            && self.subject_contains.is_empty()
            && self.recipient_addresses.is_empty()
            && self.recipient_domains.is_empty()
            && self.categories.is_empty()
            && self.folders.is_empty()
            && self.has_attachments.is_none()
    }

    /// Whether the rule constrains on folder (so backfill knows to resolve names).
    pub fn uses_folders(&self) -> bool {
        !self.folders.is_empty()
    }
}

/// Display labels for a filter's folder values — well-known keys only (see
/// [`microsoft::well_known_folder_label`]); a raw Graph folder id is simply
/// omitted so the frontend's own fallback kicks in. No Graph round-trip: this
/// feeds the workflows list/create response, a hot path.
fn folder_labels_for(filter: &WorkflowFilter) -> HashMap<String, String> {
    filter
        .folders
        .iter()
        .filter_map(|f| microsoft::well_known_folder_label(f).map(|label| (f.clone(), label.to_string())))
        .collect()
}

/// True if `addr` belongs to `domain` (case-insensitive; `domain` may include a
/// leading `@`).
fn addr_in_domain(addr: &str, domain: &str) -> bool {
    let domain = domain.trim_start_matches('@');
    addr.rsplit('@')
        .next()
        .is_some_and(|d| d.eq_ignore_ascii_case(domain))
}

fn any_eq_ignore_case(haystack: &[String], needle: &str) -> bool {
    haystack.iter().any(|h| h.eq_ignore_ascii_case(needle))
}

/// Evaluate a filter against a message's extracted fields. Pure — unit-tested
/// without any Graph types.
pub(crate) fn matches(f: &WorkflowFilter, m: &microsoft::MatchInput) -> bool {
    if f.is_empty() {
        return false;
    }

    // From: address OR domain.
    if !f.from_addresses.is_empty() || !f.from_domains.is_empty() {
        let from = match m.from.as_deref() {
            Some(a) => a,
            None => return false,
        };
        let ok = any_eq_ignore_case(&f.from_addresses, from)
            || f.from_domains.iter().any(|d| addr_in_domain(from, d));
        if !ok {
            return false;
        }
    }

    // Recipients (To/Cc): any recipient matching an address OR domain.
    if !f.recipient_addresses.is_empty() || !f.recipient_domains.is_empty() {
        let ok = m.recipients.iter().any(|r| {
            any_eq_ignore_case(&f.recipient_addresses, r)
                || f.recipient_domains.iter().any(|d| addr_in_domain(r, d))
        });
        if !ok {
            return false;
        }
    }

    // Subject contains any keyword.
    if !f.subject_contains.is_empty() {
        let subject = m.subject.to_lowercase();
        let ok = f
            .subject_contains
            .iter()
            .any(|k| subject.contains(&k.to_lowercase()));
        if !ok {
            return false;
        }
    }

    // Category: any of.
    if !f.categories.is_empty() {
        let ok = f
            .categories
            .iter()
            .any(|c| any_eq_ignore_case(&m.categories, c));
        if !ok {
            return false;
        }
    }

    // Folder: any of (the message's well-known folder).
    if !f.folders.is_empty() {
        let ok = m
            .folder
            .as_deref()
            .is_some_and(|fld| any_eq_ignore_case(&f.folders, fld));
        if !ok {
            return false;
        }
    }

    // Attachment state.
    if let Some(want) = f.has_attachments {
        if m.has_attachments != want {
            return false;
        }
    }

    true
}

// ── Storage ───────────────────────────────────────────────────────────────────

/// An enabled rule as the webhook needs it.
pub(crate) struct ActiveWorkflow {
    pub user_id: Uuid,
    pub graph_id: Uuid,
    pub filter: WorkflowFilter,
}

/// All enabled rules for an account (evaluated on each notification).
pub(crate) async fn enabled_for_account(
    pool: &sqlx::PgPool,
    account_id: Uuid,
) -> Result<Vec<ActiveWorkflow>, ConnectorError> {
    // `outlook_workflows` carries an `account_id` RLS policy (migration 0083)
    // — read inside a `begin_account_scoped` transaction (org-tenancy
    // conversion; inert pre-OPS-4). Covers the Graph webhook's rule
    // evaluation, which calls through here with its resolved account.
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("enabled_for_account scoped tx: {e}")))?;
    let rows: Vec<(Uuid, Uuid, sqlx::types::Json<WorkflowFilter>)> = sqlx::query_as(
        "SELECT user_id, graph_id, filter FROM outlook_workflows WHERE account_id = $1 AND enabled",
    )
    .bind(account_id)
    .fetch_all(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("enabled_for_account: {e}")))?;
    tx.rollback().await.ok();
    Ok(rows
        .into_iter()
        .map(|(user_id, graph_id, filter)| ActiveWorkflow {
            user_id,
            graph_id,
            filter: filter.0,
        })
        .collect())
}

#[derive(sqlx::FromRow)]
struct WorkflowRow {
    id: Uuid,
    name: String,
    enabled: bool,
    filter: sqlx::types::Json<WorkflowFilter>,
    created_at: DateTime<Utc>,
}

#[derive(Serialize)]
struct WorkflowDto {
    id: Uuid,
    name: String,
    enabled: bool,
    filter: WorkflowFilter,
    created_at: DateTime<Utc>,
}

impl From<WorkflowRow> for WorkflowDto {
    fn from(r: WorkflowRow) -> Self {
        let mut filter = r.filter.0;
        // Server-computed on every read so it can't drift from the stored
        // `folders` list (and so it's populated even for rows persisted before
        // this field existed).
        filter.folder_labels = folder_labels_for(&filter);
        WorkflowDto {
            id: r.id,
            name: r.name,
            enabled: r.enabled,
            filter,
            created_at: r.created_at,
        }
    }
}

// ── Handlers ──────────────────────────────────────────────────────────────────

#[derive(Deserialize)]
pub struct CreateWorkflowRequest {
    pub name: String,
    pub filter: WorkflowFilter,
}

/// `POST /v1/connectors/microsoft/workflows` — create a rule, ensure the
/// subscriptions feeding it exist, and backfill recent matches.
pub async fn create_workflow(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<CreateWorkflowRequest>,
) -> Response {
    if let Some(reject) = reject_org_workflow_access(&tenant) {
        return reject;
    }
    // Past the org gate this is always Personal / flag-off, where
    // `tenant.pool` IS the legacy consumer pool (org-tenancy PR 1.6; was
    // ambient `state.db`).
    let pool = match tenant.pool.clone() {
        Some(p) => p,
        None => return service_unavailable(),
    };
    let name = body.name.trim();
    if name.is_empty() {
        return bad_request("name must not be empty");
    }
    if body.filter.is_empty() {
        return bad_request("filter must set at least one criterion");
    }

    // Attributable (member-private-oceans PR 5, plan §3): `user_ctx` is
    // verified here. `reject_org_workflow_access` above already 403s any org
    // `TenancyDecision` before this line, so today this always resolves on
    // the Personal path — threading the real caller identity is
    // forward-looking plumbing for whenever that gate is lifted. Plane is
    // hardcoded `Personal` for the same reason (org-tenancy plane split,
    // step 0, `AccountPlane`'s doc comment).
    let graph_id = match resolve_primary_graph(
        &pool,
        user_ctx.account_id,
        AccountPlane::Personal,
        IngestCaller::Attributable(user_ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "workflow: failed to resolve primary graph");
            return internal_error();
        }
    };

    // `folder_labels` is server-computed on every read (see `folder_labels_for`)
    // — strip whatever a client sent so stale/bogus labels never land in storage.
    let mut store_filter = body.filter.clone();
    store_filter.folder_labels.clear();

    // One `begin_account_scoped` transaction for the insert (org-tenancy RLS
    // conversion; `outlook_workflows` carries an `account_id` policy).
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id)
        .await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "workflow: failed to open scoped tx");
            return internal_error();
        }
    };
    let row: Result<WorkflowRow, _> = sqlx::query_as(
        r#"
        INSERT INTO outlook_workflows (account_id, user_id, graph_id, name, filter, backfill_complete)
        VALUES ($1, $2, $3, $4, $5, FALSE)
        RETURNING id, name, enabled, filter, created_at
        "#,
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .bind(graph_id)
    .bind(name)
    .bind(sqlx::types::Json(&store_filter))
    .fetch_one(&mut *tx)
    .await;
    let row = match row {
        Ok(r) => tx.commit().await.map(|_| r),
        Err(e) => Err(e),
    };

    let dto: WorkflowDto = match row {
        Ok(r) => r.into(),
        Err(e) => {
            tracing::error!(error = %e, "workflow: insert failed");
            return internal_error();
        }
    };

    // Ensure subscriptions exist + backfill, off the request path. The spawn
    // owns `pool` — the `Tenant`-resolved handle cloned above — rather than
    // re-deriving an ambient pool from `state` inside the task (org-tenancy
    // PR 1.6: every spawn in a migrated route owns a tenant-scoped handle).
    let filter = body.filter.clone();
    let wf_name = name.to_string();
    let account_id = user_ctx.account_id;
    let user_id = user_ctx.user_id;
    let workflow_id = dto.id;
    let bg = state.clone();
    let bg_pool = pool.clone();
    // The spawn owns its DEK plane too, resolved from THIS request's binding
    // decision rather than pinned to `consumer` inside the task — same rule
    // as `pool` above, and the reason lifting `reject_org_workflow_access`
    // cannot silently start reading an org account's key off control.
    let bg_dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), &pool);
    tokio::spawn(async move {
        activate_workflow(bg, bg_pool, bg_dek_plane, account_id, user_id, graph_id, workflow_id, wf_name, filter)
            .await;
    });

    (StatusCode::CREATED, Json(dto)).into_response()
}

/// `GET /v1/connectors/microsoft/workflows` — list the caller's rules.
pub async fn list_workflows(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_workflow_access(&tenant) {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    let mut tx = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id)
        .await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "workflow: failed to open scoped tx");
            return internal_error();
        }
    };
    let rows: Result<Vec<WorkflowRow>, _> = sqlx::query_as(
        r#"
        SELECT id, name, enabled, filter, created_at
        FROM outlook_workflows
        WHERE account_id = $1 AND user_id = $2
        ORDER BY created_at DESC
        "#,
    )
    .bind(user_ctx.account_id)
    .bind(user_ctx.user_id)
    .fetch_all(&mut *tx)
    .await;
    tx.rollback().await.ok();

    match rows {
        Ok(rows) => {
            let dtos: Vec<WorkflowDto> = rows.into_iter().map(Into::into).collect();
            (StatusCode::OK, Json(json!({ "workflows": dtos }))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "workflow: list failed");
            internal_error()
        }
    }
}

#[derive(Deserialize)]
pub struct UpdateWorkflowRequest {
    pub enabled: bool,
}

/// `PATCH /v1/connectors/microsoft/workflows/{id}` — enable/disable a rule.
pub async fn update_workflow(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<UpdateWorkflowRequest>,
) -> Response {
    if let Some(reject) = reject_org_workflow_access(&tenant) {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    let res = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id).await
    {
        Ok(mut tx) => {
            let r = sqlx::query(
                r#"
                UPDATE outlook_workflows SET enabled = $1, updated_at = now()
                WHERE id = $2 AND account_id = $3 AND user_id = $4
                "#,
            )
            .bind(body.enabled)
            .bind(id)
            .bind(user_ctx.account_id)
            .bind(user_ctx.user_id)
            .execute(&mut *tx)
            .await;
            match r {
                Ok(r) => tx.commit().await.map(|_| r),
                Err(e) => Err(e),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "workflow: failed to open scoped tx");
            return internal_error();
        }
    };
    match res {
        Ok(r) if r.rows_affected() == 0 => not_found(),
        Ok(_) => (StatusCode::OK, Json(json!({ "ok": true }))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "workflow: update failed");
            internal_error()
        }
    }
}

/// `DELETE /v1/connectors/microsoft/workflows/{id}` — delete a rule. Already-
/// ingested corals and watched conversations are left intact.
pub async fn delete_workflow(
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Some(reject) = reject_org_workflow_access(&tenant) {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    let res = match crate::tenancy::scope::begin_account_scoped(&pool, user_ctx.account_id).await
    {
        Ok(mut tx) => {
            let r = sqlx::query(
                "DELETE FROM outlook_workflows WHERE id = $1 AND account_id = $2 AND user_id = $3",
            )
            .bind(id)
            .bind(user_ctx.account_id)
            .bind(user_ctx.user_id)
            .execute(&mut *tx)
            .await;
            match r {
                Ok(r) => tx.commit().await.map(|_| r),
                Err(e) => Err(e),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "workflow: failed to open scoped tx");
            return internal_error();
        }
    };
    match res {
        Ok(r) if r.rows_affected() == 0 => not_found(),
        Ok(_) => (StatusCode::OK, Json(json!({ "ok": true }))).into_response(),
        Err(e) => {
            tracing::error!(error = %e, "workflow: delete failed");
            internal_error()
        }
    }
}

// ── Activation: ensure subscriptions + backfill ────────────────────────────────

/// Ensure the mail subscriptions exist (so live matching fires) and backfill
/// existing matches for a newly-created rule. Best-effort.
///
/// `pool` is the OWNED handle the spawning handler resolved through its
/// `Tenant` extractor (org-tenancy PR 1.6; this used to re-derive the ambient
/// `state.db` inside the task) — `state` remains only for the process-wide
/// `http_client`.
#[allow(clippy::too_many_arguments)]
async fn activate_workflow(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    dek_plane: crate::crypto::DekPlane,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    workflow_id: Uuid,
    name: String,
    filter: WorkflowFilter,
) {
    // Every Graph call this task makes (subscription ensure + the candidate
    // scan + per-conversation fetches) runs under one rate-limit scope keyed
    // on the account — m365 Train 2 PR 2.6. See `graph_rate_limit`'s module
    // doc for why the key travels via task-local rather than a parameter on
    // every fetcher.
    crate::graph_rate_limit::scoped(account_id.to_string(), async move {
        activate_workflow_inner(state, pool, dek_plane, account_id, user_id, graph_id, workflow_id, name, filter)
            .await;
    })
    .await
}

#[allow(clippy::too_many_arguments)]
async fn activate_workflow_inner(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    dek_plane: crate::crypto::DekPlane,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    workflow_id: Uuid,
    name: String,
    filter: WorkflowFilter,
) {
    let mut creds =
        match load_creds_bg(&pool, &dek_plane, account_id, "microsoft", TokenOwner::AccountWide).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                tracing::info!(%account_id, "workflow created but Microsoft not connected — rule will activate on connect");
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "workflow activation: load creds failed");
                return;
            }
        };
    let connector = MicrosoftConnector;
    if let Err(e) = connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
    {
        tracing::warn!(error = %e, %account_id, "workflow activation: token refresh failed");
        return;
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ = store_token_bundle(
            &pool,
            &dek_plane,
            account_id,
            "microsoft",
            TokenOwner::AccountWide,
            refreshed,
        )
        .await;
    }

    // `TokenOwner::AccountWide` (m365 enterprise train PR 3.3 added the
    // parameter): workflows stay Personal/AccountWide-only, matching every
    // other credential call in this function — org-plane workflow activation
    // is not part of this PR's scope.
    if let Err(e) = microsoft::ensure_mail_subscriptions(
        &state.http_client,
        &pool,
        account_id,
        &creds.access_token,
        TokenOwner::AccountWide,
    )
    .await
    {
        tracing::warn!(error = %e, %account_id, "workflow activation: ensure subscriptions failed");
    }

    backfill(
        &state,
        &pool,
        account_id,
        user_id,
        graph_id,
        workflow_id,
        &name,
        &filter,
        &creds.access_token,
        None,
    )
    .await;
}

/// Re-run any workflow's candidate-scan backfill that a previous pass left
/// incomplete (`outlook_workflows.backfill_complete = FALSE` — see
/// [`BackfillPage`](microsoft::BackfillPage)'s doc and this module's `backfill`
/// below). Driven by `workers::graph_subscriptions::tick_backfill_continue`.
/// Retries the FULL scan from scratch rather than resuming a Graph-native
/// cursor: a `mailSearchFolder`'s `@odata.nextLink` is scoped to that
/// (ephemeral, deleted-after-use) folder id, so there is no cursor to resume
/// across calls — but re-running is safe because both `upsert_watched_
/// conversation` and `microsoft::fetch_conversation` are themselves
/// idempotent/resumable, so a retry just re-discovers already-watched threads
/// as no-ops. Returns how many pending workflows were retried.
/// `pool` is the tenant-loop tick's CONTENT pool (an org tenant's own
/// database under `TenantId::Org`); `control_pool` and `dek` are separate
/// because neither the `oauth_tokens` ROW (control-plane-only, for every
/// account) nor the account's DEK (the org's tenant `account_keys`) lives on
/// the content pool. Reading credentials off `pool` was a latent hard error
/// on the org arm — `oauth_tokens` does not exist on a tenant database at
/// all.
pub(crate) async fn continue_workflow_backfills(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    control_pool: &sqlx::PgPool,
    dek: &crate::crypto::DekPlaneSource,
    account_id: Uuid,
) -> usize {
    let pending = match incomplete_backfills_for_account(pool, account_id).await {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "workflow backfill continuation: list failed");
            return 0;
        }
    };
    if pending.is_empty() {
        return 0;
    }
    let dek_plane = dek.plane_for(account_id);
    let mut creds = match load_creds_bg(
        control_pool,
        &dek_plane,
        account_id,
        "microsoft",
        TokenOwner::AccountWide,
    )
    .await
    {
        Ok(Some(c)) => c,
        Ok(None) => return 0,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "workflow backfill continuation: load creds failed");
            return 0;
        }
    };
    let connector = MicrosoftConnector;
    if let Err(e) = connector.refresh_if_needed(&state.http_client, &mut creds).await {
        tracing::warn!(error = %e, %account_id, "workflow backfill continuation: token refresh failed");
        return 0;
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ = store_token_bundle(
            control_pool,
            &dek_plane,
            account_id,
            "microsoft",
            TokenOwner::AccountWide,
            refreshed,
        )
        .await;
    }

    let n = pending.len();
    for wf in pending {
        backfill(
            state,
            pool,
            account_id,
            wf.user_id,
            wf.graph_id,
            wf.id,
            &wf.name,
            &wf.filter,
            &creds.access_token,
            None,
        )
        .await;
    }
    n
}

/// A workflow whose candidate-scan backfill did not finish on its last pass
/// — the continuation tick's work set.
pub(crate) struct PendingBackfillWorkflow {
    pub id: Uuid,
    pub user_id: Uuid,
    pub graph_id: Uuid,
    pub name: String,
    pub filter: WorkflowFilter,
}

pub(crate) async fn incomplete_backfills_for_account(
    pool: &sqlx::PgPool,
    account_id: Uuid,
) -> Result<Vec<PendingBackfillWorkflow>, ConnectorError> {
    // `begin_tenant_system_scoped`, NOT the plain `begin_account_scoped` this
    // file's OTHER `outlook_workflows` readers use (m365 Train 2 PR 2.6): this
    // function's only caller is `workers::graph_subscriptions::
    // continue_backfills_for_account`, a tenant-loop tick — `pool` can
    // genuinely be an org tenant's own pool there, unlike every pre-existing
    // call site in this file (all confirmed post-`reject_org_*`-gate/
    // consumer-only, per this file's `tenant_rls_coverage.manifest` exempt
    // entry). `outlook_workflows` carries the same `migrations_tenant/0009`
    // `member_ocean_isolation` policy as `teams_watched_resources`/`outlook_
    // watched_conversations` (all three are schema-parity tables); no acting
    // end user is available on this background-tick path, so the system-actor
    // bypass is correct, mirroring `connectors::microsoft::scoped_tx`.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("incomplete_backfills_for_account scoped tx: {e}")))?;
    // Deterministic order + a per-tick cap (CodeRabbit review, PR #673): the
    // query had neither, so one account with many incomplete workflows made
    // the tick that discovered it unbounded — each row triggers a full
    // candidate scan (search-folder create/poll/list/delete, then one
    // `fetch_conversation` per discovered thread). The remainder is picked up
    // on the next sweep, which is already the contract for incomplete work
    // (same as every other `NOT backfill_complete` work-set query in this
    // module/`connectors::microsoft`).
    let rows: Result<Vec<(Uuid, Uuid, Uuid, String, sqlx::types::Json<WorkflowFilter>)>, _> =
        sqlx::query_as(
            "SELECT id, user_id, graph_id, name, filter FROM outlook_workflows \
             WHERE account_id = $1 AND NOT backfill_complete \
             ORDER BY created_at, id \
             LIMIT 20",
        )
        .bind(account_id)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("incomplete_backfills_for_account: {e}")));
    tx.rollback().await.ok();
    rows.map(|rows| {
        rows.into_iter()
            .map(|(id, user_id, graph_id, name, filter)| PendingBackfillWorkflow {
                id,
                user_id,
                graph_id,
                name,
                filter: filter.0,
            })
            .collect()
    })
}

async fn set_workflow_backfill_complete(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    workflow_id: Uuid,
    complete: bool,
) -> Result<(), ConnectorError> {
    // `begin_tenant_system_scoped` — see `incomplete_backfills_for_account`'s
    // identical comment; this function is called from the same tenant-loop
    // continuation path (`workers::graph_subscriptions`) as well as from
    // `backfill`, which is itself reachable from that same tick (via
    // `continue_workflow_backfills`), not only from the request-scoped
    // `activate_workflow` path.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_workflow_backfill_complete scoped tx: {e}")))?;
    sqlx::query("UPDATE outlook_workflows SET backfill_complete = $1 WHERE id = $2 AND account_id = $3")
        .bind(complete)
        .bind(workflow_id)
        .bind(account_id)
        .execute(&mut *tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_workflow_backfill_complete: {e}")))?;
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("set_workflow_backfill_complete commit: {e}")))?;
    Ok(())
}

// ── Backfill ──────────────────────────────────────────────────────────────────

/// Escape a single quote inside an OData string literal by doubling it.
fn odata_lit(s: &str) -> String {
    s.replace('\'', "''")
}

/// Build an OData `filterQuery` for a `mailSearchFolder` from the criteria that
/// are safely expressible (from address/domain, subject, has-attachments). This
/// is intentionally a *superset* narrower — recipients, categories and folder are
/// left to the in-process matcher, which remains the source of truth. `None` when
/// no criterion is expressible (caller falls back to a recent-mail scan).
pub(crate) fn build_filter_query(f: &WorkflowFilter) -> Option<String> {
    let mut groups: Vec<String> = Vec::new();

    let mut from_terms: Vec<String> = Vec::new();
    for a in &f.from_addresses {
        from_terms.push(format!("from/emailAddress/address eq '{}'", odata_lit(a)));
    }
    for d in &f.from_domains {
        let dom = d.trim_start_matches('@');
        // `contains '@domain'` is a safe superset of an exact domain match.
        from_terms.push(format!(
            "contains(from/emailAddress/address, '@{}')",
            odata_lit(dom)
        ));
    }
    if !from_terms.is_empty() {
        groups.push(format!("({})", from_terms.join(" or ")));
    }

    if !f.subject_contains.is_empty() {
        let subj: Vec<String> = f
            .subject_contains
            .iter()
            .map(|k| format!("contains(subject, '{}')", odata_lit(k)))
            .collect();
        groups.push(format!("({})", subj.join(" or ")));
    }

    match f.has_attachments {
        Some(true) => groups.push("hasAttachments eq true".into()),
        Some(false) => groups.push("hasAttachments eq false".into()),
        None => {}
    }

    if groups.is_empty() {
        None
    } else {
        Some(groups.join(" and "))
    }
}

/// Resolve the folders a search folder should cover: the filter's named folders,
/// or the whole mailbox root when none are specified.
async fn source_folder_ids(
    http: &reqwest::Client,
    access_token: &str,
    filter: &WorkflowFilter,
) -> Vec<String> {
    let mut ids = Vec::new();
    for name in &filter.folders {
        if let Ok(Some(id)) = microsoft::resolve_folder_id(http, access_token, name).await {
            ids.push(id);
        }
    }
    if ids.is_empty() {
        if let Ok(Some(root)) =
            microsoft::resolve_folder_id(http, access_token, "msgfolderroot").await
        {
            ids.push(root);
        }
    }
    ids
}

/// Gather backfill candidates via a native `mailSearchFolder`. Returns `None`
/// (→ caller falls back to a recent-mail scan) when the filter can't be
/// expressed or the folder can't be created. The temporary folder is always
/// deleted — search folders are cheap to recreate and Exchange re-evaluates
/// the same `filterQuery` identically each time, which is what makes a
/// from-scratch retry (rather than a resumed Graph cursor into a deleted
/// folder) a safe continuation strategy; see `continue_workflow_backfills`'s
/// doc. The `bool` in the `Some` case is [`BackfillPage::complete`] —
/// `false` means [`microsoft::BACKFILL_SCAN_CAP`]'s per-call budget was hit.
async fn search_folder_candidates(
    state: &Arc<AppState>,
    access_token: &str,
    display_name: &str,
    filter: &WorkflowFilter,
) -> Option<(Vec<microsoft::GraphMessage>, bool)> {
    let query = build_filter_query(filter)?;
    let sources = source_folder_ids(&state.http_client, access_token, filter).await;
    if sources.is_empty() {
        return None;
    }
    let folder_id = microsoft::create_search_folder(
        &state.http_client,
        access_token,
        display_name,
        &query,
        &sources,
    )
    .await
    .map_err(|e| tracing::warn!(error = %e, "workflow backfill: search folder create failed"))
    .ok()?;

    microsoft::await_folder_population(&state.http_client, access_token, &folder_id).await;
    let listed =
        microsoft::list_messages_in_folder(&state.http_client, access_token, &folder_id, None).await;

    if let Err(e) =
        microsoft::delete_mail_folder(&state.http_client, access_token, &folder_id).await
    {
        tracing::debug!(error = %e, "workflow backfill: search-folder cleanup failed");
    }

    match listed {
        Ok(page) => Some((page.items, page.complete)),
        Err(e) => {
            tracing::warn!(error = %e, "workflow backfill: search folder listing failed");
            None
        }
    }
}

/// Backfill existing matches for a rule and ingest their threads. Primary path
/// is a native `mailSearchFolder` (Exchange evaluates the query across the whole
/// mailbox); falls back to a recent-mail scan when the filter isn't expressible.
/// The in-process matcher always makes the final decision (exact semantics).
///
/// `_resume_cursor` is currently unused — the candidate SCAN itself always
/// restarts from scratch on a retry (see `search_folder_candidates`'s doc for
/// why a Graph-native cursor isn't viable there); the per-conversation drill-
/// down (`microsoft::fetch_conversation`) still resumes correctly via each
/// conversation's own `outlook_watched_conversations.backfill_cursor`,
/// independent of this parameter. Kept as an explicit parameter (rather than
/// silently dropped) so a future search-folder-keep-alive optimization has an
/// obvious place to plug in without another signature change.
#[allow(clippy::too_many_arguments)]
async fn backfill(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    workflow_id: Uuid,
    name: &str,
    filter: &WorkflowFilter,
    access_token: &str,
    _resume_cursor: Option<&str>,
) {
    let folder_map = if filter.uses_folders() {
        microsoft::resolve_well_known_folders(&state.http_client, access_token).await
    } else {
        HashMap::new()
    };

    let display_name: String = format!("CoralSwarm · {}", name.chars().take(60).collect::<String>());
    let (candidates, source, scan_complete) =
        match search_folder_candidates(state, access_token, &display_name, filter).await {
            Some((c, complete)) => (c, "search-folder", complete),
            None => match microsoft::list_recent_messages(&state.http_client, access_token, None).await
            {
                Ok(page) => (page.items, "recent-scan", page.complete),
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "workflow backfill: candidate fetch failed");
                    return;
                }
            },
        };

    let mut seen_conversations: HashSet<String> = HashSet::new();
    let mut threads = 0usize;
    let mut all_conversations_complete = true;
    for msg in &candidates {
        let folder = msg
            .parent_folder_id()
            .and_then(|id| folder_map.get(id).cloned());
        if !matches(filter, &msg.match_input(folder)) {
            continue;
        }
        let Some(cid) = msg.conversation_id() else {
            continue;
        };
        if !seen_conversations.insert(cid.to_string()) {
            continue;
        }
        // Existing manual preferences outrank automatic rule expansion:
        // an unfollowed or message-bounded membership must not acquire its
        // historical thread through this workflow backfill path.
        match microsoft::watched_status(pool, account_id, cid).await {
            Ok(Some(false)) => continue,
            Ok(Some(true)) => {
                match microsoft::watched_conversation_graph(pool, account_id, cid).await {
                    Ok(Some((_, _, Some(_)))) => continue,
                    Ok(_) => {},
                    Err(e) => {
                        tracing::warn!(error = %e, %cid, "workflow backfill: watch boundary lookup failed");
                        all_conversations_complete = false;
                        continue;
                    }
                }
            }
            Ok(None) => {},
            Err(e) => {
                tracing::warn!(error = %e, %cid, "workflow backfill: watch status lookup failed");
                all_conversations_complete = false;
                continue;
            }
        }
        // `true` (gmail-options backend train, PR 1 of 4): a workflow-rule
        // auto-watch, not the UI's opt-in follow toggle — always follow, the
        // byte-identical behavior that existed before that PR's
        // `follow_replies` column.
        if let Err(e) = microsoft::upsert_watched_conversation(
            pool,
            account_id,
            user_id,
            graph_id,
            cid,
            true,
        )
        .await
        {
            tracing::warn!(error = %e, "workflow backfill: watch upsert failed");
            continue;
        }
        match microsoft::fetch_conversation(&state.http_client, access_token, cid, None).await {
            Ok(page) => {
                let complete = page.complete;
                let page_len = page.items.len();
                let mut ingested = 0usize;
                for m in page.items {
                    let item = microsoft::message_to_item(m);
                    if ingest_message_stored(state, account_id, user_id, graph_id, item).await {
                        ingested += 1;
                    }
                }
                if ingested != page_len {
                    // A message in this page failed to ingest (transient DB
                    // or embedding error, swallowed inside `ingest_message`).
                    // Do NOT advance the cursor past it — persisting `page.
                    // next_cursor`/`complete` here would make the loss
                    // permanent, since neither the continuation tick nor the
                    // backstop resync would ever revisit this page. Leave the
                    // stored cursor untouched (safe: `ingest_message`'s own
                    // dedup makes a re-ingested message a no-op) and keep the
                    // workflow pending so this conversation is retried.
                    tracing::warn!(
                        %cid, ingested, page_len,
                        "workflow backfill: partial ingest failure this page — cursor not \
                         advanced, page will be retried"
                    );
                    all_conversations_complete = false;
                } else {
                    if let Err(e) = microsoft::set_conversation_backfill_state(
                        pool,
                        account_id,
                        cid,
                        page.next_cursor.as_deref(),
                        complete,
                    )
                    .await
                    {
                        tracing::warn!(error = %e, %cid, "workflow backfill: cursor persist failed");
                    }
                    if !complete {
                        all_conversations_complete = false;
                    }
                }
                threads += 1;
            }
            Err(e) => {
                // A fetch FAILURE is strictly worse than a capped page — this
                // workflow must stay pending so the continuation tick retries
                // it, exactly as the `!complete`/partial-ingest arms above do.
                all_conversations_complete = false;
                tracing::warn!(error = %e, %cid, "workflow backfill: conversation fetch failed");
            }
        }
    }

    // `scan_complete` (did the CANDIDATE scan itself hit BACKFILL_SCAN_CAP)
    // and `all_conversations_complete` (did every discovered conversation's
    // OWN backfill finish) are independent signals — either one being false
    // means this workflow's backfill is not fully done yet, and
    // `tick_backfill_continue` should retry it. A per-conversation
    // incompleteness self-heals via that conversation's own
    // `outlook_watched_conversations` row regardless (the backstop resync and
    // the continuation tick both drive it forward), but leaving THIS
    // workflow row marked complete while a conversation it just discovered is
    // still mid-backfill would stop this workflow from ever being retried —
    // so it stays incomplete until both are true.
    let complete = scan_complete && all_conversations_complete;
    if let Err(e) = set_workflow_backfill_complete(pool, account_id, workflow_id, complete).await {
        tracing::warn!(error = %e, %account_id, %workflow_id, "workflow backfill: complete-flag persist failed");
    }
    tracing::info!(
        %account_id,
        source,
        scanned = candidates.len(),
        threads,
        complete,
        "workflow backfill pass complete"
    );
}

// ── Small response helpers ────────────────────────────────────────────────────

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}
fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}
fn internal_error() -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "internal error" }))).into_response()
}
fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({ "error": "service unavailable" }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connectors::microsoft::MatchInput;

    fn input(from: &str, subject: &str) -> MatchInput {
        MatchInput {
            from: Some(from.into()),
            recipients: vec![],
            subject: subject.into(),
            categories: vec![],
            has_attachments: false,
            folder: None,
        }
    }

    #[test]
    fn empty_filter_never_matches() {
        assert!(!matches(&WorkflowFilter::default(), &input("a@b.com", "hi")));
    }

    #[test]
    fn from_domain_and_subject_are_anded() {
        let f = WorkflowFilter {
            from_domains: vec!["acme.com".into()],
            subject_contains: vec!["invoice".into()],
            ..Default::default()
        };
        // both match
        assert!(matches(&f, &input("jane@acme.com", "Q3 Invoice ready")));
        // domain matches, subject doesn't
        assert!(!matches(&f, &input("jane@acme.com", "lunch?")));
        // subject matches, domain doesn't
        assert!(!matches(&f, &input("jane@other.com", "the invoice")));
    }

    #[test]
    fn from_address_or_domain_within_field() {
        let f = WorkflowFilter {
            from_addresses: vec!["ceo@corp.com".into()],
            from_domains: vec!["acme.com".into()],
            ..Default::default()
        };
        assert!(matches(&f, &input("ceo@corp.com", "x")));
        assert!(matches(&f, &input("anyone@acme.com", "x")));
        assert!(!matches(&f, &input("bob@nope.com", "x")));
    }

    #[test]
    fn matching_is_case_insensitive() {
        let f = WorkflowFilter {
            from_domains: vec!["Acme.COM".into()],
            subject_contains: vec!["INVOICE".into()],
            ..Default::default()
        };
        assert!(matches(&f, &input("Jane@acme.com", "your invoice")));
    }

    #[test]
    fn filter_query_builds_from_and_subject_groups() {
        let f = WorkflowFilter {
            from_domains: vec!["acme.com".into()],
            from_addresses: vec!["ceo@corp.com".into()],
            subject_contains: vec!["invoice".into()],
            has_attachments: Some(true),
            ..Default::default()
        };
        let q = build_filter_query(&f).expect("expressible");
        // AND across groups, OR within; from group has both eq and contains.
        assert!(q.contains("from/emailAddress/address eq 'ceo@corp.com'"));
        assert!(q.contains("contains(from/emailAddress/address, '@acme.com')"));
        assert!(q.contains("contains(subject, 'invoice')"));
        assert!(q.contains("hasAttachments eq true"));
        assert!(q.contains(" and "));
        assert!(q.contains(" or "));
    }

    #[test]
    fn filter_query_none_when_only_recipient_or_category() {
        // Recipients/categories/folders aren't put in the query (matcher handles
        // them) — so a recipient-only rule has no expressible query → scan fallback.
        let f = WorkflowFilter {
            recipient_domains: vec!["deals.com".into()],
            categories: vec!["Legal".into()],
            folders: vec!["inbox".into()],
            ..Default::default()
        };
        assert!(build_filter_query(&f).is_none());
    }

    #[test]
    fn filter_query_escapes_quotes() {
        let f = WorkflowFilter {
            subject_contains: vec!["o'brien".into()],
            ..Default::default()
        };
        let q = build_filter_query(&f).unwrap();
        assert!(q.contains("contains(subject, 'o''brien')"));
    }

    #[test]
    fn recipient_domain_matches_to_or_cc() {
        let f = WorkflowFilter {
            recipient_domains: vec!["deals.com".into()],
            ..Default::default()
        };
        let m = MatchInput {
            from: Some("a@b.com".into()),
            recipients: vec!["x@other.com".into(), "team@deals.com".into()],
            subject: "hi".into(),
            categories: vec![],
            has_attachments: false,
            folder: None,
        };
        assert!(matches(&f, &m));
    }

    #[test]
    fn category_folder_and_attachment_criteria() {
        let f = WorkflowFilter {
            categories: vec!["Deals".into()],
            folders: vec!["inbox".into()],
            has_attachments: Some(true),
            ..Default::default()
        };
        let yes = MatchInput {
            from: None,
            recipients: vec![],
            subject: "x".into(),
            categories: vec!["deals".into()],
            has_attachments: true,
            folder: Some("inbox".into()),
        };
        assert!(matches(&f, &yes));

        let wrong_folder = MatchInput {
            folder: Some("sentitems".into()),
            ..yes_clone(&yes)
        };
        assert!(!matches(&f, &wrong_folder));

        let no_attachment = MatchInput {
            has_attachments: false,
            ..yes_clone(&yes)
        };
        assert!(!matches(&f, &no_attachment));
    }

    // Tiny clone helper (MatchInput isn't Clone by design — it's a throwaway).
    fn yes_clone(m: &MatchInput) -> MatchInput {
        MatchInput {
            from: m.from.clone(),
            recipients: m.recipients.clone(),
            subject: m.subject.clone(),
            categories: m.categories.clone(),
            has_attachments: m.has_attachments,
            folder: m.folder.clone(),
        }
    }

    #[test]
    fn folder_labels_resolve_well_known_keys_and_omit_raw_ids() {
        let f = WorkflowFilter {
            folders: vec![
                "inbox".into(),
                "SENTITEMS".into(), // case-insensitive
                "AAMkAGI2raw-graph-folder-id==".into(),
            ],
            ..Default::default()
        };
        let labels = folder_labels_for(&f);
        assert_eq!(labels.get("inbox").map(String::as_str), Some("Inbox"));
        assert_eq!(
            labels.get("SENTITEMS").map(String::as_str),
            Some("Sent"),
            "lookup key matches the original (unlowercased) folder value"
        );
        assert!(
            !labels.contains_key("AAMkAGI2raw-graph-folder-id=="),
            "a raw Graph folder id has no static label — no Graph round-trip here"
        );
    }
}
