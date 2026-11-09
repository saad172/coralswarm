//! The generic connector catalog (connectors redesign, layer B1; see
//! `docs/design/connectors-redesign/PLAN.md`).
//!
//! * `GET /v1/connectors`: every connector's [`ConnectorSummary`], in display
//!   order, for the caller. A fresh member sees ten, all `not_connected`.
//! * `GET /v1/connectors/{provider}`: the same summary plus `account_label`,
//!   `connected_at` and `topics`.
//!
//! Connect and disconnect (layer B2):
//!
//! * `POST /v1/connectors/{provider}/connect` with `{return_to}`: where to send
//!   the browser (`authorize_url`), until when, and a `pending_id` for hosted
//!   sign-ins.
//! * `POST /v1/connectors/{provider}/connect/complete` with `{pending_id}`:
//!   finish a hosted sign-in; answers the connector's summary.
//! * `DELETE /v1/connectors/{provider}`: revoke the sign-in and stop capture
//!   (204). Captured atoms stay.
//!
//! Scope and pause (layer B3):
//!
//! * `GET /v1/connectors/{provider}/sources/{source}/scope-items?q=&cursor=&limit=`:
//!   the items a source lets the member pick, searched and paged, each with
//!   whether it is picked.
//! * `PUT /v1/connectors/{provider}/sources/{source}/scope` with
//!   `{mode: "pick" | "everything", item_ids?}`: store the choice; 422 when the
//!   source's shape does not allow the mode (a meetings source has no
//!   `everything`, a workspace-wide source no `pick`).
//! * `PATCH /v1/connectors/{provider}/sources/{source}` with `{enabled?, paused?}`:
//!   turn one source on or off, pause or resume it. A paused or off source
//!   captures nothing at any door (`connectors::catalog::pause`).
//!
//! Import history (layer B4):
//!
//! * `POST /v1/connectors/{provider}/sources/{source}/backfill` with `{days}`
//!   (`0 | 7 | 30 | 90`, `0` records "None" and runs nothing): 202 with the
//!   source's summary. See `connectors::catalog::history` for the rules.
//!
//!
//! Auth and tenancy are the neighbouring connector routes' exactly: the
//! authenticated [`UserContext`] names the member (never a parameter), `oauth_*`
//! style control-plane rows are read through `state.control_db()` in one
//! account-scoped transaction, and the content-plane pool comes from [`Tenant`].
//! Payloads carry problem codes and params only, never sentences, and never name
//! the broker behind the Beta toolkits.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;

use crate::connectors::catalog::history::{self, HistoryError};
use crate::connectors::catalog::summary::{self, ConnectorDetail, ConnectorSummary, SourceSummary};
use crate::connectors::catalog::{
    connect, registry, scope, AdapterCx, AdapterError, ConnectorAdapter, Facts, ReturnTo,
    ScopeItemsQuery, ScopeMode, ScopeSelection, SourceSpec,
};
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectorList {
    pub connectors: Vec<ConnectorSummary>,
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}

fn not_found() -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "unknown connector"})),
    )
        .into_response()
}

fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "connector catalog: failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
        .into_response()
}

async fn load_facts(
    state: &AppState,
    tenant: &Tenant,
    user: &UserContext,
) -> Result<Facts, Response> {
    if state.control_db().is_none() {
        return Err(unavailable());
    }
    Facts::load(state, tenant, user.account_id, user.user_id)
        .await
        .map_err(|e| internal("load facts", e))
}

#[utoipa_path(
    get,
    path = "/v1/connectors",
    tag = "connectors",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Every connector, in display order, with the caller's status, sources and problem", body = ConnectorList),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list(
    state: State<Arc<AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::connectors::ConnectorsList>(
        state,
        tenant,
        user,
        original,
        Path(std::collections::BTreeMap::new()),
        query,
    )
    .await
    .into_response()
}

#[utoipa_path(
    get,
    path = "/v1/connectors/{provider}",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(("provider" = String, Path, description = "Connector key, e.g. `slack`, `microsoft`, `jira`")),
    responses(
        (status = 200, description = "One connector with its account label, connect time and topics", body = ConnectorDetail),
        (status = 404, description = "No such connector"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn detail(
    state: State<Arc<AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    path: Path<std::collections::BTreeMap<String, String>>,
    query: Query<std::collections::BTreeMap<String, String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::connectors::ConnectorDetail>(
        state, tenant, user, original, path, query,
    )
    .await
    .into_response()
}

// ---------------------------------------------------------------------------
// Connect, complete, disconnect (B2)
// ---------------------------------------------------------------------------

/// The wire value of [`ReturnTo`].
#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReturnToWire {
    Wizard,
    Onboarding,
}

impl From<ReturnToWire> for ReturnTo {
    fn from(w: ReturnToWire) -> Self {
        match w {
            ReturnToWire::Wizard => Self::Wizard,
            ReturnToWire::Onboarding => Self::Onboarding,
        }
    }
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ConnectRequest {
    /// Where the sign-in tab hands back to afterwards.
    pub return_to: ReturnToWire,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ConnectResponse {
    /// Where to send the browser next.
    pub authorize_url: String,
    /// When the sign-in can no longer finish and the member has to start again.
    #[schema(value_type = String, format = DateTime)]
    pub expires_at: DateTime<Utc>,
    /// Set for a hosted sign-in: pass it to `connect/complete` once the member
    /// is back. Absent for a browser-redirect sign-in, which finishes in its
    /// callback and lands on `/connectors/return`.
    pub pending_id: Option<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct CompleteRequest {
    pub pending_id: String,
}

/// An adapter's failure as the route's answer. The body carries a stable code
/// (`error` and `code` are the same value) and never a sentence.
fn adapter_error(what: &str, e: AdapterError) -> Response {
    adapter_error_ref(what, &e)
}

fn adapter_error_ref(what: &str, e: &AdapterError) -> Response {
    let (status, code): (u16, &str) = match e {
        AdapterError::NotImplemented(_) => (501, "not_implemented"),
        AdapterError::Database(_) => return internal(what, e),
        AdapterError::NotConfigured => (503, "not_configured"),
        AdapterError::SignInPending => (409, "sign_in_pending"),
        AdapterError::SignInExpired => (410, "sign_in_expired"),
        AdapterError::NotConnected => (404, "not_connected"),
        AdapterError::Refused { status, code } => (*status, code),
    };
    tracing::info!(step = what, status, code, "connector catalog: refused");
    (
        StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_GATEWAY),
        Json(json!({"error": code, "code": code})),
    )
        .into_response()
}

/// These are human, browser-session actions, like the older connect and
/// disconnect routes (`middleware::auth::authority_allowed`).
fn session_only() -> Option<Response> {
    crate::middleware::auth::interactive_session_refusal().map(IntoResponse::into_response)
}

#[utoipa_path(
    post,
    path = "/v1/connectors/{provider}/connect",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(("provider" = String, Path, description = "Connector key, e.g. `slack`, `microsoft`, `jira`")),
    request_body = ConnectRequest,
    responses(
        (status = 200, description = "Where to send the browser to sign in", body = ConnectResponse),
        (status = 403, description = "The workspace does not allow this connect (code `forbidden`)"),
        (status = 404, description = "No such connector"),
        (status = 503, description = "Not configured on this deployment (code `not_configured`) or database unavailable"),
    )
)]
pub async fn connect_start(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(provider): Path<String>,
    Json(body): Json<ConnectRequest>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let facts = match load_facts(&state, &tenant, &user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    match adapter.connect(&cx, body.return_to.into()).await {
        Ok(start) => (
            StatusCode::OK,
            Json(ConnectResponse {
                authorize_url: start.authorize_url,
                expires_at: start.expires_at,
                pending_id: start.pending_id,
            }),
        )
            .into_response(),
        Err(e) => adapter_error("connect", e),
    }
}

#[utoipa_path(
    post,
    path = "/v1/connectors/{provider}/connect/complete",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(("provider" = String, Path, description = "Connector key of a hosted sign-in, e.g. `jira`")),
    request_body = CompleteRequest,
    responses(
        (status = 200, description = "The connector after the sign-in finished", body = ConnectorSummary),
        (status = 400, description = "Not a hosted sign-in (code `nothing_to_complete`) or a malformed pending_id"),
        (status = 404, description = "No such connector, or no such pending sign-in for the caller"),
        (status = 409, description = "The member has not finished signing in (code `sign_in_pending`)"),
        (status = 410, description = "The sign-in expired (code `sign_in_expired`)"),
    )
)]
pub async fn connect_complete(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(provider): Path<String>,
    Json(body): Json<CompleteRequest>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let facts = match load_facts(&state, &tenant, &user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    if let Err(e) = adapter.complete_connect(&cx, &body.pending_id).await {
        return adapter_error("complete connect", e);
    }
    // A finished connect leaves the connector set up to be chosen from.
    if let Err(e) = connect::record_connect(&state, user.account_id, user.user_id, adapter).await {
        return internal("record connect", e);
    }
    // The summary is read after, from fresh facts: it must show what just changed.
    summary_response(&state, &tenant, &user, adapter).await
}

async fn summary_response(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    adapter: &dyn ConnectorAdapter,
) -> Response {
    if let Err(e) =
        crate::read_models::dirty::connector_changed(state, tenant, user.account_id, user.user_id)
            .await
    {
        return internal("invalidate connector models", e);
    }
    let facts = match load_facts(state, tenant, user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, state, tenant, user);
    match summary::build(adapter, &cx).await {
        Ok((summary, _)) => (StatusCode::OK, Json(summary)).into_response(),
        Err(e) => internal("build summary", e),
    }
}

#[utoipa_path(
    delete,
    path = "/v1/connectors/{provider}",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(("provider" = String, Path, description = "Connector key, e.g. `slack`, `microsoft`, `jira`")),
    responses(
        (status = 204, description = "The sign-in is revoked and capture has stopped; captured atoms stay"),
        (status = 403, description = "Only an organization admin may disconnect this (code `forbidden`)"),
        (status = 404, description = "No such connector, or the caller has no connection to it"),
        (status = 502, description = "Capture stopped but the provider refused the revocation; safe to retry"),
    )
)]
pub async fn disconnect(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(provider): Path<String>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let facts = match load_facts(&state, &tenant, &user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    if let Err(e) = adapter.disconnect(&cx).await {
        return adapter_error("disconnect", e);
    }
    // The sign-in is gone; so is this member's setup state for it, so a later
    // connect starts the wizard afresh. Atoms are not touched.
    if let Err(e) =
        connect::clear_sources(&state, user.account_id, user.user_id, adapter.key()).await
    {
        return internal("clear sources", e);
    }
    if let Err(e) =
        crate::read_models::dirty::connector_changed(&state, &tenant, user.account_id, user.user_id)
            .await
    {
        return internal("invalidate connector models", e);
    }
    StatusCode::NO_CONTENT.into_response()
}

// ---------------------------------------------------------------------------
// Scope and pause (B3)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, utoipa::IntoParams)]
pub struct ScopeItemsParams {
    /// Search text, matched against the item's label.
    q: Option<String>,
    /// The `next_cursor` of the previous page.
    cursor: Option<String>,
    /// Page size, 1 to 200; 50 when absent.
    limit: Option<u32>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ScopeItemView {
    pub id: String,
    pub label: String,
    /// A terse machine-readable detail (a member count, an ISO date); the web
    /// words it. `null` when the provider has none.
    pub meta: Option<String>,
    pub selected: bool,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ScopeItemsResponse {
    pub items: Vec<ScopeItemView>,
    /// Pass back as `cursor` for the next page; `null` on the last.
    pub next_cursor: Option<String>,
    /// How many items match the search, or `null` when the provider cannot say.
    pub total: Option<u32>,
}

#[derive(Debug, Clone, Copy, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ScopeModeWire {
    Pick,
    Everything,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct ScopeRequest {
    pub mode: ScopeModeWire,
    /// The picked items, for `pick`. Ignored for `everything`.
    #[serde(default)]
    pub item_ids: Vec<String>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SourcePatch {
    pub enabled: Option<bool>,
    pub paused: Option<bool>,
}

/// The adapter and the source's spec, from the registry alone: unknown names are
/// a 404 whatever the database is doing, and nothing about the caller is read.
fn lookup_source(
    provider: &str,
    source: &str,
) -> Result<(&'static dyn ConnectorAdapter, &'static SourceSpec), Box<Response>> {
    let Some(adapter) = registry::lookup(provider) else {
        return Err(Box::new(not_found()));
    };
    match adapter.sources().iter().find(|s| s.key == source) {
        Some(spec) => Ok((adapter, spec)),
        None => Err(Box::new(
            (
                StatusCode::NOT_FOUND,
                Json(json!({"error": "unknown_source", "code": "unknown_source"})),
            )
                .into_response(),
        )),
    }
}

/// [`lookup_source`] plus proof that the caller has signed in, and what was read
/// to prove it: the one thing every scope and pause route needs before it
/// touches anything of the caller's.
async fn resolve_source(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    provider: &str,
    source: &str,
) -> Result<(&'static dyn ConnectorAdapter, &'static SourceSpec, Facts), Response> {
    let (adapter, spec) = lookup_source(provider, source).map_err(|r| *r)?;
    let facts = load_facts(state, tenant, user).await?;
    let cx = AdapterCx::new(&facts, state, tenant, user);
    match adapter.grant(&cx).await {
        Ok(Some(_)) => Ok((adapter, spec, facts)),
        Ok(None) => Err(adapter_error("source", AdapterError::NotConnected)),
        Err(e) => Err(internal("grant", e)),
    }
}

/// One source's summary, read afresh so it shows what just changed.
async fn source_response(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user: &UserContext,
    adapter: &dyn ConnectorAdapter,
    source: &str,
) -> Response {
    if let Err(e) =
        crate::read_models::dirty::connector_changed(state, tenant, user.account_id, user.user_id)
            .await
    {
        return internal("invalidate connector models", e);
    }
    let facts = match load_facts(state, tenant, user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, state, tenant, user);
    match summary::build(adapter, &cx).await {
        Ok((summary, _)) => match summary
            .sources
            .into_iter()
            .find(|s: &SourceSummary| s.key == source)
        {
            Some(one) => (StatusCode::OK, Json(one)).into_response(),
            None => not_found(),
        },
        Err(e) => internal("summary", e),
    }
}

// Import history (B4)
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize, ToSchema)]
pub struct BackfillRequest {
    /// How far back to import: `0` (None: record the choice, run nothing), `7`,
    /// `30` or `90`. A source may offer less than 90 (its `history.max_days`).
    pub days: u32,
}

/// A refused history request as the route's answer. The body carries stable codes
/// and never a sentence; `history_unsupported` adds the source's reason code.
fn history_error(e: HistoryError) -> Response {
    let (status, code, extra): (StatusCode, &str, serde_json::Value) = match &e {
        HistoryError::Unsupported(reason) => (
            StatusCode::CONFLICT,
            "history_unsupported",
            json!({"reason": reason}),
        ),
        HistoryError::InvalidDays => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_days",
            json!({"allowed": history::ALLOWED_DAYS}),
        ),
        HistoryError::ExceedsMax { max_days } => (
            StatusCode::UNPROCESSABLE_ENTITY,
            "exceeds_max_days",
            json!({"max_days": max_days}),
        ),
        HistoryError::SourceOff => (StatusCode::CONFLICT, "source_off", json!({})),
        HistoryError::SourcePaused => (StatusCode::CONFLICT, "source_paused", json!({})),
        HistoryError::Running => (StatusCode::CONFLICT, "backfill_running", json!({})),
        // `adapter_error_ref` owns the mapping and the logging for these.
        HistoryError::Adapter(e) => return adapter_error_ref("backfill", e),
    };
    tracing::info!(step = "backfill", code, "connector catalog: refused");
    let mut body = json!({"error": code, "code": code});
    if let (Some(body), Some(extra)) = (body.as_object_mut(), extra.as_object()) {
        body.extend(extra.clone());
    }
    (status, Json(body)).into_response()
}

#[utoipa_path(
    post,
    path = "/v1/connectors/{provider}/sources/{source}/backfill",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(
        ("provider" = String, Path, description = "Connector key, e.g. `slack`, `github`"),
        ("source" = String, Path, description = "Source key, e.g. `channels`, `repos`"),
    ),
    request_body = BackfillRequest,
    responses(
        (status = 202, description = "The choice is recorded and, for a window, the import has started; the source reads `importing` until it ends", body = SourceSummary),
        (status = 404, description = "No such connector or source, or the caller has no connection (code `not_connected`)"),
        (status = 409, description = "The source cannot backfill (code `history_unsupported`, with `reason`), is off (`source_off`) or paused (`source_paused`), the connection needs reconnecting (`reconnect_needed`), or an import is already running (`backfill_running`)"),
        (status = 422, description = "`days` is not 0, 7, 30 or 90 (code `invalid_days`) or is more than the source offers (code `exceeds_max_days`, with `max_days`)"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn backfill(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((provider, source)): Path<(String, String)>,
    Json(body): Json<BackfillRequest>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let Some(spec) = adapter.sources().iter().find(|s| s.key == source) else {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown source"})),
        )
            .into_response();
    };
    if state.control_db().is_none() {
        return unavailable();
    }
    if let Err(e) = history::request(&state, &tenant, &user, adapter, spec, body.days).await {
        return history_error(e);
    }
    if let Err(e) =
        crate::read_models::dirty::connector_changed(&state, &tenant, user.account_id, user.user_id)
            .await
    {
        return internal("invalidate connector models", e);
    }
    // Read after, from fresh facts: the summary must show the run it started.
    let facts = match load_facts(&state, &tenant, &user).await {
        Ok(f) => f,
        Err(r) => return r,
    };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    match summary::build(adapter, &cx).await {
        Ok((summary, _)) => match summary.sources.into_iter().find(|s| s.key == spec.key) {
            Some(source) => (StatusCode::ACCEPTED, Json(source)).into_response(),
            None => internal("find source", "source missing from its own connector"),
        },
        Err(e) => internal("build summary", e),
    }
}

#[utoipa_path(
    get,
    path = "/v1/connectors/{provider}/sources/{source}/scope-items",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(
        ("provider" = String, Path, description = "Connector key, e.g. `slack`, `microsoft`"),
        ("source" = String, Path, description = "Source key, e.g. `channels`, `teams_chats`"),
        ScopeItemsParams,
    ),
    responses(
        (status = 200, description = "The items the member can pick for this source, searched and paged", body = ScopeItemsResponse),
        (status = 404, description = "No such connector or source, or the caller has not signed in"),
        (status = 409, description = "The sign-in cannot be used to list right now (code `sign_in_unusable`)"),
        (status = 422, description = "The source has no items to pick (code `no_scope_items`)"),
        (status = 502, description = "The provider refused the listing (code `provider_error`)"),
    )
)]
pub async fn scope_items(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((provider, source)): Path<(String, String)>,
    Query(params): Query<ScopeItemsParams>,
) -> Response {
    let (adapter, spec, facts) =
        match resolve_source(&state, &tenant, &user, &provider, &source).await {
            Ok(r) => r,
            Err(resp) => return resp,
        };
    if spec.scope.noun().is_none() {
        return adapter_error(
            "scope items",
            AdapterError::Refused {
                status: 422,
                code: "no_scope_items",
            },
        );
    }
    let query = ScopeItemsQuery {
        q: params.q,
        cursor: params.cursor,
        limit: scope::clamp_limit(params.limit),
    };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    match adapter.scope_items(&cx, spec, &query).await {
        Ok(page) => (
            StatusCode::OK,
            Json(ScopeItemsResponse {
                items: page
                    .items
                    .into_iter()
                    .map(|i| ScopeItemView {
                        id: i.id,
                        label: i.label,
                        meta: i.meta,
                        selected: i.selected,
                    })
                    .collect(),
                next_cursor: page.next_cursor,
                total: page.total,
            }),
        )
            .into_response(),
        Err(e) => adapter_error("scope items", e),
    }
}

#[utoipa_path(
    put,
    path = "/v1/connectors/{provider}/sources/{source}/scope",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(
        ("provider" = String, Path, description = "Connector key"),
        ("source" = String, Path, description = "Source key"),
    ),
    request_body = ScopeRequest,
    responses(
        (status = 200, description = "The source after the choice was stored", body = SourceSummary),
        (status = 404, description = "No such connector or source, or the caller has not signed in"),
        (status = 422, description = "The source's shape does not allow this mode (codes `everything_not_allowed`, `pick_not_allowed`, `too_many_items`)"),
    )
)]
pub async fn put_scope(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((provider, source)): Path<(String, String)>,
    Json(body): Json<ScopeRequest>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    // The mode is checked against the source's shape before anything of the
    // caller's is read: it depends on the source alone.
    let (_, spec) = match lookup_source(&provider, &source) {
        Ok(found) => found,
        Err(resp) => return *resp,
    };
    let selection = ScopeSelection {
        mode: match body.mode {
            ScopeModeWire::Pick => ScopeMode::Pick,
            ScopeModeWire::Everything => ScopeMode::Everything,
        },
        item_ids: body.item_ids,
    };
    if let Err(e) = scope::validate(spec.scope, &selection) {
        return adapter_error("scope", e);
    }
    let (adapter, spec, facts) =
        match resolve_source(&state, &tenant, &user, &provider, &source).await {
            Ok(r) => r,
            Err(resp) => return resp,
        };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    if let Err(e) = adapter.set_scope(&cx, spec, &selection).await {
        return adapter_error("scope", e);
    }
    source_response(&state, &tenant, &user, adapter, spec.key).await
}

#[utoipa_path(
    patch,
    path = "/v1/connectors/{provider}/sources/{source}",
    tag = "connectors",
    security(("bearer_token" = [])),
    params(
        ("provider" = String, Path, description = "Connector key"),
        ("source" = String, Path, description = "Source key"),
    ),
    request_body = SourcePatch,
    responses(
        (status = 200, description = "The source after the change. A paused or off source captures nothing", body = SourceSummary),
        (status = 400, description = "Neither `enabled` nor `paused` was given (code `nothing_to_change`)"),
        (status = 404, description = "No such connector or source, or the caller has not signed in"),
    )
)]
pub async fn patch_source(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path((provider, source)): Path<(String, String)>,
    Json(body): Json<SourcePatch>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    if body.enabled.is_none() && body.paused.is_none() {
        return adapter_error(
            "patch source",
            AdapterError::Refused {
                status: 400,
                code: "nothing_to_change",
            },
        );
    }
    let (adapter, spec, facts) =
        match resolve_source(&state, &tenant, &user, &provider, &source).await {
            Ok(r) => r,
            Err(resp) => return resp,
        };
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    // On before paused, so one request can turn a source on and pause it.
    if let Some(on) = body.enabled {
        if let Err(e) = adapter.set_enabled(&cx, spec, on).await {
            return adapter_error("set enabled", e);
        }
    }
    if let Some(paused) = body.paused {
        if let Err(e) = adapter.set_paused(&cx, spec, paused).await {
            return adapter_error("set paused", e);
        }
    }
    source_response(&state, &tenant, &user, adapter, spec.key).await
}
#[utoipa_path(get, path = "/v1/connectors/{provider}/captures", tag = "connectors", security(("bearer_token" = [])), params(("provider" = String, Path), ("source" = Option<String>, Query), ("cursor" = Option<String>, Query), ("limit" = Option<u32>, Query)), responses((status = 200, body = crate::connectors::catalog::activity::CapturePage, description = "Caller captures, newest first")))]
pub async fn captures(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(provider): Path<String>,
    axum::extract::Query(query): axum::extract::Query<
        crate::connectors::catalog::activity::CaptureQuery,
    >,
) -> Response {
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let facts = Facts::default();
    let cx = AdapterCx::new(&facts, &state, &tenant, &user);
    match crate::connectors::catalog::activity::captures(&cx, adapter, query).await {
        Ok(page) => (
            [(axum::http::header::CACHE_CONTROL, "no-store")],
            Json(page),
        )
            .into_response(),
        Err(AdapterError::Refused { status, code }) => (
            StatusCode::from_u16(status).unwrap_or(StatusCode::BAD_REQUEST),
            Json(json!({"error":code})),
        )
            .into_response(),
        Err(e) => internal("capture feed", e),
    }
}

/// Retry only the explicitly named failure; this never repairs provider consent.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct RecoveryRequest {
    pub problem: String,
}

#[utoipa_path(post, path = "/v1/connectors/{provider}/recover", tag = "connectors",
    security(("bearer_token" = [])),
    params(("provider" = String, Path, description = "Connector key")),
    request_body = RecoveryRequest,
    responses((status = 200, description = "Fresh summary; inspect problem to determine recovery", body = ConnectorSummary),
        (status = 409, description = "recovery_unsupported, recovery_not_applicable or recovery_in_progress"),
        (status = 404, description = "Unknown connector or not_connected")))]
pub async fn recover(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(provider): Path<String>,
    Json(body): Json<RecoveryRequest>,
) -> Response {
    if let Some(refusal) = session_only() {
        return refusal;
    }
    let Some(adapter) = registry::lookup(&provider) else {
        return not_found();
    };
    let Some(recipe) = crate::connectors::composio::recipe::get(&provider)
        .filter(|_| body.problem == "capture_unavailable")
    else {
        return adapter_error(
            "recover",
            AdapterError::Refused {
                status: 409,
                code: "recovery_unsupported",
            },
        );
    };
    match crate::routes::composio::recover_catalog(&state, &tenant, &user, recipe).await {
        Ok(()) => summary_response(&state, &tenant, &user, adapter).await,
        Err(response) => response,
    }
}

#[cfg(test)]
mod recovery_tests {
    use super::*;
    use crate::billing::test_support::{body_json, user_context};
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;
    use uuid::Uuid;

    #[tokio::test]
    async fn recovery_http_contract_and_authority() {
        let app = axum::Router::new()
            .nest("/v1", crate::v1_routes())
            .layer(Extension(user_context(Uuid::new_v4(), Uuid::new_v4())))
            .with_state(crate::test_app_state());
        for (provider, problem, interactive, expected) in [
            ("github", "sync_failed", true, StatusCode::CONFLICT),
            ("jira", "access_revoked", true, StatusCode::CONFLICT),
            (
                "unknown",
                "capture_unavailable",
                true,
                StatusCode::NOT_FOUND,
            ),
            ("jira", "capture_unavailable", false, StatusCode::FORBIDDEN),
        ] {
            let request = Request::builder()
                .method("POST")
                .uri(format!("/v1/connectors/{provider}/recover"))
                .header("content-type", "application/json")
                .body(Body::from(json!({"problem":problem}).to_string()))
                .unwrap();
            let response = crate::middleware::auth::with_request_authority_for_test(
                interactive,
                app.clone().oneshot(request),
            )
            .await
            .unwrap();
            assert_eq!(response.status(), expected);
            let body = body_json(response).await;
            if expected == StatusCode::CONFLICT {
                assert_eq!(body["code"], "recovery_unsupported");
            }
            if !interactive {
                assert_eq!(body["code"], "interactive_session_required");
            }
        }
    }

    #[tokio::test]
    async fn recovery_owner_resolution_and_concurrency_pg() {
        let Ok(url) = std::env::var("RECOVERY_ISOLATED_TEST_DATABASE_URL") else {
            eprintln!("SKIP: isolated recovery database absent");
            return;
        };
        let pool = sqlx::PgPool::connect(&url).await.unwrap();
        // This test owns a disposable database, never the shared migration fixture.
        sqlx::query("CREATE TABLE composio_connections(id uuid PRIMARY KEY, account_id uuid, owner_user_id uuid, service text, status text, self_check text, provider_namespace text, created_at timestamptz DEFAULT now())")
            .execute(&pool).await.unwrap();
        let account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let id = Uuid::new_v4();
        sqlx::query("INSERT INTO composio_connections(id,account_id,owner_user_id,service,status,self_check,provider_namespace) VALUES($1,$2,$3,'jira','active','passed','workspace')")
            .bind(id).bind(account).bind(member).execute(&pool).await.unwrap();
        let state = crate::test_app_state_owned_with_db(pool.clone(), None);
        let tenant = Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        };
        let recipe = crate::connectors::composio::recipe::get("jira").unwrap();
        for user in [
            user_context(account, Uuid::new_v4()),
            user_context(Uuid::new_v4(), member),
        ] {
            let response = crate::routes::composio::recover_catalog(&state, &tenant, &user, recipe)
                .await
                .unwrap_err();
            assert_eq!(response.status(), StatusCode::NOT_FOUND);
        }
        let user = user_context(account, member);
        for _ in 0..2 {
            assert!(
                crate::routes::composio::recover_catalog(&state, &tenant, &user, recipe)
                    .await
                    .is_ok()
            );
        }
        let mut lock = pool.begin().await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1,0))")
            .bind(format!("catalog-recovery:{account}:{member}:jira"))
            .execute(&mut *lock)
            .await
            .unwrap();
        let response = crate::routes::composio::recover_catalog(&state, &tenant, &user, recipe)
            .await
            .unwrap_err();
        assert_eq!(body_json(response).await["code"], "recovery_in_progress");
        lock.rollback().await.unwrap();
        sqlx::query("UPDATE composio_connections SET status='reconnect_needed',self_check='failed' WHERE id=$1")
            .bind(id).execute(&pool).await.unwrap();
        let response = crate::routes::composio::recover_catalog(&state, &tenant, &user, recipe)
            .await
            .unwrap_err();
        assert_eq!(body_json(response).await["code"], "recovery_not_applicable");
        sqlx::query("DROP TABLE composio_connections")
            .execute(&pool)
            .await
            .unwrap();
    }
}
