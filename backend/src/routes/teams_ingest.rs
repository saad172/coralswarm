//! Microsoft Teams ingest (CS-119) — list + per-resource opt-in + backfill.
//!
//! Delegated, per-user, opt-in (mirrors Slack/Outlook): the user picks a channel
//! or chat to ingest; we record it in `teams_watched_resources` and backfill its
//! recent messages into their ocean. Live capture (Graph change notifications)
//! is a follow-up phase and reuses `graph_subscriptions` + `/v1/webhooks/graph`.
//!
//! Routes (all bearer-authed, account+user scoped — this file is a
//! Clerk-JWT CONTENT connector, NOT a webhook; the signature-authenticated
//! Teams webhook is `routes::teams_bot`):
//! * `GET    /v1/connectors/microsoft/teams`                       — joined teams
//! * `GET    /v1/connectors/microsoft/teams/{team_id}/channels`    — channels
//! * `GET    /v1/connectors/microsoft/teams/chats`                 — chats
//! * `POST   /v1/connectors/microsoft/teams/ingest`                — opt in + backfill
//! * `GET    /v1/connectors/microsoft/teams/watched`               — opted-in list
//! * `DELETE /v1/connectors/microsoft/teams/watched/{id}`          — opt out
//!
//! Org tenancy (PR 1.6 → m365 Train 2 PR 2.5): every handler above fails
//! closed (403) under an org `TenancyDecision` via [`reject_org_teams_access`]
//! — same policy shape as the other connector routes — and resolves its pool
//! through the `Tenant` extractor on BOTH planes. Pre-PR-2.5 this was an
//! UNCONDITIONAL org refusal; it is now a per-org flag gate (registry
//! provider `"microsoft"`, see [`org_microsoft_teams_enabled_for`]'s own doc
//! comment for why NOT `"teams"`, which already means something unrelated in
//! this same registry). Flag off (the zero-row default): byte-identical to
//! pre-2.5 — every org still gets today's clean 403. Flag on for an org: the
//! handler proceeds with a `tenant.pool`-resolved TENANT pool instead of the
//! Personal-only consumer pool, and every plane-dependent decision below
//! (primary-graph resolution, `install_watch`'s org key model, credential
//! loads) is now genuinely reachable under `AccountPlane::Org` rather than
//! dead code. The `pub(crate)` helpers shared with the `teams_bot` webhook
//! (`install_watch`/`remove_watch`/`ensure_live_subscriptions`/
//! `backfill_history`/…) take an explicit pool from their caller: the JWT
//! handlers pass their `Tenant`-resolved pool (Personal or Org), the
//! (allowlisted, consumer-plane) webhook passes its own consumer pool.
//!
//! **Credentials stay control-only regardless of plane.** `oauth_tokens` is
//! a CONTROL-plane-only table (Foundation train, `connector_flags.rs`'s own
//! "two-pool split" convention) — [`microsoft_token`] resolves
//! `state.control_db()` itself rather than accepting the caller's
//! (potentially tenant) pool, exactly the class of bug m365 Train 2 PR 2.5's
//! cross-plane read sweep found and fixed here (see that function's own doc
//! comment) — Slack's own step-5a sweep (#492) found the identical shape of
//! bug in `slack_integrations`.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::{
    connectors::{credential_health, microsoft, ConnectorError, MicrosoftConnector, TokenOwner},
    middleware::auth::UserContext,
    routes::{
        connectors::load_creds_bg,
        graph_webhook::{ingest_container_messages, ingest_message_stored},
        oauth::store_token_bundle,
        slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    },
    tenancy::extractor::Tenant,
    AppState,
};

/// Pure parsing logic behind [`org_microsoft_teams_enabled`], factored out so
/// the flag semantics are unit-testable without mutating process-global env
/// state — mirrors `routes::oauth::parse_org_slack_oauth_flag` /
/// `workers::member_ocean_reconciler::parse_enabled_flag`.
fn parse_org_microsoft_teams_flag(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// The fleet-wide DEFAULT for whether an ORG account may reach the Microsoft
/// Teams CONNECTOR routes in this file (registry provider `"microsoft"` —
/// see [`org_microsoft_teams_enabled_for`]'s own doc comment). Same
/// fail-closed contract as `routes::oauth::org_slack_oauth_enabled`: `"1"`
/// or a case-insensitive `"true"` is on; unset or anything else is off. Read
/// fresh on every call — never cached at process start.
///
/// **No env var ever gated this before m365 Train 2 PR 2.5.**
/// [`reject_org_teams_access`] used to 403 every org context
/// UNCONDITIONALLY, so this reader's default-off behavior (unset ⇒ `false`)
/// is what keeps a zero-row `org_connector_flags` registry byte-identical to
/// pre-2.5 behavior for every org.
pub(crate) fn org_microsoft_teams_enabled() -> bool {
    parse_org_microsoft_teams_flag(std::env::var("ORG_MICROSOFT_TEAMS_ENABLED").ok().as_deref())
}

/// Whether org `clerk_org_id` may reach the Microsoft Teams CONNECTOR routes
/// in this file — [`org_microsoft_teams_enabled`]'s env default, overridable
/// per-org via the `org_connector_flags` registry (Foundation train PR 1.3,
/// `crate::connector_flags::resolve`).
///
/// **Registry provider is `"microsoft"`, deliberately NOT `"teams"`.** The
/// `"teams"` provider in this SAME registry already means something entirely
/// different: `routes::teams::org_teams_active_for`'s `ORG_TEAMS_ENABLED` /
/// `MEMBER_OCEANS_ENABLED`-coupled gate for CS-094 NAMED TEAMS (CoralSwarm's
/// own key-envelope team-ocean feature) — a coincidental name collision with
/// "Microsoft Teams" the connector, confirmed by reading `routes/teams.rs`
/// and its `admin::validate_connector_flag_write`/`lib.rs::run()` startup
/// assert, none of which reference Microsoft or this file at all. Migration
/// `0178_org_connector_flags.sql`'s own header foresaw exactly this split
/// ("an enum would need widening for every future flag this registry picks
/// up (Microsoft, per the M365 plan's Train 2/3)"). Reusing `"teams"` here
/// would have wired an operator's Microsoft-Teams pilot flip onto CS-094's
/// `MEMBER_OCEANS_ENABLED` coupling guard and `lib.rs`'s startup assert for
/// no reason — two unrelated features sharing one flag. `"microsoft"` also
/// keeps this consistent with every other Microsoft-provider key in this
/// codebase (`oauth_tokens.provider`, `connector_installs.provider`, the
/// connector id `MicrosoftConnector::id()`).
///
/// This is also, deliberately, a SEPARATE env var from `ORG_TEAMS_ENABLED`
/// (`ORG_MICROSOFT_TEAMS_ENABLED`, not a second reader of the same var) for
/// the identical reason — see [`org_microsoft_teams_enabled`]'s own doc
/// comment. `lib.rs::run()`'s `ORG_TEAMS_ENABLED → MEMBER_OCEANS_ENABLED`
/// startup assert is therefore UNTOUCHED by this PR: it was never about this
/// flag, and stays scoped to CS-094 exactly as `routes::teams::
/// org_teams_active_for`'s own doc comment already documents.
pub(crate) async fn org_microsoft_teams_enabled_for(state: &AppState, clerk_org_id: &str) -> bool {
    crate::connector_flags::resolve(
        state,
        clerk_org_id,
        "microsoft",
        org_microsoft_teams_enabled(),
    )
    .await
}

/// Derive [`AccountPlane`] from an already-gated `Tenant` (m365 Train 2 PR
/// 2.5). Every call site here runs AFTER [`reject_org_teams_access`], so an
/// `Org` `TenancyDecision` reaching this point has already had
/// [`org_microsoft_teams_enabled_for`] confirmed true for its org — no extra
/// DB read is needed to re-derive the plane the way `routes::slack::
/// account_plane` must for callers with no `Tenant` in scope at all (the
/// webhook funnels in `graph_webhook.rs`/`teams_bot.rs`).
fn plane_of(tenant: &Tenant) -> AccountPlane {
    match tenant.org_context() {
        Some(_) => AccountPlane::Org,
        None => AccountPlane::Personal,
    }
}

/// Why the caller has no usable Microsoft identity for private meeting capture.
enum CaptureRefusal {
    /// No current grant for this caller (also: plane/owner mismatch, lookup failure).
    Disconnected,
    /// A consumer Microsoft account: Graph has no transcript API for it.
    Consumer,
}

/// The caller's capture identity, derived from the account's own metadata and the
/// authenticated `UserContext` — never from a meeting selector, install or chat id.
/// A personal workspace resolves to its real owner and account-wide work/school grant.
async fn caller_capture_identity(
    state: &AppState,
    tenant: &Tenant,
    ctx: &UserContext,
) -> Result<crate::routes::teams_onboarding::CaptureIdentity, CaptureRefusal> {
    use crate::routes::teams_onboarding::{capture_identity, CaptureIdentityError, ExpectedPlane};
    let Some(control) = state.control_db() else {
        return Err(CaptureRefusal::Disconnected);
    };
    let expected = match tenant.org_context() {
        Some(_) => ExpectedPlane::Org,
        None => ExpectedPlane::Personal,
    };
    match capture_identity(control, ctx.account_id, ctx.user_id, expected).await {
        Ok(identity) => Ok(identity),
        Err(CaptureIdentityError::ConsumerUnsupported) => Err(CaptureRefusal::Consumer),
        Err(CaptureIdentityError::NotConnected) => Err(CaptureRefusal::Disconnected),
        Err(CaptureIdentityError::Unavailable(error)) => {
            tracing::warn!(%error, account_id = %ctx.account_id, "teams capture identity lookup failed");
            Err(CaptureRefusal::Disconnected)
        }
    }
}

/// Access token for exactly the grant `identity` verified. A personal grant is
/// pinned to its Microsoft directory; org keeps the unchanged member lookup.
async fn capture_token(
    state: &Arc<AppState>,
    ctx: &UserContext,
    identity: &crate::routes::teams_onboarding::CaptureIdentity,
) -> Result<String, Response> {
    let pin = matches!(
        identity.plane,
        crate::routes::teams_onboarding::CapturePlane::Personal { .. }
    )
    .then_some(identity.tenant.as_str());
    microsoft_token_for(state, ctx.account_id, identity.user(), pin)
        .await
        .map(|(token, _)| token)
}

fn consumer_identity_unsupported() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({"code":"consumer_identity_unsupported","error":"Microsoft does not provide meeting transcripts for personal Microsoft accounts. Connect a work or school account to capture this meeting."})),
    )
        .into_response()
}

/// Fail-closed gate for the Teams connector routes under an org
/// `TenancyDecision` (org-tenancy PR 1.6, security-review P0 fix: this file
/// was wrongly treated as a webhook and left ungated; m365 Train 2 PR 2.5:
/// lifted the unconditional refusal behind [`org_microsoft_teams_enabled_for`]'s
/// per-org flag) — same policy shape as `routes::connectors::
/// reject_org_connector_access`/`routes::oauth::reject_org_oauth_management`.
/// Flag off: refuse cleanly under Org before any pool resolution, DB write,
/// or backfill spawn — Personal / flag-off requests are byte-identical to
/// before this PR. Flag on for this org: proceed, and every call site below
/// resolves its pool through `tenant.pool` (the org's own tenant pool, not
/// the consumer pool) instead of assuming Personal.
pub(crate) async fn reject_org_teams_access(state: &AppState, tenant: &Tenant) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if org_microsoft_teams_enabled_for(state, &ctx.org_id).await {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        "teams connector route refused under org context (ORG_MICROSOFT_TEAMS_ENABLED/\
         org_connector_flags resolved off for this org)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "Teams ingestion is available in your personal workspace only; \
                          switch out of the organization workspace to manage it"
            })),
        )
            .into_response(),
    )
}

// ── Listing (picker) ──────────────────────────────────────────────────────────

/// `GET /v1/connectors/microsoft/teams`
pub async fn list_teams(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    // `microsoft_token` no longer takes a pool at all (m365 Train 2 PR 2.5:
    // credentials are always control-plane, see its own doc comment) — this
    // check is purely "is a content pool configured for this plane at all"
    // (Personal: `state.db`; Org: the org's tenant pool, always `Some` once
    // `Tenant`'s Org branch resolves — see `tenancy::extractor::Tenant`).
    if tenant.pool.is_none() {
        return service_unavailable();
    }
    let token = match microsoft_token(&state, user_ctx.account_id, user_ctx.user_id).await {
        Ok((t, _)) => t,
        Err(r) => return r,
    };
    match microsoft::list_joined_teams(&state.http_client, &token).await {
        Ok(teams) => (StatusCode::OK, Json(json!({ "teams": teams }))).into_response(),
        Err(e) => connector_err(e),
    }
}

#[derive(Debug, Deserialize)]
pub struct ChannelsParams {
    team_id: String,
}

impl ChannelsParams {
    /// For the connector catalog, which lists a team's channels in-process.
    pub(crate) fn new(team_id: &str) -> Self {
        Self { team_id: team_id.to_string() }
    }
}

/// `GET /v1/connectors/microsoft/teams/channels?team_id=…`
pub async fn list_channels(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Query(params): Query<ChannelsParams>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    if tenant.pool.is_none() {
        return service_unavailable();
    }
    let token = match microsoft_token(&state, user_ctx.account_id, user_ctx.user_id).await {
        Ok((t, _)) => t,
        Err(r) => return r,
    };
    match microsoft::list_channels(&state.http_client, &token, &params.team_id).await {
        Ok(channels) => (StatusCode::OK, Json(json!({ "channels": channels }))).into_response(),
        Err(e) => connector_err(e),
    }
}

/// `GET /v1/connectors/microsoft/teams/chats`
pub async fn list_chats(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    if tenant.pool.is_none() {
        return service_unavailable();
    }
    let token = match microsoft_token(&state, user_ctx.account_id, user_ctx.user_id).await {
        Ok((t, _)) => t,
        Err(r) => return r,
    };
    match microsoft::list_chats(&state.http_client, &token).await {
        Ok(chats) => (StatusCode::OK, Json(json!({ "chats": chats }))).into_response(),
        Err(e) => connector_err(e),
    }
}

// ── Opt-in + backfill ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize)]
pub struct IngestTeamsRequest {
    /// "channel" or "chat".
    pub kind: String,
    #[serde(default)]
    pub team_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub chat_id: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

impl IngestTeamsRequest {
    /// The opt-in of one chat, for the connector catalog's scope picker.
    pub(crate) fn chat(chat_id: &str, display_name: &str) -> Self {
        Self {
            kind: "chat".into(),
            team_id: None,
            channel_id: None,
            chat_id: Some(chat_id.to_string()),
            display_name: Some(display_name.to_string()),
        }
    }

    /// The opt-in of one channel, for the connector catalog's scope picker.
    pub(crate) fn channel(team_id: &str, channel_id: &str, display_name: &str) -> Self {
        Self {
            kind: "channel".into(),
            team_id: Some(team_id.to_string()),
            channel_id: Some(channel_id.to_string()),
            chat_id: None,
            display_name: Some(display_name.to_string()),
        }
    }
}

/// `POST /v1/connectors/microsoft/teams/ingest` — opt a channel/chat in and
/// backfill its recent messages.
pub async fn ingest_resource(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<IngestTeamsRequest>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };

    // Validate the shape per kind.
    let (team_id, channel_id, chat_id) = match body.kind.as_str() {
        "channel" => match (body.team_id.as_deref(), body.channel_id.as_deref()) {
            (Some(t), Some(c)) if !t.is_empty() && !c.is_empty() => {
                (t.to_string(), c.to_string(), String::new())
            }
            _ => return bad_request("channel requires team_id and channel_id"),
        },
        "chat" => match body.chat_id.as_deref() {
            Some(c) if !c.is_empty() => (String::new(), String::new(), c.to_string()),
            _ => return bad_request("chat requires chat_id"),
        },
        _ => return bad_request("kind must be 'channel' or 'chat'"),
    };

    // Attributable (member-private-oceans PR 5, plan §3): `user_ctx` is
    // verified here — a real end user acting through their own session, on
    // either plane. Plane is derived via `plane_of` (m365 Train 2 PR 2.5:
    // `reject_org_teams_access` above already confirmed an Org caller's flag
    // is on, so this is genuinely reachable under Org now, not dead code).
    let graph_id = match resolve_primary_graph(
        &pool,
        user_ctx.account_id,
        plane_of(&tenant),
        IngestCaller::Attributable(user_ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, "teams ingest: resolve primary graph failed");
            return internal_error();
        }
    };

    // `teams_watched_resources` carries a CONSUMER-plane `account_id` RLS
    // policy (migration 0083) AND, on the TENANT plane, 0009's
    // `member_ocean_isolation` policy (`graph_id IN (SELECT * FROM
    // tenant_rls_visible_graph_ids())`, which needs `app.tenant_actor`/
    // `app.user_id` — neither set by plain `begin_account_scoped`). m365
    // Train 2 PR 2.5's tenant-rls-cutover-runbook.md §2.6 audit had this file
    // marked `exempt` from that requirement ONLY because `reject_org_teams_
    // access` made `tenant.pool` provably always the consumer pool — this PR
    // is exactly the "future change" that document's own §2.6 flagged as
    // invalidating that exemption ("this manifest's 11 new `exempt` entries
    // would need to flip back to `pending`/`converted`"). `begin_tenant_
    // scoped` (`begin_account_scoped` + `set_tenant_user`) is the correct
    // replacement here — this handler always has a real acting end user
    // (`user_ctx.user_id`, JWT-verified) — and is inert on the Personal path
    // (0083's policy never reads the extra GUCs it sets), so this is the
    // SAME "unconditional, harmless on the plane that doesn't need it" shape
    // `install_watch`'s own `begin_tenant_system_scoped` call already uses.
    let mut tx = match crate::tenancy::scope::begin_tenant_scoped(
        &pool,
        user_ctx.account_id,
        user_ctx.user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "teams ingest: failed to open scoped tx");
            return internal_error();
        }
    };
    let display_name = body.display_name.unwrap_or_default();
    let row = upsert_delegated_watch(
        &mut tx,
        user_ctx.account_id,
        user_ctx.user_id,
        graph_id,
        &body.kind,
        &team_id,
        &channel_id,
        &chat_id,
        &display_name,
    )
    .await;

    let id = match row {
        Ok(id) => {
            let _ = tx.commit().await;
            id
        }
        Err(e) => {
            tracing::error!(error = %e, "teams ingest: insert failed");
            return internal_error();
        }
    };

    // Backfill off the request path. The spawn owns `pool` — the
    // `Tenant`-resolved handle this handler already used for the watch
    // insert — rather than re-deriving an ambient pool from `state` inside
    // the task (org-tenancy PR 1.6: every spawn in a migrated route owns a
    // tenant-scoped handle).
    let bg = state.clone();
    let bg_pool = pool.clone();
    let account_id = user_ctx.account_id;
    let user_id = user_ctx.user_id;
    let kind = body.kind.clone();
    tokio::spawn(async move {
        backfill_resource(
            bg, bg_pool, account_id, user_id, graph_id, kind, team_id, channel_id, chat_id,
        )
        .await;
    });

    (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
}

/// Load delegated creds + fetch and ingest the resource's recent messages.
///
/// `pool` is the OWNED handle the spawning handler resolved through its
/// `Tenant` extractor (org-tenancy PR 1.6; this used to re-derive the
/// ambient `state.db` inside the task) — used ONLY for `ensure_subscription_
/// for`'s `graph_subscriptions` row (tenant-plane per m365 Train 2 PR 2.2)
/// below; `microsoft_token` no longer takes this pool at all (m365 Train 2
/// PR 2.5: it resolves the control pool itself — see its own doc comment).
/// Since PR 2.5's gate lift, `pool` is genuinely the org's tenant pool when
/// this spawn originates from an Org caller whose flag resolved on, not
/// always the consumer pool.
#[allow(clippy::too_many_arguments)]
async fn backfill_resource(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
) {
    // Rate-limit scope for every Graph call this spawned task makes (m365
    // Train 2 PR 2.6) — a fresh `tokio::spawn` has no inherited task-local
    // state, so this is opened here, not at the spawn site. See
    // `graph_rate_limit`'s module doc.
    crate::graph_rate_limit::scoped(
        account_id.to_string(),
        backfill_resource_inner(
            state, pool, account_id, user_id, graph_id, kind, team_id, channel_id, chat_id,
        ),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn backfill_resource_inner(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
) {
    let container = if channel_id.is_empty() { &chat_id } else { &channel_id };
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_system_scoped(&pool, account_id).await else { return; };
    let bounds = sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)>("SELECT backfill_since, backfill_until FROM teams_watched_resources WHERE account_id = $1 AND user_id = $2 AND auth_mode = 'delegated' AND (channel_id = $3 OR chat_id = $3)")
        .bind(account_id).bind(user_id).bind(container).fetch_optional(&mut *tx).await;
    tx.rollback().await.ok();
    let Ok(Some(expected_bounds)) = bounds else { return; };
    let (token, _owner) = match microsoft_token(&state, account_id, user_id).await {
        Ok(pair) => pair,
        Err(_) => {
            tracing::info!(%account_id, "teams backfill skipped — Microsoft not connected");
            return;
        }
    };

    let page = match kind.as_str() {
        "channel" => {
            microsoft::fetch_channel_messages(
                &state.http_client,
                &token,
                &team_id,
                &channel_id,
                None,
                None,
            )
            .await
        }
        "chat" => {
            microsoft::fetch_chat_messages(&state.http_client, &token, &chat_id, None, None).await
        }
        _ => return,
    };
    let page = match page {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, kind, "teams backfill: message fetch failed");
            return;
        }
    };

    let container_id = if channel_id.is_empty() {
        &chat_id
    } else {
        &channel_id
    };
    let complete = page.complete;
    let page_len = page.items.len();
    let ingested = ingest_container_messages(
        &state,
        account_id,
        user_id,
        graph_id,
        container_id,
        page.items,
    )
    .await;
    if ingested != page_len {
        // Do NOT advance the cursor past messages that failed to ingest — see
        // `backfill_history_inner`'s identical guard for the full reasoning.
        tracing::warn!(
            %account_id, kind, ingested, page_len,
            "teams backfill: partial ingest failure this page — cursor not advanced, \
             page will be retried"
        );
    } else if let Err(e) = microsoft::set_teams_backfill_state_fenced(
        &pool,
        account_id,
        container_id,
        user_id,
        "delegated",
        page.next_cursor.as_deref(),
        complete,
        Some(expected_bounds),
    )
    .await
    {
        tracing::warn!(error = %e, %account_id, kind, "teams backfill: cursor persist failed");
    }
    tracing::info!(%account_id, kind, ingested, complete, "teams backfill pass complete");

    // Delegated channel/chat push is one control-plane doorbell, not one
    // Graph subscription per watcher (Phase G wave 3, item 56). A failure
    // here leaves the 6-hour backstop covering the watch.
    if let Some(resource) =
        microsoft::teams_subscription_resource(&kind, &team_id, &channel_id, &chat_id)
    {
        if let Some(control) = state.control_db() {
            match crate::crypto::DekPlane::resolve_for_account(&state, control, account_id).await {
                Ok(dek_plane) => {
                    let dek = dek_plane.as_source();
                    if let Err(e) = crate::connectors::teams_doorbell::ensure_delegated_container(
                        &state.http_client,
                        control,
                        &dek,
                        &resource,
                        account_id,
                        user_id,
                    )
                    .await
                    {
                        tracing::info!(error = %e, %account_id, "teams doorbell unavailable (using backfill)");
                    }
                }
                Err(e) => {
                    tracing::info!(error = %e, %account_id, "teams doorbell skipped — DEK plane unresolved");
                }
            }
        }
    }
}

// ── Install via the Teams app (RSC / app-only) — CS-121 ───────────────────────────

#[derive(Debug, Deserialize)]
pub struct InstallTeamsRequest {
    /// "channel" or "chat".
    pub kind: String,
    #[serde(default)]
    pub team_id: Option<String>,
    #[serde(default)]
    pub channel_id: Option<String>,
    #[serde(default)]
    pub chat_id: Option<String>,
    /// Tenant the app was installed in — the app-only token targets it.
    pub tenant_id: String,
    #[serde(default)]
    pub display_name: Option<String>,
    /// History to backfill: `null` = all; `0` = going forward only; `n` = last n days.
    #[serde(default)]
    pub backfill_days: Option<i64>,
}

/// `POST /v1/connectors/microsoft/teams/install` — called by the Teams app's tab
/// config page when an owner adds CoralSwarm to a channel/chat. Records an
/// app-mode (RSC) watch and backfills the chosen window via an app-only token.
///
/// **Org-admin only for the pilot** (CodeRabbit review on PR #668, founder
/// decision): reuses `routes::oauth::reject_org_slack_non_admin` — despite
/// its Slack-flavored name it is provider-blind (`tenant.org_context()` +
/// `services::deletion::is_admin_role`, nothing Slack-specific), the same
/// "existing admin-role helper, not a new mechanism" convention
/// `routes::slack.rs`'s own admin-only connect/disconnect routes already
/// reuse. See this call site's own comment below for why this route in
/// particular needs it.
pub async fn install_resource(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<InstallTeamsRequest>,
) -> Response {
    // Settings launch PR 2.8: a human, browser-session action only
    // (`middleware::auth::authority_allowed`).
    if let Some(refusal) = crate::middleware::auth::interactive_session_refusal() {
        return refusal.into_response();
    }
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    // Org-admin only (pilot restriction, CodeRabbit review on PR #668,
    // founder decision). This route (via `install_watch` below) is an
    // app-only/RSC action — it does NOT touch the delegated
    // `TokenOwner::AccountWide` credential `microsoft_token`/`list_teams`/
    // `list_channels`/`list_chats`/`ingest_resource`/`sync_transcripts` share
    // in this file (that shared-credential concern is exactly why THOSE
    // browsing/opt-in routes are explicitly left member-accessible by this
    // same decision, not widened here) — it mints its own app-only Graph
    // token via `microsoft::app_token`. The reason to gate THIS route is
    // different: unlike a personal opt-in, install/uninstall here durably
    // seats and backfills a channel/chat for the WHOLE org (roster
    // auto-seating, PR 2.4) off one tenant-wide app installation — the same
    // class of broad, hard-to-undo, org-wide action
    // `routes::oauth::reject_org_slack_non_admin`'s own doc comment already
    // restricts Slack connect/disconnect to for the identical reason (org
    // governance decision 2026-08-04, piece 1). `TokenOwner::AccountWide`
    // itself is untouched and stays correct until Train 3's `owner_user_id`
    // remodel — that train is also expected to widen INSTALL rights on this
    // route from admin-only to team-owners/delegated members, not just
    // change how tokens are stored. Personal accounts are unaffected
    // (`reject_org_slack_non_admin` returns `None` whenever
    // `tenant.org_context()` is `None`).
    if let Some(reject) = crate::routes::oauth::reject_org_slack_non_admin(&tenant, &user_ctx.role)
    {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    if body.tenant_id.trim().is_empty() {
        return bad_request("tenant_id is required");
    }
    let (team_id, channel_id, chat_id) = match body.kind.as_str() {
        "channel" => match (body.team_id.as_deref(), body.channel_id.as_deref()) {
            (Some(t), Some(c)) if !t.is_empty() && !c.is_empty() => {
                (t.to_string(), c.to_string(), String::new())
            }
            _ => return bad_request("channel requires team_id and channel_id"),
        },
        "chat" => match body.chat_id.as_deref() {
            Some(c) if !c.is_empty() => (String::new(), String::new(), c.to_string()),
            _ => return bad_request("chat requires chat_id"),
        },
        _ => return bad_request("kind must be 'channel' or 'chat'"),
    };

    let display_name = body.display_name.unwrap_or_default();
    // Attributable (member-private-oceans PR 5, plan §3): `user_ctx` is
    // verified here — a real end user acting through their own session, on
    // either plane. Plane is derived via `plane_of` (m365 Train 2 PR 2.5:
    // `reject_org_teams_access` above already confirmed an Org caller's flag
    // is on, so this is genuinely reachable under Org now, not dead code).
    match install_watch(
        &state,
        &pool,
        user_ctx.account_id,
        user_ctx.user_id,
        plane_of(&tenant),
        IngestCaller::Attributable(user_ctx.user_id),
        &body.kind,
        &team_id,
        &channel_id,
        &chat_id,
        body.tenant_id.trim(),
        &display_name,
        body.backfill_days,
    )
    .await
    {
        Ok((id, _inserted)) => {
            // Connector compliance audit (m365 enterprise train PR 4.2,
            // plan decision #11 part 1): the watch/backfill above already
            // committed, so this is durability tier 2 (`record`, its own
            // scoped transaction) — a caller who cannot also record ITS
            // audit row does not get told the install succeeded. Subject is
            // whichever container id is non-empty for this `kind`.
            let subject = if !channel_id.is_empty() {
                channel_id.clone()
            } else if !chat_id.is_empty() {
                chat_id.clone()
            } else {
                team_id.clone()
            };
            if let Err(e) = crate::services::connector_audit::record(
                &pool,
                crate::services::connector_audit::AuditEntry {
                    account_id: user_ctx.account_id,
                    actor: crate::services::connector_audit::Actor::User(user_ctx.user_id),
                    action: "route_container",
                    provider: Some("microsoft".to_string()),
                    subject: Some(subject),
                    detail: Some(json!({"kind": body.kind})),
                },
            )
            .await
            {
                tracing::error!(
                    error = %e,
                    account_id = %user_ctx.account_id,
                    "teams install: connector_audit_log 'route_container' write failed — \
                     failing the response per the compliance durability bar (the watch/backfill \
                     itself already committed and is NOT rolled back)"
                );
                return (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "failed to persist audit record"})),
                )
                    .into_response();
            }

            // Tenant-aware web URL (m365 Train 2 PR 2.5, mirroring org-slack
            // step 5's `AppState::web_base_url` fix, #495): the tab config
            // page was a STATIC file (the legacy `configure.html`, since
            // deleted) served from wherever it happened to be hosted, so
            // `window.location.origin` there was NOT the CoralSwarm web app's
            // origin — it had to be told which web surface to deep-link into
            // (`https://app.{web_apex}` for BOTH planes now: org subdomains are
            // retired, see `AppState::org_web_base_url`), through the same
            // dispatcher every other user-facing link in this crate uses.
            // Best-effort: no control pool ⇒ fall back to
            // the personal base rather than failing the install itself, since
            // the watch + backfill above already committed successfully.
            let web_base_url = match state.control_db() {
                Some(control_pool) => {
                    let clerk_org_id = tenant.org_context().map(|ctx| ctx.org_id.clone());
                    state
                        .web_base_url(control_pool, clerk_org_id.as_deref())
                        .await
                }
                None => state.personal_web_base_url(),
            };
            (
                StatusCode::CREATED,
                Json(json!({ "id": id, "web_base_url": web_base_url })),
            )
                .into_response()
        }
        Err(e) if is_watch_conflict(&e) => {
            tracing::info!(
                account_id = %user_ctx.account_id, kind = %body.kind,
                "teams install: refused — this channel/chat is already watched by a different \
                 member (one-row-per-member is Train 3 scope; not silently reassigning it)"
            );
            conflict(
                "this channel or chat is already being ingested by a different member; ask them \
                 to opt out first, or wait for per-member Teams ingest (a future release)",
            )
        }
        Err(e) => {
            tracing::error!(error = %e, "teams install failed");
            internal_error()
        }
    }
}

/// Core of the RSC (app-only) install — record an app-mode watch and kick off
/// backfill + live subscription. Session-free so it is shared by the Teams tab
/// install route and the **bot-presence** endpoint (CS-122), where there is no
/// CoralSwarm user session — the account is resolved from the tenant instead.
///
/// `pool` is the CALLER's resolved handle (org-tenancy PR 1.6; was ambient
/// `state.db`): the JWT install route passes its `Tenant`-resolved pool, the
/// consumer-plane `teams_bot` webhook passes its own consumer pool. The
/// spawns below own clones of it.
///
/// `user_id` and `ingest_caller` are DELIBERATELY separate (member-private-
/// oceans PR 5, plan §3): `user_id` is bookkeeping — the `teams_watched_resources.
/// user_id` column, populated even for the bot-presence path (there it is
/// `account_members ORDER BY joined_at LIMIT 1`, an arbitrary representative
/// member, NOT the message sender). `ingest_caller` is what
/// `resolve_primary_graph` uses to pick an org-account destination, and must
/// be `NonAttributable` for that same bot-presence path — feeding the
/// representative `user_id` into it as `Attributable` would silently resolve
/// to an arbitrary member's private ocean instead of refusing.
///
/// `plane` is likewise caller-supplied (org-tenancy plane split, step 0,
/// `AccountPlane`'s doc comment): the JWT install route derives it from its
/// own already-gated `Tenant` via `plane_of` (m365 Train 2 PR 2.5 — before
/// this PR `reject_org_teams_access` made the route Personal-only and this
/// was hardcoded `AccountPlane::Personal`; now it is genuinely `Org` when
/// the caller's org has `org_microsoft_teams_enabled_for` on); the bot-
/// presence webhook has no `Tenant` in scope (an org account genuinely can
/// reach it with no JWT session at all) and must derive its plane itself via
/// `slack::account_plane` before calling this function.
///
/// **Org-admin authorization lives at the JWT route caller, not here**
/// (CodeRabbit review on PR #668, founder decision): `install_resource`
/// checks `routes::oauth::reject_org_slack_non_admin` before ever calling
/// this function — this function itself has no way to enforce that check
/// uniformly, because its OTHER caller (the bot-presence webhook) has no
/// Clerk JWT/`UserContext::role` to check at all; that path is authorized
/// entirely by Microsoft's own RSC consent + Bot Framework signature, a
/// different (and already-reviewed, PR 2.4) trust boundary. Pilot-stage
/// restriction only; Train 3's per-member delegated tokens are expected to
/// widen install rights from admin-only to team-owners/delegated members.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn install_watch(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    plane: AccountPlane,
    ingest_caller: IngestCaller,
    kind: &str,
    team_id: &str,
    channel_id: &str,
    chat_id: &str,
    tenant_id: &str,
    display_name: &str,
    backfill_days: Option<i64>,
) -> Result<(Uuid, bool), ConnectorError> {
    // Phase G item 48. RSC install is the org-install watch. After the
    // grace this returns before any row is written and before the live
    // subscription / history backfill are spawned. No control pool (or a
    // pool without the grace table) fails open inside the reader.
    if !crate::services::org_install_grace::org_install_capture_open(
        state.control_db().unwrap_or(pool),
    )
    .await
    {
        return Err(ConnectorError::Internal(format!(
            "{}: RSC app-mode watch is not installed",
            crate::services::org_install_grace::GRACE_CLOSED_INSTALL
        )));
    }
    let graph_id = resolve_primary_graph(pool, account_id, plane, ingest_caller)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve primary graph: {e}")))?;

    // `xmax = 0` is true only for a fresh INSERT (false on the conflict-update),
    // so callers can distinguish a first install from a duplicate install event
    // (Teams sends installationUpdate + conversationUpdate for one add) — CS-122.
    //
    // App-mode RSC installs stay ONE row per container (migration 0253 /
    // tenant 0104 partial unique `WHERE auth_mode = 'app'`). S4 widens only
    // the delegated path into per-member rows; this install path is NOT that
    // capture model (item 48 retires it in wave 5). The `WHERE user_id =
    // EXCLUDED.user_id` guard keeps a different bookkeeping user from
    // silently reassigning an existing app-mode row — same 409 shape as
    // before for `install_resource`. Deliberately NOT a new `ConnectorError`
    // variant; `is_watch_conflict` is the one place that string prefix is
    // interpreted.
    //
    // RLS scoping (org-tenancy conversion, tightened in m365 Train 2 PR 2.4):
    // this helper is shared by the JWT install route AND the signature-
    // authenticated `teams_bot` webhook, neither of which has a single real
    // acting end user for `teams_watched_resources`' tenant-plane policy
    // (`migrations_tenant/0009_tenant_rls`'s `member_ocean_isolation`,
    // anchored on `graph_id` via `tenant_rls_visible_graph_ids()`, which
    // reads `app.tenant_actor`/`app.user_id` — neither set by plain
    // `begin_account_scoped`). `begin_tenant_system_scoped` is the same
    // "no acting end user, tenant-wide write" scope `seats::record_seat_
    // grant`/`revoke_seat_grant` already require for the identical reason;
    // it is exactly `begin_account_scoped` plus `app.tenant_actor = 'system'`
    // — inert on the consumer plane (0083's policies never read that GUC),
    // so Personal-plane behavior is unchanged.
    // Normalized here (moved up from below the insert, m365 Train 2 PR 2.6
    // CodeRabbit round) so the INSERT itself can persist the right
    // `backfill_complete` seed instead of always writing FALSE. A negative
    // window is invalid input (the install route deserializes this from
    // untrusted JSON); treat it as "no backfill" rather than silently falling
    // through to the all-history path (Some(n<0) passes != Some(0), then the
    // `> 0` filter below would drop it to None == all history). CS-122.
    let backfill_days = match backfill_days {
        Some(n) if n < 0 => Some(0),
        other => other,
    };
    // `Some(0)` is the bot flow's "awaiting the user's history-window choice"
    // sentinel (see the `tokio::spawn` guard below) — no backfill work is
    // pending for this row YET, so it must NOT show up in `pending_teams_
    // backfills_for_account`'s work set. Persisting `backfill_complete =
    // FALSE` here (the old behavior) let the continuation tick pick up the
    // row itself and fetch with `since = None`, ingesting full history
    // before the user ever picked a window. `claim_history_choice` +
    // `backfill_history`'s own `set_teams_backfill_state` call flip this back
    // to FALSE (with a real cursor) once a window is actually chosen.
    let backfill_complete_seed = backfill_days == Some(0);
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("install_watch scoped tx: {e}")))?;
    // `backfill_complete = FALSE` on a fresh insert — same reasoning as
    // `ingest_resource`'s delegated insert above (m365 Train 2 PR 2.6) —
    // EXCEPT when the caller is deferring the choice (`Some(0)`, see above),
    // in which case the row must start out of the continuation tick's work
    // set until the user actually picks a window.
    let row: Option<(Uuid, bool)> = sqlx::query_as(
        r#"
        INSERT INTO teams_watched_resources
            (account_id, user_id, graph_id, kind, team_id, channel_id, chat_id,
             display_name, tenant_id, auth_mode, last_ingested_at, backfill_complete, backfill_since, backfill_until)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'app', now(), $10, CASE WHEN $11::bigint > 0 THEN now() - make_interval(days => $11::int) END, CASE WHEN $11::bigint > 0 THEN now() END)
        ON CONFLICT (account_id, kind, team_id, channel_id, chat_id)
            WHERE auth_mode = 'app'
        DO UPDATE SET graph_id = EXCLUDED.graph_id,
                      display_name = EXCLUDED.display_name,
                      tenant_id = EXCLUDED.tenant_id,
                      auth_mode = 'app',
                      last_ingested_at = now()
        WHERE teams_watched_resources.user_id = EXCLUDED.user_id
        RETURNING id, (xmax = 0) AS inserted
        "#,
    )
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(kind)
    .bind(team_id)
    .bind(channel_id)
    .bind(chat_id)
    .bind(display_name)
    .bind(tenant_id)
    .bind(backfill_complete_seed)
    .bind(backfill_days)
    .fetch_optional(&mut *tx)
    .await
    .map_err(|e| ConnectorError::Internal(format!("watch insert: {e}")))?;
    let Some((id, inserted)) = row else {
        // WHERE guard skipped the update — a DIFFERENT member (or
        // representative bookkeeping id) already watches this exact
        // channel/chat. Roll back (nothing to persist) and refuse.
        tx.rollback().await.ok();
        return Err(ConnectorError::Internal(format!(
            "{WATCH_CONFLICT_MARKER}{kind} {team_id}/{channel_id}/{chat_id} for account \
             {account_id} is already watched by a different member (user_id mismatch) — \
             refusing to reassign it (one-row-per-member is Train 3 scope)"
        )));
    };
    tx.commit()
        .await
        .map_err(|e| ConnectorError::Internal(format!("watch insert commit: {e}")))?;

    // ── Control-plane routing index (org-connectors-and-channel-ingest.md
    // §9c, migration 0121_connector_installs) ────────────────────────────────
    // Best-effort, in its OWN transaction against the CONTROL pool
    // (`state.control_db()`, never `pool` above — `pool` can be a tenant pool
    // once the org gate lifts, and `connector_installs` is control-only, see
    // that migration's own header) — NOT the same transaction as the
    // `teams_watched_resources` upsert just committed, unlike
    // `routes::oauth::callback`'s fail-the-install discipline for a brand-new
    // OAuth connect. That stricter discipline exists there because `oauth_
    // tokens` is the FIRST and only place a new connect is ever recorded; here
    // `account_id` was already resolved via `microsoft::resolve_microsoft_
    // account_ids` (an existing install, from this same RSC/bot flow or a
    // prior Outlook OAuth connect), so a failure here is exactly the drift
    // `resolve_microsoft_account_ids`'s self-healing fallback already exists
    // to tolerate — the next inbound event for this tenant pays one extra
    // fallback query and repairs it. Still worth attempting eagerly: a
    // Teams-only tenant (bot installed, no Outlook OAuth ever completed for
    // this account) has no `oauth_tokens` row to fall back to at all, so this
    // is the ONLY write that can make such a tenant routable.
    if let Some(control_pool) = state.control_db() {
        match crate::tenancy::scope::begin_account_scoped(control_pool, account_id).await {
            Ok(mut install_tx) => {
                if let Err(e) = crate::routes::oauth::record_connector_install(
                    &mut install_tx,
                    account_id,
                    "microsoft",
                    tenant_id,
                    None,
                    // App-only RSC tab install — tenant-wide, not per-member;
                    // account-wide, byte-identical to before this parameter
                    // existed.
                    None,
                )
                .await
                {
                    tracing::warn!(
                        error = %e, %account_id, tenant_id, connector_health = true,
                        "teams install_watch: failed to upsert connector_installs (non-fatal — \
                         the resolve-time self-healing fallback will repair it on the next \
                         inbound event for this tenant, if an oauth_tokens row exists to heal from)"
                    );
                } else if let Err(e) = install_tx.commit().await {
                    tracing::warn!(
                        error = %e, %account_id, tenant_id, connector_health = true,
                        "teams install_watch: failed to commit connector_installs upsert \
                         (non-fatal — see this call site's own comment)"
                    );
                }
            }
            Err(e) => tracing::warn!(
                error = %e, %account_id, tenant_id, connector_health = true,
                "teams install_watch: failed to open tx for connector_installs upsert (non-fatal \
                 — see this call site's own comment)"
            ),
        }
    } else {
        tracing::warn!(
            %account_id, tenant_id, connector_health = true,
            "teams install_watch: no control pool available — skipped connector_installs upsert \
             (non-fatal — see this call site's own comment)"
        );
    }

    // Start live capture immediately, independent of the history choice, so no
    // messages are missed between add and a backfill-window click (CS-122).
    {
        let bg = state.clone();
        let bg_pool = pool.clone();
        let (tenant, k, team, chan, chat) = (
            tenant_id.to_string(),
            kind.to_string(),
            team_id.to_string(),
            channel_id.to_string(),
            chat_id.to_string(),
        );
        tokio::spawn(async move {
            ensure_live_subscriptions(bg, bg_pool, account_id, tenant, k, team, chan, chat).await;
        });
    }

    // Historical backfill only when a window is requested. The tab-install route
    // passes one directly; the bot endpoint passes 0 here and runs the backfill
    // later, when the user picks a window from the in-channel prompt (CS-122).
    // (`backfill_days` was already normalized above, before the INSERT, so the
    // `Some(0)` seed used for `backfill_complete_seed` and this check agree.)
    if backfill_days != Some(0) {
        let since = backfill_days
            .filter(|d| *d > 0)
            .map(|d| Utc::now() - Duration::days(d));
        let bg = state.clone();
        let bg_pool = pool.clone();
        let (tenant, k, team, chan, chat) = (
            tenant_id.to_string(),
            kind.to_string(),
            team_id.to_string(),
            channel_id.to_string(),
            chat_id.to_string(),
        );
        tokio::spawn(async move {
            backfill_history(
                bg, bg_pool, account_id, user_id, graph_id, tenant, k, team, chan, chat, since,
            )
            .await;
        });
    }

    Ok((id, inserted))
}

/// Atomically claim the backfill choice for a watch (CS-122 lock). Sets
/// `history_choice` only if it is currently NULL, returning the watch's
/// `(graph_id, user_id, tenant_id, team_id, channel_id, chat_id)` when the claim
/// succeeds (first click) or `None` when the card is already locked / the watch is
/// gone (stale/late click → no-op). The trailing team/channel/chat ids are the
/// load-bearing reason this returns them: the click activity lacks the team's AAD
/// group id, so the backfill must reuse the ids captured at activation.
///
/// A non-`none` claim ALSO flips `backfill_complete` back to `FALSE` (and clears
/// any stale `backfill_cursor`) in the same UPDATE, in the same transaction as
/// the claim itself (m365 Train 2 PR 2.6 CodeRabbit round 2). Every row that can
/// reach this function was seeded by `install_watch`'s `Some(0)` deferred-choice
/// path with `backfill_complete = TRUE` (see that function's own comment) so it
/// stays OUT of `pending_teams_backfills_for_account`'s work set until a window is
/// actually chosen. Without this, a real ("all"/"N days") choice left the row
/// `backfill_complete = TRUE` the instant it was claimed; if the caller's
/// subsequent `backfill_history` call then failed for ANY reason — app-token
/// mint, the Graph fetch itself, or a partial ingest on the first page — nothing
/// downstream would ever set it back to pending, and `tick_backfill_continue`
/// would never retry it: a silently abandoned backfill. Doing this atomically
/// with the claim (rather than as a follow-up write after `claim_history_choice`
/// returns) means the row is retryable the moment the choice is locked in,
/// before `backfill_history` is even called — so every one of those failure
/// paths is covered for free, and a `none` choice (no backfill work exists)
/// leaves `backfill_complete`/`backfill_cursor` untouched.
pub(crate) async fn claim_history_choice(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    kind: &str,
    channel_id: &str,
    chat_id: &str,
    choice: &str,
) -> Option<(Uuid, Uuid, String, String, String, String)> {
    // Match on the globally-unique container id (channel_id for channels,
    // chat_id for chats) and NOT team_id: a click activity sometimes omits the
    // team's aadGroupId, so team_id can differ from the install event and would
    // fail to match the stored watch (CS-122). Return the *stored* team/channel/
    // chat ids — captured at activation with the correct AAD group id — so the
    // backfill addresses Graph (`/teams/{aadGroupId}`) and the RSC grant matches.
    let container = if kind == "chat" { chat_id } else { channel_id };
    // Scoped claim (m365 Train 2 PR 2.4: `begin_tenant_system_scoped`, not
    // plain `begin_account_scoped` — same "no acting end user" reasoning as
    // `install_watch`'s own comment above) — best-effort contract preserved:
    // a scope/tx failure reads as "no claim", same as before. A failure to
    // persist the `backfill_complete`/`backfill_cursor` transition below is
    // exactly such a tx failure — it rolls the whole UPDATE back, so the claim
    // itself fails too (propagated as `None`, same as any other claim failure)
    // rather than leaving `history_choice` set with the seed's stale
    // `backfill_complete = TRUE` still in place.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id)
        .await
        .ok()?;
    let claimed: Option<(Uuid, Uuid, String, String, String, String)> = sqlx::query_as(
        r#"
        UPDATE teams_watched_resources
        SET    history_choice = $4,
               backfill_since = CASE WHEN $4 IN ('7','30','90') THEN now() - make_interval(days => $4::int) END,
               backfill_until = CASE WHEN $4 IN ('7','30','90') THEN now() END,
               backfill_complete = CASE WHEN $4 = 'none' THEN backfill_complete ELSE FALSE END,
               backfill_cursor = CASE WHEN $4 = 'none' THEN backfill_cursor ELSE NULL END
        WHERE  account_id = $1 AND kind = $2
          AND  (CASE WHEN $2 = 'chat' THEN chat_id ELSE channel_id END) = $3
          AND  history_choice IS NULL
          AND  auth_mode = 'app'
        RETURNING graph_id, user_id, tenant_id, team_id, channel_id, chat_id
        "#,
    )
    .bind(account_id)
    .bind(kind)
    .bind(container)
    .bind(choice)
    .fetch_optional(&mut *tx)
    .await
    .ok()
    .flatten();
    match claimed {
        Some(row) => tx.commit().await.ok().map(|_| row),
        None => {
            tx.rollback().await.ok();
            None
        }
    }
}

/// Does this account already watch any channel in this team? Used to decide
/// whether a channel @mention is the *first* in its team (→ fan out cards to
/// every channel) or a later one (→ activate just that channel). CS-122.
pub(crate) async fn team_has_channel_watches(
    pool: &sqlx::PgPool,
    account_id: Uuid,
    team_id: &str,
) -> bool {
    // Scoped read (m365 Train 2 PR 2.4: `begin_tenant_system_scoped`, same
    // "no acting end user" reasoning as `install_watch`'s own comment
    // above); a scope failure degrades to `false`, same as the query-error
    // arm always has.
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_system_scoped(pool, account_id).await
    else {
        return false;
    };
    let exists = sqlx::query_scalar::<_, bool>(
        r#"
        SELECT EXISTS(
            SELECT 1 FROM teams_watched_resources
            WHERE account_id = $1 AND kind = 'channel' AND team_id = $2
        )
        "#,
    )
    .bind(account_id)
    .bind(team_id)
    .fetch_one(&mut *tx)
    .await
    .unwrap_or(false);
    tx.rollback().await.ok();
    exists
}

/// Stop ingesting a channel/chat (CS-122): drop the watch row and cancel its
/// live Graph subscription. Called when the CoralSwarm bot is removed from a
/// conversation. Best-effort — failures are logged, not surfaced. Returns `true`
/// only if a watch row was actually deleted, so the caller doesn't assert
/// "ingestion stopped" when there was nothing to stop (or the delete failed).
pub(crate) async fn remove_watch(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: Uuid,
    kind: &str,
    team_id: &str,
    channel_id: &str,
    chat_id: &str,
) -> bool {
    // Scoped delete (m365 Train 2 PR 2.4: `begin_tenant_system_scoped`, same
    // "no acting end user" reasoning as `install_watch`'s own comment above)
    // — a scope/commit failure reads as "nothing deleted", matching the
    // existing best-effort arm.
    let deleted_rows: Vec<(Uuid, String)> = match crate::tenancy::scope::begin_tenant_system_scoped(
        pool, account_id,
    )
    .await
    {
        Ok(mut tx) => {
            let r: Result<Vec<(Uuid, String)>, sqlx::Error> = sqlx::query_as(
                r#"
                    DELETE FROM teams_watched_resources
                    WHERE account_id = $1 AND kind = $2
                      AND team_id = $3 AND channel_id = $4 AND chat_id = $5
                      AND auth_mode = 'app'
                    RETURNING user_id, auth_mode
                    "#,
            )
            .bind(account_id)
            .bind(kind)
            .bind(team_id)
            .bind(channel_id)
            .bind(chat_id)
            .fetch_all(&mut *tx)
            .await;
            match r {
                Ok(rows) => {
                    if tx.commit().await.is_ok() {
                        rows
                    } else {
                        Vec::new()
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "teams remove: watch delete failed");
                    Vec::new()
                }
            }
        }
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams remove: scoped tx failed");
            Vec::new()
        }
    };
    let removed = !deleted_rows.is_empty();

    if let Some(resource) =
        microsoft::teams_subscription_resource(kind, team_id, channel_id, chat_id)
    {
        // App-mode rows only (see DELETE above). App-owned Graph
        // subscriptions are account-wide (`owner_user_id IS NULL`).
        for (_row_user_id, auth_mode) in &deleted_rows {
            let owner_user_id = teams_subscription_owner_user_id(auth_mode, None);
            let delegated_token = if owner_user_id.is_some() {
                microsoft_token(state, account_id, *_row_user_id)
                    .await
                    .ok()
                    .map(|(t, _)| t)
            } else {
                microsoft_token_account_wide(state, account_id).await
            };
            if let Err(e) = microsoft::delete_subscription_for(
                &state.http_client,
                pool,
                account_id,
                &resource,
                owner_user_id,
                delegated_token.as_deref(),
            )
            .await
            {
                tracing::info!(error = %e, %account_id, "teams remove: subscription cancel failed");
            }
        }
    }
    removed
}

/// Fetch + ingest a channel/chat's history via the app (RSC) token, returning
/// `(ingested, complete)`: `ingested` is the number of messages ingested THIS
/// PAGE only (not a running total — for a mailbox larger than one page, the
/// continuation tick keeps working after this call returns), and `complete`
/// is whether the whole backfill (not just this page) is done, so callers can
/// word a user-facing confirmation correctly instead of presenting a
/// single-page count as a final total. Live capture is set up separately by
/// [`ensure_live_subscriptions`] so it starts at add-time regardless of the
/// history choice. CS-122.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn backfill_history(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    tenant_id: String,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
    since: Option<DateTime<Utc>>,
) -> (usize, bool) {
    // Rate-limit scope (m365 Train 2 PR 2.6) — see `graph_rate_limit`'s module
    // doc. Both call sites (the tab-install spawn, the bot's history-choice
    // click) run this off the request path with no inherited scope.
    crate::graph_rate_limit::scoped(
        account_id.to_string(),
        backfill_history_inner(
            state, pool, account_id, user_id, graph_id, tenant_id, kind, team_id, channel_id,
            chat_id, since,
        ),
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn backfill_history_inner(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    tenant_id: String,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
    since: Option<DateTime<Utc>>,
) -> (usize, bool) {
    let container = if channel_id.is_empty() { &chat_id } else { &channel_id };
    let Ok(mut tx) = crate::tenancy::scope::begin_tenant_system_scoped(&pool, account_id).await else { return (0, false); };
    let bounds = sqlx::query_as::<_, (Option<DateTime<Utc>>, Option<DateTime<Utc>>)>("SELECT backfill_since, backfill_until FROM teams_watched_resources WHERE account_id = $1 AND user_id = $2 AND auth_mode = 'app' AND (channel_id = $3 OR chat_id = $3)")
        .bind(account_id).bind(user_id).bind(container).fetch_optional(&mut *tx).await;
    tx.rollback().await.ok();
    let Ok(Some((stored_since, until))) = bounds else { return (0, false); };
    let since = stored_since.or(since);
    if !crate::services::org_install_grace::org_install_capture_open(
        state.control_db().unwrap_or(&pool),
    )
    .await
    {
        // Stop the continuation tick from retrying an RSC history pull the
        // grace has closed. Complete, with nothing ingested.
        let container_id = if channel_id.is_empty() {
            &chat_id
        } else {
            &channel_id
        };
        if let Err(e) = microsoft::set_teams_backfill_state_fenced(
            &pool,
            account_id,
            container_id,
            user_id,
            "app",
            None,
            true,
            Some((stored_since, until)),
        )
        .await
        {
            tracing::warn!(error = %e, %account_id, "teams backfill: grace-closed completion failed");
            return (0, false);
        }
        tracing::info!(%account_id, "teams RSC backfill unreachable — org-install grace closed");
        return (0, true);
    }
    let token = match microsoft::app_token(&state.http_client, &tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams backfill: app token failed");
            return (0, false);
        }
    };
    let page = match kind.as_str() {
        "channel" => {
            microsoft::fetch_channel_messages(
                &state.http_client,
                &token,
                &team_id,
                &channel_id,
                since,
                None,
            )
            .await
        }
        "chat" => {
            microsoft::fetch_chat_messages(&state.http_client, &token, &chat_id, since, None).await
        }
        _ => return (0, false),
    };
    let mut page = match page {
        Ok(p) => p,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, kind, "teams backfill: fetch failed");
            return (0, false);
        }
    };
    let container_id = if channel_id.is_empty() {
        &chat_id
    } else {
        &channel_id
    };
    page.items.retain(|item| microsoft::in_teams_history_window(item.timestamp, since, until));
    let complete = page.complete;
    // `ingest_container_messages`/`ingest_message_with` swallow per-message
    // failures internally (a transient DB or embedding error just doesn't
    // count toward `ingested`). Compare against the page length BEFORE the
    // move so a short count is detectable.
    let page_len = page.items.len();
    let ingested = ingest_container_messages(
        &state,
        account_id,
        user_id,
        graph_id,
        container_id,
        page.items,
    )
    .await;
    if ingested != page_len {
        // Do NOT advance the cursor past messages that failed to ingest — a
        // transient failure here would otherwise be permanent: the cursor
        // moves on, and neither the continuation tick nor the backstop
        // resync ever re-fetches this page. Leaving `backfill_cursor`/
        // `backfill_complete` untouched means this exact page is re-fetched
        // next pass, which is safe (`ingest_message`'s own dedup makes a
        // re-ingested message a no-op).
        tracing::warn!(
            %account_id, kind, ingested, page_len,
            "teams backfill: partial ingest failure this page — cursor not advanced, \
             page will be retried"
        );
        return (ingested, false);
    }
    if let Err(e) = microsoft::set_teams_backfill_state_fenced(
        &pool,
        account_id,
        container_id,
        user_id,
        "app",
        page.next_cursor.as_deref(),
        complete,
        Some((stored_since, until)),
    )
    .await
    {
        tracing::warn!(error = %e, %account_id, kind, "teams backfill: cursor persist failed");
    }
    tracing::info!(%account_id, kind, ingested, complete, "teams history backfill pass complete");
    (ingested, complete)
}

/// Start live capture for a channel/chat: create the RSC change-notification
/// subscription (+ the per-tenant transcript subscription). Runs at add-time,
/// independent of the history choice, so messages are captured from the moment
/// the bot is added (standard channels/chats succeed; private/shared return 403
/// → the periodic re-poll backstop covers them). CS-122.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ensure_live_subscriptions(
    state: Arc<AppState>,
    pool: sqlx::PgPool,
    account_id: Uuid,
    tenant_id: String,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
) {
    let capture_open = crate::services::org_install_grace::org_install_capture_open(
        state.control_db().unwrap_or(&pool),
    )
    .await;
    if !capture_open {
        tracing::info!(
            %account_id,
            "teams RSC live subscription unreachable — org-install grace closed"
        );
        return;
    }
    let token = match microsoft::app_token(&state.http_client, &tenant_id).await {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, "teams subscription: app token failed");
            return;
        }
    };
    if let Some(resource) =
        microsoft::teams_subscription_resource(&kind, &team_id, &channel_id, &chat_id)
    {
        if let Err(e) = microsoft::ensure_subscription_for(
            &state.http_client,
            &pool,
            account_id,
            &token,
            &resource,
            &tenant_id,
            TokenOwner::AccountWide,
        )
        .await
        {
            tracing::info!(error = %e, %account_id, "teams live subscription unavailable (re-poll covers it)");
        }
    }
    if let Some(resource) = crate::services::org_install_grace::tenant_wide_transcript_to_subscribe(
        capture_open,
        microsoft::transcript_subscription_resource(),
    ) {
        if let Err(e) = microsoft::ensure_subscription_for(
            &state.http_client,
            &pool,
            account_id,
            &token,
            &resource,
            &tenant_id,
            TokenOwner::AccountWide,
        )
        .await
        {
            tracing::info!(error = %e, %account_id, "teams transcript subscription unavailable");
        }
    }
}

// ── Meeting transcripts (CS-119 phase 3) ──────────────────────────────────────

/// Observe real Graph results, never a cached health row, for manual one-meeting pulls.
async fn observe_targeted_transcript_health(
    control: &sqlx::PgPool,
    account: Uuid,
    member: Option<Uuid>,
    result: &Result<
        crate::connectors::teams_targeted_transcripts::Fetched,
        crate::connectors::teams_targeted_transcripts::Error,
    >,
) {
    use crate::connectors::teams_targeted_transcripts::Error;
    let observed = match result {
        Ok(_) => {
            crate::services::connector_health::note_transcript_access_recovered(
                control, account, member,
            )
            .await
        }
        Err(Error::Graph {
            code: Some(code), ..
        }) if code == "GraphAccessToTranscriptsDisabled" => {
            crate::services::connector_health::note_transcript_access_disabled(
                control, account, member,
            )
            .await
        }
        _ => return,
    };
    if let Err(error) = observed {
        tracing::warn!(%error, %account, "targeted Teams transcript health update failed");
    }
}

/// Explicit one-meeting pull into the authenticated caller's private ocean.
/// This path deliberately never calls the calendar sweep or subscription planner.
pub(crate) fn valid_meeting_return(path: &str) -> bool {
    if matches!(
        path,
        "/teams/meeting/connected" | "/outlook/meeting/connected"
    ) {
        return true;
    }
    if path.len() > 8192 || path.contains(['\r', '\n', '\\']) {
        return false;
    }
    let Ok(url) = reqwest::Url::parse(&format!("https://app.coralswarm.example{path}")) else {
        return false;
    };
    url.host_str() == Some("app.coralswarm.example")
        && url.path() == "/connectors/microsoft-teams/meeting-capture"
        && path.starts_with("/connectors/microsoft-teams/meeting-capture?")
        && url.query_pairs().all(|(k, _)| {
            matches!(
                k.as_ref(),
                "meeting_id" | "join_url" | "meeting_ticket" | "teams_meeting_id" | "teams_chat_id"
            )
        })
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeetingConnect {
    return_path: String,
}
pub async fn connect_meeting_capture(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(body): Json<MeetingConnect>,
) -> Response {
    if let Some(r) = reject_org_teams_access(&state, &tenant).await {
        return r;
    }
    if !valid_meeting_return(&body.return_path) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"Invalid meeting return path"})),
        )
            .into_response();
    }
    let Some(secret) = crate::oauth::state::state_secret() else {
        return service_unavailable();
    };
    let Ok(signed) = crate::oauth::state::sign_state_with_meeting_return(
        &secret,
        ctx.account_id,
        ctx.user_id,
        &body.return_path,
    ) else {
        return internal_error();
    };
    let Ok(url) = crate::connectors::ConnectorProvider::authorize_url(
        &crate::connectors::MicrosoftConnector,
        &state.api_base_url(),
        &signed,
    ) else {
        return internal_error();
    };
    let mut url = match reqwest::Url::parse(&url) {
        Ok(u) => u,
        Err(_) => return internal_error(),
    };
    if let Ok(return_url) =
        reqwest::Url::parse(&format!("https://app.coralswarm.example{}", body.return_path))
    {
        if let Some((_, ticket)) = return_url
            .query_pairs()
            .find(|(k, _)| k == "meeting_ticket")
        {
            let Ok(i) = crate::services::teams_meeting_capture::verify_intent(&secret, &ticket)
            else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Meeting setup link expired"})),
                )
                    .into_response();
            };
            url.set_path(&format!("/{}/oauth2/v2.0/authorize", i.tenant));
        }
    }
    Json(json!({"authorization_url":url.as_str()})).into_response()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeetingContextSelector {
    teams_meeting_id: String,
    chat_id: String,
}
pub async fn resolve_meeting_context(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(selector): Json<MeetingContextSelector>,
) -> Response {
    if let Some(r) = reject_org_teams_access(&state, &tenant).await {
        return r;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let identity=match caller_capture_identity(&state,&tenant,&ctx).await{Ok(i)=>i,Err(CaptureRefusal::Consumer)=>return consumer_identity_unsupported(),Err(CaptureRefusal::Disconnected)=>return (StatusCode::FORBIDDEN,Json(json!({"code":"connect_required","error":"Connect your own Microsoft account, then return to this meeting"}))).into_response()};
    let tid=identity.tenant.clone();
    let target=match crate::services::teams_meeting_capture::context(control,&tid,&selector.teams_meeting_id,Some(&selector.chat_id)).await {Ok(Some((meeting,_,_)))=>meeting,_=>return (StatusCode::NOT_FOUND,Json(json!({"code":"meeting_context_unavailable","error":"Add CoralSwarm to this meeting before using its meeting add-in"}))).into_response()};
    let token = match capture_token(&state, &ctx, &identity).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    let meeting = match crate::connectors::teams_targeted_transcripts::resolve(
        &state.http_client,
        &token,
        crate::connectors::teams_targeted_transcripts::Target::MeetingId(target),
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error":e.to_string()})),
            )
                .into_response()
        }
    };
    if crate::connectors::teams_targeted_transcripts::verify_occurrence(
        &state.http_client,
        &token,
        &meeting,
    )
    .await
    .is_err()
    {
        return (StatusCode::UNPROCESSABLE_ENTITY,Json(json!({"code":"occurrence_unverified","error":"Microsoft could not verify one specific meeting occurrence"}))).into_response();
    }
    Json(json!({"meeting_id":meeting.id})).into_response()
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MeetingStatusSelector {
    join_url: Option<String>,
    teams_meeting_id: Option<String>,
    chat_id: Option<String>,
    capture_id: Option<Uuid>,
}
enum StatusTarget {
    Capture(Uuid),
    Join(crate::connectors::teams_targeted_transcripts::Target),
    Native(String, String),
}
impl MeetingStatusSelector {
    fn target(self) -> Result<StatusTarget, ()> {
        match (
            self.join_url,
            self.teams_meeting_id,
            self.chat_id,
            self.capture_id,
        ) {
            (None, None, None, Some(id)) => Ok(StatusTarget::Capture(id)),
            (Some(link), None, None, None) => {
                crate::connectors::teams_targeted_transcripts::Request {
                    meeting_id: None,
                    join_url: Some(link),
                    meeting_ticket: None,
                    chat_id: None,
                }
                .target()
                .map(StatusTarget::Join)
                .map_err(|_| ())
            }
            (None, Some(meeting), Some(chat), None)
                if [&meeting, &chat].iter().all(|id| {
                    !id.is_empty()
                        && id.len() <= 2048
                        && !id.chars().any(|c| c.is_control() || c.is_whitespace())
                }) =>
            {
                Ok(StatusTarget::Native(meeting, chat))
            }
            _ => Err(()),
        }
    }
}

// PostgreSQL persists these verified bounds at microsecond precision, matching
// the timestamp binding used by the existing occurrence/consent queries.
fn stored_occurrence_matches(
    row: &crate::services::teams_meeting_capture::CaptureStatus,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>,
) -> bool {
    row.occurrence_start.timestamp_micros() == start.timestamp_micros()
        && row.occurrence_end.timestamp_micros() == end.timestamp_micros()
}

/// Read one selected meeting only. Resolving metadata does not enable capture.
/// Subsequent own-capture polls use its UUID and never re-read Microsoft.
pub async fn meeting_capture_status(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(selector): Json<MeetingStatusSelector>,
) -> Response {
    let target = match selector.target() {
        Ok(t) => t,
        Err(()) => return (StatusCode::BAD_REQUEST, Json(json!({"code":"invalid_meeting_selector","error":"Provide one selected meeting context"}))).into_response(),
    };
    if let Some(r) = reject_org_teams_access(&state, &tenant).await {
        return r;
    }
    let Some(pool) = tenant.pool.as_ref() else {
        return service_unavailable();
    };
    if let StatusTarget::Capture(id) = target {
        let connection_required = caller_capture_identity(&state, &tenant, &ctx).await.is_err();
        return match crate::services::teams_meeting_capture::owned_status(pool,ctx.account_id,ctx.user_id,id).await {
            Ok(Some(row)) => Json(json!({"meeting_id":row.meeting_id,"title":null,"connection_required":connection_required,
                "occurrence_start":row.occurrence_start,"occurrence_end":row.occurrence_end,
                "availability_window_open": chrono::Utc::now() <= row.occurrence_end + chrono::Duration::hours(24),"capture":row})).into_response(),
            Ok(None) => (StatusCode::NOT_FOUND,Json(json!({"code":"capture_not_found","error":"This meeting selection is not available to your account"}))).into_response(),
            Err(_) => internal_error(),
        };
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let identity = match caller_capture_identity(&state, &tenant, &ctx).await {
        Ok(i) => i,
        Err(CaptureRefusal::Consumer) => return consumer_identity_unsupported(),
        Err(CaptureRefusal::Disconnected) => return (StatusCode::FORBIDDEN, Json(json!({"code":"connect_required","error":"Connect your own Microsoft account, then return to this meeting"}))).into_response(),
    };
    let (tid, aad) = identity.pair();
    let mut status_chat: Option<String> = None;
    let provider_target = match target {
        StatusTarget::Join(t) => t,
        StatusTarget::Native(meeting, chat) => {
            status_chat = Some(chat.clone());
            match crate::services::teams_meeting_capture::context(control,&tid,&meeting,Some(&chat)).await {
                Ok(Some((verified,_,_))) => crate::connectors::teams_targeted_transcripts::Target::MeetingId(verified),
                _ => return (StatusCode::NOT_FOUND,Json(json!({"code":"meeting_context_unavailable","error":"Add CoralSwarm to this meeting before using its meeting add-in"}))).into_response(),
            }
        },
        StatusTarget::Capture(_) => unreachable!(),
    };
    let token = match capture_token(&state, &ctx, &identity).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    let meeting = match crate::connectors::teams_targeted_transcripts::resolve(
        &state.http_client,
        &token,
        provider_target,
    )
    .await
    {
        Ok(m) => m,
        Err(_) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"code":"meeting_unverified","error":"Microsoft could not verify this selected meeting"})),
            )
                .into_response()
        }
    };
    let meeting = match crate::connectors::teams_targeted_transcripts::verify_occurrence(
        &state.http_client,
        &token,
        &meeting,
    )
    .await
    {
        Ok(m) => m,
        Err(_) => {
            return (StatusCode::UNPROCESSABLE_ENTITY,Json(json!({"code":"occurrence_unverified","error":"Microsoft could not confirm which meeting this is. Open the meeting from your own calendar or start it from Teams, then try again."}))).into_response();
        }
    };
    let row = match crate::services::teams_meeting_capture::status(
        pool,
        ctx.account_id,
        ctx.user_id,
        &tid,
        &aad,
        None,
        Some(&meeting.id),
    )
    .await
    {
        Ok(row) => row,
        Err(_) => return internal_error(),
    };
    let parse = |v: Option<&str>| {
        v.and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
            .map(|d| d.with_timezone(&chrono::Utc))
    };
    let (Some(start), Some(end)) = (
        parse(meeting.start.as_deref()),
        parse(meeting.end.as_deref()),
    ) else {
        return (StatusCode::BAD_GATEWAY, Json(json!({"code":"occurrence_unverified","error":"Microsoft did not provide the specific meeting occurrence"}))).into_response();
    };
    // A recurring series shares one meeting ID: a capture of an earlier,
    // finished occurrence is history, not a conflict with today's occurrence.
    let row = row.filter(|r| stored_occurrence_matches(r, start, end) || r.occurrence_end >= chrono::Utc::now());
    if row
        .as_ref()
        .is_some_and(|r| !stored_occurrence_matches(r, start, end))
    {
        return (StatusCode::CONFLICT,Json(json!({"code":"occurrence_unverified","error":"The selected occurrence differs from the stored meeting capture"}))).into_response();
    }
    // Teams pane only (it sends chat_id); a failed Graph read is "not active".
    let recording_active = match status_chat.as_deref() {
        Some(chat) => match crate::connectors::teams_targeted_transcripts::recording_active(&state.http_client, &token, chat).await {
            Ok(active) => active,
            Err(e) => {
                tracing::warn!(error = %e, "teams meeting status: recording state unreadable; reporting inactive");
                false
            }
        },
        None => false,
    };
    // `auto_transcription` is kept (always "manual") for older panes only.
    Json(json!({"meeting_id":meeting.id,"title":meeting.subject,"occurrence_start":start,"occurrence_end":end,"connection_required":false,"availability_window_open":chrono::Utc::now() <= end + chrono::Duration::hours(24),"capture":row,
        "recording_active":recording_active,"auto_transcription":"manual"})).into_response()
}

/// Explicit meeting consent; metadata is verified with the caller's own grant before commit.
pub async fn capture_meeting_transcripts(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(mut body): Json<crate::connectors::teams_targeted_transcripts::Request>,
) -> Response {
    let chat_id = body.chat_id.take();
    let intent = if let Some(ticket) = body.meeting_ticket.take() {
        let Some(secret) = crate::oauth::state::state_secret() else {
            return service_unavailable();
        };
        match crate::services::teams_meeting_capture::verify_intent(&secret, &ticket) {
            Ok(i) => {
                if body.meeting_id.is_some() || body.join_url.is_some() {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(json!({"error":"Provide one meeting selector"})),
                    )
                        .into_response();
                }
                body.meeting_id = Some(i.meeting_id.clone());
                Some(i)
            }
            Err(_) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error":"Meeting setup link expired or invalid"})),
                )
                    .into_response()
            }
        }
    } else {
        None
    };
    let target = match body.target() {
        Ok(t) => t,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":e.to_string()})),
            )
                .into_response()
        }
    };
    if let Some(r) = reject_org_teams_access(&state, &tenant).await {
        return r;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    let control = state.control_db().unwrap_or(&pool);
    let identity=match caller_capture_identity(&state,&tenant,&ctx).await {Ok(identity)=>identity,Err(CaptureRefusal::Consumer)=>return consumer_identity_unsupported(),Err(CaptureRefusal::Disconnected)=>return (StatusCode::FORBIDDEN,Json(json!({"code":"connect_required","error":"Connect your own Microsoft account first, then return to confirm this meeting"}))).into_response()};
    let (tid,aad)=identity.pair();
    if intent
        .as_ref()
        .is_some_and(|i| i.tenant != tid || i.aad != aad)
    {
        return (StatusCode::CONFLICT,Json(json!({"code":"microsoft_identity_mismatch","error":"Connect the Microsoft identity that added CoralSwarm to this meeting"}))).into_response();
    }
    if let Some(i) = &intent {
        if !matches!(crate::services::teams_meeting_capture::context(control,&tid,&i.installation_id,None).await,Ok(Some((ref graph,ref installer,ref event))) if graph==&i.meeting_id && installer==&aad && event==&i.event_id)
        {
            return (StatusCode::NOT_FOUND,Json(json!({"code":"meeting_context_unavailable","error":"This meeting installation was removed or replaced"}))).into_response();
        }
    }
    let token = match capture_token(&state, &ctx, &identity).await {
        Ok(t) => t,
        Err(r) => return r,
    };
    // Teams pane (sends chat_id): capture only while recording/transcription runs.
    if let Some(chat) = chat_id.as_deref() {
        let active = crate::connectors::teams_targeted_transcripts::recording_active(&state.http_client, &token, chat)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(error = %e, "teams capture: recording state unreadable; refusing");
                false
            });
        if !active {
            return recording_off();
        }
    }
    let meeting = match crate::connectors::teams_targeted_transcripts::resolve(
        &state.http_client,
        &token,
        target,
    )
    .await
    {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error":e.to_string()})),
            )
                .into_response()
        }
    };
    let meeting = match crate::connectors::teams_targeted_transcripts::verify_occurrence(
        &state.http_client,
        &token,
        &meeting,
    )
    .await
    {
        Ok(m) => m,
        Err(_) => {
            return (StatusCode::UNPROCESSABLE_ENTITY,Json(json!({"code":"occurrence_unverified","error":"Microsoft could not confirm which meeting this is. Open the meeting from your own calendar or start it from Teams, then try again."}))).into_response();
        }
    };
    let graph = match resolve_primary_graph(
        &pool,
        ctx.account_id,
        plane_of(&tenant),
        IngestCaller::Attributable(ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(_) => return internal_error(),
    };
    let (Some(meeting_start), Some(meeting_end)) = (
        meeting
            .start
            .as_deref()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
        meeting
            .end
            .as_deref()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
    ) else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error":"Microsoft did not provide the specific meeting occurrence"})),
        )
            .into_response();
    };
    if chrono::Utc::now() > meeting_end + chrono::Duration::hours(24) {
        return (StatusCode::GONE,Json(json!({"code":"availability_window_ended","error":"This meeting's transcript capture window has ended"}))).into_response();
    }
    let generation = intent
        .as_ref()
        .map(|i| format!("{}:{}", i.installation_id, i.event_id));
    if let Some(i) = &intent {
        if crate::services::teams_meeting_capture::installation_status(
            &pool,
            ctx.account_id,
            &tid,
            generation.as_deref().unwrap(),
            &i.installation_id,
            true,
        )
        .await
        .is_err()
        {
            return internal_error();
        }
    }
    match crate::services::teams_meeting_capture::authorize_with_window(
        &pool,
        ctx.account_id,
        ctx.user_id,
        &tid,
        &aad,
        &meeting.id,
        graph,
        meeting_start,
        meeting_end,
        meeting.transcript_end,
        if intent.is_some() {
            "meeting_bot"
        } else {
            "meeting_addin"
        },
        generation.as_deref(),
        intent.as_ref().map(|i| i.event_id.as_str()),
        intent.as_ref().map(|i| i.event_at),
    )
    .await
    {
        Ok(id) => {
            if let Some(org) = tenant.org_context() {
                // The persisted ledger is the restart-safe job; wake its scoped poller.
                state.worker_activity.note_enqueue(&org.org_id);
            }
            (StatusCode::OK, Json(json!({"capture_id":id,"meeting_id":meeting.id,"status":"waiting_for_transcript","auto_transcription":"manual"}))).into_response()
        }
        Err(_) => internal_error(),
    }
}
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeMeeting {
    meeting_id: Option<String>,
    capture_id: Option<Uuid>,
}
pub async fn revoke_meeting_transcripts(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(ctx): Extension<UserContext>,
    Json(body): Json<RevokeMeeting>,
) -> Response {
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    if let (None, Some(id)) = (&body.meeting_id, body.capture_id) {
        return match crate::services::teams_meeting_capture::revoke_owned(&pool,ctx.account_id,ctx.user_id,id).await {
            Ok(true)=>Json(json!({"status":"revoked"})).into_response(),
            Ok(false)=>(StatusCode::NOT_FOUND,Json(json!({"code":"capture_not_found","error":"This meeting selection is not available to your account"}))).into_response(),
            Err(_)=>internal_error(),
        };
    }
    let (Some(meeting_id), None) = (&body.meeting_id, body.capture_id) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"code":"invalid_meeting_selector","error":"Provide one meeting selection"}),
            ),
        )
            .into_response();
    };
    if state.control_db().is_none() {
        return service_unavailable();
    }
    let tid = match caller_capture_identity(&state, &tenant, &ctx).await {
        Ok(identity) => identity.tenant,
        Err(CaptureRefusal::Consumer) => return consumer_identity_unsupported(),
        Err(CaptureRefusal::Disconnected) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error":"Microsoft identity unavailable"})),
            )
                .into_response()
        }
    };
    match crate::services::teams_meeting_capture::revoke(
        &pool,
        ctx.account_id,
        ctx.user_id,
        &tid,
        meeting_id,
    )
    .await
    {
        Ok(()) => Json(json!({"status":"revoked"})).into_response(),
        Err(_) => internal_error(),
    }
}
pub async fn list_meeting_captures(
    state: State<Arc<AppState>>,
    tenant: Tenant,
    user: Extension<UserContext>,
    original: Option<Extension<crate::tenancy::runtime::TenancyDecision>>,
    query: axum::extract::Query<std::collections::BTreeMap<String,String>>,
) -> Response {
    crate::routes::page_read::serve::<crate::read_models::connectors::TeamsTranscriptCaptures>(state,tenant,user,original,axum::extract::Path(std::collections::BTreeMap::new()),query).await.into_response()
}

pub async fn sync_meeting_transcripts(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Json(body): Json<crate::connectors::teams_targeted_transcripts::Request>,
) -> Response {
    use crate::connectors::teams_targeted_transcripts::{self, Error};
    if crate::connectors::catalog::pause::is_paused_in(
        &state,
        user_ctx.account_id,
        user_ctx.user_id,
        crate::connectors::catalog::pause::MS_TEAMS_MEETINGS,
    )
    .await
    {
        return (
            StatusCode::CONFLICT,
            Json(json!({"code": "source_paused", "error": "Resume meeting capture before syncing"})),
        )
            .into_response();
    }
    let target = match body.target() {
        Ok(target) => target,
        Err(error) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response()
        }
    };
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    // Org keeps its original order (member credential first); a personal grant is
    // only loaded after its owner/directory identity has been verified.
    let org_token = if tenant.org_context().is_some() {
        match microsoft_token(&state, user_ctx.account_id, user_ctx.user_id).await {
            Ok((token, _)) => Some(token),
            Err(response) => return response,
        }
    } else {
        None
    };
    let identity = match caller_capture_identity(&state, &tenant, &user_ctx).await {
        Ok(identity) => identity,
        Err(CaptureRefusal::Consumer) => return consumer_identity_unsupported(),
        Err(CaptureRefusal::Disconnected) => {
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error":"Connect your own Microsoft account first"})),
            )
                .into_response()
        }
    };
    let (tid, aad) = identity.pair();
    let token = match org_token {
        Some(token) => token,
        None => match capture_token(&state, &user_ctx, &identity).await {
            Ok(token) => token,
            Err(response) => return response,
        },
    };
    let member = match identity.plane {
        crate::routes::teams_onboarding::CapturePlane::Org { member } => Some(member),
        crate::routes::teams_onboarding::CapturePlane::Personal { .. } => None,
    };
    let control = state.control_db().unwrap_or(&pool);
    // A deliberate, exact-meeting retry must observe current Graph policy.
    // The six-hour suppression remains for automatic transcript workers.
    let meeting =
        match teams_targeted_transcripts::resolve(&state.http_client, &token, target).await {
            Ok(m) => m,
            Err(e) => {
                return (
                    StatusCode::BAD_GATEWAY,
                    Json(json!({"error":e.to_string()})),
                )
                    .into_response()
            }
        };
    let meeting =
        match teams_targeted_transcripts::verify_occurrence(&state.http_client, &token, &meeting)
            .await
        {
            Ok(m) => m,
            Err(_) => {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({"error":"Microsoft could not confirm which meeting this is"})),
                )
                    .into_response();
            }
        };
    let graph = match resolve_primary_graph(
        &pool,
        user_ctx.account_id,
        plane_of(&tenant),
        IngestCaller::Attributable(user_ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(_) => return internal_error(),
    };
    let (Some(meeting_start), Some(meeting_end)) = (
        meeting
            .start
            .as_deref()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
        meeting
            .end
            .as_deref()
            .and_then(|v| chrono::DateTime::parse_from_rfc3339(v).ok())
            .map(|d| d.with_timezone(&chrono::Utc)),
    ) else {
        return (
            StatusCode::BAD_GATEWAY,
            Json(json!({"error":"Microsoft did not provide the specific meeting occurrence"})),
        )
            .into_response();
    };
    if crate::services::teams_meeting_capture::authorize_with_window(
        &pool,
        user_ctx.account_id,
        user_ctx.user_id,
        &tid,
        &aad,
        &meeting.id,
        graph,
        meeting_start,
        meeting_end,
        meeting.transcript_end,
        "explicit_meeting",
        None,
        None,
        None,
    )
    .await
    .is_err()
    {
        return internal_error();
    }
    let result = crate::graph_rate_limit::scoped(
        user_ctx.account_id.to_string(),
        teams_targeted_transcripts::fetch_verified(&state.http_client, &token, meeting),
    )
    .await;
    observe_targeted_transcript_health(control, user_ctx.account_id, member, &result).await;
    let fetched = match result {
        Ok(result) => result,
        Err(Error::Graph { status, code }) => {
            if code.as_deref() == Some("GraphAccessToTranscriptsDisabled") {
                return (
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "Graph access to transcripts is disabled for this tenant" })),
                )
                    .into_response();
            }
            let (status, message) = match status {
                401 | 403 => (
                    StatusCode::FORBIDDEN,
                    "Your Microsoft account cannot read this meeting's transcripts",
                ),
                404 => (
                    StatusCode::NOT_FOUND,
                    "This meeting or transcript is not available to your Microsoft account",
                ),
                _ => (
                    StatusCode::BAD_GATEWAY,
                    "Microsoft could not complete this meeting request",
                ),
            };
            return (status, Json(json!({ "error": message }))).into_response();
        }
        Err(error) => {
            return (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": error.to_string() })),
            )
                .into_response()
        }
    };
    let graph_id = match resolve_primary_graph(
        &pool,
        user_ctx.account_id,
        plane_of(&tenant),
        IngestCaller::Attributable(user_ctx.user_id),
    )
    .await
    {
        Ok(graph_id) => graph_id,
        Err(error) => {
            tracing::error!(%error, "targeted transcript: resolve private graph failed");
            return internal_error();
        }
    };
    let count = fetched.items.len();
    let mut ingested = 0usize;
    for item in fetched.items {
        if ingest_message_stored(
            &state,
            user_ctx.account_id,
            user_ctx.user_id,
            graph_id,
            item,
        )
        .await
        {
            ingested += 1;
        }
    }
    (
        StatusCode::OK,
        Json(json!({ "meeting_id": fetched.meeting_id, "fetched": count, "ingested": ingested })),
    )
        .into_response()
}

/// Broad calendar transcript discovery is retired: an exact meeting action is required.
pub async fn sync_transcripts(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    let _ = (state, tenant, user_ctx);
    (StatusCode::GONE,Json(json!({"error":"Select a specific meeting and explicitly enable capture. Calendar and recording do not authorize transcript capture."}))).into_response()
}

// ── Watched list + opt-out ────────────────────────────────────────────────────

#[derive(Serialize, sqlx::FromRow)]
struct WatchedDto {
    id: Uuid,
    kind: String,
    team_id: String,
    channel_id: String,
    chat_id: String,
    display_name: String,
    created_at: DateTime<Utc>,
    /// `"delayed"` when this container's shared doorbell is `polling`
    /// (slot taken, or no eligible owner). Absent while push is live.
    capture: Option<String>,
}

/// `GET /v1/connectors/microsoft/teams/watched`
pub async fn list_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    // `begin_tenant_scoped`, not plain `begin_account_scoped` (m365 Train 2
    // PR 2.5 — see `ingest_resource`'s watch-insert comment above for the
    // full `tenant_rls_visible_graph_ids()`/tenant-rls-cutover-runbook.md
    // §2.6 argument): this handler always has a real acting end user, and
    // the extra GUCs are inert on the Personal path.
    let mut tx = match crate::tenancy::scope::begin_tenant_scoped(
        &pool,
        user_ctx.account_id,
        user_ctx.user_id,
    )
    .await
    {
        Ok(tx) => tx,
        Err(e) => {
            tracing::error!(error = %e, "teams: failed to open scoped tx");
            return internal_error();
        }
    };
    let rows: Result<Vec<WatchedDto>, _> = sqlx::query_as(
        r#"
        SELECT id, kind, team_id, channel_id, chat_id, display_name, created_at,
               NULL::text AS capture
        FROM teams_watched_resources
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
        Ok(mut watched) => {
            if let Some(control) = state.control_db() {
                apply_watch_capture(control, user_ctx.account_id, user_ctx.user_id, &mut watched)
                    .await;
            }
            (StatusCode::OK, Json(json!({ "watched": watched }))).into_response()
        }
        Err(e) => {
            tracing::error!(error = %e, "teams: list watched failed");
            internal_error()
        }
    }
}

/// `"delayed"` when the shared doorbell for this watch is polling.
/// One control-plane query for every watched resource of this caller.
async fn apply_watch_capture(
    control: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
    watched: &mut [WatchedDto],
) {
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT w.resource, s.state \
         FROM teams_container_subscriptions s \
         JOIN teams_container_watchers w \
           ON w.ms_tenant_id = s.ms_tenant_id AND w.resource = s.resource \
         WHERE w.account_id = $1 AND w.user_id = $2",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_all(control)
    .await
    .unwrap_or_default();
    let by_resource: HashMap<String, String> = rows.into_iter().collect();
    for row in watched {
        let Some(resource) = microsoft::teams_subscription_resource(
            &row.kind,
            &row.team_id,
            &row.channel_id,
            &row.chat_id,
        ) else {
            continue;
        };
        if by_resource.get(&resource).map(String::as_str) == Some("polling") {
            row.capture = Some("delayed".to_string());
        }
    }
}

/// `DELETE /v1/connectors/microsoft/teams/watched/{id}` — stop ingesting a
/// resource. Already-ingested corals are left intact.
///
/// **Org-admin only for the pilot** (CodeRabbit review on PR #668, founder
/// decision — same reasoning as `install_resource`'s own comment: a broad,
/// org-wide mutation restricted for the pilot, widened past admin-only in
/// Train 3). Personal accounts unaffected.
///
/// CS-452: this is the "a member un-watches a Teams container" disconnect
/// path CS-452's own brief names — before this fix it deleted ONLY the
/// `teams_watched_resources` row: the matching `graph_subscriptions` row
/// (and the live Graph subscription itself) survived untouched, so the
/// container kept being ingested via an orphaned subscription indefinitely
/// after "stop ingesting" reported success. `WHERE id = $1 AND account_id =
/// $2 AND user_id = $3` already scoped the delete to EXACTLY the caller's
/// own row (never the `owner_user_id IS NULL OR = $n` fallback shape —
/// `teams_watched_resources` has no such fallback to begin with, one row per
/// container, owner is a plain equality match), so no owner-correctness
/// change was needed here — only the missing subscription cleanup.
pub async fn delete_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    if let Some(reject) = reject_org_teams_access(&state, &tenant).await {
        return reject;
    }
    if let Some(reject) = crate::routes::oauth::reject_org_slack_non_admin(&tenant, &user_ctx.role)
    {
        return reject;
    }
    let Some(pool) = tenant.pool.clone() else {
        return service_unavailable();
    };
    // `begin_tenant_scoped`, not plain `begin_account_scoped` (m365 Train 2
    // PR 2.5 — see `ingest_resource`'s watch-insert comment for the full
    // argument): a real acting end user, extra GUCs inert on Personal.
    //
    // `RETURNING kind, team_id, channel_id, chat_id` (CS-452): the columns
    // needed to compute this row's `graph_subscriptions` resource string
    // below — a plain `sqlx::query` (the pre-CS-452 shape) only gave back a
    // row count, not enough to know WHICH subscription to cancel.
    let res = match crate::tenancy::scope::begin_tenant_scoped(
        &pool,
        user_ctx.account_id,
        user_ctx.user_id,
    )
    .await
    {
        Ok(mut tx) => {
            let r: Result<Option<(String, String, String, String, String)>, sqlx::Error> =
                sqlx::query_as(
                    "SELECT kind, team_id, channel_id, chat_id, auth_mode \
                 FROM teams_watched_resources \
                 WHERE id = $1 AND account_id = $2 AND user_id = $3",
                )
                .bind(id)
                .bind(user_ctx.account_id)
                .bind(user_ctx.user_id)
                .fetch_optional(&mut *tx)
                .await;
            match r {
                Ok(row) => tx.commit().await.map(|_| row),
                Err(e) => Err(e),
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "teams: failed to open scoped tx");
            return internal_error();
        }
    };
    let removed = match res {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(e) => {
            tracing::error!(error = %e, "teams: delete watched failed");
            return internal_error();
        }
    };

    // CS-452 / S5a: the LAST watcher deletes the upstream doorbell on their
    // token BEFORE the watch row is removed. A non-owner removes only their
    // watcher routing row. The content-plane `teams_watched_resources` row
    // is deleted after that attempt, whether or not Graph accepted it.
    let (kind, team_id, channel_id, chat_id, auth_mode) = &removed;
    if auth_mode == "delegated" {
        if let Some(resource) =
            microsoft::teams_subscription_resource(kind, team_id, channel_id, chat_id)
        {
            if let Some(control) = state.control_db() {
                match crate::crypto::DekPlane::resolve_for_account(
                    &state,
                    control,
                    user_ctx.account_id,
                )
                .await
                {
                    Ok(dek_plane) => {
                        let dek = dek_plane.as_source();
                        if let Err(e) = crate::connectors::teams_doorbell::unwatch_delegated(
                            &state.http_client,
                            control,
                            &dek,
                            &resource,
                            user_ctx.account_id,
                            user_ctx.user_id,
                        )
                        .await
                        {
                            tracing::info!(
                                error = %e,
                                account_id = %user_ctx.account_id,
                                %resource,
                                "teams: doorbell unwatch failed"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::info!(error = %e, "teams: doorbell unwatch skipped — DEK plane unresolved");
                    }
                }
            }
        }
    }
    if let Some(resource) =
        microsoft::teams_subscription_resource(kind, team_id, channel_id, chat_id)
    {
        // Delegated org-plane watches create Member-owned Graph
        // subscriptions; Personal / account-wide and app-owned rows stay
        // `owner_user_id IS NULL`. Token load is best-effort.
        let token_and_owner = microsoft_token(&state, user_ctx.account_id, user_ctx.user_id)
            .await
            .ok();
        let owner_user_id = match (
            auth_mode.as_str(),
            token_and_owner.as_ref().map(|(_, o)| *o),
        ) {
            ("delegated", Some(TokenOwner::Member(uid))) => Some(uid),
            ("delegated", Some(TokenOwner::AccountWide)) => None,
            ("delegated", None) => match state.control_db() {
                // Token load failed — only org writes Member-owned subs.
                Some(control) => {
                    match crate::routes::slack::account_plane(control, user_ctx.account_id).await {
                        Ok(AccountPlane::Org) => Some(user_ctx.user_id),
                        _ => None,
                    }
                }
                None => None,
            },
            _ => None,
        };
        let delegated_token = token_and_owner.as_ref().map(|(t, _)| t.as_str());
        if let Err(e) = microsoft::delete_subscription_for(
            &state.http_client,
            &pool,
            user_ctx.account_id,
            &resource,
            owner_user_id,
            delegated_token,
        )
        .await
        {
            tracing::info!(
                error = %e,
                account_id = %user_ctx.account_id,
                %resource,
                "teams: delete_watched subscription cancel failed"
            );
        }
    }

    match crate::tenancy::scope::begin_tenant_scoped(&pool, user_ctx.account_id, user_ctx.user_id)
        .await
    {
        Ok(mut tx) => {
            if let Err(e) = sqlx::query(
                "DELETE FROM teams_watched_resources \
                 WHERE id = $1 AND account_id = $2 AND user_id = $3",
            )
            .bind(id)
            .bind(user_ctx.account_id)
            .bind(user_ctx.user_id)
            .execute(&mut *tx)
            .await
            {
                tracing::error!(error = %e, "teams: delete watched row failed after doorbell unwatch");
                return internal_error();
            }
            if let Err(e) = tx.commit().await {
                tracing::error!(error = %e, "teams: delete watched commit failed");
                return internal_error();
            }
        }
        Err(e) => {
            tracing::error!(error = %e, "teams: failed to open scoped tx for watch delete");
            return internal_error();
        }
    }

    (StatusCode::OK, Json(json!({ "ok": true }))).into_response()
}

// ── Shared helpers ──────────────────────────────────────────────────────────────

/// Load + refresh the caller's delegated Microsoft token, returning the access
/// token or a ready error response.
///
/// **Always resolves `state.control_db()` itself — never takes a caller pool**
/// (m365 Train 2 PR 2.5 cross-plane read sweep). `oauth_tokens` is a
/// CONTROL-plane-only table (Foundation train invariant: creds live on the
/// control pool regardless of which plane the content lives on); this used
/// to take the caller's `Tenant`-resolved `pool` and pass it straight through
/// to `load_creds_bg`/`store_token_bundle`, which was harmless pre-2.5 only
/// because `tenant.pool` was ALWAYS the consumer pool on the sole reachable
/// (Personal) path. Once the org gate lifts, `pool` can genuinely be a
/// tenant database — `oauth_tokens` does not exist there at all, so the read
/// would hard-error instead of the clean 404 this function is supposed to
/// give for "not connected". Same bug CLASS as Slack's step-5a sweep (#492)
/// found in `ingest_slack_message`'s `slack_integrations` read.
///
/// **Owner (CS-456 / S4):** Org plane loads `TokenOwner::Member(user_id)` —
/// every Teams READ uses the acting member's own grant. Personal plane keeps
/// `TokenOwner::AccountWide` (the account's one member). Plane is derived via
/// [`account_plane`] on the control pool, the same helper every other
/// Teams/Slack path already uses.
pub(crate) async fn microsoft_token(
    state: &Arc<AppState>,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<(String, TokenOwner), Response> {
    microsoft_token_for(state, account_id, user_id, None).await
}

/// [`microsoft_token`], optionally pinned to the Microsoft directory the verified
/// capture identity named. The loaded credential must be that exact grant; a
/// different tenant is refused as disconnected before any refresh or provider call.
pub(crate) async fn microsoft_token_for(
    state: &Arc<AppState>,
    account_id: Uuid,
    user_id: Uuid,
    expected_workspace: Option<&str>,
) -> Result<(String, TokenOwner), Response> {
    let Some(control_pool) = state.control_db() else {
        return Err(service_unavailable());
    };
    // The ROW is on `control_pool`; the KEY may not be. This funnel is
    // reachable under an org `TenancyDecision` (the m365 carve-out), and this
    // helper takes only `account_id`, so the plane is resolved from the
    // account through THE decision table rather than pinned to consumer.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, %account_id, "teams: could not resolve the account's DEK plane");
            service_unavailable()
        })?;
    let owner = match crate::routes::slack::account_plane(control_pool, account_id).await {
        Ok(AccountPlane::Org) => TokenOwner::Member(user_id),
        Ok(AccountPlane::Personal) => TokenOwner::AccountWide,
        Err(e) => {
            tracing::error!(error = %e, %account_id, "teams: could not resolve account plane for token owner");
            return Err(internal_error());
        }
    };
    let mut creds =
        match load_creds_bg(control_pool, &dek_plane, account_id, "microsoft", owner).await {
            Ok(Some(c)) => c,
            Ok(None) => {
                return Err((
                    StatusCode::NOT_FOUND,
                    Json(json!({ "error": "Microsoft is not connected" })),
                )
                    .into_response())
            }
            Err(e) => {
                tracing::error!(error = %e, %account_id, "teams: load creds failed");
                return Err(internal_error());
            }
        };
    if expected_workspace
        .is_some_and(|expected| !creds.provider_workspace_id.eq_ignore_ascii_case(expected))
    {
        return Err((
            StatusCode::FORBIDDEN,
            Json(json!({"code":"connect_required","error":"Connect your own Microsoft account first"})),
        )
            .into_response());
    }
    let connector = MicrosoftConnector;
    if let Err(e) = connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
    {
        // CS-453: this exact `(account_id, "microsoft", owner)` row is
        // the one just above resolved credentials from — durably record a
        // `CredentialRevoked` here before mapping to the HTTP response, same
        // as `routes::connectors::trigger_sync`/`ingest_selected`'s
        // identical arms.
        credential_health::record_if_revoked(control_pool, account_id, owner, &e).await;
        return Err(connector_err(e));
    }
    if let Some(refreshed) = creds.refreshed.as_ref() {
        let _ = store_token_bundle(
            control_pool,
            &dek_plane,
            account_id,
            "microsoft",
            owner,
            refreshed,
        )
        .await;
    }
    Ok((creds.access_token, owner))
}

/// Graph-subscription `owner_user_id` for a Teams watch row: Member only
/// for delegated org-plane grants; Personal / app-owned stay `None`.
fn teams_subscription_owner_user_id(auth_mode: &str, owner: Option<TokenOwner>) -> Option<Uuid> {
    if auth_mode != "delegated" {
        return None;
    }
    match owner {
        Some(TokenOwner::Member(uid)) => Some(uid),
        Some(TokenOwner::AccountWide) | None => None,
    }
}

/// Per-member delegated watch upsert (CS-456 / S4). Unique key is
/// `(account_id, kind, team_id, channel_id, chat_id, user_id)` for
/// `auth_mode = 'delegated'`. A different member inserting the same
/// channel gets their own row; a same-member re-opt-in updates
/// `graph_id` / `display_name` and does NOT reset `backfill_complete`.
///
/// `backfill_complete = FALSE` on a fresh insert — a brand-new watch has
/// not been backfilled yet. Left untouched on conflict so an existing
/// cursor / completion state carries over.
pub(crate) async fn upsert_delegated_watch(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    account_id: Uuid,
    user_id: Uuid,
    graph_id: Uuid,
    kind: &str,
    team_id: &str,
    channel_id: &str,
    chat_id: &str,
    display_name: &str,
) -> sqlx::Result<Uuid> {
    sqlx::query_scalar(
        r#"
        INSERT INTO teams_watched_resources
            (account_id, user_id, graph_id, kind, team_id, channel_id, chat_id, display_name, last_ingested_at, backfill_complete)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now(), FALSE)
        ON CONFLICT (account_id, kind, team_id, channel_id, chat_id, user_id)
            WHERE auth_mode = 'delegated'
        DO UPDATE SET graph_id = EXCLUDED.graph_id,
                      display_name = EXCLUDED.display_name,
                      last_ingested_at = now()
        RETURNING id
        "#,
    )
    .bind(account_id)
    .bind(user_id)
    .bind(graph_id)
    .bind(kind)
    .bind(team_id)
    .bind(channel_id)
    .bind(chat_id)
    .bind(display_name)
    .fetch_one(&mut **tx)
    .await
}

/// Best-effort AccountWide Microsoft access token — bot/remove paths with no
/// acting member. Returns `None` on org plane (Member-owned only) or any load
/// failure; callers treat that as the CS-452 "skip upstream delete" arm.
async fn microsoft_token_account_wide(state: &Arc<AppState>, account_id: Uuid) -> Option<String> {
    let control_pool = state.control_db()?;
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, control_pool, account_id)
        .await
        .ok()?;
    let mut creds = load_creds_bg(
        control_pool,
        &dek_plane,
        account_id,
        "microsoft",
        TokenOwner::AccountWide,
    )
    .await
    .ok()
    .flatten()?;
    let connector = MicrosoftConnector;
    connector
        .refresh_if_needed(&state.http_client, &mut creds)
        .await
        .ok()?;
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
    Some(creds.access_token)
}

fn connector_err(e: ConnectorError) -> Response {
    match e {
        ConnectorError::NotConfigured(msg) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "error": msg })),
        )
            .into_response(),
        ConnectorError::NotImplemented(msg) => {
            (StatusCode::NOT_IMPLEMENTED, Json(json!({ "error": msg }))).into_response()
        }
        ConnectorError::Exchange(msg) | ConnectorError::UpstreamStatus { message: msg, .. } => {
            tracing::warn!(error = %msg, "teams connector upstream failure");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({ "error": "failed to reach Microsoft" })),
            )
                .into_response()
        }
        ConnectorError::Internal(msg) => {
            tracing::error!(error = %msg, "teams connector internal failure");
            internal_error()
        }
        ConnectorError::CredentialRevoked { provider, reason } => {
            // CS-453: same `409` mapping as `routes::connectors`'s own
            // `connector_error_response` — see that one's doc comment for
            // the reasoning. `microsoft_token` above already records
            // `credential_invalid_at` before this is reached.
            tracing::warn!(%provider, reason = reason.code(), "teams connector credential revoked");
            (
                StatusCode::CONFLICT,
                Json(json!({ "error": "credential revoked", "reason": reason.code() })),
            )
                .into_response()
        }
    }
}

fn recording_off() -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({"code":"recording_off","error":"Turn on recording or transcription in Teams, then add this meeting"})),
    )
        .into_response()
}

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({ "error": msg }))).into_response()
}
/// m365 Train 2 PR 2.5 (CodeRabbit review, cross-member watch-conflict fix).
fn conflict(msg: &str) -> Response {
    (StatusCode::CONFLICT, Json(json!({ "error": msg }))).into_response()
}

/// Prefix `install_watch` stamps on the `ConnectorError::Internal` it returns
/// when its cross-member conflict guard fires (m365 Train 2 PR 2.5,
/// CodeRabbit review — see that call site's own comment for the full
/// argument). Deliberately not a new `ConnectorError` variant: that enum is
/// shared across every connector's HTTP-mapping `connector_err()`-style
/// function in this crate, and widening it for one narrow, single-caller
/// case is not the "cheap variant" fix this is meant to be. `is_watch_
/// conflict` is the one place this string is interpreted.
const WATCH_CONFLICT_MARKER: &str = "watch conflict: ";

/// Whether `e` is `install_watch`'s cross-member conflict refusal (as
/// opposed to a genuine internal failure) — see [`WATCH_CONFLICT_MARKER`].
fn is_watch_conflict(e: &ConnectorError) -> bool {
    matches!(e, ConnectorError::Internal(msg) if msg.starts_with(WATCH_CONFLICT_MARKER))
}
fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({ "error": "not found" }))).into_response()
}
fn internal_error() -> Response {
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({ "error": "internal error" })),
    )
        .into_response()
}
fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({ "error": "service unavailable" })),
    )
        .into_response()
}

#[cfg(test)]
mod pg_test_support {
    //! Shared scratch-DB scaffolding for this file's `*_pg_tests` modules
    //! (they were four independent byte-identical copies before the PR 2.6
    //! follow-up consolidated them — a fix to any helper had to land in every
    //! copy, and a missed one only failed under `ORGT_TEST_BASE_URL`, which
    //! the plain workspace cargo-test job never sets).

    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::sync::Arc;
    use uuid::Uuid;

    use crate::AppState;

    pub(super) fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    pub(super) fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    pub(super) async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    // No literal `state.db = …`/`state.store = …` here — this file lives
    // under `backend/src/routes/`, which `scripts/check_no_ambient_pool.sh`
    // greps unconditionally (test modules included). `test_app_state_owned`
    // (crate-root helper) leaves `db`/`store` `None`, which is exactly what
    // the callers want: `install_watch` never touches `state.db`/`state.store`
    // at all (its content pool is the explicit `pool` parameter; its ONE
    // optional control-plane touch, the `connector_installs` upsert, is
    // gated on `state.control_db()` and already tolerates `None`).
    pub(super) fn state_no_control() -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned(None))
    }

    /// A `kind='member'` primary ocean for `owner_user_id`, the precondition
    /// `resolve_primary_graph`'s Org+Attributable branch requires (it never
    /// auto-creates — see that function's own doc comment).
    pub(super) async fn seed_member_graph(
        pool: &PgPool,
        account_id: Uuid,
        owner_user_id: Uuid,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }
}

#[cfg(test)]
mod org_microsoft_teams_flag_pg_tests {
    //! m365 Train 2 PR 2.5 (gate lift). Proves [`super::org_microsoft_teams_
    //! enabled_for`]'s two load-bearing claims against a REAL control
    //! database:
    //!
    //! 1. Its registry provider (`"microsoft"`) resolves genuinely
    //!    independently of the pre-existing `"teams"` provider row
    //!    (`routes::teams::org_teams_active_for`'s CS-094 named-teams gate,
    //!    an unrelated feature) for the SAME org — the exact collision this
    //!    PR's own doc comment on that function documents at length. A
    //!    regression here would mean an operator's Microsoft-Teams pilot
    //!    flip on one org silently also touched (or was silently gated by)
    //!    CS-094's `MEMBER_OCEANS_ENABLED` coupling.
    //! 2. A missing row falls back to the env default (unset in this test
    //!    process ⇒ `false`), proving a zero-row `org_connector_flags`
    //!    registry is byte-identical to pre-PR-2.5's unconditional org
    //!    refusal — [`super::reject_org_teams_access`]'s own contract.
    //!
    //! `connector_flags::resolve`'s pure resolution-order logic
    //! (`resolve_flag`) already has its own unit tests in
    //! `connector_flags.rs`; this module proves the SQL/integration side —
    //! the actual `org_connector_flags` row shape the admin setter writes,
    //! read back through this PR's real call site.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) because `org_microsoft_teams_
    //! enabled_for` is `pub(crate)`. Gated on `ORGT_TEST_BASE_URL`, same
    //! scratch-DB convention as every other `*_pg` suite in this tree:
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib org_microsoft_teams_flag_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool};
    use std::sync::Arc;

    use super::org_microsoft_teams_enabled_for;
    use super::pg_test_support::{base_url, recreate, with_db};
    use crate::AppState;

    // `AppState` fixture built via the crate-root test helper, never a
    // literal `state.db = …` in this file — `scripts/check_no_ambient_pool.sh`
    // forbids that under `backend/src/routes/` even inside `#[cfg(test)]`
    // (see `test_app_state_owned_with_db_and_store`'s own doc comment).
    fn state_with_control(pool: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }

    async fn set_flag(pool: &PgPool, org_id: &str, provider: &str, enabled: bool) {
        sqlx::query(
            "INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) VALUES ($1, $2, $3)",
        )
        .bind(org_id)
        .bind(provider)
        .bind(enabled)
        .execute(pool)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn microsoft_provider_row_is_independent_of_a_teams_provider_row_for_the_same_org_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping microsoft_provider_row_is_independent_of_a_teams_provider_row_for_the_\
                 same_org_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t25_microsoft_teams_flag_isolation";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let org_id = "org_t25_flag_isolation_test";
        // CS-094 named teams flag OFF for this org (a wholly unrelated
        // feature, `routes::teams::org_teams_active_for`) ...
        set_flag(&pool, org_id, "teams", false).await;
        // ... while the Microsoft Teams CONNECTOR flag is ON for the SAME
        // org — must resolve independently, proving no accidental provider
        // collision between the two.
        set_flag(&pool, org_id, "microsoft", true).await;

        let state = state_with_control(pool.clone());
        assert!(
            org_microsoft_teams_enabled_for(&state, org_id).await,
            "the 'microsoft' provider row must resolve ON regardless of the 'teams' provider \
             row (a different feature entirely) resolving OFF for the SAME org"
        );

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn missing_row_falls_back_to_env_default_off_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping missing_row_falls_back_to_env_default_off_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t25_microsoft_teams_flag_env_default";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let state = state_with_control(pool.clone());
        // No row at all for this org: must fall back to `org_microsoft_teams_
        // enabled()` (unset in this test process ⇒ `false`) — proving a
        // zero-row registry is byte-identical to pre-PR-2.5's unconditional
        // org refusal (`reject_org_teams_access`'s whole point).
        assert!(
            !org_microsoft_teams_enabled_for(&state, "org_t25_no_row_at_all").await,
            "a missing row must fall back to the env default (off/unset in this test process) — \
             byte-identical to pre-PR-2.5's unconditional refusal"
        );

        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod watch_conflict_pg_tests {
    //! App-mode RSC installs stay one-row-per-container (S4 / item 47a keeps
    //! that path; item 48 retires it later). A SECOND org member installing
    //! the exact same channel/chat via [`super::install_watch`] must still be
    //! refused ([`super::is_watch_conflict`]), and the FIRST member's row
    //! (`user_id`/`graph_id`) must be left unchanged.
    //!
    //! Delegated per-member watches (the S4 capture model) use a different
    //! unique index — see `ingest_resource` and migration 0253/0104.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) because `install_watch`/
    //! `is_watch_conflict` are `pub(crate)`/private. Migrates ONLY
    //! `migrations_tenant` — `install_watch`'s write path
    //! (`teams_watched_resources`/`graphs`) is entirely tenant-plane, and
    //! `state.control_db()` is deliberately left `None` (no control DB
    //! needed): the `connector_installs` upsert inside `install_watch` is
    //! best-effort and already tolerates a missing control pool with a
    //! logged warning, not a hard failure.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib watch_conflict_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool};
    use uuid::Uuid;

    use super::pg_test_support::{
        base_url, recreate, seed_member_graph, state_no_control, with_db,
    };
    use super::{install_watch, is_watch_conflict, AccountPlane, IngestCaller};

    #[tokio::test]
    async fn a_different_member_installing_the_same_channel_is_refused_not_reassigned_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_different_member_installing_the_same_channel_is_refused_not_\
                 reassigned_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t25_watch_conflict";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_watch_conflict_test')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let account_id = Uuid::new_v4();
        let member_a = Uuid::new_v4();
        let member_b = Uuid::new_v4();
        let graph_a = seed_member_graph(&pool, account_id, member_a).await;
        let _graph_b = seed_member_graph(&pool, account_id, member_b).await;

        let state = state_no_control();

        // Member A installs the channel first — a fresh insert, must succeed.
        let (watch_id, inserted) = install_watch(
            &state,
            &pool,
            account_id,
            member_a,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General",
            Some(0),
        )
        .await
        .expect("member A's first install must succeed");
        assert!(inserted, "member A's install must be a fresh insert");

        // Member B tries to install the EXACT SAME channel — must be
        // refused, never silently repoint the row at B's ocean.
        let err = install_watch(
            &state,
            &pool,
            account_id,
            member_b,
            AccountPlane::Org,
            IngestCaller::Attributable(member_b),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General",
            Some(0),
        )
        .await
        .expect_err("member B's conflicting install must be refused, not silently applied");
        assert!(
            is_watch_conflict(&err),
            "must be the cross-member conflict refusal specifically, got: {err}"
        );

        // The FIRST member's row must be completely unchanged — this is the
        // actual regression the pre-fix code had: `graph_id` silently
        // repointed at B's ocean while `user_id` stayed A's.
        let (stored_id, stored_user, stored_graph): (Uuid, Uuid, Uuid) = sqlx::query_as(
            "SELECT id, user_id, graph_id FROM teams_watched_resources \
             WHERE account_id = $1 AND kind = 'channel' AND channel_id = $2",
        )
        .bind(account_id)
        .bind("chan-1")
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            stored_id, watch_id,
            "still the same row — no second row was created"
        );
        assert_eq!(
            stored_user, member_a,
            "user_id must still name member A — must NOT be silently reassigned to B"
        );
        assert_eq!(
            stored_graph, graph_a,
            "graph_id must still point at A's ocean — must NOT be repointed at B's"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Positive control: the SAME member re-triggering install (e.g.
    /// reconfiguring the backfill window) must still update normally — the
    /// `WHERE user_id = EXCLUDED.user_id` guard must not turn a legitimate
    /// same-member update into a spurious conflict.
    #[tokio::test]
    async fn the_same_member_reinstalling_updates_normally_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping the_same_member_reinstalling_updates_normally_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t25_watch_same_member_reinstall";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_watch_same_member_test')",
        )
        .execute(&pool)
        .await
        .unwrap();

        let account_id = Uuid::new_v4();
        let member_a = Uuid::new_v4();
        let graph_a = seed_member_graph(&pool, account_id, member_a).await;

        let state = state_no_control();

        let (first_id, first_inserted) = install_watch(
            &state,
            &pool,
            account_id,
            member_a,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General",
            Some(0),
        )
        .await
        .expect("first install must succeed");
        assert!(first_inserted);

        let (second_id, second_inserted) = install_watch(
            &state,
            &pool,
            account_id,
            member_a,
            AccountPlane::Org,
            IngestCaller::Attributable(member_a),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General (renamed)",
            Some(0),
        )
        .await
        .expect("the SAME member's re-install must update, not conflict");
        assert_eq!(second_id, first_id, "same row, conflict-updated");
        assert!(
            !second_inserted,
            "the second call is an UPDATE, not a fresh INSERT"
        );

        let (stored_name, stored_graph): (String, Uuid) = sqlx::query_as(
            "SELECT display_name, graph_id FROM teams_watched_resources WHERE id = $1",
        )
        .bind(first_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            stored_name, "General (renamed)",
            "same-member update must actually apply"
        );
        assert_eq!(stored_graph, graph_a, "still the same member's ocean");

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// S4: two org members may each hold a *delegated* watch on the same
    /// channel — the unique key includes `user_id` for `auth_mode =
    /// 'delegated'`. Same-member re-opt-in updates without resetting
    /// `backfill_complete`.
    #[tokio::test]
    async fn two_members_can_each_hold_a_delegated_watch_on_the_same_channel_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping two_members_can_each_hold_a_delegated_watch_on_the_same_channel_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_s4_delegated_per_member";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_4 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_s4_delegated')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let member_a = Uuid::new_v4();
        let member_b = Uuid::new_v4();
        let graph_a = seed_member_graph(&pool, account_id, member_a).await;
        let graph_b = seed_member_graph(&pool, account_id, member_b).await;

        // Same upsert `ingest_resource` uses (ON CONFLICT includes user_id).
        async fn delegated_upsert(
            pool: &PgPool,
            account_id: Uuid,
            user_id: Uuid,
            graph_id: Uuid,
        ) -> Uuid {
            let mut tx = crate::tenancy::scope::begin_tenant_scoped(pool, account_id, user_id)
                .await
                .expect("scoped tx");
            let id = super::upsert_delegated_watch(
                &mut tx, account_id, user_id, graph_id, "channel", "team-1", "chan-1", "",
                "General",
            )
            .await
            .expect("delegated insert");
            tx.commit().await.expect("commit");
            id
        }

        let id_a = delegated_upsert(&pool, account_id, member_a, graph_a).await;
        let id_b = delegated_upsert(&pool, account_id, member_b, graph_b).await;
        assert_ne!(id_a, id_b, "each member must get their own watch row");

        let count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_watched_resources \
             WHERE account_id = $1 AND channel_id = 'chan-1' AND auth_mode = 'delegated'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count, 2);

        // Same-member re-opt-in must not reset backfill_complete.
        sqlx::query("UPDATE teams_watched_resources SET backfill_complete = TRUE WHERE id = $1")
            .bind(id_a)
            .execute(&pool)
            .await
            .unwrap();
        let id_a2 = delegated_upsert(&pool, account_id, member_a, graph_a).await;
        assert_eq!(id_a2, id_a);
        let complete: bool = sqlx::query_scalar(
            "SELECT backfill_complete FROM teams_watched_resources WHERE id = $1",
        )
        .bind(id_a)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            complete,
            "same-member re-opt-in must not reset backfill_complete"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod history_choice_backfill_pg_tests {
    //! m365 Train 2 PR 2.6 CodeRabbit round 2: regression tests for
    //! [`super::claim_history_choice`]'s `backfill_complete`/`backfill_cursor`
    //! transition against a REAL tenant database. `install_watch`'s `Some(0)`
    //! deferred-choice path seeds a watch with `backfill_complete = TRUE` (see
    //! that function's own comment) so `pending_teams_backfills_for_account`
    //! ignores it until a window is chosen; before this fix, claiming a real
    //! ("all"/"N days") choice left that seed in place, so a later
    //! `backfill_history` failure (app-token mint, Graph fetch, or a partial
    //! ingest) had no retry path — `tick_backfill_continue` would never revisit
    //! the row. These prove the claim itself now flips the row back to pending
    //! atomically, and that a `none` choice (no backfill work exists) does not.
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) because `install_watch`/
    //! `claim_history_choice` are `pub(crate)`. Migrates ONLY
    //! `migrations_tenant` — same reasoning as `watch_conflict_pg_tests`'s own
    //! doc comment above.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib history_choice_backfill_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool};
    use uuid::Uuid;

    use super::pg_test_support::{
        base_url, recreate, seed_member_graph, state_no_control, with_db,
    };
    use super::{claim_history_choice, install_watch, AccountPlane, IngestCaller};

    async fn backfill_state(pool: &PgPool, channel_id: &str) -> (bool, Option<String>) {
        sqlx::query_as(
            "SELECT backfill_complete, backfill_cursor FROM teams_watched_resources \
             WHERE channel_id = $1",
        )
        .bind(channel_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// The bug this PR fixes: claiming a REAL history-window choice (anything
    /// other than `none`) must flip the `Some(0)`-seeded `backfill_complete =
    /// TRUE` row back to `FALSE` (and clear its cursor) as part of the claim
    /// itself — BEFORE `backfill_history` is ever called — so the row is
    /// already retryable by `tick_backfill_continue` no matter how the
    /// caller's subsequent Graph work turns out.
    #[tokio::test]
    async fn claiming_a_real_choice_marks_the_row_pending_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping claiming_a_real_choice_marks_the_row_pending_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t26_history_choice_marks_pending";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_5 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t26_history_choice')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        seed_member_graph(&pool, account_id, member).await;
        let state = state_no_control();

        // `Some(0)`: the bot's deferred-choice install — seeds `backfill_complete
        // = TRUE`, no cursor, `history_choice` NULL (see `install_watch`'s own
        // comment).
        install_watch(
            &state,
            &pool,
            account_id,
            member,
            AccountPlane::Org,
            IngestCaller::Attributable(member),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General",
            Some(0),
        )
        .await
        .expect("deferred install must succeed");

        let (complete_before, cursor_before) = backfill_state(&pool, "chan-1").await;
        assert!(
            complete_before,
            "Some(0) seed must start backfill_complete = TRUE"
        );
        assert!(
            cursor_before.is_none(),
            "Some(0) seed must start with no cursor"
        );

        // A stale cursor from an earlier aborted backfill pass — the claim must
        // clear it, or the retry resumes mid-history from the wrong page. (The
        // seed itself starts with no cursor, so without this write the
        // cursor-clearing half of the claim UPDATE would go unexercised.)
        sqlx::query(
            "UPDATE teams_watched_resources SET backfill_cursor = 'stale-skiptoken' \
             WHERE channel_id = $1",
        )
        .bind("chan-1")
        .execute(&pool)
        .await
        .unwrap();

        // Claim a real window — must succeed and flip the row to pending in the
        // SAME atomic UPDATE as the claim.
        let claim = claim_history_choice(&pool, account_id, "channel", "chan-1", "", "7").await;
        assert!(
            claim.is_some(),
            "first claim on an unclaimed watch must succeed"
        );

        let (complete_after, cursor_after) = backfill_state(&pool, "chan-1").await;
        assert!(
            !complete_after,
            "claiming a real history window must flip backfill_complete back to FALSE so \
             tick_backfill_continue will retry this row even if backfill_history never gets \
             far enough to persist its own cursor"
        );
        assert!(
            cursor_after.is_none(),
            "claiming a real history window must clear a stale backfill_cursor so the retry \
             starts from the beginning of the chosen window, not mid-history from an aborted \
             earlier pass"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Negative control: a `none` choice means "no backfill work exists at
    /// all" — it must NOT flip `backfill_complete` to FALSE (there is nothing
    /// for `tick_backfill_continue` to retry).
    #[tokio::test]
    async fn claiming_none_leaves_backfill_complete_untouched_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping claiming_none_leaves_backfill_complete_untouched_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t26_history_choice_none_untouched";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_6 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&pool)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_t26_history_none')")
            .execute(&pool)
            .await
            .unwrap();

        let account_id = Uuid::new_v4();
        let member = Uuid::new_v4();
        seed_member_graph(&pool, account_id, member).await;
        let state = state_no_control();

        install_watch(
            &state,
            &pool,
            account_id,
            member,
            AccountPlane::Org,
            IngestCaller::Attributable(member),
            "channel",
            "team-1",
            "chan-1",
            "",
            "tenant-x",
            "General",
            Some(0),
        )
        .await
        .expect("deferred install must succeed");

        // Mirror of the stale-cursor write in the real-choice test: the `none`
        // branch of the claim UPDATE must leave the cursor exactly as it found
        // it (there is no backfill to restart, so nothing to clear).
        sqlx::query(
            "UPDATE teams_watched_resources SET backfill_cursor = 'stale-skiptoken' \
             WHERE channel_id = $1",
        )
        .bind("chan-1")
        .execute(&pool)
        .await
        .unwrap();

        let claim = claim_history_choice(&pool, account_id, "channel", "chan-1", "", "none").await;
        assert!(
            claim.is_some(),
            "first claim on an unclaimed watch must succeed"
        );

        let (complete_after, cursor_after) = backfill_state(&pool, "chan-1").await;
        assert!(
            complete_after,
            "a 'none' choice has no backfill work — backfill_complete must stay TRUE"
        );
        assert_eq!(
            cursor_after.as_deref(),
            Some("stale-skiptoken"),
            "a 'none' choice must leave backfill_cursor exactly as it found it"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod org_install_grace_rsc_watch_pg_tests {
    //! Item 48: `install_watch` is the RSC app-mode entry. While the grace
    //! is open it still inserts a watch. After the window it returns before
    //! writing one. Both use a control database so the grace row exists;
    //! the migration seed is not the instant under test.

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Executor, PgPool};
    use std::sync::Arc;
    use uuid::Uuid;

    use super::pg_test_support::{base_url, recreate, seed_member_graph, with_db};
    use super::{install_watch, AccountPlane, IngestCaller};
    use crate::services::org_install_grace::GRACE_CLOSED_INSTALL;

    fn state_with_control(pool: PgPool) -> Arc<crate::AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }

    async fn set_began_days_ago(pool: &PgPool, days_ago: i32) {
        sqlx::query(
            "UPDATE org_install_capture_grace SET connect_emails_began_at = now() - make_interval(days => $1)",
        )
        .bind(days_ago)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seed_owner(pool: &PgPool) -> (Uuid, Uuid) {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("grace-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(format!("org-grace-{}", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        (account_id, user_id)
    }

    #[tokio::test]
    async fn org_install_grace_rsc_install_watch_still_runs_before_the_window_closes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_install_grace_rsc_install_watch_still_runs_before_the_window_\
                 closes_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_grace_rsc_install_open";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_7 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        set_began_days_ago(&pool, 1).await;
        let (account_id, user_id) = seed_owner(&pool).await;
        let _graph = seed_member_graph(&pool, account_id, user_id).await;
        let state = state_with_control(pool.clone());
        let (watch_id, inserted) = install_watch(
            &state,
            &pool,
            account_id,
            user_id,
            AccountPlane::Org,
            IngestCaller::Attributable(user_id),
            "channel",
            "team-grace",
            "chan-grace",
            "",
            "tenant-grace",
            "General",
            Some(0),
        )
        .await
        .expect("inside the grace an RSC install must still be written");
        assert!(inserted);
        let stored: Option<Uuid> = sqlx::query_scalar(
            "SELECT id FROM teams_watched_resources WHERE id = $1 AND auth_mode = 'app'",
        )
        .bind(watch_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert_eq!(stored, Some(watch_id));
        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn org_install_grace_rsc_install_watch_is_unreachable_after_the_window_closes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_install_grace_rsc_install_watch_is_unreachable_after_the_window_\
                 closes_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_grace_rsc_install_closed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _database_cleanup_8 = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        set_began_days_ago(&pool, 31).await;
        let (account_id, user_id) = seed_owner(&pool).await;
        let _graph = seed_member_graph(&pool, account_id, user_id).await;
        let state = state_with_control(pool.clone());
        let err = install_watch(
            &state,
            &pool,
            account_id,
            user_id,
            AccountPlane::Org,
            IngestCaller::Attributable(user_id),
            "channel",
            "team-grace",
            "chan-grace",
            "",
            "tenant-grace",
            "General",
            Some(0),
        )
        .await
        .expect_err("after the grace an RSC install must not be written");
        assert!(
            err.to_string().contains(GRACE_CLOSED_INSTALL),
            "expected the grace refusal, got {err}"
        );
        let rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM teams_watched_resources WHERE account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            rows, 0,
            "a closed window must not leave an app-mode watch row"
        );
        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}
#[cfg(test)]
mod targeted_transcript_recovery_pg_tests {
    use super::observe_targeted_transcript_health;
    use crate::{
        connectors::teams_targeted_transcripts::{Error, Fetched},
        services::connector_health,
    };
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    #[tokio::test]
    async fn manual_transcript_recheck_recovers_only_verified_member_pg() {
        let Ok(base) = std::env::var("TEAMS_HEALTH_TEST_BASE_URL") else {
            eprintln!("skipping: set TEAMS_HEALTH_TEST_BASE_URL to disposable PostgreSQL");
            return;
        };
        let name = crate::test_db::TestDatabase::from_url(&base, "transcript_health");
        let mut root = PgConnection::connect(&base).await.unwrap();
        root.execute(format!("CREATE DATABASE {name}").as_str())
            .await
            .unwrap();
        let url = super::pg_test_support::with_db(&base, &name);
        let admin = PgPool::connect(&url).await.unwrap();
        admin.execute("CREATE TABLE accounts(id uuid PRIMARY KEY); CREATE TABLE users(id uuid PRIMARY KEY);").await.unwrap();
        sqlx::raw_sql(include_str!(
            "../../migrations_control/0243_connector_health.sql"
        ))
        .execute(&admin)
        .await
        .unwrap();
        sqlx::raw_sql(include_str!(
            "../../migrations_control/0254_connector_health_instance_key.sql"
        ))
        .execute(&admin)
        .await
        .unwrap();
        let role = name.role("transcript_health_app");
        let role_password = Uuid::new_v4().simple().to_string();
        root.execute(format!("CREATE ROLE {role} LOGIN NOBYPASSRLS PASSWORD '{role_password}'").as_str())
            .await
            .unwrap();
        admin.execute(format!("GRANT USAGE ON SCHEMA public TO {role}; GRANT SELECT,INSERT,UPDATE ON connector_health TO {role}").as_str()).await.unwrap();
        let mut app_url = reqwest::Url::parse(&url).unwrap();
        app_url.set_username(&role).unwrap();
        app_url.set_password(Some(&role_password)).unwrap();
        let app = PgPool::connect(app_url.as_str()).await.unwrap();
        let account = Uuid::new_v4();
        let other_account = Uuid::new_v4();
        let member = Uuid::new_v4();
        let sibling = Uuid::new_v4();
        sqlx::query("INSERT INTO accounts VALUES($1),($2)")
            .bind(account)
            .bind(other_account)
            .execute(&admin)
            .await
            .unwrap();
        sqlx::query("INSERT INTO users VALUES($1),($2)")
            .bind(member)
            .bind(sibling)
            .execute(&admin)
            .await
            .unwrap();
        let disabled = Err(Error::Graph {
            status: 403,
            code: Some("GraphAccessToTranscriptsDisabled".into()),
        });
        observe_targeted_transcript_health(&app, account, Some(member), &disabled).await;
        observe_targeted_transcript_health(&app, account, Some(sibling), &disabled).await;
        observe_targeted_transcript_health(&app, other_account, Some(member), &disabled).await;
        assert!(
            connector_health::transcript_access_disabled(&app, account, Some(member)).await,
            "background suppression remains active after a real policy refusal"
        );
        // A permission refusal is not evidence that transcript policy recovered.
        observe_targeted_transcript_health(
            &app,
            account,
            Some(member),
            &Err(Error::Graph {
                status: 403,
                code: Some("Forbidden".into()),
            }),
        )
        .await;
        assert!(connector_health::transcript_access_disabled(&app, account, Some(member)).await);
        // Explicit reads are observed despite cached suppression; empty successful
        // lists establish policy access just as a nonempty transcript list does.
        let empty = Ok(Fetched {
            meeting_id: "fixture".into(),
            items: vec![],
        });
        observe_targeted_transcript_health(&app, account, Some(member), &empty).await;
        assert!(!connector_health::transcript_access_disabled(&app, account, Some(member)).await);
        assert!(connector_health::transcript_access_disabled(&app, account, Some(sibling)).await);
        assert!(
            connector_health::transcript_access_disabled(&app, other_account, Some(member)).await
        );
        let resolved: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_health WHERE resolved_at IS NOT NULL",
        )
        .fetch_one(&admin)
        .await
        .unwrap();
        assert_eq!(resolved, 1);
        // A subsequent real disabled response reopens only this member's episode.
        observe_targeted_transcript_health(&app, account, Some(member), &disabled).await;
        assert!(connector_health::transcript_access_disabled(&app, account, Some(member)).await);
        app.close().await;
        admin.close().await;
        root.execute(format!("DROP DATABASE {name} WITH (FORCE)").as_str())
            .await
            .unwrap();
        root.execute(format!("DROP ROLE {role}").as_str())
            .await
            .unwrap();
    }
}

#[cfg(test)]
mod meeting_capture_return_tests {
    use super::*;
    #[tokio::test]
    async fn broad_sync_is_gone_without_accessing_a_calendar_or_database() {
        use tower::ServiceExt;
        let app = axum::Router::new()
            .route("/sync", axum::routing::post(sync_transcripts))
            .layer(Extension(UserContext {
                user_id: Uuid::new_v4(),
                account_id: Uuid::new_v4(),
                role: "member".into(),
                device_id: "fixture".into(),
            }))
            .with_state(crate::test_app_state());
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .method("POST")
                    .uri("/sync")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::GONE);
    }
    #[test]
    fn selected_status_bounds_match_provider_fraction_at_storage_precision_only() {
        let parse = |s: &str| {
            chrono::DateTime::parse_from_rfc3339(s)
                .unwrap()
                .with_timezone(&chrono::Utc)
        };
        let row = crate::services::teams_meeting_capture::CaptureStatus {
            capture_id: Uuid::new_v4(),
            meeting_id: "own-verified-meeting".into(),
            status: "stopped".into(),
            occurrence_start: parse("2026-10-02T04:00:00.123456Z"),
            occurrence_end: parse("2026-10-02T04:30:00.123456Z"),
            last_checked_at: None,
            last_retrieved_at: None,
            retrieved_transcript_count: 0,
            last_error_code: None,
            capture_generation: Uuid::new_v4(),
            graph_id: Uuid::new_v4(),
            transcript_end: None,
        };
        assert!(stored_occurrence_matches(
            &row,
            parse("2026-10-02T04:00:00.1234567Z"),
            parse("2026-10-02T04:30:00.1234567Z")
        ));
        assert!(!stored_occurrence_matches(
            &row,
            parse("2026-10-02T04:00:00.1234570Z"),
            row.occurrence_end
        ));
        assert!(!stored_occurrence_matches(
            &row,
            row.occurrence_start,
            parse("2026-10-02T04:30:00.1234570Z")
        ));
    }
    #[test]
    fn selected_status_accepts_one_context_and_rejects_mixed_or_untrusted_selectors() {
        for value in [
            json!({"capture_id":Uuid::new_v4()}),
            json!({"teams_meeting_id":"native-meeting","chat_id":"chat"}),
            json!({"join_url":"https://teams.microsoft.com/meet/123"}),
        ] {
            assert!(serde_json::from_value::<MeetingStatusSelector>(value)
                .unwrap()
                .target()
                .is_ok());
        }
        for value in [
            json!({}),
            json!({"capture_id":Uuid::new_v4(),"join_url":"https://teams.microsoft.com/meet/123"}),
            json!({"teams_meeting_id":"native-meeting"}),
            json!({"teams_meeting_id":"native-meeting","chat_id":"chat","join_url":"https://teams.microsoft.com/meet/123"}),
            json!({"teams_meeting_id":"private\nvalue","chat_id":"chat"}),
            json!({"join_url":"https://evil.example/meet/123"}),
            json!({"meeting_id":"arbitrary-provider-id"}),
        ] {
            assert!(serde_json::from_value::<MeetingStatusSelector>(value)
                .map_or(true, |selector| selector.target().is_err()));
        }
    }
    #[test]
    fn oauth_return_is_only_meeting_page_and_never_an_external_redirect() {
        assert!(valid_meeting_return(
            "/connectors/microsoft-teams/meeting-capture?meeting_id=fixture"
        ));
        assert!(valid_meeting_return("/connectors/microsoft-teams/meeting-capture?teams_meeting_id=fixture&teams_chat_id=chat"));
        assert!(valid_meeting_return("/teams/meeting/connected"));
        assert!(valid_meeting_return("/outlook/meeting/connected"));
        for bad in [
            "/teams/meeting/connected?return_url=https://evil.example",
            "/outlook/meeting/connected#private",
            "/teams/meeting/connected/../other",
            "https://evil.example/",
            "//evil.example/path",
            "/connectors/microsoft-teams/meeting-capture?return_url=https://evil.example",
            "/connectors/microsoft-teams/meeting-capture?meeting_id=fixture\n",
            "/connectors/microsoft-teams/meeting-capture/../settings?meeting_id=fixture",
        ] {
            assert!(!valid_meeting_return(bad));
        }
    }
}
