//! Connector ingestion routes.
//!
//! Two entry points, both bearer-authed and account-scoped, sharing the same
//! token-decrypt + ingest plumbing:
//!
//! * `POST /v1/connectors/{provider}/sync` — pull *self-owned, non-sensitive*
//!   items the user consented to by connecting (e.g. their own calendar). Never
//!   reads a user's inbox.
//! * `POST /v1/connectors/{provider}/ingest` — ingest *specific items the user
//!   explicitly chose* by id (the per-thread "Ingest this thread" opt-in, CS-116).
//!   For Microsoft this expands to the whole conversation and registers it for
//!   reply auto-capture (CS-117 / ADR-0008); sensitive correspondence still
//!   enters only via an explicit opt-in or a later reply on an opted-in thread.
//!
//! Both prove the ingest side of the framework is provider-agnostic: the provider
//! only produces [`NormalizedItem`]s; everything downstream (chunk → blob → dedup
//! → embed) is shared with Slack/manual ingest.

use std::sync::Arc;

use axum::{
    extract::{rejection::JsonRejection, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Extension, Json,
};
use serde::Deserialize;
use serde_json::json;

use coralswarm_core::oauth_token::decrypt_token;

use crate::{
    connectors::{
        self, ConnectorCredentials, ConnectorError, ConnectorProvider, IngestScope,
        NormalizedItem, TokenOwner,
    },
    dek_cache::DekCache,
    ingest::atom::IngestAtomRequest,
    middleware::auth::UserContext,
    routes::{
        oauth::store_token_bundle,
        slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    },
    tenancy::extractor::Tenant,
    AppState,
};

/// Fail-closed gate for connector routes under an org `TenancyDecision`
/// (org-tenancy PR 1.6, closing PR 1.4's declared gap P1-2a; gsuite Train G2
/// PR 6, founder decision 2026-08-24: lifted for Gmail's opt-in-thread path
/// only, behind `ORG_GOOGLE_GMAIL_ENABLED` — mirrors m365 Train 2 PR 2.5's
/// `reject_org_teams_access`).
///
/// Connectors are per-user and ingest into PERSONAL — on the org plane, the
/// caller's own PRIVATE MEMBER — oceans only; org oceans are promotion-fed,
/// and "connect our whole workspace to the org ocean" is a named post-cutover
/// capability, deliberately NOT part of the v1 org model (see the
/// implementation plan's Connectors section).
///
/// Two independent, provider-scoped flag lifts share this gate:
///
/// **Microsoft (m365 enterprise train PR 3.3)** — lifted for BOTH routes
/// (`trigger_sync` = calendar, `ingest_selected` = mail opt-in) behind the
/// SAME registry provider `"microsoft"` teams_ingest already established
/// (`routes::teams_ingest::org_microsoft_teams_enabled_for` — one Microsoft
/// provider, one flag; not a second, Outlook-specific env var). Flag on:
/// this route proceeds with `tenant.pool` genuinely resolved to the org's
/// TENANT pool — see `finish_ingest`'s own `plane_of` derivation, no longer
/// hardcoded `AccountPlane::Personal`.
///
/// **Gmail carve-out (gsuite Train G2 PR 6)** — `is_item_ingest`
/// distinguishes [`ingest_selected`]'s call (`true`) from [`trigger_sync`]'s
/// (`false`), because the `"google"` provider string covers BOTH routes:
/// `trigger_sync`'s `sync()` entry is Calendar-only (lifted under org by the
/// Google Calendar org lift, on its OWN `google_calendar` flag — see the
/// second carve-out below), while `ingest_selected` for `"google"` is ALWAYS Gmail
/// (`fetch_items_credentials` always resolves the per-member Gmail token) —
/// so `is_item_ingest && provider == "google"` gated on
/// [`crate::routes::oauth::org_google_gmail_enabled_for`] is exact.
/// (Microsoft doesn't need the distinction — its org lift deliberately
/// covers both surfaces, and its calendar sync also runs via the tenant-loop
/// worker regardless.)
///
/// Every OTHER provider (Slack, anything registered later) keeps refusing
/// under an org unconditionally, flag or no flag. Personal / flag-off
/// requests are byte-identical to before either lift.
async fn reject_org_connector_access(
    state: &AppState,
    tenant: &Tenant,
    provider: &str,
    is_item_ingest: bool,
) -> Option<Response> {
    let ctx = tenant.org_context()?;
    if provider == "microsoft"
        && crate::routes::teams_ingest::org_microsoft_teams_enabled_for(state, &ctx.org_id).await
    {
        return None;
    }
    if is_item_ingest
        && provider == "google"
        && crate::routes::oauth::org_google_gmail_enabled_for(state, &ctx.org_id).await
    {
        return None;
    }
    // Google Calendar org lift: `trigger_sync` for `"google"` (`is_item_ingest
    // == false`) is Calendar-only — Gmail never goes through `sync()` — so
    // that route/provider pair is gated on the org's `google_calendar` flag
    // (`ORG_GOOGLE_CALENDAR_ENABLED`) and nothing else. The Gmail flag does
    // NOT open it (and the Calendar flag does not open Gmail's item-ingest
    // arm above): each Google surface keeps its own flag exclusively.
    if !is_item_ingest
        && provider == "google"
        && crate::routes::oauth::org_google_calendar_enabled_for(state, &ctx.org_id).await
    {
        return None;
    }
    tracing::info!(
        org_id = %ctx.org_id,
        provider,
        "connector route refused under org context (personal-workspace-only, or the org's \
         resolved connector flag is off for this provider/surface)"
    );
    Some(
        (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "connectors are available in your personal workspace only; \
                          switch out of the organization workspace to manage them"
            })),
        )
            .into_response(),
    )
}

/// Derive [`AccountPlane`] from an already-gated `Tenant` — identical shape
/// to `routes::teams_ingest::plane_of` (m365 Train 2 PR 2.5). Every call
/// site here runs AFTER [`reject_org_connector_access`], so an `Org`
/// `TenancyDecision` reaching this point has already had that gate's
/// per-provider flag confirmed on for its org (Microsoft via m365 PR 3.3's
/// lift, Gmail via gsuite G2 PR 6's item-ingest carve-out) — genuinely
/// reachable under `AccountPlane::Org` for both, not dead code; no extra DB
/// read needed to re-derive the plane.
fn plane_of(tenant: &Tenant) -> AccountPlane {
    match tenant.org_context() {
        Some(_) => AccountPlane::Org,
        None => AccountPlane::Personal,
    }
}

/// `POST /v1/connectors/{provider}/sync`
///
/// Pulls the provider's self-owned, non-sensitive items (e.g. calendar) and
/// ingests them into the account's primary ocean. Returns
/// `{ "ingested": N, "fetched": M }`.
pub async fn trigger_sync(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(provider): Path<String>,
) -> Response {
    if let Some(reject) = reject_org_connector_access(&state, &tenant, &provider, false).await {
        return reject;
    }
    let connector = match connectors::lookup(&provider) {
        Some(c) => c,
        None => return unknown_provider(&provider),
    };
    if !connector.supports_sync() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("provider '{provider}' does not support sync")})),
        )
            .into_response();
    }

    // Past the org gate this is either Personal / flag-off (`tenant.pool` IS
    // the legacy consumer pool, same handle as before) or — since m365
    // enterprise train PR 3.3, `provider == "microsoft"` with the org's flag
    // on — a genuine org `TenancyDecision`, where `tenant.pool` is the org's
    // own TENANT pool. Either way, resolved through the extractor, never an
    // ambient `state.db`.
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // CREDENTIALS pool + DEK plane — the same two-axis split `ingest_selected`
    // documents at length below, applied here too (org-plane DEK fix). The
    // `oauth_tokens` ROW is control-plane-only, so it must never be read off
    // `pool` (the CONTENT pool, which IS the org's tenant DB once Microsoft's
    // org carve-out lets an Org `TenancyDecision` reach this line — that read
    // would find no such row and 404 "not connected"). The KEY is a separate
    // question again: an org account's wrapped DEK lives in its tenant DB's
    // `account_keys`, so it resolves off the request's own decision, not off
    // the control pool. Personal/flag-off callers are unaffected —
    // `state.control_db()` IS `pool` there and the decision resolves to the
    // consumer plane.
    let Some(creds_pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), creds_pool);
    // Google Calendar org lift: under an org the caller's Google Calendar
    // token is their OWN per-member row (`Surface::calendar_token_owner`), so
    // the default owner is `Member(caller)` — exact, never the
    // `IS NULL OR owner = $n` fallback. Every other provider/plane keeps the
    // unchanged `AccountWide` default (Microsoft's own Member-first lookup
    // lives inside `load_creds_owner_aware`).
    let default_owner = if connector.id() == "google" && tenant.org_context().is_some() {
        connectors::google::Surface::calendar_token_owner(true, user_ctx.user_id)
    } else {
        TokenOwner::AccountWide
    };
    let (mut creds, owner) = match load_creds_owner_aware(
        creds_pool,
        &dek_plane,
        user_ctx.account_id,
        user_ctx.user_id,
        connector.id(),
        connector,
        default_owner,
        &state.dek_cache,
    )
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    // Shared outbound rate limiter (gsuite Train G2 PR 3, design doc §item
    // 5): every Google call this handler triggers (directly, and inside
    // `finish_ingest`'s `after_ingest` side effect) runs under one ambient
    // key for this account/provider pair — `key = format!("google:
    // {account_id}")` per the design doc, generalized to
    // `"{provider}:{account_id}"` so every connector gets the same per-
    // tenant isolation (harmless, and an improvement, for Slack/Microsoft,
    // which previously fell through to `graph_rate_limit::UNSCOPED_KEY` on
    // this route — see that module's own doc on why an unscoped fallback is
    // "not the intended steady state").
    let key = format!("{}:{}", connector.id(), user_ctx.account_id);
    crate::graph_rate_limit::scoped(key, async move {
        // Calendar, either provider, either plane: the SAME pass the
        // 30-minute sweep runs (`workers::microsoft_calendar_sync`,
        // `workers::google_calendar_sync`) — per-connection lock, cursor
        // committed only after every event ingested (the trait `sync()`
        // shape committed before this handler's own ingest loop, so a
        // dropped request lost the rest of its pass). Under an org the
        // member's private ocean lives on the org's tenant pool (`pool`),
        // keyed on the caller; Google's cursor row stays on the control
        // pool, Microsoft's rides the content pool.
        let sync_result = match connector.id() {
            "google" | "microsoft" => {
                let gate = crate::calendar_pass_gate::CalendarPassGate::shared();
                return calendar_sync_response(gate, &state, &tenant, &pool, &user_ctx, connector.id(), owner, creds).await;
            }
            _ => {
                connector
                    .sync(&state.http_client, &pool, user_ctx.account_id, &mut creds)
                    .await
            }
        };
        let items = match sync_result {
            Ok(items) => items,
            Err(e) => {
                // Re-resolve the control pool off the captured `state`
                // rather than closing over the outer `creds_pool` binding —
                // `finish_ingest` below already needs `&state` whole inside
                // this `async move` block, so this sidesteps any borrow
                // interaction with that move.
                if let Some(creds_pool) = state.control_db() {
                    connectors::credential_health::record_if_revoked(
                        creds_pool,
                        user_ctx.account_id,
                        owner,
                        &e,
                    )
                    .await;
                }
                return connector_error_response(e);
            }
        };

        finish_ingest(
            &state,
            &tenant,
            &user_ctx,
            &pool,
            connector,
            connector.id(),
            owner,
            &creds,
            items,
            // `follow: true` — `trigger_sync` (self-owned/non-sensitive
            // "sync now" data, e.g. calendar) has no `scope`/`follow` knobs
            // of its own; every item it pulls is unthreaded (`thread_id`
            // always `None`), so `after_ingest`'s watched-row upsert is a
            // no-op regardless of this value — `true` just keeps this call
            // site byte-identical to before the gmail-options backend train
            // added the parameter.
            true,
            None,
        )
        .await
    })
    .await
}

/// `trigger_sync`'s calendar arm: one permit from `gate` — the process-wide
/// `CalendarPassGate::shared()` in production (a pass pins a pooled connection on its lock for
/// its whole duration; without the bound a burst of clicks and pushes
/// could pin the pool — see that module), then the provider's own pass
/// under it. Under an org the permit also carries a slot of that org's own
/// bound, sized from `pool` (the org's tenant request pool, 5 connections):
/// a Microsoft member's pass pins its lock there and a Google member's
/// ingests there, so the shared bound alone let one org's members pin
/// their whole pool. Over either bound for `ROUTE_WAIT` is `429` with
/// `Retry-After`: the slots are busy doing calendar work, including
/// possibly this very connection's.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn calendar_sync_response(
    gate: &crate::calendar_pass_gate::CalendarPassGate,
    state: &Arc<AppState>,
    tenant: &Tenant,
    pool: &sqlx::PgPool,
    user_ctx: &UserContext,
    provider: &str,
    owner: TokenOwner,
    creds: ConnectorCredentials,
) -> Response {
    use crate::calendar_pass_gate::{ROUTE_RETRY_AFTER_SECS, ROUTE_WAIT};
    let org_plane = tenant.org_context().map(|ctx| {
        let org_id = ctx.org_id.as_str();
        match provider {
            "microsoft" => crate::calendar_pass_gate::OrgSlot::microsoft(org_id, pool),
            _ => crate::calendar_pass_gate::OrgSlot::google(org_id, pool),
        }
    });
    let Some(_permit) = gate.acquire_scoped(org_plane, ROUTE_WAIT).await else {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [(axum::http::header::RETRY_AFTER, ROUTE_RETRY_AFTER_SECS.to_string())],
            Json(json!({"error": "too many calendar syncs in flight — try again shortly"})),
        )
            .into_response();
    };
    match provider {
        "microsoft" => microsoft_calendar_sync_response(state, tenant, pool, user_ctx, owner, creds).await,
        _ => google_calendar_sync_response(state, tenant, pool, user_ctx).await,
    }
}

/// `trigger_sync`'s Microsoft arm: run the connection's calendar pass
/// (`workers::microsoft_calendar_sync::sync_member` under an org,
/// `sync_personal_account` otherwise) with the credential this handler
/// already loaded and refreshed, and render its outcome in the route's
/// `{ ingested, fetched }` shape. `Busy` — the sweep (or another click) is
/// mid-pass on this connection — is `409`, not a wait: the running pass is
/// doing the same work. Nothing here goes through `finish_ingest`: the pass
/// persists a refreshed token itself and Calendar items carry no
/// `thread_id`, so `after_ingest` would be a no-op anyway.
async fn microsoft_calendar_sync_response(
    state: &Arc<AppState>,
    tenant: &Tenant,
    pool: &sqlx::PgPool,
    user_ctx: &UserContext,
    owner: TokenOwner,
    creds: ConnectorCredentials,
) -> Response {
    use crate::workers::microsoft_calendar_sync::{sync_member, sync_personal_account, PassOutcome};
    let Some(control) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let account_id = user_ctx.account_id;
    let outcome = match tenant.org_context() {
        Some(ctx) => {
            sync_member(pool, control, state, account_id, &ctx.org_id, user_ctx.user_id, Some((owner, creds)), false).await
        }
        None => sync_personal_account(state, control, account_id, Some((owner, creds))).await,
    };
    match outcome {
        Ok(PassOutcome::Synced { fetched, ingested }) => {
            (StatusCode::OK, Json(json!({"ingested": ingested, "fetched": fetched}))).into_response()
        }
        Ok(PassOutcome::Busy) => calendar_pass_busy(),
        Ok(PassOutcome::Skipped(reason)) => {
            (StatusCode::OK, Json(json!({"ingested": 0, "fetched": 0, "skipped": reason}))).into_response()
        }
        Err(e) => connector_error_response(e),
    }
}

/// `trigger_sync`'s Google Calendar arm — the Google twin of
/// [`microsoft_calendar_sync_response`]: `workers::google_calendar_sync::
/// sync_org_member` under an org (the caller's OWN per-member token and
/// cursor row on the control pool, THEIR private ocean on `pool`),
/// `sync_personal_account` otherwise. The pass loads and refreshes its own
/// credential — the handler's load above is the "connected at all" gate
/// (`404` otherwise) and never a second refresh, `refresh_if_needed` being
/// idempotent — records a `CredentialRevoked` itself, and commits the
/// cursor only after every event ingested; `Busy` is `409`. `fetched` in
/// the response is the same count as `ingested`: Google's pass ingests
/// everything it fetched or fails the commit.
pub(crate) async fn google_calendar_sync_response(
    state: &Arc<AppState>,
    tenant: &Tenant,
    pool: &sqlx::PgPool,
    user_ctx: &UserContext,
) -> Response {
    use crate::workers::google_calendar_sync::{sync_org_member, sync_personal_account, PassOutcome};
    let Some(control) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    let account_id = user_ctx.account_id;
    let outcome = match tenant.org_context() {
        Some(ctx) => sync_org_member(state, control, pool, account_id, &ctx.org_id, user_ctx.user_id).await,
        None => sync_personal_account(state, control, account_id).await,
    };
    match outcome {
        Ok(PassOutcome::Synced { ingested }) => {
            (StatusCode::OK, Json(json!({"ingested": ingested, "fetched": ingested}))).into_response()
        }
        Ok(PassOutcome::Busy) => calendar_pass_busy(),
        Ok(PassOutcome::Skipped(reason)) => {
            (StatusCode::OK, Json(json!({"ingested": 0, "fetched": 0, "skipped": reason}))).into_response()
        }
        Err(e) => connector_error_response(e),
    }
}

fn calendar_pass_busy() -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({"error": "a calendar sync for this connection is already running"})),
    )
        .into_response()
}

/// `POST /v1/connectors/{provider}/ingest`
///
/// Ingests the specific items named in the body — the per-thread opt-in path
/// (CS-116). The add-in / picker passes the provider-native ids the user chose;
/// only those are fetched and ingested. Returns `{ "ingested": N, "fetched": M }`.
pub async fn ingest_selected(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(provider): Path<String>,
    body: Result<Json<IngestSelectedRequest>, JsonRejection>,
) -> Response {
    let body = match parse_ingest_request(body) {
        Ok(body) => body,
        Err(response) => return response,
    };
    if let Some(reject) = reject_org_connector_access(&state, &tenant, &provider, true).await {
        return reject;
    }
    let connector = match connectors::lookup(&provider) {
        Some(c) => c,
        None => return unknown_provider(&provider),
    };
    if !connector.supports_fetch_items() {
        return (
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": format!("provider '{provider}' does not support item ingestion")}),
            ),
        )
            .into_response();
    }
    if body.item_ids.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "item_ids must not be empty"})),
        )
            .into_response();
    }
    // Bound the per-request fan-out so a single call can't enumerate a mailbox.
    if body.item_ids.len() > MAX_ITEMS_PER_REQUEST {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": format!("at most {MAX_ITEMS_PER_REQUEST} items per request")})),
        )
            .into_response();
    }

    // CONTENT pool (same post-gate resolution as `trigger_sync` — see its
    // comment): Personal's consumer pool, or — since gsuite Train G2 PR 6
    // lifted the gate above for Gmail — the org's own TENANT pool. Used
    // below for graph/atom resolution and the connector's own per-thread
    // opt-in bookkeeping (`gmail_watched_threads`), which has a real tenant-
    // plane twin (`migrations_tenant/0066`) and whose own internal scoping
    // (`google_gmail::scoped_tx` → `begin_tenant_system_scoped`) is already
    // plane-agnostic. See the `creds_pool` comment just below for the ONE
    // thing (`oauth_tokens`, the account DEK) that is NOT plane-aware and
    // must never receive this pool.
    let Some(pool) = tenant.pool.clone() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // CREDENTIALS pool (gsuite Train G2 PR 6, mirrors m365 Train 2 PR 2.5's
    // `microsoft_token` fix — see its own doc comment): `oauth_tokens` is a
    // CONTROL-plane-only table with no tenant twin (`routes::oauth::
    // list_connections`'s own doc comment already establishes this for the
    // generic OAuth management routes). **NOT the account DEK** — this
    // comment used to claim the same for it ("`crate::crypto::
    // resolve_dek_cached` reads `accounts.dek_wrapped`, `accounts` being
    // control-plane-only"), and that inference was the org-plane DEK bug:
    // `accounts` is indeed control-only, but an ORG account has no
    // `dek_wrapped` row there at all — its key was RELOCATED to its tenant
    // DB's `account_keys` (see `crate::crypto`'s module doc), so the control
    // pool is the right place for the ROW and the wrong place for the KEY.
    // The `dek_plane` resolved just below is that second axis.
    // Before this PR, `pool`
    // above was ALWAYS the consumer pool here too (the org gate refused
    // every call before reaching this line), so `load_creds_owner_aware`
    // could get away with taking the CONTENT pool directly. Now that Gmail's
    // carve-out makes this genuinely reachable under Org, credentials MUST
    // resolve against `state.control_db()` — reading `oauth_tokens` off the
    // org's tenant pool would silently 404 (the row lives on control,
    // absent there entirely), the exact bug class m365 Train 2 PR 2.5's own
    // cross-plane sweep found in `teams_channel_name_by_container`/
    // `slack_integrations`. Personal callers are unaffected:
    // `state.control_db()` IS `tenant.pool` there — same physical pool, see
    // `tenancy::extractor::Tenant`'s own doc comment.
    let Some(creds_pool) = state.control_db() else {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response();
    };
    // gsuite Train G2 PR 3: which `(oauth_tokens.provider, TokenOwner)` row
    // this call reads/persists is no longer always `(connector.id(),
    // AccountWide)` — Gmail's per-member isolation (`GoogleConnector::
    // fetch_items_credentials`) overrides it to `("google_gmail",
    // Member(user_id))`. See `ConnectorProvider::fetch_items_credentials`'s
    // own doc for why this can't be derived from `connector.id()` alone.
    // The trait's pair is the DEFAULT; `load_creds_owner_aware` layers
    // Microsoft's Member-first/AccountWide-fallback on top (m365 PR 3.2).
    let (provider_key, trait_owner) = connector.fetch_items_credentials(user_ctx.user_id);
    // The account DEK is a SEPARATE plane question from `creds_pool` above
    // (org-plane DEK fix): `oauth_tokens` rows are control-plane-only, but an
    // ORG account's wrapped DEK is not — it lives in that org's own tenant DB
    // (`account_keys`), never in control's `accounts.dek_wrapped`. The
    // `creds_pool` comment above used to assert both facts as one; only the
    // first is true. Resolved off the request's own gate decision so the
    // Personal/flag-off path stays byte-identical.
    let dek_plane = crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), creds_pool);
    let (mut creds, owner) = match load_creds_owner_aware(
        creds_pool,
        &dek_plane,
        user_ctx.account_id,
        user_ctx.user_id,
        provider_key,
        connector,
        trait_owner,
        &state.dek_cache,
    )
    .await
    {
        Ok(v) => v,
        Err(resp) => return resp,
    };

    // Same per-tenant rate-limit scope as `trigger_sync` — see its comment.
    // Keyed on the ACTUAL provider_key (e.g. `"google_gmail"`, not the
    // route's `"google"`) so Gmail's opt-in thread fetch/live-follow shares
    // one bucket per account with every other Gmail call this PR wraps
    // (`google_gmail.rs`, the push dispatch, the renewal worker) rather than
    // a separate one keyed on the generic route provider.
    let follow_since = (body.scope == IngestScope::Message).then(chrono::Utc::now);
    let key = format!("{provider_key}:{}", user_ctx.account_id);
    crate::graph_rate_limit::scoped(key, async move {
        let items = match connector
            .fetch_items(&state.http_client, &mut creds, &body.item_ids, body.scope)
            .await
        {
            Ok(items) => items,
            Err(e) => {
                // See `trigger_sync`'s identical arm for why the control
                // pool is re-resolved off `state` here rather than closing
                // over the outer `creds_pool` binding.
                if let Some(creds_pool) = state.control_db() {
                    connectors::credential_health::record_if_revoked(
                        creds_pool,
                        user_ctx.account_id,
                        owner,
                        &e,
                    )
                    .await;
                }
                return connector_error_response(e);
            }
        };

        finish_ingest(
            &state, &tenant, &user_ctx, &pool, connector, provider_key, owner, &creds, items,
            body.follow,
            follow_since,
        )
        .await
    })
    .await
}

/// Cap on items per explicit-ingest request (defence against using the opt-in
/// endpoint to bulk-pull).
const MAX_ITEMS_PER_REQUEST: usize = 50;

/// Body for [`ingest_selected`].
///
/// # Opt-in save options (gmail-options backend train, PR 1 of 4: backend →
/// Gmail add-on card → web list → Outlook add-in)
///
/// Two orthogonal knobs, both optional, both defaulting to today's only
/// behavior — a request that omits either or both fields is byte-identical
/// to a pre-PR request:
///
/// * `scope` — [`IngestScope::Thread`] (default) or [`IngestScope::Message`].
///   See that type's own doc. An unrecognized string is a `400` (serde
///   deserialization failure mapped by [`parse_ingest_request`]), never a
///   silent fallback.
/// * `follow` — keep auto-saving new replies (default `true`).
///
/// A watched row (`gmail_watched_threads`/`outlook_watched_conversations`)
/// is written for EVERY opt-in save, always — including `scope=message` and
/// `follow=false` — because deletion propagation
/// (`connectors::google_gmail::propagate_deletions`, the Microsoft
/// deletion-notification path in `routes::graph_webhook`) resolves the
/// target ocean FROM that row's `graph_id`; no row means an upstream
/// deletion can never be propagated again (both providers' change feeds
/// report a given deletion exactly once). Unfollowing (`follow=false`, or a
/// later `PUT`/`DELETE` on the watched resource below) flips
/// `follow_replies`, never deletes the row. A later save of an
/// already-watched thread adopts THAT save's `follow` value — re-saving with
/// `follow=true` re-follows an unfollowed thread.
#[derive(Debug, Deserialize)]
pub struct IngestSelectedRequest {
    /// Provider-native ids the user explicitly chose to ingest (e.g. Outlook
    /// message ids).
    pub item_ids: Vec<String>,
    /// Whole thread (default) or just the selected message(s). See
    /// [`IngestScope`]'s own doc.
    #[serde(default)]
    pub scope: IngestScope,
    /// Keep auto-saving new replies. Default `true`.
    #[serde(default = "default_follow")]
    pub follow: bool,
}

/// Keep the opt-in request's invalid-data status at the options contract's
/// `400`. Axum normally rejects JSON data errors (including an unknown scope
/// enum value) with `422`; explicitly translate those here, while retaining
/// its existing syntax/content-type/body-limit statuses for other rejections.
fn parse_ingest_request(
    body: Result<Json<IngestSelectedRequest>, JsonRejection>,
) -> Result<IngestSelectedRequest, Response> {
    body.map(|Json(body)| body).map_err(|rejection| {
        if matches!(rejection, JsonRejection::JsonDataError(_)) {
            (StatusCode::BAD_REQUEST, Json(json!({"error": rejection.body_text()}))).into_response()
        } else {
            rejection.into_response()
        }
    })
}

fn default_follow() -> bool {
    true
}

// ---------------------------------------------------------------------------
// Watched resource (gmail-options backend train, PR 1 of 4): provider-
// generic status / toggle / list over `gmail_watched_threads` /
// `outlook_watched_conversations`, reused by the Gmail add-on, the Outlook
// add-in and the web app. Registered alongside `ingest_selected` (see
// `lib.rs`), same auth (`require_auth` — the add-on's `csat_` PAT already
// works for it, same as `ingest_selected`) and the same
// `reject_org_connector_access` gate, with an additional personal-only
// rejection before the ingest gate's existing org carve-outs can lift it.
// Watched state must never expose another org member's private ocean.
// ---------------------------------------------------------------------------

/// Resolve `{provider}` to its connector AND confirm it supports the
/// per-thread opt-in surface these routes back — the EXACT gate
/// `ingest_selected` applies above (`connectors::lookup` +
/// `supports_fetch_items`), reused here so the two route families can never
/// drift on which providers are accepted. `Err` is a ready response — `404`
/// unknown provider (`unknown_provider`) or `400` "does not support item
/// ingestion" (same message `ingest_selected` returns) — never a third
/// shape. Today this admits exactly `"google"` and `"microsoft"`; Slack (the
/// only other registered connector) doesn't support item ingest and so has
/// no watched-thread table to back these routes with.
fn watched_provider(provider: &str) -> Result<&'static dyn ConnectorProvider, Response> {
    let connector = connectors::lookup(provider).ok_or_else(|| unknown_provider(provider))?;
    if !connector.supports_fetch_items() {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(
                json!({"error": format!("provider '{provider}' does not support item ingestion")}),
            ),
        )
            .into_response());
    }
    Ok(connector)
}

/// Shared prologue for all four watched-resource routes: the org gate (same
/// `is_item_ingest = true` `ingest_selected` uses), provider validation, and
/// the content pool. `Err` is a ready response.
async fn watched_prologue(
    state: &Arc<AppState>,
    tenant: &Tenant,
    provider: &str,
) -> Result<(&'static dyn ConnectorProvider, sqlx::PgPool), Response> {
    // The options watched surface is personal-only. Ingest's historical
    // org carve-outs do not authorize account-wide reads of other members'
    // private watched rows, or changes to their follow preferences.
    if tenant.org_context().is_some() {
        return Err((StatusCode::FORBIDDEN, Json(json!({"error": "watched threads are available in your personal workspace only"}))).into_response());
    }
    if let Some(reject) = reject_org_connector_access(state, tenant, provider, true).await {
        return Err(reject);
    }
    let connector = watched_provider(provider)?;
    let pool = tenant.pool.clone().ok_or_else(|| {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": "service unavailable"})),
        )
            .into_response()
    })?;
    Ok((connector, pool))
}

/// `GET /v1/connectors/{provider}/watched/{thread_id}` → `200
/// {"following": bool}`. No row → `200 {"following": false}` (never `404` —
/// the add-on card treats `false` as "not saved").
pub async fn get_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path((provider, thread_id)): Path<(String, String)>,
) -> Response {
    let (connector, pool) = match watched_prologue(&state, &tenant, &provider).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let account_id = user_ctx.account_id;
    let status = match connector.id() {
        "google" => connectors::google_gmail::watched_status(&pool, account_id, &thread_id).await,
        "microsoft" => connectors::microsoft::watched_status(&pool, account_id, &thread_id).await,
        other => unreachable!("watched_provider only admits google/microsoft, got {other}"),
    };
    match status {
        Ok(following) => {
            (StatusCode::OK, Json(json!({"following": following.unwrap_or(false)}))).into_response()
        }
        Err(e) => connector_error_response(e),
    }
}

/// Body for [`put_watched`].
#[derive(Debug, Deserialize)]
pub struct SetFollowRequest {
    pub follow: bool,
}

/// `PUT /v1/connectors/{provider}/watched/{thread_id}` — `{"follow": bool}` →
/// `200 {"following": bool}`; `404` when no row exists for this account +
/// thread. Never inserts, never deletes.
pub async fn put_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path((provider, thread_id)): Path<(String, String)>,
    Json(body): Json<SetFollowRequest>,
) -> Response {
    set_watched_follow(state, tenant, user_ctx, provider, thread_id, body.follow).await
}

/// `DELETE /v1/connectors/{provider}/watched/{thread_id}` — identical to
/// `PUT {"follow": false}`: `200 {"following": false}`; `404` when no row.
/// Never deletes the row (the name is the wire-level "unfollow" verb the
/// picker/card UIs use; the underlying row is immortal for deletion-
/// propagation reasons — see [`IngestSelectedRequest`]'s own doc).
pub async fn delete_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path((provider, thread_id)): Path<(String, String)>,
) -> Response {
    set_watched_follow(state, tenant, user_ctx, provider, thread_id, false).await
}

async fn set_watched_follow(
    state: Arc<AppState>,
    tenant: Tenant,
    user_ctx: UserContext,
    provider: String,
    thread_id: String,
    follow: bool,
) -> Response {
    let (connector, pool) = match watched_prologue(&state, &tenant, &provider).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let account_id = user_ctx.account_id;
    let result = match connector.id() {
        "google" => connectors::google_gmail::set_follow(&pool, account_id, &thread_id, follow).await,
        "microsoft" => connectors::microsoft::set_follow(&pool, account_id, &thread_id, follow).await,
        other => unreachable!("watched_provider only admits google/microsoft, got {other}"),
    };
    match result {
        Ok(Some(following)) => (StatusCode::OK, Json(json!({"following": following}))).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "not watching this thread"})),
        )
            .into_response(),
        Err(e) => connector_error_response(e),
    }
}

/// Query params for [`list_watched`].
#[derive(Debug, Deserialize)]
pub struct ListWatchedQuery {
    pub limit: Option<i64>,
    pub cursor: Option<uuid::Uuid>,
}

/// Default page size for [`list_watched`] when `limit` is omitted.
const DEFAULT_WATCHED_LIST_LIMIT: i64 = 50;
/// Max page size for [`list_watched`] — a caller-supplied `limit` above this
/// is clamped, never rejected (same posture as every other paginated list
/// route in this codebase).
const MAX_WATCHED_LIST_LIMIT: i64 = 200;

/// `GET /v1/connectors/{provider}/watched?limit=&cursor=` → `200
/// {"threads": [{"thread_id", "following", "title", "saved_at"}],
/// "next_cursor": string|null}` — newest first (by `saved_at`), including
/// BOTH followed and unfollowed rows (the web list filters/toggles
/// client-side). `cursor` is the opaque `next_cursor` from a prior page —
/// see `connectors::google_gmail::list_watched`'s own doc for the keyset
/// technique.
pub async fn list_watched(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user_ctx): Extension<UserContext>,
    Path(provider): Path<String>,
    Query(query): Query<ListWatchedQuery>,
) -> Response {
    let (connector, pool) = match watched_prologue(&state, &tenant, &provider).await {
        Ok(v) => v,
        Err(resp) => return resp,
    };
    let account_id = user_ctx.account_id;
    let limit = query
        .limit
        .unwrap_or(DEFAULT_WATCHED_LIST_LIMIT)
        .clamp(1, MAX_WATCHED_LIST_LIMIT);

    match connector.id() {
        "google" => {
            let result =
                connectors::google_gmail::list_watched(&pool, account_id, limit, query.cursor).await;
            let (rows, next_cursor) = match result {
                Ok(v) => v,
                Err(e) => return connector_error_response(e),
            };
            let mut threads = Vec::with_capacity(rows.len());
            for row in rows {
                // Best-effort title (see `google_gmail::best_effort_title`'s
                // own doc for why this is a cheap, indexed lookup and not a
                // live Gmail API call) — one lookup per row, capped at
                // `MAX_WATCHED_LIST_LIMIT` rows per page, so worst case is a
                // bounded number of indexed unique-constraint reads, not an
                // unbounded fan-out.
                let title =
                    connectors::google_gmail::best_effort_title(&pool, account_id, row.graph_id, &row.thread_id)
                        .await;
                threads.push(json!({
                    "thread_id": row.thread_id,
                    "following": row.following,
                    "title": title,
                    "saved_at": row.saved_at.to_rfc3339(),
                }));
            }
            (
                StatusCode::OK,
                Json(json!({"threads": threads, "next_cursor": next_cursor})),
            )
                .into_response()
        }
        "microsoft" => {
            let result =
                connectors::microsoft::list_watched(&pool, account_id, limit, query.cursor).await;
            let (rows, next_cursor) = match result {
                Ok(v) => v,
                Err(e) => return connector_error_response(e),
            };
            // `title: null`, always (gmail-options backend train, PR 1 of
            // 4, decided and documented): Outlook's `conversationId` is a
            // separately-generated Graph identifier with no relationship to
            // any message's own id (unlike Gmail's `threadId`, which IS the
            // first message's id — see `google_gmail::best_effort_title`'s
            // doc), and `atoms` carries no `thread_id`/`conversation_id`
            // column to join through generically. Resolving a title here
            // would need either a live Graph round trip per row (not
            // "cheap", burns this account's rate-limit budget on a list
            // read) or a schema change to persist a subject snapshot at
            // save time (deliberately out of scope for this additive-only
            // migration PR — left for a later PR if the founder wants it).
            let threads: Vec<_> = rows
                .into_iter()
                .map(|row| {
                    json!({
                        "thread_id": row.conversation_id,
                        "following": row.following,
                        "title": serde_json::Value::Null,
                        "saved_at": row.saved_at.to_rfc3339(),
                    })
                })
                .collect();
            (
                StatusCode::OK,
                Json(json!({"threads": threads, "next_cursor": next_cursor})),
            )
                .into_response()
        }
        other => unreachable!("watched_provider only admits google/microsoft, got {other}"),
    }
}

// ── Shared helpers ──────────────────────────────────────────────────────────────

/// Load the account's stored token for `connector` and decrypt it into
/// [`ConnectorCredentials`], or a ready [`Response`] (404 not-connected, 503
/// no-KEK, 500 decrypt/db error). `pool` is the pool the `oauth_tokens` ROW
/// is read from — always the CONTROL plane (see `ingest_selected`'s own
/// `creds_pool` comment: `oauth_tokens` has no live tenant twin).
///
/// `dek_plane` is the plane the account's KEY is read from, and it is NOT the
/// same question as `pool` (org-plane DEK fix). An org account's wrapped DEK
/// lives in its own tenant database's `account_keys`, never in control's
/// `accounts.dek_wrapped` — so the previous `crypto::resolve_dek_cached(pool,
/// ...)` here (which hardcodes `TenantId::Consumer`) returned `Ok(None)` for
/// every org account and 503'd with "encryption key not configured", the same
/// defect `routes::oauth::store_token_bundle_tx` carried on the WRITE side.
/// Callers build it from the request's own `TenancyDecision`
/// ([`crate::crypto::DekPlane::from_decision`]), so the personal/flag-off
/// path is byte-identical and the org path can never silently borrow the
/// consumer key.
///
/// `dek_cache` (Foundation PR 1.2, CS-031): the account DEK is the SAME
/// wrapped DEK content encryption uses, not the old per-oauth-token XOR
/// derivation — this is a hot, per-request path (every `sync`/`ingest`
/// call), so it goes through `AppState::dek_cache` like every other
/// request-path DEK read. The cache is keyed on `(TenantId, account_id)`, so
/// a personal and an org DEK for the same account id cannot collide in it.
async fn load_creds(
    pool: &sqlx::PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: uuid::Uuid,
    provider_key: &str,
    connector: &dyn ConnectorProvider,
    owner: TokenOwner,
    dek_cache: &DekCache,
) -> Result<ConnectorCredentials, Response> {
    // RLS scoping (org-tenancy conversion): `oauth_tokens` carries an
    // `account_id` policy (migration 0083) — the read runs inside a
    // `begin_account_scoped` transaction (inert pre-OPS-4).
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| {
            tracing::warn!(error = %e, account_id = %account_id, provider = %provider_key, "failed to open scoped tx for oauth_tokens load");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        })?;
    // Foundation PR 1.1: explicit owner predicate, not a bare `LIMIT 1` over
    // every workspace/owner row for this account+provider — the previous
    // `ORDER BY updated_at DESC LIMIT 1` shape was the "last-writer-wins"
    // bug the plan's PR 1.1 spec names: once a second owner's row exists for
    // the same account+provider, an unfiltered `LIMIT 1` could silently
    // return the wrong owner's token. See `TokenOwner`'s own doc comment.
    //
    // `provider_key` (gsuite Train G2 PR 3): the EXACT `oauth_tokens.
    // provider` literal to read — NOT always `connector.id()` (a per-surface
    // provider like Gmail stores under `"google_gmail"` while its route/
    // connector id is `"google"`; see `ConnectorProvider::
    // fetch_items_credentials`'s doc). `connector` is kept only for error
    // classification (`connector_error_response`) elsewhere in this module —
    // it plays no role in the query itself.
    let row = match owner {
        TokenOwner::AccountWide => {
            sqlx::query_as::<_, TokenRow>(
                r#"
                SELECT access_token, refresh_token, expires_at, provider_workspace_id, updated_at
                FROM oauth_tokens
                WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .bind(provider_key)
            .fetch_optional(&mut *tx)
            .await
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query_as::<_, TokenRow>(
                r#"
                SELECT access_token, refresh_token, expires_at, provider_workspace_id, updated_at
                FROM oauth_tokens
                WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .bind(provider_key)
            .bind(owner_user_id)
            .fetch_optional(&mut *tx)
            .await
        }
    }
    .map_err(|e| {
        tracing::warn!(error = %e, account_id = %account_id, provider = %provider_key, "failed to load oauth_tokens");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response()
    })?;
    tx.rollback().await.ok();

    let row = row.ok_or_else(|| {
        (
            StatusCode::NOT_FOUND,
            Json(json!({"error": format!("{} is not connected", connector.id())})),
        )
            .into_response()
    })?;

    let dek = dek_plane
        .resolve_cached(account_id, dek_cache)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, account_id = %account_id, dek_plane = ?dek_plane, "failed to resolve account DEK");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "encryption key not configured"})),
            )
                .into_response()
        })?
        .ok_or_else(|| {
            tracing::error!(account_id = %account_id, dek_plane = ?dek_plane, "account has no dek_wrapped provisioned");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "encryption key not configured"})),
            )
                .into_response()
        })?;

    let access_token = decrypt_token(&dek, &row.access_token).map_err(|e| {
        tracing::error!(error = %e, account_id = %account_id, "failed to decrypt access_token");
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "internal error"})),
        )
            .into_response()
    })?;
    let refresh_token = row
        .refresh_token
        .as_deref()
        .and_then(|enc| decrypt_token(&dek, enc).ok());

    Ok(ConnectorCredentials {
        access_token,
        refresh_token,
        expires_at: row.expires_at,
        provider_workspace_id: row.provider_workspace_id.unwrap_or_default(),
        refreshed: None,
        row_updated_at: Some(row.updated_at),
    })
}

/// [`load_creds`], but member-owner-aware for Microsoft (m365 enterprise
/// train PR 3.2). Returns the credentials AND the [`TokenOwner`] they were
/// actually loaded under, so the caller can persist a mid-run token refresh
/// (`finish_ingest`) back under that SAME owner rather than forcing
/// `AccountWide` regardless of where the read came from.
///
/// For `connector.id() == "microsoft"`: tries the CALLER's own
/// `TokenOwner::Member(user_id)` row first, falling back to
/// `TokenOwner::AccountWide` on a `404` (every Microsoft connection this
/// codebase has written to date — see `TokenOwner`'s own doc comment).
/// Application-level fallback — two distinct queries, each hitting exactly
/// one of `oauth_tokens`'s two partial unique indexes — never the SQL-level
/// `owner_user_id IS NULL OR owner_user_id = $n` shape `TokenOwner`'s own
/// doc forbids for a credential READ (that shape is fine for a metadata
/// LISTING like `list_connections`, never for handing out a token). This is
/// still forward-compat, not behavior-changing, for MICROSOFT specifically:
/// `reject_org_connector_access` (this module) still refuses every
/// org-context call to `trigger_sync`/`ingest_selected` for `provider ==
/// "microsoft"` (gsuite Train G2 PR 6 only carved out `provider == "google"`
/// for Gmail's `ingest_selected` path — Microsoft's org gate is untouched),
/// and `resolve_microsoft_token_target` (`routes::oauth`) only ever writes a
/// `Member` row on the org plane — so today the `Member` lookup below always
/// misses and this degrades to exactly `load_creds`'s old `AccountWide`-only
/// behavior. It stops being a no-op the moment Microsoft's own org gate
/// lifts, with zero further code changes needed here.
///
/// Every other connector takes `default_owner` — whatever the caller's
/// provider-selection produced (`fetch_items_credentials`'s pair for the
/// opt-in path, plain `AccountWide` for sync) — with no `Member`-first
/// attempt: Gmail's per-member isolation is unconditional by design (its
/// trait override IS the owner, no fallback wanted — gsuite G2 PR 3), and
/// Google Calendar is intentionally account-wide only.
async fn load_creds_owner_aware(
    pool: &sqlx::PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: uuid::Uuid,
    user_id: uuid::Uuid,
    provider_key: &str,
    connector: &dyn ConnectorProvider,
    default_owner: TokenOwner,
    dek_cache: &DekCache,
) -> Result<(ConnectorCredentials, TokenOwner), Response> {
    if connector.id() == "microsoft" {
        match load_creds(
            pool,
            dek_plane,
            account_id,
            provider_key,
            connector,
            TokenOwner::Member(user_id),
            dek_cache,
        )
        .await
        {
            Ok(creds) => return Ok((creds, TokenOwner::Member(user_id))),
            Err(resp) if resp.status() == StatusCode::NOT_FOUND => {
                // No per-member row yet — fall through to the default-owner
                // lookup below (legacy/personal-plane connection).
            }
            Err(resp) => return Err(resp),
        }
    }
    let creds = load_creds(
        pool,
        dek_plane,
        account_id,
        provider_key,
        connector,
        default_owner,
        dek_cache,
    )
    .await?;
    Ok((creds, default_owner))
}

/// Load + decrypt an account's stored token for `provider` outside an HTTP
/// request context (the change-notification webhook and the renewal worker).
/// `Ok(None)` means the account has no connection for the provider; callers
/// treat that as "nothing to do".
///
/// Foundation PR 1.2 (CS-031): resolves the account DEK via the SAME wrapped
/// DEK content encryption uses, not the old per-oauth-token XOR derivation.
/// Uncached (unlike `load_creds`'s `dek_cache` param): this function's callers
/// are background workers/webhooks that do not carry a `DekCache` handle —
/// the same "no `AppState` access, call the resolver directly" convention
/// `crate::crypto::resolve_dek_cached`'s own doc comment already states for
/// the embed worker.
///
/// # The two pools, and why they are separate parameters
///
/// * `pool` owns the `oauth_tokens` ROW. That table is CONTROL-plane-only for
///   every account (`accounts` does not even exist on a tenant DB), so every
///   caller passes the control pool.
/// * `dek_plane` owns the KEY. For a personal account that is also control
///   (`accounts.dek_wrapped`); for an ORG account it is the org's own tenant
///   DB (`account_keys.dek_wrapped`), which is a different physical database.
///
/// Collapsing the two into one `pool` argument WAS the bug this parameter
/// closes (see [`crate::routes::oauth::store_token_bundle_tx`]'s doc for the
/// write-side half). Before it, this function called `crypto::resolve_dek`,
/// which hardcodes `TenantId::Consumer` — so a background credential load for
/// an org account could only ever miss its key, and every webhook and renewal
/// worker was structurally unable to read a connection the OAuth callback is
/// now allowed to create. Callers name their plane from whatever they already
/// hold: [`crate::crypto::DekPlane::from_decision`] behind a request's
/// binding gate, [`crate::crypto::DekPlaneSource::plane_for`] inside a
/// `tenant_harness` tick, or [`crate::crypto::DekPlane::resolve_for_account`]
/// / [`crate::crypto::DekPlane::resolve_for_org`] in a webhook that has an
/// `AppState` but no decision.
pub(crate) async fn load_creds_bg(
    pool: &sqlx::PgPool,
    dek_plane: &crate::crypto::DekPlane,
    account_id: uuid::Uuid,
    provider: &str,
    owner: TokenOwner,
) -> Result<Option<ConnectorCredentials>, ConnectorError> {
    // Same `begin_account_scoped` scoping as `load_creds` — this variant is
    // the webhook/worker funnel, so scoping it here covers every background
    // caller (calendar, workflows, Teams, subscription renewal) at once.
    let mut tx = crate::tenancy::scope::begin_request_account_read(pool, account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("load_creds_bg scoped tx: {e}")))?;
    // Foundation PR 1.1: same explicit-owner-predicate fix as `load_creds`
    // above (see that function's comment for the last-writer-wins bug this
    // closes) — every background caller here passes `TokenOwner::AccountWide`
    // today.
    let row = match owner {
        TokenOwner::AccountWide => {
            sqlx::query_as::<_, TokenRow>(
                r#"
                SELECT access_token, refresh_token, expires_at, provider_workspace_id, updated_at
                FROM oauth_tokens
                WHERE account_id = $1 AND provider = $2 AND owner_user_id IS NULL
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .bind(provider)
            .fetch_optional(&mut *tx)
            .await
        }
        TokenOwner::Member(owner_user_id) => {
            sqlx::query_as::<_, TokenRow>(
                r#"
                SELECT access_token, refresh_token, expires_at, provider_workspace_id, updated_at
                FROM oauth_tokens
                WHERE account_id = $1 AND provider = $2 AND owner_user_id = $3
                ORDER BY updated_at DESC
                LIMIT 1
                "#,
            )
            .bind(account_id)
            .bind(provider)
            .bind(owner_user_id)
            .fetch_optional(&mut *tx)
            .await
        }
    }
    .map_err(|e| ConnectorError::Internal(format!("load_creds_bg query: {e}")))?;
    tx.rollback().await.ok();

    let Some(row) = row else {
        return Ok(None);
    };

    // The KEY comes off `dek_plane`, NOT off `pool` — see this function's own
    // doc comment. `DekPlane::resolve` refuses a plane resolved for a
    // different account (`CryptoError::TenantMismatch`) rather than reading
    // some other tenant's key, so a mixed-up caller fails closed here.
    let dek = dek_plane
        .resolve(account_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("failed to resolve account DEK: {e}")))?
        .ok_or_else(|| {
            tracing::error!(
                account_id = %account_id,
                provider = %provider,
                dek_plane = ?dek_plane,
                connector_health = true,
                "load_creds_bg: no dek_wrapped on the resolved plane"
            );
            ConnectorError::Internal("account has no dek_wrapped provisioned".into())
        })?;
    let access_token = decrypt_token(&dek, &row.access_token)
        .map_err(|e| ConnectorError::Internal(format!("decrypt access_token: {e}")))?;
    let refresh_token = row
        .refresh_token
        .as_deref()
        .and_then(|enc| decrypt_token(&dek, enc).ok());

    Ok(Some(ConnectorCredentials {
        access_token,
        refresh_token,
        expires_at: row.expires_at,
        provider_workspace_id: row.provider_workspace_id.unwrap_or_default(),
        refreshed: None,
        row_updated_at: Some(row.updated_at),
    }))
}

/// Persist any refreshed token, resolve the primary ocean, and ingest each item.
/// Shared tail of both `sync` and `ingest_selected`.
///
/// `tenant` is the request's REAL `Tenant` (org-tenancy PR 1.6, closing PR
/// 1.4's declared gap P1-2a — this used to build `Tenant::legacy` per item,
/// unconditionally funneling the ingest onto the consumer plane even for an
/// org-context caller). Both handlers reject an Org `TenancyDecision` unless
/// `reject_org_connector_access`'s per-provider flag lift applies (Microsoft
/// via m365 PR 3.3, Gmail item-ingest via gsuite G2 PR 6) — so an Org
/// `Tenant` genuinely reaches here for both. `pool` (the CONTENT pool:
/// `tenant.pool` under Org, the consumer pool under Personal) is used for
/// graph/atom resolution and the connector's own content-plane bookkeeping
/// (see `ingest_selected`'s own `pool`/`creds_pool` comments). `oauth_tokens`
/// — CONTROL-plane-only, unlike `pool` — is resolved via `state.control_db()`
/// directly below, never via `pool`.
#[allow(clippy::too_many_arguments)]
async fn finish_ingest(
    state: &Arc<AppState>,
    tenant: &Tenant,
    user_ctx: &UserContext,
    pool: &sqlx::PgPool,
    connector: &dyn ConnectorProvider,
    provider_key: &str,
    owner: TokenOwner,
    creds: &ConnectorCredentials,
    items: Vec<NormalizedItem>,
    // `follow` (gmail-options backend train, PR 1 of 4): the wire-level
    // `follow` knob, forwarded unchanged to `ConnectorProvider::
    // after_ingest`'s own `follow` parameter. `trigger_sync`'s call site
    // always passes `true` (see its own comment) — this route has no
    // `follow` knob of its own.
    follow: bool,
    follow_since: Option<chrono::DateTime<chrono::Utc>>,
) -> Response {
    let account_id = user_ctx.account_id;

    // Persist a refreshed token, if the provider rotated it mid-run
    // (non-fatal). `provider_key`/`owner` — the SAME pair the credentials
    // were actually loaded under (gsuite G2 PR 3 for the provider dimension,
    // m365 PR 3.2 for the owner dimension), NOT unconditionally
    // `(connector.id(), AccountWide)`: persisting under the wrong pair would
    // silently create-or-clobber a stray row while the row the token was
    // actually refreshed FROM goes stale — the next read then loads the old,
    // now-invalid token.
    //
    // `state.control_db()` (gsuite Train G2 PR 6), NOT `pool`: `oauth_tokens`
    // is CONTROL-plane-only (see `ingest_selected`'s own `creds_pool`
    // comment) — `pool` is the CONTENT pool, the org's tenant pool under
    // Org since this PR's Gmail carve-out. Personal callers are unaffected
    // (`state.control_db()` IS `pool` there).
    if let Some(refreshed) = creds.refreshed.as_ref() {
        match state.control_db() {
            Some(creds_pool) => {
                // The ROW's pool is control (`creds_pool`); the KEY's plane is
                // the request's own — `DekPlane::from_decision` yields the org's
                // tenant plane under an Org decision, which is exactly the
                // Microsoft/Gmail org carve-out that reaches here. Pinning it to
                // `consumer` would have re-encrypted an org account's rotated
                // token under a key no correct reader ever consults.
                let dek_plane =
                    crate::crypto::DekPlane::from_decision(tenant.decision.as_ref(), creds_pool);
                if let Err(e) = store_token_bundle(
                    creds_pool,
                    &dek_plane,
                    account_id,
                    provider_key,
                    owner,
                    refreshed,
                )
                .await
                {
                    tracing::warn!(
                        account_id = %account_id,
                        provider = %provider_key,
                        status = ?e.status(),
                        "failed to persist refreshed token"
                    );
                }
            }
            None => {
                tracing::warn!(
                    account_id = %account_id,
                    provider = %provider_key,
                    "failed to persist refreshed token: no control pool configured"
                );
            }
        }
    }

    // Attributable (member-private-oceans PR 5, plan §3): this handler carries
    // a verified `user_ctx`, a real end user acting through their own
    // session, on either plane. Plane is derived via `plane_of` (previously
    // hardcoded `AccountPlane::Personal`): `reject_org_connector_access`
    // above already confirmed the org's per-provider flag, so this is
    // genuinely reachable under `AccountPlane::Org` for Microsoft (m365 PR
    // 3.3) and Gmail item-ingest (gsuite G2 PR 6) — every other provider
    // still resolves `Personal` here. Either way this resolves to the
    // connecting member's OWN private graph — never a shared org ocean —
    // the routing mechanism itself is unchanged, only the plane it now
    // genuinely executes under.
    let graph_id = match resolve_primary_graph(
        pool,
        account_id,
        plane_of(tenant),
        IngestCaller::Attributable(user_ctx.user_id),
    )
    .await
    {
        Ok(g) => g,
        Err(e) => {
            tracing::error!(error = %e, account_id = %account_id, "failed to resolve primary graph");
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response();
        }
    };

    let fetched = items.len();
    let mut ingested = 0usize;
    let blobs = state.r2.as_deref().map(|r| r as &dyn crate::services::blob_store::PromotionBlobStore);
    for item in &items {
        // Calendar event EDITS on the manual "sync now" path — Google
        // (`POST /v1/connectors/google/sync`) and Microsoft (`POST
        // /v1/connectors/microsoft/sync`, personal plane AND an org member's
        // own private ocean via `plane_of` above) alike: the same
        // retract-stale-coral-first step `routes::google_calendar_push::
        // ingest_and_commit` and `workers::microsoft_calendar_sync::
        // sync_member` perform on their scheduled paths (see
        // `services::source_reingest`'s module doc). `ingest_atom`'s insert
        // is `DO NOTHING` on the event's source id, so a renamed/moved event
        // would otherwise be silently skipped. Calendar events only
        // (`source_reingest::is_calendar_event`): no other source type
        // reaching this loop has an "edited upstream" semantic, so no other
        // item pays the lookup. A failure is logged and the item skipped —
        // this human-driven route commits its cursor unconditionally (see
        // `google_calendar::sync`), exactly as a failed `ingest_atom` is
        // treated right below.
        match crate::services::source_reingest::retract_if_changed_item(
            pool, blobs, account_id, graph_id, item,
        )
        .await
        {
            Ok(Some(crate::services::source_reingest::ReingestOutcome::Replaced { atoms_deleted, reefs_deleted })) => {
                tracing::info!(
                    account_id = %account_id, provider = %connector.id(), source_id = %item.source_id,
                    atoms_deleted, reefs_deleted,
                    "connector sync: calendar event changed upstream — stale coral retracted, re-ingesting"
                );
            }
            Ok(_) => {}
            Err(e) => {
                tracing::warn!(
                    error = %e, account_id = %account_id, provider = %connector.id(), source_id = %item.source_id,
                    "connector sync: could not check/retract the stored coral for a changed calendar event — skipping it"
                );
                continue;
            }
        }
        let req = IngestAtomRequest {
            graph_id,
            source_type: item.source_type.clone(),
            source_id: item.source_id.clone(),
            content: item.content.clone(),
            participants: item.participants.clone(),
            timestamp: item.timestamp,
            scope: item.scope.clone(),
            fts_language: None,
            channel_id: item.channel_id.clone(),
            channel_name: None,
            author_id: item.author_id.clone(),
            author_name: item.author_name.clone(),
            project: None,
            mcp_client_name: None,
            raw_content: None,
            duration_ms: None,
            segments: None,
            session_id: None,
            session_kind: None,
            session_title: None,
            repo_path: None,
            remote: None,
            hostname: None,
            harness_version: None,
            model: None,
            branch: None,
            agent_name: None,
            platform: None,
            task: None,
            parent_session_id: None,
            trusted_user_agent: None,
            client_info: None,
            code_path: None,
        };
        // The request's REAL `Tenant` (org-tenancy PR 1.6; was
        // `Tenant::legacy` — gap P1-2a, resolved). Always Personal here
        // thanks to the handlers' org gate, so behavior is byte-identical to
        // the legacy funnel — but the plane now provably follows the
        // request's own `TenancyDecision` instead of being hardcoded.
        let source_namespace = if provider_key == "google_gmail" {
            crate::connectors::google_gmail::rate_key(account_id, &creds.provider_workspace_id)
        } else {
            format!("{}:connection:{}:{}", provider_key, account_id, user_ctx.user_id)
        };
        let origin = match crate::source_origin::native_connector_origin(provider_key,&item.source_type,"native_connector_selected")
            .and_then(|origin|origin.map(|o|o.with_oauth_grant(account_id,provider_key,&creds.provider_workspace_id,owner.owner_user_id())).transpose()) {
            Ok(origin) => origin,
            Err(error) => { tracing::warn!(%error,"invalid verified connector origin"); continue; }
        };
        let resp = crate::ingest::submit(crate::ingest::IngestRequest {
            origin: origin.as_ref(),
            ..crate::ingest::IngestRequest::private(crate::ingest::Member {
                provider_namespace: Some(&source_namespace),
                ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
            })
        }).await;
        match resp {
            Ok(_) => ingested += 1,
            Err(refused) => tracing::warn!(
                error = %refused,
                provider = %connector.id(),
                "ingest_atom failed for a connector item"
            ),
        }
    }

    // Best-effort provider side effect: Microsoft registers the just-ingested
    // conversations for reply auto-capture and ensures a live Graph subscription
    // (CS-117). Failures here never fail the ingest the user already saw succeed.
    //
    // `control_pool` (gsuite Train G2 PR 7): resolved once here, exactly like
    // the token-refresh persist a few lines above — `state.control_db()`,
    // never `pool` (the CONTENT pool, the org's tenant pool under Org). Gmail's
    // own `after_ingest` override needs it for `google_watch_channels`'
    // watch self-heal (control-plane-only, same class as `oauth_tokens`);
    // every other connector ignores the parameter.
    let control_pool = state.control_db();
    if let Err(e) = connector
        .after_ingest(
            &state.http_client,
            pool,
            account_id,
            user_ctx.user_id,
            graph_id,
            owner,
            creds,
            &items,
            control_pool,
            follow,
            follow_since,
        )
        .await
    {
        tracing::warn!(
            error = %e,
            account_id = %account_id,
            provider = %connector.id(),
            "after_ingest side effect failed (ingest itself succeeded)"
        );
    }

    tracing::info!(
        account_id = %account_id,
        provider = %connector.id(),
        fetched,
        ingested,
        "connector ingest complete"
    );

    (
        StatusCode::OK,
        Json(json!({ "ingested": ingested, "fetched": fetched })),
    )
        .into_response()
}

fn unknown_provider(provider: &str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": format!("unknown provider: {provider}")})),
    )
        .into_response()
}

/// Map a connector error to an HTTP status.
fn connector_error_response(e: ConnectorError) -> Response {
    match e {
        ConnectorError::NotConfigured(msg) => {
            (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::NotImplemented(msg) => {
            (StatusCode::NOT_IMPLEMENTED, Json(json!({"error": msg}))).into_response()
        }
        ConnectorError::Exchange(msg) | ConnectorError::UpstreamStatus { message: msg, .. } => {
            tracing::warn!(error = %msg, "connector upstream failure");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "failed to reach provider"})),
            )
                .into_response()
        }
        ConnectorError::Internal(msg) => {
            tracing::error!(error = %msg, "connector internal failure");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({"error": "internal error"})),
            )
                .into_response()
        }
        ConnectorError::CredentialRevoked { provider, reason } => {
            // CS-453: a definite dead-credential signal, not an upstream
            // hiccup — `409` (not `502` or `401`), so a client can
            // distinguish "retry later" from "this needs a reconnect"
            // without `web/lib/api.ts` treating it as an expired Clerk
            // session. `mark_credential_invalid` for the exact
            // `(account_id, provider, owner)` row is the caller's job (it
            // has that context; this function does not) — see
            // `trigger_sync`/`ingest_selected`'s own call sites.
            tracing::warn!(%provider, reason = reason.code(), "connector credential revoked");
            (
                StatusCode::CONFLICT,
                Json(json!({"error": "credential revoked", "reason": reason.code()})),
            )
                .into_response()
        }
    }
}

/// Row shape for the stored-token lookup.
#[derive(sqlx::FromRow)]
struct TokenRow {
    access_token: Vec<u8>,
    refresh_token: Option<Vec<u8>>,
    expires_at: Option<chrono::DateTime<chrono::Utc>>,
    provider_workspace_id: Option<String>,
    updated_at: chrono::DateTime<chrono::Utc>,
}

// ---------------------------------------------------------------------------
// gsuite Train G2 PR 6: `reject_org_connector_access`'s Gmail carve-out
// ---------------------------------------------------------------------------
//
// No DB needed for the gate decision itself: `state.db` is `None` on
// `crate::test_app_state()`, so `crate::connector_flags::resolve` falls back
// to the raw `ORG_GOOGLE_GMAIL_ENABLED` env default (that fn's own doc
// comment) — same no-control-pool fallback path `routes::oauth`'s
// `google_org_callback_allowed`/`reject_org_gmail_management` tests already
// exercise for the OAuth-management half of this same PR.
/// Request defaults and the actual HTTP rejection status for unknown scope.
/// The extractor test exercises the same parser the ingest handler uses:
/// plain Axum JSON data rejections are `422`, so serde-only assertions would
/// miss a regression in the explicit `400` translation above.
#[cfg(test)]
mod ingest_selected_request_serde_tests {
    use super::*;

    #[test]
    fn both_knobs_omitted_default_to_thread_and_follow_true() {
        let body: IngestSelectedRequest = serde_json::from_str(r#"{"item_ids": ["m1"]}"#).unwrap();
        assert_eq!(body.scope, IngestScope::Thread);
        assert!(body.follow);
    }

    #[test]
    fn explicit_message_scope_and_follow_false_round_trip() {
        let body: IngestSelectedRequest =
            serde_json::from_str(r#"{"item_ids": ["m1"], "scope": "message", "follow": false}"#)
                .unwrap();
        assert_eq!(body.scope, IngestScope::Message);
        assert!(!body.follow);
    }

    #[test]
    fn explicit_thread_scope_and_follow_true_round_trip() {
        let body: IngestSelectedRequest =
            serde_json::from_str(r#"{"item_ids": ["m1"], "scope": "thread", "follow": true}"#)
                .unwrap();
        assert_eq!(body.scope, IngestScope::Thread);
        assert!(body.follow);
    }

    #[test]
    fn unknown_scope_value_fails_to_deserialize_rather_than_defaulting() {
        let result: Result<IngestSelectedRequest, _> =
            serde_json::from_str(r#"{"item_ids": ["m1"], "scope": "conversation"}"#);
        assert!(
            result.is_err(),
            "an unrecognized scope string must fail to deserialize (→ parse_ingest_request maps this to a 400), never silently fall back to the default"
        );
    }

    #[tokio::test]
    async fn unknown_scope_returns_bad_request_over_http() {
        use axum::{body::Body, http::Request, routing::post, Router};
        use tower::ServiceExt;
        let app = Router::new().route("/ingest", post(|body: Result<Json<IngestSelectedRequest>, JsonRejection>| async {
            match parse_ingest_request(body) {
                Ok(_) => StatusCode::OK.into_response(),
                Err(response) => response,
            }
        }));
        let response = app.oneshot(Request::builder()
            .method("POST")
            .uri("/ingest")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"item_ids":["m1"],"scope":"conversation"}"#))
            .unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn uppercase_scope_value_also_fails_lowercase_only() {
        // `#[serde(rename_all = "lowercase")]` — confirms the wire contract
        // ("lowercase" in the deliverable) is enforced, not accidentally
        // case-insensitive.
        let result: Result<IngestSelectedRequest, _> =
            serde_json::from_str(r#"{"item_ids": ["m1"], "scope": "Thread"}"#);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod reject_org_connector_access_tests {
    use super::*;

    /// Serializes every test in this module that mutates
    /// `ORG_GOOGLE_GMAIL_ENABLED` — mirrors `routes::oauth::tests::
    /// GOOGLE_SURFACE_ENV_LOCK` exactly (same "concurrent `#[tokio::test]`s,
    /// one process-global env var" hazard).
    static ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    fn org_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: "org_gmail_ingest_gate_test".to_string(),
                    account_id: uuid::Uuid::new_v4(),
                    user_id: uuid::Uuid::new_v4(),
                    pool: sqlx::postgres::PgPoolOptions::new()
                        .connect_lazy("postgres://localhost/unused")
                        .expect("lazy pool construction never connects"),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn personal_tenant() -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    async fn with_flag<T>(value: Option<&str>, f: impl std::future::Future<Output = T>) -> T {
        let _guard = ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_GMAIL_ENABLED").ok();
        match value {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        let out = f.await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_GMAIL_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_GMAIL_ENABLED") },
        }
        out
    }

    #[tokio::test]
    async fn org_ingest_selected_google_flag_on_is_allowed() {
        let resp = with_flag(Some("true"), async {
            reject_org_connector_access(&crate::test_app_state(), &org_tenant(), "google", true)
                .await
        })
        .await;
        assert!(
            resp.is_none(),
            "org + flag on + ingest_selected + google(=Gmail) must complete"
        );
    }

    #[tokio::test]
    async fn org_ingest_selected_google_flag_off_is_refused() {
        let resp = with_flag(None, async {
            reject_org_connector_access(&crate::test_app_state(), &org_tenant(), "google", true)
                .await
        })
        .await;
        assert!(resp.is_some(), "org + flag off + gmail must stay refused");
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn org_trigger_sync_google_still_refused_even_with_flag_on() {
        // `trigger_sync`'s `provider == "google"` is ALWAYS Calendar
        // (`GoogleConnector::sync` unconditionally delegates to
        // `google_calendar::sync`) — the Gmail flag must not accidentally
        // widen it, since `is_item_ingest` is `false` here.
        let resp = with_flag(Some("true"), async {
            reject_org_connector_access(&crate::test_app_state(), &org_tenant(), "google", false)
                .await
        })
        .await;
        assert!(
            resp.is_some(),
            "org + flag on + trigger_sync(Calendar) must stay refused"
        );
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    /// Run `f` with `ORG_GOOGLE_CALENDAR_ENABLED` forced to `value`.
    async fn with_calendar_flag<T>(value: Option<&str>, f: impl std::future::Future<Output = T>) -> T {
        // The OAuth module's lock, not this module's `ENV_LOCK`: the Calendar
        // flag var is also mutated by `routes::oauth`'s tests.
        let _guard = crate::routes::oauth::GOOGLE_SURFACE_ENV_LOCK.lock().await;
        let prior = std::env::var("ORG_GOOGLE_CALENDAR_ENABLED").ok();
        match value {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
        }
        let out = f.await;
        match prior {
            Some(v) => unsafe { std::env::set_var("ORG_GOOGLE_CALENDAR_ENABLED", v) },
            None => unsafe { std::env::remove_var("ORG_GOOGLE_CALENDAR_ENABLED") },
        }
        out
    }

    /// Google Calendar org lift: `trigger_sync`'s `google` (always Calendar)
    /// is allowed under an org whose `google_calendar` flag is on...
    #[tokio::test]
    async fn org_trigger_sync_google_calendar_flag_on_is_allowed() {
        let resp = with_calendar_flag(Some("true"), async {
            reject_org_connector_access(&crate::test_app_state(), &org_tenant(), "google", false)
                .await
        })
        .await;
        assert!(resp.is_none(), "org + calendar flag on + trigger_sync(google) must be admitted");
    }

    /// ...and refused when it is off (the default).
    #[tokio::test]
    async fn org_trigger_sync_google_calendar_flag_off_is_refused() {
        let resp = with_calendar_flag(None, async {
            reject_org_connector_access(&crate::test_app_state(), &org_tenant(), "google", false)
                .await
        })
        .await;
        assert_eq!(resp.expect("flag off must refuse").status(), StatusCode::FORBIDDEN);
    }

    /// The Calendar flag opens Calendar's `sync` route ONLY: not Gmail's
    /// item-ingest arm, and not any other provider.
    #[tokio::test]
    async fn org_calendar_flag_never_opens_gmail_item_ingest_or_other_providers() {
        let (gmail_ingest, ms_sync, ms_ingest) = with_calendar_flag(Some("true"), async {
            let state = crate::test_app_state();
            (
                reject_org_connector_access(&state, &org_tenant(), "google", true).await,
                reject_org_connector_access(&state, &org_tenant(), "microsoft", false).await,
                reject_org_connector_access(&state, &org_tenant(), "microsoft", true).await,
            )
        })
        .await;
        assert!(gmail_ingest.is_some(), "the Calendar flag must not admit Gmail's item ingest");
        assert!(ms_sync.is_some() && ms_ingest.is_some(), "the Calendar flag must not admit Microsoft");
    }

    #[tokio::test]
    async fn org_ingest_selected_microsoft_still_refused_even_with_gmail_flag_on() {
        let resp = with_flag(Some("true"), async {
            reject_org_connector_access(
                &crate::test_app_state(),
                &org_tenant(),
                "microsoft",
                true,
            )
            .await
        })
        .await;
        assert!(
            resp.is_some(),
            "the Gmail flag must not leak into any other provider's org gate"
        );
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn personal_plane_is_noop_regardless_of_flag_or_route() {
        for (provider, is_item_ingest) in [("google", true), ("google", false), ("microsoft", true)]
        {
            let resp = with_flag(None, async {
                reject_org_connector_access(
                    &crate::test_app_state(),
                    &personal_tenant(),
                    provider,
                    is_item_ingest,
                )
                .await
            })
            .await;
            assert!(
                resp.is_none(),
                "personal plane must be unaffected for provider={provider} \
                 is_item_ingest={is_item_ingest}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Shared scaffolding for this file's pg-gated test modules
// ---------------------------------------------------------------------------
//
// CodeRabbit round 1 on PR 3.5 (#703): `microsoft_owner_aware_pg_tests`,
// `reject_org_connector_access_pg_tests`, and
// `microsoft_org_plane_ingest_pg_tests` each carried a verbatim copy of
// these helpers — a fix to `with_db`'s query-string handling or `teardown`
// would have had to land three times. One copy, `use`d by all three; the
// same consolidation `routes::teams_ingest::pg_test_support` already
// models. Helpers a single module genuinely owns (owner-aware's `store`,
// the gate module's `state_with_control`, the ingest module's `two_dbs`/
// `seed_member_graph`) deliberately stay local to it.
#[cfg(test)]
mod connectors_pg_test_support {
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use sqlx::{Connection, Executor, PgConnection, PgPool};

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
    pub(super) async fn teardown(base: &str, db: &str, pool: PgPool) {
        pool.close().await;
        let mut admin_conn = PgConnection::connect(base).await.unwrap();
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
    /// Same fixed-KEK convention as `routes::oauth`'s own pg test module.
    pub(super) fn set_test_kek() {
        // SAFETY: every test in this process sets `MASTER_KEK_HEX` to the
        // SAME constant, so a concurrent write from another test is
        // benign (identical bytes).
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }
    pub(super) async fn seed_user(pool: &PgPool) -> uuid::Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", uuid::Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }
    pub(super) async fn seed_account_with_dek(pool: &PgPool, owner: uuid::Uuid) -> uuid::Uuid {
        let kek = load_kek().expect("test KEK loads");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap test dek");
        sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&wrapped)
        .fetch_one(pool)
        .await
        .unwrap()
    }
    pub(super) fn outlook_bundle(label: &str) -> crate::connectors::TokenBundle {
        crate::connectors::TokenBundle {
            access_token: format!("access-{label}"),
            refresh_token: Some(format!("refresh-{label}")),
            user_token: None,
            scope: "offline_access openid profile Mail.Read Calendars.Read".into(),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: format!("tenant-{label}"),
            provider_workspace_name: Some(label.to_string()),
            provider_user_id: format!("oid-{label}"),
        }
    }
    pub(super) async fn set_flag(pool: &PgPool, org_id: &str, provider: &str, enabled: bool) {
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
}

// ---------------------------------------------------------------------------
// PR 3.2: `load_creds_owner_aware` member/AccountWide fallback (pg-gated)
// ---------------------------------------------------------------------------
//
// Against a REAL Postgres database: proves the exact fallback
// `load_creds_owner_aware` performs — a caller's own `Member` row wins when
// present, a legacy `AccountWide` row is served when it is not, and one
// member's `Member` row never leaks to a DIFFERENT member with no row of
// their own (contract I7, same invariant `routes::oauth`'s
// `google_token_isolation_pg` module proves for Gmail). Same fixed-DB,
// fixed-KEK, recreate-per-test conventions as that module.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::connectors::microsoft_owner_aware_pg_tests \
//     -- --test-threads=1
//
// Declared directly at file scope (not nested inside a broader `mod tests`)
// so `scripts/check_pg_suite_coverage.sh`'s in-module parser — which
// deliberately refuses to trust a `..._pg_tests` module nested inside
// another inline module, since it cannot safely compute that suite's full
// path — can actually enumerate and verify this suite is wired into
// `ci.yml`, rather than needing the same non-`_pg_tests`-suffixed workaround
// `routes::oauth::tests::google_token_isolation_pg` uses to stay invisible
// to that check while nested inside its file's existing `mod tests`.
#[cfg(test)]
mod microsoft_owner_aware_pg_tests {
    use super::connectors_pg_test_support::{
        base_url, outlook_bundle, recreate, seed_account_with_dek, seed_user, set_test_kek,
        teardown, with_db,
    };
    use super::*;
    use crate::routes::oauth::store_token_bundle_tx;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::Connection;
    /// The exact write `callback` performs for a given owner: open a
    /// scoped tx, call the real `store_token_bundle_tx`, commit.
    async fn store(
        pool: &sqlx::PgPool,
        account_id: uuid::Uuid,
        owner: TokenOwner,
        bundle: &crate::connectors::TokenBundle,
    ) {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .expect("begin scoped tx");
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            "microsoft",
            owner,
            bundle,
        )
        .await
        .expect("store_token_bundle_tx");
        tx.commit().await.expect("commit");
    }

    #[tokio::test]
    async fn members_own_row_wins_over_a_coexisting_account_wide_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping members_own_row_wins_over_a_coexisting_account_wide_row_pg: \
                     set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "m365_32_member_wins";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: sqlx::PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        let member = seed_user(&pool).await;

        // A legacy AccountWide row (every Microsoft connection ever
        // written before this PR) coexists with a fresh Member row for
        // this specific member.
        store(
            &pool,
            account_id,
            TokenOwner::AccountWide,
            &outlook_bundle("legacy"),
        )
        .await;
        store(
            &pool,
            account_id,
            TokenOwner::Member(member),
            &outlook_bundle("member-own"),
        )
        .await;

        let connector = connectors::lookup("microsoft").unwrap();
        let dek_cache = DekCache::new();
        let (creds, resolved_owner) =
            load_creds_owner_aware(&pool, &crate::crypto::DekPlane::consumer(pool.clone()), account_id, member, "microsoft", connector, TokenOwner::AccountWide, &dek_cache)
                .await
                .expect("load_creds_owner_aware should find the member's own row");

        assert_eq!(resolved_owner, TokenOwner::Member(member));
        assert_eq!(creds.access_token, "access-member-own");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn falls_back_to_account_wide_when_the_caller_has_no_member_row_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping falls_back_to_account_wide_when_the_caller_has_no_member_row_pg: \
                     set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "m365_32_legacy_fallback";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: sqlx::PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        let caller = seed_user(&pool).await;

        // Only the legacy AccountWide row exists — the overwhelming
        // common case today (every Microsoft connection this codebase
        // has ever written).
        store(
            &pool,
            account_id,
            TokenOwner::AccountWide,
            &outlook_bundle("legacy"),
        )
        .await;

        let connector = connectors::lookup("microsoft").unwrap();
        let dek_cache = DekCache::new();
        let (creds, resolved_owner) =
            load_creds_owner_aware(&pool, &crate::crypto::DekPlane::consumer(pool.clone()), account_id, caller, "microsoft", connector, TokenOwner::AccountWide, &dek_cache)
                .await
                .expect("load_creds_owner_aware should fall back to the account-wide row");

        assert_eq!(resolved_owner, TokenOwner::AccountWide);
        assert_eq!(creds.access_token, "access-legacy");

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn one_members_row_never_leaks_to_a_different_member_with_none_of_their_own_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping one_members_row_never_leaks_to_a_different_member_with_none_of_\
                     their_own_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let db = "m365_32_no_cross_member_leak";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: sqlx::PgPool = PgPoolOptions::new()
            .connect(&with_db(&base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();

        let owner = seed_user(&pool).await;
        let account_id = seed_account_with_dek(&pool, owner).await;
        let member_a = seed_user(&pool).await;
        let member_b = seed_user(&pool).await;

        // Member A has a real per-member row; NO AccountWide row exists
        // at all for this account, so Member B (whose lookup must never
        // cross into Member A's row) has nothing to fall back to.
        store(
            &pool,
            account_id,
            TokenOwner::Member(member_a),
            &outlook_bundle("member-a"),
        )
        .await;

        let connector = connectors::lookup("microsoft").unwrap();
        let dek_cache = DekCache::new();
        let resp = load_creds_owner_aware(&pool, &crate::crypto::DekPlane::consumer(pool.clone()), account_id, member_b, "microsoft", connector, TokenOwner::AccountWide, &dek_cache).await;

        assert!(
            resp.is_err(),
            "member B must never resolve member A's token or any other row"
        );
        let resp = resp.err().unwrap();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);

        teardown(&base, db, pool).await;
    }
}

// ---------------------------------------------------------------------------
// m365 enterprise train PR 3.3: `reject_org_connector_access` gate lift (pg-gated)
// ---------------------------------------------------------------------------
//
// Against a REAL Postgres database: proves the exact provider+flag gate this
// PR added — `provider == "microsoft"` reaches through once the org's
// `org_connector_flags` row (registry provider `"microsoft"`, the SAME row
// `routes::teams_ingest::org_microsoft_teams_enabled_for` already
// established) resolves on, every OTHER provider keeps refusing
// unconditionally regardless of that same flag, and a zero-row registry
// (flag off, the default) stays byte-identical to the pre-PR unconditional
// 403. Same fixed-DB, `AppState` fixture conventions as
// `routes::teams_ingest::org_microsoft_teams_flag_pg_tests`, which this
// mirrors — declared here (not reused from there) because `reject_org_
// connector_access` and its test fixture are `routes::connectors`-local,
// same "each file's own scratch-DB scaffolding" convention every sibling
// `*_pg_tests` module in this tree already follows.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::connectors::reject_org_connector_access_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod reject_org_connector_access_pg_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgPool};
    use std::sync::Arc;

    use super::connectors_pg_test_support::{base_url, recreate, set_flag, teardown, with_db};
    use super::reject_org_connector_access;
    use crate::tenancy::extractor::Tenant;
    use crate::tenancy::runtime::{TenancyDecision, TenantContext};
    use crate::AppState;

    fn state_with_control(pool: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(pool, None))
    }
    /// Same shape `workers::graph_ingest::process_job` builds for a genuine
    /// org tick — a real `TenancyDecision::Org`, not a hand-rolled stand-in.
    fn org_tenant(org_id: &str, account_id: uuid::Uuid, pool: PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: org_id.to_string(),
                account_id,
                user_id: uuid::Uuid::new_v4(),
                pool,
                page_pool: None,
            })),
            class: crate::route_class::RouteClass::Other,
        }
    }
    #[tokio::test]
    async fn flag_off_org_stays_403_for_microsoft_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping flag_off_org_stays_403_for_microsoft_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_t33_connector_gate_flag_off";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let state = state_with_control(pool.clone());
        let account_id = uuid::Uuid::new_v4();
        let tenant = org_tenant("org_t33_gate_flag_off", account_id, pool.clone());

        let resp = reject_org_connector_access(&state, &tenant, "microsoft", false).await;
        assert!(
            resp.is_some(),
            "a zero-row org_connector_flags registry (flag off, the default) must stay a 403 — \
             byte-identical to before this PR"
        );
        assert_eq!(resp.unwrap().status(), super::StatusCode::FORBIDDEN);

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn flag_on_org_reaches_through_for_microsoft_but_not_other_providers_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping flag_on_org_reaches_through_for_microsoft_but_not_other_providers_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t33_connector_gate_flag_on";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let org_id = "org_t33_gate_flag_on";
        set_flag(&pool, org_id, "microsoft", true).await;

        let state = state_with_control(pool.clone());
        let account_id = uuid::Uuid::new_v4();
        let tenant = org_tenant(org_id, account_id, pool.clone());

        assert!(
            reject_org_connector_access(&state, &tenant, "microsoft", false)
                .await
                .is_none(),
            "the org's microsoft flag is on — the microsoft-provider route must reach through"
        );
        // Every other provider — Google here, standing in for "anything not
        // explicitly allow-listed" — keeps refusing unconditionally, flag or
        // no flag: this org has NO row for "google" at all, so it falls back
        // to that provider's env default. (Post-#695 merge: google DOES get
        // a registry consult on the item-ingest surface — `is_item_ingest =
        // false` here probes the sync surface, which stays refused for
        // google unconditionally; see the gate's own doc comment.)
        let resp = reject_org_connector_access(&state, &tenant, "google", false).await;
        assert!(
            resp.is_some(),
            "a DIFFERENT provider must stay 403 even though this org's microsoft flag is on"
        );
        assert_eq!(resp.unwrap().status(), super::StatusCode::FORBIDDEN);

        teardown(&base, db, pool).await;
    }

    #[tokio::test]
    async fn personal_plane_is_always_unaffected_pg() {
        // No DB needed at all: `tenant.org_context()` is `None` on Personal,
        // so this returns `None` (no refusal) before ever touching a flag —
        // same short-circuit `org_slack_oauth_enabled_for`'s own Personal-
        // plane callers rely on.
        let state = crate::test_app_state_owned(None);
        let tenant = Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        };
        assert!(
            reject_org_connector_access(&Arc::new(state), &tenant, "microsoft", false)
                .await
                .is_none(),
            "a Personal-plane tenant must never be refused by this org-only gate"
        );
    }
}

// ---------------------------------------------------------------------------
// m365 enterprise train PR 3.5: org-plane `ingest_selected` path, end to end
// (pg-gated)
// ---------------------------------------------------------------------------
//
// PR 3.3 proved the gate lift in isolation (`reject_org_connector_access_pg_
// tests` above) and the credential fallback in isolation (`microsoft_owner_
// aware_pg_tests` above), each against ONE scratch database. Neither proves
// the pieces are wired together correctly end to end against a database
// shape that actually distinguishes the two planes `ingest_selected` reads
// from — CONTROL (`oauth_tokens`, via `state.control_db()`) and CONTENT
// (`outlook_watched_conversations`/`graphs`, via `tenant.pool`) — which is
// exactly the m365 Train 2 PR 2.5 "cross-plane sweep" bug class this train
// keeps finding: a call site that silently reads the wrong pool once the two
// are genuinely different databases, not (as on every un-migrated org today)
// the same physical one. This module runs the REAL exported functions
// `ingest_selected`/`finish_ingest` call, in the same order, against TWO
// separate scratch Postgres databases — one `migrations_control`, one
// `migrations_tenant` — so a pool mix-up would fail loudly (a query against
// a table that does not exist on that plane) rather than silently passing
// because both planes happened to be the same database.
//
// Does NOT drive the handlers over HTTP or through `connector.fetch_items`/
// `after_ingest`'s `ensure_mail_subscriptions`: both call live Microsoft
// Graph, and this crate has no HTTP-mocking dependency for Graph (see
// `billing::stripe_client`'s own test module for the one HTTP-mock pattern
// this repo does have — a local axum server behind a configurable base URL;
// `connectors::microsoft` has no equivalent override hook, and adding one is
// out of this docs-and-tests PR's scope). What this DOES exercise, for
// real, against real Postgres: `reject_org_connector_access` (the gate),
// `load_creds_owner_aware` (Member-first / AccountWide-fallback credential
// read, against the CONTROL pool), `plane_of` + `resolve_primary_graph`
// (destination-ocean resolution, against the CONTENT/tenant pool), and
// `connectors::microsoft::upsert_watched_conversation` (the reply-auto-
// capture bookkeeping `after_ingest` performs, also against the tenant
// pool) — every non-Graph-calling step of the real request path, in the
// real order, with the real cross-plane pool split.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::connectors::microsoft_org_plane_ingest_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod microsoft_org_plane_ingest_pg_tests {
    use super::connectors_pg_test_support::{
        base_url, outlook_bundle, recreate, seed_account_with_dek, seed_user, set_flag,
        set_test_kek, teardown, with_db,
    };
    use super::*;
    use crate::routes::oauth::store_token_bundle_tx;
    use crate::tenancy::runtime::{TenancyDecision, TenantContext};
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgPool};

    /// The exact write `routes::oauth::callback` performs for a given owner,
    /// against the CONTROL pool — `oauth_tokens` has no tenant-plane twin
    /// that is ever actually written to (outlook.md §1 C11/§6).
    async fn store_token(
        control_pool: &PgPool,
        account_id: uuid::Uuid,
        owner: TokenOwner,
        bundle: &crate::connectors::TokenBundle,
    ) {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
            .await
            .expect("begin scoped tx");
        store_token_bundle_tx(
            &mut tx,
            &crate::crypto::DekPlane::consumer(control_pool.clone()),
            account_id,
            "microsoft",
            owner,
            bundle,
        )
        .await
        .expect("store_token_bundle_tx");
        tx.commit().await.expect("commit");
    }
    /// A `kind='member'` primary ocean for `owner_user_id`, on the TENANT
    /// pool — the precondition `resolve_primary_graph`'s Org+Attributable
    /// branch requires (it never auto-creates one — see that function's own
    /// doc comment). Same shape as `routes::teams_ingest::pg_test_support::
    /// seed_member_graph`.
    async fn seed_member_graph(
        tenant_pool: &PgPool,
        account_id: uuid::Uuid,
        owner_user_id: uuid::Uuid,
    ) -> uuid::Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'My Ocean', 'member', $2, true) \
             RETURNING id",
        )
        .bind(account_id)
        .bind(owner_user_id)
        .fetch_one(tenant_pool)
        .await
        .unwrap()
    }
    fn org_tenant(org_id: &str, account_id: uuid::Uuid, user_id: uuid::Uuid, tenant_pool: PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: Some(tenant_pool.clone()),
            decision: Some(TenancyDecision::Org(TenantContext {
                org_id: org_id.to_string(),
                account_id,
                user_id,
                pool: tenant_pool,
                page_pool: None,
            })),
            class: crate::route_class::RouteClass::Other,
        }
    }

    /// Two scratch databases for one test: `(control_pool, tenant_pool)`,
    /// migrated with their respective trees, `org_connector_flags` seeded on
    /// control, `tenant_meta` seeded on tenant (same fingerprint row
    /// `routes::teams_ingest::watch_conflict_pg_tests` seeds).
    async fn two_dbs(base: &str, control_db: &str, tenant_db: &str, org_id: &str) -> (PgPool, PgPool) {
        let control_conn = recreate(base, control_db).await;
        control_conn.close().await.ok();
        let control_pool: PgPool = PgPoolOptions::new().connect(&with_db(base, control_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();

        let tenant_conn = recreate(base, tenant_db).await;
        tenant_conn.close().await.ok();
        let tenant_pool: PgPool = PgPoolOptions::new().connect(&with_db(base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant_pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&tenant_pool)
            .await
            .unwrap();

        (control_pool, tenant_pool)
    }

    /// Happy path (brief item 1, sub-bullets 1/2/3): an org member with their
    /// OWN delegated `Member` token, org microsoft flag ON. Proves, against
    /// two REAL, genuinely distinct databases: the gate reaches through; the
    /// credential read resolves the member's own row from the CONTROL pool;
    /// the destination ocean resolves to the member's own tenant-pool
    /// primary ocean; and the watched-conversation upsert lands a REAL,
    /// non-zero row on the tenant pool with the right `graph_id`/
    /// `ingested_by` — the exact "silent zero-row under Org" bug class m365
    /// Train 2 PR 2.4 found elsewhere in this connector.
    #[tokio::test]
    async fn org_member_delegated_token_full_path_lands_in_own_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_member_delegated_token_full_path_lands_in_own_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let org_id = "org_t35_member_full_path";
        let (control_pool, tenant_pool) = two_dbs(
            &base,
            "orgt_t35_ingest_member_control",
            "orgt_t35_ingest_member_tenant",
            org_id,
        )
        .await;

        let owner = seed_user(&control_pool).await;
        let account_id = seed_account_with_dek(&control_pool, owner).await;
        let member = seed_user(&control_pool).await;
        store_token(&control_pool, account_id, TokenOwner::Member(member), &outlook_bundle("member")).await;
        set_flag(&control_pool, org_id, "microsoft", true).await;

        let graph_id = seed_member_graph(&tenant_pool, account_id, member).await;

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(control_pool.clone(), None));
        let tenant = org_tenant(org_id, account_id, member, tenant_pool.clone());

        // 1. Gate: org + microsoft flag on + item-ingest surface must reach
        //    through (the exact surface `ingest_selected` gates on — the
        //    existing `reject_org_connector_access_pg_tests` module only
        //    ever probes `is_item_ingest = false`, the `trigger_sync`/
        //    Calendar surface).
        assert!(
            reject_org_connector_access(&state, &tenant, "microsoft", true)
                .await
                .is_none(),
            "org + microsoft flag on + ingest_selected's item-ingest surface must reach through"
        );

        // 2. Credentials: `load_creds_owner_aware` against the CONTROL pool
        //    (never the tenant pool — `oauth_tokens` doesn't exist there in
        //    the way this connection was written) must resolve the CALLING
        //    member's own row.
        let connector = connectors::lookup("microsoft").unwrap();
        let (provider_key, trait_owner) = connector.fetch_items_credentials(member);
        let (creds, resolved_owner) = load_creds_owner_aware(
            state.control_db().unwrap(),
            // This module's fixture account carries a CONTROL-plane DEK (it
            // is not a tenant-DEK fixture — see `org_plane_dek_pg_tests` for
            // that invariant), so the credential read resolves the consumer
            // plane here, matching how `store_token` above wrote it.
            &crate::crypto::DekPlane::consumer(state.control_db().unwrap().clone()),
            account_id,
            member,
            provider_key,
            connector,
            trait_owner,
            &state.dek_cache,
        )
        .await
        .expect("load_creds_owner_aware must resolve the member's own row from the control pool");
        assert_eq!(resolved_owner, TokenOwner::Member(member));
        assert_eq!(creds.access_token, "access-member");

        // 3. Destination: the request's plane must derive to Org, and
        //    `resolve_primary_graph` (against the TENANT pool) must resolve
        //    to THIS member's own seeded primary ocean.
        assert_eq!(plane_of(&tenant), AccountPlane::Org);
        let resolved_graph = resolve_primary_graph(
            &tenant_pool,
            account_id,
            plane_of(&tenant),
            IngestCaller::Attributable(member),
        )
        .await
        .expect("resolve_primary_graph must resolve the member's own primary ocean");
        assert_eq!(resolved_graph, graph_id, "must land in the member's OWN ocean, not any other");

        // 4. Watched-conversation bookkeeping: `after_ingest`'s real
        //    write path, against the TENANT pool, must land a real row —
        //    not silently zero-row under an Org tenant pool.
        let conversation_id = "conv-member-full-path";
        connectors::microsoft::upsert_watched_conversation(
            &tenant_pool,
            account_id,
            member,
            resolved_graph,
            conversation_id,
            true,
        )
        .await
        .expect("upsert_watched_conversation must succeed against the tenant pool");
        let (row_count, stored_graph, stored_by): (i64, uuid::Uuid, uuid::Uuid) = sqlx::query_as(
            "SELECT count(*) OVER (), graph_id, ingested_by FROM outlook_watched_conversations \
             WHERE account_id = $1 AND conversation_id = $2",
        )
        .bind(account_id)
        .bind(conversation_id)
        .fetch_one(&tenant_pool)
        .await
        .expect("watched-conversation row must exist — not a silent zero-row under Org");
        assert_eq!(row_count, 1);
        assert_eq!(stored_graph, graph_id);
        assert_eq!(stored_by, member);

        // 5. The REAL handler tail (CodeRabbit round 1 on #703): steps 2-4
        //    re-run the plane split by hand, so a regression in WHICH pool
        //    `ingest_selected` passes where would not fail them. This step
        //    drives `finish_ingest` — the in-crate production function that
        //    receives the CONTENT pool and internally resolves credentials-
        //    persist against `state.control_db()` — with an EMPTY items vec:
        //    `after_ingest` returns early with no conversation ids (no Graph
        //    call), while `plane_of` + `resolve_primary_graph` run against
        //    the pool the production code actually chose. A pool mix-up here
        //    500s instead of returning OK.
        let user_ctx = UserContext {
            user_id: member,
            account_id,
            role: "member".into(),
            device_id: "test-device".into(),
        };
        let resp = finish_ingest(
            &state,
            &tenant,
            &user_ctx,
            &tenant_pool,
            connector,
            provider_key,
            resolved_owner,
            &creds,
            Vec::new(),
            true,
            None,
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "finish_ingest itself must succeed under Org with the real pool split"
        );

        teardown(&base, "orgt_t35_ingest_member_control", control_pool).await;
        teardown(&base, "orgt_t35_ingest_member_tenant", tenant_pool).await;
    }

    /// AccountWide fallback (brief item 1, sub-bullet 4): an org member with
    /// NO `Member` row of their own, only a legacy `AccountWide` row, must
    /// still complete the full path — same two-database shape as the happy
    /// path above.
    #[tokio::test]
    async fn org_member_account_wide_fallback_full_path_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_member_account_wide_fallback_full_path_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let org_id = "org_t35_account_wide_fallback";
        let (control_pool, tenant_pool) = two_dbs(
            &base,
            "orgt_t35_ingest_awfallback_control",
            "orgt_t35_ingest_awfallback_tenant",
            org_id,
        )
        .await;

        let owner = seed_user(&control_pool).await;
        let account_id = seed_account_with_dek(&control_pool, owner).await;
        let member = seed_user(&control_pool).await;
        // Only the legacy AccountWide row exists — no Member row for this
        // member at all.
        store_token(&control_pool, account_id, TokenOwner::AccountWide, &outlook_bundle("legacy")).await;
        set_flag(&control_pool, org_id, "microsoft", true).await;

        let graph_id = seed_member_graph(&tenant_pool, account_id, member).await;

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(control_pool.clone(), None));
        let tenant = org_tenant(org_id, account_id, member, tenant_pool.clone());

        assert!(
            reject_org_connector_access(&state, &tenant, "microsoft", true)
                .await
                .is_none()
        );

        let connector = connectors::lookup("microsoft").unwrap();
        let (provider_key, trait_owner) = connector.fetch_items_credentials(member);
        let (creds, resolved_owner) = load_creds_owner_aware(
            state.control_db().unwrap(),
            // This module's fixture account carries a CONTROL-plane DEK (it
            // is not a tenant-DEK fixture — see `org_plane_dek_pg_tests` for
            // that invariant), so the credential read resolves the consumer
            // plane here, matching how `store_token` above wrote it.
            &crate::crypto::DekPlane::consumer(state.control_db().unwrap().clone()),
            account_id,
            member,
            provider_key,
            connector,
            trait_owner,
            &state.dek_cache,
        )
        .await
        .expect("load_creds_owner_aware must fall back to the account-wide row");
        assert_eq!(resolved_owner, TokenOwner::AccountWide);
        assert_eq!(creds.access_token, "access-legacy");

        let resolved_graph = resolve_primary_graph(
            &tenant_pool,
            account_id,
            plane_of(&tenant),
            IngestCaller::Attributable(member),
        )
        .await
        .expect("resolve_primary_graph must still resolve the member's own ocean");
        assert_eq!(resolved_graph, graph_id);

        let conversation_id = "conv-account-wide-fallback";
        connectors::microsoft::upsert_watched_conversation(
            &tenant_pool,
            account_id,
            member,
            resolved_graph,
            conversation_id,
            true,
        )
        .await
        .expect("upsert_watched_conversation must succeed even on the fallback-credential path");
        let row_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outlook_watched_conversations \
             WHERE account_id = $1 AND conversation_id = $2",
        )
        .bind(account_id)
        .bind(conversation_id)
        .fetch_one(&tenant_pool)
        .await
        .unwrap();
        assert_eq!(row_count, 1);

        teardown(&base, "orgt_t35_ingest_awfallback_control", control_pool).await;
        teardown(&base, "orgt_t35_ingest_awfallback_tenant", tenant_pool).await;
    }

    /// Negative direction (brief item 1, sub-bullet 1): the org microsoft
    /// flag OFF must refuse `ingest_selected`'s own surface
    /// (`is_item_ingest = true`) — control-pool-only, no tenant pool or
    /// credentials needed since the gate refuses before either is touched.
    #[tokio::test]
    async fn org_flag_off_refuses_ingest_selected_surface_for_microsoft_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_flag_off_refuses_ingest_selected_surface_for_microsoft_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_t35_ingest_flag_off_control";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = recreate(&base, db).await;
        conn.close().await.ok();
        let control_pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control_pool).await.unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(control_pool.clone(), None));
        let account_id = uuid::Uuid::new_v4();
        // No `org_connector_flags` row at all — the default, flag-off state.
        let tenant = org_tenant("org_t35_flag_off", account_id, uuid::Uuid::new_v4(), control_pool.clone());

        let resp = reject_org_connector_access(&state, &tenant, "microsoft", true).await;
        assert!(
            resp.is_some(),
            "a zero-row org_connector_flags registry must refuse ingest_selected's own surface too"
        );
        assert_eq!(resp.unwrap().status(), StatusCode::FORBIDDEN);

        teardown(&base, db, control_pool).await;
    }
}

// ---------------------------------------------------------------------------
// Org-plane DEK: the `oauth_tokens` key lives in the TENANT database (pg-gated)
// ---------------------------------------------------------------------------
//
// The regression suite for the production 503 of 2026-09-06: connecting Slack
// while on an org account returned `{"error":"encryption key not configured"}`
// because both the WRITE (`routes::oauth::store_token_bundle_tx`) and the READ
// (`load_creds`) resolved the account DEK with `crypto::resolve_dek*`, which
// hardcodes `TenantId::Consumer` and therefore reads control's
// `accounts.dek_wrapped` — a column an org account does not have (its key was
// relocated to its own tenant DB's `account_keys`; see `crate::crypto`'s
// module doc and `clerk_provision::ensure_org_account`'s shadow-provisioning
// fence).
//
// Every test here runs against TWO genuinely distinct Postgres databases, and
// the org fixture deliberately carries a DECOY `accounts.dek_wrapped` on
// control that is a DIFFERENT key from the tenant one. That decoy is the whole
// point: without it, a cross-plane fallback would look like a pass (the read
// would find *a* key and succeed). With it, any fallback either decrypts to
// garbage or silently returns the wrong plaintext, and these assertions catch
// it. One fixture account additionally has NO control DEK at all, reproducing
// prod's exact shape (account 8e16ded0…, `clerk_org_id` set, `dek_wrapped`
// NULL) so the pre-fix 503 is asserted as a real, observed status code rather
// than described in prose.
//
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
//     cargo test -p coralswarm-backend --lib routes::connectors::org_plane_dek_pg_tests \
//     -- --test-threads=1
#[cfg(test)]
mod org_plane_dek_pg_tests {
    use super::connectors_pg_test_support::{
        base_url, outlook_bundle, recreate, seed_user, set_test_kek, with_db,
    };
    use super::*;
    use crate::crypto::DekPlane;
    use crate::routes::oauth::{store_token_bundle, store_token_bundle_tx};
    use crate::tenancy::runtime::{TenancyDecision, TenantContext};
    use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
    use coralswarm_core::oauth_token::decrypt_token;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgPool};

    const ORG_ID: &str = "org_dek_plane_regression";

    /// `(control_pool, tenant_pool)`, each its own scratch database with its
    /// own migration tree, plus the tenant fingerprint row. Same shape as
    /// `microsoft_org_plane_ingest_pg_tests::two_dbs`.
    async fn two_dbs(base: &str, control_db: &str, tenant_db: &str) -> (PgPool, PgPool) {
        let c = recreate(base, control_db).await;
        c.close().await.ok();
        let control: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, control_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&control)
            .await
            .unwrap();

        let t = recreate(base, tenant_db).await;
        t.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new()
            .connect(&with_db(base, tenant_db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_tenant")
            .run(&tenant)
            .await
            .unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(ORG_ID)
            .execute(&tenant)
            .await
            .unwrap();
        (control, tenant)
    }

    async fn drop_dbs(base: &str, dbs: [&str; 2], pools: [PgPool; 2]) {
        for p in pools {
            p.close().await;
        }
        let mut admin = sqlx::PgConnection::connect(base).await.unwrap();
        for db in dbs {
            use sqlx::Executor;
            admin
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// An ORG account, its DEK in the TENANT database's `account_keys`, and
    /// (when `decoy_control_dek`) a DIFFERENT wrapped DEK sitting on control
    /// as the negative control. Returns `(account_id, real_org_dek,
    /// decoy_control_dek)`.
    async fn seed_org_account(
        control: &PgPool,
        tenant: &PgPool,
        decoy_control_dek: bool,
    ) -> (uuid::Uuid, [u8; 32], Option<[u8; 32]>) {
        let kek = load_kek().expect("test KEK loads");
        let owner = seed_user(control).await;
        let decoy = decoy_control_dek.then(generate_dek);
        let decoy_wrapped = decoy.map(|d| wrap_dek(&kek, &d).expect("wrap decoy"));
        let account_id: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) \
             VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(owner)
        .bind(ORG_ID)
        .bind(&decoy_wrapped)
        .fetch_one(control)
        .await
        .unwrap();

        let org_dek = generate_dek();
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(wrap_dek(&kek, &org_dek).expect("wrap org dek"))
            .execute(tenant)
            .await
            .unwrap();
        (account_id, org_dek, decoy)
    }

    /// The exact write `routes::oauth::callback` performs, against the given
    /// DEK plane. Returns the handler `Response` on failure so a test can
    /// assert the STATUS the caller would have seen.
    async fn store(
        control: &PgPool,
        plane: &DekPlane,
        account_id: uuid::Uuid,
        owner: TokenOwner,
        bundle: &crate::connectors::TokenBundle,
    ) -> Result<(), Response> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .expect("begin scoped tx");
        store_token_bundle_tx(&mut tx, plane, account_id, "microsoft", owner, bundle).await?;
        tx.commit().await.expect("commit");
        Ok(())
    }

    /// The org's token round-trips through the TENANT plane's DEK, and the
    /// consumer plane — holding a decoy key for the very same account — can
    /// neither write nor read it. This is the store side and the load side of
    /// the same bug, asserted against one fixture so a half-fix cannot pass.
    #[tokio::test]
    async fn org_token_store_and_load_use_the_tenant_dek_never_control_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_token_store_and_load_use_the_tenant_dek_never_control_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_creds_ctl", "orgt_dekplane_creds_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (account_id, org_dek, decoy) = seed_org_account(&control, &tenant, true).await;
        let decoy = decoy.expect("decoy requested");
        assert_ne!(
            org_dek, decoy,
            "the fixture is only meaningful if the two planes hold DIFFERENT keys"
        );

        let org_plane = DekPlane::org(ORG_ID.to_string(), tenant.clone(), account_id);
        let consumer_plane = DekPlane::consumer(control.clone());
        let bundle = outlook_bundle("org");

        // ── Write: the org plane succeeds. ───────────────────────────────
        store(
            &control,
            &org_plane,
            account_id,
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("an org account's token must persist under its TENANT DEK");

        // The ROW lives on control (there is no live tenant twin) even though
        // its KEY does not — the two-axis split this fix exists to express.
        let stored: Vec<u8> = sqlx::query_scalar(
            "SELECT access_token FROM oauth_tokens WHERE account_id = $1 AND provider = 'microsoft'",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .expect("the oauth_tokens ROW is written to the CONTROL plane");
        assert_eq!(
            decrypt_token(&org_dek, &stored).expect("tenant DEK decrypts the stored token"),
            bundle.access_token,
        );
        assert!(
            decrypt_token(&decoy, &stored).is_err(),
            "the control-plane key must NOT decrypt an org account's token — if it does, the \
             write silently used the wrong plane"
        );

        // ── Read: the org plane decrypts; the consumer plane must not. ───
        let connector = connectors::lookup("microsoft").unwrap();
        let cache = DekCache::new();
        let creds = load_creds(
            &control,
            &org_plane,
            account_id,
            "microsoft",
            connector,
            TokenOwner::AccountWide,
            &cache,
        )
        .await
        .expect("load_creds must decrypt with the tenant DEK");
        assert_eq!(creds.access_token, bundle.access_token);

        let via_consumer = load_creds(
            &control,
            &consumer_plane,
            account_id,
            "microsoft",
            connector,
            TokenOwner::AccountWide,
            &cache,
        )
        .await;
        let err = via_consumer
            .err()
            .expect("the consumer plane must never hand out an org account's credentials");
        assert_eq!(
            err.status(),
            StatusCode::INTERNAL_SERVER_ERROR,
            "a wrong-plane key must surface as a decrypt failure, never a served token"
        );

        // The cache must not have conflated the two planes for this one
        // account id either (it is keyed on `(TenantId, account_id)`).
        assert_eq!(
            org_plane
                .resolve_cached(account_id, &cache)
                .await
                .unwrap()
                .unwrap(),
            org_dek
        );
        assert_eq!(
            consumer_plane
                .resolve_cached(account_id, &cache)
                .await
                .unwrap()
                .unwrap(),
            decoy
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// Prod's exact shape (`clerk_org_id` set, `accounts.dek_wrapped` NULL):
    /// the consumer plane reproduces the observed 503 verbatim, and the org
    /// plane is what makes the same write succeed. This is the test that would
    /// have caught the 2026-09-06 incident before it shipped.
    #[tokio::test]
    async fn consumer_plane_reproduces_the_503_for_an_org_account_with_no_control_dek_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping consumer_plane_reproduces_the_503_for_an_org_account_with_no_control_\
                 dek_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_503_ctl", "orgt_dekplane_503_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (account_id, _org_dek, decoy) = seed_org_account(&control, &tenant, false).await;
        assert!(decoy.is_none(), "this fixture has NO control-plane DEK");

        let bundle = outlook_bundle("no-control-dek");
        let err = store(
            &control,
            &DekPlane::consumer(control.clone()),
            account_id,
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .err()
        .expect("the pre-fix consumer-plane write must FAIL for an org account");
        assert_eq!(
            err.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "this is the prod symptom: 503 encryption key not configured"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                .bind(account_id)
                .fetch_one(&control)
                .await
                .unwrap(),
            0,
            "a refused write must leave no row behind"
        );

        store(
            &control,
            &DekPlane::org(ORG_ID.to_string(), tenant.clone(), account_id),
            account_id,
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("the tenant plane must make the identical write succeed");

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// `DekPlane::from_decision` — the constructor every gated route uses —
    /// routes an `Org` decision to the tenant plane and a `Personal`/absent
    /// decision to control, for the SAME account id. Plus the load-bearing
    /// account assert: a plane resolved for one account refuses another.
    #[tokio::test]
    async fn from_decision_routes_org_to_the_tenant_plane_and_asserts_the_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping from_decision_routes_org_to_the_tenant_plane_and_asserts_the_account_\
                 pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_decision_ctl", "orgt_dekplane_decision_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (account_id, org_dek, decoy) = seed_org_account(&control, &tenant, true).await;
        let decoy = decoy.expect("decoy requested");

        let ctx = TenantContext {
            org_id: ORG_ID.to_string(),
            account_id,
            user_id: uuid::Uuid::new_v4(),
            pool: tenant.clone(),
            page_pool: None,
        };
        let org_decision = TenancyDecision::Org(ctx);
        let org_plane = DekPlane::from_decision(Some(&org_decision), &control);
        assert!(org_plane.is_org());
        assert_eq!(org_plane.resolve(account_id).await.unwrap().unwrap(), org_dek);

        for decision in [None, Some(&TenancyDecision::Personal)] {
            let plane = DekPlane::from_decision(decision, &control);
            assert!(!plane.is_org());
            assert_eq!(plane.resolve(account_id).await.unwrap().unwrap(), decoy);
        }

        // Fail-closed: an org plane resolved for THIS account must refuse any
        // other account, before any DB access.
        let other = uuid::Uuid::new_v4();
        assert!(
            matches!(
                org_plane.resolve(other).await,
                Err(crate::crypto::CryptoError::TenantMismatch(_))
            ),
            "an org DekPlane must never serve a different account's DEK"
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// **The half-ship regression.** Fixing only the OAuth callback made org
    /// Slack connect SUCCEED and then silently ingest nothing: the token is
    /// written under the tenant DEK, every Slack read resolved the DEK on
    /// control, and because a row now EXISTS the deferred verdict in
    /// `workers::huddle::resolve_account_wide_oauth_token` fires
    /// `Err("account has no dek_wrapped provisioned")` — which every one of
    /// its callers swallows into a `warn!`/`error!`
    /// (`slack_interactivity` answers Slack `200 OK`). That is strictly worse
    /// than the honest 503 we started from, and invisible in the product.
    ///
    /// So this drives the REAL bot-token path end to end: write the token the
    /// way `callback` now does (tenant plane), then read it back through
    /// `get_slack_bot_token_for_workspace`/`get_slack_bot_token_pub` — the
    /// exact functions `slack_events`/`slack_interactivity`/`huddle` call —
    /// and assert the consumer plane cannot serve it.
    ///
    /// Also pins the `Err` vs `Ok(None)` distinction (#741) that
    /// `slack_token_rls_pg::slack_bot_token_lookup_succeeds_under_rls` owns:
    /// "no Slack install" must stay `Ok(None)` on the ORG plane too, not
    /// become an error just because the DEK now resolves elsewhere.
    #[tokio::test]
    async fn slack_bot_token_reads_the_tenant_dek_for_an_org_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_bot_token_reads_the_tenant_dek_for_an_org_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_slack_ctl", "orgt_dekplane_slack_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (account_id, org_dek, decoy) = seed_org_account(&control, &tenant, true).await;
        let decoy = decoy.expect("decoy requested");

        let org_plane = DekPlane::org(ORG_ID.to_string(), tenant.clone(), account_id);
        let consumer_plane = DekPlane::consumer(control.clone());

        // ── 1. No install yet ⇒ Ok(None) on BOTH planes, never an Err. ────
        // The org plane resolves a real DEK here, so this proves the
        // "nothing installed" answer does not depend on the key being absent.
        for (label, plane) in [("org", &org_plane), ("consumer", &consumer_plane)] {
            let none = crate::workers::huddle::get_slack_bot_token_pub(&control, plane, account_id)
                .await
                .unwrap_or_else(|e| panic!("{label} plane: no install must be Ok(None), got Err({e})"));
            assert!(none.is_none(), "{label} plane: no install must be Ok(None)");
        }

        // ── 2. Write the token exactly as the fixed `callback` does. ──────
        let bundle = crate::connectors::TokenBundle {
            access_token: "xoxb-org-plane-bot-token".to_string(),
            refresh_token: None,
            user_token: None,
            scope: "channels:history,chat:write".to_string(),
            expires_at: None,
            provider_workspace_id: "T0ORGPLANE".to_string(),
            provider_workspace_name: Some("Org Plane".to_string()),
            provider_user_id: "U0ORGPLANE".to_string(),
        };
        let mut tx = crate::tenancy::scope::begin_account_scoped(&control, account_id)
            .await
            .expect("begin scoped tx");
        store_token_bundle_tx(
            &mut tx,
            &org_plane,
            account_id,
            "slack",
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("org Slack connect must persist under the TENANT DEK");
        tx.commit().await.expect("commit");

        // ── 3. The live Slack read paths must decrypt it. ─────────────────
        // `get_slack_bot_token_for_workspace` backs routes::slack_events'
        // `ingest_slack_message`, routes::slack, workers::slack_name_backfill
        // and connectors::slack_disconnect; `get_slack_bot_token_pub` backs
        // the app_home/member-joined/interactivity/seat-reconcile paths.
        let by_workspace = crate::workers::huddle::get_slack_bot_token_for_workspace(
            &control,
            &org_plane,
            account_id,
            &bundle.provider_workspace_id,
        )
        .await
        .expect("the org plane must resolve the DEK the token was written under")
        .expect("a row exists, so this must be Some");
        assert_eq!(by_workspace, bundle.access_token);

        let account_wide =
            crate::workers::huddle::get_slack_bot_token_pub(&control, &org_plane, account_id)
                .await
                .expect("org plane bot-token lookup must succeed")
                .expect("a row exists, so this must be Some");
        assert_eq!(account_wide, bundle.access_token);

        // ── 4. The consumer plane must NOT serve it. ──────────────────────
        // The decoy control DEK exists and unwraps fine, so this is a genuine
        // wrong-key decrypt, not a missing-key shortcut — exactly the state
        // prod's `testorg` is in. An Err here (never `Ok(Some)`), because a
        // token that cannot be decrypted is not a token.
        let via_consumer = crate::workers::huddle::get_slack_bot_token_for_workspace(
            &control,
            &consumer_plane,
            account_id,
            &bundle.provider_workspace_id,
        )
        .await;
        assert!(
            via_consumer.is_err(),
            "the consumer plane must never hand out an org account's bot token, got {via_consumer:?}"
        );
        assert_ne!(
            decoy, org_dek,
            "the fixture is only meaningful if the two planes hold different keys"
        );

        // ── 5. And the pre-fix shape — an org account with NO control DEK —
        // must be an Err on the consumer plane, not a silent Ok(None). This
        // is the arm that made the half-ship invisible: a row exists, so
        // "no dek_wrapped" stops being a benign fact about the account.
        sqlx::query("UPDATE accounts SET dek_wrapped = NULL WHERE id = $1")
            .bind(account_id)
            .execute(&control)
            .await
            .unwrap();
        let err = crate::workers::huddle::get_slack_bot_token_pub(
            &control,
            &DekPlane::consumer(control.clone()),
            account_id,
        )
        .await
        .expect_err("a row with no consumer-plane key must be an Err, never Ok(None)");
        assert!(
            err.contains("dek_wrapped"),
            "the error must name the missing key, got: {err}"
        );
        // The org plane is unaffected by the control column going away.
        assert_eq!(
            crate::workers::huddle::get_slack_bot_token_pub(&control, &org_plane, account_id)
                .await
                .expect("org plane still resolves")
                .expect("row still present"),
            bundle.access_token
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// `bin/backfill_slack_oauth_tokens` must SKIP org accounts, not
    /// "helpfully" provision a control-plane DEK for them. That binary
    /// enumerates `SELECT id FROM accounts` — every account, orgs included —
    /// and its `ensure_account_dek` writes `accounts.dek_wrapped` when it is
    /// NULL, which for an org account is exactly the competing shared-plane
    /// key `clerk_provision::ensure_org_account`'s fence exists to prevent.
    /// It would then encrypt the legacy bot token under that key, manufacturing
    /// a fresh instance of the "connect succeeds, ingest silently does nothing"
    /// bug this PR removes. Asserted here because it is an operational
    /// landmine that only fires when someone runs the one-shot.
    #[tokio::test]
    async fn slack_oauth_backfill_skips_org_accounts_and_mints_no_control_dek_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping slack_oauth_backfill_skips_org_accounts_and_mints_no_control_dek_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_backfill_ctl", "orgt_dekplane_backfill_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        // Prod's exact shape: org account, NO control-plane DEK.
        let (org_account, _org_dek, decoy) = seed_org_account(&control, &tenant, false).await;
        assert!(decoy.is_none());

        // A legacy `slack_integrations`-only connection — the very thing the
        // backfill exists to convert.
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, 'T0LEGACYORG', 'Legacy Org', 'xoxb-legacy-plaintext')",
        )
        .bind(org_account)
        .execute(&control)
        .await
        .unwrap();

        let outcome =
            crate::slack_oauth_backfill::backfill_slack_oauth_tokens(&control, Some(org_account))
                .await;

        assert_eq!(outcome.accounts_seen, 1);
        assert_eq!(outcome.accounts_skipped_org, 1, "the org account must be skipped");
        assert_eq!(outcome.rows_written, 0, "no wrong-plane row may be written");
        assert_eq!(
            outcome.accounts_provisioned_dek, 0,
            "no competing control-plane DEK may be minted for an org account"
        );
        assert_eq!(outcome.errors, 0, "a deliberate skip is not an error");

        // The load-bearing assertion: the fence exists to keep this column NULL.
        let control_dek: Option<Vec<u8>> =
            sqlx::query_scalar("SELECT dek_wrapped FROM accounts WHERE id = $1")
                .bind(org_account)
                .fetch_one(&control)
                .await
                .unwrap();
        assert!(
            control_dek.is_none(),
            "the backfill must not have provisioned accounts.dek_wrapped for an org account"
        );
        assert_eq!(
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM oauth_tokens WHERE account_id = $1")
                .bind(org_account)
                .fetch_one(&control)
                .await
                .unwrap(),
            0
        );

        // A PERSONAL account on the same pool still backfills normally — the
        // fence must not have turned the binary into a no-op.
        let personal_owner = seed_user(&control).await;
        let personal = sqlx::query_scalar::<_, uuid::Uuid>(
            "INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id",
        )
        .bind(personal_owner)
        .fetch_one(&control)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO slack_integrations (account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, 'T0LEGACYPERSONAL', 'Legacy Personal', 'xoxb-legacy-personal')",
        )
        .bind(personal)
        .execute(&control)
        .await
        .unwrap();
        let personal_outcome =
            crate::slack_oauth_backfill::backfill_slack_oauth_tokens(&control, Some(personal)).await;
        assert_eq!(personal_outcome.accounts_skipped_org, 0);
        assert_eq!(
            personal_outcome.rows_written, 1,
            "a personal legacy connection must still be backfilled"
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// **Finding 1 (background reads) + finding 2 (refresh writes), together.**
    ///
    /// `load_creds_bg` and `store_token_bundle` are the webhook/worker pair —
    /// every Google/Microsoft change notification and every renewal tick goes
    /// through them. Both used to pin the DEK plane to `consumer`, so an org
    /// connection this PR's callback fix now permits could be CREATED but
    /// never read back and never re-persisted after a refresh: the connection
    /// died at its first token rotation.
    ///
    /// Asserted in one test so a half-fix cannot pass:
    /// * the org plane round-trips (`store_token_bundle` -> `load_creds_bg`),
    /// * the CONSUMER plane, holding a decoy key for the very same account,
    ///   cannot read that row — it errors rather than returning a wrong
    ///   plaintext (the negative control; without the decoy a cross-plane
    ///   fallback would "pass" by finding *a* key),
    /// * a consumer-plane WRITE is not silently readable on the tenant plane
    ///   either — the direction that used to corrupt rows invisibly.
    #[tokio::test]
    async fn bg_creds_round_trip_on_the_org_plane_and_never_across_planes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping bg_creds_round_trip_on_the_org_plane_and_never_across_planes_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_bg_ctl", "orgt_dekplane_bg_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        // `true` — control carries a DECOY DEK for this same account, so a
        // cross-plane read finds *a* key and must still fail.
        let (org_account, org_dek, decoy) = seed_org_account(&control, &tenant, true).await;
        let decoy = decoy.expect("fixture seeds a decoy control DEK");
        assert_ne!(org_dek, decoy, "the decoy must be a different key");

        let org_plane = DekPlane::org(ORG_ID.to_string(), tenant.clone(), org_account);
        let consumer_plane = DekPlane::consumer(control.clone());
        let bundle = outlook_bundle("org-refresh");

        // -- Finding 2: the bare-pool refresh wrapper now takes a plane, and
        //    on the org plane it persists under the TENANT dek.
        store_token_bundle(
            &control,
            &org_plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("an org account's refresh must persist on its own tenant plane");
        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT access_token FROM oauth_tokens WHERE account_id = $1")
                .bind(org_account)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            decrypt_token(&org_dek, &stored).unwrap(),
            bundle.access_token,
            "the row must be encrypted under the TENANT dek"
        );
        assert!(
            decrypt_token(&decoy, &stored).is_err(),
            "the row must NOT be decryptable with control's decoy key"
        );

        // -- Finding 1: the background READ succeeds on the org plane.
        let creds = load_creds_bg(
            &control,
            &org_plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .expect("an org background credential load must succeed")
        .expect("the row just written must be found");
        assert_eq!(creds.access_token, bundle.access_token);

        // -- NEGATIVE CONTROL: the same read on the CONSUMER plane must not
        //    succeed. Control holds a real, loadable DEK for this account
        //    (the decoy), so this is a genuine wrong-key attempt, not a
        //    missing-key one — exactly the silent corruption the plane
        //    parameter exists to prevent.
        let err = load_creds_bg(
            &control,
            &consumer_plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .err()
        .expect("a consumer-plane read of an ORG account's token must fail, not return a token");
        assert!(
            format!("{err}").contains("decrypt access_token"),
            "expected a decrypt failure under the wrong key, got: {err}"
        );

        // -- The write direction of the same cross-plane error: a
        //    consumer-plane refresh write lands under the decoy, and the
        //    org-plane reader can no longer decrypt it.
        let rotated = outlook_bundle("wrong-plane-rotation");
        store_token_bundle(
            &control,
            &consumer_plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
            &rotated,
        )
        .await
        .expect("the write itself succeeds — that is precisely why it is dangerous");
        let err = load_creds_bg(
            &control,
            &org_plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .err()
        .expect("a row written on the wrong plane must not silently read back");
        assert!(
            format!("{err}").contains("decrypt access_token"),
            "expected a decrypt failure, got: {err}"
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// The WORKER plumbing, end to end: `DekPlaneSource::for_worker_tenant`
    /// is what `workers::graph_subscriptions` and
    /// `workers::google_watch_renewal` build once per tick (they hold no
    /// `AppState`, so they cannot call `DekPlane::resolve_for_account`), and
    /// it must pick the ORG'S OWN tenant pool for an `Org` tenant and the
    /// CONTROL pool for `Consumer`.
    ///
    /// The negative control is the same decoy: control holds a real, loadable
    /// DEK for this very account, so a source that picked the wrong pool
    /// would find *a* key and the round trip would silently "work" with the
    /// wrong ciphertext. Both directions are asserted.
    #[tokio::test]
    async fn worker_plane_source_picks_the_tenant_pool_for_an_org_tick_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping worker_plane_source_picks_the_tenant_pool_for_an_org_tick_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_worker_ctl", "orgt_dekplane_worker_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (org_account, org_dek, decoy) = seed_org_account(&control, &tenant, true).await;
        let decoy = decoy.expect("fixture seeds a decoy control DEK");

        // Exactly what a `tick_renew` on an org tenant builds: the harness's
        // own `(tenant, WorkerTenant::pool)` pair plus the shared control
        // pool.
        let org_source = crate::crypto::DekPlaneSource::for_worker_tenant(
            &crate::tenancy::TenantId::Org(ORG_ID.to_string()),
            &tenant,
            &control,
        );
        let plane = org_source.plane_for(org_account);
        assert!(plane.is_org(), "an org tick must produce an org plane");

        let bundle = outlook_bundle("worker-tick");
        store_token_bundle(
            &control,
            &plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("a worker-resolved org plane must persist the refreshed token");

        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT access_token FROM oauth_tokens WHERE account_id = $1")
                .bind(org_account)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(decrypt_token(&org_dek, &stored).unwrap(), bundle.access_token);
        assert!(
            decrypt_token(&decoy, &stored).is_err(),
            "the worker source must NOT have used control's decoy key"
        );

        let creds = load_creds_bg(
            &control,
            &plane,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .expect("a worker-resolved org read must succeed")
        .expect("the row just written must be found");
        assert_eq!(creds.access_token, bundle.access_token);

        // `DekPlane::as_source` (the webhook -> shared-worker-helper bridge
        // `routes::graph_webhook::process_lifecycle` uses) round-trips back to
        // the same plane.
        let widened = plane.as_source().plane_for(org_account);
        let creds2 = load_creds_bg(
            &control,
            &widened,
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .expect("as_source must reproduce the same plane")
        .expect("the row must still be found");
        assert_eq!(creds2.access_token, bundle.access_token);

        // The Consumer arm takes the CONTROL pool — and on this org account
        // that means the decoy, which cannot read the row above. This is the
        // assertion that fails if `for_worker_tenant` ever collapses its two
        // pools into one.
        let consumer_source = crate::crypto::DekPlaneSource::for_worker_tenant(
            &crate::tenancy::TenantId::Consumer,
            &tenant,
            &control,
        );
        assert!(!consumer_source.plane_for(org_account).is_org());
        let err = load_creds_bg(
            &control,
            &consumer_source.plane_for(org_account),
            org_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .err()
        .expect("the consumer arm must not read an org account's tenant-encrypted row");
        assert!(
            format!("{err}").contains("decrypt access_token"),
            "expected a decrypt failure under the decoy, got: {err}"
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// A plane resolved for one account refuses another — `DekPlane`'s
    /// `expect_account` guard, asserted through the real background funnel
    /// rather than only at the type's own unit level. A mixed-up worker
    /// fails closed instead of resolving a second account's key.
    #[tokio::test]
    async fn bg_creds_refuse_a_plane_resolved_for_a_different_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping bg_creds_refuse_a_plane_resolved_for_a_different_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_bgmix_ctl", "orgt_dekplane_bgmix_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let (account_a, _dek_a, _) = seed_org_account(&control, &tenant, false).await;

        let bundle = outlook_bundle("mixed-up");
        store_token_bundle(
            &control,
            &DekPlane::org(ORG_ID.to_string(), tenant.clone(), account_a),
            account_a,
            "microsoft",
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("seed a readable row for account A");

        // A plane resolved for SOME OTHER account.
        let other = uuid::Uuid::new_v4();
        let wrong_plane = DekPlane::org(ORG_ID.to_string(), tenant.clone(), other);
        let err = load_creds_bg(
            &control,
            &wrong_plane,
            account_a,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .err()
        .expect("a plane bound to another account must be refused");
        assert!(
            format!("{err}").contains("resolved for account"),
            "expected the expect_account mismatch, got: {err}"
        );

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }

    /// Personal accounts are untouched: `DekPlane::consumer` is byte-identical
    /// to the pre-fix hardcoded `TenantId::Consumer` resolution, on both the
    /// write and the read.
    #[tokio::test]
    async fn personal_accounts_still_round_trip_on_the_consumer_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping personal_accounts_still_round_trip_on_the_consumer_plane_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_dekplane_personal_ctl", "orgt_dekplane_personal_ten");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;

        let kek = load_kek().expect("test KEK loads");
        let personal_owner = seed_user(&control).await;
        let personal_dek = generate_dek();
        let personal_account: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id",
        )
        .bind(personal_owner)
        .bind(wrap_dek(&kek, &personal_dek).unwrap())
        .fetch_one(&control)
        .await
        .unwrap();

        let plane = DekPlane::consumer(control.clone());
        let bundle = outlook_bundle("personal-refresh");
        store_token_bundle(
            &control,
            &plane,
            personal_account,
            "microsoft",
            TokenOwner::AccountWide,
            &bundle,
        )
        .await
        .expect("a personal account's refresh must still persist on the consumer plane");
        let stored: Vec<u8> =
            sqlx::query_scalar("SELECT access_token FROM oauth_tokens WHERE account_id = $1")
                .bind(personal_account)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            decrypt_token(&personal_dek, &stored).unwrap(),
            bundle.access_token
        );
        let creds = load_creds_bg(
            &control,
            &plane,
            personal_account,
            "microsoft",
            TokenOwner::AccountWide,
        )
        .await
        .expect("a personal background load must succeed")
        .expect("the row just written must be found");
        assert_eq!(creds.access_token, bundle.access_token);

        drop_dbs(&base, [cdb, tdb], [control, tenant]).await;
    }
}

// Options-train route/store integration: exercise the response codes and
// account scoping on real PostgreSQL, alongside the provider upsert tests.
#[cfg(test)]
mod watched_resource_pg_tests {
    use super::*;
    use super::connectors_pg_test_support::{base_url, recreate, seed_user, teardown, with_db};
    use axum::body::to_bytes;
    use sqlx::{Connection, postgres::PgPoolOptions};

    fn tenant(pool: &sqlx::PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: Some(pool.clone()),
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }
    async fn json_body(response: Response, status: StatusCode) -> serde_json::Value {
        assert_eq!(response.status(), status);
        serde_json::from_slice(&to_bytes(response.into_body(), 1024 * 1024).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn watched_routes_preserve_rows_paginate_and_isolate_accounts_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping watched_routes_preserve_rows_paginate_and_isolate_accounts_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gopt_watched_resource_routes";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        recreate(&base, db).await.close().await.unwrap();
        let pool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let owner = seed_user(&pool).await;
        let account: uuid::Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner).fetch_one(&pool).await.unwrap();
        let other_owner = seed_user(&pool).await;
        let other_account: uuid::Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(other_owner).fetch_one(&pool).await.unwrap();
        let graph: uuid::Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary) VALUES ($1,'Primary','personal',true) RETURNING id")
            .bind(account).fetch_one(&pool).await.unwrap();
        let other_graph: uuid::Uuid = sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary) VALUES ($1,'Primary','personal',true) RETURNING id")
            .bind(other_account).fetch_one(&pool).await.unwrap();
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        let user = UserContext { user_id: owner, account_id: account, role: "owner".into(), device_id: "test".into() };
        for (provider, table, id_column) in [
            ("google", "gmail_watched_threads", "thread_id"),
            ("microsoft", "outlook_watched_conversations", "conversation_id"),
        ] {
            for (id, following) in [("older", false), ("newer", true)] {
                sqlx::query(&format!("INSERT INTO {table}(account_id,graph_id,ingested_by,{id_column},follow_replies,last_ingested_at,backfill_complete) VALUES ($1,$2,$3,$4,$5,CASE WHEN $4='older' THEN now()-interval '1 day' ELSE now() END,true)"))
                    .bind(account).bind(graph).bind(owner).bind(id).bind(following).execute(&pool).await.unwrap();
            }
            sqlx::query(&format!("INSERT INTO {table}(account_id,graph_id,ingested_by,{id_column},follow_replies) VALUES ($1,$2,$3,'foreign',true)"))
                .bind(other_account).bind(other_graph).bind(other_owner).execute(&pool).await.unwrap();
            super::connectors_pg_test_support::set_flag(&pool, "org_watched_deny", provider, true).await;
            let org_tenant = Tenant {
                store: None,
                pool: Some(pool.clone()),
                decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                    crate::tenancy::runtime::TenantContext {
                        org_id: "org_watched_deny".into(),
                        account_id: account,
                        user_id: owner,
                        pool: pool.clone(),
                        page_pool: None,
                    },
                )),
                class: crate::route_class::RouteClass::Other,
            };
            let denial = match watched_prologue(&state, &org_tenant, provider).await {
                Err(response) => response,
                Ok(_) => panic!("watched routes must reject even flag-enabled orgs"),
            };
            json_body(denial, StatusCode::FORBIDDEN).await;
            let path = || Path((provider.to_string(), "newer".to_string()));
            let missing = || Path((provider.to_string(), "foreign".to_string()));
            let response = get_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), missing()).await;
            assert_eq!(json_body(response, StatusCode::OK).await, json!({"following": false}));
            let response = put_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), missing(), Json(SetFollowRequest { follow: false })).await;
            json_body(response, StatusCode::NOT_FOUND).await;
            let response = delete_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), missing()).await;
            json_body(response, StatusCode::NOT_FOUND).await;
            let response = delete_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), path()).await;
            assert_eq!(json_body(response, StatusCode::OK).await, json!({"following": false}));
            let response = get_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), path()).await;
            assert_eq!(json_body(response, StatusCode::OK).await, json!({"following": false}));
            let response = put_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), path(), Json(SetFollowRequest { follow: true })).await;
            assert_eq!(json_body(response, StatusCode::OK).await, json!({"following": true}));
            let response = list_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), Path(provider.into()), Query(ListWatchedQuery {limit: Some(1),cursor:None})).await;
            let first = json_body(response, StatusCode::OK).await;
            assert_eq!(first["threads"][0]["thread_id"], "newer");
            assert_eq!(first["threads"][0]["following"], true);
            chrono::DateTime::parse_from_rfc3339(first["threads"][0]["saved_at"].as_str().unwrap()).unwrap();
            let cursor = uuid::Uuid::parse_str(first["next_cursor"].as_str().unwrap()).unwrap();
            let response = list_watched(State(state.clone()), tenant(&pool), Extension(user.clone()), Path(provider.into()), Query(ListWatchedQuery {limit: Some(1),cursor:Some(cursor)})).await;
            let last = json_body(response, StatusCode::OK).await;
            assert_eq!(last["threads"][0]["thread_id"], "older");
            assert_eq!(last["threads"][0]["following"], false);
            assert!(last["next_cursor"].is_null(), "a full final page must not require an empty next page");
            let count: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE account_id=$1"))
                .bind(account).fetch_one(&pool).await.unwrap();
            assert_eq!(count, 2, "PUT/DELETE must never delete or insert watched rows");
        }
        drop(state);
        teardown(&base, db, pool).await;
    }
}

// Org-plane Outlook "Keep saving new replies": the notification webhook only
// sees a bare Graph `subscriptionId` and can only query the CONTROL pool, but
// an org account's `graph_subscriptions` row lives on its TENANT pool. These
// tests run the real route helpers against two genuinely separate databases
// (a same-database fixture would hide exactly this bug).
//
//   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib routes::connectors::graph_mail_routes_pg_tests
#[cfg(test)]
mod graph_mail_routes_pg_tests {
    use super::connectors_pg_test_support::{
        base_url, recreate, seed_account_with_dek, seed_user, set_test_kek, teardown, with_db,
    };
    use crate::connectors::microsoft;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, PgPool};

    async fn two_dbs(base: &str, control_db: &str, tenant_db: &str) -> (PgPool, PgPool) {
        recreate(base, control_db).await.close().await.ok();
        let control: PgPool = PgPoolOptions::new().connect(&with_db(base, control_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();
        recreate(base, tenant_db).await.close().await.ok();
        let tenant: PgPool = PgPoolOptions::new().connect(&with_db(base, tenant_db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&tenant).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, 'org_mail_routes')")
            .execute(&tenant)
            .await
            .unwrap();
        (control, tenant)
    }

    async fn put_sub(tenant: &PgPool, account: uuid::Uuid, sub: &str, resource: &str, owner: uuid::Uuid) {
        sqlx::query(
            "INSERT INTO graph_subscriptions \
             (account_id, subscription_id, resource, client_state, expires_at, tenant_id, owner_user_id, change_types) \
             VALUES ($1,$2,$3,'secret', now() + interval '2 days','',$4,'created,updated,deleted')",
        )
        .bind(account)
        .bind(sub)
        .bind(resource)
        .bind(owner)
        .execute(tenant)
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn org_mail_subscription_is_routable_from_the_control_pool_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_mail_subscription_is_routable_from_the_control_pool_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        set_test_kek();
        let (cdb, tdb) = ("orgt_mailroute_control", "orgt_mailroute_tenant");
        let (control, tenant) = two_dbs(&base, cdb, tdb).await;
        let owner = seed_user(&control).await;
        let account = seed_account_with_dek(&control, owner).await;
        let member = seed_user(&control).await;
        put_sub(&tenant, account, "sub-inbox", "me/mailFolders('inbox')/messages", member).await;
        put_sub(&tenant, account, "sub-sent", "me/mailFolders('sentitems')/messages", member).await;
        // A Teams subscription on the tenant must never be routed as mail.
        put_sub(&tenant, account, "sub-teams", "teams/T/channels/C/messages", member).await;

        // BEFORE routes exist: the control-plane lookup the webhook does first
        // finds nothing — the production bug (notification dropped as unknown).
        assert!(microsoft::subscription_by_id(&control, "sub-inbox").await.unwrap().is_none());
        assert!(microsoft::mail_route_account(&control, "sub-inbox").await.unwrap().is_none());

        let n = microsoft::sync_mail_routes(&tenant, &control, account).await.unwrap();
        assert_eq!(n, 2, "inbox + sentitems only");
        assert_eq!(microsoft::mail_route_account(&control, "sub-inbox").await.unwrap(), Some(account));
        assert_eq!(microsoft::mail_route_account(&control, "sub-sent").await.unwrap(), Some(account));
        assert!(microsoft::mail_route_account(&control, "sub-teams").await.unwrap().is_none());

        // The hop the webhook makes next: row read on the TENANT pool.
        let (a, state, resource, _tid, owner_uid) =
            microsoft::subscription_by_id_for_account(&tenant, account, "sub-inbox")
                .await
                .unwrap()
                .expect("tenant row resolves through the route");
        assert_eq!((a, state.as_str(), owner_uid), (account, "secret", Some(member)));
        assert_eq!(resource, "me/mailFolders('inbox')/messages");
        // Wrong account never resolves another account's row.
        assert!(microsoft::subscription_by_id_for_account(&tenant, uuid::Uuid::new_v4(), "sub-inbox")
            .await
            .unwrap()
            .is_none());

        // Composition at the webhook resolver: control miss -> route -> row read
        // on the TENANT pool, returning that pool (not the control pool).
        let t2 = tenant.clone();
        let ((ra, rstate, _res, _tid, rowner), rpool) =
            crate::routes::graph_webhook::resolve_subscription_via(&control, "sub-inbox", |a| {
                assert_eq!(a, account);
                async move { Ok(Some(t2)) }
            })
            .await
            .unwrap()
            .expect("org subscription resolves through the route");
        assert_eq!((ra, rstate.as_str(), rowner), (account, "secret", Some(member)));
        let in_tenant: bool = sqlx::query_scalar(
            "SELECT EXISTS (SELECT 1 FROM graph_subscriptions WHERE subscription_id = 'sub-inbox')",
        )
        .fetch_one(&rpool)
        .await
        .unwrap();
        assert!(in_tenant, "returned pool must be the tenant pool");
        assert!(crate::routes::graph_webhook::resolve_subscription_via(&control, "nope", |_| async { Ok(None) })
            .await
            .unwrap()
            .is_none());
        // A plane that cannot be resolved right now surfaces as an error, so
        // a receive job retries instead of treating the subscription as unknown.
        assert!(crate::routes::graph_webhook::resolve_subscription_via(&control, "sub-inbox", |_| async {
            Err(crate::connectors::ConnectorError::Internal("tenant pool build timed out".into()))
        })
        .await
        .is_err());

        // The route's own check (`route_secrets` for the batch, then
        // `route_verdict` per entry, exactly what the webhook calls) reads
        // the hash the sync wrote: the right clientState verifies, a wrong
        // one is a mismatch, an id with no route is unknown, and a route
        // from before the hash column (NULL) is held unverified for the
        // worker to check.
        use crate::routes::graph_webhook::{route_secrets, route_verdict, RouteSecret, RouteVerdict};
        let ids = ["sub-inbox".to_string(), "sub-teams".to_string()];
        let verdict = |secrets: &std::collections::HashMap<String, RouteSecret>, id: &str, provided: &str| {
            route_verdict(secrets.get(id), provided)
        };
        let secrets = route_secrets(&control, &ids).await.unwrap();
        assert_eq!(verdict(&secrets, "sub-inbox", "secret"), RouteVerdict::Verified);
        assert_eq!(verdict(&secrets, "sub-inbox", "forged"), RouteVerdict::Mismatch);
        assert_eq!(verdict(&secrets, "sub-inbox", ""), RouteVerdict::Mismatch);
        assert_eq!(verdict(&secrets, "sub-teams", "secret"), RouteVerdict::Unknown);
        assert_eq!(
            secrets.get("sub-inbox"),
            Some(&RouteSecret::ClientStateHash(microsoft::client_state_sha256("secret"))),
            "the control route carries the hash, never the clientState"
        );
        assert_eq!(
            microsoft::mail_route_client_state_hashes(&control, &ids).await.unwrap(),
            vec![("sub-inbox".to_string(), Some(microsoft::client_state_sha256("secret")))]
        );
        sqlx::query("UPDATE graph_mail_routes SET client_state_sha256 = NULL WHERE subscription_id = 'sub-inbox'")
            .execute(&control)
            .await
            .unwrap();
        let unhashed = route_secrets(&control, &ids).await.unwrap();
        assert_eq!(unhashed.get("sub-inbox"), Some(&RouteSecret::Unhashed));
        assert_eq!(verdict(&unhashed, "sub-inbox", "anything"), RouteVerdict::Unverifiable);
        assert_eq!(microsoft::sync_mail_routes(&tenant, &control, account).await.unwrap(), 2);
        let rehashed = route_secrets(&control, &ids).await.unwrap();
        assert_eq!(verdict(&rehashed, "sub-inbox", "secret"), RouteVerdict::Verified);

        // Idempotent, and prunes a route whose subscription was rotated away.
        sqlx::query("DELETE FROM graph_subscriptions WHERE subscription_id = 'sub-sent'")
            .execute(&tenant)
            .await
            .unwrap();
        put_sub(&tenant, account, "sub-sent-2", "me/mailFolders('sentitems')/messages", member).await;
        assert_eq!(microsoft::sync_mail_routes(&tenant, &control, account).await.unwrap(), 2);
        assert!(microsoft::mail_route_account(&control, "sub-sent").await.unwrap().is_none());
        assert_eq!(microsoft::mail_route_account(&control, "sub-sent-2").await.unwrap(), Some(account));

        teardown(&base, cdb, control).await;
        teardown(&base, tdb, tenant).await;
    }
}

/// The per-org calendar pass bound at the route (`calendar_pass_gate`,
/// "Two bounds"). No database: an over-the-bound caller answers before the
/// pass resolves anything, and an admitted one reaches the pass, which on
/// this state (no control DB) answers `503`.
#[cfg(test)]
mod calendar_gate_route_tests {
    use super::*;
    use crate::calendar_pass_gate::{CalendarPassGate, OrgSlot};
    use std::time::Duration;
    use uuid::Uuid;

    fn tenant_pool() -> sqlx::PgPool {
        // The org tenant request pool's default size
        // (`tenancy::registry::DEFAULT_MAX_CONNECTIONS`).
        sqlx::postgres::PgPoolOptions::new()
            .max_connections(crate::tenancy::registry::DEFAULT_MAX_CONNECTIONS)
            .connect_lazy("postgres://localhost/unused")
            .expect("a lazy pool never connects")
    }

    fn org_tenant(org_id: &str, pool: &sqlx::PgPool) -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: org_id.to_string(),
                    account_id: Uuid::new_v4(),
                    user_id: Uuid::new_v4(),
                    pool: pool.clone(),
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn member() -> UserContext {
        UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: "member".to_string(),
            device_id: "test-device".to_string(),
        }
    }

    fn creds() -> ConnectorCredentials {
        ConnectorCredentials {
            access_token: "unused".into(),
            refresh_token: None,
            expires_at: None,
            provider_workspace_id: String::new(),
            refreshed: None,
            row_updated_at: None,
        }
    }

    /// Review finding on #1164: the shared bound (4) alone let four members
    /// of ONE org each pin a connection of that org's five-connection tenant
    /// pool. With one pass running in the org, a second member's "sync now"
    /// in the same org is `429` even though the shared gate has room, for
    /// either provider; another org's member is not held up by it.
    #[tokio::test]
    async fn a_second_sync_now_in_the_same_org_is_429_while_the_shared_gate_has_room() {
        let gate = CalendarPassGate::new(4);
        let state = crate::test_app_state();
        let pool_a = tenant_pool();
        let tenant_a = org_tenant("org_gate_a", &pool_a);
        let running = gate
            .acquire_scoped(Some(OrgSlot::microsoft("org_gate_a", &pool_a)), Duration::ZERO)
            .await
            .expect("one Microsoft pass running in org_gate_a");

        for provider in ["microsoft", "google"] {
            let resp = calendar_sync_response(
                &gate, &state, &tenant_a, &pool_a, &member(), provider, TokenOwner::Member(Uuid::new_v4()), creds(),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS, "{provider}: the org's slot is taken");
            assert!(resp.headers().contains_key(axum::http::header::RETRY_AFTER));
        }
        assert_eq!(gate.in_flight(), 1, "the shared gate had room the whole time");

        let pool_b = tenant_pool();
        let resp = calendar_sync_response(
            &gate,
            &state,
            &org_tenant("org_gate_b", &pool_b),
            &pool_b,
            &member(),
            "microsoft",
            TokenOwner::Member(Uuid::new_v4()),
            creds(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "another org is admitted (and reaches the pass)");

        drop(running);
        let resp = calendar_sync_response(
            &gate, &state, &tenant_a, &pool_a, &member(), "microsoft", TokenOwner::Member(Uuid::new_v4()), creds(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "a freed org slot admits the next sync");
        assert_eq!(gate.in_flight(), 0);
    }

    /// Weighted per-org slots: with one GOOGLE pass running in the org (one
    /// tenant connection, briefly), a second member's Google "sync now" is
    /// admitted, but a Microsoft one, which would pin a connection for its
    /// whole pass on top, is `429`.
    #[tokio::test]
    async fn a_google_pass_leaves_room_for_another_google_sync_now_but_not_a_microsoft_one() {
        let gate = CalendarPassGate::new(4);
        let state = crate::test_app_state();
        let pool_a = tenant_pool();
        let tenant_a = org_tenant("org_gate_g", &pool_a);
        let running = gate
            .acquire_scoped(Some(OrgSlot::google("org_gate_g", &pool_a)), Duration::ZERO)
            .await
            .expect("one Google pass running in org_gate_g");

        let resp = calendar_sync_response(
            &gate, &state, &tenant_a, &pool_a, &member(), "microsoft", TokenOwner::Member(Uuid::new_v4()), creds(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::TOO_MANY_REQUESTS, "a Microsoft pass needs the whole org budget");

        let resp = calendar_sync_response(
            &gate, &state, &tenant_a, &pool_a, &member(), "google", TokenOwner::Member(Uuid::new_v4()), creds(),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE, "a second Google pass is admitted (and reaches the pass)");
        drop(running);
        assert_eq!(gate.in_flight(), 0);
    }
}
