//! `GET`/`PUT /v1/orgs/auto-add` — CS-447 (org-onboarding plan §3 item 9,
//! `docs/plans/org-onboarding-from-the-invite.md`, D1/D11/D16): the
//! admin-facing toggle behind auto-add, and the tier it is allowed to
//! spend. Also `GET /v1/orgs/auto-add/queued-preview` and
//! `POST /v1/orgs/auto-add/admit-queued` — CS-449 (item 38, D11's own
//! "ask once"): the toggle's follow-up action for a backlog that was
//! already queued when the toggle flipped on.
//!
//! # What the toggle route is, and what it is not
//!
//! It is the *setting*. The decision that reads it lives in
//! `services::org_auto_add` and fires in `routes::org_join_requests::create`
//! (for a live request) and in `routes::org_join_requests::
//! regate_and_admit_one` (for an already-queued one, called from both
//! routes below and from the drain) — one predicate, several call sites,
//! never re-derived at any of them.
//!
//! Turning the toggle on via `PUT` changes what happens to the NEXT person
//! who asks and nothing about the people already in the queue — that half
//! is deliberately conservative: it never silently spends seats on a
//! backlog nobody looked at. `GET /queued-preview` and `POST /admit-queued`
//! below are the explicit, admin-initiated action that spends on that
//! backlog, one click, after the admin has seen exactly who it would add.
//!
//! # Item 38 (D11's "ask once") — the two rules, both founder-settled
//! 2026-09-22, neither up for revisiting
//!
//! 1. **When passers exceed free slots, admit what fits, oldest first.**
//!    The plan doc's literal "all or none" loses to the slot pool: if 20
//!    people pass and 5 slots are free, one click admits the 5 oldest and
//!    the other 15 stay queued — never a 0 because the batch didn't fit
//!    whole.
//! 2. **The remainder drains.** A candidate this batch could not fit is
//!    patched to `source = 'pool_full'`
//!    (`routes::org_join_requests::regate_and_admit_one`'s own PoolFull
//!    branch) — the SAME state a live request falls into when auto-add
//!    finds no free slot — so `routes::org_join_requests::drain_org` picks
//!    it up automatically the next time a slot frees, with no second "ask".
//!
//! # The preview count is advisory; the admit is not
//!
//! [`queued_preview`]'s `free_slots`/`would_admit` are a snapshot — see
//! `services::org_auto_add`'s own module doc, "the pool is deliberately NOT
//! one of `decide`'s inputs". By the time an admin clicks, another request,
//! another admin's own click, or the drain may have spent slots this
//! preview counted. [`admit_queued`] never trusts that number: every
//! candidate is re-gated AND reserved for real, one at a time, in
//! `routes::org_join_requests::regate_and_admit_one`, and the response
//! reports exactly what was and was not admitted — never more than the pool
//! actually yielded at the moment each reservation was attempted.
//!
//! # Admin-only, and personal orgs refuse
//!
//! Both verbs, including the GET, go through
//! [`super::org_invites::require_org_admin`] plus the same personal-org
//! refusal `routes::org_slots` uses. Unlike `GET /v1/orgs/slots` (readable
//! by any org member, because the members page renders a slot meter for
//! everyone), this setting is admin-only on BOTH sides: it decides whether
//! strangers are added to the org and at what cost, which is not a fact an
//! ordinary member has any call to read.
//!
//! # Off by default
//!
//! An org with no row reads `enabled: false, tier: "standard"` — see
//! `services::org_auto_add::read_settings`. The GET never 404s for "no
//! settings yet"; there is no such state, only "off".

use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::services::org_auto_add as svc;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenantContext;
use crate::AppState;

use super::org_invites::require_org_admin;

fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}
fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org_auto_add: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Same bar `routes::org_slots::refuse_personal_org` applies — see that
/// function's own doc comment for why `require_org_admin` alone does not
/// exclude a personal org (its single member always holds `role = "owner"`).
async fn refuse_personal_org(state: &AppState, ctx: &TenantContext) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(service_unavailable());
    };
    match store.account_is_real_org(ctx.account_id, ctx.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("personal accounts have no auto-add setting")),
        Err(e) => Err(internal_error("account lookup", e)),
    }
}

/// The toggle as an admin sees it.
#[derive(Debug, Serialize, ToSchema)]
pub struct AutoAddSettingsResponse {
    /// D11: `false` for every org that has not explicitly turned it on,
    /// including every org that has never seen this route.
    pub enabled: bool,
    /// `seat_tiers.code` this org's auto-adds spend against — founder
    /// decision, 2026-09-22. Always a concrete code, never `null`: a
    /// missing `default_seat_tier_id` IS `standard`, and rendering that as
    /// "none" would invite a UI to show "no tier selected" for a setting
    /// that will very much spend one.
    pub tier: String,
}

/// `standard` — the tier a toggle with no recorded choice spends against
/// (migration 0241's own NULL semantics, reused).
const DEFAULT_TIER_CODE: &str = "standard";

async fn tier_code_for(control: &sqlx::PgPool, seat_tier_id: Option<Uuid>) -> String {
    let Some(id) = seat_tier_id else {
        return DEFAULT_TIER_CODE.to_string();
    };
    let code: Option<String> = sqlx::query_scalar("SELECT code FROM seat_tiers WHERE id = $1")
        .bind(id)
        .fetch_optional(control)
        .await
        .unwrap_or(None);
    // A recorded tier whose row has since vanished degrades to the default
    // for DISPLAY only; the spend path (`services::org_auto_add::
    // resolve_tier_id`) still passes the stored id to `reserve_slot`, whose
    // own per-tier read simply finds no purchased slots and falls to
    // approval. Showing "standard" here would be a lie if it changed what
    // was spent — it does not.
    code.unwrap_or_else(|| DEFAULT_TIER_CODE.to_string())
}

#[utoipa_path(
    get,
    path = "/v1/orgs/auto-add",
    tag = "org_auto_add",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The org's auto-add setting (off, `standard`, for an org that never set one)", body = AutoAddSettingsResponse),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn get_settings(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let settings = match svc::read_settings(control, ctx.account_id).await {
        Ok(s) => s,
        Err(e) => return internal_error("read settings", e),
    };
    let tier = tier_code_for(control, settings.default_seat_tier_id).await;
    (StatusCode::OK, Json(AutoAddSettingsResponse { enabled: settings.enabled, tier })).into_response()
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct PutAutoAddSettingsRequest {
    pub enabled: bool,
    /// `seat_tiers.code`, defaulted to `standard` — the same default and
    /// the same validation `POST /v1/orgs/join-requests/{id}/approve` applies
    /// to its own per-request `tier` (CS-442, item 40). Unknown or inactive
    /// ⇒ 400: a toggle that silently fell back to `standard` because an
    /// admin typed `pro` would spend a tier they did not choose.
    #[serde(default = "default_tier")]
    pub tier: String,
}
fn default_tier() -> String {
    DEFAULT_TIER_CODE.to_string()
}

#[utoipa_path(
    put,
    path = "/v1/orgs/auto-add",
    tag = "org_auto_add",
    security(("bearer_token" = [])),
    request_body = PutAutoAddSettingsRequest,
    responses(
        (status = 200, description = "Saved", body = AutoAddSettingsResponse),
        (status = 400, description = "Unknown or inactive seat tier"),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn put_settings(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PutAutoAddSettingsRequest>,
) -> Response {
    let account=user.account_id;
    let response=put_settings_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn put_settings_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<PutAutoAddSettingsRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };

    let tier = body.tier.trim();
    let tier_id: Option<Uuid> = match sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active")
        .bind(tier)
        .fetch_optional(control)
        .await
    {
        Ok(v) => v,
        Err(e) => return internal_error("tier lookup", e),
    };
    let Some(tier_id) = tier_id else {
        return bad_request("unknown or inactive seat tier");
    };
    // `standard` is stored as NULL, not as its own id — one representation
    // of "the default tier" across `slot_reservations` (0241),
    // `org_auto_add_settings` and `org_auto_adds`, so a catalogue change
    // that re-issues the `standard` row cannot strand a stored id.
    let stored_tier_id = if tier == DEFAULT_TIER_CODE { None } else { Some(tier_id) };

    let saved = match svc::upsert_settings(control, ctx.account_id, body.enabled, stored_tier_id, user.user_id).await {
        Ok(s) => s,
        Err(e) => return internal_error("save settings", e),
    };
    tracing::info!(
        account_id = %ctx.account_id, admin_user_id = %user.user_id, enabled = saved.enabled, %tier,
        "org_auto_add: an admin changed the auto-add setting"
    );
    let tier = tier_code_for(control, saved.default_seat_tier_id).await;
    (StatusCode::OK, Json(AutoAddSettingsResponse { enabled: saved.enabled, tier })).into_response()
}

// ── CS-449 (item 38, D11's "ask once") ──────────────────────────────────

/// One OPEN request that would be admitted if the admin clicked
/// [`admit_queued`] right now — [`queued_preview`]'s own row shape.
#[derive(Debug, Serialize, ToSchema)]
pub struct QueuedPasser {
    pub request_id: Uuid,
    pub email: String,
    pub name: Option<String>,
    #[schema(value_type = String, format = DateTime)]
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct QueuedPreviewResponse {
    /// `seat_tiers.code` the toggle is currently set to spend — the SAME
    /// tier [`get_settings`] reports, read the same way.
    pub tier: String,
    /// Free slots in THAT tier, right now. **Advisory** — see this
    /// module's own doc comment, "The preview count is advisory". Never the
    /// number [`admit_queued`] trusts.
    pub free_slots: i64,
    /// `min(passers.len(), free_slots)` — the "up to this many, oldest
    /// first" figure a "flip and admit" confirmation dialog would show
    /// before the click. Not a promise: see `free_slots`.
    pub would_admit: i64,
    /// Every OPEN request that passes all five gates against the CURRENT
    /// settings — oldest first, whatever slots say. The full backlog, not
    /// truncated to `free_slots`, so an admin can see the size of what is
    /// NOT going to fit in one click before they click it.
    pub passers: Vec<QueuedPasser>,
}

/// See this module's own doc comment, "Item 38 (D11's 'ask once')" and "The
/// preview count is advisory", for the two founder rulings this route's
/// sibling [`admit_queued`] exists to honour.
///
/// Every OPEN request for the org — whatever its `source` — is re-gated
/// fresh via `services::org_auto_add::regate_queued_request`
/// (`routes::org_join_requests::regate_and_admit_one`'s own read-only
/// half), reusing the STORED ladder outcome from when each was queued (see
/// that function's own doc comment for why a live Slack/Google re-derive is
/// not available here) but re-checking every other gate — `enabled`
/// included, so flipping the toggle back off between two page loads makes
/// this list empty again without any special-casing here.
#[utoipa_path(
    get,
    path = "/v1/orgs/auto-add/queued-preview",
    tag = "org_auto_add",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The backlog that would pass right now, oldest first, plus an advisory slot count", body = QueuedPreviewResponse),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn queued_preview(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };

    let settings = svc::read_settings_or_disabled(control, ctx.account_id).await;
    // CS-449 fix (item 7): capped, not the whole backlog — see
    // `services::org_auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES`'s own doc
    // comment. A backlog larger than the cap is real and rare; this trades
    // "the preview may not show every passer" for "a page load re-gates a
    // bounded number of candidates", which is the same trade the drain and
    // the submit side make.
    let requests = match crate::services::org_join_requests::list_open_capped(
        control,
        ctx.account_id,
        svc::MAX_AUTO_ADD_BATCH_CANDIDATES,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("list open requests", e),
    };

    let now = chrono::Utc::now();
    let mut passers = Vec::new();
    for req in &requests {
        let regated = match svc::regate_queued_request(
            control,
            ctx.account_id,
            &ctx.org_id,
            req.user_id,
            &req.evidence,
            settings,
            now,
        )
        .await
        {
            Ok(Some(pair)) => pair,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(
                    error = %e, request_id = %req.id, account_id = %ctx.account_id,
                    "org_auto_add: queued_preview re-gate failed — excluding this candidate"
                );
                continue;
            }
        };
        if matches!(svc::decide(regated.0, &regated.1), svc::AutoAddDecision::TryReserve(_)) {
            passers.push(QueuedPasser {
                request_id: req.id,
                email: req.user_email.clone().unwrap_or_default(),
                name: req.user_name.clone(),
                requested_at: req.requested_at,
            });
        }
    }
    // `requests` is already oldest-first (`list_open`'s own `ORDER BY
    // requested_at ASC`), and filtering preserves order.

    let tier_id = svc::resolve_tier_id(control, settings).await;
    let tier_code = tier_code_for(control, settings.default_seat_tier_id).await;
    let free_slots = match tier_id {
        Ok(Some(id)) => svc::free_slots_for_tier(control, ctx.account_id, id).await.unwrap_or(0),
        // No tier to spend against at all (a misconfigured catalogue) or
        // the read itself failed — `admit_queued` would refuse every one of
        // these the same way (`AutoAddOutcome::Unavailable`), so 0 is
        // honest, not merely a safe default.
        Ok(None) | Err(_) => 0,
    };
    let would_admit = (passers.len() as i64).min(free_slots.max(0));

    (StatusCode::OK, Json(QueuedPreviewResponse { tier: tier_code, free_slots, would_admit, passers }))
        .into_response()
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct AdmitQueuedRequest {
    /// **ALL** passers the admin was shown by [`queued_preview`] — the
    /// whole `passers` array, not truncated to `would_admit` — oldest first.
    /// The server re-sorts by `requested_at` regardless (see
    /// `services::org_join_requests::list_open_by_ids`'s own doc comment)
    /// — this is the admin's consent to admit from THESE people, not a
    /// promise about array order.
    ///
    /// **Blocker fix, CS-449 review round 2:** this used to be only the
    /// `would_admit`-sized slice the UI intended to spend on. The problem
    /// was not the spend — it was everyone else: a candidate not sent here
    /// is never submitted to [`regate_and_admit_one`](crate::routes::
    /// org_join_requests::regate_and_admit_one) at all, so it never
    /// ACQUIRES `source = 'pool_full'` and `routes::org_join_requests::
    /// drain_org`'s candidate set (that exact filter) never contains it —
    /// not on a member removal, not on a purchase, not on any sweep. The
    /// dialog's own promise ("the rest are added as slots free up") was
    /// false for every one of them, and in the zero-free-slots case (no
    /// button shown at all) it was false for the WHOLE backlog: nothing was
    /// ever submitted, so nothing was ever queued for the drain either.
    /// Sending every passer here, and letting [`max_admit`](Self::max_admit)
    /// carry the spend limit instead, is what makes the promise true: see
    /// this route's own doc comment, rule 2.
    ///
    /// Capped at [`crate::services::org_auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES`]
    /// — more than that is refused with 400 rather than silently truncated,
    /// since silently admitting fewer than the admin asked for would be
    /// exactly the kind of silent short-fall this fix exists to close.
    pub request_ids: Vec<Uuid>,
    /// Admit **at most** this many, oldest first — D11's "admit what fits,
    /// oldest first" as a number the caller supplies rather than a slice the
    /// server infers from array length. Every [`request_ids`](Self::request_ids)
    /// candidate beyond this count that STILL passes all five gates is
    /// patched to `source = 'pool_full'` so `drain_org` collects it later,
    /// exactly like a candidate the pool itself turned away — see this
    /// route's own doc comment, rule 2, and
    /// [`NotAdmitted::reason`]'s own doc comment for the label that
    /// distinguishes "the cap stopped this" from "the pool was actually
    /// empty".
    ///
    /// **Required, not defaulted.** A missing field is a 400 (no
    /// `#[serde(default)]`): defaulting to something generous here would
    /// spend seats nobody asked for, and defaulting to `0` would silently
    /// admit nobody when a caller forgot the field, which is just as wrong
    /// in the other direction. Negative is refused with its own 400.
    /// `0` is valid and meaningful — item 38's zero-free-slots case: admit
    /// nobody, patch every passer to `pool_full` for the drain, spend
    /// nothing now.
    pub max_admit: i64,
}

/// One candidate [`admit_queued`] did not admit, and why.
#[derive(Debug, Serialize, ToSchema)]
pub struct NotAdmitted {
    pub request_id: Uuid,
    /// A stable label — see `routes::org_join_requests::
    /// regate_and_admit_one`'s own doc comment for the exact set this can
    /// be: one of `services::org_auto_add::FallReason::label`'s eight
    /// strings, `"pool_full"` (gate 2 — a real reservation attempt found
    /// the pool empty), [`crate::routes::org_join_requests::
    /// ADMIT_CAP_REACHED_REASON`] (`"admit_cap_reached"` — this candidate
    /// passed every gate but `max_admit` was already spent, so NO
    /// reservation was even attempted; distinct from `"pool_full"` on
    /// purpose, so an admin is never told the pool was empty when it was
    /// actually their own cap), or `"unavailable"`.
    pub reason: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdmitQueuedResponse {
    pub admitted: Vec<Uuid>,
    pub not_admitted: Vec<NotAdmitted>,
}

/// See this module's own doc comment, "Item 38" and "The preview count is
/// advisory" — this route is the spend half those two rulings describe.
///
/// `request_ids` not found among this account's OPEN requests (wrong
/// account, already decided since the preview, or simply unknown) are
/// silently dropped from BOTH lists — they are not this admin's to admit
/// or to be told about. Re-submitting an id for someone who is already a
/// member (admitted by this same click on a concurrent request, or by any
/// other path) is a no-op reported in `not_admitted`, never an error — see
/// `routes::org_join_requests::regate_and_admit_one`'s own `AlreadyMember`
/// branch.
#[utoipa_path(
    post,
    path = "/v1/orgs/auto-add/admit-queued",
    tag = "org_auto_add",
    security(("bearer_token" = [])),
    request_body = AdmitQueuedRequest,
    responses(
        (status = 200, description = "Admitted what the pool actually yielded, oldest first; everyone else reported with why", body = AdmitQueuedResponse),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn admit_queued(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<AdmitQueuedRequest>,
) -> Response {
    let account=user.account_id;
    let response=admit_queued_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn admit_queued_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<AdmitQueuedRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };

    // Blocker fix, CS-449 review round 2: a missing/negative budget is a
    // hard error, never a generous default — see `AdmitQueuedRequest::
    // max_admit`'s own doc comment. Serde already makes a MISSING field a
    // 400 (no `#[serde(default)]` on it); this covers the other half.
    if body.max_admit < 0 {
        return bad_request("max_admit must not be negative");
    }
    let max_admit = body.max_admit;

    if body.request_ids.is_empty() {
        return (StatusCode::OK, Json(AdmitQueuedResponse { admitted: vec![], not_admitted: vec![] }))
            .into_response();
    }
    // Item 7: bounded, the same way `queued_preview`'s own read is — see
    // `services::org_auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES`'s own doc
    // comment for why this is the SAME cap `queued_preview` shows against.
    // Refused outright rather than silently truncated: silently admitting
    // fewer than an admin explicitly listed would be exactly the kind of
    // silent short-fall the blocker fix above exists to close.
    if body.request_ids.len() as i64 > svc::MAX_AUTO_ADD_BATCH_CANDIDATES {
        return bad_request("too many request_ids in one batch");
    }

    let settings = svc::read_settings_or_disabled(control, ctx.account_id).await;

    // Server-side oldest-first (see `AdmitQueuedRequest::request_ids`'s own
    // doc comment) — also the one place an id that is not an open request
    // for this account is dropped.
    let requests = match crate::services::org_join_requests::list_open_by_ids(
        control,
        ctx.account_id,
        &body.request_ids,
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return internal_error("list open requests", e),
    };

    let now = chrono::Utc::now();
    let mut admitted = Vec::new();
    let mut not_admitted = Vec::new();
    for req in &requests {
        // One reservation attempt at a time, in order — D11's "admit what
        // fits, oldest first" IS this loop, not a batch calculation ahead
        // of it. A candidate that finds the pool empty does not break the
        // loop: the rest still get re-gated and, if eligible, patched to
        // `pool_full` for the drain (`regate_and_admit_one`'s own doc
        // comment).
        //
        // Blocker fix, CS-449 review round 2: `may_spend` is the budget
        // half of `max_admit` — once `admitted.len()` reaches it, every
        // remaining passer is still re-gated (so a gate failure is still
        // reported honestly) but is never handed a real reservation
        // attempt; `regate_and_admit_one` patches it to `pool_full` under
        // `ADMIT_CAP_REACHED_REASON` instead, which is what makes it
        // reachable by `drain_org` later — see `AdmitQueuedRequest::
        // request_ids`'s own doc comment for why sending every passer here
        // (not just the ones this budget covers) is the actual fix.
        let may_spend = (admitted.len() as i64) < max_admit;
        match crate::routes::org_join_requests::regate_and_admit_one(
            &state,
            control,
            ctx.account_id,
            &ctx.org_id,
            settings,
            req,
            now,
            may_spend,
        )
        .await
        {
            (id, Ok(())) => admitted.push(id),
            (id, Err(reason)) => not_admitted.push(NotAdmitted { request_id: id, reason: reason.to_string() }),
        }
    }

    tracing::info!(
        account_id = %ctx.account_id, admin_user_id = %user.user_id,
        requested = body.request_ids.len(), max_admit, admitted = admitted.len(), not_admitted = not_admitted.len(),
        "org_auto_add: an admin batch-admitted a queued backlog (item 38)"
    );

    (StatusCode::OK, Json(AdmitQueuedResponse { admitted, not_admitted })).into_response()
}
