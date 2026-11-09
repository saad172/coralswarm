//! The join-request queue (CS-437, org-onboarding plan §3 items 4 + 14,
//! `docs/plans/org-onboarding-from-the-invite.md`, D3/D9/D10/D19):
//! - `POST   /v1/join-requests`              — a signed-in user asks to
//!   join an org by `clerk_org_id`. Always `202`, always the same body
//!   (D9/D10: the requester never learns the pool state or a rejection).
//! - `GET    /v1/me/join-requests`            — the caller's own OPEN
//!   requests (item 39's waiting-state banner reads this).
//! - `GET    /v1/orgs/join-requests`          — admin: open requests, then
//!   recently decided ones.
//! - `GET    /v1/orgs/join-requests/count`    — admin: the open count alone
//!   (CS-438, D13) — the members-page badge's own cheap read.
//! - `POST   /v1/orgs/join-requests/{id}/reject` — admin: silent, durable
//!   (D10) — no email, no notice to the requester.
//! - `POST   /v1/orgs/join-requests/{id}/approve` — admin: D3's Clerk-first
//!   sequence (see [`approve`]'s own doc comment for the numbered steps).
//!
//! This file NEVER references `clerk_provision::ensure_org_membership`
//! directly — [`crate::routes::org_invites::provision_clerk_membership`]/
//! [`crate::routes::org_invites::mirror_local_membership`] are the only
//! Clerk-membership writers it calls, which is what keeps
//! `scripts/check_ensure_org_membership_callers.sh` green with this file
//! present (see that script's own doc comment: a helper extracted FROM
//! `routes::org_invites` stays allowlisted; a new route file calling
//! `ensure_org_membership` itself would not).
//!
//! Requester routes run on the caller's own [`UserContext`] + the control
//! pool, NOT [`Tenant`] — the requester is, by definition, not a member of
//! the org they are asking to join, so their own JWT/tenant binding carries
//! their PERSONAL org, never the target. The target org is named explicitly,
//! by `clerk_org_id`, and resolved to `accounts.id` inside the handler. Admin
//! routes run on [`Tenant`] + [`require_org_admin`], exactly like
//! `routes::org_invites`/`routes::org_slots` — an admin acts on their OWN
//! org.
//!
//! Personal orgs never receive or list join requests (D19): every route
//! below refuses one, 404 for the requester (folded into the same "unknown
//! org" response — see [`org_not_found`]) and 403 for the admin routes (the
//! same [`refuse_personal_org`] bar `routes::org_slots` uses for its own
//! admin actions).

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::billing::slot_pool::{self, ReleaseReason};
use crate::clerk_admin::{ClerkAdminClient, CreatedUser};
use crate::middleware::auth::UserContext;
use crate::services::org_auto_add as auto_add;
use crate::services::org_join_requests as svc;
use crate::tenancy::extractor::Tenant;
use crate::AppState;

use super::org_invites::{mirror_local_membership, provision_clerk_membership, require_org_admin};

// ── Small response/error helpers (module convention — see `org_invites.rs`,
//    `org_slots.rs`; this crate has no central `ApiError`) ─────────────────

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}
fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
/// The ONE 404 shape for a requester-facing route, reused for BOTH "no such
/// org" and "that org is a personal org" — D19 says a personal org's
/// existence must not be distinguishable from an unknown org id to a
/// non-member caller.
fn org_not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))).into_response()
}
fn not_found(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": msg}))).into_response()
}
fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org_join_requests: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}
/// The one sentence a requester ever sees when their request was QUEUED,
/// whatever actually happened behind it (freshly queued, already had an
/// open request, silently suppressed by a durable rejection, blocked from
/// auto-add by a tombstone, or fallen out of auto-add because the pool was
/// empty — D9/D10).
///
/// **Every auto-add refusal lands here, identically.** That is the
/// disclosure rule: the org's slot count is never observable to a
/// non-member, so "the pool was full" and "auto-add is off" and "your
/// domain is not verified" must be indistinguishable from out here.
fn sent_to_admin() -> Response {
    (StatusCode::ACCEPTED, Json(json!({"status": "sent_to_admin"}))).into_response()
}

/// CS-447 (item 9): they were auto-added and are a member NOW — there is no
/// request, and nobody is going to approve anything.
///
/// The one place this route says something other than [`sent_to_admin`],
/// and it is not a disclosure: it states a fact about the CALLER's own
/// membership, which they can see for themselves the moment they reload.
/// Telling an auto-added member "your request has been sent to your admin"
/// would be a plainly false sentence, and item 39's waiting banner (which
/// reads OPEN requests) would never appear to correct it.
///
/// It also cannot be turned into a pool oracle by a prober: auto-add is OFF
/// for every org by default, so "not added" carries no information about
/// anyone's slot count.
fn auto_added() -> Response {
    (StatusCode::ACCEPTED, Json(json!({"status": "added"}))).into_response()
}

/// `org_join_requests.source` for a request that reached auto-add and found
/// the pool empty (D9/D1). A named constant because it is written in one
/// place and asserted in another, and a typo would silently become a
/// `CHECK` violation at exactly the moment this route is trying to fall
/// back gracefully.
pub(crate) const POOL_FULL_SOURCE: &str = "pool_full";

/// [`regate_and_admit_one`]'s `not_admitted` label for a candidate that
/// passed every gate but was never handed a reservation attempt because the
/// CALLER's own budget (`routes::org_auto_add::admit_queued`'s `max_admit`)
/// was already spent — CS-449 review round 2's blocker fix. Deliberately
/// distinct from [`POOL_FULL_SOURCE`]/`"pool_full"` as a REASON string (the
/// `org_join_requests.source` column still writes `pool_full` in both
/// cases, migration 0238's own `CHECK` has no room for a sixth value and
/// both are, in the end, "waiting for a slot") — but the two are different
/// FACTS for an admin: one means "we tried to reserve and the pool was
/// genuinely empty", the other means "we never tried, because you asked for
/// at most N and N were already admitted". Telling an admin the pool was
/// empty when it was actually their own requested cap would be a false
/// statement about their own purchase.
pub(crate) const ADMIT_CAP_REACHED_REASON: &str = "admit_cap_reached";

/// Personal-org refusal for the ADMIN routes — same bar
/// `routes::org_slots::refuse_personal_org` uses for its own admin actions,
/// duplicated here (rather than made `pub(crate)` and imported) because it
/// is eight lines and this module already imports enough from
/// `org_invites`; see that function's own doc comment for why
/// `account_is_real_org` needs the CALLER'S OWN `user_id` as its second
/// argument (an admin can only ever ask about their own tenant context here,
/// so that is exactly what `ctx.user_id` is).
async fn refuse_personal_org(state: &AppState, ctx: &crate::tenancy::runtime::TenantContext) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(unavailable());
    };
    match store.account_is_real_org(ctx.account_id, ctx.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("personal accounts have no join-request queue")),
        Err(e) => Err(internal("account lookup", e)),
    }
}

// ── Requester: POST /v1/join-requests ───────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct CreateJoinRequestRequest {
    /// The target org's Clerk id (`org_...`) — the SAME identifier
    /// `TenantContext::org_id` carries for a member of that org, but this
    /// caller is not one, so it must be named explicitly rather than read
    /// off their own tenant binding.
    pub org: String,
    /// D19/CS-440 (org-onboarding plan §3 item 29) — the signed `?t=` value
    /// from a "join this org" email's link
    /// (`services::unmatched_recipient_email::verify_join_notice_token`).
    /// Optional and additive: every EXISTING caller of this route (the
    /// waiting-state banner, item 39; any other ordinary self-request) omits
    /// it and keeps working unchanged — see [`create`]'s own doc comment for
    /// exactly how a present-but-invalid/expired/mismatched token is handled
    /// (silently falls back to an ordinary self-request; NEVER a different
    /// HTTP response, NEVER an error naming which check failed).
    #[serde(default)]
    pub token: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct JoinRequestSentResponse {
    /// Always `"sent_to_admin"` — D9/D10: whether this queued a fresh
    /// request, found one already open, or was silently suppressed (a
    /// durable rejection, or a personal-org target that slipped past the
    /// route's own 404) is never disclosed to the requester.
    pub status: String,
}

/// Resolve `org` (a `clerk_org_id`) to a real, non-personal `accounts.id` —
/// shared by [`create`] and, indirectly, nothing else (the admin routes
/// resolve their own org through [`Tenant`], never through this). Returns
/// [`org_not_found`] for both "no such account" and "that account is a
/// personal org" — see this module's own doc comment for why those two must
/// read identically to the caller.
async fn resolve_target_org(control: &sqlx::PgPool, clerk_org_id: &str) -> Result<Uuid, Response> {
    let account_id: Option<Uuid> = sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(clerk_org_id)
        .fetch_optional(control)
        .await
        .map_err(|e| internal("resolve org", e))?;
    let Some(account_id) = account_id else {
        return Err(org_not_found());
    };
    if svc::is_personal_org(control, account_id).await.map_err(|e| internal("personal-org check", e))? {
        return Err(org_not_found());
    }
    Ok(account_id)
}

#[utoipa_path(
    post,
    path = "/v1/join-requests",
    tag = "join_requests",
    security(("bearer_token" = [])),
    request_body = CreateJoinRequestRequest,
    responses(
        (status = 202, description = "Always — request sent to the org's admin (D9/D10: never discloses more)", body = JoinRequestSentResponse),
        (status = 404, description = "No such org, or the org is a personal org (D19) — same body either way"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn create(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Json(body): Json<CreateJoinRequestRequest>,
) -> Response {
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let clerk_org_id = body.org.trim();
    if clerk_org_id.is_empty() {
        return org_not_found();
    }

    let account_id = match resolve_target_org(control, clerk_org_id).await {
        Ok(id) => id,
        Err(resp) => return resp,
    };

    let email: Option<String> = match sqlx::query_scalar("SELECT email FROM users WHERE id = $1")
        .bind(user.user_id)
        .fetch_optional(control)
        .await
    {
        Ok(e) => e,
        Err(e) => return internal("load requester email", e),
    };

    let tombstoned = match crate::clerk_provision::invite_suppressed_by_tombstone(
        control,
        Some(clerk_org_id),
        Some(user.user_id),
        email.as_deref(),
    )
    .await
    {
        Ok(t) => t,
        Err(e) => return internal("tombstone check", e),
    };

    // D19/CS-440 (org-onboarding plan §3 item 29) — a present `token` is
    // verified against THIS caller and THIS target org before it is ever
    // trusted. `verify_join_notice_token` is pure (a signed JWT — see
    // `services::unmatched_recipient_email`'s own "join this org" module doc
    // section for the full token design and why it changed mid-
    // implementation to accommodate CS-439's landing page), so this is a
    // sync call, no second DB round trip. Every failure mode (unknown/
    // malformed token, a bad or absent signature, an expired token, a token
    // for a DIFFERENT org, or a token whose `sub` does not equal THIS
    // authenticated caller's own `user_id` — e.g. the email was forwarded
    // to someone else, who is signed into their OWN CoralSwarm account)
    // collapses to the SAME silent fallback: an ordinary self-request. This
    // route must never disclose WHICH check failed, or even THAT one did —
    // the 202 body is byte-identical either way (this story's own brief:
    // "never disclose token validity to the caller — same 202"). A
    // tombstoned caller short-circuits this entirely (checked FIRST,
    // above): a removed member must not be able to rejoin silently by
    // replaying an invite link sent before their removal.
    let matched_not_member = if tombstoned {
        None
    } else if let Some(token) = body.token.as_deref().filter(|t| !t.trim().is_empty()) {
        crate::services::unmatched_recipient_email::join_notice_secret()
            .and_then(|secret| crate::services::unmatched_recipient_email::verify_join_notice_token(&secret, token))
            .filter(|ctx| ctx.account_id == account_id && ctx.user_id == user.user_id)
    } else {
        None
    };

    let (mut source, mut evidence) = if tombstoned {
        ("tombstoned_returning", json!({ "email": email, "tombstoned": true }))
    } else if let Some(ctx) = matched_not_member.as_ref() {
        // Borrowed, not moved (CS-447): the ladder below reads this same
        // token's `membership_class` for D16 rung 2's workspace half.
        (
            "matched_not_member",
            json!({
                "email": ctx.email,
                "workspace_id": ctx.workspace_id,
                "channel_id": ctx.channel_id,
                "invited_at": ctx.invited_at,
            }),
        )
    } else {
        ("self_request", json!({ "email": email, "tombstoned": false }))
    };

    // CS-443 (org-onboarding plan §3 item 6, D16) — the assertion ladder's
    // verdict, recorded on every queued request under `evidence.ladder`.
    //
    // This is the ladder's live caller. It does NOT auto-add anyone (item 9,
    // next wave) and it does not change this route's behaviour at all: the
    // verdict is evidence the ADMIN deciding this request can see, which is
    // the honest minimum for a ladder that would otherwise be built and
    // never called. An admin looking at a queued row can now tell "she is a
    // full member of your Slack on your verified domain" apart from "we know
    // nothing about her but the address she signed up with."
    //
    // The DIRECTORY signal is absent here by construction and is reported
    // as absent rather than guessed: no live Google directory read
    // (`Err(NO_DIRECTORY_AVAILABLE)` → `DirectorySignal::Unavailable`).
    // Deliberately NOT `Ok(&empty_map)`, which would assert "we asked
    // Acme's directory and she is not in it" — D16's own trap, and the one
    // thing this ladder must never say by accident. Firing a live directory
    // read from a requester-facing POST would also let any signed-in user
    // spend the target org's Google quota. **Consequence, stated plainly:
    // rung 1 can never fire at THIS call site, so auto-add here is a rung-2
    // feature.** Rung 1 stays reachable by the ladder for a caller that
    // already holds a directory (there is none today).
    //
    // The WORKSPACE signal (CS-447) comes from the join-notice token when
    // there is one — see `workspace_signal_from_token`. Without a token it
    // is `None` → `WorkspaceSignal::Unknown`, which never admits.
    let ladder_email = evidence
        .get("email")
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .unwrap_or_default();
    let ladder = crate::services::assertion_ladder::evaluate_for_email(
        control,
        account_id,
        &ladder_email,
        Err(crate::services::assertion_ladder::NO_DIRECTORY_AVAILABLE),
        matched_not_member.as_ref().and_then(workspace_signal_from_token),
    )
    .await;
    evidence["ladder"] = ladder.evidence();

    // ── Item 9 (CS-447): auto-add, gated on domain + pool + tombstone ─────
    //
    // The ONE decision point (D11/D12/D1). Everything below either adds
    // them outright or falls through to the SAME enqueue this route has
    // always done — the requester's answer is byte-identical in every
    // falling case, and which gate refused is recorded for the admin only.
    let settings = auto_add::read_settings_or_disabled(control, account_id).await;
    let already_member = match member_exists(control, account_id, user.user_id).await {
        Ok(m) => m,
        Err(e) => return internal("membership check", e),
    };
    // D10, on the path that ADDS. `enqueue_join_request` below applies the
    // same predicate, but the adding path returns before reaching it, so
    // without this read an admin's rejection is reversed the moment any
    // other gate opens. See `auto_add::decide`'s own doc comment for the
    // full walkthrough; this is the read it needs.
    //
    // A failure here REFUSES rather than proceeding: "we could not tell
    // whether they were rejected" must never resolve to "add them".
    let previously_rejected = match svc::has_rejected_decision(control, account_id, user.user_id).await {
        Ok(r) => r,
        Err(e) => return internal("rejection check", e),
    };
    // The token's `cls` is a snapshot of a roster walk, not a live fact.
    // Auto-add acts on it only while it is fresh; the LINK keeps its own
    // 30-day life either way.
    let classification_fresh = auto_add::token_classification_is_fresh(
        matched_not_member.as_ref().map(|c| c.invited_at),
        chrono::Utc::now(),
    );
    let decision = auto_add::decide(
        auto_add::AutoAddGates {
            enabled: settings.enabled,
            already_member,
            tombstoned,
            previously_rejected,
            classification_fresh,
        },
        &ladder,
    );
    let fall_reason = match decision {
        auto_add::AutoAddDecision::TryReserve(rung) => {
            match try_auto_add(&state, control, account_id, clerk_org_id, user.user_id, rung, settings, &ladder).await {
                // The person is IN. No request is queued — an auto-added
                // member never creates one (D13's 2026-09-22 amendment);
                // `org_auto_adds` is what tells their admin it happened.
                // Close any older OPEN request for this org so the admin
                // queue and `/v1/me/join-requests` do not keep a stale row.
                AutoAddOutcome::Added => {
                    close_matching_open_requests(
                        control,
                        account_id,
                        clerk_org_id,
                        user.user_id,
                        json!({ "auto_added": true }),
                    )
                    .await;
                    return auto_added();
                }
                AutoAddOutcome::PoolFull => {
                    // D1, the gate this whole feature exists for: an empty
                    // pool falls to approval. It does NOT purchase — that
                    // is `approve`'s branch, where an admin's click is the
                    // human consent D1 requires to spend.
                    source = POOL_FULL_SOURCE;
                    evidence["auto_add"] = json!({ "attempted": true, "blocked_by": POOL_FULL_SOURCE });
                    None
                }
                AutoAddOutcome::Unavailable(why) => {
                    evidence["auto_add"] = json!({ "attempted": true, "blocked_by": "unavailable", "detail": why });
                    None
                }
            }
        }
        auto_add::AutoAddDecision::Fall(auto_add::FallReason::AlreadyMember) => {
            // Already a local member: do not enqueue (a later approve
            // would reserve/purchase another seat) and close any leftover
            // open request as approved so the queue count is honest.
            close_matching_open_requests(
                control,
                account_id,
                clerk_org_id,
                user.user_id,
                json!({ "already_member": true }),
            )
            .await;
            return auto_added();
        }
        auto_add::AutoAddDecision::Fall(reason) => Some(reason),
    };
    if let Some(reason) = fall_reason {
        if let Some(override_source) = reason.source_override() {
            source = override_source;
        }
        evidence["auto_add"] = json!({ "attempted": false, "blocked_by": reason.label() });
    }

    match svc::enqueue_join_request(control, account_id, user.user_id, source, evidence).await {
        Ok(_outcome) => sent_to_admin(),
        Err(e) => internal("enqueue", e),
    }
}

/// Rung 2's workspace half, at a call site with no live Slack connection
/// (CS-447).
///
/// D16 rung 2 is *"full workspace member **and** verified domain match"*,
/// read (per D21) with a connected member's own token. That read already
/// happens — in `connectors::slack_unmatched_email::discover_unmatched_
/// recipients`, which walks the roster on a connected member's token and
/// classifies every person it finds. A person arriving here with a valid
/// join-notice token is arriving BECAUSE of that walk, carrying its verdict
/// with them: the token is a JWT this server minted and has just verified
/// against this caller and this org.
///
/// **`class` is carried on the token rather than re-derived**, because the
/// alternative is a live `users.lookupByEmail` + `users.info` fired from a
/// requester-facing POST, on the target org's credential and quota, for a
/// stranger who may be probing.
///
/// # Why this is not "a token means full member"
///
/// It would be very easy, and wrong, to read the mere PRESENCE of a
/// join-notice token as the rung-2 signal. `discover_unmatched_recipients`
/// admits a `Guest` or a `SlackConnect` participant to that audience when
/// their address is on the org's anchor domain (its own D2/D21 rule) — so
/// "we emailed them a join link" and "they are a full member of the
/// workspace" are genuinely different facts, and D16 is explicit that only
/// the second is worth anything. This function maps the classification and
/// nothing else:
///
/// ```text
/// FullMember         -> Some(FullMember)   (rung 2's half, satisfied)
/// Guest/SlackConnect -> Some(NotFullMember)(recorded, never admits)
/// Unavailable / none -> None               -> WorkspaceSignal::Unknown
/// ```
///
/// A token minted before CS-447 carries no class at all and yields `None`,
/// which `assertion_ladder` reads as `Unknown` — fail closed, and the
/// person simply reaches the approval queue as they did last week.
fn workspace_signal_from_token(
    ctx: &crate::services::unmatched_recipient_email::JoinNoticeContext,
) -> Option<crate::connectors::slack::MembershipClass> {
    ctx.membership_class
}

/// Is this person already a member of this org?
///
/// `create` has never had a membership precondition — an existing member
/// POSTing here simply queued a redundant request, which was harmless.
/// With auto-add live it stops being harmless: a member is a person we
/// would otherwise reserve a second slot for. Read through
/// `begin_account_scoped` like every other `account_members` read on this
/// plane.
///
/// `pub(crate)` since CS-449 (item 38): [`crate::services::org_auto_add::
/// regate_queued_request`] reuses this exact read rather than a second copy
/// of the query.
pub(crate) async fn member_exists(
    control: &sqlx::PgPool,
    account_id: Uuid,
    user_id: Uuid,
) -> Result<bool, crate::store::StoreError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id).await?;
    let exists: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)")
            .bind(account_id)
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(crate::store::StoreError::from)?;
    tx.rollback().await.ok();
    Ok(exists)
}

/// Close this user's OPEN requests for `clerk_org_id` as `approved`.
///
/// Used after auto-add succeeds, and when `create` finds they are already
/// a member. Failures are logged and never fail the caller — the
/// membership is already real (or already was), and a leftover open row is
/// bookkeeping. `decision = 'approved'` is the only value 0238's CHECK
/// admits for "they are in"; evidence carries `auto_added` / `already_member`.
async fn close_matching_open_requests(
    control: &sqlx::PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    user_id: Uuid,
    evidence: serde_json::Value,
) {
    match svc::list_open_for_user(control, user_id).await {
        Ok(open_requests) => {
            for open in open_requests.into_iter().filter(|request| request.clerk_org_id == clerk_org_id)
            {
                if let Err(e) = svc::mark_decided(
                    control,
                    open.id,
                    account_id,
                    user_id,
                    "approved",
                    evidence.clone(),
                )
                .await
                {
                    tracing::error!(
                        error = %e,
                        %account_id,
                        user_id = %user_id,
                        request_id = %open.id,
                        "join_request: could not close an open request after membership was already real"
                    );
                }
            }
        }
        Err(e) => tracing::error!(
            error = %e,
            %account_id,
            user_id = %user_id,
            "join_request: could not load open requests to close after membership was already real"
        ),
    }
}

/// What [`try_auto_add`] did.
///
/// `pub(crate)` since CS-449 (item 38): [`regate_and_admit_one`] — the
/// admit-queued/drain spine, in this same file — matches on it too.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum AutoAddOutcome {
    /// A real Clerk + local membership now exists, against a slot the org
    /// had already bought.
    Added,
    /// **Gate 2 (D1).** No free slot in the chosen tier. Falls to approval
    /// with `source = 'pool_full'`. Never a purchase.
    PoolFull,
    /// We could not complete the add for a reason that is about US, not
    /// about the person: no Clerk admin key, no linked Clerk identity, a
    /// Clerk 5xx, a local mirror failure, or no seat tier configured. The
    /// person falls to approval keeping their ordinary source, and any
    /// reservation taken is released — an admin can still let them in.
    Unavailable(&'static str),
}

/// Gates 2 and 3's spending half: reserve a bought slot, then make a real
/// membership out of it. Called only after [`auto_add::decide`] has
/// returned [`auto_add::AutoAddDecision::TryReserve`].
///
/// # D1: this function never calls Stripe, and cannot
///
/// The ONLY way a slot becomes a seat here is
/// [`slot_pool::reserve_slot`] → `mirror_local_membership` →
/// `ensure_org_membership` → `acquire_seat`'s **consume-before-buy** branch,
/// which fulfils a live reservation and returns without a Stripe call at
/// all (see `seat_lifecycle::acquire_seat`'s own "CS-435 (D1/D7/D9)"
/// section). The purchase branch is unreachable from here for a
/// structural reason, not a hopeful one: `acquire_seat` only reaches Stripe
/// when there is no live reservation AND no free slot, and this function
/// returns [`AutoAddOutcome::PoolFull`] — before touching Clerk at all —
/// the moment `reserve_slot` answers `None`, which is exactly the
/// condition under which the pool is empty. `approve` takes the other
/// branch deliberately (an admin's click is consent to spend); auto-add has
/// no admin and therefore no consent.
///
/// This is proved, not asserted: `pg_tests::auto_add_never_calls_stripe_
/// even_when_the_pool_is_empty` runs the whole path against a Stripe mock
/// that counts every request it receives and fails the test on the first.
///
/// # Order (D3's Clerk-first sequence, same as `approve`)
///
/// 1. resolve the tier — no tier ⇒ `Unavailable`, nothing reserved;
/// 2. `reserve_slot` against THAT tier ⇒ `None` is `PoolFull`;
/// 3. Clerk membership, then the local mirror (which fulfils the
///    reservation);
/// 4. on any failure in 3: release the reservation (`Released`, not
///    `Fulfilled`) and fall to approval — the person is never lost, and the
///    slot is never leaked;
/// 5. record `org_auto_adds` LAST, after the membership is real.
///
/// `state: &AppState`, not `&Arc<AppState>` — widened for CS-449 (item 38):
/// [`regate_and_admit_one`] is reachable from places that only ever hold a
/// plain `&AppState` (a member-removal webhook handler several layers past
/// its own `Arc`, a `tenant_harness` sweep worker's own `tick`). Every
/// existing caller here still compiles unchanged — `&state` where `state:
/// Arc<AppState>` deref-coerces to `&AppState` at the call site, the same
/// coercion `clerk_provision::ensure_org_membership`'s own `&AppState`
/// parameter already relies on two calls further down this same chain
/// (`mirror_local_membership` below).
///
/// `target_user_id`, not `&UserContext` — widened for the same reason: the
/// person being auto-added is the CALLER in `create`'s self-service flow
/// (hence `UserContext` there), but is an admin's or a sweep's TARGET in
/// [`regate_and_admit_one`], which has no `UserContext` for them at all,
/// only their `user_id`. Every field of `UserContext` this function ever
/// read was `.user_id` — see the pre-CS-449 diff.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn try_auto_add(
    state: &AppState,
    control: &sqlx::PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    target_user_id: Uuid,
    rung: crate::services::assertion_ladder::Rung,
    settings: auto_add::AutoAddSettings,
    ladder: &crate::services::assertion_ladder::LadderOutcome,
) -> AutoAddOutcome {
    let tier_id = match auto_add::resolve_tier_id(control, settings).await {
        Ok(Some(id)) => id,
        Ok(None) => {
            tracing::warn!(
                account_id = %account_id,
                "auto_add: no active seat tier to spend against — falling to approval rather \
                 than reserving against a guessed tier"
            );
            return AutoAddOutcome::Unavailable("no_seat_tier");
        }
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "auto_add: seat-tier lookup failed");
            return AutoAddOutcome::Unavailable("tier_lookup_failed");
        }
    };

    // ── Gate 2 (D1) ───────────────────────────────────────────────────────
    // Tier-narrowed on purpose (founder, 2026-09-22): a full `premium`
    // pool falls to approval even with `standard` slots idle.
    // `reserve_slot` reads `read_slot_pool_for_tier_ledger` — the LEDGER
    // key, never the report reader (CS-444).
    let reservation =
        match slot_pool::reserve_slot(control, account_id, target_user_id, Some(tier_id), slot_pool::reservation_ttl())
            .await
        {
            Ok(Some(id)) => id,
            // The one line D1 exists for. No Stripe, no purchase, no
            // "just this once" — approval, where a human decides.
            Ok(None) => return AutoAddOutcome::PoolFull,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %account_id, "auto_add: reserve_slot failed");
                return AutoAddOutcome::Unavailable("reserve_failed");
            }
        };

    let release = |why: &'static str| async move {
        if let Err(e) = slot_pool::release_reservation(control, reservation, ReleaseReason::Released).await {
            tracing::error!(
                error = %e, reservation_id = %reservation, %account_id, step = why,
                "auto_add: could not release the reservation after a failed add — it will still \
                 clear via the expiry sweep"
            );
        }
    };

    let Some(clerk) = ClerkAdminClient::from_env() else {
        release("clerk_key").await;
        return AutoAddOutcome::Unavailable("no_clerk_key");
    };
    let target_clerk_user_id: Option<Option<String>> =
        match sqlx::query_scalar::<_, Option<String>>("SELECT clerk_user_id FROM users WHERE id = $1")
            .bind(target_user_id)
            .fetch_optional(control)
            .await
        {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "auto_add: clerk_user_id lookup failed");
                release("clerk_user_id").await;
                return AutoAddOutcome::Unavailable("clerk_user_lookup_failed");
            }
        };
    let Some(target_clerk_user_id) = target_clerk_user_id.flatten() else {
        release("no_clerk_identity").await;
        return AutoAddOutcome::Unavailable("no_clerk_identity");
    };

    // `newly_created: false` — this person signed in to get here, so their
    // Clerk identity pre-existed and must never be deleted by an unwind
    // (`unwind_clerk`'s "never touch a reused, pre-existing Clerk identity"
    // rule, inherited through `mirror_local_membership`).
    let created = CreatedUser { clerk_user_id: target_clerk_user_id, newly_created: false, last_sign_in_at: None };
    let metadata = json!({
        "auto_added": true,
        "auto_added_at": chrono::Utc::now().to_rfc3339(),
        "ladder_rung": rung.label(),
    });
    let membership =
        match provision_clerk_membership(&clerk, control, clerk_org_id, &created, "org:member", metadata).await {
            Ok(m) => m,
            Err(_resp) => {
                release("clerk_membership").await;
                return AutoAddOutcome::Unavailable("clerk_membership_failed");
            }
        };
    if mirror_local_membership(
        state,
        &clerk,
        control,
        clerk_org_id,
        &created,
        target_user_id,
        "member",
        membership,
        Some(tier_id),
        crate::billing::seat_lifecycle::SeatPurchasePolicy::AllowPurchase,
    )
    .await
    .is_err()
    {
        release("local_mirror").await;
        return AutoAddOutcome::Unavailable("local_mirror_failed");
    }

    // The membership is real from here on. A failure below is bookkeeping,
    // and a real membership is never unwound over bookkeeping (the same
    // rule `approve`'s step 4 states for `mark_decided`) — the cost is one
    // add missing from one digest.
    let mut evidence = ladder.evidence();
    evidence["tier_id"] = json!(tier_id.to_string());
    evidence["slot"] = json!("reserved");
    if let Err(e) =
        auto_add::record_auto_add(control, account_id, target_user_id, rung, Some(tier_id), evidence).await
    {
        tracing::error!(
            error = %e, %account_id, user_id = %target_user_id,
            "auto_add: added the member but failed to record it in org_auto_adds — their admin \
             will not see this add in the daily digest. The membership is NOT unwound."
        );
    }
    tracing::info!(
        %account_id, user_id = %target_user_id, rung = rung.label(), %tier_id,
        "auto_add: added a member from a pre-purchased slot (no Stripe call)"
    );
    AutoAddOutcome::Added
}

// ── CS-449 (item 38, D11's "ask once", and the drain): re-gating an
//    ALREADY-QUEUED request ───────────────────────────────────────────────
//
// `routes::org_auto_add::admit_queued` (an admin's explicit batch click) and
// [`drain_org`] (a slot freeing, no admin present) are the two callers.
// Both route every candidate through [`regate_and_admit_one`], which is
// itself the ONE place either re-derives gates (via [`auto_add::
// regate_queued_request`], which is itself the one place that calls
// [`auto_add::decide`]) or spends a slot (via [`try_auto_add`], the same
// function `create`'s own live auto-add path uses). Neither caller
// re-implements any part of either.

/// [`auto_add::decide`] run against an ALREADY-QUEUED request, with the
/// pool spent for real when it says yes.
///
/// Returns `(request.id, Ok(()))` on a genuine admit, or
/// `(request.id, Err(label))` otherwise — `label` is a stable string an
/// admin-facing caller may show verbatim: either a
/// [`auto_add::FallReason::label`] (one of the five gates, or the ladder),
/// [`POOL_FULL_SOURCE`] (gate 2 — the pool, spent for real and found
/// empty), or `"unavailable"` (an infrastructure failure — no Clerk key, no
/// seat tier configured, a Clerk 5xx, … — [`AutoAddOutcome::Unavailable`]'s
/// own umbrella, matching what `create`'s own fall branch already writes
/// into `evidence.auto_add.blocked_by` for the identical case, and also
/// covering a request whose stored ladder evidence cannot be parsed back at
/// all — see [`auto_add::regate_queued_request`]'s own doc comment for why
/// that must refuse rather than pass).
///
/// The row's OWN state is updated to match, exactly the way `create`'s fall
/// branch would have written it for a fresh insert, so the queue never goes
/// stale relative to what a caller was just told:
///   * **Admitted** — [`svc::mark_decided`] (`decision = 'approved'`),
///     `decided_by = request.user_id` — the same "the added person decided
///     their own now-redundant request" shape `close_matching_open_requests`
///     already uses for `create`'s own auto-add path (there is no admin to
///     attribute this to; a drain has none at all, and an admin's batch
///     click already has its OWN attribution on the `POST` itself).
///   * **Already a member** — the one `Fall` reason that also closes the
///     row (nothing to spend, nothing to wait for — see `approve`'s own
///     identical branch, `member_exists` check).
///   * **Pool full** — [`svc::patch_open_request`] sets `source =
///     'pool_full'` (a no-op if it already was), so this candidate becomes
///     exactly what [`drain_org`] looks for the next time a slot frees —
///     the overflow from "admit what fits" does not require asking the
///     admin a second time.
///   * **Every other `Fall`** — evidence is patched (`source` too, for the
///     one reason that overrides it, [`auto_add::FallReason::
///     source_override`]) but the row is left open and undecided; an admin
///     can still approve it by hand.
pub(crate) async fn regate_and_admit_one(
    state: &AppState,
    control: &sqlx::PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
    settings: auto_add::AutoAddSettings,
    request: &svc::JoinRequestSummary,
    now: chrono::DateTime<chrono::Utc>,
    // CS-449 review round 2 (blocker 1): `false` means this candidate
    // passes every gate but the CALLER's own budget is already spent —
    // `admit_queued`'s `max_admit`, once `admitted.len()` reaches it.
    // `drain_org` always passes `true`: it has no admin-set budget, only
    // the pool's own (bounded by `reserve_slot` itself). See the
    // `TryReserve` arm below for what `false` actually does — it never
    // calls `try_auto_add`, so no reservation is ever attempted.
    may_spend: bool,
) -> (Uuid, Result<(), &'static str>) {
    let regated = match auto_add::regate_queued_request(
        control,
        account_id,
        clerk_org_id,
        request.user_id,
        &request.evidence,
        settings,
        now,
    )
    .await
    {
        Ok(Some(pair)) => pair,
        Ok(None) => {
            tracing::warn!(
                request_id = %request.id, %account_id,
                "auto_add: re-gate could not parse the request's stored ladder evidence — refusing"
            );
            return (request.id, Err("unavailable"));
        }
        Err(e) => {
            tracing::warn!(error = %e, request_id = %request.id, %account_id, "auto_add: re-gate failed");
            return (request.id, Err("unavailable"));
        }
    };
    let (gates, ladder) = regated;

    match auto_add::decide(gates, &ladder) {
        auto_add::AutoAddDecision::TryReserve(_) if !may_spend => {
            // Blocker fix, CS-449 review round 2: this candidate would pass
            // — every gate says yes — but the caller's own budget is
            // already spent, so no reservation is attempted at all (never
            // touches `slot_pool::reserve_slot`). Patched to `pool_full`
            // exactly like the genuinely-empty-pool branch below (same
            // `source`, same drain candidate set), but under the DISTINCT
            // `ADMIT_CAP_REACHED_REASON` label — see that constant's own
            // doc comment for why the two must never be confused.
            //
            // Item 5's no-op-write guard applies here too: a row already
            // `pool_full` (this exact evidence shape, already written by an
            // earlier pass) gains nothing from being written again.
            if request.source != POOL_FULL_SOURCE {
                if let Err(e) = svc::patch_open_request(
                    control,
                    request.id,
                    account_id,
                    Some(POOL_FULL_SOURCE),
                    json!({ "auto_add": { "attempted": false, "blocked_by": ADMIT_CAP_REACHED_REASON } }),
                )
                .await
                {
                    tracing::warn!(error = %e, request_id = %request.id, %account_id, "auto_add: admit-cap patch failed");
                }
            }
            (request.id, Err(ADMIT_CAP_REACHED_REASON))
        }
        auto_add::AutoAddDecision::TryReserve(rung) => {
            match try_auto_add(state, control, account_id, clerk_org_id, request.user_id, rung, settings, &ladder)
                .await
            {
                AutoAddOutcome::Added => {
                    if let Err(e) = svc::mark_decided(
                        control,
                        request.id,
                        account_id,
                        request.user_id,
                        "approved",
                        json!({ "auto_added": true, "admitted_via": "queued_batch" }),
                    )
                    .await
                    {
                        tracing::error!(
                            error = %e, request_id = %request.id, %account_id,
                            "auto_add: admitted from the queue but failed to mark the request decided — \
                             the membership is real; this is bookkeeping only"
                        );
                    }
                    (request.id, Ok(()))
                }
                AutoAddOutcome::PoolFull => {
                    // Item 5's no-op-write guard: `drain_org`'s entire
                    // candidate set is PRE-FILTERED to `source = 'pool_full'`
                    // (that filter is the whole of it), so patching it to
                    // the SAME value with the SAME static evidence shape is
                    // a guaranteed no-op write — multiplied by every drain
                    // pass over a backlog that has not moved. Only WRITE
                    // when the source is actually changing, which is still
                    // every time `admit_queued` is the caller (its
                    // candidates start from `self_request`/etc).
                    if request.source != POOL_FULL_SOURCE {
                        if let Err(e) = svc::patch_open_request(
                            control,
                            request.id,
                            account_id,
                            Some(POOL_FULL_SOURCE),
                            json!({ "auto_add": { "attempted": true, "blocked_by": POOL_FULL_SOURCE } }),
                        )
                        .await
                        {
                            tracing::warn!(
                                error = %e, request_id = %request.id, %account_id,
                                "auto_add: pool_full patch failed"
                            );
                        }
                    }
                    (request.id, Err(POOL_FULL_SOURCE))
                }
                AutoAddOutcome::Unavailable(why) => {
                    tracing::warn!(
                        reason = why, request_id = %request.id, %account_id,
                        "auto_add: re-gate passed but the add itself could not complete"
                    );
                    (request.id, Err("unavailable"))
                }
            }
        }
        auto_add::AutoAddDecision::Fall(auto_add::FallReason::AlreadyMember) => {
            if let Err(e) = svc::mark_decided(
                control,
                request.id,
                account_id,
                request.user_id,
                "approved",
                json!({ "already_member": true }),
            )
            .await
            {
                tracing::error!(
                    error = %e, request_id = %request.id, %account_id,
                    "auto_add: re-gate found an existing member but marking the request decided failed"
                );
            }
            (request.id, Err(auto_add::FallReason::AlreadyMember.label()))
        }
        auto_add::AutoAddDecision::Fall(reason) => {
            if let Err(e) = svc::patch_open_request(
                control,
                request.id,
                account_id,
                reason.source_override(),
                json!({ "auto_add": { "attempted": false, "blocked_by": reason.label() } }),
            )
            .await
            {
                tracing::warn!(error = %e, request_id = %request.id, %account_id, "auto_add: fall patch failed");
            }
            (request.id, Err(reason.label()))
        }
    }
}

/// [`drain_org`]'s own report — admitted ids, and every candidate that was
/// NOT admitted with the same stable label [`regate_and_admit_one`] returns.
#[derive(Debug, Default)]
pub(crate) struct DrainReport {
    pub admitted: Vec<Uuid>,
    pub not_admitted: Vec<(Uuid, &'static str)>,
}

/// **Item 9's end state.** A candidate that fell for
/// [`auto_add::FallReason::StaleClassification`] is structurally incapable
/// of ever passing again: staleness is measured against `now`, which only
/// moves forward, so a row already past the 48-hour window stays past it
/// forever (see [`auto_add::token_classification_is_fresh`]). Left in
/// [`drain_org`]'s candidate set, such a row would be re-gated (two
/// account-scoped transactions plus two queries) on every single trigger
/// and every sweep tick, FOREVER, for an answer that can never change — and
/// would keep `workers::auto_add_drain_sweep` reporting a non-idle tick
/// forever too (that worker's own item-8 fix), defeating its backoff.
///
/// The end state chosen: the row LEAVES the drain's candidate set the first
/// time this is observed — it stays exactly where
/// [`regate_and_admit_one`]'s own "every other `Fall` reason" branch
/// already left it: OPEN, `source` unchanged (`pool_full`), undecided, with
/// `evidence.auto_add.blocked_by = "classification_too_old"` recorded for a
/// human reading the ordinary admin queue to see and act on by hand.
/// Nothing NEW is written here — this predicate only stops the drain asking
/// the same unanswerable question forever once the first answer is already
/// on the row.
fn permanently_excluded_from_drain(evidence: &serde_json::Value) -> bool {
    evidence.get("auto_add").and_then(|a| a.get("blocked_by")).and_then(|b| b.as_str())
        == Some(auto_add::FallReason::StaleClassification.label())
}

/// Pull the oldest `source = 'pool_full'` request(s) for `account_id` back
/// through the gates and admit whatever now fits — CS-449 (item 38's other
/// half, "the drain"). See this file's own module doc and
/// `services::org_auto_add`'s for the full framing; see `workers::
/// auto_add_drain_sweep`, `clerk_provision::remove_org_membership`, and
/// `routes::org_slots::purchase_slots` for the three call sites.
///
/// Only for orgs where auto-add is enabled RIGHT NOW — `decide`'s own first
/// gate already refuses everyone the instant `enabled` reads false, so this
/// early return is purely to skip the work for the common case (most orgs
/// have auto-add off), not a second copy of that rule.
///
/// Oldest first (the same `ORDER BY requested_at ASC` every other queue
/// read in this file uses), capped at
/// [`auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES`] (item 7 — see that
/// constant's own doc comment) and excluding every row
/// [`permanently_excluded_from_drain`] already proved can never pass (item
/// 9), spent one at a time through [`regate_and_admit_one`] with
/// `may_spend = true` (the drain has no admin-set budget, only the pool's
/// own).
///
/// **Item 5.** The first candidate to hit a GENUINELY empty pool (a real
/// reservation attempt, `Err(POOL_FULL_SOURCE)` — never the budget-capped
/// branch, which `may_spend = true` here makes unreachable) DOES stop the
/// loop: every candidate in one `drain_org` call resolves the SAME tier
/// (`settings.default_seat_tier_id`, read once above, never re-read per
/// candidate — D11's own "the pool check is against THAT TIER ONLY"), so
/// once one reservation attempt finds it empty, every attempt after it
/// would too. Stopping saves two account-scoped transactions plus two
/// queries per skipped candidate, plus the reservation transaction itself
/// (`lock_period_rotation` included) for a result already proven — on a
/// large backlog behind a genuinely full pool, this is the difference
/// between one failed attempt and hundreds. A candidate that instead falls
/// for any OTHER reason (a gate, or the budget cap — unreachable here) does
/// NOT stop the loop: only a real pool-empty result says anything about the
/// candidates behind it.
pub(crate) async fn drain_org(
    state: &AppState,
    control: &sqlx::PgPool,
    account_id: Uuid,
    clerk_org_id: &str,
) -> DrainReport {
    let settings = auto_add::read_settings_or_disabled(control, account_id).await;
    if !settings.enabled {
        return DrainReport::default();
    }
    let requests =
        match svc::list_open_capped(control, account_id, auto_add::MAX_AUTO_ADD_BATCH_CANDIDATES).await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "auto_add: drain could not list open requests");
                return DrainReport::default();
            }
        };
    let now = chrono::Utc::now();
    let mut report = DrainReport::default();
    for request in requests
        .iter()
        .filter(|r| r.source == POOL_FULL_SOURCE && !permanently_excluded_from_drain(&r.evidence))
    {
        match regate_and_admit_one(state, control, account_id, clerk_org_id, settings, request, now, true).await {
            (id, Ok(())) => report.admitted.push(id),
            (id, Err(reason)) => {
                report.not_admitted.push((id, reason));
                if reason == POOL_FULL_SOURCE {
                    break;
                }
            }
        }
    }
    if !report.admitted.is_empty() {
        tracing::info!(
            %account_id, admitted = report.admitted.len(), not_admitted = report.not_admitted.len(),
            "auto_add: drain admitted from the pool_full backlog after a slot freed"
        );
    }
    report
}

// ── Requester: GET /v1/me/join-requests ─────────────────────────────────────

#[derive(Debug, Serialize, ToSchema)]
pub struct MyJoinRequestOrg {
    pub clerk_org_id: String,
    pub name: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MyJoinRequestItem {
    pub id: Uuid,
    pub org: MyJoinRequestOrg,
    #[schema(value_type = String, format = DateTime)]
    pub requested_at: chrono::DateTime<chrono::Utc>,
}

#[utoipa_path(
    get,
    path = "/v1/me/join-requests",
    tag = "join_requests",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "The caller's own OPEN join requests — no count, no ETA, nothing about the pool", body = [MyJoinRequestItem]),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list_mine(State(state): State<Arc<AppState>>, Extension(user): Extension<UserContext>) -> Response {
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let rows = match svc::list_open_for_user(control, user.user_id).await {
        Ok(r) => r,
        Err(e) => return internal("list mine", e),
    };

    let clerk = ClerkAdminClient::from_env();
    let mut items = Vec::with_capacity(rows.len());
    for row in rows {
        let name = match &clerk {
            Some(c) => c.get_organization(&row.clerk_org_id).await.ok().map(|o| o.name),
            None => None,
        };
        items.push(MyJoinRequestItem {
            id: row.id,
            org: MyJoinRequestOrg { clerk_org_id: row.clerk_org_id, name },
            requested_at: row.requested_at,
        });
    }
    (StatusCode::OK, Json(items)).into_response()
}

// ── Admin: GET /v1/orgs/join-requests ───────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct ListJoinRequestsQuery {
    /// How many days of DECIDED history to include alongside the open
    /// queue. Absent/`0` ⇒ open requests only.
    #[serde(default)]
    pub include_decided: Option<i64>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminJoinRequestUser {
    pub user_id: Uuid,
    pub name: Option<String>,
    pub email: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct AdminJoinRequestItem {
    pub id: Uuid,
    pub user: AdminJoinRequestUser,
    pub source: String,
    pub evidence: serde_json::Value,
    #[schema(value_type = String, format = DateTime)]
    pub requested_at: chrono::DateTime<chrono::Utc>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub decided_at: Option<chrono::DateTime<chrono::Utc>>,
    pub decided_by: Option<Uuid>,
    /// `"approved"` / `"rejected"` / `null` (still open).
    pub decision: Option<String>,
}

impl From<svc::JoinRequestSummary> for AdminJoinRequestItem {
    fn from(r: svc::JoinRequestSummary) -> Self {
        AdminJoinRequestItem {
            id: r.id,
            user: AdminJoinRequestUser { user_id: r.user_id, name: r.user_name, email: r.user_email },
            source: r.source,
            evidence: r.evidence,
            requested_at: r.requested_at,
            decided_at: r.decided_at,
            decided_by: r.decided_by,
            decision: r.decision,
        }
    }
}

#[utoipa_path(
    get,
    path = "/v1/orgs/join-requests",
    tag = "join_requests",
    security(("bearer_token" = [])),
    params(("include_decided" = Option<i64>, Query, description = "Days of decided history to include after the open queue")),
    responses(
        (status = 200, description = "Open requests first, then recently decided ones", body = [AdminJoinRequestItem]),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn list(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    axum::extract::Query(q): axum::extract::Query<ListJoinRequestsQuery>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let account_id = ctx.account_id;

    let mut items: Vec<AdminJoinRequestItem> = match svc::list_open(control, account_id).await {
        Ok(rows) => rows.into_iter().map(Into::into).collect(),
        Err(e) => return internal("list open", e),
    };
    if let Some(days) = q.include_decided.filter(|d| *d > 0) {
        match svc::list_recent_decided(control, account_id, days).await {
            Ok(rows) => items.extend(rows.into_iter().map(Into::into)),
            Err(e) => return internal("list decided", e),
        }
    }
    (StatusCode::OK, Json(items)).into_response()
}

// ── Admin: GET /v1/orgs/join-requests/count ─────────────────────────────────

/// The members-page badge's own read (CS-438, org-onboarding plan §3 item
/// 5, D13) — a bare open count, never the rows themselves, so the badge
/// stays cheap on every page load. Same admin/personal-org gate as [`list`]
/// immediately above.
#[derive(Debug, Serialize, ToSchema)]
pub struct JoinRequestCountResponse {
    pub open: i64,
}

#[utoipa_path(
    get,
    path = "/v1/orgs/join-requests/count",
    tag = "join_requests",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Open join-request count, for the admin badge (D13)", body = JoinRequestCountResponse),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn count(
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
        return unavailable();
    };
    match svc::count_open(control, ctx.account_id).await {
        Ok(open) => (StatusCode::OK, Json(JoinRequestCountResponse { open })).into_response(),
        Err(e) => internal("count open", e),
    }
}

// ── Admin: POST /v1/orgs/join-requests/{id}/reject ──────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/join-requests/{id}/reject",
    tag = "join_requests",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "org_join_requests.id")),
    responses(
        (status = 204, description = "Rejected — silent and durable (D10): no email, no notice to the requester"),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 404, description = "No such OPEN request for this org"),
        (status = 503, description = "Database unavailable"),
    )
)]
pub async fn reject(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let account=user.account_id;
    let response=reject_committed(State(state.clone()),tenant,Extension(user),Path(id)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn reject_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };

    match svc::mark_decided(control, id, ctx.account_id, user.user_id, "rejected", json!({})).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_found("no open join request with that id for this org"),
        Err(e) => internal("reject", e),
    }
}

// ── Admin: POST /v1/orgs/join-requests/{id}/approve ─────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct ApproveJoinRequestRequest {
    /// `seat_tiers.code`, defaulted to `standard` (D8: "one extra control on
    /// the row, defaulted to `standard`, so the one-click case stays one
    /// click"). Validated against `seat_tiers` (active only), recorded on
    /// the decision row's `evidence.tier`, AND (CS-442, item 40) resolved to
    /// a `seat_tiers.id` and threaded through to
    /// `clerk_provision::ensure_org_membership`'s `seat_tier_id` parameter
    /// — see [`approve`]'s own doc comment for the full step 2 sequence.
    #[serde(default = "default_tier")]
    pub tier: String,
}
fn default_tier() -> String {
    "standard".to_string()
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ApproveJoinRequestResponse {
    pub user_id: Uuid,
    /// `"reserved"` — a pre-bought slot from the pool was consumed; or
    /// `"purchased"` — the pool was full and Stripe was charged for one more
    /// (D9's own "admin's approve is the human consent D1 requires to
    /// spend"); or `"already_member"` — they already had an `account_members`
    /// row, so no slot was reserved or purchased. Never surfaced to the
    /// requester, only to the admin who just approved.
    pub slot: String,
    /// `seat_tiers.code` — the tier this member was actually provisioned at
    /// (CS-442, item 40). Echoes the (validated, possibly-defaulted)
    /// request `tier`.
    pub tier: String,
}

/// D3's Clerk-first approval sequence — in order:
///
/// 1. [`slot_pool::reserve_slot`]. `Some(id)` ⇒ `slot = "reserved"`.
///    `None` (pool full) ⇒ proceed in PURCHASE mode anyway — an admin's
///    approve click IS the human consent D1 requires to spend (D9's own
///    text) — `slot = "purchased"`, `evidence.pool_full = true`. Neither
///    outcome is ever disclosed to the requester.
/// 2. [`provision_clerk_membership`] — Clerk `create_org_membership(org_id,
///    users.clerk_user_id, "org:member", metadata)`, treating Clerk's
///    `already_a_member_in_organization` as success (unlike `create_invite`'s
///    own 409 — see that function's own doc comment), then
///    [`mirror_local_membership`] — `ensure_org_membership(.., "member",
///    true, Some(tier_id))`, which fulfils the live reservation (if any) and
///    stamps/purchases the slot AT THE RESOLVED TIER. A synthetic
///    [`CreatedUser`] stands in for the Clerk-create-user step
///    `create_invite` runs (there is none here — this person already has a
///    Clerk identity): `newly_created: false`, so a failure below NEVER
///    deletes the Clerk user (they pre-existed; see `unwind_clerk`'s own
///    "never touch a reused, pre-existing Clerk identity" rule, inherited
///    unchanged through [`mirror_local_membership`]).
/// 3. On failure of EITHER half of step 2: release whatever reservation
///    step 1 took (`Released`, not `Fulfilled`) and return the ALREADY-
///    MAPPED error. The `org_join_requests` row is left OPEN (never marked
///    decided) — an admin can retry the same approve.
/// 4. Only once step 2 succeeds: [`svc::mark_decided`]`(approved, …)`. If
///    THIS write fails after a real Clerk+local membership already exists,
///    log at error and still return success to the admin — the same
///    "recoverable double-entry" shape `routes::org_invites`'s own module
///    doc documents for its two-webhook race; a real membership is never
///    unwound over a bookkeeping row.
///
/// # Tier (CS-442, item 40, D8 — closes the gap CS-437 left open)
///
/// `tier` is resolved to a `seat_tiers.id` (active only; unknown/inactive ⇒
/// 400), recorded in `evidence.tier`, AND plumbed all the way through: step
/// 1's [`slot_pool::reserve_slot`] reserves against THAT tier's own
/// availability (never the account's total pool — a `pro` approval must
/// never silently consume a slot someone bought as `standard`), and step
/// 2's [`mirror_local_membership`] passes `Some(tier_id)` into
/// `clerk_provision::ensure_org_membership`, which assigns it on the
/// fresh-insert path — see that function's own "Seat tier" doc section for
/// why this can never touch an EXISTING member's tier (that is
/// `routes::billing_seats::change_seat_tier`'s job, a real Stripe MOVE with
/// proration semantics).
#[utoipa_path(
    post,
    path = "/v1/orgs/join-requests/{id}/approve",
    tag = "join_requests",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "org_join_requests.id")),
    request_body = ApproveJoinRequestRequest,
    responses(
        (status = 200, description = "Approved — a real Clerk + local membership now exists", body = ApproveJoinRequestResponse),
        (status = 400, description = "Unknown/inactive tier"),
        (status = 402, description = "Seat purchase failed (pool was full and Stripe declined)"),
        (status = 403, description = "Not an org context, caller is not an admin, or this is a personal org"),
        (status = 404, description = "No such OPEN request for this org"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn approve(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<ApproveJoinRequestRequest>,
) -> Response {
    let account=user.account_id;
    let response=approve_committed(State(state.clone()),tenant,Extension(user),Path(id),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn approve_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<ApproveJoinRequestRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    if let Err(resp) = refuse_personal_org(&state, ctx).await {
        return resp;
    }
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };

    let tier = body.tier.trim();
    // CS-442 (item 40, D8): resolve the REAL `seat_tiers.id`, not just an
    // EXISTS check — this is now threaded through `reserve_slot` and
    // `mirror_local_membership` below, so the chosen tier is the one that
    // actually gets provisioned, not just recorded in `evidence`.
    let tier_id: Option<Uuid> =
        match sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active")
            .bind(tier)
            .fetch_optional(control)
            .await
        {
            Ok(v) => v,
            Err(e) => return internal("tier lookup", e),
        };
    let Some(tier_id) = tier_id else {
        return bad("unknown or inactive seat tier");
    };

    let account_id = ctx.account_id;
    let org_id = ctx.org_id.clone();

    let Some(req) = (match svc::get_open(control, id, account_id).await {
        Ok(r) => r,
        Err(e) => return internal("load join request", e),
    }) else {
        return not_found("no open join request with that id for this org");
    };
    let target_user_id = req.user_id;

    // Membership BEFORE reserve_slot: an existing `account_members` row
    // means they are already in. Reserving (or purchasing) another seat
    // here is the bug Fall(AlreadyMember) used to leave reachable via
    // approve. Close the request as approved; do not touch the pool.
    match member_exists(control, account_id, target_user_id).await {
        Ok(true) => {
            if let Err(e) = svc::mark_decided(
                control,
                id,
                account_id,
                user.user_id,
                "approved",
                json!({ "already_member": true }),
            )
            .await
            {
                tracing::error!(
                    error = %e, join_request_id = %id, %target_user_id, %account_id,
                    "org_join_requests: approve found an existing member but marking the \
                     request decided failed — no slot was reserved"
                );
            }
            return (
                StatusCode::OK,
                Json(ApproveJoinRequestResponse {
                    user_id: target_user_id,
                    slot: "already_member".to_string(),
                    tier: tier.to_string(),
                }),
            )
                .into_response();
        }
        Ok(false) => {}
        Err(e) => return internal("membership check", e),
    }

    let target_clerk_user_id: Option<Option<String>> =
        match sqlx::query_scalar::<_, Option<String>>("SELECT clerk_user_id FROM users WHERE id = $1")
            .bind(target_user_id)
            .fetch_optional(control)
            .await
        {
            Ok(v) => v,
            Err(e) => return internal("load target clerk_user_id", e),
        };
    let Some(target_clerk_user_id) = target_clerk_user_id.flatten() else {
        return internal("load target clerk_user_id", "requester has no linked Clerk identity");
    };

    // ── Step 1: reserve a slot BEFORE any external call (D9) ──────────────
    // CS-442 (item 40, D8): reserved against the ADMIN'S CHOSEN tier, not
    // the account's total pool — see `slot_pool::reserve_slot`'s own doc
    // comment for why a tier-blind reservation could claim a slot someone
    // else paid for at a different tier.
    let reservation = match slot_pool::reserve_slot(
        control,
        account_id,
        target_user_id,
        Some(tier_id),
        slot_pool::reservation_ttl(),
    )
    .await
    {
        Ok(r) => r,
        Err(e) => return internal("reserve slot", e),
    };
    let (slot_label, pool_full) = match reservation {
        Some(_) => ("reserved", false),
        None => ("purchased", true),
    };

    // ── Step 2: D3's Clerk-first membership sequence ───────────────────────
    // #1540 SF2: in PURCHASE mode (no reserved slot) a self-serve org's
    // Clerk member cap equals its purchased seats, so Clerk itself would
    // refuse this membership. It runs inside `with_member_cap_headroom`
    // (cap + 1 first, given back if the approval fails); a reserved slot
    // already fits under the cap.
    let admit = async {
        let created = CreatedUser {
            clerk_user_id: target_clerk_user_id,
            // Never a ghost-user delete here — this user pre-existed (they were
            // already signed in to ask to join). See this function's own doc
            // comment.
            newly_created: false,
            last_sign_in_at: None,
        };
        let metadata = json!({
            "approved_by_user_id": user.user_id.to_string(),
            "approved_at": chrono::Utc::now().to_rfc3339(),
            "join_request_id": id.to_string(),
        });

        let membership = match provision_clerk_membership(&clerk, control, &org_id, &created, "org:member", metadata).await
        {
            // D3's build note: already_a_member_in_organization is treated
            // as success here (unlike `create_invite`'s own 409) — an admin
            // approving someone Clerk already considers a member must not fail
            // the approval. Fall through to the local mirror either way, but
            // keep the variant so a later local-mirror failure does not delete
            // a Clerk membership this call did not create.
            Ok(m) => m,
            Err(resp) => {
                // ── Step 3: unwind — release the reservation, leave the request
                //    open. provision_clerk_membership already cleaned up
                //    whatever it created on the Clerk side (there is no local
                //    membership yet — this call is what would have unlocked it).
                if let Some(reservation_id) = reservation {
                    if let Err(e) = slot_pool::release_reservation(control, reservation_id, ReleaseReason::Released).await {
                        tracing::error!(
                            error = %e, %reservation_id, join_request_id = %id,
                            "org_join_requests: approve failed AND releasing its reservation also failed — \
                             it will still clear via the expiry sweep"
                        );
                    }
                }
                return resp;
            }
        };
        if let Err(resp) = mirror_local_membership(
            &state,
            &clerk,
            control,
            &org_id,
            &created,
            target_user_id,
            "member",
            membership,
            // CS-442 (item 40, D8): the admin's validated, resolved tier —
            // applied on the fresh-insert path only, see
            // `ensure_org_membership`'s own "Seat tier" doc section.
            Some(tier_id),
            crate::billing::seat_lifecycle::SeatPurchasePolicy::AllowPurchase,
        )
        .await
        {
            // Same reservation release as above. `mirror_local_membership`
            // unwinds Clerk only for `Provisioned` — `AlreadyMember` skips the
            // delete so a pre-existing membership survives a local-mirror
            // failure. Ghost-user delete is never true here (`created.
            // newly_created` is always `false` for a join-request approval).
            if let Some(reservation_id) = reservation {
                if let Err(e) = slot_pool::release_reservation(control, reservation_id, ReleaseReason::Released).await {
                    tracing::error!(
                        error = %e, %reservation_id, join_request_id = %id,
                        "org_join_requests: approve failed AND releasing its reservation also failed — \
                         it will still clear via the expiry sweep"
                    );
                }
            }
            return resp;
        }

        // ── Step 4: mark decided — only reachable once a real membership exists ─
        let mut evidence_patch = json!({ "slot": slot_label, "tier": tier });
        if pool_full {
            evidence_patch["pool_full"] = json!(true);
        }
        if let Err(e) = svc::mark_decided(control, id, account_id, user.user_id, "approved", evidence_patch).await {
            // The membership is real; this is bookkeeping. Log loudly, never
            // unwind a real membership over it (this function's own doc
            // comment, step 4).
            tracing::error!(
                error = %e, join_request_id = %id, %target_user_id, %account_id,
                "org_join_requests: approve succeeded (Clerk + local membership both real) but marking \
                 the join request decided failed — the membership is NOT unwound"
            );
        }

        (
            StatusCode::OK,
            Json(ApproveJoinRequestResponse {
                user_id: target_user_id,
                slot: slot_label.to_string(),
                tier: tier.to_string(),
            }),
        )
            .into_response()
    };
    if reservation.is_none() {
        crate::routes::orgs_create::with_member_cap_headroom(control, &clerk, account_id, admit).await
    } else {
        admit.await
    }
}

// ── In-crate pg tests: the ONE scenario that needs a live Stripe mock ──────
//
// `StripeClient::with_base_url`'s `#[cfg(not(test))]` guard (that method's
// own doc comment: "mirrors `OpenAiProvider::with_base_url` exactly,
// including the `#[cfg(not(test))]` guard that makes the local-mock-server
// tests in this module possible") only relaxes when `coralswarm-backend`
// itself is compiled WITH `cfg(test)` — true here (an in-crate `#[cfg(test)]
// mod`), never true for `backend/tests/org_join_requests_pg.rs` (an external
// binary that merely LINKS this crate, even as a `test-support`-featured
// dev-dependency). `routes::org_invites`'s own module doc names the
// identical constraint for its Stripe seat-purchase-failure unwind test —
// see `backend/tests/org_join_requests_pg.rs`'s own module doc for the full
// split and why every OTHER scenario lives there instead.
//
// `RELINFER_TEST_DATABASE_URL`-gated against a PERSISTENT, pre-migrated
// database (this file's own PR description: `createdb cs437_relinfer` +
// apply `migrations_control/*.sql` in order) — same convention
// `clerk_provision.rs::pg_tests` uses, NOT the createdb-per-test scratch
// pattern `routes::org_invites::pg_tests` uses, since this module has no
// email/Resend surface to also stand up. Every seeded row uses a fresh
// `Uuid::new_v4()`-derived identifier, so this suite is safe to run
// alongside every other suite sharing the same persistent database.
//
// ## Running locally
//   RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/cs437_relinfer \
//     cargo test -p coralswarm-backend --lib "routes::org_join_requests::pg_tests::" -- --test-threads=1 --nocapture
#[cfg(test)]
mod pg_tests {

    async fn execute_financial_fixture<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    ) -> Result<sqlx::postgres::PgQueryResult, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.execute(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    async fn fetch_financial_fixture_id<'q>(
        pool: &sqlx::PgPool,
        account_id: Uuid,
        query: sqlx::query::QueryScalar<'q, sqlx::Postgres, Uuid, sqlx::postgres::PgArguments>,
    ) -> Result<Uuid, sqlx::Error> {
        let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        match query.fetch_one(&mut *tx).await {
            Ok(result) => {
                tx.commit().await?;
                Ok(result)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    use super::*;
    use crate::billing::slot_pool::read_slot_pool_for_tier;
    use crate::billing::stripe_client::StripeClient;
    // CS-442 (item 40): this suite now ALSO mutates `seat_tiers.stripe_price_id`
    // for `premium`, on the SAME persistent `RELINFER_TEST_DATABASE_URL`
    // database `billing::seat_lifecycle::pg_tests`/`routes::billing_seats::
    // pg_tests` already share — the crate-wide `CATALOG_PRICE_LOCK` (this
    // module's own doc comment on it: "four independent mutexes... serialize
    // nothing against EACH OTHER") is what makes a temporary price mutation
    // here safe against those other suites' own temporary mutations of the
    // SAME row.
    use crate::pg_catalog_test_support::{set_seat_tier_price, CATALOG_PRICE_LOCK};
    use axum::routing::{delete as axum_delete, post as axum_post};
    use serde_json::Value;
    use sqlx::PgPool;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex as AsyncMutex;

    static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(sqlx::postgres::PgPoolOptions::new().max_connections(4).connect(&url).await.expect("connect to test Postgres"))
    }

    #[allow(dead_code)]
    struct EnvGuard(tokio::sync::MutexGuard<'static, ()>);
    impl EnvGuard {
        async fn acquire(clerk_base: &str) -> Self {
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("CLERK_API_BASE_URL", clerk_base);
                std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_join_requests_incrate");
            }
            Self(guard)
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire`.
            unsafe {
                std::env::remove_var("CLERK_API_BASE_URL");
                std::env::remove_var("CLERK_SECRET_KEY");
            }
        }
    }

    /// A Clerk membership-create mock that always 200s — this suite is only
    /// about the Stripe side of approve, so the Clerk half is held constant
    /// at "succeeds".
    async fn spawn_clerk_ok_mock() -> String {
        let app = axum::Router::new()
            .route(
                "/v1/organizations/{org_id}/memberships",
                axum_post(|| async { (StatusCode::OK, Json(json!({"id": "orgmem_mock"}))) }),
            )
            .route(
                "/v1/organizations/{org_id}/memberships/{user_id}",
                axum_delete(|| async { (StatusCode::OK, Json(json!({"id": "orgmem_mock"}))) }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        format!("http://{addr}")
    }

    /// A Clerk mock whose membership-CREATE always answers 422
    /// `already_a_member_in_organization` (simulating a membership that
    /// PRE-DATES the approve click) and whose membership-DELETE increments
    /// a shared counter — the regression proof for CodeRabbit round 1
    /// finding 1: a subsequent LOCAL failure (a Stripe decline) must NEVER
    /// call delete, because this request never created the membership it
    /// would be deleting.
    async fn spawn_clerk_already_member_mock() -> (String, std::sync::Arc<AtomicUsize>) {
        let deletes = std::sync::Arc::new(AtomicUsize::new(0));
        let deletes_for_handler = deletes.clone();
        let app = axum::Router::new()
            .route(
                "/v1/organizations/{org_id}/memberships",
                axum_post(|| async {
                    (
                        StatusCode::UNPROCESSABLE_ENTITY,
                        Json(json!({"errors": [{"code": "already_a_member_in_organization", "message": "already a member"}]})),
                    )
                }),
            )
            .route(
                "/v1/organizations/{org_id}/memberships/{user_id}",
                axum_delete(move || {
                    let deletes = deletes_for_handler.clone();
                    async move {
                        deletes.fetch_add(1, Ordering::SeqCst);
                        (StatusCode::OK, Json(json!({"id": "orgmem_mock"})))
                    }
                }),
            );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), deletes)
    }

    /// A Stripe mock that 500s every call — used to force
    /// `mirror_local_membership` to fail (the "full pool, Stripe declines"
    /// shape) without needing a real card-decline fixture. Same shape as
    /// `clerk_provision::pg_tests::spawn_failing_stripe_mock`.
    async fn spawn_failing_stripe_mock() -> StripeClient {
        let app = axum::Router::new().fallback(axum::routing::any(|| async { StatusCode::INTERNAL_SERVER_ERROR }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}"))
    }

    /// A Stripe mock that 200s every call with one fixed subscription item
    /// — same shape as `billing::seat_lifecycle::pg_tests::spawn_seat_mock`
    /// (a near-identical copy rather than a cross-module dependency on
    /// another file's test internals, that module's own established
    /// convention). Returns a hit counter so "the pool was full, Stripe was
    /// actually called" is a positive assertion, not an inference from "a
    /// purchased slot appeared."
    async fn spawn_seat_mock(price_id: &str, quantity: i64) -> (StripeClient, std::sync::Arc<AtomicUsize>) {
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let hits_for_handler = hits.clone();
        let price_id = price_id.to_string();
        let body = json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": "active",
            "items": {
                "object": "list",
                "data": [{
                    "id": "si_mock_1",
                    "quantity": quantity,
                    "current_period_start": 1_700_000_000,
                    "current_period_end": 1_702_678_400,
                    "price": {"id": price_id},
                }],
            },
        });
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let hits = hits_for_handler.clone();
            let body = body.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                (StatusCode::OK, [("content-type", "application/json")], body.to_string())
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")), hits)
    }

    /// CS-442 (item 40): a Stripe mock whose `GET /v1/subscriptions/{id}`
    /// reports ONE existing item priced at `existing_price_id`, and which
    /// captures the raw body of every OTHER (mutating) call it receives —
    /// the "which price did this call actually name" proof
    /// [`spawn_seat_mock`]'s bare hit counter cannot give: a hit counter
    /// alone cannot tell "the mock was called" apart from "the mock was
    /// called for the WRONG tier's price", and CS-442's whole point is that
    /// the two must never be confused.
    async fn spawn_seat_mock_capturing_bodies(
        existing_price_id: &str,
    ) -> (StripeClient, std::sync::Arc<AsyncMutex<Vec<String>>>) {
        let captured: std::sync::Arc<AsyncMutex<Vec<String>>> = std::sync::Arc::new(AsyncMutex::new(Vec::new()));
        let captured_for_handler = captured.clone();
        let sub_body = json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": "active",
            "items": {
                "object": "list",
                "data": [{
                    "id": "si_mock_existing",
                    "quantity": 1,
                    "current_period_start": 1_700_000_000,
                    "current_period_end": 1_702_678_400,
                    "price": {"id": existing_price_id},
                }],
            },
        });
        let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::extract::Request| {
            let captured = captured_for_handler.clone();
            let sub_body = sub_body.clone();
            async move {
                if req.method().as_str() == "GET" {
                    return (StatusCode::OK, [("content-type", "application/json")], sub_body.to_string());
                }
                let body_bytes = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap_or_default();
                captured.lock().await.push(String::from_utf8_lossy(&body_bytes).to_string());
                (StatusCode::OK, [("content-type", "application/json")], json!({"id": "si_mock_new"}).to_string())
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")), captured)
    }

    /// A `team`-plan org with one ADMIN member (a purchased `standard`
    /// seat) and `purchased_seats` total `standard` slots — mirrors
    /// `clerk_provision::pg_tests::seed_team_org_with_member`'s shape.
    /// Returns `(account_id, clerk_org_id, admin_id, standard_price_id)`.
    async fn seed_org(pool: &PgPool, tag: &str, purchased_seats: i64) -> (Uuid, String, Uuid, String) {
        let admin_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(admin_id)
            .bind(format!("{tag}-admin-{}@join-requests-incrate.example", admin_id.simple()))
            .bind(format!("clerk_admin_{tag}_{}", admin_id.simple()))
            .execute(pool)
            .await
            .expect("insert admin user");

        let clerk_org_id = format!("org_jr_ic_{tag}_{}", Uuid::new_v4().simple());
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(admin_id)
                .bind(&clerk_org_id)
                .fetch_one(pool)
                .await
                .expect("insert org account");

        let (tier_id, price_id): (Uuid, Option<String>) =
            sqlx::query_as("SELECT id, stripe_price_id FROM seat_tiers WHERE code = 'standard' AND is_active")
                .fetch_one(pool)
                .await
                .expect("standard seat tier must be seeded (0165)");
        // `stripe_price_id` is NULL until PR C's provisioning script fills
        // it in (0165's own header) — this suite needs a real one to key
        // the mock's subscription item on, so it stamps one directly rather
        // than depending on that having run against this database.
        let price_id = match price_id {
            Some(p) => p,
            None => {
                let p = format!("price_mock_standard_{}", Uuid::new_v4().simple());
                sqlx::query("UPDATE seat_tiers SET stripe_price_id = $2 WHERE id = $1")
                    .bind(tier_id)
                    .bind(&p)
                    .execute(pool)
                    .await
                    .expect("stamp a mock stripe_price_id");
                p
            }
        };

        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'admin', $3, now())",
        )
        .bind(account_id)
        .bind(admin_id)
        .bind(tier_id)
        .execute(pool)
        .await
        .expect("insert admin membership");

        let plan_id: Uuid = sqlx::query_scalar("SELECT id FROM plans WHERE code = $1")
            .bind(crate::billing::MIXED_TIER_PLAN_CODE)
            .fetch_one(pool)
            .await
            .expect("team plan must be seeded (0167)");
        let grant_id: Uuid = fetch_financial_fixture_id(pool, account_id, sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) VALUES ($1, $2, 1, true) RETURNING id",
        )
        .bind(account_id)
        .bind(plan_id))
        .await
        .expect("insert team grant");
        if purchased_seats > 0 {
            execute_financial_fixture(pool, account_id, sqlx::query("INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) VALUES ($1, $2, $3)")
                .bind(grant_id)
                .bind(tier_id)
                .bind(purchased_seats))
                .await
                .expect("seed the purchased ledger");
        }

        execute_financial_fixture(pool, account_id, sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple())))
        .await
        .expect("link stripe subscription");

        (account_id, clerk_org_id, admin_id, price_id)
    }

    async fn seed_requester(pool: &PgPool, tag: &str) -> (Uuid, String) {
        let user_id = Uuid::new_v4();
        let clerk_user_id = format!("clerk_req_ic_{tag}_{}", user_id.simple());
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(user_id)
            .bind(format!("{tag}-requester-{}@join-requests-incrate.example", user_id.simple()))
            .bind(&clerk_user_id)
            .execute(pool)
            .await
            .expect("insert requester user");
        (user_id, clerk_user_id)
    }

    fn requester_ctx(user_id: Uuid) -> UserContext {
        UserContext { user_id, account_id: Uuid::new_v4(), role: "owner".to_string(), device_id: "jr-incrate-test".to_string() }
    }
    fn admin_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext { user_id, account_id, role: "admin".to_string(), device_id: "jr-incrate-test".to_string() }
    }
    fn org_tenant(pool: PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                crate::tenancy::runtime::TenantContext {
                    org_id: org_id.to_string(),
                    account_id,
                    user_id,
                    pool,
                    page_pool: None,
                },
            )),
            class: crate::route_class::RouteClass::Other,
        }
    }
    fn test_state(control: PgPool, stripe: StripeClient) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned_with_db_and_store(control, None);
        state.stripe = Some(std::sync::Arc::new(stripe));
        Arc::new(state)
    }
    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    /// D9: the pool is full (1 purchased seat, already held by the admin) —
    /// `reserve_slot` returns `None`, `approve` proceeds in PURCHASE mode
    /// (an admin's click IS the human consent D1 requires to spend), and
    /// `acquire_seat` falls through to a genuine Stripe push. Asserts the
    /// POSITIVE: the mock's hit counter is `> 0`, not merely that a
    /// purchased slot appeared (which a silently-broken pool consumption
    /// could also produce).
    #[tokio::test]
    async fn approve_with_a_full_pool_purchases_via_stripe() {
        let Some(pool) = pg_test_pool("approve_with_a_full_pool_purchases_via_stripe").await else {
            return;
        };
        let (account_id, org_id, admin_id, price_id) = seed_org(&pool, "jrfull", 1).await;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jrfull").await;

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let id: Uuid = sqlx::query_scalar(
            "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = EnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_seat_mock(&price_id, 1).await;
        let state = test_state(pool.clone(), stripe);

        let resp = approve(
            State(state.clone()),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(id),
            Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "approve must succeed by purchasing a slot");
        let body = body_json(resp).await;
        assert_eq!(body["slot"], json!("purchased"));

        assert!(hits.load(Ordering::SeqCst) > 0, "Stripe must actually have been called");

        let live_reservations: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM slot_reservations WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(requester_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(live_reservations, 0, "a full pool never takes a reservation row at all");

        let seat_purchased_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("a real account_members row must exist");
        assert!(seat_purchased_at.is_some());

        let (decision, evidence): (Option<String>, Value) =
            sqlx::query_as("SELECT decision, evidence FROM org_join_requests WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(decision.as_deref(), Some("approved"));
        assert_eq!(evidence["slot"], json!("purchased"));
        assert_eq!(evidence["pool_full"], json!(true));
    }

    /// CodeRabbit round 1 (CS-437 PR #1021, finding 1) — regression proof.
    /// Clerk reports `already_a_member_in_organization` (a membership that
    /// PRE-DATES this approve click), the pool is full so
    /// `mirror_local_membership` falls through to Stripe, and Stripe
    /// declines. The fix under test: `membership_created` must be `false`
    /// on the `AlreadyMember` branch, so `unwind_clerk` must NEVER call
    /// Clerk's delete-membership endpoint — deleting it would revoke real,
    /// pre-existing org access the person had before this request.
    #[tokio::test]
    async fn already_member_approve_that_then_fails_locally_never_deletes_the_preexisting_membership() {
        let Some(pool) =
            pg_test_pool("already_member_approve_that_then_fails_locally_never_deletes_the_preexisting_membership")
                .await
        else {
            return;
        };
        // Full pool (1 purchased, already held by the admin) — forces the
        // purchase-mode branch, which is what reaches Stripe at all.
        let (account_id, org_id, admin_id, _price_id) = seed_org(&pool, "jralready", 1).await;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jralready").await;

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let id: Uuid = sqlx::query_scalar(
            "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let (clerk_base, deletes) = spawn_clerk_already_member_mock().await;
        let _env = EnvGuard::acquire(&clerk_base).await;
        let stripe = spawn_failing_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let resp = approve(
            State(state.clone()),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(id),
            Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
        )
        .await;
        assert_ne!(resp.status(), StatusCode::OK, "the local mirror must fail (Stripe declined)");

        assert_eq!(
            deletes.load(Ordering::SeqCst),
            0,
            "a membership this request never created must never be deleted"
        );

        // The LOCAL `account_members` row is NOT synchronously rolled back
        // on a Stripe failure: `ensure_org_membership` commits its own
        // membership-insert transaction BEFORE ever calling `acquire_seat`
        // (Stripe pushes happen strictly after the local write commits) —
        // same documented, non-synchronous-unwind shape
        // `routes::org_invites::pg_tests::a_stripe_failure_during_acquire_seat_unwinds_the_whole_invite`
        // asserts for `create_invite`'s own identical Stripe-failure path.
        // What this test is actually proving is narrower and different:
        // the Clerk-side membership (which PRE-DATED this request) is never
        // deleted — asserted above via `deletes`. The seat itself must
        // still be unpurchased.
        let seat_purchased_at: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("the local membership row still exists — see comment above");
        assert!(seat_purchased_at.is_none(), "the seat must NOT be stamped purchased — Stripe declined");

        let decision: Option<String> =
            sqlx::query_scalar("SELECT decision FROM org_join_requests WHERE id = $1").bind(id).fetch_one(&pool).await.unwrap();
        assert_eq!(decision, None, "the request must stay open so an admin can retry");
    }

    // ── CS-442 (item 40, D8): the chosen tier is actually provisioned ─────

    /// The tier plumbing itself: approving with `tier = "premium"` (a
    /// completely unpurchased tier — `premium` has zero `grant_seat_tiers`
    /// rows here, so the pool is full for IT specifically, even though
    /// `standard` was never even touched) must charge PREMIUM's Stripe
    /// price, not `standard`'s, and must assign the member `premium`'s
    /// `seat_tier_id` — the exact gap CS-437 left stated in this file's own
    /// former doc comment ("`tier` is validated... but is NOT plumbed into
    /// step 2").
    #[tokio::test]
    async fn approve_with_a_chosen_tier_charges_that_tiers_stripe_price() {
        let Some(pool) = pg_test_pool("approve_with_a_chosen_tier_charges_that_tiers_stripe_price").await else {
            return;
        };
        let (account_id, org_id, admin_id, standard_price_id) = seed_org(&pool, "jrtier", 1).await;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jrtier").await;

        let premium_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium' AND is_active = true")
            .fetch_one(&pool)
            .await
            .expect("premium seat tier must be seeded (0165)");
        let premium_price_id = format!("price_mock_premium_{}", Uuid::new_v4().simple());

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let id: Uuid = sqlx::query_scalar(
            "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = EnvGuard::acquire(&clerk_base).await;
        // The mock's ONE pre-existing subscription item is priced at
        // STANDARD's price — proving the approval below reaches for
        // premium's price specifically, not merely "whatever the mock
        // already had".
        let (stripe, captured_bodies) = spawn_seat_mock_capturing_bodies(&standard_price_id).await;
        let state = test_state(pool.clone(), stripe);

        let catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_premium_price = set_seat_tier_price(&pool, "premium", Some(&premium_price_id)).await;
        let resp = approve(
            State(state.clone()),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(id),
            Json(ApproveJoinRequestRequest { tier: "premium".to_string() }),
        )
        .await;
        set_seat_tier_price(&pool, "premium", original_premium_price.as_deref()).await;
        drop(catalog_guard);
        assert_eq!(resp.status(), StatusCode::OK, "approve must succeed by purchasing premium's own price");
        let body = body_json(resp).await;
        assert_eq!(body["tier"], json!("premium"), "the response must echo the tier actually provisioned");

        let bodies = captured_bodies.lock().await;
        assert!(
            bodies.iter().any(|b| b.contains(&premium_price_id)),
            "the mutating Stripe call must have named premium's own price: {bodies:?}"
        );
        assert!(
            !bodies.iter().any(|b| b.contains(&standard_price_id)),
            "the mutating Stripe call must NEVER have named standard's price: {bodies:?}"
        );
        drop(bodies);

        let seat_tier_id: Uuid = sqlx::query_scalar(
            "SELECT seat_tier_id FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("the member row must exist");
        assert_eq!(seat_tier_id, premium_id, "the member must be assigned PREMIUM's tier, not standard");

        let evidence: Value =
            sqlx::query_scalar("SELECT evidence FROM org_join_requests WHERE id = $1").bind(id).fetch_one(&pool).await.unwrap();
        assert_eq!(evidence["tier"], json!("premium"));
    }

    /// Consume-before-buy is TIER-AWARE (`billing::slot_pool`'s own "CS-442"
    /// doc section): one already-consumed `standard` slot plus one FREE,
    /// unconsumed `standard` slot (`seed_org`'s own `purchased_seats = 2`
    /// shape — the admin holds the first) means a SECOND `standard`
    /// approval must consume the free one with ZERO Stripe calls (proven
    /// via a mock that 500s everything — approve only succeeds if Stripe
    /// was never actually reached), while a `premium` approval on the SAME
    /// account — a tier with NOTHING purchased — must fall through to a
    /// real Stripe purchase.
    #[tokio::test]
    async fn approve_consume_before_buy_is_tier_aware() {
        let Some(pool) = pg_test_pool("approve_consume_before_buy_is_tier_aware").await else {
            return;
        };
        // purchased_seats = 2: the admin's own membership consumes ONE
        // (seed_org's own unconditional `seat_purchased_at = now()`),
        // leaving exactly one FREE standard slot for the requester below.
        let (account_id, org_id, admin_id, _standard_price_id) = seed_org(&pool, "jrconsume", 2).await;
        let (standard_requester, _) = seed_requester(&pool, "jrconsume-std").await;
        let (premium_requester, _) = seed_requester(&pool, "jrconsume-prem").await;

        let standard_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true")
            .fetch_one(&pool)
            .await
            .unwrap();
        let premium_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'premium' AND is_active = true")
            .fetch_one(&pool)
            .await
            .unwrap();
        let premium_price_id = format!("price_mock_premium_{}", Uuid::new_v4().simple());

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = EnvGuard::acquire(&clerk_base).await;

        // ── Leg 1: standard — must CONSUME, zero Stripe calls ─────────────
        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(standard_requester)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let standard_req_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(standard_requester)
        .fetch_one(&pool)
        .await
        .unwrap();

        let failing_stripe = spawn_failing_stripe_mock().await;
        let resp = approve(
            State(test_state(pool.clone(), failing_stripe)),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(standard_req_id),
            Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a standard approval with a free standard slot must succeed WITHOUT ever calling Stripe \
             (the mock 500s everything — success here IS the proof)"
        );
        let body = body_json(resp).await;
        assert_eq!(body["slot"], json!("reserved"), "must consume the pre-bought pool, not purchase");

        // ── Leg 2: premium — nothing purchased, must fall through to Stripe ─
        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(premium_requester)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let premium_req_id: Uuid = sqlx::query_scalar(
            "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(premium_requester)
        .fetch_one(&pool)
        .await
        .unwrap();

        let (working_stripe, hits) = spawn_seat_mock(&premium_price_id, 0).await;
        let catalog_guard = CATALOG_PRICE_LOCK.lock().await;
        let original_premium_price = set_seat_tier_price(&pool, "premium", Some(&premium_price_id)).await;
        let resp = approve(
            State(test_state(pool.clone(), working_stripe)),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(premium_req_id),
            Json(ApproveJoinRequestRequest { tier: "premium".to_string() }),
        )
        .await;
        set_seat_tier_price(&pool, "premium", original_premium_price.as_deref()).await;
        drop(catalog_guard);
        assert_eq!(resp.status(), StatusCode::OK, "premium approval must succeed by purchasing");
        let body = body_json(resp).await;
        assert_eq!(body["slot"], json!("purchased"), "nothing was pre-bought for premium — must purchase");
        assert!(hits.load(Ordering::SeqCst) > 0, "Stripe must actually have been called for premium");

        // ── Final per-tier readback ────────────────────────────────────────
        let mut tx = crate::tenancy::scope::begin_account_scoped(&pool, account_id).await.expect("scope");
        let standard_pool = read_slot_pool_for_tier(&mut tx, account_id, standard_id).await.expect("read standard pool");
        let premium_pool = read_slot_pool_for_tier(&mut tx, account_id, premium_id).await.expect("read premium pool");
        tx.commit().await.unwrap();
        assert_eq!(standard_pool.purchased, 2);
        assert_eq!(standard_pool.used, 2, "the admin plus the newly-consumed standard member");
        assert_eq!(standard_pool.available(), 0);
        assert_eq!(premium_pool.purchased, 1, "the Stripe purchase above must have grown premium's own ledger");
        assert_eq!(premium_pool.used, 1);
    }

    // ── D19/CS-440: the "join this org" click (POST /v1/join-requests with
    //    a signed token) ──────────────────────────────────────────────────
    //
    // `create` itself needs no Stripe/Clerk mock at all for these — the
    // token check is a pure, synchronous verify. Reuses `ENV_LOCK` above
    // (a lock this module already holds for env-var-mutating tests) purely
    // to serialize `JOIN_NOTICE_SECRET`, not for anything Clerk-related.

    #[allow(dead_code)]
    struct JoinNoticeSecretGuard(tokio::sync::MutexGuard<'static, ()>);
    impl JoinNoticeSecretGuard {
        async fn acquire(secret: &str) -> Self {
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("JOIN_NOTICE_SECRET", secret);
            }
            Self(guard)
        }
    }
    impl Drop for JoinNoticeSecretGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire`.
            unsafe {
                std::env::remove_var("JOIN_NOTICE_SECRET");
            }
        }
    }

    const JOIN_NOTICE_TEST_SECRET: &str = "cs440-incrate-test-secret-0123456789";

    /// FAILS WITHOUT THE FIX: a token whose `sub`/`acc` verify against THIS
    /// caller and THIS org records `source = 'matched_not_member'`, with
    /// `evidence` carrying the token's own `email`/`workspace_id`/
    /// `channel_id`/`invited_at` — never the requester's own
    /// `self_request` shape.
    #[tokio::test]
    async fn create_with_a_valid_join_token_records_matched_not_member() {
        let Some(pool) = pg_test_pool("create_with_a_valid_join_token_records_matched_not_member").await
        else {
            return;
        };
        let (account_id, org_id, _admin_id, _price_id) = seed_org(&pool, "jrjointok", 0).await;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jrjointok").await;
        let _env = JoinNoticeSecretGuard::acquire(JOIN_NOTICE_TEST_SECRET).await;

        let token = crate::services::unmatched_recipient_email::mint_join_notice_token(
            JOIN_NOTICE_TEST_SECRET,
            account_id,
            requester_id,
            &org_id,
            "requester@example.com",
            Some("T1"),
            Some("C1"),
            Some("Acme"),
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let (source, evidence): (String, Value) = sqlx::query_as(
            "SELECT source, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2 \
             AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();

        assert_eq!(source, "matched_not_member");
        assert_eq!(evidence["email"], json!("requester@example.com"));
        assert_eq!(evidence["workspace_id"], json!("T1"));
        assert_eq!(evidence["channel_id"], json!("C1"));
        assert!(evidence.get("invited_at").is_some(), "evidence must carry invited_at");
    }

    /// FAILS WITHOUT THE FIX: a tampered signature must fall back to an
    /// ordinary `self_request` — same 202, no disclosure that the token was
    /// even present, let alone invalid (this story's own brief).
    #[tokio::test]
    async fn create_with_a_tampered_join_token_falls_back_to_self_request() {
        let Some(pool) = pg_test_pool("create_with_a_tampered_join_token_falls_back_to_self_request").await
        else {
            return;
        };
        let (account_id, org_id, _admin_id, _price_id) = seed_org(&pool, "jrjointamper", 0).await;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jrjointamper").await;
        let _env = JoinNoticeSecretGuard::acquire(JOIN_NOTICE_TEST_SECRET).await;

        let mut token = crate::services::unmatched_recipient_email::mint_join_notice_token(
            JOIN_NOTICE_TEST_SECRET,
            account_id,
            requester_id,
            &org_id,
            "requester@example.com",
            Some("T1"),
            Some("C1"),
            None,
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();
        token.push('x');

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED, "a tampered token must still answer 202");

        let source: String = sqlx::query_scalar(
            "SELECT source FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "self_request", "a tampered token must fall back to an ordinary self-request");
    }

    /// FAILS WITHOUT THE FIX: a token minted for a DIFFERENT org falls back
    /// to self-request against the org the caller actually named — never
    /// silently redirected to the token's own org, and never an error.
    #[tokio::test]
    async fn create_with_a_token_for_a_different_org_falls_back_to_self_request() {
        let Some(pool) = pg_test_pool("create_with_a_token_for_a_different_org_falls_back_to_self_request").await
        else {
            return;
        };
        let (account_id, org_id, _admin_id, _price_id) = seed_org(&pool, "jrjointargeta", 0).await;
        let (other_account_id, other_org_id, _admin2, _price2) = seed_org(&pool, "jrjointargetb", 0).await;
        let _ = other_account_id;
        let (requester_id, _requester_clerk) = seed_requester(&pool, "jrjointargeta").await;
        let _env = JoinNoticeSecretGuard::acquire(JOIN_NOTICE_TEST_SECRET).await;

        // Minted for `other_org_id`, but the request names `org_id`.
        let token = crate::services::unmatched_recipient_email::mint_join_notice_token(
            JOIN_NOTICE_TEST_SECRET,
            other_account_id,
            requester_id,
            &other_org_id,
            "requester@example.com",
            None,
            None,
            None,
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let source: String = sqlx::query_scalar(
            "SELECT source FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "self_request", "a token for a different org must not authorize THIS org's request");
        let leaked_into_other_org: i64 =
            sqlx::query_scalar("SELECT count(*) FROM org_join_requests WHERE account_id = $1 AND user_id = $2")
                .bind(other_account_id)
                .bind(requester_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(leaked_into_other_org, 0, "the mismatched token must never queue a request against ITS OWN org either");
    }

    /// FAILS WITHOUT THE FIX: a token whose `sub` names a DIFFERENT person
    /// (the email was forwarded, and someone else — signed into their own
    /// CoralSwarm account — clicked it) must never let that caller be
    /// recorded as the ORIGINAL invitee. Falls back to an honest
    /// self-request for whoever actually clicked.
    #[tokio::test]
    async fn create_with_a_token_for_a_different_user_falls_back_to_self_request() {
        let Some(pool) =
            pg_test_pool("create_with_a_token_for_a_different_user_falls_back_to_self_request").await
        else {
            return;
        };
        let (account_id, org_id, _admin_id, _price_id) = seed_org(&pool, "jrjointuser", 0).await;
        let (original_invitee, _c1) = seed_requester(&pool, "jrjointuser-original").await;
        let (actual_clicker, _c2) = seed_requester(&pool, "jrjointuser-clicker").await;
        let _env = JoinNoticeSecretGuard::acquire(JOIN_NOTICE_TEST_SECRET).await;

        // Minted for `original_invitee`, but `actual_clicker` is the
        // authenticated caller.
        let token = crate::services::unmatched_recipient_email::mint_join_notice_token(
            JOIN_NOTICE_TEST_SECRET,
            account_id,
            original_invitee,
            &org_id,
            "original@example.com",
            None,
            None,
            None,
            // CS-447: no classification to carry — this test is about the
            // token's OWN validity, not D16 rung 2. `Unavailable` mints
            // NO `cls` claim, so the token shape here is byte-identical
            // to what it was before CS-447.
            crate::connectors::slack::MembershipClass::Unavailable,
        )
        .unwrap();

        let resp = create(
            State(test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
            Extension(requester_ctx(actual_clicker)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let clicker_source: String = sqlx::query_scalar(
            "SELECT source FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(actual_clicker)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(clicker_source, "self_request", "the ACTUAL clicker gets an honest self-request");

        let original_invitee_has_no_row: i64 =
            sqlx::query_scalar("SELECT count(*) FROM org_join_requests WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(original_invitee)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            original_invitee_has_no_row, 0,
            "the token's own subject must never be recorded as having requested anything — they \
             never clicked it"
        );
    }

    /// CS-443 (item 6, D16) — the assertion ladder's LIVE caller. Every
    /// queued request carries the ladder's verdict under `evidence.ladder`,
    /// so the admin deciding it can see what the ladder made of the person.
    ///
    /// Two requesters against the SAME org, differing only in the domain of
    /// the address they signed up with:
    ///   - one on a domain the org has VERIFIED → rung 3, `approval_only`
    ///     (one signal alone — D16 is explicit that this never auto-adds,
    ///     and the evidence says so in as many words);
    ///   - one on an unrelated domain → `not_armed`.
    ///
    /// Both record `directory: "unavailable"`, never `"not_listed"`: this
    /// route has no live Google directory and says so, rather than asserting
    /// an absence it never checked.
    #[tokio::test]
    async fn a_queued_request_records_the_ladder_verdict_in_its_evidence() {
        let Some(pool) = pg_test_pool("a_queued_request_records_the_ladder_verdict_in_its_evidence").await
        else {
            return;
        };
        let (account_id, org_id, _admin_id, _price_id) = seed_org(&pool, "jrladder", 5).await;

        // A verified domain for this org, and a requester whose address is
        // on it.
        let domain = format!("cs443-ladder-{}.example", Uuid::new_v4().simple());
        let on_domain_user = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(on_domain_user)
            .bind(format!("dana-{}@{domain}", on_domain_user.simple()))
            .bind(format!("clerk_ladder_on_{}", on_domain_user.simple()))
            .execute(&pool)
            .await
            .expect("insert on-domain requester");
        sqlx::query(
            "INSERT INTO org_domains (account_id, domain, verification_token, verified_at, method) \
             VALUES ($1, $2, $3, now(), 'dns_txt')",
        )
        .bind(account_id)
        .bind(&domain)
        .bind(Uuid::new_v4().simple().to_string())
        .execute(&pool)
        .await
        .expect("seed a verified domain");

        let state = || {
            test_state(pool.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))
        };

        let resp = create(
            State(state()),
            Extension(requester_ctx(on_domain_user)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);

        let evidence: Value = sqlx::query_scalar(
            "SELECT evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(on_domain_user)
        .fetch_one(&pool)
        .await
        .unwrap();
        let ladder = &evidence["ladder"];
        assert_eq!(ladder["verdict"], json!("approval_only"), "a domain match ALONE is rung 3");
        assert_eq!(ladder["rung"], json!("single_signal"));
        assert_eq!(ladder["rung_number"], json!(3));
        assert_eq!(ladder["domain"], json!("match"));
        assert_eq!(ladder["matched_domain"], json!(domain));
        assert_eq!(ladder["workspace_membership"], json!("unknown"));
        assert_eq!(
            ladder["directory"],
            json!("unavailable"),
            "no live directory here — and 'could not ask' must never be recorded as 'not listed'"
        );
        assert!(ladder["directory_reason"].is_string());
        // The evidence enqueue already wrote is preserved alongside it.
        assert_eq!(evidence["tombstoned"], json!(false));

        // A requester on an unrelated domain: neither signal, not armed.
        let (off_domain_user, _clerk) = seed_requester(&pool, "jrladderoff").await;
        let resp = create(
            State(state()),
            Extension(requester_ctx(off_domain_user)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        let evidence: Value = sqlx::query_scalar(
            "SELECT evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(off_domain_user)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(evidence["ladder"]["verdict"], json!("not_armed"));
        assert_eq!(evidence["ladder"]["rung"], json!("none"));
        assert_eq!(evidence["ladder"]["domain"], json!("no_match"));
        assert!(
            evidence["ladder"].get("rung_number").is_none(),
            "there is no rung 4 — 'neither available' is not a rung"
        );
    }

    // ── CS-447 (item 9): auto-add ─────────────────────────────────────────
    //
    // Every test below drives the REAL `create` handler end to end. The
    // Stripe client handed to it is a mock that COUNTS every request it
    // receives, and "Stripe was never called" is asserted against that
    // counter rather than inferred from the absence of a purchased slot —
    // this repo has already been bitten once by a test that passed for the
    // wrong reason on exactly that inference (`offboarding_frees_the_slot_
    // without_calling_stripe`, CS-444).

    /// A Stripe mock that must never be reached. Counts every request AND
    /// 500s, so a call shows up twice over: as a non-zero counter, and as a
    /// failed add. Same shape as `spawn_seat_mock`, opposite intent.
    async fn spawn_forbidden_stripe_mock() -> (StripeClient, std::sync::Arc<AtomicUsize>) {
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let hits_for_handler = hits.clone();
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let hits = hits_for_handler.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                StatusCode::INTERNAL_SERVER_ERROR
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (StripeClient::new(reqwest::Client::new(), "sk_test_mock").with_base_url(format!("http://{addr}")), hits)
    }

    /// Both env guards at once. `EnvGuard` and `JoinNoticeSecretGuard` each
    /// take `ENV_LOCK`, so an auto-add test — which needs Clerk AND the
    /// join-notice secret — cannot hold both. One guard, one lock, three
    /// variables.
    #[allow(dead_code)]
    struct AutoAddEnvGuard(tokio::sync::MutexGuard<'static, ()>);
    impl AutoAddEnvGuard {
        async fn acquire(clerk_base: &str) -> Self {
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by `guard`, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("CLERK_API_BASE_URL", clerk_base);
                std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_auto_add_incrate");
                std::env::set_var("JOIN_NOTICE_SECRET", JOIN_NOTICE_TEST_SECRET);
            }
            Self(guard)
        }
    }
    impl Drop for AutoAddEnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire`.
            unsafe {
                std::env::remove_var("CLERK_API_BASE_URL");
                std::env::remove_var("CLERK_SECRET_KEY");
                std::env::remove_var("JOIN_NOTICE_SECRET");
            }
        }
    }

    /// A requester whose address sits at `domain` — rung 2's domain half
    /// needs the address and the verified domain to agree, so the two are
    /// seeded together or not at all.
    async fn seed_requester_at(pool: &PgPool, tag: &str, domain: &str) -> (Uuid, String) {
        let user_id = Uuid::new_v4();
        let email = format!("{tag}-{}@{domain}", user_id.simple());
        sqlx::query("INSERT INTO users (id, email, name, clerk_user_id) VALUES ($1, $2, $3, $4)")
            .bind(user_id)
            .bind(&email)
            .bind(format!("Req {tag}"))
            .bind(format!("clerk_aa_{tag}_{}", user_id.simple()))
            .execute(pool)
            .await
            .expect("insert requester user");
        (user_id, email)
    }

    /// Plant a VERIFIED `org_domains` row directly. `method` is the whole
    /// point of the parameter: `'dns_txt'` arms rung 2, `'email'` does not
    /// (D11's 2026-09-22 amendment), and a test that could not seed both
    /// could not tell the two apart.
    async fn verify_domain(pool: &PgPool, account_id: Uuid, domain: &str, method: &str) {
        sqlx::query(
            "INSERT INTO org_domains (account_id, domain, verification_token, verified_at, method) \
             VALUES ($1, $2, $3, now(), $4)",
        )
        .bind(account_id)
        .bind(domain)
        .bind(format!("tok_{}", Uuid::new_v4().simple()))
        .bind(method)
        .execute(pool)
        .await
        .expect("seed a verified domain");
    }

    /// Turn the toggle on for `tier_code`. Uses the real service writer so
    /// the tests exercise the same row shape production does.
    async fn enable_auto_add(pool: &PgPool, account_id: Uuid, admin_id: Uuid, tier_code: &str) {
        let tier_id: Option<Uuid> = if tier_code == "standard" {
            None
        } else {
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = $1 AND is_active")
                .bind(tier_code)
                .fetch_one(pool)
                .await
                .expect("tier must exist")
        };
        crate::services::org_auto_add::upsert_settings(pool, account_id, true, tier_id, admin_id)
            .await
            .expect("enable auto-add");
    }

    /// A join-notice token carrying the roster walk's `full_member`
    /// classification — D16 rung 2's workspace half, which is otherwise
    /// absent at this call site by construction.
    fn full_member_token(account_id: Uuid, user_id: Uuid, org_id: &str, email: &str) -> String {
        token_with_class(
            account_id,
            user_id,
            org_id,
            email,
            crate::connectors::slack::MembershipClass::FullMember,
        )
    }

    /// A join-notice token carrying an arbitrary workspace classification.
    /// `MembershipClass::Unavailable` mints NO `cls` claim at all, which is
    /// how a pre-CS-447 token (and a member the roster walk could not
    /// classify) is reproduced here.
    fn token_with_class(
        account_id: Uuid,
        user_id: Uuid,
        org_id: &str,
        email: &str,
        class: crate::connectors::slack::MembershipClass,
    ) -> String {
        token_with_class_at(account_id, user_id, org_id, email, class, chrono::Utc::now().timestamp())
    }

    /// The same, with `iat` supplied — for the 48-hour auto-add freshness
    /// bound, whose whole point is a token that is STILL a valid link.
    fn token_with_class_at(
        account_id: Uuid,
        user_id: Uuid,
        org_id: &str,
        email: &str,
        class: crate::connectors::slack::MembershipClass,
        issued_at: i64,
    ) -> String {
        crate::services::unmatched_recipient_email::mint_join_notice_token_at(
            JOIN_NOTICE_TEST_SECRET,
            account_id,
            user_id,
            org_id,
            email,
            Some("T_AA"),
            Some("C_AA"),
            Some("Acme"),
            class,
            issued_at,
        )
        .expect("mint a join token")
    }

    async fn auto_add_rows(pool: &PgPool, account_id: Uuid) -> Vec<(Uuid, String, Option<Uuid>)> {
        sqlx::query_as("SELECT user_id, rung, seat_tier_id FROM org_auto_adds WHERE account_id = $1 ORDER BY added_at")
            .bind(account_id)
            .fetch_all(pool)
            .await
            .expect("read org_auto_adds")
    }

    async fn queued_source(pool: &PgPool, account_id: Uuid, user_id: Uuid) -> Option<String> {
        sqlx::query_scalar("SELECT source FROM org_join_requests WHERE account_id = $1 AND user_id = $2")
            .bind(account_id)
            .bind(user_id)
            .fetch_optional(pool)
            .await
            .expect("read the queued request")
    }

    /// **CS-447 negative test 1 — D1: auto-add NEVER calls Stripe.** Also
    /// the positive path: this is the one test that proves the feature
    /// works at all.
    ///
    /// A bought, unused `standard` slot; a DNS-verified domain; a
    /// full-member token. The person is added outright — no join request,
    /// a real `account_members` row, the reservation resolved `fulfilled`,
    /// an `org_auto_adds` row naming rung 2 — and the Stripe mock's
    /// counter is **zero**.
    ///
    /// The counter is the assertion that matters. A version of this that
    /// only checked `seat_purchased_at IS NOT NULL` would pass just as
    /// happily if the slot had been BOUGHT instead of consumed, which is
    /// precisely the failure D1 exists to prevent.
    #[tokio::test]
    async fn auto_add_consumes_a_bought_slot_and_never_calls_stripe() {
        let Some(pool) = pg_test_pool("auto_add_consumes_a_bought_slot_and_never_calls_stripe").await else {
            return;
        };
        // 2 purchased `standard` slots, 1 held by the admin ⇒ exactly one free.
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aaok", 2).await;
        let domain = format!("aaok-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aaok", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(resp).await["status"], json!("added"), "the three gates all passed");

        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "D1: auto-add must consume a slot the org already bought and NEVER call Stripe"
        );

        let member: Option<Option<chrono::DateTime<chrono::Utc>>> = sqlx::query_scalar(
            "SELECT seat_purchased_at FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_optional(&pool)
        .await
        .unwrap();
        assert!(member.flatten().is_some(), "a real, slot-stamped membership must exist");

        assert_eq!(
            queued_source(&pool, account_id, requester_id).await,
            None,
            "an auto-added member never creates a join request (D13's 2026-09-22 amendment)"
        );

        let (release_reason,): (Option<String>,) = sqlx::query_as(
            "SELECT release_reason FROM slot_reservations WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(release_reason.as_deref(), Some("fulfilled"), "the reservation turned into the seat");

        let rows = auto_add_rows(&pool, account_id).await;
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, requester_id);
        assert_eq!(rows[0].1, "member_and_domain", "D13: the digest must be able to say WHICH rung");
        assert!(rows[0].2.is_some(), "and which tier was spent");
    }

    /// **CS-447 negative test 6 — D11: off by default.**
    ///
    /// Byte-for-byte the same setup as the test above, minus the one line
    /// that turns the toggle on. A fresh org adds nobody: the person is
    /// queued with their ordinary source, no slot moves, and Stripe is
    /// never called.
    ///
    /// Paired deliberately with the positive test rather than written
    /// alone — a "nobody was added" assertion proves nothing on its own if
    /// the feature is inert for an unrelated reason.
    #[tokio::test]
    async fn auto_add_is_off_by_default_for_a_fresh_org() {
        let Some(pool) = pg_test_pool("auto_add_is_off_by_default_for_a_fresh_org").await else {
            return;
        };
        let (account_id, org_id, _admin_id, _price) = seed_org(&pool, "aaoff", 5).await;
        let domain = format!("aaoff-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aaoff", &domain).await;
        // NO `enable_auto_add` — that is the whole test.

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        assert!(auto_add_rows(&pool, account_id).await.is_empty(), "a fresh org auto-adds nobody");
        let member: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2")
                .bind(account_id)
                .bind(requester_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(member, 0);
        let (source, evidence): (String, Value) = sqlx::query_as(
            "SELECT source, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "matched_not_member", "the request keeps its own source — 'off' is not a verdict about them");
        assert_eq!(evidence["auto_add"]["blocked_by"], json!("auto_add_off"));
    }

    /// **CS-447 negative test 4 — D11 as amended 2026-09-22: only a
    /// DNS-verified domain arms auto-add.**
    ///
    /// Identical to the positive test except `method = 'email'`. The
    /// address matches a genuinely verified domain — the org really did
    /// prove control of it — and auto-add still refuses, because an
    /// emailed confirmation proves one mailbox and a TXT record proves the
    /// domain.
    ///
    /// The `source` assertion is the half that would catch a lazy fix: the
    /// request must be filed as `unverified_domain`, and the ladder's own
    /// evidence must still say `match_email_verified_only` rather than the
    /// false `no_match`.
    #[tokio::test]
    async fn an_email_verified_domain_match_never_auto_adds() {
        let Some(pool) = pg_test_pool("an_email_verified_domain_match_never_auto_adds").await else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aaemail", 5).await;
        let domain = format!("aaemail-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "email").await;
        let (requester_id, email) = seed_requester_at(&pool, "aaemail", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(auto_add_rows(&pool, account_id).await.is_empty());

        let (source, evidence): (String, Value) = sqlx::query_as(
            "SELECT source, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(source, "unverified_domain", "D11's own source for an address that does not arm auto-add");
        assert_eq!(
            evidence["ladder"]["domain"],
            json!("match_email_verified_only"),
            "the evidence must stay HONEST — it matched, by the wrong method"
        );
        assert_eq!(evidence["auto_add"]["blocked_by"], json!("domain_does_not_arm_auto_add"));
    }

    /// **CS-447 negative test 2 — founder decision, 2026-09-22: the pool
    /// check is against the CHOSEN TIER only.**
    ///
    /// The toggle is set to `premium`, which has zero purchased slots. The
    /// org has five free `standard` slots sitting idle. Auto-add must fall
    /// to approval with `source = 'pool_full'` rather than quietly spending
    /// one of them — a `premium` toggle must never consume a slot an admin
    /// bought as `standard` for somebody else.
    ///
    /// Without the tier narrowing this test fails LOUDLY (the person is
    /// added), not subtly, because the account-wide pool has room.
    #[tokio::test]
    async fn a_full_tier_falls_to_approval_even_when_another_tier_has_slots() {
        let Some(pool) = pg_test_pool("a_full_tier_falls_to_approval_even_when_another_tier_has_slots").await
        else {
            return;
        };
        // Five free `standard` slots…
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aatier", 6).await;
        let domain = format!("aatier-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aatier", &domain).await;
        // …and a toggle that spends `premium`, of which the org bought none.
        enable_auto_add(&pool, account_id, admin_id, "premium").await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
        assert_eq!(
            hits.load(Ordering::SeqCst),
            0,
            "D1: an empty pool falls to approval — it does NOT purchase. That is `approve`'s branch, \
             where an admin's click is the consent"
        );
        assert!(auto_add_rows(&pool, account_id).await.is_empty());
        assert_eq!(queued_source(&pool, account_id, requester_id).await.as_deref(), Some("pool_full"));

        // And the idle `standard` slots really were idle — which is what
        // makes this a tier test rather than an empty-pool test.
        let mut conn = pool.acquire().await.unwrap();
        let standard_tier: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
                .fetch_one(&mut *conn)
                .await
                .unwrap();
        let standard = read_slot_pool_for_tier(&mut conn, account_id, standard_tier).await.unwrap();
        assert!(standard.available() > 0, "the OTHER tier must still have had room: {standard:?}");
    }

    /// **CS-447 negative test 3 — D12: a tombstone blocks auto-add, and
    /// only auto-add.**
    ///
    /// Someone Acme removed comes back, on a DNS-verified address, as a
    /// full workspace member, with a free slot waiting. Every gate but the
    /// tombstone says yes. They are NOT added.
    ///
    /// The second half is the half D12 actually argues for: they are still
    /// queued, the request carries the flag, and the admin's `approve`
    /// works normally. *"We never let you in"* and *"we took it away"* are
    /// different, and only the second needs a person to look at it.
    #[tokio::test]
    async fn a_tombstoned_address_is_never_auto_added_but_can_still_be_approved() {
        let Some(pool) = pg_test_pool("a_tombstoned_address_is_never_auto_added_but_can_still_be_approved").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aatomb", 5).await;
        let domain = format!("aatomb-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aatomb", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;
        sqlx::query("INSERT INTO org_membership_tombstones (clerk_org_id, user_id) VALUES ($1, $2)")
            .bind(&org_id)
            .bind(requester_id)
            .execute(&pool)
            .await
            .expect("tombstone them");

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(auto_add_rows(&pool, account_id).await.is_empty(), "D12: a tombstone blocks auto-add");

        let (id, source, evidence): (Uuid, String, Value) = sqlx::query_as(
            "SELECT id, source, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("D12: the request is still QUEUED — a tombstone is not a rejection");
        assert_eq!(source, "tombstoned_returning");
        assert_eq!(evidence["tombstoned"], json!(true), "the admin decides with the flag in front of them");
        assert_eq!(evidence["auto_add"]["blocked_by"], json!("tombstoned"));

        // …and approval still works, on the same bought slot auto-add
        // refused to take.
        let resp = approve(
            State(test_state(pool.clone(), spawn_forbidden_stripe_mock().await.0)),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(id),
            Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "D12: approval is NOT blocked by a tombstone");
        assert_eq!(body_json(resp).await["slot"], json!("reserved"));
    }

    /// **CS-447 negative test 7 — two auto-adds race for the last slot.**
    ///
    /// One free slot, two people who each pass all three gates, both
    /// `create` calls in flight at once. Exactly one is added; the other
    /// falls to approval as `pool_full`. Never two members on one slot, and
    /// never a Stripe call to paper over the loser.
    ///
    /// The serialization is `reserve_slot`'s own
    /// (`begin_account_scoped` + `lock_period_rotation`, CS-435); this test
    /// proves auto-add actually goes through it rather than reading the
    /// pool itself and deciding.
    #[tokio::test]
    async fn two_concurrent_auto_adds_for_the_last_slot_admit_exactly_one() {
        let Some(pool) = pg_test_pool("two_concurrent_auto_adds_for_the_last_slot_admit_exactly_one").await
        else {
            return;
        };
        // 2 purchased, 1 held by the admin ⇒ exactly ONE free slot.
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aarace", 2).await;
        let domain = format!("aarace-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (first_id, first_email) = seed_requester_at(&pool, "aarace1", &domain).await;
        let (second_id, second_email) = seed_requester_at(&pool, "aarace2", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe_a, hits_a) = spawn_forbidden_stripe_mock().await;
        let (stripe_b, hits_b) = spawn_forbidden_stripe_mock().await;
        let first_token = full_member_token(account_id, first_id, &org_id, &first_email);
        let second_token = full_member_token(account_id, second_id, &org_id, &second_email);

        let (a, b) = tokio::join!(
            create(
                State(test_state(pool.clone(), stripe_a)),
                Extension(requester_ctx(first_id)),
                Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(first_token) }),
            ),
            create(
                State(test_state(pool.clone(), stripe_b)),
                Extension(requester_ctx(second_id)),
                Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(second_token) }),
            )
        );
        let statuses = [body_json(a).await["status"].clone(), body_json(b).await["status"].clone()];
        let added = statuses.iter().filter(|s| **s == json!("added")).count();
        let queued = statuses.iter().filter(|s| **s == json!("sent_to_admin")).count();
        assert_eq!(added, 1, "exactly one racer may take the last slot, got {statuses:?}");
        assert_eq!(queued, 1, "the other falls to approval, got {statuses:?}");

        assert_eq!(hits_a.load(Ordering::SeqCst) + hits_b.load(Ordering::SeqCst), 0, "neither racer may buy its way in");

        let rows = auto_add_rows(&pool, account_id).await;
        assert_eq!(rows.len(), 1, "one add, one ledger row");
        let loser = if rows[0].0 == first_id { second_id } else { first_id };
        assert_eq!(
            queued_source(&pool, account_id, loser).await.as_deref(),
            Some("pool_full"),
            "the loser is queued for an admin, told nothing about why"
        );
        let members: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = ANY($2)",
        )
        .bind(account_id)
        .bind(vec![first_id, second_id])
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(members, 1, "one slot, one member");
    }

    /// **M3 (security review, 2026-09-22) — only `full_member` arms rung 2.**
    ///
    /// D16's rung 2 is *"full workspace member AND verified domain match"*.
    /// The workspace half arrives as a signed `cls` claim, and the audience
    /// that gets these emails deliberately INCLUDES guests and Slack
    /// Connect participants whose address sits on the org's anchor domain
    /// (D2/D21) — so "we emailed them" is not "they work here", and
    /// treating any non-`full_member` class as arming would auto-add a
    /// client's guest account on the strength of an email we sent them.
    ///
    /// Every other gate is WIDE OPEN in each case below — toggle on, a
    /// DNS-verified domain, free slots, no tombstone, no rejection, a fresh
    /// token — so the classification is the only thing that can refuse, and
    /// a regression cannot hide behind an unrelated gate.
    ///
    /// The `Unavailable` case mints no `cls` claim at all, which is both a
    /// pre-CS-447 token and a member the roster walk could not classify.
    #[tokio::test]
    async fn only_a_full_member_token_arms_auto_add() {
        let Some(pool) = pg_test_pool("only_a_full_member_token_arms_auto_add").await else {
            return;
        };
        use crate::connectors::slack::MembershipClass;
        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;

        for (class, what) in [
            (MembershipClass::Guest, "guest"),
            (MembershipClass::SlackConnect, "slack_connect"),
            (MembershipClass::Unavailable, "no cls claim at all"),
        ] {
            let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aacls", 5).await;
            let domain = format!("aacls-{}.example", Uuid::new_v4().simple());
            verify_domain(&pool, account_id, &domain, "dns_txt").await;
            let (requester_id, email) = seed_requester_at(&pool, "aacls", &domain).await;
            enable_auto_add(&pool, account_id, admin_id, "standard").await;

            let (stripe, hits) = spawn_forbidden_stripe_mock().await;
            let token = token_with_class(account_id, requester_id, &org_id, &email, class);

            let resp = create(
                State(test_state(pool.clone(), stripe)),
                Extension(requester_ctx(requester_id)),
                Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            assert_eq!(
                body_json(resp).await["status"],
                json!("sent_to_admin"),
                "a {what} token must fall to the admin queue, not add"
            );
            assert_eq!(hits.load(Ordering::SeqCst), 0, "{what}: and must never reach Stripe");

            assert!(
                auto_add_rows(&pool, account_id).await.is_empty(),
                "{what}: nobody may be auto-added"
            );
            let members: i64 = sqlx::query_scalar(
                "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2",
            )
            .bind(account_id)
            .bind(requester_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(members, 0, "{what}: no membership may be created");

            // The request IS queued — falling is not refusing the person,
            // it is routing them to a human.
            assert!(
                queued_source(&pool, account_id, requester_id).await.is_some(),
                "{what}: the request must still reach the admin queue"
            );
        }
    }

    /// **H1 (security review, 2026-09-22) — D10: auto-add must never
    /// reverse an admin's explicit rejection.**
    ///
    /// The scenario the review describes, end to end. An admin rejects
    /// Dana. Later the org turns auto-add ON — the one gate that had been
    /// refusing — and Dana re-clicks the link she still holds. Every other
    /// gate is open: her domain is DNS-verified, there are free slots, she
    /// has no tombstone (she was never a member, so `reject` writes none),
    /// and her token is fresh and says `full_member`.
    ///
    /// Before the fix, `decide` never saw the rejection — D10's suppression
    /// lives in `enqueue_join_request`, which the ADDING path returns
    /// before reaching — so she was admitted and a seat was spent.
    ///
    /// Three assertions, and the third is the one a weaker fix would miss:
    /// no membership, no ledger row, and **no new open request** either.
    /// D10 suppresses the queue row too, so her re-click is a no-op in both
    /// directions and the admin is not re-asked a question they already
    /// answered.
    #[tokio::test]
    async fn auto_add_never_reverses_an_admins_rejection() {
        let Some(pool) = pg_test_pool("auto_add_never_reverses_an_admins_rejection").await else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aarej", 5).await;
        let domain = format!("aarej-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aarej", &domain).await;

        // Day 2: she asks, and an admin says no. Through the real writer,
        // so this is the row `reject` actually produces — including the
        // fact that it writes NO tombstone.
        let request_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence) \
             VALUES ($1, $2, 'self_request', '{}') RETURNING id",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("her original request");
        let decided = svc::mark_decided(&pool, request_id, account_id, admin_id, "rejected", json!({}))
            .await
            .expect("reject her");
        assert!(decided, "the rejection must land");
        // The tombstone ledger is keyed by `clerk_org_id`, not `account_id`
        // (migration 0208's "org id is a selector" shape).
        let tombstones: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM org_membership_tombstones WHERE clerk_org_id = $1 AND user_id = $2",
        )
        .bind(&org_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            tombstones, 0,
            "`reject` writes NO tombstone — which is exactly why the D12 gate cannot cover this \
             case and D10 has to be checked on its own"
        );

        // Day 10: the org turns auto-add on, and she re-clicks her link.
        enable_auto_add(&pool, account_id, admin_id, "standard").await;
        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(
            body_json(resp).await["status"],
            json!("sent_to_admin"),
            "byte-identical to every other falling gate — she is told nothing about why"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let members: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(members, 0, "an admin's 'no' must survive the org turning auto-add on");
        assert!(auto_add_rows(&pool, account_id).await.is_empty(), "and no seat may be spent");

        let open_rows: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM org_join_requests \
              WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(open_rows, 0, "D10 suppresses the queue row too — the admin is not re-asked");
    }

    /// **H2 (founder ruling, 2026-09-22) — 48 hours for auto-add; the link
    /// keeps its 30 days.**
    ///
    /// A token issued 49 hours ago. It is STILL a perfectly valid link —
    /// `JOIN_NOTICE_TOKEN_TTL_SECS` is 30 days, so `verify_join_notice_
    /// token` accepts it and the request is filed as `matched_not_member`
    /// with its `invited_at` intact. That is the half that must not
    /// regress: an old link keeps working, it just routes to the admin
    /// queue, which is exactly what it did before auto-add existed.
    ///
    /// What it may NOT do is spend a seat on a three-week-old snapshot of a
    /// roster walk. "Is a full member of that workspace" decays; nothing
    /// re-reads the roster between the walk and the click.
    ///
    /// The `blocked_by` assertion is the half that would catch a lazy fix:
    /// the refusal must name staleness, not be folded into some other gate.
    #[tokio::test]
    async fn auto_add_refuses_a_classification_older_than_48_hours() {
        let Some(pool) = pg_test_pool("auto_add_refuses_a_classification_older_than_48_hours").await else {
            return;
        };
        use crate::services::unmatched_recipient_email::{
            AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS, JOIN_NOTICE_TOKEN_TTL_SECS,
        };
        assert!(
            AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS < JOIN_NOTICE_TOKEN_TTL_SECS,
            "this test is only meaningful while the two half-lives differ"
        );

        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aastale", 5).await;
        let domain = format!("aastale-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aastale", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;

        let issued_at = chrono::Utc::now().timestamp() - (AUTO_ADD_MAX_CLASSIFICATION_AGE_SECS + 3600);
        let token = token_with_class_at(
            account_id,
            requester_id,
            &org_id,
            &email,
            crate::connectors::slack::MembershipClass::FullMember,
            issued_at,
        );

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::ACCEPTED);
        assert_eq!(body_json(resp).await["status"], json!("sent_to_admin"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let members: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(members, 0, "a 49-hour-old classification may not spend a seat");
        assert!(auto_add_rows(&pool, account_id).await.is_empty());

        let (source, evidence): (String, Value) = sqlx::query_as(
            "SELECT source, evidence FROM org_join_requests WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            source, "matched_not_member",
            "the LINK is still valid — it routes to the admin queue, exactly as it did before auto-add"
        );
        assert_eq!(
            evidence["auto_add"]["blocked_by"],
            json!("classification_too_old"),
            "the refusal must name staleness, so an admin is not sent to fix an unrelated gate"
        );
        assert!(
            evidence.get("invited_at").is_some(),
            "and the token's own claims still land — this is not an expiry"
        );
    }

    /// CodeRabbit #1037 finding 1: a successful auto-add must close any
    /// older OPEN request for this org so the admin queue does not keep a
    /// stale row after the person is already in.
    #[tokio::test]
    async fn auto_add_closes_a_preexisting_open_request() {
        let Some(pool) = pg_test_pool("auto_add_closes_a_preexisting_open_request").await else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aarecon", 2).await;
        let domain = format!("aarecon-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aarecon", &domain).await;

        let stale_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence) \
             VALUES ($1, $2, 'self_request', '{}') RETURNING id",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("plant a stale open request");

        enable_auto_add(&pool, account_id, admin_id, "standard").await;
        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(body_json(resp).await["status"], json!("added"));
        assert_eq!(hits.load(Ordering::SeqCst), 0);

        let (decision, evidence): (Option<String>, Value) = sqlx::query_as(
            "SELECT decision, evidence FROM org_join_requests WHERE id = $1")
        .bind(stale_id)
        .fetch_one(&pool)
        .await
        .expect("re-read the stale request");
        assert_eq!(decision.as_deref(), Some("approved"), "DB decision is approved, not auto_added");
        assert_eq!(evidence["auto_added"], json!(true));
        let still_open: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM org_join_requests \
              WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still_open, 0, "no leftover OPEN request after auto-add");
    }

    /// CodeRabbit #1037 finding 1: Fall(AlreadyMember) must not enqueue,
    /// and must close any leftover open request as approved.
    #[tokio::test]
    async fn already_a_member_does_not_enqueue_and_closes_open_requests() {
        let Some(pool) = pg_test_pool("already_a_member_does_not_enqueue_and_closes_open_requests").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aamem", 2).await;
        let domain = format!("aamem-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        let (requester_id, email) = seed_requester_at(&pool, "aamem", &domain).await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let stale_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence) \
             VALUES ($1, $2, 'self_request', '{}') RETURNING id",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("plant a leftover open request");

        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
            .fetch_one(&pool)
            .await
            .expect("standard tier");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, now())",
        )
        .bind(account_id)
        .bind(requester_id)
        .bind(tier_id)
        .execute(&pool)
        .await
        .expect("they are already a member");

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let token = full_member_token(account_id, requester_id, &org_id, &email);

        let resp = create(
            State(test_state(pool.clone(), stripe)),
            Extension(requester_ctx(requester_id)),
            Json(CreateJoinRequestRequest { org: org_id.clone(), token: Some(token) }),
        )
        .await;
        assert_eq!(body_json(resp).await["status"], json!("added"));
        assert_eq!(hits.load(Ordering::SeqCst), 0, "AlreadyMember must never reach Stripe");
        assert!(auto_add_rows(&pool, account_id).await.is_empty(), "AlreadyMember is not a new auto-add");

        let (decision, evidence): (Option<String>, Value) = sqlx::query_as(
            "SELECT decision, evidence FROM org_join_requests WHERE id = $1")
        .bind(stale_id)
        .fetch_one(&pool)
        .await
        .expect("re-read the leftover request");
        assert_eq!(decision.as_deref(), Some("approved"));
        assert_eq!(evidence["already_member"], json!(true));
        let still_open: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM org_join_requests \
              WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(still_open, 0);
    }

    /// CodeRabbit #1037 finding 1: approve must refuse to reserve/purchase
    /// when `account_members` already has a row.
    #[tokio::test]
    async fn approve_of_an_existing_member_does_not_reserve_or_purchase() {
        let Some(pool) = pg_test_pool("approve_of_an_existing_member_does_not_reserve_or_purchase").await
        else {
            return;
        };
        // 1 purchased seat, held by the admin — pool is full. Approve of a
        // non-member would purchase. An existing member must not.
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "aaappr", 1).await;
        let (requester_id, _clerk) = seed_requester(&pool, "aaappr").await;

        let request_id: Uuid = sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence) \
             VALUES ($1, $2, 'self_request', '{}') RETURNING id",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .expect("plant an open request");

        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
            .fetch_one(&pool)
            .await
            .expect("standard tier");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id, seat_purchased_at) \
             VALUES ($1, $2, 'member', $3, now())",
        )
        .bind(account_id)
        .bind(requester_id)
        .bind(tier_id)
        .execute(&pool)
        .await
        .expect("they are already a member");

        // Clerk is required at the top of `approve` even though this path
        // never calls it — membership is decided before reserve_slot.
        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = EnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let resp = approve(
            State(test_state(pool.clone(), stripe)),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(request_id),
            Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
        )
        .await;
        let body = body_json(resp).await;
        assert_eq!(body["slot"], json!("already_member"));
        assert_eq!(hits.load(Ordering::SeqCst), 0, "existing member must never purchase a seat");

        let reservations: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM slot_reservations WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(requester_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(reservations, 0, "approve must not reserve_slot for an existing member");

        let (decision, evidence): (Option<String>, Value) = sqlx::query_as(
            "SELECT decision, evidence FROM org_join_requests WHERE id = $1")
        .bind(request_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(decision.as_deref(), Some("approved"));
        assert_eq!(evidence["already_member"], json!(true));
    }

    // ── CS-449 (item 38, D11's "ask once", and the drain) ───────────────
    //
    // These exercise `drain_org`/`regate_and_admit_one` directly — the SAME
    // spine `routes::org_auto_add::admit_queued` calls once per candidate —
    // rather than a live `create()` call per candidate: queuing a
    // `pool_full` row does not itself touch Clerk, so building the row
    // directly (with a REAL `assertion_ladder::evaluate` outcome recorded
    // under `evidence.ladder`, never a hand-typed string) isolates what
    // these tests are actually about — the RE-GATE — from the ladder's own
    // already-covered behaviour above.

    /// A `matched_not_member`-shaped `evidence` blob for a candidate who
    /// WOULD pass every gate at queue time — full workspace member, on a
    /// DNS-verified domain, a fresh `invited_at`. Built from the real
    /// `assertion_ladder::evaluate`, not a hand-typed JSON literal, so a
    /// test using this can never drift from what `evidence()` actually
    /// produces.
    fn queued_evidence(email: &str, domain: &str, invited_at: chrono::DateTime<chrono::Utc>) -> Value {
        use crate::services::assertion_ladder::{
            evaluate, DirectorySignal, DomainSignal, LadderSignals, WorkspaceSignal,
        };
        let ladder = evaluate(LadderSignals {
            directory: DirectorySignal::Unavailable("test: no live directory".to_string()),
            workspace: WorkspaceSignal::FullMember,
            domain: DomainSignal::Match(domain.to_string()),
        });
        json!({
            "email": email,
            "workspace_id": "W_CS449",
            "channel_id": "C_CS449",
            "invited_at": invited_at.to_rfc3339(),
            "ladder": ladder.evidence(),
        })
    }

    /// Insert an OPEN, `pool_full`-sourced request directly — the shape
    /// `regate_and_admit_one`'s own `PoolFull` branch leaves behind, and
    /// [`drain_org`]'s own candidate set.
    async fn seed_queued_pool_full(
        pool: &PgPool,
        account_id: Uuid,
        user_id: Uuid,
        requested_at: chrono::DateTime<chrono::Utc>,
        evidence: Value,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence, requested_at) \
             VALUES ($1, $2, 'pool_full', $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(user_id)
        .bind(evidence)
        .bind(requested_at)
        .fetch_one(pool)
        .await
        .expect("seed a queued pool_full request")
    }

    /// **D11 (founder, 2026-09-22): "when passers exceed free slots, admit
    /// what fits, oldest first."** Three clean `pool_full` passers, only 2
    /// slots free. `drain_org` admits exactly the two oldest and leaves the
    /// newest queued — still `pool_full`, so the NEXT slot to free finds it
    /// without anyone asking again (D11's second ruling, "the remainder
    /// drains").
    #[tokio::test]
    async fn drain_admits_what_fits_oldest_first_and_leaves_the_remainder_pool_full() {
        let Some(pool) =
            pg_test_pool("drain_admits_what_fits_oldest_first_and_leaves_the_remainder_pool_full").await
        else {
            return;
        };
        // 1 purchased `standard` slot, held by the admin ⇒ 0 free until the
        // pool grows below.
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "drainfit", 1).await;
        let domain = format!("drainfit-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let now = chrono::Utc::now();
        let mut requests = Vec::new();
        for (i, tag) in ["oldest", "middle", "newest"].iter().enumerate() {
            let (user_id, email) = seed_requester_at(&pool, &format!("drainfit-{tag}"), &domain).await;
            let evidence = queued_evidence(&email, &domain, now);
            let requested_at = now - chrono::Duration::minutes(30 - (i as i64) * 10);
            let id = seed_queued_pool_full(&pool, account_id, user_id, requested_at, evidence).await;
            requests.push(id);
        }

        // A purchase growing the pool by exactly 2, simulated directly —
        // the same `grant_seat_tiers` write `purchase_slots` itself makes.
        let tier_id: Uuid = sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
            .fetch_one(&pool)
            .await
            .unwrap();
        let grant_id: Uuid = sqlx::query_scalar("SELECT id FROM grants WHERE account_id = $1")
            .bind(account_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        execute_financial_fixture(&pool, account_id, sqlx::query("UPDATE grant_seat_tiers SET seats = seats + 2 WHERE grant_id = $1 AND seat_tier_id = $2")
            .bind(grant_id)
            .bind(tier_id))
            .await
            .unwrap();

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let report = drain_org(&state, &pool, account_id, &org_id).await;

        assert_eq!(hits.load(Ordering::SeqCst), 0, "D1: the drain never calls Stripe either");
        assert_eq!(report.admitted, vec![requests[0], requests[1]], "the two OLDEST, in order — never more than the pool yielded");
        assert_eq!(report.not_admitted, vec![(requests[2], POOL_FULL_SOURCE)]);

        let rows = auto_add_rows(&pool, account_id).await;
        assert_eq!(rows.len(), 2, "exactly the free count");

        let (newest_source, newest_decision): (String, Option<String>) = sqlx::query_as(
            "SELECT source, decision FROM org_join_requests WHERE id = $1")
        .bind(requests[2])
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(newest_source, "pool_full", "the remainder stays pool_full for the next drain");
        assert!(newest_decision.is_none(), "still open, never silently decided");
    }

    /// **The drain RE-CHECKS every gate; it does not trust the queued
    /// row.** Three candidates queued as clean `pool_full` passers — by
    /// their OWN stored evidence, every one of them would have been
    /// admitted the moment a slot freed. Between being queued and the slot
    /// actually freeing, each stops qualifying for a DIFFERENT reason: an
    /// admin rejected one, a tombstone landed on another, and the third
    /// became a member through some other path. All three must still
    /// refuse, by name, on the CURRENT database state — never silently
    /// admitted because the row they were queued with still looks clean.
    #[tokio::test]
    async fn drain_re_checks_gates_and_never_admits_a_rejected_tombstoned_or_already_member_candidate() {
        let Some(pool) = pg_test_pool(
            "drain_re_checks_gates_and_never_admits_a_rejected_tombstoned_or_already_member_candidate",
        )
        .await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "drainregate", 10).await;
        let domain = format!("drainregate-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let now = chrono::Utc::now();

        let (rejected_user, rejected_email) = seed_requester_at(&pool, "drainregate-rej", &domain).await;
        let rejected_req = seed_queued_pool_full(
            &pool,
            account_id,
            rejected_user,
            now - chrono::Duration::minutes(30),
            queued_evidence(&rejected_email, &domain, now),
        )
        .await;
        // D10's durable suppression, for the SAME (account, user) as the
        // still-open row above — a second, independent decided row is what
        // proves the re-check reads CURRENT database state, not anything
        // baked into the queued row's own evidence.
        sqlx::query(
            "INSERT INTO org_join_requests (account_id, user_id, source, evidence, decision, decided_at, decided_by) \
             VALUES ($1, $2, 'self_request', '{}'::jsonb, 'rejected', now(), $3)",
        )
        .bind(account_id)
        .bind(rejected_user)
        .bind(admin_id)
        .execute(&pool)
        .await
        .unwrap();

        let (tombstoned_user, tombstoned_email) = seed_requester_at(&pool, "drainregate-tomb", &domain).await;
        let tombstoned_req = seed_queued_pool_full(
            &pool,
            account_id,
            tombstoned_user,
            now - chrono::Duration::minutes(20),
            queued_evidence(&tombstoned_email, &domain, now),
        )
        .await;
        sqlx::query("INSERT INTO org_membership_tombstones (clerk_org_id, user_id) VALUES ($1, $2)")
            .bind(&org_id)
            .bind(tombstoned_user)
            .execute(&pool)
            .await
            .unwrap();

        let (member_user, member_email) = seed_requester_at(&pool, "drainregate-mem", &domain).await;
        let member_req = seed_queued_pool_full(
            &pool,
            account_id,
            member_user,
            now - chrono::Duration::minutes(10),
            queued_evidence(&member_email, &domain, now),
        )
        .await;
        // PR F (seat lifecycle): every `account_members` writer must pass a
        // REAL `seat_tier_id` — unlike `org_auto_add_settings`/
        // `slot_reservations`, this column has no "NULL means standard"
        // reading of its own; a trigger enforces a concrete tier. No
        // `seat_purchased_at`: this member exists to prove `already_member`
        // refuses auto-add, not to hold a purchased slot.
        let standard_tier_id: Uuid =
            sqlx::query_scalar("SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active")
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members (account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', $3)")
            .bind(account_id)
            .bind(member_user)
            .bind(standard_tier_id)
            .execute(&pool)
            .await
            .unwrap();

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let report = drain_org(&state, &pool, account_id, &org_id).await;

        assert!(report.admitted.is_empty(), "none of the three may be admitted");
        assert_eq!(hits.load(Ordering::SeqCst), 0, "a refusal must never reach Stripe either");

        let mut got = report.not_admitted.clone();
        got.sort_by_key(|(id, _)| *id);
        let mut want = vec![
            (rejected_req, "previously_rejected"),
            (tombstoned_req, "tombstoned"),
            (member_req, "already_a_member"),
        ];
        want.sort_by_key(|(id, _)| *id);
        assert_eq!(got, want, "each refuses with its OWN reason, not a shared generic one");

        assert!(auto_add_rows(&pool, account_id).await.is_empty());

        // The already-member candidate's row closes (nothing left to wait
        // for — the same branch `approve`'s own already-member arm takes);
        // the other two stay OPEN for an admin to decide by hand.
        let (member_decision,): (Option<String>,) =
            sqlx::query_as("SELECT decision FROM org_join_requests WHERE id = $1")
                .bind(member_req)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(member_decision.as_deref(), Some("approved"));
        for id in [rejected_req, tombstoned_req] {
            let (decision,): (Option<String>,) = sqlx::query_as("SELECT decision FROM org_join_requests WHERE id = $1")
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap();
            assert!(decision.is_none(), "still open — a tombstone/rejection blocks AUTO-add only");
        }
    }

    /// **Blocker fix, CS-449 review round 2.** `drain_re_checks_gates_and_
    /// never_admits_a_rejected_tombstoned_or_already_member_candidate`
    /// above cannot catch this: every candidate it seeds keeps ITS OWN
    /// domain verified for the whole test, so its `verify_domain(...)` call
    /// is inert with respect to the domain gate — nothing in that test ever
    /// re-checks whether a stale `DomainSignal::Match` is still true.
    ///
    /// This one queues a candidate against a DNS-verified domain (a real
    /// `TryReserve(Rung::MemberAndDomain)` at queue time — `queued_evidence`
    /// bakes `DomainSignal::Match(domain)` into the STORED ladder), THEN
    /// revokes the domain (the admin removed it, or verification lapsed)
    /// BEFORE the drain ever runs. The domain gate is D11's gate 1 — the
    /// entire basis of the feature — so the drain must refuse this
    /// candidate on the CURRENT state of `org_domains`, not on the
    /// evidence it was queued with.
    ///
    /// **This test fails against the code before the blocker-2 fix**: a
    /// `regate_queued_request` that only calls `assertion_ladder::
    /// outcome_from_evidence` (replaying the WHOLE stored ladder, domain
    /// included) reconstructs `DomainSignal::Match(domain)` regardless of
    /// what `org_domains` says now, `decide` sees a perfectly good rung-2
    /// match, and the drain admits a stranger on a domain this org no
    /// longer controls — with no admin present.
    #[tokio::test]
    async fn drain_refuses_a_candidate_whose_domain_was_revoked_after_queuing() {
        let Some(pool) =
            pg_test_pool("drain_refuses_a_candidate_whose_domain_was_revoked_after_queuing").await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "domainrevoke", 10).await;
        let domain = format!("domainrevoke-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let now = chrono::Utc::now();
        let (user_id, email) = seed_requester_at(&pool, "domainrevoke", &domain).await;
        // Queued while the domain was still verified — the STORED evidence
        // carries `DomainSignal::Match(domain)` and rung `member_and_domain`,
        // exactly what a real `TryReserve` would have been queued from.
        let request_id = seed_queued_pool_full(
            &pool,
            account_id,
            user_id,
            now - chrono::Duration::minutes(30),
            queued_evidence(&email, &domain, now),
        )
        .await;

        // The org removes the domain (or its verification lapses) BEFORE
        // any slot frees — the exact scenario the blocker fix closes.
        let removed = sqlx::query("DELETE FROM org_domains WHERE account_id = $1 AND domain = $2")
            .bind(account_id)
            .bind(&domain)
            .execute(&pool)
            .await
            .expect("revoke the domain");
        assert_eq!(removed.rows_affected(), 1, "the fixture's own domain must actually be removed");

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let report = drain_org(&state, &pool, account_id, &org_id).await;

        assert!(
            report.admitted.is_empty(),
            "a candidate must never be admitted on a domain this org no longer controls — got {:?}",
            report.admitted
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0, "a refusal must never reach Stripe");
        assert_eq!(
            report.not_admitted,
            vec![(request_id, "domain_does_not_arm_auto_add")],
            "with the domain gone, the re-derived signal is `NoVerifiedDomain` (this org verified \
             nothing else); replayed `workspace: FullMember` plus that domain re-evaluates to \
             rung 3 (`ApprovalOnly`), and `decide`'s own fall-through reports the domain, not \
             `ladder_approval_only` — exactly the honest distinction `FallReason::DomainDoesNotArm`'s \
             own doc comment describes"
        );
        assert!(auto_add_rows(&pool, account_id).await.is_empty(), "nobody was added to the ledger");

        let (source, decision): (String, Option<String>) =
            sqlx::query_as("SELECT source, decision FROM org_join_requests WHERE id = $1")
                .bind(request_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            source, "unverified_domain",
            "`DomainDoesNotArm`'s own source_override — the row leaves the drain's `pool_full` \
             candidate set for good, which is correct: a domain that is actually gone will never \
             re-arm on its own"
        );
        assert!(decision.is_none(), "still open — an admin can still approve this person by hand");
    }

    /// **Blocker fix, CS-449 review round 2 — `may_spend`/
    /// `ADMIT_CAP_REACHED_REASON`.** `regate_and_admit_one`'s budget
    /// parameter is `routes::org_auto_add::admit_queued`'s own loop, driven
    /// here exactly the way that route drives it: five clean passers,
    /// `may_spend = admitted.len() < max_admit` with `max_admit = 2`.
    ///
    /// This is the actual defect the fix closes, proved end to end: EVERY
    /// passer submitted here — not just the two the budget covers — ends up
    /// `source = 'pool_full'`, open, undecided. Before this fix, a caller
    /// that submitted only the `would_admit`-sized slice never reached this
    /// function for the other three at all, so they kept whatever source
    /// they were queued under and `drain_org`'s candidate filter (`source =
    /// 'pool_full'`) never saw them. The final `drain_org` call below is
    /// the proof: with the pool wide open, it picks up exactly the three
    /// the budget left behind, with no second "ask".
    #[tokio::test]
    async fn budget_capped_candidates_are_regated_but_never_reserved_and_stay_drainable() {
        let Some(pool) = pg_test_pool(
            "budget_capped_candidates_are_regated_but_never_reserved_and_stay_drainable",
        )
        .await
        else {
            return;
        };
        let (account_id, org_id, admin_id, _price) = seed_org(&pool, "admitcap", 10).await;
        let domain = format!("admitcap-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        enable_auto_add(&pool, account_id, admin_id, "standard").await;

        let now = chrono::Utc::now();
        let mut requests = Vec::new();
        for (i, tag) in ["a", "b", "c", "d", "e"].iter().enumerate() {
            let (user_id, email) = seed_requester_at(&pool, &format!("admitcap-{tag}"), &domain).await;
            let evidence = queued_evidence(&email, &domain, now);
            let requested_at = now - chrono::Duration::minutes(50 - (i as i64) * 10);
            // A clean, ordinary `self_request` row — the shape a real
            // "ask to join" queues, and exactly what `admit_queued`'s own
            // candidates look like (NOT pre-seeded as `pool_full` — that
            // would defeat the point: this test is about candidates that
            // only BECOME `pool_full` as a result of the fix under test).
            let id: Uuid = sqlx::query_scalar(
                "INSERT INTO org_join_requests (account_id, user_id, source, evidence, requested_at) \
                 VALUES ($1, $2, 'self_request', $3, $4) RETURNING id",
            )
            .bind(account_id)
            .bind(user_id)
            .bind(evidence)
            .bind(requested_at)
            .fetch_one(&pool)
            .await
            .expect("seed a clean self_request passer");
            requests.push(id);
        }

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let settings = auto_add::read_settings_or_disabled(&pool, account_id).await;
        let summaries = svc::list_open(&pool, account_id).await.expect("list open");
        assert_eq!(summaries.len(), 5, "fixture sanity");

        let max_admit: i64 = 2;
        let mut admitted = Vec::new();
        let mut not_admitted = Vec::new();
        for req in &summaries {
            let may_spend = (admitted.len() as i64) < max_admit;
            match regate_and_admit_one(&state, &pool, account_id, &org_id, settings, req, now, may_spend).await {
                (id, Ok(())) => admitted.push(id),
                (id, Err(reason)) => not_admitted.push((id, reason)),
            }
        }

        assert_eq!(hits.load(Ordering::SeqCst), 0, "D1: a budget-capped candidate never reaches Stripe");
        assert_eq!(
            admitted,
            vec![requests[0], requests[1]],
            "the two OLDEST are admitted for real — an actual reservation, an actual Clerk call"
        );
        assert_eq!(
            not_admitted,
            vec![
                (requests[2], ADMIT_CAP_REACHED_REASON),
                (requests[3], ADMIT_CAP_REACHED_REASON),
                (requests[4], ADMIT_CAP_REACHED_REASON),
            ],
            "every remaining passer is reported with the CAP reason, never `pool_full` — no \
             reservation was ever attempted for any of them, so the pool was never actually found \
             empty"
        );

        let rows = auto_add_rows(&pool, account_id).await;
        assert_eq!(rows.len(), 2, "exactly the budget was spent, not the pool");

        for id in &requests[2..] {
            let (source, decision): (String, Option<String>) =
                sqlx::query_as("SELECT source, decision FROM org_join_requests WHERE id = $1")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(
                source, POOL_FULL_SOURCE,
                "the actual blocker-1 fix: every candidate submitted but not spent on becomes \
                 drainable, not just the ones the pool itself turned away"
            );
            assert!(decision.is_none(), "still open, waiting for the drain");
        }

        // The promise the dialog makes ("the rest are added as slots free
        // up") — proved by actually draining, pool wide open this time.
        let report = drain_org(&state, &pool, account_id, &org_id).await;
        let mut got = report.admitted.clone();
        got.sort();
        let mut want = vec![requests[2], requests[3], requests[4]];
        want.sort();
        assert_eq!(
            got, want,
            "the drain admits exactly the three the budget left behind, with no second ask"
        );
    }

    /// A slot freeing in an org that has never turned auto-add on — or has
    /// since turned it off — admits nobody. `drain_org`'s own first read
    /// (`enabled`) refuses before it ever lists a candidate, mirroring
    /// `decide`'s own first gate.
    #[tokio::test]
    async fn drain_into_a_disabled_org_admits_nobody() {
        let Some(pool) = pg_test_pool("drain_into_a_disabled_org_admits_nobody").await else {
            return;
        };
        let (account_id, org_id, _admin_id, _price) = seed_org(&pool, "draindisabled", 10).await;
        let domain = format!("draindisabled-{}.example", Uuid::new_v4().simple());
        verify_domain(&pool, account_id, &domain, "dns_txt").await;
        // Deliberately NO `enable_auto_add` call — that is the whole test.

        let now = chrono::Utc::now();
        let (user_id, email) = seed_requester_at(&pool, "draindisabled", &domain).await;
        let request_id =
            seed_queued_pool_full(&pool, account_id, user_id, now, queued_evidence(&email, &domain, now)).await;

        let clerk_base = spawn_clerk_ok_mock().await;
        let _env = AutoAddEnvGuard::acquire(&clerk_base).await;
        let (stripe, hits) = spawn_forbidden_stripe_mock().await;
        let state = test_state(pool.clone(), stripe);

        let report = drain_org(&state, &pool, account_id, &org_id).await;

        assert!(report.admitted.is_empty());
        assert!(
            report.not_admitted.is_empty(),
            "a disabled org's drain returns before ever listing a candidate — there is nothing to report"
        );
        assert_eq!(hits.load(Ordering::SeqCst), 0);
        assert!(auto_add_rows(&pool, account_id).await.is_empty());

        let (decision,): (Option<String>,) = sqlx::query_as("SELECT decision FROM org_join_requests WHERE id = $1")
            .bind(request_id)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert!(decision.is_none(), "still open, exactly as it was");
    }
    /// A Clerk double that records every call in order and can refuse
    /// membership creates (#1540 SF2).
    async fn spawn_clerk_recording_mock(
        refuse_memberships: bool,
    ) -> (String, std::sync::Arc<std::sync::Mutex<Vec<(String, String, Value)>>>) {
        let calls: std::sync::Arc<std::sync::Mutex<Vec<(String, String, Value)>>> = Default::default();
        let recorded = calls.clone();
        let app = axum::Router::new().fallback(axum::routing::any(move |req: axum::extract::Request| {
            let recorded = recorded.clone();
            async move {
                let method = req.method().to_string();
                let path = req.uri().path().to_string();
                let bytes = axum::body::to_bytes(req.into_body(), usize::MAX).await.unwrap_or_default();
                let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
                recorded.lock().unwrap().push((method.clone(), path.clone(), body));
                if method == "POST" && path.ends_with("/memberships") && refuse_memberships {
                    return (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"errors": [{"code": "x", "message": "down"}]})));
                }
                (StatusCode::OK, Json(json!({"id": "orgmem_mock", "object": "organization_membership"})))
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), calls)
    }

    /// #1540 SF2: approving a join request with the pool full (PURCHASE mode)
    /// on a self-serve org raises Clerk's member cap BEFORE Clerk is asked for
    /// the membership (Clerk would otherwise refuse it at the purchased-seat
    /// cap), and an approval that fails gives the headroom back (N3's helper).
    /// Run as `coralswarm_app`.
    #[tokio::test]
    async fn approve_in_purchase_mode_raises_a_selfserve_orgs_clerk_cap_first_and_gives_it_back_on_failure() {
        let name = "approve_in_purchase_mode_raises_a_selfserve_orgs_clerk_cap_first_and_gives_it_back_on_failure";
        let Some(owner) = pg_test_pool(name).await else {
            return;
        };
        let url = std::env::var("RELINFER_TEST_DATABASE_URL").unwrap();
        let app = crate::app_role_test_support::app_role_pool(&owner, &url).await;
        let mut outcomes = Vec::new();
        for refuse in [false, true] {
            let tag = if refuse { "jrcapfail" } else { "jrcapok" };
            // Pool full: 1 purchased seat, held by the admin.
            let (account_id, seeded_org, admin_id, price_id) = seed_org(&owner, tag, 1).await;
            // A self-serve intent needs a Clerk-shaped org id (`org_` + alphanumerics).
            let org_id = format!("org_{}", Uuid::new_v4().simple());
            execute_financial_fixture(
                &owner,
                account_id,
                sqlx::query("UPDATE accounts SET clerk_org_id = $2 WHERE id = $1 AND clerk_org_id = $3")
                    .bind(account_id)
                    .bind(&org_id)
                    .bind(&seeded_org),
            )
            .await
            .expect("re-key the seeded org");
            sqlx::query(
                "INSERT INTO org_selfserve_creations (user_id, idempotency_key, name, seats, status, clerk_org_id, account_id, sweep_state) \
                 VALUES ($1, $2, 'Capped', 2, 'created', $3, $4, 'kept')",
            )
            .bind(admin_id)
            .bind(format!("jrcap-{}", Uuid::new_v4().simple()))
            .bind(&org_id)
            .bind(account_id)
            .execute(&owner)
            .await
            .expect("self-serve intent");
            let (requester_id, _) = seed_requester(&owner, tag).await;
            let resp = create(
                State(test_state(app.clone(), StripeClient::new(reqwest::Client::new(), "sk_test_unused"))),
                Extension(requester_ctx(requester_id)),
                Json(CreateJoinRequestRequest { org: org_id.clone(), token: None }),
            )
            .await;
            assert_eq!(resp.status(), StatusCode::ACCEPTED);
            let id: Uuid = sqlx::query_scalar(
                "SELECT id FROM org_join_requests WHERE account_id = $1 AND user_id = $2 AND decision IS NULL",
            )
            .bind(account_id)
            .bind(requester_id)
            .fetch_one(&owner)
            .await
            .unwrap();

            let (clerk_base, calls) = spawn_clerk_recording_mock(refuse).await;
            let _env = EnvGuard::acquire(&clerk_base).await;
            let (stripe, _hits) = spawn_seat_mock(&price_id, 1).await;
            let state = test_state(app.clone(), stripe);
            let resp = approve(
                State(state.clone()),
                org_tenant(app.clone(), &org_id, account_id, admin_id),
                Extension(admin_ctx(account_id, admin_id)),
                Path(id),
                Json(ApproveJoinRequestRequest { tier: "standard".to_string() }),
            )
            .await;
            let status = resp.status();
            let calls = calls.lock().unwrap().clone();
            let first_member_post = calls.iter().position(|(m, p, _)| m == "POST" && p.ends_with("/memberships"));
            let patches: Vec<(usize, i64)> = calls
                .iter()
                .enumerate()
                .filter(|(_, (m, p, _))| m == "PATCH" && p == &format!("/v1/organizations/{org_id}"))
                .filter_map(|(i, (_, _, b))| b["max_allowed_memberships"].as_i64().map(|c| (i, c)))
                .collect();
            outcomes.push((refuse, status, first_member_post, patches));
            sqlx::query("DELETE FROM org_selfserve_creations WHERE account_id = $1").bind(account_id).execute(&owner).await.ok();
        }

        let (_, ok_status, ok_post, ok_patches) = &outcomes[0];
        assert_eq!(*ok_status, StatusCode::OK, "{outcomes:?}");
        let post = ok_post.expect("Clerk was asked for the membership");
        // Purchased 1, one member: cap 1; +1 for the member being bought.
        assert!(
            ok_patches.iter().any(|(i, cap)| *i < post && *cap == 2),
            "the cap was raised to 2 BEFORE the membership create: {outcomes:?}"
        );
        let (_, failed_status, _, failed_patches) = &outcomes[1];
        assert!(!failed_status.is_success(), "{outcomes:?}");
        assert_eq!(
            failed_patches.iter().map(|(_, c)| *c).collect::<Vec<_>>(),
            vec![2, 1],
            "raised for the approval, given back when it failed: {outcomes:?}"
        );
    }
}
