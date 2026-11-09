//! `/v1/me/delete-account` (settings launch PR 2.4, decisions 14, 17, 19, 24).
//! The durable job is `services::account_deletion` (its module doc has the
//! state machine); this file is the HTTP edge.
//!
//! # Who may call
//!
//! * **An interactive Clerk session only.** The routes require the
//!   `InteractiveClerkSession` marker `middleware::auth::require_auth` inserts
//!   solely for a Clerk JWT that carries `sid`. An add-in PAT (`csat_`), a Microsoft add-in session, an add-in OAuth token,
//!   any third-party OAuth token (`REST_FOREIGN_OAUTH` may let those through
//!   REST) and MCP are refused with 403 `session_required`.
//! * **A fresh re-verification** (decision 24). `POST` applies Clerk's own
//!   `strict` preset (`{afterMinutes: 10, level: "second_factor"}`) to the
//!   session's `fva` claim: the second factor's age when the user has one
//!   enrolled, otherwise the first factor's, strictly under 10 minutes
//!   ([`deletion::reverification_ok`]). Otherwise 403 `reverification_required`
//!   and the web asks Clerk to re-verify and retries. A v1 token (no `fva`)
//!   can never satisfy it.
//! * **A personal account no one else shares.** A live member of ANY team
//!   organization is refused with 409 `team_org_membership` (they must leave
//!   each first; an owner transfers ownership or deletes the organization). A
//!   sole owner is pointed at in-app org deletion: each blocking org carries
//!   `owner` and `can_delete_org`, and the 409 carries `delete_org_first`
//!   when any of them can be deleted that way (control 0387). Once the
//!   owner's org deletion is past grace the org is revoked and stops
//!   blocking; during grace it still blocks (the undo must stay possible). A
//!   personal account other users are members of is refused with 409
//!   `shared_personal_account` (it holds their data too). The caller's own
//!   personal org does not count as a team org.
//! * **Who confirms by typing their email**, compared case-insensitively with
//!   the address on file (or Clerk's current primary address when that
//!   differs). Verified here, never stored.
//!
//! # Contract (for the Danger zone / web PR)
//!
//! `GET /v1/me/delete-account` -> 200
//! `{ eligible, email, reverification_required, blocking_organizations:[{account_id,
//! clerk_org_id, slug, role}], shared_account_members:[{account_id, user_id,
//! email, name, role}], deletion: null | <status> }`. Reachable DURING grace and
//! while the destructive phase runs (the auth door lets exactly these two
//! endpoints through), where `deletion` is the live status.
//!
//! `POST /v1/me/delete-account` `{ "confirm_email": "..." }` ->
//! * 202 `{ id, status: "grace", deletion }`; `deletion.purge_after` is when
//!   the data is deleted (7 days), `deletion.can_cancel` is true.
//! * 400 `confirmation_mismatch`; 403 `session_required`,
//!   `reverification_required`; 409 `team_org_membership` + `organizations[]`,
//!   `shared_personal_account` + `members[]`.
//!
//! `POST /v1/me/delete-account/cancel` (signed in, no body) -> 200
//! `{status:"cancelled"}`, 409 `too_late`, 404 `no_pending_deletion`. Replaying
//! a link whose deletion is already cancelled is 200 `{status:"cancelled"}`
//! too; `too_late` is only for a deletion that is running or completed.
//!
//! `POST /v1/account-deletion/cancel` `{ "token": "..." }` (no auth: the signed
//! link from the email, for a user who is locked out of normal auth; the link
//! is `{web}/account/delete/cancel#token=...`, the token in the FRAGMENT, and
//! the page must require a click before posting it; signed with a key derived
//! from `ACCOUNT_DELETION_LINK_SECRET`, falling back to `OAUTH_STATE_SECRET`) -> the same
//! three answers, plus 400 `invalid_token`.
//!
//! From the 202 on every other authenticated request is refused: 403
//! `account_deletion_pending` during grace, `account_deletion_in_progress`
//! once destructive, then 401 `account_deleted`.

use std::sync::Arc;

use axum::extract::{Extension, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;
use uuid::Uuid;

use crate::middleware::auth::{InteractiveClerkSession, UserContext};
use crate::services::account_deletion as deletion;
use crate::AppState;

fn err(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({"error": msg, "code": code}))).into_response()
}

fn session_required() -> Response {
    err(
        StatusCode::FORBIDDEN,
        "session_required",
        "deleting an account requires a signed-in browser session",
    )
}

fn reverification_required() -> Response {
    err(
        StatusCode::FORBIDDEN,
        "reverification_required",
        "confirm your identity again to delete your account",
    )
}

fn blocked(orgs: Vec<deletion::TeamOrg>) -> Response {
    let delete_org_first = orgs.iter().any(|o| o.can_delete_org);
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": if delete_org_first {
                "delete the organizations you own, and leave the others, before deleting your account"
            } else {
                "leave every team organization before deleting your account"
            },
            "code": "team_org_membership",
            "delete_org_first": delete_org_first,
            "organizations": orgs,
        })),
    )
        .into_response()
}

fn shared(members: Vec<deletion::SharedMember>) -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({
            "error": "other people are members of your personal account; remove them or transfer it before deleting",
            "code": "shared_personal_account",
            "members": members,
        })),
    )
        .into_response()
}

fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "delete-account: failed");
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "internal error")
}

fn fresh(session: &InteractiveClerkSession) -> bool {
    deletion::reverification_ok(session.factor_verification_age)
}

/// `(email, clerk_user_id)` of the caller's own `users` row.
async fn caller_identity(
    control: &sqlx::PgPool,
    user_id: Uuid,
) -> Result<Option<(String, Option<String>)>, sqlx::Error> {
    sqlx::query_as("SELECT email, clerk_user_id FROM users WHERE id = $1")
        .bind(user_id)
        .fetch_optional(crate::tenancy::scope::request_read(control))
        .await
}

async fn primary_account(control: &sqlx::PgPool, user_id: Uuid) -> Result<Option<Uuid>, sqlx::Error> {
    Ok(deletion::personal_account_ids(control, user_id).await?.first().copied())
}

pub async fn get_delete_account(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
) -> Response {
    let Some(Extension(session)) = session else {
        return session_required();
    };
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let identity = match caller_identity(control, user.user_id).await {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, "not_found", "user not found"),
        Err(e) => return internal("identity", e),
    };
    let orgs = match deletion::blocking_team_orgs(control, user.user_id).await {
        Ok(o) => o,
        Err(e) => return internal("orgs", e),
    };
    let members = match deletion::shared_personal_members(control, user.user_id).await {
        Ok(m) => m,
        Err(e) => return internal("shared", e),
    };
    let existing = match primary_account(control, user.user_id).await {
        Ok(Some(account)) => deletion::get_by_user(control, account, user.user_id).await.ok().flatten(),
        Ok(None) => None,
        Err(e) => return internal("accounts", e),
    };
    (
        StatusCode::OK,
        Json(json!({
            "eligible": orgs.is_empty() && members.is_empty() && existing.is_none(),
            "email": identity.0,
            "reverification_required": !fresh(&session),
            "blocking_organizations": orgs,
            "shared_account_members": members,
            "deletion": existing.map(|d| d.status_json()),
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct DeleteAccountRequest {
    pub confirm_email: String,
}

fn same_email(a: &str, b: &str) -> bool {
    !a.trim().is_empty() && a.trim().eq_ignore_ascii_case(b.trim())
}

pub async fn post_delete_account(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
    Json(body): Json<DeleteAccountRequest>,
) -> Response {
    let Some(Extension(session)) = session else {
        return session_required();
    };
    // Decision 24(a): a fresh first-factor verification, checked before anything
    // else so a stale session learns nothing about the account.
    if !fresh(&session) {
        return reverification_required();
    }
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let (email, clerk_user_id) = match caller_identity(control, user.user_id).await {
        Ok(Some(i)) => i,
        Ok(None) => return err(StatusCode::NOT_FOUND, "not_found", "user not found"),
        Err(e) => return internal("identity", e),
    };

    // 1. Team organizations, then shared personal accounts: the answer does not
    //    depend on what was typed.
    let orgs = match deletion::blocking_team_orgs(control, user.user_id).await {
        Ok(o) => o,
        Err(e) => return internal("orgs", e),
    };
    if !orgs.is_empty() {
        return blocked(orgs);
    }
    match deletion::shared_personal_members(control, user.user_id).await {
        Ok(m) if !m.is_empty() => return shared(m),
        Ok(_) => {}
        Err(e) => return internal("shared", e),
    }

    // 2. Typed confirmation, against the address on file or Clerk's current one.
    let mut confirmed = same_email(&body.confirm_email, &email);
    if !confirmed {
        if let Some(sub) = clerk_user_id.as_deref() {
            if let Some(current) = crate::clerk::fetch_primary_email(sub, &state.http_client).await {
                confirmed = same_email(&body.confirm_email, &current);
            }
        }
    }
    if !confirmed {
        return err(
            StatusCode::BAD_REQUEST,
            "confirmation_mismatch",
            "the typed email does not match this account",
        );
    }

    // 3. The caller's own personal account, whatever organization the token names.
    let account_id = match primary_account(control, user.user_id).await {
        Ok(Some(a)) => a,
        Ok(None) => match crate::clerk_provision::ensure_personal_account(&state, control, user.user_id).await {
            Ok(a) => a,
            Err(e) => return internal("personal account", e),
        },
        Err(e) => return internal("accounts", e),
    };

    match deletion::start(control, user.user_id, account_id, clerk_user_id.as_deref(), deletion::Initiator::User).await {
        Ok(started) => (
            StatusCode::ACCEPTED,
            Json(json!({
                "id": started.deletion.id,
                "status": started.deletion.state,
                "deletion": started.deletion.status_json(),
            })),
        )
            .into_response(),
        Err(deletion::StartError::BlockedByOrgs(orgs)) => blocked(orgs),
        Err(deletion::StartError::SharedAccount(members)) => shared(members),
        Err(deletion::StartError::Db(e)) => internal("start", e),
    }
}

fn cancel_response(result: Result<deletion::CancelResult, sqlx::Error>) -> Response {
    match result {
        // A replayed link for a deletion that is already cancelled is the same
        // success, not a conflict: the account is exactly as the user asked.
        Ok(deletion::CancelResult::Cancelled) | Ok(deletion::CancelResult::AlreadyCancelled) => {
            (StatusCode::OK, Json(json!({"status": "cancelled"}))).into_response()
        }
        Ok(deletion::CancelResult::TooLate) => err(
            StatusCode::CONFLICT,
            "too_late",
            "this deletion can no longer be cancelled",
        ),
        Ok(deletion::CancelResult::NotFound) => {
            err(StatusCode::NOT_FOUND, "no_pending_deletion", "there is no pending deletion")
        }
        Err(e) => internal("cancel", e),
    }
}

/// Cancel while signed in. Needs a session (not a fresh re-verification:
/// cancelling is the safe direction) and acts only on the caller's own
/// deletion.
pub async fn post_cancel_delete_account(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
) -> Response {
    if session.is_none() {
        return session_required();
    }
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let account = match primary_account(control, user.user_id).await {
        Ok(Some(a)) => a,
        Ok(None) => return cancel_response(Ok(deletion::CancelResult::NotFound)),
        Err(e) => return internal("accounts", e),
    };
    match deletion::get_by_user(control, account, user.user_id).await {
        Ok(Some(d)) => cancel_response(deletion::cancel(control, d.id).await),
        Ok(None) => cancel_response(Ok(deletion::CancelResult::NotFound)),
        Err(e) => internal("lookup", e),
    }
}

#[derive(Debug, Deserialize)]
pub struct CancelByTokenRequest {
    pub token: String,
}

/// Cancel from the emailed link. No session (the user is locked out of normal
/// auth): the signed token is the whole credential and can only cancel the one
/// deletion it names. Mounted on the open router.
pub async fn post_cancel_by_token(
    State(state): State<Arc<AppState>>,
    Json(body): Json<CancelByTokenRequest>,
) -> Response {
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let Some(key) = deletion::cancel_key_from_env() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let Some(id) = deletion::verify_cancel_token(&key, &body.token) else {
        return err(StatusCode::BAD_REQUEST, "invalid_token", "this cancel link is not valid");
    };
    cancel_response(deletion::cancel(control, id).await)
}
