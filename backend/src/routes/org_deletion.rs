//! `/v1/orgs/{org_id}/deletion`: the HTTP edge of in-app org deletion. The
//! job and its state machine are `services::org_deletion` (module doc); this
//! file only authorizes and maps answers.
//!
//! All four routes answer 404 while `ORG_DELETION_ENABLED` is unset.
//!
//! # Who may call
//!
//! * **An interactive Clerk session only** (the `InteractiveClerkSession`
//!   marker: a Clerk session JWT with `sid`). A PAT, add-in session,
//!   OAuth/MCP client is refused with 403 `session_required`, and
//!   the request-scoped authority check (#1473) answers 403
//!   `interactive_session_required` for anything that slipped past.
//! * **The in-app OWNER** for the request, the undo and the export (role
//!   `owner`, or the admin who created the org). Another admin or a member
//!   gets 403 `not_owner`; a non-member 404. Any live member may read the
//!   status (so a locked-out member's screen can say why).
//! * **A fresh re-verification** for the request (Clerk's `strict` preset,
//!   the same rule as account deletion), and the org's NAME typed exactly.
//!
//! The routes name the org in the path and read only the control plane, so
//! they work with the org's own token during grace: the auth door lets these
//! paths through the org-deletion lockout (and nothing else).
//!
//! # Contract
//!
//! `GET  /v1/orgs/{org_id}/deletion` -> 200 `{ is_owner, eligible,
//! blocked_reason, org_name, reverification_required, deletion: null |
//! <status> }`; `blocked_reason` is `not_owner` | `not_self_serve` | null.
//!
//! `POST /v1/orgs/{org_id}/deletion` `{ "confirm_name": "..." }` -> 202
//! `{ deletion }` (200 with the same body when one already exists). 400
//! `confirmation_mismatch`; 403 `session_required`, `reverification_required`,
//! `not_owner`; 404; 409 `not_self_serve`, `personal_org`, `org_deleted`.
//!
//! `POST /v1/orgs/{org_id}/deletion/cancel` -> 200 `{status:"canceled"}`
//! (access restored; the subscription stays cancelled: resubscribe from
//! Billing); 409 `not_yet` (still ending the subscription, retry shortly),
//! `too_late`; 404 `no_pending_deletion`.
//!
//! `GET  /v1/orgs/{org_id}/deletion/export` -> 200 NDJSON attachment of the
//! data the OWNER may see (owner, during `requested`/`grace` only, fresh
//! re-verification required like the request): the shared org oceans, the
//! owner's own member ocean and the owner's own conversations; never another
//! member's private data (the whole-tenant copy is the operator's). 403
//! `reverification_required`; 409 `export_closed` after grace, 409
//! `export_in_progress` (one at a time per org); 413 `export_too_large` (over
//! the download cap: contact support for an operator export).

use std::sync::Arc;

use axum::extract::{Extension, Path, State};
use axum::http::{header, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Deserialize;
use serde_json::json;

use crate::middleware::auth::{InteractiveClerkSession, UserContext};
use crate::services::org_deletion::{self as od, ExportScope, OrgDataPlane};
use crate::AppState;

fn err(status: StatusCode, code: &str, msg: &str) -> Response {
    (status, Json(json!({"error": msg, "code": code}))).into_response()
}

fn not_found() -> Response {
    err(StatusCode::NOT_FOUND, "not_found", "not found")
}

fn session_required() -> Response {
    err(
        StatusCode::FORBIDDEN,
        "session_required",
        "deleting an organization requires a signed-in browser session",
    )
}

fn not_owner() -> Response {
    err(StatusCode::FORBIDDEN, "not_owner", "only the organization's owner can do this")
}

fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org deletion route: failed");
    err(StatusCode::INTERNAL_SERVER_ERROR, "internal_error", "internal error")
}

/// The interactive-session gate shared by every route: the request-scoped
/// authority (#1473, `interactive_session_required`), then the session
/// marker itself (`session_required`).
fn interactive(session: &Option<Extension<InteractiveClerkSession>>) -> Result<InteractiveClerkSession, Response> {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return Err(r);
    }
    let Some(Extension(s)) = session else {
        return Err(session_required());
    };
    Ok(*s)
}

async fn org_name(org_id: &str) -> Option<String> {
    let clerk = crate::clerk_admin::ClerkAdminClient::from_env()?;
    clerk.get_organization(org_id).await.ok().map(|o| o.name)
}

pub async fn get_org_deletion(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
    Path(org_id): Path<String>,
) -> Response {
    if !od::enabled() {
        return not_found();
    }
    let session = match interactive(&session) {
        Ok(s) => s,
        Err(r) => return r,
    };
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let facts = match od::org_facts(control, &org_id, user.user_id).await {
        Ok(Some(f)) if f.role.is_some() && !f.personal => f,
        Ok(_) => return not_found(),
        Err(e) => return internal("facts", e),
    };
    let deletion = match od::live_for_account(control, facts.account_id).await {
        Ok(d) => d,
        Err(e) => return internal("lookup", e),
    };
    let blocked_reason = if !facts.is_owner {
        Some("not_owner")
    } else if !facts.self_serve_ready {
        Some("not_self_serve")
    } else {
        None
    };
    let name = if facts.is_owner { org_name(&org_id).await } else { None };
    (
        StatusCode::OK,
        Json(json!({
            "is_owner": facts.is_owner,
            "eligible": blocked_reason.is_none() && deletion.is_none() && !facts.revoked,
            "blocked_reason": blocked_reason,
            "org_name": name,
            "reverification_required": !crate::services::account_deletion::reverification_ok(session.factor_verification_age),
            "deletion": deletion.map(|d| d.status_json(facts.is_owner)),
        })),
    )
        .into_response()
}

#[derive(Debug, Deserialize)]
pub struct RequestBody {
    pub confirm_name: String,
}

pub async fn post_org_deletion(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
    Path(org_id): Path<String>,
    Json(body): Json<RequestBody>,
) -> Response {
    if !od::enabled() {
        return not_found();
    }
    let session = match interactive(&session) {
        Ok(s) => s,
        Err(r) => return r,
    };
    if !crate::services::account_deletion::reverification_ok(session.factor_verification_age) {
        return err(
            StatusCode::FORBIDDEN,
            "reverification_required",
            "confirm your identity again to delete this organization",
        );
    }
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    // Authorization before anything that depends on the typed name, so a
    // non-owner learns nothing about the org.
    let facts = match od::org_facts(control, &org_id, user.user_id).await {
        Ok(Some(f)) if f.role.is_some() => f,
        Ok(_) => return not_found(),
        Err(e) => return internal("facts", e),
    };
    if !facts.is_owner && !facts.personal {
        return not_owner();
    }
    let name = match org_name(&org_id).await {
        Some(n) => n,
        None if facts.personal || facts.revoked => String::new(),
        None => return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "the identity service is unavailable"),
    };
    match od::request_by_owner(control, user.user_id, &org_id, &body.confirm_name, &name).await {
        Ok(started) => (
            if started.created { StatusCode::ACCEPTED } else { StatusCode::OK },
            Json(json!({"deletion": started.deletion.status_json(true)})),
        )
            .into_response(),
        Err(e) => request_error(e),
    }
}

pub(crate) fn request_error(e: od::RequestError) -> Response {
    use od::RequestError as E;
    match e {
        E::NotFound => not_found(),
        E::NotOwner => not_owner(),
        E::PersonalOrg => err(StatusCode::CONFLICT, "personal_org", "a personal workspace is deleted with your account"),
        E::Revoked => err(StatusCode::CONFLICT, "org_deleted", "this organization is already deleted"),
        E::NotSelfServe => err(
            StatusCode::CONFLICT,
            "not_self_serve",
            "this organization was set up by CoralSwarm; contact support to delete it",
        ),
        E::NameMismatch => err(
            StatusCode::BAD_REQUEST,
            "confirmation_mismatch",
            "the typed name does not match the organization",
        ),
        E::Db(e) => internal("request", e),
    }
}

pub async fn post_cancel_org_deletion(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
    Path(org_id): Path<String>,
) -> Response {
    if !od::enabled() {
        return not_found();
    }
    if let Err(r) = interactive(&session) {
        return r;
    }
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    match od::org_facts(control, &org_id, user.user_id).await {
        Ok(Some(f)) if f.role.is_some() && !f.is_owner => return not_owner(),
        Ok(_) => {}
        Err(e) => return internal("facts", e),
    }
    match od::cancel_by_owner(control, user.user_id, &org_id).await {
        Ok(od::CancelResult::Canceled) => (
            StatusCode::OK,
            Json(json!({
                "status": "canceled",
                "subscription": "canceled",
                "note": "access is restored; the subscription stays cancelled, so subscribe again from Billing",
            })),
        )
            .into_response(),
        Ok(od::CancelResult::NotYet) => err(
            StatusCode::CONFLICT,
            "not_yet",
            "the deletion is still ending the subscription; try again in a few seconds",
        ),
        Ok(od::CancelResult::TooLate) => err(StatusCode::CONFLICT, "too_late", "this deletion can no longer be undone"),
        Ok(od::CancelResult::RevokedInClerk) => err(
            StatusCode::CONFLICT,
            "org_deleted_in_clerk",
            "org already deleted in Clerk; deletion can't be undone",
        ),
        Ok(od::CancelResult::NotFound) => err(StatusCode::NOT_FOUND, "no_pending_deletion", "there is no pending deletion"),
        Err(e) => internal("cancel", e),
    }
}

pub async fn get_org_deletion_export(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    session: Option<Extension<InteractiveClerkSession>>,
    Path(org_id): Path<String>,
) -> Response {
    if !od::enabled() {
        return not_found();
    }
    let session = match interactive(&session) {
        Ok(s) => s,
        Err(r) => return r,
    };
    // The export is the org's data leaving: it needs the same fresh
    // re-verification as the request itself.
    if !crate::services::account_deletion::reverification_ok(session.factor_verification_age) {
        return err(
            StatusCode::FORBIDDEN,
            "reverification_required",
            "confirm your identity again to download this organization's data",
        );
    }
    let Some(control) = state.control_db() else {
        return err(StatusCode::SERVICE_UNAVAILABLE, "service_unavailable", "service unavailable");
    };
    let facts = match od::org_facts(control, &org_id, user.user_id).await {
        Ok(Some(f)) if f.role.is_some() => f,
        Ok(_) => return not_found(),
        Err(e) => return internal("facts", e),
    };
    if !facts.is_owner {
        return not_owner();
    }
    let deletion = match od::live_for_account(control, facts.account_id).await {
        Ok(Some(d)) => d,
        Ok(None) => return err(StatusCode::NOT_FOUND, "no_pending_deletion", "there is no pending deletion"),
        Err(e) => return internal("lookup", e),
    };
    if !matches!(deletion.state.as_str(), "requested" | "grace") {
        return err(
            StatusCode::CONFLICT,
            "export_closed",
            "the grace period is over; contact support for the retained export",
        );
    }
    // One owner export at a time per org: an export is built in memory.
    let mut guard = match od::ExportGuard::try_acquire(control, &org_id).await {
        Ok(Some(g)) => g,
        Ok(None) => {
            return err(
                StatusCode::CONFLICT,
                "export_in_progress",
                "an export of this organization is already being prepared; try again in a minute",
            )
        }
        Err(e) => return internal("export_lock", e),
    };
    let plane = crate::services::org_deletion_live::LiveDataPlane { state: state.clone() };
    let result = plane.export_bundle(&deletion, &ExportScope::Owner { user_id: user.user_id }).await;
    guard.release().await;
    match result {
        Err(e) if e.starts_with(od::EXPORT_TOO_LARGE) => err(
            StatusCode::PAYLOAD_TOO_LARGE,
            "export_too_large",
            e.split_once(": ").map(|(_, m)| m).unwrap_or("the export is too large to download"),
        ),
        Ok(bytes) => {
            tracing::info!(deletion_id = %deletion.id, org = %org_id, user_id = %user.user_id, bytes = bytes.len(),
                "org deletion: owner downloaded the export during grace");
            (
                StatusCode::OK,
                [
                    (header::CONTENT_TYPE, "application/x-ndjson".to_string()),
                    (
                        header::CONTENT_DISPOSITION,
                        format!("attachment; filename=\"coralswarm-org-export-{}.ndjson\"", deletion.id),
                    ),
                    (header::CACHE_CONTROL, "no-store".to_string()),
                ],
                bytes,
            )
                .into_response()
        }
        Err(e) => internal("export", e),
    }
}

/// The paths the org-deletion lockout lets through (the auth door's
/// `org_deletion_paths`): exactly these four, for any org id.
pub fn is_org_deletion_path(method: &axum::http::Method, path: &str) -> bool {
    let p = path.strip_prefix("/v1").unwrap_or(path);
    let Some(rest) = p.strip_prefix("/orgs/") else { return false };
    let mut parts = rest.split('/');
    let (Some(org), Some("deletion")) = (parts.next(), parts.next()) else { return false };
    if org.is_empty() {
        return false;
    }
    match (parts.next(), parts.next()) {
        (None, None) => *method == axum::http::Method::GET || *method == axum::http::Method::POST,
        (Some("cancel"), None) => *method == axum::http::Method::POST,
        (Some("export"), None) => *method == axum::http::Method::GET,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Method;

    #[test]
    fn only_the_four_deletion_paths_pass_the_lockout() {
        assert!(is_org_deletion_path(&Method::GET, "/v1/orgs/org_1/deletion"));
        assert!(is_org_deletion_path(&Method::POST, "/v1/orgs/org_1/deletion"));
        assert!(is_org_deletion_path(&Method::POST, "/v1/orgs/org_1/deletion/cancel"));
        assert!(is_org_deletion_path(&Method::GET, "/orgs/org_1/deletion/export"));
        assert!(!is_org_deletion_path(&Method::DELETE, "/v1/orgs/org_1/deletion"));
        assert!(!is_org_deletion_path(&Method::GET, "/v1/orgs/org_1/deletion/cancel"));
        assert!(!is_org_deletion_path(&Method::GET, "/v1/orgs/org_1/members"));
        assert!(!is_org_deletion_path(&Method::GET, "/v1/orgs//deletion"));
        assert!(!is_org_deletion_path(&Method::GET, "/v1/orgs/org_1/deletion/export/x"));
        assert!(!is_org_deletion_path(&Method::GET, "/v1/bootstrap"));
    }
}

#[path = "org_deletion_pg_tests.rs"]
#[cfg(test)]
mod pg_tests;
