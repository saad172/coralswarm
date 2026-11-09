//! The self-serve team org's first Stripe Checkout (signup PR C′): the
//! card-up-front, $0-today Checkout for a free-credit trial, and its
//! "resume payment" route.
//!
//! # Who calls what
//!
//! * [`start_org_trial_checkout`] is the library entry point. `POST
//!   /v1/orgs` (PR A2) calls it right after creating the Clerk org, the
//!   `accounts` row and the creator's admin membership, and returns its
//!   `checkout_url`. That route must skip the binding gate (a brand-new org
//!   has no routing row yet), so the creation is A2's, not this module's.
//! * `POST /v1/orgs/{org_id}/trial-checkout` ([`resume_org_trial_checkout`])
//!   is "resume payment": a new Checkout for an org whose first one was
//!   abandoned, within 24 hours of the first. It names the org in the path
//!   and is meant to be called with the creator's PERSONAL session (the org
//!   has no tenant database yet, so a token carrying that org's claim is
//!   refused by the binding gate). Authority is the caller's own
//!   `account_members.role` on that org (owner/admin), plus an interactive
//!   browser session, the same bar `require_purchase_authority` sets.
//!
//! Completion (the trial pool, the grant, provisioning) is
//! `routes::stripe_webhook`'s `checkout.session.completed`; nothing here
//! grants anything.
//!
//! # One trial per creator
//!
//! Whether the creator already had a trial is checked here, BEFORE the
//! session exists (decision #9): a repeat creator gets a Checkout with no
//! trial that charges the seats on day 1. The card half of the rule can only
//! be checked once the card exists, at completion.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use uuid::Uuid;

use crate::billing::org_trial::{self, db as trial_db, TrialState};
use crate::middleware::auth::UserContext;
use crate::AppState;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OrgTrialCheckoutRequest {
    /// Seats to buy, at least 2.
    pub seats: i64,
    pub success_url: String,
    pub cancel_url: String,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct OrgTrialCheckoutResponse {
    pub checkout_url: String,
    pub session_id: String,
    /// `false` when the creator already had a trial: this Checkout charges
    /// the seats now. The web shows different copy for each.
    pub trial: bool,
    pub seats: i64,
    /// Free credits the trial comes with (when `trial`).
    pub trial_credits: i64,
    pub trial_days: i64,
}

fn refuse(status: StatusCode, code: &str, message: &str) -> Response {
    (status, Json(json!({"error": message, "code": code}))).into_response()
}

fn unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org trial checkout: failed, creating nothing");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Create the org's trial Checkout (or, for a repeat creator, its pay-now
/// Checkout) and record it on `org_trials` as `checkout_open`.
///
/// The caller has authenticated `creator` and proved they administer
/// `account_id`; this function re-checks everything that is about the ORG:
/// a real team org (not a personal org), no live subscription, and no trial
/// already past checkout.
pub(crate) async fn start_org_trial_checkout(
    state: &AppState,
    account_id: Uuid,
    creator: Uuid,
    seats: i64,
    success_url: &str,
    cancel_url: &str,
) -> Result<OrgTrialCheckoutResponse, Response> {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return Err(r);
    }
    let seats = org_trial::validate_seats(seats).map_err(|m| refuse(StatusCode::BAD_REQUEST, "invalid_seats", &m))?;
    if success_url.trim().is_empty() || cancel_url.trim().is_empty() {
        return Err(refuse(StatusCode::BAD_REQUEST, "return_urls_required", "success_url and cancel_url are required"));
    }
    let (Some(control), Some(store)) = (state.control_db(), state.control_store()) else {
        return Err(unavailable());
    };
    match store.account_is_real_org(account_id, creator).await {
        Ok(true) => {}
        Ok(false) => {
            return Err(refuse(StatusCode::BAD_REQUEST, "not_a_team_org", "only a team organization can start a trial"))
        }
        Err(crate::store::StoreError::NotFound) => {
            return Err(refuse(StatusCode::NOT_FOUND, "org_not_found", "organization not found"))
        }
        Err(e) => return Err(internal("account lookup", e)),
    }
    crate::routes::checkout::validate_return_urls(state, control, account_id, success_url, cancel_url).await?;
    crate::routes::checkout::refuse_if_already_subscribed(state, account_id).await?;

    // Where the trial is, and whether the creator already had one.
    let (current, eligible) = {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| internal("scope", e))?;
        let row = trial_db::load(&mut tx, account_id).await.map_err(|e| internal("org_trials read", e))?;
        let claimed = trial_db::already_claimed(&mut tx, Some(creator), None)
            .await
            .map_err(|e| internal("trial claim check", e))?;
        tx.commit().await.map_err(|e| internal("commit", e))?;
        (row, !claimed)
    };
    if let Some(row) = &current {
        match row.state {
            TrialState::CheckoutOpen if org_trial::checkout_abandoned(row.checkout_opened_at, chrono::Utc::now()) => {
                return Err(refuse(
                    StatusCode::GONE,
                    "checkout_expired",
                    "this sign-up was not paid within 24 hours; create the organization again",
                ));
            }
            TrialState::CheckoutOpen => {}
            TrialState::Abandoned => {
                return Err(refuse(
                    StatusCode::GONE,
                    "checkout_expired",
                    "this sign-up was not paid within 24 hours; create the organization again",
                ))
            }
            _ => {
                return Err(refuse(
                    StatusCode::CONFLICT,
                    "checkout_complete",
                    "this organization's checkout is already complete",
                ))
            }
        }
    }

    let price_id = crate::routes::checkout::resolve_seat_tier_price(control, org_trial::TRIAL_SEAT_TIER).await?;
    let customer_id = crate::routes::checkout::ensure_stripe_customer(state, account_id).await?;
    let Some(stripe) = state.stripe.as_ref() else {
        return Err(unavailable());
    };
    let trial_days = eligible.then_some(org_trial::TRIAL_DAYS);
    let session = stripe
        .create_org_trial_checkout_session(
            &customer_id,
            &price_id,
            seats,
            &account_id.to_string(),
            trial_days,
            success_url,
            cancel_url,
        )
        .await
        .map_err(|e| internal("stripe create checkout session", e))?;

    let opened = async {
        let mut tx = crate::tenancy::scope::begin_account_scoped(control, account_id)
            .await
            .map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await?;
        let seats_i32 = i32::try_from(seats).map_err(|e| sqlx::Error::Protocol(e.to_string()))?;
        let out = trial_db::open_checkout(&mut tx, account_id, creator, seats_i32, eligible, &session.id).await?;
        tx.commit().await?;
        Ok::<_, sqlx::Error>(out)
    }
    .await;
    match opened {
        Ok(trial_db::OpenOutcome::Open) => {}
        Ok(trial_db::OpenOutcome::NotOpen(s)) => {
            // Lost a race with a completion: retire the session just made.
            let _ = stripe.expire_checkout_session(&session.id).await;
            tracing::warn!(%account_id, state = s.as_str(), "org trial checkout: trial moved past checkout while creating a session");
            return Err(refuse(StatusCode::CONFLICT, "checkout_complete", "this organization's checkout is already complete"));
        }
        Err(e) => {
            let _ = stripe.expire_checkout_session(&session.id).await;
            return Err(internal("org_trials write", e));
        }
    }
    crate::routes::checkout::replace_pending_checkout_session(state, stripe, account_id, &session.id).await;
    tracing::info!(%account_id, %creator, seats, trial = eligible, "org trial checkout: session created");
    Ok(OrgTrialCheckoutResponse {
        checkout_url: session.url,
        session_id: session.id,
        trial: eligible,
        seats,
        trial_credits: org_trial::TRIAL_CREDITS,
        trial_days: org_trial::TRIAL_DAYS,
    })
}

/// `POST /v1/orgs/{org_id}/trial-checkout` — "resume payment" for an org
/// whose Checkout was not completed. See the module doc for why it names the
/// org in the path and runs on the caller's personal session.
pub async fn resume_org_trial_checkout(
    State(state): State<Arc<AppState>>,
    Extension(user): Extension<UserContext>,
    Path(org_id): Path<String>,
    Json(body): Json<OrgTrialCheckoutRequest>,
) -> Response {
    if let Some(r) = crate::middleware::auth::interactive_session_refusal() {
        return r;
    }
    let (Some(control), Some(store)) = (state.control_db(), state.control_store()) else {
        return unavailable();
    };
    let account: Option<Uuid> = match sqlx::query_scalar("SELECT id FROM accounts WHERE clerk_org_id = $1")
        .bind(&org_id)
        .fetch_optional(control)
        .await
    {
        Ok(a) => a,
        Err(e) => return internal("account lookup", e),
    };
    // Not an admin and no such org answer the same 404: the path must not
    // tell a stranger which org ids exist.
    let not_found = || refuse(StatusCode::NOT_FOUND, "org_not_found", "organization not found");
    let Some(account_id) = account else {
        return not_found();
    };
    match store.get_account_member_role(account_id, user.user_id).await {
        Ok(Some(role)) if crate::routes::org_invites::is_admin(&role) => {}
        Ok(_) => return not_found(),
        Err(e) => return internal("role check", e),
    }
    let creator = {
        let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
            Ok(tx) => tx,
            Err(e) => return internal("scope", e),
        };
        let row = match trial_db::load(&mut tx, account_id).await {
            Ok(r) => r,
            Err(e) => return internal("org_trials read", e),
        };
        let _ = tx.commit().await;
        match row {
            // The trial is keyed on whoever created the org, not on which
            // admin resumes it.
            Some(r) => r.creator_user_id.unwrap_or(user.user_id),
            None => {
                return refuse(
                    StatusCode::CONFLICT,
                    "no_pending_checkout",
                    "this organization has no sign-up waiting for payment",
                )
            }
        }
    };
    match start_org_trial_checkout(&state, account_id, creator, body.seats, &body.success_url, &body.cancel_url).await {
        Ok(resp) => (StatusCode::OK, Json(resp)).into_response(),
        Err(r) => r,
    }
}
