//! Resolve a signup-prefill token to an email address (CodeRabbit round 1,
//! #1005, Security & Privacy).
//!
//! - `GET /v1/email/prefill?t={token}` — called by `web/app/signup/page.tsx`
//!   on mount, BEFORE rendering Clerk's `<SignUp>`. No Clerk JWT, no
//!   session: the caller is a fresh visitor who just clicked a link in the
//!   unmatched-recipient nudge email
//!   (`services::unmatched_recipient_email::send_nudge_email`) and has no
//!   CoralSwarm account yet, the same trust model
//!   `routes::email_unsubscribe`'s two arms already document.
//!
//! # Why this route exists at all
//! An earlier version of the nudge email carried the recipient's address as
//! a plain `?email=` query parameter on the signup link. Browser history,
//! proxy access logs, analytics, and `Referer` headers can all retain a URL
//! query parameter — none of them should ever see a real person's email
//! address. `?pt=` on the link is an opaque, unguessable, DB-backed token
//! instead (`services::unmatched_recipient_email::claim_notice_slot`'s
//! `signup_prefill_token`, migration `0229`); this route is the ONLY thing
//! that can turn it back into an email, and it does so over a POST-free GET
//! whose response is read by client-side JS, never rendered into a second
//! URL.
//!
//! # Resolves against the CONTROL pool — for every account, personal or org
//! Same idiom, same reasoning, and the same underlying table as
//! `routes::email_unsubscribe` — see that module's own doc comment. This
//! table has no tenant-tree twin at all, so `Tenant::legacy(&state).pool` is
//! not a narrowed view of the data, it is all of it.
//!
//! # Enumeration resistance
//! Every outcome — unknown token, expired-by-deletion token, a rate-limited
//! request, and an internal error — returns the SAME `{ "email": null }`
//! shape at `200 OK`. A caller must not be able to tell "that token exists"
//! from "that token doesn't" any more than `routes::email_unsubscribe`'s own
//! callers can.

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Json, Response},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::AppState;

#[derive(Deserialize)]
pub struct PrefillQuery {
    #[serde(rename = "t")]
    token: String,
}

#[derive(Serialize)]
pub struct PrefillResponse {
    email: Option<String>,
}

/// Per-token rate limit, same posture as `routes::email_unsubscribe`'s own
/// `UNSUB_RATE_MAX` — keyed by the token itself, there being no caller
/// identity on this route to key on either.
const PREFILL_RATE_MAX: u32 = 30;
const PREFILL_RATE_WINDOW_SECS: u64 = 60;

/// Route-wide bucket, same reasoning as `routes::email_unsubscribe`'s
/// `UNSUB_GLOBAL_RATE_MAX` (CodeRabbit round 1, #1005, applied here from the
/// start rather than added after the fact): a caller varying the token on
/// every request would otherwise find each one's own bucket freshly open.
const PREFILL_GLOBAL_RATE_MAX: u32 = 300;
const PREFILL_GLOBAL_RATE_WINDOW_SECS: u64 = 60;
const PREFILL_GLOBAL_RATE_KEY: &str = "email_prefill:__global__";

/// `GET /v1/email/prefill?t={token}` — see the module doc for the full
/// trust model and enumeration-resistance discipline.
pub async fn resolve_prefill(
    State(state): State<Arc<AppState>>,
    Query(q): Query<PrefillQuery>,
) -> Response {
    let email = resolve(&state, &q.token).await;
    Json(PrefillResponse { email }).into_response()
}

async fn resolve(state: &Arc<AppState>, token: &str) -> Option<String> {
    if !state
        .rate_limiter
        .check_and_increment_key(
            PREFILL_GLOBAL_RATE_KEY,
            PREFILL_GLOBAL_RATE_MAX,
            PREFILL_GLOBAL_RATE_WINDOW_SECS,
        )
        .await
    {
        return None;
    }
    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("email_prefill:{token}"),
            PREFILL_RATE_MAX,
            PREFILL_RATE_WINDOW_SECS,
        )
        .await
    {
        return None;
    }

    // Public token lookup is always control-plane; its read executor carries
    // the open route's PageRead budget without manufacturing an Other Tenant.
    let pool = state.control_db()?;

    match crate::services::unmatched_recipient_email::resolve_prefill_token(pool, token).await {
        Ok(email) => email,
        Err(e) => {
            tracing::error!(error = %e, "email_prefill: resolve_prefill_token failed");
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::RateLimiter;

    /// Same behavioural property as `routes::email_unsubscribe`'s own
    /// global-bucket test, pinned here too since this route shipped the
    /// aggregate bucket from its first commit rather than as a later fix —
    /// proves a token-varying flood cannot bypass it via a per-token-only
    /// read.
    #[tokio::test]
    async fn global_bucket_throttles_a_token_varying_flood() {
        let limiter = crate::rate_limit::InMemoryRateLimiter::new();

        for i in 0..PREFILL_GLOBAL_RATE_MAX {
            let token = format!("distinct-prefill-token-{i}");
            assert!(
                limiter
                    .check_and_increment_key(
                        &format!("email_prefill:{token}"),
                        PREFILL_RATE_MAX,
                        PREFILL_RATE_WINDOW_SECS,
                    )
                    .await,
                "request {i}'s own never-before-seen token bucket must not be exhausted"
            );
            assert!(
                limiter
                    .check_and_increment_key(
                        PREFILL_GLOBAL_RATE_KEY,
                        PREFILL_GLOBAL_RATE_MAX,
                        PREFILL_GLOBAL_RATE_WINDOW_SECS,
                    )
                    .await,
                "request {i} should still be within the {PREFILL_GLOBAL_RATE_MAX}/min aggregate budget"
            );
        }

        assert!(
            !limiter
                .check_and_increment_key(
                    PREFILL_GLOBAL_RATE_KEY,
                    PREFILL_GLOBAL_RATE_MAX,
                    PREFILL_GLOBAL_RATE_WINDOW_SECS,
                )
                .await,
            "the aggregate route-wide bucket must refuse once exhausted, regardless of token"
        );
    }
}
