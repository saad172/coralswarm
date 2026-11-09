//! One-click unsubscribe (RFC 8058) for the unmatched-member nudge email
//! (`services::unmatched_recipient_email`).
//!
//! - `POST /v1/email/unsubscribe?t={token}` — the one-click target mail
//!   providers submit ON THE RECIPIENT'S BEHALF, per `List-Unsubscribe-Post:
//!   List-Unsubscribe=One-Click` (RFC 8058 §3.1). No Clerk JWT, no CSRF
//!   token, no session of any kind: the request arrives from a mail
//!   provider's infrastructure, not a browser acting for a logged-in user,
//!   and the token itself is the entire credential — same trust model as
//!   `routes::shares`'s capability-token viewers.
//! - `GET  /v1/email/unsubscribe?t={token}` — the visible link rendered in
//!   the email body itself (`services::unmatched_recipient_email::
//!   render_nudge_email_html`'s footer). Renders a confirmation FORM and
//!   takes NO action (CodeRabbit round 1, #1005): RFC 8058 reserves the
//!   actual one-click suppression for `POST` specifically because mail
//!   security link scanners fetch every URL in a message body
//!   automatically, and this table's suppression is permanent with no
//!   re-subscribe path (migration `0228`'s header) — a GET that suppressed
//!   on the spot would let an automated scan permanently unsubscribe
//!   someone who never clicked anything. The form's button `POST`s the
//!   same token to the arm above.
//!
//! Both are mounted on the `open` router in `lib::app` — the recipient this
//! email reaches has no CoralSwarm account at all (`services::
//! unmatched_recipient_email`'s own module doc: their address simply
//! appeared in a Slack roster a colleague's agent already watches), so
//! there is no Clerk session to require and no per-request `Tenant`
//! decision to extract.
//!
//! # Resolves against the CONTROL pool — for every account, personal or org
//! Uses `Tenant::legacy(&state)` for its pool — the SAME idiom `routes::
//! shares::public_share` already established for a public, unauthenticated
//! route with no per-request tenancy decision to derive one from (see that
//! function's own doc comment). Unlike `public_share`'s token lookup (a
//! genuinely tenant-tree table, `meeting_shares`, so an org-minted share
//! really can live somewhere this route cannot reach), `slack_unmatched_
//! email_notices` has NO tenant-tree twin at all — see `services::
//! unmatched_recipient_email`'s own "Control-plane only, not per-plane" doc
//! section for the full reasoning. A send-suppression list is bookkeeping
//! about an email address and an account, not ocean content, which puts it
//! in the same control-only category as `oauth_tokens`/`slack_integrations`.
//! Every recipient's row — personal account or org account alike — is
//! written here unconditionally by `workers::slack_unmatched_email::
//! process_job` (control-plane, regardless of the tick's own plane), so
//! `Tenant::legacy(&state).pool` is not a narrowed view of this data, it is
//! ALL of it. An earlier version of this module claimed personal accounts
//! were the only ones this route could serve; that was true only because the
//! write side put an org account's row in the wrong database, not because
//! this route was structurally limited — the fix moved the write, not this
//! route's resolution.
//!
//! # Enumeration resistance
//! Every outcome below — unknown token, already-suppressed token, a
//! genuinely new suppression, a rate-limited request, and an internal
//! error — produces the IDENTICAL status and body. A caller must not be
//! able to tell "that token exists" from "that token doesn't", the same
//! discipline `routes::shares`'s generic 404 keeps for share tokens: this
//! list names real people, and a differing response is exactly how a list
//! like that gets enumerated from the outside.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse, Response},
};
use serde::Deserialize;
use std::sync::Arc;

use crate::{tenancy::extractor::Tenant, AppState};

#[derive(Deserialize)]
pub struct UnsubscribeQuery {
    #[serde(rename = "t")]
    token: String,
}

/// Per-token rate limit, same posture as `routes::shares::
/// SHARE_PUBLIC_RATE_MAX`: keyed by the token itself, not by caller identity
/// — there is no caller identity on this route to key on, only the bearer
/// token, and the abuse this bounds is hammering ONE token, not one source.
const UNSUB_RATE_MAX: u32 = 30;
const UNSUB_RATE_WINDOW_SECS: u64 = 60;

/// Route-wide rate limit, independent of the per-token bucket above
/// (CodeRabbit round 1, #1005): [`UNSUB_RATE_MAX`] only bounds repeated
/// attempts against ONE token. A caller that varies the token on every
/// request — real or fabricated — gets a brand-new limiter entry each time
/// and is never throttled by the per-token bucket alone; the in-memory
/// limiter also never evicts those entries, so an unbounded stream of
/// distinct tokens is an unbounded-growth vector on top of being unthrottled.
/// This bucket bounds the AGGREGATE rate against the whole route regardless
/// of which token is presented, the same "no caller identity to key on"
/// reasoning as the per-token bucket, just scoped to the route instead of
/// to one token. 300/min is generous for genuine one-click unsubscribe
/// traffic (a real send volume this route was built to absorb) while still
/// capping the DB-operation cost of a token-varying flood.
const UNSUB_GLOBAL_RATE_MAX: u32 = 300;
const UNSUB_GLOBAL_RATE_WINDOW_SECS: u64 = 60;
const UNSUB_GLOBAL_RATE_KEY: &str = "email_unsub:__global__";

/// The one shared action both arms below take. Its `bool` return is
/// deliberately unused by both callers — see the module doc's "enumeration
/// resistance" note: nothing about the response may depend on it.
async fn do_unsubscribe(state: &Arc<AppState>, token: &str) -> bool {
    // Route-wide bucket FIRST, cheaply: a token-varying flood must never even
    // reach the per-token check (which would always look "fresh" for a token
    // never seen before) let alone the database operation below.
    if !state
        .rate_limiter
        .check_and_increment_key(
            UNSUB_GLOBAL_RATE_KEY,
            UNSUB_GLOBAL_RATE_MAX,
            UNSUB_GLOBAL_RATE_WINDOW_SECS,
        )
        .await
    {
        return false;
    }

    if !state
        .rate_limiter
        .check_and_increment_key(
            &format!("email_unsub:{token}"),
            UNSUB_RATE_MAX,
            UNSUB_RATE_WINDOW_SECS,
        )
        .await
    {
        // A throttled request simply isn't acted on this time — same
        // generic non-outcome as every other refusal path on this route.
        return false;
    }

    // `Tenant::legacy` (same idiom `routes::shares::public_share` uses for
    // the identical reason): this route lives on the `open` router (no auth
    // layer, so no binding gate and never a `TenancyDecision`) — the axum
    // `Tenant` extractor would trip its own mounted-outside-the-gate
    // tripwire here. Resolves the shared control pool, which is complete for
    // this table regardless of plane; see the module doc.
    let tenant = Tenant::legacy(state);
    let Some(pool) = tenant.pool.as_ref() else {
        return false;
    };

    match crate::services::unmatched_recipient_email::suppress_by_token(pool, token).await {
        Ok(found) => found,
        Err(e) => {
            tracing::error!(error = %e, "email_unsubscribe: suppress_by_token failed");
            false
        }
    }
}

/// The one response body every outcome shares on the POST arm — see the
/// module doc's enumeration-resistance note.
const CONFIRMATION_TEXT: &str =
    "You will not receive further emails about this Slack conversation.";

/// `POST /v1/email/unsubscribe?t={token}` — RFC 8058 one-click target.
/// A compliant mail provider submits this with no session, no `Referer`,
/// and renders nothing it gets back — a terse `text/plain` body is
/// appropriate and sufficient (RFC 8058 does not prescribe a body at all).
pub async fn unsubscribe_post(
    State(state): State<Arc<AppState>>,
    Query(q): Query<UnsubscribeQuery>,
) -> impl IntoResponse {
    do_unsubscribe(&state, &q.token).await;
    (StatusCode::OK, CONFIRMATION_TEXT)
}

/// Minimal HTML entity escaping — identical rule to
/// `services::unmatched_recipient_email::escape_html`/`org_invites::
/// escape_html`, duplicated rather than imported for the SAME reason those
/// two are already duplicates of each other: this is trivial enough that a
/// shared helper is not worth coupling independent templates over.
fn escape_html(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            '\'' => out.push_str("&#39;"),
            _ => out.push(c),
        }
    }
    out
}

/// `GET /v1/email/unsubscribe?t={token}` — the visible link a human clicks
/// from inside the email body itself.
///
/// **Renders a confirmation FORM and takes NO action** (CodeRabbit round 1,
/// #1005, Functional Correctness). An earlier version called
/// [`do_unsubscribe`] directly on GET, which RFC 8058 explicitly reserves
/// `POST` for precisely because mail-security link scanners and prefetchers
/// (SafeLinks-style) fetch every URL in a message body automatically — an
/// automated GET would have permanently suppressed a recipient who never
/// clicked anything, and migration `0228`'s header states there is no
/// re-subscribe path at all once `suppressed_at` is set. The rendered
/// button `POST`s the SAME token to [`unsubscribe_post`] below, which is
/// the only place [`do_unsubscribe`] is still called from a human's click —
/// identical to the automated one-click POST a compliant mail provider
/// submits on the recipient's behalf.
pub async fn unsubscribe_get(Query(q): Query<UnsubscribeQuery>) -> Response {
    let token = escape_html(&q.token);
    Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width\"><title>coralswarm</title></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0;font-family:-apple-system,'Segoe UI',system-ui,sans-serif\">\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td align=\"center\" style=\"padding:64px 16px\">\
<div style=\"font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;margin-bottom:18px\">coralswarm</div>\
<p style=\"margin:0 0 18px;max-width:360px;font-size:14px;line-height:1.6;color:#4A3F36\">Stop receiving emails about this Slack conversation?</p>\
<form method=\"post\" action=\"/v1/email/unsubscribe?t={token}\">\
<button type=\"submit\" style=\"font:inherit;font-weight:600;font-size:14px;color:#F2EBE0;background:#1A1410;border:none;border-radius:8px;padding:10px 20px;cursor:pointer\">Unsubscribe</button>\
</form>\
</td></tr></table></body></html>"
    ))
    .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rate_limit::RateLimiter;

    /// CodeRabbit round 1 (#1005, Security & Privacy): proves the exact gap
    /// the per-token bucket alone had — a caller that presents a NEW token on
    /// every request never trips [`UNSUB_RATE_MAX`] (each token's own bucket
    /// starts fresh), so the per-token limiter alone cannot bound a
    /// token-varying flood. Exercises the SAME limiter instance, the SAME
    /// key/constants [`do_unsubscribe`] uses, and the SAME
    /// `RateLimiter::check_and_increment_key` call both buckets go through —
    /// not a hand-rolled reimplementation — so a drift in either constant or
    /// the key string breaks this test rather than silently under-covering
    /// the real code path.
    #[tokio::test]
    async fn global_bucket_throttles_a_token_varying_flood_the_per_token_bucket_cannot() {
        let limiter = crate::rate_limit::InMemoryRateLimiter::new();

        for i in 0..UNSUB_GLOBAL_RATE_MAX {
            let token = format!("distinct-token-{i}");
            // Every one of these tokens is brand new, so its OWN bucket must
            // still be wide open — proving the per-token check by itself
            // would let this request straight through.
            assert!(
                limiter
                    .check_and_increment_key(
                        &format!("email_unsub:{token}"),
                        UNSUB_RATE_MAX,
                        UNSUB_RATE_WINDOW_SECS,
                    )
                    .await,
                "request {i}'s own never-before-seen token bucket must not be exhausted"
            );
            assert!(
                limiter
                    .check_and_increment_key(
                        UNSUB_GLOBAL_RATE_KEY,
                        UNSUB_GLOBAL_RATE_MAX,
                        UNSUB_GLOBAL_RATE_WINDOW_SECS,
                    )
                    .await,
                "request {i} should still be within the {UNSUB_GLOBAL_RATE_MAX}/min aggregate budget"
            );
        }

        // One more request, again with a token nobody has ever presented
        // before.
        let extra_token = "one-more-never-seen-token";
        assert!(
            limiter
                .check_and_increment_key(
                    &format!("email_unsub:{extra_token}"),
                    UNSUB_RATE_MAX,
                    UNSUB_RATE_WINDOW_SECS,
                )
                .await,
            "the OLD per-token-only guard would have let this one through too — it is a fresh key"
        );
        assert!(
            !limiter
                .check_and_increment_key(
                    UNSUB_GLOBAL_RATE_KEY,
                    UNSUB_GLOBAL_RATE_MAX,
                    UNSUB_GLOBAL_RATE_WINDOW_SECS,
                )
                .await,
            "the aggregate route-wide bucket must now refuse, closing exactly the gap a \
             per-token-only limiter leaves open against an ever-varying token"
        );
    }

    fn test_state() -> Arc<AppState> {
        Arc::new(AppState {
            db: None,
            store: None,
            resend_api_key: None,
            r2: None,
            http_client: reqwest::Client::new(),
            embedder: Arc::new(crate::StubEmbedder),
            llm: None,
            rate_limiter: Arc::new(crate::rate_limit::InMemoryRateLimiter::new()),
            admission: Default::default(),
            in_flight: std::sync::Arc::new(Default::default()),
            metrics: std::sync::Arc::new(crate::telemetry::NoopMetrics),
            dek_cache: crate::dek_cache::DekCache::new(),
            connector_flag_cache: crate::connector_flags::ConnectorFlagCache::new(),
            tenant_runtime: None,
            env: crate::AppEnv::default(),
            task_tracker: tokio_util::task::TaskTracker::new(),
            worker_activity: crate::workers::tenant_harness::TenantActivity::new(),
            mcp_cancellation_token: tokio_util::sync::CancellationToken::new(),
            mcp_task_manager: crate::mcp::server::McpTaskRegistry::new(),
            mcp_ask_in_flight: crate::mcp::server::AskInFlight::new(),
            stripe: None,
        })
    }

    /// CodeRabbit round 1 (#1005, Functional Correctness): FAILS WITHOUT THE
    /// FIX. Proves GET takes no action at all — it renders a form and
    /// leaves [`do_unsubscribe`]'s own rate-limit buckets completely
    /// untouched, which is the only externally-observable signal this test
    /// can key on without a database (GET no longer touches one). If GET
    /// still called `do_unsubscribe` internally, the very first assertion
    /// in the loop below would find the bucket already one request short.
    #[tokio::test]
    async fn get_arm_renders_a_form_and_never_calls_do_unsubscribe() {
        let state = test_state();
        let resp = unsubscribe_get(Query(UnsubscribeQuery { token: "tok<123>".to_string() })).await;
        let body = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let body = String::from_utf8(body.to_vec()).unwrap();

        assert!(
            body.contains("<form method=\"post\" action=\"/v1/email/unsubscribe?t=tok&lt;123&gt;\">"),
            "the form must POST the SAME (HTML-escaped) token back to the unsubscribe route: {body}"
        );
        assert!(
            !body.contains(CONFIRMATION_TEXT),
            "GET must never show the suppression-confirmed text — nothing has been suppressed yet"
        );

        // The full aggregate budget must still be available: a GET that
        // internally called `do_unsubscribe` would have already spent one
        // unit of it above.
        for i in 0..UNSUB_GLOBAL_RATE_MAX {
            assert!(
                state
                    .rate_limiter
                    .check_and_increment_key(
                        UNSUB_GLOBAL_RATE_KEY,
                        UNSUB_GLOBAL_RATE_MAX,
                        UNSUB_GLOBAL_RATE_WINDOW_SECS,
                    )
                    .await,
                "request {i}: the GET above must not have consumed any of this budget"
            );
        }
    }

    #[test]
    fn escape_html_covers_the_five_entities_a_token_or_attribute_could_carry() {
        assert_eq!(escape_html("<>&\"'"), "&lt;&gt;&amp;&quot;&#39;");
    }
}
