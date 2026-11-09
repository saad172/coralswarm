//! Org-invite lifecycle (org-invites train, PR 1 of the backend):
//! - `POST   /v1/orgs/invites`               — invite a member (pre-creates
//!   the Clerk user + org membership; no Clerk-native invitations).
//! - `POST   /v1/orgs/invites/{user_id}/resend` — re-send the invite email.
//! - `DELETE /v1/orgs/invites/{user_id}`     — revoke (delete the Clerk
//!   membership; GC the ghost Clerk user if never signed in and orphaned).
//!
//! See `.scratch/org-invites/plan.md` for the settled design this
//! productizes — decisions there are NOT re-litigated here. Short version:
//! Clerk stays the identity store, but invitations are our own — the prod
//! Clerk instance has native invitations disabled (`invitations_not_supported`).
//! "Invite" = immediately pre-creating a Clerk user (no password) + an org
//! membership; "pending" is DERIVED from Clerk's `last_sign_in_at IS NULL`,
//! never a table of our own. Invited-by/invited-at live in the Clerk
//! membership's `public_metadata`.
//!
//! # The two-webhook race (why the synchronous `ensure_org_membership` call
//! below is safe even though `organizationMembership.created` also fires)
//!
//! `create_invite` calls [`crate::clerk_provision::ensure_org_membership`]
//! SYNCHRONOUSLY (after creating the Clerk membership) so a seat-purchase
//! failure is visible in THIS response, not only in server logs. But Clerk
//! also delivers `organizationMembership.created` for the very membership
//! this handler just created — asynchronously, and with NO ordering
//! guarantee relative to this request. Two things make that safe:
//!   1. `ensure_org_membership` itself is idempotent — see its own doc
//!      comment's "Retry gate": the second caller (whichever of this
//!      handler / the webhook runs second) finds the membership row already
//!      present and the seat already stamped `seat_purchased_at`, so
//!      `acquire_seat` is either skipped (no `newly_inserted` and no
//!      `is_created_event`-with-still-`NULL` case) or itself a no-op.
//!   2. If THIS handler's `ensure_org_membership` call fails after the
//!      Clerk membership was already created (a genuine Stripe failure) and
//!      this handler unwinds by deleting that Clerk membership, a webhook
//!      delivery that raced in BETWEEN the create and the delete could
//!      itself purchase a seat that then gets immediately orphaned by the
//!      delete. That is not silently lost: deleting the Clerk membership is
//!      exactly what fires `organizationMembership.deleted`, whose handler
//!      (`clerk_provision::remove_org_membership`) releases whatever seat
//!      is on record — so the sequence converges to "no seat held" either
//!      way, just possibly via one extra acquire+release round trip.
//!
//! # The revoke-then-quick-re-invite race (a second ordering hazard, on the
//! DELETE side this time)
//!
//! Clerk webhook deliveries carry no ordering key. `revoke_invite` deletes
//! the Clerk org membership synchronously (this endpoint), which fires
//! `organizationMembership.deleted` asynchronously — same as any other
//! membership delete. If an admin revokes a member and then quickly
//! RE-INVITES the same email before that `.deleted` delivery lands, the
//! stale delivery can arrive AFTER the re-invite already recreated the
//! membership — and `routes::clerk_webhook::handle_membership_deleted`
//! would delete the FRESH membership and release the FRESH seat it never
//! actually applied to, based on a webhook event about a membership that no
//! longer describes reality.
//!
//! The fix lives in `clerk_webhook.rs`, not here: `handle_membership_deleted`
//! best-effort checks (via `ClerkAdminClient::user_has_org_membership`)
//! whether Clerk STILL reports this user as a member of this org right now;
//! if so, the `.deleted` delivery is stale and is skipped. If the check
//! itself fails (no Clerk key configured, transport error, ...), it fails
//! toward TODAY's behavior — the delete proceeds — same "fail toward
//! current behavior on an unrelated Clerk-lookup failure" posture the
//! create-side race above doesn't need (it has no equivalent lookup) but
//! that this guard does.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::billing::seat_lifecycle::SeatPurchasePolicy;
use crate::clerk_admin::{ClerkAdminClient, ClerkAdminError, CreatedUser};
use crate::clerk_provision::ProvisionError;
use crate::middleware::auth::UserContext;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenantContext;
use crate::AppState;

// ── Small response/error helpers (module convention — see `escrow.rs`,
//    `checkout.rs`; this crate has no central `ApiError`) ────────────────────

/// `pub(crate)` so `routes::teams::list_org_members` can gate its
/// org-invites-train enrichment fields (`pending`/`invited_by`/
/// `invited_at`) on the SAME admin check this module's own endpoints use,
/// rather than reimplementing it (org-invites train PR 1 review, finding 9).
pub(crate) fn is_admin(role: &str) -> bool {
    // Settings launch PR 2.8: no admin/owner authority for a non-interactive
    // credential — see `middleware::auth::authority_allowed`.
    (role == "admin" || role == "owner") && crate::middleware::auth::authority_allowed()
}

fn unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}
fn bad(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn not_found(msg: &str) -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": msg}))).into_response()
}
fn conflict(msg: &str) -> Response {
    (StatusCode::CONFLICT, Json(json!({"error": msg}))).into_response()
}
/// 409 `no_free_seat`: the caller sent `allow_purchase: false` and the org
/// has no free seat for this invite. Nothing was bought. The web refetches
/// the seat count and shows the quote → confirm dialog.
fn no_free_seat() -> Response {
    (
        StatusCode::CONFLICT,
        Json(json!({"error": "no free seat", "code": "no_free_seat"})),
    )
        .into_response()
}
fn internal(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org_invites: failed");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({"error": "internal error"})),
    )
        .into_response()
}

/// Org context required + admin required (this module's own gate, same bar
/// as `escrow.rs`'s `is_admin` — `UserContext.role` is the live DB role, see
/// `middleware::auth::UserContext`'s own doc comment).
///
/// `pub(crate)`: reused by `routes::org_slots` (CS-435) for the identical
/// admin bar — the "buy N slots"/pool-read routes are org-admin-gated the
/// same way this module's own invite routes are, and this module's own
/// `TenantContext`-returning shape is exactly what that route needs.
pub(crate) fn require_org_admin<'a>(
    tenant: &'a Tenant,
    user: &UserContext,
) -> Result<&'a TenantContext, Response> {
    let Some(ctx) = tenant.org_context() else {
        return Err(forbidden(
            "this endpoint is only available in an organization workspace",
        ));
    };
    if !is_admin(&user.role) {
        return Err(forbidden("admin only"));
    }
    Ok(ctx)
}

/// Map a [`ClerkAdminError`] to a response:
///   - not configured ⇒ 503
///   - Clerk 401/403 ⇒ 503 `clerk_misconfigured` — a stale/rotated
///     `CLERK_SECRET_KEY` must NEVER surface as a plain 401 to a validly
///     authenticated admin caller: `web`'s 401 interceptor treats any 401 as
///     "your session expired" and triggers a spurious token refresh/redirect
///     loop for an error that has nothing to do with the caller's own auth
///     (org-invites train PR 1 review, finding 4)
///   - Clerk 429 or 5xx ⇒ 502 (transient — the caller can retry)
///   - any other Clerk 4xx (e.g. 422) ⇒ the Clerk status code passed through
///     with its structured `code`
///   - transport/parse failures ⇒ a generic 500 (never leak a raw transport
///     error string)
///
/// The raw `message` is logged server-side ONLY, never put in the response
/// body: `ClerkAdminClient::send`'s doc comment notes a non-JSON error body
/// (a CDN/proxy 502/503 HTML page, say) falls back to raw response text as
/// `message` — echoing that straight to the caller would leak upstream
/// infrastructure detail through our own API. Only Clerk's own structured
/// `errors[0].code` (present for a real Clerk-shaped error, `None`
/// otherwise) reaches the response.
pub(crate) fn map_clerk_error(step: &str, e: ClerkAdminError) -> Response {
    match &e {
        ClerkAdminError::NotConfigured => unavailable(),
        ClerkAdminError::Api {
            status,
            code,
            message,
        } if *status == 401 || *status == 403 => {
            tracing::error!(
                status, ?code, %message, step,
                "org_invites: Clerk rejected our API key — treating as misconfigured rather than \
                 surfacing a 401/403 that would look like the CALLER's own auth failed"
            );
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "clerk_misconfigured"})),
            )
                .into_response()
        }
        ClerkAdminError::Api {
            status,
            code,
            message,
        } if *status == 429 || *status >= 500 => {
            tracing::error!(status, ?code, %message, step, "org_invites: Clerk API server/rate-limit error");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "clerk_error", "code": code})),
            )
                .into_response()
        }
        ClerkAdminError::Api {
            status,
            code,
            message,
        } => {
            tracing::warn!(status, ?code, %message, step, "org_invites: Clerk API rejected the request");
            let sc = StatusCode::from_u16(*status).unwrap_or(StatusCode::BAD_REQUEST);
            (sc, Json(json!({"error": "clerk_error", "code": code}))).into_response()
        }
        ClerkAdminError::Transport(_) | ClerkAdminError::Parse(_) => internal(step, &e),
    }
}

/// Short, greppable class for a [`crate::billing::stripe_client::StripeError`]
/// — never the raw Stripe body (which can carry card-holder-identifying
/// text) in the invite endpoint's response.
fn classify_stripe_error(e: &crate::billing::stripe_client::StripeError) -> &'static str {
    use crate::billing::stripe_client::StripeError as SE;
    match e {
        SE::NotConfigured => "not_configured",
        SE::Transport { .. } => "transport_error",
        SE::Timeout => "timeout",
        SE::Auth { .. } => "auth_error",
        SE::RateLimited { .. } => "rate_limited",
        SE::IdempotencyConflict { .. } => "idempotency_conflict",
        SE::CardDeclined { .. } => "card_declined",
        SE::Api { .. } => "api_error",
        SE::Parse(_) => "parse_error",
        _ => "stripe_error",
    }
}

/// Map a seat-purchase [`crate::billing::stripe_client::StripeError`] to a
/// response. Every variant is matched EXPLICITLY (org-invites train PR 1
/// review, finding 7 — the previous version collapsed every single
/// `StripeError` variant, including a plain Stripe 5xx/timeout/transport
/// blip that has nothing to do with the invitee's card, into a blanket 402
/// `seat_purchase_failed`). The unwind itself is unchanged — this only fixes
/// the STATUS/body the caller sees:
///   - [`SE::CardDeclined`] / [`SE::Api`] with a 4xx status ⇒ 402
///     `seat_purchase_failed` (a real payment problem — this IS the
///     invitee's/org's fault, worth surfacing distinctly)
///   - [`SE::Transport`] / [`SE::Timeout`] / [`SE::RateLimited`] /
///     [`SE::Parse`] / any other [`SE::Api`] (5xx) ⇒ 502 `billing_unavailable`
///     (Stripe-side/network trouble — retryable, not the invitee's fault)
///   - [`SE::NotConfigured`] / [`SE::Auth`] ⇒ 503 `billing_unavailable` (OUR
///     Stripe configuration is broken — same "misconfigured, don't blame the
///     caller" posture as finding 4's Clerk 401/403 mapping)
///   - [`SE::IdempotencyConflict`] / [`SE::ArchivedLookupKey`] ⇒ a generic
///     500 — both indicate a bug in OUR idempotency-key recipe or Stripe
///     catalog, never transient and never the invitee's fault to retry
///
/// `pub(crate)`: reused by `routes::org_slots` (CS-435) for its own "buy N
/// slots" Stripe push — the same classification applies verbatim (a Stripe
/// failure buying slots ahead of anyone joining is exactly as much "the
/// org's own fault, or transient upstream trouble" as a seat purchase is).
pub(crate) fn map_stripe_error_to_response(e: crate::billing::stripe_client::StripeError) -> Response {
    use crate::billing::stripe_client::StripeError as SE;
    let detail = classify_stripe_error(&e);
    match &e {
        SE::CardDeclined { .. } => (
            StatusCode::PAYMENT_REQUIRED,
            Json(json!({"error": "seat_purchase_failed", "detail": detail})),
        )
            .into_response(),
        SE::Api { status, .. } if (400..500).contains(status) => (
            StatusCode::PAYMENT_REQUIRED,
            Json(json!({"error": "seat_purchase_failed", "detail": detail})),
        )
            .into_response(),
        SE::Transport { .. }
        | SE::Timeout
        | SE::RateLimited { .. }
        | SE::Parse(_)
        | SE::Api { .. } => {
            tracing::error!(error = %e, "org_invites: seat purchase failed with a transient/upstream Stripe error");
            (
                StatusCode::BAD_GATEWAY,
                Json(json!({"error": "billing_unavailable"})),
            )
                .into_response()
        }
        SE::NotConfigured | SE::Auth { .. } => {
            tracing::error!(error = %e, "org_invites: seat purchase failed — Stripe is not configured, or our key was rejected");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "billing_unavailable"})),
            )
                .into_response()
        }
        SE::IdempotencyConflict { .. } | SE::ArchivedLookupKey { .. } => {
            internal("ensure_org_membership: seat purchase", &e)
        }
    }
}

/// Derive `InviteResponse.pending` (finding 10, org-invites train PR 1
/// review) instead of hardcoding it: true for a genuinely NEW Clerk user
/// (`newly_created`), and also true when we REUSED an existing Clerk
/// identity (`create_user`'s `form_identifier_exists` fallback) that has
/// never actually signed in — i.e. re-inviting a ghost from an earlier,
/// never-accepted invite is still "pending", not silently `false`.
fn derive_pending(created: &CreatedUser) -> bool {
    created.newly_created || created.last_sign_in_at.is_none()
}

fn valid_email(email: &str) -> bool {
    let email = email.trim();
    !email.is_empty()
        && !email.contains(char::is_whitespace)
        && email.split('@').count() == 2
        && email.split('@').all(|p| !p.is_empty())
}

/// `"member"` -> `(bare RBAC role, Clerk-namespaced role)`; `"admin"` ->
/// same. Any other value is rejected (400).
pub(crate) fn map_role(role: &str) -> Option<(&'static str, &'static str)> {
    match role {
        "member" => Some(("member", "org:member")),
        "admin" => Some(("admin", "org:admin")),
        _ => None,
    }
}

/// Naive split of an optional display name into Clerk's `first_name`/
/// `last_name` — good enough for a Clerk profile seed; the user can correct
/// it after signing in (`users.name` sync path, PR 2).
fn split_name(name: &str) -> (Option<String>, Option<String>) {
    let name = name.trim();
    if name.is_empty() {
        return (None, None);
    }
    match name.split_once(' ') {
        Some((first, rest)) => {
            let rest = rest.trim();
            (
                Some(first.to_string()),
                (!rest.is_empty()).then(|| rest.to_string()),
            )
        }
        None => (Some(name.to_string()), None),
    }
}

/// The domain half of an email address, for logging — never the full
/// address (org-invites train PR 1 review, PII finding). Falls back to the
/// whole input on a malformed address rather than panicking; this is a log
/// field, not a validity check (validation already happened in
/// [`valid_email`]).
fn email_domain(email: &str) -> &str {
    email.split('@').nth(1).unwrap_or(email)
}

fn resend_base_url() -> String {
    std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".to_string())
}

fn build_join_url(
    web_base: &str,
    email: &str,
    org_name: Option<&str>,
    inviter_name: Option<&str>,
) -> String {
    let mut url = format!("{web_base}/join?email={}", urlencoding::encode(email));
    if let Some(org) = org_name {
        url.push_str(&format!("&org={}", urlencoding::encode(org)));
    }
    if let Some(inviter) = inviter_name {
        url.push_str(&format!("&inviter={}", urlencoding::encode(inviter)));
    }
    url
}

// ── Invite email template (org-invites train PR 3) ──────────────────────────
//
// Brand-tokened HTML mirroring the settled mock
// (`.scratch/org-invites/design/InviteEmail.dc.html` / the "Org Invite Flow"
// canvas): cream document palette per the brand skill's email rules — NOT the
// live-web cool neutrals. Email-client constraints shape everything here:
// table layout, every style inline, no webfont loads (fallback stacks only —
// Georgia for the serif display voice, system sans for body, Consolas/mono
// for provenance), and the CTA is a bgcolor'd `<td>` wrapping the `<a>` so
// Outlook's Word engine still paints a button.

/// Minimal HTML entity escaping for text/attribute interpolation. Every
/// dynamic value rendered into [`render_invite_email_html`] MUST pass through
/// this — including `join_url`, whose `&org=`/`&inviter=` query separators
/// need to be `&amp;` inside an `href` attribute anyway.
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

/// Rust mirror of `web/app/onboarding/join/page.tsx`'s `safeLabel` (PR 2's
/// CodeRabbit round): strip anything outside a conservative unicode allowlist
/// (`\p{L}\p{N} .,'&-`), trim, cap at 48 chars; empty-after-stripping reads
/// as absent. Applied to org/inviter names before they render into email
/// copy so the email applies the SAME sanitization family as `/join`. Close
/// mirror, not bit-identical: `is_alphanumeric()`/scalar-count `take(48)`
/// differ from JS `\p{L}\p{N}`/UTF-16 `slice(0, 48)` at Unicode edges
/// (combining marks, astral-plane chars) — acceptable for display labels.
/// Nor is the rendered COPY guaranteed identical: `/join` additionally
/// prefers the host-derived slug over the `org` param for its org label
/// (anti-spoofing), so the email's Clerk display name can differ from what
/// the landing page titles itself.
fn safe_label(raw: Option<&str>) -> Option<String> {
    let raw = raw?;
    let cleaned: String = raw
        .chars()
        .filter(|c| c.is_alphanumeric() || matches!(c, ' ' | '.' | ',' | '\'' | '&' | '-'))
        .collect();
    let cleaned: String = cleaned.trim().chars().take(48).collect();
    let cleaned = cleaned.trim();
    (!cleaned.is_empty()).then(|| cleaned.to_string())
}

/// `https://acme.coralswarm.example` → `acme.coralswarm.example`, for the
/// "signs you in at <host>" provenance line. HOST only — a path-bearing base
/// (a pre-cutover `FRONTEND_URL` with a path, say) must not leak its path
/// into the provenance line (review round: host_display finding).
fn host_display(web_base: &str) -> &str {
    let no_scheme = web_base
        .strip_prefix("https://")
        .or_else(|| web_base.strip_prefix("http://"))
        .unwrap_or(web_base);
    no_scheme.split('/').next().unwrap_or(no_scheme)
}

/// An inviter label fit for DISPLAY. [`inviter_name`]'s `COALESCE(name,
/// email)` fallback means a nameless inviter yields their raw email address,
/// and [`safe_label`]'s allowlist strips `@` — which would render mangled
/// copy like "Alexgmail.com invited you" in the subject, heading, and footer
/// (review round: inviter-email-fallback finding). An email-shaped label is
/// dropped entirely instead, degrading to the inviter-absent copy — and it
/// is dropped from the `/join` URL's `inviter` param too (same call sites),
/// where the landing page would mangle it the same way.
fn display_inviter(inviter: Option<&str>) -> Option<&str> {
    inviter.filter(|n| !n.contains('@'))
}

/// The invite email's dynamic copy — subject (plain text), heading (HTML,
/// one italic brand-colored word max per the brand rules: the org name when
/// present, the word "invited" otherwise), and the footer's "because X added
/// you to Y" clause (HTML). ONE fallback matrix for all three surfaces
/// (review round: triplicated-fallback finding); `org`/`inviter` must
/// already be [`safe_label`]ed, and HTML-escaping happens here.
struct InviteCopy {
    subject: String,
    heading_html: String,
    footer_html: String,
}

fn invite_copy(org: Option<&str>, inviter: Option<&str>) -> InviteCopy {
    let em = |word: &str| format!("<em style=\"font-style:italic;color:#D14A38\">{word}</em>");
    let (subject, heading_html) = match (inviter, org) {
        (Some(i), Some(o)) => (
            format!("{i} invited you to {o}"),
            format!("{} invited you to {}", escape_html(i), em(&escape_html(o))),
        ),
        (None, Some(o)) => (
            format!("You're invited to {o}"),
            format!("You're invited to {}", em(&escape_html(o))),
        ),
        (Some(i), None) => (
            format!("{i} invited you to their coralswarm organization"),
            format!("{} {} you to their organization", escape_html(i), em("invited")),
        ),
        (None, None) => (
            "You've been invited to a coralswarm organization".to_string(),
            format!("You've been {}", em("invited")),
        ),
    };
    let footer_html = format!(
        "because {} added you to {}",
        inviter.map(escape_html).unwrap_or_else(|| "an admin".to_string()),
        org.map(|o| format!("the {} organization", escape_html(o)))
            .unwrap_or_else(|| "their organization".to_string()),
    );
    InviteCopy {
        subject,
        heading_html,
        footer_html,
    }
}

/// The wordmark's DotMandala, from the repo's GENERATED brand-mark asset
/// (`extensions/outlook-addin/scripts/build-mark.js` emits it from the
/// canonical ring spec; a `!extensions/outlook-addin/assets/brand-mark.svg`
/// negation in `.dockerignore` keeps it in the backend build context) —
/// never a hand-transcribed copy, so a future mark tweak regenerates every
/// surface including this one (review round: mandala-drift finding). The
/// email-specific sizing/alignment attributes are injected here. Clients
/// that strip `<svg>` (Gmail, Outlook/Word) degrade to the plain-text
/// `coralswarm` wordmark with no gap, which is the accepted fallback.
fn mandala_svg_inline() -> String {
    const RAW: &str = include_str!("../../../extensions/outlook-addin/assets/brand-mark.svg");
    RAW.trim_end().replacen(
        "<svg ",
        "<svg width=\"15\" height=\"15\" aria-hidden=\"true\" style=\"vertical-align:-2px;margin:0 1px\" ",
        1,
    )
}

const FONT_SANS: &str = "Geist,-apple-system,'Segoe UI',system-ui,sans-serif";
const FONT_SERIF: &str = "'Instrument Serif',Georgia,'Times New Roman',serif";
const FONT_MONO: &str = "'JetBrains Mono',ui-monospace,Consolas,monospace";

/// Render the invite email body. `org`/`inviter` must already be
/// [`safe_label`]ed; `to_email` and `join_url` are escaped here (org/inviter
/// escaping happens inside [`invite_copy`]). Voice rules from the brand
/// skill: the serif heading is the human voice (the invite itself), sans is
/// the explanation, mono is provenance (address, host, sent-by footer).
fn render_invite_email_html(
    to_email: &str,
    join_url: &str,
    web_base: &str,
    org: Option<&str>,
    inviter: Option<&str>,
) -> String {
    let email_esc = escape_html(to_email);
    let url_esc = escape_html(join_url);
    let host_esc = escape_html(host_display(web_base));
    let copy = invite_copy(org, inviter);
    let heading = copy.heading_html;
    let footer = copy.footer_html;
    let mandala = mandala_svg_inline();

    format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<div style=\"display:none;max-height:0;overflow:hidden;mso-hide:all\">Your slot is ready — sign in with {email_esc}.</div>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:{FONT_SANS};color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coral{mandala}swarm</div>\
<div style=\"font-family:{FONT_MONO};font-size:10px;text-transform:uppercase;letter-spacing:0.14em;color:#8A7B6E;margin-bottom:12px\">you were invited</div>\
<h1 style=\"margin:0 0 14px 0;font-family:{FONT_SERIF};font-size:30px;font-weight:400;line-height:1.2;color:#1A1410\">{heading}</h1>\
<p style=\"margin:0 0 22px 0;font-size:14px;line-height:1.6;color:#4A3F36\">Your slot is ready. Sign in with this address — <span style=\"font-family:{FONT_MONO};font-size:13px;color:#1A1410\">{email_esc}</span> — and you'll land in the org's shared ocean, next to everything the team already knows.</p>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-family:{FONT_SANS};font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Accept and sign in</a>\
</td></tr></table>\
<p style=\"margin:0 0 26px 0;font-family:{FONT_MONO};font-size:10.5px;line-height:1.6;color:#8A7B6E\">signs you in at {host_esc} · email code, no password</p>\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\"><tr>\
<td style=\"border-top:1px solid #D9CCB8;padding-top:16px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;line-height:1.7;color:#8A7B6E\">Sent by coralswarm {footer}.<br>Not expecting this? Ignore it — nothing happens until you sign in.</p>\
</td></tr></table>\
</td></tr>\
<tr><td align=\"center\" style=\"padding-top:14px\">\
<p style=\"margin:0;font-family:{FONT_MONO};font-size:9.5px;color:#8A7B6E\">coralswarm · your team's living memory</p>\
</td></tr></table>\
</td></tr></table></body></html>"
    )
}

/// Send the invite email via Resend, AWAITED (unlike `teams::invite_member`'s
/// fire-and-forget precedent) so the response can carry a real `email_sent`.
/// Best-effort: any failure (missing key, non-2xx, transport error, or a
/// hang past 10s) returns `false` and never fails the surrounding request —
/// a member who was successfully added must not be rolled back over email.
async fn send_invite_email(
    state: &AppState,
    to_email: &str,
    join_url: &str,
    web_base: &str,
    org_name: Option<&str>,
    inviter_name: Option<&str>,
) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    let org = safe_label(org_name);
    let inviter = safe_label(display_inviter(inviter_name));
    let payload = json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": invite_copy(org.as_deref(), inviter.as_deref()).subject,
        "html": render_invite_email_html(
            to_email,
            join_url,
            web_base,
            org.as_deref(),
            inviter.as_deref(),
        ),
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state
        .http_client
        .post(&url)
        .bearer_auth(&resend_key)
        .json(&payload)
        .send();
    match tokio::time::timeout(Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(
                status = %resp.status(), email_domain = email_domain(to_email),
                "org_invites: Resend returned non-success for invite email"
            );
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(
                error = %e, email_domain = email_domain(to_email),
                "org_invites: failed to send invite email via Resend"
            );
            false
        }
        Err(_) => {
            tracing::warn!(
                email_domain = email_domain(to_email),
                "org_invites: Resend send timed out"
            );
            false
        }
    }
}

/// Delete a ghost Clerk user (one WE just created) and, on success, null out
/// any `users.clerk_user_id` link the control plane may already hold to it.
///
/// The unlink matters because there is no `user.deleted` Clerk webhook: if
/// `crate::clerk_provision::ensure_user` already ran for this
/// `clerk_user_id` (it links/creates a `users` row keyed by email — see its
/// own doc comment) before a LATER step failed and triggered this cleanup,
/// the local row is left pointing at a Clerk user that no longer exists.
/// `ensure_user`'s `INSERT ... ON CONFLICT (email) DO UPDATE ... WHERE
/// users.clerk_user_id IS NULL` guard then permanently refuses to re-link
/// that row on any FUTURE invite of the same email (the `WHERE` never
/// matches a non-NULL stale id), which used to 500 forever. Best-effort and
/// never fails the caller's own error response (org-invites train PR 1
/// review, finding 1).
/// Returns whether the Clerk user was actually deleted (mirrors the
/// pre-existing `gc_ghost_user` return contract for callers that report it
/// onward, e.g. `revoke_invite`'s `user_deleted` response field).
async fn delete_ghost_clerk_user(
    clerk: &ClerkAdminClient,
    control: &sqlx::PgPool,
    clerk_user_id: &str,
) -> bool {
    if let Err(e) = clerk.delete_user(clerk_user_id).await {
        tracing::error!(
            error = %e, clerk_user_id,
            "org_invites: failed to delete the ghost Clerk user"
        );
        return false;
    }
    if let Err(e) = sqlx::query("UPDATE users SET clerk_user_id = NULL WHERE clerk_user_id = $1")
        .bind(clerk_user_id)
        .execute(control)
        .await
    {
        tracing::error!(
            error = %e, clerk_user_id,
            "org_invites: ghost Clerk user deleted, but failed to null out the stale \
             users.clerk_user_id link — re-inviting this email may 500 forever \
             (ensure_user's ON CONFLICT guard)"
        );
    }
    true
}

/// Unwind a partially-created Clerk user/membership: delete the membership,
/// then (only if THIS call created the user — never touch a reused,
/// pre-existing Clerk identity) delete the user too, via
/// [`delete_ghost_clerk_user`] (which also nulls the stale local link — see
/// its own doc comment). Best-effort and never fails the caller's own error
/// response — see this module's doc comment for why a leftover Clerk
/// membership/user here is not silently lost (the `.deleted` webhook still
/// fires off the membership delete).
async fn unwind_clerk(
    clerk: &ClerkAdminClient,
    control: &sqlx::PgPool,
    org_id: &str,
    created: &CreatedUser,
) {
    if let Err(e) = clerk
        .delete_org_membership(org_id, &created.clerk_user_id)
        .await
    {
        tracing::error!(
            error = %e, clerk_user_id = %created.clerk_user_id, org_id,
            "org_invites: unwind failed to delete the Clerk membership — a ghost membership may remain"
        );
    }
    if created.newly_created {
        delete_ghost_clerk_user(clerk, control, &created.clerk_user_id).await;
    }
}

/// Outcome of [`provision_clerk_membership`] — the two callers react to
/// "Clerk already reports a membership" differently, which is exactly why
/// this function does NOT also call [`crate::clerk_provision::
/// ensure_org_membership`] itself (an earlier version did; see
/// `mirror_local_membership`'s own doc comment for why that had to split
/// back out): `create_invite` surfaces a clean 409 to ITS caller on
/// `AlreadyMember` and never runs any local provisioning at all (admin-
/// invite semantics: re-inviting an existing member is a caller error worth
/// naming, and `ensure_user`/`ensure_org_account` must never have run —
/// see that function's own regression tests); `routes::org_join_requests::
/// approve` treats `AlreadyMember` as the approval succeeding (D3's own
/// build note — "Clerk 422 `already_a_member_in_organization` → treat as
/// success for step 3": an admin approving someone Clerk already considers
/// a member must not fail the approval) and calls
/// [`mirror_local_membership`] regardless of which variant it got.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProvisionedMembership {
    /// A genuinely new Clerk membership was created by this call.
    Provisioned,
    /// Clerk reported `already_a_member_in_organization` (400 or 422) — no new
    /// Clerk-side membership was created. What happens next is entirely the
    /// caller's call — see this enum's own doc comment.
    AlreadyMember,
}

/// Create the CLERK-SIDE org membership only — no local mirror, no
/// `our_user_id` needed. Shared by `create_invite` (the admin-invite path)
/// and `routes::org_join_requests::approve` (CS-437, the join-request
/// approval path), factored out of `create_invite`'s own original step 2
/// (this module's doc comment, D3 (`docs/plans/org-onboarding-from-the-
/// invite.md`)) rather than duplicated.
///
/// On ANY failure (including `AlreadyMember`) a ghost Clerk user created by
/// THIS invite is cleaned up — `created.newly_created` guards it, so a
/// reused, pre-existing identity is never touched — matching
/// `create_invite`'s ORIGINAL "ghost cleanup applies to EVERY failure
/// branch" behavior exactly (its own regression tests assert this).
///
/// On a non-`AlreadyMember` failure, maps the error via [`map_clerk_error`]
/// and returns `Err`. There is no membership to unwind on this path — this
/// call is what would have created it.
pub(crate) async fn provision_clerk_membership(
    clerk: &ClerkAdminClient,
    control: &sqlx::PgPool,
    org_id: &str,
    created: &CreatedUser,
    clerk_role: &str,
    metadata: serde_json::Value,
) -> Result<ProvisionedMembership, Response> {
    if let Err(e) = clerk
        .create_org_membership(org_id, &created.clerk_user_id, clerk_role, metadata)
        .await
    {
        if created.newly_created {
            delete_ghost_clerk_user(clerk, control, &created.clerk_user_id).await;
        }
        // Keyed on the code: Clerk has answered this conflict with 400 as
        // well as 422, and any other status would surface a re-invite as a
        // Clerk outage.
        if e.code() == Some("already_a_member_in_organization") {
            return Ok(ProvisionedMembership::AlreadyMember);
        }
        return Err(map_clerk_error("create_org_membership", e));
    }
    Ok(ProvisionedMembership::Provisioned)
}

/// Mirror an already-real Clerk membership locally via
/// [`crate::clerk_provision::ensure_org_membership`] (which acquires the
/// billing seat/slot itself), and unwind on failure. The other half of D3's
/// Clerk-first sequence, split from [`provision_clerk_membership`] rather
/// than folded into one function: an earlier version called both from a
/// single `provision_org_membership`, which forced `create_invite` to
/// resolve `our_user_id` (via `ensure_user`/`ensure_org_account`) BEFORE
/// attempting the Clerk call, so that provisioning ran even when Clerk
/// membership creation failed — a real regression `create_invite`'s own
/// tests caught (`membership_create_failure_deletes_the_ghost_user_and_leaves_no_local_row`
/// and friends: "`ensure_user` is never reached on a membership-create
/// failure"). Splitting lets `create_invite` keep its ORIGINAL ordering
/// (resolve `our_user_id` only after [`provision_clerk_membership`]
/// succeeds) while `routes::org_join_requests::approve` — which already has
/// `our_user_id` from the join request — calls this half only, with no
/// such ordering constraint of its own.
///
/// On failure: [`unwind_clerk`] only when `membership` is
/// [`ProvisionedMembership::Provisioned`] (delete the Clerk membership
/// [`provision_clerk_membership`] just created, plus the ghost user under
/// the same `newly_created` guard). [`ProvisionedMembership::AlreadyMember`]
/// created nothing on the Clerk side — deleting would revoke a membership
/// that predated this call. Returns the mapped [`Response`]
/// (Stripe failures via [`map_stripe_error_to_response`], anything else via
/// [`internal`]). Never touches a caller's OWN state (a slot reservation, an
/// `org_join_requests` row) — that unwind is the caller's job, run AFTER
/// this returns `Err`.
///
/// `seat_tier_id` (CS-442, item 40, D8): threaded straight through to
/// [`crate::clerk_provision::ensure_org_membership`]'s own `seat_tier_id`
/// parameter — `None` for `create_invite` (always `standard`), `Some(id)`
/// for `routes::org_join_requests::approve` (the admin's validated,
/// resolved tier choice on the approval row). See that function's own "Seat
/// tier" doc section for the fresh-insert-only contract.
///
/// `state: &AppState`, not `&Arc<AppState>` (CS-449, item 38) — this
/// function's own body only ever forwards `state` into
/// [`crate::clerk_provision::ensure_org_membership`], which has always
/// taken `&AppState`; the `Arc` here was never load-bearing. Widened so
/// `routes::org_join_requests::try_auto_add` (this function's own caller,
/// twice) can in turn be called from places that hold only a plain
/// `&AppState` — see that function's own doc comment. Every existing
/// caller keeps compiling unchanged: `&state` where `state: Arc<AppState>`
/// deref-coerces to `&AppState` at the call site.
pub(crate) async fn mirror_local_membership(
    state: &AppState,
    clerk: &ClerkAdminClient,
    control: &sqlx::PgPool,
    org_id: &str,
    created: &CreatedUser,
    our_user_id: Uuid,
    our_role: &str,
    membership: ProvisionedMembership,
    seat_tier_id: Option<Uuid>,
    seat_policy: SeatPurchasePolicy,
) -> Result<(), Response> {
    if let Err(e) = crate::clerk_provision::ensure_org_membership_with_seat_policy(
        state,
        control,
        org_id,
        our_user_id,
        our_role,
        true,
        seat_tier_id,
        // No removal fence: this request created (or found) the Clerk
        // membership itself moments ago — see `ensure_org_membership`.
        None,
        seat_policy,
    )
    .await
    {
        // Unwind only a membership THIS call created. AlreadyMember means
        // Clerk already had the person — a Stripe decline (or any other
        // local-mirror failure) must not delete that pre-existing row.
        if membership == ProvisionedMembership::Provisioned {
            unwind_clerk(clerk, control, org_id, created).await;
        }
        return Err(match e {
            ProvisionError::SeatLifecycle(crate::billing::seat_lifecycle::SeatLifecycleError::Stripe(se)) => {
                map_stripe_error_to_response(se)
            }
            ProvisionError::SeatLifecycle(crate::billing::seat_lifecycle::SeatLifecycleError::NoFreeSeat) => {
                no_free_seat()
            }
            other => internal("ensure_org_membership", other),
        });
    }
    Ok(())
}

/// `COALESCE(name, email)` — the same fallback
/// `mcp::tools::write::caller_display_name` uses, so a nameless inviter
/// never renders as `null` in the invite `public_metadata`/join email
/// (org-invites train PR 1 review, finding 11b).
async fn inviter_name(control: &sqlx::PgPool, user_id: Uuid) -> Option<String> {
    match sqlx::query_scalar::<_, Option<String>>(
        "SELECT COALESCE(name, email) FROM users WHERE id = $1",
    )
    .bind(user_id)
    .fetch_optional(control)
    .await
    {
        // `fetch_optional` ⇒ `Option<Option<String>>`: outer `None` = no
        // such user row, inner `None` = a row with neither `name` nor
        // `email` set. Both collapse to "no inviter name to show" — never
        // an error to the caller either way.
        Ok(name) => name.flatten(),
        Err(e) => {
            // No email/name in this log — just the failure and the
            // inviter's own id, which is not PII in the sense that matters
            // here (a swallowed DB error here used to look identical to a
            // genuinely nameless inviter).
            tracing::warn!(error = %e, %user_id, "org_invites: inviter_name lookup failed");
            None
        }
    }
}

async fn org_display_name(clerk: &ClerkAdminClient, org_id: &str) -> Option<String> {
    clerk.get_organization(org_id).await.ok().map(|o| o.name)
}

// ── POST /v1/orgs/invites ───────────────────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct InviteRequest {
    pub email: String,
    /// `"member"` or `"admin"`.
    pub role: String,
    #[serde(default)]
    pub name: Option<String>,
    /// Whether this invite may buy a seat when the org has none free.
    /// Defaults to `true` so a client that predates this field keeps
    /// working (web and backend deploy separately). The web sends `false`:
    /// it buys seats through the quote → confirm dialog first, and on a 409
    /// `no_free_seat` it reopens that dialog instead of buying silently.
    #[serde(default = "default_allow_purchase")]
    pub allow_purchase: bool,
}

fn default_allow_purchase() -> bool {
    true
}

#[derive(Debug, Serialize, ToSchema)]
pub struct InviteResponse {
    pub user_id: Uuid,
    pub clerk_user_id: String,
    pub email: String,
    pub role: String,
    pub pending: bool,
    pub email_sent: bool,
}

/// See this module's doc comment for the full flow and the two-webhook race
/// analysis.
#[utoipa_path(
    post,
    path = "/v1/orgs/invites",
    tag = "teams",
    security(("bearer_token" = [])),
    request_body = InviteRequest,
    responses(
        (status = 201, description = "Member invited (Clerk user + membership pre-created)", body = InviteResponse),
        (status = 400, description = "Invalid email or role"),
        (status = 402, description = "Seat purchase failed", body = serde_json::Value),
        (status = 403, description = "Not an org context, or caller is not an admin"),
        (status = 409, description = "Already a member of this organization (`already_a_member`), or `allow_purchase` is false and the org has no free seat (`code: no_free_seat`)"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn create_invite(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<InviteRequest>,
) -> Response {
    let account=user.account_id;
    let response=create_invite_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn create_invite_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<InviteRequest>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let org_id = ctx.org_id.clone();

    let email = body.email.trim().to_lowercase();
    if !valid_email(&email) {
        return bad("invalid email");
    }
    let Some((our_role, clerk_role)) = map_role(&body.role) else {
        return bad(r#"role must be "member" or "admin""#);
    };

    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };

    let (first_name, last_name) = body.name.as_deref().map(split_name).unwrap_or((None, None));

    // ── Step 0: `allow_purchase: false` — refuse early when no seat is free ─
    //    An early answer only, so the common "pool is full" case creates no
    //    Clerk user, no membership, and fires no webhook. The guarantee is
    //    `SeatPurchasePolicy::PoolOnly` in step 3, decided under the account
    //    lock; this read can go stale before then.
    let seat_policy = if body.allow_purchase {
        SeatPurchasePolicy::AllowPurchase
    } else {
        SeatPurchasePolicy::PoolOnly
    };
    if seat_policy == SeatPurchasePolicy::PoolOnly {
        let standard = match crate::clerk_provision::resolve_standard_seat_tier_id(control).await {
            Ok(id) => id,
            Err(e) => return internal("resolve standard seat tier", e),
        };
        match crate::billing::seat_lifecycle::new_member_needs_purchase(control, ctx.account_id, standard)
            .await
        {
            Ok(false) => {}
            Ok(true) => return no_free_seat(),
            Err(e) => return internal("seat pool pre-check", e),
        }
    }

    // ── Step 0b (self-serve signup A2, S4): a self-serve org's Clerk member
    //    cap follows its purchased seats. An invite allowed to buy its own
    //    seat gets one place of headroom first (the seat is bought when the
    //    membership lands, and the cap re-synced to the purchased count right
    //    after); a pool-only invite fits within the cap as is. A no-op for
    //    any org `POST /v1/orgs` did not create.
    //    #1540 N3: if the invite then fails, the headroom is given back at
    //    once (`with_member_cap_headroom`), not left until the next seat
    //    change.
    let admit = async {
        // ── Step 1: create (or reuse) the Clerk user ──────────────────────────
        let created = match clerk
            .create_user(&email, first_name.as_deref(), last_name.as_deref())
            .await
        {
            Ok(c) => c,
            Err(e) => return map_clerk_error("create_user", e),
        };

        // ── Step 2: create the Clerk org membership, carrying invite metadata ─
        //    See [`provision_clerk_membership`]'s own doc comment for why this
        //    runs BEFORE any local provisioning: `ensure_user`/`ensure_org_account`
        //    below must NEVER run when Clerk membership creation itself fails —
        //    this function's own regression tests assert exactly that ordering.
        let inviter = inviter_name(control, user.user_id).await;
        let metadata = json!({
            "invited_by_user_id": user.user_id.to_string(),
            "invited_by_name": inviter,
            "invited_at": chrono::Utc::now().to_rfc3339(),
        });
        match provision_clerk_membership(&clerk, control, &org_id, &created, clerk_role, metadata).await {
            Ok(ProvisionedMembership::Provisioned) => {}
            // "Already a member" is not a ghost-user situation to unwind loudly
            // over — cleanup already ran inside `provision_clerk_membership`,
            // just a clean 409. `create_invite`'s own external contract: NO
            // local provisioning below ever runs for this branch.
            Ok(ProvisionedMembership::AlreadyMember) => return conflict("already_a_member"),
            Err(resp) => return resp,
        }

        // ── Step 3: our own user/account rows + the local mirror (see module
        //    doc) — only reached once the Clerk membership genuinely exists ───
        let our_user_id = match crate::clerk_provision::ensure_user(
            &state,
            control,
            &created.clerk_user_id,
            Some(&email),
            body.name.as_deref(),
        )
        .await
        {
            Ok(id) => id,
            Err(e) => {
                unwind_clerk(&clerk, control, &org_id, &created).await;
                return internal("ensure_user", e);
            }
        };
        // No-op when the account already exists (the normal case — an admin
        // inviting into an already-provisioned org); mirrors the webhook's own
        // `handle_membership_upsert` call order.
        if let Err(e) =
            crate::clerk_provision::ensure_org_account(&state, control, &org_id, our_user_id, None)
                .await
        {
            unwind_clerk(&clerk, control, &org_id, &created).await;
            return internal("ensure_org_account", e);
        }
        if let Err(resp) = mirror_local_membership(
            &state,
            &clerk,
            control,
            &org_id,
            &created,
            our_user_id,
            our_role,
            ProvisionedMembership::Provisioned,
            // CS-442 (item 40, D8): an admin invite always lands on `standard` —
            // there is no tier control on `InviteRequest`; see this file's own
            // module doc for why join-request approval is the one path that
            // passes a real choice through.
            None,
            seat_policy,
        )
        .await
        {
            return resp;
        }

        // ── Step 4: email, AFTER commit, best-effort (never unwinds) ──────────
        let org_name = org_display_name(&clerk, &org_id).await;
        let web_base = state.org_web_base_url(control, &org_id).await;
        let join_url = build_join_url(
            &web_base,
            &email,
            org_name.as_deref(),
            display_inviter(inviter.as_deref()),
        );
        let email_sent = send_invite_email(
            &state,
            &email,
            &join_url,
            &web_base,
            org_name.as_deref(),
            inviter.as_deref(),
        )
        .await;

        let pending = derive_pending(&created);

        (
            StatusCode::CREATED,
            Json(InviteResponse {
                user_id: our_user_id,
                clerk_user_id: created.clerk_user_id,
                email,
                role: our_role.to_string(),
                pending,
                email_sent,
            }),
        )
            .into_response()
    };
    if seat_policy == SeatPurchasePolicy::AllowPurchase {
        crate::routes::orgs_create::with_member_cap_headroom(control, &clerk, ctx.account_id, admit).await
    } else {
        admit.await
    }
}

// ── POST /v1/orgs/invites/{user_id}/resend ──────────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/invites/{user_id}/resend",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "target member's control-plane user id (from GET /v1/orgs/members)")),
    responses(
        (status = 200, description = "Resend attempted", body = serde_json::Value),
        (status = 403, description = "Not an org context, or caller is not an admin"),
        (status = 404, description = "No such member of this organization"),
        (status = 409, description = "Member has already signed in — not pending, nothing to resend"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn resend_invite(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> Response {
    let account=user.account_id;
    let response=resend_invite_committed(State(state.clone()),tenant,Extension(user),Path(target_user_id)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn resend_invite_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let org_id = ctx.org_id.clone();
    let Some(control) = state.control_db() else {
        return unavailable();
    };

    let row: Option<(String, Option<String>)> = match sqlx::query_as(
        "SELECT u.email, u.clerk_user_id FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         WHERE am.account_id = $1 AND am.user_id = $2",
    )
    .bind(user.account_id)
    .bind(target_user_id)
    .fetch_optional(control)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal("member lookup", e),
    };
    let Some((email, clerk_user_id_opt)) = row else {
        return not_found("no such member of this organization");
    };
    // A member with no linked Clerk identity was never provisioned through
    // this invite path (or Clerk auth at all) — nothing coherent to resend.
    let Some(clerk_user_id) = clerk_user_id_opt else {
        return not_found("member has no linked Clerk identity");
    };

    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };
    // Refuse to resend to a member who has already accepted (signed in at
    // least once) — Clerk is re-queried live rather than trusted from
    // whatever was true at invite time, since that can be stale by however
    // long ago the invite was created. A Clerk lookup failure fails the
    // resend outright (never emails an active member on an unverifiable
    // state) — this is the opposite fail-direction from this module's other
    // best-effort Clerk lookups precisely because sending mail is the
    // externally-visible side effect being gated.
    let clerk_user = match clerk.get_user(&clerk_user_id).await {
        Ok(u) => u,
        Err(e) => return map_clerk_error("get_user", e),
    };
    if clerk_user.last_sign_in_at.is_some() {
        return conflict("not_pending");
    }

    let inviter = inviter_name(control, user.user_id).await;
    let org_name = org_display_name(&clerk, &org_id).await;
    let web_base = state.org_web_base_url(control, &org_id).await;
    let join_url = build_join_url(
        &web_base,
        &email,
        org_name.as_deref(),
        display_inviter(inviter.as_deref()),
    );
    let email_sent = send_invite_email(
        &state,
        &email,
        &join_url,
        &web_base,
        org_name.as_deref(),
        inviter.as_deref(),
    )
    .await;

    (StatusCode::OK, Json(json!({"email_sent": email_sent}))).into_response()
}

// ── DELETE /v1/orgs/invites/{user_id} ───────────────────────────────────────

#[utoipa_path(
    delete,
    path = "/v1/orgs/invites/{user_id}",
    tag = "teams",
    security(("bearer_token" = [])),
    params(("user_id" = Uuid, Path, description = "target member's control-plane user id (from GET /v1/orgs/members)")),
    responses(
        (status = 200, description = "Revoked", body = serde_json::Value),
        (status = 400, description = "Cannot revoke self, or the account owner"),
        (status = 403, description = "Not an org context, or caller is not an admin"),
        (status = 404, description = "No such member of this organization"),
        (status = 409, description = "Member has already signed in — not pending, use member removal instead"),
        (status = 502, description = "Clerk API error"),
        (status = 503, description = "Database, or Clerk API key, unavailable"),
    )
)]
pub async fn revoke_invite(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> Response {
    let account=user.account_id;
    let response=revoke_invite_committed(State(state.clone()),tenant,Extension(user),Path(target_user_id)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn revoke_invite_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(target_user_id): Path<Uuid>,
) -> Response {
    let ctx = match require_org_admin(&tenant, &user) {
        Ok(c) => c,
        Err(r) => return r,
    };
    let org_id = ctx.org_id.clone();
    let Some(control) = state.control_db() else {
        return unavailable();
    };
    let Some(clerk) = ClerkAdminClient::from_env() else {
        return unavailable();
    };

    // Guard (a): an admin can never revoke themselves through this
    // endpoint — cheap, no DB/Clerk round trip needed to know this.
    if target_user_id == user.user_id {
        return bad("cannot_revoke_self");
    }

    // `is_owner` reads `accounts.owner_user_id` (NOT `account_members.role
    // = 'owner'`, which on an ORG account is never actually set — org
    // members are `'admin'`/`'member'` only; `'owner'` is the PERSONAL-
    // account convention, see `routes::teams::has_account_authority`'s own
    // doc comment. `accounts.owner_user_id` is the org's real ownership
    // record — see `clerk_provision::ensure_org_account`, which stamps it
    // at org-provisioning time). Errors propagate as 500 rather than
    // defaulting `is_owner` to `false`, which would silently let a failed
    // ownership check fall through to "not the owner, proceed".
    let row: Option<(Option<String>, bool)> = match sqlx::query_as(
        "SELECT u.clerk_user_id, COALESCE(a.owner_user_id = am.user_id, false) AS is_owner \
         FROM account_members am \
         JOIN users u ON u.id = am.user_id \
         JOIN accounts a ON a.id = am.account_id \
         WHERE am.account_id = $1 AND am.user_id = $2",
    )
    .bind(user.account_id)
    .bind(target_user_id)
    .fetch_optional(control)
    .await
    {
        Ok(r) => r,
        Err(e) => return internal("member lookup", e),
    };
    // Distinguish "no such member" from "member exists but has no linked
    // Clerk identity" — the same two-step check `resend_invite` already
    // makes, rather than collapsing both into one generic 404 (org-invites
    // train PR 1 review, finding 11a).
    let Some((clerk_user_id_opt, is_owner)) = row else {
        return not_found("no such member of this organization");
    };
    let Some(clerk_user_id) = clerk_user_id_opt else {
        return not_found("member has no linked Clerk identity");
    };
    // Guard (b): the account owner is never revocable here — removing them
    // is out of this endpoint's scope entirely (they were never "invited"
    // in the sense this endpoint governs).
    if is_owner {
        return bad("cannot_revoke_owner");
    }

    // Guard (c): revoking an INVITE is this endpoint's semantics — an
    // ACTIVE member (has signed in at least once) is out of scope; removing
    // one is a different, not-yet-built operation. A Clerk lookup failure
    // fails the revoke outright rather than guessing.
    let clerk_user = match clerk.get_user(&clerk_user_id).await {
        Ok(u) => u,
        Err(e) => return map_clerk_error("get_user", e),
    };
    if clerk_user.last_sign_in_at.is_some() {
        return conflict("not_pending");
    }

    // Local row cleanup + seat release ride the existing
    // `organizationMembership.deleted` webhook (`clerk_provision::
    // remove_org_membership`) — deliberately not duplicated here; the
    // roster may show the member briefly until the webhook lands, which the
    // plan accepts.
    if let Err(e) = clerk.delete_org_membership(&org_id, &clerk_user_id).await {
        return map_clerk_error("delete_org_membership", e);
    }

    // GC guard: delete the ghost Clerk user iff they have NEVER signed in
    // AND belong to no other org. Best-effort/non-fatal past this point —
    // the membership (this org's actual access) is already gone regardless
    // of whether this cleanup succeeds.
    let user_deleted = gc_ghost_user(&clerk, control, &clerk_user_id).await;

    (
        StatusCode::OK,
        Json(json!({"revoked": true, "user_deleted": user_deleted})),
    )
        .into_response()
}

async fn gc_ghost_user(
    clerk: &ClerkAdminClient,
    control: &sqlx::PgPool,
    clerk_user_id: &str,
) -> bool {
    let clerk_user = match clerk.get_user(clerk_user_id).await {
        Ok(u) => u,
        Err(e) => {
            tracing::warn!(error = %e, clerk_user_id, "org_invites: GC get_user failed after revoke");
            return false;
        }
    };
    if clerk_user.last_sign_in_at.is_some() {
        return false;
    }
    let remaining = match clerk.list_user_org_memberships(clerk_user_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, clerk_user_id, "org_invites: GC list_user_org_memberships failed after revoke");
            return false;
        }
    };
    if !remaining.is_empty() {
        return false;
    }
    // Deletes the Clerk user AND nulls the stale local `users.clerk_user_id`
    // link — see [`delete_ghost_clerk_user`]'s doc comment (finding 1,
    // org-invites train PR 1 review): without the unlink, re-inviting this
    // same email later would hit `ensure_user`'s `WHERE clerk_user_id IS
    // NULL` guard and 500 forever, exactly like the create-side unwind.
    delete_ghost_clerk_user(clerk, control, clerk_user_id).await
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::Request as HttpRequest,
        middleware,
        routing::{delete, post},
        Router,
    };
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/orgs/invites", post(create_invite))
            .route("/v1/orgs/invites/{user_id}/resend", post(resend_invite))
            .route("/v1/orgs/invites/{user_id}", delete(revoke_invite))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                crate::middleware::auth::require_auth,
            ))
            .with_state(state)
    }

    #[tokio::test]
    async fn create_invite_requires_auth() {
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/orgs/invites")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"email":"a@test.dev","role":"member"}"#))
            .unwrap();
        let resp = router(test_state()).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn resend_invite_requires_auth() {
        let req = HttpRequest::builder()
            .method("POST")
            .uri(format!("/v1/orgs/invites/{}/resend", Uuid::new_v4()))
            .body(Body::empty())
            .unwrap();
        let resp = router(test_state()).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn revoke_invite_requires_auth() {
        let req = HttpRequest::builder()
            .method("DELETE")
            .uri(format!("/v1/orgs/invites/{}", Uuid::new_v4()))
            .body(Body::empty())
            .unwrap();
        let resp = router(test_state()).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    fn user_ctx(role: &str) -> UserContext {
        UserContext {
            user_id: Uuid::new_v4(),
            account_id: Uuid::new_v4(),
            role: role.to_string(),
            device_id: "org-invites-test".to_string(),
        }
    }

    fn no_org_tenant() -> Tenant {
        // No `TenancyDecision` at all — same shape `Tenant::legacy` produces:
        // `org_context()` is `None`, exactly the "not an organization
        // workspace" case every handler here must refuse with 403.
        Tenant {
            store: None,
            pool: None,
            decision: None,
            class: crate::route_class::RouteClass::Other,
        }
    }

    #[tokio::test]
    async fn create_invite_without_an_org_context_is_forbidden() {
        let resp = create_invite(
            State(test_state()),
            no_org_tenant(),
            Extension(user_ctx("admin")),
            Json(InviteRequest {
                email: "a@test.dev".into(),
                role: "member".into(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn resend_invite_without_an_org_context_is_forbidden() {
        let resp = resend_invite(
            State(test_state()),
            no_org_tenant(),
            Extension(user_ctx("admin")),
            Path(Uuid::new_v4()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn revoke_invite_without_an_org_context_is_forbidden() {
        let resp = revoke_invite(
            State(test_state()),
            no_org_tenant(),
            Extension(user_ctx("admin")),
            Path(Uuid::new_v4()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    // ── Pure helper unit tests ──────────────────────────────────────────────

    #[test]
    fn valid_email_accepts_a_plain_address() {
        assert!(valid_email("a@test.dev"));
    }

    #[test]
    fn valid_email_rejects_missing_at_whitespace_and_empty() {
        assert!(!valid_email(""));
        assert!(!valid_email("no-at-sign"));
        assert!(!valid_email("a b@test.dev"));
        assert!(!valid_email("@test.dev"));
        assert!(!valid_email("a@"));
        assert!(!valid_email("a@b@test.dev"));
    }

    #[test]
    fn map_role_accepts_member_and_admin_only() {
        assert_eq!(map_role("member"), Some(("member", "org:member")));
        assert_eq!(map_role("admin"), Some(("admin", "org:admin")));
        assert_eq!(map_role("owner"), None);
        assert_eq!(map_role(""), None);
    }

    #[test]
    fn split_name_splits_on_first_space() {
        assert_eq!(
            split_name("Ada Lovelace"),
            (Some("Ada".into()), Some("Lovelace".into()))
        );
        assert_eq!(split_name("Cher"), (Some("Cher".into()), None));
        assert_eq!(split_name("  "), (None, None));
        assert_eq!(
            split_name("Mary Jane Watson"),
            (Some("Mary".into()), Some("Jane Watson".into()))
        );
    }

    #[test]
    fn build_join_url_encodes_query_params() {
        let url = build_join_url(
            "https://acme.coralswarm.example",
            "a b@test.dev",
            Some("Acme Inc"),
            Some("Jane Doe"),
        );
        assert!(url.starts_with("https://acme.coralswarm.example/join?email="));
        assert!(url.contains("email=a%20b%40test.dev"));
        assert!(url.contains("org=Acme%20Inc"));
        assert!(url.contains("inviter=Jane%20Doe"));
    }

    #[test]
    fn build_join_url_omits_absent_org_and_inviter() {
        let url = build_join_url("https://acme.coralswarm.example", "a@test.dev", None, None);
        assert!(!url.contains("org="));
        assert!(!url.contains("inviter="));
    }

    // ── PR 3: email template helpers ────────────────────────────────────────

    #[test]
    fn escape_html_escapes_all_five_specials() {
        assert_eq!(
            escape_html(r#"<a href="x">&'"#),
            "&lt;a href=&quot;x&quot;&gt;&amp;&#39;"
        );
    }

    #[test]
    fn safe_label_mirrors_the_join_pages_safelabel() {
        // strips outside the allowlist, keeps unicode letters
        assert_eq!(
            safe_label(Some("<script>Acme</script>")).as_deref(),
            Some("scriptAcmescript")
        );
        assert_eq!(safe_label(Some("Über GmbH & Co.")).as_deref(), Some("Über GmbH & Co."));
        // caps at 48 then re-trims
        let long = "a".repeat(60);
        assert_eq!(safe_label(Some(&long)).unwrap().chars().count(), 48);
        // empty-after-stripping reads as absent
        assert_eq!(safe_label(Some("!!!###")), None);
        assert_eq!(safe_label(Some("   ")), None);
        assert_eq!(safe_label(None), None);
    }

    #[test]
    fn host_display_strips_scheme_trailing_slash_and_path() {
        assert_eq!(host_display("https://acme.coralswarm.example"), "acme.coralswarm.example");
        assert_eq!(host_display("http://localhost:3000/"), "localhost:3000");
        assert_eq!(host_display("acme.coralswarm.example"), "acme.coralswarm.example");
        assert_eq!(host_display("https://app.dev/some/path"), "app.dev");
    }

    #[test]
    fn display_inviter_drops_email_shaped_labels() {
        assert_eq!(display_inviter(Some("Alex")), Some("Alex"));
        assert_eq!(display_inviter(Some("person@example.com")), None);
        assert_eq!(display_inviter(None), None);
    }

    #[test]
    fn invite_copy_subject_degrades_gracefully() {
        assert_eq!(
            invite_copy(Some("Coralswarm"), Some("Alex")).subject,
            "Alex invited you to Coralswarm"
        );
        assert_eq!(
            invite_copy(Some("Coralswarm"), None).subject,
            "You're invited to Coralswarm"
        );
        assert_eq!(
            invite_copy(None, Some("Alex")).subject,
            "Alex invited you to their coralswarm organization"
        );
        assert_eq!(
            invite_copy(None, None).subject,
            "You've been invited to a coralswarm organization"
        );
    }

    #[test]
    fn mandala_svg_inline_is_the_generated_asset_with_email_sizing() {
        let svg = mandala_svg_inline();
        assert!(svg.starts_with("<svg width=\"15\" height=\"15\""));
        assert!(svg.contains("viewBox"), "generated asset's viewBox must survive");
        assert!(svg.trim_end().ends_with("</svg>"));
    }

    #[test]
    fn render_invite_email_html_escapes_every_dynamic_value() {
        // `safe_label` already strips angle brackets from org/inviter in the
        // real send path; this exercises the template's OWN escaping as the
        // second, independent layer.
        let html = render_invite_email_html(
            "a<b@test.dev",
            "https://acme.coralswarm.example/join?email=a%40test.dev&org=Acme",
            "https://acme.coralswarm.example",
            Some("Acme <Inc>"),
            Some("Jane 'J' & Co"),
        );
        assert!(!html.contains("<b@test.dev"));
        assert!(html.contains("a&lt;b@test.dev"));
        assert!(!html.contains("Acme <Inc>"));
        assert!(html.contains("Acme &lt;Inc&gt;"));
        assert!(html.contains("Jane &#39;J&#39; &amp; Co"));
        // the join URL's & is attribute-escaped inside href
        assert!(html.contains("href=\"https://acme.coralswarm.example/join?email=a%40test.dev&amp;org=Acme\""));
        // host provenance line
        assert!(html.contains("signs you in at acme.coralswarm.example"));
    }

    #[test]
    fn render_invite_email_html_heading_and_footer_fallbacks() {
        let both = render_invite_email_html(
            "a@test.dev",
            "https://x.dev/join",
            "https://x.dev",
            Some("Coralswarm"),
            Some("Alex"),
        );
        assert!(both.contains("Alex invited you to"));
        assert!(both.contains(">Coralswarm</em>"));
        assert!(both.contains("because Alex added you to the Coralswarm organization"));

        let neither =
            render_invite_email_html("a@test.dev", "https://x.dev/join", "https://x.dev", None, None);
        assert!(neither.contains("You've been"));
        assert!(neither.contains(">invited</em>"));
        assert!(neither.contains("because an admin added you to their organization"));
    }

    #[test]
    fn render_invite_email_html_uses_only_brand_token_colors() {
        // Brand-skill validation check 1: every hex in the output must be a
        // token value (cream email palette + hues + white on the brand CTA).
        let html = render_invite_email_html(
            "a@test.dev",
            "https://x.dev/join",
            "https://x.dev",
            Some("Coralswarm"),
            Some("Alex"),
        );
        // No non-hex color syntaxes at all — one notation to scan.
        for banned in ["rgb(", "rgba(", "hsl(", "color-mix("] {
            assert!(!html.contains(banned), "non-hex color syntax in email template: {banned}");
        }
        let allowed = [
            "#F2EBE0", "#FBF7F2", "#D9CCB8", "#1A1410", "#4A3F36", "#8A7B6E", "#F77764",
            "#D14A38", "#FFFFFF", "#EB7290", "#F4B860", "#2A9D8F", "#7C5295",
        ];
        for (i, _) in html.match_indices('#') {
            // Maximal ASCII-hex run after '#': entity refs (&#39;) end at the
            // ';' and are skipped by the length gate; a 3/4/8-digit hex color
            // is NOT skipped — it fails the 6-digit allowlist rather than
            // sneaking past a fixed-width scan.
            let run: String = html[i + 1..]
                .chars()
                .take_while(|c| c.is_ascii_hexdigit())
                .collect();
            if (3..=8).contains(&run.len()) {
                let hex = format!("#{run}");
                assert!(
                    allowed.contains(&hex.as_str()),
                    "non-token hex color in email template: {hex}"
                );
            }
        }
    }

    /// CS-433 / D15: the billing concept the invite email describes is a
    /// **slot**, never a "seat" — that word is reserved for team-ocean
    /// access (`crate::seats`). Guards against the rename regressing.
    #[test]
    fn render_invite_email_html_says_slot_not_seat() {
        let html = render_invite_email_html(
            "a@test.dev",
            "https://x.dev/join",
            "https://x.dev",
            Some("Coralswarm"),
            Some("Alex"),
        );
        assert!(html.contains("Your slot is ready"));
        assert!(!html.to_lowercase().contains("seat"));
    }

    /// A REAL org context (unlike `no_org_tenant`'s "not an org at all"
    /// case) but a non-admin caller — `require_org_admin`'s other refusal
    /// branch. The `TenantContext.pool` is a lazy, never-connected pool
    /// (same fixture `tenancy::extractor`'s own
    /// `org_decision_builds_a_fresh_store_over_the_tenant_pool` test uses) —
    /// `require_org_admin` returns before touching it.
    fn org_tenant_with_lazy_pool() -> Tenant {
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgres://localhost/unused")
            .expect("lazy pool construction never connects");
        let ctx = TenantContext {
            org_id: "org_test".to_string(),
            account_id: Uuid::new_v4(),
            user_id: Uuid::new_v4(),
            pool,
            page_pool: None,
        };
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)),
            class: crate::route_class::RouteClass::Other,
        }
    }

    #[tokio::test]
    async fn create_invite_by_a_plain_member_is_forbidden() {
        let resp = create_invite(
            State(test_state()),
            org_tenant_with_lazy_pool(),
            Extension(user_ctx("member")),
            Json(InviteRequest {
                email: "a@test.dev".into(),
                role: "member".into(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn resend_invite_by_a_plain_member_is_forbidden() {
        let resp = resend_invite(
            State(test_state()),
            org_tenant_with_lazy_pool(),
            Extension(user_ctx("member")),
            Path(Uuid::new_v4()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn revoke_invite_by_a_plain_member_is_forbidden() {
        let resp = revoke_invite(
            State(test_state()),
            org_tenant_with_lazy_pool(),
            Extension(user_ctx("member")),
            Path(Uuid::new_v4()),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn create_invite_rejects_an_invalid_email() {
        let resp = create_invite(
            State(test_state()),
            org_tenant_with_lazy_pool(),
            Extension(user_ctx("admin")),
            Json(InviteRequest {
                email: "not-an-email".into(),
                role: "member".into(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_invite_rejects_an_unknown_role() {
        let resp = create_invite(
            State(test_state()),
            org_tenant_with_lazy_pool(),
            Extension(user_ctx("admin")),
            Json(InviteRequest {
                email: "a@test.dev".into(),
                role: "owner".into(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    // ── Finding 4: map_clerk_error status mapping ────────────────────────────

    #[test]
    fn map_clerk_error_treats_401_and_403_as_misconfigured_not_passthrough() {
        for status in [401u16, 403] {
            let e = ClerkAdminError::Api {
                status,
                code: None,
                message: "bad key".into(),
            };
            let resp = map_clerk_error("step", e);
            assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        }
    }

    #[test]
    fn map_clerk_error_maps_429_and_5xx_to_502() {
        for status in [429u16, 500, 503] {
            let e = ClerkAdminError::Api {
                status,
                code: None,
                message: "x".into(),
            };
            let resp = map_clerk_error("step", e);
            assert_eq!(resp.status(), StatusCode::BAD_GATEWAY);
        }
    }

    #[test]
    fn map_clerk_error_passes_through_other_4xx_as_before() {
        let e = ClerkAdminError::Api {
            status: 422,
            code: Some("x".into()),
            message: "y".into(),
        };
        let resp = map_clerk_error("step", e);
        assert_eq!(resp.status(), StatusCode::UNPROCESSABLE_ENTITY);
    }

    // ── Finding 7: map_stripe_error_to_response classifies every variant ────

    #[test]
    fn map_stripe_error_to_response_classifies_every_variant() {
        use crate::billing::stripe_client::StripeError as SE;
        let cases: Vec<(SE, StatusCode)> = vec![
            (
                SE::CardDeclined {
                    code: "generic_decline".into(),
                    decline_code: None,
                    message: "declined".into(),
                    request_id: None,
                },
                StatusCode::PAYMENT_REQUIRED,
            ),
            (
                SE::Api {
                    status: 422,
                    kind: "invalid_request_error".into(),
                    code: None,
                    param: None,
                    message: "m".into(),
                    request_id: None,
                },
                StatusCode::PAYMENT_REQUIRED,
            ),
            (
                SE::Api {
                    status: 500,
                    kind: "api_error".into(),
                    code: None,
                    param: None,
                    message: "m".into(),
                    request_id: None,
                },
                StatusCode::BAD_GATEWAY,
            ),
            (
                SE::Transport {
                    public: "transport",
                    request_id: None,
                },
                StatusCode::BAD_GATEWAY,
            ),
            (SE::Timeout, StatusCode::BAD_GATEWAY),
            (
                SE::RateLimited {
                    retry_after: None,
                    request_id: None,
                },
                StatusCode::BAD_GATEWAY,
            ),
            (SE::Parse("bad".into()), StatusCode::BAD_GATEWAY),
            (SE::NotConfigured, StatusCode::SERVICE_UNAVAILABLE),
            (
                SE::Auth { request_id: None },
                StatusCode::SERVICE_UNAVAILABLE,
            ),
            (
                SE::IdempotencyConflict {
                    key: "k".into(),
                    request_id: None,
                },
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
            (
                SE::ArchivedLookupKey {
                    lookup_key: "k".into(),
                    price_id: "p".into(),
                },
                StatusCode::INTERNAL_SERVER_ERROR,
            ),
        ];
        for (err, expected) in cases {
            let resp = map_stripe_error_to_response(err);
            assert_eq!(resp.status(), expected);
        }
    }

    // ── Finding 10: pending is derived, not hardcoded ────────────────────────

    #[test]
    fn derive_pending_is_true_for_a_newly_created_user_regardless_of_last_sign_in() {
        assert!(derive_pending(&CreatedUser {
            clerk_user_id: "user_1".into(),
            newly_created: true,
            last_sign_in_at: None,
        }));
        assert!(
            derive_pending(&CreatedUser {
                clerk_user_id: "user_1".into(),
                newly_created: true,
                last_sign_in_at: Some(1_700_000_000_000),
            }),
            "newly_created alone must be enough — a fresh Clerk user has no real \
             last_sign_in_at to contradict it"
        );
    }

    #[test]
    fn derive_pending_reflects_last_sign_in_for_a_reused_identity() {
        assert!(
            derive_pending(&CreatedUser {
                clerk_user_id: "user_1".into(),
                newly_created: false,
                last_sign_in_at: None,
            }),
            "a reused, never-signed-in ghost from an earlier invite is still pending"
        );
        assert!(
            !derive_pending(&CreatedUser {
                clerk_user_id: "user_1".into(),
                newly_created: false,
                last_sign_in_at: Some(1_700_000_000_000),
            }),
            "a reused identity that has already signed in is not pending"
        );
    }
}

// ── Live-Postgres tests ──────────────────────────────────────────────────────
//
// IN-CRATE, not `backend/tests/org_invites_pg.rs`: `create_invite`/
// `resend_invite`/`revoke_invite` live in `routes` (crate-PRIVATE — `mod
// routes;` in `lib.rs`, not `pub mod routes;`), so an external
// `backend/tests/*.rs` binary (which links this crate as a normal, non-`cfg
// test` dependency) cannot name them at all — `cannot find type ... in this
// scope` / `module routes is private`, not merely a visibility warning. The
// only two ways to exercise a crate-private handler live are (a) drive it
// through the real public router (`test_support_app`, real Clerk-JWT
// verification + the Host-based binding gate — what every other
// `route_migration_org_pg.rs`-style suite does), or (b) an IN-CRATE
// `#[cfg(test)] mod ..._pg_tests` block, which the pg-suite CI-coverage
// script (`scripts/check_pg_suite_coverage.sh`) documents as a first-class,
// equally-required-to-be-wired second convention for exactly this case —
// `routes::billing_seats`'s own `pg_tests` module is the existing precedent
// (calls `change_seat_tier` directly, no router). (a) would also pull in a
// JWKS listener and full binding-gate setup this endpoint's own logic does
// not need (org gate here is the `Tenant` extractor, a plain struct with
// public fields this suite constructs directly — see `org_tenant` below).
// This suite follows (b).
//
// Gated on `ORGT_TEST_BASE_URL` (a scratch database per test, recreated) —
// same convention as every external `*_pg.rs` file, chosen over
// `RELINFER_TEST_DATABASE_URL`'s shared-persistent-DB convention so this
// suite (which also stands up two local mock HTTP servers per test) stays
// fully self-contained and parallel-safe. `StripeClient::with_base_url`'s
// `#[cfg(not(test))]` production guard (only accepts `https://` outside a
// `--cfg test` build) is naturally satisfied here since this whole module is
// `#[cfg(test)]` — the ONE thing an in-crate module gets "for free" that an
// external `tests/*.rs` binary cannot, which is what makes the Stripe-mock
// scenario possible here at all.
//
// ## Running locally
//   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:5432/postgres \
//     cargo test -p coralswarm-backend --lib "routes::org_invites::pg_tests::" -- --test-threads=1 --nocapture
#[cfg(test)]
mod pg_tests {
    mod retained_database {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/tests/support/retained_database.rs"));
    }
    use super::*;
    use axum::extract::{Path as AxumPath, State as AxumState};
    use axum::routing::{delete as axum_delete, get as axum_get, post as axum_post};
    use serde_json::Value;
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::sync::Mutex as StdMutex;
    use tokio::sync::Mutex as AsyncMutex;

    /// Serializes every test in this module — `CLERK_API_BASE_URL`/
    /// `CLERK_SECRET_KEY`/`RESEND_API_BASE_URL` are process-global env vars
    /// each test mutates to its own per-test mock addresses. Same convention
    /// as every `*_pg.rs` file's `ENV_LOCK`.
    static ENV_LOCK: AsyncMutex<()> = AsyncMutex::const_new(());

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn admin_conn(base: &str) -> PgConnection {
        PgConnection::connect(base).await.expect("connect admin")
    }
    async fn recreate(conn: &mut PgConnection, db: &str) {
        retained_database::recreate(conn, db).await.unwrap();
    }
    async fn control_pool(base: &str, db: &str) -> PgPool {
        let pool = PgPoolOptions::new()
            .max_connections(4)
            .connect(&with_db(base, db))
            .await
            .unwrap();
        sqlx::migrate!("./migrations_control")
            .run(&pool)
            .await
            .unwrap();
        pool
    }

    #[allow(dead_code)]
    struct EnvGuard {
        // Process-wide: every lib test that writes `RESEND_API_BASE_URL`
        // takes this first. This module's own lock only covers Clerk.
        _resend: std::sync::MutexGuard<'static, ()>,
        _env: tokio::sync::MutexGuard<'static, ()>,
    }
    impl EnvGuard {
        async fn acquire(clerk_base: &str, resend_base: &str) -> Self {
            let resend = crate::services::unmatched_recipient_email::RESEND_BASE_URL_LOCK
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            let guard = ENV_LOCK.lock().await;
            // SAFETY: serialized by both guards, held for this struct's whole
            // lifetime (including on panic/unwind, via `Drop` below).
            unsafe {
                std::env::set_var("CLERK_API_BASE_URL", clerk_base);
                std::env::set_var("CLERK_SECRET_KEY", "sk_test_mock_org_invites");
                std::env::set_var("RESEND_API_BASE_URL", resend_base);
            }
            Self {
                _resend: resend,
                _env: guard,
            }
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `acquire` — still serialized by the guards here.
            unsafe {
                std::env::remove_var("CLERK_API_BASE_URL");
                std::env::remove_var("CLERK_SECRET_KEY");
                std::env::remove_var("RESEND_API_BASE_URL");
            }
        }
    }

    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX)
            .await
            .unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    // ── Clerk Backend API mock ──────────────────────────────────────────────

    #[derive(Default)]
    struct ClerkMock {
        next_user_seq: u32,
        membership_create_status: u16,
        membership_create_code: Option<String>,
        created_users: Vec<String>,
        deleted_users: Vec<String>,
        deleted_memberships: Vec<(String, String)>,
        /// `None` (default) = the mocked user has never signed in.
        get_user_last_sign_in_at: Option<i64>,
        other_org_count: usize,
        /// Explicit membership org ids for `GET
        /// /v1/users/{id}/organization_memberships`, overriding
        /// `other_org_count`'s synthetic `org_other_N` generation when
        /// `Some` — the finding 2 stale-webhook-delivery guard tests need to
        /// report the CALLER's OWN org id back (not an unrelated
        /// placeholder), to simulate "Clerk still reports this membership as
        /// current".
        membership_orgs: Option<Vec<String>>,
        org_name: String,
        /// When set, `POST /v1/users` for THIS email (case-insensitive)
        /// returns Clerk's `form_identifier_exists` 422 instead of creating
        /// a new user — simulating an email that already belongs to an
        /// existing Clerk identity from before this test's invite call
        /// (finding 11, org-invites train PR 1 review: the
        /// `create_user`/`find_user_by_email` reuse fallback was previously
        /// untested — the mock's create-user always 200ed with a fresh id).
        /// `(email, clerk_user_id, last_sign_in_at)` — the latter two are
        /// what `GET /v1/users?email_address=` then serves back, in the
        /// documented paginated `{"data": [...], "total_count": N}`
        /// envelope (fix 2 above).
        existing_user: Option<(String, String, Option<i64>)>,
    }
    impl ClerkMock {
        fn ok() -> Self {
            Self {
                membership_create_status: 200,
                org_name: "Acme Inc".to_string(),
                ..Default::default()
            }
        }
    }
    type SharedClerkMock = std::sync::Arc<StdMutex<ClerkMock>>;

    async fn spawn_clerk_mock(initial: ClerkMock) -> (String, SharedClerkMock) {
        let state: SharedClerkMock = std::sync::Arc::new(StdMutex::new(initial));
        let app = axum::Router::new()
            .route(
                "/v1/users",
                axum_get(clerk_find_user_by_email).post(clerk_create_user),
            )
            .route(
                "/v1/users/{user_id}",
                axum_get(clerk_get_user).delete(clerk_delete_user),
            )
            .route(
                "/v1/users/{user_id}/organization_memberships",
                axum_get(clerk_list_user_org_memberships),
            )
            .route(
                "/v1/organizations/{org_id}/memberships",
                axum_post(clerk_create_membership),
            )
            .route(
                "/v1/organizations/{org_id}/memberships/{user_id}",
                axum_delete(clerk_delete_membership),
            )
            .route("/v1/organizations/{org_id}", axum_get(clerk_get_org))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind clerk mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), state)
    }

    async fn clerk_create_user(
        AxumState(state): AxumState<SharedClerkMock>,
        Json(body): Json<Value>,
    ) -> Response {
        let email = body
            .get("email_address")
            .and_then(|v| v.as_array())
            .and_then(|a| a.first())
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let mut s = state.lock().unwrap();
        // The `existing_user` fixture (finding 11) — this email already
        // belongs to a Clerk identity created outside this request, so
        // Clerk itself would reject a second create with 422
        // `form_identifier_exists`, not 200 a duplicate.
        if let Some((existing_email, ..)) = &s.existing_user {
            if existing_email.eq_ignore_ascii_case(&email) {
                return (
                    StatusCode::UNPROCESSABLE_ENTITY,
                    Json(json!({
                        "errors": [{
                            "code": "form_identifier_exists",
                            "message": "identifier exists",
                            "long_message": "That email address is taken. Please try another.",
                        }]
                    })),
                )
                    .into_response();
            }
        }
        s.next_user_seq += 1;
        let id = format!("user_mock_{}", s.next_user_seq);
        s.created_users.push(id.clone());
        (
            StatusCode::OK,
            Json(json!({
                "id": id,
                "email_addresses": [{"id": "idn_1", "email_address": email}],
                "primary_email_address_id": "idn_1",
                "last_sign_in_at": Value::Null,
            })),
        )
            .into_response()
    }

    /// `GET /v1/users?email_address=` — the `create_user` `422
    /// form_identifier_exists` fallback (finding 2/11). Only ever answers
    /// out of the `existing_user` fixture; everything else Clerk-side in
    /// this mock is created fresh through `clerk_create_user` and looked up
    /// by id, never by email.
    async fn clerk_find_user_by_email(
        AxumState(state): AxumState<SharedClerkMock>,
        axum::extract::RawQuery(q): axum::extract::RawQuery,
    ) -> Response {
        let q = q.unwrap_or_default();
        let queried = q
            .split('&')
            .find_map(|p| p.strip_prefix("email_address="))
            .map(|v| {
                urlencoding::decode(v)
                    .map(|c| c.into_owned())
                    .unwrap_or_default()
            })
            .unwrap_or_default();
        let s = state.lock().unwrap();
        let data: Vec<Value> = match &s.existing_user {
            Some((email, clerk_user_id, last_sign_in_at))
                if email.eq_ignore_ascii_case(&queried) =>
            {
                vec![json!({
                    "id": clerk_user_id,
                    "email_addresses": [{"id": "idn_1", "email_address": email}],
                    "primary_email_address_id": "idn_1",
                    "last_sign_in_at": last_sign_in_at,
                })]
            }
            _ => vec![],
        };
        // Documented paginated envelope (fix 2) — NOT a bare array.
        let total = data.len();
        (
            StatusCode::OK,
            Json(json!({"data": data, "total_count": total})),
        )
            .into_response()
    }

    async fn clerk_create_membership(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(_org_id): AxumPath<String>,
        Json(_body): Json<Value>,
    ) -> Response {
        let s = state.lock().unwrap();
        if s.membership_create_status == 200 {
            (StatusCode::OK, Json(json!({"id": "orgmem_mock_1"}))).into_response()
        } else {
            let code = s
                .membership_create_code
                .clone()
                .unwrap_or_else(|| "mock_error".to_string());
            let status = StatusCode::from_u16(s.membership_create_status).unwrap();
            (status, Json(json!({"errors": [{"code": code, "message": "mock error", "long_message": "mock error"}]})))
                .into_response()
        }
    }

    async fn clerk_delete_membership(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath((org_id, user_id)): AxumPath<(String, String)>,
    ) -> Response {
        state
            .lock()
            .unwrap()
            .deleted_memberships
            .push((org_id, user_id));
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    async fn clerk_delete_user(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(user_id): AxumPath<String>,
    ) -> Response {
        state.lock().unwrap().deleted_users.push(user_id);
        (StatusCode::OK, Json(json!({}))).into_response()
    }

    async fn clerk_get_user(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(user_id): AxumPath<String>,
    ) -> Response {
        let s = state.lock().unwrap();
        (
            StatusCode::OK,
            Json(json!({
                "id": user_id,
                "last_sign_in_at": s.get_user_last_sign_in_at,
                "email_addresses": [],
                "primary_email_address_id": Value::Null,
            })),
        )
            .into_response()
    }

    async fn clerk_list_user_org_memberships(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(_user_id): AxumPath<String>,
    ) -> Response {
        let s = state.lock().unwrap();
        let data: Vec<Value> = match &s.membership_orgs {
            Some(orgs) => orgs
                .iter()
                .map(|id| json!({"organization": {"id": id}}))
                .collect(),
            None => (0..s.other_org_count)
                .map(|i| json!({"organization": {"id": format!("org_other_{i}")}}))
                .collect(),
        };
        let total = data.len();
        (
            StatusCode::OK,
            Json(json!({"data": data, "total_count": total})),
        )
            .into_response()
    }

    async fn clerk_get_org(
        AxumState(state): AxumState<SharedClerkMock>,
        AxumPath(org_id): AxumPath<String>,
    ) -> Response {
        let s = state.lock().unwrap();
        (
            StatusCode::OK,
            Json(json!({"id": org_id, "name": s.org_name})),
        )
            .into_response()
    }

    // ── Resend mock ──────────────────────────────────────────────────────────

    #[derive(Default)]
    struct ResendMock {
        status: u16,
        requests: Vec<Value>,
    }
    type SharedResendMock = std::sync::Arc<StdMutex<ResendMock>>;

    async fn spawn_resend_mock(status: u16) -> (String, SharedResendMock) {
        let state: SharedResendMock = std::sync::Arc::new(StdMutex::new(ResendMock {
            status,
            requests: Vec::new(),
        }));
        let app = axum::Router::new()
            .route("/emails", axum_post(resend_send))
            .with_state(state.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind resend mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (format!("http://{addr}"), state)
    }

    async fn resend_send(
        AxumState(state): AxumState<SharedResendMock>,
        Json(body): Json<Value>,
    ) -> Response {
        let mut s = state.lock().unwrap();
        s.requests.push(body);
        let status = StatusCode::from_u16(s.status).unwrap();
        (status, Json(json!({"id": "email_mock_1"}))).into_response()
    }

    // ── Control-DB seeding ───────────────────────────────────────────────────

    /// An org account with one admin member (seat-tiered, per
    /// `account_members_seat_tier_iff_org`). Returns `(account_id,
    /// admin_user_id)`.
    async fn seed_org(control: &PgPool, org_id: &str) -> (Uuid, Uuid) {
        // The admin gets a real NAME: a nameless inviter falls back to their
        // email (`inviter_name`'s COALESCE), which `display_inviter` now
        // drops from the join URL and email copy entirely — the happy-path
        // test below asserts inviter PROPAGATION, so it needs a name to
        // propagate.
        let admin_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email, clerk_user_id, name) VALUES ($1, $2, 'Pg Admin') RETURNING id",
        )
        .bind(format!("{}@org-invites-pg.test", Uuid::new_v4()))
        .bind(format!("user_admin_{}", Uuid::new_v4()))
        .fetch_one(control)
        .await
        .expect("insert admin user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(admin_id)
        .bind(org_id)
        .fetch_one(control)
        .await
        .expect("insert org account");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'admin', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(admin_id)
        .execute(control)
        .await
        .expect("insert admin membership");
        (account_id, admin_id)
    }

    /// A second member row (as if already invited/provisioned), with a
    /// Clerk identity — for the resend/revoke tests, which act on an
    /// EXISTING member.
    async fn seed_member(
        control: &PgPool,
        account_id: Uuid,
        clerk_user_id: &str,
        email: &str,
    ) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(email, clerk_user_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(email)
        .bind(clerk_user_id)
        .fetch_one(control)
        .await
        .expect("insert member user");
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(control)
        .await
        .expect("insert member membership");
        user_id
    }

    fn user_ctx(account_id: Uuid, user_id: Uuid, role: &str) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: role.to_string(),
            device_id: "org-invites-pg-test".to_string(),
        }
    }

    fn org_tenant(control: PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
        let ctx = TenantContext {
            org_id: org_id.to_string(),
            account_id,
            user_id,
            pool: control.clone(),
            page_pool: None,
        };
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(ctx)),
            class: crate::route_class::RouteClass::Other,
        }
    }

    fn app_state(control: PgPool, resend_key: Option<&str>) -> Arc<AppState> {
        let mut state = crate::test_app_state_owned_with_db_and_store(control, None);
        state.resend_api_key = resend_key.map(str::to_string);
        Arc::new(state)
    }

    // ── Scenario 1: happy path ────────────────────────────────────────────────

    #[tokio::test]
    async fn invite_happy_path_creates_local_rows_and_sends_the_join_email() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_happy_path_creates_local_rows_and_sends_the_join_email: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi1_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi1_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let (clerk_base, clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "New.Invitee@Test.dev".to_string(),
                role: "member".to_string(),
                name: Some("New Invitee".to_string()),
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CREATED, "invite must succeed");
        let body = body_json(resp).await;
        assert_eq!(body["pending"], json!(true));
        assert_eq!(body["email_sent"], json!(true));
        assert_eq!(body["role"], json!("member"));
        assert_eq!(
            body["email"],
            json!("new.invitee@test.dev"),
            "email is lowercased/trimmed"
        );

        let clerk_user_id = body["clerk_user_id"].as_str().unwrap().to_string();
        {
            let m = clerk_mock.lock().unwrap();
            assert!(
                m.created_users.contains(&clerk_user_id),
                "create_user must have been called"
            );
            assert!(
                m.deleted_users.is_empty(),
                "no ghost cleanup on the happy path"
            );
        }

        let our_user_id: Uuid = sqlx::query_scalar("SELECT id FROM users WHERE clerk_user_id = $1")
            .bind(&clerk_user_id)
            .fetch_one(&control)
            .await
            .expect("local users row must exist");
        let role: String = sqlx::query_scalar(
            "SELECT role FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(our_user_id)
        .fetch_one(&control)
        .await
        .expect("local account_members row must exist");
        assert_eq!(role, "member");

        // CS-442 (item 40, D8): `create_invite` passes `None` into
        // `mirror_local_membership`'s own `seat_tier_id` parameter — an
        // admin invite always lands on `standard`, unaffected by this PR's
        // approve-side tier control.
        let seat_tier_code: String = sqlx::query_scalar(
            "SELECT st.code FROM account_members am \
               JOIN seat_tiers st ON st.id = am.seat_tier_id \
              WHERE am.account_id = $1 AND am.user_id = $2",
        )
        .bind(account_id)
        .bind(our_user_id)
        .fetch_one(&control)
        .await
        .expect("local account_members row must carry a real seat_tier_id");
        assert_eq!(seat_tier_code, "standard", "an admin invite must still default to standard");

        let requests = resend_mock.lock().unwrap().requests.clone();
        assert_eq!(requests.len(), 1, "exactly one email must have been sent");
        let html = requests[0]["html"].as_str().unwrap();
        assert!(
            html.contains("/join?email=new.invitee%40test.dev"),
            "join link missing/wrong email: {html}"
        );
        assert!(
            html.contains("org=Acme"),
            "join link missing org name: {html}"
        );
        assert!(
            html.contains("inviter=Pg%20Admin"),
            "join link missing inviter name: {html}"
        );

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Scenario 2: ordering/unwind on a membership-create failure ──────────

    #[tokio::test]
    async fn membership_create_failure_deletes_the_ghost_user_and_leaves_no_local_row() {
        let Some(base) = base_url() else {
            eprintln!("skipping membership_create_failure_deletes_the_ghost_user_and_leaves_no_local_row: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi2_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi2_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let mut mock = ClerkMock::ok();
        mock.membership_create_status = 500;
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "doomed@test.dev".to_string(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "a Clerk 5xx must surface as 502"
        );

        {
            let m = clerk_mock.lock().unwrap();
            assert_eq!(
                m.created_users.len(),
                1,
                "create_user must have run before the membership call failed"
            );
            assert_eq!(
                m.deleted_users, m.created_users,
                "the ghost user must be deleted on unwind"
            );
        }

        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM users WHERE email = 'doomed@test.dev'")
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            count, 0,
            "ensure_user is never reached on a membership-create failure — no local row"
        );

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Already-a-member conflict ──────────────────────────────────────────

    #[tokio::test]
    async fn invite_already_a_member_conflict_still_cleans_up_the_ghost_user() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_already_a_member_conflict_still_cleans_up_the_ghost_user: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi7_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi7_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let mut mock = ClerkMock::ok();
        mock.membership_create_status = 422;
        mock.membership_create_code = Some("already_a_member_in_organization".to_string());
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "alreadymember@test.dev".to_string(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = body_json(resp).await;
        assert_eq!(body["error"], json!("already_a_member"));

        let m = clerk_mock.lock().unwrap();
        assert_eq!(m.created_users.len(), 1);
        assert_eq!(
            m.deleted_users, m.created_users,
            "the ghost user must be cleaned up on an already-a-member conflict"
        );
        drop(m);

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // Clerk's dev instance answered a re-invite of an existing member with
    // 400, not 422 (observed 2026-10-20). The code, not the status, is what
    // identifies the conflict.
    #[tokio::test]
    async fn invite_already_a_member_conflict_is_keyed_on_the_code_not_the_status() {
        let Some(base) = base_url() else {
            assert!(
                std::env::var("REQUIRE_PG_SUITE").is_err(),
                "REQUIRE_PG_SUITE set without ORGT_TEST_BASE_URL"
            );
            eprintln!(
                "skipping invite_already_a_member_conflict_is_keyed_on_the_code_not_the_status: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi7b_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi7b_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let mut mock = ClerkMock::ok();
        mock.membership_create_status = 400;
        mock.membership_create_code = Some("already_a_member_in_organization".to_string());
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "alreadymember400@test.dev".to_string(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = body_json(resp).await;
        assert_eq!(body["error"], json!("already_a_member"));

        let m = clerk_mock.lock().unwrap();
        assert_eq!(
            m.deleted_users, m.created_users,
            "the ghost user must be cleaned up on an already-a-member conflict"
        );
        drop(m);

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Scenario 5: email failure never rolls back the invite ───────────────

    #[tokio::test]
    async fn invite_with_resend_failure_still_adds_the_member() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping invite_with_resend_failure_still_adds_the_member: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi3_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi3_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let (clerk_base, _clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(500).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "emailfail@test.dev".to_string(),
                role: "admin".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "an email failure must never roll back the invite"
        );
        let body = body_json(resp).await;
        assert_eq!(body["email_sent"], json!(false));
        assert_eq!(body["role"], json!("admin"));

        let count: i64 =
            sqlx::query_scalar("SELECT count(*) FROM users WHERE email = 'emailfail@test.dev'")
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            count, 1,
            "the member must still be added despite the email failure"
        );

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Scenario 4: revoke GC guard, both branches ───────────────────────────

    #[tokio::test]
    async fn revoke_does_not_delete_a_user_who_belongs_to_another_org() {
        let Some(base) = base_url() else {
            eprintln!("skipping revoke_does_not_delete_a_user_who_belongs_to_another_org: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi4_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi4_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;
        let clerk_user_id = format!("user_target_{}", Uuid::new_v4());
        let target_user_id =
            seed_member(&control, account_id, &clerk_user_id, "target1@test.dev").await;

        let mut mock = ClerkMock::ok();
        mock.get_user_last_sign_in_at = None; // never signed in
        mock.other_org_count = 1; // belongs to one other org
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), None);
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = revoke_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Path(target_user_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["revoked"], json!(true));
        assert_eq!(body["user_deleted"], json!(false));

        let m = clerk_mock.lock().unwrap();
        assert_eq!(
            m.deleted_memberships,
            vec![(org_id.clone(), clerk_user_id.clone())]
        );
        assert!(
            m.deleted_users.is_empty(),
            "a user who belongs to another org must never be deleted"
        );
        drop(m);

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    #[tokio::test]
    async fn revoke_deletes_a_never_signed_in_orphaned_ghost_user() {
        let Some(base) = base_url() else {
            eprintln!("skipping revoke_deletes_a_never_signed_in_orphaned_ghost_user: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi5_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi5_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;
        let clerk_user_id = format!("user_target_{}", Uuid::new_v4());
        let target_user_id =
            seed_member(&control, account_id, &clerk_user_id, "target2@test.dev").await;

        let mut mock = ClerkMock::ok();
        mock.get_user_last_sign_in_at = None; // never signed in
        mock.other_org_count = 0; // no other orgs
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), None);
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = revoke_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Path(target_user_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp).await;
        assert_eq!(body["revoked"], json!(true));
        assert_eq!(body["user_deleted"], json!(true));

        let m = clerk_mock.lock().unwrap();
        assert_eq!(
            m.deleted_memberships,
            vec![(org_id.clone(), clerk_user_id.clone())]
        );
        assert_eq!(m.deleted_users, vec![clerk_user_id.clone()]);
        drop(m);

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Resend endpoint ───────────────────────────────────────────────────────

    #[tokio::test]
    async fn resend_invite_resends_the_email_and_surfaces_failure() {
        let Some(base) = base_url() else {
            eprintln!("skipping resend_invite_resends_the_email_and_surfaces_failure: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi6_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi6_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;
        let clerk_user_id = format!("user_target_{}", Uuid::new_v4());
        let target_user_id =
            seed_member(&control, account_id, &clerk_user_id, "resendme@test.dev").await;

        let (clerk_base, _clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, resend_mock) = spawn_resend_mock(500).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = resend_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Path(target_user_id),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "resend never fails the request itself"
        );
        let body = body_json(resp).await;
        assert_eq!(body["email_sent"], json!(false));

        let requests = resend_mock.lock().unwrap().requests.clone();
        assert_eq!(requests.len(), 1);
        assert!(requests[0]["html"]
            .as_str()
            .unwrap()
            .contains("resendme%40test.dev"));

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Scenario 3: Stripe-driven seat-purchase failure unwinds the invite ──

    /// An org account with one ADMIN member (seat-tiered, seat unpurchased)
    /// and a LINKED, ACTIVE `stripe_subscriptions` row — the minimum
    /// `acquire_seat` needs to actually reach a Stripe call (see
    /// `seat_lifecycle::live_subscription_id_for_acquire`) rather than
    /// no-op as "no live subscription".
    async fn seed_org_with_active_subscription(control: &PgPool, org_id: &str) -> (Uuid, Uuid) {
        let (account_id, admin_id) = seed_org(control, org_id).await;
        let mut financial = crate::tenancy::scope::begin_account_scoped(control, account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query(
            "INSERT INTO stripe_subscriptions (account_id, stripe_customer_id, stripe_subscription_id, status) \
             VALUES ($1, $2, $3, 'active')",
        )
        .bind(account_id)
        .bind(format!("cus_{}", Uuid::new_v4().simple()))
        .bind(format!("sub_{}", Uuid::new_v4().simple()))
        .execute(&mut *financial)
        .await
        .expect("link stripe subscription");
        financial.commit().await.unwrap();
        // Migrations seed `seat_tiers` rows with a NULL `stripe_price_id`
        // (real provisioning fills it). `acquire_seat`'s `seat_tier_price`
        // treats NULL as an INTERNAL error (Db → 500) before any Stripe
        // call, so without this the two Stripe-unwind tests never reach the
        // mock at all.
        sqlx::query(
            "UPDATE seat_tiers SET stripe_price_id = 'price_mock_' || code \
             WHERE stripe_price_id IS NULL",
        )
        .execute(control)
        .await
        .expect("provision seat tier prices");
        (account_id, admin_id)
    }

    /// A local axum server standing in for Stripe: `GET
    /// /v1/subscriptions/{id}` always 500s, so `acquire_seat`'s very first
    /// Stripe call fails — `retrieve_subscription` returns
    /// `StripeError::Api`, before any DB write. This is enough to prove the
    /// unwind contract; it does not need to model a real subscription-item
    /// shape at all.
    async fn spawn_failing_stripe_mock() -> crate::billing::stripe_client::StripeClient {
        async fn always_fail() -> Response {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [("content-type", "application/json")],
                json!({"error": {"type": "api_error", "message": "mock Stripe outage"}})
                    .to_string(),
            )
                .into_response()
        }
        let app = axum::Router::new().route("/v1/subscriptions/{id}", axum_get(always_fail));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
            .with_base_url(format!("http://{addr}"))
    }

    #[tokio::test]
    async fn a_stripe_failure_during_acquire_seat_unwinds_the_whole_invite() {
        let Some(base) = base_url() else {
            eprintln!("skipping a_stripe_failure_during_acquire_seat_unwinds_the_whole_invite: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oi8_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi8_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org_with_active_subscription(&control, &org_id).await;

        let (clerk_base, clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let mut state = crate::test_app_state_owned_with_db_and_store(control.clone(), None);
        state.stripe = Some(std::sync::Arc::new(spawn_failing_stripe_mock().await));
        let state = Arc::new(state);
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: "seatfail@test.dev".to_string(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;

        // The mock returns a Stripe 5xx ("mock Stripe outage") — a
        // transient/upstream failure, not a card/payment problem, so per
        // finding 7's corrected mapping this must be 502 `billing_unavailable`,
        // NOT 402 `seat_purchase_failed` (the old, blanket-402 behavior).
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "a Stripe 5xx (transient upstream failure) must surface as 502, not 402"
        );
        let body = body_json(resp).await;
        assert_eq!(body["error"], json!("billing_unavailable"));

        let created_id = {
            let m = clerk_mock.lock().unwrap();
            assert_eq!(m.created_users.len(), 1, "create_user must have run");
            let id = m.created_users[0].clone();
            assert_eq!(
                m.deleted_memberships,
                vec![(org_id.clone(), id.clone())],
                "the Clerk membership must be deleted on unwind"
            );
            assert_eq!(
                m.deleted_users,
                vec![id.clone()],
                "the ghost user must be deleted on unwind"
            );
            id
        };
        let _ = created_id;

        // The LOCAL `account_members` row is NOT synchronously rolled back:
        // `ensure_org_membership` commits its own membership-insert
        // transaction BEFORE ever calling `acquire_seat` (Stripe pushes
        // happen strictly after the local write commits — see that
        // function's own doc comment, "Seat acquisition"). So it still
        // exists at this point; cleanup rides the async
        // `organizationMembership.deleted` webhook the Clerk membership
        // delete above just triggered (`clerk_provision::
        // remove_org_membership`), which this direct-function-call test
        // never fires — this is the one place the unwind is genuinely
        // eventual, not synchronous, and this assertion documents that (see
        // this module's own "two-webhook race" doc comment).
        let member_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id <> $2",
        )
        .bind(account_id)
        .bind(admin_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            member_count, 1,
            "the local account_members row survives the Stripe failure synchronously — see comment above"
        );

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Finding 1: unwind must not permanently wedge re-inviting the email ──

    /// invite → force unwind at the Stripe-seat-purchase step (leaving a
    /// local `users` row `ensure_user` already linked, per `unwind_clerk`'s
    /// own doc comment) → invite the SAME email again with Stripe healthy →
    /// must succeed, not 500 forever. Before finding 1's fix, the first
    /// unwind deleted the ghost Clerk user but left `users.clerk_user_id`
    /// pointing at it, permanently blocking `ensure_user`'s `WHERE
    /// clerk_user_id IS NULL` relink guard on every future invite of this
    /// email.
    #[tokio::test]
    async fn reinviting_the_same_email_after_an_unwound_invite_succeeds() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping reinviting_the_same_email_after_an_unwound_invite_succeeds: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi9_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi9_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org_with_active_subscription(&control, &org_id).await;

        let (clerk_base, clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let mut state = crate::test_app_state_owned_with_db_and_store(control.clone(), None);
        state.stripe = Some(std::sync::Arc::new(spawn_failing_stripe_mock().await));
        let state = Arc::new(state);

        let email = "wedge@test.dev".to_string();

        // First attempt: Stripe fails during `acquire_seat`, so
        // `create_invite` unwinds — deleting the just-created Clerk user
        // AFTER `ensure_user` already linked a local `users` row to it.
        let resp1 = create_invite(
            State(state.clone()),
            org_tenant(control.clone(), &org_id, account_id, admin_id),
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: email.clone(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(resp1.status(), StatusCode::BAD_GATEWAY);

        let stale_link: Option<String> =
            sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE email = $1")
                .bind(&email)
                .fetch_one(&control)
                .await
                .expect("ensure_user must have linked a local row before the Stripe failure");
        assert!(
            stale_link.is_none(),
            "the unwind must null out the stale clerk_user_id link, not leave it dangling"
        );

        // Drop the account's Stripe link so the second attempt's
        // `acquire_seat` is a clean no-op (`seat_lifecycle::
        // acquire_seat_is_a_noop_without_a_live_subscription`) — this test
        // is about the `users.email` conflict, not a second Stripe round
        // trip succeeding.
        let mut financial = crate::tenancy::scope::begin_account_scoped(&control, account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        sqlx::query("DELETE FROM stripe_subscriptions WHERE account_id = $1")
            .bind(account_id)
            .execute(&mut *financial)
            .await
            .unwrap();
        financial.commit().await.unwrap();

        let resp2 = create_invite(
            State(state),
            org_tenant(control.clone(), &org_id, account_id, admin_id),
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: email.clone(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(
            resp2.status(),
            StatusCode::CREATED,
            "re-inviting the same email after an unwound invite must succeed, not 500 forever"
        );

        let new_clerk_user_id: Option<String> =
            sqlx::query_scalar("SELECT clerk_user_id FROM users WHERE email = $1")
                .bind(&email)
                .fetch_one(&control)
                .await
                .unwrap();
        assert!(
            new_clerk_user_id.is_some(),
            "the second invite must (re)link a fresh Clerk identity"
        );

        {
            let m = clerk_mock.lock().unwrap();
            assert_eq!(
                m.created_users.len(),
                2,
                "both invite attempts must call create_user"
            );
        }

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Finding 2: stale organizationMembership.deleted webhook guard ───────

    /// A synthetic `organizationMembership.deleted` webhook payload, matching
    /// what `routes::clerk_webhook::handle_membership_deleted` reads.
    fn membership_deleted_payload(org_id: &str, clerk_user_id: &str) -> Value {
        json!({
            "organization": {"id": org_id},
            "public_user_data": {"user_id": clerk_user_id},
        })
    }

    #[tokio::test]
    async fn stale_membership_deleted_webhook_is_skipped_when_clerk_still_reports_the_membership() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping stale_membership_deleted_webhook_is_skipped_when_clerk_still_reports_the_membership: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi10_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi10_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;
        let clerk_user_id = format!("user_target_{}", Uuid::new_v4());
        let target_user_id =
            seed_member(&control, account_id, &clerk_user_id, "stale1@test.dev").await;

        let mut mock = ClerkMock::ok();
        // Clerk still reports this exact org membership as current — the
        // `.deleted` delivery below must be treated as stale.
        mock.membership_orgs = Some(vec![org_id.clone()]);
        let (clerk_base, _clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), None);
        let data = membership_deleted_payload(&org_id, &clerk_user_id);
        let result =
            crate::routes::clerk_webhook::handle_membership_deleted(&state, &control, &data).await;
        assert!(result.is_ok());

        let member_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(target_user_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            member_count, 1,
            "a stale .deleted delivery must not remove a still-current membership"
        );

        let _ = admin_id;
        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    #[tokio::test]
    async fn a_genuine_membership_deleted_webhook_still_removes_the_membership() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping a_genuine_membership_deleted_webhook_still_removes_the_membership: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi11_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi11_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;
        let clerk_user_id = format!("user_target_{}", Uuid::new_v4());
        let target_user_id =
            seed_member(&control, account_id, &clerk_user_id, "stale2@test.dev").await;

        let mut mock = ClerkMock::ok();
        // Clerk reports NO memberships for this user at all — the `.deleted`
        // delivery is genuine and must proceed.
        mock.membership_orgs = Some(vec![]);
        let (clerk_base, _clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), None);
        let data = membership_deleted_payload(&org_id, &clerk_user_id);
        let result =
            crate::routes::clerk_webhook::handle_membership_deleted(&state, &control, &data).await;
        assert!(result.is_ok());

        let member_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id = $2",
        )
        .bind(account_id)
        .bind(target_user_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(
            member_count, 0,
            "a genuine .deleted delivery must still remove the membership"
        );

        let _ = admin_id;
        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── Finding 11: existing-user reuse path (form_identifier_exists) ───────
    //
    // The mock's create-user used to always 200 with a fresh id, so the
    // `create_user` → 422 `form_identifier_exists` → `find_user_by_email`
    // reuse fallback, and the resulting `CreatedUser { newly_created: false,
    // .. }`, were never exercised by this suite — including the
    // highest-consequence invariant the whole ghost-cleanup design rests on:
    // a failure AFTER reusing an existing identity must never delete that
    // identity, only ones this request itself created.

    #[tokio::test]
    async fn invite_reuses_an_existing_clerk_identity_via_form_identifier_exists_fallback() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping invite_reuses_an_existing_clerk_identity_via_form_identifier_exists_fallback: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi12_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi12_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let email = "reused@test.dev".to_string();
        let existing_clerk_user_id = format!("user_existing_{}", Uuid::new_v4());
        let mut mock = ClerkMock::ok();
        // Already exists in Clerk, and has ALREADY signed in — proves
        // `pending` is genuinely derived from the reused identity's
        // `last_sign_in_at`, not hardcoded true/false either way.
        mock.existing_user = Some((
            email.clone(),
            existing_clerk_user_id.clone(),
            Some(1_700_000_000_000),
        ));
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: email.clone(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::CREATED,
            "reusing an existing Clerk identity must still succeed"
        );
        let body = body_json(resp).await;
        assert_eq!(body["clerk_user_id"], json!(existing_clerk_user_id));
        assert_eq!(
            body["pending"],
            json!(false),
            "pending must reflect the reused identity's real last_sign_in_at, not be hardcoded"
        );

        {
            let m = clerk_mock.lock().unwrap();
            assert!(
                m.created_users.is_empty(),
                "the existing identity must be REUSED via find_user_by_email, never genuinely \
                 created"
            );
        }

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    #[tokio::test]
    async fn membership_create_failure_after_reusing_an_existing_identity_never_deletes_it() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping membership_create_failure_after_reusing_an_existing_identity_never_deletes_it: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_oi13_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;

        let org_id = format!("org_oi13_{}", Uuid::new_v4());
        let (account_id, admin_id) = seed_org(&control, &org_id).await;

        let email = "reused-fail@test.dev".to_string();
        let existing_clerk_user_id = format!("user_existing_{}", Uuid::new_v4());
        let mut mock = ClerkMock::ok();
        mock.existing_user = Some((email.clone(), existing_clerk_user_id.clone(), None));
        // The step AFTER reuse fails — proves the unwind path (which deletes
        // a ghost user ONLY when `created.newly_created`) correctly leaves a
        // reused identity alone.
        mock.membership_create_status = 500;
        let (clerk_base, clerk_mock) = spawn_clerk_mock(mock).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;

        let state = app_state(control.clone(), Some("resend_test_key"));
        let tenant = org_tenant(control.clone(), &org_id, account_id, admin_id);

        let resp = create_invite(
            State(state),
            tenant,
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(InviteRequest {
                email: email.clone(),
                role: "member".to_string(),
                name: None,
                allow_purchase: true,
            }),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "a Clerk 5xx on membership create must surface as 502"
        );

        {
            let m = clerk_mock.lock().unwrap();
            assert!(
                m.created_users.is_empty(),
                "the reused identity was never genuinely created by this request"
            );
            assert!(
                m.deleted_users.is_empty(),
                "the highest-consequence invariant: never delete a pre-existing Clerk identity \
                 this request did not create"
            );
        }

        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM users WHERE email = $1")
            .bind(&email)
            .fetch_one(&control)
            .await
            .unwrap();
        assert_eq!(
            count, 0,
            "ensure_user is never reached on a membership-create failure — no local row"
        );

        control.close().await;
        retained_database::finish(&mut conn, db, true)
            .await
            .ok();
    }

    // ── allow_purchase (PR #1515 review): the invite never buys a seat unless
    //    the caller allows it ─────────────────────────────────────────────────
    //
    // The handler runs on a `coralswarm_app` pool (a real login), the role
    // prod's control plane connects as, so the seat pre-check and the
    // in-lock refusal are proven under RLS. Seeding and assertions use the
    // owner pool.

    async fn app_pool(owner: &PgPool, base: &str, db: &str) -> PgPool {
        crate::app_role_test_support::app_role_pool(owner, &with_db(base, db)).await
    }

    /// A Stripe stand-in that counts every request. `GET
    /// /v1/subscriptions/{id}` serves a subscription with one `standard`
    /// item; every other request (the quantity update) gets the same 200
    /// body, which is all `acquire_seat` needs from it.
    async fn spawn_counting_stripe_mock() -> (
        crate::billing::stripe_client::StripeClient,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let hits = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = hits.clone();
        let body = json!({
            "id": "sub_mock_1",
            "object": "subscription",
            "status": "active",
            "items": {"object": "list", "data": [{
                "id": "si_mock_1",
                "quantity": 1,
                "current_period_start": 1_700_000_000,
                "current_period_end": 1_702_678_400,
                "price": {"id": "price_mock_standard"},
            }]},
        });
        let app = axum::Router::new().fallback(axum::routing::any(move || {
            let counter = counter.clone();
            let body = body.clone();
            async move {
                counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                (StatusCode::OK, [("content-type", "application/json")], body.to_string())
            }
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind stripe mock");
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        (
            crate::billing::stripe_client::StripeClient::new(reqwest::Client::new(), "sk_test_mock")
                .with_base_url(format!("http://{addr}")),
            hits,
        )
    }

    /// [`seed_org_with_active_subscription`] plus the `team` grant a live
    /// subscription implies, with `standard_seats` purchased `standard`
    /// seats in the ledger. The seeded admin's own seat is unpurchased, so
    /// all `standard_seats` are free.
    async fn seed_billed_org(control: &PgPool, org_id: &str, standard_seats: i32) -> (Uuid, Uuid, Uuid) {
        let (account_id, admin_id) = seed_org_with_active_subscription(control, org_id).await;
        let mut financial = crate::tenancy::scope::begin_account_scoped(control, account_id).await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut financial, account_id).await.unwrap();
        let grant_id: Uuid = sqlx::query_scalar(
            "INSERT INTO grants (account_id, plan_id, seats, plan_is_seat_priced) \
             SELECT $1, id, $2, true FROM plans WHERE code = $3 RETURNING id",
        )
        .bind(account_id)
        .bind(standard_seats.max(1))
        .bind(crate::billing::MIXED_TIER_PLAN_CODE)
        .fetch_one(&mut *financial)
        .await
        .expect("insert team grant");
        if standard_seats > 0 {
            sqlx::query(
                "INSERT INTO grant_seat_tiers (grant_id, seat_tier_id, seats) \
                 SELECT $1, id, $2 FROM seat_tiers WHERE code = 'standard' AND is_active = true",
            )
            .bind(grant_id)
            .bind(standard_seats)
            .execute(&mut *financial)
            .await
            .expect("seed purchased standard seats");
        }
        financial.commit().await.unwrap();
        (account_id, admin_id, grant_id)
    }

    async fn standard_ledger(control: &PgPool, grant_id: Uuid) -> Option<i32> {
        sqlx::query_scalar(
            "SELECT gst.seats FROM grant_seat_tiers gst JOIN seat_tiers st ON st.id = gst.seat_tier_id \
              WHERE gst.grant_id = $1 AND st.code = 'standard'",
        )
        .bind(grant_id)
        .fetch_optional(control)
        .await
        .unwrap()
    }

    fn invite_body(email: &str, allow_purchase: Option<bool>) -> InviteRequest {
        // Through serde, so the default for an omitted field is what's tested.
        let mut v = json!({"email": email, "role": "member"});
        if let Some(allow) = allow_purchase {
            v["allow_purchase"] = json!(allow);
        }
        serde_json::from_value(v).expect("valid InviteRequest")
    }

    #[test]
    fn allow_purchase_defaults_to_true_for_older_clients() {
        assert!(invite_body("a@test.dev", None).allow_purchase);
        assert!(!invite_body("a@test.dev", Some(false)).allow_purchase);
    }

    /// `allow_purchase: false` on a live subscription with no free seat: 409
    /// `no_free_seat`, before anything is created — no Clerk user, no
    /// membership, no local row, no Stripe call, no ledger change.
    #[tokio::test]
    async fn invite_without_purchase_on_a_full_pool_is_refused_and_buys_nothing() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_without_purchase_on_a_full_pool_is_refused_and_buys_nothing: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oinp1_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let app = app_pool(&control, &base, db).await;

        let org_id = format!("org_oinp1_{}", Uuid::new_v4());
        let (account_id, admin_id, grant_id) = seed_billed_org(&control, &org_id, 0).await;

        let (clerk_base, clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;
        let (stripe, stripe_hits) = spawn_counting_stripe_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(app.clone(), None);
        state.stripe = Some(Arc::new(stripe));

        let resp = create_invite(
            State(Arc::new(state)),
            org_tenant(app.clone(), &org_id, account_id, admin_id),
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(invite_body("full@test.dev", Some(false))),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT);
        let body = body_json(resp).await;
        assert_eq!(body["code"], json!("no_free_seat"));

        assert_eq!(stripe_hits.load(std::sync::atomic::Ordering::SeqCst), 0, "no Stripe call");
        assert_eq!(standard_ledger(&control, grant_id).await, None, "no seat added to the ledger");
        {
            let m = clerk_mock.lock().unwrap();
            assert!(m.created_users.is_empty(), "no Clerk user created");
        }
        assert!(resend_mock.lock().unwrap().requests.is_empty(), "no email sent");
        let invitee_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM account_members WHERE account_id = $1 AND user_id <> $2",
        )
        .bind(account_id)
        .bind(admin_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(invitee_rows, 0, "no membership row");

        app.close().await;
        control.close().await;
        retained_database::finish(&mut conn, db, true).await.ok();
    }

    /// `allow_purchase: false` with a free purchased seat: the invite goes
    /// through and takes that seat, with no Stripe call and no ledger change.
    #[tokio::test]
    async fn invite_without_purchase_uses_a_free_seat() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_without_purchase_uses_a_free_seat: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oinp2_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let app = app_pool(&control, &base, db).await;

        let org_id = format!("org_oinp2_{}", Uuid::new_v4());
        let (account_id, admin_id, grant_id) = seed_billed_org(&control, &org_id, 2).await;

        let (clerk_base, _clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;
        let (stripe, stripe_hits) = spawn_counting_stripe_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(app.clone(), None);
        state.stripe = Some(Arc::new(stripe));

        let resp = create_invite(
            State(Arc::new(state)),
            org_tenant(app.clone(), &org_id, account_id, admin_id),
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(invite_body("room@test.dev", Some(false))),
        )
        .await;
        if resp.status() != StatusCode::CREATED {
            let status = resp.status();
            panic!("a free seat must be used: {status} {:?}", body_json(resp).await);
        }

        assert_eq!(stripe_hits.load(std::sync::atomic::Ordering::SeqCst), 0, "no Stripe call");
        assert_eq!(standard_ledger(&control, grant_id).await, Some(2), "the ledger is unchanged");
        let stamped: Option<chrono::DateTime<chrono::Utc>> = sqlx::query_scalar(
            "SELECT am.seat_purchased_at FROM account_members am JOIN users u ON u.id = am.user_id \
              WHERE am.account_id = $1 AND u.email = 'room@test.dev'",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .expect("the invitee's membership row");
        assert!(stamped.is_some(), "the invitee holds the free seat");

        app.close().await;
        control.close().await;
        retained_database::finish(&mut conn, db, true).await.ok();
    }

    /// No `allow_purchase` in the body (an older client): the invite still
    /// buys the missing seat, as before this field existed.
    #[tokio::test]
    async fn invite_without_the_field_still_buys_a_seat() {
        let Some(base) = base_url() else {
            eprintln!("skipping invite_without_the_field_still_buys_a_seat: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_oinp3_ctl";
        let mut conn = admin_conn(&base).await;
        recreate(&mut conn, db).await;
        let control = control_pool(&base, db).await;
        let app = app_pool(&control, &base, db).await;

        let org_id = format!("org_oinp3_{}", Uuid::new_v4());
        let (account_id, admin_id, grant_id) = seed_billed_org(&control, &org_id, 0).await;

        let (clerk_base, _clerk_mock) = spawn_clerk_mock(ClerkMock::ok()).await;
        let (resend_base, _resend_mock) = spawn_resend_mock(200).await;
        let _env = EnvGuard::acquire(&clerk_base, &resend_base).await;
        let (stripe, stripe_hits) = spawn_counting_stripe_mock().await;
        let mut state = crate::test_app_state_owned_with_db_and_store(app.clone(), None);
        state.stripe = Some(Arc::new(stripe));

        let resp = create_invite(
            State(Arc::new(state)),
            org_tenant(app.clone(), &org_id, account_id, admin_id),
            Extension(user_ctx(account_id, admin_id, "admin")),
            Json(invite_body("buys@test.dev", None)),
        )
        .await;
        if resp.status() != StatusCode::CREATED {
            let status = resp.status();
            panic!("the default still buys: {status} {:?}", body_json(resp).await);
        }

        assert!(stripe_hits.load(std::sync::atomic::Ordering::SeqCst) > 0, "Stripe was called to buy the seat");
        assert_eq!(standard_ledger(&control, grant_id).await, Some(1), "one seat bought into the ledger");

        app.close().await;
        control.close().await;
        retained_database::finish(&mut conn, db, true).await.ok();
    }
}
