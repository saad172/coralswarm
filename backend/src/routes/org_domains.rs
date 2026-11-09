//! Org domain verification (CS-443, org-onboarding plan §3 item 37,
//! `docs/plans/org-onboarding-from-the-invite.md`, D11):
//!
//! - `POST   /v1/orgs/domains`                    — admin: claim a domain;
//!   responds with the DNS TXT record name + value to publish.
//! - `GET    /v1/orgs/domains`                    — admin: every claimed
//!   domain, its verified state, and WHICH method proved it (D11).
//! - `POST   /v1/orgs/domains/{id}/verify`        — admin: check the DNS TXT
//!   record now.
//! - `POST   /v1/orgs/domains/{id}/confirmation`  — admin: mail a
//!   confirmation link to an address **at that domain**.
//! - `DELETE /v1/orgs/domains/{id}`               — admin: remove a domain.
//! - `GET|POST /v1/orgs/domains/confirm?t=…`      — **public**: the emailed
//!   link. GET renders a confirmation form and verifies NOTHING; POST is
//!   what actually verifies.
//!
//! # Auth
//!
//! Every route but `confirm` is admin-only, through the SAME
//! [`require_org_admin`] gate `routes::org_invites`/`routes::org_slots`/
//! `routes::org_join_requests` use, PLUS the personal-org refusal those
//! modules also carry — `require_org_admin` alone cannot tell a personal org
//! apart from a real one (a personal org's single member always holds
//! `role = "owner"`), and a personal org has no domain to verify and no
//! auto-add to arm. Unlike `org_slots`, the LIST here is admin-only too: a
//! verification token is a credential (whoever publishes it in DNS proves
//! control), so it is not an ordinary-member read.
//!
//! # The two proofs are not interchangeable (founder ruling, 2026-09-22)
//!
//! Both prove control and both are recorded, but they arm different things:
//!
//! ```text
//! verified by DNS   -> D2 guest rule + D16 rung 2 / auto-add
//! verified by email -> D2 guest rule only
//! unverified        -> neither
//! ```
//!
//! Every read here carries `method` for exactly that reason — a settings
//! surface that renders the two identically tells an admin they have armed
//! auto-add when they have not. See `services::org_domains`' own "belt and
//! braces" doc section for the full ruling.
//!
//! # Why `confirm` is public, and why GET does not verify
//!
//! D11's second proof is *"a confirmation link sent to an address at that
//! domain"*. The whole point is that whoever controls `postmaster@acme.com`
//! can answer — that person may not be a CoralSwarm admin, may not be a
//! CoralSwarm user at all, and certainly has no session. So the route is
//! mounted on the `open` router, and the signed token IS the entire
//! credential, the same trust model `routes::email_unsubscribe` and
//! `routes::shares`' capability viewers use.
//!
//! **GET renders a form and takes no action** — the identical rule
//! CodeRabbit round 1 (#1005) forced on `email_unsubscribe`, and here it is
//! load-bearing for security rather than merely for politeness: mail
//! security link scanners fetch every URL in a message body automatically,
//! so a GET that verified on the spot would let a scanner in the recipient's
//! own mail pipeline prove control of a domain that nobody ever clicked for.
//! Verification is a `POST` a human submits.
//!
//! # "Could not ask" is not "not published"
//!
//! `POST /verify` maps a resolver failure to **502**, and a resolver that
//! answered with no matching record to **409**. That split is the same
//! discipline `connectors::google_chat::fetch_domain_directory` states for
//! its own zero-results case and D16 calls "the trap in the strongest
//! signal": an admin who is told "your DNS record is wrong" when the truth
//! is "we could not reach a resolver" goes and breaks a record that was
//! fine.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{Html, IntoResponse, Response};
use axum::{Extension, Json};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::path as utoipa_path;
use utoipa::ToSchema;
use uuid::Uuid;

use crate::middleware::auth::UserContext;
use crate::services::org_domains as svc;
use crate::tenancy::extractor::Tenant;
use crate::tenancy::runtime::TenantContext;
use crate::AppState;

use super::org_invites::require_org_admin;

// ── Response/error helpers (module convention — see `org_invites.rs`,
//    `org_slots.rs`; this crate has no central `ApiError`) ─────────────────

fn bad_request(msg: &str) -> Response {
    (StatusCode::BAD_REQUEST, Json(json!({"error": msg}))).into_response()
}
fn forbidden(msg: &str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not_found"}))).into_response()
}
fn conflict(msg: &str) -> Response {
    (StatusCode::CONFLICT, Json(json!({"error": msg}))).into_response()
}
fn bad_gateway(msg: &str) -> Response {
    (StatusCode::BAD_GATEWAY, Json(json!({"error": msg}))).into_response()
}
/// The TRANSIENT 503 — a control-plane blip (`state.control_db()` is
/// `None`), not a fact about this deployment's configuration. Retryable: the
/// same request may succeed a moment later with no change on the caller's
/// part. Every route in this module uses this SAME `error` string for this
/// SAME condition — see [`confirmation_not_configured`] for the one route
/// (`request_confirmation`) that also has a second, DIFFERENT 503 cause, and
/// why the two must not share a body.
fn service_unavailable() -> Response {
    (StatusCode::SERVICE_UNAVAILABLE, Json(json!({"error": "service unavailable"}))).into_response()
}

/// The PERMANENT 503 — `request_confirmation` ONLY, when
/// `DOMAIN_VERIFICATION_SECRET` or `RESEND_API_KEY` is unset. A fact about
/// THIS deployment, not this request: retrying changes nothing, and the DNS
/// proof (`POST .../verify`) is completely unaffected and remains available.
///
/// Deliberately a DIFFERENT `error` string from [`service_unavailable`]'s
/// plain `"service unavailable"` — house convention for a 503 whose cause a
/// caller needs to distinguish, the same shape `routes::org_invites::
/// map_stripe_error_to_response`/`map_clerk_error` use (`"billing_unavailable"`,
/// `"clerk_misconfigured"`, `"not_configured"`) for their own "config vs.
/// transient" 503 splits.
///
/// Before this function existed, EVERY 503 from `request_confirmation` —
/// `state.control_db()` being `None` included — carried the same plain
/// `service_unavailable()` body, and the endpoint's own `utoipa` annotation
/// (wrongly) documented that single shared body as meaning "email sending is
/// not configured on this deployment". A caller reading the annotation, not
/// the handler, had no way to learn that a control-plane blip and a genuine
/// missing-config deployment fact were the same response — this function,
/// and the narrowed call sites in `request_confirmation`, are the fix.
fn confirmation_not_configured() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "email_confirmation_not_configured"})),
    )
        .into_response()
}
fn internal_error(what: &str, e: impl std::fmt::Display) -> Response {
    tracing::error!(error = %e, step = what, "org_domains: failed");
    (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({"error": "internal error"}))).into_response()
}

/// Same bar `routes::org_slots::refuse_personal_org` keeps for its own admin
/// actions — see that function's own doc comment for why
/// `account_is_real_org` needs the caller's own `user_id`.
async fn refuse_personal_org(state: &AppState, ctx: &TenantContext) -> Result<(), Response> {
    let Some(store) = state.control_store() else {
        return Err(service_unavailable());
    };
    match store.account_is_real_org(ctx.account_id, ctx.user_id).await {
        Ok(true) => Ok(()),
        Ok(false) => Err(forbidden("personal accounts have no org domains")),
        Err(e) => Err(internal_error("account lookup", e)),
    }
}

/// `require_org_admin` + the personal-org refusal, in the one order every
/// admin route below needs them.
async fn admin_ctx<'a>(
    state: &AppState,
    tenant: &'a Tenant,
    user: &UserContext,
) -> Result<&'a TenantContext, Response> {
    let ctx = require_org_admin(tenant, user)?;
    refuse_personal_org(state, ctx).await?;
    Ok(ctx)
}

// ── Wire shapes ───────────────────────────────────────────────────────────

#[derive(Debug, Deserialize, ToSchema)]
pub struct ClaimDomainRequest {
    /// The domain to claim, e.g. `acme.com`. Normalised server-side
    /// (`services::org_domains::normalize_domain`): case, a leading `@`, and
    /// a trailing root dot are all accepted and folded away; a URL, an email
    /// address, a single label, or a public mail provider is refused 400.
    pub domain: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrgDomainResponse {
    pub id: Uuid,
    pub domain: String,
    pub verified: bool,
    /// `"dns_txt"` or `"email"` once verified, `null` before — D11's *"the
    /// org's settings show which one proved it"*, carried on every read so
    /// no surface has to re-derive it.
    ///
    /// **It is not cosmetic.** Founder ruling, 2026-09-22: a `"dns_txt"`
    /// domain anchors D2's guest rule AND may arm auto-add (D16 rung 2); an
    /// `"email"` domain anchors the guest rule ONLY. A settings surface
    /// rendering these two identically would be telling an admin they have
    /// armed something they have not — see `services::org_domains`' own
    /// "belt and braces" doc section.
    pub method: Option<String>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub verified_at: Option<chrono::DateTime<chrono::Utc>>,
    #[schema(value_type = String, format = DateTime)]
    pub claimed_at: chrono::DateTime<chrono::Utc>,
    /// The TXT record name to publish.
    pub dns_record_name: String,
    /// The TXT record value to publish.
    pub dns_record_value: String,
    /// The address the last confirmation email was aimed at. When `method`
    /// is `"email"` this IS the address that proved the domain (a verified
    /// row refuses further confirmation sends); otherwise it is only the
    /// last attempt, and must not be rendered as proof.
    pub confirmation_email: Option<String>,
    #[schema(value_type = Option<String>, format = DateTime)]
    pub confirmation_sent_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl From<svc::OrgDomain> for OrgDomainResponse {
    fn from(d: svc::OrgDomain) -> Self {
        Self {
            dns_record_name: svc::dns_record_name(&d.domain),
            dns_record_value: svc::dns_record_value(&d.verification_token),
            id: d.id,
            domain: d.domain,
            verified: d.verified_at.is_some(),
            method: d.method,
            verified_at: d.verified_at,
            claimed_at: d.claimed_at,
            confirmation_email: d.confirmation_email,
            confirmation_sent_at: d.confirmation_sent_at,
        }
    }
}

#[derive(Debug, Serialize, ToSchema)]
pub struct OrgDomainListResponse {
    pub domains: Vec<OrgDomainResponse>,
}

#[derive(Debug, Deserialize, ToSchema)]
pub struct SendConfirmationRequest {
    /// The address to mail the confirmation link to. MUST be at the claimed
    /// domain — refused 400 otherwise, server-side
    /// (`services::org_domains::email_is_at_domain`), which is the entire
    /// proof this method rests on.
    pub email: String,
}

// ── POST /v1/orgs/domains ─────────────────────────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/domains",
    tag = "org_domains",
    security(("bearer_token" = [])),
    request_body = ClaimDomainRequest,
    responses(
        (status = 200, description = "Claimed (or already claimed) — publish the returned TXT record, or ask for an email confirmation", body = OrgDomainResponse),
        (status = 400, description = "Not a domain, a single label, or a public email provider"),
        (status = 409, description = "This org already holds the maximum number of domains"),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 503, description = "Database unavailable — transient, safe to retry"),
    )
)]
pub async fn claim_domain(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<ClaimDomainRequest>,
) -> Response {
    let account=user.account_id;
    let response=claim_domain_committed(State(state.clone()),tenant,Extension(user),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn claim_domain_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Json(body): Json<ClaimDomainRequest>,
) -> Response {
    let ctx = match admin_ctx(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let domain = match svc::normalize_domain(&body.domain) {
        Ok(d) => d,
        Err(rejection) => return bad_request(rejection.message()),
    };
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    match svc::claim_domain(control, ctx.account_id, ctx.user_id, &domain).await {
        Ok((Some(row), _outcome)) => (StatusCode::OK, Json(OrgDomainResponse::from(row))).into_response(),
        // CodeRabbit round 1 (#1031): the per-account ceiling. 409 rather
        // than 400 — the request was well formed, the account's state is
        // what refuses it, and removing a stale claim makes the same request
        // succeed.
        Ok((None, _limit_reached)) => conflict(&format!(
            "this organisation has already claimed the maximum of {} domains — remove one first",
            svc::MAX_DOMAINS_PER_ACCOUNT
        )),
        Err(e) => internal_error("claim", e),
    }
}

// ── GET /v1/orgs/domains ──────────────────────────────────────────────────

#[utoipa_path(
    get,
    path = "/v1/orgs/domains",
    tag = "org_domains",
    security(("bearer_token" = [])),
    responses(
        (status = 200, description = "Every claimed domain, with its verified state and the method that proved it", body = OrgDomainListResponse),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 503, description = "Database unavailable — transient, safe to retry"),
    )
)]
pub async fn list_domains(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
) -> Response {
    let ctx = match admin_ctx(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    match svc::list_domains(control, ctx.account_id).await {
        Ok(rows) => (
            StatusCode::OK,
            Json(OrgDomainListResponse { domains: rows.into_iter().map(OrgDomainResponse::from).collect() }),
        )
            .into_response(),
        Err(e) => internal_error("list", e),
    }
}

// ── DELETE /v1/orgs/domains/{id} ──────────────────────────────────────────

#[utoipa_path(
    delete,
    path = "/v1/orgs/domains/{id}",
    tag = "org_domains",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "org_domains.id")),
    responses(
        (status = 204, description = "Removed — any outstanding confirmation link for it stops working"),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 404, description = "No such domain for this org"),
        (status = 503, description = "Database unavailable — transient, safe to retry"),
    )
)]
pub async fn remove_domain(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let account=user.account_id;
    let response=remove_domain_committed(State(state.clone()),tenant,Extension(user),Path(id)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn remove_domain_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let ctx = match admin_ctx(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    match svc::remove_domain(control, ctx.account_id, id).await {
        Ok(true) => StatusCode::NO_CONTENT.into_response(),
        Ok(false) => not_found(),
        Err(e) => internal_error("remove", e),
    }
}

// ── POST /v1/orgs/domains/{id}/verify ─────────────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/domains/{id}/verify",
    tag = "org_domains",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "org_domains.id")),
    responses(
        (status = 200, description = "Verified by DNS TXT — including an UPGRADE from a domain that was already verified by email (method flips to `dns_txt`, arming auto-add). Echoes the current row, unchanged, if it was already `dns_txt`-verified.", body = OrgDomainResponse),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 404, description = "No such domain for this org"),
        (status = 409, description = "The resolver answered and the record is not published (yet)"),
        (status = 502, description = "We could not reach a DNS resolver — NOT the same thing as the record being absent"),
        (status = 503, description = "Database unavailable — transient, safe to retry"),
    )
)]
pub async fn verify_dns(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let account=user.account_id;
    let response=verify_dns_committed(State(state.clone()),tenant,Extension(user),Path(id)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn verify_dns_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
) -> Response {
    let ctx = match admin_ctx(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let account_id = ctx.account_id;
    let row = match svc::get_domain(control, account_id, id).await {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(e) => return internal_error("load domain", e),
    };
    // Short-circuit ONLY when the row is already `dns_txt`-verified — that
    // is idempotent (re-checking would waste a resolver call for nothing)
    // and, unlike the general "verified" case, there is nowhere stronger
    // left to go. An UNVERIFIED row, or a row verified by a method that is
    // NOT `dns_txt` (i.e. `email`), falls through and attempts the DNS
    // check below: this is the only path that can UPGRADE an
    // email-verified domain to `dns_txt` and arm auto-add (D16 rung 2) —
    // without it, an org that proved control by email had no way to reach
    // auto-add short of deleting and re-claiming the domain from scratch.
    //
    // This can never DOWNGRADE a `dns_txt` row: [`svc::mark_verified`]'s own
    // WHERE guard only ever writes `method = 'dns_txt'`, from here, and
    // refuses to touch a row that is already `dns_txt` (idempotent) or to
    // write `email` over an existing method at all.
    if row.method.as_deref() == Some(svc::VerificationMethod::DnsTxt.as_str()) {
        return (StatusCode::OK, Json(OrgDomainResponse::from(row))).into_response();
    }

    match svc::check_dns_txt(&state.http_client, &row.domain, &row.verification_token).await {
        // "Could not ask" — 502, never 409. See this module's own doc.
        Err(reason) => {
            tracing::warn!(
                reason = %reason, account_id = %account_id,
                "org_domains: DNS-over-HTTPS lookup failed — reporting 502, NOT 'record not found'"
            );
            bad_gateway("could not reach a DNS resolver — this is not a verdict on your record; try again")
        }
        Ok(svc::DnsProof::Missing { records_seen }) => conflict(&format!(
            "no matching TXT record at {} yet ({records_seen} TXT record(s) found there) — DNS can take a while to propagate",
            svc::dns_record_name(&row.domain)
        )),
        Ok(svc::DnsProof::Found) => {
            match svc::mark_verified(
                control,
                account_id,
                row.id,
                &row.verification_token,
                svc::VerificationMethod::DnsTxt,
            )
            .await
            {
                // Founder decision, 2026-09-22 (CS-447, migration 0247,
                // amending D11): a domain may be verified by ONE org,
                // globally, and the first to verify wins. 409 with a
                // message an admin can act on — NOT the "no matching TXT
                // record" 409 above, which would tell them to go fix a DNS
                // record that is already perfectly correct. Their claim row
                // survives, still unverified; claiming stays per-account.
                Ok(svc::VerifyOutcome::TakenByAnotherOrg) => conflict(&format!(
                    "{} is already verified by another organization on CoralSwarm — only one \
                     organization can verify a domain. If that is a mistake, contact support.",
                    row.domain
                )),
                Ok(svc::VerifyOutcome::Verified | svc::VerifyOutcome::NothingToVerify) => {
                    match svc::get_domain(control, account_id, id).await {
                        Ok(Some(fresh)) => (StatusCode::OK, Json(OrgDomainResponse::from(fresh))).into_response(),
                        Ok(None) => not_found(),
                        Err(e) => internal_error("reload domain", e),
                    }
                }
                Err(e) => internal_error("mark verified", e),
            }
        }
    }
}

// ── POST /v1/orgs/domains/{id}/confirmation ───────────────────────────────

#[utoipa_path(
    post,
    path = "/v1/orgs/domains/{id}/confirmation",
    tag = "org_domains",
    security(("bearer_token" = [])),
    params(("id" = Uuid, Path, description = "org_domains.id")),
    request_body = SendConfirmationRequest,
    responses(
        (status = 202, description = "Confirmation link sent"),
        (status = 400, description = "The address is not at the claimed domain"),
        (status = 403, description = "Not an org admin, or a personal org"),
        (status = 404, description = "No such domain for this org"),
        (status = 409, description = "Already verified, or another confirmation was sent very recently"),
        (status = 502, description = "The email provider refused the send — nothing was consumed, retry"),
        (status = 503, description = "Two distinct causes share this status; the `error` field discriminates. \
            `error: \"service unavailable\"` — a transient control-plane blip (the same string every other \
            route in this module returns for the same condition); safe to retry as-is. \
            `error: \"email_confirmation_not_configured\"` — `DOMAIN_VERIFICATION_SECRET` or `RESEND_API_KEY` \
            is unset on this deployment; a deployment fact, NOT retryable — verify by DNS instead \
            (`POST .../verify`, unaffected)."),
    )
)]
pub async fn request_confirmation(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<SendConfirmationRequest>,
) -> Response {
    let account=user.account_id;
    let response=request_confirmation_committed(State(state.clone()),tenant,Extension(user),Path(id),Json(body)).await;
    crate::read_models::dirty::after_control_response(&state,account,crate::read_models::dirty::DirtyEvent::OrgAdminChanged,response).await
}

async fn request_confirmation_committed(
    State(state): State<Arc<AppState>>,
    tenant: Tenant,
    Extension(user): Extension<UserContext>,
    Path(id): Path<Uuid>,
    Json(body): Json<SendConfirmationRequest>,
) -> Response {
    let ctx = match admin_ctx(&state, &tenant, &user).await {
        Ok(c) => c,
        Err(r) => return r,
    };
    let Some(control) = state.control_db() else {
        return service_unavailable();
    };
    let account_id = ctx.account_id;
    let row = match svc::get_domain(control, account_id, id).await {
        Ok(Some(row)) => row,
        Ok(None) => return not_found(),
        Err(e) => return internal_error("load domain", e),
    };
    if row.verified_at.is_some() {
        return conflict("that domain is already verified");
    }

    // D11's server-side gate — the entire proof this method rests on. An
    // address anywhere else is refused before anything is claimed, sent, or
    // written.
    //
    // `email_is_at_domain` also refuses a malformed local part (CodeRabbit
    // round 1, #1031 — `svc::local_part_is_sane`): whitespace, control
    // characters, and the comma/angle-bracket shapes that turn one recipient
    // into several. Catching those HERE turns what was a confusing 502
    // ("could not send") from Resend's own validator into a 400 that names
    // the problem, and keeps a CR/LF out of an outbound mail payload
    // entirely.
    let email = body.email.trim().to_string();
    if !svc::email_is_at_domain(&email, &row.domain) {
        return bad_request(&format!(
            "the confirmation address must be a valid address at {} — that is what proves you control it",
            row.domain
        ));
    }

    // CS-440's precedent for a missing signing secret: skip the feature with
    // a warning, never panic, and never mail a link that cannot be
    // verified.
    let Some(secret) = svc::domain_verification_secret() else {
        tracing::warn!(
            account_id = %account_id,
            "org_domains: DOMAIN_VERIFICATION_SECRET is unset — skipping the email-confirmation \
             proof entirely (the DNS TXT proof is unaffected)"
        );
        return confirmation_not_configured();
    };
    if state.resend_api_key.is_none() {
        tracing::warn!(
            account_id = %account_id,
            "org_domains: RESEND_API_KEY is unset — cannot send a domain-confirmation email"
        );
        return confirmation_not_configured();
    }

    let token = match svc::mint_confirmation_token(
        &secret,
        row.id,
        account_id,
        &row.domain,
        &email,
        &row.verification_token,
    ) {
        Ok(t) => t,
        Err(e) => return internal_error("mint confirmation token", e),
    };

    // Claim → send → commit, in that order (`workers::org_join_request_
    // digest::process_account`'s own discipline): the cooldown row is
    // claimed inside a transaction held OPEN across the Resend call and
    // rolled back if the send fails, so a failed send never consumes the
    // window that would let the admin retry immediately.
    let mut tx = match crate::tenancy::scope::begin_account_scoped(control, account_id).await {
        Ok(tx) => tx,
        Err(e) => return internal_error("scope", e),
    };
    let claimed = match svc::claim_confirmation_send(&mut tx, account_id, row.id, &email).await {
        Ok(c) => c,
        Err(e) => {
            let _ = tx.rollback().await;
            return internal_error("claim confirmation send", e);
        }
    };
    if !claimed {
        let _ = tx.rollback().await;
        return conflict("a confirmation was sent for that domain very recently — try again shortly");
    }

    let confirm_url = format!(
        "{}/v1/orgs/domains/confirm?t={}",
        state.api_base_url(),
        urlencoding::encode(&token)
    );
    if !send_confirmation_email(&state, &email, &row.domain, &confirm_url).await {
        let _ = tx.rollback().await;
        return bad_gateway("could not send the confirmation email — nothing was consumed, try again");
    }
    if let Err(e) = tx.commit().await {
        return internal_error("commit confirmation claim", e);
    }
    (StatusCode::ACCEPTED, Json(json!({"status": "sent"}))).into_response()
}

fn escape_html(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;").replace('"', "&quot;")
}

/// Send one confirmation email via Resend. Best-effort: any failure returns
/// `false` and never panics — the caller holds the claim transaction open
/// across this call and rolls it back on `false`, the same contract
/// `workers::org_join_request_digest::send_digest_email` documents.
async fn send_confirmation_email(state: &AppState, to_email: &str, domain: &str, confirm_url: &str) -> bool {
    let Some(resend_key) = state.resend_api_key.clone() else {
        return false;
    };
    let domain_esc = escape_html(domain);
    let url_esc = escape_html(confirm_url);
    let html = format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width\"></head>\
<body style=\"margin:0;padding:0;background:#F2EBE0\">\
<table role=\"presentation\" width=\"100%\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" bgcolor=\"#F2EBE0\"><tr><td align=\"center\" style=\"padding:40px 16px\">\
<table role=\"presentation\" width=\"560\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"width:560px;max-width:100%\">\
<tr><td bgcolor=\"#FBF7F2\" style=\"border:1px solid #D9CCB8;border-radius:12px;padding:34px 36px;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1A1410\">\
<div style=\"margin-bottom:26px;font-weight:600;font-size:19px;letter-spacing:-0.045em;color:#1A1410;line-height:1\">coralswarm</div>\
<h1 style=\"margin:0 0 14px 0;font-size:26px;font-weight:400;line-height:1.2;color:#1A1410\">Confirm {domain_esc}</h1>\
<p style=\"margin:0 0 16px 0;font-size:14px;line-height:1.6;color:#4A3F36\">Someone administering a CoralSwarm organisation asked to verify that your organisation controls <strong>{domain_esc}</strong>. Confirming lets them use it as their verified domain.</p>\
<p style=\"margin:0 0 22px 0;font-size:14px;line-height:1.6;color:#4A3F36\">If you were not expecting this, ignore this email — nothing happens unless you press the button.</p>\
<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" border=\"0\" style=\"margin-bottom:12px\"><tr>\
<td bgcolor=\"#F77764\" style=\"border-radius:8px\">\
<a href=\"{url_esc}\" style=\"display:inline-block;padding:11px 22px;font-size:15px;font-weight:500;color:#FFFFFF;text-decoration:none\">Confirm this domain</a>\
</td></tr></table>\
<p style=\"margin:16px 0 0 0;font-size:11px;line-height:1.7;color:#8A7B6E\">This link expires in three days.</p>\
</td></tr></table></td></tr></table></body></html>"
    );
    let payload = json!({
        "from": "coralswarm <person@example.com>",
        "to": [to_email],
        "subject": format!("Confirm {domain} for CoralSwarm"),
        "html": html,
    });
    let url = format!("{}/emails", resend_base_url());
    let send = state.http_client.post(&url).bearer_auth(&resend_key).json(&payload).send();
    match tokio::time::timeout(std::time::Duration::from_secs(10), send).await {
        Ok(Ok(resp)) if resp.status().is_success() => true,
        Ok(Ok(resp)) => {
            tracing::warn!(status = %resp.status(), "org_domains: Resend returned non-success for a confirmation email");
            false
        }
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "org_domains: failed to send the confirmation email via Resend");
            false
        }
        Err(_) => {
            tracing::warn!("org_domains: Resend send timed out");
            false
        }
    }
}

/// Same `RESEND_API_BASE_URL` override every other sender in this crate
/// reads — see `routes::org_invites::resend_base_url`.
fn resend_base_url() -> String {
    std::env::var("RESEND_API_BASE_URL").unwrap_or_else(|_| "https://api.resend.com".to_string())
}

// ── GET|POST /v1/orgs/domains/confirm (PUBLIC) ────────────────────────────

#[derive(Debug, Deserialize)]
pub struct ConfirmQuery {
    /// The signed token from the emailed link. Named `t`, matching
    /// `routes::email_unsubscribe`'s own query parameter.
    pub t: Option<String>,
}

fn confirm_page(heading: &str, body: &str, form_token: Option<&str>) -> Response {
    // The token rides in the form's ACTION query string rather than a hidden
    // body field: `confirm_post` reads `Query<ConfirmQuery>` only, so it
    // parses no request body at all. One extractor, one place a token can
    // come from.
    let form = match form_token {
        Some(token) => format!(
            "<form method=\"post\" action=\"/v1/orgs/domains/confirm?t={}\">\
<button type=\"submit\" style=\"padding:11px 22px;border:0;border-radius:8px;background:#F77764;color:#fff;font-size:15px;cursor:pointer\">Confirm this domain</button>\
</form>",
            escape_html(&urlencoding::encode(token))
        ),
        None => String::new(),
    };
    Html(format!(
        "<!doctype html><html><head><meta charset=\"utf-8\"><title>CoralSwarm</title></head>\
<body style=\"margin:0;background:#F2EBE0;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Helvetica,Arial,sans-serif;color:#1A1410\">\
<div style=\"max-width:520px;margin:64px auto;padding:32px;background:#FBF7F2;border:1px solid #D9CCB8;border-radius:12px\">\
<div style=\"margin-bottom:24px;font-weight:600;font-size:19px;letter-spacing:-0.045em\">coralswarm</div>\
<h1 style=\"margin:0 0 12px 0;font-size:24px;font-weight:400\">{}</h1>\
<p style=\"margin:0 0 20px 0;font-size:14px;line-height:1.6;color:#4A3F36\">{}</p>{form}</div></body></html>",
        escape_html(heading),
        escape_html(body)
    ))
    .into_response()
}

/// The visible link in the confirmation email. Renders a form and
/// **verifies nothing** — see this module's own doc comment for why a GET
/// that verified would hand every mail-security link scanner the power to
/// prove control of a domain.
pub async fn confirm_get(Query(q): Query<ConfirmQuery>) -> Response {
    let Some(token) = q.t.as_deref().filter(|t| !t.trim().is_empty()) else {
        return confirm_page(
            "That link is not valid",
            "This confirmation link is missing its token. Ask your CoralSwarm admin to send a new one.",
            None,
        );
    };
    // Deliberately NOT verified here: rendering the form for an expired or
    // tampered token and refusing on POST keeps this page from being an
    // oracle for token validity, the same posture `routes::email_unsubscribe`
    // keeps for its own tokens.
    confirm_page(
        "Confirm this domain",
        "Press the button to confirm that your organisation controls this domain.",
        Some(token),
    )
}

/// Consume a confirmation token and mark the domain verified by email.
///
/// The token comes from the query string — the form [`confirm_get`] renders
/// posts to `…/confirm?t=<token>` with no body at all, so there is exactly
/// one place a token can arrive from and no body parsing to get wrong.
pub async fn confirm_post(State(state): State<Arc<AppState>>, Query(q): Query<ConfirmQuery>) -> Response {
    let token = q.t.as_deref().map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
    let Some(token) = token else {
        return confirm_page("That link is not valid", "This confirmation link is missing its token.", None);
    };
    let Some(secret) = svc::domain_verification_secret() else {
        tracing::warn!(
            "org_domains: DOMAIN_VERIFICATION_SECRET is unset — a domain-confirmation link was \
             clicked and cannot be verified"
        );
        return confirm_page(
            "We cannot confirm this right now",
            "Domain confirmation is not configured on this deployment. Ask your CoralSwarm admin to \
             verify by DNS instead.",
            None,
        );
    };
    let Some(ctx) = svc::verify_confirmation_token(&secret, &token) else {
        return confirm_page(
            "That link has expired",
            "Confirmation links are valid for three days. Ask your CoralSwarm admin to send a new one.",
            None,
        );
    };
    let Some(control) = state.control_db() else {
        return confirm_page("We cannot confirm this right now", "Please try again in a few minutes.", None);
    };

    // The token names the account; `mark_verified` opens that account's own
    // scope and pins the write to the `verification_token` the token was
    // minted against, so a link for a domain that has since been removed
    // (and perhaps re-claimed, with a fresh token) verifies nothing.
    match svc::mark_verified(
        control,
        ctx.account_id,
        ctx.domain_id,
        &ctx.verification_token,
        svc::VerificationMethod::Email,
    )
    .await
    {
        Ok(svc::VerifyOutcome::Verified) => confirm_page(
            "Domain confirmed",
            &format!("{} is now a verified domain for this organisation.", ctx.domain),
            None,
        ),
        // Already verified, removed, or re-claimed since this link was
        // minted — one page for all three, rather than an oracle telling a
        // holder of an old link what happened to the row.
        Ok(svc::VerifyOutcome::NothingToVerify) => confirm_page(
            "Nothing to confirm",
            &format!("{} does not need confirming from this link.", ctx.domain),
            None,
        ),
        // Founder decision, 2026-09-22 (CS-447, migration 0247): first org
        // to verify wins, globally. Named plainly — the person clicking is
        // at the domain and the admin who sent them the link needs to know
        // WHY it did not take, not "nothing to confirm".
        Ok(svc::VerifyOutcome::TakenByAnotherOrg) => confirm_page(
            "Already verified elsewhere",
            &format!(
                "{} is already verified by another organization on CoralSwarm. Only one \
                 organization can verify a domain — ask your admin to contact support if that \
                 is a mistake.",
                ctx.domain
            ),
            None,
        ),
        Err(e) => {
            tracing::error!(error = %e, "org_domains: failed to mark a domain verified by email");
            confirm_page("We cannot confirm this right now", "Please try again in a few minutes.", None)
        }
    }
}

// ── Live-Postgres route suite ────────────────────────────────────────────
//
// `RELINFER_TEST_DATABASE_URL`-gated against a PERSISTENT, pre-migrated
// database — the same convention `routes::org_join_requests::pg_tests` uses,
// and in-crate for the same reason: these handlers take a [`Tenant`]
// extractor, so the suite calls them directly with a hand-built
// `Tenant`/`UserContext` rather than standing up the whole `TenantRuntime`
// (see `backend/tests/org_join_requests_pg.rs`' own module doc for the full
// rationale behind that split).
//
//   RELINFER_TEST_DATABASE_URL=postgres://alex@localhost:5432/cs443 \
//     cargo test -p coralswarm-backend --lib "routes::org_domains::pg_tests::" -- --test-threads=1 --nocapture
#[cfg(test)]
mod pg_tests {
    use super::*;
    use crate::services::org_domains::test_doh::{spawn_resolver, DohEnvGuard};
    use serde_json::Value;
    use sqlx::PgPool;

    async fn pg_test_pool(test_name: &str) -> Option<PgPool> {
        let url = match std::env::var("RELINFER_TEST_DATABASE_URL") {
            Ok(u) => u,
            Err(_) => {
                eprintln!("skipping {test_name}: set RELINFER_TEST_DATABASE_URL to run it");
                return None;
            }
        };
        Some(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(4)
                .connect(&url)
                .await
                .expect("connect to test Postgres"),
        )
    }

    /// An org account with one ADMIN member — the shape `require_org_admin`
    /// plus `account_is_real_org` both need. Mirrors
    /// `routes::org_join_requests::pg_tests::seed_org`, minus the
    /// billing/Stripe scaffolding no route here touches.
    async fn seed_org(pool: &PgPool) -> (Uuid, String, Uuid) {
        let admin_id = Uuid::new_v4();
        sqlx::query("INSERT INTO users (id, email, clerk_user_id) VALUES ($1, $2, $3)")
            .bind(admin_id)
            .bind(format!("cs443-route-admin-{}@example.test", admin_id.simple()))
            .bind(format!("clerk_cs443_route_{}", admin_id.simple()))
            .execute(pool)
            .await
            .expect("insert admin user");
        let clerk_org_id = format!("org_cs443_route_{}", Uuid::new_v4().simple());
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(admin_id)
                .bind(&clerk_org_id)
                .fetch_one(pool)
                .await
                .expect("insert org account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'admin', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active))",
        )
        .bind(account_id)
        .bind(admin_id)
        .execute(pool)
        .await
        .expect("insert admin membership");
        (account_id, clerk_org_id, admin_id)
    }

    fn admin_ctx(account_id: Uuid, user_id: Uuid) -> UserContext {
        UserContext {
            user_id,
            account_id,
            role: "admin".to_string(),
            device_id: "org-domains-incrate-test".to_string(),
        }
    }
    fn org_tenant(pool: PgPool, org_id: &str, account_id: Uuid, user_id: Uuid) -> Tenant {
        Tenant {
            store: None,
            pool: None,
            decision: Some(crate::tenancy::runtime::TenancyDecision::Org(
                TenantContext {
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
    fn test_state(control: PgPool) -> Arc<AppState> {
        Arc::new(crate::test_app_state_owned_with_db_and_store(control, None))
    }
    async fn body_json(resp: Response) -> Value {
        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        serde_json::from_slice(&bytes).unwrap_or(Value::Null)
    }

    /// The happy path end to end, over the real handlers: claim returns a TXT
    /// record to publish; a stub resolver carrying that exact record verifies
    /// it; and the list read shows `verified: true` with `method: "dns_txt"`
    /// — D11's *"the org's settings show which one proved it"*.
    #[tokio::test]
    async fn claim_then_verify_by_dns_records_the_method_that_proved_it() {
        let Some(pool) = pg_test_pool("claim_then_verify_by_dns_records_the_method_that_proved_it").await
        else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs443-route-{}.example", Uuid::new_v4().simple());

        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: format!("  @{} ", domain.to_uppercase()) }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let claimed = body_json(resp).await;
        assert_eq!(claimed["domain"], serde_json::json!(domain), "normalised on the way in");
        assert_eq!(claimed["verified"], serde_json::json!(false));
        assert_eq!(claimed["method"], Value::Null, "nothing has proved it yet");
        assert_eq!(
            claimed["dns_record_name"],
            serde_json::json!(format!("_coralswarm-verification.{domain}"))
        );
        let record_value = claimed["dns_record_value"].as_str().expect("a TXT value to publish").to_string();
        let domain_id: Uuid = serde_json::from_value(claimed["id"].clone()).unwrap();

        let resolver = spawn_resolver(
            StatusCode::OK,
            serde_json::json!({
                "Status": 0,
                "Answer": [{"name": "x", "type": 16, "data": format!("\"{record_value}\"")}]
            }),
        )
        .await;
        let _env = DohEnvGuard::set(&resolver);
        let resp = verify_dns(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let verified = body_json(resp).await;
        assert_eq!(verified["verified"], serde_json::json!(true));
        assert_eq!(verified["method"], serde_json::json!("dns_txt"));
        drop(_env);

        let resp = list_domains(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK);
        let listed = body_json(resp).await;
        assert_eq!(listed["domains"][0]["domain"], serde_json::json!(domain));
        assert_eq!(listed["domains"][0]["method"], serde_json::json!("dns_txt"));
    }

    /// "Could not ask" must not be reported as "your record is wrong": a
    /// resolver failure is **502** and leaves the domain unverified, while a
    /// resolver that genuinely answered with no matching record is **409**.
    /// An admin told 409 goes and checks their DNS; an admin told 502 tries
    /// again. Conflating them sends someone to break a record that was fine.
    #[tokio::test]
    async fn a_resolver_failure_is_502_and_a_missing_record_is_409() {
        let Some(pool) = pg_test_pool("a_resolver_failure_is_502_and_a_missing_record_is_409").await else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs443-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let domain_id: Uuid = serde_json::from_value(body_json(resp).await["id"].clone()).unwrap();

        let dead = spawn_resolver(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({})).await;
        let _env = DohEnvGuard::set(&dead);
        let resp = verify_dns(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::BAD_GATEWAY,
            "an unreachable resolver is 502 — not a verdict on the admin's DNS record"
        );
        drop(_env);

        let answering = spawn_resolver(
            StatusCode::OK,
            serde_json::json!({"Status": 0, "Answer": [{"name": "x", "type": 16, "data": "\"v=spf1 -all\""}]}),
        )
        .await;
        let _env = DohEnvGuard::set(&answering);
        let resp = verify_dns(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::CONFLICT, "a resolver that answered, with no record, is 409");
        drop(_env);

        // Neither attempt verified anything.
        let still_unverified: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT verified_at FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(still_unverified.is_none());
    }

    /// **NEGATIVE TEST 4 (CS-443), at the route.** A confirmation aimed at an
    /// address OUTSIDE the claimed domain is refused server-side, before any
    /// token is minted, any cooldown is claimed, or any mail is sent —
    /// enforced by the handler, not by whatever the client put in the form.
    #[tokio::test]
    async fn a_confirmation_to_an_address_outside_the_domain_is_refused() {
        let Some(pool) = pg_test_pool("a_confirmation_to_an_address_outside_the_domain_is_refused").await
        else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs443-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let domain_id: Uuid = serde_json::from_value(body_json(resp).await["id"].clone()).unwrap();

        for outside in [
            "person@example.com".to_string(),
            format!("ops@not{domain}"),
            format!("ops@{domain}.evil.test"),
            format!("ops@mail.{domain}"),
            format!("{domain}@gmail.com"),
        ] {
            let resp = request_confirmation(
                State(test_state(pool.clone())),
                org_tenant(pool.clone(), &org_id, account_id, admin_id),
                Extension(admin_ctx(account_id, admin_id)),
                Path(domain_id),
                Json(SendConfirmationRequest { email: outside.clone() }),
            )
            .await;
            assert_eq!(
                resp.status(),
                StatusCode::BAD_REQUEST,
                "{outside} is not at {domain} and must be refused"
            );
        }

        // Nothing was claimed, so nothing is on cooldown and no address was
        // recorded — the refusal happens before the claim, by construction.
        let (sent_at, sent_to): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT confirmation_sent_at, confirmation_email FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(sent_at.is_none() && sent_to.is_none());
    }

    /// The public confirm link: **GET verifies nothing** (a mail-security
    /// link scanner fetching every URL in the message body must not be able
    /// to prove control of a domain), and a POST carrying a token minted
    /// against a DIFFERENT `verification_token` verifies nothing either.
    #[tokio::test]
    async fn the_get_confirm_link_verifies_nothing_and_a_stale_token_is_inert() {
        let Some(pool) = pg_test_pool("the_get_confirm_link_verifies_nothing_and_a_stale_token_is_inert").await
        else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs443-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let claimed = body_json(resp).await;
        let domain_id: Uuid = serde_json::from_value(claimed["id"].clone()).unwrap();
        let real_token = claimed["dns_record_value"]
            .as_str()
            .unwrap()
            .trim_start_matches("coralswarm-domain-verification=")
            .to_string();

        const SECRET: &str = "cs443-route-test-secret-0123456789";
        let good = svc::mint_confirmation_token(
            SECRET,
            domain_id,
            account_id,
            &domain,
            &format!("ops@{domain}"),
            &real_token,
        )
        .expect("mint");

        // GET: renders a page, verifies nothing.
        let resp = confirm_get(Query(ConfirmQuery { t: Some(good.clone()) })).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let verified_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT verified_at FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(
            verified_at.is_none(),
            "a GET on the confirmation link must never verify — link scanners fetch every URL"
        );

        // A token bound to some OTHER claim's verification_token is inert,
        // even though its signature and expiry are both fine.
        let stale = svc::mint_confirmation_token(
            SECRET,
            domain_id,
            account_id,
            &domain,
            &format!("ops@{domain}"),
            "a-token-from-an-older-claim",
        )
        .expect("mint");
        let guard = EnvGuard::set(SECRET);
        let resp = confirm_post(State(test_state(pool.clone())), Query(ConfirmQuery { t: Some(stale) })).await;
        assert_eq!(resp.status(), StatusCode::OK, "the confirm page always renders, never an error code");
        let verified_at: Option<chrono::DateTime<chrono::Utc>> =
            sqlx::query_scalar("SELECT verified_at FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(verified_at.is_none(), "a token pinned to a different claim must verify nothing");

        // The real one does verify, and records `email` as the method.
        let resp = confirm_post(State(test_state(pool.clone())), Query(ConfirmQuery { t: Some(good) })).await;
        assert_eq!(resp.status(), StatusCode::OK);
        let (verified_at, method): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT verified_at, method FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(verified_at.is_some());
        assert_eq!(method.as_deref(), Some("email"));
        drop(guard);
    }

    /// **CHANGE 1 (CS-450 backend defect 1).** An email-verified domain is
    /// not a dead end: `POST .../verify` may UPGRADE it to `dns_txt` once
    /// the TXT record is genuinely published, arming auto-add (D16 rung 2)
    /// where it could not before. `verified_at` is bumped to the DNS
    /// proof's own time — see `svc::mark_verified`'s own doc comment for why
    /// the OLD (email) timestamp is deliberately not kept: the row stores
    /// exactly one `(verified_at, method)` pair, and leaving the old
    /// timestamp under the new method would read as "DNS TXT has proven
    /// this since `<email-proof-date>`", which is false.
    #[tokio::test]
    async fn an_email_verified_domain_upgrades_to_dns_on_verify() {
        let Some(pool) = pg_test_pool("an_email_verified_domain_upgrades_to_dns_on_verify").await else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs450-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let claimed = body_json(resp).await;
        let domain_id: Uuid = serde_json::from_value(claimed["id"].clone()).unwrap();
        let record_value = claimed["dns_record_value"].as_str().unwrap().to_string();

        // Reach "verified by email" directly through the service layer —
        // the full mailed-link round trip is already pinned by
        // `the_get_confirm_link_verifies_nothing_and_a_stale_token_is_inert`
        // above; this test's own subject is what happens AFTER that state
        // is reached, over the real `verify_dns` HANDLER.
        let row = svc::get_domain(&pool, account_id, domain_id).await.unwrap().unwrap();
        let outcome = svc::mark_verified(
            &pool,
            account_id,
            domain_id,
            &row.verification_token,
            svc::VerificationMethod::Email,
        )
        .await
        .unwrap();
        assert_eq!(outcome, svc::VerifyOutcome::Verified);
        let (email_verified_at, email_method): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT verified_at, method FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(email_method.as_deref(), Some("email"));
        let email_verified_at = email_verified_at.expect("verified by email");

        // A little real elapsed time so the DNS proof's `now()` is
        // observably later than the email proof's — pins that the upgrade
        // BUMPS `verified_at` rather than preserving the original.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        let resolver = spawn_resolver(
            StatusCode::OK,
            serde_json::json!({
                "Status": 0,
                "Answer": [{"name": "x", "type": 16, "data": format!("\"{record_value}\"")}]
            }),
        )
        .await;
        let _env = DohEnvGuard::set(&resolver);
        let resp = verify_dns(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::OK, "an email-verified row must be upgradeable by DNS");
        let upgraded = body_json(resp).await;
        assert_eq!(upgraded["verified"], serde_json::json!(true));
        assert_eq!(upgraded["method"], serde_json::json!("dns_txt"), "the method flips to dns_txt on upgrade");
        drop(_env);

        let (final_verified_at, final_method): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT verified_at, method FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(final_method.as_deref(), Some("dns_txt"));
        assert!(
            final_verified_at.expect("still verified") > email_verified_at,
            "verified_at must be bumped to the DNS proof's own time, not left at the original email-proof time"
        );
    }

    /// **CHANGE 1 (CS-450 backend defect 1), the other half.** A row that is
    /// ALREADY `dns_txt`-verified must never re-check: the route
    /// short-circuits before ever calling the resolver, and
    /// `svc::mark_verified`'s own WHERE guard is a second, DB-level refusal
    /// of the identical write. Proven by pointing the stub resolver at a
    /// DEAD one — if the route incorrectly attempted a lookup, that would
    /// surface as 502, not the 200-with-an-unchanged-row this test asserts.
    #[tokio::test]
    async fn a_dns_verified_domain_never_rechecks() {
        let Some(pool) = pg_test_pool("a_dns_verified_domain_never_rechecks").await else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs450-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let domain_id: Uuid = serde_json::from_value(body_json(resp).await["id"].clone()).unwrap();

        let row = svc::get_domain(&pool, account_id, domain_id).await.unwrap().unwrap();
        svc::mark_verified(&pool, account_id, domain_id, &row.verification_token, svc::VerificationMethod::DnsTxt)
            .await
            .unwrap();
        let (before_verified_at, _): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT verified_at, method FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();

        let dead = spawn_resolver(StatusCode::INTERNAL_SERVER_ERROR, serde_json::json!({})).await;
        let _env = DohEnvGuard::set(&dead);
        let resp = verify_dns(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
        )
        .await;
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "a dns_txt row must short-circuit rather than attempt a lookup (which would 502 against this dead resolver)"
        );
        drop(_env);

        let unchanged = body_json(resp).await;
        assert_eq!(unchanged["method"], serde_json::json!("dns_txt"));
        let (after_verified_at, after_method): (Option<chrono::DateTime<chrono::Utc>>, Option<String>) =
            sqlx::query_as("SELECT verified_at, method FROM org_domains WHERE id = $1")
                .bind(domain_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(after_method.as_deref(), Some("dns_txt"));
        assert_eq!(after_verified_at, before_verified_at, "must not re-date an already dns_txt-verified row");
    }

    /// **CHANGE 2 (CS-450 backend defect 2).** `request_confirmation`'s 503
    /// has TWO distinct causes and must not answer with the same body for
    /// both: `state.control_db()` being `None` is a TRANSIENT control-plane
    /// blip (`error: "service unavailable"` — the same string this module
    /// returns for the identical condition everywhere else in it);
    /// `DOMAIN_VERIFICATION_SECRET`/`RESEND_API_KEY` being unset is a
    /// PERMANENT, NOT-retryable deployment fact (`error:
    /// "email_confirmation_not_configured"`). Before this fix all three
    /// collapsed onto the exact same generic body — this test fails against
    /// that code.
    #[tokio::test]
    async fn the_two_503_causes_on_request_confirmation_are_distinguishable() {
        let Some(pool) = pg_test_pool("the_two_503_causes_on_request_confirmation_are_distinguishable").await
        else {
            return;
        };
        let (account_id, org_id, admin_id) = seed_org(&pool).await;
        let domain = format!("cs450-route-{}.example", Uuid::new_v4().simple());
        let resp = claim_domain(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Json(ClaimDomainRequest { domain: domain.clone() }),
        )
        .await;
        let domain_id: Uuid = serde_json::from_value(body_json(resp).await["id"].clone()).unwrap();
        let at_domain_email = format!("ops@{domain}");

        // Cause 1: TRANSIENT — `state.control_db()` reads `None`, but
        // `state.control_store()` still works (the admin gate ahead of the
        // handler body needs it to succeed), so this exercises
        // `request_confirmation`'s OWN `control_db()` check specifically,
        // not an earlier gate failing for the same underlying reason.
        let store_only_state = Arc::new(crate::test_app_state_owned_with_store_only(pool.clone(), None));
        let resp = request_confirmation(
            State(store_only_state),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
            Json(SendConfirmationRequest { email: at_domain_email.clone() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let transient_body = body_json(resp).await;
        assert_eq!(transient_body["error"], serde_json::json!("service unavailable"));

        // Cause 2: PERMANENT, first check — `DOMAIN_VERIFICATION_SECRET`
        // unset (the test-process default; nothing in this suite sets it
        // outside `EnvGuard::set`, which always cleans up on `Drop`).
        let resp = request_confirmation(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
            Json(SendConfirmationRequest { email: at_domain_email.clone() }),
        )
        .await;
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let secret_unset_body = body_json(resp).await;
        assert_eq!(secret_unset_body["error"], serde_json::json!("email_confirmation_not_configured"));

        // Cause 2, the OTHER genuine config check — secret present, but
        // `RESEND_API_KEY` unset (the test state's own default).
        const SECRET: &str = "cs450-route-test-secret-0123456789";
        let guard = EnvGuard::set(SECRET);
        let resp = request_confirmation(
            State(test_state(pool.clone())),
            org_tenant(pool.clone(), &org_id, account_id, admin_id),
            Extension(admin_ctx(account_id, admin_id)),
            Path(domain_id),
            Json(SendConfirmationRequest { email: at_domain_email.clone() }),
        )
        .await;
        drop(guard);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let resend_unset_body = body_json(resp).await;
        assert_eq!(resend_unset_body["error"], serde_json::json!("email_confirmation_not_configured"));

        // The whole point: the transient cause and the permanent cause(s)
        // must NOT share a body.
        assert_ne!(
            transient_body["error"], secret_unset_body["error"],
            "a control-plane blip and a not-configured-here deployment fact must be distinguishable"
        );
    }

    /// Serializes the process-global `DOMAIN_VERIFICATION_SECRET` for the
    /// confirm-link tests above. Nothing else in this crate touches that var
    /// (unlike `DOH_RESOLVER_URL`, whose guard lives in
    /// `services::org_domains::test_doh` precisely because two modules share
    /// it), so a module-local lock is the whole of what it needs.
    struct EnvGuard(#[allow(dead_code)] std::sync::MutexGuard<'static, ()>);
    static SECRET_ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());
    impl EnvGuard {
        fn set(secret: &str) -> Self {
            let guard = SECRET_ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // SAFETY: serialized by `guard`, held for this struct's whole lifetime.
            unsafe {
                std::env::set_var("DOMAIN_VERIFICATION_SECRET", secret);
            }
            Self(guard)
        }
    }
    impl Drop for EnvGuard {
        fn drop(&mut self) {
            // SAFETY: see `set`.
            unsafe {
                std::env::remove_var("DOMAIN_VERIFICATION_SECRET");
            }
        }
    }
}
