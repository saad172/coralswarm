//! Auth middleware — validates an `Authorization: Bearer` token and injects a
//! [`UserContext`] into request extensions for protected routes.
//!
//! Two recognized bearer SHAPES: an ordinary Clerk session JWT (the sole
//! method until the Gmail Add-on), or a `csat_`-prefixed add-on personal
//! access token (PAT) minted by `routes::addin`'s pairing flow — see that
//! module's doc comment for why the add-on needs its own credential at all
//! (Apps Script cannot hold a live Clerk session the way the Outlook add-in
//! does). A PAT is a second recognized token SHAPE, not a parallel auth
//! system: both paths converge on the same [`UserContext`] and the same
//! binding-gate call below. A further add-in shape follows the same
//! pattern: `csms_` (`routes::addin_microsoft` — the short-lived Microsoft-SSO session credential
//! of the Teams/Outlook panes, which MAY bind an org workspace; see that
//! module's doc for how the plane is derived from LIVE workspace access on every
//! request). The legacy opaque session-token path (OTP
//! `/auth/*` endpoints + `sessions` table) has been removed entirely.
//!
//! # Flow
//! 1. **Binding gate, phase 1 (BEFORE auth):** when the tenancy runtime is
//!    enabled, classify the request `Host` and reject everything that host
//!    alone already condemns — a foreign host, and an unknown / provisioning /
//!    suspended / offboarded org subdomain (404/503/403). Nothing is verified
//!    and nothing is written first.
//! 2. Extract the raw token from the `Authorization` header (missing → 401).
//! 3. Require a database pool (absent → 503).
//! 4. **Authenticate**, branching on the token's shape:
//!    * a `csat_`-prefixed token resolves against `addin_tokens` (SHA-256
//!      hash lookup, `revoked_at IS NULL`) — see
//!      [`crate::routes::addin::resolve_addin_token`]. No Clerk verification,
//!      no org claim (an add-on token is personal-plane only by
//!      construction — see that module's doc). Not found / revoked → 401,
//!      identically to an invalid Clerk JWT.
//!    * anything else is verified as a Clerk JWT against the instance JWKS,
//!      and the caller is resolved/provisioned (`resolve_clerk_context`).
//!      Non-JWT / invalid / expired → 401.
//! 5. Fire-and-forget observability, per shape: a Clerk JWT gets
//!    `user_access_log` capture (`middleware::access_log`,
//!    platform-admin-metrics PR 4 of 7) — first-sight of this token's `sid`
//!    claim IS the login signal, see that module's doc for the full
//!    rationale. A PAT instead gets its own `addin_tokens.last_used_at`
//!    touch (`routes::addin::touch_last_used`) — there is no `sid` to key an
//!    access-log row on. Neither is ever awaited, and neither can fail the
//!    request.
//! 6. **Binding gate, phase 2 (AFTER auth):** the claim-binding half — is
//!    THIS token's org the one this subdomain routes to (403 on a mismatch),
//!    and, on the apex, what the org's own routing row says — then acquire the
//!    tenant pool. This half inherently needs verified claims; a PAT supplies
//!    `None` for the org claim (see step 4), which is exactly what an
//!    ordinary personal Clerk token supplies too — the gate cannot tell the
//!    two apart, by design.
//! 7. Attach [`UserContext`] + the `TenancyDecision` to request extensions and
//!    call `next`.
//!
//! # Why the gate is split
//!
//! The plan (`docs/plans/org-tenancy-implementation-plan.md`, PR 1.2 step 2)
//! specifies "unknown subdomain → **404 fail-closed before auth**". Running the
//! whole gate after `resolve_clerk_context` broke that in two ways: an
//! authenticated request to a host that does not exist still paid a full JWT
//! verification AND the control-plane provisioning WRITES inside
//! `resolve_clerk_context` (`ensure_org_account`,
//! `refresh_org_membership_from_claims`), and an unauthenticated one got a 401
//! — which distinguishes "real subdomain, bad token" from "no such subdomain"
//! and hands out an enumeration oracle. Phase 1 answers those cases from the
//! `Host` alone.
//!
//! Safety of the split rests on `tenancy::gate::precheck` returning EXACTLY
//! what `decide` would have returned whenever it answers at all — proven
//! exhaustively over the (host × routing × token) cross-product by
//! `precheck_matches_decide_for_every_token`. No status code moves.

use axum::{
    extract::{Request, State},
    http::{HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;
use std::sync::Arc;
use uuid::Uuid;

use crate::AppState;

// ── Public types ──────────────────────────────────────────────────────────────

/// Identity and authorization context attached to every authenticated request.
///
/// Fields are read by downstream handlers as they land; allow dead_code until
/// the first protected handler is wired up.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct UserContext {
    pub user_id: Uuid,
    pub account_id: Uuid,
    pub role: String,
    /// Per-device identity from the session, used by the sync routes to filter
    /// out the caller's own ops on pull (ADR-0005).
    pub device_id: String,
}

/// Present on a request only when it was authenticated by an INTERACTIVE Clerk
/// session JWT (one that carries `sid`, [`OauthKind::Session`]): not a Clerk
/// OAuth access token (native add-in clients, DCR-registered clients when
/// `REST_FOREIGN_OAUTH` is not `reject`), not an MCP token, not a `csat_` PAT,
/// or `csms_` add-in session. Routes that must be a person
/// at a signed-in browser (admin authority, delete account) require it; nothing
/// else inserts it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct InteractiveClerkSession {
    /// The session token's `fva` claim: `[first_factor_minutes,
    /// second_factor_minutes]` since the user last verified each factor, `-1`
    /// for a factor they have not enrolled. `None` on a v1 token. Delete
    /// account demands a recent one by Clerk's own `second_factor` rule
    /// ([`crate::services::account_deletion::reverification_ok`]).
    pub(crate) factor_verification_age: Option<[i64; 2]>,
}

// ── Admin authority needs an interactive session (settings launch PR 2.8) ───

/// What [`require_auth`] learned about THIS request's credential, for the
/// authority predicates below. `refused` records that a predicate denied
/// authority because the credential was not interactive, so `require_auth`
/// can answer the handler's resulting 403 with the specific
/// `interactive_session_required` code.
struct RequestAuthority {
    interactive: bool,
    refused: std::cell::Cell<bool>,
}

tokio::task_local! {
    static REQUEST_AUTHORITY: RequestAuthority;
}

/// May this request exercise admin/owner authority (or mint a credential,
/// start a connect flow, mutate a team)? `false` only for a request
/// [`require_auth`] authenticated with a NON-interactive credential: a Clerk
/// OAuth access token (no `sid` — add-in clients, or any DCR-registered
/// client when `REST_FOREIGN_OAUTH` is not `reject`), a `csat_` PAT or a
/// `csms_` add-in session.
///
/// **Enforced where authority is decided, not by route list.** Every
/// admin/owner predicate consults this — `routes::org_invites::is_admin`
/// (so `require_org_admin` and every route built on it), `routes::escrow`'s
/// admin check, `services::deletion::is_admin_role`, `tenancy::scope::
/// verify_org_admin`, `routes::teams`' account-authority check, and
/// `routes::checkout::require_purchase_authority` — so a new admin route
/// inherits it by using the existing helpers. Self-service routes with no
/// role check that still must be human-initiated (credential minting,
/// connect-flow starts, provider disconnects, team mutations) call it
/// directly.
///
/// Outside a request `require_auth` handled (MCP's own auth layer, workers,
/// handler unit tests) there is no recorded
/// credential and this answers `true`, exactly as before this existed.
///
/// Why not only `REST_FOREIGN_OAUTH=reject`: that is an environment variable
/// whose code default is `warn` (allow). Production sets `reject`; staging
/// and local do not, and a config change that dropped it would silently
/// re-open org-admin mutations to any MCP client a member authorised.
pub(crate) fn authority_allowed() -> bool {
    REQUEST_AUTHORITY
        .try_with(|a| {
            if !a.interactive {
                a.refused.set(true);
            }
            a.interactive
        })
        .unwrap_or(true)
}

/// Run `fut` as if `require_auth` had authenticated the request with an
/// interactive (`true`) or non-interactive (`false`) credential. Test-only:
/// lets a handler's pg suite prove its own authority check without the
/// whole router.
#[cfg(test)]
pub(crate) async fn with_request_authority_for_test<F: std::future::Future>(interactive: bool, fut: F) -> F::Output {
    REQUEST_AUTHORITY.scope(RequestAuthority { interactive, refused: std::cell::Cell::new(false) }, fut).await
}

/// [`authority_allowed`] for a handler that returns `Response`: `Some(403
/// interactive_session_required)` when refused.
pub(crate) fn interactive_session_refusal() -> Option<Response> {
    (!authority_allowed()).then(interactive_session_required)
}

fn interactive_session_required() -> Response {
    (
        StatusCode::FORBIDDEN,
        Json(json!({
            "error": "this action needs you signed in to CoralSwarm in a browser",
            "code": "interactive_session_required",
        })),
    )
        .into_response()
}

/// Run `next` with this request's [`RequestAuthority`] in scope, and turn a
/// 403 that a refused authority check produced into the specific
/// `interactive_session_required` answer (each module's own "admin only"
/// body would otherwise hide why).
async fn run_with_authority(interactive: bool, req: Request, next: Next) -> Response {
    let authority = RequestAuthority { interactive, refused: std::cell::Cell::new(false) };
    let (response, refused) = REQUEST_AUTHORITY
        .scope(authority, async move {
            let response = next.run(req).await;
            (response, REQUEST_AUTHORITY.with(|a| a.refused.get()))
        })
        .await;
    if refused && response.status() == StatusCode::FORBIDDEN {
        tracing::warn!(
            target: "auth.foreign_oauth",
            "refused an admin/credential action from a non-interactive credential"
        );
        return interactive_session_required();
    }
    response
}

// ── OAuth token classification (add-in Clerk OAuth sign-in) ──────────────────

/// How a verified Clerk token relates to the add-in OAuth client.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OauthKind {
    /// An interactive Clerk session JWT (carries `sid`). Unaffected.
    Session,
    /// An OAuth access token (no `sid`) minted for the Outlook/Teams add-in's
    /// own client (`ADDIN_CLERK_OAUTH_CLIENT_ID`).
    Addin,
    /// As [`OauthKind::Addin`], for the Gmail add-on's own client
    /// (`GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID`). Scoped to the Gmail add-on's
    /// routes only. If both env vars name the same client, `Addin` wins.
    GmailAddin,
    /// As [`OauthKind::Addin`], for the Google Meet side-panel add-on's own
    /// client (`MEET_ADDIN_CLERK_OAUTH_CLIENT_ID`). Scoped to the Meet panel's
    /// routes only. Checked after the Outlook and Gmail slots.
    MeetAddin,
    /// An OAuth access token (no `sid`) for any other client.
    Foreign,
}

/// Pure classification. A client id that is `None` (or empty) is that
/// feature being off. Only the `client_id` claim is compared: Clerk OAuth
/// access tokens carry it and no `azp`.
pub(crate) fn classify_oauth_token(
    claims: &crate::clerk::ClerkClaims,
    addin_client_id: Option<&str>,
    gmail_client_id: Option<&str>,
    meet_client_id: Option<&str>,
) -> OauthKind {
    if claims.sid.is_some() {
        return OauthKind::Session;
    }
    let Some(got) = claims.client_id.as_deref() else {
        return OauthKind::Foreign;
    };
    if addin_client_id.filter(|c| !c.is_empty()) == Some(got) {
        OauthKind::Addin
    } else if gmail_client_id.filter(|c| !c.is_empty()) == Some(got) {
        OauthKind::GmailAddin
    } else if meet_client_id.filter(|c| !c.is_empty()) == Some(got) {
        OauthKind::MeetAddin
    } else {
        OauthKind::Foreign
    }
}

/// `ADDIN_CLERK_OAUTH_CLIENT_ID` — read per request so it can be set without a
/// code change; unset/blank means the add-in OAuth path is off.
fn addin_oauth_client_id() -> Option<String> {
    env_client_id("ADDIN_CLERK_OAUTH_CLIENT_ID")
}

/// `GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID` — the Gmail add-on's own OAuth client.
fn gmail_addin_oauth_client_id() -> Option<String> {
    env_client_id("GMAIL_ADDIN_CLERK_OAUTH_CLIENT_ID")
}

/// `MEET_ADDIN_CLERK_OAUTH_CLIENT_ID` — the Google Meet side panel's own OAuth client.
fn meet_addin_oauth_client_id() -> Option<String> {
    env_client_id("MEET_ADDIN_CLERK_OAUTH_CLIENT_ID")
}

fn env_client_id(var: &str) -> Option<String> {
    std::env::var(var)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

/// `REST_FOREIGN_OAUTH`: what REST does with an OAuth token from a client
/// other than the add-in's.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ForeignOauthMode {
    Off,
    Warn,
    Reject,
}

impl ForeignOauthMode {
    /// Unset or unrecognised -> `Warn` (log only; behaviour unchanged).
    pub(crate) fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("off") => Self::Off,
            Some("reject") => Self::Reject,
            _ => Self::Warn,
        }
    }
}

fn rest_foreign_oauth_mode() -> ForeignOauthMode {
    ForeignOauthMode::parse(std::env::var("REST_FOREIGN_OAUTH").ok().as_deref())
}

// ── Helper: build a 401 response ─────────────────────────────────────────────

fn unauthorized(msg: &'static str) -> Response {
    (StatusCode::UNAUTHORIZED, Json(json!({"error": msg}))).into_response()
}

fn unauthorized_with_www_auth(msg: &'static str) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, Json(json!({"error": msg}))).into_response();
    resp.headers_mut().insert(
        axum::http::header::WWW_AUTHENTICATE,
        HeaderValue::from_static("Bearer error=\"invalid_token\""),
    );
    resp
}

// ── Middleware function ───────────────────────────────────────────────────────

/// Axum `from_fn_with_state` compatible middleware.
///
/// Designed to wrap a *protected* sub-router only; the open routes
/// (`/healthz`, `/readyz`, `/auth/*`) live outside this layer.
pub async fn require_auth(
    State(state): State<Arc<AppState>>,
    mut req: Request,
    next: Next,
) -> Response {
    // ── 1. Binding gate, phase 1: classify the Host BEFORE authenticating ────
    //
    // Pass-through unless a TenantRuntime is present AND enabled; when disabled
    // nothing here runs and behavior is byte-identical to today.
    //
    // This is the plan's "unknown subdomain → 404 fail-closed *before auth*".
    // Running it here — ahead of token extraction, JWT verification and, above
    // all, `resolve_clerk_context`'s provisioning writes — means a request to a
    // host that does not route anywhere costs one cached routing lookup and
    // writes NOTHING, and gets the same 404 whether or not it carried a token
    // (no "real subdomain vs no such subdomain" enumeration signal).
    //
    // The `Host` header is the ONLY input. `X-Forwarded-Host` and friends are
    // never consulted here or anywhere else on this path — see `HostPrecheck`'s
    // doc comment and the spoofed-forwarded-host regression probes in
    // `binding_gate_pg` / `cross_tenant_probe_pg`.
    let gate = state.tenant_runtime.as_ref().filter(|r| r.enabled);
    let host_precheck = match gate {
        Some(rt) => {
            let host = req
                .headers()
                .get(axum::http::header::HOST)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_owned();
            match rt.precheck_host(state.db.as_ref(), &host).await {
                Ok(pre) => Some(pre),
                // A host under our OWN apex whose label can never be a slug
                // (a Clerk-auto-generated org slug over 32 chars is the case
                // seen in production) gets the same 404 as any foreign host,
                // plus the reason — otherwise the only signal anyone has is a
                // bare 404 on their own subdomain. This is diagnostic only:
                // `precheck_host` already decided, and the reason is a pure
                // function of the `Host` header, so BOTH gate phases would
                // produce it identically and the "indistinguishable on the
                // wire" property `gate_reject_response` documents is
                // preserved. (In practice phase 2 can never see this: an
                // unroutable label is rejected here, pre-auth.)
                Err(rej) => {
                    return gate_reject_response_with_reason(
                        rej,
                        crate::tenancy::gate::unroutable_label_reason(&host, &rt.apex),
                    )
                }
            }
        }
        None => None,
    };

    // ── 2. Extract the Bearer token (stateless — no DB needed) ───────────────
    //
    // Check the header first so that a missing/malformed token always returns
    // 401 regardless of whether the database pool is configured.
    let raw_token = match req
        .headers()
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
    {
        Some(t) => t.to_owned(),
        None => return unauthorized("missing token"),
    };

    // ── 3. Require a database pool ────────────────────────────────────────────
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response();
        }
    };

    // ── 4. Authenticate, branching on the token's SHAPE ──────────────────────
    //
    // Both arms must produce `account_id`/`user_id`; `token_kind`/
    // `display_name` feed the binding gate in step 6 exactly the way an
    // ordinary PERSONAL Clerk token already does (`TokenKind::Personal` /
    // `None`) — see this module's doc comment on why the gate cannot tell a
    // PAT apart from a personal Clerk token, by design (a PAT is
    // personal-plane only, and can never be `TokenKind::PersonalOrg` either —
    // it carries no org claim of any kind to classify).
    let account_id: Uuid;
    let user_id: Uuid;
    let token_kind: crate::tenancy::gate::TokenKind;
    let display_name: Option<String>;

    if raw_token.starts_with(crate::routes::addin::ADDIN_TOKEN_PREFIX) {
        // Gmail Add-on PAT (`routes::addin`'s module doc) — the credential
        // itself IS the authentication; no Clerk JWT verification runs at
        // all on this branch.
        let identity = match crate::routes::addin::resolve_addin_token(&pool, &raw_token).await {
            Ok(Some(v)) => v,
            Ok(None) => return unauthorized_with_www_auth("invalid or expired token"),
            Err(e) => {
                tracing::error!(error = %e, "addin token lookup failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };
        account_id = identity.account_id;
        user_id = identity.user_id;
        token_kind = crate::tenancy::gate::TokenKind::Personal;
        display_name = None;
        req.extensions_mut().insert(UserContext {
            user_id,
            account_id,
            // An add-on token is always the pairing user's own — same
            // "owner" role a personal (no-org) Clerk token resolves to
            // below, never a role read from anything client-supplied.
            role: "owner".to_string(),
            device_id: "gmail-addin".to_string(),
        });
        // The PAT analogue of step 5's Clerk access-log capture below — best
        // effort, fire-and-forget, never fails the request. There is no
        // `sid` here to key a `user_access_log` row on, so this bumps
        // `addin_tokens.last_used_at` instead.
        crate::routes::addin::touch_last_used(pool.clone(), identity.token_id);
    } else if raw_token.starts_with(crate::routes::addin_microsoft::SESSION_TOKEN_PREFIX) {
        // Microsoft add-in session credential (`routes::addin_microsoft`'s
        // module doc) — the short-lived credential the Teams/Outlook panes get
        // from `POST /v1/addin/microsoft/session`. Unlike the Gmail PAT it MAY
        // name an org workspace, so the plane it binds is decided by the
        // workspace's LIVE access, never by anything on the token row alone:
        //   * resolve the session (unknown / revoked / expired / its link
        //     gone-disconnected-or-re-pointed → 401);
        //   * re-verify, on EVERY request, that the link's user may still act
        //     in the linked account (personal: its owner; org: a live
        //     `account_members` row, same query `/mcp` and the Clerk path use).
        //     Lost access → the same 401 a bogus token gets, so the pane
        //     re-exchanges and is told `link_required` — it is NEVER served a
        //     different plane instead;
        //   * org → `TokenKind::Org(clerk_org_id)` (the tenant plane through the
        //     binding gate below, exactly as an org Clerk session); personal →
        //     `TokenKind::Personal`. There is no arm that maps an org link to
        //     `Personal`.
        use crate::routes::addin_microsoft::{self as ms, AddinMicrosoftAuth, WorkspaceKind};
        let identity = match ms::resolve_session(&pool, &raw_token).await {
            Ok(Some(v)) => v,
            Ok(None) => return unauthorized_with_www_auth("invalid or expired token"),
            Err(e) => {
                tracing::error!(error = %e, "addin microsoft session lookup failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };
        let access = match ms::verify_workspace_access(&pool, identity.user_id, identity.account_id).await {
            Ok(Some(a)) => a,
            Ok(None) => return unauthorized_with_www_auth("invalid or expired token"),
            Err(e) => {
                tracing::error!(error = %e, "addin microsoft session access check failed");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };
        account_id = identity.account_id;
        user_id = identity.user_id;
        display_name = None;
        token_kind = match access.kind {
            WorkspaceKind::Personal => crate::tenancy::gate::TokenKind::Personal,
            WorkspaceKind::Org(org_id) => crate::tenancy::gate::TokenKind::Org(org_id),
        };
        req.extensions_mut().insert(UserContext {
            user_id,
            account_id,
            // The member's CURRENT role from `account_members` (org) or
            // `owner` (personal) — never read from the credential.
            role: access.role,
            device_id: "ms-addin".to_string(),
        });
        req.extensions_mut().insert(AddinMicrosoftAuth {
            session_id: identity.session_id,
            tenant_id: identity.tenant_id,
            object_id: identity.object_id,
        });
        ms::touch_last_used(pool.clone(), identity.session_id);
    } else {
        // ── Clerk JWT — the sole method before the Gmail Add-on, and still
        // the only one for every other client. Verify against the instance
        // JWKS and resolve/provision the caller; any non-JWT / invalid /
        // expired token → 401.
        let cfg = match crate::clerk::ClerkConfig::from_env() {
            Some(c) => c,
            None => {
                tracing::error!("Clerk is not configured; cannot authenticate requests");
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
        };
        let claims = match crate::clerk::verify(&raw_token, &cfg, &state.http_client).await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "clerk token verification failed");
                return unauthorized_with_www_auth("invalid or expired token");
            }
        };
        // OAuth-shaped token classification (add-in Clerk OAuth sign-in): a
        // Clerk OAuth access token has no `sid`. The add-in's own client is
        // accepted (scoped, membership re-checked live, below); any OTHER
        // OAuth client is governed by `REST_FOREIGN_OAUTH`.
        let oauth_kind = classify_oauth_token(
            &claims,
            addin_oauth_client_id().as_deref(),
            gmail_addin_oauth_client_id().as_deref(),
            meet_addin_oauth_client_id().as_deref(),
        );
        if let OauthKind::Foreign = oauth_kind {
            match rest_foreign_oauth_mode() {
                ForeignOauthMode::Off => {}
                ForeignOauthMode::Warn => {
                    tracing::warn!(
                        target: "auth.foreign_oauth",
                        client_id = %claims.client_id.as_deref().or(claims.azp.as_deref()).unwrap_or(""),
                        scope = %claims.scope.as_deref().unwrap_or(""),
                        mode = "warn",
                        "OAuth access token from a non-add-in client presented to REST (allowed; REST_FOREIGN_OAUTH=warn)"
                    );
                }
                ForeignOauthMode::Reject => {
                    tracing::warn!(
                        target: "auth.foreign_oauth",
                        client_id = %claims.client_id.as_deref().or(claims.azp.as_deref()).unwrap_or(""),
                        scope = %claims.scope.as_deref().unwrap_or(""),
                        mode = "reject",
                        "rejecting OAuth access token from a non-add-in client on REST (REST_FOREIGN_OAUTH=reject)"
                    );
                    return unauthorized_with_www_auth("invalid or expired token");
                }
            }
        }
        // Personal-organization translation (control migration 0208). Runs BEFORE
        // `resolve_clerk_context` so it is total: the active account, the
        // membership check and the binding gate all read the translated claims,
        // never the raw ones. A personal org appears in the web organization menu
        // like any other org, so this is the path a user takes when they select
        // their own ocean there.
        // Settings launch 2.4: a user whose account deletion is pending or
        // running may still reach exactly the deletion status and cancel
        // endpoints (so the web can show the countdown and offer Cancel);
        // every other route is refused at the door.
        let deletion_paths = matches!(
            (req.method(), req.uri().path()),
            (&axum::http::Method::GET, "/me/delete-account" | "/v1/me/delete-account")
                | (
                    &axum::http::Method::POST,
                    "/me/delete-account/cancel" | "/v1/me/delete-account/cancel"
                )
        );
        // In-app org deletion (control 0387): while an org is being deleted,
        // only its four deletion routes (status, request, undo, export) are
        // reachable with that org's token; the handlers then allow the owner
        // alone to act. Every other route is refused at the door.
        let org_deletion_paths = crate::routes::org_deletion::is_org_deletion_path(req.method(), req.uri().path());
        let claims = match normalize_personal_org_claim_for(&pool, claims, deletion_paths, org_deletion_paths).await {
            Ok(c) => c,
            Err(resp) => return resp,
        };
        let is_addin_oauth = matches!(oauth_kind, OauthKind::Addin | OauthKind::GmailAddin | OauthKind::MeetAddin);
        let ctx = match resolve_clerk_context(
            &state,
            &pool,
            &claims,
            match oauth_kind {
                OauthKind::Addin => Some("addin-oauth".to_string()),
                OauthKind::GmailAddin => Some("gmail-addin-oauth".to_string()),
                OauthKind::MeetAddin => Some("meet-addin-oauth".to_string()),
                _ => None,
            },
            // Exactly as MCP: an OAuth token's org claim never provisions a
            // membership; a LIVE `account_members` row is required per request.
            if is_addin_oauth {
                OrgMembershipPolicy::RequireExisting
            } else {
                OrgMembershipPolicy::ProvisionFromClaims
            },
        )
        .await
        {
            Ok(ctx) => ctx,
            Err(resp) => return resp,
        };
        if is_addin_oauth {
            req.extensions_mut().insert(crate::routes::addin_microsoft::AddinOAuthAuth {
                client: match oauth_kind {
                    OauthKind::GmailAddin => crate::routes::addin_microsoft::AddinOAuthClient::Gmail,
                    OauthKind::MeetAddin => crate::routes::addin_microsoft::AddinOAuthClient::Meet,
                    _ => crate::routes::addin_microsoft::AddinOAuthClient::Outlook,
                },
                personal_org: claims.is_personal_org(),
            });
        }
        account_id = ctx.account_id;
        user_id = ctx.user_id;
        token_kind = claims.token_kind();
        if oauth_kind == OauthKind::Session {
            req.extensions_mut().insert(InteractiveClerkSession {
                factor_verification_age: claims.claims().fva.as_ref().map(|v| {
                    [v.first().copied().unwrap_or(-1), v.get(1).copied().unwrap_or(-1)]
                }),
            });
        }
        display_name = claims.claims().name.clone();
        // #1496: what the door's one control read already proved about THIS
        // context, for opted-in read fences. Absent whenever `ctx` came off
        // the slow path; see `IdentityPrefetch::door_admission`. Interactive
        // Clerk sessions only: Clerk OAuth access tokens (the add-in clients
        // and any foreign client) keep the full control re-check.
        if let Some(door) = claims
            .prefetch()
            .filter(|_| oauth_kind == OauthKind::Session)
            .and_then(|p| p.door_admission(claims.claims().org_id.as_deref(), &ctx))
        {
            req.extensions_mut().insert(door);
        }
        req.extensions_mut().insert(ctx);

        // ── 5. Access-log capture (platform-admin-metrics PR 4 of 7) ─────────
        //
        // Best-effort, fire-and-forget "one row per login" write — see
        // `middleware::access_log`'s module doc for the full `sid`-first-sight
        // rationale. `control_db()` because `user_access_log` is a CONTROL-only
        // table (migration 0095's header) with no tenant-tree counterpart, same
        // plane `pool` above already is, just named for the table it targets.
        // Purely synchronous header reads followed by a detached `tokio::spawn` —
        // no `.await` here, so this can never add latency to this request, and
        // any DB failure is swallowed inside the spawned task, never surfaced.
        crate::middleware::access_log::spawn_capture(
            state.control_db().cloned(),
            req.headers(),
            user_id,
            claims.claims().sid.as_deref(),
        );
    }

    // ── 6. Binding gate, phase 2: bind the VERIFIED identity to the host ────
    //
    // The org-match check (`token_kind` vs the tenant this subdomain routes
    // to) and the apex's routing-row-by-org lookup both need a verified
    // identity, so they run here, on the classification phase 1 already made
    // from the `Host`. `gate` and `host_precheck` are `Some` together (both
    // derive from the same `state.tenant_runtime.filter(enabled)`), so when
    // the runtime is disabled no TenancyDecision is stamped and behavior is
    // byte-identical to today. `token_kind` is `TokenKind::Personal` for BOTH
    // an ordinary personal Clerk token and a PAT — the gate has no way to
    // tell them apart, which is exactly right: a PAT is personal-plane only
    // by construction (see `routes::addin`'s module doc — it can never be
    // `TokenKind::PersonalOrg` either, having no org claim to classify), so
    // it must resolve through the IDENTICAL path a personal Clerk token
    // already does, not a bespoke one.
    let mut tenancy_log = TenancyLogFields::unresolved(account_id);
    if let (Some(rt), Some(pre)) = (gate, host_precheck) {
        match run_binding_gate(
            rt,
            &pool,
            pre,
            token_kind,
            account_id,
            user_id,
            display_name.as_deref(),
        )
        .await
        {
            Ok(decision) => {
                tenancy_log = TenancyLogFields::from_decision(&decision, account_id);
                req.extensions_mut().insert(decision);
            }
            Err(resp) => return resp,
        }
    }

    // ── 7. Lift the tenancy fields onto the RESPONSE (claim-based tenancy
    //    PR 1.4, decision 12) ─────────────────────────────────────────────
    //
    // `request_id_and_latency` is the outermost layer and writes the one log
    // line per request, but by the time it runs, `req` has been moved into
    // `next.run` and whatever we stamped on its extensions is gone. The
    // response is the only channel back out, so the fields ride on it.
    let interactive = req.extensions().get::<InteractiveClerkSession>().is_some();
    let mut response = run_with_authority(interactive, req, next).await;
    response.extensions_mut().insert(tenancy_log);
    response
}

/// Which plane served a request, for the access log.
///
/// **Why this exists (decision 12 — "logging gates the cutover").** Claim-based
/// tenancy accepts, deliberately, that a wrong-tenant request becomes SILENT:
/// with the Host gone there is no second signal to disagree with the claim, so
/// a token with no `org_id` served the consumer plane is simply a valid
/// request. Decision 9 chose a visible org indicator over server-side
/// detection. That trade is only survivable if the failure is *discoverable
/// after the fact* — and this log line is the only place it can be discovered.
/// It is not observability garnish; it is the compensating control.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TenancyLogFields {
    /// `"org"` or `"consumer"` — the field to group by when asking "did an org
    /// user get served the shared plane?". `"unresolved"` when the tenant
    /// runtime is disabled, so a missing decision never renders as
    /// `"consumer"`; absence and the consumer plane are different facts.
    pub plane: &'static str,
    /// The bound org, present only on the tenant plane.
    pub org_id: Option<String>,
    pub account_id: Option<Uuid>,
}

impl TenancyLogFields {
    /// The tenant runtime is off (or this route is outside the gate), so no
    /// decision was made. Deliberately NOT `"consumer"`.
    fn unresolved(account_id: Uuid) -> Self {
        Self {
            plane: "unresolved",
            org_id: None,
            account_id: Some(account_id),
        }
    }

    /// `fallback_account` is the caller's own account from `UserContext` —
    /// used for the consumer plane, which has a real account even though the
    /// decision carries no context of its own. Without it a `consumer` line
    /// would be un-attributable, which is precisely the line we most need to
    /// be able to chase.
    fn from_decision(
        decision: &crate::tenancy::runtime::TenancyDecision,
        fallback_account: Uuid,
    ) -> Self {
        use crate::tenancy::runtime::TenancyDecision;
        match decision {
            TenancyDecision::Personal => Self {
                plane: "consumer",
                org_id: None,
                account_id: Some(fallback_account),
            },
            TenancyDecision::Org(ctx) => Self {
                plane: "org",
                org_id: Some(ctx.org_id.clone()),
                account_id: Some(ctx.account_id),
            },
        }
    }
}

// ── Binding gate ──────────────────────────────────────────────────────────────

/// The response for a request from a user whose account deletion exists.
/// `grace` and `running` are 403 (the client shows the deletion status and,
/// during grace, Cancel); once `completed` the credential is dead for good, so
/// 401 lets a client sign out.
pub(crate) fn account_deletion_refusal(state: &str) -> Response {
    match state {
        "completed" => (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "this account has been deleted", "code": "account_deleted"})),
        )
            .into_response(),
        "grace" => (
            StatusCode::FORBIDDEN,
            Json(json!({
                "error": "this account is scheduled for deletion; cancel the deletion to use it again",
                "code": "account_deletion_pending",
            })),
        )
            .into_response(),
        _ => (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "this account is being deleted", "code": "account_deletion_in_progress"})),
        )
            .into_response(),
    }
}

/// The response for a request bound to an org that is being deleted
/// (control 0387). `requested`/`grace` is `org_deletion_pending` (the web
/// shows the deletion screen: the owner can export or undo, everyone else
/// sees why the org is gone); after grace it is `org_deletion_in_progress`.
pub(crate) fn org_deletion_refusal(state: &str) -> Option<Response> {
    let code = crate::services::org_deletion::lockout_code(state)?;
    let msg = if code == "org_deletion_pending" {
        "this organization is scheduled for deletion; only its owner can export its data or undo the deletion"
    } else {
        "this organization is being deleted"
    };
    Some((StatusCode::FORBIDDEN, Json(json!({"error": msg, "code": code}))).into_response())
}

fn forbidden(msg: &'static str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}
fn not_found() -> Response {
    (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
}
/// A 404 that also says why this particular host can never route — same
/// status, same shape, one extra field. Used only for hosts under our own
/// apex (see [`gate::unroutable_label_reason`]), never for foreign domains,
/// so it discloses nothing about what we do or don't host.
///
/// [`gate::unroutable_label_reason`]: crate::tenancy::gate::unroutable_label_reason
fn not_found_with_reason(reason: &'static str) -> Response {
    (
        StatusCode::NOT_FOUND,
        Json(json!({"error": "not found", "reason": reason})),
    )
        .into_response()
}
fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}
/// The same 503 as [`service_unavailable`], same status and same `error`,
/// plus `"status":"provisioning"` so the web can say "your workspace is being
/// set up" instead of "check your connection" (self-serve signup PR A2). A
/// client that reads only `error` sees exactly what it saw before.
pub(crate) fn provisioning_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable", "status": "provisioning"})),
    )
        .into_response()
}

/// Map a gate rejection to its HTTP response. One mapping, shared by both gate
/// phases, so an early (pre-auth) reject and a late (post-auth) reject of the
/// same kind are indistinguishable on the wire.
fn gate_reject_response(rej: crate::tenancy::runtime::GateReject) -> Response {
    gate_reject_response_with_reason(rej, None)
}

/// As [`gate_reject_response`], plus an optional host diagnostic attached to a
/// 404. `reason` NEVER changes the status — only a 404 body gains a field, and
/// only when the caller could compute a reason from the `Host` header alone.
fn gate_reject_response_with_reason(
    rej: crate::tenancy::runtime::GateReject,
    reason: Option<&'static str>,
) -> Response {
    use crate::tenancy::runtime::GateReject;
    match (rej, reason) {
        (GateReject::NotFound, Some(why)) => not_found_with_reason(why),
        (GateReject::NotFound, None) => not_found(),
        (GateReject::Provisioning, _) => provisioning_unavailable(),
        (GateReject::Unavailable, _) => service_unavailable(),
        (GateReject::Forbidden(msg), _) => forbidden(msg),
    }
}

/// Finish the request's tenancy binding (delegates to
/// [`TenantRuntime::finish_binding`], phase 2) and map a rejection to an HTTP
/// response. `pre` is the pre-auth classification of this request's own `Host`
/// from [`TenantRuntime::precheck_host`].
///
/// Takes the [`TokenKind`](crate::tenancy::gate::TokenKind) itself, proved by
/// the caller, rather than a `&NormalizedClaims` or a bare org id — same
/// reasoning as [`TenantRuntime::finish_binding`]'s own doc comment (a
/// personal organization and a real one are both `Some(org_id)`, and only the
/// caller that already ran the classification may tell them apart). For an
/// ordinary Clerk token that caller is [`NormalizedClaims::token_kind`]; for
/// a Gmail Add-on PAT (which has no `NormalizedClaims` at all — no Clerk
/// verification runs on that path) it is [`require_auth`] passing
/// `TokenKind::Personal` directly, identical to what a personal (no-org)
/// Clerk token's own classification already produces. `display_name`, when
/// present, feeds the member-ocean first-sight trigger's display-name hint
/// (member-private-oceans PR 4) — the same claim `resolve_clerk_context`
/// already uses to seed/refresh `users.name` just above its call site, so a
/// first-sight member ocean is named from the SAME source of truth as the
/// user's own display name, with no extra query. Threaded as a plain
/// `Option<&str>` parameter rather than read off `NormalizedClaims` inside
/// this function, for the same PAT reason — a PAT has no claims to read one
/// from.
///
/// [`TenantRuntime::finish_binding`]: crate::tenancy::runtime::TenantRuntime::finish_binding
/// [`TenantRuntime::precheck_host`]: crate::tenancy::runtime::TenantRuntime::precheck_host
async fn run_binding_gate(
    rt: &crate::tenancy::runtime::TenantRuntime,
    control_pool: &sqlx::PgPool,
    pre: crate::tenancy::runtime::HostPrecheck,
    token: crate::tenancy::gate::TokenKind,
    account_id: Uuid,
    user_id: Uuid,
    display_name: Option<&str>,
) -> Result<crate::tenancy::runtime::TenancyDecision, Response> {
    rt.finish_binding(control_pool, pre, token, account_id, user_id, display_name)
        .await
        .map_err(gate_reject_response)
}

// ── Clerk identity resolution (ADR-0007 D4) ───────────────────────────────────

/// How `resolve_clerk_context` treats the caller's org membership when the
/// verified token carries an `org_id` claim (org-tenancy PR 2.2, fenced
/// further by PR 4.2).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum OrgMembershipPolicy {
    /// Provision from the token's own `org_id`/`org_role` claims (the
    /// interactive session path, `require_auth`): ensure the org `accounts`
    /// row exists, go through
    /// [`clerk_provision::refresh_org_membership_from_claims`] — a
    /// tombstone-/revocation-guarded, INSERT-ONLY membership write — and then
    /// run the SAME live [`live_org_membership`] re-check `RequireExisting`
    /// runs, rejecting the request if no row survives (org-tenancy PR 4.2).
    /// First-sight provisioning still works (a new member whose
    /// `organizationMembership.created` webhook hasn't landed yet, or local
    /// dev with no webhook receiver): the insert above creates the row, and
    /// the live re-check then finds it and lets the request through. But a
    /// request-path claim can neither resurrect a membership the offboarding
    /// webhook deleted nor change an existing row's role — the insert
    /// silently no-ops for a removed/revoked (org, user) — and, as of PR 4.2,
    /// a request whose insert no-op'd (or whose row was removed at any point
    /// before this call) is now REJECTED outright rather than served with the
    /// account/role resolved from the token's own claims.
    ///
    /// Before PR 4.2, this branch resolved `(account_id, role)` from
    /// `ensure_org_account`/the token's claims regardless of whether the
    /// insert above actually landed a row — so a member removed from an org
    /// kept full REST access (ingest, queries, graphs, nodes, atlas,
    /// relationships, conversations, ...) via `require_auth` for the rest of
    /// their token's TTL, even though `/mcp` (which has always used
    /// `RequireExisting`) already 403'd them on their very next call
    /// (adversarial review finding on PR 2.2). PR 4.2 closes that gap by
    /// making this policy's OUTCOME match `RequireExisting`'s in every case
    /// except the first-sight insert step.
    ///
    /// [`clerk_provision::refresh_org_membership_from_claims`]: crate::clerk_provision::refresh_org_membership_from_claims
    /// [`clerk_provision::ensure_org_membership`]: crate::clerk_provision::ensure_org_membership
    ProvisionFromClaims,
    /// Require a PRE-EXISTING `account_members` row and take the role from it,
    /// never from the token (MCP OAuth path, `mcp::auth::mcp_auth`). No row
    /// => the request itself is 403'd (live per-request revocation —
    /// offboarding takes effect on the removed member's very next `/mcp`
    /// call), and no write of any kind happens (no membership insert, no org
    /// `accounts` provisioning — zero side effects from a possibly stale
    /// token; an org unknown to the control plane simply 403s). As of PR 4.2
    /// this is no longer stricter than `ProvisionFromClaims` in the
    /// rejection dimension — both reject a removed/revoked member identically
    /// via the same [`live_org_membership`] query — only in the provisioning
    /// dimension (this policy never writes).
    RequireExisting,
}

/// Live `account_members` re-check shared by both [`OrgMembershipPolicy`]
/// branches (org-tenancy PR 4.2 unifies `require_auth`'s check with the one
/// `/mcp` already had since PR 2.2 — same query, same fail-closed semantics).
/// One indexed join from the verified `org_id` claim + this user's id to the
/// account row and CURRENT role, gated by the same `org_revocations`
/// wholesale-revocation guard `refresh_org_membership_from_claims`/
/// `ensure_org_membership` use. `Ok(None)` means "reject": no live
/// membership for this (org, user) pair — removed, never joined, or the org
/// is revoked — and the DB row is authoritative, never the token's own
/// `org_role` claim (a demotion is effective on the very next call, same as
/// a full removal). Deliberately uncached, same trade as the `/mcp` version
/// (revocation-on-next-call is the point; see the plan's control-plane-
/// caching open item for both together).
///
/// **In-app org deletion (control 0387).** While the org has a deletion in
/// `requested`, `grace`, `awaiting_approval` or `destroying`, nobody is a
/// live member here, the owner included. This is the shared predicate behind
/// the MEMBER-FACING paths that do not pass through the Clerk door's
/// `org_deletion_state` refusal: the Microsoft add-in sessions, the Teams bot
/// and onboarding, Slack interactivity and App Home (`resolve_clicking_user`,
/// the role read, `resolve_org_tenant_pool` via `org_deletion_state_for_org`),
/// and MCP selections. The owner's status/export/undo
/// routes read the control plane directly
/// (`services::org_deletion::org_facts`) and do not need this answer.
///
/// CAPTURE paths (webhook ingest, transcript ingest, meeting sync, schedulers)
/// must NOT use this: capture continues during grace so an owner's undo loses
/// nothing. They use [`live_org_membership_for_capture`], which keeps the
/// pre-deletion revocation-only semantics.
///
/// `pub(crate)` (claim-based tenancy §Phase 3 PR 3.2): also reused, unchanged,
/// by `mcp::tenant_selection::live_membership_for_selection` — a STORED
/// selection's org id gets the identical re-check a claim's org id gets here,
/// rather than a separately-maintained copy of this query. See that module's
/// doc for why the two must share one implementation.
pub(crate) async fn live_org_membership(
    pool: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT a.id, am.role FROM accounts a \
         JOIN account_members am ON am.account_id = a.id AND am.user_id = $2 \
         WHERE a.clerk_org_id = $1 \
           AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $1) \
           AND COALESCE(org_deletion_state_for_org($1), '') \
               NOT IN ('requested', 'grace', 'awaiting_approval', 'destroying')",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_optional(crate::tenancy::scope::request_read(pool))
    .await
}

/// Capture-side twin of [`live_org_membership`]: the same query WITHOUT the
/// in-app deletion refusal, i.e. a live member unless removed or the org is
/// revoked wholesale (`org_revocations`, written by the destroy step's
/// `revoke_access`). Capture continues through the deletion's grace period so
/// that an owner's undo loses nothing (`docs/plans/org-offboarding-design.md`);
/// once destruction starts, `revoke_access` makes this answer `None` exactly as
/// before. Use ONLY in capture/ingest paths. Anything a member can read or act
/// through stays on the strict [`live_org_membership`].
pub(crate) async fn live_org_membership_for_capture(
    pool: &sqlx::PgPool,
    org_id: &str,
    user_id: Uuid,
) -> Result<Option<(Uuid, String)>, sqlx::Error> {
    sqlx::query_as(
        "SELECT a.id, am.role FROM accounts a \
         JOIN account_members am ON am.account_id = a.id AND am.user_id = $2 \
         WHERE a.clerk_org_id = $1 \
           AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $1)",
    )
    .bind(org_id)
    .bind(user_id)
    .fetch_optional(pool)
    .await
}

// ── Personal organizations (control migration 0208) ──────────────────────────

/// Clerk claims that have been through [`normalize_personal_org_claim`].
///
/// [`resolve_clerk_context`] takes this instead of a bare
/// [`crate::clerk::ClerkClaims`], so **forgetting the personal-organization
/// translation on a new authentication path is a compile error rather than a
/// silent wrong-plane answer.**
///
/// That matters more than it looks. A skipped translation does not fail
/// loudly: a personal-org claim would reach `gate::decide`, find no routing
/// row, and 403 — which reads as "this org is not provisioned" and sends the
/// next reader hunting for a missing row that is missing on purpose. And the
/// inverse mistake, on a path that strips claims of its own, serves the caller
/// their personal ocean with a `200` while they believe they are in an
/// organization. Neither is visible from outside, which is exactly the failure
/// #804 recorded for `mcp::tenant_selection`: a guard that is green and
/// guarding nothing looks identical to one that works.
///
/// **Scope of the guarantee, stated honestly.** This forces the translation to
/// run before the caller's account is resolved. It does not force every later
/// reader of `org_id` to read the translated copy — `run_binding_gate` and
/// `resolve_mcp_tenancy` still take `&ClerkClaims`, and are handed
/// [`Self::claims`]. Those are fail-closed directions (an untranslated
/// personal org 403s at the gate), so they are guarded by consequence rather
/// than by type.
#[derive(Debug, Clone)]
pub(crate) struct NormalizedClaims {
    claims: crate::clerk::ClerkClaims,
    /// `Some(org_id)` when the claim named a personal organization belonging
    /// to this caller. The org id is ALSO still on `claims.org_id` — this
    /// field records the *classification*, which the claim alone cannot carry.
    personal_org: Option<String>,
    /// What the door's one control-plane read ALSO learned about this
    /// caller, for [`resolve_clerk_context`]'s fast path. `None` only for
    /// the test-only constructors, which therefore always take the slow
    /// (query-per-step) path — byte-identical to the pre-prefetch behavior.
    prefetch: Option<IdentityPrefetch>,
}

/// Everything [`resolve_clerk_context`] needs for an ALREADY-PROVISIONED
/// caller, read in the same round trip as the personal-org classification
/// (see [`classify_personal_org_claim`]). Each field is `None`/`false` whenever the
/// corresponding row is absent, and absence always means "take the existing
/// slow path", never "skip a step": a missing user still goes through
/// `ensure_user`, a missing membership still goes through
/// `ensure_org_account` → `refresh_org_membership_from_claims` →
/// [`live_org_membership`], a missing personal account still goes through
/// `ensure_personal_account`. The prefetch can only ever REMOVE queries whose
/// answer it already holds; it cannot change any answer.
#[derive(Debug, Clone)]
pub(crate) struct IdentityPrefetch {
    /// `users.id` for the token's `sub`, if that user already exists.
    user_id: Option<Uuid>,
    /// `(account_id, role)` for the token's `org_id` claim — the SAME
    /// predicate as [`live_org_membership`] (account row for the org, this
    /// user's `account_members` row, no `org_revocations` row), evaluated in
    /// the same statement as the rest. `None` is "reject or provision",
    /// exactly as `live_org_membership`'s `None` is.
    org_membership: Option<(Uuid, String)>,
    /// The caller's personal account-of-one (`personal_account::
    /// personal_account_id`'s predicate), read only for a no-org token.
    personal_account_id: Option<Uuid>,
    /// `account_deletions.state` for this Clerk subject, or `None` when no
    /// deletion exists (settings launch 2.4). Any `Some` refuses the request
    /// in [`normalize_personal_org_claim`] before a user row is resolved.
    deletion_state: Option<String>,
    /// `org_deletions.state` for the token's org claim (control 0387), or
    /// `None` when the token names no org or the org has no live deletion.
    /// A live one refuses the request in [`normalize_personal_org_claim`]
    /// unless it is one of the org-deletion routes.
    org_deletion_state: Option<String>,
    /// Whether `personal_account_id` (when `Some`) ALSO has this user's
    /// `account_members` row. Owning an account (`accounts.owner_user_id`,
    /// the personal-account predicate) is NOT membership, and the content
    /// readers' control fence (`source_evidence::control_actor_admitted_tx`)
    /// checks membership, so the personal arm of [`DoorAdmission`] needs this
    /// read too (#1496). Never consulted by [`resolve_clerk_context`]: it
    /// changes no auth answer.
    personal_membership: bool,
    /// When the statement that read every field above was SENT — the
    /// earliest instant its answer can describe. [`DoorAdmission`]'s
    /// freshness is measured from here, never from when the reply arrived.
    read_at: std::time::Instant,
}

impl IdentityPrefetch {
    /// The admission fact this prefetch proves for the [`UserContext`]
    /// `resolve_clerk_context` built from it, if any (#1496).
    ///
    /// Minted ONLY when the context's `(user_id, account_id)` is exactly the
    /// pair this prefetch's own rows name, so a context produced by the slow
    /// path (first-sight provisioning, `live_org_membership` after
    /// `ensure_org_account`) never carries one — it simply keeps the full
    /// control-plane re-check downstream. Two arms, each the SAME predicate
    /// `source_evidence::control_actor_admitted_tx` evaluates:
    ///
    /// * org claim: `org_membership` is `classify_personal_org_claim`'s
    ///   `accounts.clerk_org_id = claim` × this user's `account_members` row ×
    ///   no `org_revocations` row for the claim org.
    /// * no org claim: the personal account-of-one AND this user's
    ///   `account_members` row on it (`personal_membership`). Ownership
    ///   alone mints nothing.
    pub(crate) fn door_admission(
        &self,
        claim_org: Option<&str>,
        ctx: &UserContext,
    ) -> Option<DoorAdmission> {
        if self.user_id != Some(ctx.user_id) {
            return None;
        }
        let clerk_org_id = match claim_org {
            Some(org) => {
                let (acct, _) = self.org_membership.as_ref()?;
                if *acct != ctx.account_id {
                    return None;
                }
                Some(org.to_string())
            }
            None => {
                if self.personal_account_id != Some(ctx.account_id) || !self.personal_membership {
                    return None;
                }
                None
            }
        };
        Some(DoorAdmission {
            account_id: ctx.account_id,
            user_id: ctx.user_id,
            clerk_org_id,
            read_at: self.read_at,
        })
    }
}

/// What this request's own authentication read already proved on the
/// CONTROL plane about the caller's admission — carried as a request
/// extension so a content reader's control fence can skip re-reading it in a
/// second transaction (#1496: `GET /v1/conversations/{id}` paid a whole
/// `begin_account_scoped` on control for an answer `require_auth` had read
/// milliseconds earlier).
///
/// **Who may honour it.** Only a READ-side fence that would otherwise ask
/// exactly [`DoorAdmission::covers`]' question, and only through an explicit
/// opt-in (`source_evidence::still_admitted_at_door`). Every existing
/// `still_admitted*` caller keeps its own re-check unchanged; writers,
/// promotions, billing admission and the `*_in_tx` writer fences never see
/// this type. `grep -rn DoorAdmission backend/src` is the full list.
///
/// **What it cannot do.** It vouches for one `(account, user, org)` triple
/// and nothing else: no graph reachability (still read per graph on the
/// content plane), no role, no org-admin grant, no other account. It cannot
/// be built outside this module (private fields, no public constructor), it
/// is only inserted by `require_auth`'s Clerk branch for an INTERACTIVE
/// session token (`OauthKind::Session`; never a Clerk OAuth access token,
/// PAT, add-in session or MCP request), and it expires after
/// [`DoorAdmission::MAX_AGE`] — after that, or on any mismatch, the fence
/// runs its control transaction exactly as before.
///
/// **The security trade, stated plainly.** A revocation (member removed, org
/// revoked) that commits between the door read and the fence is observed by
/// the re-check and not by this fact. That window is bounded by `MAX_AGE`,
/// and is the same kind of window that already exists between the fence and
/// the response leaving the server. The NEXT request re-reads at the door, so
/// revocation is still effective on the next call.
#[derive(Clone, Debug)]
pub struct DoorAdmission {
    account_id: Uuid,
    user_id: Uuid,
    clerk_org_id: Option<String>,
    read_at: std::time::Instant,
}

impl DoorAdmission {
    /// How long after the door read the fact may stand in for a re-check.
    /// Longer than an ordinary read's time-to-fence, short enough that a
    /// handler which awaited slow work (blob I/O, a model call) before its
    /// fence falls back to the live read.
    pub const MAX_AGE: std::time::Duration = std::time::Duration::from_secs(2);

    /// Whether this fact answers `control_actor_admitted_tx(user, org)` —
    /// "`user` has an `account_members` row on `user.account_id` [whose
    /// account is `org` and `org` is not revoked]" — as of now.
    pub(crate) fn covers(&self, user: &UserContext, org: Option<&str>) -> bool {
        self.covers_at(user, org, std::time::Instant::now())
    }

    fn covers_at(&self, user: &UserContext, org: Option<&str>, now: std::time::Instant) -> bool {
        let fresh = now
            .checked_duration_since(self.read_at)
            .is_some_and(|age| age <= Self::MAX_AGE);
        fresh
            && self.user_id == user.user_id
            && self.account_id == user.account_id
            && match org {
                // Membership row on this account: both arms read it.
                None => true,
                // Org-qualified + unrevoked: only the org arm read that, and
                // only for the org it names.
                Some(org) => self.clerk_org_id.as_deref() == Some(org),
            }
    }

    /// Test-only constructor for the fence's own unit tests.
    #[cfg(test)]
    pub(crate) fn for_test(
        account_id: Uuid,
        user_id: Uuid,
        clerk_org_id: Option<&str>,
        read_at: std::time::Instant,
    ) -> Self {
        DoorAdmission { account_id, user_id, clerk_org_id: clerk_org_id.map(str::to_string), read_at }
    }
}

impl NormalizedClaims {
    /// Borrow the translated claims.
    pub(crate) fn claims(&self) -> &crate::clerk::ClerkClaims {
        &self.claims
    }

    /// The door's prefetched identity rows, if this value came through
    /// [`normalize_personal_org_claim`] (it always does in production).
    fn prefetch(&self) -> Option<&IdentityPrefetch> {
        self.prefetch.as_ref()
    }

    /// Which [`TokenKind`] the tenancy gate must be handed for this caller.
    ///
    /// Derived HERE, from the classification this type already carries, rather
    /// than reconstructed by each call site from an `Option<&str>` plus a
    /// boolean flag. A `bool` parameter next to an org id is not a gate — it
    /// is an invitation for one call site to pass the wrong one, and the whole
    /// point of `TokenKind::PersonalOrg` is that only a *proved* personal org
    /// may reach the shared plane.
    ///
    /// [`TokenKind`]: crate::tenancy::gate::TokenKind
    pub(crate) fn token_kind(&self) -> crate::tenancy::gate::TokenKind {
        use crate::tenancy::gate::TokenKind;
        match (&self.personal_org, self.claims.org_id.as_deref()) {
            (Some(org), _) => TokenKind::PersonalOrg(org.clone()),
            (None, Some(org)) => TokenKind::Org(org.to_string()),
            (None, None) => TokenKind::Personal,
        }
    }

    /// Whether this caller's claim named their own personal organization.
    pub(crate) fn is_personal_org(&self) -> bool {
        self.personal_org.is_some()
    }

    // `without_org` used to live here: it dropped the org claim (and the
    // classification with it) for `mcp::auth::claims_for_resource`, the ONLY
    // caller it ever had. #825 restored `user:org:read` on `/mcp`, which made
    // an `org_id` claim there a deliberate consent-screen choice rather than
    // a leftover, and that strip — with this helper — is gone. Nothing else
    // in the codebase ever needed to un-say an org claim, so it is deleted
    // rather than left as a loaded gun a future path could reach for.

    /// Build without running the translation. **Test-only**: production code
    /// must reach this type through [`normalize_personal_org_claim`], which is
    /// the entire point of the type existing.
    #[cfg(test)]
    pub(crate) fn for_test(claims: crate::clerk::ClerkClaims) -> Self {
        NormalizedClaims { claims, personal_org: None, prefetch: None }
    }

    /// Test-only: a caller whose claim names their OWN personal organization.
    #[cfg(test)]
    pub(crate) fn for_test_personal_org(claims: crate::clerk::ClerkClaims) -> Self {
        let personal_org = claims.org_id.clone();
        NormalizedClaims { claims, personal_org, prefetch: None }
    }
}

/// Classify the token's `org_id` claim against the `personal_orgs` map —
/// and, in the SAME statement, prefetch the identity rows
/// [`resolve_clerk_context`] is about to look up one by one.
///
/// One round trip per authenticated request, replacing what used to be up to
/// six serial control-plane reads before the handler (personal-org map,
/// `users`, `accounts`, membership presence, write-grant upsert, live
/// membership). Every arm is an indexed point lookup, LEFT JOINed so the
/// statement always yields exactly one row; a missing row shows up as NULL
/// and sends `resolve_clerk_context` down the unchanged provisioning path for
/// that step. The membership arm is deliberately the SAME predicate as
/// [`live_org_membership`] (org account × this user's `account_members` row
/// × no `org_revocations` row) rather than a looser one — this read IS the
/// fence for the common case, so it must not be allowed to drift from the
/// query the slow path and `/mcp` run. The ONE deliberate difference is the
/// in-app org-deletion refusal: the membership arm omits it (so the owner can
/// still reach the deletion routes), and the door applies the lockout itself
/// from the `org_deletion_state` column read in this same statement.
///
/// The map's `clerk_user_id` is compared against the token's own `sub`. That
/// comparison is deliberately the shortest path from token to decision: no
/// join, and no local row whose absence, staleness or provisioning order could
/// change the answer. It also has to work BEFORE
/// [`resolve_clerk_context`] has mapped the Clerk subject to a `users` row, so
/// there is no internal UUID available to compare against even if we wanted
/// one — which is why the `personal_orgs` arm keys on the claim text, not on
/// the `users` row this same statement happens to join.
pub(crate) async fn classify_personal_org_claim(
    pool: &sqlx::PgPool,
    claims: &crate::clerk::ClerkClaims,
) -> Result<(crate::tenancy::personal_org::PersonalOrgClaim, IdentityPrefetch), sqlx::Error> {
    use crate::tenancy::personal_org::PersonalOrgClaim;

    let org_id = claims.org_id.as_deref();
    // Taken BEFORE the statement is sent: the earliest instant its answer can
    // describe, which is what `DoorAdmission`'s freshness must be measured
    // from (#1496).
    let read_at = std::time::Instant::now();
    // `me` is the alias `PERSONAL_ACCOUNT_SUBQUERY` correlates on.
    //
    // The sixth column (#1496) is the personal account's `account_members`
    // row for this user — the same `(account_id, user_id)` row
    // `source_evidence::control_actor_admitted_tx` checks. Same statement,
    // same snapshot, one more index probe; NULL whenever there is an org
    // claim, exactly like the personal-account column beside it.
    let (owner, user_id, account_id, role, personal_account_id, personal_membership, deletion_state, org_deletion_state): (
        Option<String>,
        Option<Uuid>,
        Option<Uuid>,
        Option<String>,
        Option<Uuid>,
        Option<bool>,
        Option<String>,
        Option<String>,
    ) = sqlx::query_as(&format!(
        "SELECT \
             (SELECT p.clerk_user_id FROM personal_orgs p WHERE p.clerk_org_id = $2), \
             me.id, \
             a.id, \
             am.role, \
             CASE WHEN $2::text IS NULL THEN ({personal}) END, \
             CASE WHEN $2::text IS NULL THEN EXISTS ( \
                 SELECT 1 FROM account_members pm \
                 WHERE pm.user_id = me.id AND pm.account_id = ({personal})) END, \
             account_deletion_state_for_clerk_user($1), \
             CASE WHEN $2::text IS NOT NULL THEN org_deletion_state_for_org($2) END \
         FROM (SELECT 1) AS one \
         LEFT JOIN users me ON me.clerk_user_id = $1 \
         LEFT JOIN accounts a ON a.clerk_org_id = $2 \
              AND NOT EXISTS (SELECT 1 FROM org_revocations r WHERE r.clerk_org_id = $2) \
         LEFT JOIN account_members am ON am.account_id = a.id AND am.user_id = me.id",
        personal = crate::personal_account::PERSONAL_ACCOUNT_SUBQUERY,
    ))
    .bind(&claims.sub)
    .bind(org_id)
    .fetch_one(crate::tenancy::scope::request_read(pool))
    .await?;

    let claim = match (org_id, owner) {
        (None, _) => PersonalOrgClaim::NoOrgClaim,
        (Some(_), None) => PersonalOrgClaim::OrdinaryOrg,
        (Some(_), Some(owner_sub)) if owner_sub == claims.sub => {
            PersonalOrgClaim::PersonalOrgOfCaller
        }
        (Some(_), Some(_)) => PersonalOrgClaim::PersonalOrgOfAnotherUser,
    };
    let prefetch = IdentityPrefetch {
        user_id,
        org_membership: match (account_id, role) {
            (Some(acct), Some(role)) => Some((acct, role)),
            _ => None,
        },
        personal_account_id,
        deletion_state,
        org_deletion_state,
        personal_membership: personal_membership.unwrap_or(false),
        read_at,
    };
    Ok((claim, prefetch))
}

/// The door. Translate a personal-org claim back to the caller's ordinary
/// personal path, before anything downstream tries to resolve a tenant from
/// it.
///
/// **Both the REST path (`require_auth`) and the MCP path (`mcp::auth::mcp_auth`)
/// call this, and both call it before [`resolve_clerk_context`].** Running it
/// on only one of them would strand a user on the other: a personal org is an
/// ordinary Clerk org, so it appears in the web organization menu as well as on
/// the OAuth consent screen, and selecting it in either place puts its id on
/// every subsequent request.
///
/// Running it *before* `resolve_clerk_context` is required, not merely tidier.
/// That function asks `live_org_membership` whether the caller belongs to the
/// claimed org, and for a personal org the honest answer is **yes**: since
/// 2026-09-08 a personal org has a real `accounts` row and real
/// `account_members` rows. So the membership check does not stop it — it waves
/// it through as an ordinary org, and `resolve_clerk_context` binds it as a
/// tenant. The ordering is required because that check SUCCEEDS, not because it
/// fails.
///
/// A database error fails closed with 503, matching every other control-plane
/// read on this path.
pub(crate) async fn normalize_personal_org_claim(
    pool: &sqlx::PgPool,
    claims: crate::clerk::ClerkClaims,
) -> Result<NormalizedClaims, Response> {
    normalize_personal_org_claim_for(pool, claims, false, false).await
}

/// [`normalize_personal_org_claim`], with `deletion_paths` true only for the
/// two account-deletion endpoints: a user whose deletion is in `grace` or
/// `running` is let through to them (and nowhere else). A `completed`
/// deletion is refused on every path.
pub(crate) async fn normalize_personal_org_claim_for(
    pool: &sqlx::PgPool,
    claims: crate::clerk::ClerkClaims,
    deletion_paths: bool,
    org_deletion_paths: bool,
) -> Result<NormalizedClaims, Response> {
    use crate::tenancy::personal_org::{resolve_personal_org_plane, ClaimAction};

    let (claim, prefetch) = match classify_personal_org_claim(pool, &claims).await {
        Ok(c) => c,
        Err(e) => {
            tracing::error!(error = %e, "personal-org map lookup failed");
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response());
        }
    };
    // Settings launch 2.4: a user whose account is being (or has been)
    // deleted makes no further authenticated request. Checked first, before the
    // personal-org decision and before any user is resolved or provisioned.
    if let Some(state) = prefetch.deletion_state.as_deref().filter(|st| !(deletion_paths && *st != "completed")) {
        tracing::warn!(
            target: "auth.account_deletion",
            deletion_state = state,
            "refusing request: the account is being deleted"
        );
        return Err(account_deletion_refusal(state));
    }
    // In-app org deletion (control 0387): the token names an org whose
    // deletion is live. Members are locked out from the moment it is
    // requested; only the org-deletion routes get through (their handlers
    // admit the owner alone).
    if !org_deletion_paths {
        if let Some(refusal) = prefetch.org_deletion_state.as_deref().and_then(org_deletion_refusal) {
            tracing::warn!(
                target: "auth.org_deletion",
                org_deletion_state = prefetch.org_deletion_state.as_deref().unwrap_or("-"),
                "refusing request: the organization is being deleted"
            );
            return Err(refusal);
        }
    }
    match resolve_personal_org_plane(&claim) {
        ClaimAction::KeepClaim => Ok(NormalizedClaims {
            claims,
            personal_org: None,
            prefetch: Some(prefetch),
        }),
        ClaimAction::ServeAsPersonalOrg => {
            let org = claims.org_id.clone();
            tracing::info!(
                target: "auth.personal_org",
                reason = "personal_org_served_on_shared_plane",
                org = %org.as_deref().unwrap_or(""),
                "serving this caller's own personal organization on the shared plane"
            );
            // The claim is KEPT, not stripped. `resolve_clerk_context` resolves
            // the organization's OWN `accounts` row from it (provisioned by
            // Clerk's `organization.created` webhook), and `token_kind` turns
            // it into `TokenKind::PersonalOrg` for the gate.
            Ok(NormalizedClaims {
                claims,
                personal_org: org,
                prefetch: Some(prefetch),
            })
        }
        ClaimAction::Refuse(msg) => {
            tracing::warn!(
                target: "auth.personal_org",
                reason = "personal_org_of_another_user",
                org = %claims.org_id.as_deref().unwrap_or(""),
                sub = %claims.sub,
                "refusing: this organization stands for a different user\'s ocean"
            );
            Err(forbidden(msg))
        }
    }
}

/// Bound on the fire-and-forget `users.name` refresh `resolve_clerk_context`
/// spawns off the request's critical path — long enough for an ordinary
/// `UPDATE` under normal load, short enough that a wedged/saturated pool
/// doesn't accumulate an unbounded number of stuck background tasks.
const DISPLAY_NAME_REFRESH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(2);

/// Build the [`UserContext`] for a verified Clerk session token, provisioning
/// the internal user/account rows on first sight (shared with the webhook via
/// `clerk_provision`). The active account is the org account when the session has
/// an org context, else the user's personal account-of-one (Model B). The
/// reachable-ocean union keys off `user_id`, so a caller can still read across
/// all their tenancies regardless of active account.
///
/// `device_id_override`, when `Some`, replaces the session's `sid`-derived
/// device id. The MCP auth path (`mcp::auth::mcp_auth`) uses this: OAuth access
/// tokens carry no `sid`, and MCP callers need a stable per-connected-app device
/// id (`mcp:{client_id}`) rather than the `"clerk"` fallback every MCP client
/// would otherwise share. `None` preserves the original session-JWT behavior.
///
/// `org_membership` picks how an `org_id` claim is bound to a control-plane
/// membership — see [`OrgMembershipPolicy`]. Personal (no-org) tokens are
/// unaffected by the choice.
pub(crate) async fn resolve_clerk_context(
    state: &AppState,
    pool: &sqlx::PgPool,
    claims: &NormalizedClaims,
    device_id_override: Option<String>,
    org_membership: OrgMembershipPolicy,
) -> Result<UserContext, Response> {
    use crate::clerk_provision as prov;

    // Unwrapped once, here, so the rest of this function is unchanged. The
    // type's job is done the moment the caller had to produce one.
    let claims_norm = claims;
    let claims = claims_norm.claims();

    let map_err = |e: prov::ProvisionError| -> Response {
        match e {
            prov::ProvisionError::Db(err) => {
                tracing::warn!(error = %err, "clerk provisioning DB error");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response()
            }
            prov::ProvisionError::Store(msg) => {
                tracing::error!(error = %msg, "clerk provisioning store error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({"error": "internal error"})),
                )
                    .into_response()
            }
            // Org-creation gate (org-tenancy PR 3.3): a stray org JWT (or a
            // first-sight sign-in) for an org this deployment has never
            // provisioned, under `ORG_SELFSERVE_DISABLED`. A clean, permanent
            // rejection — never a shadow account. 403, not 401: the token
            // itself verified fine, it's this org that isn't provisioned.
            prov::ProvisionError::SelfServeDisabled => {
                tracing::warn!(
                    org_id = %claims.org_id.as_deref().unwrap_or(""),
                    "clerk provisioning: org not provisioned (self-serve org creation disabled)"
                );
                (
                    StatusCode::FORBIDDEN,
                    Json(json!({"error": "organization not provisioned — contact support"})),
                )
                    .into_response()
            }
            // Legacy-token fence (org-tenancy PR 4.2): the org has already
            // been wholesale-revoked and never had a control `accounts` row —
            // see `ensure_org_account`'s doc comment. 403, not 401/404: the
            // token verified fine, it's this org that is permanently gone.
            prov::ProvisionError::AccountDeleted => account_deletion_refusal("completed"),
            prov::ProvisionError::OrgRevoked => {
                tracing::warn!(
                    target: "auth.require_auth",
                    reason = "org_revoked",
                    org_id = %claims.org_id.as_deref().unwrap_or(""),
                    "rejecting org-context request: organization is wholesale-revoked"
                );
                forbidden("organization is not active")
            }
            // Billing self-serve PR F: unreachable from THIS call path in
            // practice — `refresh_org_membership_from_claims` (the only
            // membership writer this function calls) deliberately never
            // calls `billing::seat_lifecycle::acquire_seat`/`release_seat`
            // (see that function's own doc comment: no Stripe call on the
            // hot auth path) — but `ProvisionError` is one shared enum
            // across every `clerk_provision` writer, so this match must
            // still be exhaustive. Treated like `ProvisionError::Db`: a
            // transient backend failure, 503, never surfaced as an org-
            // access decision.
            prov::ProvisionError::SeatLifecycle(err) => {
                tracing::warn!(error = %err, "clerk provisioning: seat lifecycle error (unreachable from the request-path claims refresh)");
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response()
            }
            // Only `ensure_org_membership` (webhook path) returns this; the
            // claims refresh never does. Exhaustiveness only: transient, 503.
            prov::ProvisionError::RemovedSinceObserved => (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "service unavailable"})),
            )
                .into_response(),
        }
    };

    // Hot-path collapse: the door (`normalize_personal_org_claim`) already
    // read this caller's `users`/`accounts`/`account_members`
    // rows in ONE statement (`classify_personal_org_claim`).
    // Each step below consults that prefetch first and only falls back to its
    // original query-per-step path when the prefetch has no row for it — so
    // first-sight provisioning and the membership insert still run through
    // the exact functions (and guards) they always did. The prefetch never widens anything: every `Some` it hands
    // out is a row the slow path's own query would have returned.
    let prefetch = claims_norm.prefetch();
    let user_id = match prefetch.and_then(|p| p.user_id) {
        Some(id) => id,
        None => prov::ensure_user(state, pool, &claims.sub, None, claims.name.as_deref())
            .await
            .map_err(map_err)?,
    };
    // Best-effort refresh of `users.name` from the verified token's `name`
    // claim on EVERY auth (not just first sight) — see `sync_user_display_name`'s
    // own doc comment for why this never adds a live Clerk API call. Dispatched
    // OFF this request's critical path (fire-and-forget `tokio::spawn`, same
    // pattern as the `last_seen_at` touch above) rather than awaited inline:
    // this is a single non-critical `UPDATE`, and nothing downstream in THIS
    // request reads `users.name` back, so stalling every authenticated request
    // on it would trade real latency for no observable benefit. Only spawned
    // when there's actually a claim to write (skips the no-op case entirely
    // rather than spawning a task that would just return immediately). Bounded
    // by `DISPLAY_NAME_REFRESH_TIMEOUT` so a wedged DB can't leak an unbounded
    // number of in-flight tasks; `sync_user_display_name` already logs/swallows
    // its own DB errors, so this timeout only needs to catch "took too long".
    if let Some(name) = claims.name.clone() {
        let pool_refresh = pool.clone();
        tokio::spawn(async move {
            if tokio::time::timeout(
                DISPLAY_NAME_REFRESH_TIMEOUT,
                prov::sync_user_display_name(&pool_refresh, user_id, Some(name.as_str())),
            )
            .await
            .is_err()
            {
                tracing::warn!(
                    user_id = %user_id,
                    timeout_secs = DISPLAY_NAME_REFRESH_TIMEOUT.as_secs(),
                    "users.name refresh from Clerk claim timed out"
                );
            }
        });
    }
    let (account_id, role) = match claims.org_id.as_deref() {
        Some(org_id) => match org_membership {
            OrgMembershipPolicy::ProvisionFromClaims
                if prefetch.is_some_and(|p| p.org_membership.is_some()) =>
            {
                // Already-provisioned member, the overwhelmingly common
                // case: the door's prefetch found this user's LIVE
                // `account_members` row under an unrevoked org account — the
                // same membership predicate `live_org_membership` below
                // evaluates, read microseconds ago in the same request.
                // The prefetch INTENTIONALLY omits `live_org_membership`'s
                // in-app org-deletion refusal: the Clerk door applies the
                // deletion lockout itself from the same statement
                // (`org_deletion_state`), and lets the owner through to the
                // deletion routes (status / export / undo), which a
                // `live_org_membership` answer of `None` would shut out. On this branch
                // the slow path's `ensure_org_account` (account exists) and
                // `refresh_org_membership_from_claims` (membership exists,
                // so its presence check short-circuits before any INSERT)
                // would both be pure no-ops, so no statement is sent at
                // all. A member REMOVED since the token was
                // minted has no `account_members` row, so the prefetch holds
                // `None`, this arm does not match, and the request takes the
                // full slow path below and is rejected by the same
                // `live_org_membership` fence as before — zero caching, zero
                // staleness.
                let p = prefetch.expect("guarded by the match arm");
                let (acct, role) = p.org_membership.clone().expect("guarded by the match arm");
                (acct, role)
            }
            OrgMembershipPolicy::ProvisionFromClaims => {
                let acct = prov::ensure_org_account(state, pool, org_id, user_id, None)
                    .await
                    .map_err(map_err)?;
                let claimed_role = prov::map_org_role(claims.org_role.as_deref());
                // Tombstone-/revocation-guarded + insert-only (PR 2.2 P0 fix):
                // first-sight provisioning only — never resurrects a
                // webhook-removed membership, never rewrites an existing
                // row's role. See `OrgMembershipPolicy::ProvisionFromClaims`.
                // On its own this does NOT reject anything — a blocked insert
                // silently no-ops — so it must never be the last word; the
                // live re-check just below is what actually gates the
                // request (org-tenancy PR 4.2, the `require_auth` analogue of
                // `/mcp`'s `RequireExisting` fence).
                prov::refresh_org_membership_from_claims(pool, org_id, user_id, claimed_role)
                    .await
                    .map_err(map_err)?;
                match live_org_membership(pool, org_id, user_id)
                    .await
                    .map_err(|e| map_err(prov::ProvisionError::Db(e)))?
                {
                    Some((acct2, role)) => {
                        debug_assert_eq!(
                            acct, acct2,
                            "ensure_org_account and live_org_membership must resolve the same account"
                        );
                        (acct, role)
                    }
                    None => {
                        // Legacy-token fence (org-tenancy PR 4.2): the caller
                        // is not a live member of this org — removed since
                        // the token was minted, or the org was revoked
                        // wholesale — even though the token itself is still
                        // signature-valid and unexpired. Closes the gap the
                        // PR 2.2 review flagged: before this check,
                        // `require_auth` (ingest, queries, graphs, nodes,
                        // atlas, relationships, conversations, ...) kept
                        // honoring a stale org token's claims for the token's
                        // full TTL while `/mcp` was already fenced. Fail
                        // closed, before any handler/content read or write.
                        tracing::warn!(
                            target: "auth.require_auth",
                            reason = "org_membership_revoked_or_missing",
                            org_id,
                            user_id = %user_id,
                            "rejecting require_auth org-context request: no live account_members row"
                        );
                        return Err(forbidden("not a member of this organization"));
                    }
                }
            }
            OrgMembershipPolicy::RequireExisting => {
                // Live membership re-check (org-tenancy PR 2.2, factored into
                // `live_org_membership` by PR 4.2 for reuse by the
                // `ProvisionFromClaims` branch above): one indexed lookup
                // joining the org account to THIS user's membership row. No
                // row (member removed, or org never provisioned) => 403.
                // Role comes from the DB row, not the token — a demotion is
                // effective on the next call, same as a removal. DB errors
                // map to 503 via `map_err`'s Db arm: closed either way.
                //
                // The door's prefetch already evaluated this exact predicate
                // (see `classify_personal_org_claim`), so when it is present
                // its answer — `Some` or `None` — is used as-is rather than
                // re-asked; the query only runs when there is no prefetch
                // (test-only constructors).
                let live = match prefetch {
                    Some(p) => p.org_membership.clone(),
                    None => live_org_membership(pool, org_id, user_id)
                        .await
                        .map_err(|e| map_err(prov::ProvisionError::Db(e)))?,
                };
                match live {
                    Some((acct, role)) => (acct, role),
                    None => {
                        tracing::warn!(
                            target: "mcp.auth",
                            reason = "org_membership_revoked_or_missing",
                            org_id,
                            user_id = %user_id,
                            "rejecting org-context request: no live account_members row"
                        );
                        return Err(forbidden("not a member of this organization"));
                    }
                }
            }
        },
        None => (
            // The prefetch ran `ensure_personal_account`'s own find step
            // (`personal_account::PERSONAL_ACCOUNT_SUBQUERY`) against the
            // `users` row it joined; a hit skips that function's identical
            // first query, a miss still goes through its race-safe create.
            match prefetch.and_then(|p| p.personal_account_id) {
                Some(id) => id,
                None => prov::ensure_personal_account(state, pool, user_id)
                    .await
                    .map_err(map_err)?,
            },
            "owner".to_string(),
        ),
    };
    let device_id = device_id_override
        .unwrap_or_else(|| claims.sid.clone().unwrap_or_else(|| "clerk".to_string()));
    Ok(UserContext {
        user_id,
        account_id,
        role,
        device_id,
    })
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        middleware,
        routing::get,
        Router,
    };
    use tower::ServiceExt; // oneshot

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    // ── OAuth token classification (add-in Clerk OAuth sign-in) ──────────────

    fn claims_json(v: serde_json::Value) -> crate::clerk::ClerkClaims {
        serde_json::from_value(v).expect("claims")
    }

    const ADDIN_CLIENT: &str = "example_18";

    // ── Door admission fact (#1496) ──────────────────────────────────────────

    fn ctx_for(user_id: Uuid, account_id: Uuid) -> UserContext {
        UserContext { user_id, account_id, role: "owner".into(), device_id: "t".into() }
    }

    fn prefetch_for(user: Uuid) -> IdentityPrefetch {
        IdentityPrefetch {
            user_id: Some(user),
            org_membership: None,
            personal_account_id: None,
            deletion_state: None,
            org_deletion_state: None,
            personal_membership: false,
            read_at: std::time::Instant::now(),
        }
    }

    #[test]
    fn door_admission_is_minted_only_for_the_context_the_prefetch_names() {
        let (user, acct, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let ctx = ctx_for(user, acct);

        // Org arm: the live, unrevoked membership row for exactly this pair.
        let org = IdentityPrefetch { org_membership: Some((acct, "member".into())), ..prefetch_for(user) };
        let door = org.door_admission(Some("org_1"), &ctx).expect("org member is admitted at the door");
        assert!(door.covers(&ctx, Some("org_1")));
        // ...and that membership row also answers the un-org-qualified question.
        assert!(door.covers(&ctx, None));
        // A different account than the prefetch row (slow-path context): none.
        assert!(org.door_admission(Some("org_1"), &ctx_for(user, other)).is_none());
        // Slow path / provisioning: no live membership row was read.
        assert!(prefetch_for(user).door_admission(Some("org_1"), &ctx).is_none());
        // A user the prefetch did not resolve (first sight): none.
        let unknown = IdentityPrefetch { user_id: None, ..org.clone() };
        assert!(unknown.door_admission(Some("org_1"), &ctx).is_none());
        let someone_else = IdentityPrefetch { user_id: Some(other), ..org };
        assert!(someone_else.door_admission(Some("org_1"), &ctx).is_none());

        // Personal arm: ownership is NOT membership. The control fence checks
        // `account_members`, so an owned account without the row mints nothing.
        let owned = IdentityPrefetch { personal_account_id: Some(acct), ..prefetch_for(user) };
        assert!(owned.door_admission(None, &ctx).is_none(), "ownership alone must not vouch for membership");
        let member = IdentityPrefetch { personal_membership: true, ..owned };
        let door = member.door_admission(None, &ctx).expect("personal member is admitted at the door");
        assert!(door.covers(&ctx, None));
        // The personal arm never read an org or a revocation: it cannot
        // answer an org-qualified question for any org.
        assert!(!door.covers(&ctx, Some("org_1")));
        assert!(member.door_admission(None, &ctx_for(user, other)).is_none());
        // A personal-account row read for a no-org token is not an org fact.
        assert!(member.door_admission(Some("org_1"), &ctx).is_none());
    }

    #[test]
    fn door_admission_covers_only_its_own_triple_and_only_while_fresh() {
        let (user, acct, other) = (Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4());
        let read_at = std::time::Instant::now();
        let door = DoorAdmission::for_test(acct, user, Some("org_1"), read_at);
        let ctx = ctx_for(user, acct);
        let at = |d: std::time::Duration| read_at + d;

        assert!(door.covers_at(&ctx, Some("org_1"), at(std::time::Duration::ZERO)));
        assert!(door.covers_at(&ctx, Some("org_1"), at(DoorAdmission::MAX_AGE)));
        // Stale: the fence must take the live read again.
        assert!(!door.covers_at(&ctx, Some("org_1"), at(DoorAdmission::MAX_AGE + std::time::Duration::from_millis(1))));
        // A clock that reads before the door read is not "fresh".
        if let Some(before) = read_at.checked_sub(std::time::Duration::from_millis(1)) {
            assert!(!door.covers_at(&ctx, Some("org_1"), before));
        }
        // Any other org, account or user: re-check.
        assert!(!door.covers_at(&ctx, Some("org_2"), read_at));
        assert!(!door.covers_at(&ctx_for(user, other), Some("org_1"), read_at));
        assert!(!door.covers_at(&ctx_for(other, acct), Some("org_1"), read_at));
        assert!(!door.covers_at(&ctx_for(user, other), None, read_at));
    }

    #[test]
    fn addin_oauth_token_is_recognised_only_by_client_id_and_no_sid() {
        let tok = claims_json(json!({"sub":"user_a","exp":0,"client_id":ADDIN_CLIENT,"org_id":"org_x"}));
        assert_eq!(classify_oauth_token(&tok, Some(ADDIN_CLIENT), None, None), OauthKind::Addin);
        // Feature off (env unset or blank): never Addin, falls to Foreign.
        assert_eq!(classify_oauth_token(&tok, None, None, None), OauthKind::Foreign);
        assert_eq!(classify_oauth_token(&tok, Some(""), Some(""), None), OauthKind::Foreign);
        // Another client id, and `azp` alone, never qualify.
        let other = claims_json(json!({"sub":"user_a","exp":0,"client_id":"someone_else"}));
        assert_eq!(classify_oauth_token(&other, Some(ADDIN_CLIENT), Some("gm"), None), OauthKind::Foreign);
        let azp = claims_json(json!({"sub":"user_a","exp":0,"azp":ADDIN_CLIENT}));
        assert_eq!(classify_oauth_token(&azp, Some(ADDIN_CLIENT), None, None), OauthKind::Foreign);
    }

    #[test]
    fn gmail_client_is_classified_separately_and_off_when_unset() {
        let gm = claims_json(json!({"sub":"user_a","exp":0,"client_id":"GMAIL_CLIENT"}));
        assert_eq!(classify_oauth_token(&gm, Some(ADDIN_CLIENT), Some("GMAIL_CLIENT"), None), OauthKind::GmailAddin);
        // Each client id only matches its own slot.
        assert_eq!(classify_oauth_token(&gm, Some("GMAIL_CLIENT"), None, None), OauthKind::Addin);
        // Both unset -> foreign, so REST_FOREIGN_OAUTH governs it.
        assert_eq!(classify_oauth_token(&gm, None, None, None), OauthKind::Foreign);
        assert_eq!(classify_oauth_token(&gm, Some(ADDIN_CLIENT), Some(""), None), OauthKind::Foreign);
    }

    #[test]
    fn meet_client_is_classified_separately_and_off_when_unset() {
        let mt = claims_json(json!({"sub":"user_a","exp":0,"client_id":"MEET_CLIENT","org_id":"org_x"}));
        assert_eq!(
            classify_oauth_token(&mt, Some(ADDIN_CLIENT), Some("GMAIL_CLIENT"), Some("MEET_CLIENT")),
            OauthKind::MeetAddin
        );
        // The Meet slot never matches another add-in's client id...
        let gm = claims_json(json!({"sub":"user_a","exp":0,"client_id":"GMAIL_CLIENT"}));
        assert_eq!(
            classify_oauth_token(&gm, Some(ADDIN_CLIENT), Some("GMAIL_CLIENT"), Some("MEET_CLIENT")),
            OauthKind::GmailAddin
        );
        let ol = claims_json(json!({"sub":"user_a","exp":0,"client_id":ADDIN_CLIENT}));
        assert_eq!(
            classify_oauth_token(&ol, Some(ADDIN_CLIENT), Some("GMAIL_CLIENT"), Some("MEET_CLIENT")),
            OauthKind::Addin
        );
        // ...and unset / blank means the Meet token is foreign.
        assert_eq!(classify_oauth_token(&mt, Some(ADDIN_CLIENT), Some("GMAIL_CLIENT"), None), OauthKind::Foreign);
        assert_eq!(classify_oauth_token(&mt, None, None, Some("")), OauthKind::Foreign);
        // A session JWT carrying the Meet client id is still a session.
        let sess = claims_json(json!({"sub":"user_a","exp":0,"sid":"s","client_id":"MEET_CLIENT"}));
        assert_eq!(classify_oauth_token(&sess, None, None, Some("MEET_CLIENT")), OauthKind::Session);
    }

    #[test]
    fn a_session_jwt_is_never_classified_as_oauth_even_with_the_addin_client_id() {
        let sess = claims_json(json!({"sub":"user_a","exp":0,"sid":"sess_1"}));
        assert_eq!(classify_oauth_token(&sess, Some(ADDIN_CLIENT), None, None), OauthKind::Session);
        let spoof = claims_json(json!({"sub":"user_a","exp":0,"sid":"sess_1","client_id":ADDIN_CLIENT}));
        assert_eq!(classify_oauth_token(&spoof, Some(ADDIN_CLIENT), None, None), OauthKind::Session);
    }

    #[test]
    fn foreign_oauth_mode_defaults_to_warn_and_parses_the_three_modes() {
        assert_eq!(ForeignOauthMode::parse(None), ForeignOauthMode::Warn);
        assert_eq!(ForeignOauthMode::parse(Some("")), ForeignOauthMode::Warn);
        assert_eq!(ForeignOauthMode::parse(Some("bogus")), ForeignOauthMode::Warn);
        assert_eq!(ForeignOauthMode::parse(Some("warn")), ForeignOauthMode::Warn);
        assert_eq!(ForeignOauthMode::parse(Some("off")), ForeignOauthMode::Off);
        assert_eq!(ForeignOauthMode::parse(Some(" REJECT ")), ForeignOauthMode::Reject);
    }

    // ── Admin authority needs an interactive session (settings PR 2.8) ─────

    /// Through the real wrapper: inside a NON-interactive request every
    /// admin predicate refuses and the handler's 403 becomes
    /// `interactive_session_required`; an interactive request is untouched;
    /// a non-admin 403 (refusal never consulted) keeps its own body; and
    /// outside any request the predicate answers `true` as before.
    #[tokio::test]
    async fn non_interactive_requests_hold_no_admin_authority() {
        use axum::routing::post;
        async fn admin_only() -> Response {
            if crate::routes::org_invites::is_admin("owner") {
                (StatusCode::OK, "done").into_response()
            } else {
                (StatusCode::FORBIDDEN, Json(json!({"error": "admin only"}))).into_response()
            }
        }
        async fn member_forbidden() -> Response {
            (StatusCode::FORBIDDEN, Json(json!({"error": "not yours"}))).into_response()
        }
        async fn wrap(req: axum::extract::Request, next: Next) -> Response {
            let interactive = req.headers().contains_key("x-test-session");
            run_with_authority(interactive, req, next).await
        }
        let app = Router::new()
            .route("/admin", post(admin_only))
            .route("/other", post(member_forbidden))
            .layer(middleware::from_fn(wrap));
        let send = |uri: &str, session: bool| {
            let mut b = HttpRequest::builder().method("POST").uri(uri);
            if session {
                b = b.header("x-test-session", "1");
            }
            app.clone().oneshot(b.body(Body::empty()).unwrap())
        };
        let refused = send("/admin", false).await.unwrap();
        assert_eq!(refused.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(refused.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["code"], "interactive_session_required");
        assert_eq!(send("/admin", true).await.unwrap().status(), StatusCode::OK);
        let other = send("/other", false).await.unwrap();
        let body = axum::body::to_bytes(other.into_body(), 4096).await.unwrap();
        assert_eq!(serde_json::from_slice::<serde_json::Value>(&body).unwrap()["error"], "not yours");
        assert!(authority_allowed(), "outside require_auth nothing changes");
        assert!(crate::routes::org_invites::is_admin("owner"));
    }

    // ── Tenancy access-log fields (claim-based tenancy PR 1.4) ──────────────

    /// Decision 12 makes this log line the compensating control for a failure
    /// class the plan deliberately accepts as silent, so the mapping has to be
    /// right in all three states — and `consumer` in particular must be a
    /// thing the gate DECIDED, never a stand-in for "we didn't look".
    // `PgPool::connect_lazy` builds the handle with no I/O but still needs a
    // Tokio context, so this is a `tokio::test` despite testing pure mapping.
    #[tokio::test]
    async fn tenancy_log_fields_distinguish_the_three_states() {
        use crate::tenancy::runtime::{TenancyDecision, TenantContext};

        let account = Uuid::new_v4();
        let org_account = Uuid::new_v4();

        // No decision at all (tenant runtime off, or a route outside the gate).
        let unresolved = TenancyLogFields::unresolved(account);
        assert_eq!(unresolved.plane, "unresolved");
        assert_eq!(unresolved.org_id, None);

        // Consumer plane — a real decision, and still attributable.
        let personal = TenancyLogFields::from_decision(&TenancyDecision::Personal, account);
        assert_eq!(personal.plane, "consumer");
        assert_eq!(personal.org_id, None);
        assert_eq!(
            personal.account_id,
            Some(account),
            "a consumer line must stay attributable — it is the line most likely to \
             need chasing"
        );

        // Tenant plane — carries the bound org and ITS account, not the
        // caller's fallback.
        let ctx = TenantContext {
            org_id: "org_a".into(),
            account_id: org_account,
            user_id: Uuid::new_v4(),
            // Never connected: `connect_lazy` builds the handle without I/O.
            pool: sqlx::postgres::PgPool::connect_lazy("postgres://localhost/unused").unwrap(),
            page_pool: None,
        };
        let org = TenancyLogFields::from_decision(&TenancyDecision::Org(ctx), account);
        assert_eq!(org.plane, "org");
        assert_eq!(org.org_id.as_deref(), Some("org_a"));
        assert_eq!(
            org.account_id,
            Some(org_account),
            "the tenant plane logs the ORG's account, not the caller's fallback"
        );
    }

    /// The three planes must be mutually distinguishable in a log query. If
    /// two ever render the same string, the one question this line exists to
    /// answer — "was an org caller served the shared plane?" — stops having an
    /// answer.
    #[test]
    fn every_plane_label_is_distinct() {
        let account = Uuid::new_v4();
        let labels = [
            TenancyLogFields::unresolved(account).plane,
            TenancyLogFields::from_decision(
                &crate::tenancy::runtime::TenancyDecision::Personal,
                account,
            )
            .plane,
            "org",
        ];
        let mut seen: Vec<&str> = labels.to_vec();
        seen.sort_unstable();
        seen.dedup();
        assert_eq!(seen.len(), labels.len(), "plane labels collided: {labels:?}");
    }

    /// A trivial protected handler that returns 200 when UserContext is present.
    async fn protected_handler() -> StatusCode {
        StatusCode::OK
    }

    fn protected_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/protected", get(protected_handler))
            .layer(middleware::from_fn_with_state(
                state.clone(),
                require_auth,
            ))
            .with_state(state)
    }

    // ── Missing Authorization header → 401 ────────────────────────────────────

    #[tokio::test]
    async fn missing_auth_header_returns_401() {
        let router = protected_router(test_state());
        let req = HttpRequest::builder()
            .uri("/protected")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Malformed Authorization header → 401 ─────────────────────────────────

    #[tokio::test]
    async fn malformed_auth_header_returns_401() {
        let router = protected_router(test_state());
        let req = HttpRequest::builder()
            .uri("/protected")
            .header("Authorization", "Token notbearer")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── No DB pool → 503 ─────────────────────────────────────────────────────

    #[tokio::test]
    async fn no_db_pool_returns_503() {
        // With db=None the middleware returns 503 before touching any token.
        // We still supply a well-formed Bearer so we can distinguish 503 vs 401.
        let router = protected_router(test_state());
        let req = HttpRequest::builder()
            .uri("/protected")
            .header("Authorization", "Bearer sometoken")
            .body(Body::empty())
            .unwrap();
        // db is None → 503 before the "missing token" 401.
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Bogus/expired token (db=None exercises the 503 path, not 401) ─────────
    //
    // The expired-token 401 path requires a live DB to return "no rows".
    // We document that path is covered by integration tests, and here we
    // verify the 503 path takes priority when no pool is configured.

    #[tokio::test]
    async fn bogus_token_with_no_db_returns_503() {
        let router = protected_router(test_state());
        let req = HttpRequest::builder()
            .uri("/protected")
            .header("Authorization", "Bearer bogus_expired_token")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── Response body for missing token contains expected JSON ────────────────

    #[tokio::test]
    async fn missing_token_response_body() {
        use axum::body::to_bytes;

        let router = protected_router(test_state());
        let req = HttpRequest::builder()
            .uri("/protected")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        let body_bytes = to_bytes(resp.into_body(), 1024).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body["error"], "missing token");
    }
}
