//! `mcp_auth` + `origin_guard` middleware — the auth spine for `/mcp`
//! (docs/plans/remote-mcp-v1.md §4.1). Deliberately **not** `require_auth`
//! (`middleware::auth`): remote MCP only ever accepts Clerk **OAuth** access
//! tokens (never the legacy opaque session-token fallback, never a Clerk
//! **session** JWT — both are rejected here even though `require_auth` would
//! accept one of them).
//!
//! Threat model reminder (plan §4): every `/mcp` caller is an LLM-driven agent
//! that must be treated as adversarial. Nothing here trusts client-supplied
//! data for authorization — identity is re-derived from the verified token on
//! every request.

use async_trait::async_trait;
use axum::{
    extract::{Request, State},
    http::{header, HeaderValue, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::json;

use crate::clerk::{self, ClerkClaims, ClerkError};
use crate::middleware::auth::{resolve_clerk_context, OrgMembershipPolicy, UserContext};
use crate::tenancy::runtime::{GateReject, TenancyDecision};
use crate::AppState;

use super::{McpState, ResourceIdentity, ResourceTenancy, ScopeSet};

// ── Token validator seam ──────────────────────────────────────────────────────

/// Verifies a raw bearer token into [`ClerkClaims`]. A trait (rather than
/// calling `clerk::verify` directly) so tests can inject a mock — avoids both
/// standing up a live JWKS endpoint for every test case and the env-var
/// toggling races parallel `cargo test` runs would hit if tests instead tried
/// to flip Clerk config through the environment (plan §5 Phase 1 step 4).
#[async_trait]
pub(crate) trait TokenValidator: Send + Sync {
    async fn validate(&self, token: &str) -> Result<ClerkClaims, ClerkError>;
}

/// Production validator: verifies OAuth access JWTs bound to the configured
/// protected resource. Web session verification remains a separate policy.
pub(crate) struct ClerkTokenValidator {
    pub cfg: clerk::ClerkConfig,
    pub http: reqwest::Client,
    pub resource_url: String,
}

#[async_trait]
impl TokenValidator for ClerkTokenValidator {
    async fn validate(&self, token: &str) -> Result<ClerkClaims, ClerkError> {
        clerk::verify_oauth(token, &self.cfg, &self.http, &self.resource_url).await
    }
}

// ── Auth context ──────────────────────────────────────────────────────────────

/// Identity + authorization context for a verified `/mcp` request, inserted
/// into request extensions by [`mcp_auth`]. Tool handlers (Phase 2) extract
/// this per-call — never cached on any long-lived handler struct (rmcp
/// streamable-HTTP is stateless-mode multi-connection; see plan §2.3).
///
/// Not read anywhere yet in Phase 1 beyond the stub handler dropping it — the
/// real read/write tool router (Phase 2) is what actually consumes `scopes`
/// and the identity fields; `allow(dead_code)` mirrors the same rationale
/// already on `middleware::auth::UserContext`.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) struct McpAuthContext {
    pub user: UserContext,
    pub scopes: ScopeSet,
    /// Whether this call's token named the caller's own **personal
    /// organization** (control migration `0208_personal_orgs.sql`).
    ///
    /// Recorded rather than re-derived because it cannot be recovered
    /// downstream: a personal org resolves to
    /// `TenancyDecision::Personal` — it lives on the shared plane by design —
    /// so from the decision alone it is indistinguishable from a caller who
    /// has chosen nothing at all. The tool-catalogue gate needs exactly that
    /// distinction: one caller is already bound, the other still has to pick.
    ///
    /// Set from `NormalizedClaims::is_personal_org`, i.e. from a claim already
    /// proved against `personal_orgs` AND the caller's own `sub`. It is never
    /// taken from anything the caller can set.
    pub personal_org: bool,
    /// The REAL organization this connection's token names, if any — the
    /// choice the user made on Clerk's OAuth consent screen when they
    /// connected (#825). `None` for a token with no `org_id` claim, and for
    /// one naming the caller's own personal organization (that is
    /// [`Self::personal_org`]'s job, and it denotes the consumer plane).
    ///
    /// Recorded rather than re-derived for the same reason
    /// [`Self::personal_org`] is: it cannot be recovered from
    /// [`Self::decision`] downstream. `Some(Org(..))` is reached from a
    /// stored `select_tenant` selection AND from this claim, and the two need
    /// telling apart by exactly one caller — `tools::tenancy::select_tenant`,
    /// whose "clear the selection and return to your personal ocean" branch
    /// cannot do what it says for a connection whose TOKEN names an
    /// organization, and must say so instead of silently no-op'ing.
    ///
    /// Set from `tenant_selection::ConsentOrg::from_claims`, i.e. from a
    /// verified claim already classified against `personal_orgs`.
    pub consent_org: Option<String>,
    /// The OAuth client id that requested the token (`client_id` or `azp`),
    /// when the token carries one. Feeds `device_id` and the audit trail.
    pub client_id: Option<String>,
    /// The token's `jti`, if present — audit correlation only, never used for
    /// replay prevention (verification stays fully stateless).
    pub token_jti: Option<String>,
    /// Best-effort identity of the calling MCP harness/client (Claude Code,
    /// Cursor, Claude Desktop, etc.), sourced from this request's `User-Agent`
    /// header and capped at [`MCP_CLIENT_NAME_CAP`] chars. Fed to
    /// `add_context`'s `mcp_client_name` atom-provenance stamp (migration
    /// 0072) and, in `mcp::tools::mod`'s structuredContent-only negotiation,
    /// to `tool_success`'s client-hint match.
    ///
    /// **Deliberately still sourced from `User-Agent`, not the MCP protocol's
    /// own `clientInfo`**, even though a real per-request `clientInfo` IS
    /// available now: SEP-2575 (MCP `2026-07-28`, the version this backend
    /// runs — rmcp 3.1.2) moved client context onto every request's `_meta`,
    /// not just the `initialize` handshake, and a tool handler CAN read it
    /// with no cross-request session correlation at all, even though `/mcp`
    /// runs rmcp's streamable-HTTP transport in **stateless mode**
    /// (`NeverSessionManager` — see `mcp/mod.rs`'s `mcp_router` doc comment
    /// on multi-replica Fly safety) — the earlier claim here that
    /// `clientInfo` was "unusable" predates SEP-2575 and no longer holds
    /// (see `services::platform::detect_platform`, which DOES read it,
    /// ahead of this very field, for `platform_detected`).
    ///
    /// One important trap along the way, verified against a REAL end-to-end
    /// run rather than assumed from the API surface: read `_meta` directly
    /// (`RequestContext::meta` — `RequestMetaObject::client_info()`), NOT
    /// via `RequestContext::client_info()`'s own convenience method. That
    /// method has a legacy fallback (`peer.peer_info().client_info`) for
    /// pre-2026-07-28 sessions, and in stateless mode that fallback is a
    /// SYNTHETIC, non-`None` `Implementation::default()` — rmcp's OWN crate
    /// name/version (`{"rmcp", "3.1.2"}`, `Implementation::from_build_env()`)
    /// — reconstructed by `peer_info_for_stateless_request`
    /// (`transport/streamable_http_server/tower.rs`) for EVERY stateless
    /// request that isn't a real `initialize`. Calling the convenience
    /// method would have silently detected every caller that sends no
    /// `_meta.clientInfo` (every client in production as of this writing) as
    /// platform `"rmcp"` — caught by `mcp_pg_e2e.rs`'s pre-existing
    /// `agent_sessions_carry_identity_and_filter_by_agent_platform_and_task_on_real_postgres`
    /// test regressing once this field started being read for real. `_meta`
    /// alone has no such fallback: it decodes
    /// `_meta.io.modelcontextprotocol/clientInfo` and nothing else, so it is
    /// `None` exactly when this request's own body carried no such key.
    ///
    /// These two uses stay on `User-Agent` anyway, by choice, not necessity:
    /// the structuredContent negotiation because reclassifying a client by a
    /// different signal changes the wire shape of every tool response for
    /// that client, a much bigger blast radius than a documentation fix
    /// should carry; the provenance stamp because every `mcp_client_name`
    /// value already written to `atoms` is a UA string, and switching the
    /// source out from under one column with no way to tell old rows from
    /// new would make the column mean two different things depending on when
    /// a row was written. See migration 0072's doc comment for the same
    /// rationale from the schema side, and `mcp::tools::mod`'s
    /// structuredContent-only negotiation doc for the equivalent update on
    /// that consumer.
    pub mcp_client_name: Option<String>,
    /// The request's resolved tenancy binding (org-tenancy PR 2.3), stamped
    /// by [`mcp_auth`] via [`resolve_mcp_tenancy`] AFTER the live membership
    /// re-check has passed:
    ///
    /// * `None` — the tenancy runtime is absent or disabled
    ///   (`MULTITENANT_ENABLED` unset, the default). Every downstream
    ///   consumer treats this exactly like `Personal` (the
    ///   `ReachabilityResolver`/`resolve_dek_cached_dispatch` convention),
    ///   so flag-off behavior is byte-identical to pre-2.3.
    /// * `Some(Personal)` — runtime enabled, personal (no-org) token:
    ///   consumer store/pool, same handles as today.
    /// * `Some(Org(ctx))` — runtime enabled, org-scoped token: `ctx.pool` is
    ///   the org's fingerprint-verified TENANT pool, and every tool's
    ///   CONTENT operation (and its `mcp_tool_calls` audit row) must run
    ///   against it, never the ambient consumer plane.
    ///
    /// `/mcp` stays on the apex with CLAIM-ONLY org selection (PR 2.2): the
    /// subdomain binding gate never runs here, so this is resolved from the
    /// verified token's `org_id` claim + the control-plane routing row — not
    /// from the Host header.
    pub decision: Option<TenancyDecision>,
}

/// Cap on the `User-Agent`-derived [`McpAuthContext::mcp_client_name`], in
/// chars. Generous enough for any real client's UA string while bounding an
/// adversarial caller who sends an oversized header (this value flows
/// straight into a `TEXT` column and MCP tool-call audit-adjacent paths).
pub(crate) const MCP_CLIENT_NAME_CAP: usize = 256;

/// Extract a capped, best-effort client identity from the `User-Agent`
/// header. `None` when the header is absent or not valid UTF-8 (never a hard
/// failure — this is enrichment metadata, not an auth decision).
fn extract_client_name(req: &Request) -> Option<String> {
    let raw = req.headers().get(header::USER_AGENT)?.to_str().ok()?;
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.chars().take(MCP_CLIENT_NAME_CAP).collect())
}

// ── Bearer extraction ─────────────────────────────────────────────────────────

/// Extract the bearer token from the `Authorization` header only. Tokens
/// passed via query string are never read here at all (rejection-by-omission
/// rather than an explicit query-string check) — a caller who tries one just
/// looks unauthenticated and gets the standard 401 challenge.
fn extract_bearer(req: &Request) -> Option<String> {
    req.headers()
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::to_owned)
}

/// OAuth-only claims gate (plan §1 "Clerk session JWTs at /mcp" resolution).
///
/// `sid` (the Clerk session id) is checked FIRST and is an unconditional
/// rejection: Clerk **session** JWTs always carry `sid`, and OAuth access
/// tokens never do, so its presence alone conclusively identifies a session
/// token regardless of anything else on the claim set.
///
/// This ordering matters: **`azp` alone is NOT a safe distinguisher.** `azp`
/// is the standard OIDC "authorized party" claim, and Clerk's own browser
/// session tokens carry it by default (it is not OAuth-access-token-specific
/// the way `scope`/`client_id`/`jti` are). An earlier version of this gate
/// treated any of `scope`/`client_id`/`azp`/`jti` as sufficient on their own,
/// which meant an ordinary web-app session token — carrying both `sid` AND
/// `azp` — passed the gate with no OAuth consent flow at all (security review
/// finding H1). Checking `sid` first closes that hole: a session token is
/// rejected before its `azp` claim is ever consulted.
fn has_oauth_claims(claims: &ClerkClaims) -> bool {
    if claims.sid.is_some() {
        return false;
    }
    claims.scope.is_some() || claims.client_id.is_some() || claims.azp.is_some() || claims.jti.is_some()
}

// ── 401 challenge ─────────────────────────────────────────────────────────────

/// Build the `WWW-Authenticate` challenge required by RFC 9728 / the MCP auth
/// spec: `resource_metadata` points the client at the PRM document so OAuth
/// discovery can proceed; `scope` advertises the scopes the client should
/// request from Clerk. This MUST be a Clerk-grantable scope set
/// ([`super::MCP_SCOPES_PERSONAL`] — the only one there is, since claim-based
/// tenancy §Phase 3 PR 3.3) — the product's `coralswarm:*` scopes are
/// derived server-side (`effective_scopes`, dispatch-time gate), never issued by
/// Clerk, and advertising them here aborts the client's registration/authorize
/// flow. A dedicated helper (not `middleware::auth::unauthorized_with_www_auth`,
/// which is a different, simpler shape used by the legacy session path) per plan
/// §4.1 step 1.
/// `res` is the request's resolved [`ResourceIdentity`] ([`host_guard`]), so the
/// challenge advertises the metadata document + scopes of the resource the
/// client actually addressed — which is always the personal one now (there is
/// only one resource; see [`super::McpConfig::resolve_resource`]).
fn challenge_401(res: &ResourceIdentity, msg: &'static str) -> Response {
    let mut resp = (StatusCode::UNAUTHORIZED, Json(json!({"error": msg}))).into_response();
    let value = format!(
        "Bearer resource_metadata=\"{}/.well-known/oauth-protected-resource/mcp\", scope=\"{}\"",
        res.metadata_base, res.scopes,
    );
    if let Ok(header_value) = HeaderValue::from_str(&value) {
        resp.headers_mut()
            .insert(header::WWW_AUTHENTICATE, header_value);
    }
    resp
}

fn service_unavailable() -> Response {
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error": "service unavailable"})),
    )
        .into_response()
}

fn forbidden(msg: &'static str) -> Response {
    (StatusCode::FORBIDDEN, Json(json!({"error": msg}))).into_response()
}

// ── Host guard + tenant binding (URL-selected tenancy) ────────────────────────

/// Resolve the request's [`ResourceIdentity`] from its `Host` header and reject
/// any host we do not serve — the FIRST half of the tenancy chooser, and the
/// replacement for rmcp's own DNS-rebinding Host allowlist (see
/// `super::mcp_router_with_validator` for why we own that check now).
///
/// Runs before [`mcp_auth`], so a bad Host costs no JWT verification and no DB
/// round-trip, and the identity it stamps into extensions drives both the 401
/// challenge's `scope`/`resource_metadata` and the tenancy decision.
///
/// A rejected Host gets `403` with rmcp's own wording — this check subsumes
/// rmcp's, so it keeps rmcp's status and message rather than inventing new ones.
/// Note it is NOT the tenant-existence check: a syntactically valid
/// `<slug>.<apex>` for an org that does not exist passes here and is rejected by
/// the gate in [`mcp_auth`], after the caller has proven who they are.
pub(crate) async fn host_guard(
    State(mcp): State<McpState>,
    mut req: Request,
    next: Next,
) -> Response {
    let host = request_authority(&req);
    let apex = tenant_apex(&mcp.app);

    let Some(identity) = mcp.cfg.resolve_resource(host.as_deref(), apex.as_deref()) else {
        tracing::warn!(
            target: "mcp.auth",
            reason = "disallowed_host",
            "rejecting /mcp request: Host is neither our apex, a tenant subdomain, nor allowlisted"
        );
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "Forbidden: Host header is not allowed"})),
        )
            .into_response();
    };

    req.extensions_mut().insert(identity);
    next.run(req).await
}

/// The authority the client actually addressed: the `Host` header, falling back
/// to the HTTP/2 `:authority` pseudo-header (which is where the authority lives
/// over h2, and which `axum::Router::nest` can strip from the synthesized `Host`).
///
/// Shared with `well_known::protected_resource_metadata` so the PRM document and
/// the `/mcp` guard cannot disagree about which resource a request addressed —
/// they were byte-identical copies, and a copy that drifts means a client
/// discovers one resource and is then authorized against another.
pub(crate) fn request_authority(req: &Request) -> Option<String> {
    req.headers()
        .get(header::HOST)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned)
        .or_else(|| req.uri().authority().map(|a| a.as_str().to_owned()))
}

/// The routing apex, or `None` when the tenancy runtime is absent/disabled.
/// Shared with the PRM handler for the same reason as [`request_authority`].
pub(crate) fn tenant_apex(app: &AppState) -> Option<String> {
    app.tenant_runtime
        .as_ref()
        .filter(|rt| rt.enabled)
        .map(|rt| rt.apex.clone())
}

/// The request's resolved resource identity, or the configured personal one as a
/// fallback. [`host_guard`] always inserts it on the `/mcp` route; the fallback
/// only matters if this module is ever reused without that layer. It mirrors what
/// `host_guard` itself would produce for an unresolvable authority (the personal
/// apex, with org-claim stripping active exactly when an apex is configured)
/// rather than hardcoding a guess — and it is built infallibly, so no request path
/// can panic on an invariant owned by another function.
fn resource_of(req: &Request, mcp: &McpState) -> ResourceIdentity {
    req.extensions()
        .get::<ResourceIdentity>()
        .cloned()
        .unwrap_or_else(|| {
            mcp.cfg
                .personal_identity()
        })
}

/// Map a [`GateReject`] to the `/mcp` response shape (the gate's own statuses;
/// `middleware::auth` maps the identical variants for REST).
fn reject_to_response(r: GateReject) -> Response {
    match r {
        GateReject::NotFound => {
            (StatusCode::NOT_FOUND, Json(json!({"error": "not found"}))).into_response()
        }
        GateReject::Provisioning => crate::middleware::auth::provisioning_unavailable(),
        GateReject::Unavailable => service_unavailable(),
        GateReject::Forbidden(msg) => forbidden(msg),
    }
}

// ── The org-claim strip is GONE (#825) ────────────────────────────────────────
//
// `claims_for_resource` used to sit here: on the PERSONAL resource it dropped
// an `org_id` claim before anything else ran, logging
// `org_claim_stripped_on_personal_resource` with the reason "this resource
// does not advertise user:org:read".
//
// That reason stopped being true when #825 (`f015e3d7`) put `user:org:read`
// back into `super::MCP_SCOPES_PERSONAL`. Since then Clerk DOES render the
// organization picker on the consent screen and DOES mint an `org_id` claim
// for whatever the user picked — so the strip was deleting a deliberate,
// signed choice and then asking the user to make the same choice a second
// time in-band (a `tools/list` narrowed to `list_tenants`/`select_tenant` by
// `server::CoralSwarmMcp::tool_catalog_restricted_to_tenancy_tools`, because
// the discarded claim left the decision at `TenancyDecision::Personal`).
// Production logged that strip 13 times in the last 100 log lines before this
// PR, all for one real organization.
//
// The case the strip WAS written for — a client registered before the
// resource split, still holding a pre-cutover grant it never asked for —
// cannot exist anymore either: #825 changed the SEP-2352 byte-frozen PRM
// `scopes_supported`, which invalidated every pre-existing grant and forced
// every client to reconnect through the current consent screen.
//
// Nothing replaced it, deliberately: an org claim on `/mcp` is now honored on
// exactly the terms a stored `select_tenant` selection is —
// `resolve_personal_tenancy` below, through
// `tenant_selection::resolve_request_tenancy`.

/// Resolve the request's [`TenancyDecision`] from the resource the client
/// addressed (`res`) plus the VERIFIED token's `org_id` claim.
///
/// **`/mcp` is never host-bound.** Phases 1–2 made it host-bound like REST
/// (one resource per tenancy, chosen by the URL) specifically because
/// `user:org:read`'s consent screen had no personal row. PR 3.3 dropped that
/// scope, which made the host distinction pointless —
/// [`super::McpConfig::resolve_resource`] resolves EVERY host to
/// [`ResourceTenancy::Personal`], so this match's `Org` arm below is
/// unreachable through any real request (nothing upstream constructs it
/// anymore); it survives only as the logic a hand-built [`ResourceIdentity`]
/// exercises directly in this module's own unit tests. The real, reachable
/// path is entirely the `Personal` arm.
///
/// **#825 gave the consent screen its picker back** (personal organizations
/// made "my own ocean" a selectable row — see `super::MCP_SCOPES_PERSONAL`),
/// so a `/mcp` token can once again carry an `org_id` claim. That claim is
/// now HONORED rather than stripped, on the same terms a stored selection
/// gets, and the host still has nothing to do with it:
///
/// * [`ResourceTenancy::Personal`] → resolved by [`resolve_personal_tenancy`]
///   below, from a stored [`super::tenant_selection`] selection AND the
///   token's own consent-screen claim, with the stored selection winning
///   whenever both are present. See that function's own doc.
/// * [`ResourceTenancy::Org`] → **vestigial, unreachable via `resolve_resource`
///   since PR 3.3** (see above). Still the FULL host gate on paper, reusing
///   the exact code path REST uses (`TenantRuntime::precheck_host` +
///   `finish_binding`, i.e. `gate::decide`'s `Subdomain` arm) — left exactly
///   as PR 2.3 wrote it, since removing it buys nothing once nothing can
///   construct the input it matches on.
///
/// Reusing `finish_binding` (in the vestigial arm) also closed a known gap
/// for free when this path was still live: it is what provisions a
/// first-sight MEMBER ocean (member-private-oceans PR 4, trigger 2), which an
/// MCP-only member previously never hit because the old hand-rolled path
/// built its own `TenantContext`.
///
/// Ordering invariant (unchanged from PR 2.3): called strictly AFTER
/// `resolve_clerk_context(.., OrgMembershipPolicy::RequireExisting)`, so a
/// removed member is 403'd by the live membership re-check before any routing
/// lookup or lazy tenant-pool build happens.
///
/// `Ok(None)` when the tenancy runtime is absent/disabled (`MULTITENANT_ENABLED`
/// unset — the default): byte-identical flag-off behavior, every downstream
/// consumer treats it as `Personal`.
async fn resolve_mcp_tenancy(
    app: &AppState,
    control_pool: &sqlx::PgPool,
    res: &ResourceIdentity,
    claims: &crate::middleware::auth::NormalizedClaims,
    user: &UserContext,
    client_id: Option<&str>,
) -> Result<Option<TenancyDecision>, Response> {
    let Some(rt) = app.tenant_runtime.as_ref().filter(|rt| rt.enabled) else {
        return Ok(None);
    };

    let slug = match &res.tenancy {
        ResourceTenancy::Personal => {
            return resolve_personal_tenancy(rt, control_pool, claims, user, client_id).await;
        }
        ResourceTenancy::Org(slug) => slug,
    };

    // Re-classifies the Host (pure, no I/O) and resolves the routing row through
    // the runtime's TTL cache — the same lookup REST's gate makes, so an org
    // request costs no more here than it does there.
    let host = format!("{slug}.{}", rt.apex);
    let pre = rt
        .precheck_host(Some(control_pool), &host)
        .await
        .map_err(reject_to_response)?;
    rt.finish_binding(
        control_pool,
        pre,
        // Vestigial org-subdomain arm (unreachable since PR 3.3), but no
        // longer reconstructing a `TokenKind` from a raw claim.
        //
        // It used to read `claims.org_id` directly and map `Some` to
        // `TokenKind::Org`, with a comment asserting a personal org could not
        // reach it. That safety was the comment, not the type — the identical
        // construction at the stored-selection call site below carried the
        // same assertion and was WRONG, which is the lockout this PR fixes.
        // `NormalizedClaims::token_kind()` is the only function that consults
        // `personal_orgs`, so routing through it makes the claim here
        // unable to name a personal org as an organization by construction,
        // whether or not this arm ever becomes reachable again.
        claims.token_kind(),
        user.account_id,
        user.user_id,
        claims.claims().name.as_deref(),
    )
    .await
    .map(Some)
    .map_err(reject_to_response)
}

/// Resolve tenancy for the PERSONAL/apex resource — the only reachable
/// resource — from TWO inputs, in this precedence:
///
/// 1. an explicit STORED selection (`super::tenant_selection`, written by the
///    `select_tenant` MCP tool), which always wins when a row exists;
/// 2. otherwise, the verified token's own consent-screen `org_id` claim
///    (#825), when it names a REAL organization.
///
/// Both are resolved in one place —
/// [`super::tenant_selection::resolve_request_tenancy`] — so the guard that
/// module exists to provide (an org id becomes a `TokenKind::Org` only after
/// a live-membership re-check on THIS call) covers both sources identically.
/// Neither can reach `finish_binding` except as
/// [`super::tenant_selection::SelectionOutcome::Org`].
///
/// Before #825 only (1) existed here: the claim was stripped at the door, so
/// there was nothing else to consult. Restoring `user:org:read` made the
/// consent screen a real chooser again, and (2) is that chooser's answer
/// arriving at the decision instead of being discarded.
///
/// `client_id` is `None` only for a token with neither a `client_id` nor an
/// `azp` claim (`mcp_auth`'s own `client_id` variable) — there is then no
/// stable per-client identifier to key a selection row by, so there can be no
/// stored selection for that caller and only (2) can apply.
async fn resolve_personal_tenancy(
    rt: &crate::tenancy::runtime::TenantRuntime,
    control_pool: &sqlx::PgPool,
    claims: &crate::middleware::auth::NormalizedClaims,
    user: &UserContext,
    client_id: Option<&str>,
) -> Result<Option<TenancyDecision>, Response> {
    use super::tenant_selection::{resolve_request_tenancy, SelectionOutcome};

    let display_name = claims.claims().name.as_deref();

    let outcome =
        match resolve_request_tenancy(control_pool, user.user_id, client_id, claims).await {
            Ok(o) => o,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    user_id = %user.user_id,
                    "tenant-selection lookup failed"
                );
                return Err(service_unavailable());
            }
        };

    match outcome {
        SelectionOutcome::Personal => Ok(Some(TenancyDecision::Personal)),
        SelectionOutcome::Forbidden(msg) => Err(forbidden(msg)),
        SelectionOutcome::Org(org_id) => {
            // The SAME apex gate path REST's org traffic uses (claim-based-
            // tenancy PR 1.2's `finish_binding` Apex+Org arm): `decide`'s own
            // claim-vs-routing-row check runs regardless of where `org_claim`
            // came from, so a stale/suspended/offboarded routing row is
            // refused identically whether this org id arrived as a stored
            // selection or as a consent-screen claim.
            //
            // That is the SECOND of the two checks every org id on this path
            // gets. The first is `resolve_request_tenancy`'s live-membership
            // re-check, which is necessary but not sufficient on its own —
            // see `tenant_selection`'s module doc, "Why this needs its own
            // guard", and, for the consent-claim source specifically, "The
            // consent screen is a second way to name a tenant".
            //
            // `rt.apex` (not the request's real Host) is used deliberately:
            // `McpConfig::resolve_resource` already treats the configured
            // apex, a reserved apex label (`app.`/`www.`), and an explicit
            // allowlisted foreign host (the raw Fly hostname, `localhost`)
            // as IDENTICAL personal resources with no behavioral difference
            // between them — so this classifies to `HostTarget::Apex` the
            // same way regardless of which of those three the caller
            // actually used, rather than depending on it.
            let pre = rt
                .precheck_host(Some(control_pool), &rt.apex)
                .await
                .map_err(reject_to_response)?;

            // `finish_binding`'s `account_id` parameter MUST be the BOUND
            // org's own account, not `user.account_id`.
            //
            // For a STORED selection, `user.account_id` is still the caller's
            // PERSONAL account: `resolve_clerk_context` resolved it from
            // claims that carried no `org_id` (or one naming a personal org,
            // which resolves to the caller's own account either way), and a
            // stored selection has no claim to derive an org account from.
            // Every OTHER `finish_binding` caller gets this alignment for
            // free — their `account_id` was itself derived moments earlier
            // from the SAME org claim now being passed as `org_claim` (REST's
            // `ProvisionFromClaims` path, `resolve_mcp_tenancy`'s own
            // org-subdomain arm above, and, since #825, a consent-sourced
            // claim here, whose `user.account_id` came from
            // `RequireExisting`'s `live_org_membership` on this very org).
            //
            // So this line is load-bearing for the selection source and
            // agrees with `user.account_id` for the claim source — the
            // routing row is used unconditionally rather than branching on
            // which source named the org, because it is the more direct
            // record of "this org's account" in both cases and needs no
            // assumption about how the caller's own was resolved (the same
            // reason `finish_binding` builds the POOL from `row.account_id`).
            // Get it wrong and `finish_binding`'s member-ocean provisioning
            // (`maybe_provision_member_ocean`) and the resulting
            // `TenantContext.account_id` both end up scoped to the caller's
            // PERSONAL account while `TenantContext.pool` points at the
            // ORG's tenant database: every `ctx.account_id`-scoped tenant
            // query (`tenant_list_reachable_graphs`, etc.) then looks for
            // content under an account id that has no rows in that
            // database, and a first-sight member ocean gets INSERTed under
            // the wrong `account_id` inside the org's own tenant DB.
            //
            // `rt.resolve_routing_by_org` is the SAME TTL-cached lookup
            // `finish_binding` itself makes internally for this exact
            // (`HostTarget::Apex`, `TokenKind::Org`) arm (see that
            // function's doc), so calling it here first costs one query
            // (or, within the TTL window, a cache hit) and `finish_binding`'s
            // own call lands on the now-warm cache entry — not a doubled
            // round trip in steady state.
            // In-app org deletion (control 0387): an org whose deletion is
            // live serves no MCP traffic. A claim naming it was already
            // refused at the door (`normalize_personal_org_claim`); a STORED
            // selection reaches only here, so it is checked here too.
            match crate::services::org_deletion::state_for_org(control_pool, &org_id).await {
                Ok(Some(st)) => {
                    if let Some(refusal) = crate::middleware::auth::org_deletion_refusal(&st) {
                        return Err(refusal);
                    }
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!(error = %e, org = %org_id, "org deletion state lookup failed");
                    return Err(service_unavailable());
                }
            }
            let org_account_id = match rt.resolve_routing_by_org(control_pool, &org_id).await {
                Ok(Some(row)) => row.account_id,
                // No routing row: `finish_binding`'s OWN lookup (moments
                // later) finds the same absence and refuses via
                // `gate::decide` (404/503) before this value is ever read —
                // `TenantContext.account_id` is only constructed on a
                // `GateOutcome::Org`, which cannot happen without a routing
                // row. Any placeholder is safe here; `user.account_id` is
                // used rather than a dummy `Uuid` so a future refactor that
                // DID start reading this value on a non-`Org` outcome would
                // fail toward the caller's own real account, not garbage.
                Ok(None) => user.account_id,
                Err(e) => {
                    tracing::error!(error = %e, org = %org_id, "tenant routing-by-org lookup failed");
                    return Err(service_unavailable());
                }
            };

            rt.finish_binding(
                control_pool,
                pre,
                // `org_id` here is a REAL organization, and that is now a
                // property of the type rather than of this comment — from
                // EITHER source, checked in `personal_orgs` on both sides:
                //
                //   * a STORED selection reaches `SelectionOutcome::Org` only
                //     via `StoredSelection::Org`, and `load_stored_selection`
                //     — the only producer of a `StoredSelection` — asks
                //     `personal_orgs` on the same statement that reads the
                //     row, so a selection naming THIS caller's own personal
                //     org is classified `StoredSelection::OwnPersonalOrg` and
                //     resolves to `Personal` before it can reach here;
                //   * a CONSENT-screen claim (#825) reaches it only via
                //     `ConsentOrg::Org`, which is derived from
                //     `NormalizedClaims::token_kind()` — the one function
                //     that consults `personal_orgs` — so a personal-org claim
                //     is `TokenKind::PersonalOrg`, becomes `ConsentOrg::None`,
                //     and likewise resolves to `Personal`.
                //
                // The previous version of this comment asserted the same
                // conclusion from the wrong premise — that a personal org is
                // "classified at the door" — which is true only of a CLAIM. A
                // stored selection has no claim to classify (that is the
                // whole reason this path exists), so nothing at the door ever
                // saw this org id. A personal org reached this line, was
                // handed to `finish_binding` as `TokenKind::Org`, found
                // `RoutingState::Missing`, and 403'd from inside `mcp_auth`
                // before tool dispatch — permanently, since `select_tenant`
                // sits behind that same dispatch.
                crate::tenancy::gate::TokenKind::Org(org_id.clone()),
                org_account_id,
                user.user_id,
                display_name,
            )
            .await
            .map(Some)
            .map_err(reject_to_response)
        }
    }
}

// ── mcp_auth middleware ───────────────────────────────────────────────────────

/// Steps per plan §4.1, extended by org-tenancy PR 2.2:
/// 1. Extract Bearer (header only) — missing/malformed => 401 + challenge.
/// 2. Verify via the injected [`TokenValidator`] (JWKS-cached in production).
/// 3. Reject anything that isn't OAuth-shaped (`has_oauth_claims`) — rejects
///    Clerk session JWTs.
/// 4. Resolve to the same [`UserContext`] shape `require_auth` builds, via
///    `resolve_clerk_context`, with an MCP-specific device id — and, when the
///    token carries an `org_id` claim, under
///    [`OrgMembershipPolicy::RequireExisting`]: a LIVE `account_members`
///    re-check on every request, so a member removed from the org (offboarding)
///    is 403'd on their very next call even while their OAuth token is still
///    valid. Personal (no-org) tokens skip this entirely — zero added queries.
///    Since #825 a token presented to the PERSONAL resource can carry a real
///    `org_id` claim again (the consent screen's picker is back), and this
///    step is what fences it: a claim naming an org the caller is not a live,
///    non-revoked member of is 403'd HERE, before step 5 resolves any tenancy
///    or builds any pool. `user.account_id` is then the ORG's account, which
///    is what keeps it coherent with the binding step 5 goes on to make.
///    Per-request cost of the re-check: ONE indexed control-DB lookup
///    (`accounts.clerk_org_id` unique index + `account_members` PK), and it
///    REPLACES the `ensure_org_membership` upsert the session path performs,
///    so org-token requests do not gain a query — they trade a write for a
///    read. Deliberately uncached (plan panel finding #1 defers a bounded-TTL
///    membership cache): revocation-on-next-call is the whole point, and a
///    TTL would reintroduce a staleness window.
/// 5. Resolve the tenant binding from the RESOURCE the client addressed plus
///    the verified claim ([`resolve_mcp_tenancy`]): an org resource goes through
///    the same host gate REST uses and gets its fingerprint-verified TENANT pool
///    (built lazily, only after the step-4 membership re-check passed), the
///    personal resource the consumer plane, and flag-off deployments `None`
///    (byte-identical behavior).
/// 6. Compute effective scopes (env write ceiling AND the token's own
///    `coralswarm:*` scope when it carries one — see `effective_scopes`; no
///    database lookup).
/// 7. Insert [`McpAuthContext`] into request extensions.
pub(crate) async fn mcp_auth(State(mcp): State<McpState>, mut req: Request, next: Next) -> Response {
    // Which resource the client addressed (`host_guard`) — decides the tenancy
    // plane below, and the `scope` every 401 challenge advertises.
    let res = resource_of(&req, &mcp);

    // The service is stateless and never offers the optional server-to-client
    // SSE stream, so rmcp answers an authenticated GET with 405 anyway. Answer
    // it before the token check too: some clients open that GET without a
    // bearer and treat a 401 there as a failed sign-in, even though their
    // authenticated POSTs succeed.
    if req.method() == axum::http::Method::GET {
        let mut resp = StatusCode::METHOD_NOT_ALLOWED.into_response();
        resp.headers_mut()
            .insert(header::ALLOW, HeaderValue::from_static("POST"));
        return resp;
    }

    let Some(raw_token) = extract_bearer(&req) else {
        tracing::warn!(target: "mcp.auth", reason = "missing_token", "rejecting /mcp request with no bearer token");
        return challenge_401(&res, "missing bearer token");
    };

    // No fallback to the legacy opaque session-token path on `/mcp` — a token
    // that doesn't even look like a JWT is rejected outright rather than
    // silently trying a different auth scheme.
    if !clerk::looks_like_jwt(&raw_token) {
        tracing::warn!(target: "mcp.auth", reason = "not_a_jwt", "rejecting /mcp request: bearer is not a JWT");
        return challenge_401(&res, "invalid token");
    }

    let claims = match mcp.validator.validate(&raw_token).await {
        Ok(c) => c,
        Err(e) => {
            let reason = classify_verify_error(&e);
            tracing::warn!(target: "mcp.auth", reason, error = %e, "token verification failed");
            return challenge_401(&res, "invalid or expired token");
        }
    };

    if !has_oauth_claims(&claims) {
        tracing::warn!(target: "mcp.auth", reason = "not_oauth_token", "rejecting /mcp request: token has no OAuth-only claims (looks like a Clerk session JWT)");
        return challenge_401(
            &res,
            "session tokens are not accepted at /mcp; use an OAuth access token",
        );
    }

    let Some(pool) = mcp.app.db.clone() else {
        return service_unavailable();
    };

    // Personal-organization translation (control migration 0208): classifies
    // the token's `org_id` claim against `personal_orgs` and refuses another
    // user's personal org outright. Runs BEFORE `resolve_clerk_context` so
    // the classification is total — the active account, the membership
    // policy, write governance and the tenancy decision all see the same
    // normalized claims, and every later consumer reads the classification
    // through `NormalizedClaims::token_kind()` rather than re-deriving it
    // from a bare `org_id`.
    //
    // This used to be followed immediately by `claims_for_resource`, which
    // STRIPPED the org claim on the personal resource. That is gone — see the
    // block comment above `resolve_mcp_tenancy` — so the claim survives to
    // step 4's membership fence and step 5's tenancy decision.
    let claims =
        match crate::middleware::auth::normalize_personal_org_claim(&pool, claims).await {
            Ok(c) => c,
            Err(resp) => return resp,
        };

    let client_id = claims.claims().client_id.clone().or_else(|| claims.claims().azp.clone());
    let device_id = client_id
        .as_deref()
        .map(|c| format!("mcp:{c}"))
        .unwrap_or_else(|| "mcp".to_string());

    let mut user = match resolve_clerk_context(
        &mcp.app,
        &pool,
        &claims,
        Some(device_id),
        // Live per-request membership re-check for org-context tokens; see the
        // step-4 doc above and `OrgMembershipPolicy::RequireExisting`.
        OrgMembershipPolicy::RequireExisting,
    )
    .await
    {
        Ok(u) => u,
        Err(resp) => return resp,
    };

    // Tenant binding (org-tenancy PR 2.3, extended by claim-based tenancy
    // §Phase 3 PR 3.2): resolved from the verified claim (org resource) or a
    // stored selection (personal resource), strictly AFTER the
    // RequireExisting membership re-check above passed — a revoked member is
    // already 403'd before any tenant pool is touched or lazily built.
    // `client_id` (computed above, for `device_id`) is reused here to key the
    // stored-selection lookup on the personal resource — see
    // `resolve_mcp_tenancy`/`resolve_personal_tenancy`.
    let decision = match resolve_mcp_tenancy(
        &mcp.app,
        &pool,
        &res,
        &claims,
        &user,
        client_id.as_deref(),
    )
    .await
    {
        Ok(d) => d,
        Err(resp) => return resp,
    };

    // Claim-based tenancy §Phase 3 PR 3.2: keep `user.account_id` coherent
    // with the just-resolved `decision`.
    //
    // For EVERY call site that predates this PR, this is a no-op: `user`
    // above was built by `resolve_clerk_context` from claims that ALREADY
    // carried the org_id (`ProvisionFromClaims`/`RequireExisting` both
    // resolve `account_id` from the token's own `org_id`), and
    // `resolve_mcp_tenancy`'s org-subdomain arm binds a `TenantContext` for
    // that SAME org — so `ctx.account_id` and `user.account_id` already
    // agree. A STORED selection breaks that for the first time: `user` was
    // resolved from a claim carrying NO `org_id` at all (a personal token),
    // so `user.account_id` is the caller's PERSONAL account, while
    // `resolve_personal_tenancy` may have just bound `decision` to a
    // DIFFERENT org's `TenantContext`. Left unsynced, everything downstream
    // that reads `McpAuthContext.user.account_id` directly instead of
    // `decision` — rate-limit bucket keys, the `mcp_tool_calls` audit row's
    // `account_id` column (`server::CoralSwarmMcp::audit`), EVERY
    // `begin_scoped_tx!`/`begin_member_scoped` RLS-scope call in
    // and `tools/read.rs`/`tools/write.rs` — would silently scope to the
    // caller's PERSONAL account while `decision.pool` serves the ORG's
    // tenant database: RLS-scoped queries find no matching rows, a
    // first-sight member ocean gets inserted under the wrong `account_id`
    // inside the org's own tenant DB. Caught by
    // `tests/mcp_tenant_selection_pg.rs`'s tenant-pool-audit assertion.
    match &decision {
        Some(TenancyDecision::Org(ctx)) => {
            user.account_id = ctx.account_id;
        }
        Some(TenancyDecision::Personal) => {
            // Security review finding on MCP listing-prep (#900, item 2): the
            // Org arm above was the ONLY one that re-synced `user.account_id`
            // — but a STORED selection can resolve to `Personal` even when
            // the TOKEN's own claim named a real org (`ConsentOrg::Org`,
            // e.g. `select_tenant` called with no `org_id` after signing in
            // against that org, or `StoredSelection::OwnPersonalOrg`).
            // `user.account_id` was set by `resolve_clerk_context` from that
            // claim BEFORE this function ever ran, so — left unsynced here —
            // it still names the ORG's account while `decision` says
            // "personal", and every tool that scopes by `user.account_id`
            // (RLS scope, audit row, member-ocean lookup) would run against
            // the wrong account.
            //
            // Re-resolve to the caller's own personal account explicitly —
            // the same lookup `clerk_provision::ensure_personal_account`
            // itself uses to FIND (never create) it, so this can't disagree
            // with what "the caller's personal account" means anywhere else
            // in the codebase.
            match crate::personal_account::personal_account_id(&pool, user.user_id).await {
                Ok(Some(id)) => user.account_id = id,
                Ok(None) => {
                    // Unreachable in practice — `ensure_personal_account`
                    // guarantees every provisioned user has this row — but
                    // continuing with the STALE (org) account_id under
                    // `Personal` governance is exactly the hole this fix
                    // closes, so refuse outright rather than guess.
                    tracing::error!(
                        user_id = %user.user_id,
                        "mcp_auth: Personal tenancy decision but no personal account exists for this user"
                    );
                    return forbidden("no personal account found for this user");
                }
                Err(e) => {
                    tracing::error!(error = %e, user_id = %user.user_id, "mcp_auth: personal_account_id lookup failed");
                    return service_unavailable();
                }
            }
        }
        None => {}
    }

    let scopes = effective_scopes(&mcp, claims.claims());
    let mcp_client_name = extract_client_name(&req);

    let personal_org = claims.is_personal_org();
    // The consent-screen choice this token carries, classified by the same
    // function the tenancy decision above used — never re-derived from a bare
    // `claims.org_id`, which cannot tell a real organization from the
    // caller's own personal one.
    let consent_org = match super::tenant_selection::ConsentOrg::from_claims(&claims) {
        super::tenant_selection::ConsentOrg::Org(org) => Some(org),
        super::tenant_selection::ConsentOrg::None => None,
    };
    req.extensions_mut().insert(McpAuthContext {
        personal_org,
        consent_org,
        user,
        scopes,
        client_id,
        token_jti: claims.claims().jti.clone(),
        mcp_client_name,
        decision,
    });

    next.run(req).await
}

/// Best-effort classification of a verification failure for the `mcp.auth`
/// tracing line's `reason` field (never the token contents — see plan §4.6).
/// `jsonwebtoken`'s `Display` impl names its `ErrorKind` (e.g.
/// `"ExpiredSignature"`), so a substring check is enough without needing the
/// error's structured kind threaded through `ClerkError`.
fn classify_verify_error(e: &ClerkError) -> &'static str {
    let msg = e.to_string();
    if msg.contains("Expired") {
        "expired"
    } else if msg.contains("Invalid") || msg.contains("Signature") || msg.contains("issuer") {
        "bad_signature"
    } else {
        "verification_failed"
    }
}

/// Effective scopes (plan §4.2), computed fresh on every request (never
/// cached). MCP write is ALWAYS ON for an authenticated caller — there is no
/// per-user or per-account setting (settings launch plan, decision 6). Two
/// real gates remain, and nothing else:
///
/// - **The server write ceiling** (`MCP_DEFAULT_SCOPES` must include write). A
///   hard kill switch: if it is off, nobody writes.
/// - **The token's own scope**, when it carries one. A token whose `scope`
///   claim contains any `coralswarm:*` entry is read/write exactly as that
///   scope says (write needs `coralswarm:write`, read needs
///   `coralswarm:read`), still AND-ed with the ceiling. A token with no
///   product scopes (every Clerk-minted token today — Clerk cannot mint
///   `coralswarm:*`) gets read, and write when the ceiling allows it.
///
/// Where a write may LAND is not decided here: write tools only ever target
/// the caller's own writable private ocean, and shared org oceans receive
/// reefs only through promote (see `tools/write.rs`).
///
/// Historical `user_account_prefs.mcp_write_enabled = false` rows (migration
/// 0080) and the legacy `user_prefs.mcp_write_enabled` flag (0060) are no
/// longer read anywhere, so an old deny cannot block a write.
fn effective_scopes(mcp: &McpState, claims: &ClerkClaims) -> ScopeSet {
    let ceiling = mcp.cfg.default_scopes.write;
    if let Some(scope_str) = &claims.scope {
        let tokens: Vec<&str> = scope_str.split_whitespace().collect();
        if tokens.iter().any(|t| t.starts_with("coralswarm:")) {
            return ScopeSet {
                read: tokens.contains(&"coralswarm:read"),
                write: ceiling && tokens.contains(&"coralswarm:write"),
            };
        }
    }
    ScopeSet {
        read: true,
        write: ceiling,
    }
}

// ── origin_guard middleware ───────────────────────────────────────────────────

/// Reject a request whose `Origin` header is present but not in
/// `MCP_ALLOWED_ORIGINS`. A request with no `Origin` header at all (every
/// native/non-browser MCP client: Claude Desktop, Cursor, VS Code extensions,
/// server-to-server) is always allowed — DNS-rebinding and other
/// browser-origin attacks always carry *some* Origin, so an absent header is
/// not the vector this guards against. Belt-and-suspenders on top of bearer-only auth (no cookies —
/// see plan §1 "`/mcp` CORS" resolution), which already makes CSRF inert.
pub(crate) async fn origin_guard(State(mcp): State<McpState>, req: Request, next: Next) -> Response {
    if let Some(origin) = req
        .headers()
        .get(header::ORIGIN)
        .and_then(|v| v.to_str().ok())
    {
        if !mcp.cfg.allowed_origins.iter().any(|o| o == origin) {
            tracing::warn!(target: "mcp.auth", reason = "origin_rejected", origin, "rejecting /mcp request: Origin not in MCP_ALLOWED_ORIGINS");
            return (
                StatusCode::FORBIDDEN,
                Json(json!({"error": "origin not allowed"})),
            )
                .into_response();
        }
    }
    next.run(req).await
}

// ── Response security headers ─────────────────────────────────────────────────

/// Security-review L2: `/mcp` responses were missing the headers plan §4.6
/// requires. Attaches `Cache-Control: no-store` (a `/mcp` response can carry
/// tool-call results or auth-error detail — never cache it, at any layer) and
/// `X-Content-Type-Options: nosniff` to EVERY response the `/mcp` endpoint
/// produces, not just the 200 stub — added as the OUTERMOST layer on the
/// `/mcp` sub-router (mod.rs), so it also covers the 401 from `mcp_auth`, the
/// 403 from `origin_guard`, and the 503 from a DB/provisioning failure.
/// HSTS is set at the Fly edge, not here (plan §4.6 note).
///
/// Deliberately NOT applied to the well-known metadata endpoints
/// (`well_known.rs`) — those are meant to be cached (`Cache-Control:
/// max-age=3600`) and fetched cross-origin (`Access-Control-Allow-Origin:
/// *`), which this layer would otherwise fight with.
pub(crate) async fn security_headers(req: Request, next: Next) -> Response {
    let mut resp = next.run(req).await;
    let headers = resp.headers_mut();
    headers.insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    resp
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clerk::test_support::*;
    use crate::mcp::{mcp_router_with_validator, McpConfig, ScopeSet as Scopes};
    use crate::test_app_state;
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use std::sync::Arc;
    use tower::ServiceExt;
    use uuid::Uuid;

    fn test_cfg() -> McpConfig {
        McpConfig {
            enabled: true,
            resource_url: "https://coralswarm-backend.example/mcp".to_string(),
            default_scopes: Scopes {
                read: true,
                write: true,
            },
            allowed_origins: vec!["https://claude.ai".to_string()],
            allowed_hosts: vec!["coralswarm-backend.example".to_string()],
            clerk: clerk::ClerkConfig {
                issuer: ISSUER.to_string(),
                jwks_url: format!("{ISSUER}/.well-known/jwks.json"),
            },
        }
    }

    /// A validator that returns a fixed result regardless of the token text —
    /// the mock-injection seam the plan calls for, so most tests below never
    /// touch JWKS/network at all.
    struct MockValidator(Result<ClerkClaims, ClerkError>);

    #[async_trait]
    impl TokenValidator for MockValidator {
        async fn validate(&self, _token: &str) -> Result<ClerkClaims, ClerkError> {
            self.0.clone().map_err(|e| match e {
                ClerkError::Invalid(m) => ClerkError::Invalid(m),
                ClerkError::NotJwt => ClerkError::NotJwt,
                ClerkError::NotConfigured => ClerkError::NotConfigured,
                ClerkError::Jwks(m) => ClerkError::Jwks(m),
                ClerkError::UnknownKid => ClerkError::UnknownKid,
            })
        }
    }

    fn oauth_claims(sub: &str) -> ClerkClaims {
        // Constructed via JSON round-trip since `ClerkClaims` only derives
        // `Deserialize` (it's never hand-built in production code either).
        serde_json::from_value(json!({
            "sub": sub,
            "exp": future_exp(),
            "client_id": "client_abc123",
            "jti": "jti_1"
        }))
        .unwrap()
    }

    /// An OAuth access token with an active-org context (`org_id` claim) —
    /// the claim-only org selection `/mcp` uses (org-tenancy PR 2.2).
    fn oauth_claims_with_org(sub: &str, org_id: &str) -> ClerkClaims {
        serde_json::from_value(json!({
            "sub": sub,
            "exp": future_exp(),
            "client_id": "client_abc123",
            "jti": "jti_1",
            "org_id": org_id,
            "org_role": "org:member"
        }))
        .unwrap()
    }

    fn session_claims(sub: &str) -> ClerkClaims {
        serde_json::from_value(json!({
            "sub": sub,
            "exp": future_exp(),
            "sid": "sess_xyz"
        }))
        .unwrap()
    }

    /// A session token shaped like Clerk's REAL default browser session JWT:
    /// carries `sid` (always present on a session token) AND `azp` (the
    /// standard OIDC authorized-party claim, which Clerk sessions carry too —
    /// it is NOT OAuth-specific). Regression coverage for security review
    /// finding H1: an earlier version of `has_oauth_claims` treated `azp`
    /// alone as sufficient proof of an OAuth token, so this exact shape
    /// bypassed the session-JWT rejection gate.
    fn session_claims_with_azp(sub: &str) -> ClerkClaims {
        serde_json::from_value(json!({
            "sub": sub,
            "exp": future_exp(),
            "sid": "sess_xyz",
            "azp": "https://app.example.com"
        }))
        .unwrap()
    }

    fn router_with(validator: MockValidator) -> axum::Router {
        mcp_router_with_validator(test_app_state(), test_cfg(), Arc::new(validator))
    }

    fn bearer_req(uri: &str, token: Option<&str>) -> HttpRequest<Body> {
        let mut builder = HttpRequest::builder().method("POST").uri(uri);
        if let Some(t) = token {
            builder = builder.header("Authorization", format!("Bearer {t}"));
        }
        builder.body(Body::empty()).unwrap()
    }

    // ── 401 challenge shape ───────────────────────────────────────────────────

    #[tokio::test]
    async fn missing_token_returns_401_with_challenge() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let resp = router.oneshot(bearer_req("/mcp", None)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        let challenge = resp
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();
        assert!(challenge.starts_with("Bearer "));
        assert!(challenge.contains(
            "resource_metadata=\"https://coralswarm-backend.example/.well-known/oauth-protected-resource/mcp\""
        ));
        // Challenge advertises Clerk-grantable scopes, not coralswarm:* (which
        // Clerk rejects during DCR) — see MCP_SCOPES_PERSONAL. This request has no
        // Host header, so it resolves to the PERSONAL resource: no `user:org:read`.
        assert!(challenge.contains(&format!("scope=\"{}\"", super::super::MCP_SCOPES_PERSONAL)));
        assert!(!challenge.contains("coralswarm:"));
        assert!(challenge.contains("user:org:read"));
    }

    // ── host_guard: the URL half of the tenancy chooser ───────────────────────

    fn req_with_host(host: &str) -> HttpRequest<Body> {
        HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap()
    }

    /// `host_guard` replaced rmcp's DNS-rebinding Host allowlist, so it must keep
    /// rejecting a host we do not serve — and do it BEFORE any token work, so the
    /// response is a plain 403 with no `WWW-Authenticate` challenge.
    #[tokio::test]
    async fn disallowed_host_is_rejected_before_auth() {
        let router = router_with(MockValidator(Ok(oauth_claims("user_1"))));
        let resp = router.oneshot(req_with_host("evil.example")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
        assert!(resp.headers().get(header::WWW_AUTHENTICATE).is_none());
    }

    /// The rejection still goes through the security-header layer (outermost).
    #[tokio::test]
    async fn disallowed_host_403_carries_security_headers() {
        let router = router_with(MockValidator(Ok(oauth_claims("user_1"))));
        let resp = router.oneshot(req_with_host("evil.example")).await.unwrap();
        assert_eq!(resp.headers().get(header::CACHE_CONTROL).unwrap(), "no-store");
        assert_eq!(
            resp.headers().get("x-content-type-options").unwrap(),
            "nosniff"
        );
    }

    /// An allowlisted host proceeds to normal auth handling (401 here, since the
    /// request carries no bearer) — i.e. the guard did not over-reject.
    #[tokio::test]
    async fn allowlisted_host_proceeds_to_auth() {
        let router = router_with(MockValidator(Ok(oauth_claims("user_1"))));
        let resp = router
            .oneshot(req_with_host("coralswarm-backend.example"))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── Resource fixtures ─────────────────────────────────────────────────────

    /// Wrap raw claims in the type production code reaches
    /// `resolve_clerk_context` through, so no path can skip the
    /// personal-organization translation.
    #[allow(dead_code)]
    fn norm(c: ClerkClaims) -> crate::middleware::auth::NormalizedClaims {
        crate::middleware::auth::NormalizedClaims::for_test(c)
    }

    fn personal_resource() -> ResourceIdentity {
        ResourceIdentity {
            resource_url: "https://api.example.com/mcp".to_string(),
            metadata_base: "https://api.example.com".to_string(),
            scopes: super::super::MCP_SCOPES_PERSONAL,
            tenancy: ResourceTenancy::Personal,
        }
    }

    /// A hand-built `ResourceTenancy::Org` fixture, for `resolve_mcp_tenancy`'s
    /// Org arm in isolation. **Since claim-based-tenancy
    /// §Phase 3 PR 3.3, `McpConfig::resolve_resource` never constructs this
    /// shape for any real request** (that arm now collapses to
    /// `personal_identity` — see its own doc); this fixture, and the tests
    /// below that use it, exist only to keep the Org arm's logic covered as
    /// unreachable-but-not-orphaned code. The scope string is a plain literal,
    /// not `MCP_SCOPES_ORG` (deleted in the same PR — there is only one scope
    /// set advertised anywhere now, `super::super::MCP_SCOPES_PERSONAL`); this
    /// fixture predates that constant's existence too, so a literal is no
    /// less authentic a stand-in than the deleted constant would have been.
    fn org_resource(slug: &str) -> ResourceIdentity {
        ResourceIdentity {
            resource_url: format!("https://{slug}.api.example.com/mcp"),
            metadata_base: format!("https://{slug}.api.example.com"),
            scopes: "openid profile email offline_access user:org:read",
            tenancy: ResourceTenancy::Org(slug.to_string()),
        }
    }

    // ── The org-claim strip's four tests are DELETED, not updated ────────────
    //
    // `personal_resource_strips_every_org_field`,
    // `org_resource_preserves_the_org_claim`,
    // `personal_token_on_personal_resource_is_unchanged` and
    // `org_claim_survives_when_no_org_resources_exist` all called
    // `claims_for_resource`, which no longer exists (see the block comment
    // above `resolve_mcp_tenancy`). Three of them asserted a behaviour that
    // is now WRONG — a real org claim on the personal resource is honored,
    // not stripped — and the fourth
    // (`org_claim_survives_when_no_org_resources_exist`) asserted a carve-out
    // for the no-apex case that is now simply the universal rule.
    //
    // Nothing is left uncovered by deleting them. What replaces them is
    // stricter and closer to the real path:
    //
    //   * that a real org claim SURVIVES to the tenancy decision and binds
    //     that org — `mcp_tenant_pg.rs`'s Part 3, through the real router;
    //   * that a claim without a live membership is REFUSED — the same
    //     file's Part 2;
    //   * that a personal-org claim still lands on the consumer plane —
    //     `mcp_tenant_selection_pg.rs::your_own_personal_org_claim_reaches_
    //     your_own_ocean` (unchanged, and it passes for a new reason now:
    //     `ConsentOrg::from_claims` classifies it rather than the strip
    //     happening to leave the personal-org arm alone);
    //   * that a plain personal token is untouched — every other test in
    //     this module and in `mcp_pg_e2e.rs` uses one.

    /// The challenge advertises the scopes of the resource the client addressed:
    /// an org resource asks for `user:org:read` (so Clerk renders its org
    /// selector) and points at that host's own metadata document.
    #[test]
    fn challenge_is_per_resource() {
        let personal = challenge_401(&personal_resource(), "missing bearer token");
        let value = personal
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();
        assert!(value.contains("scope=\"openid profile email offline_access user:org:read\""));
        assert!(value.contains("user:org:read"));
        assert!(value.contains(
            "resource_metadata=\"https://api.example.com/.well-known/oauth-protected-resource/mcp\""
        ));

        let org = challenge_401(&org_resource("testorg"), "missing bearer token");
        let value = org
            .headers()
            .get(header::WWW_AUTHENTICATE)
            .and_then(|v| v.to_str().ok())
            .unwrap()
            .to_string();
        assert!(value.contains("user:org:read"));
        assert!(value.contains(
            "resource_metadata=\"https://testorg.api.example.com/.well-known/oauth-protected-resource/mcp\""
        ));
    }

    // ── L2: /mcp response security headers ────────────────────────────────────

    /// Security review L2: `Cache-Control: no-store` + `X-Content-Type-Options:
    /// nosniff` must be present on EVERY `/mcp` response, including ones
    /// short-circuited before the stub handler ever runs (here: the 401 from
    /// `mcp_auth` on a missing token).
    #[tokio::test]
    async fn missing_token_401_carries_security_headers() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let resp = router.oneshot(bearer_req("/mcp", None)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            "nosniff"
        );
    }

    /// Same assertion, but on the 503 short-circuit that fires once a token
    /// has cleared every auth gate (no DB configured on `test_app_state()`) —
    /// proves the headers aren't only attached to the happy-path stub
    /// response.
    #[tokio::test]
    async fn authenticated_503_carries_security_headers() {
        let router = router_with(MockValidator(Ok(oauth_claims("user_ok"))));
        let resp = router
            .oneshot(bearer_req(
                "/mcp",
                Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig"),
            ))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
        assert_eq!(
            resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).unwrap(),
            "nosniff"
        );
    }

    // ── token-in-query-string rejection ───────────────────────────────────────

    #[tokio::test]
    async fn query_string_token_is_ignored_and_401s() {
        let router = router_with(MockValidator(Ok(oauth_claims("user_x"))));
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp?access_token=some.jwt.value")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── expired/malformed token ────────────────────────────────────────────────

    #[tokio::test]
    async fn expired_token_returns_401() {
        let router = router_with(MockValidator(Err(ClerkError::Invalid(
            "ExpiredSignature".to_string(),
        ))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn malformed_bearer_is_not_a_jwt_and_401s() {
        // Doesn't even have the 3-segment JWT shape, so it's rejected before
        // ever reaching the validator.
        let router = router_with(MockValidator(Ok(oauth_claims("user_x"))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("not-a-jwt-at-all")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── session-JWT rejection ─────────────────────────────────────────────────

    #[tokio::test]
    async fn session_jwt_without_oauth_claims_is_rejected() {
        let router = router_with(MockValidator(Ok(session_claims("user_sess"))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    /// H1 regression: a session token carrying BOTH `sid` and `azp` — exactly
    /// what Clerk's real default browser session JWT looks like — must still
    /// be rejected. Before the H1 fix, `azp.is_some()` alone satisfied
    /// `has_oauth_claims`, so this shape passed straight through.
    #[tokio::test]
    async fn session_jwt_with_sid_and_azp_is_still_rejected() {
        let router = router_with(MockValidator(Ok(session_claims_with_azp("user_sess"))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── mock validator injection + authenticated stub response ───────────────

    #[tokio::test]
    async fn oauth_token_with_no_db_returns_503_after_passing_token_checks() {
        // No `db` on `test_app_state()`, so once the token itself verifies and
        // passes the OAuth-claims gate, provisioning correctly 503s rather
        // than 401ing — proves the request got past all the token-shape gates.
        let router = router_with(MockValidator(Ok(oauth_claims("user_ok"))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    // ── origin_guard ──────────────────────────────────────────────────────────

    #[tokio::test]
    async fn disallowed_origin_is_rejected_before_auth_runs() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header("Origin", "https://evil.example")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn absent_origin_is_allowed_through_to_auth() {
        // No Origin header at all -> passes origin_guard, then still hits the
        // normal 401 (no token) from mcp_auth.
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let resp = router.oneshot(bearer_req("/mcp", None)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn allowlisted_origin_is_allowed_through() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header("Origin", "https://claude.ai")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        // Still 401 (no token) — the point is it's NOT 403.
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── body-size limit (security review 2026-07-15) ─────────────────────────
    //
    // `RequestBodyLimitLayer` sits BEFORE `mcp_auth` in the layer stack
    // (`mod.rs`), so a request with no bearer token at all still gets `413`
    // for an oversized body rather than `401` — proving the cap is enforced
    // independently of, and before, any auth/DB work, not merely as a
    // by-product of `mcp_auth`'s own 401 short-circuit. `DefaultBodyLimit`
    // (the pre-fix implementation) would have let this same request through
    // to `mcp_auth` and then rmcp uncapped, since it never reads a body at
    // all — it only sets a marker extension axum's `FromRequest` extractors
    // consult, and rmcp's `StreamableHttpService` isn't one of them.
    //
    // These tests set `Content-Length` explicitly: real HTTP transports
    // (hyper, and the client rmcp itself would send) always populate it for a
    // non-chunked body, but `tower::ServiceExt::oneshot`'s in-process request
    // construction does not derive it from the body automatically. Setting it
    // exercises `RequestBodyLimit`'s immediate Content-Length short-circuit —
    // the same path a real oversized request hits — which returns `413`
    // WITHOUT ever calling the next inner service (`mcp_auth`, then
    // `mcp_service`), the strongest form of "the rmcp service never processed
    // it".

    #[tokio::test]
    async fn oversized_body_is_rejected_413_before_the_rmcp_service_or_auth_runs() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let oversized = vec![0u8; 2 * 1024 * 1024 + 1];
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header(header::CONTENT_LENGTH, oversized.len().to_string())
            // Deliberately NO Authorization header — if the body limit fired
            // only as a consequence of auth running first, this would 401
            // instead of 413.
            .body(Body::from(oversized))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[tokio::test]
    async fn oversized_body_413_still_carries_security_headers() {
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let oversized = vec![0u8; 2 * 1024 * 1024 + 1];
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header(header::CONTENT_LENGTH, oversized.len().to_string())
            .body(Body::from(oversized))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );
    }

    #[tokio::test]
    async fn body_at_exactly_the_cap_is_not_rejected_by_the_limit() {
        // At-the-cap should pass the body-limit layer (proving this isn't an
        // off-by-one that rejects legitimate max-sized `add_context` calls) —
        // it still 401s afterward since there's no token, which is the point:
        // NOT 413.
        let router = router_with(MockValidator(Err(ClerkError::NotJwt)));
        let at_cap = vec![0u8; 2 * 1024 * 1024];
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/mcp")
            .header(header::CONTENT_LENGTH, at_cap.len().to_string())
            .body(Body::from(at_cap))
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // ── flag-off -> 404 (McpConfig::from_env resolves to None) ───────────────

    #[tokio::test]
    async fn app_without_mcp_config_404s_mcp_and_well_known() {
        // Serialized against `mcp::tests`' `McpConfig::from_env` tests, which
        // mutate the same `MCP_ENABLED` var — see that module's `test_support`.
        let _guard = crate::mcp::test_support::env_lock().lock().await;
        unsafe {
            std::env::remove_var("MCP_ENABLED");
        }
        let router = crate::app(test_app_state());
        for uri in [
            "/mcp",
            "/.well-known/oauth-protected-resource",
            "/.well-known/oauth-protected-resource/mcp",
        ] {
            let req = HttpRequest::builder()
                .uri(uri)
                .body(Body::empty())
                .unwrap();
            let resp = router.clone().oneshot(req).await.unwrap();
            assert_eq!(
                resp.status(),
                StatusCode::NOT_FOUND,
                "expected 404 for {uri} when MCP_ENABLED is unset"
            );
        }
    }

    // ── stub /mcp response shape once authenticated ───────────────────────────

    #[tokio::test]
    async fn authenticated_stub_call_returns_jsonrpc_not_yet_enabled() {
        // db present via a live pool isn't needed to exercise this because we
        // want to confirm the SHAPE of the stub only when auth genuinely
        // succeeds end-to-end; that requires a DB for provisioning, which
        // Layer C (pg-gated) tests exercise. Here we instead assert on the
        // 503 short-circuit's body, since `test_app_state()` has no pool —
        // this is the same "further than expired/malformed" proof as above,
        // just checking response *body* shape isn't a 401/403.
        let router = router_with(MockValidator(Ok(oauth_claims("user_ok"))));
        let resp = router
            .oneshot(bearer_req("/mcp", Some("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ4In0.fakesig")))
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
        let bytes = to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["error"], "service unavailable");
    }

    /// Build a bare `ClerkClaims` carrying the given OAuth `scope` string.
    fn claims_with_scope(scope: &str) -> ClerkClaims {
        serde_json::from_value(json!({
            "sub": "u",
            "exp": future_exp(),
            "client_id": "client_abc123",
            "jti": "jti_1",
            "scope": scope
        }))
        .unwrap()
    }

    fn state_with_ceiling(write: bool) -> McpState {
        let mut cfg = test_cfg();
        cfg.default_scopes.write = write;
        McpState {
            app: test_app_state(),
            cfg: Arc::new(cfg),
            validator: Arc::new(MockValidator(Ok(oauth_claims("u")))),
        }
    }

    /// Settings launch plan decision 6: MCP write is always on. A token with
    /// no product scopes (every Clerk-minted token today) gets write whenever
    /// the server ceiling allows it. Synchronous: no database is consulted.
    #[test]
    fn unscoped_token_gets_write_when_the_ceiling_allows_it() {
        let scopes = effective_scopes(&state_with_ceiling(true), &oauth_claims("u"));
        assert!(scopes.read && scopes.write);
    }

    /// The ceiling is a hard kill switch for every token shape.
    #[test]
    fn ceiling_off_denies_write_for_unscoped_and_scoped_tokens() {
        let state = state_with_ceiling(false);
        let unscoped = effective_scopes(&state, &oauth_claims("u"));
        assert!(unscoped.read, "read is always granted for an OAuth token");
        assert!(!unscoped.write, "ceiling off must deny an unscoped token");
        let scoped = effective_scopes(&state, &claims_with_scope("coralswarm:read coralswarm:write"));
        assert!(scoped.read);
        assert!(!scoped.write, "ceiling off must deny even a coralswarm:write token");
    }

    /// A token that carries product scopes is held to them: no
    /// `coralswarm:write` means no write, even with the ceiling on.
    #[test]
    fn token_without_the_write_scope_is_refused_write() {
        let scopes = effective_scopes(&state_with_ceiling(true), &claims_with_scope("coralswarm:read"));
        assert!(scopes.read, "coralswarm:read grants read");
        assert!(!scopes.write, "no coralswarm:write scope means no write");
    }

    /// ... and with `coralswarm:write` (ceiling on) it succeeds.
    #[test]
    fn token_with_the_write_scope_gets_write() {
        let scopes = effective_scopes(
            &state_with_ceiling(true),
            &claims_with_scope("openid coralswarm:read coralswarm:write"),
        );
        assert!(scopes.read && scopes.write);
    }

    /// A write-only token does not silently gain read.
    #[test]
    fn write_scope_alone_does_not_grant_read() {
        let scopes = effective_scopes(&state_with_ceiling(true), &claims_with_scope("coralswarm:write"));
        assert!(!scopes.read);
        assert!(scopes.write);
    }

    // ── Security review (MCP listing-prep #900, item 2): account resync ─────
    //
    // The tests below need a real Postgres, so they are gated on
    // `ORGT_TEST_BASE_URL` exactly like the external `*_pg.rs` suites. Kept
    // IN-CRATE so they can call `mcp_auth` directly and read the resulting
    // `McpAuthContext` back.

    fn control_test_pool_url(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    async fn recreate_scratch_db(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
    }

    async fn migrated_control_pool(base: &str, db: &str) -> sqlx::PgPool {
        recreate_scratch_db(base, db).await;
        let pool = sqlx::postgres::PgPoolOptions::new()
            .max_connections(4)
            .connect(&control_test_pool_url(base, db))
            .await
            .unwrap();
        crate::CONTROL_MIGRATOR.run(&pool).await.unwrap();
        pool
    }

    /// `mcp_auth` used to resync `user.account_id` from the resolved tenancy
    /// decision ONLY on the `TenancyDecision::Org` arm. A caller whose
    /// STORED selection heals to `SelectionOutcome::Personal` (a
    /// `StoredSelection::OwnPersonalOrg` row, which ALWAYS heals to Personal
    /// regardless of what the token's claim names) while the TOKEN's claim
    /// names a REAL organization left `user.account_id` at the REAL org's
    /// account — this test drives `mcp_auth` itself (not the full `/mcp`
    /// router) and reads the resulting `McpAuthContext` back, proving
    /// `user.account_id` now names the caller's OWN personal account instead.
    #[tokio::test]
    async fn mcp_auth_resyncs_account_id_to_personal_when_the_decision_heals_to_personal() {
        use axum::routing::post;
        use axum::Extension;

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping mcp_auth_resyncs_account_id_to_personal_when_the_decision_heals_to_personal: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let pool = migrated_control_pool(&base, "orgt_auth_resync_ctl").await;

        let sub = format!("resync_{}", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{sub}@resync.test"))
                .bind(&sub)
                .fetch_one(&pool)
                .await
                .expect("seed user");

        // The caller's own personal organization — the account every tool
        // must scope to once the fix lands.
        let personal_org_id = format!("org_resync_own_{}", Uuid::new_v4());
        sqlx::query("INSERT INTO personal_orgs (clerk_org_id, clerk_user_id) VALUES ($1, $2)")
            .bind(&personal_org_id)
            .bind(&sub)
            .execute(&pool)
            .await
            .expect("seed personal_orgs row");
        let personal_account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&personal_org_id)
        .fetch_one(&pool)
        .await
        .expect("seed personal-org account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'admin', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(personal_account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed personal-org membership");

        // A REAL org the same caller also belongs to — the account the BUG
        // would have left `user.account_id` pointing at instead.
        let real_org_id = format!("org_resync_real_{}", Uuid::new_v4());
        let real_account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(user_id)
        .bind(&real_org_id)
        .fetch_one(&pool)
        .await
        .expect("seed real org account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))",
        )
        .bind(real_account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed real org membership");

        // The STORED selection: this client_id's row names the caller's OWN
        // personal org — `StoredSelection::OwnPersonalOrg`, which heals to
        // `SelectionOutcome::Personal` regardless of the token's own claim.
        // Must match `oauth_claims_with_org`'s hardcoded `client_id` claim
        // (`"client_abc123"`) — `mcp_auth` derives the stored-selection key
        // from the TOKEN's own `client_id`/`azp` claim, not from a value this
        // test can choose independently.
        let client_id = "client_abc123";
        sqlx::query(
            "INSERT INTO mcp_tenant_selection (user_id, client_id, clerk_org_id) VALUES ($1, $2, $3)",
        )
        .bind(user_id)
        .bind(client_id)
        .bind(&personal_org_id)
        .execute(&pool)
        .await
        .expect("seed the OwnPersonalOrg selection row");

        let mut app_state: AppState = (*test_app_state()).clone();
        app_state.db = Some(pool.clone());
        app_state.tenant_runtime = Some(crate::tenancy::runtime::TenantRuntime::new(
            true,
            "coralswarm.example",
            std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(std::sync::Arc::new(
                crate::tenancy::secrets::StaticMapResolver::from_pairs(Vec::<(String, String)>::new()),
            ))),
        ));
        let state = McpState {
            app: Arc::new(app_state),
            cfg: Arc::new(test_cfg()),
            // The TOKEN's own claim names the REAL org — `ConsentOrg::Org(real_org)`.
            validator: Arc::new(MockValidator(Ok(oauth_claims_with_org(&sub, &real_org_id)))),
        };

        async fn probe(Extension(auth): Extension<McpAuthContext>) -> Json<serde_json::Value> {
            Json(json!({
                "account_id": auth.user.account_id,
                "is_personal_decision": matches!(auth.decision, Some(TenancyDecision::Personal)),
                "write": auth.scopes.write,
            }))
        }

        let router = axum::Router::new()
            .route("/probe", post(probe))
            .layer(axum::middleware::from_fn_with_state(state.clone(), mcp_auth))
            .with_state(state);

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/probe")
            .header("Authorization", "Bearer eyJhbGci.eyJzdWIi.sig")
            .body(Body::empty())
            .unwrap();
        let resp = router.oneshot(req).await.unwrap();
        let status = resp.status();
        let bytes = to_bytes(resp.into_body(), 4096).await.unwrap();
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes).to_string()));
        assert_eq!(status, StatusCode::OK, "mcp_auth must let this request through: {body}");

        assert_eq!(
            body["is_personal_decision"],
            json!(true),
            "fixture precondition: OwnPersonalOrg must heal to TenancyDecision::Personal: {body}"
        );
        assert_eq!(
            body["account_id"],
            json!(personal_account_id),
            "user.account_id must be resynced to the caller's OWN personal account when the \
             decision heals to Personal, never left at the real org's account from the raw \
             claim: {body}"
        );
        assert_ne!(
            body["account_id"],
            json!(real_account_id),
            "must not still be the real org's account: {body}"
        );

        pool.close().await;
        recreate_scratch_db(&base, "orgt_auth_resync_ctl").await;
    }

    /// Settings launch plan decision 6 (PR 2.1), against a real Postgres: an
    /// OLD explicit `false` in `user_account_prefs` (migration 0080) AND in
    /// the legacy `user_prefs` (0060) must not block MCP write. The only gates
    /// left are the server ceiling and the token's `coralswarm:write` scope:
    /// a token carrying that scope writes despite the old deny rows, a token
    /// carrying only `coralswarm:read` is still refused.
    #[tokio::test]
    async fn old_false_pref_rows_do_not_block_write_and_the_scope_gate_still_holds() {
        use axum::routing::post;
        use axum::Extension;

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping old_false_pref_rows_do_not_block_write_and_the_scope_gate_still_holds: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let pool = migrated_control_pool(&base, "orgt_auth_oldfalse_ctl").await;

        let sub = format!("oldfalse_{}", Uuid::new_v4());
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users (email, clerk_user_id) VALUES ($1, $2) RETURNING id")
                .bind(format!("{sub}@oldfalse.test"))
                .bind(&sub)
                .fetch_one(&pool)
                .await
                .expect("seed user");
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts (owner_user_id) VALUES ($1) RETURNING id",
        )
        .bind(user_id)
        .fetch_one(&pool)
        .await
        .expect("seed personal account");
        sqlx::query(
            "INSERT INTO account_members (account_id, user_id, role) VALUES ($1, $2, 'owner')",
        )
        .bind(account_id)
        .bind(user_id)
        .execute(&pool)
        .await
        .expect("seed membership");
        // The OLD explicit denies the removed Settings toggle could have left.
        sqlx::query(
            "INSERT INTO user_account_prefs (user_id, account_id, mcp_write_enabled) VALUES ($1, $2, false)",
        )
        .bind(user_id)
        .bind(account_id)
        .execute(&pool)
        .await
        .expect("seed old explicit false (user_account_prefs)");
        sqlx::query("INSERT INTO user_prefs (user_id, mcp_write_enabled) VALUES ($1, false)")
            .bind(user_id)
            .execute(&pool)
            .await
            .expect("seed old explicit false (legacy user_prefs)");

        async fn probe(Extension(auth): Extension<McpAuthContext>) -> Json<serde_json::Value> {
            Json(json!({"read": auth.scopes.read, "write": auth.scopes.write}))
        }

        let mut app_state: AppState = (*test_app_state()).clone();
        app_state.db = Some(pool.clone());
        let app = Arc::new(app_state);

        // (scope claim, expected write)
        let cases: [(Option<&str>, bool); 3] = [
            (Some("coralswarm:read coralswarm:write"), true),
            (Some("coralswarm:read"), false),
            (None, true), // a token with no product scopes: the ceiling alone decides
        ];
        for (scope, expect_write) in cases {
            let mut claims = json!({
                "sub": sub, "exp": future_exp(), "client_id": "client_abc123", "jti": "jti_1"
            });
            if let Some(scope) = scope {
                claims["scope"] = json!(scope);
            }
            let claims: ClerkClaims = serde_json::from_value(claims).unwrap();
            let state = McpState {
                app: app.clone(),
                cfg: Arc::new(test_cfg()),
                validator: Arc::new(MockValidator(Ok(claims))),
            };
            let router = axum::Router::new()
                .route("/probe", post(probe))
                .layer(axum::middleware::from_fn_with_state(state.clone(), mcp_auth))
                .with_state(state);
            let req = HttpRequest::builder()
                .method("POST")
                .uri("/probe")
                .header("Authorization", "Bearer eyJhbGci.eyJzdWIi.sig")
                .body(Body::empty())
                .unwrap();
            let resp = router.oneshot(req).await.unwrap();
            let status = resp.status();
            let bytes = to_bytes(resp.into_body(), 4096).await.unwrap();
            let body: serde_json::Value = serde_json::from_slice(&bytes)
                .unwrap_or_else(|_| json!(String::from_utf8_lossy(&bytes).to_string()));
            assert_eq!(status, StatusCode::OK, "scope {scope:?}: {body}");
            assert_eq!(
                body["write"],
                json!(expect_write),
                "scope {scope:?} with old explicit-false pref rows present: {body}"
            );
        }

        pool.close().await;
        recreate_scratch_db(&base, "orgt_auth_oldfalse_ctl").await;
    }

    // ── extract_client_name (User-Agent -> McpAuthContext.mcp_client_name) ────
    //
    // See `McpAuthContext::mcp_client_name`'s doc comment for why this reads
    // `User-Agent` rather than the MCP protocol's own `initialize`
    // `clientInfo.name`: in this deployment's stateless streamable-HTTP mode,
    // `clientInfo` is only ever real on the literal `initialize` request
    // itself (rmcp's `peer_info_for_stateless_request` reconstructs
    // `Implementation::default()` for every other request, including every
    // `tools/call`), so it is unusable from inside a tool handler here.

    fn req_with_ua(ua: Option<&str>) -> Request {
        let mut builder = axum::http::Request::builder().method("POST").uri("/mcp");
        if let Some(ua) = ua {
            builder = builder.header(header::USER_AGENT, ua);
        }
        builder.body(axum::body::Body::empty()).unwrap()
    }

    #[test]
    fn extract_client_name_reads_the_user_agent_header() {
        let req = req_with_ua(Some("claude-code/1.0"));
        assert_eq!(extract_client_name(&req).as_deref(), Some("claude-code/1.0"));
    }

    #[test]
    fn extract_client_name_is_none_when_the_header_is_absent() {
        let req = req_with_ua(None);
        assert_eq!(extract_client_name(&req), None);
    }

    #[test]
    fn extract_client_name_is_none_for_a_whitespace_only_header() {
        let req = req_with_ua(Some("   "));
        assert_eq!(extract_client_name(&req), None);
    }

    #[test]
    fn extract_client_name_trims_surrounding_whitespace() {
        let req = req_with_ua(Some("  cursor/2.0  "));
        assert_eq!(extract_client_name(&req).as_deref(), Some("cursor/2.0"));
    }

    #[test]
    fn extract_client_name_truncates_to_the_cap() {
        let oversized = "x".repeat(MCP_CLIENT_NAME_CAP + 100);
        let req = req_with_ua(Some(&oversized));
        let got = extract_client_name(&req).expect("some name");
        assert_eq!(got.chars().count(), MCP_CLIENT_NAME_CAP);
    }

    // `mcp_auth`'s own end-to-end wiring of `McpAuthContext.mcp_client_name`
    // (not just the pure `extract_client_name` helper above) is deliberately
    // NOT unit-tested in this in-crate module: `mcp_auth` only reaches the
    // `extract_client_name` call AFTER `mcp.app.db` presence is checked
    // (`let Some(pool) = mcp.app.db.clone() else { return
    // service_unavailable(); };` runs first) — see the source above — so
    // against `test_app_state()` (no `db`) every request 503s BEFORE
    // `mcp_client_name` is ever computed, regardless of what `User-Agent`
    // header it carries. An earlier version of this test asserted only that
    // shape (a 503 with a `User-Agent` header attached) and mistakenly
    // claimed that proved the header was read; it didn't (CodeRabbit
    // review). The real end-to-end proof — driven through the actual public
    // HTTP router against a REAL Postgres pool, so `mcp_auth` runs to
    // completion and the value is asserted off the persisted atom — is
    // `tests/mcp_pg_e2e.rs`'s
    // `add_context_stamps_mcp_client_name_from_user_agent_against_real_postgres`.
}
