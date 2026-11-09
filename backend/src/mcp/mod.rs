//! Remote MCP server. See `docs/plans/remote-mcp-v1.md`.
//!
//! This module is crate-private: it is mounted by [`crate::app`] only when
//! [`McpConfig::from_env`] resolves to `Some` (flag on + Clerk configured), and
//! nothing outside `backend/` ever needs to name these types directly.
//!
//! Phase 1 wired the auth spine: `mcp_auth` + `origin_guard` middleware
//! (`auth.rs`) and RFC 9728 protected-resource metadata + a cached Clerk
//! authorization-server-metadata mirror (`well_known.rs`).
//!
//! Phase 2 (this module's `/mcp` route) replaces the Phase-1 stub handler
//! with rmcp's real `StreamableHttpService` (stateless mode), backed by the
//! `CoralSwarmMcp` `ServerHandler` (`server.rs`) and its 12 read tools
//! (`tools/read.rs`). The auth/origin/security-header middleware stack around
//! `/mcp` is unchanged by this swap — the rmcp service is just a different
//! inner `tower::Service` mounted at the same route.
//!
//! Phase 3 adds the 4 write tools (`tools/write.rs`), populates
//! `tools::WRITE_TOOLS` (the scope filter/gate `list_tools`/`call_tool`
//! already consult), and applies a 2 MiB request-body ceiling to `/mcp`
//! (`MCP_MAX_BODY_BYTES`) — the auth/origin/security-header stack is
//! otherwise unchanged again.

use std::sync::Arc;

use axum::Router;
use rmcp::transport::streamable_http_server::{
    session::never::NeverSessionManager, StreamableHttpServerConfig, StreamableHttpService,
};
use tower_http::limit::RequestBodyLimitLayer;

use crate::AppState;

/// Body-size ceiling for `/mcp` requests (plan §3.2 #11, §4.7 abuse-case
/// "oversized ingest / cost abuse"). `add_context` is the only write tool that
/// takes meaningfully large input (content ≤100,000 chars, well under this),
/// but the limit is applied to the whole `/mcp` route rather than per-tool —
/// there is no per-tool body-size hook in rmcp's streamable-HTTP transport,
/// and every other tool's inputs are small (uuids/short strings), so one
/// route-wide ceiling is both simpler and sufficient. Precedent:
/// `SYNC_MAX_BODY_BYTES` (`lib.rs`).
///
/// **Security review (2026-07-15): must be `tower_http::limit::RequestBodyLimitLayer`,
/// NOT `axum::extract::DefaultBodyLimit`.** `DefaultBodyLimit` works by
/// inserting a marker extension that axum's OWN `FromRequest` body extractors
/// (`Json`, `Bytes`, etc.) consult before buffering — `/mcp` is mounted via
/// `route_service` with rmcp's `StreamableHttpService`, which reads the body
/// directly via `http_body_util`'s `collect()` and never looks at that marker,
/// so `DefaultBodyLimit` was a silent no-op here (rmcp would buffer an
/// arbitrarily large body). `RequestBodyLimitLayer` instead wraps the request
/// body itself in `tower_http::body::Limited` (short-circuiting to `413`
/// immediately when `Content-Length` exceeds the cap, and erroring the body
/// stream mid-read when it doesn't), which every consumer sees regardless of
/// whether it goes through axum's extractor machinery.
///
/// rmcp 3.x (`mcp_router_with_validator`'s `StreamableHttpServerConfig::
/// with_max_request_body_bytes`) additionally enforces this SAME ceiling
/// itself while streaming the POST body (new in 3.x — 2.2.0 had no such
/// config field). This layer stays regardless: it is what actually protects
/// today (short-circuits on `Content-Length` before rmcp ever starts reading,
/// and the axum layer is proven correct by the security review above),
/// rmcp's is defense-in-depth against a future path that bypasses this outer
/// layer, and duplicating the cap costs nothing.
pub(crate) const MCP_MAX_BODY_BYTES: usize = 2 * 1024 * 1024;

/// Scopes advertised to MCP clients (in the protected-resource-metadata
/// `scopes_supported` and the `WWW-Authenticate` challenge hint) and therefore
/// requested by clients from Clerk during DCR + authorization.
///
/// These MUST be scopes the authorization server (Clerk) actually grants —
/// Clerk rejects a Dynamic Client Registration whose `scope` contains anything
/// outside its allowed set (`openid profile email offline_access user:org:read
/// public_metadata private_metadata`) with `400 invalid_client_metadata`, which
/// aborts the connect flow in every client. The product's own `coralswarm:read`
/// / `coralswarm:write` scopes are deliberately NOT advertised: Clerk cannot
/// mint them today, and the backend derives read/write server-side from the env
/// ceiling and the token's own scope (see [`auth::effective_scopes`]), so
/// advertising them bought nothing and broke DCR. `offline_access` yields a
/// refresh token so connectors survive access-token expiry without re-consent.
/// Revisit if Clerk ships custom OAuth scopes (plan §7 open question #5).
///
/// The ONLY scope set advertised to MCP clients, on every resource — there is
/// only one resource now (see [`McpConfig::resolve_resource`]).
///
/// # `user:org:read` IS advertised, and that is the whole tenancy story
///
/// Clerk renders an **organization picker** on the OAuth consent screen for
/// `user:org:read`, and mints an `org_id` claim naming whatever the user
/// picked. So a `/mcp` token is org-shaped exactly when its owner chose an
/// organization at connect time, and personal-shaped otherwise. That claim is
/// honored (`auth::resolve_personal_tenancy` →
/// [`tenant_selection::resolve_request_tenancy`]), so the connect screen is a
/// real tenant chooser and not a decoration.
///
/// **This scope has been removed and restored once; do not remove it a third
/// time without reading both halves.**
///
/// * **#810 (claim-based-tenancy §Phase 3 PR 3.3) dropped it.** The picker it
///   renders lists ONLY organizations, with no row for a person's own ocean
///   (verified against a real consent screen), so advertising it put every org
///   member's personal ocean permanently out of reach over MCP. Phase 3
///   replaced the URL-side choice — and this scope — with an explicit in-band
///   one (`tools::tenancy::{list_tenants,select_tenant}`), which is why the
///   separate `MCP_SCOPES_ORG` constant and the org-subdomain resource it was
///   advertised on are both gone for good.
/// * **#825 (`f015e3d7`) restored it**, because personal organizations
///   (control migration `0208_personal_orgs.sql`) gave that picker the missing
///   row: a Clerk organization that STANDS FOR one person's own ocean, which
///   `middleware::auth::normalize_personal_org_claim` translates back to the
///   consumer plane. With that row present the picker can express every
///   choice, so the reason #810 dropped the scope no longer holds.
///
/// The in-band chooser stays regardless — it is how a caller SWITCHES tenant
/// without re-consenting, and a stored selection outranks the claim (see
/// [`tenant_selection::resolve_selected_or_consented_tenant`]).
///
/// Changing this string is not a local edit: it is byte-frozen into the
/// SEP-2352 protected-resource metadata, so every existing OAuth grant is
/// invalidated and every client must reconnect (that is exactly what #825's
/// own rollout did).
pub(crate) const MCP_SCOPES_PERSONAL: &str =
    "openid profile email offline_access user:org:read";

pub(crate) mod audit;
pub(crate) mod auth;
pub(crate) mod header_backfill;
pub(crate) mod widget_trace;
pub(crate) mod server;
/// The `/mcp` tenancy gate: the pure invariant (PR 3.1), the real
/// `mcp_tenant_selection`-backed I/O around it (PR 3.2), and — since #825 —
/// the consent screen's own `org_id` claim as a second, lower-precedence way
/// to name a tenant. One real call site (`resolve_request_tenancy`, consumed
/// by `auth::resolve_mcp_tenancy`); see the module doc.
pub(crate) mod tenant_selection;
pub(crate) mod tools;
pub(crate) mod well_known;

// ── Config ────────────────────────────────────────────────────────────────────

/// A parsed `MCP_DEFAULT_SCOPES` / effective-scope value. Two independent
/// booleans (not a bitflag) because the two scopes are checked independently
/// and read almost always wins by construction (any authenticated OAuth token
/// gets `read`; see `auth::effective_scopes`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct ScopeSet {
    pub read: bool,
    pub write: bool,
}

impl ScopeSet {
    pub(crate) fn none() -> Self {
        Self::default()
    }

    /// Parse a space-delimited scope string (`MCP_DEFAULT_SCOPES` env format,
    /// and the same format Clerk's own `scope` claim uses).
    pub(crate) fn parse(raw: &str) -> Self {
        let mut set = Self::none();
        for tok in raw.split_whitespace() {
            match tok {
                "coralswarm:read" => set.read = true,
                "coralswarm:write" => set.write = true,
                _ => {}
            }
        }
        set
    }
}

/// Resolved remote-MCP configuration. Built once in [`crate::app`] (never
/// per-request — see the module doc on `mcp_router`) from the environment.
#[derive(Clone, Debug)]
pub(crate) struct McpConfig {
    /// Always `true` for a `McpConfig` that exists — kept as a field (rather
    /// than relying purely on `Option<McpConfig>` at the call site) so the
    /// struct is self-describing in tests and logs.
    #[allow(dead_code)]
    pub enabled: bool,
    /// The resource identifier advertised in RFC 9728 PRM documents and the
    /// `WWW-Authenticate` challenge, e.g.
    /// `https://coralswarm-backend.example/mcp`. Compared byte-for-byte by
    /// clients — no trailing slash, exact scheme+host+path.
    pub resource_url: String,
    /// The environment ceiling on write access (§1 "Write-scope source"
    /// resolution). It is a hard upper bound: if `write` is off here, no caller
    /// gets write regardless of anything else. When `write` is on, a token with
    /// no `coralswarm:*` scopes may write; one that carries them needs
    /// `coralswarm:write` (see `auth::effective_scopes`). There is no per-user
    /// or per-account write setting.
    pub default_scopes: ScopeSet,
    /// Exact-match allowlist for the `Origin` header on `/mcp` requests
    /// (`origin_guard`). Requests with no `Origin` header (native/non-browser
    /// clients) are always allowed — only a *present-but-unallowed* Origin is
    /// rejected.
    pub allowed_origins: Vec<String>,
    /// Allowlist for the `Host` header on `/mcp` requests, enforced by rmcp's
    /// `StreamableHttpService` DNS-rebinding guard. rmcp defaults this to
    /// `localhost`/`127.0.0.1`/`::1` only (its guard targets locally-running
    /// servers), which 403s every request to a real deployment host. We derive
    /// it from `MCP_RESOURCE_URL`'s authority (so it's correct per-environment
    /// automatically) plus the loopback hosts for local dev, overridable via
    /// `MCP_ALLOWED_HOSTS` (comma-separated) when a deployment is reached under
    /// more than one host (e.g. a custom domain in front of the Fly host).
    pub allowed_hosts: Vec<String>,
    /// Embedded Clerk verification config (issuer + JWKS URL) — the same
    /// config `middleware::auth::require_auth` uses for session JWTs.
    pub clerk: crate::clerk::ClerkConfig,
}

/// Which tenancy plane a request's resource URL selects — the URL-side half of
/// the tenancy decision (see [`McpConfig::resolve_resource`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ResourceTenancy {
    /// The apex (or any configured non-tenant host): the consumer plane. An
    /// `org_id` claim presented here is STRIPPED, not honored — see
    /// [`auth::mcp_auth`].
    Personal,
    /// `<slug>.<apex>`: that org's tenant plane, pending the full gate policy
    /// (the slug is syntactically valid but not yet known to exist).
    ///
    /// **Unreachable via [`McpConfig::resolve_resource`] since claim-based-
    /// tenancy §Phase 3 PR 3.3** — that function's `Subdomain` arm now
    /// returns [`ResourceTenancy::Personal`] like every other host (the org
    /// MCP resource is retired). Kept, not deleted:
    /// `auth::resolve_mcp_tenancy` still handles this variant correctly,
    /// covered by its own unit tests against a hand-built
    /// [`ResourceIdentity`] — `#[allow(dead_code)]` is the honest way to say
    /// "no live constructor reaches this anymore" without deleting logic that
    /// still typechecks and is still exercised, in isolation, by name.
    #[allow(dead_code)]
    Org(String),
}

/// The resource identity a single request resolved to: which plane it selects,
/// the RFC 9728 resource identifier to advertise for it, and the scopes a client
/// should request for it. Built once per request by
/// [`McpConfig::resolve_resource`] and stamped into request extensions by
/// [`auth::host_guard`], so the PRM document, the `WWW-Authenticate` challenge,
/// and the tenancy decision are all driven by the same resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ResourceIdentity {
    /// RFC 9728 `resource` — compared byte-for-byte by clients against the URL
    /// they used to reach us.
    pub resource_url: String,
    /// Origin the `/.well-known/*` and `resource_metadata=` URLs are built
    /// against (`resource_url` minus the `/mcp` suffix).
    pub metadata_base: String,
    /// Scopes advertised for this resource — always [`MCP_SCOPES_PERSONAL`]
    /// since claim-based-tenancy §Phase 3 PR 3.3 (there is only one resource
    /// now; kept as a field, not inlined, so the PRM handler and the
    /// `WWW-Authenticate` challenge builder don't need to know that).
    pub scopes: &'static str,
    pub tenancy: ResourceTenancy,
    // `org_resources_available` used to live here: a flag saying whether a
    // tenant apex was configured, whose ONLY consumer was
    // `auth::claims_for_resource`'s org-claim strip. Its stated justification
    // — "stripping is only correct when the caller has an org URL to go to
    // instead" — outlived its own premise twice over: #810 deleted every org
    // URL there was, and #825 then made an `org_id` claim on this resource
    // the user's own consent-screen choice rather than something to strip at
    // all. The strip is gone, so the flag has no consumer and is gone with
    // it.
}

/// Canonicalize a raw `Host` header for comparison and for reflection into a
/// resource URL: trim, lowercase (ASCII), drop a single trailing dot. The
/// `:port` suffix is KEPT — it is part of the authority the client actually
/// used, and a reflected `resource` that dropped it would no longer match the
/// client's own resource identifier byte-for-byte.
///
/// Deliberately NOT a re-implementation of [`crate::tenancy::gate::classify_host`]'s
/// canonicalization: that function does its own (port-stripping) normalization
/// internally and is the sole authority on classification. This only produces
/// the string form.
fn canonical_host(raw: &str) -> Option<String> {
    let mut h = raw.trim().to_ascii_lowercase();
    if h.ends_with('.') {
        h.pop();
    }
    (!h.is_empty()).then_some(h)
}

/// Strip RFC 3986 brackets from an IPv6 literal (`"[::1]"` -> `"::1"`), for
/// COMPARISON only — never for reflection, where a bracketless literal would
/// produce an invalid URL.
///
/// Needed because the two sides arrive in different shapes: real HTTP clients
/// send IPv6 authorities bracketed (`Host: [::1]:8080`), while an allowlist entry
/// is written bare — [`McpConfig::from_env`] seeds `"::1"` and an operator setting
/// `MCP_ALLOWED_HOSTS` would too. Comparing those byte-for-byte never matches, so
/// the loopback entry would 403 every genuine IPv6 caller (MCP Inspector over
/// `::1`, for one).
fn strip_brackets(host: &str) -> &str {
    host.strip_prefix('[')
        .and_then(|h| h.strip_suffix(']'))
        .unwrap_or(host)
}

/// Split a canonical authority into `(host, port)`.
///
/// A trailing `:<digits>` is a port ONLY when what precedes it cannot itself be a
/// bare IPv6 address: a bracketed literal (`[::1]:8080`) or a string with no other
/// colon. So `"::1"` stays a host — splitting it into `(":", Some("1"))` would let
/// an allowlisted `::1` match nothing (or worse, match something else).
fn split_host_port(authority: &str) -> (&str, Option<&str>) {
    match authority.rsplit_once(':') {
        Some((host, port))
            if !port.is_empty()
                && port.bytes().all(|b| b.is_ascii_digit())
                && (host.ends_with(']') || !host.contains(':')) =>
        {
            (host, Some(port))
        }
        _ => (authority, None),
    }
}

/// Extract the authority (`host[:port]`) from a URL string without pulling in a
/// URL-parsing dependency: drop the scheme, then take everything before the
/// first `/`, `?`, or `#`. Returns `None` for an empty authority.
fn authority_of(url: &str) -> Option<String> {
    let after_scheme = url.split_once("://").map(|(_, r)| r).unwrap_or(url);
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim();
    (!authority.is_empty()).then(|| authority.to_string())
}

impl McpConfig {
    /// Build from the environment. Returns `None` (flag mounts nothing) unless
    /// `MCP_ENABLED` is truthy AND `CLERK_ISSUER` is set. If the flag is on but
    /// the issuer is missing, this logs an error and still returns `None` —
    /// refusing to mount rather than mounting an auth spine with no
    /// authorization server to verify against.
    pub(crate) fn from_env() -> Option<Self> {
        let enabled = env_truthy("MCP_ENABLED");
        if !enabled {
            return None;
        }

        let Some(clerk) = crate::clerk::ClerkConfig::from_env() else {
            tracing::error!(
                "MCP_ENABLED is set but CLERK_ISSUER is unset — refusing to mount /mcp \
                 (the resource server has no authorization server to verify tokens against)"
            );
            return None;
        };

        // Startup invariant (remote-mcp-v1.md §4.3): the in-memory rate
        // limiter is per-VM, so on multi-replica Fly it would silently
        // multiply every limit. This is a warning, not a mount refusal — MCP
        // still works correctly on a single replica, and operators may be
        // testing locally.
        if std::env::var("RATE_LIMIT_BACKEND").as_deref() != Ok("pg") {
            tracing::warn!(
                "MCP_ENABLED is set but RATE_LIMIT_BACKEND != \"pg\" — per-user/account rate \
                 limits will NOT be shared across replicas on a multi-machine deployment; set \
                 RATE_LIMIT_BACKEND=pg before enabling MCP outside a single-replica dev/test env"
            );
        }

        let resource_url = std::env::var("MCP_RESOURCE_URL")
            .unwrap_or_else(|_| "http://localhost:8080/mcp".to_string());
        let default_scopes = std::env::var("MCP_DEFAULT_SCOPES")
            .ok()
            .map(|s| ScopeSet::parse(&s))
            .unwrap_or(ScopeSet {
                read: true,
                write: true,
            });
        let allowed_origins = std::env::var("MCP_ALLOWED_ORIGINS")
            .ok()
            .map(|s| {
                s.split(',')
                    .map(|o| o.trim().to_string())
                    .filter(|o| !o.is_empty())
                    .collect()
            })
            .unwrap_or_default();

        // rmcp's Host allowlist, built additively so no legitimate Host is ever
        // dropped:
        //   1. the loopback hosts (local dev / MCP Inspector over localhost),
        //   2. the deployment authority from MCP_RESOURCE_URL (the Host that
        //      real hosted requests actually carry — must ALWAYS be allowed),
        //   3. any explicit MCP_ALLOWED_HOSTS entries (additional hosts a
        //      deployment is also reached under, e.g. a custom domain in front
        //      of the Fly host).
        // MCP_ALLOWED_HOSTS ADDS to (1)+(2); it never replaces them — replacing
        // the derived authority would silently 403 the real deployment host.
        let mut allowed_hosts = vec![
            "localhost".to_string(),
            "127.0.0.1".to_string(),
            "::1".to_string(),
        ];
        let push_host = |h: String, hosts: &mut Vec<String>| {
            if !h.is_empty() && !hosts.contains(&h) {
                hosts.push(h);
            }
        };
        if let Some(authority) = authority_of(&resource_url) {
            push_host(authority, &mut allowed_hosts);
        }
        if let Ok(s) = std::env::var("MCP_ALLOWED_HOSTS") {
            for h in s.split(',').map(|h| h.trim().to_string()) {
                push_host(h, &mut allowed_hosts);
            }
        }

        Some(Self {
            enabled,
            resource_url,
            default_scopes,
            allowed_origins,
            allowed_hosts,
            clerk,
        })
    }

    /// The origin the `/.well-known/oauth-protected-resource*` and
    /// `WWW-Authenticate: resource_metadata=` URLs are built against — the
    /// resource URL with its `/mcp` suffix (and any trailing slash) stripped.
    pub(crate) fn metadata_base(&self) -> String {
        self.resource_url
            .trim_end_matches("/mcp")
            .trim_end_matches('/')
            .to_string()
    }

    /// The configured (personal-plane) resource identity — the apex resource,
    /// byte-exact as configured, with no Host reflection at all.
    ///
    /// Infallible on purpose: callers on a request path (`auth::resource_of`, the
    /// PRM handler's fallback) need a personal identity without depending on an
    /// invariant that lives inside [`Self::resolve_resource`] — routing it through
    /// the fallible resolver and unwrapping would turn a future change to that
    /// function's missing-Host arm into a per-request panic in middleware.
    pub(crate) fn personal_identity(&self) -> ResourceIdentity {
        ResourceIdentity {
            resource_url: self.resource_url.clone(),
            metadata_base: self.metadata_base(),
            scopes: MCP_SCOPES_PERSONAL,
            tenancy: ResourceTenancy::Personal,
        }
    }

    /// Membership in [`Self::allowed_hosts`], matching rmcp's DNS-rebinding guard
    /// semantics exactly (see [`mcp_router_with_validator`] for why we now own
    /// that check): the host parts must be equal, and the PORT is only compared
    /// when the allowlist entry specifies one.
    ///
    /// That port rule is not a nicety — it is what makes a bare `localhost` /
    /// `127.0.0.1` entry match a server bound to an ephemeral port, which is how
    /// every real-HTTP test (and any local dev server on a non-default port) is
    /// admitted. An exact string comparison here 403s all of them.
    fn host_allowed(&self, canon_host: &str) -> bool {
        let (req_host, req_port) = split_host_port(canon_host);
        let req_host = strip_brackets(req_host);
        self.allowed_hosts
            .iter()
            .filter_map(|h| canonical_host(h))
            .any(|allowed| {
                let (a_host, a_port) = split_host_port(&allowed);
                strip_brackets(a_host) == req_host && (a_port.is_none() || a_port == req_port)
            })
    }

    /// Resolve which resource a request addressed, from its `Host` header and
    /// the configured tenant `apex`. `None` means "a host we do not serve":
    /// the caller rejects the request.
    ///
    /// **The org MCP resource is RETIRED (claim-based-tenancy §Phase 3 PR
    /// 3.3, correction 2).** Every host this function can classify now
    /// resolves to [`ResourceTenancy::Personal`], advertising
    /// [`MCP_SCOPES_PERSONAL`] — there is no other scope set to advertise
    /// anymore (see that constant's doc for why). Tenancy is not a URL
    /// decision at all: the "personal ocean or which org?" question is
    /// answered by the token's own consent-screen claim, or in-band per call
    /// by `tools::tenancy::{list_tenants,select_tenant}` — both resolved by
    /// [`tenant_selection::resolve_request_tenancy`], never by which host the
    /// client happened to connect to.
    ///
    /// * apex (or `app.`/`www.`) → personal, as before.
    /// * `<slug>.<apex>` → **also personal now** (this arm used to be
    ///   [`ResourceTenancy::Org`], with its own PRM document reflecting the
    ///   subdomain into `resource_url`/`metadata_base` — see this function's
    ///   git history for that shape). It is unreachable in practice anyway:
    ///   `crate::retired_org_host_guard` 410s a subdomain Host one layer
    ///   outside `/mcp`. This arm was deliberately kept distinct through
    ///   Phases 1–2 ("delete, don't orphan" — a resource nothing could reach
    ///   yet was still a resource someone might be mid-migration to); Phase 3
    ///   PR 3.3 retired it, and #825 did not bring it back: restoring
    ///   `user:org:read` (see [`MCP_SCOPES_PERSONAL`]) put the tenant choice
    ///   on the CONSENT SCREEN, not back into the URL.
    /// * any other host that is explicitly allowed (`localhost`, the
    ///   `MCP_RESOURCE_URL` authority, `MCP_ALLOWED_HOSTS` entries) → personal,
    ///   as before.
    /// * anything else → `None`.
    ///
    /// `apex` is `None` when the tenancy runtime is absent/unconfigured
    /// (`TENANT_APEX_DOMAIN` unset — the flag-off default). Then there are no
    /// org subdomains to speak of and this degrades to exactly the
    /// pre-org-tenancy behavior: the configured resource, personal, for any
    /// allowed host — unchanged by this PR.
    ///
    /// A missing/unparseable Host is treated as the personal apex rather than
    /// rejected: `Host` is absent only for non-browser callers (and h2 requests
    /// whose `:authority` a middleware dropped), and DNS rebinding — the only
    /// threat this check exists for — is a browser attack that always carries
    /// one. `origin_guard` independently covers browser callers.
    ///
    /// Reflection safety: since the retirement above, the returned
    /// `resource_url` NEVER echoes the request Host on any arm — every arm
    /// returns the CONFIGURED resource URL, so an arbitrary (or a real org)
    /// Host can never appear in a metadata document.
    pub(crate) fn resolve_resource(
        &self,
        host: Option<&str>,
        apex: Option<&str>,
    ) -> Option<ResourceIdentity> {
        let apex = apex.map(str::trim).filter(|a| !a.is_empty());
        let Some(canon) = host.and_then(canonical_host) else {
            return Some(self.personal_identity());
        };
        let Some(apex) = apex else {
            // No apex configured: pre-org-tenancy behavior, gated by the
            // allowlist.
            return self
                .host_allowed(&canon)
                .then(|| self.personal_identity());
        };

        match crate::tenancy::gate::classify_host(&canon, apex) {
            // The org MCP resource is retired (see this function's doc): a
            // subdomain is no longer a distinct resource, so it collapses to
            // EXACTLY the same personal identity the apex gets — same
            // (configured) `resource_url`, same scopes, no Host reflection.
            crate::tenancy::gate::HostTarget::Apex
            | crate::tenancy::gate::HostTarget::Subdomain(_) => Some(self.personal_identity()),
            // Not under our apex — but a deployment may legitimately also be
            // reached at its Fly host or over loopback in dev.
            crate::tenancy::gate::HostTarget::Foreign => {
                self.host_allowed(&canon).then(|| self.personal_identity())
            }
        }
    }
}

fn env_truthy(name: &str) -> bool {
    matches!(
        std::env::var(name).ok().as_deref(),
        Some("1") | Some("true") | Some("TRUE") | Some("True") | Some("yes") | Some("on")
    )
}

// ── Shared state for the mcp sub-router ──────────────────────────────────────

/// State threaded through every `/mcp` + well-known handler and middleware.
/// Distinct from [`AppState`] (rather than extending it) because `McpConfig`
/// and the token validator are meaningful only when MCP is mounted at all, and
/// keeping them separate means `AppState` doesn't grow an `Option` that's
/// `None` on every non-MCP request path (`/v1/*`).
#[derive(Clone)]
pub(crate) struct McpState {
    pub app: Arc<AppState>,
    pub cfg: Arc<McpConfig>,
    pub validator: Arc<dyn auth::TokenValidator>,
}

// ── Router ────────────────────────────────────────────────────────────────────

/// Build the mcp sub-router (well-known metadata + the `/mcp` endpoint) using
/// the real Clerk-backed token validator. Called once from [`crate::app`] when
/// `McpConfig::from_env()` resolves — config and the validator are therefore
/// resolved once per process (or once per `app()` call in tests), never
/// per-request.
pub(crate) fn mcp_router(app: Arc<AppState>, cfg: McpConfig) -> Router {
    let validator: Arc<dyn auth::TokenValidator> = Arc::new(auth::ClerkTokenValidator {
        cfg: cfg.clerk.clone(),
        http: app.http_client.clone(),
        resource_url: cfg.resource_url.clone(),
    });
    mcp_router_with_validator(app, cfg, validator)
}

/// Same as [`mcp_router`], but with an injectable [`auth::TokenValidator`] —
/// used by tests to avoid depending on a live Clerk instance / JWKS endpoint,
/// and to avoid the env-var-toggling races parallel `cargo test` runs would
/// otherwise hit if tests tried to swap validators via the environment.
pub(crate) fn mcp_router_with_validator(
    app: Arc<AppState>,
    cfg: McpConfig,
    validator: Arc<dyn auth::TokenValidator>,
) -> Router {
    let state = McpState {
        app: app.clone(),
        cfg: Arc::new(cfg),
        validator,
    };

    // The real rmcp streamable-HTTP transport (Phase 2). `legacy_session_mode:
    // false` (rmcp 3.x rename of `stateful_mode` — SEP-2567) per plan §1
    // "Session mode" resolution (multi-replica Fly safety, tools-only
    // server); `NeverSessionManager` is the stateless-mode session manager
    // (its methods are simply never called when `legacy_session_mode` is
    // false — see rmcp's `tower.rs::handle_post`, which only ever touches
    // `self.session_manager` inside the `legacy_session_mode: true` branch).
    // A modern-era (`2026-07-28`) request is ALWAYS served statelessly
    // regardless of this setting per SEP-2567 — `legacy_session_mode` only
    // governs sessions/GET/DELETE/priming/resumability for protocol versions
    // older than that. A fresh `CoralSwarmMcp` is built by the factory
    // closure for every request (stateless mode serves each request its own
    // `serve_directly` call) — cheap (an `Arc` clone + a small `HashMap` of
    // tool routes), and per the module doc on per-request identity plumbing,
    // no per-request state may be cached on the handler struct anyway.
    let mcp_app_state = app.clone();
    // rmcp's DNS-rebinding guard is an EXACT-match Host allowlist
    // (`host_is_allowed`), so it cannot express "any valid `<slug>.<apex>`" —
    // it would 403 every org resource, and orgs are created at runtime, so no
    // static list can enumerate them. We therefore take the check over
    // ourselves: `auth::host_guard` runs BEFORE this service (see the layer
    // order below) and enforces a STRICTLY STRONGER rule — the same exact-match
    // allowlist rmcp used, PLUS apex/subdomain classification against our own
    // configured apex, rejecting everything else. Emptying rmcp's list makes its
    // guard allow-all (`host_is_allowed` returns true for an empty list), which
    // is only sound BECAUSE `host_guard` has already rejected any host we do not
    // serve. Origin checking is untouched — `origin_guard` owns that, outermost.
    let mcp_service = StreamableHttpService::new(
        move || Ok(server::CoralSwarmMcp::new(mcp_app_state.clone())),
        Arc::new(NeverSessionManager::default()),
        StreamableHttpServerConfig::default()
            .with_legacy_session_mode(false)
            .disable_allowed_hosts()
            // Defense in depth alongside the `RequestBodyLimitLayer` below
            // (which is what actually enforces the ceiling today — see that
            // layer's doc comment on `mcp_service`'s route below for why
            // `axum::extract::DefaultBodyLimit` doesn't work here). rmcp 3.x
            // added its own body-size enforcement inside the streamable-HTTP
            // transport itself, so this now also caps the body BEFORE it
            // reaches `mcp_service`'s own JSON-RPC parsing, independent of
            // the axum layer.
            .with_max_request_body_bytes(MCP_MAX_BODY_BYTES)
            // SEP-2567/2549: prefer a plain `application/json` response for
            // ordinary request/response tool calls instead of SSE — every one
            // of our 20 tools is plain request/response (no server-initiated
            // notifications/requests before the final result), so rmcp never
            // needs to fall back to `text/event-stream` for us in practice.
            // Plain JSON is simpler for every client here (Claude Code, the
            // Claude Desktop connector, ChatGPT) to consume than parsing SSE frames
            // for a single-shot response.
            .with_json_response(true)
            // Wire rmcp's shutdown token to the process's own graceful-
            // shutdown signal (`run()` in `lib.rs`) instead of leaving it a
            // detached `CancellationToken::new()` that nothing ever cancels.
            // `AppState::mcp_cancellation_token` is cancelled from the same
            // spot the job-worker `Shutdown` flag is triggered.
            //
            // This is NOT "let in-flight requests finish" — read against
            // rmcp 3.1.2's `tower.rs` for OUR exact config (stateless:
            // `legacy_session_mode: false` + `NeverSessionManager`, no
            // `EventStore`), there are no sessions for this token to
            // "terminate" (that's the field doc's language, written for
            // `legacy_session_mode: true`). What actually happens: every
            // `/mcp` POST races `self.config.cancellation_token.child_token()`
            // against the tool handler's own response channel inside
            // `handle_post`/`serve_negotiated_request_directly`. If the
            // handler hasn't produced its terminal message by the time this
            // token fires, that race returns an immediate "empty response"
            // 500 to the caller — the handler itself is NOT cancelled (it
            // keeps running, detached from the now-abandoned HTTP response,
            // until it finishes naturally or the process exits) and is NOT
            // drained/waited-on either. Because a child token created from an
            // already-cancelled parent is itself already cancelled, this
            // applies to every `/mcp` request dispatched from the moment
            // SIGTERM/CTRL-C fires onward, not only ones already in flight.
            // Accepted for now: our 20 tools are plain, fast request/response
            // calls (see the `with_json_response` comment above) with no
            // long-running server push, so the exposure window is small;
            // revisit if that stops being true.
            .with_cancellation_token(app.mcp_cancellation_token.clone())
            // Left at its default (`false`), which DISABLES this
            // requirement: set to `true`, rmcp would require every stateless
            // request to carry `MCP-Protocol-Version` + per-request `_meta`
            // before dispatch. Our clients today are legacy-era (Claude Code,
            // the Claude Desktop connector) and don't send that per-request
            // metadata, so turning this on would break them; `false` keeps
            // legacy requests working exactly as before while modern-era
            // (`2026-07-28`) requests still get the SDK's own stricter
            // built-in validation for that era regardless of this flag.
            .with_stateless_protocol_metadata_required(false),
    );

    // Layer order: `.layer()` wraps outermost-last, so:
    //   1. `mcp_auth` (added first) is innermost — closest to the rmcp
    //      service — and inserts `McpAuthContext` into the request
    //      extensions that ride through to every tool call (see
    //      `tools::auth`).
    //   1b. `host_guard` (added next) wraps `mcp_auth` — runs BEFORE it, so a
    //      Host we do not serve is rejected before any JWT verification, and
    //      resolves the request's `ResourceIdentity` into extensions for
    //      `mcp_auth` (tenancy plane + the challenge's `scope`/
    //      `resource_metadata`). It also REPLACES rmcp's own DNS-rebinding
    //      Host allowlist — see `mcp_service` above.
    //   2. `RequestBodyLimitLayer` (added next) wraps that — runs BEFORE
    //      `mcp_auth`, so an oversized body is rejected with `413` before any
    //      JWT verification or DB round-trip is spent on it (security review
    //      2026-07-15: cheapest-check-first, same rationale as `origin_guard`
    //      running before `mcp_auth`). Content-Length short-circuits
    //      immediately without ever touching the body; a body that lies about
    //      its length errors out of `Limited` mid-read, before `mcp_service`
    //      (or anything downstream of it) ever sees the excess bytes.
    //   3. `origin_guard` (added next) wraps that — runs BEFORE the body
    //      limit too, rejecting a disallowed Origin before doing any other
    //      work ("added last -> runs first").
    //   4. `security_headers` (added last) is OUTERMOST — it post-processes
    //      every response this sub-router produces (200 tool result, 401,
    //      403, 413, 503 alike) to attach the security-review-mandated
    //      response headers (L2), regardless of which inner layer generated
    //      the response.
    let mcp_endpoint = Router::new()
        .route_service("/mcp", mcp_service)
        //   0. `header_backfill` (added first of all) is the INNERMOST layer —
        //      it runs last, on an already-authenticated, size-bounded request,
        //      right before rmcp's own SEP-2243 header validation. It adds the
        //      `Mcp-Method`/`Mcp-Name` headers a 2026-07-28 client left out
        //      (ChatGPT's template fetch, 2026-09-14) and never rewrites a
        //      header that is present. See `header_backfill.rs`.
        .layer(axum::middleware::from_fn(
            header_backfill::backfill_mcp_headers,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::mcp_auth,
        ))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::host_guard,
        ))
        .layer(RequestBodyLimitLayer::new(MCP_MAX_BODY_BYTES))
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            auth::origin_guard,
        ))
        .layer(axum::middleware::from_fn(auth::security_headers))
        // Lazy, capped request tee: observes guard denials without reading
        // unauthenticated bodies; response streams are never intercepted.
        .layer(axum::middleware::from_fn_with_state(
            env_truthy("MCP_WIDGET_FETCH_TRACE"),
            widget_trace::observe,
        ));

    Router::new()
        .merge(mcp_endpoint)
        .merge(well_known::router())
        .with_state(state)
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scope_set_parses_read_and_write() {
        let s = ScopeSet::parse("coralswarm:read coralswarm:write");
        assert!(s.read && s.write);
    }

    #[test]
    fn scope_set_parses_read_only() {
        let s = ScopeSet::parse("coralswarm:read");
        assert!(s.read && !s.write);
    }

    #[test]
    fn scope_set_ignores_unknown_tokens() {
        let s = ScopeSet::parse("openid profile coralswarm:read");
        assert!(s.read && !s.write);
    }

    /// A config whose apex resource is `https://api.example.com/mcp`, plus the
    /// Fly host as an extra allowed host (the shape prod actually runs).
    fn cfg_for_apex() -> McpConfig {
        McpConfig {
            enabled: true,
            resource_url: "https://api.example.com/mcp".to_string(),
            default_scopes: ScopeSet {
                read: true,
                write: true,
            },
            allowed_origins: vec![],
            allowed_hosts: vec![
                "localhost".to_string(),
                "api.example.com".to_string(),
                "coralswarm-backend.example".to_string(),
            ],
            clerk: crate::clerk::ClerkConfig {
                issuer: "https://example.clerk.accounts.dev".to_string(),
                jwks_url: "https://example.clerk.accounts.dev/.well-known/jwks.json".to_string(),
            },
        }
    }

    const APEX: Option<&str> = Some("api.example.com");

    // ── resolve_resource: the tenancy chooser ─────────────────────────────────

    /// **Renamed from `apex_host_is_personal_and_omits_org_scope`.** #825
    /// flipped this test's assertion (`user:org:read` is advertised again)
    /// and left the NAME and the inline comment saying the opposite — "no
    /// org selector, so Clerk mints no org_id claim" — which is how a live
    /// consent-screen picker went on being described as absent for weeks.
    /// Name, comment and assertion now agree.
    #[test]
    fn apex_host_is_personal_and_advertises_the_org_scope() {
        let cfg = cfg_for_apex();
        let res = cfg.resolve_resource(Some("api.example.com"), APEX).unwrap();
        // "Personal" names the RESOURCE, not the plane the caller ends up on:
        // there is only one resource, and the tenant choice rides on the
        // token's consent-screen claim (or a stored selection), never the URL.
        assert_eq!(res.tenancy, ResourceTenancy::Personal);
        // Byte-exact as configured — no Host reflection on this arm.
        assert_eq!(res.resource_url, "https://api.example.com/mcp");
        assert_eq!(res.metadata_base, "https://api.example.com");
        assert_eq!(res.scopes, MCP_SCOPES_PERSONAL);
        // The whole point: WITH this scope Clerk renders the organization
        // picker and mints an `org_id` claim for what the user picked, which
        // `mcp::auth::resolve_personal_tenancy` then honors.
        assert!(
            res.scopes.contains("user:org:read"),
            "the apex resource must advertise user:org:read — without it Clerk mints no \
             org_id claim and the consent screen shows no organization picker at all"
        );
    }

    // `the_mcp_exemption_is_only_sound_while_mcp_still_classifies_org_hosts`
    // (PR #807's expiry test for `crate::RETIRED_ORG_HOST_MCP_EXEMPT_PATHS`)
    // did its job and is DELETED, not merely updated: this PR (claim-based-
    // tenancy §Phase 3 PR 3.3) is exactly the "PR 3.3 lands" window that test
    // existed to catch. Confirmed red before this deletion —
    // `assertion left == right failed: ... DELETE crate::
    // RETIRED_ORG_HOST_MCP_EXEMPT_PATHS ...` naming this exact constant — and
    // the constant plus its one call site are now gone from `lib.rs`. Nothing
    // is orphaned: the exemption itself had no purpose left the moment
    // `resolve_resource`'s `Subdomain` arm (below) stopped producing `Org`.

    /// Claim-based-tenancy §Phase 3 PR 3.3: the org MCP resource is retired.
    /// This replaces `org_subdomain_is_org_tenancy_with_org_scope_and_own_
    /// resource_url` (this same test, pre-PR-3.3, asserted the OPPOSITE: `Org`
    /// tenancy, `user:org:read` in scopes, and the subdomain reflected into
    /// `resource_url`) with the new truth: a subdomain host collapses to
    /// EXACTLY the same identity the apex gets.
    #[test]
    fn org_subdomain_now_resolves_to_the_same_personal_identity_as_the_apex() {
        let cfg = cfg_for_apex();
        let apex_res = cfg.resolve_resource(Some("api.example.com"), APEX).unwrap();
        let subdomain_res = cfg
            .resolve_resource(Some("testorg.api.example.com"), APEX)
            .unwrap();
        assert_eq!(subdomain_res.tenancy, ResourceTenancy::Personal);
        // Byte-exact as CONFIGURED, never reflecting the subdomain the
        // request actually used.
        assert_eq!(subdomain_res.resource_url, "https://api.example.com/mcp");
        assert_eq!(subdomain_res.metadata_base, "https://api.example.com");
        assert_eq!(subdomain_res.scopes, MCP_SCOPES_PERSONAL);
        assert!(subdomain_res.scopes.contains("user:org:read"));
        // Identical in every field to the apex's own identity — there is no
        // longer any distinction between the two hosts at all.
        assert_eq!(subdomain_res, apex_res);
    }

    #[test]
    fn host_is_canonicalized_before_classification() {
        let cfg = cfg_for_apex();
        // Uppercase + trailing dot + surrounding whitespace all normalize.
        assert_eq!(
            cfg.resolve_resource(Some(" API.Example.com. "), APEX)
                .unwrap()
                .tenancy,
            ResourceTenancy::Personal
        );
        // A canonicalized SUBDOMAIN also classifies (never `None`/rejected) —
        // it just resolves personal now, per
        // `org_subdomain_now_resolves_to_the_same_personal_identity_as_the_apex`.
        let res = cfg
            .resolve_resource(Some("TestOrg.API.example.com"), APEX)
            .unwrap();
        assert_eq!(res.tenancy, ResourceTenancy::Personal);
        assert_eq!(res.resource_url, "https://api.example.com/mcp");
    }

    #[test]
    fn allowlisted_non_apex_host_stays_personal() {
        let cfg = cfg_for_apex();
        for host in ["coralswarm-backend.example", "localhost"] {
            let res = cfg.resolve_resource(Some(host), APEX).unwrap();
            assert_eq!(
                res.tenancy,
                ResourceTenancy::Personal,
                "{host} should stay on the personal plane"
            );
            assert_eq!(res.resource_url, "https://api.example.com/mcp");
        }
    }

    #[test]
    fn foreign_host_is_rejected() {
        let cfg = cfg_for_apex();
        for host in [
            "evil.example",
            // Nested label under our apex: `classify_host` refuses it, so it
            // must never be treated as the slug `deep`.
            "deep.nested.api.example.com",
            // Reserved apex labels are not tenant slugs.
            "app.evil.example",
            // Too short to be a valid slug.
            "ab.api.example.com",
        ] {
            assert!(
                cfg.resolve_resource(Some(host), APEX).is_none(),
                "{host} must not resolve to any resource"
            );
        }
    }

    #[test]
    fn reserved_apex_labels_are_personal_not_tenants() {
        let cfg = cfg_for_apex();
        for host in ["app.api.example.com", "www.api.example.com"] {
            let res = cfg.resolve_resource(Some(host), APEX).unwrap();
            assert_eq!(res.tenancy, ResourceTenancy::Personal, "{host}");
        }
    }

    #[test]
    fn missing_host_resolves_to_the_personal_apex() {
        let cfg = cfg_for_apex();
        let res = cfg.resolve_resource(None, APEX).unwrap();
        assert_eq!(res.tenancy, ResourceTenancy::Personal);
        assert_eq!(res.scopes, MCP_SCOPES_PERSONAL);
    }

    /// rmcp's port rule, which this check replaced: a bare allowlist entry matches
    /// ANY port. Regression coverage — an exact string comparison here 403s every
    /// real-HTTP test server and every local dev server on a non-default port
    /// (caught by CI's `mcp_pg_e2e`, which binds an ephemeral port).
    #[test]
    fn allowlist_entry_without_a_port_matches_any_port() {
        let cfg = cfg_for_apex();
        // `localhost` and `api.example.com` are bare entries in this fixture (and
        // `from_env` always seeds bare loopback entries in production).
        for host in [
            "localhost",
            "localhost:54321",
            "api.example.com:8080",
            "coralswarm-backend.example:443",
        ] {
            assert!(
                cfg.resolve_resource(Some(host), None).is_some(),
                "{host} must be admitted by the bare allowlist entry"
            );
        }
        // A DIFFERENT host is still refused, port or no port.
        assert!(cfg.resolve_resource(Some("evil.example:8080"), None).is_none());
    }

    /// An allowlist entry that DOES pin a port only matches that port.
    #[test]
    fn allowlist_entry_with_a_port_matches_only_that_port() {
        let mut cfg = cfg_for_apex();
        cfg.allowed_hosts = vec!["dev.example:8080".to_string()];
        assert!(cfg.resolve_resource(Some("dev.example:8080"), None).is_some());
        assert!(cfg.resolve_resource(Some("dev.example:9090"), None).is_none());
        assert!(cfg.resolve_resource(Some("dev.example"), None).is_none());
    }

    #[test]
    fn split_host_port_never_mangles_a_bare_ipv6_literal() {
        assert_eq!(split_host_port("::1"), ("::1", None));
        assert_eq!(split_host_port("[::1]:8080"), ("[::1]", Some("8080")));
        assert_eq!(split_host_port("localhost:8080"), ("localhost", Some("8080")));
        assert_eq!(split_host_port("localhost"), ("localhost", None));
        // Not a port: non-numeric.
        assert_eq!(split_host_port("host:abc"), ("host:abc", None));
    }

    /// The bare `::1` default entry must admit a REAL IPv6 caller, which per RFC
    /// 3986 always sends the authority bracketed (`[::1]`, `[::1]:8080`). An
    /// earlier version of this test asserted the bracketed form was REJECTED while
    /// its own doc comment claimed v6 callers were admitted — the assertion was
    /// wrong, not the comment, and it would have shipped a guard that 403s every
    /// IPv6 loopback client.
    #[test]
    fn ipv6_loopback_is_allowlisted_in_bracketed_and_bare_form() {
        let mut cfg = cfg_for_apex();
        cfg.allowed_hosts = vec!["::1".to_string()];
        for host in ["::1", "[::1]", "[::1]:8080", "[::1]:54321"] {
            assert!(
                cfg.resolve_resource(Some(host), None).is_some(),
                "{host} must be admitted by the bare ::1 allowlist entry"
            );
        }
        // A different v6 address is still refused.
        assert!(cfg.resolve_resource(Some("[::2]:8080"), None).is_none());
    }

    /// The reverse direction: a bracketed ALLOWLIST entry must admit a bare or
    /// bracketed caller too — normalization has to be symmetric, since operators
    /// write `MCP_ALLOWED_HOSTS` by hand.
    #[test]
    fn bracketed_allowlist_entry_matches_either_form() {
        let mut cfg = cfg_for_apex();
        cfg.allowed_hosts = vec!["[::1]".to_string()];
        for host in ["::1", "[::1]", "[::1]:9999"] {
            assert!(
                cfg.resolve_resource(Some(host), None).is_some(),
                "{host} must be admitted by the bracketed allowlist entry"
            );
        }
    }

    /// A port pinned on a bracketed v6 entry is still honored exactly.
    #[test]
    fn bracketed_ipv6_entry_with_a_port_matches_only_that_port() {
        let mut cfg = cfg_for_apex();
        cfg.allowed_hosts = vec!["[::1]:8080".to_string()];
        assert!(cfg.resolve_resource(Some("[::1]:8080"), None).is_some());
        assert!(cfg.resolve_resource(Some("[::1]:9090"), None).is_none());
        assert!(cfg.resolve_resource(Some("[::1]"), None).is_none());
    }

    #[test]
    fn strip_brackets_only_unwraps_a_fully_bracketed_literal() {
        assert_eq!(strip_brackets("[::1]"), "::1");
        assert_eq!(strip_brackets("::1"), "::1");
        assert_eq!(strip_brackets("localhost"), "localhost");
        // Not a bracketed literal — left alone rather than half-stripped.
        assert_eq!(strip_brackets("[::1"), "[::1");
        assert_eq!(strip_brackets("::1]"), "::1]");
    }

    #[test]
    fn without_an_apex_only_allowlisted_hosts_resolve_and_all_are_personal() {
        let cfg = cfg_for_apex();
        // Flag-off shape: no apex configured, so there are no org subdomains and
        // the exact-match allowlist is the whole rule (what rmcp used to do).
        let res = cfg.resolve_resource(Some("api.example.com"), None).unwrap();
        assert_eq!(res.tenancy, ResourceTenancy::Personal);
        assert!(cfg
            .resolve_resource(Some("testorg.api.example.com"), None)
            .is_none());
        assert!(cfg.resolve_resource(Some("evil.example"), None).is_none());
        // A blank apex is treated as unconfigured, not as a wildcard.
        assert!(cfg
            .resolve_resource(Some("testorg.api.example.com"), Some("   "))
            .is_none());
    }

    /// Claim-based-tenancy §Phase 3 PR 3.3: this used to prove a subdomain's
    /// REFLECTED `resource_url` preserved the configured scheme/port (e.g.
    /// `http://testorg.api.localtest.me:8080/mcp`). There is no more
    /// reflection to preserve anything IN — the org resource is retired, so a
    /// subdomain now returns the CONFIGURED resource verbatim, unreflected,
    /// same as every other host. This is the property that replaces it: even
    /// with a nonstandard scheme/port configured (a live dev-server
    /// combination the old reflection path had to get right), a subdomain
    /// Host never leaks into the returned `resource_url`.
    #[test]
    fn org_subdomain_no_longer_reflects_into_resource_url_even_with_a_nonstandard_scheme_and_port() {
        let mut cfg = cfg_for_apex();
        cfg.resource_url = "http://api.localtest.me:8080/mcp".to_string();
        let res = cfg
            .resolve_resource(Some("testorg.api.localtest.me:8080"), Some("api.localtest.me"))
            .unwrap();
        assert_eq!(res.resource_url, "http://api.localtest.me:8080/mcp");
        assert!(!res.resource_url.contains("testorg"));
    }

    #[test]
    fn metadata_base_strips_mcp_suffix() {
        let cfg = McpConfig {
            enabled: true,
            resource_url: "https://coralswarm-backend.example/mcp".to_string(),
            default_scopes: ScopeSet {
                read: true,
                write: true,
            },
            allowed_origins: vec![],
            allowed_hosts: vec!["coralswarm-backend.example".to_string()],
            clerk: crate::clerk::ClerkConfig {
                issuer: "https://example.clerk.accounts.dev".to_string(),
                jwks_url: "https://example.clerk.accounts.dev/.well-known/jwks.json".to_string(),
            },
        };
        assert_eq!(cfg.metadata_base(), "https://coralswarm-backend.example");
    }

    // ── McpConfig::from_env ───────────────────────────────────────────────────
    //
    // These mutate process env vars, which `cargo test`'s default in-binary
    // thread-pool parallelism turns into a real race against ANY other test
    // that touches the same vars (including `mcp::auth`'s
    // `app_without_mcp_config_404s_mcp_and_well_known`) — unlike the
    // single-var, single-test case the crate already accepted elsewhere (see
    // `lib.rs`'s `openapi_json_returns_200_in_dev`), this module has multiple
    // tests mutating the SAME vars to DIFFERENT values, which reliably
    // flaked. Serialized on `super::test_support::env_lock()`.
    #[test]
    fn from_env_none_when_flag_unset() {
        let _guard = test_support::env_lock().blocking_lock();
        unsafe {
            std::env::remove_var("MCP_ENABLED");
        }
        assert!(McpConfig::from_env().is_none());
    }

    #[test]
    fn from_env_none_when_flag_on_but_issuer_missing() {
        let _guard = test_support::env_lock().blocking_lock();
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::remove_var("CLERK_ISSUER");
        }
        assert!(McpConfig::from_env().is_none());
        unsafe {
            std::env::remove_var("MCP_ENABLED");
        }
    }

    #[test]
    fn from_env_some_when_flag_and_issuer_set() {
        let _guard = test_support::env_lock().blocking_lock();
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", "https://example.clerk.accounts.dev");
            std::env::set_var(
                "MCP_RESOURCE_URL",
                "https://coralswarm-backend.example/mcp",
            );
        }
        let cfg = McpConfig::from_env().expect("config should resolve");
        assert_eq!(cfg.resource_url, "https://coralswarm-backend.example/mcp");
        assert!(cfg.default_scopes.read && cfg.default_scopes.write);
        // allowed_hosts derives the deployment host from MCP_RESOURCE_URL and
        // keeps the loopback hosts — without the deployment host, rmcp's
        // DNS-rebinding guard 403s every real request.
        assert!(cfg.allowed_hosts.contains(&"coralswarm-backend.example".to_string()));
        assert!(cfg.allowed_hosts.contains(&"localhost".to_string()));
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("MCP_RESOURCE_URL");
        }
    }

    #[test]
    fn from_env_allowed_hosts_override_adds_to_loopback() {
        let _guard = test_support::env_lock().blocking_lock();
        // SAFETY: all env mutation in these from_env tests is serialized on
        // test_support::env_lock(), so no other test reads/writes the process
        // environment concurrently while these vars are set and removed.
        unsafe {
            std::env::set_var("MCP_ENABLED", "1");
            std::env::set_var("CLERK_ISSUER", "https://example.clerk.accounts.dev");
            std::env::set_var("MCP_RESOURCE_URL", "https://a.fly.dev/mcp");
            // Override lists ONLY the custom domain — the deployment authority
            // (a.fly.dev) must still be allowed, or real hosted requests 403.
            std::env::set_var("MCP_ALLOWED_HOSTS", "custom.example.com");
        }
        let cfg = McpConfig::from_env().expect("config should resolve");
        // Loopback + the derived resource authority are ALWAYS retained; the
        // override is added on top (never replaces the derived authority).
        for h in ["localhost", "127.0.0.1", "::1", "a.fly.dev", "custom.example.com"] {
            assert!(cfg.allowed_hosts.contains(&h.to_string()), "missing {h}");
        }
        // SAFETY: same serialization guarantee as above.
        unsafe {
            std::env::remove_var("MCP_ENABLED");
            std::env::remove_var("CLERK_ISSUER");
            std::env::remove_var("MCP_RESOURCE_URL");
            std::env::remove_var("MCP_ALLOWED_HOSTS");
        }
    }

    #[test]
    fn authority_of_extracts_host_and_port() {
        assert_eq!(
            authority_of("https://coralswarm-backend.example/mcp").as_deref(),
            Some("coralswarm-backend.example")
        );
        assert_eq!(
            authority_of("http://localhost:8080/mcp").as_deref(),
            Some("localhost:8080")
        );
        assert_eq!(authority_of("").as_deref(), None);
        assert_eq!(
            authority_of("https://host.example/mcp?x=1#f").as_deref(),
            Some("host.example")
        );
    }
}

/// Test-only support shared across the `mcp` module's sub-module test suites
/// (`mcp::tests`, `mcp::auth::tests`) — specifically a process-wide lock so
/// tests that mutate `MCP_ENABLED`/`CLERK_ISSUER`/etc. don't race each other
/// under `cargo test`'s default in-binary thread-pool parallelism. A
/// `tokio::sync::Mutex` (not `std::sync::Mutex`) because a couple of callers
/// hold the guard across an `.await` (an async HTTP round-trip through the
/// router under test) — holding a `std::sync::Mutex` guard across an await
/// point is a real footgun with multi-threaded executors and correctly
/// clippy-denied (`await_holding_lock`).
#[cfg(any(test, feature = "test-support"))]
pub(crate) mod test_support {
    use std::sync::Arc;

    use rmcp::model::{
        CallToolRequestParams, CallToolResult, ClientJsonRpcMessage, ClientRequest, ErrorData,
        JsonRpcMessage, ListResourceTemplatesResult, ListResourcesResult, ListToolsResult,
        PaginatedRequestParams, ReadResourceRequestParams, ReadResourceResult,
        Request as RmcpRequest, RequestId, RequestOptionalParam, ServerResult,
    };
    use rmcp::service::serve_directly;
    use rmcp::transport::OneshotTransport;
    use rmcp::RoleServer;
    use tokio::sync::Mutex;
    use uuid::Uuid;

    use crate::mcp::auth::McpAuthContext;
    use crate::mcp::server::CoralSwarmMcp;
    use crate::mcp::ScopeSet;
    use crate::middleware::auth::UserContext;

    pub(crate) fn env_lock() -> &'static Mutex<()> {
        static LOCK: Mutex<()> = Mutex::const_new(());
        &LOCK
    }

    /// A fixed [`McpAuthContext`] for tool-dispatch tests: a fresh random
    /// `user_id`/`account_id` (so successive calls in one test are one
    /// consistent identity, and independent tests never collide on rate-limit
    /// bucket keys) and caller-chosen scopes.
    pub(crate) fn test_auth(read: bool, write: bool) -> McpAuthContext {
        McpAuthContext {
            personal_org: false,
            // No consent-screen organization: the in-crate harness mints
            // personal-shaped tokens, which is also the shape every caller
            // who picks their personal row on the consent screen arrives in.
            consent_org: None,
            user: UserContext {
                user_id: Uuid::new_v4(),
                account_id: Uuid::new_v4(),
                role: "owner".to_string(),
                device_id: "mcp:test".to_string(),
            },
            scopes: ScopeSet { read, write },
            client_id: Some("test_client".to_string()),
            token_jti: Some("test_jti".to_string()),
            mcp_client_name: None,
            // No tenancy runtime in the in-crate harness — the flag-off
            // state, which every consumer treats exactly like Personal.
            decision: None,
        }
    }

    /// Build the `http::request::Parts` carrying `auth` in its extensions —
    /// exactly the shape `mcp_auth` (`auth.rs`) inserts into a real request,
    /// and what `tools::auth` (`tools/mod.rs`) extracts back out.
    fn parts_with(auth: McpAuthContext) -> axum::http::request::Parts {
        let (parts, ()) = axum::http::Request::builder()
            .body(())
            .unwrap()
            .into_parts();
        let mut parts = parts;
        parts.extensions.insert(auth);
        parts
    }

    /// Drive one request through the real [`CoralSwarmMcp`] `ServerHandler`
    /// dispatch, bypassing the HTTP layer and `mcp_auth` middleware entirely
    /// (`mcp_auth` needs a live Postgres pool to resolve identity, which the
    /// in-crate unit-test tier deliberately has none of) by injecting a
    /// pre-verified [`McpAuthContext`] directly into the request's
    /// extensions — precisely what `mcp_auth` does for a real request before
    /// handing off to the streamable-HTTP transport.
    ///
    /// Built on `rmcp::transport::OneshotTransport` + `rmcp::service::
    /// serve_directly`, the same public "skip the session handshake"
    /// primitives `StreamableHttpService`'s own stateless-mode dispatch path
    /// uses internally (`tower.rs::handle_post`). This is the closest
    /// available in-process harness for exercising the REAL dispatcher (scope
    /// gate, rate limits, audit hook, tool router) end-to-end:
    /// `rmcp::service::Peer::new` — needed to hand-build a bare
    /// `RequestContext<RoleServer>` directly — is `pub(crate)` to rmcp 2.2.0,
    /// so there is no lighter-weight public construction path available from
    /// outside the rmcp crate.
    async fn dispatch(
        handler: Arc<CoralSwarmMcp>,
        client_request: ClientRequest,
        auth: McpAuthContext,
    ) -> ServerResult {
        dispatch_result(handler, client_request, auth)
            .await
            .unwrap_or_else(|e| panic!("unexpected protocol-level error: {e:?}"))
    }

    /// Same dispatch as [`dispatch`], but surfaces a protocol-level error
    /// instead of panicking on one — needed by callers that must assert on
    /// the error itself (e.g. `read_resource` of an unknown URI, which is a
    /// protocol-level `resource_not_found`, not a `CallToolResult` tool
    /// error like every OTHER tool-body failure this harness's `call_tool`
    /// helper deals with).
    pub(crate) async fn dispatch_result(
        handler: Arc<CoralSwarmMcp>,
        client_request: ClientRequest,
        auth: McpAuthContext,
    ) -> Result<ServerResult, ErrorData> {
        let parts = parts_with(auth);
        let client_request = match client_request {
            ClientRequest::CallToolRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::CallToolRequest(r)
            }
            ClientRequest::GetTaskRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::GetTaskRequest(r)
            }
            ClientRequest::ListToolsRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::ListToolsRequest(r)
            }
            ClientRequest::ListResourcesRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::ListResourcesRequest(r)
            }
            ClientRequest::ReadResourceRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::ReadResourceRequest(r)
            }
            ClientRequest::ListResourceTemplatesRequest(mut r) => {
                r.extensions.insert(parts);
                ClientRequest::ListResourceTemplatesRequest(r)
            }
            other => other,
        };
        let msg = ClientJsonRpcMessage::request(client_request, RequestId::Number(1));
        let (transport, mut receiver) = OneshotTransport::<RoleServer>::new(msg);
        let running = serve_directly(handler, transport, None);
        let response = receiver
            .recv()
            .await
            .expect("dispatcher produced no response message");
        let _ = running.waiting().await;
        match response {
            JsonRpcMessage::Response(r) => Ok(r.result),
            JsonRpcMessage::Error(e) => Err(e.error),
            other => panic!("unexpected message shape from dispatcher: {other:?}"),
        }
    }

    /// An [`McpAuthContext`] for a `user_id`/`account_id` pair. `write` sets
    /// the scope so both read- and write-tool callers can share this one
    /// helper.
    pub(crate) fn auth_for(user_id: Uuid, account_id: Uuid, write: bool) -> McpAuthContext {
        let mut a = test_auth(true, write);
        a.user.user_id = user_id;
        a.user.account_id = account_id;
        a
    }

    /// Extract `error.code` from a tool result's text content, for asserting
    /// on the specific error code a denied/failed call returned.
    pub(crate) fn tool_error_code(r: &CallToolResult) -> String {
        let text = r
            .content
            .first()
            .and_then(|c| c.as_text())
            .expect("tool result has text content");
        let body: serde_json::Value =
            serde_json::from_str(&text.text).expect("tool result text is JSON");
        body["error"]["code"].as_str().unwrap_or_default().to_string()
    }

    /// Call a tool by name through the real dispatcher (see [`dispatch`]).
    pub(crate) async fn call_tool(
        handler: Arc<CoralSwarmMcp>,
        name: &str,
        arguments: serde_json::Value,
        auth: McpAuthContext,
    ) -> CallToolResult {
        let args = match arguments {
            serde_json::Value::Object(m) => Some(m),
            serde_json::Value::Null => None,
            other => panic!("tool arguments must be a JSON object or null, got {other:?}"),
        };
        let mut params = CallToolRequestParams::new(name.to_string());
        if let Some(a) = args {
            params = params.with_arguments(a);
        }
        let request = ClientRequest::CallToolRequest(RmcpRequest::new(params));
        match dispatch(handler, request, auth).await {
            ServerResult::CallToolResult(r) => r,
            other => panic!("expected CallToolResult, got {other:?}"),
        }
    }

    /// List tools through the real dispatcher (see [`dispatch`]).
    pub(crate) async fn list_tools(handler: Arc<CoralSwarmMcp>, auth: McpAuthContext) -> ListToolsResult {
        let request = ClientRequest::ListToolsRequest(RequestOptionalParam::<_, PaginatedRequestParams> {
            method: Default::default(),
            params: None,
            extensions: Default::default(),
        });
        match dispatch(handler, request, auth).await {
            ServerResult::ListToolsResult(r) => r,
            other => panic!("expected ListToolsResult, got {other:?}"),
        }
    }

    /// List resources through the real dispatcher (see [`dispatch`]).
    pub(crate) async fn list_resources(
        handler: Arc<CoralSwarmMcp>,
        auth: McpAuthContext,
    ) -> ListResourcesResult {
        let request =
            ClientRequest::ListResourcesRequest(RequestOptionalParam::<_, PaginatedRequestParams> {
                method: Default::default(),
                params: None,
                extensions: Default::default(),
            });
        match dispatch(handler, request, auth).await {
            ServerResult::ListResourcesResult(r) => r,
            other => panic!("expected ListResourcesResult, got {other:?}"),
        }
    }

    /// List resource templates through the real dispatcher (see [`dispatch`]).
    pub(crate) async fn list_resource_templates(
        handler: Arc<CoralSwarmMcp>,
        auth: McpAuthContext,
    ) -> ListResourceTemplatesResult {
        let request = ClientRequest::ListResourceTemplatesRequest(RequestOptionalParam::<
            _,
            PaginatedRequestParams,
        > {
            method: Default::default(),
            params: None,
            extensions: Default::default(),
        });
        match dispatch(handler, request, auth).await {
            ServerResult::ListResourceTemplatesResult(r) => r,
            other => panic!("expected ListResourceTemplatesResult, got {other:?}"),
        }
    }

    /// Read a resource by URI through the real dispatcher — returns the
    /// protocol-level `Err` rather than panicking (see [`dispatch_result`]),
    /// since an unknown URI's `resource_not_found` IS the outcome some
    /// callers assert on.
    pub(crate) async fn read_resource(
        handler: Arc<CoralSwarmMcp>,
        uri: &str,
        auth: McpAuthContext,
    ) -> Result<ReadResourceResult, ErrorData> {
        let request = ClientRequest::ReadResourceRequest(RmcpRequest::new(
            ReadResourceRequestParams::new(uri.to_string()),
        ));
        match dispatch_result(handler, request, auth).await? {
            ServerResult::ReadResourceResult(r) => Ok(r),
            other => panic!("expected ReadResourceResult, got {other:?}"),
        }
    }
}
