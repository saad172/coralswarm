//! RFC 9728 protected-resource metadata + a cached mirror of Clerk's
//! authorization-server metadata (docs/plans/remote-mcp-v1.md §2.4).
//!
//! `rmcp` ships no server-side RFC 9728 support, so these are plain Axum
//! handlers. All three routes are mounted only when [`super::McpConfig`]
//! resolves (see `super::mcp_router`), so the flag gates discovery too — a
//! client that somehow already knew the URLs still gets a 404 with MCP off.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use axum::{
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Json,
};
use serde_json::{json, Value};
use tokio::sync::RwLock;

use super::McpState;

/// How long the Clerk AS-metadata mirror is cached before a refetch. Matches
/// the order of magnitude of `clerk::JWKS_TTL` — this document changes about
/// as often as signing keys do (essentially never in normal operation).
const AS_METADATA_TTL: Duration = Duration::from_secs(600);

/// Security review L3: when there is no last-known-good copy at all (cold
/// cache, e.g. right after a restart) AND Clerk is unreachable, suppress
/// repeated outbound fetches for this long before trying again — otherwise
/// every unauthenticated `GET` during a Clerk outage triggers a fresh request.
const AS_METADATA_NEGATIVE_TTL: Duration = Duration::from_secs(30);

/// Per-request timeout for the Clerk AS-metadata fetch specifically. Shorter
/// than `state.http_client`'s general-purpose (LLM-sized, `LLM_TIMEOUT_SECS`,
/// default 60s) timeout — this is a small, latency-sensitive discovery
/// document, not an LLM call, and a slow/hanging Clerk endpoint shouldn't be
/// able to hold a well-known request open anywhere near that long.
const AS_METADATA_FETCH_TIMEOUT: Duration = Duration::from_secs(5);

pub(crate) fn router() -> axum::Router<McpState> {
    axum::Router::new()
        .route(
            "/.well-known/oauth-protected-resource/mcp",
            get(protected_resource_metadata),
        )
        .route(
            "/.well-known/oauth-protected-resource",
            get(protected_resource_metadata),
        )
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server_metadata),
        )
}

/// Attach the metadata-endpoint-only headers: explicit
/// `Access-Control-Allow-Origin: *` (these documents are meant to be fetched
/// cross-origin by any OAuth client discovering this resource server) plus a
/// short `Cache-Control`.
///
/// The TTL is deliberately short (5 min): these documents change rarely, but
/// when they DO (e.g. the advertised `scopes_supported` fix), a long-lived
/// cache pins clients to the stale document. A client that fetched the old PRM
/// under `max-age=3600` kept sending the pre-fix `coralswarm:*` scopes for up
/// to an hour and its Dynamic Client Registration kept failing with Clerk's
/// `invalid_client_metadata` long after the server was corrected. 5 minutes
/// keeps discovery cheap while letting a metadata correction propagate fast.
fn with_metadata_headers(mut resp: Response) -> Response {
    resp.headers_mut().insert(
        header::ACCESS_CONTROL_ALLOW_ORIGIN,
        HeaderValue::from_static("*"),
    );
    resp.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("max-age=300"),
    );
    resp
}

/// `GET /.well-known/oauth-protected-resource[/mcp]` — RFC 9728 protected
/// resource metadata. Identical body at both paths (some clients only check
/// the bare well-known path, others append the resource path per RFC 9728's
/// path-insertion convention). `resource` is byte-exact — clients compare it as
/// an opaque string against the resource identifier they used to reach this
/// server — so it is `MCP_RESOURCE_URL` verbatim, for every Host: since
/// claim-based-tenancy §Phase 3 PR 3.3, there is only ever the one resource
/// (`McpConfig::resolve_resource`'s doc — the org MCP resource is retired).
///
/// `resource`/`scopes_supported` are still keyed off `McpConfig::
/// resolve_resource`'s output rather than hardcoded here, but that function
/// now returns [`super::MCP_SCOPES_PERSONAL`] for every classifiable Host —
/// there is no more org-subdomain arm advertising `user:org:read`. Before PR
/// 3.3 this document was genuinely PER-HOST: the apex advertised
/// `MCP_SCOPES_PERSONAL` and `<slug>.<apex>` advertised a second scope set
/// (`user:org:read` added) so Clerk would render its org selector. See that
/// PR's history for the shape this replaced.
///
/// A Host we do not serve falls back to the CONFIGURED resource rather than
/// erroring: this document is public and non-authoritative, and the fallback
/// (personal, configured URL — never the request's own Host) is what keeps an
/// arbitrary Host out of a metadata document. `/mcp` itself still rejects that
/// same Host at `auth::host_guard`.
async fn protected_resource_metadata(
    axum::extract::State(mcp): axum::extract::State<McpState>,
    req: axum::extract::Request,
) -> Response {
    // Both derivations are the guard's own (`auth::request_authority` /
    // `auth::tenant_apex`), not local copies: `Host` OR the h2 `:authority`, so a
    // client that discovers a resource here is authorized against that same
    // resource at `/mcp`. This mattered most when `<slug>.<apex>` was a
    // DISTINCT resource with its own `resource` string and its own scope set:
    // reading only `Host` handed every h2 caller the apex document instead,
    // its byte-exact resource check failed, and the follow-up `/mcp` call was
    // rejected for a claim/host mismatch. There is one resource now (and one
    // scope set, `user:org:read` included — see `super::MCP_SCOPES_PERSONAL`),
    // so the two derivations agree on every Host; they stay shared with the
    // guard so a future divergence cannot appear on one side only.
    let host = super::auth::request_authority(&req);
    let apex = super::auth::tenant_apex(&mcp.app);
    let res = mcp
        .cfg
        .resolve_resource(host.as_deref(), apex.as_deref())
        .unwrap_or_else(|| mcp.cfg.personal_identity());

    // `jwks_uri` deliberately omitted (MCP listing-prep item 6): it is not a
    // required PRM field (RFC 9728 §2), and this server derived it from
    // `CLERK_JWKS_URL` — which prod overrides to Clerk's own
    // `https://api.clerk.com/v1/jwks`, an AUTH-REQUIRED endpoint. A client
    // that resolves and fetches this "convenience" field 401s, which is
    // strictly worse than not offering it at all: every field this document
    // DOES need (`authorization_servers`) already points a compliant client
    // at the real authorization-server metadata, which carries its OWN
    // `jwks_uri` (see `authorization_server_metadata` below) — the correct
    // and already-working path to the actual signing keys.
    let body = json!({
        "resource": res.resource_url,
        "authorization_servers": [mcp.cfg.clerk.issuer],
        "scopes_supported": res.scopes.split_whitespace().collect::<Vec<_>>(),
        "bearer_methods_supported": ["header"],
    });
    with_metadata_headers((StatusCode::OK, Json(body)).into_response())
}

struct CachedAsMetadata {
    body: Value,
    fetched: Instant,
}

/// Caching/resilience policy for the Clerk AS-metadata mirror (security
/// review L3), factored out of the HTTP-handling code entirely so it's
/// testable without a real network call: the `resolve` method takes the
/// actual fetch as an injected closure, which tests replace with an in-memory
/// stub (see `tests::` below) and production wires to
/// [`fetch_as_metadata_from_clerk`].
///
/// Entries are keyed by the upstream issuer (`cfg.clerk.issuer`) passed to
/// `resolve`. A production process only ever serves one issuer, so this is a
/// no-op there — a `HashMap` with a single key. What it buys instead is
/// structural test isolation: [`shared_as_metadata_cache`] is a process-wide
/// `OnceLock` singleton shared by every test in this binary that exercises
/// [`fetch_as_metadata`] through the real HTTP router, so without an issuer
/// key, one test's cached document could be handed to a different test that
/// happens to run afterward in the same process. Keying by issuer makes that
/// impossible by construction, as long as each such test uses its own unique
/// `cfg.clerk.issuer` (every test that does this today binds a fresh
/// ephemeral local listener for it — see
/// `tests::as_metadata_mirror_preserves_upstream_issuer_and_fields_exactly`).
///
/// Policy (per key):
/// 1. Fast path, no lock: cache is fresh (`< ttl` old) — return it.
/// 2. Otherwise take the single-flight `refresh_lock` (shared across all
///    keys — harmless given production only ever has one), then re-check the
///    cache (another caller may have refreshed — or negative-cached — while
///    we waited): still fresh -> return it. This is what prevents a stampede
///    of concurrent callers each independently fetching on a cold cache.
/// 3. If there is no last-known-good copy AT ALL for this key and the last
///    attempt for this key failed within `negative_ttl`, fail fast without a
///    network call.
/// 4. Otherwise actually fetch. On success, cache it under this key and clear
///    any negative cache for this key. On failure: serve the stale
///    last-known-good copy for this key if one exists (log a warning, don't
///    fail the request) — "stale-serve"; if there is truly no last-known-good
///    copy for this key, record the negative cache and return the error.
struct AsMetadataCache {
    cache: RwLock<HashMap<String, CachedAsMetadata>>,
    last_failure: RwLock<HashMap<String, Instant>>,
    refresh_lock: tokio::sync::Mutex<()>,
}

impl AsMetadataCache {
    fn new() -> Self {
        Self {
            cache: RwLock::new(HashMap::new()),
            last_failure: RwLock::new(HashMap::new()),
            refresh_lock: tokio::sync::Mutex::new(()),
        }
    }

    async fn resolve<F, Fut>(
        &self,
        key: &str,
        ttl: Duration,
        negative_ttl: Duration,
        fetch: F,
    ) -> Result<Value, String>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<Value, String>>,
    {
        if let Some(cached) = self.cache.read().await.get(key) {
            if cached.fetched.elapsed() < ttl {
                return Ok(cached.body.clone());
            }
        }

        let _refresh_guard = self.refresh_lock.lock().await;

        // Re-check now that we hold the refresh lock — a concurrent caller
        // may have already refreshed (or negative-cached) while we waited.
        if let Some(cached) = self.cache.read().await.get(key) {
            if cached.fetched.elapsed() < ttl {
                return Ok(cached.body.clone());
            }
        }
        let has_last_known_good = self.cache.read().await.contains_key(key);
        if !has_last_known_good {
            if let Some(failed_at) = self.last_failure.read().await.get(key) {
                if failed_at.elapsed() < negative_ttl {
                    return Err(
                        "authorization server metadata unavailable (negative-cached after a recent failed fetch)"
                            .to_string(),
                    );
                }
            }
        }

        match fetch().await {
            Ok(body) => {
                self.cache.write().await.insert(
                    key.to_string(),
                    CachedAsMetadata {
                        body: body.clone(),
                        fetched: Instant::now(),
                    },
                );
                self.last_failure.write().await.remove(key);
                Ok(body)
            }
            Err(e) => {
                if let Some(stale) = self.cache.read().await.get(key) {
                    tracing::warn!(
                        error = %e,
                        stale_age_secs = stale.fetched.elapsed().as_secs(),
                        "Clerk AS-metadata refresh failed; serving last-known-good copy"
                    );
                    return Ok(stale.body.clone());
                }
                self.last_failure
                    .write()
                    .await
                    .insert(key.to_string(), Instant::now());
                Err(e)
            }
        }
    }
}

/// The process-wide singleton used by [`fetch_as_metadata`] in production.
fn shared_as_metadata_cache() -> &'static AsMetadataCache {
    static CACHE: OnceLock<AsMetadataCache> = OnceLock::new();
    CACHE.get_or_init(AsMetadataCache::new)
}

/// `GET /.well-known/oauth-authorization-server` — a cached mirror of Clerk's
/// own AS metadata, for older-spec clients that expect to find it on the
/// resource server's origin rather than following `authorization_servers` in
/// the PRM document to Clerk's origin directly.
async fn authorization_server_metadata(
    axum::extract::State(mcp): axum::extract::State<McpState>,
) -> Response {
    match fetch_as_metadata(&mcp).await {
        Ok(body) => with_metadata_headers((StatusCode::OK, Json(body)).into_response()),
        Err(e) => {
            tracing::warn!(error = %e, "failed to fetch/refresh Clerk authorization-server metadata");
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "authorization server metadata unavailable"})),
            )
                .into_response()
        }
    }
}

async fn fetch_as_metadata(mcp: &McpState) -> Result<Value, String> {
    shared_as_metadata_cache()
        .resolve(
            &mcp.cfg.clerk.issuer,
            AS_METADATA_TTL,
            AS_METADATA_NEGATIVE_TTL,
            || fetch_as_metadata_from_clerk(mcp),
        )
        .await
}

/// The actual outbound fetch, with its own bounded timeout
/// (`AS_METADATA_FETCH_TIMEOUT`) rather than relying solely on
/// `state.http_client`'s general-purpose (LLM-sized) default.
async fn fetch_as_metadata_from_clerk(mcp: &McpState) -> Result<Value, String> {
    let url = format!(
        "{}/.well-known/oauth-authorization-server",
        mcp.cfg.clerk.issuer
    );
    mcp.app
        .http_client
        .get(&url)
        .timeout(AS_METADATA_FETCH_TIMEOUT)
        .send()
        .await
        .map_err(|e| e.to_string())?
        .error_for_status()
        .map_err(|e| e.to_string())?
        .json()
        .await
        .map_err(|e| e.to_string())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clerk::test_support::ISSUER;
    use crate::mcp::{mcp_router_with_validator, McpConfig, ScopeSet};
    use crate::test_app_state;
    use axum::body::{to_bytes, Body};
    use axum::http::Request as HttpRequest;
    use tower::ServiceExt;

    fn test_cfg() -> McpConfig {
        McpConfig {
            enabled: true,
            resource_url: "https://coralswarm-backend.example/mcp".to_string(),
            default_scopes: ScopeSet {
                read: true,
                write: true,
            },
            allowed_origins: vec![],
            allowed_hosts: vec!["coralswarm-backend.example".to_string()],
            clerk: crate::clerk::ClerkConfig {
                issuer: ISSUER.to_string(),
                jwks_url: format!("{ISSUER}/.well-known/jwks.json"),
            },
        }
    }

    struct AlwaysNotJwt;
    #[async_trait::async_trait]
    impl super::super::auth::TokenValidator for AlwaysNotJwt {
        async fn validate(
            &self,
            _token: &str,
        ) -> Result<crate::clerk::ClerkClaims, crate::clerk::ClerkError> {
            Err(crate::clerk::ClerkError::NotJwt)
        }
    }

    fn router() -> axum::Router {
        mcp_router_with_validator(test_app_state(), test_cfg(), std::sync::Arc::new(AlwaysNotJwt))
    }

    async fn body_json(resp: Response) -> serde_json::Value {
        let bytes = to_bytes(resp.into_body(), 16 * 1024).await.unwrap();
        serde_json::from_slice(&bytes).unwrap()
    }

    /// An app state whose tenancy runtime is enabled with `apex`, so
    /// `resolve_resource` can classify org subdomains (the default
    /// `test_app_state()` has no runtime at all, i.e. the flag-off shape). No
    /// database is involved — classification is pure.
    fn router_with_apex(apex: &str) -> axum::Router {
        let app = crate::test_app_state_with_tenant_runtime(Some(
            crate::tenancy::runtime::TenantRuntime::new(
                true,
                apex,
                std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(
                    std::sync::Arc::new(crate::tenancy::secrets::StaticMapResolver::new(
                        Default::default(),
                    )),
                )),
            ),
        ));
        let mut cfg = test_cfg();
        cfg.resource_url = format!("https://{apex}/mcp");
        cfg.allowed_hosts = vec![apex.to_string()];
        mcp_router_with_validator(app, cfg, std::sync::Arc::new(AlwaysNotJwt))
    }

    async fn prm_for_host(apex: &str, host: &str) -> serde_json::Value {
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .header(header::HOST, host)
            .body(Body::empty())
            .unwrap();
        let resp = router_with_apex(apex).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        body_json(resp).await
    }

    /// Claim-based-tenancy §Phase 3 PR 3.3: the PRM is no longer per-host.
    /// This replaces `prm_is_per_host_for_apex_and_org_subdomain` (pre-PR-3.3:
    /// asserted the OPPOSITE — an org subdomain's document differed, carrying
    /// `user:org:read` and its own reflected `resource`) with the new truth:
    /// the apex and a former org subdomain now produce the byte-IDENTICAL
    /// document.
    #[tokio::test]
    async fn prm_no_longer_differs_between_apex_and_org_subdomain() {
        let apex = "api.example.com";

        let personal = prm_for_host(apex, apex).await;
        assert_eq!(personal["resource"], "https://api.example.com/mcp");
        assert_eq!(
            personal["scopes_supported"],
            json!(["openid", "profile", "email", "offline_access", "user:org:read"])
        );

        let subdomain = prm_for_host(apex, "testorg.api.example.com").await;
        assert_eq!(
            subdomain, personal,
            "a (former org) subdomain must now get the byte-identical apex document"
        );
    }

    /// HTTP/2 carries the authority in `:authority`, not `Host`. This path
    /// still needs covering post-retirement (claim-based-tenancy §Phase 3 PR
    /// 3.3): an h2 caller with no `Host` header must still resolve
    /// successfully (the `:authority` fallback still runs — a regression here
    /// would 500, not merely mis-scope), and naming a former org subdomain
    /// there no longer gets it anything special: no `user:org:read`, and
    /// `resource` names the CONFIGURED apex, never the `:authority` the
    /// caller actually sent. Every other test here builds HTTP/1.1 requests
    /// with an explicit `Host`, so this is the only one that covers the h2
    /// path at all.
    #[tokio::test]
    async fn prm_uses_the_h2_authority_when_no_host_header_is_present() {
        let apex = "api.example.com";
        let req = HttpRequest::builder()
            .uri("https://testorg.api.example.com/.well-known/oauth-protected-resource/mcp")
            .body(Body::empty())
            .unwrap();
        assert!(
            req.headers().get(header::HOST).is_none(),
            "fixture must exercise the :authority path, not the Host header"
        );
        let resp = router_with_apex(apex).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let doc = body_json(resp).await;
        assert_eq!(doc["resource"], "https://api.example.com/mcp");
        assert!(
            !doc.to_string().contains("testorg"),
            "the :authority's subdomain must never be reflected: {doc:?}"
        );
        assert_eq!(
            doc["scopes_supported"],
            json!(["openid", "profile", "email", "offline_access", "user:org:read"])
        );
    }

    /// A Host we do not serve must never be reflected into a metadata document;
    /// it falls back to the configured resource, on the personal plane.
    #[tokio::test]
    async fn prm_never_reflects_a_foreign_host() {
        let doc = prm_for_host("api.example.com", "evil.example").await;
        assert_eq!(doc["resource"], "https://api.example.com/mcp");
        assert!(!doc.to_string().contains("evil.example"));
        assert_eq!(
            doc["scopes_supported"],
            json!(["openid", "profile", "email", "offline_access", "user:org:read"])
        );
    }

    #[tokio::test]
    async fn prm_document_is_byte_identical_at_both_paths() {
        let router = router();

        let req_a = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource")
            .body(Body::empty())
            .unwrap();
        let resp_a = router.clone().oneshot(req_a).await.unwrap();
        assert_eq!(resp_a.status(), StatusCode::OK);
        let bytes_a = to_bytes(resp_a.into_body(), 16 * 1024).await.unwrap();

        let req_b = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .body(Body::empty())
            .unwrap();
        let resp_b = router.oneshot(req_b).await.unwrap();
        assert_eq!(resp_b.status(), StatusCode::OK);
        let bytes_b = to_bytes(resp_b.into_body(), 16 * 1024).await.unwrap();

        assert_eq!(bytes_a, bytes_b, "PRM body must be byte-identical at both paths");
    }

    #[tokio::test]
    async fn prm_document_has_expected_fields() {
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .body(Body::empty())
            .unwrap();
        let resp = router().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get(header::ACCESS_CONTROL_ALLOW_ORIGIN).unwrap(),
            "*"
        );
        let body = body_json(resp).await;
        assert_eq!(body["resource"], "https://coralswarm-backend.example/mcp");
        assert_eq!(body["authorization_servers"][0], ISSUER);
        // Advertised scopes MUST be Clerk-grantable (not the product's
        // coralswarm:* scopes, which break DCR) — see MCP_SCOPES_PERSONAL.
        // Assert the full array so dropping a scope (e.g. offline_access → no
        // refresh token) or ADDING one fails CI. `user:org:read` must NOT be
        // here — dropped entirely in claim-based-tenancy §Phase 3 PR 3.3
        // (`MCP_SCOPES_PERSONAL`'s doc): advertising it ever again would put
        // an `org_id` claim on a token, which is precisely what made the
        // personal ocean unreachable for org members before the resource
        // split, and precisely why this PR removes the split instead.
        assert_eq!(
            body["scopes_supported"],
            json!(["openid", "profile", "email", "offline_access", "user:org:read"])
        );
        assert_eq!(body["bearer_methods_supported"][0], "header");
    }

    /// L2's `security_headers` layer is scoped to the `/mcp` endpoint only
    /// (`mod.rs`'s `mcp_endpoint` router) — it must NOT leak onto the
    /// well-known metadata endpoints and override their own caching headers.
    #[tokio::test]
    async fn well_known_headers_are_unaffected_by_the_mcp_security_headers_layer() {
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .body(Body::empty())
            .unwrap();
        let resp = router().oneshot(req).await.unwrap();
        assert_eq!(
            resp.headers().get(header::CACHE_CONTROL).unwrap(),
            "max-age=300",
            "well-known metadata must keep its own short Cache-Control, not /mcp's no-store"
        );
        assert!(
            resp.headers().get(header::X_CONTENT_TYPE_OPTIONS).is_none(),
            "the /mcp-only security_headers layer must not apply to well-known routes"
        );
    }

    #[tokio::test]
    async fn resource_url_is_byte_exact_no_trailing_slash() {
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource")
            .body(Body::empty())
            .unwrap();
        let resp = router().oneshot(req).await.unwrap();
        let body = body_json(resp).await;
        let resource = body["resource"].as_str().unwrap();
        assert!(!resource.ends_with('/'));
        assert_eq!(resource, "https://coralswarm-backend.example/mcp");
    }

    // ── Grant-longevity byte-stability (SEP-2352) ─────────────────────────────
    //
    // Clients key OAuth credentials by (among other things) the resource
    // server's `issuer` and the RFC 9728 protected-resource `resource`
    // string — see the handoff note and SEP-2352 ("bind dynamic client
    // registration credentials to the issuing authorization server"). A
    // byte-level change to either (e.g. a trailing slash added or dropped, a
    // scheme downgrade, a field reordered by a future refactor that swaps
    // `json!` for a hand-built `Value`) silently invalidates every
    // already-issued grant and forces a GLOBAL re-auth of every connected
    // client. These two tests assert the FULL serialized response body
    // against a hardcoded literal, not just the individual fields already
    // covered above — any drift, even one that leaves `resource` and
    // `authorization_servers` untouched (e.g. a key rename, an added field,
    // whitespace), fails loudly here instead of shipping unnoticed. The
    // literal was captured from a real run of this handler (`serde_json`
    // has no `preserve_order` feature enabled in this workspace — see
    // `Cargo.lock` — so `Map` is a `BTreeMap` and keys serialize in
    // alphabetical order, not `json!`'s source-literal order); if a FUTURE
    // rmcp/serde_json/field change legitimately alters this byte sequence,
    // update the literal deliberately — don't relax the test to a
    // structural/field-subset comparison, which is exactly what would let a
    // future PR quietly break the byte-stability guarantee itself.
    #[tokio::test]
    async fn apex_prm_body_is_byte_frozen_for_grant_longevity() {
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .body(Body::empty())
            .unwrap();
        let resp = router().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 16 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).expect("PRM body must be valid UTF-8");
        assert_eq!(
            body,
            "{\"authorization_servers\":[\"https://example.clerk.accounts.dev\"],\
             \"bearer_methods_supported\":[\"header\"],\
             \"resource\":\"https://coralswarm-backend.example/mcp\",\
             \"scopes_supported\":[\"openid\",\"profile\",\"email\",\"offline_access\",\"user:org:read\"]}",
            "FROZEN (SEP-2352): the apex PRM document's exact bytes are what every \
             personal-plane client's stored grant is keyed against — see this test's \
             doc comment before changing the literal"
        );
    }

    /// **The SEP-2352 freeze on the org-plane document is LIFTED here,
    /// DELIBERATELY, in claim-based-tenancy §Phase 3 PR 3.3** — this test is
    /// kept, not deleted, so the NEW truth stays pinned the same way the old
    /// one was (per this PR's own instructions: don't delete a frozen test,
    /// make it assert the new truth).
    ///
    /// Before this PR: `resource` reflected the org subdomain byte-exact
    /// against the Host the client used to discover it, and
    /// `scopes_supported` carried an extra `user:org:read` entry that made
    /// Clerk render its org selector — see this function's git history for
    /// that literal.
    ///
    /// **Why the freeze is safe to break now:** SEP-2352 exists so an
    /// already-issued grant keeps resolving against byte-identical PRM bytes.
    /// Dropping `user:org:read` (item 1 of this PR) changes `scopes_supported`
    /// for real, and retiring the `Subdomain` resource arm (item 2) removes
    /// this document's distinct `resource` entirely — both are intentional
    /// breaks of the freeze, not regressions, for the same reason the rest of
    /// this PR is safe to ship at all: there are exactly TWO live org-plane
    /// MCP clients in existence (the founder and a co-founder, per the plan's
    /// correction 1), and BOTH reconnect against the apex address as part of
    /// this same change — see the PR description for the exact, literal
    /// reconnect steps. No other stored grant is keyed against the bytes this
    /// test used to freeze.
    ///
    /// The new truth: an org-subdomain PRM request returns the
    /// byte-IDENTICAL apex document, with `resource` naming the CONFIGURED
    /// apex and never the subdomain the caller's Host carried.
    ///
    /// **This doc said "no `user:org:read`" until #825 put that scope back**
    /// (see `super::super::MCP_SCOPES_PERSONAL`) — the literal below carries
    /// it, and has since #825, so the sentence was describing a document that
    /// no longer existed. The freeze itself was broken a SECOND time by that
    /// PR, deliberately and for the same reason: changing `scopes_supported`
    /// invalidates every stored grant and forces every client to reconnect,
    /// which is precisely how the org picker got back in front of users.
    #[tokio::test]
    async fn org_subdomain_prm_body_is_byte_frozen_for_grant_longevity() {
        let apex = "api.example.com";
        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-protected-resource/mcp")
            .header(header::HOST, "testorg.api.example.com")
            .body(Body::empty())
            .unwrap();
        let resp = router_with_apex(apex).oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = to_bytes(resp.into_body(), 16 * 1024).await.unwrap();
        let body = String::from_utf8(bytes.to_vec()).expect("PRM body must be valid UTF-8");
        assert_eq!(
            body,
            "{\"authorization_servers\":[\"https://example.clerk.accounts.dev\"],\
             \"bearer_methods_supported\":[\"header\"],\
             \"resource\":\"https://api.example.com/mcp\",\
             \"scopes_supported\":[\"openid\",\"profile\",\"email\",\"offline_access\",\"user:org:read\"]}",
            "an org-subdomain PRM request must now return the byte-identical PERSONAL \
             document — see this test's doc comment for why the SEP-2352 freeze was \
             deliberately lifted in this PR"
        );
    }

    /// The AS-metadata mirror (`/.well-known/oauth-authorization-server`) is
    /// a pass-through of CLERK's own document, not one we author — so there
    /// is no fixed literal to freeze the way the two PRM tests above do.
    /// What grant longevity actually needs from this endpoint is narrower
    /// and just as important: the `issuer` string our mirror serves must be
    /// EXACTLY what the upstream published (SEP-2468: clients validate the
    /// discovered AS metadata's `issuer` against the one they expect), and
    /// the mirror must not silently drop or corrupt fields in transit.
    /// `fetch_as_metadata_from_clerk` round-trips the upstream body through
    /// `reqwest::Response::json::<Value>()` (a parse, not a byte copy), so a
    /// raw byte-diff against upstream would be the WRONG test — the parse
    /// step is expected to (and, per this test, does) normalize key order
    /// without touching any value. Structural (order-independent) equality
    /// against the exact upstream fixture is therefore the correct
    /// longevity pin here; the hardcoded `issuer` literal is asserted
    /// separately so a future refactor that reshapes the fixture can't
    /// silently stop testing it.
    ///
    /// This is the one test in this module that drives `fetch_as_metadata`
    /// through the real router, which means it goes through the process-wide
    /// `shared_as_metadata_cache()` singleton rather than a fresh
    /// `AsMetadataCache` like the L3 tests below. That's made safe by
    /// `AsMetadataCache` keying entries by issuer (see its doc comment): this
    /// test's `cfg.clerk.issuer` is a fresh ephemeral `http://127.0.0.1:PORT`
    /// bound below, unique to this test run, so no other test's cached
    /// document can be returned here and this test's own document can't leak
    /// into another test either.
    #[tokio::test]
    async fn as_metadata_mirror_preserves_upstream_issuer_and_fields_exactly() {
        // Deliberately NOT alphabetically ordered and NOT matching this
        // crate's `ISSUER` test constant byte-for-byte in shape (includes
        // extra fields a real AS document carries) — proves the mirror
        // doesn't accidentally rely on some coincidental ordering/shape
        // match with our own fixtures.
        let upstream_body = json!({
            "issuer": "https://frozen-issuer.example.clerk.accounts.dev",
            "token_endpoint": "https://frozen-issuer.example.clerk.accounts.dev/oauth/token",
            "authorization_endpoint": "https://frozen-issuer.example.clerk.accounts.dev/oauth/authorize",
            "registration_endpoint": "https://frozen-issuer.example.clerk.accounts.dev/oauth/register",
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind local AS-metadata listener");
        let addr = listener.local_addr().unwrap();
        let upstream_for_route = upstream_body.clone();
        let upstream_app = axum::Router::new().route(
            "/.well-known/oauth-authorization-server",
            axum::routing::get(move || {
                let body = upstream_for_route.clone();
                async move { Json(body) }
            }),
        );
        tokio::spawn(async move {
            axum::serve(listener, upstream_app).await.ok();
        });

        let mut cfg = test_cfg();
        cfg.clerk.issuer = format!("http://{addr}");
        let app = mcp_router_with_validator(
            crate::test_app_state(),
            cfg,
            std::sync::Arc::new(AlwaysNotJwt),
        );

        let req = HttpRequest::builder()
            .uri("/.well-known/oauth-authorization-server")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let mirrored = body_json(resp).await;

        // The hardcoded literal: this is what SEP-2468 clients actually
        // validate against, and what a future refactor could most easily
        // corrupt (e.g. reading the wrong response field, or a copy/paste
        // of a different fixture's issuer).
        assert_eq!(
            mirrored["issuer"], "https://frozen-issuer.example.clerk.accounts.dev",
            "FROZEN: the mirrored issuer must be byte-exact with what Clerk published"
        );
        // Full structural fidelity: nothing dropped, nothing added, no value
        // altered — order-independent because the mirror necessarily
        // re-serializes (see doc comment above).
        assert_eq!(
            mirrored, upstream_body,
            "the AS-metadata mirror must reproduce the upstream document exactly"
        );
    }

    // ── L3: AS-metadata mirror resilience ─────────────────────────────────────
    //
    // These exercise `AsMetadataCache` directly, each against its OWN fresh
    // instance (never the process-wide `shared_as_metadata_cache()` singleton)
    // with an injected in-memory fetch closure — no real network call, and no
    // shared state to leak between tests or across the rest of the test binary.

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn stale_last_known_good_is_served_when_refresh_fails() {
        let cache = AsMetadataCache::new();
        let key = "https://good.example";
        // Seed a last-known-good copy that is already past its TTL (a real
        // successful fetch long enough ago that the next `resolve` call must
        // attempt a refresh) — avoids sleeping in the test.
        cache.cache.write().await.insert(
            key.to_string(),
            CachedAsMetadata {
                body: json!({"issuer": "https://good.example"}),
                fetched: Instant::now() - Duration::from_secs(AS_METADATA_TTL.as_secs() + 1),
            },
        );

        let result = cache
            .resolve(key, AS_METADATA_TTL, AS_METADATA_NEGATIVE_TTL, || async {
                Err::<Value, String>("clerk unreachable".to_string())
            })
            .await;

        assert_eq!(
            result.expect("stale copy should be served instead of erroring")["issuer"],
            "https://good.example"
        );
    }

    #[tokio::test]
    async fn negative_cache_suppresses_repeated_fetch_attempts_when_cold() {
        let cache = AsMetadataCache::new();
        let key = "https://cold.example";
        let calls = Arc::new(AtomicUsize::new(0));
        let negative_ttl = Duration::from_secs(30);

        let c1 = calls.clone();
        let first = cache
            .resolve(key, AS_METADATA_TTL, negative_ttl, move || {
                let c1 = c1.clone();
                async move {
                    c1.fetch_add(1, Ordering::SeqCst);
                    Err::<Value, String>("clerk unreachable".to_string())
                }
            })
            .await;
        assert!(first.is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 1);

        // Cold cache still has no last-known-good and we're still within the
        // negative-cache TTL — a second call must NOT invoke fetch again.
        let c2 = calls.clone();
        let second = cache
            .resolve(key, AS_METADATA_TTL, negative_ttl, move || {
                let c2 = c2.clone();
                async move {
                    c2.fetch_add(1, Ordering::SeqCst);
                    Err::<Value, String>("clerk unreachable".to_string())
                }
            })
            .await;
        assert!(second.is_err());
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "negative cache should have suppressed the second fetch attempt"
        );
    }

    #[tokio::test]
    async fn concurrent_cold_callers_single_flight_to_one_fetch() {
        let cache = Arc::new(AsMetadataCache::new());
        let key = "https://good.example";
        let calls = Arc::new(AtomicUsize::new(0));

        let mut handles = Vec::new();
        for _ in 0..8 {
            let cache = cache.clone();
            let calls = calls.clone();
            handles.push(tokio::spawn(async move {
                cache
                    .resolve(key, AS_METADATA_TTL, AS_METADATA_NEGATIVE_TTL, || async {
                        calls.fetch_add(1, Ordering::SeqCst);
                        // Simulate network latency so the concurrent callers
                        // actually overlap instead of trivially serializing.
                        tokio::time::sleep(Duration::from_millis(20)).await;
                        Ok(json!({"issuer": "https://good.example"}))
                    })
                    .await
            }));
        }

        for handle in handles {
            let result = handle.await.unwrap();
            assert_eq!(result.unwrap()["issuer"], "https://good.example");
        }
        assert_eq!(
            calls.load(Ordering::SeqCst),
            1,
            "only one of the concurrent cold-cache callers should have actually fetched"
        );
    }

    #[tokio::test]
    async fn fresh_cache_never_invokes_fetch() {
        let cache = AsMetadataCache::new();
        let key = "https://good.example";
        cache.cache.write().await.insert(
            key.to_string(),
            CachedAsMetadata {
                body: json!({"issuer": "https://good.example"}),
                fetched: Instant::now(),
            },
        );

        let result = cache
            .resolve(key, AS_METADATA_TTL, AS_METADATA_NEGATIVE_TTL, || async {
                panic!("fetch should not be called when the cache is fresh")
            })
            .await;
        assert_eq!(result.unwrap()["issuer"], "https://good.example");
    }
}
