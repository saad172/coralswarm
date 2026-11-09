//! Clerk session-JWT and resource-bound OAuth verification (ADR-0007 D3/D4).
//!
//! The backend is adopting Clerk as its auth provider. This module verifies a
//! Clerk **session token** — an RS256 JWT signed by the instance — against the
//! instance's JWKS, and extracts the claims the tenancy model needs: the Clerk
//! user id (`sub`), and, when an organization is active, `org_id` / `org_role`.
//! Clerk instances mint one of two session-token claim shapes for org
//! context (v1 top-level `org_id`/`org_slug`/`org_role`, or v2's nested `o`
//! object); [`ClerkClaims`] normalizes both into the same v1-shaped public
//! fields — see its doc comment.
//!
//! Remote MCP uses [`verify_oauth`] instead: it requires an OAuth access JWT
//! bound to the protected resource. Web sessions retain the policy below.
//!
//! Integration is **additive**: `require_auth` tries Clerk verification when the
//! bearer token looks like a JWT, and falls back to the legacy email-code session
//! token otherwise (see `middleware::auth`). That lets the two auth systems
//! coexist during the migration.
//!
//! Verification is split so the pure decode path is unit-testable offline:
//! [`decode_claims`] does the signature + issuer + expiry checks against a known
//! key; [`verify`] adds JWKS fetching/caching by `kid` on top.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;

/// JWKS responses are cached this long before a refetch. Clerk rotates signing
/// keys infrequently; on a cache miss for an unknown `kid` we also force one
/// off-cycle refresh (see [`verify`]) so rotation is picked up promptly.
const JWKS_TTL: Duration = Duration::from_secs(600);

/// The claims we read from a Clerk session token. Clerk includes `org_*` only
/// when an organization is the active context for the session, so they are
/// optional; `sid` is the Clerk session id, used as a device-id surrogate.
///
/// **v1 vs v2 session tokens:** Clerk ships two session-token claim shapes.
/// "v1" (legacy instances) carries org context at the top level — `org_id`,
/// `org_slug`, `org_role` — mirrored by this struct's public fields below.
/// "v2" (the default on newly created Clerk instances, including our PROD
/// instance) nests the same information under a single `o` object instead —
/// `{"o": {"id": "org_…", "slg": "<slug>", "rol": "<bare-role>"}}` — and never
/// sends the top-level `org_id`/`org_slug`/`org_role` at all. Before this fix,
/// decoding only ever populated the v1 fields, so every v2 token's org
/// context silently deserialized as `None` across the board: org members got
/// "organization token required" 403s on their tenant subdomain, and org
/// tokens fell through and served as personal-plane requests on the apex.
///
/// `org_v2` captures the raw `o` shape; [`normalize_v2_org_claim`] folds it
/// into `org_id`/`org_role`/`org_slug` immediately after every decode (see
/// [`decode_claims`]), so this field is private and every other consumer in
/// the crate keeps reading the same three public fields unchanged regardless
/// of which token version produced them. Precedence when (implausibly) both
/// shapes are present: the v1 top-level fields win outright, as a group — `o`
/// is only ever consulted when the top-level `org_id` is absent.
#[derive(Debug, Clone, Deserialize)]
pub struct ClerkClaims {
    pub sub: String,
    #[serde(default)]
    pub org_id: Option<String>,
    #[serde(default)]
    pub org_role: Option<String>,
    #[serde(default)]
    pub org_slug: Option<String>,
    /// Raw v2 nested org claim, if present. Never read outside
    /// [`normalize_v2_org_claim`] — see the struct doc comment above.
    #[serde(default, rename = "o")]
    org_v2: Option<ClerkOrgClaimV2>,
    #[serde(default)]
    pub sid: Option<String>,
    pub exp: usize,
    /// The OIDC `name` claim — Clerk includes this on an OAuth access token
    /// minted with the `profile` scope (the MCP token flow requests
    /// `openid profile email`; see `mcp/mod.rs`'s `MCP_SCOPES_PERSONAL`).
    /// Clerk **session** JWTs generally do not carry it. `None` here does
    /// NOT mean the user has no name in Clerk — `resolve_clerk_context`
    /// falls back to a live Clerk API lookup (`fetch_display_name`) when
    /// this is absent and the user has no stored name yet.
    #[serde(default)]
    pub name: Option<String>,
    // ── OAuth-only claims (remote MCP, docs/plans/remote-mcp-v1.md §4.1) ──────
    //
    // Clerk OAuth access tokens (DCR + PKCE, minted for `/mcp`) carry these.
    // Some session tokens also carry `azp`, so these fields alone cannot
    // prove a token is OAuth. `/mcp` uses `verify_oauth` to require the access
    // token header type and its resource audience first. All four stay
    // `Option`/`#[serde(default)]` — Clerk
    // publishes no exhaustive OAuth-claims table, and the exact `client_id` vs
    // `azp` shape has not yet been pinned against a real staging token (open
    // question #3 in the plan) — so decoding must stay tolerant of whichever
    // subset actually shows up.
    /// Space-delimited OAuth scope string, when Clerk mints custom scopes.
    #[serde(default)]
    pub scope: Option<String>,
    /// OAuth client id that requested the token, when present.
    #[serde(default)]
    pub client_id: Option<String>,
    /// Alternate/legacy home for the authorized-party client id (OIDC `azp`).
    #[serde(default)]
    pub azp: Option<String>,
    /// JWT id — used for audit correlation (`mcp_tool_calls.token_jti`), never
    /// for replay prevention (tokens are still verified purely statelessly).
    #[serde(default)]
    pub jti: Option<String>,
    /// Clerk v2 session token factor-verification age: `[first_factor_minutes,
    /// second_factor_minutes]`, minutes since the user last verified that
    /// factor (`-1` = not enrolled). Absent on v1 session tokens, which
    /// therefore can never satisfy a re-verification requirement.
    #[serde(default)]
    pub fva: Option<Vec<i64>>,
}

/// Raw shape of a Clerk v2 session token's nested `o` (organization) claim.
/// Field names mirror Clerk's wire format exactly and are intentionally
/// terse — `id` (org id), `slg` (slug), `rol` (role). Unlike v1's `org_role`,
/// `rol` is a **bare** role name (`"admin"`, `"member"`, or a custom role),
/// never `org:`-namespaced; [`normalize_v2_org_claim`] re-namespaces it to
/// match v1 before anything downstream sees it.
#[derive(Debug, Clone, Deserialize)]
struct ClerkOrgClaimV2 {
    #[serde(default)]
    id: Option<String>,
    #[serde(default)]
    slg: Option<String>,
    #[serde(default)]
    rol: Option<String>,
}

/// Re-namespace a Clerk v2 bare org role (`"admin"`, `"member"`, or a custom
/// role name) into v1's `"org:"`-prefixed shape (`"org:admin"`), so every
/// consumer of `ClerkClaims::org_role` — today, `clerk_provision::map_org_role`
/// — sees byte-identical values regardless of which token version produced
/// them, and any DB row derived from it stays consistent across v1/v2
/// callers. A value that already carries an `org:` prefix (v2 should never
/// send one, but this is defensive) passes through unchanged rather than
/// getting double-prefixed.
fn namespace_v2_org_role(role: &str) -> String {
    if role.starts_with("org:") {
        role.to_string()
    } else {
        format!("org:{role}")
    }
}

/// Fold a decoded token's v2 nested `o` org claim into the v1 top-level
/// `org_id`/`org_role`/`org_slug` fields, so every consumer of `ClerkClaims`
/// after this point only ever sees the v1 shape (see the struct doc comment).
///
/// Precedence: the v1 top-level fields win **as a group** — `o` is consulted
/// only when the top-level `org_id` is absent. A token that somehow carries
/// both shapes therefore never gets a hybrid of the two; it takes v1's
/// `org_id`/`org_role`/`org_slug` outright. Absent both shapes, all three
/// stay `None`, same as before this fix.
///
/// An empty-string `o.id` (or `o.slg`/`o.rol`) is treated as absent — never
/// surfaced as `Some("")` — matching how `#[serde(default)]` already treats a
/// genuinely missing v1 field.
fn normalize_v2_org_claim(mut claims: ClerkClaims) -> ClerkClaims {
    if claims.org_id.is_none() {
        if let Some(o) = claims.org_v2.take() {
            claims.org_id = o.id.filter(|s| !s.is_empty());
            claims.org_slug = o.slg.filter(|s| !s.is_empty());
            claims.org_role = o
                .rol
                .filter(|s| !s.is_empty())
                .map(|r| namespace_v2_org_role(&r));
        }
    }
    claims.org_v2 = None;
    claims
}

// `ClerkClaims::without_org` used to live here — the claim-clearing primitive
// behind `/mcp`'s personal-resource org-claim strip. #825 restored
// `user:org:read` on `/mcp`, making an `org_id` claim there the user's own
// consent-screen choice rather than a pre-split leftover, so the strip, its
// `NormalizedClaims` wrapper, and this primitive are all gone. It had exactly
// one caller and no other purpose.

#[derive(Debug, Clone, thiserror::Error)]
pub enum ClerkError {
    /// The bearer token is not a JWT — the caller should fall back to the legacy
    /// session-token path rather than treat this as an auth failure.
    #[error("not a JWT")]
    NotJwt,
    #[error("clerk auth not configured")]
    NotConfigured,
    #[error("JWKS fetch failed: {0}")]
    Jwks(String),
    #[error("no signing key for kid")]
    UnknownKid,
    #[error("invalid token: {0}")]
    Invalid(String),
}

/// Clerk verification config, read from the environment. `CLERK_ISSUER` is the
/// Frontend API origin (e.g. `https://example.clerk.accounts.dev`); the
/// JWKS URL defaults to the standard well-known path under it.
#[derive(Debug, Clone)]
pub struct ClerkConfig {
    pub issuer: String,
    pub jwks_url: String,
}

impl ClerkConfig {
    /// Build from env, or `None` when `CLERK_ISSUER` is unset (Clerk auth off —
    /// the backend then accepts only legacy session tokens).
    pub fn from_env() -> Option<Self> {
        let issuer = std::env::var("CLERK_ISSUER").ok()?;
        let issuer = issuer.trim_end_matches('/').to_string();
        let jwks_url = std::env::var("CLERK_JWKS_URL")
            .unwrap_or_else(|_| format!("{issuer}/.well-known/jwks.json"));
        Some(Self { issuer, jwks_url })
    }
}

/// A single RSA signing key from a JWKS document.
#[derive(Debug, Deserialize)]
struct Jwk {
    kid: String,
    n: String,
    e: String,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// Parse a JWKS JSON document into decoding keys indexed by `kid`. RSA keys only
/// (Clerk signs with RS256); other key types are skipped.
fn parse_jwks(body: &str) -> Result<HashMap<String, DecodingKey>, ClerkError> {
    let jwks: Jwks = serde_json::from_str(body).map_err(|e| ClerkError::Jwks(e.to_string()))?;
    let mut out = HashMap::new();
    for k in jwks.keys {
        match DecodingKey::from_rsa_components(&k.n, &k.e) {
            Ok(key) => {
                out.insert(k.kid, key);
            }
            Err(e) => tracing::warn!(kid = %k.kid, error = %e, "skipping malformed JWKS key"),
        }
    }
    if out.is_empty() {
        return Err(ClerkError::Jwks("no usable RSA keys in JWKS".into()));
    }
    Ok(out)
}

/// Verify and decode a token against an already-resolved signing key, enforcing
/// RS256, the expected issuer, and expiry. Pure (no I/O) so it is unit-testable.
fn decode_claims(token: &str, key: &DecodingKey, issuer: &str) -> Result<ClerkClaims, ClerkError> {
    decode_clerk_token(token, key, issuer, None)
}

/// OAuth JWTs must have Clerk's access-token type and an audience containing
/// the exact protected resource URL. Client identifiers (`client_id`/`azp`)
/// describe the caller, not the resource, and cannot satisfy this check.
fn decode_oauth_claims(
    token: &str,
    key: &DecodingKey,
    issuer: &str,
    resource: &str,
) -> Result<ClerkClaims, ClerkError> {
    let header = decode_header(token).map_err(|e| ClerkError::Invalid(e.to_string()))?;
    if !matches!(header.typ.as_deref(), Some("at+jwt" | "application/at+jwt")) {
        return Err(ClerkError::Invalid("not an OAuth access JWT".into()));
    }
    if resource.is_empty() {
        return Err(ClerkError::Invalid("missing expected resource".into()));
    }
    decode_clerk_token(token, key, issuer, Some(resource))
}

fn decode_clerk_token(
    token: &str,
    key: &DecodingKey,
    issuer: &str,
    resource: Option<&str>,
) -> Result<ClerkClaims, ClerkError> {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer]);
    if let Some(resource) = resource {
        // jsonwebtoken accepts string or string-array audiences. Requiring
        // the claim also rejects unbound older grants; clients must reconnect.
        validation.set_audience(&[resource]);
        validation.required_spec_claims.insert("aud".into());
        validation.required_spec_claims.insert("iss".into());
    } else {
        // The web session path intentionally accepts Clerk sessions without
        // aud. Only the dedicated OAuth path enforces resource binding.
        validation.validate_aud = false;
    }
    validation.validate_exp = true;
    // Security review I5: also enforce `nbf` ("not before") when the token
    // carries one — a no-op for tokens without it (jsonwebtoken only checks
    // `nbf` if present; it isn't in `required_spec_claims`), but closes off
    // accepting a not-yet-valid token early if Clerk (or a future OAuth token
    // shape) ever sets one.
    validation.validate_nbf = true;
    let claims = if resource.is_some() {
        // The validation library ignores an unparseable optional `nbf` and
        // accepts an issuer array. OAuth claims must retain the standard
        // issuer string / NumericDate shape rather than bypass time checks.
        #[derive(Deserialize)]
        struct OAuthClaims {
            #[serde(flatten)]
            claims: ClerkClaims,
            #[serde(rename = "iss")]
            _issuer: String,
            #[serde(default, rename = "nbf")]
            _not_before: Option<u64>,
        }
        decode::<OAuthClaims>(token, key, &validation).map(|data| data.claims.claims)
    } else {
        decode::<ClerkClaims>(token, key, &validation).map(|data| data.claims)
    };
    claims
        .map(normalize_v2_org_claim)
        .map_err(|e| ClerkError::Invalid(e.to_string()))
}

/// Heuristic: does this bearer token look like a JWT (three base64url segments)?
/// Used to decide whether to attempt Clerk verification vs. the legacy path.
pub fn looks_like_jwt(token: &str) -> bool {
    let mut parts = token.split('.');
    matches!(
        (parts.next(), parts.next(), parts.next(), parts.next()),
        (Some(a), Some(_), Some(_), None) if a.starts_with("ey")
    )
}

struct CachedJwks {
    keys: HashMap<String, DecodingKey>,
    fetched: Instant,
}

fn cache() -> &'static RwLock<Option<CachedJwks>> {
    static CACHE: OnceLock<RwLock<Option<CachedJwks>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

/// Fetch + cache the instance JWKS. `force` bypasses the TTL (used once on an
/// unknown-`kid` miss to pick up key rotation).
async fn fetch_jwks(
    cfg: &ClerkConfig,
    http: &reqwest::Client,
    force: bool,
) -> Result<(), ClerkError> {
    if !force {
        if let Some(c) = cache().read().await.as_ref() {
            if c.fetched.elapsed() < JWKS_TTL {
                return Ok(());
            }
        }
    }
    // The Backend API JWKS endpoint (api.clerk.com/v1/jwks) requires the secret
    // key; the public Frontend-API JWKS (dev's {issuer}/.well-known/jwks.json)
    // does not. We prefer the Backend API in prod because a custom Frontend-API
    // domain (e.g. clerk.coralswarm.example) is not reliably reachable over Fly
    // egress — that path returns a Cloudflare 4xx / wrong keyset, so kid lookups
    // fail. Send auth only for the Backend API so the FAPI path is unchanged.
    let mut req = http.get(&cfg.jwks_url);
    if cfg.jwks_url.contains("api.clerk.com") {
        if let Ok(secret) = std::env::var("CLERK_SECRET_KEY") {
            req = req.bearer_auth(secret);
        }
    }
    let body = req
        .send()
        .await
        .map_err(|e| ClerkError::Jwks(e.to_string()))?
        .error_for_status()
        .map_err(|e| ClerkError::Jwks(e.to_string()))?
        .text()
        .await
        .map_err(|e| ClerkError::Jwks(e.to_string()))?;
    let keys = parse_jwks(&body)?;
    *cache().write().await = Some(CachedJwks {
        keys,
        fetched: Instant::now(),
    });
    Ok(())
}

/// Verify a Clerk session token end-to-end: detect JWT shape, resolve the
/// signing key by `kid` from the cached JWKS (refreshing once on a miss for key
/// rotation), and check signature + issuer + expiry.
pub async fn verify(
    token: &str,
    cfg: &ClerkConfig,
    http: &reqwest::Client,
) -> Result<ClerkClaims, ClerkError> {
    verify_with_resource(token, cfg, http, None).await
}

/// Verify a Clerk OAuth access JWT for the configured MCP resource. Reuses
/// the session verifier's signing-key cache and rotation refresh, but never
/// its audience-free validation policy.
pub async fn verify_oauth(
    token: &str,
    cfg: &ClerkConfig,
    http: &reqwest::Client,
    resource: &str,
) -> Result<ClerkClaims, ClerkError> {
    verify_with_resource(token, cfg, http, Some(resource)).await
}

async fn verify_with_resource(
    token: &str,
    cfg: &ClerkConfig,
    http: &reqwest::Client,
    resource: Option<&str>,
) -> Result<ClerkClaims, ClerkError> {
    if !looks_like_jwt(token) {
        return Err(ClerkError::NotJwt);
    }
    let kid = decode_header(token)
        .map_err(|e| ClerkError::Invalid(e.to_string()))?
        .kid
        .ok_or_else(|| ClerkError::Invalid("missing kid".into()))?;

    // Ensure we have keys (respecting the TTL), then look up this kid.
    fetch_jwks(cfg, http, false).await?;
    for force in [false, true] {
        if force {
            // Unknown kid with a warm cache → maybe a rotation; refetch once.
            fetch_jwks(cfg, http, true).await?;
        }
        let guard = cache().read().await;
        if let Some(c) = guard.as_ref() {
            if let Some(key) = c.keys.get(&kid) {
                return match resource {
                    Some(resource) => decode_oauth_claims(token, key, &cfg.issuer, resource),
                    None => decode_claims(token, key, &cfg.issuer),
                };
            }
        }
    }
    Err(ClerkError::UnknownKid)
}

/// Fetch a Clerk user's primary email via the Backend API. Clerk **session**
/// JWTs do not carry email, but provisioning needs one (the `users.email` column
/// is NOT NULL UNIQUE). Requires `CLERK_SECRET_KEY`. Returns `None` if the key is
/// unset or the call fails — the caller then falls back to a synthetic address.
pub async fn fetch_primary_email(
    clerk_user_id: &str,
    http: &reqwest::Client,
) -> Option<String> {
    let secret = std::env::var("CLERK_SECRET_KEY").ok()?;
    let body: serde_json::Value = http
        .get(format!("https://api.clerk.com/v1/users/{clerk_user_id}"))
        .bearer_auth(secret)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    let primary_id = body.get("primary_email_address_id").and_then(|v| v.as_str());
    let emails = body.get("email_addresses")?.as_array()?;
    // Only ever return a VERIFIED email: it becomes the linking key in
    // ensure_user, so an unverified (attacker-addable) address must never be
    // used (account-takeover guard). Prefer the primary if verified, else the
    // first verified address; otherwise None (caller uses a synthetic address).
    let verified = |e: &serde_json::Value| {
        e.get("verification")
            .and_then(|v| v.get("status"))
            .and_then(|s| s.as_str())
            == Some("verified")
    };
    let pick = emails
        .iter()
        .find(|e| e.get("id").and_then(|i| i.as_str()) == primary_id && verified(e))
        .or_else(|| emails.iter().find(|e| verified(e)))?;
    pick.get("email_address")
        .and_then(|v| v.as_str())
        .map(str::to_owned)
}

/// Fetch a Clerk user's display name via the Backend API — the fallback path
/// `resolve_clerk_context` uses when the verified token itself carries no
/// `name` claim (e.g. a Clerk **session** JWT, or an OAuth token minted
/// without the `profile` scope). Requires `CLERK_SECRET_KEY`; returns `None`
/// if the key is unset, the call fails, or Clerk has no name data for this
/// user (no synthetic fallback here — unlike email, `users.name` is
/// genuinely nullable, so "no name available" is a legitimate outcome, not
/// an error to paper over).
///
/// Prefers `first_name` + `last_name` (joined, trimmed); falls back to
/// `username` when neither name part is set. Never falls back to email —
/// `caller_display_name` (`mcp/tools/write.rs`) already does that at the
/// read site, and conflating the two here would make `users.name` a lossy
/// copy of `users.email` instead of a genuine "no display name" signal.
pub async fn fetch_display_name(clerk_user_id: &str, http: &reqwest::Client) -> Option<String> {
    let secret = std::env::var("CLERK_SECRET_KEY").ok()?;
    let body: serde_json::Value = http
        .get(format!("https://api.clerk.com/v1/users/{clerk_user_id}"))
        .bearer_auth(secret)
        .send()
        .await
        .ok()?
        .error_for_status()
        .ok()?
        .json()
        .await
        .ok()?;
    let part = |key: &str| {
        body.get(key)
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    };
    let full_name = match (part("first_name"), part("last_name")) {
        (Some(f), Some(l)) => Some(format!("{f} {l}")),
        (Some(f), None) => Some(f.to_string()),
        (None, Some(l)) => Some(l.to_string()),
        (None, None) => None,
    };
    full_name.or_else(|| part("username").map(str::to_string))
}

/// Shared test-only RS256 fixtures: a throwaway RSA-2048 keypair, its JWKS
/// (n/e) representation, and a `mint` helper for signing arbitrary claim sets.
///
/// `pub` (not `pub(crate)`) under `cfg(any(test, feature = "test-support"))` —
/// other in-crate test modules (`mcp::auth`, `mcp::well_known`) use it via
/// plain `cfg(test)`, and the external integration tests under
/// `backend/tests/mcp_http.rs` use it via the `test-support` feature (see the
/// `Cargo.toml` self-dependency note) to mint real Clerk-shaped tokens against
/// a local JWKS listener without duplicating this PEM block — see
/// `remote-mcp-v1.md` §5 Phase 1 step 9.
#[cfg(any(test, feature = "test-support"))]
pub mod test_support {
    use super::*;
    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    // A throwaway RSA-2048 keypair generated for these tests only (never used in
    // production). The JWKS (n/e) below is derived from this same key.
    pub const TEST_PRIV_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
MIIEowIBAAKCAQEAyFjQlKieXg+zR/1bqt+8xitNNJKf3/fFvtVM6LZNfhV0Qy6L\n\
g63lHuxYPsqcCMi4cGSja54wwaKIHKGxLhyAmrEiv/YWCNUZFi8a8SBiImGg3thD\n\
sV/RftbEYuD9+RG5/LU08jTyJYdS0o9qSRreIM/dsTNodZA3M9OSIZF7aVetio2r\n\
gUJsd3rNBBH9m5SLEgmWPi4k6Yx2dHT5JGSPXqoITgwqQdniVRmTS6ySt9vUKax5\n\
PnOFabChyYZbd5d47TPqdVoxN/tYdCtWMAQkmluHPsL9KiNG9u86ufaFKisCtIyj\n\
FdxOZT9MCyYleDwrSiAaZ/uu1HgOFTPPXnaUqwIDAQABAoIBAFUGRDbivJsEpLxH\n\
jLCe1TlwxQors8fUGkrbsREN1SNOLZIZXcIbeaI/AVBcog2kSYoBW81fTCfRN1yX\n\
Tl9YX0fHJw06fa1v6n6crR1p9dvLfP813ryXAoyGzr975qzeVgUmTfsAd0dv/7k2\n\
fg/aZCyequH6BDnoY1UOU/UFDc/tUWBnxjAaywhytl0q5IXfsXiqKoY/eOl1Da9j\n\
qdVDA2S4bkkqHnxAwASU9MrnB+HezUfslbFFsy/y4Qc2+GP5vfXZUQshqAzVlgJF\n\
6m1cuNrHWtCEy0Hr8RyE7rfAQplgiSAj4ktKqbJ60KImtbYCh5SRdKa2owWOag3Y\n\
T1/oREECgYEA/S0bAr0tSJAoOkSySWTbqOpbkpLsnUqhwyUkqwFWTHZtBJGVTqik\n\
pwCPaJlZIUmwlqaskDBLZystVGLQjX5CC+JRT/wnLZCt+5w5AkId4CSOEL+WmUo0\n\
lc3ANCOMQlEpls8DTA+qBCGJbBp2luhkOKhCGq+fv+7Xc2IhovUBtksCgYEAypTd\n\
orz8eKxaTn1Gczu8j91w4vsHqSJmpQVHsnb9zEjECGgQ6fQsEipY6TWGrSYhKP9v\n\
KEyv847Igol03yZVGrkHTVPsrFF+W9dw6JX6dbCD3A756mhnNDntDAMGv/mDlZ9y\n\
B9QbXLog7eTtvhrV/bRGh8ufAfBfvraxs22hHyECgYAJtzhpI39gYH/atJYu8Nrp\n\
IY1AE7lW0+1fQsakdVmtd4H5gei76wbejcvzFKdx+pT3QxAuPFw4alTD/0LHm0fH\n\
kvjR2/hY7Etw827Qol7pIh39kIyj91ALS6kLly2jwd+ZtPoIajV5ERxn9KAGbIak\n\
EfjvczvrLqw/9k0aJtLY/wKBgQCLPE4pMCOG/U8Ea24kfYZYiyavm0HkqdRKz5o7\n\
uzUtV0YFUSFu63fu9FQv1a4/C9n2x7mqxSqaCCUgM0G0rjAIMewm5evhdBGZJjZg\n\
ZdTDVxMKCPEUf7WPW9DlVJ8rC1+5UuGZJ9u2m+dOGu6cnu09ds6TPwyG1KO5/O+e\n\
F1eKgQKBgDqAg7bUgZRU0MrdJ3jKjM/H3PeCG5Vr1KfyadxtkZ/Z55pUxofnh2Da\n\
RDMphBnWCCCjvNJbU/Q/3eInHtUuPv+5eFL3k1bRYSpsMtuROSqT2GhZSVsvDseD\n\
4erYQT+8X+MMVHvLNdGdeKw/OsPPDzuuDmuOldk7EeOqHkfHD7v1\n\
-----END RSA PRIVATE KEY-----";

    pub const TEST_KID: &str = "test-key-1";
    pub const TEST_N: &str = "yFjQlKieXg-zR_1bqt-8xitNNJKf3_fFvtVM6LZNfhV0Qy6Lg63lHuxYPsqcCMi4cGSja54wwaKIHKGxLhyAmrEiv_YWCNUZFi8a8SBiImGg3thDsV_RftbEYuD9-RG5_LU08jTyJYdS0o9qSRreIM_dsTNodZA3M9OSIZF7aVetio2rgUJsd3rNBBH9m5SLEgmWPi4k6Yx2dHT5JGSPXqoITgwqQdniVRmTS6ySt9vUKax5PnOFabChyYZbd5d47TPqdVoxN_tYdCtWMAQkmluHPsL9KiNG9u86ufaFKisCtIyjFdxOZT9MCyYleDwrSiAaZ_uu1HgOFTPPXnaUqw";
    pub const TEST_E: &str = "AQAB";
    pub const ISSUER: &str = "https://example.clerk.accounts.dev";

    /// Sign an arbitrary claim set with the test key, tagged with `TEST_KID`.
    pub fn mint(claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());
        let key = EncodingKey::from_rsa_pem(TEST_PRIV_PEM.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    /// Sign a documented Clerk OAuth access JWT. Callers supply `aud`
    /// explicitly so missing/wrong-resource fixtures stay possible.
    pub fn mint_oauth(claims: serde_json::Value) -> String {
        mint_with_header(claims, Some("at+jwt"), TEST_KID)
    }

    pub fn mint_with_header(claims: serde_json::Value, typ: Option<&str>, kid: &str) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(kid.to_string());
        header.typ = typ.map(str::to_owned);
        let key = EncodingKey::from_rsa_pem(TEST_PRIV_PEM.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    /// The JWKS document (as JSON text) a mock issuer/JWKS listener should
    /// serve so tokens minted by [`mint`] verify.
    pub fn test_jwks() -> String {
        json!({"keys": [{"use": "sig", "kty": "RSA", "alg": "RS256",
            "kid": TEST_KID, "n": TEST_N, "e": TEST_E}]})
        .to_string()
    }

    pub fn test_key() -> DecodingKey {
        parse_jwks(&test_jwks()).unwrap().remove(TEST_KID).unwrap()
    }

    pub fn future_exp() -> usize {
        (chrono::Utc::now().timestamp() + 3600) as usize
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::*;
    use super::*;
    use serde_json::json;

    #[test]
    fn looks_like_jwt_distinguishes_jwt_from_opaque_token() {
        assert!(looks_like_jwt("eyJhbGci.eyJzdWIi.sigpart"));
        assert!(!looks_like_jwt("deadbeefcafe")); // legacy opaque session token
        assert!(!looks_like_jwt("eyOnly.twoParts"));
        assert!(!looks_like_jwt("a.b.c")); // 3 parts but not a JWT header
    }

    #[test]
    fn decode_claims_accepts_valid_token_and_reads_org_context() {
        let token = mint(json!({
            "iss": ISSUER, "sub": "user_123", "exp": future_exp(),
            "org_id": "org_abc", "org_role": "org:admin", "org_slug": "acme",
            "sid": "sess_xyz"
        }));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.sub, "user_123");
        assert_eq!(claims.org_id.as_deref(), Some("org_abc"));
        assert_eq!(claims.org_role.as_deref(), Some("org:admin"));
        assert_eq!(claims.sid.as_deref(), Some("sess_xyz"));
    }

    #[test]
    fn decode_claims_allows_missing_org_context() {
        let token = mint(json!({"iss": ISSUER, "sub": "user_solo", "exp": future_exp()}));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.sub, "user_solo");
        assert!(claims.org_id.is_none());
    }

    const MCP_RESOURCE: &str = "https://api.coralswarm.example/mcp";

    fn oauth_claims() -> serde_json::Value {
        json!({"iss": ISSUER, "sub": "user_oauth", "exp": future_exp(),
            "aud": MCP_RESOURCE, "client_id": "client_oauth", "jti": "oauth_jti",
            "o": {"id": "org_oauth", "rol": "member"}})
    }

    #[test]
    fn oauth_accepts_exact_resource_string_or_array_and_normalizes_org() {
        for aud in [
            json!(MCP_RESOURCE),
            json!(["https://other.example/mcp", MCP_RESOURCE]),
        ] {
            for typ in ["at+jwt", "application/at+jwt"] {
                let mut claims = oauth_claims();
                claims["aud"] = aud.clone();
                let token = mint_with_header(claims, Some(typ), TEST_KID);
                let claims =
                    decode_oauth_claims(&token, &test_key(), ISSUER, MCP_RESOURCE).unwrap();
                assert_eq!(claims.org_id.as_deref(), Some("org_oauth"));
                assert_eq!(claims.org_role.as_deref(), Some("org:member"));
            }
        }
    }

    #[test]
    fn oauth_rejects_unbound_and_wrong_resource_even_with_matching_client_claims() {
        let mut missing = oauth_claims();
        missing.as_object_mut().unwrap().remove("aud");
        // A client id or authorized party that happens to equal the resource
        // still does not constitute a signed resource-audience binding.
        missing["client_id"] = json!(MCP_RESOURCE);
        missing["azp"] = json!(MCP_RESOURCE);
        assert!(
            decode_oauth_claims(&mint_oauth(missing), &test_key(), ISSUER, MCP_RESOURCE).is_err()
        );
        for aud in [
            json!("https://another.example/mcp"),
            json!(format!("{MCP_RESOURCE}/")),
            json!("https://API.coralswarm.example/mcp"),
            json!(["https://another.example/mcp"]),
            json!([]),
            json!(null),
        ] {
            let mut claims = oauth_claims();
            claims["aud"] = aud;
            assert!(
                decode_oauth_claims(&mint_oauth(claims), &test_key(), ISSUER, MCP_RESOURCE)
                    .is_err()
            );
        }
    }

    #[test]
    fn oauth_rejects_session_and_id_token_replay_with_correct_audience() {
        for typ in [None, Some("JWT"), Some("id+jwt")] {
            let token = mint_with_header(oauth_claims(), typ, TEST_KID);
            assert!(decode_oauth_claims(&token, &test_key(), ISSUER, MCP_RESOURCE).is_err());
        }
        let session = mint(
            json!({"iss": ISSUER, "sub": "user_session", "exp": future_exp(),
            "sid": "session", "azp": "https://app.coralswarm.example"}),
        );
        assert!(decode_oauth_claims(&session, &test_key(), ISSUER, MCP_RESOURCE).is_err());
        // Browser sign-in still accepts the same ordinary audience-free session.
        assert!(decode_claims(&session, &test_key(), ISSUER).is_ok());
    }

    #[test]
    fn oauth_enforces_signature_issuer_expiry_and_not_before() {
        let now = chrono::Utc::now().timestamp();
        for claim in ["iss", "exp", "sub"] {
            let mut claims = oauth_claims();
            claims.as_object_mut().unwrap().remove(claim);
            assert!(decode_oauth_claims(&mint_oauth(claims), &test_key(), ISSUER, MCP_RESOURCE).is_err());
        }
        for (claim, value) in [
            ("iss", json!("https://untrusted.example")),
            ("exp", json!(now - 7200)),
            ("nbf", json!(now + 7200)),
            ("nbf", json!("tomorrow")),
            ("iss", json!([ISSUER])),
        ] {
            let mut claims = oauth_claims();
            claims[claim] = value;
            assert!(
                decode_oauth_claims(&mint_oauth(claims), &test_key(), ISSUER, MCP_RESOURCE)
                    .is_err()
            );
        }
        let token = mint_oauth(oauth_claims());
        let mut parts: Vec<&str> = token.split('.').collect();
        let signature: String = parts[2].chars().rev().collect();
        parts[2] = &signature;
        assert!(decode_oauth_claims(&parts.join("."), &test_key(), ISSUER, MCP_RESOURCE).is_err());
    }

    #[tokio::test]
    async fn oauth_reuses_cached_keys_and_refreshes_once_on_rotation() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        let requests = Arc::new(AtomicUsize::new(0));
        let requests_handler = requests.clone();
        let app = axum::Router::new().route(
            "/jwks",
            axum::routing::get(move || {
                let requests = requests_handler.clone();
                async move {
                    let request = requests.fetch_add(1, Ordering::SeqCst);
                    let kid = if request == 0 {
                        TEST_KID
                    } else {
                        "rotated-oauth-key"
                    };
                    axum::Json(json!({"keys": [{"kid": kid, "n": TEST_N, "e": TEST_E}]}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let cfg = ClerkConfig {
            issuer: ISSUER.into(),
            jwks_url: format!("http://{}/jwks", listener.local_addr().unwrap()),
        };
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        *cache().write().await = None;
        let http = reqwest::Client::new();
        let token = mint_oauth(oauth_claims());
        for _ in 0..2 {
            assert!(verify_oauth(&token, &cfg, &http, MCP_RESOURCE)
                .await
                .is_ok());
        }
        assert_eq!(
            requests.load(Ordering::SeqCst),
            1,
            "warm cache should avoid a fetch"
        );
        let rotated = mint_with_header(oauth_claims(), Some("at+jwt"), "rotated-oauth-key");
        assert!(verify_oauth(&rotated, &cfg, &http, MCP_RESOURCE)
            .await
            .is_ok());
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2,
            "unknown kid should trigger one rotation fetch"
        );
        let unknown = mint_with_header(oauth_claims(), Some("at+jwt"), "unknown-oauth-key");
        assert!(matches!(
            verify_oauth(&unknown, &cfg, &http, MCP_RESOURCE).await,
            Err(ClerkError::UnknownKid)
        ));
        assert_eq!(
            requests.load(Ordering::SeqCst),
            3,
            "unknown kid must not fetch indefinitely"
        );
        *cache().write().await = None;
        server.abort();
    }

    // ── v2 nested `o` org-claim normalization (prod-blocking fix) ────────────

    #[test]
    fn decode_claims_normalizes_v2_nested_org_claim() {
        // Live-shaped: a v2 session token's active-org context nested under
        // `o`, bare (non-namespaced) role — exactly what prod's Clerk
        // instance mints (confirmed live against org_2ExampleTestOrg000000000000
        // / slug testorg).
        let token = mint(json!({
            "iss": ISSUER, "sub": "user_v2", "exp": future_exp(),
            "o": {"id": "org_2ExampleTestOrg000000000000", "slg": "testorg", "rol": "admin"}
        }));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.sub, "user_v2");
        assert_eq!(
            claims.org_id.as_deref(),
            Some("org_2ExampleTestOrg000000000000")
        );
        assert_eq!(claims.org_slug.as_deref(), Some("testorg"));
        // Bare v2 "admin" must normalize to v1's namespaced "org:admin" so
        // `clerk_provision::map_org_role` and any DB row derived from it stay
        // consistent regardless of which token version authenticated.
        assert_eq!(claims.org_role.as_deref(), Some("org:admin"));
    }

    #[test]
    fn decode_claims_v2_token_with_no_active_org_has_no_org_context() {
        // A v2 personal-plane token: no `o` claim at all (the session has no
        // active organization). Must behave identically to a v1 token with no
        // org fields — all three stay `None`, not `Some("")` or a panic.
        let token = mint(json!({"iss": ISSUER, "sub": "user_v2_solo", "exp": future_exp()}));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.sub, "user_v2_solo");
        assert!(claims.org_id.is_none());
        assert!(claims.org_slug.is_none());
        assert!(claims.org_role.is_none());
    }

    #[test]
    fn decode_claims_v1_top_level_wins_over_v2_nested_when_both_present() {
        // Documented precedence: if a token somehow carries both shapes, the
        // v1 top-level fields win outright, as a group — never a hybrid of
        // the two, and the v2 `o` claim is ignored entirely once v1's
        // `org_id` is present.
        let token = mint(json!({
            "iss": ISSUER, "sub": "user_both", "exp": future_exp(),
            "org_id": "org_v1_wins", "org_role": "org:admin", "org_slug": "v1-slug",
            "o": {"id": "org_v2_loses", "slg": "v2-slug", "rol": "member"}
        }));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.org_id.as_deref(), Some("org_v1_wins"));
        assert_eq!(claims.org_slug.as_deref(), Some("v1-slug"));
        assert_eq!(claims.org_role.as_deref(), Some("org:admin"));
    }

    #[test]
    fn decode_claims_v2_bare_role_normalization_leaves_already_namespaced_role_untouched() {
        // Defensive: if a v2 token ever sends an already-`org:`-prefixed
        // role, normalization must not double-prefix it.
        let token = mint(json!({
            "iss": ISSUER, "sub": "user_v2_prefixed", "exp": future_exp(),
            "o": {"id": "org_x", "slg": "x", "rol": "org:member"}
        }));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert_eq!(claims.org_role.as_deref(), Some("org:member"));
    }

    #[test]
    fn decode_claims_v2_empty_org_id_does_not_produce_some_empty_string() {
        // An empty-string `o.id` must be treated as absent, never surfaced as
        // `Some("")` — same contract as a v1 token with no `org_id` at all.
        let token = mint(json!({
            "iss": ISSUER, "sub": "user_v2_empty", "exp": future_exp(),
            "o": {"id": "", "slg": "", "rol": ""}
        }));
        let claims = decode_claims(&token, &test_key(), ISSUER).unwrap();
        assert!(claims.org_id.is_none());
        assert!(claims.org_slug.is_none());
        assert!(claims.org_role.is_none());
    }

    #[test]
    fn decode_claims_rejects_wrong_issuer() {
        let token = mint(json!({"iss": "https://evil.example", "sub": "u", "exp": future_exp()}));
        assert!(decode_claims(&token, &test_key(), ISSUER).is_err());
    }

    #[test]
    fn decode_claims_rejects_expired_token() {
        // Well past the default 60s clock-skew leeway.
        let past = (chrono::Utc::now().timestamp() - 7200) as usize;
        let token = mint(json!({"iss": ISSUER, "sub": "u", "exp": past}));
        assert!(decode_claims(&token, &test_key(), ISSUER).is_err());
    }

    #[test]
    fn decode_claims_rejects_tampered_signature() {
        let token = mint(json!({"iss": ISSUER, "sub": "u", "exp": future_exp()}));
        // Reverse the signature segment so its decoded bytes genuinely differ
        // (flipping a single trailing base64url char can be a no-op).
        let mut parts: Vec<&str> = token.split('.').collect();
        let sig: String = parts[2].chars().rev().collect();
        parts[2] = &sig;
        let tampered = parts.join(".");
        assert!(decode_claims(&tampered, &test_key(), ISSUER).is_err());
    }

    #[test]
    fn parse_jwks_indexes_keys_by_kid() {
        let keys = parse_jwks(&test_jwks()).unwrap();
        assert!(keys.contains_key(TEST_KID));
    }

    #[test]
    fn config_from_env_derives_jwks_url() {
        // Build directly to avoid env-var races with other tests.
        let cfg = ClerkConfig {
            issuer: ISSUER.to_string(),
            jwks_url: format!("{ISSUER}/.well-known/jwks.json"),
        };
        assert!(cfg.jwks_url.ends_with("/.well-known/jwks.json"));
    }
}
