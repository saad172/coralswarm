//! Microsoft (Entra ID) access-token validation for the Teams / Outlook add-in
//! SSO exchange (`routes::addin_microsoft`).
//!
//! The Teams meeting pane (`microsoftTeams.authentication.getAuthToken()`) and
//! the Outlook panes (Office nested app auth) hand the page an AAD **v2 access
//! token for OUR API app** (`access_as_user` scope). This module is the only
//! place that decides whether such a token is real. Nothing the client says is
//! trusted: the identity this returns — `(tid, oid)` — comes exclusively from
//! the SIGNED claims of a token whose signature, issuer, audience, scope and
//! lifetime all verified.
//!
//! # What is checked
//!
//! * **Signature** — RS256 only, against Microsoft's published signing keys
//!   (`https://login.microsoftonline.com/common/discovery/v2.0/keys`), looked up
//!   by `kid`. The key set is cached ([`JWKS_TTL`]); an unknown `kid` forces ONE
//!   refetch to pick up a rotation, but never more often than
//!   [`JWKS_MIN_REFETCH`] — this endpoint is unauthenticated, so a stream of
//!   junk `kid`s must not become a stream of outbound requests to Microsoft.
//! * **Issuer** — `https://login.microsoftonline.com/{tid}/v2.0` for a v2 token
//!   (`ver == "2.0"`). The v1 form `https://sts.windows.net/{tid}/` is accepted
//!   ONLY when the token itself says it is v1 (`ver == "1.0"`); a v2 token can
//!   never satisfy the v1 issuer or vice versa. `{tid}` is the token's own
//!   `tid` claim, which is safe because the signature already covers it, and
//!   it must be a GUID — the multi-tenant JWKS signs tokens for EVERY tenant, so
//!   "signed by Microsoft" alone proves nothing about which tenant minted it.
//! * **Audience** — the configured app id OR its application-ID URI. Both are
//!   config ([`MicrosoftSsoConfig`]); a token minted for any other app is
//!   refused even though Microsoft signed it.
//! * **Scope** — `scp` must contain `access_as_user`. An app-only token (no
//!   `scp`, `roles` instead) is refused: it carries no user.
//! * **Lifetime** — `exp` (required) and `nbf`, with 30s of clock-skew leeway.
//! * **Identity** — `tid` and `oid`, both GUIDs, both lower-cased.
//!
//! The JWKS source is injectable at two levels so tests need no network:
//! [`verify_with_keys`] is pure (a parsed key set in, an identity out), and
//! `MICROSOFT_ADDIN_JWKS_URL` repoints the cached fetch at a local listener.

use std::collections::HashMap;
use std::sync::OnceLock;
use std::time::{Duration, Instant};

use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use tokio::sync::RwLock;
use uuid::Uuid;

/// The Entra application (client) id of the CoralSwarm add-in API app.
pub(crate) const DEFAULT_APP_ID: &str = "00000000-0000-0000-0000-000000000000";
/// The application-ID URI that app exposes (`Expose an API`).
pub(crate) const DEFAULT_APP_ID_URI: &str =
    "api://app.coralswarm.example/00000000-0000-0000-0000-000000000000";
/// Teams desktop/mobile and Teams web client ids — pre-authorized for the
/// add-in API's `access_as_user` scope in the Entra app registration.
pub(crate) const TEAMS_CLIENT_IDS: [&str; 2] = [
    "00000000-0000-4000-8000-e34a138d45f9",
    "00000000-0000-4000-8000-8d36f4e05941",
];
/// Office host client ids for legacy Office SSO (`Office.auth.getAccessToken`,
/// the Outlook panes' fallback when nested app auth is unavailable), per
/// Microsoft's "Register an Office Add-in that uses legacy Office SSO":
/// Microsoft Office, Office on the web, Outlook on the web. (The umbrella
/// `ea5a67f6-…` id is what the Entra app PRE-AUTHORIZES; the token's
/// `azp`/`appid` carries one of these concrete host ids.)
pub(crate) const OFFICE_CLIENT_IDS: [&str; 3] = [
    "00000000-0000-4000-8000-402213b72cab",
    "93d53678-613d-4013-afc1-62e9e444a0a5",
    "00000000-0000-4000-8000-9182e2c88ff3",
];
const DEFAULT_JWKS_URL: &str = "https://login.microsoftonline.com/common/discovery/v2.0/keys";

/// The delegated scope the add-in's pre-authorized clients request.
pub(crate) const REQUIRED_SCOPE: &str = "access_as_user";

/// Microsoft rotates signing keys on the order of weeks; an hour is far inside
/// that, and an unknown `kid` forces an early refetch regardless.
const JWKS_TTL: Duration = Duration::from_secs(3600);
/// Floor between two FORCED refetches (unknown-`kid` path). See module doc.
const JWKS_MIN_REFETCH: Duration = Duration::from_secs(30);
/// Floor between attempts while NOTHING usable is cached. A failed cold fetch
/// must not lock every sign-in out for [`JWKS_MIN_REFETCH`]; a few seconds still
/// keeps concurrent callers single-flight and an outage from being hammered.
const JWKS_EMPTY_MIN_REFETCH: Duration = Duration::from_secs(3);
/// Clock-skew leeway for `exp`/`nbf`.
const CLOCK_SKEW_LEEWAY_SECS: u64 = 30;

/// Flag parse, factored out so it is testable without touching process env —
/// same shape as `routes::teams_ingest::parse_org_microsoft_teams_flag`.
fn parse_flag(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// `ADDIN_MICROSOFT_SSO_ENABLED` — default OFF (unset or anything but `1`/`true`).
/// Read fresh on every call so an operator flip needs no restart-time caching
/// to reason about. When off, `POST /v1/addin/microsoft/session` answers `404
/// {"error":"microsoft_sso_disabled"}` and the panes fall back to Clerk sign-in.
pub(crate) fn sso_enabled() -> bool {
    parse_flag(std::env::var("ADDIN_MICROSOFT_SSO_ENABLED").ok().as_deref())
}

/// Everything token validation needs from configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MicrosoftSsoConfig {
    /// Accepted `aud` values: the app id and the application-ID URI.
    pub(crate) audiences: Vec<String>,
    pub(crate) jwks_url: String,
    /// Client applications allowed to present a token (canonical lower-case
    /// GUIDs): the token's `azp` (v2) / `appid` (v1) must be one of these. The
    /// audience check proves the token is FOR our API; this proves it was
    /// obtained BY a client we ship, so another app a user consented to cannot
    /// turn its `access_as_user` token into a CoralSwarm credential.
    pub(crate) allowed_clients: Vec<String>,
}

impl MicrosoftSsoConfig {
    /// `MICROSOFT_ADDIN_APP_ID` / `MICROSOFT_ADDIN_APP_ID_URI` (both default to
    /// the production add-in app) and `MICROSOFT_ADDIN_JWKS_URL` (default
    /// Microsoft's public key endpoint; overridable so tests can serve their
    /// own JWKS from a local listener — never set in production).
    pub(crate) fn from_env() -> Self {
        let get = |name: &str, default: &str| {
            std::env::var(name)
                .ok()
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .unwrap_or_else(|| default.to_string())
        };
        let mut cfg = Self::new(
            get("MICROSOFT_ADDIN_APP_ID", DEFAULT_APP_ID),
            get("MICROSOFT_ADDIN_APP_ID_URI", DEFAULT_APP_ID_URI),
            get("MICROSOFT_ADDIN_JWKS_URL", DEFAULT_JWKS_URL),
        );
        if let Ok(raw) = std::env::var("MICROSOFT_ADDIN_ALLOWED_CLIENT_IDS") {
            let list = parse_client_ids(&raw);
            if !list.is_empty() {
                cfg.allowed_clients = list;
            }
        }
        cfg
    }

    /// Default client allowlist: the two Teams clients, the Office host clients
    /// (legacy Office SSO fallback in the Outlook panes), plus the app itself
    /// (the client id in nested app auth). `MICROSOFT_ADDIN_ALLOWED_CLIENT_IDS`
    /// REPLACES this default.
    pub(crate) fn new(app_id: String, app_id_uri: String, jwks_url: String) -> Self {
        let mut allowed_clients: Vec<String> =
            TEAMS_CLIENT_IDS.iter().chain(OFFICE_CLIENT_IDS.iter()).map(|s| s.to_string()).collect();
        if let Some(own) = canonical_guid(&app_id) {
            allowed_clients.push(own);
        }
        Self {
            audiences: vec![app_id, app_id_uri],
            jwks_url,
            allowed_clients,
        }
    }
}

/// A Microsoft identity proved by a verified token. Both ids are lower-case
/// GUIDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VerifiedMicrosoftIdentity {
    pub(crate) tenant_id: String,
    pub(crate) object_id: String,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MicrosoftTokenError {
    /// Not a JWT / undecodable / missing `kid` or required claims.
    Malformed,
    /// Signature, issuer, audience, scope or lifetime check failed.
    Invalid(String),
    /// Signed with a key Microsoft does not publish (even after one refetch).
    UnknownKey,
    /// We could not obtain Microsoft's keys. NOT the caller's fault — the route
    /// answers 503, never "your token is invalid".
    KeysUnavailable(String),
}

/// The claims read from the token, AFTER the library has verified the
/// signature, `iss`, `aud`, `exp` and `nbf`. `iss`/`aud`/`exp`/`nbf` are not
/// listed: the library consumed them.
#[derive(Debug, Deserialize)]
struct Claims {
    #[serde(default)]
    tid: Option<String>,
    #[serde(default)]
    oid: Option<String>,
    #[serde(default)]
    scp: Option<String>,
    /// Client application id: `azp` in a v2 token.
    #[serde(default)]
    azp: Option<String>,
    /// Client application id: `appid` in a v1 token.
    #[serde(default)]
    appid: Option<String>,
}

/// The unverified peek used ONLY to derive the expected issuer. Never trusted
/// on its own: the real decode below re-checks the issuer and the signature
/// covers `tid`, so a forged `tid` here can only make verification fail.
#[derive(Debug, Deserialize)]
struct Peek {
    #[serde(default)]
    tid: Option<String>,
    #[serde(default)]
    ver: Option<String>,
}

fn peek(token: &str) -> Result<Peek, MicrosoftTokenError> {
    use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
    let mut parts = token.split('.');
    let (Some(_), Some(payload), Some(_), None) =
        (parts.next(), parts.next(), parts.next(), parts.next())
    else {
        return Err(MicrosoftTokenError::Malformed);
    };
    let bytes = URL_SAFE_NO_PAD
        .decode(payload)
        .map_err(|_| MicrosoftTokenError::Malformed)?;
    serde_json::from_slice(&bytes).map_err(|_| MicrosoftTokenError::Malformed)
}

/// Comma/whitespace-separated GUID list -> canonical lower-case GUIDs
/// (malformed entries are dropped, never kept as a wildcard).
fn parse_client_ids(raw: &str) -> Vec<String> {
    raw.split(|c: char| c == ',' || c.is_whitespace())
        .filter(|s| !s.is_empty())
        .filter_map(canonical_guid)
        .collect()
}

/// Lower-case GUID, or `None`. `Uuid::parse_str` also accepts braces/urn forms;
/// the canonical hyphenated rendering is what we store and compare.
fn canonical_guid(raw: &str) -> Option<String> {
    Uuid::parse_str(raw.trim()).ok().map(|u| u.hyphenated().to_string())
}

/// The issuer a token with this `ver`/`tid` must carry.
fn expected_issuer(ver: &str, tid: &str) -> Result<String, MicrosoftTokenError> {
    match ver {
        "2.0" => Ok(format!("https://login.microsoftonline.com/{tid}/v2.0")),
        // v1 form accepted only for a token that declares itself v1.
        "1.0" => Ok(format!("https://sts.windows.net/{tid}/")),
        _ => Err(MicrosoftTokenError::Invalid("unsupported token version".into())),
    }
}

/// Pure verification against an already-resolved signing key. No I/O.
pub(crate) fn verify_with_key(
    token: &str,
    key: &DecodingKey,
    cfg: &MicrosoftSsoConfig,
) -> Result<VerifiedMicrosoftIdentity, MicrosoftTokenError> {
    let peeked = peek(token)?;
    let tid = peeked
        .tid
        .as_deref()
        .and_then(canonical_guid)
        .ok_or_else(|| MicrosoftTokenError::Invalid("missing or malformed tid".into()))?;
    let ver = peeked
        .ver
        .as_deref()
        .ok_or_else(|| MicrosoftTokenError::Invalid("missing ver".into()))?;
    let issuer = expected_issuer(ver, &tid)?;

    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&[issuer.as_str()]);
    validation.set_audience(&cfg.audiences);
    validation.set_required_spec_claims(&["exp", "iss", "aud"]);
    validation.validate_exp = true;
    validation.validate_nbf = true;
    validation.leeway = CLOCK_SKEW_LEEWAY_SECS;

    let claims = decode::<Claims>(token, key, &validation)
        .map_err(|e| MicrosoftTokenError::Invalid(e.to_string()))?
        .claims;

    // The signed `tid` must be the one the issuer was derived from. They are
    // read from the same payload, so this can only fail on a logic error — it
    // is asserted anyway because the whole issuer check hangs on it.
    if claims.tid.as_deref().and_then(canonical_guid).as_deref() != Some(tid.as_str()) {
        return Err(MicrosoftTokenError::Invalid("tid mismatch".into()));
    }

    let has_scope = claims
        .scp
        .as_deref()
        .is_some_and(|scp| scp.split_whitespace().any(|s| s == REQUIRED_SCOPE));
    if !has_scope {
        return Err(MicrosoftTokenError::Invalid(format!(
            "missing required scope {REQUIRED_SCOPE}"
        )));
    }

    // The calling CLIENT must be one we ship (v2 carries it as `azp`, v1 as
    // `appid`; a token missing the version's claim is refused, not defaulted).
    let client = if ver == "2.0" { claims.azp.as_deref() } else { claims.appid.as_deref() }
        .and_then(canonical_guid);
    if !client.as_ref().is_some_and(|c| cfg.allowed_clients.contains(c)) {
        return Err(MicrosoftTokenError::Invalid("client application not allowed".into()));
    }

    let oid = claims
        .oid
        .as_deref()
        .and_then(canonical_guid)
        .ok_or_else(|| MicrosoftTokenError::Invalid("missing or malformed oid".into()))?;

    Ok(VerifiedMicrosoftIdentity {
        tenant_id: tid,
        object_id: oid,
    })
}

#[derive(Debug, Deserialize)]
struct Jwk {
    #[serde(default)]
    kty: String,
    #[serde(default)]
    kid: String,
    #[serde(default)]
    n: String,
    #[serde(default)]
    e: String,
}

#[derive(Debug, Deserialize)]
struct Jwks {
    keys: Vec<Jwk>,
}

/// A parsed key set, indexed by `kid`. Only RSA keys are kept.
pub(crate) struct KeySet {
    keys: HashMap<String, DecodingKey>,
}

impl KeySet {
    /// Parse a JWKS JSON document.
    pub(crate) fn parse(body: &str) -> Result<Self, MicrosoftTokenError> {
        let jwks: Jwks = serde_json::from_str(body)
            .map_err(|e| MicrosoftTokenError::KeysUnavailable(format!("bad JWKS: {e}")))?;
        let mut keys = HashMap::new();
        for k in jwks.keys {
            if k.kty != "RSA" || k.kid.is_empty() {
                continue;
            }
            match DecodingKey::from_rsa_components(&k.n, &k.e) {
                Ok(key) => {
                    keys.insert(k.kid, key);
                }
                Err(e) => tracing::warn!(kid = %k.kid, error = %e, "skipping malformed Microsoft JWKS key"),
            }
        }
        if keys.is_empty() {
            return Err(MicrosoftTokenError::KeysUnavailable("no usable RSA keys in JWKS".into()));
        }
        Ok(Self { keys })
    }
}

/// Verify `token` against an in-memory key set (the injectable seam the unit
/// suite uses; production goes through [`verify`], which adds the cached fetch).
#[cfg(test)]
pub(crate) fn verify_with_keys(
    token: &str,
    keys: &KeySet,
    cfg: &MicrosoftSsoConfig,
) -> Result<VerifiedMicrosoftIdentity, MicrosoftTokenError> {
    let kid = header_kid(token)?;
    let key = keys.keys.get(&kid).ok_or(MicrosoftTokenError::UnknownKey)?;
    verify_with_key(token, key, cfg)
}

fn header_kid(token: &str) -> Result<String, MicrosoftTokenError> {
    let header = decode_header(token).map_err(|_| MicrosoftTokenError::Malformed)?;
    if header.alg != Algorithm::RS256 {
        return Err(MicrosoftTokenError::Invalid("unsupported algorithm".into()));
    }
    header.kid.ok_or(MicrosoftTokenError::Malformed)
}

struct Cached {
    keys: KeySet,
    fetched: Instant,
}

/// Keyed by JWKS URL (production has exactly one) so differently-configured
/// callers, e.g. tests pointing at their own listeners, never evict each other.
fn cache() -> &'static RwLock<std::collections::HashMap<String, Cached>> {
    static CACHE: OnceLock<RwLock<std::collections::HashMap<String, Cached>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(std::collections::HashMap::new()))
}

/// One single-flight gate PER JWKS URL; it remembers when the last fetch was
/// ATTEMPTED, success or failure. Holding a gate across its fetch is what makes
/// the refetch floor real: concurrent callers for the same URL queue here
/// instead of each seeing "no recent fetch" and each going to Microsoft, and a
/// failed fetch still consumes the slot, so an outage cannot turn junk `kid`s
/// into one outbound attempt per request. A slow fetch for one URL never blocks
/// another URL.
type Gate = std::sync::Arc<tokio::sync::Mutex<Option<Instant>>>;
fn fetch_gate(url: &str) -> Gate {
    static GATES: OnceLock<std::sync::Mutex<std::collections::HashMap<String, Gate>>> = OnceLock::new();
    GATES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .entry(url.to_owned())
        .or_default()
        .clone()
}

/// `Err((error, retryable))`: only transport failures, 5xx and 429 are worth a
/// quick retry; other 4xx (a wrong URL) and malformed documents are not.
async fn fetch_keys_once(
    url: &str,
    http: &reqwest::Client,
) -> Result<KeySet, (MicrosoftTokenError, bool)> {
    let unavailable = |e: reqwest::Error, retryable: bool| {
        (MicrosoftTokenError::KeysUnavailable(e.to_string()), retryable)
    };
    let response = http
        .get(url)
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .map_err(|e| unavailable(e, true))?;
    let status = response.status();
    let response = response.error_for_status().map_err(|e| {
        unavailable(e, status.is_server_error() || status == reqwest::StatusCode::TOO_MANY_REQUESTS)
    })?;
    let body = response.text().await.map_err(|e| unavailable(e, true))?;
    KeySet::parse(&body).map_err(|e| (e, false))
}

/// One quick retry on a transient failure (a single dropped connection is the
/// common cause of a failed cold fetch). Failures are logged at ERROR with the
/// URL, so the cause is visible after the request that hit it has returned.
async fn fetch_keys(url: &str, http: &reqwest::Client) -> Result<KeySet, MicrosoftTokenError> {
    let mut result = fetch_keys_once(url, http).await;
    if matches!(&result, Err((_, true))) {
        tokio::time::sleep(Duration::from_millis(250)).await;
        result = fetch_keys_once(url, http).await;
    }
    let result = result.map_err(|(e, _)| e);
    if let Err(e) = &result {
        tracing::error!(url = %url, error = ?e, "Microsoft JWKS fetch failed");
    }
    result
}

/// Populate the key cache at startup. Never fatal, and a failed pre-warm does
/// not consume the throttle slot: the first real sign-in still gets its own
/// fetch (single-flight still applies to concurrent callers).
pub(crate) async fn prewarm(cfg: &MicrosoftSsoConfig, http: &reqwest::Client) {
    match refresh(cfg, http, None).await {
        Ok(()) => tracing::info!("Microsoft add-in signing keys pre-warmed"),
        Err(e) => {
            *fetch_gate(&cfg.jwks_url).lock().await = None;
            tracing::warn!(error = ?e, "Microsoft add-in key pre-warm failed; first request will retry")
        }
    }
}

/// Is the cache usable for `url` right now, and (when `kid` is given) does it
/// hold that key?
async fn cache_satisfies(url: &str, kid: Option<&str>) -> bool {
    let guard = cache().read().await;
    match guard.get(url) {
        Some(c) if c.fetched.elapsed() < JWKS_TTL => {
            kid.map_or(true, |k| c.keys.keys.contains_key(k))
        }
        _ => false,
    }
}

/// Make the cache hold a fresh key set (and `kid`, when given), fetching from
/// Microsoft at most once per [`JWKS_MIN_REFETCH`] per URL no matter how many
/// callers arrive at once. Returns `Ok` when the cache already satisfies the
/// request or when a recent attempt is still within the floor (the caller then
/// reports `UnknownKey`, or `KeysUnavailable` if there is nothing cached at all).
async fn refresh(
    cfg: &MicrosoftSsoConfig,
    http: &reqwest::Client,
    kid: Option<&str>,
) -> Result<(), MicrosoftTokenError> {
    if cache_satisfies(&cfg.jwks_url, kid).await {
        return Ok(());
    }
    let gate = fetch_gate(&cfg.jwks_url);
    let mut last = gate.lock().await;
    // Re-check: whoever held the gate before us may have just fetched.
    if cache_satisfies(&cfg.jwks_url, kid).await {
        return Ok(());
    }
    if let Some(at) = *last {
        // Only a FRESH entry counts: one past JWKS_TTL must never verify a token.
        let usable = cache_satisfies(&cfg.jwks_url, None).await;
        // Anti-amplification floor (unknown `kid`, populated cache) stays long;
        // with nothing usable cached the only cost of waiting is a lockout, so it is short.
        let floor = if usable { JWKS_MIN_REFETCH } else { JWKS_EMPTY_MIN_REFETCH };
        if at.elapsed() < floor {
            return if usable {
                Ok(())
            } else {
                Err(MicrosoftTokenError::KeysUnavailable("key fetch recently failed".into()))
            };
        }
    }
    // Claim the slot BEFORE the I/O so a failure consumes it too.
    *last = Some(Instant::now());
    let keys = fetch_keys(&cfg.jwks_url, http).await?;
    cache().write().await.insert(
        cfg.jwks_url.clone(),
        Cached {
            keys,
            fetched: Instant::now(),
        },
    );
    Ok(())
}

/// Verify a Microsoft access token end to end: key lookup (cached, with a
/// rate-limited, single-flight refetch on an unknown `kid`) + [`verify_with_key`].
pub(crate) async fn verify(
    token: &str,
    cfg: &MicrosoftSsoConfig,
    http: &reqwest::Client,
) -> Result<VerifiedMicrosoftIdentity, MicrosoftTokenError> {
    let kid = header_kid(token)?;
    refresh(cfg, http, Some(&kid)).await?;
    let guard = cache().read().await;
    match guard
        .get(&cfg.jwks_url)
        .filter(|c| c.fetched.elapsed() < JWKS_TTL)
        .and_then(|c| c.keys.keys.get(&kid))
    {
        Some(key) => verify_with_key(token, key, cfg),
        None => Err(MicrosoftTokenError::UnknownKey),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::clerk::test_support::{future_exp, mint, test_jwks, TEST_KID};
    use serde_json::json;

    const APP_ID: &str = DEFAULT_APP_ID;
    const TID: &str = "11111111-2222-3333-4444-555555555555";
    const OID: &str = "AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE";

    fn cfg() -> MicrosoftSsoConfig {
        MicrosoftSsoConfig::new(
            DEFAULT_APP_ID.to_string(),
            DEFAULT_APP_ID_URI.to_string(),
            "http://unused.invalid/keys".to_string(),
        )
    }

    fn keys() -> KeySet {
        KeySet::parse(&test_jwks()).unwrap()
    }

    fn good_claims() -> serde_json::Value {
        json!({
            "iss": format!("https://login.microsoftonline.com/{TID}/v2.0"),
            "aud": APP_ID,
            "ver": "2.0",
            "tid": TID,
            "oid": OID,
            "scp": "access_as_user",
            "azp": TEAMS_CLIENT_IDS[0],
            "exp": future_exp(),
            "nbf": (chrono::Utc::now().timestamp() - 60),
        })
    }

    fn check(claims: serde_json::Value) -> Result<VerifiedMicrosoftIdentity, MicrosoftTokenError> {
        verify_with_keys(&mint(claims), &keys(), &cfg())
    }

    fn with(mut base: serde_json::Value, key: &str, value: serde_json::Value) -> serde_json::Value {
        base[key] = value;
        base
    }

    fn without(mut base: serde_json::Value, key: &str) -> serde_json::Value {
        base.as_object_mut().unwrap().remove(key);
        base
    }

    #[test]
    fn good_v2_token_yields_lowercased_identity() {
        let id = check(good_claims()).unwrap();
        assert_eq!(id.tenant_id, TID);
        assert_eq!(id.object_id, OID.to_lowercase(), "oid must be normalized to lower case");
    }

    #[test]
    fn audience_may_be_the_app_id_uri() {
        let id = check(with(good_claims(), "aud", json!(DEFAULT_APP_ID_URI))).unwrap();
        assert_eq!(id.tenant_id, TID);
    }

    #[test]
    fn wrong_audience_is_refused() {
        let err = check(with(good_claims(), "aud", json!("00000000-0000-0000-0000-000000000000")))
            .unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        // Microsoft Graph's own audience must not be accepted either.
        let err = check(with(good_claims(), "aud", json!("https://graph.microsoft.com"))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn wrong_issuer_is_refused() {
        // Right shape, a different tenant than the token's own `tid`.
        let other = "99999999-2222-3333-4444-555555555555";
        let err = check(with(
            good_claims(),
            "iss",
            json!(format!("https://login.microsoftonline.com/{other}/v2.0")),
        ))
        .unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        // A look-alike host.
        let err = check(with(
            good_claims(),
            "iss",
            json!(format!("https://login.microsoftonline.com.evil.example/{TID}/v2.0")),
        ))
        .unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn v1_issuer_is_accepted_only_for_a_v1_token() {
        let v1_iss = json!(format!("https://sts.windows.net/{TID}/"));
        // A genuine v1 token.
        let v1 = with(with(good_claims(), "iss", v1_iss.clone()), "ver", json!("1.0"));
        let v1 = with(without(v1, "azp"), "appid", json!(TEAMS_CLIENT_IDS[1]));
        assert_eq!(check(v1).unwrap().tenant_id, TID);
        // A token claiming v2 must not get away with the v1 issuer...
        let err = check(with(good_claims(), "iss", v1_iss)).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        // ...and a v1 token must not carry the v2 issuer.
        let err = check(with(good_claims(), "ver", json!("1.0"))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn client_application_must_be_allowlisted() {
        // Missing, unlisted, malformed, and the wrong version's claim are all refused.
        assert!(check(without(good_claims(), "azp")).is_err());
        let e = check(with(good_claims(), "azp", json!("00000000-1111-2222-3333-444444444444"))).unwrap_err();
        assert!(matches!(e, MicrosoftTokenError::Invalid(_)), "{e:?}");
        assert!(check(with(good_claims(), "azp", json!("*"))).is_err());
        // v2 reads `azp`, not `appid`.
        assert!(check(with(without(good_claims(), "azp"), "appid", json!(TEAMS_CLIENT_IDS[0]))).is_err());
        // The app's own id (nested app auth) and the second Teams client pass; case-insensitive.
        assert!(check(with(good_claims(), "azp", json!(DEFAULT_APP_ID.to_uppercase()))).is_ok());
        assert!(check(with(good_claims(), "azp", json!(TEAMS_CLIENT_IDS[1]))).is_ok());
        // Legacy Office SSO (Outlook fallback): every Office host client passes.
        for office in OFFICE_CLIENT_IDS {
            assert!(check(with(good_claims(), "azp", json!(office))).is_ok(), "{office}");
        }
        // The umbrella pre-authorization id is NOT a token client id.
        assert!(check(with(good_claims(), "azp", json!("00000000-0000-4000-8000-a28eedc2bbc0"))).is_err());
        // v1 reads `appid`, not `azp`.
        let v1_iss = json!(format!("https://sts.windows.net/{TID}/"));
        let v1 = with(with(good_claims(), "iss", v1_iss), "ver", json!("1.0"));
        assert!(check(v1.clone()).is_err(), "v1 with only azp has no appid");
        assert!(check(with(v1, "appid", json!("00000000-1111-2222-3333-444444444444"))).is_err());
    }

    #[test]
    fn client_id_list_parsing_drops_malformed_entries() {
        let l = parse_client_ids("1FEC8E78-bce4-4aaf-ab1b-5451cc387264, nope ,*,00000000-0000-4000-8000-8d36f4e05941");
        assert_eq!(l, vec![TEAMS_CLIENT_IDS[0].to_string(), TEAMS_CLIENT_IDS[1].to_string()]);
        assert!(parse_client_ids("*").is_empty(), "a wildcard is never honored");
    }

    #[test]
    fn unknown_or_missing_version_is_refused() {
        assert!(check(with(good_claims(), "ver", json!("3.0"))).is_err());
        assert!(check(without(good_claims(), "ver")).is_err());
    }

    #[test]
    fn missing_or_wrong_scope_is_refused() {
        let err = check(without(good_claims(), "scp")).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        let err = check(with(good_claims(), "scp", json!("User.Read profile"))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        // Substring is not a match: `access_as_user_admin` is a different scope.
        let err = check(with(good_claims(), "scp", json!("access_as_user_admin"))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
        // Among several, it is found.
        assert!(check(with(good_claims(), "scp", json!("profile access_as_user openid"))).is_ok());
    }

    #[test]
    fn an_app_only_token_is_refused() {
        // `roles` instead of `scp` — an application permission, no delegated user.
        let claims = with(without(good_claims(), "scp"), "roles", json!(["access_as_user"]));
        assert!(check(claims).is_err());
    }

    #[test]
    fn expired_token_is_refused() {
        let past = (chrono::Utc::now().timestamp() - 3600) as usize;
        let err = check(with(good_claims(), "exp", json!(past))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn not_yet_valid_token_is_refused() {
        let future = chrono::Utc::now().timestamp() + 3600;
        let err = check(with(good_claims(), "nbf", json!(future))).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn missing_exp_is_refused() {
        assert!(check(without(good_claims(), "exp")).is_err());
    }

    #[test]
    fn bad_signature_is_refused() {
        let token = mint(good_claims());
        let (head, sig) = token.rsplit_once('.').unwrap();
        // Flip one character of the signature.
        let mut sig = sig.to_string();
        let flipped = if sig.starts_with('A') { 'B' } else { 'A' };
        sig.replace_range(0..1, &flipped.to_string());
        let err = verify_with_keys(&format!("{head}.{sig}"), &keys(), &cfg()).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn tampered_payload_is_refused() {
        use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
        let token = mint(good_claims());
        let mut parts: Vec<&str> = token.split('.').collect();
        // Swap in a payload naming a different oid, keep the original signature.
        let forged = URL_SAFE_NO_PAD.encode(
            serde_json::to_vec(&with(good_claims(), "oid", json!("deadbeef-0000-0000-0000-000000000000")))
                .unwrap(),
        );
        parts[1] = &forged;
        let err = verify_with_keys(&parts.join("."), &keys(), &cfg()).unwrap_err();
        assert!(matches!(err, MicrosoftTokenError::Invalid(_)), "{err:?}");
    }

    #[test]
    fn unknown_kid_is_refused() {
        let other = KeySet::parse(
            &json!({"keys": [{"kty": "RSA", "kid": "some-other-kid",
                "n": crate::clerk::test_support::TEST_N, "e": "AQAB"}]})
            .to_string(),
        )
        .unwrap();
        let err = verify_with_keys(&mint(good_claims()), &other, &cfg()).unwrap_err();
        assert_eq!(err, MicrosoftTokenError::UnknownKey);
        // And the right set accepts it (the kid constant is what mint() stamps).
        assert_eq!(TEST_KID, "test-key-1");
        assert!(verify_with_keys(&mint(good_claims()), &keys(), &cfg()).is_ok());
    }

    #[test]
    fn missing_or_malformed_identity_claims_are_refused() {
        assert!(check(without(good_claims(), "oid")).is_err());
        assert!(check(with(good_claims(), "oid", json!("not-a-guid"))).is_err());
        assert!(check(without(good_claims(), "tid")).is_err());
        assert!(check(with(good_claims(), "tid", json!("../../etc"))).is_err());
    }

    #[test]
    fn garbage_is_malformed_not_a_panic() {
        for t in ["", "abc", "a.b", "a.b.c", "eyJhbGciOiJSUzI1NiJ9.e30.sig"] {
            assert!(verify_with_keys(t, &keys(), &cfg()).is_err(), "{t:?}");
        }
    }

    #[test]
    fn hs256_alg_confusion_is_refused() {
        // An HS256 token signed with the PUBLIC modulus as the HMAC secret — the
        // classic alg-confusion attempt. The header alg gate rejects it before
        // any key is consulted.
        use jsonwebtoken::{encode, EncodingKey, Header};
        let mut h = Header::new(Algorithm::HS256);
        h.kid = Some(TEST_KID.to_string());
        let token = encode(&h, &good_claims(), &EncodingKey::from_secret(b"anything")).unwrap();
        assert!(verify_with_keys(&token, &keys(), &cfg()).is_err());
    }

    /// Spawn a JWKS listener that counts requests; `status` is what it answers.
    async fn counting_jwks(status: axum::http::StatusCode) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let app = axum::Router::new().route(
            "/keys",
            axum::routing::get(move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    // A little latency so concurrent callers genuinely overlap.
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    (status, test_jwks())
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.ok() });
        (format!("http://{addr}/keys"), hits)
    }

    fn junk_kid_token() -> String {
        use jsonwebtoken::{encode, EncodingKey, Header};
        let mut h = Header::new(Algorithm::RS256);
        h.kid = Some(format!("junk-{}", Uuid::new_v4()));
        let key = EncodingKey::from_rsa_pem(crate::clerk::test_support::TEST_PRIV_PEM.as_bytes()).unwrap();
        encode(&h, &good_claims(), &key).unwrap()
    }

    /// The JWKS endpoint is unauthenticated: a burst of requests (cold cache or
    /// junk `kid`s) must produce ONE outbound fetch, not one per request — and a
    /// FAILED fetch consumes the slot too, so an outage is not hammered.
    #[tokio::test]
    async fn concurrent_requests_share_one_outbound_key_fetch() {
        use std::sync::atomic::Ordering;
        let http = reqwest::Client::new();

        // Cold cache + unknown kids, 20 at once, then 20 more: one fetch total.
        let (url, hits) = counting_jwks(axum::http::StatusCode::OK).await;
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        let run = |c: MicrosoftSsoConfig| {
            let http = http.clone();
            async move {
                let tokens: Vec<String> = (0..20).map(|_| junk_kid_token()).collect();
                let calls = tokens.iter().map(|t| verify(t, &c, &http));
                futures::future::join_all(calls).await
            }
        };
        for r in run(c.clone()).await.into_iter().chain(run(c.clone()).await) {
            assert_eq!(r, Err(MicrosoftTokenError::UnknownKey));
        }
        assert_eq!(hits.load(Ordering::SeqCst), 1, "40 junk-kid requests must cost one outbound fetch");

        // A failing upstream: still ONE attempt, every caller told "unavailable".
        let (url, hits) = counting_jwks(axum::http::StatusCode::INTERNAL_SERVER_ERROR).await;
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        for r in run(c.clone()).await.into_iter().chain(run(c).await) {
            assert!(matches!(r, Err(MicrosoftTokenError::KeysUnavailable(_))), "{r:?}");
        }
        // One failed cold attempt = the request plus its single quick retry.
        assert_eq!(hits.load(Ordering::SeqCst), 2, "a failed fetch must consume the throttle slot");
    }

    /// JWKS listener that answers 500 for the first `failures` requests, then 200.
    async fn flaky_jwks(failures: usize) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let app = axum::Router::new().route(
            "/keys",
            axum::routing::get(move || {
                let h = h.clone();
                async move {
                    let n = h.fetch_add(1, Ordering::SeqCst);
                    if n < failures {
                        (axum::http::StatusCode::INTERNAL_SERVER_ERROR, String::new())
                    } else {
                        (axum::http::StatusCode::OK, test_jwks())
                    }
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.ok() });
        (format!("http://{addr}/keys"), hits)
    }

    #[tokio::test]
    async fn one_transient_failure_is_absorbed_by_the_quick_retry() {
        use std::sync::atomic::Ordering;
        let (url, hits) = flaky_jwks(1).await;
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        refresh(&c, &reqwest::Client::new(), None).await.expect("retry should recover");
        assert_eq!(hits.load(Ordering::SeqCst), 2);
    }

    /// An empty cache must not be locked out for the 30s floor after a failed
    /// cold fetch: only the short floor applies, then the next call retries.
    #[tokio::test]
    async fn empty_cache_failure_uses_short_floor_not_thirty_seconds() {
        use std::sync::atomic::Ordering;
        let (url, hits) = flaky_jwks(2).await; // attempt + its retry both fail
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        let http = reqwest::Client::new();
        assert!(matches!(refresh(&c, &http, None).await, Err(MicrosoftTokenError::KeysUnavailable(_))));
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        // Inside the short floor: single-flight, no new outbound fetch.
        assert!(refresh(&c, &http, None).await.is_err());
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        // Past the short floor (and far inside 30s): the fetch is retried and succeeds.
        tokio::time::sleep(JWKS_EMPTY_MIN_REFETCH + Duration::from_millis(200)).await;
        refresh(&c, &http, None).await.expect("recovers after the short floor");
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        assert!(JWKS_EMPTY_MIN_REFETCH < JWKS_MIN_REFETCH);
    }

    /// JWKS listener with a fixed status that counts requests.
    async fn status_jwks(code: u16) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
        use std::sync::atomic::{AtomicUsize, Ordering};
        let hits = std::sync::Arc::new(AtomicUsize::new(0));
        let h = hits.clone();
        let app = axum::Router::new().route(
            "/keys",
            axum::routing::get(move || {
                let h = h.clone();
                async move {
                    h.fetch_add(1, Ordering::SeqCst);
                    (axum::http::StatusCode::from_u16(code).unwrap(), String::new())
                }
            }),
        );
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = l.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(l, app).await.ok() });
        (format!("http://{addr}/keys"), hits)
    }

    #[tokio::test]
    async fn only_transient_statuses_are_retried() {
        use std::sync::atomic::Ordering;
        let http = reqwest::Client::new();
        for (code, expected) in [(404, 1), (403, 1), (500, 2), (503, 2), (429, 2)] {
            let (url, hits) = status_jwks(code).await;
            assert!(fetch_keys(&url, &http).await.is_err());
            assert_eq!(hits.load(Ordering::SeqCst), expected, "status {code}");
        }
    }

    /// A failed startup pre-warm must not make the first sign-in 503 inside the floor.
    #[tokio::test]
    async fn failed_prewarm_does_not_consume_the_first_requests_fetch() {
        use std::sync::atomic::Ordering;
        let (url, hits) = flaky_jwks(2).await; // pre-warm attempt + retry fail
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        let http = reqwest::Client::new();
        prewarm(&c, &http).await;
        assert_eq!(hits.load(Ordering::SeqCst), 2);
        // Immediately (inside the 3s floor): the sign-in fetches and succeeds.
        refresh(&c, &http, Some(TEST_KID)).await.expect("first request gets its own fetch");
        assert_eq!(hits.load(Ordering::SeqCst), 3);
        assert!(cache_satisfies(&c.jwks_url, Some(TEST_KID)).await);
    }

    /// A key set past its TTL is unusable even when a refetch is throttled.
    #[tokio::test]
    async fn expired_cache_entry_is_never_used_for_verification() {
        use std::sync::atomic::Ordering;
        let (url, hits) = flaky_jwks(usize::MAX).await; // upstream down
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url.clone());
        let stale = Instant::now() - JWKS_TTL - Duration::from_secs(1);
        cache().write().await.insert(
            url.clone(),
            Cached { keys: KeySet::parse(&test_jwks()).unwrap(), fetched: stale },
        );
        let http = reqwest::Client::new();
        // Refetch attempted and failed: refuse rather than verify with the old key.
        assert!(matches!(
            refresh(&c, &http, Some(TEST_KID)).await,
            Err(MicrosoftTokenError::KeysUnavailable(_))
        ));
        let before = hits.load(Ordering::SeqCst);
        // Throttled (inside the floor): still refused, not silently Ok.
        assert!(matches!(
            refresh(&c, &http, Some(TEST_KID)).await,
            Err(MicrosoftTokenError::KeysUnavailable(_))
        ));
        assert_eq!(hits.load(Ordering::SeqCst), before);
        // And verify() itself must not pick the expired key.
        let token = mint(good_claims());
        assert!(matches!(
            verify(&token, &c, &http).await,
            Err(MicrosoftTokenError::KeysUnavailable(_))
        ));
    }

    #[tokio::test]
    async fn prewarm_populates_cache_and_failure_is_not_fatal() {
        let (url, hits) = flaky_jwks(0).await;
        let c = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), url);
        prewarm(&c, &reqwest::Client::new()).await;
        assert_eq!(hits.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(cache_satisfies(&c.jwks_url, Some(TEST_KID)).await);
        let dead = MicrosoftSsoConfig::new(DEFAULT_APP_ID.into(), DEFAULT_APP_ID_URI.into(), "http://127.0.0.1:1/keys".into());
        prewarm(&dead, &reqwest::Client::new()).await; // must simply return
        assert!(!cache_satisfies(&dead.jwks_url, None).await);
    }

    #[test]
    fn flag_parsing() {
        assert!(!parse_flag(None));
        assert!(!parse_flag(Some("")));
        assert!(!parse_flag(Some("0")));
        assert!(!parse_flag(Some("yes")));
        assert!(parse_flag(Some("1")));
        assert!(parse_flag(Some("true")));
        assert!(parse_flag(Some("TRUE")));
    }

    #[test]
    fn config_defaults_cover_both_audiences() {
        let c = MicrosoftSsoConfig::new("a".into(), "b".into(), "u".into());
        assert_eq!(c.audiences, vec!["a".to_string(), "b".to_string()]);
    }
}
