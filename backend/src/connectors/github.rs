//! GitHub connector — `ConnectorProvider` implementation, GitHub App JWT
//! minting, installation-token cache, and a small typed REST client with
//! `Link`-header pagination and rate-limit awareness.
//!
//! docs/connectors/github.md is the design doc this module implements (PR A,
//! "sync plumbing"); docs/connectors/CONNECTOR_CONTRACT.md is the
//! cross-connector invariant set it must satisfy (I1-I10). Read both before
//! changing this module's shape.
//!
//! # One App, two credential kinds (§3.1)
//!
//! * **Installation tokens** — minted on demand from `GITHUB_APP_ID` +
//!   `GITHUB_APP_PRIVATE_KEY`, cached in memory ONLY until expiry
//!   ([`installation_token`]), **never persisted**. This is what reaches
//!   repository content, on both the org and personal install paths — the
//!   credential kind is decided by WHICH account installed the App, not by
//!   who is asking.
//! * **User-to-server OAuth tokens** — minted by [`GithubConnector::
//!   exchange_code`], stored in `oauth_tokens` under `TokenOwner::Member`
//!   (I7: explicit owner, never account-wide, never a fallback read). Used
//!   only to identify the connecting human and to complete the App-install
//!   callback; content reads always go through the installation token above.
//!
//! # Env (§4), read at CALL TIME via `std::env::var` — never cached in a
//! `static`/`OnceLock`, so a rotated secret takes effect on the next request
//! with no redeploy (existing repo-wide convention, e.g. `SlackConnector::
//! authorize_url`'s own `SLACK_CLIENT_ID` read).
//!
//! | Var | Purpose |
//! |---|---|
//! | `GITHUB_APP_ID` | App id, the JWT `iss` claim |
//! | `GITHUB_APP_PRIVATE_KEY` | PEM text, checked FIRST |
//! | `GITHUB_APP_PRIVATE_KEY_FILE` | PEM file path, checked when the inline var is absent — dev convenience (`.github-app-dev.pem` in the worktree), read at call time |
//! | `GITHUB_CLIENT_ID` / `GITHUB_CLIENT_SECRET` | user-to-server OAuth |
//! | `GITHUB_WEBHOOK_SECRET` | HMAC for `X-Hub-Signature-256` |
//! | `GITHUB_APP_SLUG` | builds the install URL |
//!
//! Missing env -> [`ConnectorError::NotConfigured`] -> `503`, same contract
//! every other connector's env read gives.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use serde::Deserialize;
use sqlx::PgPool;
use std::sync::LazyLock;
use uuid::Uuid;

use super::{ConnectorError, ConnectorProvider, TokenBundle, TokenOwner};

pub(crate) const GITHUB_API_BASE: &str = "https://api.github.com";

/// The GitHub REST base URL, read at CALL time so a test can point the whole
/// connector at a local mock server — the exact seam
/// `connectors::slack::slack_api_base` provides for Slack, and for the same
/// reason: before it existed, every GitHub call site hardcoded the real host,
/// so nothing above the database layer could be tested at all. A test that
/// sets `GITHUB_API_BASE` must serialize against every other test in the
/// `--lib` binary that does (see `connectors::slack`'s own ENV_LOCK note).
/// [`GITHUB_API_BASE`] remains the production default.
pub(crate) fn github_api_base() -> String {
    std::env::var("GITHUB_API_BASE").unwrap_or_else(|_| GITHUB_API_BASE.to_string())
}

/// Consent and token exchange transport override exists only in E2E builds.
fn github_web_base() -> String {
    #[cfg(feature = "e2e")]
    if let Ok(base) = std::env::var("E2E_GITHUB_WEB_BASE") {
        return base.trim_end_matches('/').to_string();
    }
    "https://github.com".to_string()
}

// ---------------------------------------------------------------------------
// Env accessors — read at call time, never cached
// ---------------------------------------------------------------------------

fn env_nonempty(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.is_empty())
}

fn app_id() -> Result<String, ConnectorError> {
    env_nonempty("GITHUB_APP_ID").ok_or_else(|| ConnectorError::NotConfigured("GITHUB_APP_ID not configured".into()))
}

/// The App's RS256 private key PEM. `GITHUB_APP_PRIVATE_KEY` (inline PEM
/// text) is checked first; `GITHUB_APP_PRIVATE_KEY_FILE` (a path, read at
/// call time — never cached) is the local-dev fallback for a PEM that lives
/// on disk (`.github-app-dev.pem` in the worktree, gitignored). Neither set
/// -> `NotConfigured`. A file that fails to read is also `NotConfigured`
/// (never silently falls through to "no key"), with the underlying IO error
/// logged, never echoed to a caller.
async fn app_private_key_pem() -> Result<String, ConnectorError> {
    if let Some(pem) = env_nonempty("GITHUB_APP_PRIVATE_KEY") {
        return Ok(pem);
    }
    if let Some(path) = env_nonempty("GITHUB_APP_PRIVATE_KEY_FILE") {
        return tokio::fs::read_to_string(&path).await.map_err(|e| {
            tracing::error!(error = %e, path = %path, "GITHUB_APP_PRIVATE_KEY_FILE could not be read");
            ConnectorError::NotConfigured("GITHUB_APP_PRIVATE_KEY_FILE could not be read".into())
        });
    }
    Err(ConnectorError::NotConfigured(
        "GITHUB_APP_PRIVATE_KEY or GITHUB_APP_PRIVATE_KEY_FILE not configured".into(),
    ))
}

fn client_id() -> Result<String, ConnectorError> {
    env_nonempty("GITHUB_CLIENT_ID")
        .ok_or_else(|| ConnectorError::NotConfigured("GITHUB_CLIENT_ID not configured".into()))
}

fn client_secret() -> Result<String, ConnectorError> {
    env_nonempty("GITHUB_CLIENT_SECRET")
        .ok_or_else(|| ConnectorError::NotConfigured("GITHUB_CLIENT_SECRET not configured".into()))
}

pub(crate) fn webhook_secret() -> Result<String, ConnectorError> {
    env_nonempty("GITHUB_WEBHOOK_SECRET")
        .ok_or_else(|| ConnectorError::NotConfigured("GITHUB_WEBHOOK_SECRET not configured".into()))
}

fn app_slug() -> Result<String, ConnectorError> {
    env_nonempty("GITHUB_APP_SLUG")
        .ok_or_else(|| ConnectorError::NotConfigured("GITHUB_APP_SLUG not configured".into()))
}

// ---------------------------------------------------------------------------
// App JWT — RS256, 10-minute expiry, iat skewed 60s back (clock drift)
// ---------------------------------------------------------------------------

#[derive(serde::Serialize)]
struct AppJwtClaims {
    iat: i64,
    exp: i64,
    iss: String,
}

/// Duration of a minted App JWT — GitHub's own hard cap is 10 minutes.
pub(crate) const APP_JWT_TTL_SECS: i64 = 600;
/// `iat` is stamped this many seconds in the PAST, to tolerate the minting
/// host's clock running slightly ahead of GitHub's — a JWT whose `iat` is in
/// GitHub's future is rejected outright, whereas an `iat` a little in the
/// past is always valid (GitHub does not enforce an `iat`-not-before-now
/// lower bound).
const APP_JWT_IAT_SKEW_SECS: i64 = 60;

/// Mint a fresh App-level JWT (RS256) — the credential used to (a) list/
/// inspect installations and (b) mint a short-lived installation access
/// token. Minted fresh on every call (no caching — it is cheap to compute
/// and 10 minutes is too short a window to be worth caching across
/// requests); callers that need many calls in a row should mint once and
/// reuse the `String` locally.
pub(crate) async fn mint_app_jwt() -> Result<String, ConnectorError> {
    let app_id = app_id()?;
    let pem = app_private_key_pem().await?;

    let now = Utc::now().timestamp();
    let claims = AppJwtClaims {
        iat: now - APP_JWT_IAT_SKEW_SECS,
        exp: now - APP_JWT_IAT_SKEW_SECS + APP_JWT_TTL_SECS,
        iss: app_id,
    };

    let key = jsonwebtoken::EncodingKey::from_rsa_pem(pem.as_bytes()).map_err(|e| {
        tracing::error!(error = %e, "GITHUB_APP_PRIVATE_KEY is not a valid RSA PEM");
        ConnectorError::NotConfigured("GITHUB_APP_PRIVATE_KEY is not a valid RSA PEM".into())
    })?;
    jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        &key,
    )
    .map_err(|e| ConnectorError::Internal(format!("failed to mint GitHub App JWT: {e}")))
}

// ---------------------------------------------------------------------------
// Installation token cache — in-memory only, never persisted
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct CachedInstallationToken {
    token: String,
    /// The expiry GitHub reported. The cache treats a token as unusable
    /// [`INSTALLATION_TOKEN_SAFETY_MARGIN_SECS`] before this, so a caller
    /// never starts a multi-request operation on a token that could expire
    /// mid-flight.
    expires_at: DateTime<Utc>,
}

/// How long before a cached installation token's real expiry it is treated
/// as already-expired and re-minted — bounds the "token expires mid-request"
/// race without adding a retry loop to every caller.
const INSTALLATION_TOKEN_SAFETY_MARGIN_SECS: i64 = 60;

static INSTALLATION_TOKEN_CACHE: LazyLock<DashMap<i64, CachedInstallationToken>> = LazyLock::new(DashMap::new);

/// Drop a cached installation token so a suspend/delete cannot keep serving
/// a still-live credential from this process.
pub(crate) fn invalidate_installation_token(installation_id: i64) {
    INSTALLATION_TOKEN_CACHE.remove(&installation_id);
}

#[derive(Deserialize)]
struct InstallationTokenResponse {
    token: String,
    expires_at: DateTime<Utc>,
}

/// Mint (or reuse a cached, still-fresh) installation access token for
/// `installation_id`. **Never persisted** — cached in-process only, for at
/// most ~1 hour (GitHub's own installation-token TTL) minus the safety
/// margin above. This is the credential every repository-content read uses,
/// on both the org and personal install paths (§3.1/§3.2) — the reach
/// boundary IS the installation, which is the entire point of the App model
/// over a classic OAuth App with `repo` scope.
pub(crate) async fn installation_token(
    http: &reqwest::Client,
    installation_id: i64,
) -> Result<String, ConnectorError> {
    if let Some(cached) = INSTALLATION_TOKEN_CACHE.get(&installation_id) {
        if cached.expires_at - chrono::Duration::seconds(INSTALLATION_TOKEN_SAFETY_MARGIN_SECS) > Utc::now() {
            return Ok(cached.token.clone());
        }
    }

    let jwt = mint_app_jwt().await?;
    let url = format!("{}/app/installations/{installation_id}/access_tokens", github_api_base());
    let resp = http
        .post(&url)
        .bearer_auth(&jwt)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "coralswarm")
        .send()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("installation token request failed: {e}")))?;

    if let Some(retry_after) = rate_limit_retry_after(&resp) {
        return Err(ConnectorError::Exchange(format!(
            "installation token request rate-limited (retry_after={retry_after:?})"
        )));
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        return Err(ConnectorError::Exchange(format!(
            "installation token request failed: {status} {body}"
        )));
    }

    let parsed: InstallationTokenResponse = resp
        .json()
        .await
        .map_err(|e| ConnectorError::Exchange(format!("installation token response was not valid JSON: {e}")))?;

    INSTALLATION_TOKEN_CACHE.insert(
        installation_id,
        CachedInstallationToken {
            token: parsed.token.clone(),
            expires_at: parsed.expires_at,
        },
    );
    Ok(parsed.token)
}

// ---------------------------------------------------------------------------
// Rate limit / pagination-aware REST client
// ---------------------------------------------------------------------------

/// A GitHub REST call failure, distinguishing "back off and retry" from
/// every other failure shape — PR B's backfill worker needs to tell the two
/// apart to implement `X-RateLimit-Remaining`/`Retry-After` backoff without
/// this crate having to parse response headers a second time.
#[derive(Debug)]
pub(crate) enum GithubApiError {
    /// Primary or secondary rate limit — `retry_after` is GitHub's own
    /// `Retry-After` header (secondary limits) or the seconds until
    /// `X-RateLimit-Reset` (primary limit exhaustion), whichever the
    /// response actually carried.
    RateLimited { retry_after: std::time::Duration },
    /// Any other non-2xx response.
    Http { status: u16, body: String },
    /// Transport-level failure (DNS, TLS, connect, timeout).
    Transport(String),
    /// A 2xx response whose body did not parse as the expected shape.
    Parse(String),
}

impl std::fmt::Display for GithubApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GithubApiError::RateLimited { retry_after } => {
                write!(f, "rate limited, retry after {retry_after:?}")
            }
            GithubApiError::Http { status, body } => write!(f, "http {status}: {body}"),
            GithubApiError::Transport(e) => write!(f, "transport error: {e}"),
            GithubApiError::Parse(e) => write!(f, "parse error: {e}"),
        }
    }
}

impl std::error::Error for GithubApiError {}

impl From<GithubApiError> for ConnectorError {
    fn from(e: GithubApiError) -> Self {
        ConnectorError::Exchange(e.to_string())
    }
}

/// `Some(delay)` only when the response is genuinely rate limited. A `429` is
/// always a rate limit; a `403` is one ONLY when it carries `Retry-After` or
/// `X-RateLimit-Remaining: 0`. Every other `403` (suspended installation,
/// missing permission, SAML/IP block) is a durable failure and must surface
/// as `Http`, so the worker fails it instead of rescheduling it forever.
fn rate_limit_retry_after(resp: &reqwest::Response) -> Option<std::time::Duration> {
    match resp.status() {
        reqwest::StatusCode::TOO_MANY_REQUESTS => {
            Some(retry_after_from_headers(resp.headers()).unwrap_or(std::time::Duration::from_secs(60)))
        }
        reqwest::StatusCode::FORBIDDEN => retry_after_from_headers(resp.headers()),
        _ => None,
    }
}

fn retry_after_from_headers(headers: &reqwest::header::HeaderMap) -> Option<std::time::Duration> {
    if let Some(v) = headers.get("retry-after").and_then(|v| v.to_str().ok()) {
        if let Ok(secs) = v.parse::<u64>() {
            return Some(std::time::Duration::from_secs(secs));
        }
    }
    if let Some(v) = headers.get("x-ratelimit-remaining").and_then(|v| v.to_str().ok()) {
        if v == "0" {
            if let Some(reset) = headers.get("x-ratelimit-reset").and_then(|v| v.to_str().ok()) {
                if let Ok(reset_epoch) = reset.parse::<i64>() {
                    let now = Utc::now().timestamp();
                    let secs = (reset_epoch - now).max(1) as u64;
                    return Some(std::time::Duration::from_secs(secs));
                }
            }
        }
    }
    None
}

/// Parse a `Link` response header's `rel="next"` target, RFC 8288 shape:
/// `<https://api.github.com/...&page=2>; rel="next", <...>; rel="last"`.
pub(crate) fn parse_next_link(link_header: &str) -> Option<String> {
    for part in link_header.split(',') {
        let part = part.trim();
        let mut segments = part.split(';');
        let url_part = segments.next()?.trim();
        let is_next = segments.any(|s| s.trim() == r#"rel="next""#);
        if is_next {
            let url = url_part.trim_start_matches('<').trim_end_matches('>');
            return Some(url.to_string());
        }
    }
    None
}

/// GET a JSON array from `url`, following every `Link: rel="next"` page
/// until exhausted, up to `max_pages` (a backstop against a pathological or
/// adversarial cursor loop — the same convention `slack.rs`'s own
/// paginators use). `token` is a bearer credential (installation token or
/// App JWT, caller's choice).
pub(crate) async fn get_paginated(
    http: &reqwest::Client,
    token: &str,
    url: &str,
    max_pages: usize,
) -> Result<Vec<serde_json::Value>, GithubApiError> {
    let mut items = Vec::new();
    let mut next_url = Some(url.to_string());
    let mut pages = 0usize;

    while let Some(u) = next_url.take() {
        pages += 1;
        if pages > max_pages {
            break;
        }
        let resp = http
            .get(&u)
            .bearer_auth(token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "coralswarm")
            .send()
            .await
            .map_err(|e| GithubApiError::Transport(e.to_string()))?;

        if let Some(retry_after) = rate_limit_retry_after(&resp) {
            return Err(GithubApiError::RateLimited { retry_after });
        }
        if !resp.status().is_success() {
            let status = resp.status().as_u16();
            let body = resp.text().await.unwrap_or_default();
            return Err(GithubApiError::Http { status, body });
        }

        let link_next = resp
            .headers()
            .get("link")
            .and_then(|v| v.to_str().ok())
            .and_then(parse_next_link);

        let page: serde_json::Value = resp.json().await.map_err(|e| GithubApiError::Parse(e.to_string()))?;
        match page {
            serde_json::Value::Array(arr) => items.extend(arr),
            other => items.push(other),
        }
        next_url = link_next;
    }

    Ok(items)
}

/// Parse a `Link` response header's `rel="last"` target's `page=` query
/// param — the backfill engine's cheapest signal for `total_hint` (§3.6's
/// `backfill_progress` shape): `last_page * per_page` approximates the total
/// item count for a type without a second, dedicated count call.
pub(crate) fn parse_last_page_number(link_header: &str) -> Option<usize> {
    for part in link_header.split(',') {
        let part = part.trim();
        let mut segments = part.split(';');
        let url_part = segments.next()?.trim();
        let is_last = segments.any(|s| s.trim() == r#"rel="last""#);
        if is_last {
            let url = url_part.trim_start_matches('<').trim_end_matches('>');
            let query = url.split('?').nth(1)?;
            for kv in query.split('&') {
                if let Some(page) = kv.strip_prefix("page=") {
                    return page.parse().ok();
                }
            }
        }
    }
    None
}

/// GET exactly ONE page (no automatic follow) — the backfill engine's own
/// pagination primitive, distinct from [`get_paginated`] (which follows
/// every `rel="next"` link to exhaustion): the backfill needs to persist a
/// cursor after EVERY page, which means it must control the page boundary
/// itself. Returns `(items, next_page_url, last_page_number)`.
pub(crate) async fn get_page(
    http: &reqwest::Client,
    token: &str,
    url: &str,
) -> Result<(Vec<serde_json::Value>, Option<String>, Option<usize>), GithubApiError> {
    let resp = http
        .get(url)
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "coralswarm")
        .send()
        .await
        .map_err(|e| GithubApiError::Transport(e.to_string()))?;

    if let Some(retry_after) = rate_limit_retry_after(&resp) {
        return Err(GithubApiError::RateLimited { retry_after });
    }
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(GithubApiError::Http { status: 404, body: String::new() });
    }
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(GithubApiError::Http { status, body });
    }

    let link_header = resp.headers().get("link").and_then(|v| v.to_str().ok()).map(str::to_string);
    let next_url = link_header.as_deref().and_then(parse_next_link);
    let last_page = link_header.as_deref().and_then(parse_last_page_number);

    let page: serde_json::Value = resp.json().await.map_err(|e| GithubApiError::Parse(e.to_string()))?;
    let items = match page {
        serde_json::Value::Array(arr) => arr,
        other => vec![other],
    };
    Ok((items, next_url, last_page))
}

/// GET a single JSON object (no pagination) — installation lookups,
/// single-repo fetches.
pub(crate) async fn get_one(
    http: &reqwest::Client,
    token: &str,
    url: &str,
) -> Result<serde_json::Value, GithubApiError> {
    let resp = http
        .get(url)
        .bearer_auth(token)
        .header("Accept", "application/vnd.github+json")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .header("User-Agent", "coralswarm")
        .send()
        .await
        .map_err(|e| GithubApiError::Transport(e.to_string()))?;

    if let Some(retry_after) = rate_limit_retry_after(&resp) {
        return Err(GithubApiError::RateLimited { retry_after });
    }
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Err(GithubApiError::Http { status: 404, body: String::new() });
    }
    if !resp.status().is_success() {
        let status = resp.status().as_u16();
        let body = resp.text().await.unwrap_or_default();
        return Err(GithubApiError::Http { status, body });
    }
    resp.json().await.map_err(|e| GithubApiError::Parse(e.to_string()))
}

// ---------------------------------------------------------------------------
// Webhook signature verification — HMAC-SHA256, constant-time
// ---------------------------------------------------------------------------

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

/// Verify `X-Hub-Signature-256` (format `sha256=<hex>`) over the RAW request
/// body, constant-time — mirrors `routes::slack_events::
/// verify_slack_signature_with_secret`'s M1-hardened shape (decode hex to a
/// fixed-size array first, compare with `subtle::ConstantTimeEq`, no
/// early-return branch that leaks length via timing). GitHub's scheme has no
/// timestamp component (unlike Slack's `v0:ts:body`), so there is no replay
/// window to also check here.
pub(crate) fn verify_webhook_signature(secret: &str, body: &[u8], signature_header: &str) -> bool {
    use hmac::Mac;
    use subtle::ConstantTimeEq;

    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return false,
    };
    mac.update(body);
    let computed: [u8; 32] = mac.finalize().into_bytes().into();

    let Some(hex_part) = signature_header.strip_prefix("sha256=") else {
        return false;
    };
    let received: [u8; 32] = match hex::decode(hex_part) {
        Ok(v) if v.len() == 32 => {
            let mut arr = [0u8; 32];
            arr.copy_from_slice(&v);
            arr
        }
        _ => return false,
    };
    computed.ct_eq(&received).into()
}

// ---------------------------------------------------------------------------
// connector_installs / github_installations bookkeeping
// ---------------------------------------------------------------------------

/// Learn about an installation from its id alone, via `GET
/// /app/installations/{id}` under the App JWT — the path the design doc
/// requires for the case where the backend hears about an installation only
/// through the selection API (GitHub's Setup URL round trip), with no prior
/// OAuth callback for it. Returns `(account_login, account_type,
/// suspended_at)`.
pub(crate) async fn fetch_installation(
    http: &reqwest::Client,
    installation_id: i64,
) -> Result<InstallationInfo, ConnectorError> {
    let jwt = mint_app_jwt().await?;
    let url = format!("{}/app/installations/{installation_id}", github_api_base());
    let body = get_one(http, &jwt, &url).await?;
    Ok(InstallationInfo::from_json(&body))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct InstallationInfo {
    pub(crate) account_login: String,
    pub(crate) account_type: String,
    pub(crate) suspended: bool,
}

impl InstallationInfo {
    fn from_json(v: &serde_json::Value) -> Self {
        let account_login = v
            .get("account")
            .and_then(|a| a.get("login"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let account_type = v
            .get("account")
            .and_then(|a| a.get("type"))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string();
        let suspended = v.get("suspended_at").is_some_and(|s| !s.is_null());
        InstallationInfo { account_login, account_type, suspended }
    }
}

/// Upsert the control-plane routing index (`connector_installs`,
/// `provider = 'github'`, `external_workspace_id = installation_id` as text
/// — docs/connectors/github.md §3.3) AND the GitHub-specific side table
/// (`github_installations`, this connector's `slack_integrations` analog —
/// see migration `0231`'s own header for why a second table exists at all).
/// Both writes happen in one transaction: a routing row with no metadata row
/// (or vice versa) is a half-recorded installation, exactly the failure mode
/// `routes::oauth::callback`'s own `record_connector_install` avoids for
/// every other provider.
///
/// The `github_installations` upsert is checked for ownership BEFORE the
/// `connector_installs` routing row is written. A second CoralSwarm account
/// that presents the same GitHub `installation_id` is refused so the two
/// tables cannot disagree on who owns the install.
pub(crate) async fn record_installation(
    control_pool: &PgPool,
    account_id: Uuid,
    installation_id: i64,
    info: &InstallationInfo,
    connected_by: Option<Uuid>,
) -> Result<(), RecordInstallationError> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("record_installation scoped tx: {e}")))?;

    let owner: Option<Uuid> = sqlx::query_scalar(
        r#"
        INSERT INTO github_installations (account_id, installation_id, account_login, account_type, connected_by, suspended_at)
        VALUES ($1, $2, $3, $4, $5, CASE WHEN $6 THEN now() ELSE NULL END)
        ON CONFLICT (installation_id) DO UPDATE SET
            account_login = EXCLUDED.account_login,
            account_type  = EXCLUDED.account_type,
            suspended_at  = CASE WHEN $6 THEN COALESCE(github_installations.suspended_at, now()) ELSE NULL END,
            updated_at    = now()
        WHERE github_installations.account_id = EXCLUDED.account_id
        RETURNING account_id
        "#,
    )
    .bind(account_id)
    .bind(installation_id)
    .bind(&info.account_login)
    .bind(&info.account_type)
    .bind(connected_by)
    .bind(info.suspended)
    .fetch_optional(&mut *tx)
    .await?;

    match owner {
        Some(existing) if existing == account_id => {}
        Some(existing_account_id) => {
            return Err(RecordInstallationError::OwnedByOtherAccount { existing_account_id });
        }
        None => {
            let existing_account_id: Option<Uuid> = sqlx::query_scalar(
                "SELECT account_id FROM github_installations WHERE installation_id = $1",
            )
            .bind(installation_id)
            .fetch_optional(&mut *tx)
            .await?;
            if let Some(existing_account_id) = existing_account_id {
                if existing_account_id != account_id {
                    return Err(RecordInstallationError::OwnedByOtherAccount { existing_account_id });
                }
            }
            return Err(RecordInstallationError::Sql(sqlx::Error::Protocol(
                "github_installations upsert returned no row".into(),
            )));
        }
    }

    crate::routes::oauth::record_connector_install(
        &mut tx,
        account_id,
        "github",
        &installation_id.to_string(),
        Some(&info.account_login),
        None,
    )
    .await?;

    tx.commit().await?;
    Ok(())
}

#[derive(Debug)]
pub(crate) enum RecordInstallationError {
    Sql(sqlx::Error),
    OwnedByOtherAccount { existing_account_id: Uuid },
}

impl From<sqlx::Error> for RecordInstallationError {
    fn from(e: sqlx::Error) -> Self {
        RecordInstallationError::Sql(e)
    }
}

impl std::fmt::Display for RecordInstallationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordInstallationError::Sql(e) => write!(f, "{e}"),
            RecordInstallationError::OwnedByOtherAccount { existing_account_id } => {
                write!(f, "installation already owned by account {existing_account_id}")
            }
        }
    }
}

/// I7: load the GitHub user-to-server token ciphertext for exactly one
/// [`TokenOwner::Member`]. Never account-wide, never a fallback read.
pub(crate) async fn load_member_oauth_token_ciphertext(
    pool: &PgPool,
    account_id: Uuid,
    owner: TokenOwner,
) -> Result<Option<Vec<u8>>, sqlx::Error> {
    let Some(owner_user_id) = owner.owner_user_id() else {
        return Ok(None);
    };
    let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("load_member_oauth_token_ciphertext scoped tx: {e}")))?;
    let row: Option<Vec<u8>> = sqlx::query_scalar(
        "SELECT access_token FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'github' AND owner_user_id = $2 \
         ORDER BY updated_at DESC LIMIT 1",
    )
    .bind(account_id)
    .bind(owner_user_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();
    Ok(row)
}

/// Resolve `(account_id, suspended)` for an inbound webhook's
/// `installation.id` — I8's tenant-resolution step, before ANY tenant-plane
/// data is touched. No RLS on `github_installations` (migration `0231`'s own
/// header — same "resolved before any scope exists" posture as
/// `connector_installs`), so this is a plain, unscoped lookup by design.
pub(crate) async fn resolve_installation(
    control_pool: &PgPool,
    installation_id: i64,
) -> Result<Option<(Uuid, bool)>, sqlx::Error> {
    let row: Option<(Uuid, Option<DateTime<Utc>>)> = sqlx::query_as(
        "SELECT account_id, suspended_at FROM github_installations WHERE installation_id = $1",
    )
    .bind(installation_id)
    .fetch_optional(control_pool)
    .await?;
    Ok(row.map(|(account_id, suspended_at)| (account_id, suspended_at.is_some())))
}

/// Mark an installation suspended/unsuspended, and (for `deleted`) drop the
/// routing row entirely — `installation.deleted` means the App no longer has
/// ANY reach into this account, so leaving a `connector_installs` row behind
/// would let a resurrected installation id (GitHub does not reuse ids, but a
/// stale row is still dead weight) keep resolving. `github_watched_repos`
/// rows are paused separately (§3.8 never-purge) by the caller — this
/// function only owns the two installation-level tables.
pub(crate) async fn set_installation_lifecycle(
    control_pool: &PgPool,
    installation_id: i64,
    lifecycle: InstallationLifecycle,
) -> Result<Option<Uuid>, sqlx::Error> {
    match lifecycle {
        InstallationLifecycle::Suspended => {
            let account_id: Option<Uuid> = sqlx::query_scalar(
                "UPDATE github_installations SET suspended_at = now(), updated_at = now() \
                 WHERE installation_id = $1 RETURNING account_id",
            )
            .bind(installation_id)
            .fetch_optional(control_pool)
            .await?;
            invalidate_installation_token(installation_id);
            Ok(account_id)
        }
        InstallationLifecycle::Unsuspended => {
            let account_id: Option<Uuid> = sqlx::query_scalar(
                "UPDATE github_installations SET suspended_at = NULL, updated_at = now() \
                 WHERE installation_id = $1 RETURNING account_id",
            )
            .bind(installation_id)
            .fetch_optional(control_pool)
            .await?;
            Ok(account_id)
        }
        InstallationLifecycle::Deleted => {
            let account_id: Option<Uuid> = sqlx::query_scalar(
                "DELETE FROM github_installations WHERE installation_id = $1 RETURNING account_id",
            )
            .bind(installation_id)
            .fetch_optional(control_pool)
            .await?;
            if let Some(account_id) = account_id {
                sqlx::query(
                    "DELETE FROM connector_installs WHERE provider = 'github' AND external_workspace_id = $1 AND account_id = $2",
                )
                .bind(installation_id.to_string())
                .bind(account_id)
                .execute(control_pool)
                .await?;
            }
            invalidate_installation_token(installation_id);
            Ok(account_id)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum InstallationLifecycle {
    Suspended,
    Unsuspended,
    Deleted,
}

// ---------------------------------------------------------------------------
// ConnectorProvider
// ---------------------------------------------------------------------------

pub struct GithubConnector;

#[derive(Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    scope: String,
    #[serde(default)]
    expires_in: Option<i64>,
}

#[derive(Deserialize)]
struct GithubUser {
    login: String,
    id: i64,
}

#[async_trait]
impl ConnectorProvider for GithubConnector {
    fn id(&self) -> &'static str {
        "github"
    }

    fn display_name(&self) -> &'static str {
        "GitHub"
    }

    /// The six source types §3.5 registers. Only the first four are still
    /// PRODUCED: the connector stopped capturing repository source code on
    /// 2026-10-27 (issue #1516), so nothing writes a new `github_code` or
    /// `github_code_summary` atom. Both stay RECOGNISED here because the
    /// atoms written before then are kept, and every downstream switch site
    /// that keys off `source_types()` must still render them.
    fn source_types(&self) -> &'static [&'static str] {
        &[
            "github_pr",
            "github_issue",
            "github_release",
            "github_commit",
            "github_code",
            "github_code_summary",
        ]
    }

    /// The App-install URL (§3.1/§4) — `state` round-trips through GitHub's
    /// own install flow and returns on the shared `/v1/oauth/github/callback`
    /// with `code`, `installation_id`, `setup_action`, and this same `state`.
    fn authorize_url(&self, _base_url: &str, state: &str) -> Result<String, ConnectorError> {
        let slug = app_slug()?;
        Ok(format!(
            "{}/apps/{}/installations/new?state={}",
            github_web_base(),
            urlencoding::encode(&slug),
            urlencoding::encode(state),
        ))
    }

    /// Exchange the user-to-server `code` for an access token, then resolve
    /// the connecting GitHub user's `login`/`id` (`GET /user`) so the
    /// generic `oauth_tokens`/`connector_installs` write has a
    /// `provider_workspace_id`/`provider_user_id` to key on. Unlike Slack's
    /// team-scoped workspace id, a GitHub user token has no single
    /// "workspace" — the connecting user's own login is the closest analog,
    /// and is never used for I8 tenant routing (that is `installation_id`,
    /// recorded separately by [`record_installation`]).
    async fn exchange_code(
        &self,
        http: &reqwest::Client,
        base_url: &str,
        code: &str,
    ) -> Result<TokenBundle, ConnectorError> {
        let client_id = client_id()?;
        let client_secret = client_secret()?;
        let redirect_uri = format!("{base_url}/v1/oauth/github/callback");

        let resp = http
            .post(format!("{}/login/oauth/access_token", github_web_base()))
            .header("Accept", "application/json")
            .form(&[
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("code", code),
                ("redirect_uri", redirect_uri.as_str()),
            ])
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("github token exchange transport error: {e}")))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::Exchange(format!("github token exchange failed: {status} {body}")));
        }

        let parsed: OAuthTokenResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("github token exchange response was not valid JSON: {e}")))?;

        let user = http
            .get(format!("{}/user", github_api_base()))
            .bearer_auth(&parsed.access_token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .header("User-Agent", "coralswarm")
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("github /user transport error: {e}")))?;
        if !user.status().is_success() {
            let status = user.status();
            return Err(ConnectorError::Exchange(format!("github /user failed: {status}")));
        }
        let user: GithubUser = user
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("github /user response was not valid JSON: {e}")))?;

        Ok(TokenBundle {
            access_token: parsed.access_token,
            refresh_token: parsed.refresh_token,
            user_token: None,
            scope: parsed.scope,
            expires_at: parsed.expires_in.map(|secs| Utc::now() + chrono::Duration::seconds(secs)),
            provider_workspace_id: user.login.clone(),
            provider_workspace_name: Some(user.login),
            provider_user_id: user.id.to_string(),
        })
    }

    // `post_store` stays the default no-op: recording the INSTALLATION
    // (as opposed to the per-member OAuth token this method would otherwise
    // mirror `slack_integrations` for) needs `installation_id`/
    // `setup_action` off the raw callback query string, which this trait
    // method's signature does not carry (see `ConnectorProvider::post_store`'s
    // own doc for why — every other provider's side effect fits inside
    // `bundle`). `routes::oauth::callback` calls [`record_installation`]
    // directly instead, right after the generic write commits — see that
    // call site's own comment.
}

/// Serializes every test in the CRATE that mutates the process-global GitHub
/// env vars (`GITHUB_API_BASE`, `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, …).
///
/// Crate-visible on purpose rather than private to this module's own `tests`:
/// `connectors::github_conformance` sets the SAME vars, and both modules
/// compile into one `--lib` test binary that the harness runs concurrently,
/// so two separate mutexes would serialize neither against the other. Same
/// convention as `routes::slack_events`'s `slack_signing_secret_lock` and the
/// ENV_LOCK `connectors::slack` documents.
#[cfg(test)]
pub(crate) fn test_env_lock() -> &'static std::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| std::sync::Mutex::new(()))
}

/// A freshly generated 2048-bit RSA key in PKCS#1 PEM form — throwaway test
/// material, never a real App key (the real ones live only in gitignored
/// `.pem` files). Crate-visible so `connectors::github_conformance` can mint
/// an App JWT against its mock GitHub with the same material this module's
/// own JWT-claims test uses.
#[cfg(test)]
pub(crate) fn test_throwaway_rsa_pem() -> String {
    use rsa::pkcs1::EncodeRsaPrivateKey;
    let mut rng = rand::thread_rng();
    let key = rsa::RsaPrivateKey::new(&mut rng, 2048).expect("generate throwaway RSA key");
    key.to_pkcs1_pem(rsa::pkcs1::LineEnding::LF).expect("encode PEM").to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── env / NotConfigured ──────────────────────────────────────────────

    fn clear_env() {
        for k in [
            "GITHUB_APP_ID",
            "GITHUB_APP_PRIVATE_KEY",
            "GITHUB_APP_PRIVATE_KEY_FILE",
            "GITHUB_CLIENT_ID",
            "GITHUB_CLIENT_SECRET",
            "GITHUB_WEBHOOK_SECRET",
            "GITHUB_APP_SLUG",
        ] {
            unsafe { std::env::remove_var(k) };
        }
    }

    /// Serializes every test in this module that touches process-global env
    /// vars — mirrors `routes::slack_events`'s own `slack_signing_secret_lock`
    /// convention (this crate has no per-test env sandbox).
    fn env_lock() -> &'static std::sync::Mutex<()> {
        super::test_env_lock()
    }

    #[test]
    fn authorize_url_missing_slug_is_not_configured() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        let connector = GithubConnector;
        let err = connector.authorize_url("http://x", "state").unwrap_err();
        assert!(matches!(err, ConnectorError::NotConfigured(_)));
    }

    #[test]
    fn authorize_url_builds_the_app_install_url() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        unsafe { std::env::set_var("GITHUB_APP_SLUG", "coralswarm-dev") };
        let connector = GithubConnector;
        let url = connector.authorize_url("http://x", "abc123").unwrap();
        assert_eq!(url, "https://github.com/apps/coralswarm-dev/installations/new?state=abc123");
        clear_env();
    }

    #[tokio::test]
    async fn mint_app_jwt_missing_app_id_is_not_configured() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        let err = mint_app_jwt().await.unwrap_err();
        assert!(matches!(err, ConnectorError::NotConfigured(_)));
    }

    // ── App JWT claims, against a throwaway test RSA key (NEVER the real
    // PEM) — proves iss/exp-iat/alg without touching any real secret. ─────

    /// A 2048-bit RSA key (jsonwebtoken's own RSA validation rejects
    /// anything smaller — `TooSmall`), generated fresh by THIS test process
    /// via `rsa`'s pure-Rust keygen. Not committed anywhere; exists only for
    /// the lifetime of this test.
    fn throwaway_rsa_pem() -> String {
        super::test_throwaway_rsa_pem()
    }

    #[tokio::test]
    async fn app_jwt_claims_iss_exp_iat_and_alg_pg_free() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        unsafe {
            std::env::set_var("GITHUB_APP_ID", "123456");
            std::env::set_var("GITHUB_APP_PRIVATE_KEY", throwaway_rsa_pem());
        }

        let before = Utc::now().timestamp();
        let jwt = mint_app_jwt().await.expect("mint jwt with throwaway key");
        let after = Utc::now().timestamp();

        let header = jsonwebtoken::decode_header(&jwt).expect("decode header");
        assert_eq!(header.alg, jsonwebtoken::Algorithm::RS256);

        // Decode claims without signature verification (we don't have the
        // public key handy in this shape) — just parse the middle segment.
        let parts: Vec<&str> = jwt.split('.').collect();
        assert_eq!(parts.len(), 3);
        use base64::Engine;
        let claims_json = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(parts[1])
            .expect("base64 decode claims");
        let claims: serde_json::Value = serde_json::from_slice(&claims_json).expect("parse claims json");

        assert_eq!(claims["iss"], "123456");
        let iat = claims["iat"].as_i64().unwrap();
        let exp = claims["exp"].as_i64().unwrap();
        assert_eq!(exp - iat, APP_JWT_TTL_SECS, "exp - iat must be exactly the 10-minute TTL");
        // iat is skewed ~60s into the past relative to when we minted it.
        assert!(iat <= before - APP_JWT_IAT_SKEW_SECS + 1);
        assert!(iat >= after - APP_JWT_IAT_SKEW_SECS - 5);

        clear_env();
    }

    #[tokio::test]
    async fn app_private_key_file_is_read_at_call_time() {
        let _guard = env_lock().lock().unwrap();
        clear_env();
        let dir = std::env::temp_dir();
        let path = dir.join(format!("coralswarm-github-test-{}.pem", Uuid::new_v4()));
        std::fs::write(&path, throwaway_rsa_pem()).unwrap();
        unsafe {
            std::env::set_var("GITHUB_APP_ID", "999");
            std::env::set_var("GITHUB_APP_PRIVATE_KEY_FILE", path.to_str().unwrap());
        }
        let jwt = mint_app_jwt().await;
        assert!(jwt.is_ok(), "should mint successfully reading the PEM from disk: {jwt:?}");
        std::fs::remove_file(&path).ok();
        clear_env();
    }

    // ── webhook signature verification ──────────────────────────────────

    #[test]
    fn webhook_signature_valid_is_accepted() {
        use hmac::Mac;
        let secret = "test-webhook-secret";
        let body = b"{\"zen\":\"hello\"}";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = hex::encode(mac.finalize().into_bytes());
        let header = format!("sha256={sig}");
        assert!(verify_webhook_signature(secret, body, &header));
    }

    #[test]
    fn webhook_signature_tampered_body_is_rejected() {
        use hmac::Mac;
        let secret = "test-webhook-secret";
        let body = b"{\"zen\":\"hello\"}";
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        let sig = hex::encode(mac.finalize().into_bytes());
        let header = format!("sha256={sig}");
        let tampered_body = b"{\"zen\":\"goodbye\"}";
        assert!(!verify_webhook_signature(secret, tampered_body, &header));
    }

    #[test]
    fn webhook_signature_missing_prefix_is_rejected() {
        assert!(!verify_webhook_signature("secret", b"body", "not-a-real-signature"));
    }

    #[test]
    fn webhook_signature_wrong_secret_is_rejected() {
        use hmac::Mac;
        let body = b"payload";
        let mut mac = HmacSha256::new_from_slice(b"secret-a").unwrap();
        mac.update(body);
        let sig = hex::encode(mac.finalize().into_bytes());
        let header = format!("sha256={sig}");
        assert!(!verify_webhook_signature("secret-b", body, &header));
    }

    // ── Link header pagination ───────────────────────────────────────────

    #[test]
    fn parse_next_link_extracts_the_next_url() {
        let header = r#"<https://api.github.com/installation/repositories?page=2>; rel="next", <https://api.github.com/installation/repositories?page=5>; rel="last""#;
        assert_eq!(
            parse_next_link(header),
            Some("https://api.github.com/installation/repositories?page=2".to_string())
        );
    }

    #[test]
    fn parse_next_link_no_next_returns_none() {
        let header = r#"<https://api.github.com/installation/repositories?page=1>; rel="last""#;
        assert_eq!(parse_next_link(header), None);
    }

    #[test]
    fn parse_last_page_number_extracts_the_last_page() {
        let header = r#"<https://api.github.com/repos/acme/repo/pulls?page=2>; rel="next", <https://api.github.com/repos/acme/repo/pulls?per_page=25&page=14>; rel="last""#;
        assert_eq!(parse_last_page_number(header), Some(14));
    }

    #[test]
    fn parse_last_page_number_no_last_returns_none() {
        let header = r#"<https://api.github.com/repos/acme/repo/pulls?page=2>; rel="next""#;
        assert_eq!(parse_last_page_number(header), None);
    }

    // ── InstallationInfo parsing ─────────────────────────────────────────

    #[test]
    fn installation_info_parses_suspended() {
        let v = serde_json::json!({
            "account": {"login": "acme", "type": "Organization"},
            "suspended_at": "2026-01-01T00:00:00Z",
        });
        let info = InstallationInfo::from_json(&v);
        assert_eq!(info.account_login, "acme");
        assert_eq!(info.account_type, "Organization");
        assert!(info.suspended);
    }

    #[test]
    fn installation_info_parses_active() {
        let v = serde_json::json!({
            "account": {"login": "someone", "type": "User"},
            "suspended_at": null,
        });
        let info = InstallationInfo::from_json(&v);
        assert!(!info.suspended);
    }
}
