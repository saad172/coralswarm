//! Google Cloud Pub/Sub push webhook (gsuite Train G1 PR 2,
//! `.scratch/gsuite-enterprise/plan.md` decision 7).
//!
//! Public endpoint (`/v1/webhooks/google`) that backs the push half of every
//! Google Workspace change-detection path: Gmail `users.watch` and Chat's
//! Workspace Events API subscriptions both publish onto one of our Pub/Sub
//! topics (`gmail-push` / `chat-events`, per `scripts/
//! provision_google_workspace.sh`), and Pub/Sub delivers them here as a push
//! subscription. (Calendar's `events.watch` webhook channels push straight to
//! us with no Pub/Sub involved — a different envelope shape and verification
//! story — and are NOT handled by this route; see
//! `docs/connectors/google-workspace.md` for that split once it lands.)
//!
//! # Authentication
//!
//! Pub/Sub push carries no shared secret of its own (unlike Graph's
//! `clientState` — see `routes::graph_webhook`'s module doc). Instead, the
//! push subscription is configured with an OIDC token: Pub/Sub mints a
//! Google-signed `Authorization: Bearer` JWT for every push, asserting the
//! configured service account's identity and audience. [`verify_push_token`]
//! checks:
//!
//! * signature, against Google's own JWKS (cached; see [`fetch_certs`]),
//! * `iss` is `accounts.google.com` or `https://accounts.google.com` (Google
//!   emits either form depending on token vintage),
//! * `aud` matches [`PushConfig::audience`] (configured on the subscription
//!   at provisioning time — see the script above),
//! * `email` matches [`PushConfig::service_account_email`] AND
//!   `email_verified` is true — an unverified or wrong-SA token is rejected
//!   even if it otherwise verifies, so a differently-scoped Google-signed
//!   token (e.g. minted for some other Google API) cannot be replayed here.
//!
//! A token that fails any of these is a straight 401/403 — this is the
//! entire trust boundary for this endpoint (no Clerk JWT, no `Tenant`
//! extractor; mirrors `routes::graph_webhook`/`routes::teams_bot` in being a
//! signature-authenticated public webhook, not a session-authenticated one).
//!
//! # Never unbounded-redeliver
//!
//! Pub/Sub retries a push until it gets a 2xx (per the subscription's retry
//! policy) — same redelivery hazard `routes::graph_webhook`'s module doc
//! flags for Graph. So: a verified-but-malformed body is logged and ACKed
//! (200), never a 5xx, exactly like `graph_notifications` does for an
//! unparseable Graph batch. Only an AUTH failure (bad/missing/wrong-audience
//! token) gets a non-2xx — and Pub/Sub's retry backoff on a 401/403 is its
//! problem to back off, not ours to avoid by faking success.
//!
//! # Dispatch (gsuite Train G2 PR 3)
//!
//! A verified, well-formed push is handed to [`dispatch`], which routes on
//! `envelope.subscription` — the surface discriminator
//! `scripts/provision_google_workspace.sh:220-241` documents (`gmail-push-
//! sub` and `chat-events-sub`, both pointing at this one endpoint):
//!
//! * **Gmail** (`gmail-push-sub`): [`decode_gmail_push_data`] the base64
//!   `message.data` into `{emailAddress, historyId}`. The `historyId` here
//!   is a TRIGGER ONLY, never trusted as a cursor —
//!   `connectors::google_gmail`'s own `sync_token` (the stored, durable
//!   `historyId`) is authoritative; this notification's job is only to say
//!   "something changed, go diff from where you left off" (module doc,
//!   `connectors::google_gmail`, "Live follow"). `emailAddress` resolves
//!   EVERY watching `(account_id, owner_user_id)` via `connectors::
//!   google_gmail::resolve_watch_owners_for_push` (ids only, never secrets —
//!   Pub/Sub push has no per-notification shared secret to read back; the
//!   OIDC bearer token above is the entire trust boundary) and
//!   [`run_gmail_sync_for_mailbox`] runs one pass per account on that
//!   account's own plane (org tenant pool for an org account —
//!   `crypto::DekPlane` decides). [`ingest_and_commit`] then
//!   fetches + ingests, committing the cursor ONLY on full success — same
//!   commit-after-ingest discipline `routes::google_calendar_push::
//!   ingest_and_commit` established (422 = a deterministic per-item
//!   rejection, consumed rather than blocking; anything else blocks the
//!   commit so the next pass re-diffs the same range).
//! * **Chat messages** (`chat-events-sub`): stays a log-only stub — the
//!   Workspace Events message decode is G4.4's, not this PR's.
//! * **Chat app interaction events** (`chat-app-events-sub`, gsuite Train
//!   G4 PR 3): a SEPARATE delivery channel from Workspace Events (design
//!   doc §1.4, §3.1's D6) — Chat's own app-installed/removed/interaction
//!   JSON, not a CloudEvent. `ADDED_TO_SPACE` creates an UNROUTED
//!   `google_chat_watched_spaces` row (reading `spaces.get` for
//!   `spaceType`/`historyState` with the adding member's own token if
//!   they're already connected; if they are not, this DEFERS — no row is
//!   created, and nothing retries it automatically today; a later
//!   connected member's own action against the space is what completes
//!   it, a known G4.3-scope boundary named in the PR body).
//!   `REMOVED_FROM_SPACE` deactivates EVERY account's row for the space —
//!   resolved by the SPACE itself (`connectors::google_chat::
//!   resolve_chat_space_accounts`, D8), never by the removing member's own
//!   identity, which is the common case where nobody can be identified at
//!   all. Every other interaction type (`APP_COMMAND`,
//!   `APP_HOME`, `CARD_CLICKED`, …) is logged and dropped — G4.5's App Home
//!   fast-follow, not this PR's.
//!
//!   **Two wire envelopes, one dispatch.** The CoralSwarm Chat app is
//!   configured as a Google **Workspace add-on** ("Build this Chat app as
//!   a Workspace add-on" on the Chat API configuration page) — a setting
//!   that is now locked ON (its own tooltip: "Currently disabled."), so it
//!   cannot be reverted. The add-on framework emits a DIFFERENT
//!   `ADDED_TO_SPACE`/`REMOVED_FROM_SPACE` envelope than the classic Chat
//!   app framework this module was originally written against — no `type`
//!   field at all, the interaction nested under `chat`, and the kind
//!   carried as WHICH of `chat.addedToSpacePayload`/
//!   `chat.removedFromSpacePayload` is present, not a value. Production
//!   dropped every one of these until [`decode_chat_interaction_event`]
//!   learned both shapes — see its doc comment and
//!   `backend/tests/fixtures/google_chat_addon/` for the real captures.
//! * **Anything else**: logged and dropped — never a non-2xx (see "Never
//!   unbounded-redeliver" above; an unrecognized-but-verified subscription
//!   is malformed-body territory, not an auth failure).
//!
//! # Receive stage (ADR-0016)
//!
//! The route itself runs none of the above. A verified push is written as one
//! durable `JobKind::IngestReceive` job and answered 200; `workers::
//! ingest_receive` runs the Gmail pass or [`dispatch`] from it. A Gmail push
//! is held as its mailbox (`ReceivedEvent::GmailPush { email_address }`, in
//! `jobs.payload` until the job settles, then scrubbed; a row the reaper
//! dead-letters is scrubbed by the reaper), keyed by an HMAC of the address
//! ([`gmail_mailbox_key`]) so that pushes for one mailbox coalesce while a
//! pass is pending or waiting on backoff, without the address appearing in
//! the key column, which is never redacted. A transient failure (a 5xx, a
//! timeout, our own storage) retries on the queue's backoff; a permanent one
//! (a 4xx a retry cannot change, an envelope that no longer decodes)
//! completes the job, so a Chat or Meet push is as durable as a Gmail one.
//! If the job cannot be written the push answers 503, so Pub/Sub redelivers
//! it rather than it being lost.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
};
use futures::future::BoxFuture;
use jsonwebtoken::{decode, decode_header, Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use sqlx::PgPool;
use tokio::sync::RwLock;
use uuid::Uuid;

use crate::{
    calendar_pass_gate::CalendarPassGate,
    connectors::{google_chat, google_gmail, ConnectorError, GoogleConnector, TokenOwner},
    jobs::ReceivedEvent,
    workers::ingest_receive::{plane_error, FanOut, ReceiveError},
    ingest::atom::IngestAtomRequest,
    middleware::auth::UserContext,
    routes::{
        connectors::load_creds_bg,
        oauth::store_token_bundle,
        slack::{resolve_primary_graph, AccountPlane, IngestCaller},
    },
    store::{PostgresStore, Store},
    tenancy::extractor::Tenant,
    tenancy::runtime::{TenancyDecision, TenantContext},
    AppState,
};

/// Google's OIDC signing-cert JWKS. Overridable via `GOOGLE_OIDC_CERTS_URL`
/// for tests / a future cert rotation; Google itself documents this exact
/// endpoint for verifying `service-account`-issued OIDC tokens (distinct from
/// `oauth2/v1/certs`, which serves Google's *user* auth certs in a different
/// format).
const DEFAULT_CERTS_URL: &str = "https://www.googleapis.com/oauth2/v3/certs";

/// JWKS cache TTL — Google rotates these keys infrequently. Mirrors
/// `clerk::JWKS_TTL`'s value and rationale exactly (a cache miss for an
/// unknown `kid` forces one off-cycle refresh regardless — see [`fetch_certs`]).
const CERTS_TTL: Duration = Duration::from_secs(600);

/// Accepted `iss` values — Google's OIDC tokens use either form depending on
/// minting path/vintage; both are Google, both are accepted.
const ACCEPTED_ISSUERS: [&str; 2] = ["accounts.google.com", "https://accounts.google.com"];

// ── Config ───────────────────────────────────────────────────────────────────

/// Push verification config, read from the environment. Both vars are
/// required for this route to accept anything — see [`PushConfig::from_env`].
#[derive(Debug, Clone)]
pub struct PushConfig {
    /// Expected `aud` claim — the audience configured on the Pub/Sub push
    /// subscription's `oidcToken.audience` at provisioning time (see
    /// `scripts/provision_google_workspace.sh`). Conventionally the webhook's
    /// own URL, but Pub/Sub accepts any string here, so this is read from env
    /// rather than derived from the request.
    pub audience: String,
    /// Expected `email` claim — the service account Pub/Sub mints push OIDC
    /// tokens as (`oidcToken.serviceAccountEmail`).
    pub service_account_email: String,
    pub certs_url: String,
}

impl PushConfig {
    /// `None` when either required var is unset — the caller then rejects
    /// every push with 401 rather than guessing a default (there is no safe
    /// default audience/service-account to fall back to).
    pub fn from_env() -> Option<Self> {
        let audience = std::env::var("GOOGLE_PUSH_AUDIENCE").ok()?;
        let service_account_email = std::env::var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL").ok()?;
        if audience.trim().is_empty() || service_account_email.trim().is_empty() {
            return None;
        }
        let certs_url =
            std::env::var("GOOGLE_OIDC_CERTS_URL").unwrap_or_else(|_| DEFAULT_CERTS_URL.to_string());
        Some(Self {
            audience,
            service_account_email,
            certs_url,
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PushAuthError {
    #[error("missing or malformed Authorization header")]
    MissingToken,
    #[error("push verification not configured")]
    NotConfigured,
    #[error("certs fetch failed: {0}")]
    CertsFetch(String),
    #[error("no signing key for kid")]
    UnknownKid,
    #[error("invalid token: {0}")]
    InvalidToken(String),
    /// Signature/issuer/expiry all checked out, but `aud` or `email` didn't
    /// match the configured expectation — a differently-scoped Google token,
    /// not a forgery. Distinguished from [`PushAuthError::InvalidToken`] so
    /// the route can answer 403 (authenticated as *something*, but not
    /// authorized) rather than 401.
    #[error("token identity mismatch: {0}")]
    IdentityMismatch(String),
}

// ── Claims ───────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Deserialize)]
struct PushClaims {
    // `iss`/`exp` are enforced by `jsonwebtoken::decode`'s `Validation`
    // (issuer allowlist + expiry) at decode time — they exist on this struct
    // only so deserialization can see them, and are otherwise unread
    // (`iss` IS read back out in this module's own tests, to assert which
    // accepted issuer form a token used; `exp` never needs to be, since
    // `validate_exp` is the sole consumer of it).
    #[allow(dead_code)]
    iss: String,
    aud: String,
    #[allow(dead_code)]
    exp: usize,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    email_verified: Option<bool>,
}

// ── JWKS fetch + cache (mirrors clerk.rs's cache shape) ─────────────────────

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

fn parse_jwks(body: &str) -> Result<HashMap<String, DecodingKey>, PushAuthError> {
    let jwks: Jwks = serde_json::from_str(body).map_err(|e| PushAuthError::CertsFetch(e.to_string()))?;
    let mut out = HashMap::new();
    for k in jwks.keys {
        match DecodingKey::from_rsa_components(&k.n, &k.e) {
            Ok(key) => {
                out.insert(k.kid, key);
            }
            Err(e) => tracing::warn!(kid = %k.kid, error = %e, "skipping malformed Google JWKS key"),
        }
    }
    if out.is_empty() {
        return Err(PushAuthError::CertsFetch("no usable RSA keys in Google JWKS".into()));
    }
    Ok(out)
}

struct CachedCerts {
    keys: HashMap<String, DecodingKey>,
    fetched: Instant,
}

fn cache() -> &'static RwLock<Option<CachedCerts>> {
    static CACHE: OnceLock<RwLock<Option<CachedCerts>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(None))
}

/// Minimum time between off-cycle (unknown-`kid`) forced cert refetches.
/// Much shorter than [`CERTS_TTL`] — a genuine Google key rotation should
/// still be picked up promptly — but bounds how often an UNAUTHENTICATED
/// caller can make this route fetch from `googleapis.com`: a `kid` is read
/// straight out of the token's header before any signature check, so an
/// attacker presenting a stream of distinct, made-up `kid`s could otherwise
/// force one outbound refetch per request (CodeRabbit review finding on PR
/// #674). Combined with [`certs_fetch_lock`]'s single-flight, a flood of
/// such requests collapses to at most one outbound call per cooldown window,
/// and every other concurrent/rapid caller lands on [`PushAuthError::
/// UnknownKid`] without triggering a fetch of its own.
const FORCE_REFRESH_COOLDOWN: Duration = Duration::from_secs(30);

/// Single-flight guard for [`fetch_certs`], shared by BOTH refresh paths —
/// the normal cache-miss/TTL-expiry fetch (`force = false`) and the
/// off-cycle unknown-`kid` forced refetch (`force = true`). Holds the last
/// time a fetch actually ran (`None` = never), consumed by
/// [`force_refresh_due`] for the force path's cooldown. Held across the
/// fetch itself for BOTH paths, so concurrent callers of either collapse
/// onto one outbound call rather than each firing their own — round 2
/// CodeRabbit finding on PR #674: round 1 only guarded the force path,
/// leaving a cold-cache/TTL-expiry thundering herd across concurrent
/// NORMAL-path callers (every request arriving while the cache is empty or
/// just past its 10-minute TTL would otherwise launch its own fetch).
fn certs_fetch_lock() -> &'static tokio::sync::Mutex<Option<Instant>> {
    static LOCK: OnceLock<tokio::sync::Mutex<Option<Instant>>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(None))
}

/// Pure decision — testable without any I/O or shared state — for whether a
/// forced refresh is due given the last time one ran.
fn force_refresh_due(last: Option<Instant>, now: Instant, cooldown: Duration) -> bool {
    match last {
        None => true,
        Some(l) => now.duration_since(l) >= cooldown,
    }
}

/// Fetch + cache Google's OIDC signing certs. `force = false` is the normal
/// cache-miss/TTL-expiry path; `force = true` is the off-cycle unknown-`kid`
/// refetch (rate-limited by [`FORCE_REFRESH_COOLDOWN`]). Both funnel through
/// [`certs_fetch_lock`] with double-checked freshness — a fast, lock-free
/// check first (the overwhelmingly common "cache is fine" case), then, only
/// if a fetch might be needed, acquire the lock and re-check before actually
/// calling out, so a herd of concurrent callers that all saw a stale/cold
/// cache still collapse onto a single outbound request instead of each
/// firing their own.
async fn fetch_certs(cfg: &PushConfig, http: &reqwest::Client, force: bool) -> Result<(), PushAuthError> {
    // Fast, lock-free path: cache is already fresh. Only ever short-circuits
    // here for `force = false` — a forced refresh always at least evaluates
    // the cooldown below, even if the cache happens to look fresh (it
    // wouldn't, or the kid wouldn't be unknown in the first place).
    if !force {
        if let Some(c) = cache().read().await.as_ref() {
            if c.fetched.elapsed() < CERTS_TTL {
                return Ok(());
            }
        }
    }

    let mut guard = certs_fetch_lock().lock().await;
    let now = Instant::now();
    if force {
        if !force_refresh_due(*guard, now, FORCE_REFRESH_COOLDOWN) {
            return Ok(());
        }
    } else {
        // Double-checked: another caller may have refreshed the cache while
        // this one waited for the lock — the common case under concurrent
        // load, and exactly what turns "N callers, N fetches" into "N
        // callers, 1 fetch".
        if let Some(c) = cache().read().await.as_ref() {
            if c.fetched.elapsed() < CERTS_TTL {
                return Ok(());
            }
        }
    }
    // `guard` (and therefore the lock) stays held across the network fetch
    // below — this IS the single-flight: every other concurrent caller,
    // either path, blocks here until this fetch finishes and updates
    // `cache()`, then re-checks the (now fresh) cache above instead of
    // firing a redundant fetch of its own.
    let fetch_result = fetch_jwks_body(cfg, http).await;
    // Record the cooldown/single-flight timestamp only once the attempt has
    // actually RESOLVED — success or error — never before it starts
    // (CodeRabbit round 3 finding on PR #674). Stamping it at `now`, before
    // awaiting the fetch, understates how recently a fetch genuinely
    // completed: a fetch slower than `FORCE_REFRESH_COOLDOWN` would let a
    // `force = true` caller queued behind this same lock acquire it, see
    // `force_refresh_due` pass (the clock started ticking before THIS fetch
    // even finished), and fire a redundant second fetch immediately after
    // this one lands.
    *guard = Some(Instant::now());
    let body = fetch_result?;
    let keys = parse_jwks(&body)?;
    *cache().write().await = Some(CachedCerts {
        keys,
        fetched: Instant::now(),
    });
    Ok(())
}

/// The bare HTTP GET + error-mapping half of a certs fetch, split out so
/// [`fetch_certs`] can capture its `Result` and stamp the cooldown timestamp
/// AFTER it resolves (success or error) without an early `?` return skipping
/// that stamp — see the round 3 fix note at its call site.
async fn fetch_jwks_body(cfg: &PushConfig, http: &reqwest::Client) -> Result<String, PushAuthError> {
    http.get(&cfg.certs_url)
        .send()
        .await
        .map_err(|e| PushAuthError::CertsFetch(e.to_string()))?
        .error_for_status()
        .map_err(|e| PushAuthError::CertsFetch(e.to_string()))?
        .text()
        .await
        .map_err(|e| PushAuthError::CertsFetch(e.to_string()))
}

/// Verify + decode a token against an already-resolved signing key: RS256,
/// issuer, expiry — pure (no I/O), unit-testable, mirrors `clerk::
/// decode_claims`.
fn decode_claims(token: &str, key: &DecodingKey) -> Result<PushClaims, PushAuthError> {
    let mut validation = Validation::new(Algorithm::RS256);
    validation.set_issuer(&ACCEPTED_ISSUERS);
    // `aud` is checked explicitly against the configured PushConfig::audience
    // after decode (not here) so a mismatch is reported as IdentityMismatch
    // (403) rather than a bare decode failure (401) — see verify_push_token.
    validation.validate_aud = false;
    validation.validate_exp = true;
    validation.validate_nbf = true;
    decode::<PushClaims>(token, key, &validation)
        .map(|d| d.claims)
        .map_err(|e| PushAuthError::InvalidToken(e.to_string()))
}

/// Verify a Pub/Sub push OIDC token end-to-end: resolve the signing key by
/// `kid` (refreshing the cert cache once on a miss), check signature +
/// issuer + expiry, then enforce audience + service-account identity. Not
/// `pub`: today's only caller is [`google_push`] in this same module — kept
/// as its own named function (rather than inlined) so the module's tests can
/// exercise decode/identity logic directly, mirroring `clerk::verify`'s
/// shape.
async fn verify_push_token(
    token: &str,
    cfg: &PushConfig,
    http: &reqwest::Client,
) -> Result<PushClaims, PushAuthError> {
    let kid = decode_header(token)
        .map_err(|e| PushAuthError::InvalidToken(e.to_string()))?
        .kid
        .ok_or_else(|| PushAuthError::InvalidToken("missing kid".into()))?;

    fetch_certs(cfg, http, false).await?;
    let mut claims = None;
    for force in [false, true] {
        if force {
            fetch_certs(cfg, http, true).await?;
        }
        let guard = cache().read().await;
        if let Some(c) = guard.as_ref() {
            if let Some(key) = c.keys.get(&kid) {
                claims = Some(decode_claims(token, key)?);
                break;
            }
        }
    }
    let claims = claims.ok_or(PushAuthError::UnknownKid)?;

    if claims.aud != cfg.audience {
        return Err(PushAuthError::IdentityMismatch(format!(
            "aud {:?} != configured audience",
            claims.aud
        )));
    }
    let email_ok = claims
        .email
        .as_deref()
        .map(|e| e.eq_ignore_ascii_case(&cfg.service_account_email))
        .unwrap_or(false);
    if !email_ok || claims.email_verified != Some(true) {
        return Err(PushAuthError::IdentityMismatch(format!(
            "email {:?} (verified={:?}) != configured service account",
            claims.email, claims.email_verified
        )));
    }
    Ok(claims)
}

fn bearer_token(headers: &HeaderMap) -> Result<&str, PushAuthError> {
    headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .ok_or(PushAuthError::MissingToken)
}

// ── Pub/Sub push envelope ───────────────────────────────────────────────────

/// The standard Pub/Sub push request body shape
/// (https://cloud.google.com/pubsub/docs/push#receive_push).
#[derive(Debug, Deserialize)]
struct PushEnvelope {
    message: PushMessage,
    #[serde(default)]
    subscription: String,
}

#[derive(Debug, Deserialize)]
struct PushMessage {
    /// Base64-encoded payload. For Gmail this is the `{emailAddress,
    /// historyId}` JSON `users.watch` publishes; for Chat it is the
    /// Workspace Events API's CloudEvent payload. Left un-decoded here — no
    /// per-surface handling exists yet (see the module doc).
    #[serde(default)]
    data: String,
    #[serde(default, rename = "messageId")]
    message_id: String,
    #[serde(default, rename = "publishTime")]
    publish_time: String,
    #[serde(default)]
    attributes: HashMap<String, String>,
}

/// `POST /v1/webhooks/google` — Pub/Sub push delivery for Gmail `users.watch`
/// and Chat Workspace Events subscriptions.
pub async fn google_push(State(state): State<Arc<AppState>>, headers: HeaderMap, body: Bytes) -> Response {
    let Some(cfg) = PushConfig::from_env() else {
        tracing::warn!(error = %PushAuthError::NotConfigured, "google push received but not configured — rejecting");
        return StatusCode::UNAUTHORIZED.into_response();
    };

    let token = match bearer_token(&headers) {
        Ok(t) => t,
        Err(e) => {
            tracing::warn!(error = %e, "google push missing/malformed Authorization header");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    };

    match verify_push_token(token, &cfg, &state.http_client).await {
        Ok(_claims) => {}
        Err(PushAuthError::IdentityMismatch(reason)) => {
            tracing::warn!(%reason, "google push token identity mismatch — rejecting");
            return StatusCode::FORBIDDEN.into_response();
        }
        Err(e) => {
            tracing::warn!(error = %e, "google push token verification failed — rejecting");
            return StatusCode::UNAUTHORIZED.into_response();
        }
    }

    // Verified. From here on, never a non-2xx for a malformed body — Pub/Sub
    // redelivers on anything else and a malformed body will never parse
    // differently on retry (same "ack anyway, log, nothing to do" pattern
    // `graph_notifications` uses for an unparseable Graph batch).
    // The raw envelope is what the receive job holds; the typed view is read
    // from it in place, so the body is parsed once and never copied.
    let raw = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(raw) => raw,
        Err(e) => {
            tracing::warn!(error = %e, "unparseable google push envelope body");
            return StatusCode::OK.into_response();
        }
    };
    let envelope = match PushEnvelope::deserialize(&raw) {
        Ok(envelope) => envelope,
        Err(e) => {
            tracing::warn!(error = %e, "unparseable google push envelope body");
            return StatusCode::OK.into_response();
        }
    };
    let Some(received) = received_event(envelope, raw) else {
        return StatusCode::OK.into_response();
    };
    if !crate::workers::ingest_receive::enqueue(&state, vec![received]).await {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    }
    StatusCode::OK.into_response()
}

/// The receive job a verified push becomes, with its dedupe key, or `None`
/// for a push with nothing to do. No I/O.
///
/// A Gmail push is keyed by its mailbox ([`gmail_mailbox_key`]): the pass
/// diffs from the stored cursor, so one pending pass covers every push that
/// arrives before it starts. Every other surface is keyed by its Pub/Sub
/// subscription and message id ([`workspace_event_key`]): a redelivery
/// repeats the id, but an id is unique only within its topic, so the
/// Chat, Chat app and Meet subscriptions each key their own namespace.
fn received_event(envelope: PushEnvelope, raw: serde_json::Value) -> Option<(ReceivedEvent, Option<String>)> {
    match classify_subscription(&envelope.subscription) {
        PushSurface::Gmail => match decode_gmail_push_data(&envelope.message.data) {
            Ok(parsed) => {
                let key = gmail_mailbox_key(&parsed.email_address);
                Some((ReceivedEvent::GmailPush { email_address: parsed.email_address }, key))
            }
            Err(e) => {
                tracing::warn!(error = %e, "google push: gmail message.data decode failed — dropping");
                None
            }
        },
        PushSurface::Meet | PushSurface::Chat | PushSurface::ChatAppEvents => {
            let key = workspace_event_key(&envelope.subscription, &envelope.message.message_id);
            Some((ReceivedEvent::GoogleWorkspaceEvent { envelope: raw }, key))
        }
        PushSurface::Unknown => {
            tracing::warn!(
                subscription = %envelope.subscription,
                message_id = %envelope.message.message_id,
                publish_time = %envelope.message.publish_time,
                attributes = ?envelope.message.attributes,
                "google push: unrecognized subscription — dropping"
            );
            None
        }
    }
}

/// The `gmail-push-mailbox` key, derived once per process from
/// `MASTER_KEK_HEX` ([`crate::crypto::kdf::DerivedKey`]).
static GMAIL_MAILBOX_KEY: crate::crypto::kdf::DerivedKey =
    crate::crypto::kdf::DerivedKey::warning_when_unset(crate::crypto::kdf::purpose::GMAIL_PUSH_MAILBOX);

/// The Gmail receive job's dedupe key: `ingest_receive:gmail:` plus the hex
/// HMAC-SHA256 of the mailbox address under the `gmail-push-mailbox` key
/// derived from `MASTER_KEK_HEX` ([`GMAIL_MAILBOX_KEY`], the same key
/// management as `routes::orgs_create::cap_bucket_key`). Keyed, not a bare
/// hash: an address has little entropy, so a plain digest in
/// `jobs.idempotency_key` would be a dictionary away from the address it
/// stands for, and the key column is never redacted. `None` (logged by the
/// key helper) when the KEK is unavailable: the push is still held, only
/// without coalescing.
fn gmail_mailbox_key(email_address: &str) -> Option<String> {
    GMAIL_MAILBOX_KEY.get().map(|key| mailbox_key_with(&key, email_address))
}

/// [`gmail_mailbox_key`]'s pure core, for a given derived key.
fn mailbox_key_with(key: &[u8; crate::crypto::kdf::DERIVED_KEY_LEN], email_address: &str) -> String {
    use hmac::{Hmac, Mac};
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(key).expect("HMAC-SHA256 accepts any key length");
    mac.update(email_address.as_bytes());
    format!("ingest_receive:gmail:{}", hex::encode(mac.finalize().into_bytes()))
}

/// The receive job's dedupe key for a Chat, Chat app or Meet push:
/// `ingest_receive:google:<subscription>:<messageId>`. A Pub/Sub message id
/// is unique only within its topic, so the full subscription resource name
/// is part of the key and the same id on two subscriptions never
/// collapses into one job. `None` when the push carries no message id.
/// No I/O.
fn workspace_event_key(subscription: &str, message_id: &str) -> Option<String> {
    (!message_id.is_empty()).then(|| format!("ingest_receive:google:{subscription}:{message_id}"))
}

/// Run one received Chat, Chat app or Meet push — `workers::ingest_receive`'s
/// entry for [`ReceivedEvent::GoogleWorkspaceEvent`]. An envelope or event
/// that no longer decodes is [`ReceiveError::Malformed`] (completed, never
/// retried); a handler failure is classified by the worker, so a transient
/// one retries the push.
pub(crate) async fn receive_workspace_event(
    state: Arc<AppState>,
    envelope: serde_json::Value,
) -> Result<(), ReceiveError> {
    let envelope = PushEnvelope::deserialize(&envelope)
        .map_err(|e| ReceiveError::Malformed(format!("google push envelope: {e}")))?;
    dispatch(state, envelope).await
}

/// Which surface a verified push's `subscription` names — see module doc's
/// "Dispatch" section. Pure — no I/O — so the routing decision itself is
/// directly testable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PushSurface {
    Gmail,
    Meet,
    Chat,
    /// `chat-app-events-sub` — Chat app INTERACTION events (`ADDED_TO_SPACE`
    /// etc., gsuite Train G4 PR 3), a distinct delivery channel from
    /// `Chat`'s Workspace Events messages — see the module doc's Dispatch
    /// section. Must never be conflated with [`PushSurface::Chat`].
    ChatAppEvents,
    Unknown,
}

fn classify_subscription(subscription: &str) -> PushSurface {
    if subscription.ends_with("gmail-push-sub") {
        PushSurface::Gmail
    } else if subscription.ends_with("chat-app-events-sub") {
        // Checked BEFORE the plain `chat-events-sub` suffix below — the two
        // literals don't actually collide (`"chat-app-events-sub"` does not
        // end with `"chat-events-sub"`), but ordering the more specific
        // literal first keeps that non-collision from being load-bearing.
        PushSurface::ChatAppEvents
    } else if subscription.ends_with("meet-events-sub") {
        PushSurface::Meet
    } else if subscription.ends_with("chat-events-sub") {
        PushSurface::Chat
    } else {
        PushSurface::Unknown
    }
}

/// Gmail's `users.watch` push payload — `{emailAddress, historyId}`, decoded
/// from `message.data`. `historyId` is deserialized but deliberately UNUSED
/// beyond this struct (see module doc: it is a trigger only, never a
/// cursor) — kept on the struct anyway as documentation of the real payload
/// shape and so a future consumer doesn't have to re-derive it.
#[derive(Debug, Deserialize)]
struct GmailPushData {
    #[serde(rename = "emailAddress")]
    email_address: String,
    #[allow(dead_code)]
    #[serde(rename = "historyId")]
    history_id: serde_json::Value,
}

/// Decode a Gmail push notification's `message.data` — STANDARD base64
/// (Pub/Sub's own envelope encoding), distinct from the base64URL Gmail API
/// message bodies use (`connectors::email_body`) — into `{emailAddress,
/// historyId}`. Pure — no I/O.
fn decode_gmail_push_data(data: &str) -> Result<GmailPushData, String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| format!("base64 decode: {e}"))?;
    serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))
}

/// Chat app interaction event `message.data` (`chat-app-events-sub`,
/// gsuite Train G4 PR 3, design doc §1.4/§3.1) — NOT a Workspace Events
/// CloudEvent, and must not be conflated with [`decode_gmail_push_data`]'s
/// sibling decoder for the OTHER Chat subscription. Also not a base64
/// concern at all — the base64 decode is shared, only the JSON inside
/// differs.
///
/// **Two wire shapes reach this decoder**, gated by whether "Build this
/// Chat app as a Workspace add-on" is checked on the Chat API
/// configuration page:
///
/// - **Classic** (checkbox unchecked): Chat's own interaction JSON at the
///   TOP LEVEL — `{"type": "ADDED_TO_SPACE", "eventTime": "...", "space":
///   {...}, "user": {...}}` (see `backend/tests/fixtures/google_chat/
///   pubsub_push_chat_app_added_to_space.json`).
/// - **Workspace add-on** (checkbox checked — CoralSwarm's own Chat app,
///   and the checkbox is now locked ON, its tooltip reading "Currently
///   disabled.", so this is not a config choice we can revert): the
///   add-on framework's OWN envelope, `{"commonEventObject": {...}, "chat":
///   {"user": {...}, "eventTime": "...", "addedToSpacePayload":
///   {"space": {...}, "configCompleteRedirectUri": "..."}}}` (or
///   `removedFromSpacePayload` in place of `addedToSpacePayload`, which
///   never carries a `configCompleteRedirectUri`). There is no `type`
///   field at all — the interaction kind is which of
///   `chat.addedToSpacePayload` / `chat.removedFromSpacePayload` is
///   present, not a value. See `backend/tests/fixtures/google_chat_addon/`
///   for two real captures pulled from the live `chat-app-events-probe`
///   Pub/Sub subscription (2026-09-19) — production was dropping every one
///   of these until this shape was added, per that fixture directory's
///   README.
///
/// [`decode_chat_interaction_event`] tries the classic shape first — via
/// [`ChatInteractionEvent`] directly, so that shape's behavior (including
/// its error on a body that is neither shape) is unchanged from before
/// add-on support existed — then the add-on shape via
/// [`ChatAddonEnvelope`]/[`ChatAddonSpacePayload`], converted into the same
/// [`ChatInteractionEvent`] so every downstream consumer
/// ([`dispatch`], [`handle_chat_added_to_space`],
/// [`handle_chat_removed_from_space`]) stays shape-agnostic. A payload that
/// satisfies neither shape is always a clean `Err` — never a
/// half-populated event guessed from a partial match.
///
/// `space.name` is the only field either handler actually reads off
/// `ChatInteractionSpace` today (`space_type`/`display_name` are
/// deliberately untrusted — see below); both real wire shapes carry it
/// under the same `space.name` key, so [`ChatInteractionSpace`] is reused
/// as-is for both. Note the add-on shape's `space` object ALSO carries a
/// `type` field (e.g. `"ROOM"`, a legacy archetype label) alongside
/// `spaceType` (e.g. `"SPACE"`) — these are NOT the same field, and this
/// struct only ever reads `spaceType` (`#[serde(rename = "spaceType")]`),
/// so the unrelated `type` key is simply ignored by both shapes' decode.
#[derive(Debug, Deserialize)]
struct ChatInteractionSpace {
    #[serde(default)]
    name: String,
    // Deserialized but deliberately UNTRUSTED: `handle_chat_added_to_space`
    // always re-reads `spaceType`/`displayName` authoritatively via
    // `spaces.get` rather than the bare interaction event — "the
    // discriminator is read from `Space.spaceType` and nowhere else"
    // (#672's instruction, design doc §3.6/§4.1). Kept on the struct only
    // as documentation of the real wire shape and so a future reader isn't
    // tempted to reach for it instead. NEITHER wire shape's interaction
    // event carries `singleUserBotDm` (confirmed against both real add-on
    // captures in `backend/tests/fixtures/google_chat_addon/`) — that field
    // lives only on `connectors::chat_body::Space`, populated from the
    // authoritative `spaces.get` read `handle_chat_added_to_space` always
    // performs before creating a watched-space row, so its absence here
    // was never a gap for either handler.
    #[allow(dead_code)]
    #[serde(default, rename = "spaceType")]
    space_type: String,
    #[allow(dead_code)]
    #[serde(default, rename = "displayName")]
    display_name: String,
}

#[derive(Debug, Deserialize)]
struct ChatInteractionUser {
    #[serde(default)]
    name: String,
}

#[derive(Debug, Deserialize)]
struct ChatInteractionEvent {
    #[serde(rename = "type", default)]
    event_type: String,
    space: ChatInteractionSpace,
    #[serde(default)]
    user: Option<ChatInteractionUser>,
}

/// The Workspace add-on framework's chat-interaction envelope — see
/// [`ChatInteractionEvent`]'s doc for why this shape exists at all and
/// where its real captures live. `chat` is the only top-level field this
/// connector reads; `commonEventObject` (host app/platform) is present on
/// the wire but has no consumer.
#[derive(Debug, Deserialize)]
struct ChatAddonEnvelope {
    chat: ChatAddonInteraction,
}

#[derive(Debug, Deserialize)]
struct ChatAddonInteraction {
    #[serde(default)]
    user: Option<ChatInteractionUser>,
    #[serde(rename = "addedToSpacePayload", default)]
    added_to_space_payload: Option<ChatAddonSpacePayload>,
    #[serde(rename = "removedFromSpacePayload", default)]
    removed_from_space_payload: Option<ChatAddonSpacePayload>,
    // `eventTime` deliberately not modeled: [`ChatInteractionEvent`] itself
    // has never carried it (the classic shape's own `eventTime` is dropped
    // on the floor the same way today), and neither handler has a use for
    // it — see the module doc's dispatch section for what actually gates
    // ADDED_TO_SPACE/REMOVED_FROM_SPACE handling.
}

/// One space payload, keyed by which of `addedToSpacePayload`/
/// `removedFromSpacePayload` it was found under in
/// [`ChatAddonInteraction`]. `configCompleteRedirectUri` (present only on
/// `addedToSpacePayload`) is deliberately not modeled — never consumed.
#[derive(Debug, Deserialize)]
struct ChatAddonSpacePayload {
    space: ChatInteractionSpace,
}

impl TryFrom<ChatAddonEnvelope> for ChatInteractionEvent {
    type Error = String;

    /// The add-on envelope carries no `type` field — the interaction kind
    /// is derived from WHICH of `addedToSpacePayload`/
    /// `removedFromSpacePayload` is present, per the module doc. Neither
    /// present is a clean `Err`, never a silently-guessed event — this is
    /// the only way this conversion can fail.
    fn try_from(envelope: ChatAddonEnvelope) -> Result<Self, Self::Error> {
        let ChatAddonInteraction { user, added_to_space_payload, removed_from_space_payload } = envelope.chat;
        if let Some(payload) = added_to_space_payload {
            return Ok(ChatInteractionEvent {
                event_type: "ADDED_TO_SPACE".to_string(),
                space: payload.space,
                user,
            });
        }
        if let Some(payload) = removed_from_space_payload {
            return Ok(ChatInteractionEvent {
                event_type: "REMOVED_FROM_SPACE".to_string(),
                space: payload.space,
                user,
            });
        }
        Err("add-on chat envelope's \"chat\" object has neither addedToSpacePayload nor \
             removedFromSpacePayload — an add-on interaction kind this connector does not \
             yet handle (see module doc's dispatch section: only ADDED_TO_SPACE/\
             REMOVED_FROM_SPACE are in scope)"
            .to_string())
    }
}

fn decode_chat_interaction_event(data: &str) -> Result<ChatInteractionEvent, String> {
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|e| format!("base64 decode: {e}"))?;
    let value: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))?;

    // Classic shape first — trying `ChatInteractionEvent` directly means
    // this arm's behavior (including its error on a payload that matches
    // neither shape) is byte-for-byte what it was before add-on support
    // existed.
    if let Ok(event) = serde_json::from_value::<ChatInteractionEvent>(value.clone()) {
        return Ok(event);
    }

    // Workspace add-on shape.
    let addon: ChatAddonEnvelope = serde_json::from_value(value).map_err(|e| {
        format!("json parse: not a classic or Workspace add-on chat interaction event: {e}")
    })?;
    ChatInteractionEvent::try_from(addon)
}

// ── Chat Workspace Events (`chat-events-sub`, gsuite Train G4 PR 4) ─────────

/// The parts of a Chat Workspace Events CloudEvent this connector actually
/// routes on (design doc §2.6, §3.5/D8).
///
/// **`space_id` is the routing key. There is no `sub`-keyed push resolver at
/// all** — D8: *"the push routing key is the space resource name, resolved
/// through `google_chat_watched_spaces`, never the subscriber."* That falls
/// out of the container model: a space has exactly one destination (I2), so
/// the destination is a property of the SPACE, not of whichever of the N
/// members' subscriptions happened to deliver this copy. Saying so here is
/// worth the words, because "resolve the owner from the push" is the reflex
/// Gmail and Meet train an implementer into, and this connector's blast
/// radius is strictly smaller for not doing it.
#[derive(Debug, Clone, PartialEq, Eq)]
struct ChatCloudEvent {
    /// e.g. `google.workspace.chat.message.v1.created`.
    event_type: String,
    /// `"spaces/{id}"`.
    space_id: String,
    /// `"spaces/{space}/messages/{message}"` when the event names one.
    /// Present only for observability — the ingest pass re-derives what to
    /// fetch from the space's own stored cursor, so a push whose body shape
    /// surprises us still triggers a correct pass (contract §4's C7 row:
    /// treat pushes as triggers, not as data).
    message_name: Option<String>,
    /// `"spaces/{space}/members/{member}"` for a membership event.
    membership_name: Option<String>,
}

/// Strip Google's `//{service}/` resource-name prefix, leaving the bare
/// `spaces/{id}` form every Chat API call and every
/// `google_chat_watched_spaces.space_id` uses. Pure.
fn bare_space_name(raw: &str) -> Option<String> {
    let trimmed = raw.trim();
    let without_host = match trimmed.strip_prefix("//") {
        Some(rest) => rest.split_once('/').map(|(_, path)| path).unwrap_or(""),
        None => trimmed,
    };
    // Keep only the `spaces/{id}` head — a `ce-id` is the SpaceEvent
    // resource name (`spaces/{id}/spaceEvents/{event}`) and a message name
    // is `spaces/{id}/messages/{m}`; both reduce to the same space.
    let mut parts = without_host.split('/');
    match (parts.next(), parts.next()) {
        (Some("spaces"), Some(id)) if !id.is_empty() => Some(format!("spaces/{id}")),
        _ => None,
    }
}

/// Decode a verified Chat Workspace Events push.
///
/// Reads the **binary-mode CloudEvent attributes first** (`ce-type`,
/// `ce-subject`, `ce-id`) and only then falls back to the base64 body. That
/// order is deliberate: per the fixture README, the `ce-*` attribute
/// contract is the platform-documented CloudEvents-over-Pub/Sub one and is
/// **not** in the UNVERIFIED category, whereas the decoded `data` payload's
/// internal shape **is** (design doc §2.7, risk 9/14 — step 0's live capture
/// has not landed). Preferring the verified half means an unexpected body
/// costs us at most the optional `message_name`, never the routing key.
///
/// Every failure is an `Err(String)` the caller logs and drops. Nothing here
/// can panic on a malformed body, and nothing here can produce a 5xx.
fn decode_chat_cloud_event(
    attributes: &HashMap<String, String>,
    data: &str,
) -> Result<ChatCloudEvent, String> {
    let body: serde_json::Value = if data.is_empty() {
        serde_json::Value::Null
    } else {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(data)
            .map_err(|e| format!("base64 decode: {e}"))?;
        serde_json::from_slice(&bytes).map_err(|e| format!("json parse: {e}"))?
    };

    let event_type = attributes
        .get("ce-type")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .or_else(|| body.get("eventType").and_then(|v| v.as_str()).map(str::to_string))
        .ok_or_else(|| "no ce-type attribute and no eventType in the body".to_string())?;

    let space_id = attributes
        .get("ce-subject")
        .and_then(|s| bare_space_name(s))
        .or_else(|| attributes.get("ce-id").and_then(|s| bare_space_name(s)))
        .or_else(|| body.get("name").and_then(|v| v.as_str()).and_then(bare_space_name))
        .ok_or_else(|| "could not recover a space resource name from ce-subject/ce-id/body".to_string())?;

    // Optional, best-effort: several wrapper shapes are tolerated because
    // the body's exact envelope is UNVERIFIED. A miss is fine.
    let pick = |keys: &[&str]| -> Option<String> {
        for key in keys {
            if let Some(name) = body
                .get(*key)
                .and_then(|d| d.get("message").or_else(|| d.get("membership")))
                .and_then(|m| m.get("name"))
                .and_then(|v| v.as_str())
                .filter(|s| !s.trim().is_empty())
            {
                return Some(name.trim().to_string());
            }
        }
        None
    };
    let message_name = pick(&["messageCreatedEventData", "messageDeletedEventData"]);
    let membership_name = pick(&[
        "membershipCreatedEventData",
        "membershipDeletedEventData",
        "membershipUpdatedEventData",
    ]);

    Ok(ChatCloudEvent {
        event_type,
        space_id,
        message_name,
        membership_name,
    })
}

/// Handle one decoded Chat Workspace Event.
///
/// **Space-keyed, fanned out across every account that watches the space.**
/// `resolve_chat_space_account` (control-only `SECURITY DEFINER`, migration
/// `0198`, already shipped by G4.3's security review) deliberately does NOT
/// fail closed on more than one match: two CoralSwarm accounts can each
/// independently hold a container row for the same physical space, and both
/// must ingest it. That is the opposite posture from the IDENTITY resolvers,
/// which must fail closed — there is no "wrong" account to act on here.
///
/// A space with **no watched row under any account is discarded**, not an
/// error (§4.3): a member's `spaces/-` subscription deliberately reaches
/// every space they belong to, wider than our consent set, and this discard
/// is what reconciles the two. It is doing real work, not defensive
/// padding.
///
/// Discovery unions control-plane personal routing IDs with account-scoped
/// watched rows in every registered tenant database. Content and credentials
/// always resolve independently to the account's real plane. Subscription
/// owners and event actors never imply container or mailbox consent.
/// Discover routing IDs on control, then query each real tenant under its
/// own account scope. Never derive watched consent from the event actor.
async fn chat_targets(
    state: &Arc<AppState>,
    control: &PgPool,
    space: &str,
) -> Result<(Vec<Uuid>, Vec<(Uuid, Uuid)>), ConnectorError> {
    let mut containers: std::collections::HashSet<Uuid> =
        google_chat::resolve_chat_space_accounts(control, space)
            .await?
            .into_iter()
            .collect();
    let mut mailboxes: std::collections::HashSet<(Uuid, Uuid)> =
        google_chat::resolve_chat_mailbox_space_owners(control, space)
            .await?
            .into_iter()
            .collect();
    let accounts: Vec<Uuid> =
        sqlx::query_scalar("SELECT account_id FROM tenant_databases ORDER BY account_id")
            .fetch_all(control)
            .await
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
    for account in accounts {
        let plane =
            match crate::crypto::DekPlane::resolve_for_account(state, control, account).await {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(%account, error=%e, "Chat push tenant resolution failed");
                    continue;
                }
            };
        let mut tx = match crate::tenancy::scope::begin_tenant_system_scoped(plane.content_pool(), account).await {
            Ok(tx) => tx,
            Err(e) => { tracing::warn!(%account,error=%e,"Chat metadata scope failed; continuing other accounts");continue; }
        };
        let owners: Result<Vec<Option<Uuid>>, sqlx::Error> = sqlx::query_scalar("SELECT owner_user_id FROM google_chat_watched_spaces WHERE account_id=$1 AND space_id=$2")
            .bind(account).bind(space).fetch_all(&mut *tx).await;
        tx.rollback().await.ok();
        let owners = match owners {
            Ok(owners) => owners,
            Err(e) => { tracing::warn!(%account,error=%e,"Chat watched discovery failed; continuing other accounts");continue; }
        };
        for owner in owners {
            match owner {
                Some(owner) => {
                    mailboxes.insert((account, owner));
                }
                None => {
                    containers.insert(account);
                }
            }
        }
    }
    Ok((
        containers.into_iter().collect(),
        mailboxes.into_iter().collect(),
    ))
}

/// Run one Chat Workspace event for every account that watches its space.
/// `Err` when some account failed transiently (the receive job retries, see
/// [`FanOut`]); a permanent failure for one account is logged and does not
/// fail the event for the others.
async fn run_chat_event(state: &Arc<AppState>, event: &ChatCloudEvent) -> Result<usize, ConnectorError> {
    let Some(pool) = state.db.clone() else {
        return Err(ConnectorError::Internal("no consumer pool configured".into()));
    };

    let (account_ids, mut mailbox_owners) = chat_targets(state, &pool, &event.space_id).await?;
    if event.event_type.contains(".membership.v1.") { mailbox_owners.clear(); }
    if account_ids.is_empty() && mailbox_owners.is_empty() {
        tracing::info!(
            space = %event.space_id, event_type = %event.event_type,
            "google push: chat event for a space with no watched row under any account or \
             mailbox opt-in — discarded (ADR-0008 storage scoping: the subscription's reach is \
             deliberately wider than our consent set, and this filter is what reconciles them)"
        );
        return Ok(0);
    }

    let mut total = 0usize;
    // Every account runs; a transient failure for any of them fails the
    // event so the receive job retries it, a permanent one is logged and
    // does not (ADR-0016, the same rule as the Gmail fan-out).
    let mut fan_out = FanOut::new();
    // One event, but potentially several accounts watching the same
    // physical space (ADR-0008 storage scoping, this function's own doc
    // comment above) — this loop is a "pass" in [`google_chat::
    // DomainDirectoryCache`]'s sense too, so one cache is created here and
    // shared across every account this single push touches (Thread 23).
    let mut directory_cache = google_chat::DomainDirectoryCache::new();
    for account_id in account_ids {
        // Resolve the content plane independently for each discovered account.
        let dek_plane = match crate::crypto::DekPlane::resolve_for_account(state, &pool, account_id).await {
            Ok(p) => p,
            Err(e) => {
                fan_out.failed(plane_error(e), |e, retry| {
                    tracing::warn!(
                        error = %e, %account_id, space = %event.space_id, retry,
                        "google push: chat event could not resolve this account's tenancy plane \
                         (continuing with any others)"
                    )
                });
                continue;
            }
        };
        let content_pool = dek_plane.content_pool().clone();
        let tenant = dek_plane.tenant().clone();
        if let crate::tenancy::TenantId::Org(org_id) = &tenant {
            if !crate::routes::oauth::org_google_chat_enabled_for(state, org_id).await { continue; }
        }
        if dek_plane.is_org() {
            google_chat::warn_if_chat_space_plane_mismatch(&pool, account_id, &event.space_id).await;
        }

        if event.event_type.contains(".membership.v1.") {
            // A membership event runs the roster reconcile for that space
            // (§4.3), never an ingest.
            if let Err(e) =
                handle_chat_membership_event(state, &pool, &content_pool, &tenant, account_id, event, &mut directory_cache).await
            {
                fan_out.failed(e, |e, retry| {
                    tracing::warn!(
                        error = %e, %account_id, space = %event.space_id, retry,
                        "google push: chat membership event handling failed for this account \
                         (continuing with any others)"
                    )
                });
            }
            continue;
        }

        // A message event: run the space's own bounded pass. The push is a
        // TRIGGER — the stored per-space cursor decides what is actually
        // fetched, so a redelivery, an out-of-order delivery, and a delivery
        // whose body shape we mis-read all converge on the same correct
        // work.
        let space = match google_chat::ingestable_space(&content_pool, account_id, &event.space_id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                tracing::info!(
                    %account_id, space = %event.space_id,
                    "google push: chat event for a space that is not currently routed+active for \
                     this account — discarded (unrouted spaces have no destination, and a removed \
                     space stops ingest immediately)"
                );
                continue;
            }
            Err(e) => {
                fan_out.failed(e, |e, retry| {
                    tracing::warn!(error = %e, %account_id, space = %event.space_id, retry, "google push: watched-space read failed")
                });
                continue;
            }
        };
        match crate::workers::google_chat_sweep::sync_space_once(
            state,
            &pool,
            &content_pool,
            &tenant,
            account_id,
            &space,
            // NOT on a pushed message (review round 1, M4). This handler runs
            // once per `message.v1.created`, and an unconditional reconcile
            // meant every Chat message triggered a full `spaces.members.list`
            // walk plus identity resolution plus a seat write on the push-ack
            // path. The 30-minute sweep owns the reconcile; a genuine
            // membership change gets one immediately via
            // `handle_chat_membership_event` below.
            crate::workers::google_chat_sweep::RosterReconcile::No,
            &mut directory_cache,
        )
        .await
        {
            // `Ok(None)` = the pass did not run (the per-space lock was
            // already held by a concurrent sweep, nobody can read the space,
            // or it resolved to no destination). Nothing to add, and nothing
            // to report as an error — the sweep re-walks the same range.
            Ok(n) => total += n.unwrap_or(0),
            Err(e) => fan_out.failed(e, |e, retry| {
                tracing::warn!(
                    error = %e, %account_id, space = %event.space_id, retry,
                    "google push: chat space sync failed for this account (continuing with any others)"
                )
            }),
        }
    }

    // G4.5 — the mailbox archetype's opt-ins for this same physical space.
    // Reader is ALWAYS the opting-in owner (no election, no failover); no
    // roster reconcile call exists for this archetype at all (excluded
    // above for membership events, and a message event never reconciles a
    // roster to begin with).
    for (account_id, owner_user_id) in mailbox_owners {
        let plane = match crate::crypto::DekPlane::resolve_for_account(state, &pool, account_id).await {
            Ok(plane) => plane,
            Err(e) => {
                fan_out.failed(plane_error(e), |e, retry| {
                    tracing::warn!(%account_id, %owner_user_id, error = %e, retry, "Chat mailbox plane resolution failed; continuing other owners")
                });
                continue;
            }
        };
        let content_pool = plane.content_pool();
        let space = match google_chat::ingestable_mailbox_space(content_pool, account_id, &event.space_id, owner_user_id).await {
            Ok(Some(s)) => s,
            Ok(None) => {
                tracing::info!(
                    %account_id, %owner_user_id, space = %event.space_id,
                    "google push: chat event for a mailbox opt-in that is not currently active for \
                     this owner — discarded (an opted-out mailbox stops ingest immediately)"
                );
                continue;
            }
            Err(e) => {
                fan_out.failed(e, |e, retry| {
                    tracing::warn!(error = %e, %account_id, %owner_user_id, space = %event.space_id, retry, "google push: watched mailbox-space read failed")
                });
                continue;
            }
        };
        match crate::workers::google_chat_sweep::sync_mailbox_space_once(
            state,
            &pool,
            content_pool,
            plane.tenant(),
            account_id,
            owner_user_id,
            &space,
        )
        .await
        {
            Ok(n) => total += n.unwrap_or(0),
            Err(e) => fan_out.failed(e, |e, retry| {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id, space = %event.space_id, retry,
                    "google push: chat mailbox sync failed for this owner (continuing with any others)"
                )
            }),
        }
    }

    fan_out.finish(total)
}

/// `membership.v1.created` / `.deleted` for one account's watched space
/// (§3.4's seating half, conformance test 7).
///
/// A **creation** re-runs the ordinary roster reconcile — grant-only, and
/// idempotent, so a redelivered event is free.
///
/// A **deletion** revokes what THIS space granted and nothing else, via
/// `seats::revoke_seat_grant`'s DELETE-first, count-the-dropped-rows
/// discipline: a `preexisting` (hand-granted) seat is never ours to revoke,
/// and a member seated by a second routed space keeps their seat. The
/// departing member is identified from the MEMBERSHIP RESOURCE NAME's last
/// segment (`spaces/{space}/members/{member}` — `{member}` is the same
/// Google user id `User.name`'s `users/{id}` carries), because D5 means the
/// push payload carries no resource data to read a `member.name` out of.
/// An unresolvable member is logged and dropped: we never revoke a seat we
/// cannot attribute.
///
/// **Plane-explicit as of CS-432 (D17 point 4).** `control_pool` is used
/// ONLY for [`google_chat::resolve_google_identity_in_account`] — an
/// `oauth_tokens` read, control-only for every plane, same invariant every
/// other provider's identity resolution in this codebase already holds.
/// `content_pool`/`tenant` are the caller's already-resolved
/// [`crate::crypto::DekPlane`] pair for `account_id` and cover everything
/// else: the watched-space read, the sweep's own reconcile pass, and the
/// seat grant/revoke tables (`resolve_space_team_for_revoke`/
/// `revoke_space_seat`, both already plane-explicit by parameter name —
/// see their own doc comments).
async fn handle_chat_membership_event(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    content_pool: &PgPool,
    tenant: &crate::tenancy::TenantId,
    account_id: Uuid,
    event: &ChatCloudEvent,
    directory_cache: &mut google_chat::DomainDirectoryCache,
) -> Result<(), ConnectorError> {
    if !event.event_type.ends_with(".deleted") {
        // Created/updated: a plain reconcile pass. Reuses the sweep's own
        // path so grant behaviour cannot drift between push and sweep.
        let Some(space) = google_chat::ingestable_space(content_pool, account_id, &event.space_id).await? else {
            return Ok(());
        };
        crate::workers::google_chat_sweep::sync_space_once(
            state,
            control_pool,
            content_pool,
            tenant,
            account_id,
            &space,
            // A membership event is exactly the trigger a reconcile exists
            // for, so this one DOES reconcile (M4's carve-out).
            crate::workers::google_chat_sweep::RosterReconcile::Yes,
            directory_cache,
        )
        .await?;
        return Ok(());
    }

    let Some(membership_name) = event.membership_name.as_deref() else {
        tracing::info!(
            %account_id, space = %event.space_id,
            "google push: membership.v1.deleted with no membership resource name in the payload — \
             cannot attribute the departure, so no seat is revoked (the sweep's grant-only \
             reconcile never revokes, by design, so this leaves the seat standing rather than \
             guessing)"
        );
        return Ok(());
    };
    let sub = membership_name.rsplit('/').next().unwrap_or_default().trim();
    if sub.is_empty() {
        return Ok(());
    }
    let Some(user_id) = google_chat::resolve_google_identity_in_account(control_pool, account_id, sub).await? else {
        tracing::debug!(
            %account_id, space = %event.space_id,
            "google push: departing Chat member has no CoralSwarm identity in this account — \
             they hold no grant, so there is nothing to revoke"
        );
        return Ok(());
    };
    // The REVOKE resolver, which deliberately does NOT require the space to
    // be `active` (security review, M1 — reproduced). `record_removed_from_space`
    // sets `active = false`, but the per-member subscription is on
    // `spaces/-` and `resolve_chat_space_account` returns accounts regardless
    // of `active`, so a `membership.v1.deleted` still arrives after the app
    // is removed. With the grant-side predicate here, that departure resolved
    // no team, this `let … else` swallowed it, and the member kept an
    // unbounded-history seat on the destination team ocean forever.
    let Some(team_id) = google_chat::resolve_space_team_for_revoke(content_pool, account_id, &event.space_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve_space_team_for_revoke: {e}")))?
    else {
        return Ok(());
    };
    let outcome = google_chat::revoke_space_seat(content_pool, account_id, team_id, user_id, &event.space_id)
        .await
        .map_err(|e| ConnectorError::Internal(format!("revoke_space_seat: {e}")))?;
    tracing::info!(
        %account_id, %team_id, %user_id, space = %event.space_id, ?outcome,
        "google push: chat membership departure processed"
    );
    Ok(())
}

/// Real dispatch for a verified, well-formed push (gsuite Train G2 PR 3
/// replaces G1 PR 2's log-only stub — see module doc's "Dispatch" section).
///
/// # Never logs the mailbox address, not even hashed (CodeRabbit round 2
/// finding #2 on PR #686, privacy)
/// Round 1 replaced the raw `emailAddress` in these log lines with an
/// unsalted MD5 fingerprint. That is still a re-identifiable personal
/// identifier: MD5 is fast and unsalted, so an offline dictionary/rainbow
/// pass over a candidate email list recovers the exact address from the
/// fingerprint — hashing alone does not anonymize a low-entropy input like
/// an email address. This codebase has no established secret suitable for
/// keying an HMAC-style redaction here (`OAUTH_STATE_SECRET` exists, but
/// it's the OAuth CSRF-state signing key — a distinct security domain,
/// reusing it for log redaction would be its own smell, and it isn't
/// carried on `AppState` for this route to reach anyway). So: no
/// mailbox-derived identifier is logged at all before
/// `run_gmail_sync_for_email_with` resolves `(account_id, owner_user_id)` —
/// those two internal ids ARE what every log line after that point uses
/// (see that function's own advisory-lock/refresh-failure log lines) — and
/// the two log lines below report outcome/count only.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct MeetEvent {
    pub(crate) subscription: String,
    pub(crate) record: String,
}

fn decode_meet_event(
    attributes: &HashMap<String, String>,
    data: &str,
) -> Result<MeetEvent, String> {
    if attributes.get("ce-type").map(String::as_str)
        != Some(crate::connectors::google_meet_events::EVENT_TYPE)
    {
        return Err("unsupported Meet event type".into());
    }
    let subscription = attributes
        .get("ce-source")
        .and_then(|s| s.strip_prefix("//workspaceevents.googleapis.com/"))
        .filter(|s| {
            s.starts_with("subscriptions/") && s.split('/').count() == 2 && !s.ends_with('/')
        })
        .ok_or("invalid Meet subscription source")?
        .to_string();
    // Google documents `ce-subject` only as "the Google Workspace resource
    // where the event occurred" and gives no Meet example; production sent
    // something other than the transcript. The payload's `transcript.name`
    // is the documented identity, so `ce-subject` never gates delivery: the
    // OIDC envelope, the known `ce-source` subscription and the record
    // re-fetch are the fences. See
    // https://developers.google.com/workspace/events#structure and
    // https://developers.google.com/workspace/events/guides/events-meet.
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(data)
        .map_err(|_| "invalid Meet payload encoding")?;
    let body: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|_| "invalid Meet payload JSON")?;
    let transcript = body
        .get("transcript")
        .and_then(|t| t.get("name"))
        .and_then(|n| n.as_str())
        .ok_or("invalid Meet transcript resource")?;
    let parts: Vec<_> = transcript.split('/').collect();
    if parts.len() != 4
        || parts[0] != "conferenceRecords"
        || parts[2] != "transcripts"
        || parts[1].is_empty()
        || parts[3].is_empty()
        || transcript.chars().any(char::is_whitespace)
    {
        return Err("invalid Meet transcript resource".into());
    }
    Ok(MeetEvent {
        subscription,
        record: format!("conferenceRecords/{}", parts[1]),
    })
}

/// What a dropped Meet push looked like, with every identifier removed so
/// the next drop is diagnosable from the log alone.
struct MeetDecodeDiagnostics {
    attribute_keys: String,
    subject_shape: String,
    source_shape: String,
}

fn meet_decode_diagnostics(attributes: &HashMap<String, String>) -> MeetDecodeDiagnostics {
    let mut keys: Vec<&str> = attributes.keys().map(String::as_str).collect();
    keys.sort_unstable();
    let shape = |key: &str| resource_shape(attributes.get(key).map_or("", String::as_str));
    MeetDecodeDiagnostics {
        attribute_keys: keys.join(","),
        subject_shape: shape("ce-subject"),
        source_shape: shape("ce-source"),
    }
}

/// `//service/collection/id/collection/id` with each id replaced by `*`.
/// Anything that is not a full resource name reports only its length.
fn resource_shape(value: &str) -> String {
    if value.is_empty() {
        return "absent".into();
    }
    let Some(rest) = value.strip_prefix("//") else {
        return format!("opaque(len={})", value.len());
    };
    let mut segments = rest.split('/');
    let mut shape = format!("//{}", segments.next().unwrap_or(""));
    for (i, segment) in segments.enumerate() {
        shape.push('/');
        shape.push_str(if i % 2 == 0 { segment } else { "*" });
    }
    shape
}

pub(crate) async fn run_meet_event(state: &Arc<AppState>, event: &MeetEvent) -> Result<(), ConnectorError> {
    use crate::connectors::google_meet::{
        self, MeetPlane, OrgMeetPlane, RecordScope, ResolvedAttendee,
    };
    let control = state
        .db
        .as_ref()
        .ok_or_else(|| ConnectorError::Internal("missing control pool".into()))?;
    for (id, account, owner) in
        crate::connectors::google_meet_events::resolve_subscription(control, &event.subscription)
            .await?
    {
        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(control, account)
            .await
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM google_watch_channels WHERE id=$1 AND account_id=$2 AND owner_user_id=$3 AND surface='meet' AND channel_id=$4)")
            .bind(id).bind(account).bind(owner).bind(&event.subscription).fetch_one(&mut *tx).await.map_err(|e| ConnectorError::Internal(e.to_string()))?;
        tx.rollback().await.ok();
        if !exists || !google_chat::is_current_account_member(control, account, owner).await? {
            continue;
        }
        let dek = crate::crypto::DekPlane::resolve_for_account(state, control, account)
            .await
            .map_err(|e| ConnectorError::Internal(e.to_string()))?;
        let plane = match dek.tenant() {
            crate::tenancy::TenantId::Consumer => MeetPlane::Personal,
            crate::tenancy::TenantId::Org(org_id) => {
                if !crate::routes::oauth::org_google_meet_enabled_for(state, org_id).await {
                    continue;
                }
                MeetPlane::Org(OrgMeetPlane {
                    org_id: org_id.clone(),
                    account_id: account,
                    pool: dek.content_pool().clone(),
                })
            }
        };
        let Some(mut creds) = load_creds_bg(
            control,
            &dek,
            account,
            "google_meet",
            TokenOwner::Member(owner),
        )
        .await?
        else {
            continue;
        };
        crate::connectors::GoogleConnector
            .refresh_if_needed(&state.http_client, &mut creds)
            .await?;
        if let Some(refreshed) = creds.refreshed.as_ref() {
            crate::routes::oauth::store_token_bundle(
                control,
                &dek,
                account,
                "google_meet",
                TokenOwner::Member(owner),
                refreshed,
            )
            .await
            .map_err(|_| {
                ConnectorError::Internal("Meet refreshed token persistence failed".into())
            })?;
        }
        if let Some(record) = google_meet::get_conference_record(
            &state.http_client,
            &creds.access_token,
            &event.record,
        )
        .await?
        {
            if record.name != event.record {
                return Err(ConnectorError::Exchange(
                    "Meet returned a different conference record".into(),
                ));
            }
            google_meet::process_record_opt_in(
                &plane,
                &state.http_client,
                control,
                state,
                ResolvedAttendee {
                    account_id: account,
                    owner_user_id: owner,
                },
                &creds.access_token,
                &record,
                RecordScope::Sweep,
            )
            .await?;
        }
    }
    Ok(())
}

/// Run one Workspace push. A decode failure is [`ReceiveError::Malformed`]:
/// the same bytes never decode differently, so it is logged and completed,
/// never retried (and never a 5xx or a panic: the CloudEvent body's exact
/// shape is still unverified, design doc §2.7/risk 14). A handler failure
/// is returned for the receive worker to classify.
async fn dispatch(state: Arc<AppState>, envelope: PushEnvelope) -> Result<(), ReceiveError> {
    match classify_subscription(&envelope.subscription) {
        // Routed by `received_event` before any job is written.
        PushSurface::Gmail | PushSurface::Unknown => {
            tracing::warn!(subscription = %envelope.subscription, "google push: not a workspace event — dropping");
            Ok(())
        }
        PushSurface::Meet => {
            let shape = meet_decode_diagnostics(&envelope.message.attributes);
            let event = match decode_meet_event(&envelope.message.attributes, &envelope.message.data) {
                Ok(event) => event,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        attribute_keys = %shape.attribute_keys,
                        subject_shape = %shape.subject_shape,
                        source_shape = %shape.source_shape,
                        "invalid Meet event dropped"
                    );
                    return Err(ReceiveError::Malformed(format!("meet event: {e}")));
                }
            };
            tracing::info!(subject_shape = %shape.subject_shape, "Meet push decoded");
            run_meet_event(&state, &event).await.map_err(|e| {
                tracing::warn!(error = %e, "Meet push failed; approved capture remains durable for pull retry");
                ReceiveError::from(e)
            })
        }
        PushSurface::Chat => {
            let event = match decode_chat_cloud_event(&envelope.message.attributes, &envelope.message.data) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        message_id = %envelope.message.message_id,
                        "google push: chat workspace-event decode failed — dropping"
                    );
                    return Err(ReceiveError::Malformed(format!("chat event: {e}")));
                }
            };
            match run_chat_event(&state, &event).await {
                Ok(n) => {
                    tracing::info!(
                        event_type = %event.event_type, space = %event.space_id, ingested = n,
                        "google push: chat event handled"
                    );
                    Ok(())
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e, event_type = %event.event_type, space = %event.space_id,
                        "google push: chat event handling failed"
                    );
                    Err(ReceiveError::from(e))
                }
            }
        }
        PushSurface::ChatAppEvents => {
            let event = match decode_chat_interaction_event(&envelope.message.data) {
                Ok(e) => e,
                Err(e) => {
                    tracing::warn!(error = %e, "google push: chat app interaction event decode failed — dropping");
                    return Err(ReceiveError::Malformed(format!("chat app event: {e}")));
                }
            };
            // The install gate's own failures (a storage read, `spaces.get`,
            // a token refresh) surface so the worker classifies them: a
            // transient one retries the event, and both handlers are safe
            // to re-run (an upsert and a deactivate).
            let result = match event.event_type.as_str() {
                "ADDED_TO_SPACE" => handle_chat_added_to_space(&state, event).await,
                "REMOVED_FROM_SPACE" => handle_chat_removed_from_space(&state, event).await,
                other => {
                    tracing::info!(
                        event_type = other,
                        space = %event.space.name,
                        "google push: chat app interaction event logged and dropped (G4.5's App \
                         Home fast-follow handles APP_COMMAND/APP_HOME/CARD_CLICKED, not this train)"
                    );
                    Ok(())
                }
            };
            result.map_err(|e| {
                tracing::warn!(error = %e, "google push: chat app interaction event handling failed");
                ReceiveError::from(e)
            })
        }
    }
}

/// `ADDED_TO_SPACE` — the Chat-app-install gate itself (design doc §3.1,
/// D6). Resolves which CoralSwarm account the ADDING member belongs to
/// (`google_chat::resolve_google_identity_by_sub`, control-only,
/// identification-only — never a credential read), then, ONLY if that
/// member already has a connected `google_chat` token, refreshes/uses it to
/// read `spaces.get` for authoritative `spaceType`/`historyState` before
/// creating the unrouted watched-space row.
///
/// **Deferred, not retried, if the adding member is not yet connected.**
/// Per this train's scope (see the module doc's Dispatch section): without
/// a resolvable identity there is no account to attach a row to at all
/// (this function returns early); with a resolvable identity but no
/// `google_chat` connection, this function still defers rather than
/// guessing `spaceType`/`historyState` from the bare interaction event —
/// `Space.historyState` in particular drives the `HISTORY_OFF` disclosure
/// (§5.4) and must come from a real API read, not be assumed `HISTORY_ON`.
/// Nothing re-triggers this automatically today; a later connected
/// member's own action against the space (e.g. the destination-pick route,
/// once G4.4/G4.5 widen its bootstrap path) is what would complete it —
/// named explicitly in the PR body as a known G4.3-scope boundary, not a
/// silent gap.
///
/// **Plane-explicit as of CS-432 (D17 point 4, G4.6's org lift).** This
/// dispatch path still has no `Tenant`/HTTP request context (signature/JWT-
/// authenticated push, no session) — same reasoning
/// [`run_gmail_sync_for_email_with`]'s own doc gives — but it already resolves
/// `account_id`'s [`crate::crypto::DekPlane`] for the token read/refresh
/// below, and that plane's [`crate::crypto::DekPlane::content_pool`] is
/// reused here as the watched-space row's tenant pool too: an ordinary
/// org's row now lands on its own tenant database, a personal or
/// personal-org account's row lands on `state.db` exactly as before. Before
/// this fix every account, org or not, wrote here unconditionally
/// (`AccountPlane::Personal`, `state.db` for both the control and tenant
/// argument) — the defect D17 point 4 traced: the row was written where the
/// org's other data is not, so `tick_org`/the REST picker/the destination
/// pick (all genuinely tenant-aware) never saw it.
///
/// **Failures surface (ADR-0016).** A storage read, the identity
/// resolution, the plane resolution, the token load or refresh and
/// `spaces.get` return their error, so the receive worker classifies it
/// and a transient one retries the event. Re-running is safe: the row
/// write is an upsert (`record_added_to_space` creates or reactivates) and
/// the cold-start notice posts only when due. An event with nothing to
/// act on (no usable identity, an unconnected member, a non-SPACE
/// archetype) is `Ok(())`: a retry could not change it.
async fn handle_chat_added_to_space(state: &Arc<AppState>, event: ChatInteractionEvent) -> Result<(), ConnectorError> {
    let Some(pool) = state.db.clone() else {
        return Err(ConnectorError::Internal("no consumer pool configured".into()));
    };

    let adder_sub = event.user.as_ref().map(|u| google_chat::bare_sub(&u.name).to_string());
    let Some(sub) = adder_sub.filter(|s| !s.is_empty()) else {
        tracing::warn!(space = %event.space.name, "google push: ADDED_TO_SPACE with no usable adding-user identity — dropping");
        return Ok(());
    };
    let resolved = google_chat::resolve_google_identity_by_sub(&pool, &sub).await.inspect_err(|e| {
        tracing::warn!(space = %event.space.name, error = %e, "google push: ADDED_TO_SPACE identity resolution failed")
    })?;
    let Some((account_id, adder_user_id)) = resolved else {
        tracing::info!(
            space = %event.space.name,
            "google push: ADDED_TO_SPACE — adding member has no resolvable CoralSwarm identity \
             yet; deferring (no watched-space row created — see this function's doc comment)"
        );
        return Ok(());
    };

    // Cold-start notice (item 25). The member who caused this add may have
    // no Chat token yet — that is the case the notice exists for. It posts
    // as the Chat app, never with this member's credential.
    crate::connectors::chat_cold_start::on_added_to_space(
        state,
        &pool,
        account_id,
        &event.space.name,
        &event.space.space_type,
    )
    .await;

    // Signature/JWT-authenticated push, no request `Tenant` to read a plane
    // off — resolve it from the account through THE decision table
    // (`crypto::DekPlane::resolve_for_org`). One indexed `accounts` lookup by
    // primary key, once per ADDED_TO_SPACE event.
    let dek_plane = crate::crypto::DekPlane::resolve_for_account(state, &pool, account_id).await.map_err(|e| {
        tracing::warn!(space = %event.space.name, %account_id, error = %e, "google push: ADDED_TO_SPACE could not resolve the account's DEK plane");
        plane_error(e)
    })?;
    let Some(mut creds) =
        load_creds_bg(&pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(adder_user_id)).await?
    else {
        tracing::info!(
            space = %event.space.name, %account_id,
            "google push: ADDED_TO_SPACE — adding member is a known CoralSwarm identity but has \
             no connected google_chat token yet; deferring rather than guessing spaceType/\
             historyState from the bare interaction event"
        );
        return Ok(());
    };

    let google = GoogleConnector;
    google.refresh_if_needed(&state.http_client, &mut creds).await.inspect_err(|e| {
        tracing::warn!(space = %event.space.name, %account_id, error = %e, "google push: ADDED_TO_SPACE token refresh failed")
    })?;
    if let Some(refreshed) = creds.refreshed.as_ref() {
        if let Err(e) = store_token_bundle(&pool, &dek_plane, account_id, "google_chat", TokenOwner::Member(adder_user_id), refreshed).await {
            tracing::warn!(%account_id, status = ?e.status(), "google push: failed to persist refreshed google_chat token (non-fatal)");
        }
    }

    let key = format!("google_chat:{account_id}");
    let space = crate::graph_rate_limit::scoped(key, async {
        google_chat::get_space(&state.http_client, &creds.access_token, &event.space.name).await
    })
    .await
    .inspect_err(|e| {
        tracing::warn!(space = %event.space.name, %account_id, error = %e, "google push: spaces.get failed for ADDED_TO_SPACE")
    })?;

    // CS-432: the tenant pool is `dek_plane`'s, not `pool` a second time —
    // `AccountPlane::Org` for a genuine tenant-DB org, `Personal` for a
    // personal or personal-org account (unchanged, see the DEK plane's own
    // decision table doc comment for why a personal org never gets `Org`
    // here).
    let account_plane = google_chat::added_to_space_account_plane(dek_plane.is_org());
    match google_chat::record_added_to_space(&pool, dek_plane.content_pool(), account_plane, account_id, Some(adder_user_id), &space).await {
        Ok(Some(row)) => tracing::info!(space_id = %row.space_id, %account_id, "google push: ADDED_TO_SPACE — watched-space row created/reactivated (unrouted)"),
        Ok(None) => tracing::info!(space = %event.space.name, %account_id, "google push: ADDED_TO_SPACE for a non-SPACE archetype — no row created (see connectors::google_chat::record_added_to_space)"),
        Err(e) => {
            tracing::warn!(space = %event.space.name, %account_id, error = %e, "google push: failed to record ADDED_TO_SPACE");
            return Err(ConnectorError::Internal(format!("record_added_to_space: {e}")));
        }
    }
    Ok(())
}

/// `REMOVED_FROM_SPACE` — deactivate EVERY account's watched-space row for
/// this space, stopping ingest immediately (Slack #502's lesson).
///
/// **Routed by the SPACE, never the removing member's identity (H1 fix,
/// security review round 2, design doc §3.5/D8).** The original version of
/// this handler resolved the acting account from the REMOVER's own Google
/// `sub` (the same shape [`handle_chat_added_to_space`] correctly uses for
/// its account-DISCOVERY bootstrap) and deactivated only that one account's
/// row. That is wrong for `REMOVED_FROM_SPACE` specifically: removal is an
/// act on the SPACE, and the overwhelmingly likely case is that whoever
/// removes a Workspace-installed Chat app from a space has NEVER connected
/// CoralSwarm at all — the resolver returned `None`, the handler dropped
/// the event, and the row that was actually feeding an ocean was NEVER
/// deactivated. Now uses [`google_chat::resolve_chat_space_accounts`]
/// (space-keyed, control-only `SECURITY DEFINER`, migration `0198`) to find
/// every CoralSwarm account holding a row for this space — there can
/// legitimately be more than one — and deactivates each independently.
/// A space with no watched row anywhere, or one whose account(s) are all
/// already inactive, both log and drop, never an error (Pub/Sub redelivers
/// this exact event on any non-2xx, and a redelivered `REMOVED_FROM_SPACE`
/// for an already-deactivated row is expected, not a bug). The removing
/// member's identity (if resolvable) is logged for observability only —
/// it never gates whether deactivation happens.
///
/// Discovery unions personal IDs with scoped tenant watched rows. Each
/// account is deactivated on its actual content pool, independently of actor.
///
/// **Failures surface (ADR-0016).** The cold-start retire, the space's
/// account discovery, each account's plane resolution and each
/// deactivation return their error through the fan-out rule ([`FanOut`]):
/// every account is still tried, and a transient failure for any of them
/// retries the event. Re-running is safe: the retire and the deactivation
/// are idempotent (an already-inactive row answers `false`, never an
/// error). The remover's identity lookup stays informational and never
/// fails the event.
async fn handle_chat_removed_from_space(state: &Arc<AppState>, event: ChatInteractionEvent) -> Result<(), ConnectorError> {
    let Some(pool) = state.db.clone() else {
        return Err(ConnectorError::Internal("no consumer pool configured".into()));
    };
    let mut fan_out = FanOut::new();

    if let Err(e) = crate::connectors::chat_cold_start::retire_container(&pool, &event.space.name).await {
        fan_out.failed(ConnectorError::Internal(format!("retire_container: {e}")), |e, retry| {
            tracing::warn!(space = %event.space.name, error = %e, retry, "google push: REMOVED_FROM_SPACE cold-start retire failed")
        });
    }

    // Observability only — never gates deactivation (H1).
    if let Some(u) = &event.user {
        let sub = google_chat::bare_sub(&u.name);
        if !sub.is_empty() {
            match google_chat::resolve_google_identity_by_sub(&pool, sub).await {
                Ok(Some((account_id, remover_user_id))) => tracing::info!(
                    space = %event.space.name, %account_id, %remover_user_id,
                    "google push: REMOVED_FROM_SPACE — removing member identified (informational only)"
                ),
                Ok(None) => tracing::info!(
                    space = %event.space.name,
                    "google push: REMOVED_FROM_SPACE — removing member has no resolvable CoralSwarm \
                     identity (expected, the common case — this does not stop deactivation, unlike \
                     the pre-H1-fix behaviour)"
                ),
                Err(e) => tracing::warn!(space = %event.space.name, error = %e, "google push: REMOVED_FROM_SPACE remover identity lookup failed (non-fatal, informational only)"),
            }
        }
    }

    let (account_ids, _) = chat_targets(state, &pool, &event.space.name).await.inspect_err(|e| {
        tracing::warn!(space = %event.space.name, error = %e, "google push: REMOVED_FROM_SPACE space-account resolution failed")
    })?;
    if account_ids.is_empty() {
        tracing::info!(space = %event.space.name, "google push: REMOVED_FROM_SPACE for a space with no watched row under any account — nothing to do");
        return fan_out.finish(());
    }
    for account_id in account_ids {
        // CS-432: resolve THIS account's real content plane before
        // deactivating — same decision table `handle_chat_added_to_space`/
        // `run_chat_event` use. A failure here skips deactivation for this
        // one account only (the row stays active, so ingest is unaffected
        // either way until the next REMOVED_FROM_SPACE redelivery) rather
        // than dropping the whole event and leaving every other account's
        // row un-deactivated too.
        let content_pool = match crate::crypto::DekPlane::resolve_for_account(state, &pool, account_id).await {
            Ok(p) => p.content_pool().clone(),
            Err(e) => {
                fan_out.failed(plane_error(e), |e, retry| {
                    tracing::warn!(
                        space = %event.space.name, %account_id, error = %e, retry,
                        "google push: REMOVED_FROM_SPACE could not resolve this account's tenancy \
                         plane (the row stays active until the retry; continuing with any others)"
                    )
                });
                continue;
            }
        };
        match google_chat::record_removed_from_space(&content_pool, account_id, &event.space.name).await {
            Ok(true) => tracing::info!(space = %event.space.name, %account_id, "google push: REMOVED_FROM_SPACE — watched-space row deactivated"),
            Ok(false) => tracing::info!(space = %event.space.name, %account_id, "google push: REMOVED_FROM_SPACE — row already inactive for this account"),
            Err(e) => fan_out.failed(ConnectorError::Internal(format!("record_removed_from_space: {e}")), |e, retry| {
                tracing::warn!(space = %event.space.name, %account_id, error = %e, retry, "google push: failed to record REMOVED_FROM_SPACE for this account (continuing with any other accounts)")
            }),
        }
    }
    fan_out.finish(())
}

/// The Gmail round trip of one per-account pass — token load/refresh plus
/// the `history.list` diff (`google_gmail::fetch_pending`) — behind a seam,
/// so the route's own decisions (the per-mailbox fan-out, one account's
/// failure not reaching the others, the org flag gate, the advisory lock,
/// the plane choice, commit-after-ingest) run for real in a test without a
/// DEK-wrapped token or a network. Production hands in [`gmail_pass`]; a
/// test hands in a fake answering with a hand-built `PendingSync`.
pub(crate) type GmailPass = dyn for<'a> Fn(GmailPassInput<'a>) -> BoxFuture<'a, Result<google_gmail::PendingSync, ConnectorError>>
    + Send
    + Sync;

pub(crate) struct GmailPassInput<'a> {
    state: &'a Arc<AppState>,
    control_pool: &'a PgPool,
    content_pool: &'a PgPool,
    dek_plane: &'a crate::crypto::DekPlane,
    account_id: Uuid,
    owner_user_id: Uuid,
    plane: AccountPlane,
    email: &'a str,
}

pub(crate) fn gmail_pass(input: GmailPassInput<'_>) -> BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> {
    Box::pin(async move {
        let GmailPassInput { state, control_pool, content_pool, dek_plane, account_id, owner_user_id, plane, email } = input;
        let mut creds = load_creds_bg(control_pool, dek_plane, account_id, "google_gmail", TokenOwner::Member(owner_user_id))
            .await?
            .ok_or_else(|| ConnectorError::NotConfigured("gmail not connected".into()))?;
        let google = GoogleConnector;
        google.refresh_if_needed(&state.http_client, &mut creds).await?;
        if let Some(refreshed) = creds.refreshed.as_ref() {
            if let Err(e) = store_token_bundle(
                control_pool,
                dek_plane,
                account_id,
                "google_gmail",
                TokenOwner::Member(owner_user_id),
                refreshed,
            )
            .await
            {
                tracing::warn!(
                    %account_id, %owner_user_id, status = ?e.status(),
                    "google push: failed to persist refreshed gmail token (non-fatal)"
                );
            }
        }
        google_gmail::fetch_pending(
            &state.http_client,
            control_pool,
            content_pool,
            account_id,
            owner_user_id,
            plane,
            email,
            &creds.access_token,
        )
        .await
    })
}

/// Fan one mailbox's push out to every CoralSwarm account watching it —
/// `workers::ingest_receive`'s entry for [`ReceivedEvent::GmailPush`]. A
/// mailbox connected under both a personal account and an org account (the
/// normal shape for anyone using CoralSwarm in both) holds its own token,
/// cursor and content plane under each, and the notification is a bare
/// "this mailbox changed" trigger for all of them
/// (`connectors::google_gmail::resolve_watch_owners_for_push`). Each
/// account's pass is independent: one failing never blocks the others, and
/// the per-account outcome is logged in [`run_gmail_sync_for_mailbox`]. The
/// aggregate is an error only when some account failed transiently
/// (`ingest_receive::transient`), so the receive job retries and the
/// accounts that succeeded re-diff from their advanced cursors and find
/// nothing. A permanent failure (a 4xx, a dead credential) for one account
/// is logged and does not fail the push: a retry would re-run the healthy
/// accounts for the same answer. Otherwise `Ok(total ingested)`.
pub(crate) async fn run_gmail_sync_for_email_with(
    state: &Arc<AppState>,
    email: &str,
    pass: &GmailPass,
) -> Result<usize, ConnectorError> {
    let Some(pool) = state.db.clone() else {
        return Err(ConnectorError::Internal("no consumer pool configured".into()));
    };

    let targets = google_gmail::resolve_watch_owners_for_push(&pool, email).await?;
    if targets.is_empty() {
        return Err(ConnectorError::NotConfigured("gmail push: unknown mailbox".into()));
    }

    let mut ingested = 0usize;
    let mut fan_out = FanOut::new();
    for (account_id, owner_user_id) in targets {
        match run_gmail_sync_for_mailbox(state, &pool, CalendarPassGate::shared(), account_id, owner_user_id, email, pass)
            .await
        {
            Ok(n) => ingested += n,
            Err(e) => fan_out.failed(e, |e, retry| {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id, retry,
                    "google push: gmail sync failed for one watching account"
                )
            }),
        }
    }
    fan_out.finish(ingested)
}

/// One account's pass for one mailbox: load + refresh that account's Gmail
/// token, run an incremental `history.list` diff pass (commit-after-ingest
/// — see [`ingest_and_commit`]'s doc), and ingest every returned item into
/// the connecting member's own private ocean on the account's own content
/// plane. The push-triggered analog of `routes::connectors::ingest_selected`
/// + `finish_ingest`, without an HTTP request's `Tenant`/`UserContext` to
/// lean on — keyed by `owner_user_id` (per-member) rather than the account
/// owner. Returns how many items were ingested.
///
/// # Plane (org Gmail push lift)
/// A Pub/Sub push carries no `TenancyDecision`, so the account's
/// `clerk_org_id` plus THE decision table (`crypto::DekPlane::
/// resolve_for_org`) pick the plane — the same shape
/// [`handle_chat_added_to_space`] and `routes::google_calendar_push::
/// resolve_org_member_channel` use. An ordinary org resolves to its own
/// tenant pool, `AccountPlane::Org` and a real org `Tenant` for
/// `ingest_atom`; a personal account (or a personal-org without a tenant
/// database) resolves to `pool` and `Personal`, byte-identical to before.
/// `pool` stays the CONTROL pool for `oauth_tokens` and the
/// `google_watch_channels` cursor on both planes (`connectors::google_gmail`
/// module doc, "Watch rows are CONTROL-plane, always"); the content pool
/// carries `gmail_watched_threads`, `gmail_everything_mode`, graphs and
/// atoms. Before this lift the route hardcoded `Personal` with `pool` for
/// both, so an org member's pass read the watched-thread/everything-mode
/// tables on the wrong database, found nothing, dropped every live-follow
/// add and deletion, and advanced the cursor anyway. An org whose Gmail
/// flag is off is a logged no-op, like the Calendar push's own flag check.
///
/// # Per-mailbox serialization (CodeRabbit round 1 finding #3 on PR #686,
/// stability)
/// Pub/Sub does not guarantee at-most-once delivery, and this function runs
/// once per receive job, jobs running concurrently — two pushes for the SAME mailbox arriving close together would
/// otherwise run two concurrent `history.list` diffs from the SAME stored
/// cursor, then race to commit it: duplicate ingest at best, a REGRESSED
/// cursor at worst (a slower task's stale-cursor commit overwriting a
/// faster task's already-advanced one). A `pg_try_advisory_xact_lock` keyed
/// on `(account_id, 'gmail', email)` closes this: the SECOND concurrent
/// call for the same mailbox finds the lock already held and returns
/// immediately (`Ok(0)`, not an error) rather than blocking. Within one
/// process that can no longer happen — see "Pass bound" below: a push for a
/// mailbox with a pass queued or running here re-arms that pass instead —
/// so the lock now only guards against a pass on ANOTHER machine. A push
/// that misses it marks the watch row and the holder diffs once more after
/// it releases the lock ([`gmail_pass_once`]):
/// no worker re-runs Gmail `history.list` (the renewal tick only renews the
/// watch), so without that the push would wait for the next change. The
/// transaction-scoped variant (not `pg_try_advisory_lock`'s session-scoped
/// form) is deliberate: it auto-releases at commit/rollback regardless of
/// an early return, a propagated error, or a task panic, so there is no
/// manual-unlock call to forget and no leaked lock scenario to reason
/// about — same primitive `routes::oauth::refuse_second_org_slack_
/// workspace_locked` already uses for its own per-account advisory lock
/// (that one blocking, since it guards a human-driven action; this one
/// try-and-skip, since it guards a background retriggerable one).
///
/// # Pass bound (queue, never skip)
/// Holding that lock pins a CONTROL connection for the whole pass, and an
/// org member's ingest goes through the org's small tenant request pool, so
/// one task per push let a burst pin either pool. The pass now takes a
/// permit from `gate` (`CalendarPassGate::shared()` in production — the
/// same bound the calendar passes share, since they draw from the same
/// pools; `calendar_pass_gate`, "Gmail push shares both bounds") BEFORE the
/// lock: the process-wide slot, plus the org's own slot on an org plane.
/// Unlike a calendar push it never skips on a full gate: no worker re-runs
/// Gmail `history.list` (the renewal tick only renews the watch), so a
/// skipped push would sit until the mailbox's next change. It waits with no
/// deadline instead, holding no connection while queued.
///
/// A push for a mailbox whose pass is already queued or running in this
/// process returns `Ok(0)` at once and marks it dirty ([`MailboxPassMark`]);
/// when that pass ends with the flag set it clears it and diffs once more
/// from the cursor it just committed, under a fresh permit. So pushes are
/// never lost to the gate or to the mailbox lock within a process, and the
/// backlog stays one pass per `(account, mailbox)`. The org flag is checked
/// before the wait and again under the lock, so an org that turns Gmail off
/// while a pass is queued is not synced.
///
/// Limits: the marks are per process (across machines the watch row's
/// `push_pending_at` stands in, see [`gmail_pass_once`]). A queued or
/// re-arming pass dropped
/// with its task (a machine stopping) leaves its receive job `running`
/// until the reaper re-queues it (the cursor never moved). A push folded
/// into a pass gives that pass one more run if it lands mid-pass; a failing
/// pass returns its error, and a transient one retries the receive job.
async fn run_gmail_sync_for_mailbox(
    state: &Arc<AppState>,
    pool: &PgPool,
    gate: &CalendarPassGate,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    pass: &GmailPass,
) -> Result<usize, ConnectorError> {
    // Flag and plane first — control-plane reads, no lock — so the permit
    // below can carry the org's own slot (module doc above, "Pass bound").
    let org_id: Option<String> = sqlx::query_scalar("SELECT clerk_org_id FROM accounts WHERE id = $1")
        .bind(account_id)
        .fetch_optional(pool)
        .await
        .map_err(|e| ConnectorError::Internal(format!("gmail push org lookup: {e}")))?
        .flatten();
    if let Some(org_id) = org_id.as_deref() {
        if !crate::routes::oauth::org_google_gmail_enabled_for(state, org_id).await {
            tracing::info!(%account_id, %org_id, "google push: org gmail flag is off — not syncing");
            return Ok(0);
        }
    }
    // The member's pause or off switch for Gmail, before the mailbox is read or
    // any token refreshed. The history cursor stays, so a resumed source diffs
    // from it.
    if crate::connectors::catalog::pause::is_paused(
        pool, account_id, owner_user_id, crate::connectors::catalog::pause::GOOGLE_GMAIL,
    ).await {
        tracing::info!(%account_id, %owner_user_id, "google push: gmail source is paused or off -- not syncing");
        return Ok(0);
    }
    let dek_plane = crate::crypto::DekPlane::resolve_for_org(state, pool, account_id, org_id.as_deref())
        .await
        .map_err(|e| ConnectorError::Internal(format!("resolve DEK plane: {e}")))?;
    // Live membership, not just an unarchived ocean (settings launch PR 2.8):
    // a member who left the org must not have their mailbox read, their token
    // refreshed, or deletions applied, in the window before the member-ocean
    // reconciler archives their ocean — or ever, if it is off. A refusal also
    // offboards their capture, so this watch stops for good.
    if org_id.is_some()
        && !crate::connectors::member_offboarding::admit_capture_or_offboard(
            &state.http_client,
            pool,
            Some(&dek_plane),
            account_id,
            owner_user_id,
            "gmail_push",
        )
        .await
    {
        return Ok(0);
    }
    let content_pool = dek_plane.content_pool().clone();
    let org_id = org_id.filter(|_| dek_plane.is_org());

    let Some(mut mark) = MailboxPassMark::enter(account_id, email) else {
        tracing::info!(
            %account_id, %owner_user_id,
            "google push: a gmail pass for this mailbox is already queued or running here — \
             marked dirty, it diffs once more after this push"
        );
        return Ok(0);
    };
    let tenant_bound = org_id.as_deref().map(|org_id| crate::calendar_pass_gate::OrgSlot::google(org_id, &content_pool));
    let mut ingested = 0usize;
    loop {
        let permit = gate.wait_scoped(tenant_bound).await;
        // Cleared BEFORE the pass reads the cursor: a push landing from here
        // on may postdate that read, so it re-arms the flag for one more run.
        mark.take_dirty();
        let GmailPassOnce { result, rerun } = gmail_pass_once(
            state,
            pool,
            &content_pool,
            &dek_plane,
            org_id.as_deref(),
            account_id,
            owner_user_id,
            email,
            pass,
        )
        .await;
        drop(permit);
        if rerun {
            mark.set_dirty();
        }
        if mark.finish_unless_dirty() {
            return result.map(|n| ingested + n);
        }
        match result {
            Ok(n) => ingested += n,
            Err(e) => tracing::warn!(
                error = %e, %account_id, %owner_user_id,
                "google push: gmail pass failed; a push landed meanwhile, so it runs once more"
            ),
        }
    }
}

/// What one [`gmail_pass_once`] did.
struct GmailPassOnce {
    result: Result<usize, ConnectorError>,
    /// This attempt held the mailbox lock, and a push on another machine
    /// missed it meanwhile: run once more.
    rerun: bool,
}

impl GmailPassOnce {
    fn done(result: Result<usize, ConnectorError>) -> Self {
        GmailPassOnce { result, rerun: false }
    }
}

macro_rules! or_done {
    ($e:expr) => {
        match $e {
            Ok(v) => v,
            Err(e) => return GmailPassOnce::done(Err(e)),
        }
    };
}

/// A mailbox pass under the gate permit and transaction-scoped advisory lock.
/// A lock miss commits a pending mark before a second lock attempt. That retry
/// covers a holder releasing before the mark commits. The holder clears the
/// mark before reading history and checks it after releasing the lock, so a
/// push arriving during the pass causes another diff. A mark consumed by a
/// failed or cancelled pass still relies on a later push for recovery.
#[allow(clippy::too_many_arguments)]
async fn gmail_pass_once(
    state: &Arc<AppState>,
    pool: &PgPool,
    content_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    org_id: Option<&str>,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    pass: &GmailPass,
) -> GmailPassOnce {
    let mut lock_tx = or_done!(pool
        .begin()
        .await
        .map_err(|e| ConnectorError::Internal(format!("gmail sync lock: begin failed: {e}"))));
    let mut acquired = or_done!(try_mailbox_lock(&mut lock_tx, account_id, email).await);
    if !acquired {
        if let Err(e) = google_gmail::mark_push_pending(pool, account_id, email).await {
            lock_tx.rollback().await.ok();
            return GmailPassOnce::done(Err(e));
        }
        acquired = or_done!(try_mailbox_lock(&mut lock_tx, account_id, email).await);
    }
    if !acquired {
        tracing::info!(
            %account_id, %owner_user_id,
            "google push: a gmail sync for this mailbox is in flight on another machine — \
             marked the mailbox; that pass diffs once more when it ends"
        );
        lock_tx.rollback().await.ok();
        return GmailPassOnce::done(Ok(0));
    }

    // Re-checked under the lock: the early check in the caller may be from
    // before an unbounded wait for the gate, and an admin may have turned
    // Gmail off since.
    if let Some(org_id) = org_id {
        if !crate::routes::oauth::org_google_gmail_enabled_for(state, org_id).await {
            tracing::info!(%account_id, %org_id, "google push: org gmail flag turned off while queued — not syncing");
            lock_tx.rollback().await.ok();
            return GmailPassOnce::done(Ok(0));
        }
        // Membership re-checked under the lock for the same reason: the
        // member may have left while this pass waited for the gate. Refuse
        // only; the next push's entry gate does the offboarding.
        if !matches!(
            crate::connectors::member_offboarding::capture_owner_is_live_member(pool, account_id, owner_user_id).await,
            Ok(true)
        ) {
            tracing::warn!(%account_id, %owner_user_id, "google push: watch owner left the org while queued — not syncing");
            lock_tx.rollback().await.ok();
            return GmailPassOnce::done(Ok(0));
        }
    }

    // Before the pass reads the cursor: every push that marked before this
    // is covered by this pass.
    if let Err(e) = google_gmail::clear_push_pending(pool, account_id, email).await {
        lock_tx.rollback().await.ok();
        return GmailPassOnce::done(Err(e));
    }

    let result =
        gmail_pass_under_lock(state, pool, content_pool, dek_plane, org_id, account_id, owner_user_id, email, pass).await;

    // Release before checking the mark, so a later lock miss either reaches
    // this check or takes the lock on its second attempt.
    lock_tx.rollback().await.ok();

    let rerun = match google_gmail::push_pending(pool, account_id, email).await {
        Ok(pending) => pending,
        Err(e) => {
            tracing::warn!(
                error = %e, %account_id, %owner_user_id,
                "google push: could not read the gmail push mark after a pass; a push another machine \
                 could not run waits for the mailbox's next change"
            );
            false
        }
    };
    if rerun {
        tracing::info!(
            %account_id, %owner_user_id,
            "google push: a push on another machine missed this mailbox's lock during the pass — diffing once more"
        );
    }
    GmailPassOnce { result, rerun }
}

/// `pg_try_advisory_xact_lock` on the mailbox's pass lock, in `lock_tx`.
async fn try_mailbox_lock(
    lock_tx: &mut sqlx::Transaction<'static, sqlx::Postgres>,
    account_id: Uuid,
    email: &str,
) -> Result<bool, ConnectorError> {
    sqlx::query_scalar("SELECT pg_try_advisory_xact_lock(hashtext('gmail_sync:' || $1::text || ':' || $2))")
        .bind(account_id.to_string())
        .bind(email)
        .fetch_one(&mut **lock_tx)
        .await
        .map_err(|e| ConnectorError::Internal(format!("gmail sync lock: acquire failed: {e}")))
}

/// The Gmail round trip and the ingest + cursor commit, under the mailbox
/// lock ([`gmail_pass_once`]).
#[allow(clippy::too_many_arguments)]
async fn gmail_pass_under_lock(
    state: &Arc<AppState>,
    pool: &PgPool,
    content_pool: &PgPool,
    dek_plane: &crate::crypto::DekPlane,
    org_id: Option<&str>,
    account_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    pass: &GmailPass,
) -> Result<usize, ConnectorError> {
    let (plane, tenant) = match org_id {
        Some(org_id) => (AccountPlane::Org, org_tenant(content_pool, org_id.to_string(), account_id, owner_user_id)),
        None => (AccountPlane::Personal, Tenant::legacy(state)),
    };

    // Per Google user: one mailbox can be watched by a personal and an org
    // account, and Gmail's quota is per user (`google_gmail::rate_key`).
    let key = google_gmail::rate_key(account_id, email);
    crate::graph_rate_limit::scoped(key, async move {
        let pending = pass(GmailPassInput {
            state,
            control_pool: pool,
            content_pool,
            dek_plane,
            account_id,
            owner_user_id,
            plane,
            email,
        })
        .await?;

        if pending.items.is_empty() {
            // Nothing to ingest — advance unconditionally (this also covers
            // a pure-deletion pass, already applied by `fetch_pending`).
            google_gmail::commit_pending(pool, account_id, email, pending.commit).await?;
            return Ok(0);
        }

        let graph_id = resolve_primary_graph(content_pool, account_id, plane, IngestCaller::Attributable(owner_user_id))
            .await
            .map_err(|e| ConnectorError::Internal(format!("resolve_primary_graph: {e}")))?;

        ingest_and_commit(state, pool, tenant, account_id, graph_id, owner_user_id, email, pending).await
    })
    .await
}

/// The `(account, mailbox)` pairs with a Gmail push pass queued or running
/// in this process ([`run_gmail_sync_for_mailbox`], "Pass bound"), each
/// with its dirty flag: set by any push that lands while the pass is
/// queued or running, cleared just before each run reads the cursor. The
/// mark lives until the pass loop ends ([`Self::finish_unless_dirty`]);
/// dropped any other way (the task cancelled, a machine stopping) it logs
/// the drop — the cursor never moved, so the mailbox's next change
/// re-diffs everything.
struct MailboxPassMark {
    key: (Uuid, String),
    finished: bool,
}

type MailboxPassMarks = std::sync::Mutex<HashMap<(Uuid, String), bool>>;

impl MailboxPassMark {
    fn marks() -> &'static MailboxPassMarks {
        static MARKS: OnceLock<MailboxPassMarks> = OnceLock::new();
        MARKS.get_or_init(Default::default)
    }

    fn lock() -> std::sync::MutexGuard<'static, HashMap<(Uuid, String), bool>> {
        Self::marks().lock().unwrap_or_else(|p| p.into_inner())
    }

    /// The mark, or `None` (having set the dirty flag) when a pass for this
    /// mailbox is already queued or running here.
    fn enter(account_id: Uuid, email: &str) -> Option<Self> {
        let key = (account_id, email.to_string());
        let mut marks = Self::lock();
        if let Some(dirty) = marks.get_mut(&key) {
            *dirty = true;
            return None;
        }
        marks.insert(key.clone(), false);
        drop(marks);
        Some(MailboxPassMark { key, finished: false })
    }

    fn set_dirty(&self) {
        if let Some(dirty) = Self::lock().get_mut(&self.key) {
            *dirty = true;
        }
    }

    fn take_dirty(&self) {
        if let Some(dirty) = Self::lock().get_mut(&self.key) {
            *dirty = false;
        }
    }

    /// End the loop unless a push landed during this run — atomically, so
    /// a push either sees the mark and re-arms it, or finds none and starts
    /// its own pass.
    fn finish_unless_dirty(&mut self) -> bool {
        let mut marks = Self::lock();
        if marks.get(&self.key).copied().unwrap_or(false) {
            return false;
        }
        marks.remove(&self.key);
        self.finished = true;
        true
    }
}

impl Drop for MailboxPassMark {
    fn drop(&mut self) {
        if self.finished {
            return;
        }
        let mut marks = Self::lock();
        marks.remove(&self.key);
        tracing::warn!(
            account_id = %self.key.0, still_marked_here = marks.len(),
            "google push: a gmail pass was dropped before it finished (task cancelled or machine stopping); \
             the cursor did not move, so the mailbox's next change re-diffs it"
        );
    }
}

/// The `Tenant` an org member's push-triggered ingest runs under — the org's
/// own content pool and a real `TenancyDecision::Org`, what the extractor
/// would have built for a request from that member.
fn org_tenant(content_pool: &PgPool, org_id: String, account_id: Uuid, owner_user_id: Uuid) -> Tenant {
    let store: Arc<dyn Store> = Arc::new(PostgresStore::new(content_pool.clone()));
    Tenant {
        store: Some(store),
        pool: Some(content_pool.clone()),
        decision: Some(TenancyDecision::Org(TenantContext {
            org_id,
            account_id,
            user_id: owner_user_id,
            pool: content_pool.clone(),
            page_pool: None,
        })),
        class: crate::route_class::RouteClass::Other,
    }
}

/// Ingest every item in `pending.items` under `tenant` (the org's own
/// `TenancyDecision::Org` tenant on the org plane, `Tenant::legacy` on the
/// personal plane), then commit `pending.commit` (advance the history cursor
/// on `control_pool`) ONLY IF every one of them succeeded — see
/// `connectors::google_gmail::fetch_pending`'s doc, "commit-after-ingest",
/// byte-identical discipline to `routes::google_calendar_push::
/// ingest_and_commit` (including the 422-is-consumed rule). A live-follow
/// arrival lands in `graph_id`; a restored (un-trashed / "not spam") message
/// lands back in the ocean its hide tombstoned it out of
/// (`PendingItem::restore_graph_id`, on the content pool `tenant` carries).
/// Its tombstone is cleared only AFTER the cursor commit: cleared per item,
/// a restore that succeeded next to an item that failed would leave the
/// replay with no tombstone, and the message would go through live-follow
/// into `graph_id` — a duplicate when that is not the tombstoned ocean (the
/// re-ingest into the tombstoned one is deduped by `ON CONFLICT`). A restore
/// refused with 403/404 — the tombstoned ocean is no longer writable by its
/// owner, e.g. an archived member ocean — is handled, not blocking: the
/// tombstone is dropped with the commit, since retrying would refuse the
/// same way and one such message must not wedge every later hide, delete
/// and live-follow for the mailbox. Split out from
/// [`run_gmail_sync_for_mailbox`] so a test can drive it directly against a
/// hand-built `PendingSync`. Returns how many items were ingested.
#[allow(clippy::too_many_arguments)]
async fn ingest_and_commit(
    state: &Arc<AppState>,
    control_pool: &PgPool,
    tenant: Tenant,
    account_id: Uuid,
    graph_id: Uuid,
    owner_user_id: Uuid,
    email: &str,
    pending: google_gmail::PendingSync,
) -> Result<usize, ConnectorError> {
    let mut ingested = 0usize;
    let mut all_succeeded = true;
    let mut tombstones_to_clear: Vec<(Uuid, String)> = Vec::new();
    for google_gmail::PendingItem { item, restore_graph_id } in pending.items {
        if restore_graph_id.is_none() {
            if let Some(thread_id) = item.thread_id.as_deref() {
                let content_pool = tenant.pool.as_ref().ok_or_else(|| {
                    ConnectorError::Internal("gmail push: no content pool to record the reply destination".into())
                })?;
                google_gmail::record_reply_landing_graph(content_pool, account_id, owner_user_id, thread_id, graph_id).await?;
            }
        }
        let user_ctx = UserContext {
            user_id: owner_user_id,
            account_id,
            role: "member".into(),
            device_id: String::new(),
        };
        let source_id_for_log = item.source_id.clone();
        let req = IngestAtomRequest {
            graph_id: restore_graph_id.unwrap_or(graph_id),
            source_type: item.source_type,
            source_id: item.source_id,
            content: item.content,
            participants: item.participants,
            timestamp: item.timestamp,
            scope: item.scope,
            fts_language: None,
            channel_id: item.channel_id,
            channel_name: None,
            author_id: item.author_id,
            author_name: item.author_name,
            project: None,
            mcp_client_name: None,
            raw_content: None,
            duration_ms: None,
            segments: None,
            session_id: None,
            session_kind: None,
            session_title: None,
            repo_path: None,
            remote: None,
            hostname: None,
            harness_version: None,
            model: None,
            branch: None,
            agent_name: None,
            platform: None,
            task: None,
            parent_session_id: None,
            trusted_user_agent: None,
            client_info: None,
            code_path: None,
        };
        let source_namespace = crate::connectors::google_gmail::rate_key(account_id, email);
        let refused = crate::ingest::submit(crate::ingest::IngestRequest::private(crate::ingest::Member {
            provider_namespace: Some(&source_namespace),
            ..crate::ingest::Member::new(state, &tenant, &user_ctx, req)
        })).await.err();
        match refused {
            None => {
                ingested += 1;
                if let Some(restore_graph_id) = restore_graph_id {
                    tombstones_to_clear.push((restore_graph_id, source_id_for_log));
                }
            }
            // Deterministic validation rejection — never blocks the cursor
            // (same reasoning `routes::google_calendar_push::
            // ingest_and_commit`'s own comment gives; a retry would never
            // turn it into success). A restore's tombstone is left in
            // place: harmless, and cleared by the permanent delete.
            Some(refused) if refused.is_invalid() => {
                tracing::warn!(
                    %account_id, %owner_user_id, source_id = %source_id_for_log, error = %refused,
                    "google push: the ingest door rejected this message as invalid — \
                     skipping permanently, not blocking the history cursor"
                );
            }
            Some(refused) => match restore_graph_id {
                // The ocean a restore points at is no longer the owner's to
                // write (the door's 403; it never answers 404). Drop the
                // tombstone rather than hold the cursor.
                Some(restore_graph_id) if refused.is_forbidden() => {
                    tracing::warn!(
                        error = %refused, %account_id, %owner_user_id, source_id = %source_id_for_log, %restore_graph_id,
                        "google push: the ocean this message was hidden from is no longer writable by its owner — \
                         dropping the tombstone, not blocking the history cursor"
                    );
                    tombstones_to_clear.push((restore_graph_id, source_id_for_log));
                }
                _ => {
                    all_succeeded = false;
                    tracing::warn!(
                        error = %refused, %account_id, %owner_user_id, source_id = %source_id_for_log,
                        "google push: ingest_atom failed for one message — history cursor will NOT advance this pass"
                    );
                }
            },
        }
    }

    if all_succeeded {
        google_gmail::commit_pending(control_pool, account_id, email, pending.commit).await?;
        if !tombstones_to_clear.is_empty() {
            let content_pool = tenant
                .pool
                .as_ref()
                .ok_or_else(|| ConnectorError::Internal("gmail push: no content pool to clear the tombstones on".into()))?;
            for (restore_graph_id, message_id) in tombstones_to_clear {
                google_gmail::clear_hidden_message(content_pool, account_id, restore_graph_id, &message_id).await?;
            }
        }
    } else {
        tracing::warn!(
            %account_id, %owner_user_id, ingested, skipped_committing = true,
            "google push: one or more ingests failed — history cursor left unchanged, \
             next pass re-fetches this range"
        );
    }
    Ok(ingested)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `ce-source` is the subscription's full resource name, as documented.
    /// `ce-subject` is a guess at the user target: Google documents it only
    /// as "the resource where the event occurred", so the decoder must not
    /// depend on it. The transcript name travels in the payload.
    fn meet_push_attributes() -> HashMap<String, String> {
        HashMap::from([
            ("ce-specversion".into(), "1.0".into()),
            ("ce-time".into(), "2026-10-10T17:33:55.000000Z".into()),
            ("ce-datacontenttype".into(), "application/json".into()),
            ("ce-type".into(), crate::connectors::google_meet_events::EVENT_TYPE.into()),
            ("ce-source".into(), "//workspaceevents.googleapis.com/subscriptions/s1".into()),
            ("ce-subject".into(), "//cloudidentity.googleapis.com/users/108123456789".into()),
        ])
    }

    fn meet_push_data(transcript_name: &str) -> String {
        use base64::Engine as _;
        base64::engine::general_purpose::STANDARD
            .encode(format!(r#"{{"transcript":{{"name":"{transcript_name}"}}}}"#))
    }

    #[test]
    fn meet_event_routes_on_the_payload_transcript_not_the_subject() {
        let attributes = meet_push_attributes();
        let body = meet_push_data("conferenceRecords/r1/transcripts/t1");
        assert_eq!(
            decode_meet_event(&attributes, &body).unwrap(),
            MeetEvent { subscription: "subscriptions/s1".into(), record: "conferenceRecords/r1".into() }
        );
        let mut no_subject = attributes.clone();
        no_subject.remove("ce-subject");
        assert!(decode_meet_event(&no_subject, &body).is_ok(), "ce-subject is informational");
        let mut record_subject = attributes.clone();
        record_subject.insert("ce-subject".into(), "//meet.googleapis.com/conferenceRecords/r1".into());
        assert!(decode_meet_event(&record_subject, &body).is_ok(), "an undocumented subject shape still decodes");
        assert_eq!(classify_subscription("projects/p/subscriptions/meet-events-sub"), PushSurface::Meet);
    }

    #[test]
    fn meet_event_rejects_wrong_type_source_or_transcript_shape() {
        let attributes = meet_push_attributes();
        let body = meet_push_data("conferenceRecords/r1/transcripts/t1");
        let mut wrong_type = attributes.clone();
        wrong_type.insert("ce-type".into(), "google.workspace.meet.transcript.v2.started".into());
        assert_eq!(decode_meet_event(&wrong_type, &body).unwrap_err(), "unsupported Meet event type");
        let mut wrong_source = attributes.clone();
        wrong_source.insert("ce-source".into(), "//wrong.googleapis.com/subscriptions/s1".into());
        assert_eq!(decode_meet_event(&wrong_source, &body).unwrap_err(), "invalid Meet subscription source");
        let mut nested_source = attributes.clone();
        nested_source.insert("ce-source".into(), "//workspaceevents.googleapis.com/subscriptions/s1/extra".into());
        assert!(decode_meet_event(&nested_source, &body).is_err());
        for bad in [
            "conferenceRecords/r1",
            "conferenceRecords//transcripts/t1",
            "conferenceRecords/r1/recordings/x1",
            "conferenceRecords/r 1/transcripts/t1",
            "spaces/abc/transcripts/t1",
        ] {
            assert_eq!(
                decode_meet_event(&attributes, &meet_push_data(bad)).unwrap_err(),
                "invalid Meet transcript resource",
                "{bad}"
            );
        }
        assert_eq!(decode_meet_event(&attributes, "not base64!").unwrap_err(), "invalid Meet payload encoding");
        let not_json = {
            use base64::Engine as _;
            base64::engine::general_purpose::STANDARD.encode("{")
        };
        assert_eq!(decode_meet_event(&attributes, &not_json).unwrap_err(), "invalid Meet payload JSON");
    }

    #[test]
    fn meet_push_drop_diagnostics_keep_ids_out_of_the_log() {
        assert_eq!(
            resource_shape("//cloudidentity.googleapis.com/users/108123456789"),
            "//cloudidentity.googleapis.com/users/*"
        );
        assert_eq!(
            resource_shape("//meet.googleapis.com/conferenceRecords/abc/transcripts/def"),
            "//meet.googleapis.com/conferenceRecords/*/transcripts/*"
        );
        assert_eq!(resource_shape("conferenceRecords/abc/transcripts/def"), "opaque(len=37)");
        assert_eq!(resource_shape(""), "absent");
        let keys = meet_decode_diagnostics(&meet_push_attributes());
        assert_eq!(
            keys.attribute_keys,
            "ce-datacontenttype,ce-source,ce-specversion,ce-subject,ce-time,ce-type"
        );
        assert_eq!(keys.subject_shape, "//cloudidentity.googleapis.com/users/*");
        assert_eq!(keys.source_shape, "//workspaceevents.googleapis.com/subscriptions/*");
    }

    use jsonwebtoken::{encode, EncodingKey, Header};
    use serde_json::json;

    // Throwaway RSA-2048 test keypair, never used in production. Mirrors
    // `clerk::test_support`'s fixture exactly (same key material is fine to
    // reuse across modules — it is a public test fixture, not a secret).
    const TEST_PRIV_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\n\
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
    const TEST_KID: &str = "test-key-1";
    const TEST_N: &str = "yFjQlKieXg-zR_1bqt-8xitNNJKf3_fFvtVM6LZNfhV0Qy6Lg63lHuxYPsqcCMi4cGSja54wwaKIHKGxLhyAmrEiv_YWCNUZFi8a8SBiImGg3thDsV_RftbEYuD9-RG5_LU08jTyJYdS0o9qSRreIM_dsTNodZA3M9OSIZF7aVetio2rgUJsd3rNBBH9m5SLEgmWPi4k6Yx2dHT5JGSPXqoITgwqQdniVRmTS6ySt9vUKax5PnOFabChyYZbd5d47TPqdVoxN_tYdCtWMAQkmluHPsL9KiNG9u86ufaFKisCtIyjFdxOZT9MCyYleDwrSiAaZ_uu1HgOFTPPXnaUqw";
    const TEST_E: &str = "AQAB";

    #[tokio::test]
    async fn meet_public_ingress_requires_signed_identity_and_drops_unknown_subscription_pg() {
        let Some(base)=chat_test_base_url() else {return;};let db="gws_meet_public_ingress";
        chat_recreate(&base,db).await;
        let pool=PgPoolOptions::new().connect(&chat_with_db(&base,db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();
        let state=Arc::new(crate::test_app_state_owned_with_db(pool.clone(),None));
        std::env::set_var("GOOGLE_PUSH_AUDIENCE",test_cfg().audience);
        std::env::set_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL",test_cfg().service_account_email);
        *cache().write().await=Some(CachedCerts {keys:HashMap::from([(TEST_KID.into(),test_key())]),fetched:Instant::now()});
        use base64::Engine as _;
        let data=base64::engine::general_purpose::STANDARD.encode(r#"{"transcript":{"name":"conferenceRecords/r1/transcripts/t1"}}"#);
        let body=Bytes::from(serde_json::to_vec(&json!({"subscription":"projects/test/subscriptions/meet-events-sub","message":{"data":data,"attributes":{"ce-type":crate::connectors::google_meet_events::EVENT_TYPE,"ce-source":"//workspaceevents.googleapis.com/subscriptions/unknown","ce-subject":"//cloudidentity.googleapis.com/users/108123456789"}}})).unwrap());
        assert_eq!(google_push(State(state.clone()),HeaderMap::new(),body.clone()).await.status(),StatusCode::UNAUTHORIZED);
        let mut wrong=good_claims();wrong["aud"]=json!("another-app");let mut headers=HeaderMap::new();
        headers.insert("authorization",format!("Bearer {}",mint(wrong)).parse().unwrap());
        assert_eq!(google_push(State(state.clone()),headers.clone(),body.clone()).await.status(),StatusCode::FORBIDDEN);
        headers.insert("authorization",format!("Bearer {}",mint(good_claims())).parse().unwrap());
        assert_eq!(google_push(State(state.clone()),headers,body.clone()).await.status(),StatusCode::OK);
        let _ = dispatch(state.clone(),serde_json::from_slice(&body).unwrap()).await;
        for table in ["atoms","google_meet_captures","google_watch_channels"] {assert_eq!(sqlx::query_scalar::<_,i64>(&format!("SELECT count(*) FROM {table}")).fetch_one(&pool).await.unwrap(),0);}
        std::env::remove_var("GOOGLE_PUSH_AUDIENCE");std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
        *cache().write().await=None;
        drop(state);pool.close().await;chat_drop(&base,db).await;
    }

    fn mint(claims: serde_json::Value) -> String {
        let mut header = Header::new(Algorithm::RS256);
        header.kid = Some(TEST_KID.to_string());
        let key = EncodingKey::from_rsa_pem(TEST_PRIV_PEM.as_bytes()).unwrap();
        encode(&header, &claims, &key).unwrap()
    }

    fn test_key() -> DecodingKey {
        DecodingKey::from_rsa_components(TEST_N, TEST_E).unwrap()
    }

    fn future_exp() -> usize {
        (chrono::Utc::now().timestamp() + 3600) as usize
    }

    fn good_claims() -> serde_json::Value {
        json!({
            "iss": "https://accounts.google.com",
            "aud": "https://api.example.com/v1/webhooks/google",
            "exp": future_exp(),
            "email": "push-sa@my-project.iam.gserviceaccount.com",
            "email_verified": true,
        })
    }

    fn test_cfg() -> PushConfig {
        PushConfig {
            audience: "https://api.example.com/v1/webhooks/google".into(),
            service_account_email: "push-sa@my-project.iam.gserviceaccount.com".into(),
            certs_url: DEFAULT_CERTS_URL.into(),
        }
    }

    // ── force_refresh_due (pure — no I/O, no shared state; CodeRabbit review
    // finding on PR #674: cooldown + single-flight for the unknown-kid
    // forced-refresh path) ──────────────────────────────────────────────────

    #[test]
    fn force_refresh_due_when_never_refreshed() {
        assert!(force_refresh_due(None, Instant::now(), FORCE_REFRESH_COOLDOWN));
    }

    #[test]
    fn force_refresh_not_due_within_cooldown() {
        let last = Instant::now();
        let now = last + Duration::from_secs(1);
        assert!(!force_refresh_due(Some(last), now, FORCE_REFRESH_COOLDOWN));
    }

    #[test]
    fn force_refresh_due_once_cooldown_elapses() {
        let last = Instant::now();
        let now = last + FORCE_REFRESH_COOLDOWN;
        assert!(force_refresh_due(Some(last), now, FORCE_REFRESH_COOLDOWN));
        let just_under = last + FORCE_REFRESH_COOLDOWN - Duration::from_millis(1);
        assert!(!force_refresh_due(Some(last), just_under, FORCE_REFRESH_COOLDOWN));
    }

    // ── decode_claims (pure, no I/O) ────────────────────────────────────────

    #[test]
    fn decode_claims_accepts_valid_token_either_issuer_form() {
        for iss in ["accounts.google.com", "https://accounts.google.com"] {
            let mut c = good_claims();
            c["iss"] = json!(iss);
            let token = mint(c);
            let claims = decode_claims(&token, &test_key()).unwrap();
            assert_eq!(claims.iss, iss);
        }
    }

    #[test]
    fn decode_claims_rejects_wrong_issuer() {
        let mut c = good_claims();
        c["iss"] = json!("https://evil.example");
        let token = mint(c);
        assert!(decode_claims(&token, &test_key()).is_err());
    }

    #[test]
    fn decode_claims_rejects_expired_token() {
        let mut c = good_claims();
        c["exp"] = json!((chrono::Utc::now().timestamp() - 7200) as usize);
        let token = mint(c);
        assert!(decode_claims(&token, &test_key()).is_err());
    }

    #[test]
    fn decode_claims_rejects_tampered_signature() {
        let token = mint(good_claims());
        let mut parts: Vec<&str> = token.split('.').collect();
        let sig: String = parts[2].chars().rev().collect();
        parts[2] = &sig;
        let tampered = parts.join(".");
        assert!(decode_claims(&tampered, &test_key()).is_err());
    }

    // ── PushConfig::from_env ────────────────────────────────────────────────

    static GOOGLE_PUSH_ENV_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

    #[tokio::test]
    async fn push_config_from_env_none_when_unset() {
        let _guard = GOOGLE_PUSH_ENV_LOCK.lock().await;
        std::env::remove_var("GOOGLE_PUSH_AUDIENCE");
        std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
        assert!(PushConfig::from_env().is_none());
    }

    #[tokio::test]
    async fn push_config_from_env_none_when_only_one_set() {
        let _guard = GOOGLE_PUSH_ENV_LOCK.lock().await;
        std::env::set_var("GOOGLE_PUSH_AUDIENCE", "https://api.example.com/v1/webhooks/google");
        std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
        assert!(PushConfig::from_env().is_none());
        std::env::remove_var("GOOGLE_PUSH_AUDIENCE");
    }

    #[tokio::test]
    async fn push_config_from_env_some_when_both_set_defaults_certs_url() {
        let _guard = GOOGLE_PUSH_ENV_LOCK.lock().await;
        std::env::set_var("GOOGLE_PUSH_AUDIENCE", "aud-1");
        std::env::set_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL", "sa@example.iam.gserviceaccount.com");
        std::env::remove_var("GOOGLE_OIDC_CERTS_URL");
        let cfg = PushConfig::from_env().expect("both set");
        assert_eq!(cfg.audience, "aud-1");
        assert_eq!(cfg.certs_url, DEFAULT_CERTS_URL);
        std::env::remove_var("GOOGLE_PUSH_AUDIENCE");
        std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
    }

    // ── bearer_token ─────────────────────────────────────────────────────────

    #[test]
    fn bearer_token_extracts_from_authorization_header() {
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, "Bearer abc.def.ghi".parse().unwrap());
        assert_eq!(bearer_token(&headers).unwrap(), "abc.def.ghi");
    }

    #[test]
    fn bearer_token_rejects_missing_or_malformed() {
        assert!(bearer_token(&HeaderMap::new()).is_err());
        let mut headers = HeaderMap::new();
        headers.insert(axum::http::header::AUTHORIZATION, "Basic abc".parse().unwrap());
        assert!(bearer_token(&headers).is_err());
        let mut empty = HeaderMap::new();
        empty.insert(axum::http::header::AUTHORIZATION, "Bearer ".parse().unwrap());
        assert!(bearer_token(&empty).is_err());
    }

    // ── verify_push_token: audience/email enforcement (pure claims logic,
    // exercised via decode_claims + the same comparisons verify_push_token
    // makes — full HTTP-fetch path covered by the pg/network-free unit tests
    // above and left to a live-endpoint smoke test, same posture as clerk's
    // JWKS fetch not being unit-tested against a live Clerk instance).

    #[test]
    fn identity_check_rejects_wrong_audience() {
        let cfg = test_cfg();
        let mut c = good_claims();
        c["aud"] = json!("https://someone-else.example/webhook");
        let token = mint(c);
        let claims = decode_claims(&token, &test_key()).unwrap();
        assert_ne!(claims.aud, cfg.audience);
    }

    #[test]
    fn identity_check_rejects_unverified_email() {
        let cfg = test_cfg();
        let mut c = good_claims();
        c["email_verified"] = json!(false);
        let token = mint(c);
        let claims = decode_claims(&token, &test_key()).unwrap();
        assert_eq!(claims.email.as_deref(), Some(cfg.service_account_email.as_str()));
        assert_ne!(claims.email_verified, Some(true));
    }

    #[test]
    fn identity_check_rejects_wrong_service_account() {
        let cfg = test_cfg();
        let mut c = good_claims();
        c["email"] = json!("someone-else@other-project.iam.gserviceaccount.com");
        let token = mint(c);
        let claims = decode_claims(&token, &test_key()).unwrap();
        assert_ne!(claims.email.as_deref(), Some(cfg.service_account_email.as_str()));
    }

    // ── Envelope parsing (malformed body must never fail loudly) ───────────

    #[test]
    fn push_envelope_parses_standard_shape() {
        let body = json!({
            "message": {
                "data": "eyJoaXN0b3J5SWQiOiI0MTIzIn0=",
                "messageId": "12345",
                "publishTime": "2026-08-22T00:00:00Z",
                "attributes": {"foo": "bar"}
            },
            "subscription": "projects/my-project/subscriptions/gmail-push-sub"
        });
        let envelope: PushEnvelope = serde_json::from_value(body).unwrap();
        assert_eq!(envelope.message.message_id, "12345");
        assert_eq!(envelope.subscription, "projects/my-project/subscriptions/gmail-push-sub");
        assert_eq!(envelope.message.attributes.get("foo").map(String::as_str), Some("bar"));
    }

    #[test]
    fn push_envelope_rejects_missing_message_field() {
        let body = json!({"subscription": "projects/p/subscriptions/s"});
        let result: Result<PushEnvelope, _> = serde_json::from_value(body);
        assert!(result.is_err(), "message is required, not defaulted");
    }

    // ── Subscription routing (gsuite Train G2 PR 3) ─────────────────────────

    #[test]
    fn classify_subscription_routes_gmail_chat_unknown() {
        assert_eq!(
            classify_subscription("projects/coralswarm-prod/subscriptions/gmail-push-sub"),
            PushSurface::Gmail
        );
        assert_eq!(
            classify_subscription("projects/coralswarm-prod/subscriptions/chat-events-sub"),
            PushSurface::Chat
        );
        assert_eq!(classify_subscription("projects/coralswarm-prod/subscriptions/something-else"), PushSurface::Unknown);
        assert_eq!(classify_subscription(""), PushSurface::Unknown);
    }

    /// Meet has its own arm and must never be decoded as a Chat event.
    #[test]
    fn classify_subscription_full_matrix_including_meet_which_is_not_a_chat_event() {
        for (subscription, expected) in [
            ("projects/p/subscriptions/gmail-push-sub", PushSurface::Gmail),
            ("projects/p/subscriptions/chat-events-sub", PushSurface::Chat),
            ("projects/p/subscriptions/chat-app-events-sub", PushSurface::ChatAppEvents),
            ("projects/p/subscriptions/meet-events-sub", PushSurface::Meet),
            ("projects/p/subscriptions/calendar-events-sub", PushSurface::Unknown),
            ("", PushSurface::Unknown),
        ] {
            assert_eq!(
                classify_subscription(subscription),
                expected,
                "classification drifted for {subscription:?}"
            );
        }
    }

    // ── Chat Workspace Events CloudEvent decode (gsuite Train G4 PR 4) ──────

    #[test]
    fn bare_space_name_reduces_every_documented_resource_form_to_spaces_slash_id() {
        // `ce-subject` carries the `//{service}/` form; `ce-id` carries a
        // SpaceEvent resource name; a body may carry a message name. All
        // three must reduce to the SAME `spaces/{id}` the watched-space
        // table keys on.
        assert_eq!(
            bare_space_name("//chat.googleapis.com/spaces/AAA").as_deref(),
            Some("spaces/AAA")
        );
        assert_eq!(
            bare_space_name("spaces/AAA/spaceEvents/ev-1").as_deref(),
            Some("spaces/AAA")
        );
        assert_eq!(
            bare_space_name("spaces/AAA/messages/m-1").as_deref(),
            Some("spaces/AAA")
        );
        assert_eq!(bare_space_name("spaces/AAA").as_deref(), Some("spaces/AAA"));
        // Malformed input never panics and never yields a poisoned key.
        for bad in ["", "garbage", "spaces/", "//chat.googleapis.com/", "users/1001"] {
            assert_eq!(bare_space_name(bad), None, "{bad:?} must not resolve to a space");
        }
    }

    #[test]
    fn decode_chat_cloud_event_reads_the_unverified_fixture_envelope() {
        const FIXTURE: &str =
            include_str!("../../tests/fixtures/google_chat/pubsub_push_chat_message_created.json");
        let envelope: PushEnvelope = serde_json::from_str(FIXTURE).unwrap();
        assert_eq!(classify_subscription(&envelope.subscription), PushSurface::Chat);

        let event = decode_chat_cloud_event(&envelope.message.attributes, &envelope.message.data).unwrap();
        assert_eq!(event.event_type, "google.workspace.chat.message.v1.created");
        assert_eq!(
            event.space_id, "spaces/AAAAAAAAAAA",
            "the routing key (D8) comes from ce-subject — the platform-documented, NOT-unverified \
             half of this envelope"
        );
        assert_eq!(
            event.message_name.as_deref(),
            Some("spaces/AAAAAAAAAAA/messages/msg-0002")
        );
        assert_eq!(event.membership_name, None);
    }

    #[test]
    fn decode_chat_cloud_event_routes_on_attributes_alone_when_the_body_shape_surprises_us() {
        // The `data` payload's internal shape is UNVERIFIED pending step 0's
        // live capture (see the fixture README). Preferring the `ce-*`
        // attributes means a body we cannot parse the way we expected still
        // routes correctly — it costs us only the optional `message_name`,
        // never the space.
        let attrs: HashMap<String, String> = [
            ("ce-type".to_string(), "google.workspace.chat.message.v1.created".to_string()),
            ("ce-subject".to_string(), "//chat.googleapis.com/spaces/ZZZ".to_string()),
        ]
        .into_iter()
        .collect();

        use base64::Engine as _;
        let alien = base64::engine::general_purpose::STANDARD
            .encode(serde_json::json!({"somethingWeHaveNeverSeen": {"nested": true}}).to_string());
        let event = decode_chat_cloud_event(&attrs, &alien).unwrap();
        assert_eq!(event.space_id, "spaces/ZZZ");
        assert_eq!(event.message_name, None, "an unknown body shape costs only the optional half");

        // An empty body is fine too.
        assert_eq!(decode_chat_cloud_event(&attrs, "").unwrap().space_id, "spaces/ZZZ");
    }

    #[test]
    fn decode_chat_cloud_event_fails_contained_on_a_body_it_cannot_route() {
        // A contained, testable failure — never a panic, and (because the
        // caller logs and returns) never a 5xx on a VERIFIED body, which
        // Pub/Sub would redeliver forever.
        let empty: HashMap<String, String> = HashMap::new();
        assert!(decode_chat_cloud_event(&empty, "not-base64!!!").is_err());
        assert!(
            decode_chat_cloud_event(&empty, "").is_err(),
            "no ce-type and no body means nothing to route on"
        );

        use base64::Engine as _;
        let typed_but_spaceless = base64::engine::general_purpose::STANDARD.encode(
            serde_json::json!({"eventType": "google.workspace.chat.message.v1.created"}).to_string(),
        );
        assert!(
            decode_chat_cloud_event(&empty, &typed_but_spaceless).is_err(),
            "D8 routes by SPACE — an event we cannot attribute to one must be dropped, never \
             guessed at"
        );
    }

    #[test]
    fn decode_chat_cloud_event_recovers_a_membership_resource_name() {
        // D5 means the payload carries no resource DATA, only resource
        // NAMES — and a membership name's last segment is the same Google
        // user id `User.name`'s `users/{id}` carries, which is what makes
        // `membership.v1.deleted` attributable at all.
        use base64::Engine as _;
        let attrs: HashMap<String, String> = [
            ("ce-type".to_string(), "google.workspace.chat.membership.v1.deleted".to_string()),
            ("ce-subject".to_string(), "//chat.googleapis.com/spaces/AAA".to_string()),
        ]
        .into_iter()
        .collect();
        let data = base64::engine::general_purpose::STANDARD.encode(
            serde_json::json!({
                "membershipDeletedEventData": {"membership": {"name": "spaces/AAA/members/1002"}}
            })
            .to_string(),
        );
        let event = decode_chat_cloud_event(&attrs, &data).unwrap();
        assert_eq!(event.space_id, "spaces/AAA");
        assert_eq!(event.membership_name.as_deref(), Some("spaces/AAA/members/1002"));
        assert!(event.event_type.contains(".membership.v1."));
        assert!(event.event_type.ends_with(".deleted"));
        // The sub the revoke path extracts.
        assert_eq!(
            event.membership_name.as_deref().and_then(|n| n.rsplit('/').next()),
            Some("1002")
        );
    }

    // ── Conformance tests 2 and 3, against the real Chat push arm ────────
    //
    // Review round 1, M5: these two were previously CLAIMED by
    // `connectors::google_chat` tests that proved something else (a
    // missing-DEK bail, and member/account scoping). Both are written here
    // now, against `run_chat_event` itself — the actual verified-push entry
    // point — because that is where §6's wording ("a verified push …") puts
    // them.

    fn chat_test_base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }

    fn chat_with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }

    async fn chat_recreate(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn.close().await.ok();
    }

    async fn chat_drop(base: &str, db: &str) {
        use sqlx::{Connection, Executor};
        let mut conn = sqlx::PgConnection::connect(base).await.unwrap();
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    fn message_created_event(space_id: &str) -> ChatCloudEvent {
        ChatCloudEvent {
            event_type: "google.workspace.chat.message.v1.created".into(),
            space_id: space_id.into(),
            message_name: Some(format!("{space_id}/messages/m1")),
            membership_name: None,
        }
    }

    // ── Gmail push: fan-out, flag gate, cursors — route-level, through the
    // `GmailPass` seam (no token, no network; everything else is real). ────

    async fn seed_owner_and_account(pool: &PgPool, clerk_org_id: Option<&str>) -> (Uuid, Uuid) {
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(clerk_org_id)
                .fetch_one(pool)
                .await
                .unwrap();
        (account_id, owner)
    }

    /// An org watch owner is a CURRENT member — the shape production always
    /// has. Since settings launch PR 2.8 the Gmail push refuses (and
    /// offboards) a watch owner with no `account_members` row, so a fixture
    /// that omits this is testing a departed member, not a live one.
    async fn seed_org_owner_membership(pool: &PgPool, account_id: Uuid, owner: Uuid) {
        let n = sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             SELECT $1, $2, 'owner', t.id FROM seat_tiers t WHERE t.code = 'standard' AND t.is_active",
        )
        .bind(account_id)
        .bind(owner)
        .execute(pool)
        .await
        .unwrap()
        .rows_affected();
        assert_eq!(n, 1, "seed the org owner's membership");
    }

    async fn seed_gmail_watch(pool: &PgPool, account_id: Uuid, owner: Uuid, email: &str, history_id: &str) {
        sqlx::query(
            "INSERT INTO google_watch_channels \
                (account_id, surface, resource_id, owner_user_id, topic_name, expires_at, sync_token, updated_at) \
             VALUES ($1, 'gmail', $2, $3, 'projects/p/topics/gmail-push', now() + interval '7 days', $4, now())",
        )
        .bind(account_id)
        .bind(email)
        .bind(owner)
        .bind(history_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn set_gmail_flag(pool: &PgPool, clerk_org_id: &str, enabled: bool) {
        sqlx::query("INSERT INTO org_connector_flags (clerk_org_id, provider, enabled) VALUES ($1, 'google_gmail', $2)")
            .bind(clerk_org_id)
            .bind(enabled)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn gmail_cursor(pool: &PgPool, account_id: Uuid, email: &str) -> String {
        sqlx::query_scalar(
            "SELECT sync_token FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A fake Gmail round trip: records which accounts it ran for and
    /// answers per account with a new history id (`Ok`) or a failure.
    fn recording_pass(
        seen: Arc<std::sync::Mutex<Vec<Uuid>>>,
        answer: impl Fn(Uuid) -> Result<String, ConnectorError> + Send + Sync + 'static,
    ) -> impl for<'a> Fn(GmailPassInput<'a>) -> BoxFuture<'a, Result<google_gmail::PendingSync, ConnectorError>> + Send + Sync
    {
        move |input: GmailPassInput<'_>| {
            seen.lock().unwrap().push(input.account_id);
            let result = answer(input.account_id).map(|history_id| google_gmail::PendingSync {
                items: Vec::new(),
                commit: google_gmail::PendingCommit::Complete(history_id),
            });
            let fut: BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> = Box::pin(async move { result });
            fut
        }
    }

    /// One mailbox watched under two accounts: the first account's round
    /// trip fails, the second's completes — the push still runs for both,
    /// the failing account's cursor is left alone, the other's advances, and
    /// the transient failure fails the push so its receive job retries; a
    /// second push advances each cursor to its own value.
    #[tokio::test]
    async fn gmail_push_one_failing_account_does_not_stop_the_other_and_each_cursor_advances_on_its_own_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping gmail_push_one_failing_account_does_not_stop_the_other_and_each_cursor_advances_on_its_own_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_gmail_push_fanout_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let email = "shared@example.com";
        let (personal, personal_owner) = seed_owner_and_account(&control, None).await;
        let (org, org_owner) = seed_owner_and_account(&control, Some("org_gmail_fanout")).await;
        seed_org_owner_membership(&control, org, org_owner).await;
        set_gmail_flag(&control, "org_gmail_fanout", true).await;
        seed_gmail_watch(&control, personal, personal_owner, email, "p-1000").await;
        seed_gmail_watch(&control, org, org_owner, email, "o-2000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = recording_pass(seen.clone(), move |account_id| {
            if account_id == personal {
                Err(ConnectorError::upstream_code(500, "gmail history.list failed: 500 Internal Server Error: "))
            } else {
                Ok("o-2001".to_string())
            }
        });
        assert!(
            matches!(run_gmail_sync_for_email_with(&state, email, &pass).await, Err(ConnectorError::UpstreamStatus { status: 500, .. })),
            "one account's transient failure retries the push"
        );
        let mut ran = seen.lock().unwrap().clone();
        ran.sort();
        let mut both = vec![personal, org];
        both.sort();
        assert_eq!(ran, both, "the pass must run for BOTH watching accounts, failure or not");
        assert_eq!(gmail_cursor(&control, personal, email).await, "p-1000", "a failed pass leaves its own cursor alone");
        assert_eq!(gmail_cursor(&control, org, email).await, "o-2001", "the other account's cursor advances regardless");

        let pass = recording_pass(Arc::new(std::sync::Mutex::new(Vec::new())), |account_id| Ok(format!("{account_id}-next")));
        run_gmail_sync_for_email_with(&state, email, &pass).await.unwrap();
        assert_eq!(gmail_cursor(&control, personal, email).await, format!("{personal}-next"));
        assert_eq!(gmail_cursor(&control, org, email).await, format!("{org}-next"));

        // One account answering a permanent 4xx does not fail the push: a
        // retry would only re-run the healthy account for the same answer.
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = recording_pass(seen.clone(), move |account_id| {
            if account_id == personal {
                Err(ConnectorError::upstream_code(403, "gmail history.list failed: 403 Forbidden: reasons=[insufficientPermissions]"))
            } else {
                Ok("o-3000".to_string())
            }
        });
        assert_eq!(run_gmail_sync_for_email_with(&state, email, &pass).await.unwrap(), 0, "a permanent failure completes the push");
        assert_eq!(seen.lock().unwrap().len(), 2, "both accounts still ran");
        assert_eq!(gmail_cursor(&control, personal, email).await, format!("{personal}-next"));
        assert_eq!(gmail_cursor(&control, org, email).await, "o-3000");

        let pass = recording_pass(Arc::new(std::sync::Mutex::new(Vec::new())), |_| {
            Err(ConnectorError::upstream_code(503, "gmail history.list failed: 503 Service Unavailable: "))
        });
        assert!(
            run_gmail_sync_for_email_with(&state, email, &pass).await.is_err(),
            "every account failing is the one case that fails the push"
        );
        assert!(
            matches!(
                run_gmail_sync_for_email_with(&state, "nobody@example.com", &pass).await,
                Err(ConnectorError::NotConfigured(_))
            ),
            "an unwatched mailbox is NotConfigured, never a pass"
        );

        control.close().await;
        chat_drop(&base, control_db).await;
    }

    /// An org whose Gmail flag is off: the pass never starts and the cursor
    /// does not move; an org whose flag is on runs and advances. Through the
    /// fan-out the two coexist on one mailbox.
    #[tokio::test]
    async fn gmail_push_org_flag_off_skips_the_pass_and_leaves_the_cursor_alone_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping gmail_push_org_flag_off_skips_the_pass_and_leaves_the_cursor_alone_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_gmail_push_flag_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let email = "member@example.com";
        let (off, off_owner) = seed_owner_and_account(&control, Some("org_gmail_off")).await;
        let (on, on_owner) = seed_owner_and_account(&control, Some("org_gmail_on")).await;
        seed_org_owner_membership(&control, off, off_owner).await;
        seed_org_owner_membership(&control, on, on_owner).await;
        set_gmail_flag(&control, "org_gmail_off", false).await;
        set_gmail_flag(&control, "org_gmail_on", true).await;
        seed_gmail_watch(&control, off, off_owner, email, "off-1").await;
        seed_gmail_watch(&control, on, on_owner, email, "on-1").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));

        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = recording_pass(seen.clone(), |account_id| Ok(format!("{account_id}-advanced")));

        let n = run_gmail_sync_for_mailbox(&state, &control, CalendarPassGate::shared(), off, off_owner, email, &pass).await.unwrap();
        assert_eq!(n, 0);
        assert!(seen.lock().unwrap().is_empty(), "flag off: the Gmail round trip must not even start");
        assert_eq!(gmail_cursor(&control, off, email).await, "off-1", "flag off: the cursor must not advance");

        let n = run_gmail_sync_for_mailbox(&state, &control, CalendarPassGate::shared(), on, on_owner, email, &pass).await.unwrap();
        assert_eq!(n, 0);
        assert_eq!(*seen.lock().unwrap(), vec![on]);
        assert_eq!(gmail_cursor(&control, on, email).await, format!("{on}-advanced"));

        seen.lock().unwrap().clear();
        run_gmail_sync_for_email_with(&state, email, &pass).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![on], "through the fan-out only the flag-on org runs");
        assert_eq!(gmail_cursor(&control, off, email).await, "off-1");

        control.close().await;
        chat_drop(&base, control_db).await;
    }

    /// A member who paused (or turned off) the Gmail source: the push runs no
    /// pass at all and the history cursor does not move; resuming lets the next
    /// push through, and it reads on from that cursor. Another member's pass over
    /// the same mailbox address is not touched by this member's row.
    #[tokio::test]
    async fn gmail_push_a_paused_source_runs_no_pass_and_leaves_the_cursor_alone_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping gmail_push_a_paused_source_runs_no_pass_and_leaves_the_cursor_alone_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_gmail_push_pause_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let email = "paused@example.com";
        let (account, owner) = seed_owner_and_account(&control, None).await;
        seed_gmail_watch(&control, account, owner, email, "g-1000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = recording_pass(seen.clone(), |account_id| Ok(format!("{account_id}-advanced")));
        let set = |paused: bool, enabled: bool| {
            let control = control.clone();
            async move {
                sqlx::query(
                    "INSERT INTO connector_sources (account_id, owner_user_id, provider, source, enabled, paused_at) \
                     VALUES ($1, $2, 'google', 'gmail', $4, CASE WHEN $3 THEN now() END) \
                     ON CONFLICT (account_id, owner_user_id, provider, source) DO UPDATE \
                         SET enabled = $4, paused_at = CASE WHEN $3 THEN now() END",
                )
                .bind(account)
                .bind(owner)
                .bind(paused)
                .bind(enabled)
                .execute(&control)
                .await
                .unwrap();
            }
        };

        set(true, true).await;
        let n = run_gmail_sync_for_mailbox(&state, &control, CalendarPassGate::shared(), account, owner, email, &pass).await.unwrap();
        assert_eq!(n, 0);
        assert!(seen.lock().unwrap().is_empty(), "paused: the Gmail round trip must not even start");
        assert_eq!(gmail_cursor(&control, account, email).await, "g-1000", "paused: the cursor must not advance");

        set(false, false).await;
        run_gmail_sync_for_mailbox(&state, &control, CalendarPassGate::shared(), account, owner, email, &pass).await.unwrap();
        assert!(seen.lock().unwrap().is_empty(), "off: no pass either");
        assert_eq!(gmail_cursor(&control, account, email).await, "g-1000");

        set(false, true).await;
        run_gmail_sync_for_mailbox(&state, &control, CalendarPassGate::shared(), account, owner, email, &pass).await.unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![account], "resumed: the pass runs");
        assert_eq!(gmail_cursor(&control, account, email).await, format!("{account}-advanced"));

        control.close().await;
        chat_drop(&base, control_db).await;
    }

    /// **Conformance test 2 — tenant-resolution refusal (I8).**
    ///
    /// §6: *"A verified push for a missing/suspended tenant writes nothing,
    /// on either plane."* For Chat the tenant-resolution step IS the
    /// space-keyed `resolve_chat_space_account` bootstrap (D8) — there is no
    /// subscriber-keyed resolver at all. A verified push whose space
    /// resolves to no account must therefore write nothing and, critically,
    /// must NOT be an error: Pub/Sub redelivers on any non-2xx, so an error
    /// here would be an infinite redelivery loop for every space a member
    /// belongs to that CoralSwarm was never invited into — which, because
    /// the subscription is on `spaces/-`, is most of them.
    #[tokio::test]
    async fn conformance_2_a_verified_push_for_an_unresolvable_tenant_writes_nothing_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping conformance_2_a_verified_push_for_an_unresolvable_tenant_writes_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_g44_conf2";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        chat_recreate(&base, db).await;
        let url = chat_with_db(&base, db);
        let pool = sqlx::postgres::PgPoolOptions::new().connect(&url).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // A verified, well-formed event for a space no account watches.
        let ingested = run_chat_event(&state, &message_created_event("spaces/no-such-space"))
            .await
            .expect("an unresolvable space must be DISCARDED, never an error — Pub/Sub redelivers on a non-2xx");
        assert_eq!(ingested, 0);

        // Nothing was written anywhere.
        for table in ["atoms", "google_chat_watched_spaces", "team_seat_grants", "team_members"] {
            let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&pool)
                .await
                .unwrap();
            assert_eq!(n, 0, "a push for an unresolvable tenant must not write {table}");
        }

        // A membership event for the same unresolvable space is equally
        // inert — it must not seat or revoke anybody.
        let membership = ChatCloudEvent {
            event_type: "google.workspace.chat.membership.v1.deleted".into(),
            space_id: "spaces/no-such-space".into(),
            message_name: None,
            membership_name: Some("spaces/no-such-space/members/1001".into()),
        };
        assert_eq!(run_chat_event(&state, &membership).await.unwrap(), 0);
        let grants: i64 = sqlx::query_scalar("SELECT count(*) FROM team_seat_grants")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(grants, 0);

        pool.close().await;
        chat_drop(&base, db).await;
    }

    /// **Conformance test 3 — no consumer-plane fallback.**
    ///
    /// §6: *"Multi-tenancy off + org-scoped payload ⇒ refuse, never write to
    /// the shared plane."* **Still true after CS-432 (D17 point 4), for a
    /// DIFFERENT reason than before.** `run_chat_event` used to be
    /// hardcoded `TenantId::Consumer` with `state.db` for both pools — CS-432
    /// removed that once an account IS found, resolving each account's real
    /// [`crate::crypto::DekPlane`] instead (see
    /// `run_chat_event_for_a_tenant_db_org_reads_the_watched_space_on_its_
    /// own_tenant_pool_pg` below for that half). What THIS test still pins:
    /// `resolve_chat_space_account` (migration `0198`) is a CONTROL-only
    /// `SECURITY DEFINER` reading `google_chat_watched_spaces` in the control
    /// database — account DISCOVERY from a bare `space_id`, unchanged by
    /// CS-432 (see that fix's own doc comment on `run_chat_event` for why:
    /// closing this would need a control-plane index the container table
    /// doesn't have). A row that exists ONLY on an org's tenant database,
    /// with no `accounts` row on control at all, is structurally invisible
    /// to that discovery step — there is no account to resolve a plane FOR
    /// — so the push is discarded before CS-432's fix ever engages. It can
    /// never fall back to writing the org's content onto the shared
    /// consumer plane either way.
    ///
    /// This is proved with TWO GENUINELY SEPARATE physical databases: the
    /// row exists only in the tenant one, and the push — which only ever
    /// sees the control one — writes nothing anywhere.
    #[tokio::test]
    async fn conformance_3_an_org_scoped_push_never_falls_back_to_the_shared_plane_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping conformance_3_an_org_scoped_push_never_falls_back_to_the_shared_plane_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let control_db = "gws_g44_conf3_control";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_g44_conf3_tenant";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        chat_recreate(&base, control_db).await;
        chat_recreate(&base, tenant_db).await;

        let control: PgPool = sqlx::postgres::PgPoolOptions::new()
            .connect(&chat_with_db(&base, control_db))
            .await
            .unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let tenant: PgPool = sqlx::postgres::PgPoolOptions::new()
            .connect(&chat_with_db(&base, tenant_db))
            .await
            .unwrap();
        sqlx::migrate!("../backend/migrations_tenant").run(&tenant).await.unwrap();

        // The org account's watched space exists ONLY on its own tenant
        // database — which is exactly where an org tenant's content lives.
        let org_account_id = Uuid::new_v4();
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Org Ocean', 'personal', true) RETURNING id",
        )
        .bind(org_account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, mode, graph_id, active) \
             VALUES ($1, 'spaces/ORG', 'SPACE', 'single', $2, true)",
        )
        .bind(org_account_id)
        .bind(graph_id)
        .execute(&tenant)
        .await
        .unwrap();

        // `state.db` is the CONSUMER/CONTROL pool — the only one this push
        // path has, by construction (D1).
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));
        let ingested = run_chat_event(&state, &message_created_event("spaces/ORG")).await.unwrap();
        assert_eq!(
            ingested, 0,
            "an org-scoped push must be refused/discarded on the consumer plane, never served \
             from it"
        );

        // Nothing on the shared plane…
        for table in ["atoms", "google_chat_watched_spaces"] {
            let n: i64 = sqlx::query_scalar(&format!("SELECT count(*) FROM {table}"))
                .fetch_one(&control)
                .await
                .unwrap();
            assert_eq!(n, 0, "the consumer plane must not have gained a {table} row");
        }
        // …and nothing written into the org's own tenant database either —
        // this path never reaches it at all, which is the point.
        let org_atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms")
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(org_atoms, 0);
        // The org's row is still there, untouched — proving the two
        // databases really are separate and the test is not vacuous.
        let org_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces")
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(org_rows, 1, "the org row must exist on the tenant plane, or this proves nothing");

        control.close().await;
        tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    // ── CS-432 (D17 point 4) — the org tenant pool ─────────────────────────

    /// Mint an org's `tenant_databases` routing row at `Active` — same shape
    /// as `routes::slack_events::tests::mint_tenant_databases_row`.
    async fn mint_tenant_databases_row(
        control_pool: &PgPool,
        org_id: &str,
        slug: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) {
        crate::billing::renewal::execute_financial_fixture(
            control_pool,
            account_id,
            sqlx::query(
                "INSERT INTO tenant_databases \
                    (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
                 VALUES ($1, $2, $3, $4, 'neon', 1, 'active')",
            )
            .bind(org_id)
            .bind(account_id)
            .bind(slug)
            .bind(secret_ref),
        )
        .await
        .unwrap();
    }

    /// Build (connect + migrate the TENANT tree + stamp `tenant_meta`) a
    /// second scratch database standing in for one org's real tenant DB —
    /// same shape as `routes::slack_events::tests::build_tenant_pool`.
    async fn chat_build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&chat_with_db(base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    /// **The CS-432 regression test.** An org account WITH its own tenant
    /// database must have its push-event space resolution (`run_chat_event`)
    /// read `google_chat_watched_spaces` on ITS OWN TENANT POOL, never on
    /// control — even when a ROUTED row for the exact same `(account_id,
    /// space_id)` sits on control (the pre-CS-432 shape: every write landed
    /// on `state.db` regardless of tenancy).
    ///
    /// **Why this proves the read plane, not just the outcome.** The
    /// control row is deliberately left ROUTED (`mode = 'single'`, a real
    /// `graph_id`) — the shape that, if `ingestable_space` ever read it,
    /// would be picked up and handed to `sync_space_once` (which starts
    /// with `mark_space_attempted`, an unconditional `UPDATE ...
    /// last_resolved_at = now()` on whichever pool it is given, BEFORE any
    /// network call). The tenant pool carries no row for this space at all.
    /// So: read tenant (the fix) → `ingestable_space` finds nothing →
    /// discarded, `Ok(0)`, and the control row's `last_resolved_at` is
    /// UNTOUCHED; read control (the pre-CS-432 bug) → finds the routed row →
    /// `sync_space_once` runs → `mark_space_attempted` bumps
    /// `last_resolved_at` on control before this test could even reach its
    /// assertions. Comparing the control row's `last_resolved_at`
    /// before/after is therefore a network-call-free proof of which pool was
    /// actually read — no wiremock needed for a push whose real content
    /// lives on a pool this test never has to fetch from.
    ///
    /// This also exercises the plane-mismatch warning
    /// (`connectors::google_chat::warn_if_chat_space_plane_mismatch`) for
    /// real: the control row is exactly the "left behind on the wrong
    /// plane" case D17 point 4 and CS-432 build item 3 describe, so the
    /// (account, space) cooldown slot must be CONSUMED by the run below.
    #[tokio::test]
    async fn run_chat_event_for_a_tenant_db_org_reads_the_watched_space_on_its_own_tenant_pool_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping run_chat_event_for_a_tenant_db_org_reads_the_watched_space_on_its_own_tenant_pool_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_cs432_run_event_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_cs432_run_event_tnt";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        chat_recreate(&base, control_db).await;
        chat_recreate(&base, tenant_db).await;

        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_cs432_{}", Uuid::new_v4());
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&org_id)
        .fetch_one(&control)
        .await
        .unwrap();

        // The LEFT-BEHIND control-plane row (D17 point 4's exact shape):
        // ROUTED, so it is exactly what a pre-CS-432 read would have acted
        // on.
        let control_graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Stale Control Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
        let control_last_resolved_before: chrono::DateTime<chrono::Utc> = sqlx::query_scalar(
            "INSERT INTO google_chat_watched_spaces \
                (account_id, space_id, space_type, mode, graph_id, active) \
             VALUES ($1, 'spaces/ORGTENANT', 'SPACE', 'single', $2, true) \
             RETURNING last_resolved_at",
        )
        .bind(account_id)
        .bind(control_graph_id)
        .fetch_one(&control)
        .await
        .unwrap();

        // The org's own tenant database — genuinely separate, no row for
        // this space at all (the "real" content plane has nothing yet).
        mint_tenant_databases_row(&control, &org_id, "cs432-org", account_id, "SECRET_CS432").await;
        let tenant = chat_build_tenant_pool(&base, tenant_db, &org_id).await;

        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([(
            "SECRET_CS432",
            chat_with_db(&base, tenant_db),
        )]);
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(std::sync::Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        sqlx::query("INSERT INTO org_connector_flags(clerk_org_id,provider,enabled) VALUES($1,'google_chat',true)").bind(&org_id).execute(&control).await.unwrap();
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), Some(runtime)));

        let ingested = run_chat_event(&state, &message_created_event("spaces/ORGTENANT"))
            .await
            .expect("a tenant-DB org's push event must not error");
        assert_eq!(
            ingested, 0,
            "the tenant pool has no row for this space — the push must discard, not fall back to \
             the control row it must never read"
        );

        // The decisive proof: the control row's `last_resolved_at` did NOT
        // move. If `run_chat_event` had read (and therefore acted on) the
        // control row, `sync_space_once` → `mark_space_attempted` would have
        // bumped it before any network call was even attempted.
        let control_last_resolved_after: chrono::DateTime<chrono::Utc> =
            sqlx::query_scalar("SELECT last_resolved_at FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/ORGTENANT'")
                .bind(account_id)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(
            control_last_resolved_before, control_last_resolved_after,
            "the control row must be untouched — run_chat_event must never read/write it for a \
             tenant-DB org"
        );

        // Nothing ingested anywhere.
        for (label, pool) in [("control", &control), ("tenant", &tenant)] {
            let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(pool).await.unwrap();
            assert_eq!(atoms, 0, "{label} must have gained no atoms");
        }
        // The tenant plane gained no watched-space row either — this test
        // must not be mistaken for `record_added_to_space` creating one.
        let tenant_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces")
            .fetch_one(&tenant)
            .await
            .unwrap();
        assert_eq!(tenant_rows, 0);

        // The plane-mismatch warning's cooldown slot for this exact
        // (account, space) must have been CONSUMED by the run above — proof
        // `warn_if_chat_space_plane_mismatch` detected the left-behind
        // control row and fired (see that function's own doc comment for
        // why this is the check available without a tracing subscriber).
        assert!(
            !google_chat::should_warn_chat_plane_mismatch(account_id, "spaces/ORGTENANT"),
            "the plane-mismatch warning must have fired exactly once for this (account, space)"
        );

        control.close().await;
        tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    #[tokio::test]
    async fn a_chat_event_with_no_consumer_pool_refuses_rather_than_half_acting() {
        // NAME AND ASSERTION MUST AGREE. This test was called
        // `a_chat_event_for_an_unwatched_space_is_discarded_never_an_error`,
        // but `test_app_state()` leaves `state.db` as `None`, so
        // `run_chat_event` returns on its very first line and the
        // unwatched-space discard is never reached. A future change that
        // broke the `Ok(0)` discard would have left this green while its
        // name pointed the reader at the behaviour it does not cover.
        //
        // What it DOES pin: with no pool there is nothing to resolve a
        // watched row against, so the handler refuses outright instead of
        // treating "cannot look it up" as "not watched" and silently
        // dropping a real event.
        //
        // §4.3's actual discard — a member's `spaces/-` subscription reaches
        // every space they belong to, wider than our consent set, and an
        // unwatched space must be a quiet `Ok(0)` because a non-2xx would
        // make Pub/Sub redeliver forever — is covered where it can be seeded:
        // `connectors::google_chat`'s pg suite.
        let state = crate::test_app_state();
        let event = ChatCloudEvent {
            event_type: "google.workspace.chat.message.v1.created".into(),
            space_id: "spaces/never-watched".into(),
            message_name: None,
            membership_name: None,
        };
        assert!(state.db.is_none(), "the refusal under test requires no pool");
        assert!(run_chat_event(&state, &event).await.is_err());
    }

    // ── Chat app interaction events (gsuite Train G4 PR 3) ──────────────────

    #[test]
    fn classify_subscription_routes_chat_app_events_distinctly_from_chat() {
        assert_eq!(
            classify_subscription("projects/coralswarm-prod/subscriptions/chat-app-events-sub"),
            PushSurface::ChatAppEvents,
            "must not be conflated with the plain chat-events-sub (Workspace Events messages) arm"
        );
        // The reverse never happens either — pinned so a future edit to
        // either literal can't silently make them collide.
        assert_ne!(PushSurface::ChatAppEvents, PushSurface::Chat);
    }

    #[test]
    fn decode_chat_interaction_event_decodes_added_to_space() {
        use base64::Engine as _;
        let raw = serde_json::json!({
            "type": "ADDED_TO_SPACE",
            "eventTime": "2026-08-20T12:00:00.000000Z",
            "space": {"name": "spaces/FFFFFFFFFFF", "spaceType": "SPACE", "displayName": "new-team-space", "singleUserBotDm": false},
            "user": {"name": "users/1001", "displayName": "Alice Ng", "type": "HUMAN"}
        });
        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&raw).unwrap());
        let parsed = decode_chat_interaction_event(&encoded).unwrap();
        assert_eq!(parsed.event_type, "ADDED_TO_SPACE");
        assert_eq!(parsed.space.name, "spaces/FFFFFFFFFFF");
        assert_eq!(parsed.space.space_type, "SPACE");
        assert_eq!(parsed.user.unwrap().name, "users/1001");
    }

    #[test]
    fn decode_chat_interaction_event_rejects_garbage() {
        assert!(decode_chat_interaction_event("not-valid-base64!!!").is_err());
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"not json");
        assert!(decode_chat_interaction_event(&encoded).is_err());
    }

    // ── Fixture-driven (backend/tests/fixtures/google_chat/
    // pubsub_push_chat_app_added_to_space.json — see that directory's
    // README for provenance) ────────────────────────────────────────────────

    const PUBSUB_PUSH_CHAT_APP_ADDED_TO_SPACE: &str =
        include_str!("../../tests/fixtures/google_chat/pubsub_push_chat_app_added_to_space.json");

    #[test]
    fn fixture_pubsub_push_chat_app_added_to_space_decodes_and_routes() {
        let envelope: PushEnvelope = serde_json::from_str(PUBSUB_PUSH_CHAT_APP_ADDED_TO_SPACE).unwrap();
        assert_eq!(classify_subscription(&envelope.subscription), PushSurface::ChatAppEvents);
        let event = decode_chat_interaction_event(&envelope.message.data).unwrap();
        assert_eq!(event.event_type, "ADDED_TO_SPACE");
        assert_eq!(event.space.space_type, "SPACE");
        assert!(event.user.is_some());
    }

    // ── Workspace add-on chat interaction envelope (the "Build this Chat
    // app as a Workspace add-on" checkbox — locked ON for CoralSwarm's own
    // Chat app; see `decode_chat_interaction_event`'s doc comment and
    // `backend/tests/fixtures/google_chat_addon/README.md`) ────────────────

    #[test]
    fn decode_chat_interaction_event_decodes_addon_added_to_space() {
        use base64::Engine as _;
        let raw = serde_json::json!({
            "commonEventObject": {"hostApp": "CHAT", "platform": "WEB"},
            "chat": {
                "user": {"name": "users/1001", "displayName": "Alice Ng", "type": "HUMAN"},
                "eventTime": "2026-09-19T04:16:30.852421Z",
                "addedToSpacePayload": {
                    "space": {
                        "name": "spaces/AAAEXAMPLE01",
                        "type": "ROOM",
                        "spaceType": "SPACE",
                        "displayName": "Example Room",
                        "spaceHistoryState": "HISTORY_ON"
                    },
                    "configCompleteRedirectUri": "https://chat.google.com/api/bot_config_complete?token=..."
                }
            }
        });
        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&raw).unwrap());
        let parsed = decode_chat_interaction_event(&encoded).unwrap();
        assert_eq!(parsed.event_type, "ADDED_TO_SPACE");
        assert_eq!(parsed.space.name, "spaces/AAAEXAMPLE01");
        // The add-on shape's `type: "ROOM"` must never be confused with the
        // `spaceType` field this decoder actually reads.
        assert_eq!(parsed.space.space_type, "SPACE");
        assert_eq!(parsed.user.unwrap().name, "users/1001");
    }

    #[test]
    fn decode_chat_interaction_event_decodes_addon_removed_from_space() {
        use base64::Engine as _;
        let raw = serde_json::json!({
            "commonEventObject": {"hostApp": "CHAT", "platform": "WEB"},
            "chat": {
                "user": {"name": "users/1001", "displayName": "Alice Ng", "type": "HUMAN"},
                "eventTime": "2026-09-19T04:22:47.198013Z",
                "removedFromSpacePayload": {
                    "space": {
                        "name": "spaces/AAAEXAMPLE01",
                        "type": "ROOM",
                        "spaceType": "SPACE",
                        "displayName": "Example Room"
                    }
                    // No `configCompleteRedirectUri` — REMOVED_FROM_SPACE
                    // never carries one. The decoder must not require it.
                }
            }
        });
        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&raw).unwrap());
        let parsed = decode_chat_interaction_event(&encoded).unwrap();
        assert_eq!(parsed.event_type, "REMOVED_FROM_SPACE");
        assert_eq!(parsed.space.name, "spaces/AAAEXAMPLE01");
    }

    #[test]
    fn decode_chat_interaction_event_rejects_addon_with_neither_payload_key() {
        // Valid JSON, valid `chat` object, but an add-on interaction kind
        // this connector does not (yet) handle — e.g. a future
        // `messagePayload`. Must be a clean `Err`, never a guessed event.
        use base64::Engine as _;
        let raw = serde_json::json!({
            "commonEventObject": {"hostApp": "CHAT", "platform": "WEB"},
            "chat": {
                "user": {"name": "users/1001"},
                "eventTime": "2026-09-19T04:16:30.852421Z"
            }
        });
        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&raw).unwrap());
        assert!(decode_chat_interaction_event(&encoded).is_err());
    }

    const PUBSUB_PUSH_CHAT_ADDON_ADDED_TO_SPACE: &str =
        include_str!("../../tests/fixtures/google_chat_addon/pubsub_push_chat_addon_added_to_space.json");
    const PUBSUB_PUSH_CHAT_ADDON_REMOVED_FROM_SPACE: &str =
        include_str!("../../tests/fixtures/google_chat_addon/pubsub_push_chat_addon_removed_from_space.json");

    #[test]
    fn fixture_pubsub_push_chat_addon_added_to_space_decodes_and_routes() {
        let envelope: PushEnvelope = serde_json::from_str(PUBSUB_PUSH_CHAT_ADDON_ADDED_TO_SPACE).unwrap();
        assert_eq!(classify_subscription(&envelope.subscription), PushSurface::ChatAppEvents);
        let event = decode_chat_interaction_event(&envelope.message.data).unwrap();
        assert_eq!(event.event_type, "ADDED_TO_SPACE");
        assert_eq!(event.space.name, "spaces/AAAEXAMPLE01");
        assert_eq!(event.space.space_type, "SPACE");
        assert_eq!(event.user.unwrap().name, "users/100000000000000000001");
    }

    #[test]
    fn fixture_pubsub_push_chat_addon_removed_from_space_decodes_and_routes() {
        let envelope: PushEnvelope = serde_json::from_str(PUBSUB_PUSH_CHAT_ADDON_REMOVED_FROM_SPACE).unwrap();
        assert_eq!(classify_subscription(&envelope.subscription), PushSurface::ChatAppEvents);
        let event = decode_chat_interaction_event(&envelope.message.data).unwrap();
        assert_eq!(event.event_type, "REMOVED_FROM_SPACE");
        assert_eq!(event.space.name, "spaces/AAAEXAMPLE01");
    }

    // ── Gmail push data decode (gsuite Train G2 PR 3) ───────────────────────

    #[test]
    fn decode_gmail_push_data_decodes_standard_base64_json() {
        use base64::Engine as _;
        let raw = serde_json::json!({"emailAddress": "a@b.com", "historyId": 4123});
        let encoded = base64::engine::general_purpose::STANDARD.encode(serde_json::to_vec(&raw).unwrap());
        let parsed = decode_gmail_push_data(&encoded).unwrap();
        assert_eq!(parsed.email_address, "a@b.com");
    }

    #[test]
    fn decode_gmail_push_data_rejects_garbage() {
        assert!(decode_gmail_push_data("not-valid-base64!!!").is_err());
        // Valid base64, but not JSON.
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(b"not json");
        assert!(decode_gmail_push_data(&encoded).is_err());
    }

    // ── Fixture-driven (backend/tests/fixtures/gmail/pubsub_push_gmail.json —
    // see that directory's README for provenance) ──────────────────────────

    const PUBSUB_PUSH_GMAIL: &str = include_str!("../../tests/fixtures/gmail/pubsub_push_gmail.json");

    #[test]
    fn fixture_pubsub_push_envelope_decodes_and_routes_to_gmail() {
        let envelope: PushEnvelope = serde_json::from_str(PUBSUB_PUSH_GMAIL).unwrap();
        assert_eq!(classify_subscription(&envelope.subscription), PushSurface::Gmail);
        let parsed = decode_gmail_push_data(&envelope.message.data).unwrap();
        assert_eq!(parsed.email_address, "sam.rivera@acme-corp.example");
    }

    // ── ADDED_TO_SPACE / REMOVED_FROM_SPACE dispatch (gsuite Train G4 PR 3,
    // pg-gated) ───────────────────────────────────────────────────────────
    //
    //   ORGT_TEST_BASE_URL=postgres://postgres:postgres@localhost:15544/postgres \
    //     cargo test -p coralswarm-backend --lib routes::google_push::tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};

    fn pg_base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn pg_with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn pg_recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }

    /// Proves the DISPATCH wiring, not the underlying primitives (those are
    /// pg-tested directly in `connectors::google_chat`'s own test module):
    /// an unresolvable adding-identity creates NOTHING; a resolvable
    /// identity with no `google_chat` connection DEFERS (no row, no
    /// panic — this module's own doc comment names that boundary
    /// explicitly); `REMOVED_FROM_SPACE` for a space with no watched row is
    /// a clean no-op, never an error.
    #[tokio::test]
    async fn added_to_space_and_removed_from_space_dispatch_defer_cleanly_without_a_connected_chat_token_pg() {
        let Some(base) = pg_base_url() else {
            eprintln!(
                "skipping added_to_space_and_removed_from_space_dispatch_defer_cleanly_without_a_connected_chat_token_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g43_push_dispatch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = pg_recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&pg_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // Unresolvable identity: no CoralSwarm account has ever connected
        // this sub to anything — no row, no panic.
        let unresolvable_event = ChatInteractionEvent {
            event_type: "ADDED_TO_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/UNRESOLVABLE".into(),
                space_type: "SPACE".into(),
                display_name: "unresolvable".into(),
            },
            user: Some(ChatInteractionUser { name: "users/no-such-sub".into() }),
        };
        handle_chat_added_to_space(&state, unresolvable_event).await.expect("nothing to act on is not an error");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/UNRESOLVABLE'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0);

        // Resolvable identity (connected via google_gmail, per §3.3's
        // `provider LIKE 'google%'` — identification, not credential-exact)
        // but no `google_chat` connection: defers, no row.
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'defer@test.local') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, owner_user_id, provider_user_id, access_token, refresh_token) \
             VALUES ($1, 'google_gmail', 'test-scope', $2, 'sub-defer', 'enc', 'enc')",
        )
        .bind(account_id)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();
        let deferred_event = ChatInteractionEvent {
            event_type: "ADDED_TO_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/DEFERRED".into(),
                space_type: "SPACE".into(),
                display_name: "deferred".into(),
            },
            user: Some(ChatInteractionUser { name: "users/sub-defer".into() }),
        };
        handle_chat_added_to_space(&state, deferred_event).await.expect("deferring is not an error");
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE space_id = 'spaces/DEFERRED'")
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(count, 0, "no google_chat connection yet — must defer, not guess spaceType/historyState");

        // REMOVED_FROM_SPACE for a space with no watched row: a clean no-op.
        let removed_event = ChatInteractionEvent {
            event_type: "REMOVED_FROM_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/DEFERRED".into(),
                space_type: "SPACE".into(),
                display_name: "".into(),
            },
            user: Some(ChatInteractionUser { name: "users/sub-defer".into() }),
        };
        handle_chat_removed_from_space(&state, removed_event).await.expect("REMOVED_FROM_SPACE must settle cleanly");

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The H1 regression test, at the exact dispatch layer the bug lived
    /// in. Seeds a watched-space row exactly the way a CONNECTED member's
    /// `ADDED_TO_SPACE` would (`connectors::google_chat::
    /// record_added_to_space`, called directly to avoid a live Chat API
    /// dependency in this test), then dispatches a real
    /// `REMOVED_FROM_SPACE` interaction event whose `user` resolves to NO
    /// CoralSwarm identity at all — the overwhelmingly common real-world
    /// case (most people who can remove a Workspace-installed Chat app
    /// from a space have never connected CoralSwarm). Asserts the row IS
    /// deactivated anyway — before the H1 fix, this event would have been
    /// logged and dropped with the row left `active = true` forever.
    #[tokio::test]
    async fn removed_from_space_deactivates_the_row_even_when_the_remover_is_a_completely_unconnected_third_party_pg() {
        let Some(base) = pg_base_url() else {
            eprintln!(
                "skipping removed_from_space_deactivates_the_row_even_when_the_remover_is_a_completely_unconnected_third_party_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_g43_h1_dispatch";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let conn = pg_recreate(&base, db).await;
        conn.close().await.ok();
        let pool: PgPool = PgPoolOptions::new().connect(&pg_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));

        // Seed the account that originally added + routed the space —
        // exactly as `handle_chat_added_to_space` would have left it.
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'adder@test.local') RETURNING id")
            .fetch_one(&pool)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
            .bind(owner)
            .fetch_one(&pool)
            .await
            .unwrap();
        let seed_space = crate::connectors::chat_body::ChatSpace {
            name: "spaces/H1-DISPATCH".to_string(),
            space_type: "SPACE".to_string(),
            display_name: "h1-dispatch-test".to_string(),
            space_threading_state: "THREADED_MESSAGES".to_string(),
            history_state: "HISTORY_ON".to_string(),
            single_user_bot_dm: false,
            membership_count: None,
        };
        google_chat::record_added_to_space(&pool, &pool, AccountPlane::Personal, account_id, Some(owner), &seed_space)
            .await
            .unwrap();
        let active_before: bool = sqlx::query_scalar(
            "SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/H1-DISPATCH'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(active_before, "sanity: the seeded row starts active");

        // REMOVED_FROM_SPACE from an entirely UNRESOLVABLE identity — no
        // `oauth_tokens` row anywhere names this sub. The pre-H1-fix
        // handler would have resolved no account here and dropped the
        // event; the fixed handler resolves the account BY SPACE instead
        // and never even looks at this sub for that purpose (it's only
        // used for an informational log line now).
        let removed_event = ChatInteractionEvent {
            event_type: "REMOVED_FROM_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/H1-DISPATCH".into(),
                space_type: "SPACE".into(),
                display_name: "".into(),
            },
            user: Some(ChatInteractionUser { name: "users/completely-unconnected-third-party".into() }),
        };
        handle_chat_removed_from_space(&state, removed_event).await.expect("REMOVED_FROM_SPACE must settle cleanly");

        let active_after: bool = sqlx::query_scalar(
            "SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/H1-DISPATCH'",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(
            !active_after,
            "H1: REMOVED_FROM_SPACE from an unconnected third party MUST still deactivate the row \
             — routing is by SPACE, never by the remover's own identity"
        );

        pool.close().await;
        let mut admin_conn = PgConnection::connect(&base).await.unwrap();
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// **CodeRabbit round 1 (CS-432).** `handle_chat_removed_from_space` had
    /// the same defect shape `handle_chat_added_to_space` did before this
    /// PR: it deactivated on `pool` (`state.db`) unconditionally, so a
    /// tenant-DB org's row — now correctly created on its own tenant pool
    /// by this PR's `ADDED_TO_SPACE` fix — would never actually be
    /// deactivated. A stale control-plane leftover for the SAME account and
    /// space (seeded here to satisfy `resolve_chat_space_accounts`'
    /// control-only discovery — see that function's own doc comment for why
    /// this is the reachable case the fix closes, and `run_chat_event`'s
    /// doc comment for the still-open discovery-only gap this does NOT
    /// close) is asserted UNTOUCHED, proving the deactivation genuinely
    /// landed on tenant and not control.
    #[tokio::test]
    async fn removed_from_space_deactivates_the_tenant_row_for_an_org_account_not_a_stale_control_leftover_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping removed_from_space_deactivates_the_tenant_row_for_an_org_account_not_a_stale_control_leftover_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_cs432_removed_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_cs432_removed_tnt";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        chat_recreate(&base, control_db).await;
        chat_recreate(&base, tenant_db).await;

        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_cs432_removed_{}", Uuid::new_v4());
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&org_id)
        .fetch_one(&control)
        .await
        .unwrap();

        // The STALE control-plane leftover (D17 point 4's shape) — active,
        // so discovery finds this account, and so a bug that deactivates on
        // control instead of tenant would look like it "worked".
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, active) \
             VALUES ($1, 'spaces/ORGREMOVED', 'SPACE', true)",
        )
        .bind(account_id)
        .execute(&control)
        .await
        .unwrap();

        mint_tenant_databases_row(&control, &org_id, "cs432-removed-org", account_id, "SECRET_CS432_REMOVED").await;
        let tenant = chat_build_tenant_pool(&base, tenant_db, &org_id).await;

        // The REAL, active row on the org's own tenant pool.
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, active) \
             VALUES ($1, 'spaces/ORGREMOVED', 'SPACE', true)",
        )
        .bind(account_id)
        .execute(&tenant)
        .await
        .unwrap();

        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([(
            "SECRET_CS432_REMOVED",
            chat_with_db(&base, tenant_db),
        )]);
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(std::sync::Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), Some(runtime)));

        let removed_event = ChatInteractionEvent {
            event_type: "REMOVED_FROM_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/ORGREMOVED".into(),
                space_type: "SPACE".into(),
                display_name: "".into(),
            },
            user: None,
        };
        handle_chat_removed_from_space(&state, removed_event).await.expect("REMOVED_FROM_SPACE must settle cleanly");

        let tenant_active: bool = sqlx::query_scalar(
            "SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/ORGREMOVED'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert!(!tenant_active, "the org's own tenant-plane row must be deactivated");

        let control_active: bool = sqlx::query_scalar(
            "SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/ORGREMOVED'",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
        assert!(
            control_active,
            "the stale control-plane leftover must be left exactly as it was — deactivation must \
             never touch control for a tenant-DB org"
        );

        control.close().await;
        tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }
    #[tokio::test]
    async fn new_tenant_only_chat_space_is_discovered_without_actor_or_control_copy_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping new_tenant_only_chat_space_is_discovered_without_actor_or_control_copy_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_newchat_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        let tenant_db = "gws_newchat_tnt";
        let _cleanup_tenant_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", tenant_db);
        chat_recreate(&base, control_db).await;
        chat_recreate(&base, tenant_db).await;

        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_cs432_removed_{}", Uuid::new_v4());
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control)
            .await
            .unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(&org_id)
        .fetch_one(&control)
        .await
        .unwrap();

        mint_tenant_databases_row(&control, &org_id, "cs432-removed-org", account_id, "SECRET_CS432_REMOVED").await;
        let tenant = chat_build_tenant_pool(&base, tenant_db, &org_id).await;

        // The REAL, active row on the org's own tenant pool.
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, active) \
             VALUES ($1, 'spaces/ORGREMOVED', 'SPACE', true)",
        )
        .bind(account_id)
        .execute(&tenant)
        .await
        .unwrap();

        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([(
            "SECRET_CS432_REMOVED",
            chat_with_db(&base, tenant_db),
        )]);
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(std::sync::Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), Some(runtime)));

        let (targets, mailboxes) = chat_targets(&state,&control,"spaces/ORGREMOVED").await.unwrap();
        assert_eq!(targets,vec![account_id]);assert!(mailboxes.is_empty());
        // Mailboxes are discovered from tenant rows and unioned with every
        // personal owner watching the same physical group chat.
        let tenant_graph:Uuid=sqlx::query_scalar("INSERT INTO graphs(account_id,name,kind,is_primary,owner_user_id) VALUES($1,'private','member',true,$2) RETURNING id").bind(account_id).bind(owner).fetch_one(&tenant).await.unwrap();
        sqlx::query("INSERT INTO google_chat_watched_spaces(account_id,space_id,space_type,active,mode,graph_id,owner_user_id) VALUES($1,'spaces/TENANTDM','GROUP_CHAT',true,'single',$2,$3)").bind(account_id).bind(tenant_graph).bind(owner).execute(&tenant).await.unwrap();
        let mut expected=std::collections::HashSet::from([(account_id,owner)]);
        for _ in 0..2 {
            let (personal_account,personal_owner)=seed_gmail_account(&control).await;
            let graph=seed_gmail_graph(&control,personal_account,"private",true).await;
            sqlx::query("INSERT INTO google_chat_watched_spaces(account_id,space_id,space_type,active,mode,graph_id,owner_user_id) VALUES($1,'spaces/TENANTDM','GROUP_CHAT',true,'single',$2,$3)").bind(personal_account).bind(graph).bind(personal_owner).execute(&control).await.unwrap();
            expected.insert((personal_account,personal_owner));
        }
        let (containers,owners)=chat_targets(&state,&control,"spaces/TENANTDM").await.unwrap();
        assert!(containers.is_empty());assert_eq!(owners.into_iter().collect::<std::collections::HashSet<_>>(),expected);
        let removed_event = ChatInteractionEvent {
            event_type: "REMOVED_FROM_SPACE".into(),
            space: ChatInteractionSpace {
                name: "spaces/ORGREMOVED".into(),
                space_type: "SPACE".into(),
                display_name: "".into(),
            },
            user: None,
        };
        handle_chat_removed_from_space(&state, removed_event).await.expect("REMOVED_FROM_SPACE must settle cleanly");

        let tenant_active: bool = sqlx::query_scalar(
            "SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/ORGREMOVED'",
        )
        .bind(account_id)
        .fetch_one(&tenant)
        .await
        .unwrap();
        assert!(!tenant_active, "the org's own tenant-plane row must be deactivated");

        let control_count: i64 = sqlx::query_scalar("SELECT count(*) FROM google_chat_watched_spaces WHERE account_id=$1")
            .bind(account_id).fetch_one(&control).await.unwrap();
        assert_eq!(control_count,0,"no leftover control row was needed for discovery");

        control.close().await;
        tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    // ── Gmail restores through `ingest_and_commit` ──────────────────────────

    fn set_test_kek() {
        // SAFETY: every test sets the same constant, so concurrent writes are benign.
        unsafe { std::env::set_var("MASTER_KEK_HEX", "cc".repeat(32)) };
    }

    /// A personal account with its content DEK provisioned, so a restore can
    /// really write through `ingest_atom` against `pool`.
    async fn seed_gmail_account(pool: &PgPool) -> (Uuid, Uuid) {
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        set_test_kek();
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(pool)
            .await
            .unwrap();
        let wrapped = wrap_dek(&load_kek().unwrap(), &generate_dek()).unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, dek_wrapped) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(&wrapped)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(owner)
            .execute(pool)
            .await
            .unwrap();
        (account_id, owner)
    }

    async fn seed_gmail_graph(pool: &PgPool, account_id: Uuid, name: &str, is_primary: bool) -> Uuid {
        sqlx::query_scalar("INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, $2, 'personal', $3) RETURNING id")
            .bind(account_id)
            .bind(name)
            .bind(is_primary)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// What a hide leaves behind for `message_id` in `graph_id`.
    async fn seed_gmail_tombstone(pool: &PgPool, account_id: Uuid, graph_id: Uuid, owner: Uuid, message_id: &str) {
        sqlx::query(
            "INSERT INTO gmail_hidden_messages (account_id, graph_id, owner_user_id, thread_id, message_id) \
             VALUES ($1, $2, $3, 'thread-x', $4)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(owner)
        .bind(message_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn gmail_tombstones(pool: &PgPool, account_id: Uuid) -> Vec<(Uuid, String)> {
        sqlx::query_as("SELECT graph_id, message_id FROM gmail_hidden_messages WHERE account_id = $1 ORDER BY message_id")
            .bind(account_id)
            .fetch_all(pool)
            .await
            .unwrap()
    }

    async fn gmail_atom_count(pool: &PgPool, graph_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1 AND source_type = 'gmail'")
            .bind(graph_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    fn gmail_pending_item(message_id: &str, restore_graph_id: Option<Uuid>) -> google_gmail::PendingItem {
        google_gmail::PendingItem {
            item: crate::connectors::NormalizedItem {
                source_type: google_gmail::SOURCE_TYPE.into(),
                source_id: message_id.into(),
                content: format!("Subject: {message_id}\n\nhello again"),
                participants: vec![],
                timestamp: chrono::Utc::now(),
                scope: None,
                thread_id: None,
                channel_id: None,
                author_id: None,
                author_name: None,
            },
            restore_graph_id,
        }
    }

    /// Review item 1 (second round): a restore whose tombstoned ocean its
    /// owner can no longer write to must not wedge the mailbox. `ingest_atom`
    /// answers 403, and before this fix that counted as a blocking failure —
    /// the cursor never advanced and every later hide, delete and live-follow
    /// arrival for the mailbox was lost behind this one message. The
    /// personal plane has no archived-ocean refusal of its own (a deleted
    /// ocean cascades its tombstones), so an ocean of another account stands
    /// in for the org plane's archived member ocean; the refusal is the same
    /// `ReachabilityResolver` 403.
    #[tokio::test]
    async fn a_restore_into_an_ocean_its_owner_cannot_write_drops_the_tombstone_and_still_advances_the_cursor_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping a_restore_into_an_ocean_its_owner_cannot_write_drops_the_tombstone_and_still_advances_the_cursor_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_restore_unwritable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        chat_recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();

        let email = "restore@example.com";
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let primary = seed_gmail_graph(&pool, account_id, "Primary", true).await;
        let (other_account, _) = seed_gmail_account(&pool).await;
        let unreachable = seed_gmail_graph(&pool, other_account, "Someone else's", true).await;
        seed_gmail_tombstone(&pool, account_id, unreachable, owner, "msg-gone").await;
        seed_gmail_watch(&pool, account_id, owner, email, "1000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let pending = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-gone", Some(unreachable))],
            commit: google_gmail::PendingCommit::Complete("1001".into()),
        };
        let ingested = ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, primary, owner, email, pending).await.unwrap();
        assert_eq!(ingested, 0, "nothing can be put back into an ocean the owner cannot write");
        assert_eq!(gmail_atom_count(&pool, unreachable).await, 0);
        assert_eq!(gmail_cursor(&pool, account_id, email).await, "1001", "one unrestorable message must not park the cursor");
        assert!(gmail_tombstones(&pool, account_id).await.is_empty(), "the tombstone has nothing left to restore into");

        pool.close().await;
        chat_drop(&base, db).await;
    }

    /// Review item 4 (second round): the tombstone is cleared only once the
    /// cursor commits. Cleared per item, a restore that succeeded next to a
    /// failing item would leave the replay with no tombstone, and the
    /// message would take the live-follow route into the primary ocean — a
    /// duplicate when the tombstoned ocean is a different one. The replay's
    /// re-ingest into the tombstoned ocean is deduped by `ON CONFLICT`.
    #[tokio::test]
    async fn a_restores_tombstone_outlives_a_failed_batch_and_clears_only_once_the_cursor_commits_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping a_restores_tombstone_outlives_a_failed_batch_and_clears_only_once_the_cursor_commits_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_gmail_restore_post_commit";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        chat_recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();

        let email = "restore@example.com";
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let primary = seed_gmail_graph(&pool, account_id, "Primary", true).await;
        let saved_into = seed_gmail_graph(&pool, account_id, "Project ocean", false).await;
        seed_gmail_tombstone(&pool, account_id, saved_into, owner, "msg-r").await;
        seed_gmail_watch(&pool, account_id, owner, email, "2000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        // The live-follow arrival is pointed at a graph that does not exist
        // (`ingest_atom` 403 — blocking); the restore next to it succeeds.
        let missing_primary = Uuid::new_v4();
        let pending = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-r", Some(saved_into)), gmail_pending_item("msg-new", None)],
            commit: google_gmail::PendingCommit::Complete("2001".into()),
        };
        let ingested = ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, missing_primary, owner, email, pending).await.unwrap();
        assert_eq!(ingested, 1, "the restore itself landed");
        let restored = gmail_atom_count(&pool, saved_into).await;
        assert!(restored > 0, "the restored message is back in the ocean it was hidden from");
        assert_eq!(gmail_cursor(&pool, account_id, email).await, "2000", "a failed item holds the cursor");
        assert_eq!(
            gmail_tombstones(&pool, account_id).await,
            vec![(saved_into, "msg-r".to_string())],
            "the tombstone must outlive the failed batch so the replay restores into the same ocean again"
        );

        let pending = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-r", Some(saved_into))],
            commit: google_gmail::PendingCommit::Complete("2001".into()),
        };
        ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, primary, owner, email, pending).await.unwrap();
        assert_eq!(gmail_atom_count(&pool, saved_into).await, restored, "the replayed restore is deduped");
        assert_eq!(gmail_atom_count(&pool, primary).await, 0, "and never routed into the primary");
        assert_eq!(gmail_cursor(&pool, account_id, email).await, "2001");
        assert!(gmail_tombstones(&pool, account_id).await.is_empty(), "cleared once the cursor committed");

        pool.close().await;
        chat_drop(&base, db).await;
    }

    /// A live-follow reply of `thread_id` — what `fetch_pending` hands back
    /// for a new message in a followed thread (`restore_graph_id: None`).
    fn gmail_reply_item(thread_id: &str, message_id: &str) -> google_gmail::PendingItem {
        let mut p = gmail_pending_item(message_id, None);
        p.item.thread_id = Some(thread_id.into());
        p
    }

    async fn seed_followed_thread(pool: &PgPool, account_id: Uuid, graph_id: Uuid, owner: Uuid, thread_id: &str) {
        sqlx::query(
            "INSERT INTO gmail_watched_threads (account_id, graph_id, ingested_by, thread_id, follow_replies) \
             VALUES ($1, $2, $3, $4, TRUE)",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(owner)
        .bind(thread_id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn make_primary(pool: &PgPool, account_id: Uuid, graph_id: Uuid) {
        sqlx::query("UPDATE graphs SET is_primary = (id = $2) WHERE account_id = $1 AND archived_at IS NULL")
            .bind(account_id)
            .bind(graph_id)
            .execute(pool)
            .await
            .unwrap();
    }

    // Drive the arrival through ingest so a primary switch cannot hide a
    // missing destination record behind fixture setup.
    #[tokio::test]
    async fn trashing_a_reply_that_landed_in_a_former_primary_retracts_it_from_that_ocean_pg() {
        former_primary_hide_pg(false).await;
    }

    #[tokio::test]
    async fn trashing_a_legacy_reply_without_a_landing_record_retracts_it_from_its_former_primary_pg() {
        former_primary_hide_pg(true).await;
    }

    async fn former_primary_hide_pg(legacy: bool) {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping trashing_a_reply_that_landed_in_a_former_primary_retracts_it_from_that_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = if legacy { "gws_gmail_legacy_primary_hide" } else { "gws_gmail_former_primary_hide" };
        chat_recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();

        let email = "former@example.com";
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let ocean_a = seed_gmail_graph(&pool, account_id, "Ocean A", true).await;
        let saved_into = seed_gmail_graph(&pool, account_id, "Project ocean", false).await;
        let ocean_b = seed_gmail_graph(&pool, account_id, "Ocean B", false).await;
        let promoted = seed_gmail_graph(&pool, account_id, "Promoted copy", false).await;
        seed_followed_thread(&pool, account_id, saved_into, owner, "thread-f").await;
        seed_gmail_watch(&pool, account_id, owner, email, "3000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));

        let pending = google_gmail::PendingSync {
            items: vec![gmail_reply_item("thread-f", "msg-reply")],
            commit: google_gmail::PendingCommit::Complete("3001".into()),
        };
        let n = ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, ocean_a, owner, email, pending).await.unwrap();
        assert_eq!(n, 1);
        assert!(gmail_atom_count(&pool, ocean_a).await > 0, "the reply landed in the then-primary A");

        let copy = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-reply", None)],
            commit: google_gmail::PendingCommit::Complete("3002".into()),
        };
        ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, promoted, owner, email, copy).await.unwrap();
        sqlx::query("UPDATE atoms SET promoted_from_atom_id = (SELECT atom_id FROM atoms WHERE graph_id = $1 LIMIT 1) WHERE graph_id = $2")
            .bind(ocean_a).bind(promoted).execute(&pool).await.unwrap();
        assert!(gmail_atom_count(&pool, promoted).await > 0);

        let landings: Vec<Uuid> = sqlx::query_scalar("SELECT landing_graph_ids FROM gmail_watched_threads WHERE account_id = $1")
            .bind(account_id).fetch_one(&pool).await.unwrap();
        assert_eq!(landings, vec![ocean_a], "the arrival records its destination before the switch");
        make_primary(&pool, account_id, ocean_b).await;
        if !legacy {
            sqlx::raw_sql(include_str!("../../migrations_control/0284_gmail_watched_threads_landing_graphs.sql"))
                .execute(&pool).await.unwrap();
            let preserved: Vec<Uuid> = sqlx::query_scalar("SELECT landing_graph_ids FROM gmail_watched_threads WHERE account_id = $1")
                .bind(account_id).fetch_one(&pool).await.unwrap();
            assert_eq!(preserved, vec![ocean_a], "migration replay preserves a recorded former primary");
        }
        if legacy {
            sqlx::query("UPDATE gmail_watched_threads SET landing_graph_ids = '{}' WHERE account_id = $1")
                .bind(account_id).execute(&pool).await.unwrap();
        }
        google_gmail::propagate_hides(&pool, account_id, owner, &[("thread-f".to_string(), "msg-reply".to_string())])
            .await
            .unwrap();

        assert_eq!(gmail_atom_count(&pool, ocean_a).await, 0, "the trashed reply must leave the former primary A");
        assert_eq!(
            gmail_tombstones(&pool, account_id).await,
            vec![(ocean_a, "msg-reply".to_string())],
            "the untrash restores it into A, where it was"
        );

        assert!(gmail_atom_count(&pool, promoted).await > 0, "a copy outside connector landing oceans survives");
        google_gmail::propagate_hides(&pool, account_id, owner, &[("thread-f".into(), "msg-reply".into())]).await.unwrap();
        assert_eq!(gmail_tombstones(&pool, account_id).await.len(), 1, "a replay adds no tombstone");
        let restored = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-reply", Some(ocean_a))],
            commit: google_gmail::PendingCommit::Complete("3003".into()),
        };
        ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, ocean_b, owner, email, restored).await.unwrap();
        assert!(gmail_atom_count(&pool, ocean_a).await > 0, "untrash restores the former primary");
        assert_eq!(gmail_atom_count(&pool, ocean_b).await, 0, "untrash does not move the reply into the new primary");
        assert!(gmail_tombstones(&pool, account_id).await.is_empty());

        pool.close().await;
        chat_drop(&base, db).await;
    }

    #[tokio::test]
    async fn reply_landing_record_failure_keeps_atoms_and_history_cursor_unchanged_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping reply_landing_record_failure_keeps_atoms_and_history_cursor_unchanged_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_landing_failure";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        chat_recreate(&base, db).await;
        let pool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let primary = seed_gmail_graph(&pool, account_id, "Primary", true).await;
        let saved_into = seed_gmail_graph(&pool, account_id, "Saved", false).await;
        seed_followed_thread(&pool, account_id, saved_into, owner, "thread-fail").await;
        seed_gmail_watch(&pool, account_id, owner, "failed@example.com", "4000").await;
        sqlx::query("ALTER TABLE gmail_watched_threads DROP COLUMN landing_graph_ids").execute(&pool).await.unwrap();
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        let pending = google_gmail::PendingSync {
            items: vec![gmail_reply_item("thread-fail", "msg-fail")],
            commit: google_gmail::PendingCommit::Complete("4001".into()),
        };
        let result = ingest_and_commit(&state, &pool, Tenant::legacy(&state), account_id, primary, owner, "failed@example.com", pending).await;
        assert!(result.is_err());
        assert_eq!(gmail_atom_count(&pool, primary).await, 0, "no unrecorded reply is written");
        let cursor: String = sqlx::query_scalar("SELECT sync_token FROM google_watch_channels WHERE account_id = $1 AND surface = 'gmail'")
            .bind(account_id).fetch_one(&pool).await.unwrap();
        assert_eq!(cursor, "4000", "failed destination persistence leaves the history range retryable");
        pool.close().await;
        chat_drop(&base, db).await;
    }

    // ── Org plane: real plane resolution with a tenant DB ───────────────────

    struct OrgGmailFixture {
        control: PgPool,
        tenant: PgPool,
        state: Arc<AppState>,
        org_id: String,
        account_id: Uuid,
        alice: Uuid,
        /// alice's primary member ocean on the tenant plane.
        alice_graph: Uuid,
        /// a member ocean of alice's that has since been archived.
        archived_graph: Uuid,
    }

    /// An org with its own tenant database: the account (with `clerk_org_id`
    /// and its DEK) on control, the DEK again in the tenant's `account_keys`,
    /// the Gmail org flag on, a `tenant_databases` routing row resolved by a
    /// static secret map, and alice's member oceans on the tenant tree.
    async fn seed_org_gmail_fixture(base: &str, control_db: &str, tenant_db: &str, org_id: &str) -> OrgGmailFixture {
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        set_test_kek();
        chat_recreate(base, control_db).await;
        chat_recreate(base, tenant_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let tenant = chat_build_tenant_pool(base, tenant_db, org_id).await;

        let alice: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("alice-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control)
            .await
            .unwrap();
        let wrapped = wrap_dek(&load_kek().unwrap(), &generate_dek()).unwrap();
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id, dek_wrapped) VALUES ($1, $2, $3) RETURNING id",
        )
        .bind(alice)
        .bind(org_id)
        .bind(&wrapped)
        .fetch_one(&control)
        .await
        .unwrap();
        // Org capture checks current membership on CONTROL, not the content pool.
        let membership = sqlx::query(
            "INSERT INTO account_members(account_id,user_id,role,seat_tier_id) \
             SELECT a.id,a.owner_user_id,'owner',t.id FROM accounts a CROSS JOIN seat_tiers t \
             WHERE a.id=$1 AND a.owner_user_id=$2 AND a.clerk_org_id=$3 AND a.is_org_account \
               AND t.code='standard' AND t.is_active",
        )
        .bind(account_id)
        .bind(alice)
        .bind(org_id)
        .execute(&control)
        .await
        .expect("seed the Gmail org owner's CONTROL membership with an explicit tier");
        assert_eq!(membership.rows_affected(), 1, "exactly the existing org owner must be admitted");
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(&wrapped)
            .execute(&tenant)
            .await
            .unwrap();
        set_gmail_flag(&control, org_id, true).await;
        let secret = format!("SECRET_{}", org_id.to_uppercase());
        mint_tenant_databases_row(&control, org_id, &org_id.replace('_', "-"), account_id, &secret).await;

        let mut graphs = Vec::new();
        for (name, is_primary, archived) in [("alice's ocean", true, false), ("alice's old ocean", false, true)] {
            let g: Uuid = sqlx::query_scalar(
                "INSERT INTO graphs(account_id, name, kind, is_primary, owner_user_id, archived_at) \
                 VALUES ($1, $2, 'member', $3, $4, CASE WHEN $5 THEN now() END) RETURNING id",
            )
            .bind(account_id)
            .bind(name)
            .bind(is_primary)
            .bind(alice)
            .bind(archived)
            .fetch_one(&tenant)
            .await
            .unwrap();
            graphs.push(g);
        }

        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([(secret.as_str(), chat_with_db(base, tenant_db))]);
        let registry = Arc::new(crate::tenancy::registry::TenantRegistry::new(Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(control.clone(), Some(runtime)));
        OrgGmailFixture {
            control,
            tenant,
            state,
            org_id: org_id.to_string(),
            account_id,
            alice,
            alice_graph: graphs[0],
            archived_graph: graphs[1],
        }
    }

    /// A fake Gmail round trip for the org tests: records the plane and the
    /// database the content pool it was handed points at, then answers with
    /// a restore into `restore_graph` plus a live-follow arrival.
    fn plane_recording_pass(
        seen: Arc<std::sync::Mutex<Vec<(AccountPlane, String)>>>,
        restore_graph: Uuid,
    ) -> impl for<'a> Fn(GmailPassInput<'a>) -> BoxFuture<'a, Result<google_gmail::PendingSync, ConnectorError>> + Send + Sync
    {
        move |input: GmailPassInput<'_>| {
            let seen = seen.clone();
            let fut: BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> = Box::pin(async move {
                let db: String = sqlx::query_scalar("SELECT current_database()")
                    .fetch_one(input.content_pool)
                    .await
                    .unwrap();
                seen.lock().unwrap().push((input.plane, db));
                Ok(google_gmail::PendingSync {
                    items: vec![gmail_pending_item("msg-r", Some(restore_graph)), gmail_pending_item("msg-new", None)],
                    commit: google_gmail::PendingCommit::Complete("o-2".into()),
                })
            });
            fut
        }
    }

    /// Review item 5 (second round), first half: an org account whose org
    /// has a tenant database goes through the REAL plane resolution. The
    /// fake pass records the plane and the database its content pool points
    /// at, and hands back a restore plus a live-follow arrival; both land in
    /// alice's member ocean on the tenant plane, the tombstone is cleared
    /// there, and the cursor advances on control. Nothing touches the
    /// consumer plane's `atoms`.
    #[tokio::test]
    async fn gmail_push_for_a_tenant_db_org_resolves_the_org_plane_and_restores_into_the_members_own_ocean_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping gmail_push_for_a_tenant_db_org_resolves_the_org_plane_and_restores_into_the_members_own_ocean_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_push_org_plane_ctrl", "gws_gmail_push_org_plane_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_push_plane").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;
        seed_gmail_tombstone(&f.tenant, f.account_id, f.alice_graph, f.alice, "msg-r").await;

        let seen: Arc<std::sync::Mutex<Vec<(AccountPlane, String)>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = plane_recording_pass(seen.clone(), f.alice_graph);
        let n = run_gmail_sync_for_mailbox(&f.state, &f.control, CalendarPassGate::shared(), f.account_id, f.alice, email, &pass)
            .await
            .expect("an org member's pass must run on the org plane");
        assert_eq!(n, 2);
        assert_eq!(
            *seen.lock().unwrap(),
            vec![(AccountPlane::Org, tenant_db.to_string())],
            "the pass must run as Org against the org's own tenant database"
        );
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-2", "the cursor lives on control");
        let in_ocean: Vec<String> = sqlx::query_scalar(
            "SELECT DISTINCT split_part(source_id, ':chunk:', 1) FROM atoms WHERE graph_id = $1 AND source_type = 'gmail' ORDER BY 1",
        )
        .bind(f.alice_graph)
        .fetch_all(&f.tenant)
        .await
        .unwrap();
        assert_eq!(in_ocean, vec!["msg-new".to_string(), "msg-r".to_string()], "both land in alice's member ocean");
        assert!(gmail_tombstones(&f.tenant, f.account_id).await.is_empty(), "the tombstone is cleared on the tenant plane");
        let on_control: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms").fetch_one(&f.control).await.unwrap();
        assert_eq!(on_control, 0, "nothing of an org member's mail touches the consumer plane");

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    /// Settings launch PR 2.8: a Gmail push for a member who has LEFT the org
    /// — the shape a replayed or late Pub/Sub delivery takes after
    /// offboarding — writes nothing. The pass is never run (so their mailbox
    /// is not read and their token not refreshed), no atom lands in their
    /// still-unarchived member ocean, the cursor does not move, and the gate
    /// offboards their capture: the watch row is gone, so the NEXT push for
    /// that mailbox resolves no target at all.
    #[tokio::test]
    async fn a_replayed_gmail_push_for_a_departed_member_writes_nothing_and_stops_the_watch_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping a_replayed_gmail_push_for_a_departed_member_writes_nothing_and_stops_the_watch_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_push_departed_ctrl", "gws_gmail_push_departed_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_push_departed").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, owner_user_id, provider, scope, access_token, provider_user_id, provider_workspace_id) \
             VALUES ($1, $2, 'google_gmail', 'test', '\\x00'::bytea, 'sub-alice', $3)",
        )
        .bind(f.account_id)
        .bind(f.alice)
        .bind(email)
        .execute(&f.control)
        .await
        .unwrap();
        // alice leaves; the reconciler has NOT archived her ocean yet (the
        // window this gate exists for).
        sqlx::query("DELETE FROM account_members WHERE account_id = $1 AND user_id = $2")
            .bind(f.account_id)
            .bind(f.alice)
            .execute(&f.control)
            .await
            .unwrap();

        let seen: Arc<std::sync::Mutex<Vec<(AccountPlane, String)>>> = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = plane_recording_pass(seen.clone(), f.alice_graph);
        let n = run_gmail_sync_for_mailbox(&f.state, &f.control, CalendarPassGate::shared(), f.account_id, f.alice, email, &pass)
            .await
            .expect("a refused push is not an error");
        assert_eq!(n, 0);
        assert!(seen.lock().unwrap().is_empty(), "the departed member's mailbox must never be read");
        let atoms: i64 = sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(f.alice_graph)
            .fetch_one(&f.tenant)
            .await
            .unwrap();
        assert_eq!(atoms, 0, "nothing lands in the departed member's ocean");
        let tokens: i64 = sqlx::query_scalar("SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND owner_user_id = $2")
            .bind(f.account_id)
            .bind(f.alice)
            .fetch_one(&f.control)
            .await
            .unwrap();
        assert_eq!(tokens, 0, "the refusal offboarded the departed member's org token");
        let watches: i64 = sqlx::query_scalar("SELECT count(*) FROM google_watch_channels WHERE account_id = $1")
            .bind(f.account_id)
            .fetch_one(&f.control)
            .await
            .unwrap();
        assert_eq!(watches, 0, "and stopped the watch");
        let err = run_gmail_sync_for_email_with(&f.state, email, &pass).await.unwrap_err();
        assert!(matches!(err, ConnectorError::NotConfigured(_)), "the next push finds no watching account: {err:?}");
        assert!(seen.lock().unwrap().is_empty());

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    /// The per-org pass bound on the Gmail push (`calendar_pass_gate`): an
    /// org member's push whose org slot is taken (here by another pass,
    /// e.g. a calendar sync) QUEUES for it instead of skipping — there is no
    /// Gmail sweep to catch a skipped push up, only the mailbox's next
    /// change — and pins no connection while it waits. A second push for
    /// the same mailbox while the first is still queued is coalesced into
    /// it (the queued pass diffs from the stored cursor after both arrived),
    /// so the backlog stays one pass per mailbox. Once the slot frees the
    /// queued pass runs exactly once and advances the cursor, and the next
    /// push queues afresh.
    #[tokio::test]
    async fn gmail_push_waits_for_its_orgs_pass_slot_and_coalesces_a_queued_duplicate_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping gmail_push_waits_for_its_orgs_pass_slot_and_coalesces_a_queued_duplicate_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_push_gate_ctrl", "gws_gmail_push_gate_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_push_gate").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;

        let gate = Arc::new(CalendarPassGate::new(4));
        let request_pool = crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
            &f.state, &f.control, &f.org_id,
        ).await.expect("routed tenant request pool");
        let held = fill_org_bound(&gate, &f.org_id, &request_pool).await;
        let org_bound = held.len();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = Arc::new(recording_pass(seen.clone(), |_| Ok("o-2".to_string())));

        let queued = tokio::spawn({
            let (state, control, gate, pass) = (f.state.clone(), f.control.clone(), gate.clone(), pass.clone());
            let (account_id, alice) = (f.account_id, f.alice);
            async move { run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, alice, email, &*pass).await }
        });
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!queued.is_finished(), "a full org bound queues the push, never skips it");
        assert!(seen.lock().unwrap().is_empty(), "no Gmail round trip before the slot is free");
        assert_eq!(gate.in_flight(), org_bound, "the queued push holds no process-wide slot");
        let advisory: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks WHERE locktype = 'advisory' \
             AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
        )
        .fetch_one(&f.control)
        .await
        .unwrap();
        assert_eq!(advisory, 0, "the queued push takes its mailbox lock only after its permit");

        let dup = tokio::time::timeout(
            Duration::from_secs(2),
            run_gmail_sync_for_mailbox(&f.state, &f.control, &gate, f.account_id, f.alice, email, &*pass),
        )
        .await
        .expect("a duplicate of a queued push returns at once")
        .unwrap();
        assert_eq!(dup, 0);
        assert!(seen.lock().unwrap().is_empty(), "the duplicate is folded into the queued pass");
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-1");

        drop(held);
        let n = tokio::time::timeout(Duration::from_secs(10), queued)
            .await
            .expect("the queued push runs once the slot frees")
            .unwrap()
            .unwrap();
        assert_eq!(n, 0);
        assert_eq!(*seen.lock().unwrap(), vec![f.account_id], "exactly one pass for the two pushes");
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-2");
        assert_eq!(gate.in_flight(), 0, "the permit is released after the pass");

        run_gmail_sync_for_mailbox(&f.state, &f.control, &gate, f.account_id, f.alice, email, &*pass)
            .await
            .unwrap();
        assert_eq!(seen.lock().unwrap().len(), 2, "with nothing queued the next push runs its own pass");

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    /// Fill `org_id`'s per-org bound on `gate` (whatever size the
    /// fixture's tenant pool gives it), as other passes would.
    async fn fill_org_bound(
        gate: &crate::calendar_pass_gate::CalendarPassGate,
        org_id: &str,
        tenant: &PgPool,
    ) -> Vec<crate::calendar_pass_gate::PassPermit> {
        let mut held = Vec::new();
        while let Some(p) = gate.acquire_scoped(Some(crate::calendar_pass_gate::OrgSlot::google(org_id, tenant)), Duration::ZERO).await {
            held.push(p);
        }
        assert!(!held.is_empty() && held.len() < gate.size(), "the org bound, not the shared one, is what is full");
        held
    }

    /// Fix round 1, S1: the org flag is checked again once the pass holds
    /// its mailbox lock. A push queued behind the org's bound while an admin
    /// turns Gmail off must not run when its slot frees — no Gmail call, no
    /// cursor move — however long ago its early check passed.
    #[tokio::test]
    async fn a_gmail_push_queued_across_an_org_flag_flip_to_off_never_runs_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_gmail_push_queued_across_an_org_flag_flip_to_off_never_runs_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_push_flagflip_ctrl", "gws_gmail_push_flagflip_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_push_flagflip").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;

        let gate = Arc::new(CalendarPassGate::new(4));
        let request_pool = crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
            &f.state, &f.control, &f.org_id,
        ).await.expect("routed tenant request pool");
        let held = fill_org_bound(&gate, &f.org_id, &request_pool).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = Arc::new(recording_pass(seen.clone(), |_| Ok("o-2".to_string())));
        let queued = tokio::spawn({
            let (state, control, gate, pass) = (f.state.clone(), f.control.clone(), gate.clone(), pass.clone());
            let (account_id, alice) = (f.account_id, f.alice);
            async move { run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, alice, email, &*pass).await }
        });
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!queued.is_finished(), "queued behind the org bound, past its early flag check");

        // The admin turns Gmail off (the toggle route invalidates the cache).
        sqlx::query("UPDATE org_connector_flags SET enabled = false WHERE clerk_org_id = $1 AND provider = 'google_gmail'")
            .bind(&f.org_id)
            .execute(&f.control)
            .await
            .unwrap();
        f.state.connector_flag_cache.invalidate(&f.org_id, "google_gmail");

        drop(held);
        let n = tokio::time::timeout(Duration::from_secs(10), queued).await.expect("the queued push finishes").unwrap().unwrap();
        assert_eq!(n, 0);
        assert!(seen.lock().unwrap().is_empty(), "flag off by the time the slot freed: no Gmail round trip");
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-1", "and the cursor does not move");
        assert_eq!(gate.in_flight(), 0);

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    /// A fake Gmail round trip whose FIRST call parks until `release` gets a
    /// permit (a pass in flight); every call answers a fresh history id,
    /// `p-1001`, `p-1002`, … in call order.
    fn parking_first_pass(
        calls: Arc<std::sync::atomic::AtomicUsize>,
        release: Arc<tokio::sync::Semaphore>,
    ) -> impl for<'a> Fn(GmailPassInput<'a>) -> BoxFuture<'a, Result<google_gmail::PendingSync, ConnectorError>> + Send + Sync
    {
        move |_input: GmailPassInput<'_>| {
            let n = calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1;
            let release = release.clone();
            let fut: BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> = Box::pin(async move {
                if n == 1 {
                    release.acquire().await.unwrap().forget();
                }
                Ok(google_gmail::PendingSync {
                    items: Vec::new(),
                    commit: google_gmail::PendingCommit::Complete(format!("p-{}", 1000 + n)),
                })
            });
            fut
        }
    }

    /// Fix round 1, S2: pushes that land while a pass for the mailbox is
    /// RUNNING (here with free gate slots, so before the fix each got a
    /// permit, missed the mailbox lock and skipped) must not be lost: they
    /// mark the mailbox dirty and the running pass diffs once more when it
    /// ends, from the cursor it just committed. Several such pushes cost one
    /// re-run, and afterwards a push runs its own pass as usual.
    #[tokio::test]
    async fn gmail_pushes_during_a_running_pass_rerun_it_once_it_ends_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping gmail_pushes_during_a_running_pass_rerun_it_once_it_ends_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let control_db = "gws_gmail_push_rerun_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let email = "solo@example.com";
        let (account_id, owner) = seed_owner_and_account(&control, None).await;
        seed_gmail_watch(&control, account_id, owner, email, "p-1000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));

        let gate = Arc::new(CalendarPassGate::new(4));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let pass = Arc::new(parking_first_pass(calls.clone(), release.clone()));
        let t0 = tokio::spawn({
            let (state, control, gate, pass) = (state.clone(), control.clone(), gate.clone(), pass.clone());
            async move { run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &*pass).await }
        });
        for _ in 0..200 {
            if calls.load(SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(calls.load(SeqCst), 1, "T0 is mid-pass, holding the mailbox lock");

        for _ in 0..3 {
            let n = tokio::time::timeout(
                Duration::from_secs(2),
                run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &*pass),
            )
            .await
            .expect("a push during a running pass returns at once")
            .unwrap();
            assert_eq!(n, 0);
        }
        assert_eq!(calls.load(SeqCst), 1, "none of them ran a pass of its own while T0 holds the mailbox");

        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), t0).await.expect("T0 finishes").unwrap().unwrap();
        assert_eq!(calls.load(SeqCst), 2, "T0 diffs once more for the pushes that landed during it");
        assert_eq!(gmail_cursor(&control, account_id, email).await, "p-1002", "and commits that re-run's cursor");
        assert_eq!(gate.in_flight(), 0);

        run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &*pass).await.unwrap();
        assert_eq!(calls.load(SeqCst), 3, "with nothing in flight the next push runs its own pass");

        control.close().await;
        chat_drop(&base, control_db).await;
    }

    async fn other_machine_pass_once(
        state: &Arc<AppState>,
        control: &PgPool,
        account_id: Uuid,
        owner: Uuid,
        email: &str,
        pass: &GmailPass,
    ) -> GmailPassOnce {
        let dek_plane = crate::crypto::DekPlane::resolve_for_org(state, control, account_id, None).await.unwrap();
        let content_pool = dek_plane.content_pool().clone();
        gmail_pass_once(state, control, &content_pool, &dek_plane, None, account_id, owner, email, pass).await
    }

    async fn gmail_push_pending(pool: &PgPool, account_id: Uuid, email: &str) -> bool {
        sqlx::query_scalar(
            "SELECT push_pending_at IS NOT NULL FROM google_watch_channels \
             WHERE account_id = $1 AND surface = 'gmail' AND resource_id = $2",
        )
        .bind(account_id)
        .bind(email)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Gap 1 (cross-machine lock miss): a pass is running on machine A,
    /// holding the mailbox lock and past its cursor read, when a push lands
    /// on machine B. B misses the lock and returns without a pass. Before
    /// the fix that push waited for the mailbox's next change. Now B leaves
    /// a durable mark on the watch row, and A, once it has released the
    /// lock, sees the mark and diffs once more from its own new cursor.
    #[tokio::test]
    async fn a_push_missing_the_lock_on_another_machine_reruns_the_holders_pass_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        use std::sync::atomic::{AtomicUsize, Ordering::SeqCst};
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_push_missing_the_lock_on_another_machine_reruns_the_holders_pass_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let control_db = "gws_gmail_push_xmachine_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let email = "xmachine@example.com";
        let (account_id, owner) = seed_owner_and_account(&control, None).await;
        seed_gmail_watch(&control, account_id, owner, email, "p-1000").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));

        let gate = Arc::new(CalendarPassGate::new(4));
        let calls = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let pass = Arc::new(parking_first_pass(calls.clone(), release.clone()));
        let machine_a = tokio::spawn({
            let (state, control, gate, pass) = (state.clone(), control.clone(), gate.clone(), pass.clone());
            async move { run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &*pass).await }
        });
        for _ in 0..200 {
            if calls.load(SeqCst) == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(calls.load(SeqCst), 1, "machine A is mid-pass, holding the mailbox lock");

        let b = tokio::time::timeout(
            Duration::from_secs(2),
            other_machine_pass_once(&state, &control, account_id, owner, email, &*pass),
        )
        .await
        .expect("machine B does not wait for the lock");
        assert_eq!(b.result.unwrap(), 0);
        assert!(!b.rerun, "B ran nothing, so it owes no re-run of its own");
        assert_eq!(calls.load(SeqCst), 1, "B ran no pass while A holds the mailbox");
        assert!(gmail_push_pending(&control, account_id, email).await, "B left a durable mark on the watch row");

        release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(10), machine_a).await.expect("A finishes").unwrap().unwrap();
        assert_eq!(calls.load(SeqCst), 2, "A diffs once more for the push B could not run");
        assert_eq!(gmail_cursor(&control, account_id, email).await, "p-1002", "and commits that re-run's cursor");
        assert!(!gmail_push_pending(&control, account_id, email).await, "the re-run consumed the mark");
        assert_eq!(gate.in_flight(), 0);

        control.close().await;
        chat_drop(&base, control_db).await;
    }

    /// Gap 1, the holder seen only as a lock: another connection holds the
    /// mailbox lock (any machine's pass, any version). The push returns at
    /// once without a pass and leaves the durable mark; once the lock is
    /// free the next pass consumes the mark and runs exactly once (it does
    /// not loop on its own mark).
    #[tokio::test]
    async fn a_push_missing_a_lock_held_elsewhere_leaves_a_mark_the_next_pass_consumes_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping a_push_missing_a_lock_held_elsewhere_leaves_a_mark_the_next_pass_consumes_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let control_db = "gws_gmail_push_xmark_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let email = "xmark@example.com";
        let (account_id, owner) = seed_owner_and_account(&control, None).await;
        seed_gmail_watch(&control, account_id, owner, email, "p-1").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));
        assert!(!gmail_push_pending(&control, account_id, email).await);

        let mut other = PgConnection::connect(&chat_with_db(&base, control_db)).await.unwrap();
        other.execute("BEGIN").await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gmail_sync:' || $1::text || ':' || $2))")
            .bind(account_id.to_string())
            .bind(email)
            .execute(&mut other)
            .await
            .unwrap();

        let gate = CalendarPassGate::new(4);
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = recording_pass(seen.clone(), |_| Ok("p-2".to_string()));
        let n = tokio::time::timeout(
            Duration::from_secs(2),
            run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &pass),
        )
        .await
        .expect("a lock miss returns at once")
        .unwrap();
        assert_eq!(n, 0);
        assert!(seen.lock().unwrap().is_empty(), "no pass while the lock is held elsewhere");
        assert!(gmail_push_pending(&control, account_id, email).await, "the miss is recorded on the watch row");
        assert_eq!(gmail_cursor(&control, account_id, email).await, "p-1");

        other.execute("ROLLBACK").await.unwrap();
        run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, owner, email, &pass).await.unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1, "the next pass runs once and does not re-run on the mark it consumed");
        assert!(!gmail_push_pending(&control, account_id, email).await);
        assert_eq!(gmail_cursor(&control, account_id, email).await, "p-2");

        other.close().await.ok();
        control.close().await;
        chat_drop(&base, control_db).await;
    }

    #[tokio::test]
    async fn a_holder_releasing_before_the_pending_mark_commits_is_covered_by_the_second_lock_try_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping second_lock_try_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let control_db = "gws_gmail_push_retry_ctrl";
        let _cleanup_control_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", control_db);
        chat_recreate(&base, control_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, control_db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();
        let email = "retry@example.com";
        let (account_id, owner) = seed_owner_and_account(&control, None).await;
        seed_gmail_watch(&control, account_id, owner, email, "p-1").await;
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), None));
        let mut other = PgConnection::connect(&chat_with_db(&base, control_db)).await.unwrap();
        other.execute("BEGIN").await.unwrap();
        let holder_pid: i32 = sqlx::query_scalar("SELECT pg_backend_pid()").fetch_one(&mut other).await.unwrap();
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('gmail_sync:' || $1::text || ':' || $2))")
            .bind(account_id.to_string()).bind(email).execute(&mut other).await.unwrap();
        sqlx::query("SELECT id FROM google_watch_channels WHERE account_id = $1 FOR UPDATE")
            .bind(account_id).execute(&mut other).await.unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let push = tokio::spawn({
            let (state, control, seen) = (state.clone(), control.clone(), seen.clone());
            async move {
                let pass = recording_pass(seen, |_| Ok("p-2".to_string()));
                run_gmail_sync_for_mailbox(&state, &control, &CalendarPassGate::new(4), account_id, owner, email, &pass).await
            }
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let blocked: bool = sqlx::query_scalar(
                    "SELECT EXISTS (SELECT 1 FROM pg_stat_activity WHERE $1 = ANY(pg_blocking_pids(pid)) \
                     AND query LIKE 'UPDATE google_watch_channels SET push_pending_at%')",
                ).bind(holder_pid).fetch_one(&control).await.unwrap();
                if blocked { break; }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }).await.expect("the push missed the advisory lock and is waiting to commit its mark");
        other.execute("ROLLBACK").await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), push).await.unwrap().unwrap().unwrap();
        assert_eq!(seen.lock().unwrap().len(), 1, "the second attempt handles the push without another notification");
        assert_eq!(gmail_cursor(&control, account_id, email).await, "p-2");
        assert!(!gmail_push_pending(&control, account_id, email).await);
        other.close().await.ok();
        control.close().await;
        chat_drop(&base, control_db).await;
    }

    /// Fix round 1, N5: a queued push whose task is cancelled (aborted here;
    /// a stopping machine drops it the same way) leaves no marker behind —
    /// the next push for the mailbox queues and runs normally instead of
    /// folding into a pass that no longer exists.
    #[tokio::test]
    async fn a_cancelled_queued_gmail_push_leaves_no_marker_behind_pg() {
        use crate::calendar_pass_gate::CalendarPassGate;
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_cancelled_queued_gmail_push_leaves_no_marker_behind_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_push_cancel_ctrl", "gws_gmail_push_cancel_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_push_cancel").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;

        let gate = Arc::new(CalendarPassGate::new(4));
        let request_pool = crate::routes::graph_webhook::resolve_org_tenant_pool_for_graph_ingest(
            &f.state, &f.control, &f.org_id,
        ).await.expect("routed tenant request pool");
        let held = fill_org_bound(&gate, &f.org_id, &request_pool).await;
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let pass = Arc::new(recording_pass(seen.clone(), |_| Ok("o-2".to_string())));
        let spawn_push = || {
            let (state, control, gate, pass) = (f.state.clone(), f.control.clone(), gate.clone(), pass.clone());
            let (account_id, alice) = (f.account_id, f.alice);
            tokio::spawn(async move { run_gmail_sync_for_mailbox(&state, &control, &gate, account_id, alice, email, &*pass).await })
        };

        let first = spawn_push();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!first.is_finished());
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());

        let second = spawn_push();
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(!second.is_finished(), "the next push queues; it does not fold into the cancelled one");
        drop(held);
        tokio::time::timeout(Duration::from_secs(10), second).await.expect("it runs once the slot frees").unwrap().unwrap();
        assert_eq!(*seen.lock().unwrap(), vec![f.account_id]);
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-2");

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    /// Review item 5, second half (and item 1's real shape): restore items
    /// through `ingest_and_commit` under the org `Tenant`. A restore next to
    /// a failing item lands but keeps its tombstone and the cursor; a
    /// restore into alice's ARCHIVED member ocean is refused (403) and
    /// handled — tombstone dropped, cursor free — instead of wedging the
    /// mailbox; the replayed restore is deduped.
    #[tokio::test]
    async fn an_org_restore_keeps_its_tombstone_past_a_failed_batch_and_an_archived_ocean_does_not_wedge_the_cursor_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping an_org_restore_keeps_its_tombstone_past_a_failed_batch_and_an_archived_ocean_does_not_wedge_the_cursor_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (control_db, tenant_db) = ("gws_gmail_org_restore_ctrl", "gws_gmail_org_restore_tnt");
        let f = seed_org_gmail_fixture(&base, control_db, tenant_db, "org_gmail_restore").await;
        let email = "alice@example.com";
        seed_gmail_watch(&f.control, f.account_id, f.alice, email, "o-1").await;
        seed_gmail_tombstone(&f.tenant, f.account_id, f.alice_graph, f.alice, "msg-r").await;
        seed_gmail_tombstone(&f.tenant, f.account_id, f.archived_graph, f.alice, "msg-a").await;
        let tenant = org_tenant(&f.tenant, f.org_id.clone(), f.account_id, f.alice);

        let pending = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-r", Some(f.alice_graph)), gmail_pending_item("msg-new", None)],
            commit: google_gmail::PendingCommit::Complete("o-2".into()),
        };
        let missing_primary = Uuid::new_v4();
        let n = ingest_and_commit(&f.state, &f.control, tenant.clone(), f.account_id, missing_primary, f.alice, email, pending)
            .await
            .unwrap();
        assert_eq!(n, 1, "the restore landed, the live-follow item was refused");
        let restored = gmail_atom_count(&f.tenant, f.alice_graph).await;
        assert!(restored > 0);
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-1", "a failed item holds the cursor");
        assert_eq!(
            gmail_tombstones(&f.tenant, f.account_id).await,
            vec![(f.archived_graph, "msg-a".to_string()), (f.alice_graph, "msg-r".to_string())],
            "no tombstone is cleared before the cursor commits"
        );

        let pending = google_gmail::PendingSync {
            items: vec![gmail_pending_item("msg-r", Some(f.alice_graph)), gmail_pending_item("msg-a", Some(f.archived_graph))],
            commit: google_gmail::PendingCommit::Complete("o-2".into()),
        };
        let n = ingest_and_commit(&f.state, &f.control, tenant, f.account_id, f.alice_graph, f.alice, email, pending)
            .await
            .unwrap();
        assert_eq!(n, 1, "the archived ocean refuses the restore");
        assert_eq!(gmail_atom_count(&f.tenant, f.alice_graph).await, restored, "the replayed restore is deduped");
        assert_eq!(gmail_atom_count(&f.tenant, f.archived_graph).await, 0);
        assert_eq!(gmail_cursor(&f.control, f.account_id, email).await, "o-2", "one unrestorable message must not park the cursor");
        assert!(gmail_tombstones(&f.tenant, f.account_id).await.is_empty(), "both tombstones are gone with the commit");

        f.control.close().await;
        f.tenant.close().await;
        chat_drop(&base, control_db).await;
        chat_drop(&base, tenant_db).await;
    }

    // ── Gmail push contract (ADR-0016 layer 3 pins) ─────────────────────────

    fn gmail_push_body(email: &str) -> Bytes {
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD.encode(format!(r#"{{"emailAddress":"{email}","historyId":"1"}}"#));
        Bytes::from(
            serde_json::to_vec(&json!({
                "subscription": "projects/p/subscriptions/gmail-push-sub",
                "message": {"data": data, "messageId": "m-1", "publishTime": "2026-10-29T00:00:00Z"},
            }))
            .unwrap(),
        )
    }

    fn signed(claims: serde_json::Value) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("authorization", format!("Bearer {}", mint(claims)).parse().unwrap());
        headers
    }

    /// The route answers a push without a token 401 and a token minted for
    /// another audience 403. A verified push answers 200, including one whose
    /// body or `message.data` can never parse (Pub/Sub would redeliver it
    /// forever otherwise).
    #[tokio::test]
    async fn a_gmail_push_is_refused_unverified_and_acked_verified_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_gmail_push_is_refused_unverified_and_acked_verified_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _env = GOOGLE_PUSH_ENV_LOCK.lock().await;
        let db = "gws_gmail_push_route";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        chat_recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        std::env::set_var("GOOGLE_PUSH_AUDIENCE", test_cfg().audience);
        std::env::set_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL", test_cfg().service_account_email);
        *cache().write().await = Some(CachedCerts { keys: HashMap::from([(TEST_KID.into(), test_key())]), fetched: Instant::now() });

        let body = gmail_push_body("nobody@example.com");
        assert_eq!(google_push(State(state.clone()), HeaderMap::new(), body.clone()).await.status(), StatusCode::UNAUTHORIZED);
        let mut wrong = good_claims();
        wrong["aud"] = json!("another-app");
        assert_eq!(google_push(State(state.clone()), signed(wrong), body.clone()).await.status(), StatusCode::FORBIDDEN);
        let mut unverified = good_claims();
        unverified["email_verified"] = json!(false);
        assert_eq!(google_push(State(state.clone()), signed(unverified), body.clone()).await.status(), StatusCode::FORBIDDEN);
        assert_eq!(receive_jobs(&pool).await, 0, "an unverified push holds nothing");
        assert_eq!(google_push(State(state.clone()), signed(good_claims()), body).await.status(), StatusCode::OK);
        assert_eq!(receive_jobs(&pool).await, 1, "a verified push is held as one receive job");
        let garbage = Bytes::from_static(b"not json");
        assert_eq!(google_push(State(state.clone()), signed(good_claims()), garbage).await.status(), StatusCode::OK);
        let bad_data = Bytes::from(
            serde_json::to_vec(&json!({
                "subscription": "projects/p/subscriptions/gmail-push-sub",
                "message": {"data": "%%%", "messageId": "m-2"},
            }))
            .unwrap(),
        );
        assert_eq!(google_push(State(state.clone()), signed(good_claims()), bad_data).await.status(), StatusCode::OK);
        assert_eq!(receive_jobs(&pool).await, 1, "a push that never parses holds nothing");

        std::env::remove_var("GOOGLE_PUSH_AUDIENCE");
        std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
        *cache().write().await = None;
        drop(state);
        pool.close().await;
        chat_drop(&base, db).await;
    }

    /// A Gmail round trip whose diff holds one new message.
    fn one_new_message(_: GmailPassInput<'_>) -> BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> {
        Box::pin(async {
            Ok(google_gmail::PendingSync {
                items: vec![gmail_pending_item("msg-new", None)],
                commit: google_gmail::PendingCommit::Complete("101".into()),
            })
        })
    }

    async fn receive_jobs(pool: &PgPool) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind = 'IngestReceive'").fetch_one(pool).await.unwrap()
    }

    /// The route only writes the job; the Gmail round trip runs in the
    /// receive worker.
    fn no_round_trip(_: GmailPassInput<'_>) -> BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> {
        panic!("the webhook made a Gmail call")
    }

    fn failing_round_trip(_: GmailPassInput<'_>) -> BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> {
        Box::pin(async { Err(ConnectorError::upstream_code(503, "gmail history.list failed: 503 Service Unavailable: ")) })
    }

    struct VerifiedPush {
        state: Arc<AppState>,
        pool: PgPool,
        primary: Uuid,
        account_id: Uuid,
        _env: tokio::sync::MutexGuard<'static, ()>,
    }

    /// A watched personal mailbox (cursor `100`, primary ocean) and the push
    /// verification config.
    async fn verified_push_fixture(base: &str, db: &str, email: &str) -> VerifiedPush {
        let env = GOOGLE_PUSH_ENV_LOCK.lock().await;
        set_test_kek();
        chat_recreate(base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let primary = seed_gmail_graph(&pool, account_id, "Primary", true).await;
        seed_gmail_watch(&pool, account_id, owner, email, "100").await;
        let state = Arc::new(crate::test_app_state_owned_with_db_and_store(pool.clone(), None));
        std::env::set_var("GOOGLE_PUSH_AUDIENCE", test_cfg().audience);
        std::env::set_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL", test_cfg().service_account_email);
        *cache().write().await = Some(CachedCerts { keys: HashMap::from([(TEST_KID.into(), test_key())]), fetched: Instant::now() });
        VerifiedPush { state, pool, primary, account_id, _env: env }
    }

    impl VerifiedPush {
        async fn push(&self, email: &str) -> StatusCode {
            google_push(State(self.state.clone()), signed(good_claims()), gmail_push_body(email)).await.status()
        }

        async fn close(self, base: &str, db: &str) {
            std::env::remove_var("GOOGLE_PUSH_AUDIENCE");
            std::env::remove_var("GOOGLE_PUSH_SERVICE_ACCOUNT_EMAIL");
            *cache().write().await = None;
            drop(self.state);
            self.pool.close().await;
            chat_drop(base, db).await;
        }
    }

    /// A verified push for a watched personal mailbox is held as one receive
    /// job with no Gmail call; the worker stores the new message through
    /// `submit` and advances the cursor. Pushes for the mailbox while its job
    /// is pending coalesce into it, and a push replayed after it ran stores
    /// nothing new.
    #[tokio::test]
    async fn a_gmail_push_for_a_watched_mailbox_stores_the_new_message_once_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_gmail_push_for_a_watched_mailbox_stores_the_new_message_once_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_push_stores";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let email = "watched@example.com";
        let f = verified_push_fixture(&base, db, email).await;

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(receive_jobs(&f.pool).await, 1, "pushes for a mailbox coalesce while its job is pending");
        assert_eq!(gmail_atom_count(&f.pool, f.primary).await, 0, "nothing is stored before the worker runs");

        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &one_new_message).await, 1);
        let stored = gmail_atom_count(&f.pool, f.primary).await;
        assert!(stored > 0, "the new message is stored in the primary ocean");
        assert_eq!(gmail_cursor(&f.pool, f.account_id, email).await, "101");
        let (status, payload): (String, serde_json::Value) =
            sqlx::query_as("SELECT status::text, payload FROM jobs WHERE kind = 'IngestReceive'").fetch_one(&f.pool).await.unwrap();
        assert_eq!(status, "done");
        assert_eq!(payload["email_address"], "[redacted on completion]");

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &one_new_message).await, 1);
        assert_eq!(gmail_atom_count(&f.pool, f.primary).await, stored, "a replayed push stores nothing new");
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &no_round_trip).await, 0);

        f.close(&base, db).await;
    }

    /// A Gmail round trip that fails mid-fetch leaves the push's job pending
    /// on the backoff ladder with the cursor unmoved; the retry stores the
    /// message.
    #[tokio::test]
    async fn a_gmail_push_whose_fetch_fails_retries_and_keeps_the_work_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_gmail_push_whose_fetch_fails_retries_and_keeps_the_work_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_push_retry";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let email = "retry@example.com";
        let f = verified_push_fixture(&base, db, email).await;

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &failing_round_trip).await, 1);
        let (status, attempts, waits): (String, i32, bool) = sqlx::query_as(
            "SELECT status::text, attempts, run_after > now() FROM jobs WHERE kind = 'IngestReceive'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!((status.as_str(), attempts, waits), ("pending", 1, true), "the failed fetch backs off and stays queued");
        assert_eq!(gmail_cursor(&f.pool, f.account_id, email).await, "100");
        assert_eq!(gmail_atom_count(&f.pool, f.primary).await, 0);

        sqlx::query("UPDATE jobs SET run_after = now() WHERE kind = 'IngestReceive'").execute(&f.pool).await.unwrap();
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &one_new_message).await, 1);
        assert!(gmail_atom_count(&f.pool, f.primary).await > 0, "the retry stores the message");
        assert_eq!(gmail_cursor(&f.pool, f.account_id, email).await, "101");

        f.close(&base, db).await;
    }

    /// The Gmail dedupe key is an HMAC of the address under the derived
    /// `gmail-push-mailbox` key: it changes with the key and is not the
    /// address's bare digest, so `jobs.idempotency_key` (never redacted)
    /// does not stand for the address.
    #[test]
    fn the_gmail_dedupe_key_is_an_hmac_of_the_mailbox_not_a_bare_hash() {
        use sha2::{Digest, Sha256};
        let key_a = crate::crypto::kdf::derive_key(&[1u8; 32], crate::crypto::kdf::purpose::GMAIL_PUSH_MAILBOX);
        let key_b = crate::crypto::kdf::derive_key(&[2u8; 32], crate::crypto::kdf::purpose::GMAIL_PUSH_MAILBOX);
        let a = mailbox_key_with(&key_a, "someone@example.com");
        assert_eq!(a, mailbox_key_with(&key_a, "someone@example.com"), "deterministic under one key");
        assert_ne!(a, mailbox_key_with(&key_b, "someone@example.com"), "keyed");
        assert_ne!(a, mailbox_key_with(&key_a, "someone.else@example.com"));
        assert!(a.starts_with("ingest_receive:gmail:"));
        assert!(!a.contains(&format!("{:x}", Sha256::digest(b"someone@example.com"))), "not a bare digest");
        assert_eq!(a.len(), "ingest_receive:gmail:".len() + 64);
    }

    /// A Workspace push is keyed by subscription and message id together:
    /// Pub/Sub ids are unique per topic, so the same id on the Chat, Chat
    /// app and Meet subscriptions is three deliveries, not one.
    #[test]
    fn a_workspace_push_key_is_scoped_to_its_subscription() {
        let chat = "projects/p/subscriptions/chat-events-sub";
        let app = "projects/p/subscriptions/chat-app-events-sub";
        let meet = "projects/p/subscriptions/meet-events-sub";
        let keys = [chat, app, meet].map(|s| workspace_event_key(s, "m-1").unwrap());
        assert_eq!(keys[0], "ingest_receive:google:projects/p/subscriptions/chat-events-sub:m-1");
        assert_eq!(keys[1], "ingest_receive:google:projects/p/subscriptions/chat-app-events-sub:m-1");
        assert_eq!(keys[2], "ingest_receive:google:projects/p/subscriptions/meet-events-sub:m-1");
        assert_eq!(keys.iter().collect::<std::collections::HashSet<_>>().len(), 3, "no collision across surfaces");
        assert_eq!(workspace_event_key(chat, "m-1"), workspace_event_key(chat, "m-1"), "a redelivery repeats the key");
        assert_ne!(workspace_event_key(chat, "m-1"), workspace_event_key(chat, "m-2"));
        assert_eq!(workspace_event_key(chat, ""), None, "no message id, no key");
        // Through the envelope: the key the route writes.
        let envelope = |subscription: &str| {
            let raw = json!({"subscription": subscription, "message": {"data": "e30=", "messageId": "m-1"}});
            let (_, key) = received_event(PushEnvelope::deserialize(&raw).unwrap(), raw.clone()).unwrap();
            key.unwrap()
        };
        assert_ne!(envelope(chat), envelope(meet));
        assert_eq!(envelope(app), keys[1]);
    }

    /// A Gmail round trip answering a permanent 4xx.
    fn gone_round_trip(_: GmailPassInput<'_>) -> BoxFuture<'_, Result<google_gmail::PendingSync, ConnectorError>> {
        Box::pin(async { Err(ConnectorError::upstream_code(401, "gmail history.list failed: 401 Unauthorized: status=UNAUTHENTICATED")) })
    }

    async fn receive_job_row(pool: &PgPool) -> (String, i32, serde_json::Value) {
        sqlx::query_as("SELECT status::text, attempts, payload FROM jobs WHERE kind = 'IngestReceive' ORDER BY created_at DESC LIMIT 1")
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A Gmail round trip that fails with a permanent 4xx completes the push
    /// without a retry, scrubbed, with the cursor unmoved; a transient one
    /// that exhausts its attempts is dead-lettered, scrubbed as well.
    #[tokio::test]
    async fn a_gmail_push_failing_permanently_completes_and_an_exhausted_one_dies_redacted_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_gmail_push_failing_permanently_completes_and_an_exhausted_one_dies_redacted_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_push_permanent";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let email = "permanent@example.com";
        let f = verified_push_fixture(&base, db, email).await;

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &gone_round_trip).await, 1);
        let (status, attempts, payload) = receive_job_row(&f.pool).await;
        assert_eq!((status.as_str(), attempts), ("done", 0), "a permanent failure is not retried");
        assert_eq!(payload["email_address"], "[redacted on completion]");
        assert_eq!(gmail_cursor(&f.pool, f.account_id, email).await, "100");
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &no_round_trip).await, 0);

        assert_eq!(f.push(email).await, StatusCode::OK);
        sqlx::query("UPDATE jobs SET max_attempts = 1 WHERE kind = 'IngestReceive' AND status = 'pending'")
            .execute(&f.pool)
            .await
            .unwrap();
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &failing_round_trip).await, 1);
        let (status, attempts, payload) = receive_job_row(&f.pool).await;
        assert_eq!((status.as_str(), attempts), ("dead", 1), "the exhausted transient failure is dead-lettered");
        assert_eq!(payload["email_address"], "[redacted on completion]", "a dead row is scrubbed too");

        f.close(&base, db).await;
    }

    /// A Meet push whose event cannot be decoded completes without a retry,
    /// its message data scrubbed; one whose handler fails transiently (here,
    /// storage gone) is a retryable error, so the receive job retries rather
    /// than completing as done work.
    #[tokio::test]
    async fn a_workspace_push_completes_when_malformed_and_retries_on_a_transient_failure_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping a_workspace_push_completes_when_malformed_and_retries_on_a_transient_failure_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_workspace_push_durable";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let f = verified_push_fixture(&base, db, "unused@example.com").await;
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD.encode(r#"{"transcript":{"name":"conferenceRecords/r1/transcripts/t1"}}"#);

        // No CloudEvent attributes: the Meet event never decodes.
        let malformed = Bytes::from(
            serde_json::to_vec(&json!({
                "subscription": "projects/p/subscriptions/meet-events-sub",
                "message": {"data": data, "messageId": "meet-malformed-1"},
            }))
            .unwrap(),
        );
        assert_eq!(google_push(State(f.state.clone()), signed(good_claims()), malformed).await.status(), StatusCode::OK);
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &no_round_trip).await, 1);
        let (status, attempts, payload) = receive_job_row(&f.pool).await;
        assert_eq!((status.as_str(), attempts), ("done", 0), "a push that never decodes is not retried");
        assert_eq!(payload["envelope"]["message"]["data"], "[redacted on completion]");

        // A well-formed Meet event whose handler cannot reach storage.
        let envelope = json!({
            "subscription": "projects/p/subscriptions/meet-events-sub",
            "message": {"data": data, "messageId": "meet-transient-1", "attributes": {
                "ce-type": crate::connectors::google_meet_events::EVENT_TYPE,
                "ce-source": "//workspaceevents.googleapis.com/subscriptions/unknown",
                "ce-subject": "//cloudidentity.googleapis.com/users/108123456789",
            }},
        });
        let closed: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        closed.close().await;
        let no_storage = Arc::new(crate::test_app_state_owned_with_db(closed, None));
        let err = receive_workspace_event(no_storage, envelope.clone())
            .await
            .expect_err("a handler failure must surface, not complete as done work");
        assert!(err.retryable(), "{err}");
        assert!(matches!(err, ReceiveError::Connector(_)));
        // The same push through a working state is a clean no-op (unknown
        // subscription), as `dispatch` answered before the receive stage.
        receive_workspace_event(f.state.clone(), envelope).await.expect("an unknown Meet subscription is dropped cleanly");

        f.close(&base, db).await;
    }


    /// A Chat event for a space one account watches on a tenant plane that
    /// cannot be resolved right now fails the receive job (a retry can find
    /// the plane), instead of being logged and completed; the same account
    /// on a write-frozen tenant is permanent, so the event completes.
    #[tokio::test]
    async fn a_chat_event_retries_when_an_accounts_plane_is_unavailable_and_completes_when_it_is_frozen_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!(
                "skipping a_chat_event_retries_when_an_accounts_plane_is_unavailable_and_completes_when_it_is_frozen_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "gws_chat_event_plane_failures";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        chat_recreate(&base, db).await;
        let control: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_plane_fail_{}", Uuid::new_v4());
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
            .bind(format!("owner-{}@test.local", Uuid::new_v4()))
            .fetch_one(&control)
            .await
            .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
                .bind(owner)
                .bind(&org_id)
                .fetch_one(&control)
                .await
                .unwrap();
        let graph_id: Uuid = sqlx::query_scalar(
            "INSERT INTO graphs (account_id, name, kind, is_primary) VALUES ($1, 'Ocean', 'personal', true) RETURNING id",
        )
        .bind(account_id)
        .fetch_one(&control)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO google_chat_watched_spaces (account_id, space_id, space_type, mode, graph_id, active) \
             VALUES ($1, 'spaces/PLANE', 'SPACE', 'single', $2, true)",
        )
        .bind(account_id)
        .bind(graph_id)
        .execute(&control)
        .await
        .unwrap();
        // The org's tenant row names a secret this process cannot resolve:
        // the pool cannot be built, which is a transient condition.
        mint_tenant_databases_row(&control, &org_id, "plane-fail", account_id, "SECRET_NOBODY_HAS").await;
        let resolver = crate::tenancy::secrets::StaticMapResolver::from_pairs([("SECRET_SOMEONE_ELSE", chat_with_db(&base, db))]);
        let registry = std::sync::Arc::new(crate::tenancy::registry::TenantRegistry::new(std::sync::Arc::new(resolver)));
        let runtime = crate::tenancy::runtime::TenantRuntime::new(true, "coralswarm.test", registry);
        let state = Arc::new(crate::test_app_state_owned_with_db(control.clone(), Some(runtime)));

        let err = run_chat_event(&state, &message_created_event("spaces/PLANE"))
            .await
            .expect_err("an unresolvable plane must fail the event, not complete it");
        assert!(crate::workers::ingest_receive::transient(&err), "{err}");
        assert!(matches!(err, ConnectorError::Internal(_)), "{err:?}");

        // Write-frozen: no retry can change it, so the event completes.
        sqlx::query("UPDATE tenant_databases SET status = 'suspended' WHERE clerk_org_id = $1")
            .bind(&org_id)
            .execute(&control)
            .await
            .unwrap();
        assert_eq!(
            run_chat_event(&state, &message_created_event("spaces/PLANE")).await.expect("a frozen tenant is permanent"),
            0
        );

        drop(state);
        control.close().await;
        chat_drop(&base, db).await;
    }

    fn gate_event(event_type: &str, user: Option<&str>) -> ChatInteractionEvent {
        ChatInteractionEvent {
            event_type: event_type.into(),
            space: ChatInteractionSpace { name: "spaces/GATE".into(), space_type: "SPACE".into(), display_name: "Gate".into() },
            user: user.map(|name| ChatInteractionUser { name: name.into() }),
        }
    }

    /// The Chat app install gate surfaces its failures to the receive
    /// worker: `spaces.get` answering 503 is retryable, 403 is permanent, a
    /// storage failure is retryable; a retried `ADDED_TO_SPACE` reactivates
    /// the one row it already created, and a retried `REMOVED_FROM_SPACE`
    /// finds it already inactive. Nothing is retried blind: each outcome is
    /// the handler's own `Result`.
    #[tokio::test]
    async fn the_install_gate_surfaces_its_failures_and_retries_are_idempotent_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping the_install_gate_surfaces_its_failures_and_retries_are_idempotent_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_install_gate_failures";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let _env = GOOGLE_PUSH_ENV_LOCK.lock().await;
        chat_recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        sqlx::migrate!("../backend/migrations_control").run(&pool).await.unwrap();
        let (account_id, owner) = seed_gmail_account(&pool).await;
        let state = Arc::new(crate::test_app_state_owned_with_db(pool.clone(), None));
        let plane = crate::crypto::DekPlane::resolve_for_account(&state, &pool, account_id).await.unwrap();
        let token = crate::connectors::TokenBundle {
            access_token: "gate-token".into(),
            refresh_token: None,
            user_token: None,
            scope: "chat.spaces.readonly".into(),
            expires_at: Some(chrono::Utc::now() + chrono::Duration::hours(1)),
            provider_workspace_id: "gate@example.com".into(),
            provider_workspace_name: None,
            provider_user_id: "sub-gate".into(),
        };
        store_token_bundle(&pool, &plane, account_id, "google_chat", TokenOwner::Member(owner), &token).await.unwrap();

        use std::sync::atomic::{AtomicU16, Ordering};
        let answer = Arc::new(AtomicU16::new(503));
        let answer_srv = answer.clone();
        let app = axum::Router::new().route(
            "/spaces/GATE",
            axum::routing::get(move || {
                let answer = answer_srv.clone();
                async move {
                    let status = StatusCode::from_u16(answer.load(Ordering::SeqCst)).unwrap();
                    (status, axum::Json(json!({"name": "spaces/GATE", "spaceType": "SPACE", "historyState": "HISTORY_ON", "displayName": "Gate"})))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rows = || async {
            sqlx::query_as::<_, (bool,)>("SELECT active FROM google_chat_watched_spaces WHERE account_id = $1 AND space_id = 'spaces/GATE'")
                .bind(account_id)
                .fetch_all(&pool)
                .await
                .unwrap()
        };

        google_chat::TEST_CHAT_API_BASE
            .scope(url, async {
                let err = handle_chat_added_to_space(&state, gate_event("ADDED_TO_SPACE", Some("users/sub-gate")))
                    .await
                    .expect_err("a 503 from spaces.get must surface");
                assert!(matches!(err, ConnectorError::UpstreamStatus { status: 503, .. }), "{err:?}");
                assert!(ReceiveError::from(err).retryable(), "a 503 retries");
                assert!(rows().await.is_empty(), "nothing is recorded on a failed read");

                answer.store(403, Ordering::SeqCst);
                let err = handle_chat_added_to_space(&state, gate_event("ADDED_TO_SPACE", Some("users/sub-gate")))
                    .await
                    .expect_err("a 403 from spaces.get must surface");
                assert!(!ReceiveError::from(err).retryable(), "a 403 completes the job");
                assert!(rows().await.is_empty());

                answer.store(200, Ordering::SeqCst);
                handle_chat_added_to_space(&state, gate_event("ADDED_TO_SPACE", Some("users/sub-gate"))).await.unwrap();
                assert_eq!(rows().await, vec![(true,)], "the retry that succeeds records the space");
                handle_chat_added_to_space(&state, gate_event("ADDED_TO_SPACE", Some("users/sub-gate"))).await.unwrap();
                assert_eq!(rows().await, vec![(true,)], "a redelivered ADDED_TO_SPACE upserts the same row");
            })
            .await;

        handle_chat_removed_from_space(&state, gate_event("REMOVED_FROM_SPACE", Some("users/nobody"))).await.unwrap();
        assert_eq!(rows().await, vec![(false,)], "deactivated");
        handle_chat_removed_from_space(&state, gate_event("REMOVED_FROM_SPACE", None)).await.unwrap();
        assert_eq!(rows().await, vec![(false,)], "a redelivered REMOVED_FROM_SPACE is a clean no-op");

        // Storage gone: both halves of the gate are retryable errors.
        let closed: PgPool = PgPoolOptions::new().connect(&chat_with_db(&base, db)).await.unwrap();
        closed.close().await;
        let no_storage = Arc::new(crate::test_app_state_owned_with_db(closed, None));
        let err = handle_chat_added_to_space(&no_storage, gate_event("ADDED_TO_SPACE", Some("users/sub-gate")))
            .await
            .expect_err("a storage failure must surface");
        assert!(ReceiveError::from(err).retryable());
        let err = handle_chat_removed_from_space(&no_storage, gate_event("REMOVED_FROM_SPACE", None))
            .await
            .expect_err("a storage failure must surface");
        assert!(ReceiveError::from(err).retryable());
        // And through `dispatch`, as the worker reaches it.
        use base64::Engine as _;
        let data = base64::engine::general_purpose::STANDARD
            .encode(r#"{"type":"REMOVED_FROM_SPACE","space":{"name":"spaces/GATE","type":"ROOM","displayName":"Gate"}}"#);
        let envelope = json!({
            "subscription": "projects/p/subscriptions/chat-app-events-sub",
            "message": {"data": data, "messageId": "gate-removed-1"},
        });
        let err = receive_workspace_event(no_storage.clone(), envelope.clone()).await.expect_err("dispatch surfaces the gate's failure");
        assert!(err.retryable(), "{err}");
        receive_workspace_event(state.clone(), envelope).await.expect("with storage back, the event settles");

        drop(no_storage);
        drop(state);
        pool.close().await;
        chat_drop(&base, db).await;
    }

    /// While a mailbox's push waits on backoff after a transient failure,
    /// further pushes for that mailbox coalesce into the waiting job: the
    /// retry runs the whole pass, so they add nothing. Before, the failed
    /// job dropped its dedupe key and every push during an outage added a
    /// row.
    #[tokio::test]
    async fn pushes_for_a_mailbox_coalesce_into_its_waiting_retry_pg() {
        let Some(base) = chat_test_base_url() else {
            eprintln!("skipping pushes_for_a_mailbox_coalesce_into_its_waiting_retry_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "gws_gmail_push_coalesce_retry";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let email = "coalesce@example.com";
        let f = verified_push_fixture(&base, db, email).await;

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &failing_round_trip).await, 1);
        let (status, key_kept, waits): (String, bool, bool) = sqlx::query_as(
            "SELECT status::text, idempotency_key IS NOT NULL, run_after > now() FROM jobs WHERE kind = 'IngestReceive'",
        )
        .fetch_one(&f.pool)
        .await
        .unwrap();
        assert_eq!((status.as_str(), key_kept, waits), ("pending", true, true), "the waiting retry keeps its key");

        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(f.push(email).await, StatusCode::OK);
        assert_eq!(receive_jobs(&f.pool).await, 1, "pushes during the outage coalesce into the waiting job");

        sqlx::query("UPDATE jobs SET run_after = now() WHERE kind = 'IngestReceive'").execute(&f.pool).await.unwrap();
        assert_eq!(crate::workers::ingest_receive::drain(&f.state, &one_new_message).await, 1);
        assert!(gmail_atom_count(&f.pool, f.primary).await > 0, "the one retry covers every push");
        assert_eq!(receive_jobs(&f.pool).await, 1);

        f.close(&base, db).await;
    }
}
