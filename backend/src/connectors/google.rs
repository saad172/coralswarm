//! Google connector — Google Calendar (read-only) over the connector framework
//! (cs-uxp4a-calendar), now with the per-surface scope split laid down for the
//! rest of Google Workspace (gsuite Train G1 PR 1,
//! `.scratch/gsuite-enterprise/plan.md`).
//!
//! This is the first non-Microsoft OAuth2 authorization-code + offline-refresh
//! connector. It mirrors the Microsoft connector's shape exactly (env-var creds,
//! `id_token` claim parsing for the account label, refresh-if-expired) so the
//! generic OAuth routes (`routes/oauth.rs`) and the calendar read route
//! (`routes/calendar.rs`) treat every provider the same way.
//!
//! # Surfaces and scope (decision 3)
//! Four surfaces exist under the single `google` OAuth client/connector:
//! [`Surface::Calendar`] (shipped) and, as of gsuite Train G2 PR 1,
//! [`Surface::Gmail`] can complete real OAuth connect/list/disconnect with
//! its own per-member-isolated `oauth_tokens` row (see [`Surface::
//! token_provider`]/[`Surface::token_owner_for`] and `routes::oauth::
//! callback`) — Gmail's actual mailbox *ingest* (`fetch_items`/`sync`) is
//! still unbuilt, tracked for Train G2's follow-on PR. [`Surface::Chat`] and
//! [`Surface::Meet`] were declared but **inert** end to end — no OAuth
//! completion, no ingest — until Trains G4/G3 layered them on: Chat's OAuth
//! completes as of G4.1 with ingest live as of G4.4, and **Meet's OAuth
//! completes AND ingests as of gsuite Train G3 PR 2** (real
//! `TokenOwner::Member`-owned `google_meet` tokens, driven by
//! `workers::google_meet_sweep` — see `connectors::google_meet`'s own module
//! doc for the fan-out design). Each surface's
//! data scope (see [`Surface::data_scope`]) is treated as **final the moment
//! it is ever requested for real** — Google supports incremental consent
//! (`include_granted_scopes=true` on every request, so a member who granted
//! Calendar keeps that grant when they later add Gmail), but it does **not**
//! let us silently widen an already-granted scope: doing so re-prompts every
//! member who already consented. So the scope set per surface is chosen once,
//! here, rather than grown opportunistically later.
//!
//! **The one deliberate exception:** Chat's scope set was widened a second
//! time (chat-per-user-setup roster-matching PR, 2026-09-20) to add
//! `directory.readonly`, AFTER real members had already consented to the
//! original three — see [`Surface::Chat`]'s `data_scope` match arm for the
//! founder-approved reasoning and the exact re-consent cost this accepts.
//! "Chosen once" above describes the steady-state policy, not an
//! unconditional rule; a widening is still possible, it is just never free.
//!

//! `openid email` is requested for every surface (not just Calendar) because
//! [`ensure_stable_identity`] needs the `id_token`'s `sub`/`email` claims
//! regardless of which data scope came with them.
//!
//! # Gating (`ORG_GOOGLE_*` flags)
//! Calendar is grandfathered on unconditionally (it shipped before the
//! per-org connector flag registry existed — see `connector_flags`'s module
//! doc for why "zero rows = today's behavior" is the load-bearing invariant
//! there). Gmail/Chat/Meet each gate on their own `ORG_GOOGLE_<SURFACE>_ENABLED`
//! env default, overridable per Clerk org via the same `org_connector_flags`
//! registry Slack/Teams use (provider keys `google_gmail`/`google_chat`/
//! `google_meet` — see [`Surface::flag_provider`]). `routes::oauth::authorize`
//! is where this is actually enforced, since it is the one place that already
//! resolves the caller's account/org from the signed `state` token without
//! requiring the full tenancy-gate `Tenant` extractor (which this open,
//! browser-redirect route deliberately does not carry — see its own module
//! doc). Default OFF for the three inert surfaces, matching "surfaces beyond
//! calendar stay inert" until their train ships.
//!
//! # What it powers
//! Two independent things, both driven by the same OAuth connection:
//!
//! * `GET /v1/calendar/upcoming` (`routes/calendar.rs`) — a read-only signal
//!   source for the client: meeting titles + attendee rosters for the
//!   pre-meeting nudge. Unpaginated, `singleEvents=true` (expanded
//!   occurrences), unchanged by this PR.
//! * Calendar sync (gsuite Train G3 PR 1, `google_calendar` module) — events
//!   **ingested as atoms** into the connecting member's own ocean, exactly
//!   like Microsoft's calendar. Contract §7 Calendar archetype: C1/C3
//!   absent, C4 trivial (the owner), self-owned and non-sensitive, so the
//!   `supports_sync()` path is the right one (founder decision 2026-08-23).
//!   The passes themselves live in `workers::google_calendar_sync` (one
//!   per plane, run by the sweep, the push webhook and the manual route
//!   alike); see `connectors::google_calendar`'s module doc for the
//!   ingest/watch/sync-token design.
//!
//! Gmail (Mailbox archetype), Chat (its own archetype), and Meet (§8
//! Meetings archetype, per-attendee fan-out) layer onto this same provider —
//! see `docs/connectors/google-workspace.md` for the capability
//! declarations.
//!
//! # Security
//! Tokens are encrypted at rest by the generic store path
//! (`store_token_bundle`, AES-256-GCM under the per-account DEK) — identical to
//! Slack/Microsoft. Access/refresh tokens and event contents are never logged.

use async_trait::async_trait;
use base64::Engine as _;
use chrono::{DateTime, Duration, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

use super::{ConnectorCredentials, ConnectorError, ConnectorProvider, CredentialDeathReason, TokenBundle};

// ---------------------------------------------------------------------------
// Shared outbound rate limiter (gsuite Train G2 PR 3, design doc §item 5) —
// `crate::graph_rate_limit` is Microsoft-agnostic despite the module name
// (see its own doc's "Future connectors" section); this is the first Google
// call site to actually use it. Mirrors `connectors::microsoft::graph_send`
// byte-for-byte: acquire a permit for the ambient `scoped()` key (falling
// back to `UNSCOPED_KEY` for any call site nobody wrapped — a defensive
// fallback, not the intended steady state), honor a `Retry-After` response
// with one bounded retry. Every outbound HTTP call in `google.rs`,
// `google_calendar.rs`, `google_gmail.rs`, and `workers::
// google_watch_renewal`'s renew functions goes through this.
// ---------------------------------------------------------------------------

pub(crate) async fn google_send(
    build: impl Fn() -> reqwest::RequestBuilder,
) -> Result<reqwest::Response, ConnectorError> {
    google_send_with_pacing(build, || async {}).await
}

pub(crate) async fn google_send_with_pacing<F: std::future::Future<Output = ()>>(
    build: impl Fn() -> reqwest::RequestBuilder,
    before_send: impl Fn() -> F,
) -> Result<reqwest::Response, ConnectorError> {
    let key = crate::graph_rate_limit::current_key()
        .unwrap_or_else(|| crate::graph_rate_limit::UNSCOPED_KEY.to_string());
    let limiter = crate::graph_rate_limit::shared();

    for attempt in 0..2u8 {
        let _permit = limiter
            .acquire(&key)
            .await
            // Not `{e}`: the error names the limiter key, which for Gmail
            // is a hash of the mailbox address, and this text reaches
            // `last_error` and logs.
            .map_err(|_| ConnectorError::Exchange("google rate limiter saturated: no slot within budget".into()))?;
        before_send().await;
        let resp = build()
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;
        let status = resp.status();
        if attempt == 0
            && (status == reqwest::StatusCode::TOO_MANY_REQUESTS
                || status == reqwest::StatusCode::SERVICE_UNAVAILABLE)
        {
            let retry_after = parse_retry_after(resp.headers());
            tracing::info!(%key, ?retry_after, %status, "google throttled; honoring Retry-After and retrying once");
            limiter.penalize(&key, retry_after).await;
            continue;
        }
        return Ok(resp);
    }
    unreachable!("loop always returns on its second iteration")
}

/// Parse a `Retry-After` header (seconds) into a [`std::time::Duration`],
/// falling back to a fixed default when absent/unparsable. Mirrors
/// `connectors::microsoft::parse_retry_after` exactly.
fn parse_retry_after(headers: &reqwest::header::HeaderMap) -> std::time::Duration {
    headers
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.trim().parse::<u64>().ok())
        .map(std::time::Duration::from_secs)
        .unwrap_or(crate::graph_rate_limit::DEFAULT_RETRY_AFTER)
}

// ---------------------------------------------------------------------------
// Google API error bodies — keep the diagnosis, drop everything else
// ---------------------------------------------------------------------------

/// Upper bound (in chars) on the detail [`google_error_detail`] returns. Sized
/// so the common shape, `"upstream exchange failed: gmail messages.list
/// failed: 403 Forbidden: "` plus the detail, fits the 500-char `last_error`
/// column cap `workers::gmail_everything_mode::mark_failed_no_progress`
/// applies. A caller that wraps the message in a longer prefix can exceed
/// 500, and then the tail is what gets cut. The detail always puts
/// `reasons=[...]` first and the free-text `message` last, so a cut loses
/// message text before it loses a reason.
pub(crate) const GOOGLE_ERROR_DETAIL_MAX_CHARS: usize = 360;

/// Cap on Google's free-text `error.message` inside the detail. The message
/// is where the actual diagnosis lives ("Gmail API has not been used in
/// project N before or it is disabled", "Request had insufficient
/// authentication scopes", "Quota exceeded for ..."), but it is still free
/// text, so it is bounded separately from the structured fields.
const GOOGLE_ERROR_MESSAGE_MAX_CHARS: usize = 200;

/// Cap on how many bytes of an error body are read at all — a Google error
/// envelope is a few hundred bytes; anything larger is not one.
const GOOGLE_ERROR_BODY_MAX_BYTES: usize = 16 * 1024;

#[derive(Debug, Default, Deserialize)]
struct GoogleApiErrorEnvelope {
    #[serde(default)]
    error: Option<GoogleApiError>,
}

#[derive(Debug, Default, Deserialize)]
struct GoogleApiError {
    #[serde(default)]
    code: Option<i64>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    status: Option<String>,
    /// Legacy `errors[]` — `reason` is `rateLimitExceeded`,
    /// `userRateLimitExceeded`, `dailyLimitExceeded`, `domainPolicy`,
    /// `insufficientPermissions`, `accessNotConfigured`, `forbidden`, ...
    #[serde(default)]
    errors: Vec<GoogleApiErrorItem>,
    /// `google.rpc.Status.details[]` — only `ErrorInfo` entries carry a
    /// `reason` (`SERVICE_DISABLED`, `ACCESS_TOKEN_SCOPE_INSUFFICIENT`,
    /// `RATE_LIMIT_EXCEEDED`, ...); other detail types are ignored.
    #[serde(default)]
    details: Vec<GoogleApiErrorDetail>,
}

#[derive(Debug, Default, Deserialize)]
struct GoogleApiErrorItem {
    #[serde(default)]
    reason: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct GoogleApiErrorDetail {
    #[serde(default)]
    reason: Option<String>,
}

/// Collapse whitespace and cap at `max` chars, marking a cut with `…`.
fn bounded(s: &str, max: usize) -> String {
    let collapsed = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if collapsed.chars().count() <= max {
        return collapsed;
    }
    let mut out: String = collapsed.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Turn a Google API error body into a short, bounded, durable diagnosis:
/// `reasons=[userRateLimitExceeded,RATE_LIMIT_EXCEEDED] status=PERMISSION_DENIED
/// code=403 message="..."`. Pure (no I/O) so it is unit-testable.
///
/// Only the structured envelope fields are kept — `error.errors[].reason`,
/// `error.details[].reason`, `error.status`, `error.code`, and a capped
/// `error.message`. A body that is NOT Google's JSON envelope is never echoed
/// (only its byte length), so an HTML interstitial or any echoed content
/// cannot leak into `last_error`/logs. Google error envelopes never contain
/// the bearer token; the request's `Authorization` header is never read here.
pub(crate) fn google_error_detail(body: &str) -> String {
    let parsed = serde_json::from_str::<GoogleApiErrorEnvelope>(body)
        .ok()
        .and_then(|e| e.error);
    let Some(err) = parsed else {
        return if body.trim().is_empty() {
            "empty error body".to_string()
        } else {
            format!("non-JSON error body ({} bytes)", body.len())
        };
    };

    let mut reasons: Vec<String> = Vec::new();
    for r in err
        .errors
        .iter()
        .filter_map(|e| e.reason.as_deref())
        .chain(err.details.iter().filter_map(|d| d.reason.as_deref()))
    {
        let r = bounded(r, 64);
        if !r.is_empty() && !reasons.contains(&r) {
            reasons.push(r);
        }
    }

    let mut parts: Vec<String> = Vec::new();
    if !reasons.is_empty() {
        parts.push(format!("reasons=[{}]", reasons.join(",")));
    }
    if let Some(status) = err.status.as_deref().filter(|s| !s.trim().is_empty()) {
        parts.push(format!("status={}", bounded(status, 64)));
    }
    if let Some(code) = err.code {
        parts.push(format!("code={code}"));
    }
    if let Some(message) = err.message.as_deref().filter(|s| !s.trim().is_empty()) {
        parts.push(format!(
            "message=\"{}\"",
            bounded(message, GOOGLE_ERROR_MESSAGE_MAX_CHARS)
        ));
    }
    if parts.is_empty() {
        return "JSON error body with no recognised fields".to_string();
    }
    bounded(&parts.join(" "), GOOGLE_ERROR_DETAIL_MAX_CHARS)
}

/// Consume a non-success Google API response into a
/// [`ConnectorError::UpstreamStatus`] that carries the status and keeps
/// Google's own diagnosis as the message:
/// `"{what} failed: {status}: {google_error_detail}"`. Every Gmail call site
/// uses this instead of discarding the body (the everything-mode `403` that
/// could not be told apart as quota vs. policy vs. scope, 2026-09-24).
pub(crate) async fn google_api_error(what: &str, resp: reqwest::Response) -> ConnectorError {
    let failure = google_api_failure(what, resp).await;
    ConnectorError::upstream(failure.status, failure.message)
}

/// The message half of [`google_api_error`], for call sites that branch on
/// the failure (log it) rather than return it.
pub(crate) async fn google_api_error_message(what: &str, resp: reqwest::Response) -> String {
    google_api_failure(what, resp).await.message
}

/// Google's reasons for "slow down and try again later": the per-user and
/// per-project rate/quota limits (legacy `errors[].reason`, and the
/// `google.rpc.ErrorInfo` reason the same responses carry). Gmail answers
/// these with `403`, not `429` — the 2026-09-24 everything-mode incident's
/// `403 ... reasons=[rateLimitExceeded,RATE_LIMIT_EXCEEDED] ... 'Total Query
/// Cost' ... 'Units per minute per user'`.
const GOOGLE_RATE_LIMIT_REASONS: &[&str] = &["rateLimitExceeded", "userRateLimitExceeded", "RATE_LIMIT_EXCEEDED"];

/// Is this failed response a rate limit (retry later) rather than a real
/// failure? `429` always is; `403` is only when the body names one of
/// [`GOOGLE_RATE_LIMIT_REASONS`] — a `403` for missing scopes, a disabled
/// API, or domain policy is NOT, and must keep failing loudly. Pure, so it
/// is unit-testable.
pub(crate) fn google_error_is_rate_limit(status: reqwest::StatusCode, body: &str) -> bool {
    if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        return true;
    }
    if status != reqwest::StatusCode::FORBIDDEN {
        return false;
    }
    let Some(err) = serde_json::from_str::<GoogleApiErrorEnvelope>(body).ok().and_then(|e| e.error) else {
        return false;
    };
    err.errors
        .iter()
        .filter_map(|e| e.reason.as_deref())
        .chain(err.details.iter().filter_map(|d| d.reason.as_deref()))
        .any(|r| GOOGLE_RATE_LIMIT_REASONS.contains(&r))
}

/// Whether an already-formatted failure message ([`google_api_failure`]'s
/// `message`, which lists `reasons=[...]`) names a rate limit. For a caller
/// holding only the [`ConnectorError`] a Gmail call returned, where the
/// response itself is gone: Gmail's `403` rate limit must retry while a
/// `403` for scopes or policy must not (`workers::ingest_receive::transient`).
pub(crate) fn message_names_rate_limit(message: &str) -> bool {
    GOOGLE_RATE_LIMIT_REASONS.iter().any(|r| message.contains(r))
}

/// A consumed non-success Google response: the bounded diagnosis
/// ([`google_api_error_message`]'s text) plus whether it is a rate limit
/// ([`google_error_is_rate_limit`]), read from ONE pass over the body.
pub(crate) struct GoogleApiFailure {
    pub status: reqwest::StatusCode,
    pub message: String,
    pub rate_limited: bool,
}

pub(crate) async fn google_api_failure(what: &str, resp: reqwest::Response) -> GoogleApiFailure {
    let status = resp.status();
    let (detail, rate_limited) = match read_body_bounded(resp, GOOGLE_ERROR_BODY_MAX_BYTES).await {
        Ok(bytes) => {
            let body = String::from_utf8_lossy(&bytes);
            (google_error_detail(&body), google_error_is_rate_limit(status, &body))
        }
        Err(e) => (
            format!("error body unreadable: {}", bounded(&e.to_string(), 120)),
            status == reqwest::StatusCode::TOO_MANY_REQUESTS,
        ),
    };
    GoogleApiFailure {
        status,
        message: format!("{what} failed: {status}: {detail}"),
        rate_limited,
    }
}

/// Read at most `max` bytes of a response body, chunk by chunk, and stop
/// there. The rest of the body is never buffered, so a huge or endless
/// error body costs at most `max` bytes. A cut can land mid-UTF-8-sequence;
/// callers decode with `String::from_utf8_lossy`, which turns the partial
/// tail into U+FFFD instead of panicking.
async fn read_body_bounded(mut resp: reqwest::Response, max: usize) -> Result<Vec<u8>, reqwest::Error> {
    let mut out: Vec<u8> = Vec::with_capacity(max.min(4096));
    while out.len() < max {
        match resp.chunk().await? {
            Some(chunk) => {
                let take = chunk.len().min(max - out.len());
                out.extend_from_slice(&chunk[..take]);
            }
            None => break,
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Endpoints
// ---------------------------------------------------------------------------

const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
/// The user's primary calendar. `calendar.readonly` covers it without needing a
/// separate calendar-list call.
const EVENTS_ENDPOINT: &str =
    "https://www.googleapis.com/calendar/v3/calendars/primary/events";

/// Max events pulled per `upcoming` request. The 24h horizon keeps this small in
/// practice; the cap bounds a pathologically busy calendar.
const MAX_EVENTS: usize = 50;

// ---------------------------------------------------------------------------
// Env helpers — read at call time (connectors are zero-sized + stateless)
// ---------------------------------------------------------------------------

fn client_id() -> Result<String, ConnectorError> {
    std::env::var("GOOGLE_CALENDAR_CLIENT_ID").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_CALENDAR_CLIENT_ID not configured".into())
    })
}

fn client_secret() -> Result<String, ConnectorError> {
    std::env::var("GOOGLE_CALENDAR_CLIENT_SECRET").map_err(|_| {
        ConnectorError::NotConfigured("GOOGLE_CALENDAR_CLIENT_SECRET not configured".into())
    })
}

/// Space-separated scopes. `openid email` label the connection; `calendar.readonly`
/// is the only data scope. Overridable via `GOOGLE_CALENDAR_SCOPES` but the
/// default is already minimal and read-only.
fn scopes() -> String {
    std::env::var("GOOGLE_CALENDAR_SCOPES").unwrap_or_else(|_| {
        "openid email https://www.googleapis.com/auth/calendar.readonly".into()
    })
}

fn redirect_uri(base_url: &str) -> String {
    format!("{base_url}/v1/oauth/google/callback")
}

// ---------------------------------------------------------------------------
// Per-surface scope split (gsuite Train G1 PR 1) — see module doc.
// ---------------------------------------------------------------------------

/// The four Google Workspace surfaces this connector is aware of (gsuite plan
/// decision 1: Gmail + Calendar + Meet + Chat; Drive is explicitly out of
/// scope). Only [`Surface::Calendar`] has any ingest/read code today — the
/// rest are declared so their scope sets are frozen and each surface can be
/// gated independently as Trains G2 (Gmail), G3 (Meet), and G4 (Chat) ship.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Surface {
    /// Shipped: `GET /v1/calendar/upcoming` (`routes/calendar.rs`).
    Calendar,
    /// Declared, INERT until Train G2. Mailbox archetype (contract §7):
    /// `fetch_items`-only, owner's private ocean, no fan-out (C1/C3 absent).
    Gmail,
    /// Declared, INERT until Train G4. Its own archetype — no contract
    /// precedent (see `docs/connectors/google-workspace.md` §3 and gsuite
    /// plan decision 4: spaces = Slack model, DMs/group chats = private
    /// ocean).
    Chat,
    /// Declared, INERT until Train G3. §8 Meetings archetype (platform
    /// artifact; conference records + transcripts).
    Meet,
}

impl Surface {
    /// Every surface, in a stable order — used by tests and by anything that
    /// needs to enumerate the full set (e.g. a future admin "connectors"
    /// listing). `#[allow(dead_code)]`: not called from non-test code yet —
    /// same forward-looking posture as `TokenOwner::Member`'s own
    /// `#[allow(dead_code)]` (`connectors/mod.rs`).
    #[allow(dead_code)]
    pub const ALL: [Surface; 4] = [Surface::Calendar, Surface::Gmail, Surface::Chat, Surface::Meet];

    /// Parse the `?surface=` query param on `GET /v1/oauth/google/authorize`.
    /// Case-sensitive lowercase, matching [`Surface::as_str`] exactly — this
    /// is a fixed, small vocabulary, not free text.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "calendar" => Some(Surface::Calendar),
            "gmail" => Some(Surface::Gmail),
            "chat" => Some(Surface::Chat),
            "meet" => Some(Surface::Meet),
            _ => None,
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Surface::Calendar => "calendar",
            Surface::Gmail => "gmail",
            Surface::Chat => "chat",
            Surface::Meet => "meet",
        }
    }

    /// This surface's `org_connector_flags` provider key (Foundation train PR
    /// 1.3 registry; see `connector_flags`'s module doc for the resolution
    /// contract). Calendar has none — it never gates on this registry, see
    /// [`Surface::is_grandfathered`].
    pub fn flag_provider(&self) -> &'static str {
        match self {
            Surface::Calendar => "google_calendar",
            Surface::Gmail => "google_gmail",
            Surface::Chat => "google_chat",
            Surface::Meet => "google_meet",
        }
    }

    /// Calendar shipped before the per-org connector flag registry existed
    /// and is grandfathered on unconditionally — gating it now would be a
    /// regression for every account already using it, not a rollout control.
    /// The other three surfaces have never shipped anything, so gating them
    /// from day one is free.
    pub fn is_grandfathered(&self) -> bool {
        matches!(self, Surface::Calendar)
    }

    /// Whether this surface's OAuth flow may complete at all yet. Calendar,
    /// Gmail (gsuite Train G2 PR 1) and, as of gsuite Train G4 PR 1 ("G4.1"),
    /// Chat report `true`; Meet stays declared-not-implemented (contract
    /// §5.1's `source_types` stays empty for it — see
    /// `docs/connectors/google-workspace.md`).
    ///
    /// **This flips before any Chat ingest code exists — read that as
    /// deliberate, not premature.** G4.1's whole point is to lift the OAuth
    /// gate (and land the corrected scope set below) BEFORE the first real
    /// member consents to `google_chat`, because amending a frozen scope set
    /// after even one member has connected costs a re-consent for every
    /// connected member plus a Workspace-admin re-allowlist, while amending
    /// it now costs nothing — see `Surface::Chat`'s arm of
    /// [`data_scope`](Self::data_scope) for the amendment itself. A
    /// completed Chat OAuth flow after this PR stores a real per-member
    /// `oauth_tokens` row (`token_provider`/`token_owner_for` below) that
    /// nothing reads yet — `source_types`/`post_store`/
    /// `fetch_items_credentials` gain their Chat arms in gsuite Train G4 PRs
    /// 3/4, not here — so the token just sits there, unread, until then.
    /// That is a safe, inert state precisely because [`token_provider`]
    /// keys it under its own `"google_chat"` literal and
    /// [`token_owner_for`] keys it `Member`-owned: it can never collide
    /// with, or be mistaken for, Calendar's `AccountWide` `"google"` row.
    ///
    /// This is also the gate `routes::oauth::authorize` uses to refuse a
    /// surface's OAuth flow even when its `ORG_GOOGLE_*` flag is on —
    /// `callback` had no per-surface token isolation before G2.1, so letting
    /// an unimplemented surface's flow complete would have silently
    /// overwritten an existing Calendar-scoped token (CodeRabbit #669
    /// finding). Gmail's and Chat's gates could each lift specifically
    /// because `callback` resolves owner + provider key strictly from
    /// [`token_provider`](Self::token_provider) /
    /// [`token_owner_for`](Self::token_owner_for) via the verified OAuth
    /// `state` (never a client param, never `bundle.scope`) — neither
    /// consent can land on the Calendar-scoped row, and per-member storage
    /// means the same is true across two different members of the SAME
    /// account: `routes::oauth::authorize` additionally refuses (400) a Chat
    /// (or Gmail) authorize request whose `state` is not itself a verifiable
    /// signed token embedding a connecting user id, so an unauthenticated or
    /// stale `state` can never complete into a token with no real owner
    /// (`require_verified_surface_state`, `routes::oauth`).
    ///
    /// **Meet flips `true` as of gsuite Train G3 PR 2** — mirroring exactly
    /// how G4.1 flipped Chat: real ingest now reads a stored `google_meet`
    /// token (`workers::google_meet_sweep`, driven by `connectors::
    /// google_meet::sync_owner_once`), so the flip is no longer premature.
    /// That flip did not by itself open any org-plane surface; the org
    /// lift came later and is gated on its own flag:
    /// `routes::oauth::google_org_callback_allowed` has a Meet arm gated on
    /// `org_google_meet_enabled_for` (default OFF), mirroring Gmail's and
    /// Chat's (`docs/connectors/google-workspace.md` §2/§11).
    pub fn is_implemented(&self) -> bool {
        matches!(self, Surface::Calendar | Surface::Gmail | Surface::Chat | Surface::Meet)
    }

    /// The `oauth_tokens.provider` literal this surface's token is stored
    /// under (gsuite Train G2 PR 1, item 1). Calendar keeps the original,
    /// pre-split literal `"google"` — grandfathered, byte-identical, no
    /// migration or backfill needed since `oauth_tokens`/`connector_installs`
    /// already key on a free-text `provider` column. Gmail gets its own
    /// literal so its row can never collide with (or overwrite) Calendar's —
    /// see `callback`'s own doc comment for why that used to be a real
    /// hazard (CodeRabbit #669). Chat gets its own literal too, reachable as
    /// of gsuite Train G4 PR 1 ([`is_implemented`](Self::is_implemented) is
    /// now `true` for it, so `callback` resolves a real token target under
    /// `"google_chat"`). Meet is declared for symmetry with
    /// [`flag_provider`](Self::flag_provider) but stays unreachable
    /// ([`is_implemented`](Self::is_implemented) is `false` for it, so
    /// `callback` never resolves a token target for it).
    pub fn token_provider(&self) -> &'static str {
        match self {
            Surface::Calendar => "google",
            Surface::Gmail => "google_gmail",
            Surface::Chat => "google_chat",
            Surface::Meet => "google_meet",
        }
    }

    /// The [`TokenOwner`](super::TokenOwner) this surface's token is stored
    /// under (gsuite Train G2 PR 1, item 2) — the first call site outside
    /// tests to construct `TokenOwner::Member`. Calendar stays
    /// `AccountWide` HERE, frozen — this is the PERSONAL-plane answer, where
    /// account-wide and per-owner coincide and re-keying an existing
    /// connection's storage would be a needless migration for zero behavior
    /// change (`docs/connectors/google-workspace.md` §5/§7 document this as
    /// deliberate). The ORG plane (Google Calendar org lift, §7.7) is
    /// per-member: use [`calendar_token_owner`](Self::calendar_token_owner),
    /// which is what `routes::oauth::callback` and every Calendar reader
    /// actually call. Gmail — and Chat/Meet, declared for symmetry —
    /// are per-member: `user_id` is the id of whichever CoralSwarm user just
    /// completed the OAuth flow, taken from the verified `state` token's
    /// `uid` claim by `routes::oauth::callback`, never from a client
    /// parameter.
    pub fn token_owner_for(&self, user_id: uuid::Uuid) -> super::TokenOwner {
        match self {
            Surface::Calendar => super::TokenOwner::AccountWide,
            Surface::Gmail | Surface::Chat | Surface::Meet => super::TokenOwner::Member(user_id),
        }
    }

    /// Calendar's token owner by account plane (Google Calendar org lift,
    /// mirroring `routes::oauth::resolve_microsoft_token_target`): the
    /// PERSONAL plane keeps the grandfathered [`TokenOwner::AccountWide`]
    /// row (one owner per personal account, so account-wide and per-owner
    /// coincide, and every existing connection stays byte-identical); an ORG
    /// account stores `TokenOwner::Member(user_id)` under the SAME `"google"`
    /// provider literal — N members share one `account_id` on the org plane,
    /// so an account-wide row would let the newest connect overwrite every
    /// other member's credential, and would sync one member's calendar into
    /// whoever's ocean the account owner resolves to.
    pub fn calendar_token_owner(is_org: bool, user_id: uuid::Uuid) -> super::TokenOwner {
        if is_org {
            super::TokenOwner::Member(user_id)
        } else {
            super::TokenOwner::AccountWide
        }
    }

    /// This surface's frozen, minimal data scope (decision 3) — does NOT
    /// include `openid`/`email`, which [`scopes_for`] always adds regardless
    /// of surface.
    pub fn data_scope(&self) -> &'static str {
        match self {
            // Read-only; unchanged from the connector's original (pre-split)
            // default so Calendar's authorize/refresh path is byte-identical.
            Surface::Calendar => "https://www.googleapis.com/auth/calendar.readonly",
            // Mailbox archetype: read-only. Write/send scopes are never
            // requested by this connector.
            Surface::Gmail => "https://www.googleapis.com/auth/gmail.readonly",
            // Chat spaces archetype, three read-only scopes, amended in
            // gsuite Train G4 PR 1 ("G4.1") — the freeze below dates from
            // THIS PR's merge, not from G1's original (and, it turns out,
            // wrong) reading of the API. What each scope actually buys,
            // verified against Google's primary reference 2026-08-24:
            //   - `chat.spaces.readonly` — space METADATA only: `spaceType`
            //     (the SPACE vs GROUP_CHAT/DIRECT_MESSAGE discriminator),
            //     `historyState`, `spaceThreadingState`, and space-lifecycle
            //     events. It buys no roster at all, despite G1's comment
            //     here previously claiming otherwise.
            //   - `chat.messages.readonly` — message content:
            //     `spaces.messages.list`/`.get`, the `spaces.spaceEvents.list`
            //     catch-up sweep, and the Workspace Events
            //     `message.v1.*` push subscription.
            //   - `chat.memberships.readonly` — the scope G1's original set
            //     was missing: it is the ONLY read-only scope that admits
            //     `spaces.members.list` (roster enumeration, contract C3)
            //     and the Workspace Events `membership.v1.*` subscription
            //     (roster-churn push). Without it, decision 4's roster
            //     auto-seating (contract I4) has no roster to seat FROM —
            //     the whole spaces-are-the-Slack-model half of the Chat
            //     train is undeliverable. Added here, before the OAuth gate
            //     lifts (`Surface::is_implemented`) and therefore before any
            //     member has ever consented to `google_chat`, so this
            //     amendment costs zero re-prompts — see
            //     `.scratch/gsuite-enterprise/g4-chat-design.md` §0 for the
            //     full rationale (founder-approved, gsuite plan decision
            //     record, "Q1").
            //   - `directory.readonly` — added by the chat-per-user-setup
            //     roster-matching PR (2026-09-20), UNLIKE the three scopes
            //     above: `Surface::Chat::is_implemented` had ALREADY flipped
            //     `true` by then (Train G4 PR 1 shipped, Chat connect/spaces
            //     UI is live — #992/#995), so this widening does NOT land
            //     before anyone consented. It costs a real re-prompt for
            //     every member who already granted the three-scope set —
            //     `include_granted_scopes=true` means an already-connected
            //     member keeps their existing grant until they next hit
            //     `authorize_url_for_surface`, at which point Google shows
            //     them the incremental-consent screen for this one new
            //     scope, not a full re-auth. Deliberate and founder-approved
            //     anyway, because without it Chat can only ever identify
            //     colleagues who happened to connect Google to CoralSwarm
            //     already (`resolve_google_identity_in_account`'s `sub`-only
            //     match) — a Workspace *domain directory* lookup
            //     (`people.listDirectoryPeople`, see
            //     `connectors::google_chat`'s "Domain directory" section)
            //     needs this scope and no other; it is user-consented, NOT
            //     the Admin SDK / domain-wide-delegation path decision 3
            //     already rejected. `people.listDirectoryPeople` additionally
            //     requires the customer's Workspace admin to have enabled
            //     "external contact and profile sharing" for the domain — a
            //     precondition this connector does not control and cannot
            //     detect except by the directory call itself coming back
            //     empty, which is why the directory fetch models "empty
            //     result" as `Unavailable`, never as "nobody in this space
            //     has a CoralSwarm account" (`google_chat::fetch_domain_
            //     directory`'s own doc comment).
            Surface::Chat => "https://www.googleapis.com/auth/chat.spaces.readonly \
                 https://www.googleapis.com/auth/chat.messages.readonly \
                 https://www.googleapis.com/auth/chat.memberships.readonly \
                 https://www.googleapis.com/auth/directory.readonly",
            // Meet REST API: conference records, participants, transcript
            // metadata, AND transcript entry TEXT — resolved, not merely
            // declared, by gsuite Train G3 PR 2 (`connectors::google_meet`'s
            // own module doc): verified live against Google's REST reference
            // (`conferenceRecords.transcripts.entries.list`'s own
            // "Authorization scopes" section, 2026-09-07) that
            // `TranscriptEntry.text` is returned under this exact scope
            // alone. No Drive scope needed; decision 1's Drive exclusion is
            // unaffected.
            Surface::Meet => "https://www.googleapis.com/auth/meetings.space.readonly",
        }
    }
}

/// `ORG_GOOGLE_<SURFACE>_ENABLED` env-var reader — the registry's
/// `env_default` argument (Foundation train PR 1.3 shape: env default, row
/// override — see `connector_flags`'s module doc). Same fail-closed parse as
/// `routes::oauth::org_slack_oauth_enabled`: `"1"` or a case-insensitive
/// `"true"` is on, anything else (including unset) is off. Calendar has no
/// env var at all — it is grandfathered on; see [`Surface::is_grandfathered`].
pub(crate) fn surface_env_default(surface: Surface) -> bool {
    if surface.is_grandfathered() {
        return true;
    }
    let var = surface.env_var_name();
    parse_google_surface_flag(std::env::var(var).ok().as_deref())
}

/// The org-plane Calendar flag's env default (`ORG_GOOGLE_CALENDAR_ENABLED`),
/// the registry's `env_default` argument for provider key `google_calendar`
/// ([`Surface::flag_provider`]). **Default OFF**, same fail-closed parse as
/// every sibling flag. Calendar is grandfathered ON for the PERSONAL plane
/// ([`surface_env_default`] returns `true` for it, and `authorize` never
/// consults a flag for it there); this flag governs ONLY whether an ORG
/// account may connect/sync Calendar — see
/// `routes::oauth::org_google_calendar_enabled_for`, its only caller.
pub(crate) fn org_calendar_env_default() -> bool {
    parse_google_surface_flag(std::env::var(Surface::Calendar.env_var_name()).ok().as_deref())
}

/// The `ORG_GOOGLE_<SURFACE>_ENABLED` env var name for a surface. Calendar's
/// is read ONLY by [`org_calendar_env_default`]: [`surface_env_default`]
/// short-circuits to `true` for it ([`Surface::is_grandfathered`]) so the
/// personal plane is untouched by this variable.
impl Surface {
    fn env_var_name(&self) -> &'static str {
        match self {
            Surface::Calendar => "ORG_GOOGLE_CALENDAR_ENABLED",
            Surface::Gmail => "ORG_GOOGLE_GMAIL_ENABLED",
            Surface::Chat => "ORG_GOOGLE_CHAT_ENABLED",
            Surface::Meet => "ORG_GOOGLE_MEET_ENABLED",
        }
    }
}

/// Pure parsing logic behind [`surface_env_default`], factored out so the
/// flag semantics are unit-testable without mutating process-global env
/// state — same convention as `routes::oauth::parse_org_slack_oauth_flag` /
/// `workers::member_ocean_reconciler::parse_enabled_flag`.
fn parse_google_surface_flag(raw: Option<&str>) -> bool {
    matches!(raw, Some(v) if v == "1" || v.eq_ignore_ascii_case("true"))
}

/// Build the full scope string for one surface: `openid email` (identity
/// claims, needed regardless of which data scope accompanies them) plus that
/// surface's own frozen [`Surface::data_scope`].
pub(crate) fn scopes_for(surface: Surface) -> String {
    format!("openid email {}", surface.data_scope())
}

/// Which of [`Surface::Chat`]'s currently-requested data scopes are absent
/// from a stored `oauth_tokens.scope` grant — CS-431 (org-onboarding-from-
/// the-invite.md §3 item 34), the connectors-page re-consent prompt for the
/// exact D17-point-5 gap: `directory.readonly` was added to `data_scope`
/// AFTER real members had already consented to the original three (see that
/// match arm's own doc comment), and nothing before this read
/// `oauth_tokens.scope` for Chat at all.
///
/// Checked against [`Surface::data_scope`] only, not [`scopes_for`]'s
/// `openid email` prefix: those two are requested (and granted) identically
/// on every Google surface a member has ever connected, so they can never be
/// the scope a re-consent prompt needs to name.
///
/// Delegates to [`super::missing_scopes`] for the actual set difference,
/// which already returns `None` (never a guessed `Vec`) for a blank stored
/// scope — the shape a Google token refresh response that omits `scope`
/// would produce if it were ever persisted (see
/// [`GoogleConnector::refresh_if_needed`]'s own doc comment on why Google
/// does not guarantee `scope` on a refresh response). Chat has no
/// legacy-backfill path the way Slack does (every `google_chat` row was
/// written by the one generic OAuth callback, which always records the
/// exchange response's real `scope` string), so the blank case is the only
/// "unknown" this function needs.
pub(crate) fn chat_missing_scopes(stored_scope: &str) -> Option<Vec<String>> {
    let required: Vec<&str> = Surface::Chat.data_scope().split_whitespace().collect();
    super::missing_scopes(stored_scope, &required)
}

/// Build the Google consent URL for one surface — the per-surface analog of
/// [`GoogleConnector::authorize_url`] (which stays Calendar-only, unchanged,
/// for backward compatibility with every existing caller that doesn't pass
/// `?surface=`). Gating (whether the caller may request this surface at all)
/// is the caller's job — see `routes::oauth::authorize` — this function only
/// builds the URL.
pub(crate) fn authorize_url_for_surface(
    base_url: &str,
    oauth_state: &str,
    surface: Surface,
) -> Result<String, ConnectorError> {
    let scopes = scopes_for(surface);
    Ok(super::google_transport::endpoint(&build_authorize_url(&client_id()?, base_url, &scopes, oauth_state)))
}

/// Build the Google consent URL for an arbitrary, already-assembled scope
/// string — the one-button "Connect Google Workspace" flow
/// (`connectors::google_workspace`), whose scope is the UNION of several
/// surfaces' frozen scope sets rather than one surface's. Gating (which
/// surfaces may be in that union) is the caller's job, exactly like
/// [`authorize_url_for_surface`].
pub(crate) fn authorize_url_for_scopes(
    base_url: &str,
    oauth_state: &str,
    scopes: &str,
) -> Result<String, ConnectorError> {
    Ok(super::google_transport::endpoint(&build_authorize_url(&client_id()?, base_url, scopes, oauth_state)))
}

/// Build the Google consent URL (pure — no env reads — so it is parallel-test
/// safe). `access_type=offline` + `prompt=consent` make Google return a refresh
/// token (and re-issue one even if the user previously consented), so offline
/// refresh works on every connect; `include_granted_scopes` keeps incremental
/// auth well-behaved.
fn build_authorize_url(client_id: &str, base_url: &str, scopes: &str, state: &str) -> String {
    format!(
        "{AUTH_ENDPOINT}?client_id={}&redirect_uri={}&response_type=code&scope={}\
         &access_type=offline&prompt=consent&include_granted_scopes=true&state={}",
        urlencoding::encode(client_id),
        urlencoding::encode(&redirect_uri(base_url)),
        urlencoding::encode(scopes),
        urlencoding::encode(state),
    )
}

// ---------------------------------------------------------------------------
// Token response + id_token claims (mirror microsoft.rs)
// ---------------------------------------------------------------------------

/// The body Google's `/token` endpoint returns on a non-2xx (RFC 6749 §5.2
/// shape: `{"error": "...", "error_description": "..."}`).
#[derive(Debug, Default, Deserialize)]
struct GoogleTokenErrorResponse {
    #[serde(default)]
    error: Option<String>,
}

/// Classify a Google `/token` refresh failure into a [`CredentialDeathReason`]
/// — ONLY from the one DEFINITE dead-credential signal Google emits. The
/// Google twin of `connectors::microsoft::classify_microsoft_token_error`,
/// narrower because Google's token endpoint carries no sub-code:
///
///   * **`invalid_grant`** — the refresh token is revoked (the user removed
///     the app at myaccount.google.com, a Workspace admin revoked it, a
///     password change on a sensitive-scope grant) or expired (a testing-
///     mode app's 7-day tokens, six months unused, the 50-token-per-client
///     cap). Every one of these needs the human to re-consent; a retry can
///     never repair it. → [`CredentialDeathReason::GoogleInvalidGrant`].
///   * Left OUT, deliberately: `invalid_client`/`unauthorized_client` (OUR
///     client configuration, not the member's grant), `invalid_request`,
///     5xx, 429 and plain network errors — D16's "could not ask ≠ does not
///     work" (`connector_health`'s migration `0243` header). All stay
///     [`ConnectorError::Exchange`].
pub(crate) fn classify_google_token_error(error: Option<&str>) -> Option<CredentialDeathReason> {
    match error {
        Some("invalid_grant") => Some(CredentialDeathReason::GoogleInvalidGrant),
        _ => None,
    }
}

#[derive(Debug, Deserialize)]
struct GoogleTokenResponse {
    access_token: String,
    /// Google returns a refresh token only on the FIRST consent for a given
    /// scope set (with `access_type=offline&prompt=consent`); refreshes omit it,
    /// so [`refresh_if_needed`](GoogleConnector::refresh_if_needed) preserves the
    /// stored one.
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    id_token: Option<String>,
    #[serde(default)]
    scope: Option<String>,
    /// Seconds until `access_token` expires.
    #[serde(default)]
    expires_in: Option<i64>,
}

/// Claims read out of the (already-trusted, first-party) `id_token`. We do NOT
/// verify the signature — the token came straight from Google's token endpoint
/// over TLS in direct response to our client-secret-authenticated request, so it
/// is only used to label the connection (email + subject), never for authz. Same
/// reasoning as the Microsoft connector.
#[derive(Debug, Deserialize)]
struct IdTokenClaims {
    /// Stable Google account id → `provider_user_id`.
    sub: Option<String>,
    /// The account's email → the human-facing connection label and part of the
    /// `oauth_tokens` unique key (`provider_workspace_id`).
    email: Option<String>,
}

/// Decode the middle (payload) segment of a JWT without signature verification.
fn decode_id_token_claims(id_token: &str) -> Option<IdTokenClaims> {
    let payload = id_token.split('.').nth(1)?;
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload)
        .ok()?;
    serde_json::from_slice(&bytes).ok()
}

fn token_response_to_bundle(r: GoogleTokenResponse) -> TokenBundle {
    let claims = r
        .id_token
        .as_deref()
        .and_then(decode_id_token_claims)
        .unwrap_or(IdTokenClaims {
            sub: None,
            email: None,
        });

    let expires_at = r.expires_in.map(|secs| Utc::now() + Duration::seconds(secs));
    let email = claims.email.unwrap_or_default();

    TokenBundle {
        access_token: r.access_token,
        refresh_token: r.refresh_token,
        user_token: None,
        scope: r.scope.unwrap_or_default(),
        expires_at,
        // Google has no "workspace"; the account email is the stable, human
        // identity and forms the `oauth_tokens` unique key (one row per Google
        // account). The connection label (`/v1/calendar/status`) reads it back.
        provider_workspace_id: email.clone(),
        provider_workspace_name: if email.is_empty() { None } else { Some(email) },
        provider_user_id: claims.sub.unwrap_or_default(),
    }
}

/// Guard an INITIAL connection's identity: `provider_workspace_id` (email) is the
/// `oauth_tokens` row key and `provider_user_id` (sub) anchors the account, both
/// derived from the id_token. A scope override dropping `openid email` (or an
/// undecodable id_token) leaves these empty, which would collide across accounts /
/// overwrite an existing connection — so reject it. Refresh is EXEMPT: it reuses
/// the already-validated stored identity (see `refresh_if_needed`), so it never
/// calls this. Kept as a named function (not an inline check) so tests exercise the
/// real guard rather than a copy of its predicate.
fn ensure_stable_identity(bundle: &TokenBundle) -> Result<(), ConnectorError> {
    if bundle.provider_workspace_id.trim().is_empty() || bundle.provider_user_id.trim().is_empty()
    {
        return Err(ConnectorError::Exchange(
            "Google id_token missing email/sub — cannot establish a stable \
             calendar identity (ensure the 'openid email' scopes are granted)"
                .into(),
        ));
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Connector
// ---------------------------------------------------------------------------

/// The Google (Calendar, read-only) OAuth connector.
pub struct GoogleConnector;

impl GoogleConnector {
    /// Exchange a refresh token for a fresh access token. Google does not rotate
    /// (or re-return) the refresh token on this call, so the caller preserves the
    /// stored one — see [`refresh_if_needed`](Self::refresh_if_needed).
    ///
    /// `revoked_provider` is the `oauth_tokens.provider` literal a definite
    /// dead-credential answer should be raised FOR (`Some("google")` from the
    /// Calendar entry points); `None` leaves even `invalid_grant` as a plain
    /// [`ConnectorError::Exchange`] — see
    /// [`refresh_if_needed_for`](Self::refresh_if_needed_for).
    async fn refresh(
        &self,
        http: &reqwest::Client,
        refresh_token: &str,
        revoked_provider: Option<&'static str>,
    ) -> Result<TokenBundle, ConnectorError> {
        let client_id = client_id()?;
        let client_secret = client_secret()?;
        let resp = google_send(|| {
            http.post(super::google_transport::endpoint(TOKEN_ENDPOINT)).form(&[
                ("grant_type", "refresh_token"),
                ("client_id", client_id.as_str()),
                ("client_secret", client_secret.as_str()),
                ("refresh_token", refresh_token),
            ])
        })
        .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            if let Some(provider) = revoked_provider {
                let err_body: GoogleTokenErrorResponse = serde_json::from_str(&body).unwrap_or_default();
                if let Some(reason) = classify_google_token_error(err_body.error.as_deref()) {
                    tracing::info!(
                        %provider, reason = reason.code(),
                        "google token refresh: classified as a dead credential"
                    );
                    return Err(ConnectorError::CredentialRevoked { provider, reason });
                }
            }
            return Err(ConnectorError::upstream(status, format!(
                "token refresh failed: {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }

        let parsed: GoogleTokenResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("refresh parse: {e}")))?;
        Ok(token_response_to_bundle(parsed))
    }

    /// Refresh the access token in-place if it is expired (or within 60s of it),
    /// recording the new bundle on `creds.refreshed` so the caller can persist it.
    /// Preserves the stored refresh token and account email when Google omits them
    /// from the refresh response (it always does for the refresh token) so the
    /// re-persisted row keeps its identity and can refresh again next time.
    ///
    /// A revoked refresh token (`invalid_grant`) surfaces as a plain
    /// [`ConnectorError::Exchange`] here: one `GoogleConnector` refreshes the
    /// rows of four `oauth_tokens.provider` literals (`google`, `google_gmail`,
    /// `google_chat`, `google_meet`) and `creds` does not say which, so this
    /// method cannot name the row a [`ConnectorError::CredentialRevoked`]
    /// would be recorded against (`credential_health::record_if_revoked`
    /// writes to the provider the error names). Callers that know their row
    /// use [`refresh_if_needed_for`](Self::refresh_if_needed_for).
    pub(crate) async fn refresh_if_needed(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
    ) -> Result<(), ConnectorError> {
        self.refresh_if_needed_as(http, creds, None, Duration::seconds(60)).await
    }

    /// [`refresh_if_needed`](Self::refresh_if_needed) with a caller-chosen
    /// margin: refresh when the token expires within `margin`, not just the
    /// default 60 seconds. For a long-running caller (`workers::
    /// gmail_everything_mode`'s walk runs for a minute or more) a token that
    /// is valid at the start must stay valid to the end — otherwise every
    /// call after the expiry 401s.
    pub(crate) async fn refresh_if_expiring_within(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
        margin: std::time::Duration,
    ) -> Result<(), ConnectorError> {
        let margin = Duration::from_std(margin).unwrap_or(Duration::seconds(60));
        self.refresh_if_needed_as(http, creds, None, margin).await
    }

    /// [`refresh_if_needed`](Self::refresh_if_needed) for a caller that knows
    /// which `oauth_tokens.provider` row `creds` came from: a `400
    /// invalid_grant` becomes `CredentialRevoked { provider, GoogleInvalidGrant }`
    /// so `credential_health::record_if_revoked` can mark exactly that row
    /// and `workers::google_calendar_sync`'s resolver stops sweeping it until
    /// a reconnect (`routes::oauth::store_token_bundle_tx` clears the mark).
    pub(crate) async fn refresh_if_needed_for(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
        provider: &'static str,
    ) -> Result<(), ConnectorError> {
        self.refresh_if_needed_as(http, creds, Some(provider), Duration::seconds(60)).await
    }

    async fn refresh_if_needed_as(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
        revoked_provider: Option<&'static str>,
        margin: Duration,
    ) -> Result<(), ConnectorError> {
        let needs_refresh = creds
            .expires_at
            .map(|exp| exp <= Utc::now() + margin)
            .unwrap_or(false);
        if !needs_refresh {
            return Ok(());
        }
        let Some(refresh_token) = creds.refresh_token.clone() else {
            // No refresh token stored (user connected before offline access, or
            // revoked). Nothing we can do; let the API call surface a 401 the
            // route maps to "reconnect".
            return Ok(());
        };

        let mut bundle = self.refresh(http, &refresh_token, revoked_provider).await?;
        // Google omits the refresh token on refresh — keep the stored one.
        if bundle.refresh_token.is_none() {
            bundle.refresh_token = Some(refresh_token);
        }
        // A refresh may also omit the id_token; preserve the account email so the
        // `oauth_tokens` unique key (account, 'google', email) is stable and we
        // UPDATE the existing row rather than INSERT a duplicate.
        if bundle.provider_workspace_id.is_empty() {
            bundle.provider_workspace_id = creds.provider_workspace_id.clone();
        }
        creds.access_token = bundle.access_token.clone();
        creds.expires_at = bundle.expires_at;
        creds.refreshed = Some(bundle);
        Ok(())
    }

    /// List events between `time_min` and `time_max` on the user's primary
    /// calendar, shaped into [`CalendarEvent`]s. `singleEvents=true` expands
    /// recurring series into individual instances; `orderBy=startTime` returns
    /// them chronologically.
    pub(crate) async fn fetch_upcoming(
        &self,
        http: &reqwest::Client,
        access_token: &str,
        time_min: DateTime<Utc>,
        time_max: DateTime<Utc>,
    ) -> Result<Vec<CalendarEvent>, ConnectorError> {
        let url = format!(
            "{EVENTS_ENDPOINT}?singleEvents=true&orderBy=startTime&maxResults={MAX_EVENTS}\
             &timeMin={}&timeMax={}",
            urlencoding::encode(&time_min.to_rfc3339()),
            urlencoding::encode(&time_max.to_rfc3339()),
        );

        let resp = google_send(|| http.get(super::google_transport::endpoint(&url)).bearer_auth(access_token)).await?;

        if !resp.status().is_success() {
            let status = resp.status();
            // Do NOT include the body — a Google error body can echo event data.
            return Err(ConnectorError::upstream(status, format!(
                "calendar events fetch failed: {status}"
            )));
        }

        let value: serde_json::Value = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("events parse: {e}")))?;
        events_from_json(value)
    }
}

#[async_trait]
impl ConnectorProvider for GoogleConnector {
    fn id(&self) -> &'static str {
        "google"
    }

    fn display_name(&self) -> &'static str {
        "Google Calendar"
    }

    fn source_types(&self) -> &'static [&'static str] {
        // Calendar + Gmail (gsuite Train G2 PR 3) + Chat (gsuite Train G4 PR
        // 4 — spaces ingest) + Meet (gsuite Train G3 PR 2 — transcript
        // fan-out). An empty entry here keeps a connector invisible in STATE
        // entirely (`connectors/mod.rs:365`-`:369`).
        &[
            super::google_calendar::SOURCE_TYPE,
            super::google_gmail::SOURCE_TYPE,
            super::google_chat::SOURCE_TYPE,
            super::google_meet::SOURCE_TYPE,
        ]
    }

    fn authorize_url(&self, base_url: &str, state: &str) -> Result<String, ConnectorError> {
        Ok(super::google_transport::endpoint(&build_authorize_url(&client_id()?, base_url, &scopes(), state)))
    }

    /// Best-effort, provider-specific side effect after the generic
    /// `oauth_tokens` upsert (gsuite Train G3 PR 1): ensure a live Calendar
    /// `events.watch` channel exists for this connection.
    ///
    /// Gated on `provider_key` (gsuite Train G2 PR 1, CodeRabbit round-1
    /// fix), NOT `bundle.scope` content — a scope-content check used to be
    /// sufficient (Calendar was the only completable Google surface, so
    /// `bundle.scope` always meant Calendar), but it stopped being safe the
    /// moment Gmail's OAuth flow became completable too: Google's
    /// incremental consent (`include_granted_scopes=true` on every request,
    /// see this module's own doc §1.1) means a member who already granted
    /// Calendar and now connects Gmail gets a token whose `scope` contains
    /// BOTH surfaces' data scopes, so `bundle.scope.contains(calendar.
    /// readonly)` would misfire on a pure Gmail connect and needlessly
    /// create/renew a Calendar watch channel using the Gmail-flow's own
    /// (still Calendar-capable, from the earlier grant) access token.
    /// `provider_key` is `callback`'s own per-surface literal
    /// ([`Surface::token_provider`]) — exact and unambiguous regardless of
    /// what else the token's `scope` happens to contain. Never fails the
    /// OAuth redirect — errors are logged only (trait contract).
    async fn post_store(
        &self,
        http: &reqwest::Client,
        pool: &sqlx::PgPool,
        account_id: uuid::Uuid,
        bundle: &TokenBundle,
        provider_key: &str,
    ) {
        if provider_key == Surface::Calendar.token_provider() {
            // Google Calendar org lift: an ORG connect stored a per-member
            // row (`Surface::calendar_token_owner`), so the watch channel
            // must be keyed on THAT member. `post_store`'s trait signature
            // carries no user id, so — exactly like Gmail's lookback below —
            // the owner is recovered from the `oauth_tokens` row
            // `routes::oauth::callback` wrote moments earlier in this same
            // request. `None` (no owner on the row) is the unchanged
            // personal account-wide shape. `pool` is ALWAYS the control
            // pool here (callback's own `state.db`), which is where
            // Calendar watch rows live on both planes.
            let owner = match super::google_calendar::resolve_connection_owner(
                pool,
                account_id,
                &bundle.provider_workspace_id,
            )
            .await
            {
                Ok(owner) => owner,
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id,
                        "google calendar: post_store owner lookback failed — skipping watch \
                         bootstrap (non-fatal — workers::google_calendar_sync's next tick retries)"
                    );
                    return;
                }
            };
            let target = super::google_calendar::CalendarTarget { control: pool, content: pool, account_id, owner };
            let key = match owner {
                Some(uid) => format!("google:{account_id}:{uid}"),
                None => format!("google:{account_id}"),
            };
            if let Err(e) = crate::graph_rate_limit::scoped(key, super::google_calendar::ensure_watch(
                http,
                &target,
                &bundle.access_token,
            ))
            .await
            {
                tracing::warn!(
                    error = %e, %account_id,
                    "google calendar: failed to register events.watch channel on connect \
                     (non-fatal — workers::google_calendar_sync's next tick retries, both planes)"
                );
            }
            return;
        }
        if provider_key == Surface::Gmail.token_provider() {
            // gsuite Train G2 PR 3: `post_store` has no `user_id` in its
            // trait signature (see `ConnectorProvider::post_store`'s own
            // doc), so the owner is recovered from the `oauth_tokens` row
            // `routes::oauth::callback` just wrote, moments earlier, in the
            // SAME request — see `google_gmail::resolve_owner_user_id`'s doc.
            let email = bundle.provider_workspace_id.clone();
            let owner_user_id = match super::google_gmail::resolve_owner_user_id(pool, account_id, &email).await {
                Ok(Some(id)) => id,
                Ok(None) => {
                    tracing::warn!(
                        %account_id,
                        "gmail: post_store could not recover the owner_user_id for the \
                         just-stored connection — skipping watch bootstrap (non-fatal, the \
                         first opt-in ingest self-heals it)"
                    );
                    return;
                }
                Err(e) => {
                    tracing::warn!(error = %e, %account_id, "gmail: post_store owner lookback failed");
                    return;
                }
            };
            // `pool` here is ALWAYS the control-plane pool (this method's
            // caller, `routes::oauth::callback`, passes its own `state.db` —
            // never a content/tenant pool), which already satisfies gsuite
            // Train G2 PR 7's single-plane decision for `google_watch_channels`
            // (surface `'gmail'`) without any change to this call — see
            // `google_gmail`'s own module doc, "Watch rows are CONTROL-plane,
            // always".
            // Per Google user (`google_gmail::rate_key`), like every Gmail call.
            let key = super::google_gmail::rate_key(account_id, &email);
            if let Err(e) = crate::graph_rate_limit::scoped(
                key,
                super::google_gmail::ensure_watch(http, pool, account_id, owner_user_id, &email, &bundle.access_token),
            )
            .await
            {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id,
                    "gmail: failed to register users.watch on connect \
                     (non-fatal — the first opt-in ingest / renewal sweep retries)"
                );
            }
            return;
        }
        if provider_key == Surface::Chat.token_provider() {
            // gsuite Train G4 PR 4 (design doc §4.1): one Workspace Events
            // subscription per connected member, targeting
            // `//chat.googleapis.com/spaces/-` — every space AND every DM
            // they belong to, with no churn when they join or leave one.
            //
            // The member's Google `sub` is the subscription row's stable
            // upsert key (§1.2), and it is already in hand: `id_token.sub` →
            // `token_response_to_bundle` → `bundle.provider_user_id`
            // (refused empty at `ensure_stable_identity`). Unlike Gmail —
            // which has to look its owner back up because `post_store`'s
            // trait signature carries no `user_id` — Chat still needs the
            // OWNER for the row's `owner_user_id`, and recovers it the same
            // way, from the `oauth_tokens` row `routes::oauth::callback`
            // wrote moments earlier in this same request. Keyed on the SUB,
            // account-scoped: `resolve_google_identity_in_account`, never
            // the cross-account `resolve_google_identity_by_sub` (#725's M3
            // rule — never call the bootstrap resolver from anything that
            // already holds an `account_id`).
            let sub = bundle.provider_user_id.clone();
            if sub.trim().is_empty() {
                tracing::warn!(
                    %account_id,
                    "google chat: post_store has no provider_user_id (sub) for the just-stored \
                     connection — skipping subscription bootstrap (non-fatal; the first sweep \
                     tick self-heals once a token with an id_token lands)"
                );
                return;
            }
            let owner_user_id =
                match super::google_chat::resolve_google_identity_in_account(pool, account_id, &sub).await {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        tracing::warn!(
                            %account_id,
                            "google chat: post_store could not recover the owner_user_id for the \
                             just-stored connection — skipping subscription bootstrap (non-fatal, \
                             the first sweep tick self-heals it)"
                        );
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %account_id, "google chat: post_store owner lookback failed");
                        return;
                    }
                };
            // `pool` here is ALWAYS the control-plane pool — this method's
            // caller (`routes::oauth::callback`) passes its own `state.db`,
            // never a content/tenant pool — which is what
            // `google_watch_channels` wants, exactly as Gmail's arm above
            // documents.
            let key = format!("google_chat:{account_id}");
            if let Err(e) = crate::graph_rate_limit::scoped(
                key,
                super::google_chat::ensure_subscription(
                    http,
                    pool,
                    account_id,
                    owner_user_id,
                    &sub,
                    &bundle.access_token,
                ),
            )
            .await
            {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id,
                    "google chat: failed to register the Workspace Events subscription on connect \
                     (non-fatal — the sweep worker's own dual call site retries)"
                );
            }
        }
        if provider_key == Surface::Meet.token_provider() {
            // gsuite Train G3 PR 2: bootstrap this member's
            // `google_meet_sync_state` row so `workers::google_meet_sweep`'s
            // discovery query (`resolve_google_meet_sync_due()`) has
            // something to claim. Register transcript-ready push as well;
            // failures are nonfatal because the sweep heals missing watches.
            //
            // Owner recovery mirrors Chat's own arm exactly: `post_store`'s
            // trait signature carries no `user_id`, so the owner is looked
            // back up from the `oauth_tokens` row `routes::oauth::callback`
            // just wrote, moments earlier, in the SAME request — keyed on the
            // SUB (`bundle.provider_user_id`), account-scoped
            // (`resolve_google_identity_in_account`, never the cross-account
            // bootstrap resolver — this caller already holds `account_id`).
            let sub = bundle.provider_user_id.clone();
            if sub.trim().is_empty() {
                tracing::warn!(
                    %account_id,
                    "google meet: post_store has no provider_user_id (sub) for the just-stored \
                     connection — skipping sync-state bootstrap (non-fatal; a later reconnect \
                     with a valid id_token self-heals it)"
                );
                return;
            }
            let owner_user_id =
                match super::google_chat::resolve_google_identity_in_account(pool, account_id, &sub).await {
                    Ok(Some(id)) => id,
                    Ok(None) => {
                        tracing::warn!(
                            %account_id,
                            "google meet: post_store could not recover the owner_user_id for the \
                             just-stored connection — skipping sync-state bootstrap (non-fatal)"
                        );
                        return;
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, %account_id, "google meet: post_store owner lookback failed");
                        return;
                    }
                };
            // `pool` is ALWAYS the control-plane pool here (`routes::oauth::
            // callback`'s own `state.db`) — the same plane
            // `google_meet_sync_state` lives on exclusively (see that
            // migration's own header).
            if let Err(e) = super::google_meet_events::ensure_subscription(http, pool, account_id, owner_user_id, &sub, &bundle.access_token).await {
                tracing::warn!(error = %e, %account_id, %owner_user_id, "Meet subscription registration failed; sweep will retry");
            }
            if let Err(e) = super::google_meet::ensure_sync_state(pool, account_id, owner_user_id).await {
                tracing::warn!(
                    error = %e, %account_id, %owner_user_id,
                    "google meet: failed to bootstrap sync-state row on connect \
                     (non-fatal — a later reconnect retries)"
                );
            }
        }
    }

    /// `true` for the route's gate only: `routes::connectors::trigger_sync`
    /// dispatches Google to `workers::google_calendar_sync`'s locked,
    /// commit-after-ingest passes (the former trait `sync()` here committed
    /// the cursor before the route's own ingest loop and took no lock), so
    /// the trait's default `sync()` is never reached for this provider.
    fn supports_sync(&self) -> bool {
        true
    }

    async fn exchange_code(
        &self,
        http: &reqwest::Client,
        base_url: &str,
        code: &str,
    ) -> Result<TokenBundle, ConnectorError> {
        let client_id = client_id()?;
        let client_secret = client_secret()?;
        let redirect_uri = redirect_uri(base_url);

        let resp = http
            .post(super::google_transport::endpoint(TOKEN_ENDPOINT))
            .timeout(std::time::Duration::from_secs(30))
            .form(&[
                ("grant_type", "authorization_code"),
                ("client_id", &client_id),
                ("client_secret", &client_secret),
                ("code", code),
                ("redirect_uri", &redirect_uri),
            ])
            .send()
            .await
            .map_err(|e| ConnectorError::Exchange(e.to_string()))?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            return Err(ConnectorError::upstream(status, format!(
                "code exchange failed: {status}: {}",
                body.chars().take(300).collect::<String>()
            )));
        }

        let parsed: GoogleTokenResponse = resp
            .json()
            .await
            .map_err(|e| ConnectorError::Exchange(format!("token parse: {e}")))?;
        let bundle = token_response_to_bundle(parsed);
        // Reject an initial connection with no stable identity. Factored into
        // `ensure_stable_identity` so the guard is exercised directly by tests
        // (a duplicated predicate in the test wouldn't catch weakening this rule).
        ensure_stable_identity(&bundle)?;
        Ok(bundle)
    }

    /// gsuite Train G2 PR 3: Gmail's per-thread opt-in path
    /// (`docs/connectors/CONNECTOR_CONTRACT.md`'s Mailbox archetype) —
    /// `supports_sync` stays Calendar's own `true` above, deliberately NOT
    /// consulted for Gmail (contract §4: bulk mailbox pull is never allowed;
    /// only explicit item ingest reaches this path). Chat/Meet stay inert
    /// (Trains G4/G3 respectively).
    fn supports_fetch_items(&self) -> bool {
        true
    }

    /// Which `(oauth_tokens.provider, TokenOwner)` `routes::connectors::
    /// ingest_selected` should load/persist Gmail credentials under — see
    /// `ConnectorProvider::fetch_items_credentials`'s own doc for why this
    /// can't be `(self.id(), AccountWide)` the way every other connector's
    /// default is. The AccountWide-never-for-Gmail invariant (design doc PR
    /// G2.3 item 3) lives HERE, structurally: there is no other code path in
    /// this connector that can select `TokenOwner::AccountWide` for a Gmail
    /// credential load.
    fn fetch_items_credentials(&self, user_id: uuid::Uuid) -> (&'static str, super::TokenOwner) {
        (Surface::Gmail.token_provider(), Surface::Gmail.token_owner_for(user_id))
    }

    /// `threads.get(format=full)` per opened message id, whole-thread expand
    /// — delegates to `google_gmail::fetch_items`, which mirrors
    /// `microsoft::MicrosoftConnector::fetch_items`'s dedup shape exactly.
    /// See that module's own doc for the full design.
    ///
    /// No `graph_rate_limit::scoped` call here (CodeRabbit round 1 finding
    /// #4 on PR #686): `routes::connectors::ingest_selected` already opens
    /// one scope, `"{provider_key}:{account_id}"`
    /// (`"google_gmail:{account_id}"` for this connector — see
    /// `ConnectorProvider::fetch_items_credentials`'s own doc), around its
    /// ENTIRE call to `fetch_items` + `finish_ingest` (which is what
    /// eventually calls [`after_ingest`](Self::after_ingest) below too). A
    /// second, INNER `scoped()` call here would not layer on top of that —
    /// `graph_rate_limit::scoped`'s task-local key SHADOWS for the duration
    /// of the future it wraps — so this method inherits the caller's key.
    /// (An early version re-keyed on the raw mailbox email here, and
    /// collapsed to the single shared key `"google_gmail:"` whenever
    /// `provider_workspace_id` was empty.)
    ///
    /// That generic, user-initiated path is the one Gmail caller still keyed
    /// per CoralSwarm account. The push path, the watch setup and renewal,
    /// and the everything-mode worker key per Google USER
    /// (`google_gmail::rate_key`: a hash of the mailbox email, falling back
    /// to the account when there is none), because Gmail's quota is per user
    /// and one mailbox can be connected from two accounts.
    async fn fetch_items(
        &self,
        http: &reqwest::Client,
        creds: &mut ConnectorCredentials,
        item_ids: &[String],
        scope: super::IngestScope,
    ) -> Result<Vec<super::NormalizedItem>, ConnectorError> {
        self.refresh_if_needed(http, creds).await?;
        super::google_gmail::fetch_items(http, &creds.access_token, item_ids, scope).await
    }

    /// Register every distinct thread just ingested for live-follow, and
    /// ensure a live Gmail watch exists (self-heal) — delegates to
    /// `google_gmail::after_ingest`, which mirrors `microsoft::
    /// MicrosoftConnector::after_ingest`'s shape exactly. `creds.
    /// provider_workspace_id` is the connected mailbox's email
    /// (`token_response_to_bundle`'s own mapping, this module's doc §"What
    /// it powers") — the `resource_id` Gmail's watch rows are keyed on.
    ///
    /// No `graph_rate_limit::scoped` call here either — same reasoning as
    /// [`fetch_items`](Self::fetch_items) above: `after_ingest` is only ever
    /// reached via `routes::connectors::finish_ingest`, itself only ever
    /// reached from inside `trigger_sync`'s/`ingest_selected`'s own
    /// already-open scope.
    async fn after_ingest(
        &self,
        http: &reqwest::Client,
        pool: &sqlx::PgPool,
        account_id: uuid::Uuid,
        user_id: uuid::Uuid,
        graph_id: uuid::Uuid,
        _owner: super::TokenOwner,
        creds: &ConnectorCredentials,
        items: &[super::NormalizedItem],
        control_pool: Option<&sqlx::PgPool>,
        follow: bool,
        follow_since: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), ConnectorError> {
        // `_owner` (m365 enterprise train PR 3.3): unused here — Gmail's own
        // per-member isolation is unconditional and independent of this
        // parameter (`fetch_items_credentials` always resolves
        // `TokenOwner::Member(user_id)`, no `AccountWide` fallback to
        // disambiguate — gsuite Train G2 PR 3), so `user_id` below already IS
        // the real owner; nothing downstream needs it restated.
        //
        // `pool` is the CONTENT plane (the org's tenant pool under Org,
        // unchanged); `control_pool` (gsuite Train G2 PR 7) is the CONTROL
        // plane `google_watch_channels` (surface `'gmail'`) now always lives
        // on — see `google_gmail`'s own module doc, "Watch rows are
        // CONTROL-plane, always". `gmail_watched_threads` stays on `pool`.
        super::google_gmail::after_ingest(
            http,
            control_pool,
            pool,
            account_id,
            user_id,
            &creds.provider_workspace_id,
            &creds.access_token,
            graph_id,
            items,
            follow,
            follow_since,
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Public event shape (the `/v1/calendar/upcoming` contract)
// ---------------------------------------------------------------------------

/// One attendee on a calendar event, for speaker-name suggestions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct CalendarAttendee {
    /// Display name when the provider has one; else `None` (client falls back to
    /// the email local-part).
    pub name: Option<String>,
    pub email: String,
}

/// A shaped calendar event returned by `GET /v1/calendar/upcoming`.
#[derive(Debug, Clone, Serialize)]
pub struct CalendarEvent {
    /// Provider-native event id (stable across polls; used for notification
    /// dedupe on the client).
    pub id: String,
    pub title: String,
    pub start: DateTime<Utc>,
    /// End time when known (absent for malformed events); RFC3339 on the wire.
    pub end: Option<DateTime<Utc>>,
    /// True for an all-day / date-only event (Google supplies `date`, not
    /// `dateTime`). `start` is then that date at UTC midnight — NOT a real meeting
    /// time — so clients must exclude all-day events from the pre-meeting nudge and
    /// live-meeting matching (a 00:00 nudge for an all-day event would be wrong).
    pub all_day: bool,
    pub attendees: Vec<CalendarAttendee>,
    /// Zoom/Meet/Teams join URL parsed from the event, when present.
    pub meeting_url: Option<String>,
}

// ---------------------------------------------------------------------------
// Google Calendar API raw shapes + pure parsing (unit-tested, no HTTP)
// ---------------------------------------------------------------------------

/// `pub(crate)` (gsuite Train G3 PR 1): the ingest path in `google_calendar.rs`
/// deserializes `events.list` responses (both the full-backfill and
/// incremental/`syncToken` shapes) directly into this, so it needs the two
/// pagination/sync-cursor fields `fetch_upcoming`'s single-page call never
/// reads (`nextPageToken`/`nextSyncToken`) — adding them here is additive and
/// doesn't change `fetch_upcoming`'s existing (unpaginated, `singleEvents=true`)
/// behavior at all.
#[derive(Debug, Deserialize)]
pub(crate) struct GEventsList {
    #[serde(default)]
    pub(crate) items: Vec<GEvent>,
    /// Present when more pages remain (either a full-backfill pass or a
    /// syncToken pass that hasn't reached its terminal page yet).
    #[serde(default, rename = "nextPageToken")]
    pub(crate) next_page_token: Option<String>,
    /// Present ONLY on the terminal page of a listing (full or incremental) —
    /// the new incremental-sync cursor to persist for next time.
    #[serde(default, rename = "nextSyncToken")]
    pub(crate) next_sync_token: Option<String>,
}

/// `pub(crate)` (gsuite Train G3 PR 1) — reused by `google_calendar.rs` so
/// event parsing/shaping (cancellation status, id, attendees, meeting-URL
/// extraction) is written once and shared between the read-only `/upcoming`
/// route and calendar ingest, rather than forked into two copies that could
/// silently drift.
#[derive(Debug, Deserialize)]
pub(crate) struct GEvent {
    pub(crate) id: Option<String>,
    pub(crate) summary: Option<String>,
    pub(crate) start: Option<GEventTime>,
    pub(crate) end: Option<GEventTime>,
    #[serde(default)]
    pub(crate) attendees: Vec<GAttendee>,
    #[serde(rename = "hangoutLink")]
    pub(crate) hangout_link: Option<String>,
    #[serde(rename = "conferenceData")]
    pub(crate) conference_data: Option<GConferenceData>,
    pub(crate) location: Option<String>,
    pub(crate) description: Option<String>,
    /// "cancelled" events still appear when a recurring instance is removed; we
    /// drop them so a nudge never fires for a cancelled meeting. Ingest
    /// (`google_calendar.rs`) uses the same field to detect a deletion.
    #[serde(default)]
    pub(crate) status: Option<String>,
    /// Set on an EXCEPTION instance of a recurring series (a single occurrence
    /// that was individually edited or cancelled) — points back at the master
    /// event's id. Unread by `fetch_upcoming` (which expands via
    /// `singleEvents=true` and never sees exceptions as a distinct concept);
    /// deserialized by ingest, which lists with `singleEvents=false` and
    /// therefore DOES see master + exception rows (see `google_calendar.rs`'s
    /// module doc for the recurring-event design) — kept on the struct as
    /// documentation of that shape and for a future correction/dedup pass
    /// (`#[allow(dead_code)]`: not programmatically consumed yet, same
    /// forward-looking posture as `TokenOwner::Member`'s own attribute in
    /// `connectors/mod.rs` — both fields/variants exist because the shape
    /// they describe is real, not because something reads them today).
    #[allow(dead_code)]
    #[serde(default, rename = "recurringEventId")]
    pub(crate) recurring_event_id: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GEventTime {
    #[serde(rename = "dateTime")]
    pub(crate) date_time: Option<String>,
    /// All-day events carry `date` (YYYY-MM-DD) instead of `dateTime`.
    pub(crate) date: Option<String>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GAttendee {
    pub(crate) email: Option<String>,
    #[serde(rename = "displayName")]
    pub(crate) display_name: Option<String>,
    /// Meeting rooms / equipment are attendees with `resource=true`; excluded
    /// from the human roster.
    #[serde(default)]
    pub(crate) resource: bool,
}

#[derive(Debug, Deserialize)]
pub(crate) struct GConferenceData {
    #[serde(default, rename = "entryPoints")]
    entry_points: Vec<GEntryPoint>,
}

#[derive(Debug, Deserialize)]
struct GEntryPoint {
    #[serde(rename = "entryPointType")]
    entry_point_type: Option<String>,
    uri: Option<String>,
}

/// Parse a Google event time. Prefers `dateTime` (RFC3339 with offset); falls
/// back to an all-day `date` interpreted as UTC midnight. `pub(crate)`: reused
/// by `google_calendar.rs`'s ingest shaping (gsuite Train G3 PR 1).
pub(crate) fn parse_g_time(t: Option<&GEventTime>) -> Option<DateTime<Utc>> {
    let t = t?;
    if let Some(dt) = t.date_time.as_deref() {
        if let Ok(parsed) = DateTime::parse_from_rfc3339(dt) {
            return Some(parsed.with_timezone(&Utc));
        }
    }
    if let Some(d) = t.date.as_deref() {
        if let Ok(date) = NaiveDate::parse_from_str(d, "%Y-%m-%d") {
            return date.and_hms_opt(0, 0, 0).map(|ndt| ndt.and_utc());
        }
    }
    None
}

/// Whether an event time is all-day / date-only (Google supplies `date` and no
/// `dateTime`). Such an event's `start` is a bare date at UTC midnight, not a
/// meeting time, so it must be kept out of the nudge / live matching.
pub(crate) fn is_all_day(t: Option<&GEventTime>) -> bool {
    t.map(|t| t.date_time.is_none() && t.date.is_some())
        .unwrap_or(false)
}

/// Hosts we recognise as video-meeting providers. A candidate URL is accepted
/// only when its PARSED host is exactly one of these or a subdomain of one — never
/// a substring match — so `https://zoom.us.evil.example/` and
/// `https://evil.example/zoom.us` are both rejected.
const MEETING_HOSTS: &[&str] = &[
    "zoom.us",
    "meet.google.com",
    "teams.microsoft.com",
    "teams.live.com",
    "whereby.com",
    "webex.com",
];

/// Validate one candidate join URL: require the `https` scheme and a host that is
/// an allowed meeting host or a subdomain of one, matched on the parsed hostname
/// (not a substring test). Returns the trimmed URL on success. A malicious invite
/// therefore cannot smuggle a phishing link through "Join".
fn validated_meeting_url(candidate: &str) -> Option<String> {
    let trimmed = candidate.trim();
    let url = reqwest::Url::parse(trimmed).ok()?;
    if url.scheme() != "https" {
        return None;
    }
    let host = url.host_str()?.to_ascii_lowercase();
    let allowed = MEETING_HOSTS
        .iter()
        .any(|h| host == *h || host.ends_with(&format!(".{h}")));
    allowed.then(|| trimmed.to_string())
}

/// Extract the first `https://` token from `text` whose parsed host is a known
/// meeting provider. The token ends at the first whitespace or common delimiter so
/// surrounding prose is not captured; each candidate is then hostname-validated.
pub(crate) fn find_meeting_link_in_text(text: &str) -> Option<String> {
    for (idx, _) in text.match_indices("https://") {
        let tail = &text[idx..];
        let end = tail
            .find(|c: char| c.is_whitespace() || matches!(c, '"' | '\'' | '<' | '>' | ')' | ']'))
            .unwrap_or(tail.len());
        if let Some(url) = validated_meeting_url(&tail[..end]) {
            return Some(url);
        }
    }
    None
}

/// Derive the join URL for an event, in priority order: Google Meet
/// (`hangoutLink`), a conference video entry point, then a link scanned from the
/// location or description (Zoom/Teams/etc.). Every candidate — including the
/// Google-supplied ones — is hostname-validated by [`validated_meeting_url`].
/// `pub(crate)`: reused by `google_calendar.rs`'s ingest shaping.
pub(crate) fn extract_meeting_url(ev: &GEvent) -> Option<String> {
    if let Some(link) = ev.hangout_link.as_deref().and_then(validated_meeting_url) {
        return Some(link);
    }
    if let Some(cd) = ev.conference_data.as_ref() {
        for ep in &cd.entry_points {
            if ep.entry_point_type.as_deref() == Some("video") {
                if let Some(link) = ep.uri.as_deref().and_then(validated_meeting_url) {
                    return Some(link);
                }
            }
        }
    }
    if let Some(link) = ev.location.as_deref().and_then(find_meeting_link_in_text) {
        return Some(link);
    }
    if let Some(link) = ev.description.as_deref().and_then(find_meeting_link_in_text) {
        return Some(link);
    }
    None
}

/// Shape a raw Google Calendar `events.list` response into [`CalendarEvent`]s.
/// Drops events without an id or a parseable start, and cancelled instances.
fn events_from_json(value: serde_json::Value) -> Result<Vec<CalendarEvent>, ConnectorError> {
    let list: GEventsList = serde_json::from_value(value)
        .map_err(|e| ConnectorError::Exchange(format!("events decode: {e}")))?;

    let mut out = Vec::with_capacity(list.items.len());
    for ev in list.items {
        if ev.status.as_deref() == Some("cancelled") {
            continue;
        }
        let Some(id) = ev.id.clone() else { continue };
        let Some(start) = parse_g_time(ev.start.as_ref()) else {
            continue;
        };
        let all_day = is_all_day(ev.start.as_ref());
        let end = parse_g_time(ev.end.as_ref());
        let meeting_url = extract_meeting_url(&ev);

        let title = ev
            .summary
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .unwrap_or("(no title)")
            .to_string();

        let attendees = ev
            .attendees
            .into_iter()
            .filter(|a| !a.resource)
            .filter_map(|a| {
                a.email.filter(|e| !e.trim().is_empty()).map(|email| {
                    CalendarAttendee {
                        name: a
                            .display_name
                            .map(|n| n.trim().to_string())
                            .filter(|n| !n.is_empty()),
                        email,
                    }
                })
            })
            .collect();

        out.push(CalendarEvent {
            id,
            title,
            start,
            end,
            all_day,
            attendees,
            meeting_url,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Tests — pure parsing + shaping (no live HTTP, mirroring oauth/slack.rs)
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    /// A real-shaped Gmail `users.messages.list` 403 with both the legacy
    /// `errors[]` and the `google.rpc` `details[]` ErrorInfo.
    const GMAIL_403_SERVICE_DISABLED: &str = r#"{
      "error": {
        "code": 403,
        "message": "Gmail API has not been used in project 123456789 before or it is disabled. Enable it by visiting https://console.developers.google.com/apis/api/gmail.googleapis.com/overview?project=123456789 then retry.",
        "errors": [
          { "message": "Gmail API has not been used ...", "domain": "usageLimits", "reason": "accessNotConfigured", "extendedHelp": "https://console.developers.google.com" }
        ],
        "status": "PERMISSION_DENIED",
        "details": [
          { "@type": "type.googleapis.com/google.rpc.ErrorInfo", "reason": "SERVICE_DISABLED", "domain": "googleapis.com",
            "metadata": { "service": "gmail.googleapis.com", "consumer": "projects/123456789" } },
          { "@type": "type.googleapis.com/google.rpc.Help", "links": [ { "description": "Google developers console API activation", "url": "https://console.developers.google.com" } ] }
        ]
      }
    }"#;

    #[test]
    fn google_error_detail_keeps_reasons_status_code_and_message() {
        let d = google_error_detail(GMAIL_403_SERVICE_DISABLED);
        assert!(d.starts_with("reasons=[accessNotConfigured,SERVICE_DISABLED]"), "{d}");
        assert!(d.contains("status=PERMISSION_DENIED"), "{d}");
        assert!(d.contains("code=403"), "{d}");
        assert!(d.contains("message=\"Gmail API has not been used in project 123456789"), "{d}");
        assert!(d.chars().count() <= GOOGLE_ERROR_DETAIL_MAX_CHARS, "{d}");
    }

    #[test]
    fn google_error_detail_dedupes_reasons_and_handles_rate_limit_shape() {
        let body = r#"{"error":{"code":403,"message":"User-rate limit exceeded",
            "errors":[{"reason":"userRateLimitExceeded"},{"reason":"userRateLimitExceeded"}],
            "status":"PERMISSION_DENIED"}}"#;
        assert_eq!(
            google_error_detail(body),
            "reasons=[userRateLimitExceeded] status=PERMISSION_DENIED code=403 message=\"User-rate limit exceeded\""
        );
    }

    #[test]
    fn rate_limit_is_429_or_a_403_naming_a_rate_limit_reason() {
        use reqwest::StatusCode;
        // The prod 2026-09-24 body shape: legacy reason + ErrorInfo reason.
        let quota = r#"{"error":{"code":403,"message":"Quota exceeded for quota metric 'Total Query Cost'",
            "errors":[{"reason":"rateLimitExceeded"}],"status":"PERMISSION_DENIED",
            "details":[{"@type":"type.googleapis.com/google.rpc.ErrorInfo","reason":"RATE_LIMIT_EXCEEDED"}]}}"#;
        assert!(google_error_is_rate_limit(StatusCode::FORBIDDEN, quota));
        let user = r#"{"error":{"code":403,"errors":[{"reason":"userRateLimitExceeded"}]}}"#;
        assert!(google_error_is_rate_limit(StatusCode::FORBIDDEN, user));
        let info_only = r#"{"error":{"code":403,"details":[{"reason":"RATE_LIMIT_EXCEEDED"}]}}"#;
        assert!(google_error_is_rate_limit(StatusCode::FORBIDDEN, info_only));
        assert!(google_error_is_rate_limit(StatusCode::TOO_MANY_REQUESTS, ""));

        // A 403 that is NOT a rate limit keeps failing loudly.
        assert!(!google_error_is_rate_limit(StatusCode::FORBIDDEN, GMAIL_403_SERVICE_DISABLED));
        let scope = r#"{"error":{"code":403,"errors":[{"reason":"insufficientPermissions"}]}}"#;
        assert!(!google_error_is_rate_limit(StatusCode::FORBIDDEN, scope));
        assert!(!google_error_is_rate_limit(StatusCode::FORBIDDEN, "<html>nope</html>"));
        // The reason only counts on a 403.
        assert!(!google_error_is_rate_limit(StatusCode::BAD_REQUEST, user));
    }

    #[test]
    fn google_error_detail_truncates_a_long_message() {
        let long = "x".repeat(5000);
        let body = format!(r#"{{"error":{{"code":403,"message":"{long}","errors":[{{"reason":"domainPolicy"}}]}}}}"#);
        let d = google_error_detail(&body);
        assert!(d.contains("reasons=[domainPolicy]"), "{d}");
        assert!(d.contains('…'), "a cut message must be marked: {d}");
        assert!(d.chars().count() <= GOOGLE_ERROR_DETAIL_MAX_CHARS, "{}", d.chars().count());
        // The message itself is capped well below the overall bound.
        assert!(d.matches('x').count() < GOOGLE_ERROR_MESSAGE_MAX_CHARS, "{d}");
    }

    #[test]
    fn google_error_detail_never_echoes_a_non_json_body() {
        let html = "<html><body>secret-looking ya29.SHOULD_NOT_APPEAR</body></html>";
        let d = google_error_detail(html);
        assert_eq!(d, format!("non-JSON error body ({} bytes)", html.len()));
        assert!(!d.contains("ya29"));
        assert_eq!(google_error_detail("  "), "empty error body");
        assert_eq!(google_error_detail(r#"{"error":{}}"#), "JSON error body with no recognised fields");
    }

    #[test]
    fn google_error_detail_truncates_a_multibyte_message_without_panicking() {
        let body = format!(
            r#"{{"error":{{"code":403,"message":"{}","errors":[{{"reason":"rateLimitExceeded"}}]}}}}"#,
            "é".repeat(300)
        );
        let d = google_error_detail(&body);
        assert!(d.starts_with("reasons=[rateLimitExceeded]"), "{d}");
        assert!(d.contains('…'), "{d}");
        assert!(d.chars().count() <= GOOGLE_ERROR_DETAIL_MAX_CHARS, "{}", d.chars().count());
    }

    #[tokio::test]
    async fn read_body_bounded_stops_at_the_cap_across_chunks() {
        // Three chunks totalling well past the cap; the reader must stop at
        // exactly `max` bytes and never pull the third chunk.
        let chunks: Vec<Result<Vec<u8>, std::io::Error>> =
            vec![Ok(vec![b'a'; 10]), Ok(vec![b'b'; 10]), Ok(vec![b'c'; 10])];
        let body = reqwest::Body::wrap_stream(futures::stream::iter(chunks));
        let resp = reqwest::Response::from(axum::http::Response::builder().status(403).body(body).unwrap());
        let out = read_body_bounded(resp, 15).await.unwrap();
        assert_eq!(out, [vec![b'a'; 10], vec![b'b'; 5]].concat());

        // Under the cap: the whole body, nothing invented.
        let resp = reqwest::Response::from(axum::http::Response::builder().status(403).body("short").unwrap());
        assert_eq!(read_body_bounded(resp, 15).await.unwrap(), b"short");
    }

    #[tokio::test]
    async fn google_api_error_survives_a_cut_inside_a_multibyte_char() {
        // A body larger than the read cap whose cut lands mid-'é': lossy
        // decode, no panic, reported as a non-JSON body (the envelope is
        // incomplete), never echoed.
        let big = "é".repeat(GOOGLE_ERROR_BODY_MAX_BYTES); // 2 bytes each
        let resp = reqwest::Response::from(
            axum::http::Response::builder().status(403).body(format!("x{big}")).unwrap(),
        );
        let s = google_api_error_message("gmail messages.list", resp).await;
        assert!(s.starts_with("gmail messages.list failed: 403 Forbidden: non-JSON error body ("), "{s}");
        assert!(!s.contains('é'));
    }

    #[tokio::test]
    async fn google_api_error_carries_the_403_reason_and_message_into_the_error_string() {
        let http_resp = axum::http::Response::builder()
            .status(403)
            .header("content-type", "application/json")
            .body(GMAIL_403_SERVICE_DISABLED.to_string())
            .unwrap();
        let err = google_api_error("gmail messages.list", reqwest::Response::from(http_resp)).await;
        let s = err.to_string();
        assert!(matches!(err, ConnectorError::UpstreamStatus { status: 403, .. }), "the status is a field: {err:?}");
        assert!(s.starts_with("upstream exchange failed: gmail messages.list failed: 403 Forbidden: "), "{s}");
        assert!(s.contains("SERVICE_DISABLED") && s.contains("accessNotConfigured"), "{s}");
        assert!(s.contains("Gmail API has not been used"), "{s}");
        // Must survive `mark_failed_no_progress`'s 500-char `last_error` cap
        // intact, so the reason is never the part that gets cut.
        assert!(s.chars().count() <= 500, "{}", s.chars().count());
    }

    #[test]
    fn authorize_url_is_implemented_never_not_implemented() {
        // Google is a real connector, not a stub. `authorize_url` either succeeds
        // (client id configured) or reports NotConfigured — NEVER NotImplemented.
        // Asserting the *absence* of NotImplemented is env-independent, so this
        // test mutates no process-global state and can't race others (env-var
        // reads race across the process — see routes::oauth notes).
        let r = GoogleConnector.authorize_url("http://localhost:8080", "state-token");
        assert!(
            !matches!(r, Err(ConnectorError::NotImplemented(_))),
            "google must be implemented, got {r:?}"
        );
    }

    #[test]
    fn authorize_url_is_google_consent_with_offline_access() {
        // Pure builder — no env reads, so parallel-test safe (env-var reads race
        // across the process; see routes::oauth notes on the same hazard).
        let url = build_authorize_url(
            "test-client.apps.googleusercontent.com",
            "https://api.example.com",
            "openid email https://www.googleapis.com/auth/calendar.readonly",
            "st8",
        );
        assert!(url.starts_with(AUTH_ENDPOINT), "{url}");
        assert!(url.contains("access_type=offline"), "{url}");
        assert!(url.contains("prompt=consent"), "{url}");
        assert!(url.contains("state=st8"), "{url}");
        // redirect must point back at our callback for this provider
        assert!(
            url.contains(
                &urlencoding::encode("https://api.example.com/v1/oauth/google/callback")
                    .into_owned()
            ),
            "{url}"
        );
        // calendar.readonly scope present; no write scope
        assert!(url.contains("calendar.readonly"), "{url}");
    }

    #[test]
    fn id_token_claims_decode_email_and_sub() {
        // Build a fake JWT: header.payload.signature (payload base64url, unsigned).
        let payload = serde_json::json!({ "sub": "1234567890", "email": "user@example.com" });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let jwt = format!("eyJhbGciOiJSUzI1NiJ9.{b64}.sig");
        let claims = decode_id_token_claims(&jwt).unwrap();
        assert_eq!(claims.email.as_deref(), Some("user@example.com"));
        assert_eq!(claims.sub.as_deref(), Some("1234567890"));
    }

    #[test]
    fn token_response_maps_email_to_workspace_id() {
        let payload = serde_json::json!({ "sub": "sub-1", "email": "a@b.com" });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let r = GoogleTokenResponse {
            access_token: "at".into(),
            refresh_token: Some("rt".into()),
            id_token: Some(format!("h.{b64}.s")),
            scope: Some("openid email https://www.googleapis.com/auth/calendar.readonly".into()),
            expires_in: Some(3600),
        };
        let bundle = token_response_to_bundle(r);
        assert_eq!(bundle.provider_workspace_id, "a@b.com");
        assert_eq!(bundle.provider_workspace_name.as_deref(), Some("a@b.com"));
        assert_eq!(bundle.provider_user_id, "sub-1");
        assert_eq!(bundle.refresh_token.as_deref(), Some("rt"));
        assert!(bundle.expires_at.is_some());
    }

    #[test]
    fn parse_g_time_prefers_rfc3339_datetime() {
        let t = GEventTime {
            date_time: Some("2026-07-15T10:00:00-07:00".into()),
            date: None,
        };
        let dt = parse_g_time(Some(&t)).unwrap();
        // 10:00 -07:00 == 17:00 UTC
        assert_eq!(dt.to_rfc3339(), "2026-07-15T17:00:00+00:00");
    }

    #[test]
    fn parse_g_time_handles_all_day_date() {
        let t = GEventTime {
            date_time: None,
            date: Some("2026-07-15".into()),
        };
        let dt = parse_g_time(Some(&t)).unwrap();
        assert_eq!(dt.to_rfc3339(), "2026-07-15T00:00:00+00:00");
    }

    #[test]
    fn meeting_url_prefers_hangout_link() {
        let ev: GEvent = serde_json::from_value(serde_json::json!({
            "id": "e1",
            "hangoutLink": "https://meet.google.com/abc-defg-hij",
            "location": "https://zoom.us/j/999",
        }))
        .unwrap();
        assert_eq!(
            extract_meeting_url(&ev).as_deref(),
            Some("https://meet.google.com/abc-defg-hij")
        );
    }

    #[test]
    fn meeting_url_from_conference_video_entry_point() {
        let ev: GEvent = serde_json::from_value(serde_json::json!({
            "id": "e1",
            "conferenceData": {
                "entryPoints": [
                    { "entryPointType": "phone", "uri": "tel:+1-555" },
                    { "entryPointType": "video", "uri": "https://meet.google.com/xyz" }
                ]
            }
        }))
        .unwrap();
        assert_eq!(
            extract_meeting_url(&ev).as_deref(),
            Some("https://meet.google.com/xyz")
        );
    }

    #[test]
    fn meeting_url_scanned_from_description_zoom() {
        let ev: GEvent = serde_json::from_value(serde_json::json!({
            "id": "e1",
            "description": "Join here: https://us02web.zoom.us/j/123456789?pwd=abc please be on time",
        }))
        .unwrap();
        assert_eq!(
            extract_meeting_url(&ev).as_deref(),
            Some("https://us02web.zoom.us/j/123456789?pwd=abc")
        );
    }

    #[test]
    fn meeting_url_none_for_non_meeting_link() {
        let ev: GEvent = serde_json::from_value(serde_json::json!({
            "id": "e1",
            "location": "Conference Room B — see https://wiki.internal/agenda",
        }))
        .unwrap();
        assert_eq!(extract_meeting_url(&ev), None);
    }

    #[test]
    fn meeting_url_rejects_lookalike_hosts() {
        // Substring matching would accept these; hostname parsing must reject them.
        for evil in [
            "https://zoom.us.evil.example/j/1",       // suffixed host
            "https://evil.example/zoom.us",           // host in path
            "https://notzoom.us/j/1",                 // not a subdomain of zoom.us
            "http://meet.google.com/insecure",        // not https
        ] {
            let ev: GEvent = serde_json::from_value(serde_json::json!({
                "id": "e1", "description": format!("Join: {evil}"),
            }))
            .unwrap();
            assert_eq!(extract_meeting_url(&ev), None, "must reject {evil}");
        }
    }

    #[test]
    fn meeting_url_accepts_exact_and_subdomain_hosts() {
        assert_eq!(
            validated_meeting_url("https://zoom.us/j/1").as_deref(),
            Some("https://zoom.us/j/1")
        );
        assert_eq!(
            validated_meeting_url("https://acme.webex.com/meet/x").as_deref(),
            Some("https://acme.webex.com/meet/x")
        );
        assert_eq!(validated_meeting_url("https://evil.example/webex.com"), None);
    }

    #[test]
    fn exchange_identity_guard_rejects_missing_identity() {
        // Drive the REAL guard `exchange_code` runs (`ensure_stable_identity`), not a
        // duplicated predicate — so weakening the production rule fails this test. An
        // id_token-less response yields empty email/sub, which must be rejected as
        // ConnectorError::Exchange.
        let missing = token_response_to_bundle(GoogleTokenResponse {
            access_token: "at".into(),
            refresh_token: Some("rt".into()),
            id_token: None,
            scope: Some("https://www.googleapis.com/auth/calendar.readonly".into()),
            expires_in: Some(3600),
        });
        let err = ensure_stable_identity(&missing)
            .expect_err("missing email/sub must be rejected by the identity guard");
        assert!(matches!(err, ConnectorError::Exchange(_)), "got {err:?}");
    }

    #[test]
    fn exchange_identity_guard_accepts_complete_identity() {
        // The same guard passes when the id_token carried both email and sub.
        let payload = serde_json::json!({ "sub": "sub-1", "email": "a@b.com" });
        let b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let complete = token_response_to_bundle(GoogleTokenResponse {
            access_token: "at".into(),
            refresh_token: Some("rt".into()),
            id_token: Some(format!("h.{b64}.s")),
            scope: Some("openid email https://www.googleapis.com/auth/calendar.readonly".into()),
            expires_in: Some(3600),
        });
        assert!(ensure_stable_identity(&complete).is_ok());
    }

    #[test]
    fn events_from_json_shapes_and_filters() {
        let raw = serde_json::json!({
            "items": [
                {
                    "id": "evt-1",
                    "summary": "Weekly Standup",
                    "start": { "dateTime": "2026-07-15T10:00:00Z" },
                    "end":   { "dateTime": "2026-07-15T10:30:00Z" },
                    "hangoutLink": "https://meet.google.com/abc",
                    "attendees": [
                        { "email": "alice@ex.com", "displayName": "Alice Ng" },
                        { "email": "bob@ex.com" },
                        { "email": "room-4@resource.calendar.google.com", "resource": true }
                    ]
                },
                {
                    // cancelled instance — dropped
                    "id": "evt-2",
                    "summary": "Cancelled",
                    "status": "cancelled",
                    "start": { "dateTime": "2026-07-15T11:00:00Z" }
                },
                {
                    // no start — dropped
                    "id": "evt-3",
                    "summary": "No time"
                },
                {
                    // no summary → "(no title)", all-day
                    "id": "evt-4",
                    "start": { "date": "2026-07-16" },
                    "end": { "date": "2026-07-17" }
                }
            ]
        });

        let events = events_from_json(raw).unwrap();
        assert_eq!(events.len(), 2, "cancelled + no-start dropped");

        let e1 = &events[0];
        assert_eq!(e1.id, "evt-1");
        assert_eq!(e1.title, "Weekly Standup");
        assert_eq!(e1.meeting_url.as_deref(), Some("https://meet.google.com/abc"));
        // resource room filtered out; two humans remain
        assert_eq!(e1.attendees.len(), 2);
        assert_eq!(e1.attendees[0].name.as_deref(), Some("Alice Ng"));
        assert_eq!(e1.attendees[0].email, "alice@ex.com");
        assert_eq!(e1.attendees[1].name, None);
        assert!(e1.end.is_some());
        assert!(!e1.all_day, "timed event is not all-day");

        let e4 = &events[1];
        assert_eq!(e4.title, "(no title)");
        assert!(e4.attendees.is_empty());
        assert!(e4.all_day, "date-only event must be flagged all_day");
    }

    // ── Surface split (gsuite Train G1 PR 1) ────────────────────────────────

    #[test]
    fn surface_parse_round_trips_as_str() {
        for s in Surface::ALL {
            assert_eq!(Surface::parse(s.as_str()), Some(s), "{s:?}");
        }
    }

    #[test]
    fn surface_parse_rejects_unknown() {
        assert_eq!(Surface::parse("drive"), None);
        assert_eq!(Surface::parse("Calendar"), None, "case-sensitive");
        assert_eq!(Surface::parse(""), None);
    }

    #[test]
    fn only_calendar_is_grandfathered() {
        assert!(Surface::Calendar.is_grandfathered());
        for s in [Surface::Gmail, Surface::Chat, Surface::Meet] {
            assert!(!s.is_grandfathered(), "{s:?}");
        }
    }

    #[test]
    fn every_surface_is_implemented() {
        // gsuite Train G2 PR 1 lifted Gmail's 501 gate; gsuite Train G4 PR 1
        // ("G4.1") lifted Chat's; gsuite Train G3 PR 2 lifts Meet's — the
        // last of the four, so this assertion is now unconditional rather
        // than carving Meet out as the still-inert exception.
        assert!(Surface::Calendar.is_implemented());
        assert!(Surface::Gmail.is_implemented());
        assert!(
            Surface::Chat.is_implemented(),
            "Chat's 501 gate lifted in gsuite Train G4 PR 1"
        );
        assert!(
            Surface::Meet.is_implemented(),
            "Meet's 501 gate lifts in this PR (gsuite Train G3 PR 2) — real ingest \
             (workers::google_meet_sweep) reads the stored token from day one"
        );
    }

    #[test]
    fn token_provider_keys_are_distinct_and_calendar_is_the_grandfathered_literal() {
        // Calendar keeps the original pre-split `oauth_tokens.provider`
        // literal — byte-identical, no migration/backfill.
        assert_eq!(Surface::Calendar.token_provider(), "google");
        let mut seen = std::collections::HashSet::new();
        for s in Surface::ALL {
            assert!(seen.insert(s.token_provider()), "duplicate token_provider: {s:?}");
        }
    }

    #[test]
    fn token_owner_for_calendar_is_always_account_wide() {
        let user = uuid::Uuid::new_v4();
        assert_eq!(
            Surface::Calendar.token_owner_for(user),
            crate::connectors::TokenOwner::AccountWide
        );
    }

    #[test]
    fn token_owner_for_gmail_is_the_connecting_member() {
        let user = uuid::Uuid::new_v4();
        assert_eq!(
            Surface::Gmail.token_owner_for(user),
            crate::connectors::TokenOwner::Member(user)
        );
    }

    #[test]
    fn token_owner_for_chat_is_the_connecting_member() {
        // Per-member storage is load-bearing for Chat (design doc §3.3/§3.5
        // — seating and push routing both need a `uid`, never account-wide):
        // no silent `AccountWide` fallback, same as Gmail.
        let user = uuid::Uuid::new_v4();
        assert_eq!(
            Surface::Chat.token_owner_for(user),
            crate::connectors::TokenOwner::Member(user)
        );
    }

    #[test]
    fn flag_provider_keys_are_distinct_and_prefixed() {
        let mut seen = std::collections::HashSet::new();
        for s in Surface::ALL {
            let key = s.flag_provider();
            assert!(key.starts_with("google_"), "{key}");
            assert!(seen.insert(key), "duplicate flag_provider key: {key}");
        }
    }

    #[test]
    fn scopes_for_always_includes_openid_email_and_the_surface_data_scope() {
        for s in Surface::ALL {
            let scopes = scopes_for(s);
            assert!(scopes.contains("openid"), "{s:?}: {scopes}");
            assert!(scopes.contains("email"), "{s:?}: {scopes}");
            assert!(scopes.contains(s.data_scope()), "{s:?}: {scopes}");
        }
    }

    #[test]
    fn calendar_data_scope_is_unchanged_from_the_pre_split_default() {
        // Byte-identical to the original hardcoded scope, so existing
        // Calendar connections need no re-consent.
        assert_eq!(
            Surface::Calendar.data_scope(),
            "https://www.googleapis.com/auth/calendar.readonly"
        );
    }

    #[test]
    fn chat_data_scope_is_the_amended_four_scope_set_pinned_verbatim() {
        // gsuite Train G4 PR 1 ("G4.1") froze the original three; the
        // chat-per-user-setup roster-matching PR (2026-09-20) amended that
        // frozen set a second time, this time AFTER real members had already
        // consented (unlike G4.1's zero-cost timing) — see
        // `Surface::Chat::data_scope`'s own doc comment for the re-consent
        // cost this amendment deliberately accepts. Pinned exactly — a scope
        // set is a contract (design doc §0/§2.1). Compared as a sorted set so
        // this test is insensitive to which order `Surface::Chat::
        // data_scope`'s literal lists the four scopes in, while still
        // failing hard on any scope added, removed, or renamed —
        // `directory.readonly` (without which `people.listDirectoryPeople`,
        // the domain-directory email match, cannot be called at all) must
        // never silently drift, exactly like `chat.memberships.readonly`
        // before it.
        let mut got: Vec<&str> = Surface::Chat.data_scope().split_whitespace().collect();
        got.sort_unstable();
        assert_eq!(
            got,
            vec![
                "https://www.googleapis.com/auth/chat.memberships.readonly",
                "https://www.googleapis.com/auth/chat.messages.readonly",
                "https://www.googleapis.com/auth/chat.spaces.readonly",
                "https://www.googleapis.com/auth/directory.readonly",
            ],
            "Chat's data scope must be exactly these four read-only scopes, no more, no less"
        );
    }

    // ── chat_missing_scopes (CS-431) ────────────────────────────────────────

    /// The exact D17-point-5 shape: a member connected while `data_scope`
    /// still had only the original three scopes, so `directory.readonly` is
    /// absent from their stored grant.
    #[test]
    fn chat_missing_scopes_lists_directory_readonly_when_absent() {
        let stored = "openid email https://www.googleapis.com/auth/chat.spaces.readonly \
                       https://www.googleapis.com/auth/chat.messages.readonly \
                       https://www.googleapis.com/auth/chat.memberships.readonly";
        let missing = chat_missing_scopes(stored).expect("a real grant string is never unknown");
        assert_eq!(
            missing,
            vec!["https://www.googleapis.com/auth/directory.readonly".to_string()],
            "{missing:?}"
        );
    }

    #[test]
    fn chat_missing_scopes_empty_when_fully_granted() {
        let stored = scopes_for(Surface::Chat);
        let missing = chat_missing_scopes(&stored).expect("known grant");
        assert!(missing.is_empty(), "a grant matching data_scope() exactly must have nothing missing: {missing:?}");
    }

    /// `openid`/`email` are never checked — every Google surface requests
    /// them identically, so they can never be the scope a Chat re-consent
    /// prompt needs to name, even when they happen to be absent from a
    /// hand-built stored string.
    #[test]
    fn chat_missing_scopes_ignores_openid_email() {
        let stored = Surface::Chat.data_scope();
        let missing = chat_missing_scopes(stored).expect("known grant");
        assert!(missing.is_empty(), "{missing:?}");
    }

    #[test]
    fn chat_missing_scopes_unknown_for_a_blank_grant() {
        assert_eq!(chat_missing_scopes(""), None);
        assert_eq!(chat_missing_scopes("   "), None);
    }

    #[test]
    fn scope_sets_are_disjoint_across_surfaces() {
        // Frozen-per-surface (decision 3): no surface's data scope should
        // silently overlap with another's — that would mean two surfaces
        // are secretly sharing a consent grant.
        let scopes: Vec<&str> = Surface::ALL.iter().map(|s| s.data_scope()).collect();
        for i in 0..scopes.len() {
            for j in (i + 1)..scopes.len() {
                assert_ne!(scopes[i], scopes[j], "{:?} vs {:?}", Surface::ALL[i], Surface::ALL[j]);
            }
        }
    }

    #[test]
    fn parse_google_surface_flag_matches_slack_teams_convention() {
        // Same fail-closed contract as `parse_org_slack_oauth_flag` /
        // `parse_enabled_flag`: "1" or case-insensitive "true" is on,
        // everything else (including unset/typo) is off.
        assert!(parse_google_surface_flag(Some("1")));
        assert!(parse_google_surface_flag(Some("true")));
        assert!(parse_google_surface_flag(Some("TRUE")));
        assert!(!parse_google_surface_flag(Some("0")));
        assert!(!parse_google_surface_flag(Some("yes")));
        assert!(!parse_google_surface_flag(None));
    }

    #[test]
    fn calendar_org_env_var_is_distinct_and_never_affects_the_personal_default() {
        assert_eq!(Surface::Calendar.env_var_name(), "ORG_GOOGLE_CALENDAR_ENABLED");
        let mut seen = std::collections::HashSet::new();
        for s in Surface::ALL {
            assert!(seen.insert(s.env_var_name()), "duplicate env var: {s:?}");
        }
        // The personal-plane default is grandfathered ON no matter what the
        // org flag's env var says (it is read only by `org_calendar_env_default`).
        assert!(surface_env_default(Surface::Calendar));
    }

    #[test]
    fn calendar_token_owner_follows_the_account_plane() {
        let user = uuid::Uuid::new_v4();
        assert_eq!(
            Surface::calendar_token_owner(false, user),
            crate::connectors::TokenOwner::AccountWide,
            "personal plane stays the grandfathered account-wide row"
        );
        assert_eq!(
            Surface::calendar_token_owner(true, user),
            crate::connectors::TokenOwner::Member(user),
            "org plane is per-member"
        );
        // The frozen surface-level answer is unchanged.
        assert_eq!(Surface::Calendar.token_owner_for(user), crate::connectors::TokenOwner::AccountWide);
    }

    #[test]
    fn calendar_surface_env_default_is_always_true() {
        // Grandfathered — never reads an env var, always on.
        assert!(surface_env_default(Surface::Calendar));
    }

    #[test]
    fn authorize_url_for_surface_builds_incremental_consent_url_for_gmail() {
        let url = authorize_url_for_surface(
            "https://api.example.com",
            "st8",
            Surface::Gmail,
        );
        // No client id configured in the test process — NotConfigured, never
        // NotImplemented (Gmail is declared, not a stub).
        assert!(
            !matches!(url, Err(ConnectorError::NotImplemented(_))),
            "{url:?}"
        );
    }

    /// Only `invalid_grant` is a dead credential; every other `/token`
    /// failure (ours to fix, or transient) stays `Exchange`.
    #[test]
    fn classify_google_token_error_is_invalid_grant_only() {
        assert_eq!(
            classify_google_token_error(Some("invalid_grant")),
            Some(CredentialDeathReason::GoogleInvalidGrant)
        );
        for other in ["invalid_client", "unauthorized_client", "invalid_request", "server_error", ""] {
            assert_eq!(classify_google_token_error(Some(other)), None, "{other}");
        }
        assert_eq!(classify_google_token_error(None), None);
    }
}
