//! `POST /v1/slack/events` — unified Slack Event API webhook.
//!
//! This is the **single Request URL** that must be set in the Slack app configuration.
//! It dispatches all Slack events this connector acts on: channel message
//! ingestion, huddle-notes capture, and the member-joined destination/backfill
//! prompts.
//!
//! # Security
//!
//! Every request (except `url_verification`) is authenticated via
//! HMAC-SHA256 over the raw body.  The signing secret is read from
//! `SLACK_SIGNING_SECRET` at request time (matching the pattern used by
//! other env-checked handlers).  Requests older than 300 seconds are
//! rejected for replay protection.
//!
//! # Behaviour
//!
//! - `url_verification` → echo the challenge (no signature required).
//! - `event_callback / message` whose subtype is `None`, `bot_message` or
//!   `thread_broadcast` → hand off to `ingest_slack_message` in a background
//!   task so the handler responds to Slack fast. **Bot-authored messages are
//!   included** (2026-09), with their `attachments`/`blocks` flattened into
//!   the ingested text by `connectors::slack_message_text` — 235 of the 253
//!   bot messages measured in the founder's workspace carry an EMPTY `text`,
//!   so the flattening is what makes them ingestible at all. Two exclusions
//!   remain: every OTHER subtype (edits, deletes, joins, `file_share`) is
//!   still dropped, and OUR OWN bot's posts are dropped at enqueue time —
//!   this connector posts destination prompts, welcome DMs and "✅ Ingested
//!   N past messages" confirmations into the very channels it watches. The
//!   `SLACK_INGEST_BOT_MESSAGES` env var (default ON) turns the whole
//!   capability off.
//! - `event_callback / message` matching [`huddle_capture_trigger`] (a
//!   `message_changed` edit whose `room.transcript_file_id` — the AI-notes
//!   canvas id, see that function's doc comment — is present) → durably
//!   enqueue a `jobs::JobKind::SlackHuddleCapture` job INLINE, on the
//!   pre-response path, returning 503 on failure so Slack redelivers (see
//!   `enqueue_huddle_capture`'s doc comment for why this one path breaks the
//!   "never block the response on a DB call" rule the rest of this handler
//!   follows). This check runs BEFORE the sub-typed-message filter below,
//!   because `message_changed` always carries a `subtype` and would
//!   otherwise be silently dropped by it — exactly the bug that made huddle
//!   capture dead for its entire prior life (reaction-based opt-in, removed
//!   in the same rewrite that added this trigger). Still true after the
//!   2026-09 subtype allowlist: `message_changed` is not on it.
//! - `event_callback / member_joined_channel` (our own bot joining a channel)
//!   → in a background task, posts the per-channel destination picker
//!   (`post_destination_prompt`) and the backfill-choice prompt
//!   (`post_backfill_prompt`) into the channel. See `handle_member_joined`'s
//!   doc comment.
//! - All other event types → ack 200 immediately (fast path for Slack retries).
//!
//! The handler never calls Slack APIs synchronously; huddle ingest happens in
//! `workers::huddle`, off the durable job queue, in the background.

use axum::{
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
    Json,
};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha2::Sha256;
use sqlx::Row;
use std::sync::Arc;
use std::sync::Mutex;
use std::collections::HashMap;

use crate::AppState;

// ── Bot identity cache ────────────────────────────────────────────────────────

/// Who THIS install's own Slack app is, as `auth.test` reports it.
///
/// Two identifiers, because a message can name our app either way: a message
/// our bot posts carries `bot_id` (`B…`, the per-install bot record) at the
/// top level AND `user` (`U…`, the bot's user record). `auth.test` returns
/// both — `user_id` and `bot_id` — so one call answers both questions.
#[derive(Clone, Debug)]
pub(crate) struct BotIdentity {
    /// `auth.test`'s `user_id` — the `U…` our bot posts as.
    pub(crate) user_id: String,
    /// `auth.test`'s `bot_id` — the `B…` stamped on every message our app
    /// posts. `None` only for a token that is not a bot token at all (a user
    /// token), which no install of ours uses; treated as "cannot self-
    /// identify by bot_id" rather than as an error.
    pub(crate) bot_id: Option<String>,
}

/// Per-process cache of `(account_id, workspace_id)` → [`BotIdentity`],
/// resolved via `auth.test`.
///
/// **Keyed on the workspace too, not on the account alone.** A bot identity
/// is per INSTALL, and `slack_integrations` is `UNIQUE (account_id,
/// workspace_id)` — one account can install into T1 and T2 and get a
/// different `bot_id`/`user_id` in each. The account-only key this replaced
/// meant that once any T1 event warmed the cache, the self-bot check for T2
/// compared against T1's identity, never matched, and ingested our OWN posts
/// in T2 — the destination prompt, the "✅ Ingested N past messages"
/// confirmation, `notify_unmatched_content_not_saved` — which is the exact
/// feedback loop the check exists to prevent (review finding 1).
///
/// The pre-existing `member_joined_channel` caller ([`resolve_bot_user_id`],
/// feeding `is_bot_self_join`) had the SAME latent bug for the same reason:
/// on a dual-workspace install it could compare a T2 joiner against T1's bot
/// user id, mistake our own bot's join for a human's, and seat it. That
/// caller now passes its `team_id` too, so the fix covers it as well.
///
/// `auth.test` is cheap but we avoid calling it on every
/// `member_joined_channel` event — and, since bot-message ingest landed, on
/// every bot-authored message. A positive entry is never invalidated: bot
/// user ids and bot ids are stable for the lifetime of an installation.
static BOT_USER_ID_CACHE: std::sync::OnceLock<Mutex<HashMap<BotIdentityKey, BotIdentity>>> =
    std::sync::OnceLock::new();

/// `(account_id, workspace_id)` — see [`BOT_USER_ID_CACHE`] for why both.
type BotIdentityKey = (uuid::Uuid, String);

fn bot_user_id_cache() -> &'static Mutex<HashMap<BotIdentityKey, BotIdentity>> {
    BOT_USER_ID_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

/// Per-process cache of installs whose own identity could NOT be resolved,
/// with a TTL.
///
/// Without it a revoked or undecryptable token turned every bot message —
/// and every Slack retry of one — into a fresh `oauth_tokens` read, a DEK
/// derivation and a live `auth.test`, forever, because the fail-closed drop
/// is not an error the caller retries out of (review finding 13). Same
/// `OnceLock<Mutex<HashMap<_, Instant>>>` + TTL shape as
/// `routes::slack::negative_resolve_cache`, the in-repo precedent.
///
/// A stale negative entry costs at most [`BOT_IDENTITY_NEGATIVE_TTL`] of
/// dropped bot messages after a token is repaired — recoverable by backfill,
/// unlike the alternative failure (ingesting our own posts), and human
/// messages are unaffected because they never consult this at all.
fn bot_identity_negative_cache() -> &'static Mutex<HashMap<BotIdentityKey, std::time::Instant>> {
    static C: std::sync::OnceLock<Mutex<HashMap<BotIdentityKey, std::time::Instant>>> =
        std::sync::OnceLock::new();
    C.get_or_init(|| Mutex::new(HashMap::new()))
}

/// How long a failed own-identity resolution suppresses re-resolving. Short
/// enough that a repaired token starts working again within minutes rather
/// than needing a process restart; long enough to bound a retry storm.
const BOT_IDENTITY_NEGATIVE_TTL: std::time::Duration = std::time::Duration::from_secs(300);

fn bot_identity_negative_hit(key: &BotIdentityKey) -> bool {
    let cache = bot_identity_negative_cache().lock().unwrap();
    matches!(cache.get(key), Some(t) if t.elapsed() < BOT_IDENTITY_NEGATIVE_TTL)
}

fn cache_bot_identity_failure(key: BotIdentityKey) {
    bot_identity_negative_cache()
        .lock()
        .unwrap()
        .insert(key, std::time::Instant::now());
}

/// Test seam: pre-seed the `auth.test` cache so a test can drive the
/// self-bot exclusion without an HTTP stub.
///
/// The alternative — letting the enqueue path call the real `auth.test` — is
/// a live network call from a unit test, which is exactly what the existing
/// `enqueue_slack_message_ingest_..._pg` tests avoid by only ever feeding
/// HUMAN messages (a human message never reaches the resolve at all; see
/// [`enqueue_slack_message_ingest`]).
#[cfg(test)]
pub(crate) fn seed_bot_identity_for_test(
    account_id: uuid::Uuid,
    workspace_id: &str,
    user_id: &str,
    bot_id: Option<&str>,
) {
    bot_user_id_cache().lock().unwrap().insert(
        (account_id, workspace_id.to_string()),
        BotIdentity {
            user_id: user_id.to_string(),
            bot_id: bot_id.map(str::to_string),
        },
    );
}

/// Whether an admitted message was posted by THIS install's own bot.
///
/// **Both sides of each comparison must be present.** The first version of
/// this check was `own.bot_id.as_deref() == event_bot_id`, and
/// [`BotIdentity::bot_id`] is documented as `None` for a token that is not a
/// bot token — while the admission rule deliberately admits legacy
/// `subtype: "bot_message"` posts that carry no `bot_id` either. `None ==
/// None` then read as a self-match, so on such an install EVERY one of those
/// messages was dropped as "our own bot", with a `debug!` that named no
/// cause (review finding 2).
///
/// `user` is compared as well as `bot_id` because a modern app post carries
/// both: the `B…` bot record and the `U…` the app posts as. Either one
/// matching ours is decisive.
pub(crate) fn is_our_own_bot_message(
    own: &BotIdentity,
    event_bot_id: Option<&str>,
    event_user: Option<&str>,
) -> bool {
    let bot_id_matches = match (own.bot_id.as_deref(), event_bot_id) {
        (Some(ours), Some(theirs)) => !ours.is_empty() && ours == theirs,
        _ => false,
    };
    let user_matches = match event_user {
        Some(theirs) => !own.user_id.is_empty() && own.user_id == theirs,
        None => false,
    };
    bot_id_matches || user_matches
}

/// True for strings shaped like a Slack actor id (`U…`, `W…`, `B…`).
///
/// Deliberately narrow: channel (`C…`), team (`T…`) and file (`F…`) ids are routing
/// values that must stay readable — `huddle_capture_trigger` below relies on this to
/// shape-validate `room.participant_history` entries before they reach the durable
/// `SlackHuddleCapture` job payload.
fn is_slack_actor_id(s: &str) -> bool {
    let mut chars = s.chars();
    matches!(chars.next(), Some('U') | Some('W') | Some('B'))
        && s.len() >= 7
        && s.len() <= 21
        && chars.all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
}

// ── Huddle capture trigger ────────────────────────────────────────────────────
//
// MEASURED ground truth (backend/tests/fixtures/slack_huddle/README.md, four
// COMPLETE real events for one huddle, captured from a live workspace): the
// trigger is `subtype == "message_changed"` AND
// `event.message.room.transcript_file_id` is PRESENT. It appears on exactly
// ONE of the four events; the other three (a `huddle_thread` start, and two
// earlier `message_changed` edits with `summary_status` `pending`/absent) are
// real huddle traffic that must NOT fire — see [`huddle_capture_trigger`]'s
// own tests below, run directly against the committed fixtures via
// `include_str!`.
//
// `room.recording.summary` is `true` on all four events from t=0, so it can
// never be the trigger — this is why an earlier draft of
// `docs/connectors/slack.md` §3.1 guessed wrong twice before this was
// measured directly.
//
// NAMING TRAP: `room.transcript_file_id` holds the CANVAS file id (Slack's
// AI-notes quip doc), not the transcript — the actual transcript lives at
// `files[0].huddle_transcript_file_id` and is UNFETCHABLE (measured: 302
// redirect to the workspace web app, bot token and user token alike). There
// is deliberately no code anywhere in this connector that tries to fetch it.

/// One trigger-fired huddle, extracted from a `message_changed` event whose
/// `room.transcript_file_id` (the canvas id — see the module section above)
/// is present. Pure and unit-testable: no I/O, no Slack API calls, no DB.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct HuddleTrigger {
    /// The AI-notes CANVAS file id (`room.transcript_file_id`) — resolve a
    /// fresh `url_private` for it via `files.info` rather than trusting any
    /// embedded copy (`workers::huddle`).
    pub(crate) canvas_file_id: String,
    pub(crate) channel_id: String,
    /// `room.thread_root_ts`, falling back to `event.message.ts` when Slack
    /// omits it — the huddle's own identity, and the third column of
    /// `slack_huddle_captures`'s idempotency key.
    pub(crate) huddle_thread_ts: String,
    /// `room.participant_history` (raw Slack user ids) — the authoritative
    /// attendee list. Empty when Slack sent none.
    pub(crate) participant_history: Vec<String>,
    /// `room.date_start` / `room.date_end`, unix seconds.
    pub(crate) date_start: Option<i64>,
    pub(crate) date_end: Option<i64>,
}

/// Read a string field, treating an EMPTY string as absent (`None`), not
/// present-but-blank.
///
/// Slack sends empty strings for unset `room` fields — fixture 02 carries
/// `"media_server": ""` and `"name": ""`, so this is measured behaviour, not
/// a hypothetical. Without this filter, `room.transcript_file_id: ""` would
/// satisfy `.and_then(Value::as_str)?` (a `Some("")`, not a `None`) and mint
/// a job for a canvas id that can never resolve, and
/// `room.thread_root_ts: ""` would satisfy the same check and suppress the
/// `event.message.ts` fallback below it — yielding an EMPTY
/// `huddle_thread_ts`, which then collides across every huddle in the same
/// channel in both the job idempotency key and the `slack_huddle_captures`
/// unique key (a second huddle would be skipped as "already recorded" and
/// lost).
fn non_empty_str<'a>(obj: &'a Value, key: &str) -> Option<&'a str> {
    obj.get(key).and_then(Value::as_str).filter(|s| !s.is_empty())
}

/// Hard cap on how many `room.participant_history` entries are carried into
/// the durable `SlackHuddleCapture` job payload.
///
/// Each surviving entry drives up to TWO sequential Slack API calls in
/// `workers::huddle::capture_huddle` (`is_bot_user`, then
/// `resolve_user_name`) — an unbounded, un-validated array turns one webhook
/// payload into an unbounded number of outbound Slack calls made by the
/// worker later. 500 is far above any real huddle's attendance (Slack caps
/// huddle participants well below this) and exists purely as a backstop
/// against a malformed or adversarial payload, not a tuning knob for normal
/// traffic.
#[cfg(test)]
const MAX_PARTICIPANT_HISTORY: usize = 500;

/// Detect whether `event` (the `event_callback.event` object, inner type
/// `"message"`) is the huddle-capture trigger, and extract everything the
/// ingest job needs. Returns `None` for every other Slack event shape,
/// including the three real-but-non-triggering huddle events the fixtures
/// pin (see the module section above).
pub(crate) fn huddle_capture_trigger(event: &Value) -> Option<HuddleTrigger> {
    if event.get("subtype").and_then(Value::as_str) != Some("message_changed") {
        return None;
    }
    // `message_changed` nests the edited message (and, with it, `room`)
    // under `event.message` — a newly-posted huddle message instead carries
    // `room` at the top level, `event.room`, with no `event.message` wrapper
    // at all. That split (not a hypothetical: measured across the fixtures at
    // `backend/tests/fixtures/slack_huddle/`) is why this reads
    // `event.message.room` here rather than `event.room`.
    let message = event.get("message")?;
    let room = message.get("room")?;

    // Empties are filtered on EVERY string extraction below — see
    // `non_empty_str`'s doc comment for why a bare `Value::as_str` is wrong
    // here.
    let canvas_file_id = non_empty_str(room, "transcript_file_id")?.to_owned();

    let channel_id = non_empty_str(event, "channel")?.to_owned();

    let huddle_thread_ts = non_empty_str(room, "thread_root_ts")
        .or_else(|| non_empty_str(message, "ts"))?
        .to_owned();

    // Carry metadata for a later authoritative re-read, never a partial
    // history. Missing/malformed/oversized attendance is explicitly unknown.
    let participant_history = crate::connectors::slack_huddle_consent::authoritative_attendance(room)
        .unwrap_or_default();

    let date_start = room.get("date_start").and_then(Value::as_i64);
    let date_end = room.get("date_end").and_then(Value::as_i64);

    Some(HuddleTrigger {
        canvas_file_id,
        channel_id,
        huddle_thread_ts,
        participant_history,
        date_start,
        date_end,
    })
}

// ── HMAC type alias ───────────────────────────────────────────────────────────

type HmacSha256 = Hmac<Sha256>;

// ── Signature verification ────────────────────────────────────────────────────

/// Outcome of signature verification.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum VerifyResult {
    Ok,
    /// `SLACK_SIGNING_SECRET` env var is absent.
    SecretNotSet,
    /// Request timestamp missing or skew > 300 s.
    TimestampInvalid,
    /// `X-Slack-Signature` missing or does not match computed HMAC.
    SignatureMismatch,
}

/// Verify with the signing secret read from the environment.
///
/// Returns `VerifyResult::SecretNotSet` when `SLACK_SIGNING_SECRET` is absent
/// or empty — callers should respond 503.
fn verify_slack_signature(headers: &HeaderMap, body: &[u8]) -> VerifyResult {
    let secret = match std::env::var("SLACK_SIGNING_SECRET") {
        Ok(s) if !s.is_empty() => s,
        _ => return VerifyResult::SecretNotSet,
    };
    verify_slack_signature_with_secret(&secret, headers, body)
}

/// Inner verification that accepts the secret directly (used in unit tests to
/// avoid relying on process-global env-var state under parallel testing).
///
/// **This is the only Slack signature verifier in the codebase.** Both
/// Slack entry points call it, and neither has its own:
///
/// * `/v1/slack/events` — via [`verify_slack_signature`] above, which supplies
///   the secret from `SLACK_SIGNING_SECRET`.
/// * `/v1/slack/interactivity` — `routes::slack_interactivity::slack_interactivity`
///   calls this function directly.
///
/// The legacy `/v1/webhooks/slack` route once had a weaker private verifier
/// of its own (it hashed `String::from_utf8_lossy(body)` and compared with
/// short-circuiting `String` equality). That verifier, and later the route
/// itself, were deleted (ADR-0016).
///
/// M1 fix:
/// - HMAC is fed incrementally over raw bytes instead of building a lossy String.
/// - Received hex signature is decoded to [u8; 32] first; length != 32 or
///   hex-decode failure are treated as mismatch WITHOUT an early length-leak
///   branch on the raw string.
/// - Constant-time comparison is performed on the decoded [u8; 32] arrays
///   using `subtle::ConstantTimeEq`.
pub(crate) fn verify_slack_signature_with_secret(
    secret: &str,
    headers: &HeaderMap,
    body: &[u8],
) -> VerifyResult {
    let timestamp = match headers
        .get("x-slack-request-timestamp")
        .and_then(|v| v.to_str().ok())
    {
        Some(t) => t.to_owned(),
        None => return VerifyResult::TimestampInvalid,
    };

    // Replay protection: reject if skew > 5 minutes.
    let ts_secs: i64 = match timestamp.parse() {
        Ok(n) => n,
        Err(_) => return VerifyResult::TimestampInvalid,
    };
    let now = chrono::Utc::now().timestamp();
    if (now - ts_secs).unsigned_abs() > 300 {
        return VerifyResult::TimestampInvalid;
    }

    let sig_header = match headers
        .get("x-slack-signature")
        .and_then(|v| v.to_str().ok())
    {
        Some(s) => s.to_owned(),
        None => return VerifyResult::SignatureMismatch,
    };

    // M1: Feed HMAC incrementally over raw bytes — no from_utf8_lossy allocation.
    // This ensures body bytes are fed verbatim, regardless of encoding.
    let mut mac = match HmacSha256::new_from_slice(secret.as_bytes()) {
        Ok(m) => m,
        Err(_) => return VerifyResult::SignatureMismatch,
    };
    mac.update(b"v0:");
    mac.update(timestamp.as_bytes());
    mac.update(b":");
    mac.update(body);
    let computed_bytes: [u8; 32] = mac.finalize().into_bytes().into();

    // M1: Decode the received hex signature to bytes first.
    // Expected format: "v0=" followed by 64 hex chars (32 bytes).
    // Treat any decode failure or wrong length as mismatch — no early return
    // that leaks the expected length via a distinct branch.
    let received_bytes: [u8; 32] = {
        let hex_part = match sig_header.strip_prefix("v0=") {
            Some(h) => h,
            None => return VerifyResult::SignatureMismatch,
        };
        match hex::decode(hex_part) {
            Ok(v) if v.len() == 32 => {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&v);
                arr
            }
            _ => return VerifyResult::SignatureMismatch,
        }
    };

    // M1: Constant-time comparison on [u8; 32] — no length-leak branch.
    use subtle::ConstantTimeEq;
    if computed_bytes.ct_eq(&received_bytes).into() {
        VerifyResult::Ok
    } else {
        VerifyResult::SignatureMismatch
    }
}

// ── Handler ───────────────────────────────────────────────────────────────────

/// `POST /v1/slack/events`
///
/// Receives Slack Event API payloads and dispatches message ingestion,
/// huddle-notes capture, and member-joined prompts — see the module doc
/// comment's Behaviour list for the full dispatch set.
pub async fn slack_events(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    // ── Parse raw bytes as JSON ───────────────────────────────────────────────
    let payload: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "invalid JSON"})),
            )
                .into_response();
        }
    };

    let event_type = payload
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("");

    // ── URL verification (no signature required per Slack docs) ──────────────
    if event_type == "url_verification" {
        let challenge = payload
            .get("challenge")
            .and_then(Value::as_str)
            .unwrap_or("");
        return (StatusCode::OK, Json(json!({"challenge": challenge}))).into_response();
    }

    // ── Signature verification (required for all other event types) ───────────
    //
    // Every arm below now logs — before this fix, NONE of the three failure
    // modes emitted anything at all: a signature mismatch, a stale/missing
    // timestamp, and a missing/empty signing secret all failed completely
    // silently. That is distinct from the equivalent-sounding gap in
    // `enqueue_huddle_capture`/`ingest_slack_message` (those DID log, just at
    // `debug!`, below the production filter) — here there was zero output at
    // any level, so an attacker probing this endpoint, a misconfigured
    // `SLACK_SIGNING_SECRET` on deploy, or Slack's own clock skew all looked
    // identical to "nothing happened" in production logs. Each arm is kept
    // distinguishable (not a single shared message) so the two genuinely
    // different failure classes — bad/forged request vs. misconfigured
    // deployment — are not conflated at read time.
    let webhook_team_id = || payload.get("team_id").and_then(Value::as_str).unwrap_or("");
    match verify_slack_signature(&headers, &body) {
        VerifyResult::Ok => {}
        VerifyResult::SecretNotSet => {
            // `error!`, not `warn!`: this takes the ENTIRE connector offline
            // (every non-`url_verification` event is rejected with 503 until
            // an operator sets the env var) — the one arm here with no
            // benign cause and no self-healing path.
            tracing::error!(
                connector_health = true,
                "Slack webhook: SLACK_SIGNING_SECRET is not set — the connector is offline (every \
                 event other than url_verification is being rejected with 503) until it is \
                 configured"
            );
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": "SLACK_SIGNING_SECRET not configured"})),
            )
                .into_response();
        }
        VerifyResult::TimestampInvalid => {
            tracing::warn!(
                team_id = webhook_team_id(),
                connector_health = true,
                "Slack webhook: rejected — request timestamp missing or outside the 300s replay \
                 window"
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid signature"})),
            )
                .into_response();
        }
        VerifyResult::SignatureMismatch => {
            tracing::warn!(
                team_id = webhook_team_id(),
                connector_health = true,
                "Slack webhook: rejected — signature mismatch"
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid signature"})),
            )
                .into_response();
        }
    }

    // ── Only handle event_callback ────────────────────────────────────────────
    if event_type != "event_callback" {
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    let Some(event) = payload.get("event") else {
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    };

    let inner_type = event.get("type").and_then(Value::as_str).unwrap_or("");

    let log_subtype = event.get("subtype").and_then(Value::as_str).unwrap_or("");
    tracing::info!(
        event_type = inner_type,
        subtype = log_subtype,
        "slack event received"
    );

    // ── Dispatch: message events → channel message ingestion ─────────────────
    if inner_type == "message" {
        // ── Huddle capture trigger — MUST sit above the sub-typed-message
        // filter below. `message_changed` always carries `subtype`, so
        // leaving this check after that filter (as the removed
        // reaction-based opt-in path implicitly did, by never checking this
        // shape at all) is exactly why huddle capture has always been
        // silently dropped. See `huddle_capture_trigger`'s doc comment for
        // the measured trigger shape.
        if let Some(trigger) = huddle_capture_trigger(event) {
            let team_id = payload.get("team_id").and_then(Value::as_str).unwrap_or("").to_owned();
            // A: enqueue INLINE, on the pre-response path, and return 503 on
            // failure so Slack redelivers — see `enqueue_huddle_capture`'s
            // doc comment for the durability argument and the explicit
            // trade this makes against the rest of this handler's
            // never-block-on-DB posture. `state.db` absent (no pool at all,
            // e.g. local dev with no DATABASE_URL) is NOT a failure of that
            // durable path — there is nothing durable to enqueue INTO — so it
            // still acks 200, exactly like the deleted `reaction_added`
            // branch this mirrors.
            let Some(pool) = state.db.clone() else {
                tracing::warn!("DB not available — skipping huddle capture trigger");
                return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
            };
            if let Err(e) = enqueue_huddle_capture(&state, &pool, &team_id, &trigger).await {
                tracing::warn!(
                    error = %e,
                    team_id = %team_id,
                    channel_id = %trigger.channel_id,
                    huddle_thread_ts = %trigger.huddle_thread_ts,
                    "failed to durably enqueue huddle capture — returning 503 so Slack redelivers \
                     (the trigger fires exactly once per huddle; losing this enqueue loses the \
                     huddle's content forever)"
                );
                return (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(json!({"error": "service unavailable"})),
                )
                    .into_response();
            }
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        }

        // ── Which messages are ingestible ────────────────────────────────────
        //
        // Until 2026-09 this was `bot_id.is_some() || subtype.is_some()` —
        // drop — followed by a plain non-empty-`text` guard. Measured in
        // the founder's workspace since 2026-08-01 that discarded 253 bot
        // messages against 27 human ones ingested, and 235 of the 253 were
        // third-party app notifications whose content is real, wanted,
        // and lives entirely in `attachments`/`blocks`.
        //
        // The rule itself lives in ONE place —
        // `connectors::slack_message_text::admit_slack_message` — shared with
        // `backfill_channel`. It was three copy-pasted copies (the third in
        // the since-deleted legacy `/v1/webhooks/slack` route) and they had
        // already drifted (review finding 14): `bot_name` was derived three
        // different ways and the legacy route's self-bot guard used a
        // narrower predicate than this one's.
        //
        // Two exclusions remain, and neither is a relaxation of the old
        // filter: every subtype outside the allowlist is still dropped, and
        // OUR OWN bot is dropped at enqueue time (where this install's
        // `bot_id` is knowable at all — see `enqueue_slack_message_ingest`).
        // That is the original filter's real reason: this connector posts its
        // own destination prompts, welcome DMs and "✅ Ingested N past
        // messages" confirmations into the very channels it watches.
        let Some(admitted) = crate::connectors::slack_message_text::admit_slack_message(event)
        else {
            return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
        };
        let crate::connectors::slack_message_text::AdmittedMessage {
            text,
            bot_id,
            bot_name,
        } = admitted;

        let channel = event
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();

        let msg_user = event
            .get("user")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();

        let msg_ts = event
            .get("ts")
            .and_then(Value::as_str)
            .unwrap_or("0")
            .to_owned();

        let thread_ts = event
            .get("thread_ts")
            .and_then(Value::as_str)
            .map(str::to_owned);

        let msg_team_id = payload
            .get("team_id")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();

        // Resolve tenant reach + durably enqueue in the background — return
        // 200 fast (Slack retries on slow responses). Tracked (not a bare
        // `tokio::spawn`) so `run()`'s shutdown drain (`AppState::
        // task_tracker`) gets a bounded window to let an in-flight
        // resolve+enqueue finish instead of the process aborting it outright
        // on deploy.
        //
        // Durable tenant hand-off (org-slack step 2): this no longer calls
        // `ingest_slack_message` directly. It resolves which account(s) this
        // workspace belongs to and which PLANE each belongs on, then
        // enqueues `jobs::JobKind::SlackMessageIngest` onto that plane's own
        // `jobs` table — see `enqueue_slack_message_ingest`'s doc comment for
        // the full routing decision table. `workers::slack_ingest`'s
        // tenant-looped worker drains it and calls the same `ingest_slack_
        // message` this spawn used to call inline.
        let state_clone = state.clone();
        state.task_tracker.spawn(async move {
            enqueue_slack_message_ingest(
                state_clone,
                msg_team_id,
                channel,
                msg_user,
                msg_ts,
                thread_ts,
                text,
                bot_id,
                bot_name,
            )
            .await;
        });

        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Dispatch: member_joined_channel → backfill prompt ───────────────────
    if inner_type == "member_joined_channel" {
        let join_user = event
            .get("user")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let join_channel = event
            .get("channel")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        let join_team_id = payload
            .get("team_id")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();
        // The event's own `ts` — the actual join moment (m365 enterprise
        // train PR 4.1), so a member who joins a routed channel LATE gets a
        // `history_from` stamped at the moment they actually joined, not at
        // whatever instant the reconciler next happens to sweep. Parsed at
        // the seating call site (`seat_joining_member`), same as
        // `msg_ts`/`timestamp` above — kept as a raw string here so this
        // dispatch block doesn't need a second parse path.
        let join_ts = event
            .get("ts")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned();

        if !join_user.is_empty() && !join_channel.is_empty() {
            // Tracked — see the message-ingest spawn above's comment; same
            // "runs after the response, so needs a drain on shutdown" reasoning.
            let state_clone = state.clone();
            state.task_tracker.spawn(async move {
                handle_member_joined(state_clone, join_team_id, join_channel, join_user, join_ts)
                    .await;
            });
        }

        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Dispatch: member_left_channel → seat revocation ─────────────────────
    // The counterpart to auto-seating. Routing a channel into a team ocean
    // seats its members (founder decision bd17b10 / PR #372); this is where
    // that stops being a one-way ratchet. Everything that decides whether a
    // seat actually moves lives in `crate::seats` — this branch only carries
    // the event across.
    if inner_type == "member_left_channel" {
        let left_user = event.get("user").and_then(Value::as_str).unwrap_or("").to_owned();
        let left_channel = event.get("channel").and_then(Value::as_str).unwrap_or("").to_owned();
        let left_team_id = payload.get("team_id").and_then(Value::as_str).unwrap_or("").to_owned();

        if !left_user.is_empty() && !left_channel.is_empty() {
            let state_clone = state.clone();
            state.task_tracker.spawn(async move {
                handle_member_left(state_clone, left_team_id, left_channel, left_user).await;
            });
        }

        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Dispatch: app_uninstalled → full disconnect ──────────────────────────
    // Org governance decision (2026-08-04), piece 3. Slack fires this when a
    // human removes the app from the workspace via the Slack admin console —
    // OUR access is already gone by the time this arrives, so this is not a
    // request to disconnect, it is Slack telling us the disconnect already
    // happened on its side and our own records need to catch up.
    //
    // **Requires a manual Slack app-config step**, not code-controllable from
    // here: the `app_uninstalled` event subscription must be added in the
    // Slack app's Event Subscriptions page (alongside the existing
    // `message.channels`/`member_joined_channel`/etc. subscriptions this
    // handler already dispatches). Until that is done, Slack never sends
    // this event and this branch is unreachable in production — the code
    // ships ahead of the dashboard change so the two can land independently.
    if inner_type == "app_uninstalled" {
        let team_id = payload.get("team_id").and_then(Value::as_str).unwrap_or("").to_owned();
        if !team_id.is_empty() {
            let state_clone = state.clone();
            state.task_tracker.spawn(async move {
                handle_slack_full_disconnect(state_clone, team_id, "app_uninstalled").await;
            });
        }
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Dispatch: tokens_revoked → full disconnect (bot) + per-member mark (oauth) ──
    // Org governance decision (2026-08-04), piece 3. Slack's `tokens_revoked`
    // payload carries `event.tokens.{oauth,bot}`, two independent arrays —
    // `oauth` is per-USER token revocations (someone individually revoked
    // their own OAuth grant to the app; the app itself is still installed
    // for everyone else), `bot` is the workspace-wide BOT token. Only a
    // `bot` entry means the same thing `app_uninstalled` means for THIS
    // app's purposes — CoralSwarm has exactly one bot-token-holding
    // integration per workspace, never per-user ones — so a `bot`-array
    // entry still drives the SAME full-workspace purge it always has.
    //
    // CS-453 (D21, Phase G wave 1): an `oauth`-array entry is NO LONGER
    // ignored. It names a specific member's Slack user id whose OWN grant
    // was revoked — `mark_tokens_oauth_revocations` durably records that as
    // a `credential_invalid_at` on the matching `oauth_tokens` row(s), for
    // whichever later story (a reconnect prompt, a subscription-owner
    // re-election) needs to react to it. Runs unconditionally, independent
    // of whether `bot` also fired in the SAME payload — if it did, the
    // spawned full-disconnect purge below will delete the row shortly after
    // anyway, so the mark is at worst a harmless, immediately-superseded
    // write, never a correctness problem either order lands in.
    //
    // Same manual Slack app-config step as `app_uninstalled` above: the
    // `tokens_revoked` event subscription must be added before Slack ever
    // sends this.
    if inner_type == "tokens_revoked" {
        let team_id = payload.get("team_id").and_then(Value::as_str).unwrap_or("").to_owned();
        let bot_token_ids: Vec<String> = event
            .pointer("/tokens/bot")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let oauth_user_ids: Vec<String> = event
            .pointer("/tokens/oauth")
            .and_then(Value::as_array)
            .map(|arr| {
                arr.iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();

        if !team_id.is_empty() && !oauth_user_ids.is_empty() {
            let state_clone = state.clone();
            let team_id_clone = team_id.clone();
            state.task_tracker.spawn(async move {
                mark_tokens_oauth_revocations(state_clone, team_id_clone, oauth_user_ids).await;
            });
        }

        if !team_id.is_empty() && !bot_token_ids.is_empty() {
            let state_clone = state.clone();
            state.task_tracker.spawn(async move {
                handle_slack_full_disconnect(state_clone, team_id, "tokens_revoked").await;
            });
        } else {
            tracing::debug!(
                team_id = %team_id,
                "tokens_revoked: no bot token id in payload — user-level revocation only, \
                 nothing to purge"
            );
        }
        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // ── Dispatch: app_home_opened → publish the onboarding status Home tab ──
    // docs/connectors/slack-onboarding.md §5.2. No new OAuth scope: the Home
    // tab is an app-config setting (Slack dashboard), not a permission —
    // `views.publish` works with the existing bot token the moment the
    // config is turned on, unlike the welcome DM's `im:write` gate
    // (`connectors::slack::send_welcome_dm`'s doc comment).
    if inner_type == "app_home_opened" {
        // Slack fires this event for BOTH tabs of the app's surface — the
        // "home" tab (what this renders) and the "messages" tab (the app's
        // own DM thread, which this app does not customize). Only "home"
        // is worth a `views.publish` call.
        let tab = event.get("tab").and_then(Value::as_str).unwrap_or("");
        if tab == "home" {
            let home_user = event.get("user").and_then(Value::as_str).unwrap_or("").to_owned();
            let home_team_id =
                payload.get("team_id").and_then(Value::as_str).unwrap_or("").to_owned();

            if !home_user.is_empty() && !home_team_id.is_empty() {
                // Tracked — same "runs after the response, needs a drain on
                // shutdown" reasoning as the message-ingest/member-joined
                // spawns above.
                let state_clone = state.clone();
                state.task_tracker.spawn(async move {
                    handle_app_home_opened(state_clone, home_team_id, home_user).await;
                });
            }
        }

        return (StatusCode::OK, Json(json!({"ok": true}))).into_response();
    }

    // Every other event type → ack 200 immediately (fast path for Slack
    // retries). Huddle capture and channel-message ingestion are both
    // dispatched above, inside the `inner_type == "message"` branch; the
    // reaction-based opt-in flow this file used to also dispatch here was
    // removed in the huddle-capture rewrite (huddle capture is no longer
    // opt-in — see `huddle_capture_trigger`).
    (StatusCode::OK, Json(json!({"ok": true}))).into_response()
}

/// Handle one `app_home_opened` event for the "home" tab: resolve which ONE
/// CoralSwarm account the Slack user who opened the tab is entitled to see,
/// derive that account's onboarding status via the SAME function the JSON
/// endpoint uses (`connectors::slack_onboarding::derive_status` —
/// docs/connectors/slack-onboarding.md §3's "one derivation" contract), and
/// publish it.
///
/// # Why this does NOT fan out over every matched account (CWE-863)
///
/// **One workspace can map to SEVERAL CoralSwarm accounts** (a personal
/// install and one or more orgs) — see `handle_member_joined_for_account`'s
/// doc comment and PR #505's fan-out fix. The first version of this handler
/// mirrored that fan-out: it looped over every account matched to `team_id`,
/// rendered a section per account, and published ONE combined view. **That
/// was a data leak, and the reasoning behind it was a real mistake worth
/// recording so nobody re-derives it.**
///
/// PR #505's fan-out is correct for **ingest**: each matched account gets its
/// OWN copy of the content, and no account learns anything about the others.
/// It is wrong for **display**. `views.publish` is keyed by SLACK USER, not
/// by account — which was (correctly) read as "we cannot publish one view per
/// account" and then (incorrectly) resolved as "so combine them all into one
/// view". The consequence: ANY member of the Slack workspace who opened the
/// CoralSwarm App Home tab — including someone with no CoralSwarm account at
/// all — was shown every CoralSwarm account's workspace name, connected
/// channel names, routing counts and org identity-coverage numbers. Two
/// unrelated companies (or a company and an employee's personal install)
/// sharing one Slack workspace each read the other's setup.
///
/// The correct resolution of "one view per Slack user" is to **pick the one
/// account that Slack user belongs to**, not to union them. If nobody can be
/// confidently resolved, this publishes a GENERIC, account-free view
/// ([`render_generic_home_section`]) — never "show them everything". Fail
/// closed: an unhelpful Home tab is a far smaller harm than a cross-tenant
/// disclosure, and there is no honest way to ask "which of these accounts did
/// you mean" inside a published view.
///
/// # The account switcher — entitled SET vs. rendered account
///
/// A Slack user CAN genuinely belong to more than one CoralSwarm account on
/// one workspace (a personal install plus their employer's org account, the
/// exact shape a real production workspace hit) — Slack gives us one Home
/// view per user and no built-in way to ask which they meant, so this used
/// to just pick the first confirmed match and stop, with no way to reach the
/// other. [`collect_entitled_app_home_accounts`] now collects EVERY account
/// [`resolve_app_home_account_for_viewer`] confirms the viewer belongs to
/// (not just the first), so a `static_select` "switch account" block can
/// list the others. **This is not the leak this doc comment's header
/// describes reintroduced**: the entitled set carries only the identifiers
/// needed to OFFER a switch (`account_id`/`plane`/`workspace_id`/
/// `workspace_name`) — never a second account's rendered
/// `SlackOnboardingStatus`. Exactly ONE account's status is ever derived and
/// rendered per publish, by [`render_app_home_for_viewer`], for whichever
/// account the caller designates as `selected` — the default pick here
/// (first confirmed, see below), or the account [`handle_app_home_switch`]
/// re-confirmed the viewer belongs to after a dropdown click. See that
/// function's own doc comment for why the account id in a switcher click is
/// never trusted directly.
///
/// Stateless by design: there is no per-user preference persisted anywhere
/// (no migration, no table) — a fresh `app_home_opened` ALWAYS falls back to
/// the same default pick below, even right after a switcher click. That is a
/// deliberate v1 tradeoff (see [`handle_app_home_switch`]'s doc comment), not
/// an oversight.
///
/// # How the viewer is resolved
///
/// [`resolve_app_home_account_for_viewer`] answers "does this Slack user
/// belong to THIS candidate account", per plane, and the loop checks every
/// candidate rather than stopping at the first (needed for the entitled set
/// above). Resolution failure for one candidate never aborts the others (the
/// same per-account fault isolation the fan-out version had, kept
/// deliberately) — it just means that candidate is not the answer.
///
/// **Pool split.** `derive_status` takes a `control_pool`/`tenant_pool` pair
/// (see its own doc comment for why). `ORG_SLACK_OAUTH_ENABLED` is live in
/// production, so this handler CAN reach Org-plane accounts, and their
/// content lives on their own tenant database, not on control — passing
/// `pool` for both parameters here used to render a confident "nothing
/// captured yet" for an org whose ingest was working fine, because the
/// tenant-pool reads (`slack_watched_channels`, the `atoms`/`graphs` join,
/// part of identity coverage) landed on the wrong database.
/// [`resolve_app_home_tenant_pool`] is what now resolves the right
/// `tenant_pool` per confirmed viewer, and its own doc comment has the full
/// per-outcome breakdown; short version: an Org-plane viewer gets
/// [`resolve_org_plane`]'s resolved tenant pool
/// (`routes::clerk_webhook`'s precedent shape: `org_is_revoked` fence →
/// `routing::lookup_by_org` → `row.is_servable()` → `registry.get_or_build`),
/// `control_pool` stays `pool` either way, and if a real tenant pool cannot
/// be resolved this renders the same transient-error block a `derive_status`
/// failure renders — NEVER falling back to `pool` for `tenant_pool`, which
/// would silently reproduce the exact bug being fixed (confident zeroes
/// instead of an honest "couldn't load"). A Personal-plane viewer keeps
/// `control_pool` and `tenant_pool` as the SAME physical pool, unchanged —
/// the established convention `resolve_channel_destinations`'s own doc
/// comment describes for every other consumer-plane caller.
async fn handle_app_home_opened(state: Arc<AppState>, team_id: String, slack_user_id: String) {
    let Some(pool) = state.db.clone() else {
        tracing::debug!("DB not available — skipping app_home_opened");
        return;
    };

    let Some((entitled, publish_token)) =
        collect_entitled_app_home_accounts(&state, &pool, &team_id, &slack_user_id).await
    else {
        return;
    };

    let Some(bot_token) = publish_token else {
        tracing::debug!(
            team_id = %team_id,
            "no usable bot token for any matched account — skipping views.publish"
        );
        return;
    };

    // The default pick on every FRESH open: the first confirmed account, in
    // `resolve_slack_account_planes`' deterministic order — byte-identical to
    // this function's pre-switcher behaviour. See this function's doc
    // comment's "The account switcher" section for why a fresh open always
    // reverts to this default rather than remembering a prior switcher pick.
    let blocks: Vec<serde_json::Value> = match entitled.first() {
        Some(selected) => {
            render_app_home_for_viewer(&state, &pool, selected, &entitled, &slack_user_id).await
        }
        None => {
            // Fail closed. This viewer could not be tied to any one account
            // on this workspace — an unlinked workspace member, an identity
            // lookup that could not be completed, or a workspace whose
            // matched accounts all failed their own lookups. Nothing
            // account-specific is disclosed.
            tracing::debug!(
                team_id = %team_id,
                slack_user_id = %slack_user_id,
                "app_home_opened: no account could be confidently resolved for this Slack user — \
                 publishing the generic view"
            );
            render_generic_home_section()
        }
    };

    publish_app_home_view(&state.http_client, &bot_token, &slack_user_id, &team_id, blocks).await;
}

/// One account a viewer is CONFIRMED to belong to, plus everything needed to
/// either render it ([`render_app_home_for_viewer`]) or offer it as a
/// switcher option ([`build_account_switcher_block`]). Never constructed
/// except by [`collect_entitled_app_home_accounts`] after
/// [`resolve_app_home_account_for_viewer`] returns `true` for it.
///
/// Deliberately carries only identifiers — `account_id`/`plane`/
/// `workspace_id`/`workspace_name` — never a rendered
/// [`crate::connectors::slack_onboarding::SlackOnboardingStatus`]. That is
/// the whole safety property behind collecting a `Vec` of these (see
/// [`handle_app_home_opened`]'s doc comment's "The account switcher"
/// section): a `Vec` of IDENTIFIERS is safe to hand to a dropdown, a `Vec` of
/// RENDERED STATUS is the exact leak this module's header describes.
#[derive(Clone)]
struct EntitledAppHomeAccount {
    account_id: uuid::Uuid,
    plane: super::slack::AccountPlane,
    workspace_id: String,
    workspace_name: String,
    /// This account's own bot token — used both to derive its status
    /// ([`render_app_home_for_viewer`]) and as one candidate for the
    /// `views.publish` credential (see [`collect_entitled_app_home_accounts`]'s
    /// `publish_token` doc comment).
    bot_token: String,
    /// `None` for Personal, `Some(clerk_org_id)` for Org — carried through so
    /// [`resolve_app_home_tenant_pool`] can resolve this account's real
    /// tenant pool. See [`handle_app_home_opened`]'s doc comment's "Pool
    /// split" section.
    clerk_org_id: Option<String>,
}

/// Every account this Slack workspace (`team_id`) resolves to, filtered down
/// to the ones `slack_user_id` is CONFIRMED to belong to (per
/// [`resolve_app_home_account_for_viewer`]). Shared by
/// [`handle_app_home_opened`] (fresh tab open) and
/// [`handle_app_home_switch`] (the account-switcher's `block_actions`
/// handler) so entitlement can never drift between the two surfaces — this
/// is the ONLY function in this file that decides "which accounts may Slack
/// user X ever be shown", full stop.
///
/// Returns `None` if the workspace has no matched accounts at all, or the
/// initial account lookup itself failed — the caller has nothing to do
/// either way (the same early-return `handle_app_home_opened` made before
/// this loop was extracted for the switcher). `Some((entitled, publish_
/// token))` otherwise: `entitled` may still be empty (no candidate confirmed
/// the viewer), and `publish_token` is `None` only if not ONE matched account
/// had a usable bot token — see its own field doc on
/// [`EntitledAppHomeAccount`] for why any one of them works for the publish
/// call itself.
async fn collect_entitled_app_home_accounts(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    team_id: &str,
    slack_user_id: &str,
) -> Option<(Vec<EntitledAppHomeAccount>, Option<String>)> {
    let matches = match super::slack::resolve_slack_account_planes(pool, team_id).await {
        Ok(m) if m.is_empty() => {
            tracing::debug!(
                team_id = %team_id,
                "no oauth_tokens/connector_installs for workspace — ignoring app_home event"
            );
            return None;
        }
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, team_id = %team_id, "DB error looking up account(s) for app_home event");
            return None;
        }
    };

    let mut entitled: Vec<EntitledAppHomeAccount> = Vec::new();
    // Any matched account's bot token works for the `views.publish` call
    // itself (Home tab publish is user+app scoped, not per-workspace-token) —
    // the first one found is used. This is NOT account-specific data: it is
    // the credential the app authenticates the publish call with, and the
    // generic fallback view needs one too. If no matched account has a usable
    // token there is nothing to publish with at all.
    let mut publish_token: Option<String> = None;

    for (account_id, clerk_org_id) in matches {
        // `resolve_slack_account_planes` already handed back this account's
        // `clerk_org_id`, so the shared plane table is reached without a
        // second `accounts` lookup. An `Err` here is an infrastructure
        // condition, not "no token" — it takes the same `continue`-with-
        // `error!` arm the token lookup's own `Err` does, never the `Ok(None)`
        // "this account has no install" arm.
        let dek_plane = match crate::crypto::DekPlane::resolve_for_org(
            state,
            pool,
            account_id,
            clerk_org_id.as_deref(),
        )
        .await
        {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "failed to resolve DEK plane on app_home event");
                continue;
            }
        };
        let bot_token = match crate::workers::huddle::get_slack_bot_token_pub(
            pool,
            &dek_plane,
            account_id,
        )
        .await
        {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::warn!(account_id = %account_id, "no bot token for account on app_home event");
                continue;
            }
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "failed to fetch bot token on app_home event");
                continue;
            }
        };

        if publish_token.is_none() {
            publish_token = Some(bot_token.clone());
        }

        let plane = match super::slack::account_plane(pool, account_id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %account_id, "failed to resolve account plane on app_home event");
                continue;
            }
        };

        // Same "most-recently-connected row" pick `routes::slack::
        // onboarding_status` makes — see that handler's doc comment. Scoped:
        // `slack_integrations` carries 0083's `account_isolation`.
        let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
            Ok(tx) => tx,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %account_id, "failed to open scoped tx for slack_integrations lookup on app_home event");
                continue;
            }
        };
        let row = sqlx::query(
            "SELECT workspace_id, workspace_name FROM slack_integrations \
             WHERE account_id = $1 ORDER BY created_at DESC LIMIT 1",
        )
        .bind(account_id)
        .fetch_optional(&mut *tx)
        .await;
        tx.rollback().await.ok();
        let row = match row {
            Ok(Some(r)) => r,
            Ok(None) => continue,
            Err(e) => {
                tracing::warn!(error = %e, account_id = %account_id, "failed to look up slack_integrations on app_home event");
                continue;
            }
        };
        let workspace_id: String = row.get("workspace_id");
        let workspace_name: String = row.get("workspace_name");

        // THE authorization check. Everything ELSE about this account
        // (rendered onboarding status) must not be fetched or shown for a
        // viewer who has not been proven to belong to `account_id` — the
        // workspace id/name above is metadata, not content, and is only ever
        // used to LABEL a switcher option, never to derive a status.
        let belongs = resolve_app_home_account_for_viewer(
            state,
            pool,
            &state.http_client,
            account_id,
            plane,
            &workspace_id,
            &bot_token,
            slack_user_id,
            clerk_org_id.as_deref(),
        )
        .await;
        if !belongs {
            continue;
        }

        entitled.push(EntitledAppHomeAccount {
            account_id,
            plane,
            workspace_id,
            workspace_name,
            bot_token,
            clerk_org_id,
        });
        // Deliberately NO `break` here anymore: every OTHER confirmed
        // account matters now, for the switcher's dropdown. See this
        // function's own doc comment and [`EntitledAppHomeAccount`]'s for why
        // collecting all of them is still safe — this loop never fetches or
        // renders a second account's onboarding status, only its
        // identifiers.
    }

    Some((entitled, publish_token))
}

/// Render the Home tab for `selected` — MUST be an element of `entitled`.
/// Both call sites satisfy this by construction: [`handle_app_home_opened`]
/// picks `entitled.first()`, and [`handle_app_home_switch`] looks `selected`
/// up BY VALUE inside a freshly-recomputed `entitled` before ever reaching
/// here (see that function's doc comment for why the switcher's payload
/// value is never trusted directly).
///
/// One account's [`crate::connectors::slack_onboarding::SlackOnboardingStatus`],
/// rendered via [`render_onboarding_home_section`] off the SAME derivation
/// call `handle_app_home_opened` always used
/// (`connectors::slack_onboarding::derive_status` —
/// docs/connectors/slack-onboarding.md §3's "one derivation" contract) —
/// resolving the tenant pool that derivation needs via
/// [`resolve_app_home_tenant_pool`], see that function's own doc comment for
/// the "Pool split" bug this exists to avoid.
///
/// Plus — only when `entitled` has more than one element — a `static_select`
/// switcher block ([`build_account_switcher_block`]) listing every OTHER
/// entitled account (never `selected` itself: switching to the account
/// already shown is not a choice). A viewer entitled to exactly one account
/// gets NO dropdown at all: a switcher with one option is noise, not a
/// feature.
async fn render_app_home_for_viewer(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    selected: &EntitledAppHomeAccount,
    entitled: &[EntitledAppHomeAccount],
    slack_user_id: &str,
) -> Vec<serde_json::Value> {
    let tenant_pool = resolve_app_home_tenant_pool(
        state,
        control_pool,
        selected.account_id,
        selected.plane,
        selected.clerk_org_id.as_deref(),
    )
    .await;

    let mut blocks = match &tenant_pool {
        None => transient_onboarding_error_block(),
        Some(tenant_pool) => match crate::connectors::slack_onboarding::derive_status(
            control_pool,
            &tenant_pool,
            &state.http_client,
            selected.account_id,
            selected.plane,
            // This surface resolves an ACCOUNT for its viewer, never a ROLE
            // — it is a signature-authenticated webhook with no Clerk
            // request context, and it renders one generic view for everyone
            // entitled to the account. `Unresolved` says exactly that
            // (`viewer_role: null`, no member section) rather than claiming
            // a role we did not check; see `Viewer::Unresolved`'s own doc
            // comment.
            crate::connectors::slack_onboarding::Viewer::Unresolved,
            &selected.workspace_id,
            &selected.workspace_name,
            &selected.bot_token,
        )
        .await
        {
            Ok(status) => render_onboarding_home_section(&selected.workspace_name, &status),
            Err(e) => {
                // A derivation failure is a TRANSIENT problem for a viewer we
                // DID identify — say so, rather than silently dropping to the
                // generic "set up Slack" view, which would read as "you have
                // nothing connected" to someone who does.
                tracing::warn!(error = %e, account_id = %selected.account_id, "failed to derive Slack onboarding status for app_home");
                transient_onboarding_error_block()
            }
        },
    };

    // High-signal review: the top clarifications, with one-tap buttons wired
    // through `cs_review_*` interactivity. Best-effort — a fetch failure
    // renders no section (absence of asks is the product working; a spurious
    // error block here would cry wolf on every transient DB hiccup).
    //
    // Per VIEWER (settings 2.12): only items in oceans the person who opened
    // the tab can read, never another member's private ocean. The viewer is
    // resolved through `resolve_clicking_user`, the identity path the
    // `cs_review_*` buttons themselves use; a viewer it cannot resolve could
    // not act on the buttons either, so they get no section (fail closed).
    if let Some(tenant_pool) = &tenant_pool {
        let viewer = super::slack_interactivity::resolve_clicking_user(
            control_pool,
            tenant_pool,
            &state.http_client,
            selected.account_id,
            &selected.workspace_id,
            &selected.bot_token,
            slack_user_id,
        )
        .await;
        match viewer {
            Some(viewer) => {
                let decision = crate::connectors::slack_review::viewer_decision(
                    selected.plane,
                    selected.clerk_org_id.as_deref(),
                    selected.account_id,
                    viewer,
                    tenant_pool,
                );
                match crate::connectors::slack_review::review_items_for_viewer(
                    Some(&decision),
                    tenant_pool,
                    selected.account_id,
                    viewer,
                )
                .await
                {
                    Ok(items) => blocks.extend(crate::connectors::slack_review::render_review_section(
                        selected.account_id,
                        &items,
                    )),
                    Err(e) => {
                        tracing::warn!(error = %e, account_id = %selected.account_id, "app_home review section fetch failed");
                    }
                }
            }
            None => {
                tracing::debug!(account_id = %selected.account_id, "app_home review section skipped: viewer not resolved to a CoralSwarm user");
            }
        }
    }

    let others = other_entitled_accounts(selected, entitled);
    if !others.is_empty() {
        blocks.push(build_account_switcher_block(selected, &others));
    }

    blocks
}

/// Every OTHER entitled account relative to `selected` — the account already
/// shown is never offered as a switch target (switching to it would not be a
/// choice, and listing it would make the currently-shown account
/// indistinguishable from a switch option in the dropdown). Pure and
/// synchronous — the only non-async computation in
/// [`render_app_home_for_viewer`] — so it is unit-tested directly with no DB
/// or network call; see `app_home_switcher_tests` below.
fn other_entitled_accounts<'a>(
    selected: &EntitledAppHomeAccount,
    entitled: &'a [EntitledAppHomeAccount],
) -> Vec<&'a EntitledAppHomeAccount> {
    entitled.iter().filter(|a| a.account_id != selected.account_id).collect()
}

/// POST `views.publish` for `slack_user_id` with `blocks`, logging (never
/// panicking or propagating) on any Slack/HTTP/JSON failure — a
/// `views.publish` error has no human waiting on a response the way an
/// interactivity ack does, so logging is the only recourse. Shared by
/// [`handle_app_home_opened`] and [`handle_app_home_switch`] so the two
/// surfaces cannot drift in how a publish failure is reported.
async fn publish_app_home_view(
    http: &reqwest::Client,
    bot_token: &str,
    slack_user_id: &str,
    team_id: &str,
    blocks: Vec<serde_json::Value>,
) {
    let body = serde_json::json!({
        "user_id": slack_user_id,
        "view": {
            "type": "home",
            "blocks": blocks,
        }
    });

    let resp = http.post(format!("{}/views.publish", crate::connectors::slack::slack_api_base())).bearer_auth(bot_token).json(&body).send().await;
    match resp {
        Ok(r) => match r.json::<serde_json::Value>().await {
            Ok(json) => {
                if json.get("ok").and_then(Value::as_bool) != Some(true) {
                    let err = json.get("error").and_then(Value::as_str).unwrap_or("unknown");
                    tracing::warn!(error = %err, team_id = %team_id, "views.publish Slack error");
                }
            }
            Err(e) => tracing::warn!(error = %e, team_id = %team_id, "views.publish JSON parse error"),
        },
        Err(e) => tracing::warn!(error = %e, team_id = %team_id, "views.publish HTTP error"),
    }
}

/// The account-switcher `static_select`'s `action_id` —
/// `routes::slack_interactivity::slack_interactivity`'s `block_actions`
/// dispatch routes this straight to [`handle_app_home_switch`].
pub(crate) const APP_HOME_SWITCH_ACTION_ID: &str = "cs_apphome_switch";

/// Slack's `plain_text` option/placeholder label cap for `static_select`
/// blocks (Block Kit reference: 75 characters) — truncated defensively
/// rather than letting a long workspace name make `views.publish` reject the
/// whole Home tab.
const SLACK_OPTION_TEXT_MAX: usize = 75;

fn truncate_option_label(label: &str) -> String {
    if label.chars().count() <= SLACK_OPTION_TEXT_MAX {
        return label.to_string();
    }
    let truncated: String = label.chars().take(SLACK_OPTION_TEXT_MAX - 1).collect();
    format!("{truncated}…")
}

/// Build the switcher's `static_select` action block offering every account
/// in `others` (already filtered to exclude the currently-shown account by
/// the caller, [`render_app_home_for_viewer`]).
///
/// The option `value` is the account's `account_id` as plain text — this is
/// UNTRUSTED the moment it round-trips through Slack: [`handle_app_home_switch`]
/// re-derives the viewer's entire entitled set from scratch and only uses
/// this value to select WITHIN it, never as a bare lookup key on its own. See
/// that function's doc comment.
fn build_account_switcher_block(
    selected: &EntitledAppHomeAccount,
    others: &[&EntitledAppHomeAccount],
) -> serde_json::Value {
    let options: Vec<serde_json::Value> = others
        .iter()
        .map(|a| {
            let plane_label = match a.plane {
                super::slack::AccountPlane::Personal => "Personal",
                super::slack::AccountPlane::Org => "Org",
            };
            let label = truncate_option_label(&format!("{} ({plane_label})", a.workspace_name));
            serde_json::json!({
                "text": { "type": "plain_text", "text": label, "emoji": true },
                "value": a.account_id.to_string(),
            })
        })
        .collect();

    serde_json::json!({
        "type": "actions",
        "block_id": "cs_apphome_switcher",
        "elements": [{
            "type": "static_select",
            "action_id": APP_HOME_SWITCH_ACTION_ID,
            "placeholder": {
                "type": "plain_text",
                "text": truncate_option_label(&format!("Switch from {}", selected.workspace_name)),
                "emoji": true
            },
            "options": options,
        }]
    })
}

/// Handle a confirmed `block_actions` / [`APP_HOME_SWITCH_ACTION_ID`] click —
/// the account-switcher `static_select`'s only consumer. Called from a
/// `tokio::spawn`ed task AFTER
/// `routes::slack_interactivity::slack_interactivity` has already acked
/// Slack's ~3-second interactivity deadline, the same ack-then-spawn shape
/// that file's `cs_backfill_*`/`cs_dest_*` handlers already use:
/// `derive_status` makes a live `conversations.list` call plus several DB
/// reads, on top of this handler's own `collect_entitled_app_home_accounts`
/// pass, and none of that belongs on the ack path.
///
/// # The payload's account id is NEVER trusted directly
///
/// `requested_account_id` is `selected_option.value` from a Slack
/// interaction payload — a string a Slack user fully controls; Slack does
/// not verify that a `static_select` response matches an option this server
/// actually rendered, and nothing stops a crafted payload from naming any
/// `account_id` at all. It is used ONLY to select an entry OUT OF
/// [`collect_entitled_app_home_accounts`]'s freshly-recomputed result — the
/// SAME entitlement question [`handle_app_home_opened`] answers, re-asked
/// from scratch, never assumed from whatever the dropdown showed at render
/// time. If `requested_account_id` names an account the viewer is not (or is
/// no longer) confirmed to belong to, this refuses: logs a warning and
/// returns WITHOUT calling `views.publish` at all, so nothing about the
/// requested account — or anything else — changes on the Home tab. This is
/// the crafted-payload case this feature's design review called out by name,
/// and this function is its enforcement point.
///
/// Re-deriving the ENTIRE entitled set (rather than only re-checking the one
/// requested id) also means a membership change since the tab was last
/// opened (e.g. an `account_members` revocation) is honoured immediately —
/// the freshly-rendered switcher reflects the CURRENT set, not a stale one.
///
/// # Stateless — no per-user preference is persisted
///
/// This re-publishes the Home tab for the NEWLY selected account, but writes
/// nothing to any table: there is no per-Slack-user "last selected account"
/// row, and this PR deliberately adds no migration to create one (see this
/// feature's design constraints). Consequence: the NEXT fresh
/// `app_home_opened` (a genuine tab close/reopen) reverts to
/// `handle_app_home_opened`'s default pick, not to whatever this function
/// last rendered. That is an accepted v1 tradeoff — the complaint this
/// feature exists to fix is "there is no way to reach the other account AT
/// ALL", not "the switch does not stick forever" — and persisting it durably
/// is the natural v2 (a per-user preference table, once one exists for other
/// reasons).
pub(crate) async fn handle_app_home_switch(
    state: Arc<AppState>,
    team_id: String,
    slack_user_id: String,
    requested_account_id: String,
) -> AppHomeSwitchOutcome {
    let Some(pool) = state.db.clone() else {
        tracing::debug!("DB not available — skipping app_home switch");
        return AppHomeSwitchOutcome::NoDbOrToken;
    };

    let Ok(requested) = uuid::Uuid::parse_str(&requested_account_id) else {
        tracing::warn!(
            requested = %requested_account_id,
            "app_home switch: selected value is not a valid account id — ignoring"
        );
        return AppHomeSwitchOutcome::Refused;
    };

    let Some((entitled, publish_token)) =
        collect_entitled_app_home_accounts(&state, &pool, &team_id, &slack_user_id).await
    else {
        return AppHomeSwitchOutcome::NoDbOrToken;
    };

    let Some(bot_token) = publish_token else {
        tracing::debug!(team_id = %team_id, "app_home switch: no usable bot token — skipping views.publish");
        return AppHomeSwitchOutcome::NoDbOrToken;
    };

    // THE re-confirmation this function's doc comment describes. `requested`
    // only ever selects WITHIN this freshly-recomputed confirmed set.
    let Some(selected) = entitled.iter().find(|a| a.account_id == requested) else {
        tracing::warn!(
            team_id = %team_id,
            slack_user_id = %slack_user_id,
            requested = %requested,
            "app_home switch: requested account is not in the viewer's confirmed entitled set — \
             refusing (crafted-payload / stale-entitlement case); nothing about the requested \
             account is rendered"
        );
        return AppHomeSwitchOutcome::Refused;
    };
    let selected_account_id = selected.account_id;

    let blocks = render_app_home_for_viewer(&state, &pool, selected, &entitled, &slack_user_id).await;
    publish_app_home_view(&state.http_client, &bot_token, &slack_user_id, &team_id, blocks).await;
    AppHomeSwitchOutcome::Published { account_id: selected_account_id }
}

/// [`handle_app_home_switch`]'s outcome — an observable return value purely
/// for testability (the production dispatch in
/// `routes::slack_interactivity` ignores it; Slack has already been acked by
/// the time this runs). Lets a test assert the crafted-payload refusal
/// WITHOUT needing to observe a `views.publish` network call (which never
/// happens on the `Refused` path — see this function's doc comment) or scrape
/// log output.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum AppHomeSwitchOutcome {
    /// The requested account was confirmed in the viewer's entitled set and
    /// the Home tab was republished for it.
    Published { account_id: uuid::Uuid },
    /// Refused: an unparseable account id, or one not in the viewer's
    /// freshly-recomputed entitled set (the crafted-payload / stale-
    /// entitlement case this function's doc comment describes). Nothing is
    /// rendered or published.
    Refused,
    /// Nothing to do at all: no DB configured, or no matched account on this
    /// workspace had a usable bot token. Distinct from `Refused` (which means
    /// "we could answer the entitlement question and the answer was no")
    /// only for a test's benefit — both are equally inert in production.
    NoDbOrToken,
}

/// The Home tab block shown for a viewer we DID identify (an account we
/// confirmed they belong to) when their onboarding status genuinely cannot be
/// derived right now — a `derive_status` DB error, or (Org-plane) a tenant
/// pool that could not be resolved. Deliberately the SAME block for both: from
/// this viewer's perspective "the derivation query failed" and "we could not
/// even get to the right database" are the identical honest answer, "try
/// again in a moment", and neither is the generic "you have nothing set up"
/// view [`render_generic_home_section`] renders for a viewer who could not be
/// tied to any account at all. See [`handle_app_home_opened`]'s doc comment's
/// "Pool split" section for why this must never be swapped for a silent
/// fallback to a wrong pool instead.
fn transient_onboarding_error_block() -> Vec<serde_json::Value> {
    vec![serde_json::json!({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "CoralSwarm couldn't load your onboarding status right now. Try \
                     reopening this tab in a moment."
        }
    })]
}

/// Fail-closed: `true` when the org has a live in-app deletion (members are
/// locked out) or the state cannot be read.
async fn org_deletion_blocks_members(control_pool: &sqlx::PgPool, org_id: &str) -> bool {
    match crate::services::org_deletion::state_for_org(control_pool, org_id).await {
        Ok(None) => false,
        Ok(Some(st)) => crate::services::org_deletion::locks_members_out(&st),
        Err(e) => {
            tracing::warn!(org = %org_id, error = %e, "app_home: org deletion state unreadable - rendering nothing");
            true
        }
    }
}

/// The tenant pool a Slack full disconnect (uninstall) purges on. This is
/// system cleanup, not a member-facing read, so it goes through
/// [`resolve_org_plane`] directly: [`resolve_app_home_tenant_pool`] returns
/// `None` for an org with a live in-app deletion (members are locked out),
/// which would skip the watched-channel/seat purge for a workspace uninstalled
/// during the grace period and leave destinations that resurrect on reconnect.
async fn resolve_disconnect_tenant_pool(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    plane: super::slack::AccountPlane,
    clerk_org_id: Option<&str>,
) -> Option<sqlx::PgPool> {
    match (plane, clerk_org_id) {
        (super::slack::AccountPlane::Personal, _) => Some(control_pool.clone()),
        (super::slack::AccountPlane::Org, Some(org_id)) => {
            match resolve_org_plane(state, control_pool, org_id).await {
                OrgPlaneResolution::Pool(p, _) => Some(p),
                OrgPlaneResolution::Dropped | OrgPlaneResolution::Unavailable(_) => None,
            }
        }
        (super::slack::AccountPlane::Org, None) => {
            tracing::error!(%account_id, "slack full disconnect: Org-plane account has no clerk_org_id");
            None
        }
    }
}

/// Resolve the pool [`handle_app_home_opened`] must pass as `derive_status`'s
/// `tenant_pool` argument for a CONFIRMED viewer's account/plane. See that
/// function's doc comment's "Pool split" section for the bug this exists to
/// fix (an org's content silently read off `control_pool`, rendering real
/// ingest as "nothing captured yet").
///
/// * **Personal** — always `Some(control_pool.clone())`. Same physical pool
///   as `control_pool`, unchanged from before this fix.
/// * **Org** — resolves the account's real tenant pool via
///   [`resolve_org_plane`] (`routes::clerk_webhook`'s precedent shape:
///   `org_is_revoked` fence → `routing::lookup_by_org` → `row.is_servable()`
///   → `registry.get_or_build`). Three of its four outcomes collapse to
///   `None` here — revoked, not `is_servable()` (mid-migration/torn-down), or
///   a transient lookup/build failure/timeout — each logged at `warn!` so a
///   silent "nothing captured" never happens unexplained. The fourth,
///   "never onboarded onto a tenant database" (`Missing`), is `Some(control_
///   pool.clone())`: that org's content genuinely still lives on the
///   consumer plane, exactly `enqueue_slack_message_ingest`'s identical case
///   — not a guess, a fact about where ingest actually wrote.
/// * **Org, but `clerk_org_id` is `None`** — an invariant break (`account_
///   plane` derives `Org` from `accounts.clerk_org_id IS NOT NULL`, and
///   `clerk_org_id` here came from the SAME table moments earlier via
///   `resolve_slack_account_planes`), logged at `error!` since it should be
///   unreachable. `None` — fail closed rather than guess a pool.
///
/// `None` means "do not call `derive_status` at all" — the caller renders
/// [`transient_onboarding_error_block`] instead. This function NEVER falls
/// back to `control_pool` for an Org account's `tenant_pool` on any failure
/// path; doing so would silently reproduce the exact bug being fixed.
///
/// Called from TWO places for the same confirmed Org account per Home tab
/// publish: once by [`resolve_app_home_account_for_viewer`] (to resolve the
/// pool `resolve_clicking_user` needs for the entitlement check itself — see
/// that function's "Org-plane pool split" doc section) and once by
/// [`render_app_home_for_viewer`] (for `derive_status`). Both calls are cheap
/// in the steady state — see [`resolve_org_plane`]'s own doc comment and
/// `routes::slack_interactivity`'s identical reasoning for
/// `registry.get_or_build`: real cost is paid once per org per process
/// lifetime, an already-built pool is returned instantly after.
async fn resolve_app_home_tenant_pool(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    plane: super::slack::AccountPlane,
    clerk_org_id: Option<&str>,
) -> Option<sqlx::PgPool> {
    match plane {
        super::slack::AccountPlane::Personal => Some(control_pool.clone()),
        super::slack::AccountPlane::Org => match clerk_org_id {
            Some(org_id) => match resolve_org_plane(state, control_pool, org_id).await {
                // App Home is member-facing: an org with a live in-app
                // deletion shows nothing (members are locked out from the
                // request). `resolve_org_plane` itself stays deletion-blind
                // because message CAPTURE keeps flowing during grace.
                OrgPlaneResolution::Pool(..) if org_deletion_blocks_members(control_pool, org_id).await => {
                    tracing::info!(account_id = %account_id, org_id = %org_id, "app_home_opened: org deletion is live — rendering no org content");
                    None
                }
                OrgPlaneResolution::Pool(p, _) => Some(p),
                OrgPlaneResolution::Dropped => {
                    tracing::warn!(
                        account_id = %account_id,
                        org_id = %org_id,
                        "app_home_opened: org's tenant plane is not servable (revoked, or \
                         routing row present but mid-migration/torn-down) — cannot render a \
                         real onboarding status"
                    );
                    None
                }
                OrgPlaneResolution::Unavailable(reason) => {
                    tracing::warn!(
                        account_id = %account_id,
                        org_id = %org_id,
                        reason = %reason,
                        "app_home_opened: org tenant pool unavailable — cannot render a real \
                         onboarding status"
                    );
                    None
                }
            },
            None => {
                tracing::error!(
                    account_id = %account_id,
                    "app_home_opened: Org-plane account has no clerk_org_id — invariant broken, \
                     refusing to guess a tenant pool"
                );
                None
            }
        },
    }
}

/// Is `slack_user_id` entitled to be shown `account_id`'s onboarding status?
/// [`handle_app_home_opened`]'s authorization check — see that function's
/// "Why this does NOT fan out over every matched account" section for the
/// leak this exists to prevent.
///
/// Returns `false` on ANY doubt, and the caller renders the account-free
/// generic view when nothing returns `true`. The two planes prove identity by
/// genuinely different means, and neither falls back to the other:
///
/// * **Personal** — `oauth_tokens.provider_user_id`: the Slack user id of the
///   member who ran the OAuth install, captured by Slack itself at consent
///   time (`connectors::slack`'s `authed_user_id`). That is a FIRST-PARTY
///   proof, strictly stronger than the email inference the org plane has to
///   settle for, and docs/connectors/slack.md §1's C5 row records that this
///   is exactly why the personal plane never needed the email matcher at all.
///   A personal account has one member, so "is this the installer" and "is
///   this a member of the account" are the same question. Every OTHER member
///   of the workspace correctly gets `false` here: an employee's personal
///   CoralSwarm install is not the workspace's shared property, and their
///   colleagues have no claim on its channel list.
///
/// * **Org** — `slack_interactivity::resolve_clicking_user`, the SAME single
///   identity path every routing decision already goes through (verified
///   email match, then an `account_members` check that the matched user is a
///   CURRENT member of THIS account). Deliberately reused rather than
///   reimplemented: a second matcher that drifts from the first is how a
///   membership revocation ends up honoured on one surface and not another.
///
/// The `account_members` half is not optional even on the org plane, and is
/// why this cannot just call `resolve_verified_slack_identity` directly: that
/// matcher answers "is there ANY CoralSwarm user with this verified email",
/// which would let a member of account A be rendered account B's data
/// whenever both accounts sit on one Slack workspace — the precise
/// cross-account read this whole fix is about, merely relocated one function
/// deeper.
///
/// **Org-plane pool split (the SAME wiring bug #516 fixed at the
/// `derive_status` call site).** `resolve_clicking_user` takes its own
/// `control_pool`/`tenant_pool` pair — see that function's doc comment for
/// the full split (`connector_identities`' directory read is against
/// `tenant_pool`, `users` is always `control_pool`). This function used to
/// pass `pool` for BOTH, under a comment claiming the org branch was
/// "unreachable in practice today" because org Slack OAuth was gated off.
/// That premise is dead — `ORG_SLACK_OAUTH_ENABLED` is LIVE IN PRODUCTION —
/// so this now resolves the account's real tenant pool via
/// [`resolve_app_home_tenant_pool`] (the same helper #516 added for
/// `derive_status`) before calling `resolve_clicking_user`, instead of
/// silently substituting `pool` for `tenant_pool`.
///
/// Severity note, checked rather than assumed: passing the wrong pool here
/// does NOT let a viewer be shown an account they do not belong to. The
/// actual identity match (`users.email`, inside
/// `match_and_persist_verified_email`) always runs on `control_pool`
/// regardless of what gets passed as `tenant_pool` — `control_pool` was
/// already correct before this fix — and the `account_members` membership
/// check `resolve_clicking_user` layers on top also always reads
/// `control_pool`. What the wrong pool actually breaks is the
/// `connector_identities` DIRECTORY-FIRST cache read/write:
/// a first-time org viewer still resolves correctly (falls through to the
/// live `users.info` + email match), but the confirmed link then gets
/// persisted into `control_pool`'s `connector_identities` table instead of
/// the org's real tenant-tree copy, so a viewer already linked there via a
/// DIFFERENT path (e.g. ordinary message ingest, which correctly uses the
/// tenant pool) never gets a directory hit here and re-runs a live Slack API
/// call and a stray duplicate persist on every Home tab open. A caching-
/// efficiency gap, not a cross-account disclosure — fixed anyway below since
/// this function is being edited regardless, and the stale "unreachable"
/// comment corrected so nobody re-derives the wrong severity from it.
#[allow(clippy::too_many_arguments)]
async fn resolve_app_home_account_for_viewer(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    http: &reqwest::Client,
    account_id: uuid::Uuid,
    plane: super::slack::AccountPlane,
    workspace_id: &str,
    bot_token: &str,
    slack_user_id: &str,
    clerk_org_id: Option<&str>,
) -> bool {
    if slack_user_id.is_empty() {
        return false;
    }

    match plane {
        super::slack::AccountPlane::Personal => {
            // Scoped + explicitly predicated on `account_id`. `oauth_tokens`
            // carries 0083's `account_isolation` policy, and since the
            // `coralswarm_app` role cutover (OPS-4 — run and verified in
            // production) the scope wrapper makes that policy a live second
            // layer; the `account_id = $2` predicate below remains the first.
            // Both, not either.
            let mut tx = match crate::tenancy::scope::begin_account_scoped(pool, account_id).await {
                Ok(tx) => tx,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        account_id = %account_id,
                        "app_home_opened: installer lookup scope failed — treating the viewer as \
                         unresolved (fail closed)"
                    );
                    return false;
                }
            };
            // Foundation PR 1.1: `owner_user_id IS NULL` — Slack has no
            // per-member OAuth (§14.1), the workspace's install row is always
            // account-wide.
            let is_installer: Option<bool> = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM oauth_tokens \
                 WHERE provider = 'slack' AND provider_workspace_id = $1 AND account_id = $2 \
                 AND provider_user_id = $3 AND owner_user_id IS NULL)",
            )
            .bind(workspace_id)
            .bind(account_id)
            .bind(slack_user_id)
            .fetch_one(&mut *tx)
            .await
            .map_err(|e| {
                tracing::warn!(
                    error = %e,
                    account_id = %account_id,
                    "app_home_opened: installer lookup failed — treating the viewer as unresolved \
                     (fail closed)"
                );
            })
            .ok();
            tx.rollback().await.ok();
            is_installer.unwrap_or(false)
        }
        super::slack::AccountPlane::Org => {
            // Resolve the SAME real tenant pool `derive_status` will later
            // use for this account (see this function's doc comment's
            // "Org-plane pool split" section) — never `pool` substituted for
            // both parameters. Failure to resolve one fails CLOSED (`false`):
            // there is no honest way to confirm membership without the real
            // tenant pool, and falling back to `pool` would reproduce the
            // exact class of bug this fix removes.
            let Some(tenant_pool) =
                resolve_app_home_tenant_pool(state, pool, account_id, plane, clerk_org_id).await
            else {
                return false;
            };

            super::slack_interactivity::resolve_clicking_user(
                pool,
                &tenant_pool,
                http,
                account_id,
                workspace_id,
                bot_token,
                slack_user_id,
            )
            .await
            .is_some()
        }
    }
}

/// The account-free Home view, published whenever no single account could be
/// confidently resolved for the Slack user who opened the tab. Deliberately
/// says nothing about which CoralSwarm accounts exist on this workspace, how
/// many there are, or whether the viewer's own workspace is connected —
/// "there are 2 CoralSwarm accounts here but you're not in either" is itself
/// a disclosure. Pure (no DB/network), like
/// [`render_onboarding_home_section`], so it is unit-testable directly.
fn render_generic_home_section() -> Vec<serde_json::Value> {
    vec![
        serde_json::json!({
            "type": "header",
            "text": { "type": "plain_text", "text": "CoralSwarm", "emoji": true }
        }),
        serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Open CoralSwarm to connect this Slack workspace and set up capture.\n\n\
                         If you already use CoralSwarm, sign in with the same email address as your \
                         Slack account and reopen this tab — we show your workspace's setup here \
                         once we can confirm which account you belong to."
            }
        }),
    ]
}

/// Render one connected workspace's
/// [`crate::connectors::slack_onboarding::SlackOnboardingStatus`] as a stack
/// of Block Kit blocks — the OTHER renderer of docs/connectors/
/// slack-onboarding.md §3's payload (the JSON one is
/// `routes::slack::onboarding_status`). Pure (no DB/network), so it is unit-
/// tested directly against a hand-built status value rather than requiring a
/// live `app_home_opened` event.
fn render_onboarding_home_section(
    workspace_name: &str,
    status: &crate::connectors::slack_onboarding::SlackOnboardingStatus,
) -> Vec<serde_json::Value> {
    use crate::connectors::slack_onboarding::ChannelsState;

    let mut blocks = vec![serde_json::json!({
        "type": "header",
        "text": { "type": "plain_text", "text": workspace_name, "emoji": true }
    })];

    if !status.connected {
        // Should not actually happen here — `handle_app_home_opened` only
        // calls `derive_status` for a workspace it already found a
        // `slack_integrations` row for — but handled rather than assumed,
        // since this function is also exercised directly by tests against a
        // hand-built `not_connected` status.
        blocks.push(serde_json::json!({
            "type": "section",
            "text": { "type": "mrkdwn", "text": "Not connected." }
        }));
        return blocks;
    }

    let channels_text = match &status.channels {
        Some(c) if c.state == ChannelsState::Ok => {
            let count = c.count.unwrap_or(0);
            if count == 0 {
                "*1. Invite the bot* — not in any channel yet. Run `/invite @CoralSwarm` in a \
                 channel to get started."
                    .to_string()
            } else {
                let names_suffix =
                    if c.names.is_empty() { String::new() } else { format!(" ({})", c.names.join(", ")) };
                format!(
                    "*1. Invite the bot* — in *{count}* channel{}{names_suffix}.",
                    if count == 1 { "" } else { "s" }
                )
            }
        }
        // "unavailable" renders as "couldn't check", never as "not in any
        // channels" — the same distinction the JSON contract enforces (see
        // `connectors::slack_onboarding::ChannelsStatus`'s doc comment).
        Some(c) => format!(
            "*1. Invite the bot* — couldn't check just now ({}). This is NOT the same as \"not in \
             any channels\" — try reopening this tab shortly.",
            c.error.as_deref().unwrap_or("unknown error")
        ),
        None => "*1. Invite the bot* — status unavailable.".to_string(),
    };
    blocks.push(serde_json::json!({"type": "section", "text": {"type": "mrkdwn", "text": channels_text}}));

    if let Some(r) = &status.routed {
        let fanout_suffix =
            if r.fanout > 0 { format!(" ({} to everyone's own ocean)", r.fanout) } else { String::new() };
        blocks.push(serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": format!(
                    "*2. Choose a destination* — {} channel{} explicitly routed{}.",
                    r.count, if r.count == 1 { "" } else { "s" }, fanout_suffix
                )
            }
        }));
    }

    if let Some(c) = &status.capture {
        let text = if c.atom_count == 0 {
            "*3. First capture* — nothing captured yet.".to_string()
        } else {
            let last = c.last_at.map(|t| t.to_rfc3339()).unwrap_or_default();
            format!(
                "*3. First capture* — {} item{} captured, most recently {last}.",
                c.atom_count,
                if c.atom_count == 1 { "" } else { "s" }
            )
        };
        blocks.push(serde_json::json!({"type": "section", "text": {"type": "mrkdwn", "text": text}}));
    }

    if let Some(h) = &status.huddles {
        // "skipped" is normal, not an error — rendered in a plain `context`
        // block (no warning styling), matching
        // `connectors::slack_onboarding::HuddlesStatus`'s own doc comment.
        let mut text = format!(
            "*Huddles* — {} seen, {} saved, {} skipped (\"not enough to summarize\" is the normal \
             case for a short huddle, not a failure).",
            h.seen, h.ingested, h.skipped
        );
        if !h.top_skip_reasons.is_empty() {
            let reasons: Vec<String> =
                h.top_skip_reasons.iter().map(|r| format!("{} ({})", r.reason, r.count)).collect();
            text.push_str(&format!("\nTop reasons: {}", reasons.join("; ")));
        }
        blocks.push(serde_json::json!({"type": "context", "elements": [{"type": "mrkdwn", "text": text}]}));
    }

    if let Some(i) = &status.identity {
        blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": format!("Identified {} of {} members by verified email.", i.identified, i.total)
            }]
        }));
    }

    blocks
}

#[cfg(test)]
mod app_home_render_tests {
    //! [`render_onboarding_home_section`] is the OTHER renderer of
    //! docs/connectors/slack-onboarding.md §3's payload (the JSON one is
    //! `routes::slack::onboarding_status`). Pure — no DB/network — so these
    //! tests build a [`SlackOnboardingStatus`] directly rather than driving a
    //! real `app_home_opened` event through the whole webhook stack.

    use super::{render_generic_home_section, render_onboarding_home_section};
    use crate::connectors::slack_onboarding::{
        CaptureStatus, ChannelsState, ChannelsStatus, HuddlesStatus, IdentityCoverage, Plane,
        RoutedStatus, SkipReasonCount, SlackOnboardingStatus, Viewer,
    };

    fn connected_status() -> SlackOnboardingStatus {
        SlackOnboardingStatus {
            connected: true,
            workspace_id: Some("T1".into()),
            workspace_name: Some("Acme".into()),
            plane: Plane::Personal,
            channels: Some(ChannelsStatus {
                state: ChannelsState::Ok,
                count: Some(2),
                names: vec!["#platform".into(), "#design".into()],
                error: None,
            }),
            routed: Some(RoutedStatus { count: 1, fanout: 0 }),
            capture: Some(CaptureStatus {
                atom_count: 142,
                first_at: Some(chrono::Utc::now()),
                last_at: Some(chrono::Utc::now()),
            }),
            huddles: Some(HuddlesStatus {
                seen: 5,
                ingested: 2,
                skipped: 3,
                top_skip_reasons: vec![SkipReasonCount { reason: "too short".into(), count: 2 }],
            }),
            identity: None,
            // The App Home resolves an ACCOUNT for its viewer, never a ROLE
            // — see `Viewer::Unresolved`. This renderer is deliberately
            // generic, so neither field reaches it.
            viewer_role: None,
            member: None,
        }
    }

    fn blocks_text(blocks: &[serde_json::Value]) -> String {
        blocks
            .iter()
            .filter_map(|b| {
                b.pointer("/text/text")
                    .or_else(|| b.pointer("/elements/0/text"))
                    .and_then(serde_json::Value::as_str)
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    #[test]
    fn not_connected_renders_a_short_notice_and_nothing_else() {
        let blocks =
            render_onboarding_home_section(
                "Acme",
                &SlackOnboardingStatus::not_connected(Plane::Personal, Viewer::Unresolved),
            );
        // Header block + one "Not connected." section — never a channels/
        // routed/capture/huddles block for a disconnected workspace.
        assert_eq!(blocks.len(), 2);
        assert!(blocks_text(&blocks).contains("Not connected."));
    }

    #[test]
    fn zero_channels_renders_the_invite_command_not_a_bare_zero() {
        let mut status = connected_status();
        status.channels = Some(ChannelsStatus {
            state: ChannelsState::Ok,
            count: Some(0),
            names: Vec::new(),
            error: None,
        });
        let blocks = render_onboarding_home_section("Acme", &status);
        let text = blocks_text(&blocks);
        assert!(text.contains("/invite @CoralSwarm"), "must surface the exact copyable command");
        assert!(!text.contains("couldn't check"), "a real zero must not read as a failed check");
    }

    #[test]
    fn unavailable_channels_never_renders_as_zero_channels() {
        let mut status = connected_status();
        status.channels = Some(ChannelsStatus {
            state: ChannelsState::Unavailable,
            count: None,
            names: Vec::new(),
            error: Some("conversations.list: rate-limited by Slack".to_string()),
        });
        let blocks = render_onboarding_home_section("Acme", &status);
        let text = blocks_text(&blocks);
        // FAILS WITHOUT THE FIX: a renderer that fell back to
        // `count.unwrap_or(0)` without checking `state` first would print
        // "in *0* channels" here — indistinguishable from the real-zero case
        // above, which is exactly the flattening this pins against.
        assert!(text.contains("couldn't check"));
        assert!(text.contains("rate-limited by Slack"));
        assert!(!text.contains("in *0* channel"));
    }

    #[test]
    fn huddle_skips_render_without_error_language() {
        let status = connected_status();
        let blocks = render_onboarding_home_section("Acme", &status);
        let text = blocks_text(&blocks);
        assert!(text.contains("3 skipped"));
        assert!(text.contains("too short (2)"));
        // The copy is allowed — expected, even — to explicitly reassure "not
        // a failure" in these words; what it must never do is carry actual
        // warning/error styling language (an "error"/"⚠️" marker), which is
        // what would make a client render this as a fault rather than
        // information.
        assert!(!text.contains("error"), "a normal skip must not read as an error: {text:?}");
        assert!(!text.contains('⚠'), "a normal skip must not carry a warning marker: {text:?}");
    }

    #[test]
    fn identity_omitted_on_personal_present_on_org() {
        let personal = connected_status();
        let personal_blocks = render_onboarding_home_section("Acme", &personal);
        assert!(!blocks_text(&personal_blocks).contains("Identified"));

        let mut org = connected_status();
        org.plane = Plane::Org;
        org.identity = Some(IdentityCoverage { identified: 8, total: 12 });
        let org_blocks = render_onboarding_home_section("Acme", &org);
        assert!(blocks_text(&org_blocks).contains("Identified 8 of 12 members"));
    }

    /// The fail-closed view published when no ONE account could be resolved
    /// for the Slack user who opened the tab. It must be constructible
    /// WITHOUT a `SlackOnboardingStatus` at all — that is the property that
    /// makes it impossible to accidentally leak an account into it — and it
    /// must not name any account-specific fact.
    #[test]
    fn generic_home_section_discloses_nothing_account_specific() {
        let text = blocks_text(&render_generic_home_section());
        assert!(text.contains("Open CoralSwarm"), "must still tell the viewer what to do: {text:?}");

        // Everything `render_onboarding_home_section` would have disclosed
        // for an account the viewer is NOT entitled to see. Spot-checking the
        // literal marker strings that renderer emits is deliberate: if
        // someone "helpfully" makes the generic view richer by reusing a
        // status section, one of these fires.
        for leaked in [
            "Invite the bot",
            "Choose a destination",
            "First capture",
            "Huddles",
            "Identified",
            "channel",
            "captured",
        ] {
            assert!(
                !text.contains(leaked),
                "the generic view must not carry {leaked:?} — it is published to viewers we could \
                 NOT tie to an account: {text:?}"
            );
        }
    }
}

#[cfg(test)]
mod app_home_switcher_tests {
    //! Pure, synchronous coverage of the account switcher's core safety
    //! property — [`super::other_entitled_accounts`] and
    //! [`super::build_account_switcher_block`] — with NO database and NO
    //! network call. The end-to-end "two accounts confirmed, one rendered"
    //! and "crafted payload refused" invariants live in
    //! `app_home_viewer_pg_tests` below (they need Postgres for
    //! `collect_entitled_app_home_accounts`/`handle_app_home_switch`); this
    //! module isolates the one piece of that pipeline that is pure enough to
    //! not need it — the exact thing that decides which OTHER accounts a
    //! viewer's dropdown offers, which is the property a regression here
    //! would silently break without either of the DB-backed tests failing
    //! LOUDLY (a subtle off-by-one in the filter could still leave `entitled`
    //! and the switcher's `views.publish` call both "succeeding").

    use super::{build_account_switcher_block, other_entitled_accounts, EntitledAppHomeAccount};
    use crate::routes::slack::AccountPlane;
    use uuid::Uuid;

    fn account(plane: AccountPlane, workspace_name: &str) -> EntitledAppHomeAccount {
        EntitledAppHomeAccount {
            account_id: Uuid::new_v4(),
            plane,
            workspace_id: "T1".to_string(),
            workspace_name: workspace_name.to_string(),
            bot_token: "xoxb-unused".to_string(),
            clerk_org_id: match plane {
                AccountPlane::Personal => None,
                AccountPlane::Org => Some(format!("org-{}", Uuid::new_v4())),
            },
        }
    }

    /// THE regression test for the safety property this whole feature rests
    /// on: with two entitled accounts, `other_entitled_accounts` must return
    /// exactly the OTHER one — never the currently-selected account, and
    /// never zero when a genuine other exists.
    ///
    /// Negative control performed by hand while writing this test: with the
    /// filter predicate in `other_entitled_accounts` changed from
    /// `a.account_id != selected.account_id` to the unconditional `true`
    /// (i.e. the exclusion removed), this test fails as:
    ///   assertion `left == right` failed: the selected account must never
    ///   appear as its own switch option
    ///     left: 2
    ///     right: 1
    /// — proving the test actually exercises the guard rather than passing
    /// vacuously. Restored immediately after (see this module's own doc
    /// comment; the guard is back to `!=` in the committed code).
    #[test]
    fn two_entitled_accounts_the_other_is_offered_never_the_selected_one() {
        let personal = account(AccountPlane::Personal, "Acme Personal");
        let org = account(AccountPlane::Org, "Acme Org");
        let entitled = vec![personal.clone(), org.clone()];

        let others = other_entitled_accounts(&personal, &entitled);
        assert_eq!(others.len(), 1, "the selected account must never appear as its own switch option");
        assert_eq!(others[0].account_id, org.account_id, "the ONE other entitled account must be offered");

        // Symmetric: switching FROM the org account offers the personal one.
        let others_from_org = other_entitled_accounts(&org, &entitled);
        assert_eq!(others_from_org.len(), 1);
        assert_eq!(others_from_org[0].account_id, personal.account_id);
    }

    /// A viewer entitled to exactly ONE account must get NO switch options —
    /// `render_app_home_for_viewer`'s `if !others.is_empty()` gate relies on
    /// this being genuinely empty, not just "falsy".
    #[test]
    fn single_entitled_account_has_no_others() {
        let only = account(AccountPlane::Personal, "Solo Co");
        let entitled = vec![only.clone()];
        assert!(
            other_entitled_accounts(&only, &entitled).is_empty(),
            "a single-entitlement viewer must get zero switch options — a switcher with one \
             option (itself) is not a real choice"
        );
    }

    /// The built `static_select` block's option identifies the OTHER
    /// account by `account_id` (never the selected one) and labels it with
    /// its workspace name and plane — the two pieces of information the
    /// design explicitly allows here (never a rendered onboarding status,
    /// which is what the module header's leak was about).
    #[test]
    fn switcher_block_options_carry_the_other_accounts_id_and_plane_label() {
        let selected = account(AccountPlane::Personal, "Acme Personal");
        let other = account(AccountPlane::Org, "Acme Org");
        let others = vec![&other];

        let block = build_account_switcher_block(&selected, &others);
        let options = block
            .pointer("/elements/0/options")
            .and_then(|v| v.as_array())
            .expect("switcher block must carry an options array");
        assert_eq!(options.len(), 1);

        let value = options[0].get("value").and_then(|v| v.as_str()).expect("option must carry a value");
        assert_eq!(value, other.account_id.to_string(), "the option value must be the OTHER account's id");
        assert_ne!(
            value,
            selected.account_id.to_string(),
            "the option must never carry the currently-selected account's id"
        );

        let text = options[0]
            .pointer("/text/text")
            .and_then(|v| v.as_str())
            .expect("option must carry label text");
        assert!(text.contains("Acme Org"), "label must name the OTHER workspace: {text:?}");
        assert!(text.contains("Org"), "label must disclose the OTHER account's plane: {text:?}");

        let action_id = block.get("action_id").and_then(|v| v.as_str());
        assert_eq!(
            action_id, None,
            "action_id belongs on the static_select element, not the actions block itself"
        );
        let element_action_id =
            block.pointer("/elements/0/action_id").and_then(|v| v.as_str()).unwrap();
        assert_eq!(element_action_id, super::APP_HOME_SWITCH_ACTION_ID);
    }
}

#[cfg(test)]
mod app_home_viewer_pg_tests {
    //! [`super::resolve_app_home_account_for_viewer`] is the authorization
    //! check that stops the App Home tab disclosing one CoralSwarm account's
    //! Slack setup to another account's people — see
    //! [`super::handle_app_home_opened`]'s doc comment for the leak it
    //! replaces. The decisive case (two accounts, ONE Slack workspace) cannot
    //! be expressed as a pure unit test: it is entirely about what a WHERE
    //! clause matches.
    //!
    //! In-crate (the function is private), gated on `ORGT_TEST_BASE_URL`,
    //! migrating the CONTROL tree — same convention and rationale as
    //! `slack_watched_channel_destination_tests` above, not re-argued here.
    //! Connects as the database OWNER, so RLS is inert IN THESE TESTS and
    //! they prove the predicate itself, not the policy. (Production has since
    //! cut over to the non-`BYPASSRLS` `coralswarm_app` role — OPS-4 has run
    //! — so the policy is live there; the owner connection here is what keeps
    //! these an honest proof of the predicate layer on its own.)
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib app_home_viewer_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::resolve_app_home_account_for_viewer;
    use crate::routes::slack::AccountPlane;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn setup(db: &str) -> Option<(PgConnection, PgPool)> {
        let base = base_url()?;
        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.unwrap();
        admin_conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        Some((admin_conn, pool))
    }
    async fn teardown(mut admin_conn: PgConnection, pool: PgPool, db: &str) {
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A personal account plus the `oauth_tokens` row the control-plane-only
    /// OAuth callback writes, whose `provider_user_id` is the Slack id of the
    /// member who ran the install.
    async fn seed_personal_install(pool: &PgPool, workspace_id: &str, installer_slack_id: &str) -> Uuid {
        let user_id: Uuid =
            sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id")
                .bind(format!("{}@example.com", Uuid::new_v4()))
                .fetch_one(pool)
                .await
                .unwrap();
        let account_id: Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(user_id)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query("INSERT INTO account_members(account_id, user_id, role) VALUES ($1, $2, 'owner')")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, \
                 provider_user_id, provider_workspace_id) \
             VALUES ($1, 'slack', 'chat:write', 'unused-ciphertext', $2, $3)",
        )
        .bind(account_id)
        .bind(installer_slack_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
        account_id
    }

    /// In-app org deletion (control 0387): a member of a deleting org gets no
    /// App Home org content (entitlement false, no tenant pool), and an undo
    /// restores it. Message capture's `resolve_org_plane` is not touched.
    #[tokio::test]
    async fn app_home_shows_a_locked_out_member_no_org_content_and_an_undo_restores_it_pg() {
        let db = "orgt_app_home_org_deletion";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping app_home_shows_a_locked_out_member_no_org_content_and_an_undo_restores_it_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let owner: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'o@x.test') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let member: Uuid = sqlx::query_scalar("INSERT INTO users(id, email) VALUES (gen_random_uuid(), 'm@x.test') RETURNING id")
            .fetch_one(&pool).await.unwrap();
        let org = format!("org-{}", Uuid::new_v4());
        let account: Uuid = sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(owner).bind(&org).fetch_one(&pool).await.unwrap();
        for (u, role) in [(owner, "owner"), (member, "member")] {
            sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, $3, (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
                .bind(account).bind(u).bind(role).execute(&pool).await.unwrap();
        }
        sqlx::query("INSERT INTO connector_identities (provider, workspace_id, external_user_id, canonical_name, user_id, link_method, link_verified_at) VALUES ('slack', 'T-ORGDEL', 'U-ORGDEL-MEMBER', 'M', $1, 'verified_email', now())")
            .bind(member).execute(&pool).await.unwrap();
        let state = crate::test_app_state();
        let http = reqwest::Client::builder().timeout(std::time::Duration::from_secs(5)).build().unwrap();
        let entitled = || super::resolve_app_home_account_for_viewer(
            &state, &pool, &http, account, AccountPlane::Org, "T-ORGDEL", "xoxb-garbage", "U-ORGDEL-MEMBER", Some(&org),
        );
        let tenant_pool = || super::resolve_app_home_tenant_pool(&state, &pool, account, AccountPlane::Org, Some(&org));

        assert!(entitled().await, "baseline: a member sees the org");
        assert!(tenant_pool().await.is_some());

        let deletion: Uuid = sqlx::query_scalar("INSERT INTO org_deletions (account_id, clerk_org_id, initiated_by) VALUES ($1, $2, 'owner') RETURNING id")
            .bind(account).bind(&org).fetch_one(&pool).await.unwrap();
        for live in ["requested", "grace", "awaiting_approval", "destroying"] {
            if live == "destroying" {
                sqlx::query("UPDATE org_deletions SET state = 'destroying', destroying_since = now(), approval_required = false WHERE id = $1")
                    .bind(deletion).execute(&pool).await.unwrap();
            } else {
                sqlx::query("UPDATE org_deletions SET state = $2 WHERE id = $1").bind(deletion).bind(live).execute(&pool).await.unwrap();
            }
            assert!(!entitled().await, "{live}: no entitlement, so no review items / onboarding for the org");
            assert!(tenant_pool().await.is_none(), "{live}: no tenant pool for App Home");
            // System cleanup is not a member-facing read: a Slack uninstall
            // during the deletion still purges watched channels and seats.
            assert!(
                super::resolve_disconnect_tenant_pool(&state, &pool, account, AccountPlane::Org, Some(&org)).await.is_some(),
                "{live}: the full-disconnect purge still gets a pool"
            );
            // No seat for a joiner and no invite email for a closing org.
            assert!(crate::services::org_deletion::account_org_closing_down(&pool, account).await, "{live}: seat gate");
            assert!(!super::org_allows_unmatched_email(&pool, Some(&org)).await, "{live}: unmatched-email gate");
        }
        sqlx::query("UPDATE org_deletions SET state = 'canceled', canceled_at = now() WHERE id = $1")
            .bind(deletion).execute(&pool).await.unwrap();
        assert!(entitled().await, "undo restores App Home");
        assert!(tenant_pool().await.is_some());
        assert!(!crate::services::org_deletion::account_org_closing_down(&pool, account).await, "undo reopens seating");
        assert!(super::org_allows_unmatched_email(&pool, Some(&org)).await, "undo reopens the invite email");
        assert!(super::org_allows_unmatched_email(&pool, None).await, "a personal account is never gated");
        teardown(admin_conn, pool, db).await;
    }

    /// THE regression test for the Critical. Two independent CoralSwarm
    /// accounts install on the SAME Slack workspace `T1` — a supported shape
    /// (`resolve_slack_account_planes` returns both, which is why the App
    /// Home handler had a loop at all). Each account's installer must resolve
    /// to their OWN account and to NOTHING else.
    ///
    /// FAILS WITHOUT THE FIX in the strongest sense available: before this,
    /// there was no per-viewer check at all — every one of these calls was
    /// effectively `true`, and both accounts' sections were rendered into one
    /// published view for whoever opened the tab.
    #[tokio::test]
    async fn personal_plane_matches_only_the_installer_of_that_account_pg() {
        let db = "orgt_app_home_viewer";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping personal_plane_matches_only_the_installer_of_that_account_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let http = reqwest::Client::new();

        let account_a = seed_personal_install(&pool, "T1", "U-ALICE").await;
        let account_b = seed_personal_install(&pool, "T1", "U-BOB").await;

        let state = crate::test_app_state();
        let check = |account: Uuid, slack_user: &'static str| {
            let pool = pool.clone();
            let http = http.clone();
            let state = state.clone();
            async move {
                resolve_app_home_account_for_viewer(
                    &state,
                    &pool,
                    &http,
                    account,
                    AccountPlane::Personal,
                    "T1",
                    "xoxb-unused-on-the-personal-path",
                    slack_user,
                    None,
                )
                .await
            }
        };

        assert!(check(account_a, "U-ALICE").await, "the installer must see their own account");
        assert!(check(account_b, "U-BOB").await, "…and so must the other account's installer");

        // The leak, stated directly: Bob opening the App Home must NOT
        // resolve Alice's account, even though both live on workspace T1.
        assert!(
            !check(account_a, "U-BOB").await,
            "account B's installer must not be shown account A's data on a shared workspace"
        );
        assert!(!check(account_b, "U-ALICE").await, "…and symmetrically");

        // A workspace member with no CoralSwarm install at all — the common
        // case, and the one that made this Critical rather than Major: any
        // member of the workspace could open the tab.
        assert!(!check(account_a, "U-CAROL").await);
        assert!(!check(account_b, "U-CAROL").await);

        // Empty Slack id → no identity, fail closed (never "matches the row
        // whose provider_user_id is also empty").
        assert!(!check(account_a, "").await);

        // Right Slack user, WRONG workspace: an install on T2 must not be
        // satisfied by a T1 event. `provider_workspace_id` is part of the
        // predicate, not decoration.
        let account_c = seed_personal_install(&pool, "T2", "U-DAVE").await;
        assert!(
            !resolve_app_home_account_for_viewer(
                &state,
                &pool,
                &http,
                account_c,
                AccountPlane::Personal,
                "T1",
                "xoxb-unused-on-the-personal-path",
                "U-DAVE",
                None,
            )
            .await,
            "an install on a different workspace must not authorize a T1 viewer"
        );

        teardown(admin_conn, pool, db).await;
    }

    /// A DB error inside the check must read as "unresolved", never as
    /// "authorized". Cheapest faithful stand-in for any transient failure:
    /// the table the predicate selects from is gone.
    #[tokio::test]
    async fn a_failed_installer_lookup_fails_closed_pg() {
        let db = "orgt_app_home_viewer_faildb";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping a_failed_installer_lookup_fails_closed_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let http = reqwest::Client::new();
        let state = crate::test_app_state();
        let account_id = seed_personal_install(&pool, "T1", "U-ALICE").await;

        sqlx::query("DROP TABLE oauth_tokens").execute(&pool).await.unwrap();

        assert!(
            !resolve_app_home_account_for_viewer(
                &state,
                &pool,
                &http,
                account_id,
                AccountPlane::Personal,
                "T1",
                "xoxb-unused-on-the-personal-path",
                "U-ALICE",
                None,
            )
            .await,
            "a failed lookup must degrade to 'not this viewer's account', never to authorized"
        );

        teardown(admin_conn, pool, db).await;
    }

    // ── The account switcher — entitled-set collection + crafted-payload
    //    refusal ─────────────────────────────────────────────────────────────
    //
    // `resolve_app_home_account_for_viewer` above proves the per-account
    // predicate. These prove the LOOP built on top of it
    // (`collect_entitled_app_home_accounts`) and the interactivity handler
    // built on top of THAT (`handle_app_home_switch`) — the two pieces that
    // did not exist before the switcher and are exactly where a fan-out bug
    // or a "trust the payload" bug would actually surface.

    /// [`seed_personal_install`] plus a REAL decryptable `oauth_tokens.
    /// access_token` (so `collect_entitled_app_home_accounts`'s
    /// `get_slack_bot_token_pub` call succeeds) and a `slack_integrations`
    /// row (so its workspace-metadata lookup succeeds).
    ///
    /// Foundation PR 1.2 (CS-031): `get_slack_bot_token` resolves the account
    /// DEK via `crate::crypto::resolve_dek` now (the SAME wrapped DEK
    /// content encryption uses), not the retired `ACCOUNT_MASTER_KEK`-XOR
    /// derivation — so this provisions a REAL wrapped DEK on
    /// `accounts.dek_wrapped` (mirroring `clerk_provision::
    /// provision_account_dek`, same idiom as this file's own `seed_dek`
    /// below) and encrypts the fake token under that SAME DEK. Requires
    /// `MASTER_KEK_HEX` to already be set — callers hold
    /// `crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK` for their whole
    /// test body (the same lock this file's other `MASTER_KEK_HEX`-touching
    /// tests already share crate-wide).
    async fn seed_full_personal_account(
        pool: &PgPool,
        workspace_id: &str,
        workspace_name: &str,
        installer_slack_id: &str,
    ) -> Uuid {
        let account_id = seed_personal_install(pool, workspace_id, installer_slack_id).await;
        let kek = coralswarm_core::kek::load_kek()
            .expect("MASTER_KEK_HEX must be set before calling seed_full_personal_account");
        let dek = coralswarm_core::kek::generate_dek();
        let wrapped =
            coralswarm_core::kek::wrap_dek(&kek, &dek).expect("wrap_dek must succeed with a valid KEK");
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("seed dek_wrapped");
        let encrypted = coralswarm_core::oauth_token::encrypt_token(&dek, "xoxb-test-token")
            .expect("encrypt_token must succeed");
        sqlx::query("UPDATE oauth_tokens SET access_token = $1 WHERE account_id = $2")
            .bind(encrypted)
            .bind(account_id)
            .execute(pool)
            .await
            .unwrap();
        sqlx::query(
            "INSERT INTO slack_integrations(account_id, workspace_id, workspace_name, bot_token) \
             VALUES ($1, $2, $3, 'xoxb-unused-dev-token')",
        )
        .bind(account_id)
        .bind(workspace_id)
        .bind(workspace_name)
        .execute(pool)
        .await
        .unwrap();
        account_id
    }

    /// THE regression test for the switcher's dropdown-population half: the
    /// exact production shape (one Slack user entitled to TWO CoralSwarm
    /// accounts on the same workspace — here, two personal accounts each
    /// installed BY THE SAME Slack member, the closest DB-only stand-in for
    /// "a personal install plus their employer's org account" that does not
    /// require a live tenant database or a network call) must produce an
    /// entitled set containing BOTH accounts, in
    /// `resolve_slack_account_planes`' order.
    #[tokio::test]
    async fn collect_entitled_returns_both_accounts_for_a_dual_entitled_viewer_pg() {
        let db = "orgt_app_home_switch_dual";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping collect_entitled_returns_both_accounts_for_a_dual_entitled_viewer_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_guard` (held for this test's whole body) serializes every
        // `MASTER_KEK_HEX` mutation against any other test that touches it.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x22u8; 32]));
        }

        let account_a = seed_full_personal_account(&pool, "T1", "Acme A", "U-ALICE").await;
        let account_b = seed_full_personal_account(&pool, "T1", "Acme B", "U-ALICE").await;

        let state = crate::test_app_state();
        let (entitled, publish_token) =
            super::collect_entitled_app_home_accounts(&state, &pool, "T1", "U-ALICE")
                .await
                .expect("a workspace with matched accounts must return Some");

        assert!(publish_token.is_some(), "at least one matched account has a usable bot token");
        assert_eq!(
            entitled.len(),
            2,
            "a viewer confirmed for BOTH accounts must get BOTH in the entitled set, not just the \
             first (this is the exact `break` this feature removes)"
        );
        let ids: std::collections::HashSet<Uuid> = entitled.iter().map(|a| a.account_id).collect();
        assert!(ids.contains(&account_a) && ids.contains(&account_b));

        // Ties directly into the switcher gate: feeding this REAL entitled
        // set into `other_entitled_accounts` must offer exactly the OTHER
        // account, never the selected one — connecting the DB-derived set to
        // the pure dropdown-filtering property `app_home_switcher_tests`
        // proves in isolation.
        let others = super::other_entitled_accounts(&entitled[0], &entitled);
        assert_eq!(others.len(), 1);
        assert_ne!(others[0].account_id, entitled[0].account_id);

        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);
        teardown(admin_conn, pool, db).await;
    }

    /// The mirror image: a viewer entitled to exactly ONE account on the
    /// workspace (a second account exists, installed by someone else) must
    /// get an entitled set of length ONE — so `render_app_home_for_viewer`'s
    /// dropdown gate sees a genuinely empty "others" and renders no switcher.
    #[tokio::test]
    async fn collect_entitled_returns_exactly_one_for_a_single_entitled_viewer_pg() {
        let db = "orgt_app_home_switch_single";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping collect_entitled_returns_exactly_one_for_a_single_entitled_viewer_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x33u8; 32]));
        }

        let account_a = seed_full_personal_account(&pool, "T1", "Acme A", "U-ALICE").await;
        let _account_b = seed_full_personal_account(&pool, "T1", "Acme B", "U-BOB").await;

        let state = crate::test_app_state();
        let (entitled, _publish_token) =
            super::collect_entitled_app_home_accounts(&state, &pool, "T1", "U-ALICE")
                .await
                .expect("a workspace with matched accounts must return Some");

        assert_eq!(entitled.len(), 1, "Alice is entitled to exactly her own account, not Bob's");
        assert_eq!(entitled[0].account_id, account_a);
        assert!(
            super::other_entitled_accounts(&entitled[0], &entitled).is_empty(),
            "a single-entitlement viewer must get zero switch options"
        );

        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);
        teardown(admin_conn, pool, db).await;
    }

    /// **The security test.** A crafted `block_actions` payload names an
    /// `account_id` the viewer is NOT entitled to (a random uuid — not even a
    /// real account, the strongest version of "attacker-controlled value").
    /// `handle_app_home_switch` must refuse: return
    /// [`super::AppHomeSwitchOutcome::Refused`] WITHOUT ever calling
    /// `views.publish` (unreachable on this path — see that function's doc
    /// comment), so nothing about the requested account, or anything else,
    /// changes on the Home tab.
    ///
    /// Negative control performed by hand while writing this test: with
    /// `handle_app_home_switch`'s re-confirmation changed from
    /// `entitled.iter().find(|a| a.account_id == requested)` to
    /// `entitled.first()` (i.e. "trust that SOME account matched, ignore
    /// which one was actually requested" — the exact bug class this design
    /// exists to prevent), this test no longer completes: with the guard
    /// intact all five tests in this module finish in ~2s total; with the
    /// guard replaced by `entitled.first()` this ONE test alone still had not
    /// returned after 20 wall-clock seconds and had to be killed — it had
    /// proceeded past the refusal point toward `render_app_home_for_viewer`/
    /// `views.publish` for Alice's own account and was blocked on a real
    /// network call to `slack.com` with a fake bearer token, never reaching
    /// an assertion at all. That stark timing difference (milliseconds vs.
    /// "still hanging at 20s") is the proof this test actually exercises the
    /// re-confirmation guard rather than passing vacuously. Restored
    /// immediately after; the guard is back to the `.find(...)` form in the
    /// committed code, and the full module is back to completing in ~2s.
    #[tokio::test]
    async fn switch_refuses_a_crafted_account_id_the_viewer_is_not_entitled_to_pg() {
        let db = "orgt_app_home_switch_crafted";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", db);
        let Some((admin_conn, pool)) = setup(db).await else {
            eprintln!("skipping switch_refuses_a_crafted_account_id_the_viewer_is_not_entitled_to_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let _guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x44u8; 32]));
        }

        let _account_a = seed_full_personal_account(&pool, "T1", "Acme A", "U-ALICE").await;

        let mut state_owned = crate::test_app_state_owned(None);
        state_owned.db = Some(pool.clone());
        let state = std::sync::Arc::new(state_owned);

        let crafted = Uuid::new_v4();
        let outcome = super::handle_app_home_switch(
            state,
            "T1".to_string(),
            "U-ALICE".to_string(),
            crafted.to_string(),
        )
        .await;

        assert_eq!(
            outcome,
            super::AppHomeSwitchOutcome::Refused,
            "a viewer must never be switched to an account they are not confirmed to belong to, \
             no matter what the interaction payload names"
        );

        match prior_kek {
            Some(v) => unsafe { std::env::set_var("MASTER_KEK_HEX", v) },
            None => unsafe { std::env::remove_var("MASTER_KEK_HEX") },
        }
        drop(_guard);
        teardown(admin_conn, pool, db).await;
    }
}

#[cfg(test)]
mod app_home_tenant_pool_pg_tests {
    //! [`super::resolve_app_home_tenant_pool`] — the pool-split fix for
    //! `app_home_opened`'s onboarding-status derivation. See
    //! [`super::handle_app_home_opened`]'s doc comment's "Pool split"
    //! section for the bug this exists to fix: an Org-plane account's
    //! `tenant_pool` argument to `derive_status` used to silently read off
    //! `control_pool` instead, rendering a real org's live Slack ingest as
    //! "nothing captured yet" on its own App Home tab.
    //!
    //! In-crate (the function under test is private), gated on
    //! `ORGT_TEST_BASE_URL`, same convention as
    //! `slack_watched_channel_destination_tests` above — not re-argued here.
    //! Needs an EXPLICIT `cargo test --lib <module>` step (added to the
    //! `cross-tenant-probe` job in this PR): the workspace `cargo test` job
    //! has no Postgres service and would silently skip every test here.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib app_home_tenant_pool_pg_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use std::collections::HashMap;
    use std::sync::Arc;
    use uuid::Uuid;

    use super::resolve_app_home_tenant_pool;
    use crate::routes::slack::AccountPlane;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::routing::TenantStatus;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str()).await.unwrap();
        conn
    }
    async fn drop_db(admin_conn: &mut PgConnection, db: &str) {
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A control-only org account: `users` + `accounts(clerk_org_id = ...)`.
    /// No `account_members` row — [`super::resolve_app_home_tenant_pool`]
    /// never reads that table.
    async fn seed_org_account(pool: &PgPool, clerk_org_id: &str) -> Uuid {
        let user_id: Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("owner-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query_scalar("INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id")
            .bind(user_id)
            .bind(clerk_org_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Mint an org's `tenant_databases` routing row at an explicit
    /// [`TenantStatus`] — parameterized (unlike
    /// `slack_watched_channel_destination_tests`'s always-`Active` helper of
    /// the same name) so this module can also seed `Suspended`/`Provisioning`
    /// rows for the not-servable proof.
    async fn mint_tenant_databases_row(
        control_pool: &PgPool,
        org_id: &str,
        slug: &str,
        account_id: Uuid,
        secret_ref: &str,
        status: TenantStatus,
    ) {
        // Enter the real account/period financial transaction for this guarded fixture write.
        let mut fixture_financial_tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut fixture_financial_tx, account_id)
            .await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases \
                (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
             VALUES ($1, $2, $3, $4, 'neon', 1, $5)",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(status.as_str())
        .execute(&mut *fixture_financial_tx)
        .await
        .unwrap();
        fixture_financial_tx.commit().await.unwrap();
    }

    /// Build (connect + migrate the TENANT tree + stamp `tenant_meta`) a
    /// second scratch database standing in for one org's real tenant DB —
    /// same shape as `slack_watched_channel_destination_tests`'s helper of
    /// the same name.
    async fn build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    /// The decisive assertion every test below reduces to: which PHYSICAL
    /// database did the resolved pool actually connect to.
    async fn current_database(pool: &PgPool) -> String {
        sqlx::query_scalar("SELECT current_database()").fetch_one(pool).await.unwrap()
    }

    fn test_state(control: &PgPool, runtime: Option<Arc<TenantRuntime>>) -> Arc<crate::AppState> {
        let mut state = crate::test_app_state_owned(runtime);
        state.db = Some(control.clone());
        Arc::new(state)
    }

    fn empty_resolver() -> StaticMapResolver {
        StaticMapResolver::new(HashMap::new())
    }

    /// Personal accounts are unaffected by this fix: `tenant_pool` stays the
    /// SAME physical pool as `control_pool`, and no `TenantRuntime` at all is
    /// needed — the Personal branch must never require one.
    #[tokio::test]
    async fn personal_plane_resolves_to_the_control_pool_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping personal_plane_resolves_to_the_control_pool_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let ctl_db = "orgt_apphome_pool_personal";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_0 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let state = test_state(&control, None);
        let resolved =
            resolve_app_home_tenant_pool(&state, &control, Uuid::new_v4(), AccountPlane::Personal, None)
                .await;
        let resolved = resolved.expect("Personal must always resolve a pool");
        assert_eq!(current_database(&resolved).await, ctl_db);

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }

    /// THE regression test. An Org-plane account with an `Active` tenant
    /// database must resolve ITS OWN tenant pool, never `control_pool`.
    ///
    /// FAILS WITHOUT THE FIX (verified by hand: temporarily made the `Org`
    /// arm of `resolve_app_home_tenant_pool` return
    /// `Some(control_pool.clone())` unconditionally — the exact shape of the
    /// original bug — and reran; `current_database(&resolved)` came back
    /// `"orgt_apphome_pool_ctl"` instead of `"orgt_apphome_pool_tnt"`,
    /// failing the first assertion below with a clear expected/actual
    /// mismatch. Restored before committing; see this PR's report for the
    /// diff-verify.) This is the regression guard for the production
    /// incident this PR fixes: workspace T6D4612D34B / org account
    /// `f0f92fc0`'s Home tab reading "nothing captured yet" while its
    /// tenant-plane ingest ran fine.
    #[tokio::test]
    async fn org_with_active_tenant_resolves_to_its_own_tenant_pool_not_control_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_with_active_tenant_resolves_to_its_own_tenant_pool_not_control_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_apphome_pool_ctl", "orgt_apphome_pool_tnt");
        let _database_cleanup_1 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        {
            let _database_cleanup_2 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
            let conn = recreate(&base, tnt_db).await;
            conn.close().await.ok();
        }
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_apphome_{}", Uuid::new_v4());
        let account_id = seed_org_account(&control, &org_id).await;
        mint_tenant_databases_row(
            &control,
            &org_id,
            "apphome-org",
            account_id,
            "SECRET_APPHOME",
            TenantStatus::Active,
        )
        .await;
        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let resolver = StaticMapResolver::from_pairs([("SECRET_APPHOME", with_db(&base, tnt_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let state = test_state(&control, Some(runtime));

        let resolved = resolve_app_home_tenant_pool(
            &state,
            &control,
            account_id,
            AccountPlane::Org,
            Some(&org_id),
        )
        .await;
        let resolved = resolved.expect("an Active tenant row must resolve to a real tenant pool");
        assert_eq!(
            current_database(&resolved).await,
            tnt_db,
            "an Org account's tenant_pool must be its OWN tenant database, never control"
        );
        assert_ne!(
            current_database(&resolved).await,
            current_database(&control).await,
            "must never resolve to the same physical database as control"
        );

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            drop_db(&mut admin_conn, db).await;
        }
    }

    /// An org with NO `tenant_databases` row at all (never onboarded onto a
    /// tenant database) is not an error — its content genuinely still lives
    /// on the consumer plane, same as `enqueue_slack_message_ingest`'s
    /// identical `Missing` case (`resolve_org_plane`'s doc comment).
    #[tokio::test]
    async fn org_never_onboarded_falls_back_to_control_pool_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_never_onboarded_falls_back_to_control_pool_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let ctl_db = "orgt_apphome_pool_missing";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_3 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_apphome_missing_{}", Uuid::new_v4());
        let account_id = seed_org_account(&control, &org_id).await;
        // Deliberately NO tenant_databases row for this org.

        let registry = Arc::new(TenantRegistry::new(Arc::new(empty_resolver())));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let state = test_state(&control, Some(runtime));

        let resolved = resolve_app_home_tenant_pool(
            &state,
            &control,
            account_id,
            AccountPlane::Org,
            Some(&org_id),
        )
        .await;
        let resolved = resolved.expect("a never-onboarded org must fall back to the control pool");
        assert_eq!(current_database(&resolved).await, ctl_db);

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }

    /// A revoked org must NEVER resolve any pool — not control, not tenant —
    /// even though its routing row is still `Active`.
    #[tokio::test]
    async fn org_revoked_returns_none_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_revoked_returns_none_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let ctl_db = "orgt_apphome_pool_revoked";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_4 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_apphome_revoked_{}", Uuid::new_v4());
        let account_id = seed_org_account(&control, &org_id).await;
        mint_tenant_databases_row(
            &control,
            &org_id,
            "apphome-revoked",
            account_id,
            "SECRET_REVOKED",
            TenantStatus::Active,
        )
        .await;
        sqlx::query("INSERT INTO org_revocations(clerk_org_id) VALUES ($1)")
            .bind(&org_id)
            .execute(&control)
            .await
            .unwrap();

        let registry = Arc::new(TenantRegistry::new(Arc::new(empty_resolver())));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let state = test_state(&control, Some(runtime));

        let resolved = resolve_app_home_tenant_pool(
            &state,
            &control,
            account_id,
            AccountPlane::Org,
            Some(&org_id),
        )
        .await;
        assert!(
            resolved.is_none(),
            "a revoked org must never resolve a pool for App Home either"
        );

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }

    /// A routing row that EXISTS but is not `Active` (mid-migration or torn
    /// down) must resolve to `None`, never a guessed pool.
    #[tokio::test]
    async fn org_routing_row_not_servable_returns_none_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_routing_row_not_servable_returns_none_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let ctl_db = "orgt_apphome_pool_notservable";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_5 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_apphome_suspended_{}", Uuid::new_v4());
        let account_id = seed_org_account(&control, &org_id).await;
        mint_tenant_databases_row(
            &control,
            &org_id,
            "apphome-suspended",
            account_id,
            "SECRET_SUSPENDED",
            TenantStatus::Suspended,
        )
        .await;

        let registry = Arc::new(TenantRegistry::new(Arc::new(empty_resolver())));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let state = test_state(&control, Some(runtime));

        let resolved = resolve_app_home_tenant_pool(
            &state,
            &control,
            account_id,
            AccountPlane::Org,
            Some(&org_id),
        )
        .await;
        assert!(resolved.is_none(), "a suspended routing row must never resolve a pool");

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }

    /// A tenant pool BUILD failure (here: the resolver has no entry at all
    /// for the row's `db_secret_ref` — a deterministic stand-in for any
    /// transient secret-resolution/connect failure) must resolve to `None`,
    /// never a fallback to `control_pool`.
    #[tokio::test]
    async fn org_pool_build_failure_returns_none_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_pool_build_failure_returns_none_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let ctl_db = "orgt_apphome_pool_buildfail";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_6 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_apphome_buildfail_{}", Uuid::new_v4());
        let account_id = seed_org_account(&control, &org_id).await;
        mint_tenant_databases_row(
            &control,
            &org_id,
            "apphome-buildfail",
            account_id,
            "SECRET_NOWHERE", // deliberately absent from the resolver below
            TenantStatus::Active,
        )
        .await;

        let registry = Arc::new(TenantRegistry::new(Arc::new(empty_resolver())));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let state = test_state(&control, Some(runtime));

        let resolved = resolve_app_home_tenant_pool(
            &state,
            &control,
            account_id,
            AccountPlane::Org,
            Some(&org_id),
        )
        .await;
        assert!(
            resolved.is_none(),
            "an unresolvable tenant secret must resolve to None, never control as a fallback"
        );

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }

    /// Invariant break: `plane == Org` but `clerk_org_id` is `None` (should be
    /// unreachable given `account_plane`'s own contract — see this function's
    /// doc comment). Must fail closed rather than panic or guess a pool.
    #[tokio::test]
    async fn org_plane_without_clerk_org_id_returns_none_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping org_plane_without_clerk_org_id_returns_none_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let ctl_db = "orgt_apphome_pool_noorgid";
        let _cleanup_ctl_db = crate::test_db::TestDatabase::adopt_from_env("ORGT_TEST_BASE_URL", ctl_db);
        let _database_cleanup_7 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let state = test_state(&control, None);
        let resolved =
            resolve_app_home_tenant_pool(&state, &control, Uuid::new_v4(), AccountPlane::Org, None)
                .await;
        assert!(resolved.is_none(), "Org plane with no clerk_org_id must fail closed to None");

        control.close().await;
        drop_db(&mut admin_conn, ctl_db).await;
    }
}

/// Resolve `team_id` → `account_id` and durably enqueue the huddle's
/// `jobs::JobKind::SlackHuddleCapture` job.
///
/// # Durability trade (A): called INLINE, on the pre-response path
///
/// This function is now `.await`ed directly from the handler, on the path to
/// Slack's 200/503 — NOT spawned. Every other background dispatch in this
/// file (message ingest, member-joined prompts) deliberately stays a
/// detached spawn precisely so a slow/saturated pool
/// never turns high-volume traffic into duplicate Slack deliveries; this is
/// the ONE exception, and the trade is explicit:
///
/// * **The trade**: this puts a DB round trip (two indexed statements —
///   `resolve_oauth_account_id` then `jobs::queue::enqueue`'s
///   `INSERT ... ON CONFLICT`) on the pre-response path, something the rest
///   of this handler deliberately avoids.
/// * **Why it's acceptable HERE ONLY**: the huddle trigger fires AT MOST ONCE
///   per huddle — there is no natural Slack retry for it (unlike a message
///   edit, which Slack will simply re-deliver on its own timeline). A
///   previous version enqueued via a bare detached `tokio::spawn`, whose
///   every failure path only logged: a transient DB error, pool saturation,
///   or the process exiting in the window between the 200 and the INSERT
///   lost the huddle's content FOREVER, with nothing louder than a `warn!`.
///   Returning 503 on failure instead makes Slack redeliver the same
///   `message_changed` event, giving this exactly the at-least-once
///   durability the deleted `reaction_added` branch had for the same class
///   of error (see that branch's post-mortem in this module's git history).
/// * **Why the ordinary message path is UNCHANGED**: `ingest_slack_message`
///   fires on every plain message, at whatever volume the workspace
///   produces, and stays a detached background spawn — the huddle
///   trigger's "exactly once, no redelivery" property is what makes this
///   trade safe, and that property does NOT hold for ordinary messages
///   (Slack does redeliver a message webhook on a slow/failed ack, so a lost
///   in-process ingest attempt is not a permanent loss the way a lost huddle
///   trigger is).
///
/// `state.db` being entirely absent (no pool at all) is handled by the
/// caller, not here — see the call site's comment.
async fn enqueue_huddle_capture(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    team_id: &str,
    trigger: &HuddleTrigger,
) -> Result<(), String> {
    // Plane-explicit and fanning out, matching `enqueue_slack_message_ingest`.
    //
    // This function used to take ONE pool (its caller passed `state.db` — the
    // CONTROL pool) and resolve ONE account via the singular
    // `resolve_slack_account_id`. Both were wrong once org-slack step 4 moved
    // `workers::huddle` onto the tenant loop:
    //
    //  * PLANE. The worker dequeues from `t.pool`, so an org account's job —
    //    written to control — was claimed by the harness's CONSUMER entry
    //    (`queue::dequeue` filters by KIND only). `capture_huddle` then ran
    //    with `t.tenant = Consumer` while `account_plane()` correctly said
    //    `Org`, so the org fan-out read `connector_identities` from control
    //    instead of the tenant, found none, and recorded a legitimate-looking
    //    `Skipped` — every huddle in an org workspace silently lost. This is
    //    the exact rule stated on `ingest_slack_message`'s doc: an enqueue and
    //    its worker must flip planes together, or content silently vanishes.
    //    (This module's own doc claimed the two moved in the same commit. The
    //    worker moved; this did not. That divergence is fixed here.)
    //
    //  * FAN-OUT. `resolve_slack_account_id` is `…ids().next()` — the FIRST
    //    match. One workspace shared by several tenants huddle-captured for
    //    only one of them, while message ingest correctly fanned out to all.
    //
    // Failure posture differs from ingest and deliberately so — see
    // `OrgPlaneResolution`. A huddle trigger fires exactly once, so a
    // TRANSIENT plane-resolution failure returns `Err` and the caller 503s to
    // make Slack redeliver; a DELIBERATE refusal (revoked org) skips that
    // account without begging for a redelivery we would only refuse again.
    // Redelivery is safe: the idempotency key is per-account, so accounts
    // already enqueued dedupe rather than double-capturing.
    // Every DB call below is BOUNDED, because this runs inline on the
    // pre-response path (see this function's "Durability trade (A)" section):
    // an unbounded control-pool call here parks the Slack webhook handler
    // itself, and a hung DB becomes a hung webhook rather than a 503.
    //
    // Cumulative cost is the thing to watch, not any single call. This
    // function now does one resolve plus, PER MATCHED ACCOUNT, a bounded
    // plane resolution and an enqueue — where it used to do exactly one
    // resolve and one enqueue. Slack wants an ack within ~3s, so a workspace
    // shared by several orgs can exceed that budget on a cold/slow control
    // plane. That degrades to a redelivery rather than to lost data, and
    // redelivery CONVERGES: the per-account idempotency key makes accounts
    // enqueued on an earlier attempt dedupe immediately, so each retry gets
    // strictly further through the list. Losing the trigger is the outcome
    // worth preventing; spending a retry is not.
    let matches = match tokio::time::timeout(
        SLACK_TENANT_ENQUEUE_TIMEOUT,
        super::slack::resolve_slack_account_planes(control_pool, team_id),
    )
    .await
    {
        Ok(Ok(m)) => m,
        Ok(Err(e)) => return Err(format!("resolving account(s) for huddle capture trigger: {e}")),
        Err(_) => {
            return Err(format!(
                "resolving account(s) for huddle capture trigger timed out after {}s",
                SLACK_TENANT_ENQUEUE_TIMEOUT.as_secs()
            ));
        }
    };

    if matches.is_empty() {
            // Not a failure of the durable path — there is no account to
            // enqueue against — so this is NOT an `Err` (the caller acks 200
            // rather than 503). Raised to `warn!` (was `debug!`, below the
            // production log filter): this is exactly the "legacy install
            // wrote only `slack_integrations`, never `oauth_tokens`" gap
            // documented in `docs/connectors/slack.md` §5 — a workspace can
            // silently and permanently never get huddle capture, and nothing
            // above `debug!` ever recorded that fact.
            tracing::warn!(
                team_id = %team_id,
                connector_health = true,
                "no oauth_tokens entry for Slack workspace — dropping huddle capture trigger \
                 (see docs/connectors/slack.md §5: this workspace likely installed through the \
                 legacy path that never wrote oauth_tokens)"
            );
        return Ok(());
    }

    for (account_id, clerk_org_id) in matches {
        let target_pool = match &clerk_org_id {
            None => control_pool.clone(),
            Some(org_id) => match resolve_org_plane(state, control_pool, org_id).await {
                OrgPlaneResolution::Pool(p, _) => p,
                // Deliberate refusal — this account gets no huddle, and a
                // redelivery would only be refused again. Already logged.
                OrgPlaneResolution::Dropped => continue,
                // Transient — 503 so Slack redelivers the whole trigger.
                OrgPlaneResolution::Unavailable(why) => {
                    return Err(format!(
                        "resolving plane for org {org_id} (account {account_id}): {why}"
                    ));
                }
            },
        };

        let idempotency = format!(
            "huddle_capture:{account_id}:{}:{}",
            trigger.channel_id, trigger.huddle_thread_ts
        );
        let kind = crate::jobs::JobKind::SlackHuddleCapture {
            account_id,
            workspace_id: team_id.to_owned(),
            channel_id: trigger.channel_id.clone(),
            huddle_thread_ts: trigger.huddle_thread_ts.clone(),
            canvas_file_id: trigger.canvas_file_id.clone(),
            participant_history: trigger.participant_history.clone(),
            date_start: trigger.date_start,
            date_end: trigger.date_end,
        };

        let enqueued = match tokio::time::timeout(
            SLACK_TENANT_ENQUEUE_TIMEOUT,
            crate::jobs::queue::enqueue(&target_pool, &kind, Some(account_id), Some(&idempotency), 0),
        )
        .await
        {
            Ok(r) => r,
            // Our own budget, not a `sqlx` condition — reported as itself
            // rather than dressed up as a pool error.
            Err(_) => {
                return Err(format!(
                    "enqueueing SlackHuddleCapture for account {account_id} timed out after {}s",
                    SLACK_TENANT_ENQUEUE_TIMEOUT.as_secs()
                ));
            }
        };
        match enqueued {
            Ok(Some(job_id)) => {
                tracing::info!(
                    %job_id,
                    account_id = %account_id,
                    org = clerk_org_id.as_deref().unwrap_or("-"),
                    channel_id = %trigger.channel_id,
                    huddle_thread_ts = %trigger.huddle_thread_ts,
                    "huddle capture trigger fired — job enqueued"
                );
                // Wake this tenant's harness (#498): the row is on the org's
                // OWN jobs table and the write is invisible to a `huddle`
                // worker sitting in idle backoff. Org branch only — the
                // consumer plane never backs off.
                if let Some(org_id) = &clerk_org_id {
                    state.worker_activity.note_enqueue(org_id);
                }
            }
            Ok(None) => {
                // Idempotency-key collision: a pending job for this exact huddle
                // already exists (e.g. Slack redelivered the same
                // `message_changed` event, or this very 503→redelivery already
                // succeeded once and Slack retried anyway). Not an error.
                tracing::debug!(
                    account_id = %account_id,
                    channel_id = %trigger.channel_id,
                    huddle_thread_ts = %trigger.huddle_thread_ts,
                    "huddle capture trigger fired — job already pending, skipping duplicate enqueue"
                );
            }
            Err(e) => return Err(format!("enqueueing SlackHuddleCapture job: {e}")),
        }
    }
    Ok(())
}

// ── Durable tenant hand-off (org-slack step 2) ─────────────────────────────────

/// Bound on the control-plane lookups and the tenant pool build this hand-off
/// makes per matching account. Same value and reasoning as `routes::
/// clerk_webhook::MEMBER_OCEAN_WEBHOOK_TIMEOUT`: this runs from a webhook
/// handler's background spawn, not a live user's own request, so it can
/// afford to be more patient than the request-path binding gate's own
/// timeout before giving up and deferring.
const SLACK_TENANT_ENQUEUE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

/// This install's own `(bot_id, bot user id)` for `account_id`, or `None`
/// when it cannot be determined.
///
/// Peeks the process-wide `auth.test` cache FIRST, before touching the
/// database at all: the answer is immutable for the lifetime of an
/// installation, so after one resolution per account this costs a mutex and
/// a hash lookup on the hot ingest path — and it is what lets a test seed the
/// answer (see [`seed_bot_identity_for_test`]) without stubbing HTTP.
///
/// The `(content_pool, tenant)` pair is the plane this message is being
/// enqueued onto, threaded through for exactly one reason: the `oauth_tokens`
/// ROW is control-only, but for an org account its KEY lives in that org's
/// tenant `account_keys` — the same two-pools-two-questions split
/// `ingest_slack_message`'s own bot-token read documents.
pub(crate) async fn resolve_own_bot_identity(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    content_pool: &sqlx::PgPool,
    tenant: &crate::tenancy::TenantId,
    account_id: uuid::Uuid,
    workspace_id: &str,
) -> Option<BotIdentity> {
    let key: BotIdentityKey = (account_id, workspace_id.to_string());
    {
        let cache = bot_user_id_cache().lock().unwrap();
        if let Some(id) = cache.get(&key) {
            return Some(id.clone());
        }
    }
    // A recent failure short-circuits before the DB read + DEK derive + HTTP
    // call — see `bot_identity_negative_cache`.
    if bot_identity_negative_hit(&key) {
        return None;
    }

    let dek_plane = crate::crypto::DekPlane::from_tenant(tenant, content_pool.clone(), account_id);
    let bot_token = match crate::workers::huddle::get_slack_bot_token_for_workspace(
        control_pool,
        &dek_plane,
        account_id,
        workspace_id,
    )
    .await
    {
        Ok(Some(t)) => t,
        Ok(None) => {
            tracing::warn!(
                account_id = %account_id,
                workspace_id = %workspace_id,
                "self-bot check: no bot token for this workspace"
            );
            cache_bot_identity_failure(key);
            return None;
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                workspace_id = %workspace_id,
                "self-bot check: bot token resolution failed"
            );
            cache_bot_identity_failure(key);
            return None;
        }
    };

    match resolve_bot_identity(
        &state.http_client,
        control_pool,
        account_id,
        workspace_id,
        &bot_token,
    )
    .await
    {
        Ok(id) => Some(id),
        Err(e) => {
            tracing::warn!(
                error = %e,
                account_id = %account_id,
                workspace_id = %workspace_id,
                "self-bot check: auth.test failed"
            );
            cache_bot_identity_failure(key);
            None
        }
    }
}

/// Durable tenant hand-off (org-slack step 2, `docs/plans/org-connectors-
/// and-channel-ingest.md` §9c): resolve every `connector_installs` account
/// this Slack workspace matches, decide ONE plane per account, and enqueue
/// `jobs::JobKind::SlackMessageIngest` onto THAT plane's own `jobs` table.
/// `workers::slack_ingest`'s tenant-looped worker drains it and calls the
/// same `ingest_slack_message` this handler used to call directly (see that
/// function's own doc comment for why it is plane-explicit now).
///
/// Called from a background `state.task_tracker` spawn — the webhook already
/// acked 200 by the time this runs, same "never block Slack's response on a
/// DB call" posture the ordinary message path always had (the huddle trigger
/// is the one documented exception, and is untouched by this change).
///
/// **Fan-out is correct, not duplication.** `resolve_slack_account_planes`
/// can legitimately return more than one `(account_id, clerk_org_id)` pair
/// for one workspace — two different CoralSwarm accounts, or a customer plus
/// someone's personal account, both having installed the SAME Slack
/// workspace — and each gets its own independent enqueue attempt, onto
/// whichever plane THAT account resolves to.
///
/// **Per-account plane decision:**
/// * `clerk_org_id` is `NULL` (personal/consumer account) → enqueue onto
///   `state.db`'s own `jobs` table (`TenantId::Consumer`).
/// * `clerk_org_id` is `Some` (an org account) → [`resolve_org_tenant_pool_
///   for_ingest`] follows `routes::clerk_webhook`'s precedent shape
///   (`org_is_revoked` fence → `routing::lookup_by_org` →
///   `row.is_servable()` → `registry.get_or_build`, each bounded by
///   [`SLACK_TENANT_ENQUEUE_TIMEOUT`]) to decide between three outcomes —
///   see that function's own doc comment for the full table (revoked ⇒
///   drop; never onboarded ⇒ the consumer plane, unchanged from before this
///   PR; mid-migration/torn-down ⇒ drop rather than guess; `Active` ⇒ the
///   org's own tenant pool, the new capability this step adds).
#[allow(clippy::too_many_arguments)]
async fn enqueue_slack_message_ingest(
    state: Arc<AppState>,
    team_id: String,
    channel: String,
    user: String,
    ts: String,
    thread_ts: Option<String>,
    text: String,
    bot_id: Option<String>,
    bot_name: Option<String>,
) {
    let Some(control_pool) = state.db.clone() else {
        tracing::warn!("DB not available — skipping Slack message hand-off");
        return;
    };

    let matches = match super::slack::resolve_slack_account_planes(&control_pool, &team_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(
                error = %e,
                team_id = %team_id,
                "failed to resolve Slack account(s) for message hand-off"
            );
            return;
        }
    };

    for (account_id, clerk_org_id) in matches {
        let target_pool = match &clerk_org_id {
            None => Some((control_pool.clone(), crate::tenancy::TenantId::Consumer)),
            Some(org_id) => {
                resolve_org_tenant_pool_for_ingest(&state, &control_pool, org_id).await
            }
        };
        let Some((target_pool, target_tenant)) = target_pool else {
            // Every `None` arm above already logged why (revoked,
            // not-yet-onboarded-but-that's-fine-goes-to-consumer is a
            // `Some`, mid-migration/torn-down, pool build failure/timeout).
            continue;
        };

        // ── Self-bot exclusion ───────────────────────────────────────────────
        //
        // Here, and not in the handler, because this is the first point where
        // `account_id` exists — and our own bot id is knowable ONLY per
        // account, via `auth.test` on that account's bot token (nothing
        // persists it: `slack_integrations` stores id/account/workspace/name/
        // token, and `oauth_tokens.provider_user_id` is the installing HUMAN).
        //
        // Guarded on `is_bot_message` so a HUMAN message never triggers an
        // `auth.test` call — the existing
        // `enqueue_slack_message_ingest_fans_out_to_the_correct_plane_per_
        // account_pg` tests drive this function with no HTTP stub and would
        // otherwise make a live network call.
        //
        // **Fail closed.** If we cannot learn our own identity (no bot token,
        // a decrypt failure, an `auth.test` error) the bot message is
        // DROPPED, not ingested: "our own confirmations must never feed back
        // into the ocean" is the invariant this whole check exists for, and
        // silently ingesting them when the lookup blips would violate it in
        // the one situation nobody is watching. A third-party bot message
        // lost that way is recoverable (backfill re-reads it); a self-ingest
        // loop is not.
        // `bot_name.is_some()` widens this past `bot_id`: a legacy
        // `subtype: "bot_message"` is an app post even in the (Slack-
        // documented-as-impossible, but cheap to survive) case where the
        // payload carries no `bot_id` at all. Its `user` is then absent too,
        // so the `own.user_id == user` half of the comparison below is what
        // catches it.
        let is_bot_message = bot_id.is_some() || bot_name.is_some();
        if is_bot_message {
            match resolve_own_bot_identity(
                &state,
                &control_pool,
                &target_pool,
                &target_tenant,
                account_id,
                &team_id,
            )
            .await
            {
                Some(own) => {
                    if is_our_own_bot_message(&own, bot_id.as_deref(), Some(user.as_str())) {
                        tracing::debug!(
                            account_id = %account_id,
                            channel_id = %channel,
                            ts = %ts,
                            bot_id = ?bot_id,
                            "slack ingest: dropping a message posted by our OWN bot"
                        );
                        continue;
                    }
                }
                None => {
                    tracing::warn!(
                        account_id = %account_id,
                        channel_id = %channel,
                        ts = %ts,
                        bot_id = ?bot_id,
                        "slack ingest: cannot resolve this install's own bot identity — \
                         dropping the bot message (fail closed, so our own posts can never \
                         feed back into the ocean)"
                    );
                    continue;
                }
            }
        }

        let kind = jobs::JobKind::SlackMessageIngest {
            account_id,
            workspace_id: team_id.clone(),
            channel_id: channel.clone(),
            user_id: user.clone(),
            ts: ts.clone(),
            thread_ts: thread_ts.clone(),
            text: text.clone(),
            bot_id: bot_id.clone(),
            bot_name: bot_name.clone(),
        };
        // One chunk-independent job per (account, channel, message) — the
        // SAME `(graph_id, source_type, source_id)` atom-level dedup inside
        // `ingest_slack_message` already makes a duplicate enqueue harmless,
        // but this key avoids piling up redundant `pending` rows for a
        // message Slack redelivers before the first job is claimed.
        let idempotency = format!("slack_ingest:{account_id}:{channel}:{ts}");
        match jobs::queue::enqueue(&target_pool, &kind, Some(account_id), Some(&idempotency), 0).await
        {
            Ok(Some(_)) => {
                // Wake this tenant's harness. The row is now durable on the
                // org's OWN `jobs` table, and nothing about writing it is
                // visible to `slack_ingest`, which polls on an idle backoff
                // that escalates to 20 minutes for an org that has been quiet.
                // Without this the message is ingested correctly but MINUTES
                // late (measured: 11m04s for an ingest that ran in 340ms).
                //
                // Org branch only: the consumer plane never backs off (see
                // `run_multi_tenant`'s post-tick delay match), so its work is
                // already picked up within one `base_poll`.
                if let Some(org_id) = &clerk_org_id {
                    state.worker_activity.note_enqueue(org_id);
                }
            }
            Ok(None) => {
                tracing::debug!(
                    account_id = %account_id,
                    channel = %channel,
                    "SlackMessageIngest job already pending — deduped"
                );
            }
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    account_id = %account_id,
                    "failed to enqueue SlackMessageIngest job"
                );
            }
        }
    }

    // Per-member capture (item 46b). The loop above is the org-install fan-out
    // and is left untouched for item 48. This path returns `MemberCaptureTargets`
    // from the control-plane index and ingests on each member's own token.
    crate::connectors::slack_member_capture::capture_message_event(
        &state,
        &control_pool,
        &team_id,
        &channel,
        &ts,
        &text,
        &user,
    )
    .await;
}

/// The org branch of [`enqueue_slack_message_ingest`]'s plane decision —
/// follows `routes::clerk_webhook`'s `provision_member_ocean_best_effort`
/// shape exactly (`org_is_revoked` fence → `routing::lookup_by_org` →
/// `row.is_servable()` → `registry.get_or_build`, each step bounded by
/// [`SLACK_TENANT_ENQUEUE_TIMEOUT`]), adapted to ingest's own routing
/// decision rather than that trigger's provisioning one:
///
/// * **Revoked** (`org_is_revoked`): `None` — a revoked org is a hard fence
///   with no carve-out (member-private-oceans PR 7 §D); never land new
///   content for it on ANY plane.
/// * **No routing row at all** (`Missing` — this org has never been
///   onboarded onto its own tenant database): `Some(control_pool.clone())`.
///   Unlike `clerk_webhook`'s provisioning trigger (which has no meaningful
///   consumer-plane fallback — a member ocean cannot exist there), ordinary
///   Slack ingest for an org account has ALWAYS worked against the consumer
///   plane (`connectors::slack::resolve_org_fanout_destinations` doesn't
///   care which physical pool it's handed) — so this is not a guess, it
///   reflects where this org's content genuinely still lives, unchanged
///   from before this PR.
/// * **A routing row exists but is not `is_servable()`** (`Provisioning`,
///   `Suspended`, `Offboarded`): `None`. This is the one case where
///   guessing a plane is actually dangerous — the org's content may be
///   mid-migration OR already torn down off the consumer plane, so writing
///   new content to either one risks a split-brain (some atoms on each
///   plane) or orphaning content in a database being decommissioned. Drop
///   and log loudly; the eventual reconciler/re-onboarding catches this org
///   up, at which point new messages resolve correctly again.
/// * **`Active`**: `registry.get_or_build` the tenant pool and return it —
///   the new capability this step exists to add.
/// Why [`resolve_org_plane`] could not hand back a pool — the distinction
/// [`resolve_org_tenant_pool_for_ingest`]'s bare `Option` cannot express.
///
/// Message ingest treats every non-pool outcome the same (drop the message)
/// and is right to: Slack will send more messages, and a dropped one is one
/// message. Huddle capture cannot. Its trigger fires EXACTLY ONCE per huddle
/// — `enqueue_huddle_capture`'s caller returns 503 precisely so Slack
/// redelivers — so collapsing "this org is revoked, never land content for
/// it" together with "the tenant pool failed to build just now" would either
/// lose a huddle forever or beg Slack to redeliver a huddle we are deliberately
/// refusing. They are different answers and the type now says so.
enum OrgPlaneResolution {
    /// Enqueue onto this pool, which pairs with this [`crate::tenancy::TenantId`].
    ///
    /// The tenant is carried alongside the pool (rather than re-derived by
    /// the caller) because the two arms that produce a pool disagree about
    /// it and only this function knows which one fired: a never-onboarded
    /// org falls back to the CONTROL pool, whose keys live in
    /// `accounts.dek_wrapped` (`TenantId::Consumer`), while an `Active` org
    /// gets its own tenant pool, whose keys live in that database's
    /// `account_keys` (`TenantId::Org`). A caller that guessed `Org` for the
    /// first case would fail every DEK resolution on it.
    Pool(sqlx::PgPool, crate::tenancy::TenantId),
    /// A deliberate, durable refusal (revoked org; routing row present but not
    /// servable). Skip this account — do NOT ask Slack to redeliver.
    Dropped,
    /// A transient failure (lookup or pool build failed / timed out). A caller
    /// whose payload is unrepeatable should surface this so Slack redelivers.
    Unavailable(String),
}

/// [`resolve_org_plane`] narrowed to the bare `Option` message ingest wants —
/// both refusal kinds collapse to `None`, exactly as before this split.
async fn resolve_org_tenant_pool_for_ingest(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    clerk_org_id: &str,
) -> Option<(sqlx::PgPool, crate::tenancy::TenantId)> {
    match resolve_org_plane(state, control_pool, clerk_org_id).await {
        OrgPlaneResolution::Pool(p, tenant) => Some((p, tenant)),
        OrgPlaneResolution::Dropped | OrgPlaneResolution::Unavailable(_) => None,
    }
}

async fn resolve_org_plane(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    clerk_org_id: &str,
) -> OrgPlaneResolution {
    match tokio::time::timeout(
        SLACK_TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::org_is_revoked(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(true)) => {
            tracing::info!(
                org = %clerk_org_id,
                "Slack message hand-off: org is revoked — dropping (never landing new content \
                 for a revoked org)"
            );
            return OrgPlaneResolution::Dropped;
        }
        Ok(Ok(false)) => {}
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id,
                error = %e,
                "Slack message hand-off: org_revocations lookup failed — dropping this message \
                 rather than guessing a plane"
            );
            return OrgPlaneResolution::Unavailable("org_revocations lookup failed".into());
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = SLACK_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "Slack message hand-off: org_revocations lookup timed out — dropping this \
                 message rather than guessing a plane"
            );
            return OrgPlaneResolution::Unavailable("org_revocations lookup timed out".into());
        }
    }

    let row = match tokio::time::timeout(
        SLACK_TENANT_ENQUEUE_TIMEOUT,
        crate::tenancy::routing::lookup_by_org(control_pool, clerk_org_id),
    )
    .await
    {
        Ok(Ok(row)) => row,
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id,
                error = %e,
                "Slack message hand-off: routing lookup failed — dropping this message rather \
                 than guessing a plane"
            );
            return OrgPlaneResolution::Unavailable("tenant routing lookup failed".into());
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = SLACK_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "Slack message hand-off: routing lookup timed out — dropping this message \
                 rather than guessing a plane"
            );
            return OrgPlaneResolution::Unavailable("tenant routing lookup timed out".into());
        }
    };

    let Some(row) = row else {
        // Missing: never onboarded onto a tenant database — its content
        // genuinely still lives on the shared consumer plane. See this
        // function's own doc comment.
        return OrgPlaneResolution::Pool(control_pool.clone(), crate::tenancy::TenantId::Consumer);
    };

    if !row.is_servable() {
        tracing::warn!(
            org = %clerk_org_id,
            status = %row.status,
            connector_health = true,
            "Slack message hand-off: tenant routing row exists but is not active (mid-migration \
             or torn down) — dropping this message rather than risking a split-brain write"
        );
        return OrgPlaneResolution::Dropped;
    }

    let Some(rt) = state.tenant_runtime.as_ref() else {
        // Defensive only: an `Active` routing row implies MULTITENANT_ENABLED
        // was on when it was created, but the runtime could since have been
        // disabled without the row itself changing.
        tracing::warn!(
            org = %clerk_org_id,
            "Slack message hand-off: active tenant routing row but no TenantRuntime configured \
             — dropping"
        );
        return OrgPlaneResolution::Unavailable("no TenantRuntime configured".into());
    };

    match tokio::time::timeout(
        SLACK_TENANT_ENQUEUE_TIMEOUT,
        rt.registry.get_or_build(clerk_org_id, row.account_id, &row.db_secret_ref),
    )
    .await
    {
        Ok(Ok(pool)) => {
            OrgPlaneResolution::Pool(pool, crate::tenancy::TenantId::Org(clerk_org_id.to_string()))
        }
        Ok(Err(e)) => {
            tracing::warn!(
                org = %clerk_org_id,
                error = %e,
                "Slack message hand-off: tenant pool build failed — dropping this message rather \
                 than guessing a plane"
            );
            OrgPlaneResolution::Unavailable("tenant pool build failed".into())
        }
        Err(_) => {
            tracing::warn!(
                org = %clerk_org_id,
                timeout_secs = SLACK_TENANT_ENQUEUE_TIMEOUT.as_secs(),
                "Slack message hand-off: tenant pool build timed out — dropping this message \
                 rather than guessing a plane"
            );
            OrgPlaneResolution::Unavailable("tenant pool build timed out".into())
        }
    }
}

// ── Seat grant / revocation on channel membership ─────────────────────────────

/// Resolve `(slack team_id, channel_id)` to the CoralSwarm account and the
/// team whose ocean that channel routes into, or `None` if this channel
/// cannot grant seats.
///
/// Shared by the join and leave paths so they can never disagree about which
/// channels are seat-bearing — a divergence there would grant seats the
/// departure path does not know to take back.
async fn resolve_seat_bearing_channel(
    pool: &sqlx::PgPool,
    slack_team_id: &str,
    channel_id: &str,
) -> Option<(uuid::Uuid, String, uuid::Uuid)> {
    let account_id = match super::slack::resolve_slack_account_id(pool, slack_team_id).await {
        Ok(Some(id)) => id,
        Ok(None) => return None,
        Err(e) => {
            tracing::warn!(error = %e, slack_team_id, "seat: DB error resolving account");
            return None;
        }
    };

    match crate::seats::resolve_channel_team(pool, account_id, slack_team_id, channel_id).await {
        Ok(Some(team_id)) => Some((account_id, slack_team_id.to_string(), team_id)),
        // The ordinary case for most channels: fan-out, or no destination
        // picked. Neither grants seats, so neither can revoke one.
        Ok(None) => None,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, channel_id, "seat: DB error resolving team");
            None
        }
    }
}

/// Parse a Slack `ts` ("1234567890.123456") into a `DateTime<Utc>`,
/// preserving the fractional part as nanoseconds (CodeRabbit round 1 on
/// #680: truncating to whole seconds floors `history_from` up to a second
/// EARLY, so a same-second message sent just before the join would slip
/// inside the window — `atoms."timestamp" >= floor` is a full-precision
/// comparison). Falls back to `Utc::now()` for an empty/unparsable string —
/// never guessed further, never an error that would refuse the seat.
fn parse_slack_ts(ts: &str) -> chrono::DateTime<Utc> {
    let (sec_part, frac_part) = match ts.split_once('.') {
        Some((s, f)) => (s, f),
        None => (ts, ""),
    };
    let secs: i64 = sec_part.parse().unwrap_or(0);
    if secs <= 0 {
        return Utc::now();
    }
    // Right-pad/truncate the fraction to 9 digits = nanoseconds. A
    // non-numeric fraction degrades to 0 nanos rather than `now()` — the
    // seconds part alone is still the right join moment.
    let nanos: u32 = if frac_part.is_empty() {
        0
    } else {
        let digits: String = frac_part.chars().take(9).collect();
        if digits.chars().all(|c| c.is_ascii_digit()) {
            let padded = format!("{digits:0<9}");
            padded.parse().unwrap_or(0)
        } else {
            0
        }
    };
    chrono::DateTime::from_timestamp(secs, nanos).unwrap_or_else(Utc::now)
}

#[cfg(test)]
mod parse_slack_ts_tests {
    use super::parse_slack_ts;
    use chrono::{DateTime, Timelike, Utc};

    #[test]
    fn preserves_fractional_precision_for_same_second_ordering() {
        // The CodeRabbit round-1 case on #680: a join at .9s and a message
        // at .1s of the SAME second. Truncation would floor the join to .0s
        // and let the earlier message inside the window.
        let join = parse_slack_ts("1700000000.900000");
        let earlier_msg = DateTime::<Utc>::from_timestamp(1_700_000_000, 100_000_000).unwrap();
        assert_eq!(join.timestamp(), 1_700_000_000);
        assert_eq!(join.nanosecond(), 900_000_000);
        assert!(
            earlier_msg < join,
            "a same-second message sent before the join must sort BEFORE the join moment"
        );
    }

    #[test]
    fn short_and_missing_fractions_parse() {
        assert_eq!(parse_slack_ts("1700000000.9").nanosecond(), 900_000_000);
        let bare = parse_slack_ts("1700000000");
        assert_eq!((bare.timestamp(), bare.nanosecond()), (1_700_000_000, 0));
        // Over-long fractions truncate to nanos rather than falling back.
        assert_eq!(parse_slack_ts("1700000000.1234567891").nanosecond(), 123_456_789);
    }

    #[test]
    fn garbage_falls_back_to_now_not_epoch() {
        let now = Utc::now();
        for ts in ["", "bad", "0.5", "-5.1", "170000000x.5"] {
            let parsed = parse_slack_ts(ts);
            assert!(
                (parsed - now).num_seconds().abs() < 60,
                "{ts:?} must fall back to now(), got {parsed}"
            );
        }
        // A numeric-seconds / garbage-fraction ts keeps the seconds part.
        assert_eq!(
            (parse_slack_ts("1700000000.9x").timestamp(), parse_slack_ts("1700000000.9x").nanosecond()),
            (1_700_000_000, 0)
        );
    }
}

/// A human joined a seat-bearing channel — give them the same seat everyone
/// already in that channel got, and record why they have it.
async fn seat_joining_member(
    state: &Arc<AppState>,
    account_id: uuid::Uuid,
    slack_team_id: &str,
    channel_id: &str,
    join_user: &str,
    bot_token: &str,
    join_ts: &str,
) {
    let Some(pool) = state.db.clone() else { return };

    // In-app org deletion: members are locked out from the request, so no one
    // is granted a team-ocean seat in an org that is being deleted (or is
    // already revoked). Fails closed.
    if crate::services::org_deletion::account_org_closing_down(&pool, account_id).await {
        tracing::info!(%account_id, channel_id, "seat: org is being deleted - not seating the joining member");
        return;
    }

    let team_id =
        match crate::seats::resolve_channel_team(&pool, account_id, slack_team_id, channel_id).await
        {
            Ok(Some(t)) => t,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, channel_id, "seat: join team lookup failed");
                return;
            }
        };

    // The FULL matcher here, not the directory-only lookup the leave path
    // uses: a newly joining member is exactly the person we may never have
    // seen before, so a live `users.info` is the point rather than waste.
    // Unmatched is an ordinary outcome — they simply hold no seat, same as
    // any unidentified member at routing time.
    //
    // Single-pool call site (org-slack step 5a): `handle_member_joined`
    // (this function's only caller) resolves `account_id` straight off
    // `state.db` — the member_joined/member_left Events API handlers are
    // not (yet) tenant-looped the way message ingest is — so `pool` here is
    // always the control/consumer pool. Passed for both the matcher's
    // `control_pool` and `content_pool` parameters — byte-identical to
    // before `resolve_verified_slack_identity`'s two-pool split.
    let Some(user_id) = crate::connectors::slack::resolve_verified_slack_identity(
        &pool,
        &pool,
        &state.http_client,
        account_id,
        slack_team_id,
        join_user,
        bot_token,
    )
    .await
    else {
        tracing::debug!(
            %account_id, channel_id, join_user,
            "seat: joining member could not be identity-matched — no seat granted (never guessed)"
        );
        return;
    };

    // `account_members` gate, same as `resolve_channel_members` applies at
    // routing time: being matched to SOME CoralSwarm user is not being a
    // member of the account that routed this workspace.
    let is_member: bool = match sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM account_members WHERE account_id = $1 AND user_id = $2)",
    )
    .bind(account_id)
    .bind(user_id)
    .fetch_one(&pool)
    .await
    {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!(error = %e, %account_id, %user_id, "seat: account_members check failed");
            return;
        }
    };
    if !is_member {
        return;
    }

    let source = crate::seats::slack_channel_source(channel_id);
    // Late joiner (m365 enterprise train PR 4.1): this call site fires for a
    // human joining a channel that is ALREADY routed and already being
    // ingested — exactly the case `docs/plans/
    // org-connectors-and-channel-ingest.md` §4 says `from_join` governs. The
    // event's own `ts` is the actual join moment; a redelivered/malformed
    // `ts` falls back to `now()` rather than blocking the seat (same
    // "never guessed, but also never refused" posture the rest of this
    // handler already has for identity matching).
    let history_from = Some(parse_slack_ts(join_ts));
    let seated = crate::routes::slack_interactivity::seat_members_for_source(
        &pool,
        account_id,
        team_id,
        &[user_id],
        &source,
        history_from,
    )
    .await;

    tracing::info!(
        %account_id, %team_id, %user_id, channel_id, seated,
        "seat: granted a team-ocean seat to a member who joined a routed channel"
    );
}

/// A human left a channel. Drop the grant that channel was providing, and let
/// [`crate::seats::revoke_seat_grant`] decide whether the seat itself goes —
/// it survives if any other reason for it remains.
async fn handle_member_left(
    state: Arc<AppState>,
    slack_team_id: String,
    channel_id: String,
    left_user: String,
) {
    let Some(pool) = state.db.clone() else {
        tracing::debug!("DB not available — skipping member_left_channel handling");
        return;
    };

    if let Err(e) = crate::connectors::slack_member_capture::apply_membership_event(
        &pool,
        &slack_team_id,
        &channel_id,
        &left_user,
        false,
    )
    .await
    {
        tracing::warn!(error = %e, "slack member index: member_left_channel update failed");
    }

    let Some((account_id, workspace_id, team_id)) =
        resolve_seat_bearing_channel(&pool, &slack_team_id, &channel_id).await
    else {
        return;
    };

    // Directory-only: someone we never linked holds no grant, so a live
    // `users.info` could not change the outcome — see
    // `seats::resolve_linked_user_directory_only`'s doc comment.
    let user_id =
        match crate::seats::resolve_linked_user_directory_only(&pool, &workspace_id, &left_user)
            .await
        {
            Ok(Some(u)) => u,
            Ok(None) => return,
            Err(e) => {
                tracing::warn!(error = %e, %account_id, "seat: directory lookup failed on leave");
                return;
            }
        };

    let source = crate::seats::slack_channel_source(&channel_id);
    match crate::seats::revoke_seat_grant(&pool, account_id, team_id, user_id, &source).await {
        Ok(outcome) => {
            use crate::seats::RevokeOutcome;
            match outcome {
                RevokeOutcome::SeatRevoked => tracing::info!(
                    %account_id, %team_id, %user_id, channel_id,
                    "seat: revoked — the last reason for this seat was this channel"
                ),
                RevokeOutcome::GrantDropped { remaining } => tracing::info!(
                    %account_id, %team_id, %user_id, channel_id, remaining,
                    "seat: kept — this channel was one of several reasons for it"
                ),
                RevokeOutcome::NotOurs => tracing::debug!(
                    %account_id, %team_id, %user_id, channel_id,
                    "seat: untouched — no grant on record for this channel, so this seat is not \
                     ours to revoke"
                ),
                RevokeOutcome::AdminPreserved => tracing::info!(
                    %account_id, %team_id, %user_id, channel_id,
                    "seat: kept — the member holds it as an admin, and auto-seating never grants \
                     'admin' so it must never remove one"
                ),
            }
        }
        Err(e) => {
            tracing::error!(
                error = %e, %account_id, %team_id, %user_id, channel_id,
                "seat: revocation failed — the seat stands; a reconciler sweep will retry"
            );
        }
    }
}

// ── app_uninstalled / tokens_revoked: full disconnect ─────────────────────────
//
// Org governance decision (2026-08-04), piece 3. Slack itself revoked our
// access — a human removed the app from the workspace, or revoked its bot
// token from the Slack admin console — so unlike the two manual-disconnect
// routes (`routes::slack::delete_integration`, `routes::oauth::
// delete_connection`'s Slack arm), there is no single authenticated caller
// or `Tenant` extractor to resolve a plane from. Every account this
// workspace was ever installed under gets the same treatment, fanned out via
// `resolve_slack_account_planes` — the SAME multi-account fan-out
// `handle_member_joined` and `enqueue_slack_message_ingest` already use for
// exactly this reason (two different CoralSwarm accounts, or a customer plus
// someone's personal account, both having installed the same workspace).

/// CS-453 (D21, Phase G wave 1): the `tokens.oauth` half of a
/// `tokens_revoked` payload — one or more Slack user ids whose OWN grant to
/// the app was individually revoked, independent of the workspace-wide bot
/// install. Durably marks the matching `oauth_tokens` row(s)
/// `credential_invalid_at` for EVERY account this workspace resolves to
/// (same `resolve_slack_account_planes` fan-out [`handle_slack_full_
/// disconnect`] uses, and for the identical reason: two different
/// CoralSwarm accounts can both have installed the same workspace).
///
/// Matches by `provider_user_id` (+ `provider_workspace_id = team_id`)
/// across BOTH `oauth_tokens.provider` literals a Slack identity can live
/// under — `"slack"` (today's account-wide bot-token row) and `"slack_user"`
/// (CS-454's future per-member rows) — via [`connectors::credential_health::
/// mark_slack_provider_user_revoked`]; see that function's own doc comment
/// for why matching on the provider identity (rather than a `TokenOwner` this
/// caller does not have) is correct here and is NOT the `owner_user_id IS
/// NULL OR = x` fallback `TokenOwner`'s doc forbids. A payload naming a user
/// id nothing matches (no member Slack row yet — the common case before
/// CS-454 ships) is a silent no-op, not an error: there is nothing to mark.
async fn mark_tokens_oauth_revocations(state: Arc<AppState>, team_id: String, slack_user_ids: Vec<String>) {
    let Some(control_pool) = state.db.clone() else {
        tracing::debug!(team_id = %team_id, "tokens_revoked (oauth): DB not available — skipping");
        return;
    };

    let matches = match super::slack::resolve_slack_account_planes(&control_pool, &team_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(
                error = %e, team_id = %team_id,
                "tokens_revoked (oauth): failed to resolve account(s) for this workspace"
            );
            return;
        }
    };
    if matches.is_empty() {
        tracing::debug!(
            team_id = %team_id,
            "tokens_revoked (oauth): no account matched this workspace — nothing to mark"
        );
        return;
    }

    for (account_id, _clerk_org_id) in matches {
        for slack_user_id in &slack_user_ids {
            match crate::connectors::credential_health::mark_slack_provider_user_revoked(
                &control_pool,
                account_id,
                &team_id,
                slack_user_id,
            )
            .await
            {
                Ok(rows) if rows > 0 => {
                    tracing::info!(
                        %account_id, team_id = %team_id, slack_user_id = %slack_user_id, rows,
                        "tokens_revoked (oauth): marked credential_invalid_at"
                    );
                }
                Ok(_) => {
                    // No matching row for this account/user — expected
                    // whenever the workspace has no per-member Slack row for
                    // this user yet (pre-CS-454, or a user this account
                    // never separately connected). Not an error.
                }
                Err(e) => {
                    tracing::warn!(
                        error = %e, %account_id, team_id = %team_id, slack_user_id = %slack_user_id,
                        "tokens_revoked (oauth): failed to mark credential_invalid_at"
                    );
                }
            }
        }
    }
}

/// Full disconnect for every account matched to `team_id`: the shared
/// [`crate::connectors::slack_disconnect::purge_slack_workspace`] purge PLUS
/// the token/integration row deletes the two manual-disconnect routes also
/// do — see [`delete_slack_account_rows`]. Called from both the
/// `app_uninstalled` and `tokens_revoked` (bot-token) dispatch arms above;
/// `reason` only labels the log lines so the two triggers stay distinguishable.
async fn handle_slack_full_disconnect(state: Arc<AppState>, team_id: String, reason: &'static str) {
    let Some(control_pool) = state.db.clone() else {
        tracing::debug!(team_id = %team_id, reason, "DB not available — skipping full disconnect");
        return;
    };

    let matches = match super::slack::resolve_slack_account_planes(&control_pool, &team_id).await {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(
                error = %e, team_id = %team_id, reason,
                "slack full disconnect: failed to resolve account(s) for this workspace"
            );
            return;
        }
    };
    if matches.is_empty() {
        tracing::debug!(
            team_id = %team_id, reason,
            "slack full disconnect: no account matched this workspace — nothing to purge \
             (already fully disconnected, or was never durably recorded here)"
        );
        return;
    }

    for (account_id, clerk_org_id) in matches {
        // Bot token BEFORE any row is deleted — the purge's best-effort
        // `conversations.leave` step needs it. It is almost certainly
        // already invalid (Slack just revoked it), which `leave_conversation`'s
        // own best-effort handling inside the purge routine already
        // tolerates — a failed leave call here is expected, not a bug.
        // ROW on control, KEY on the org's own plane (org-plane DEK fix).
        // `clerk_org_id` is already in hand from `resolve_slack_account_planes`
        // above. A plane failure degrades exactly like a token-lookup failure
        // does here — warn and continue with `None`, which only skips the
        // purge's best-effort `conversations.leave`; the purge itself still
        // runs. That is the pre-existing posture for this call, deliberately
        // preserved rather than made fatal.
        let dek_plane = match crate::crypto::DekPlane::resolve_for_org(
            &state,
            &control_pool,
            account_id,
            clerk_org_id.as_deref(),
        )
        .await
        {
            Ok(p) => Some(p),
            Err(e) => {
                tracing::warn!(
                    error = %e, %account_id, team_id = %team_id, reason,
                    "slack full disconnect: could not resolve the DEK plane — \
                     conversations.leave will be skipped"
                );
                None
            }
        };
        let bot_token = match dek_plane.as_ref() {
            None => None,
            Some(dek_plane) => crate::connectors::slack_disconnect::fetch_slack_bot_token(
                &control_pool,
                dek_plane,
                account_id,
                &team_id,
            )
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    error = %e, %account_id, team_id = %team_id, reason,
                    "slack full disconnect: bot token lookup failed"
                );
                None
            }),
        };

        let plane = if clerk_org_id.is_some() {
            super::slack::AccountPlane::Org
        } else {
            super::slack::AccountPlane::Personal
        };
        let tenant_pool = resolve_disconnect_tenant_pool(
            &state,
            &control_pool,
            account_id,
            plane,
            clerk_org_id.as_deref(),
        )
        .await;

        match &tenant_pool {
            Some(tenant_pool) => {
                let outcome = crate::connectors::slack_disconnect::purge_slack_workspace(
                    &state.http_client,
                    &control_pool,
                    tenant_pool,
                    account_id,
                    &team_id,
                    bot_token.as_deref(),
                )
                .await;
                if outcome.purge_had_a_failure() {
                    // See `PurgeOutcome::purge_had_a_failure`'s own doc
                    // comment — the token/integration rows are removed below
                    // regardless, so a failed purge here must never read as
                    // "complete".
                    tracing::warn!(
                        %account_id, team_id = %team_id, reason, ?outcome,
                        connector_health = true,
                        "slack full disconnect: watched-channel/seat purge FAILED (partially) — \
                         token/integration rows are still removed below; stale destinations may \
                         resurrect if this workspace is ever reconnected"
                    );
                } else {
                    tracing::info!(
                        %account_id, team_id = %team_id, reason, ?outcome,
                        "slack full disconnect: watched-channel/seat purge complete"
                    );
                }
            }
            None => tracing::warn!(
                %account_id, team_id = %team_id, reason,
                "slack full disconnect: could not resolve a tenant pool — skipping \
                 watched-channel/seat purge (token/integration rows are still removed below)"
            ),
        }

        // Full disconnect equivalent: also remove the token/integration rows
        // a manual disconnect removes (`routes::oauth::delete_connection`'s
        // Slack arm / `routes::slack::delete_integration`) — Slack itself
        // already revoked the actual access; this just makes our own
        // records agree, so nothing keeps resolving this workspace to this
        // account after this point.
        if let Err(e) = delete_slack_account_rows(&control_pool, account_id, &team_id).await {
            tracing::error!(
                error = %e, %account_id, team_id = %team_id, reason,
                "slack full disconnect: failed to delete oauth_tokens/slack_integrations/\
                 connector_installs rows"
            );
        }
    }
}

/// Delete every CONTROL-plane row that makes `(account_id, workspace_id)`
/// resolvable as an installed Slack workspace — `oauth_tokens`,
/// `slack_integrations`, and `connector_installs` — the same three tables
/// the two manual-disconnect routes remove between them (`routes::oauth::
/// delete_connection`'s Slack arm removes `oauth_tokens` +
/// `slack_integrations` + `connector_installs`; `routes::slack::
/// delete_integration` removes the latter two only). One scoped transaction,
/// all three tables, since — unlike those two routes — there is no `oauth_tokens`
/// vs. `slack_integrations`-only distinction to preserve here: Slack already
/// revoked the access both rows represent, so both go.
async fn delete_slack_account_rows(
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    workspace_id: &str,
) -> Result<(), sqlx::Error> {
    let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
        .await
        .map_err(|e| sqlx::Error::Protocol(format!("delete_slack_account_rows scoped tx: {e}")))?;

    // Foundation PR 1.1: `owner_user_id IS NULL` — Slack has no per-member
    // OAuth (§14.1), so there is only ever the account-wide row to remove
    // here.
    sqlx::query(
        "DELETE FROM oauth_tokens \
         WHERE account_id = $1 AND provider = 'slack' AND provider_workspace_id = $2 \
           AND owner_user_id IS NULL",
    )
    .bind(account_id)
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    sqlx::query("DELETE FROM slack_integrations WHERE account_id = $1 AND workspace_id = $2")
        .bind(account_id)
        .bind(workspace_id)
        .execute(&mut *tx)
        .await?;

    sqlx::query(
        "DELETE FROM connector_installs \
         WHERE account_id = $1 AND provider = 'slack' AND external_workspace_id = $2",
    )
    .bind(account_id)
    .bind(workspace_id)
    .execute(&mut *tx)
    .await?;

    tx.commit().await
}

// ── member_joined_channel helpers ─────────────────────────────────────────────

/// Called in a spawned task when a `member_joined_channel` event arrives.
///
/// 1. Resolves the account and fetches the bot token.
/// 2. Calls `auth.test` to discover the bot's own Slack user id (cached).
/// 3. Checks whether the joining user is the bot itself.
/// 4. If yes, posts the Block Kit backfill-choice prompt to the channel.
async fn handle_member_joined(
    state: Arc<AppState>,
    team_id: String,
    channel_id: String,
    join_user: String,
    join_ts: String,
) {
    let pool = match &state.db {
        Some(p) => p.clone(),
        None => {
            tracing::debug!("DB not available — skipping member_joined_channel handling");
            return;
        }
    };

    if let Err(e) = crate::connectors::slack_member_capture::apply_membership_event(
        &pool,
        &team_id,
        &channel_id,
        &join_user,
        true,
    )
    .await
    {
        tracing::warn!(error = %e, "slack member index: member_joined_channel update failed");
    }

    // Resolve account_id from team_id — see `slack::resolve_slack_account_id`.
    // FAN OUT over every account this workspace maps to, not just the first.
    //
    // This used the singular `resolve_slack_account_id` — `…ids().next()` —
    // so when one Slack workspace served a personal install AND an org (or two
    // orgs), only the earliest-created account was handled: the others got no
    // destination picker, no backfill prompt, and no seat for a joining human.
    // Observed in production on 2026-08-04: a private channel created after a
    // second install landed content on the personal plane only, and testorg
    // was never even offered a picker.
    //
    // Same singular→plural correction #501 made for `enqueue_huddle_capture`
    // and step 2 made for `enqueue_slack_message_ingest`. Each matched account
    // has its OWN bot token, its own seat rules, and its own destination
    // decision, so each gets its own pass below.
    let matches = match super::slack::resolve_slack_account_planes(&pool, &team_id).await {
        Ok(m) if m.is_empty() => {
            tracing::debug!(team_id = %team_id, "no oauth_tokens for workspace — ignoring join");
            return;
        }
        Ok(m) => m,
        Err(e) => {
            tracing::warn!(error = %e, team_id = %team_id, "DB error looking up account(s) on member join");
            return;
        }
    };

    for (account_id, clerk_org_id) in matches {
        handle_member_joined_for_account(
            &state,
            &pool,
            account_id,
            clerk_org_id.as_deref(),
            &team_id,
            &channel_id,
            &join_user,
            &join_ts,
        )
        .await;
    }
}

/// One account's share of a `member_joined_channel` event — the body
/// `handle_member_joined` used to run exactly once, now run per matched
/// account. Isolated per account on purpose: one account's missing bot token
/// or failed prompt must not stop the others being handled.
/// Whether the unmatched-member invite email may be sent at all.
///
/// **Default OFF**, and read fresh per call rather than cached, exactly like
/// [`crate::routes::clerk_webhook`]'s own self-delivery flag — so arming it is
/// a `fly secrets set` plus a restart, and disarming it does not need a
/// deploy. The reasoning for why it starts off lives at the call site, next to
/// the enqueue it guards, because that is where someone deciding to flip it
/// will actually be reading.
///
/// Note the polarity differs from `SLACK_INGEST_BOT_MESSAGES`, which is
/// default-ON because it gates SHIPPED behaviour and exists only as a kill
/// switch. This gates behaviour that has never run in production and has a
/// known missing piece (no opt-out), so it is default-off in the
/// `MEMBER_OCEANS_ENABLED` family's sense: unfinished capability, not a
/// regression valve.
fn unmatched_email_enabled() -> bool {
    matches!(
        std::env::var("SLACK_UNMATCHED_EMAIL_ENABLED").ok().as_deref(),
        Some(v) if v == "1" || v.eq_ignore_ascii_case("true")
    )
}

/// `false` while the org is being deleted or is revoked (a personal account,
/// `None`, is always allowed). Fails closed on a read error.
async fn org_allows_unmatched_email(control_pool: &sqlx::PgPool, clerk_org_id: Option<&str>) -> bool {
    match clerk_org_id {
        None => true,
        Some(org_id) => !crate::services::org_deletion::org_closing_down(control_pool, org_id).await.unwrap_or(true),
    }
}

async fn handle_member_joined_for_account(
    state: &Arc<AppState>,
    pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    clerk_org_id: Option<&str>,
    team_id: &str,
    channel_id: &str,
    join_user: &str,
    join_ts: &str,
) {
    // Fetch bot token — account-scoped inside `get_slack_bot_token_pub`
    // (org-tenancy OPS-4; `account_id` above came from 0084's
    // `resolve_oauth_account_id` bootstrap). This runs in a spawned task with
    // no response to shape, so a failure can only be logged — `error!`, not
    // `warn!`, since post-conversion there is no benign cause and this arm is
    // where a regression would go unnoticed.
    // No `clerk_org_id` in scope on this path (the caller resolved only the
    // account), so the shared table does its own one-row `accounts` lookup.
    // Same posture as the token lookup itself: `Err` is infrastructure and is
    // logged at `error!`, never confused with "this account has no install".
    let dek_plane =
        match crate::crypto::DekPlane::resolve_for_account(state, &pool, account_id).await {
            Ok(p) => p,
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "failed to resolve DEK plane on member join");
                return;
            }
        };
    let bot_token =
        match crate::workers::huddle::get_slack_bot_token_pub(&pool, &dek_plane, account_id).await {
            Ok(Some(t)) => t,
            Ok(None) => {
                tracing::warn!(account_id = %account_id, "no bot token for account on member join");
                return;
            }
            Err(e) => {
                tracing::error!(error = %e, account_id = %account_id, "failed to fetch bot token on member join");
                return;
            }
        };

    // Resolve bot user id (cached per account AND workspace — an account with
    // installs in two workspaces has a different bot user id in each, and the
    // account-only key this used to pass could compare a T2 joiner against
    // T1's bot; see `BOT_USER_ID_CACHE`'s doc comment).
    let bot_user_id =
        resolve_bot_user_id(&state.http_client, &pool, account_id, &team_id, &bot_token).await;
    let bot_user_id = match bot_user_id {
        Ok(id) => id,
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "failed to resolve bot user id");
            return;
        }
    };

    // A HUMAN joined a channel we watch. If that channel routes into a team
    // ocean, the seating rule that applied to everyone already in the channel
    // applies to them too — otherwise "routing a channel seats its members"
    // would silently mean "seats whoever happened to be in it that day", and
    // the roster would drift apart from the seats from the first join onward.
    if !crate::routes::slack_interactivity::is_bot_self_join(&join_user, &bot_user_id) {
        seat_joining_member(
            &state,
            account_id,
            &team_id,
            &channel_id,
            &join_user,
            &bot_token,
            join_ts,
        )
        .await;
        return;
    }

    tracing::info!(
        account_id = %account_id,
        channel_id = %channel_id,
        "bot added to channel — posting destination + backfill prompts"
    );

    crate::connectors::cold_start::maybe_post_slack_cold_start(
        state,
        pool,
        account_id,
        clerk_org_id,
        &bot_token,
        channel_id,
    )
    .await;

    // This handler itself does NOT write a `slack_watched_channels` row —
    // docs/connectors/slack.md §2/§4 + migration
    // `0101_slack_watched_channels`'s header: the self-join is an OBSERVABLE
    // gate event (contract I1/I2), but it names no destination on its own,
    // and "pick an arbitrary representative account member and freeze their
    // ocean as this channel's destination forever" is a guess, not an
    // authorisation. The prompt below is what turns this gate event into an
    // actual human choice (answered in `routes::slack_interactivity`); until
    // a human answers it, the account-level default is computed live
    // instead — see `connectors::slack::resolve_channel_destinations`.
    if let Err(e) = post_destination_prompt(
        &pool,
        &state.http_client,
        &bot_token,
        account_id,
        &team_id,
        &channel_id,
    )
    .await
    {
        tracing::warn!(error = %e, channel_id = %channel_id, "failed to post destination prompt");
    }

    if let Err(e) =
        post_backfill_prompt(&state.http_client, &bot_token, account_id, &channel_id).await
    {
        tracing::warn!(error = %e, channel_id = %channel_id, "failed to post backfill prompt");
    }

    // Chat-per-user plan PR 6 ("email the unmatched", docs/plans/chat-per-
    // user-ingest-and-in-chat-ask.md §8 row 6): the SAME gate event that just
    // posted the destination/backfill prompts above IS the spec's step 3
    // ("we tell them it works by adding the agent to a container"); this is
    // step 5 ("we email the people ... who have not linked yet"). Durably
    // queued (never sent inline here) so a large channel's roster walk never
    // blocks this webhook's own response, and so a retried/redelivered
    // `member_joined_channel` event costs at most one extra job — the
    // idempotency key coalesces concurrent duplicates the same way
    // `RegenerateDigest`'s `digest:{node_id}` does, and the persisted
    // per-recipient cooldown (`services::unmatched_recipient_email::
    // claim_notice_slot`) makes an eventual re-run harmless either way.
    //
    // ── DEFAULT OFF, and it must stay that way until two things exist ──────
    //
    // This is the only outbound path in the product that emails a person who
    // never gave us their address. Everything else — `org_invites`, Clerk auth
    // mail — goes to someone who asked, or whom an admin deliberately invited.
    // Here the address came from a Slack roster because a COLLEAGUE added the
    // agent to a channel, and the recipient did nothing at all.
    //
    // Two things are missing before that is safe to switch on, and neither is
    // a code detail:
    //   1. **No opt-out.** The message carries no `List-Unsubscribe` header
    //      and no unsubscribe link, and there is no suppression list to
    //      honour one with. Bulk-sender requirements at Google and Yahoo ask
    //      for one-click unsubscribe, and `coralswarm.example`'s Postmaster
    //      compliance currently passes on exactly that axis.
    //   2. **Domain reputation is unestablished** — Postmaster reports "not
    //      enough volume to determine deliverability", which is precisely the
    //      state in which spam complaints do the most lasting damage. A cold
    //      send is the worst possible first volume.
    //
    // The flag is checked HERE, at the enqueue, rather than in the worker, so
    // a workspace that hits this gate event while the flag is off creates no
    // job at all — nothing sits in the queue waiting to fire the moment
    // somebody arms it. Same posture and shape as
    // `routes::clerk_webhook::clerk_email_self_delivery_enabled`, which is
    // default-off for the same class of reason.
    if !unmatched_email_enabled() {
        tracing::debug!(
            %account_id, channel_id = %channel_id,
            "unmatched-member invite email is disabled (SLACK_UNMATCHED_EMAIL_ENABLED unset) — \
             not enqueuing"
        );
        return;
    }

    // No per-plane REFUSAL here (an earlier version of this function refused
    // ORG accounts at this exact point, on the theory that the public
    // unsubscribe route could not reach an org tenant's notice row). That
    // was fixing the wrong side: `services::unmatched_recipient_email`'s
    // suppression list moved to the CONTROL plane, unconditionally, so
    // `routes::email_unsubscribe` now resolves an org account's token
    // exactly as completely as a personal account's — see that module's own
    // "Control-plane only, not per-plane" doc section.
    //
    // The SUPPRESSION TABLE'S plane is not the JOB QUEUE'S plane, though
    // (CodeRabbit round 1, #1005, Data Integrity) — an earlier version of
    // THIS enqueue call still handed `pool` (== `state.db`, the CONTROL
    // pool) to every account unconditionally. `workers::slack_unmatched_
    // email` is tenant-looped (`workers::tenant_harness::run_multi_tenant`)
    // and dequeues from its OWN tick's `t.pool` — for an ACTIVE org that is
    // that org's own tenant database, a physically different Postgres
    // instance. A job written to control for an org account is therefore
    // either invisible to the org's own tick forever, or — worse — silently
    // CLAIMED by the CONSUMER tick instead (`queue::dequeue` filters by job
    // KIND only, never by which account/plane enqueued it), which would then
    // run the roster walk against the wrong identity tables entirely. Same
    // bug class `enqueue_huddle_capture`'s own doc comment documents for
    // exactly this job/worker pairing; same fix, resolved per account via
    // [`resolve_org_plane`] rather than assumed to be `pool`.
    // In-app org deletion: no invite or join-request emails on behalf of an
    // org that is being deleted or is revoked (members are locked out; the
    // worker drops anything already queued). Fails closed.
    if !org_allows_unmatched_email(pool, clerk_org_id).await {
        tracing::info!(%account_id, channel_id = %channel_id, "unmatched-member email: org is being deleted - not enqueuing");
        return;
    }
    let Some(target_pool) =
        resolve_email_unmatched_target_pool(state, pool, account_id, clerk_org_id).await
    else {
        return;
    };
    if let Err(e) = crate::jobs::queue::enqueue(
        &target_pool,
        &crate::jobs::JobKind::EmailUnmatchedSlackChannelMembers {
            account_id,
            workspace_id: team_id.to_string(),
            channel_id: channel_id.to_string(),
        },
        Some(account_id),
        Some(&format!("email-unmatched:{account_id}:{team_id}:{channel_id}")),
        0,
    )
    .await
    {
        tracing::warn!(
            error = %e, account_id = %account_id, channel_id = %channel_id,
            "failed to enqueue EmailUnmatchedSlackChannelMembers"
        );
    }
}

/// Resolve which `jobs` table `JobKind::EmailUnmatchedSlackChannelMembers`
/// belongs on for one account (CodeRabbit round 1, #1005, Data Integrity).
/// Extracted out of [`handle_member_joined_for_account`] specifically so it
/// is testable without the rest of that function's live-Slack-API
/// dependencies (bot token, DEK plane, the destination/backfill prompts).
///
/// `None` covers both [`OrgPlaneResolution::Dropped`] (a deliberate,
/// durable refusal — never guess a plane for a revoked/non-servable org)
/// and [`OrgPlaneResolution::Unavailable`] (a transient failure, logged
/// here since the caller has no webhook response left to redeliver
/// against) — the caller's only correct reaction to either is "skip this
/// account", so it does not need to tell them apart.
async fn resolve_email_unmatched_target_pool(
    state: &Arc<AppState>,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    clerk_org_id: Option<&str>,
) -> Option<sqlx::PgPool> {
    match clerk_org_id {
        None => Some(control_pool.clone()),
        Some(org_id) => match resolve_org_plane(state, control_pool, org_id).await {
            OrgPlaneResolution::Pool(p, _) => Some(p),
            OrgPlaneResolution::Dropped => None,
            OrgPlaneResolution::Unavailable(why) => {
                tracing::warn!(
                    account_id = %account_id, org = %org_id, reason = %why,
                    "email-unmatched: could not resolve org plane for enqueue — skipping this \
                     account rather than guessing (or worse, defaulting to control)"
                );
                None
            }
        },
    }
}

/// Post the Block Kit per-channel DESTINATION prompt (docs/connectors/slack.md
/// §2's three-option picker), fired at the same gate event as the existing
/// backfill prompt (`handle_member_joined`) — adding CoralSwarm to a channel
/// is the authorising act (contract I1/I2). Answered in
/// `routes::slack_interactivity::handle_destination_action`.
///
/// Every plane's prompt carries the same three disclosures
/// (docs/connectors/slack.md §3.1/§3.4/§5), stated PLAINLY regardless of
/// which option (if any) a human ends up picking:
///
/// 1. Slack's automatic huddle notes must be switched on for this workspace,
///    or huddles here are never saved — we cannot verify or flip that
///    setting ourselves.
/// 2. Huddle notes require Slack AI, a PAID add-on. Without it there are no
///    notes at all, and that failure mode looks identical to "notes are
///    off".
/// 3. Deleting a message or huddle in Slack does not remove the copy already
///    saved here.
///
/// The ORG plane additionally carries a fourth disclosure specific to its
/// "everyone's individual oceans" option: identity mapping (capability C5)
/// reaches only members whose Slack account has been matched to a CoralSwarm
/// account by verified email. Unmatched people are skipped and their content
/// is not saved anywhere, so coverage is a property of the directory, not of
/// the pick — an admin choosing this needs to know that before choosing it.
///
/// And a fifth, specific to its "existing team ocean" option: routing this
/// channel there auto-seats the channel's members with full read access
/// (founder decision, PR #372 / commit `bd17b10` — the admin's act of
/// routing IS the decision to admit them). A team ocean is not
/// channel-shaped, so that seat opens more than this channel: the ocean's
/// meetings, documents, and any other channel already routed into it. An
/// admin cannot own a consequence they were not shown, so this disclosure
/// states a concrete headcount, fetched live via `conversations.members` and
/// resolved through the SAME verified-email matcher the actual seating write
/// uses (`connectors::slack::resolve_channel_members` — phase two, wired
/// 2026-07-27), so the count shown here and the count actually seated later
/// can never drift apart. A failed count (the roster fetch itself failing)
/// is disclosed as a failure, never rounded down to an absent or fabricated
/// number.
///
/// Per-plane options (docs/connectors/slack.md §2):
/// * Personal: a `static_select` of the account's own oceans, shown only
///   when there is more than one to choose from (a single-ocean account has
///   nothing to pick between — it already defaults to that ocean).
/// * Org: "everyone's individual oceans" (button) / an existing team ocean
///   (`static_select`, shown only when the account has any) — admin
///   membership is enforced SERVER-SIDE at click time
///   (`slack_interactivity::check_team_ocean_admin`), not by filtering this
///   list, since Block Kit cannot show a different option set to different
///   viewers of the same message / a new channel-mirrored team ocean
///   (button). Never the org ocean (docs/connectors/slack.md §2's table).
async fn post_destination_prompt(
    pool: &sqlx::PgPool,
    http: &reqwest::Client,
    bot_token: &str,
    account_id: uuid::Uuid,
    workspace_id: &str,
    channel_id: &str,
) -> Result<(), String> {
    let is_org: bool =
        sqlx::query_scalar("SELECT clerk_org_id IS NOT NULL FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(pool)
            .await
            .map_err(|e| format!("account lookup: {e}"))?
            .unwrap_or(false);

    let mut blocks = vec![serde_json::json!({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "*Before this channel starts feeding an ocean, a few things worth knowing:*\n\
                • Turn on Slack's *automatic huddle notes* for this workspace, or huddles here \
                  will never be saved — we can't flip that setting for you.\n\
                • Huddle notes need *Slack AI* (a paid add-on). Without it there are no notes at \
                  all, and that looks identical to \"notes are off.\"\n\
                • Deleting a message or huddle in Slack does *not* remove the copy already saved \
                  here."
        }
    })];

    if is_org {
        // Never hide an option a viewer doesn't administer — Block Kit
        // shows every viewer of this message the SAME options, so "only if
        // they admin it" (docs/connectors/slack.md §2) is enforced at
        // CLICK time (`slack_interactivity::check_team_ocean_admin`), not
        // by filtering this list. Listing every team here and refusing a
        // non-admin's pick afterwards is honest about that; hiding options
        // per-viewer is not possible at all in a single posted message.
        // Scoped: `teams` carries 0083's `account_isolation` policy, whose
        // USING clause casts `current_setting('app.account_id')` to uuid. On
        // a bare pool connection that setting is the empty string and the
        // cast ERRORS — it does not merely filter to zero rows. See the
        // sibling `graphs` read below for the production evidence.
        let teams: Vec<(uuid::Uuid, String)> = {
            let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
                .await
                .map_err(|e| format!("teams lookup scope: {e}"))?;
            let rows = sqlx::query_as("SELECT id, name FROM teams WHERE account_id = $1 ORDER BY name")
                .bind(account_id)
                .fetch_all(&mut *tx)
                .await
                .map_err(|e| format!("teams lookup: {e}"))?;
            tx.rollback().await.ok();
            rows
        };

        blocks.push(serde_json::json!({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "*Where should this channel's messages (and huddles) go?*"
            }
        }));
        blocks.push(serde_json::json!({
            "type": "actions",
            // Names the account this prompt belongs to — see
            // `ACCOUNT_BLOCK_ID_PREFIX`.
            "block_id": account_block_id(account_id, "org_fanout"),
            "elements": [
                {
                    "type": "button",
                    "text": { "type": "plain_text", "text": "Everyone's individual oceans", "emoji": true },
                    "action_id": "cs_dest_org_fanout"
                },
                {
                    "type": "button",
                    "text": { "type": "plain_text", "text": "New channel-mirrored team ocean", "emoji": true },
                    "action_id": "cs_dest_org_team_new"
                }
            ]
        }));
        // Phase two (2026-07-27) wired identity mapping, so the warning that
        // used to sit here — "doesn't work yet ... seats no one and ingests
        // nothing" — became false the moment fan-out started resolving
        // members. What replaces it states the real, still-important limit:
        // the pick reaches the people we can actually identify, and nobody
        // else. Silence would be worse than either; an admin picking this
        // needs to know coverage is a function of who has been matched.
        blocks.push(serde_json::json!({
            "type": "context",
            "elements": [{
                "type": "mrkdwn",
                "text": "ℹ️ *\"Everyone's individual oceans\" reaches identified members only* \
                    — each person's messages land in their own ocean once their Slack account \
                    is matched to a CoralSwarm account by verified email. Anyone unmatched is \
                    skipped, and their content isn't saved anywhere."
            }]
        }));

        if !teams.is_empty() {
            // Auto-seating disclosure (founder decision, PR #372 / `bd17b10`):
            // routing this channel to an EXISTING team ocean auto-seats the
            // channel's members with full read access to everything already
            // in that ocean — not just this channel. "An admin cannot own a
            // consequence they were not shown," so this states a concrete
            // headcount rather than a vague warning. The identified count is
            // now REAL (phase two, 2026-07-27): resolved through the SAME
            // matcher `auto_seat_channel_members` uses at pick time
            // (`connectors::slack::resolve_channel_members`), so this number
            // and the number actually seated later can never drift apart. A
            // failed/rate-limited roster fetch is disclosed as a failure —
            // see `resolve_channel_members`'s doc comment for why a missing
            // count must never be silently rounded to zero or omitted.
            // Consumer-plane call site: control and content are the same
            // pool here (org-slack step 4 — see `resolve_channel_members`'s
            // own doc comment).
            let seat_disclosure = match crate::connectors::slack::resolve_channel_members(
                pool,
                pool,
                http,
                account_id,
                workspace_id,
                bot_token,
                channel_id,
            )
            .await
            {
                Ok((matched, count)) => {
                    let identified = matched.len();
                    let member_word = if count == 1 { "member" } else { "members" };
                    format!(
                        "⚠️ *Routing to an existing team ocean seats its members.* This channel \
                         has *{count} {member_word}*. Everyone we can identify will be able to \
                         read *everything already in that ocean* — its meetings, documents, and \
                         any other channel already routed into it, not only this channel's \
                         messages. We can identify *{identified} of {count}* today, based on \
                         verified email matches."
                    )
                }
                Err(e) => format!(
                    "⚠️ *Routing to an existing team ocean seats its members* — and we could \
                     not confirm how many. Fetching this channel's member list failed ({e}), so \
                     we're not showing a count we can't stand behind. Until it's confirmed, \
                     treat this pick as seating an unknown number of people with read access to \
                     that ocean's meetings, documents, and any other routed channel."
                ),
            };
            blocks.push(serde_json::json!({
                "type": "context",
                "elements": [{ "type": "mrkdwn", "text": seat_disclosure }]
            }));

            let options: Vec<serde_json::Value> = teams
                .iter()
                .map(|(id, name)| {
                    serde_json::json!({
                        "text": { "type": "plain_text", "text": name, "emoji": true },
                        "value": id.to_string()
                    })
                })
                .collect();
            blocks.push(serde_json::json!({
                "type": "actions",
                // Names the account this prompt belongs to — see
                // `ACCOUNT_BLOCK_ID_PREFIX`.
                "block_id": account_block_id(account_id, "org_team"),
                "elements": [{
                    "type": "static_select",
                    "placeholder": {
                        "type": "plain_text",
                        "text": "Route to a team ocean you admin"
                    },
                    "action_id": "cs_dest_org_team_existing",
                    "options": options
                }]
            }));
        }
    } else {
        // Scoped, for the reason this whole function failed in production
        // from the day it shipped until 2026-07-27: `graphs` carries 0083's
        // `account_isolation` policy, and its `current_setting('app.
        // account_id')::uuid` cast raises
        //
        //     invalid input syntax for type uuid: ""
        //
        // on a connection that never set the GUC. Not "returns nothing" — a
        // hard error, which `post_destination_prompt`'s caller logs as
        // `failed to post destination prompt` and swallows. Every channel the
        // bot joined silently got no picker at all and fell through to the
        // account-level default, on both planes (the `teams` read above fails
        // identically for org accounts).
        //
        // Found by an end-to-end test against real Slack, not by the suite:
        // every pg test runs as the database OWNER, for whom RLS is inert, so
        // an unscoped read passes locally and fails only in production under
        // `coralswarm_app`. `scripts/check_rls_coverage.sh` did not catch it
        // either — its gate is per-FILE, and this file already contained a
        // `begin_account_scoped` elsewhere.
        let personal_oceans: Vec<(uuid::Uuid, String, bool)> = {
            let mut tx = crate::tenancy::scope::begin_account_scoped(pool, account_id)
                .await
                .map_err(|e| format!("personal oceans lookup scope: {e}"))?;
            let rows = sqlx::query_as(
                "SELECT id, name, is_primary FROM graphs \
                 WHERE account_id = $1 AND kind = 'personal' ORDER BY is_primary DESC, name",
            )
            .bind(account_id)
            .fetch_all(&mut *tx)
            .await
            .map_err(|e| format!("personal oceans lookup: {e}"))?;
            tx.rollback().await.ok();
            rows
        };

        if personal_oceans.len() > 1 {
            let options: Vec<serde_json::Value> = personal_oceans
                .iter()
                .map(|(id, name, is_primary)| {
                    let label = if *is_primary {
                        format!("{name} (default)")
                    } else {
                        name.clone()
                    };
                    serde_json::json!({
                        "text": { "type": "plain_text", "text": label, "emoji": true },
                        "value": id.to_string()
                    })
                })
                .collect();
            blocks.push(serde_json::json!({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "*Which of your oceans should this channel feed?* Defaults to your \
                        primary ocean — nothing changes unless you pick a different one."
                }
            }));
            blocks.push(serde_json::json!({
                "type": "actions",
                // Names the account this prompt belongs to — see
                // `ACCOUNT_BLOCK_ID_PREFIX`.
                "block_id": account_block_id(account_id, "personal"),
                "elements": [{
                    "type": "static_select",
                    "placeholder": { "type": "plain_text", "text": "Choose an ocean" },
                    "action_id": "cs_dest_personal",
                    "options": options
                }]
            }));
        }
    }

    let body = serde_json::json!({
        "channel": channel_id,
        "text": "CoralSwarm destination settings for this channel.",
        "blocks": blocks
    });

    let resp = http
        .post(format!("{}/chat.postMessage", crate::connectors::slack::slack_api_base()))
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("chat.postMessage HTTP error: {e}"))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("chat.postMessage JSON parse: {e}"))?;

    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = json
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        return Err(format!("chat.postMessage Slack error: {err}"));
    }

    Ok(())
}

/// Call `auth.test` to get the bot's own Slack user ID, using the in-process
/// cache to avoid hammering the API.
async fn resolve_bot_user_id(
    http: &reqwest::Client,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    workspace_id: &str,
    bot_token: &str,
) -> Result<String, String> {
    resolve_bot_identity(http, control_pool, account_id, workspace_id, bot_token)
        .await
        .map(|id| id.user_id)
}

/// Call `auth.test` to get BOTH of this install's own identifiers (`user_id`
/// and `bot_id`), using the in-process cache to avoid hammering the API.
///
/// The `bot_id` half is what the self-bot exclusion in
/// [`enqueue_slack_message_ingest`] compares against, and it is genuinely not
/// obtainable anywhere else: `slack_integrations` stores only id/account/
/// workspace/name/token, and `oauth_tokens.provider_user_id` is the installing
/// HUMAN, not the app.
pub(crate) async fn resolve_bot_identity(
    http: &reqwest::Client,
    control_pool: &sqlx::PgPool,
    account_id: uuid::Uuid,
    workspace_id: &str,
    bot_token: &str,
) -> Result<BotIdentity, String> {
    let key: BotIdentityKey = (account_id, workspace_id.to_string());
    // Check cache first. Keyed on the WORKSPACE as well as the account — see
    // `BOT_USER_ID_CACHE`'s own doc comment for the cross-workspace
    // self-ingest this prevents. The `bot_token` argument is per-workspace
    // too, so a cache hit that ignored the workspace was also answering for a
    // token it had never seen.
    {
        let cache = bot_user_id_cache().lock().unwrap();
        if let Some(id) = cache.get(&key) {
            return Ok(id.clone());
        }
    }

    // Call auth.test.
    let resp = http
        .get(format!("{}/auth.test", crate::connectors::slack::slack_api_base()))
        .bearer_auth(bot_token)
        .send()
        .await
        .map_err(|e| format!("auth.test HTTP error: {e}"))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("auth.test JSON parse error: {e}"))?;

    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = json
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        crate::connectors::slack::record_if_stored_slack_revoked(control_pool, account_id, err)
            .await;
        return Err(format!("auth.test Slack error: {err}"));
    }

    let user_id = json
        .get("user_id")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| "auth.test: missing user_id".to_string())?
        .to_owned();

    // `bot_id` is present for a bot token and absent for a user token. Absent
    // is not an error here — the caller that needs it treats "we could not
    // learn our own bot_id" as "do not ingest this bot message", never as a
    // reason to fail the whole event.
    let bot_id = json
        .get("bot_id")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned);

    let identity = BotIdentity { user_id, bot_id };

    // Cache it.
    {
        let mut cache = bot_user_id_cache().lock().unwrap();
        cache.insert(key, identity.clone());
    }

    Ok(identity)
}

/// Post the Block Kit backfill-choice prompt to the channel.
/// Prefix for the `block_id` that carries which ACCOUNT a prompt belongs to.
///
/// One Slack workspace can map to several CoralSwarm accounts (a personal
/// install and one or more orgs), and each needs its OWN destination and
/// backfill decision — so each gets its own prompt. Slack echoes `block_id`
/// back on every `block_actions` payload, which makes it the natural carrier:
/// additive, and it leaves the buttons' `value` untouched. That matters
/// because `slack_interactivity` falls back to the button `value` when the
/// payload has no `/channel/id`, so widening `value` into a composite would
/// have silently corrupted `channel_id` on that path.
///
/// A prompt posted BEFORE this shipped has no such `block_id`; the reader
/// treats that as "no preference" and keeps the old first-match behaviour, so
/// buttons already sitting in channels keep working.
pub(crate) const ACCOUNT_BLOCK_ID_PREFIX: &str = "cs_acct:";

/// `block_id` naming the account a prompt was posted for.
/// `block_id` naming the account a prompt was posted for.
///
/// `slot` disambiguates blocks WITHIN one message. Slack requires `block_id`
/// to be unique per message, and a destination prompt for an org that has
/// teams renders several action blocks at once — so the account id alone
/// would collide. The reader parses only up to the first `:` after the
/// prefix, so the slot is free-form and never has to be kept in sync.
pub(crate) fn account_block_id(account_id: uuid::Uuid, slot: &str) -> String {
    format!("{ACCOUNT_BLOCK_ID_PREFIX}{account_id}:{slot}")
}

async fn post_backfill_prompt(
    http: &reqwest::Client,
    bot_token: &str,
    account_id: uuid::Uuid,
    channel_id: &str,
) -> Result<(), String> {
    let body = serde_json::json!({
        "channel": channel_id,
        "text": "CoralSwarm is now capturing new messages in this channel.",
        "blocks": [
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "👋 *CoralSwarm is now capturing new messages in this channel.* Want to ingest past messages too?"
                }
            },
            {
                "type": "actions",
                // Names the account this prompt belongs to — see
                // `ACCOUNT_BLOCK_ID_PREFIX`. Without it, two accounts sharing
                // one workspace post two identical prompts and BOTH resolve to
                // whichever account sorts first.
                "block_id": account_block_id(account_id, "backfill"),
                "elements": [
                    {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "Going forward only", "emoji": false },
                        "action_id": "cs_backfill_none",
                        "value": channel_id,
                        "style": "primary"
                    },
                    {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "Last 7 days", "emoji": false },
                        "action_id": "cs_backfill_7d",
                        "value": channel_id
                    },
                    {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "Last 30 days", "emoji": false },
                        "action_id": "cs_backfill_30d",
                        "value": channel_id
                    },
                    {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "Last 90 days", "emoji": false },
                        "action_id": "cs_backfill_90d",
                        "value": channel_id
                    },
                    {
                        "type": "button",
                        "text": { "type": "plain_text", "text": "All history", "emoji": false },
                        "action_id": "cs_backfill_all",
                        "value": channel_id
                    }
                ]
            }
        ]
    });

    let resp = http
        .post(format!("{}/chat.postMessage", crate::connectors::slack::slack_api_base()))
        .bearer_auth(bot_token)
        .json(&body)
        .send()
        .await
        .map_err(|e| format!("chat.postMessage HTTP error: {e}"))?;

    let json: serde_json::Value = resp
        .json()
        .await
        .map_err(|e| format!("chat.postMessage JSON parse: {e}"))?;

    if json.get("ok").and_then(serde_json::Value::as_bool) != Some(true) {
        let err = json
            .get("error")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("unknown");
        return Err(format!("chat.postMessage Slack error: {err}"));
    }

    Ok(())
}

// ── Shared Slack message ingest funnel (promoted from `routes::slack`) ───────
//
// org-tenancy PR 1.6: `ingest_slack_message`, its atomic
// `persist_identity_and_enqueue_backfill` helper, and the pure
// `canonical_author_name` core moved here from `routes/slack.rs` VERBATIM
// (only the module path and these imports changed). They are CONSUMER-plane
// webhook machinery — invoked only from the signature-authenticated
// endpoints (`/v1/slack/events` below and `/v1/slack/interactivity`), which
// carry no Clerk JWT, hit the apex, and resolve `slack_integrations`/`graphs`/`atoms` on
// the consumer DB. This file is on `scripts/check_no_ambient_pool.sh`'s
// allowlist for exactly that reason; `routes/slack.rs`'s remaining
// JWT-authed management routes are fully `Tenant`-resolved and are not.

use crate::crypto::CryptoError;
use crate::jobs;
use crate::routes::slack::{resolve_channel_name, resolve_user_name};
use chrono::Utc;
use sha2::Digest;
use uuid::Uuid;

/// Decide the canonical author name for a Slack message author (CS-ER Part B),
/// given the persisted directory hit (if any), the name freshly derived from
/// Slack, and the raw Slack user id. Returns `(canonical_name, should_persist)`:
///
/// - A directory hit always wins → ONE stable name across atoms, never persisted
///   again (already stored).
/// - On a miss, the freshly-derived name is canonical; persist it ONLY when it is
///   a genuine display name (≠ the raw id), so a transient `users.info` failure —
///   which makes `resolve_user_name` fall back to the raw id — never freezes the
///   wrong value. A later success then writes the real name.
///
/// This is the pure core of the ingest canonicalization, factored out so the
/// "same human → same author_name" invariant is unit-testable without a DB.
fn canonical_author_name(
    directory_hit: Option<&str>,
    derived: &str,
    raw_user_id: &str,
) -> (String, bool) {
    match directory_hit {
        Some(name) => (name.to_string(), false),
        None => {
            let should_persist = derived != raw_user_id;
            (derived.to_string(), should_persist)
        }
    }
}

/// Commit the `connector_identities` upsert and the `SlackNameBackfill`
/// repair-job enqueue — see the call site's doc comment for why losing
/// atomicity between them is a durability gap on the CONSUMER plane (a
/// failed enqueue after a committed identity write is never retried).
/// Deliberately bypasses the `Store` trait (which has no "as part of an
/// existing transaction" seam) with raw SQL mirroring `PostgresStore::
/// upsert_connector_identity` exactly; connector identities are
/// Postgres-only server infrastructure regardless (migration 0049's doc
/// comment).
///
/// **`SlackNameBackfill` stays on the CONSUMER plane (org-slack step 2 —
/// deliberate, do not move).** `workers::slack_name_backfill` is
/// CONSUMER-ONLY, untouched by this step (`workers::tenant_harness`'s
/// decision table): an enqueue onto an org tenant's OWN `jobs` table would
/// be invisible to that worker's `state.db`-only dequeuer — the exact
/// "enqueue and its worker must flip planes together, or content silently
/// stops being processed" trap this PR's huddle-capture sequencing
/// constraint exists to name. So `control_pool` (always the shared
/// control/consumer pool) is where the job lands, REGARDLESS of which
/// plane `content_pool` is:
///
/// * **Consumer plane** (`content_pool` and `control_pool` are the SAME
///   physical database): unchanged, byte-identical to before this function
///   became plane-aware — one atomic transaction covers both writes.
/// * **Org tenant plane** (`content_pool` is a genuine, different physical
///   database from `control_pool`): atomicity across the two is impossible
///   by construction. The identity upsert — the load-bearing half, it is
///   what makes "same human, one name" hold at all — commits on
///   `content_pool` first; the enqueue then runs as its own best-effort
///   step against `control_pool`, logged and non-fatal on failure (the very
///   next message from this user re-enters the `should_persist_identity`
///   branch and retries, same self-healing property the original
///   single-transaction design relied on `fail`-and-retry for). Whether
///   that repair job then does anything USEFUL for an org-tenant graph is a
///   separate, not-yet-solved gap: `workers::slack_name_backfill` has no
///   tenant-loop counterpart, so its `UPDATE atoms ... WHERE graph_id = $1`
///   can only ever match consumer-plane rows — enqueuing it here for an
///   org-tenant `graph_id` produces an INERT job (matches zero rows), never
///   a WRONG one. Giving that worker real tenant reach is out of scope for
///   this step.
async fn persist_identity_and_enqueue_backfill(
    content_pool: &sqlx::PgPool,
    control_pool: &sqlx::PgPool,
    tenant: &crate::tenancy::TenantId,
    account_id: Uuid,
    graph_id: Uuid,
    team_id: &str,
    user: &str,
    canonical_name: &str,
) -> Result<(), sqlx::Error> {
    let kind = jobs::JobKind::SlackNameBackfill {
        graph_id: Some(graph_id),
        workspace_id: Some(team_id.to_string()),
        author_id: Some(user.to_string()),
        after_atom_id: None,
    };
    let idempotency = format!("slack_name_backfill:{graph_id}:{team_id}:{user}");

    match tenant {
        crate::tenancy::TenantId::Consumer => {
            // `content_pool` IS `control_pool` here — single atomic
            // transaction, exactly as before this function took two pools.
            let mut tx = content_pool.begin().await?;
            sqlx::query(
                "INSERT INTO connector_identities \
                     (provider, workspace_id, external_user_id, canonical_name) \
                 VALUES ('slack', $1, $2, $3) \
                 ON CONFLICT (provider, workspace_id, external_user_id) \
                 DO UPDATE SET canonical_name = EXCLUDED.canonical_name, updated_at = now()",
            )
            .bind(team_id)
            .bind(user)
            .bind(canonical_name)
            .execute(&mut *tx)
            .await?;
            jobs::queue::enqueue_tx(&mut tx, &kind, Some(account_id), Some(&idempotency), 0).await?;
            tx.commit().await
        }
        crate::tenancy::TenantId::Org(_) => {
            let mut tx = content_pool.begin().await?;
            sqlx::query(
                "INSERT INTO connector_identities \
                     (provider, workspace_id, external_user_id, canonical_name) \
                 VALUES ('slack', $1, $2, $3) \
                 ON CONFLICT (provider, workspace_id, external_user_id) \
                 DO UPDATE SET canonical_name = EXCLUDED.canonical_name, updated_at = now()",
            )
            .bind(team_id)
            .bind(user)
            .bind(canonical_name)
            .execute(&mut *tx)
            .await?;
            tx.commit().await?;

            if let Err(e) =
                jobs::queue::enqueue(control_pool, &kind, Some(account_id), Some(&idempotency), 0).await
            {
                tracing::warn!(
                    error = %e,
                    %account_id,
                    %graph_id,
                    "failed to enqueue SlackNameBackfill repair job onto the control plane \
                     (non-fatal — the identity write on the tenant plane already committed; \
                     retried on the next message from this user)"
                );
            }
            Ok(())
        }
    }
}

/// Ingest a single Slack message as atom(s) under the workspace's graph.
///
/// Shared by the unified `/v1/slack/events` endpoint ([`slack_events`] above, via the
/// durable `JobKind::SlackMessageIngest` hand-off and
/// `workers::slack_ingest`'s tenant-looped worker), and the interactivity
/// backfill (`routes::slack_interactivity::backfill_channel`). Dedups via the
/// `(graph_id, source_type, source_id)` unique key (`ON CONFLICT DO NOTHING`),
/// and uploads blobs to R2 when configured — so every call site shares one
/// canonical ingest path and can never drift.
///
/// **Plane-explicit (org-slack step 2).** This used to resolve `account_id`
/// itself from `team_id` via `resolve_slack_account_id` (a read against
/// `connector_installs`, a CONTROL-ONLY table — migration
/// `0121_connector_installs`'s own header), and to derive `pool`/`store`
/// from `state.db`/`state.store`
/// (always the control/consumer pool). Both assumptions broke the moment
/// this needed to run against a genuine org TENANT pool, so the caller now
/// resolves and supplies:
///
/// * `control_pool` — the control/consumer pool. Used for `resolve_channel_
///   destinations`' `account_members` read, the `slack_integrations` bot-
///   token/workspace-name lookup below (CONTROL-ONLY — see that read's own
///   comment for why), and the owner-authored self-reef lookups further
///   down (`oauth_tokens.provider_user_id`, `accounts.owner_user_id` — both
///   CONTROL-ONLY tables absent from a tenant database entirely). Every
///   OTHER read/write in this function (the message itself, `atoms`,
///   `connector_identities`, DEK resolution) is CONTENT-plane, on `pool`.
/// * `pool` / `store` — the CONTENT pool this account's data actually lives
///   on: `state.db`/`state.store` for a personal account or an org account
///   not (yet) migrated off the consumer plane, or the org's own resolved
///   tenant pool/store once one exists. `store` MUST wrap the exact same
///   pool as `pool` (see `workers::tenant_harness::WorkerTenant`'s own doc
///   comment on why a mismatched pair is a plane-mixing bug, not a
///   performance choice).
/// * `tenant` — which of the two `pool` is, driving DEK resolution
///   (`crypto::resolve_dek_for_tenant`) and the R2 blob-key scheme
///   (`coralswarm_core::r2::blob_key_for`'s org prefix).
/// * `account_id` / `account_plane` — resolved once by the caller (from
///   `connector_installs` on the control pool) and threaded straight
///   through; this function never re-derives either.
///
/// Every transaction below uses `begin_tenant_system_scoped`, not
/// `begin_account_scoped`: this is reached only from signature/webhook-
/// authenticated funnels with no acting end user, so it always asserts the
/// tenant SYSTEM principal (`0009_tenant_rls`'s member-ocean policies) —
/// harmless on the consumer plane, whose `0083` policies never read that GUC
/// (same convention `mcp::audit` already uses for its own no-acting-user
/// writes).
#[allow(clippy::too_many_arguments)]
pub(crate) async fn ingest_slack_message(
    state: Arc<AppState>,
    control_pool: sqlx::PgPool,
    pool: sqlx::PgPool,
    store: Arc<dyn crate::store::Store>,
    tenant: crate::tenancy::TenantId,
    account_id: Uuid,
    account_plane: super::slack::AccountPlane,
    team_id: String,
    channel: String,
    user: String,
    ts: String,
    thread_ts: Option<String>,
    text: String,
    // `bot_id`: `Some(B…)` when this message was posted by an APP, not a
    // human. Set at enqueue time from the raw event (the only place it
    // exists — the event does not survive into the job payload), and
    // load-bearing in three places below: it replaces the `users.info`
    // author resolution, it becomes the atom's `author_id`, and it forces
    // `is_owner_authored` false.
    //
    // `bot_name`: the app's display name, resolved at enqueue time by
    // `connectors::slack_message_text::bot_author_name`. Only read when
    // `bot_id` is `Some`; `None` there degrades to the same `"bot"` constant
    // that helper's last arm produces.
    bot_id: Option<String>,
    bot_name: Option<String>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    use coralswarm_core::chunker::chunk;

    // CONTROL, not `pool` — `slack_integrations` (like `oauth_tokens`) is
    // only ever written by the OAuth callback (`routes::oauth::callback` /
    // the legacy `routes::slack::oauth_callback`), and BOTH of those are
    // control-plane-only for every provider: the bot token lives on
    // CONTROL, only content goes to the tenant (the same split
    // `workers::huddle::get_slack_bot_token` already applies for the huddle
    // path). Before this fix this read ran on `pool` (the CONTENT pool),
    // which does not carry this row on a genuine org TENANT — every message
    // from that workspace fell into the "no integration found" arm below
    // and was silently discarded, never even reaching destination
    // resolution.
    //
    // Foundation PR 1.5: `workspace_name` (non-sensitive) is still read from
    // `slack_integrations` here; the bot token itself now comes from the
    // encrypted `oauth_tokens` copy via
    // `workers::huddle::get_slack_bot_token_for_workspace` — see that
    // function's doc comment. An account connected only through the legacy
    // `routes::slack::oauth_callback` path before this PR shipped has no
    // `oauth_tokens` row until `bin/backfill_slack_oauth_tokens` runs for it;
    // until then this workspace falls into the same "no integration found"
    // arm below as a genuinely missing `slack_integrations` row — the exact
    // "disconnect didn't stop ingest" incident class this PR closes reversed:
    // ingest does not resume for an unbackfilled legacy connection either.
    let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&control_pool, account_id).await?;
    let row = sqlx::query(
        "SELECT workspace_name \
         FROM slack_integrations \
         WHERE account_id = $1 AND workspace_id = $2 \
         LIMIT 1",
    )
    .bind(account_id)
    .bind(&team_id)
    .fetch_optional(&mut *tx)
    .await?;
    tx.rollback().await.ok();

    let workspace_name: String = match row {
        Some(r) => r.get("workspace_name"),
        None => {
            // This arm is reached when `resolve_slack_account_id` found an
            // account (via `connector_installs`) but the `slack_integrations`
            // row itself is missing on re-read — e.g. a concurrent uninstall,
            // or `record_connector_install`/`post_store` succeeded while the
            // sibling `slack_integrations` write failed independently (both
            // are separate best-effort writes — see
            // `routes/oauth.rs::record_connector_install`'s doc comment).
            // Just as much a dead connector for this call as a resolve-miss,
            // so raised the same way (this one is local, not centralized,
            // since it is specific to the `slack_integrations` re-read, not
            // the account resolution itself).
            tracing::warn!(
                team_id = %team_id,
                connector_health = true,
                "no integration found for Slack workspace — skipping message ingest"
            );
            return Ok(());
        }
    };

    // Two pools, two questions (org-plane DEK fix): the `oauth_tokens` ROW is
    // control-only, but for an org account its KEY lives in the tenant DB's
    // `account_keys`. `tenant`/`pool` are the plane this ingest already
    // resolved for content — the SAME pair the content DEK below is resolved
    // from (`resolve_dek_cached_for_tenant`), so the bot token and the atom
    // bodies can never end up on different planes.
    let bot_token_dek_plane =
        crate::crypto::DekPlane::from_tenant(&tenant, pool.clone(), account_id);
    let bot_token = match crate::workers::huddle::get_slack_bot_token_for_workspace(
        &control_pool,
        &bot_token_dek_plane,
        account_id,
        &team_id,
    )
    .await
    {
        Ok(Some(token)) => token,
        Ok(None) => {
            tracing::warn!(
                team_id = %team_id,
                connector_health = true,
                "no oauth_tokens bot token for Slack workspace (not yet backfilled from the \
                 legacy slack_integrations connection?) — skipping message ingest"
            );
            return Ok(());
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                team_id = %team_id,
                connector_health = true,
                "failed to resolve Slack bot token from oauth_tokens — skipping message ingest"
            );
            return Ok(());
        }
    };

    // Slack messages ingest into the CHANNEL's destination(s)
    // (docs/connectors/slack.md §2/§4,
    // docs/connectors/CONNECTOR_CONTRACT.md §4), read via
    // `connectors::slack::resolve_channel_destinations` instead of resolving
    // a single primary ocean at ingest time. An explicit
    // `slack_watched_channels` row (contract I2) always wins; a channel with
    // none yet falls back to the account-level default: a PERSONAL account's
    // own primary ocean (byte-identical to before this table existed), or —
    // for an ORG account — a live fan-out to each MAPPED member's own ocean
    // (never a shared ocean, contract I4). Identity mapping is not wired for
    // Slack today (capability C5 —
    // `connectors::slack::resolve_org_fanout_destinations`'s doc comment), so
    // in practice this resolves to an EMPTY destination set for every org
    // account with no explicit row until that lands: we log the unmapped
    // count and land nothing rather than guessing a seat (never fabricate).
    //
    // This previously created a separate "Slack: {workspace_name}" ocean,
    // which polluted the personal/team/org taxonomy (ADR-0006 / migration
    // 0026 designates `is_primary` as the default ingest destination
    // instead).
    let (grant, unmapped_members) = crate::ingest::OrgSharedGrant::slack_channel(
        &control_pool, &pool, &tenant, account_id, account_plane, &team_id, &channel,
    )
    .await?;
    if unmapped_members > 0 {
        tracing::warn!(
            account_id = %account_id,
            workspace = %workspace_name,
            channel = %channel,
            unmapped_members,
            connector_health = true,
            "Slack org fan-out: could not map {unmapped_members} account member(s) to a \
             Slack identity yet (capability C5 unwired) — their oceans are skipped, \
             never guessed"
        );
    }
    if grant.oceans().is_empty() {
        tracing::debug!(
            account_id = %account_id,
            workspace = %workspace_name,
            channel = %channel,
            "Slack ingest → no mappable destination for this org account yet, skipping \
             (never fabricating a seat)"
        );
        // C5's "unmatched content drops and notifies in-channel"
        // requirement (docs/connectors/slack.md): a human who
        // watched a message vanish deserves to know it wasn't saved
        // and why. Best-effort — never fails ingest over a courtesy
        // message.
        //
        // Rate-limited per (account, channel). This branch runs for
        // EVERY message in an unmapped channel, and `backfill_
        // channel` replays history through this same function, so
        // ungated it is an unbounded bot flood rather than a
        // courtesy. The cooldown bounds backfill too, which is why
        // there is no separate is-backfill flag threaded through
        // `ingest_slack_message`: the flood is the thing being
        // fixed, and one notice mid-backfill is accurate anyway.
        if crate::connectors::slack::should_send_unmatched_notice(account_id, &channel) {
            // The signal that was missing entirely. Everything about
            // this state looked healthy from our side — the job
            // completes, no error, Slack gets its 200 — and the ONLY
            // report that content was being dropped went to the Slack
            // channel itself, via the notice just below. The `debug!`
            // above sits under the production log filter, so an org
            // could capture nothing for days while every dashboard
            // stayed green. That is exactly what happened on testorg:
            // the bot told the channel "this wasn't saved" hours
            // before anyone looked at the database.
            //
            // Deliberately on the notice's OWN 6-hour cooldown rather
            // than per message: this branch runs for every message in
            // an unmapped channel and `backfill_channel` replays
            // history through it, so an ungated `warn!` is the
            // "logged every few seconds until it read as noise"
            // failure that POOL_FAILURE_ALERT_THRESHOLD's doc
            // describes. One alert per channel per shift, matching
            // what the humans in that channel are told.
            tracing::warn!(
                account_id = %account_id,
                workspace = %workspace_name,
                channel = %channel,
                unmapped_members,
                connector_health = true,
                // Cause-NEUTRAL on purpose. An empty fan-out has more
                // than one cause: no member's Slack identity linked
                // yet, OR identities linked but member-ocean
                // provisioning has not caught up (see
                // `resolve_org_fanout_destinations`, whose own comment
                // names both). Asserting the first would send an
                // operator hunting identity links while the real fault
                // was provisioning — and an alert that misdiagnoses is
                // worse than one that just states the fact. The
                // `unmapped_members` field above is what discriminates:
                // > 0 means identities are missing, 0 means they
                // resolved and the oceans did not.
                "Slack org fan-out resolved to ZERO destinations — every message in this \
                 channel is being dropped. No member ocean could be resolved for this \
                 account. Check Slack identity links and member-ocean provisioning; the \
                 `unmapped_members` field distinguishes them (>0 = identities missing, \
                 0 = identities resolved but no ocean)."
            );
            crate::connectors::slack::notify_unmatched_content_not_saved(
                &state.http_client,
                &bot_token,
                &channel,
                "This message",
            )
            .await;
        }
        return Ok(());
    }
    tracing::debug!(
        account_id = %account_id,
        workspace = %workspace_name,
        channel = %channel,
        destinations = grant.oceans().len(),
        "Slack ingest → destination(s) resolved"
    );

    // Parse the Slack timestamp into a DateTime.
    let timestamp: chrono::DateTime<Utc> = {
        // Slack ts is "1234567890.123456" — take the integer seconds part.
        let secs: i64 = ts.split('.').next().unwrap_or("0").parse().unwrap_or(0);
        chrono::DateTime::from_timestamp(secs, 0).unwrap_or_else(Utc::now)
    };

    // Build a unique source_id: channel/ts (optionally thread_ts).
    let source_id = match &thread_ts {
        Some(tts) => format!("{channel}/{tts}/{ts}"),
        None => format!("{channel}/{ts}"),
    };

    // Resolve human-readable channel + sender names (cached; falls back to raw
    // ids on any API error so ingest never breaks). These become the displayed
    // "who / where / when" context — attached to each atom's content and to its
    // participants — so an ingested message isn't an anonymous, context-free blob.
    let channel_name = resolve_channel_name(&state.http_client, account_id, &channel, &bot_token).await;
    // The message permalink, from the workspace origin (`auth.test`, cached
    // per workspace) and ids in hand: no `chat.getPermalink` per message.
    let permalink = crate::routes::slack::resolve_workspace_url(&state.http_client, account_id, &team_id, &bot_token)
        .await
        .and_then(|origin| crate::routes::slack::message_permalink(&origin, &channel, &ts, thread_ts.as_deref()));

    // ── Author canonicalization (CS-ER Part B) ────────────────────────────────
    // The SAME human must surface under ONE `author_name` across atoms so their
    // stated relationships anchor to a single author reef. The per-process name
    // cache in `resolve_user_name` is evicted on restart and then falls back to
    // the RAW Slack id (e.g. "U57C3A9EF09") — so one person's atoms scatter
    // between "Alex Example" and "U57C3A9EF09". We back it with the persistent
    // `connector_identities` directory keyed by (provider, workspace, user id):
    // consult it first; on a miss, derive once via Slack and persist, so the
    // canonical name is stable forever after.
    //
    // Graph-independent — computed ONCE for the message, then reused for every
    // destination graph in the loop below. Only the DECISION to persist +
    // schedule a backfill is made here; the persist+enqueue itself happens
    // per-destination-graph inside the loop (the backfill job is scoped to one
    // graph's atoms, so a fan-out message needs one job per destination).
    //
    // ── Bot-authored messages take NONE of this path ─────────────────────────
    // An app has no `users.info` record worth fetching (`users.info?user=
    // U…` for a GitHub-shaped app answers, but with the app's opaque
    // integration user, and a legacy `bot_message` carries no `user` AT ALL —
    // the pre-change code sent the literal string "unknown" to Slack and got
    // `user_not_found` back). Its identity is already in the event:
    // `bot_profile.name` / `username`, resolved at enqueue time. So a bot
    // message makes NO `users.info` call, consults NO directory, and
    // deliberately persists NO `connector_identities` row — that table is the
    // human-name directory the `SlackNameBackfill` repair job sweeps, and
    // seeding it with app names would put "GitHub" into the same namespace as
    // the humans it exists to de-duplicate.
    // `bot_name.is_some()` for the same reason the enqueue-side check widens:
    // a legacy `bot_message` with no `bot_id` is still an app post, and must
    // not fall into the human branch — which would send the literal string
    // "unknown" (this function's `user` default) to `users.info` and get
    // `user_not_found` back, exactly the wasted round-trip this whole branch
    // exists to avoid.
    let is_bot_authored = bot_id.is_some() || bot_name.is_some();
    let (sender_name_base, should_persist_identity): (String, bool) = if is_bot_authored {
        (
            bot_name.clone().unwrap_or_else(|| "bot".to_string()),
            false,
        )
    } else {
        let directory_hit = store
            .connector_identity_name("slack", &team_id, &user)
            .await
            .ok()
            .flatten();
        match directory_hit {
            Some(name) => (name, false),
            None => {
                let derived =
                    resolve_user_name(&state.http_client, account_id, &user, &bot_token).await;
                canonical_author_name(None, &derived, &user)
            }
        }
    };

    // The atom's `author_id` — see `slack_message_text::author_id_for` for the
    // three id spaces and why the third one exists. In short: `B…` for a
    // modern app post, `U…` for a human, and a namespaced `slack_app:{slug}`
    // for a legacy `bot_message` that carries no `bot_id`. That last case used
    // to fall through to `user`, whose default in this function is the literal
    // string `"unknown"` — collapsing every such app in every workspace onto
    // one author id, and voiding the `B…`/`U…` no-collision argument for
    // exactly the payload shape it did not cover (review finding 6).
    let author_id: String = crate::connectors::slack_message_text::author_id_for(
        &crate::connectors::slack_message_text::AdmittedMessage {
            text: String::new(),
            bot_id: bot_id.clone(),
            bot_name: bot_name.clone(),
        },
        &user,
    );

    // ── Author-mapped `ingested_by` attribution (journal completeness gap 1) ──
    // Every journal read filters `atoms.ingested_by = $user`
    // (`store::journal_hour_inputs`), so a NULL-authored atom is invisible to
    // EVERY user's journal forever, no matter which ocean it landed in. Resolve
    // the Slack author to a CoralSwarm user via the SAME confirmed
    // `connector_identities` directory `resolve_org_fanout_destinations` and
    // `handle_member_left` already read through
    // [`crate::seats::resolve_linked_user_directory_only`] — directory-only, no
    // live Slack API call, and no live-API side effect makes it safe to run
    // unconditionally on the hot ingest path.
    //
    // Deliberately unscoped (`&pool`, no `begin_account_scoped`):
    // `connector_identities` carries no RLS policy at all (migration 0104's
    // "RLS: none added, and none needed" header — it is shared reference data
    // keyed by (provider, workspace_id, external_user_id), not by account), the
    // same posture `resolve_linked_user_directory_only`'s own callers already
    // rely on.
    //
    // An author with no confirmed link (never signed up for CoralSwarm, or an
    // unlinked guest) resolves to `None` — the atom keeps `ingested_by = NULL`,
    // same accepted "invisible to journals" degradation as today, never
    // guessed from a display name. Graph-independent (the author doesn't
    // change per destination) — computed once, reused for every destination
    // graph in the loop below, exactly like `sender_name_base` above.
    //
    // Best-effort, not `?`: a TRANSIENT DB error here (a blip on this one
    // query) must never drop the whole message — losing attribution for one
    // message is acceptable (same degradation as an unmapped author), losing
    // the message itself is not. Every other best-effort lookup in this
    // function (`owner_slack_id`, `directory_hit`) already follows this same
    // "log and degrade" posture rather than propagating.
    //
    // ── OPEN QUESTION for the founder: bot atoms are journal-invisible ───────
    //
    // A BOT author is never a CoralSwarm user, so the lookup is skipped
    // outright rather than run and missed — one less query on the hot path,
    // and no chance of a `B…` id ever matching a `connector_identities` row.
    //
    // The CONSEQUENCE, called out by the review of #861 and deliberately
    // shipped as-is rather than decided unilaterally: `ingested_by = NULL`
    // makes every bot atom invisible to every journal and rollup read, all
    // of which filter on this column — `store::mod.rs` :25826, :26004,
    // :26145, :26222 and :26916 (`a.ingested_by = $1`) and :26383, :26528,
    // :26649, :26781 (`a.ingested_by IS NOT NULL`). So the ~235 messages a
    // month this change exists to capture DO land in the ocean and ARE
    // searchable and citable, but never appear in a Today page or any
    // journal rollup.
    //
    // NULL is arguably the semantically correct value — no human ingested
    // these, and guessing an owner would put a machine's output in a
    // person's day. The alternative, if the founder wants them in journals,
    // is to attribute each bot atom to the OWNER OF THE CHANNEL'S
    // DESTINATION graph (already resolved in the loop below), which is a
    // behaviour change, not a bug fix, and belongs in its own PR with its
    // own decision. Documented in `docs/connectors/slack.md` §2.1 too.
    let ingested_by: Option<Uuid> = if is_bot_authored {
        None
    } else {
        match crate::seats::resolve_linked_user_directory_only(&pool, &team_id, &user).await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(
                    error = %e,
                    workspace_id = %team_id,
                    slack_author = %user,
                    "slack ingest: ingested_by attribution lookup failed — degrading to NULL \
                     (best-effort; the message itself is still ingested)"
                );
                None
            }
        }
    };

    // ── First-person binds to the per-ocean self reef (CS-ER Part C) ──────────
    // When the message author is the OCEAN OWNER, "I"/"my X" must attach to the
    // owner's single self reef even if the owner's display name varies. The Slack
    // user who connected this workspace (`oauth_tokens.provider_user_id`) is the
    // account owner; if this message's author id matches, we ensure the self reef
    // and use its title as the author label fed to extraction.
    // CONTROL, not `pool` (org-slack step 5a) — `oauth_tokens` is only ever
    // written by the control-plane-only OAuth callback (same reasoning as
    // the `slack_integrations` read above), so on a genuine org TENANT pool
    // this table exists (kept for cross-tree schema parity) but is
    // permanently EMPTY: the read used to succeed with zero rows rather
    // than error, so `is_owner_authored` silently and permanently read
    // `false` for every org account instead of surfacing the plane
    // mismatch. Best-effort by contract — a scope failure degrades to "not
    // the owner", exactly as the pre-existing `unwrap_or(None)` did for a
    // query error.
    //
    // Graph-independent (the workspace owner doesn't change per destination) —
    // computed once; the self reef it binds to below IS per-graph (one self
    // reef per ocean), so that bind stays inside the loop.
    let owner_slack_id: Option<String> =
        match crate::tenancy::scope::begin_tenant_system_scoped(&control_pool, account_id).await {
            Ok(mut tx) => {
                // Foundation PR 1.1: `owner_user_id IS NULL` — the workspace
                // installer's row is always account-wide (Slack has no
                // per-member OAuth, §14.1).
                let v = sqlx::query_scalar(
                    "SELECT provider_user_id FROM oauth_tokens \
                     WHERE provider = 'slack' AND provider_workspace_id = $1 AND account_id = $2 \
                     AND owner_user_id IS NULL \
                     LIMIT 1",
                )
                .bind(&team_id)
                .bind(account_id)
                .fetch_optional(&mut *tx)
                .await
                .unwrap_or(None)
                .flatten();
                tx.rollback().await.ok();
                v
            }
            Err(e) => {
                tracing::warn!(error = %e, "slack ingest: owner lookup scoped tx failed");
                None
            }
        };
    // `!is_bot_authored` is belt-and-braces, not the load-bearing guard: the
    // owner id is a `U…` and a bot author's `author_id` is a `B…`, so the
    // comparison below cannot match for an app anyway. Stated explicitly so a
    // future change to either side cannot quietly bind an app's messages to
    // the human owner's self reef.
    let is_owner_authored =
        !is_bot_authored && owner_slack_id.as_deref() == Some(user.as_str());

    // `owner_user_id` is the human the self reef stands for (the ocean's
    // account owner) — CONTROL, not `pool` (org-slack step 5a), same
    // reasoning as `owner_slack_id` immediately above: `accounts` is a
    // CONTROL-ONLY table, entirely absent from a tenant database
    // (`migrations_tenant/0001_tenant_baseline.sql`'s own header). This
    // used to run INSIDE the per-destination loop below, on `accounts a
    // JOIN graphs g ON g.account_id = a.id WHERE g.id = $1` — a JOIN across
    // what are, under a genuine org tenant, TWO SEPARATE PHYSICAL DATABASES
    // (impossible to express as one query at all; it failed outright with
    // "relation \"accounts\" does not exist", swallowed by `unwrap_or(None)`
    // into the same silent "not the owner" degradation as a benign scope
    // failure). The `graphs` JOIN existed only to turn `graph_id` into this
    // message's `account_id` — this function already HAS that `account_id`
    // as its own parameter, so the JOIN was never load-bearing; querying
    // `accounts` directly by `account_id` removes the cross-plane JOIN.
    //
    // Hoisted OUT of the per-destination loop (review finding, same class
    // as the two bugs above): this is keyed only by `account_id`, so on an
    // org fan-out with N>1 destinations (capability C5) it used to run the
    // SAME query N times on the hot ingest path for an identical answer —
    // its sibling `owner_slack_id` above was already hoisted for exactly
    // this reason, and this one should have been too. Only queried when
    // actually owner-authored; every other message never touches `accounts`
    // at all. `accounts` carries no 0083 policy (same convention
    // `routes::slack_interactivity::resolve_account_owner` already uses),
    // so no scoped transaction is needed. Best-effort: a query error still
    // degrades to "not the owner", never propagated.
    let owner_user_id: Option<Uuid> = if is_owner_authored {
        sqlx::query_scalar("SELECT owner_user_id FROM accounts WHERE id = $1")
            .bind(account_id)
            .fetch_optional(&control_pool)
            .await
            .unwrap_or(None)
    } else {
        None
    };

    let when = timestamp.format("%Y-%m-%d %H:%M UTC");

    // Slack messages chunk with the Slack strategy (`ingest::capture::
    // chunk_strategy` for "slack"). Graph-independent — computed once.
    let chunks = chunk(&text, crate::ingest::capture::chunk_strategy("slack", None, None));

    // Content hash for idempotency key (computed once over the full text,
    // matching the convention in ingest.rs).
    let content_hash = hex::encode(sha2::Sha256::digest(text.as_bytes()));

    // ── Resolve the real per-account DEK ─────────────────────────────────────
    // Use resolve_dek_cached_for_tenant (org-slack step 2) so repeated
    // ingestion within the 5-minute TTL window avoids an extra DB round-trip,
    // AND so an org-tenant account's DEK is read from ITS tenant `account_keys`
    // rather than the control plane's `accounts.dek_wrapped` — the same split
    // `crypto`'s module doc describes for every other tenant-aware caller.  On
    // failure we log and bail — a null-key "encryption" would be a security
    // hole, so we never fall through to the placeholder path. Graph-independent
    // (one DEK per account).
    let dek: [u8; 32] =
        match crate::crypto::resolve_dek_cached_for_tenant(&tenant, &pool, account_id, &state.dek_cache)
            .await
        {
        Ok(Some(d)) => d,
        Ok(None) => {
            tracing::warn!(
                account_id = %account_id,
                "account has no DEK — skipping Slack message ingest"
            );
            return Ok(());
        }
        Err(
            ref e @ (CryptoError::MasterKekNotSet(_)
            | CryptoError::MasterKekInvalid(_)
            | CryptoError::DbError(_)),
        ) => {
            tracing::warn!(error = %e, account_id = %account_id, "KEK/DB error resolving DEK — skipping Slack ingest");
            return Ok(());
        }
        Err(e) => {
            tracing::warn!(error = %e, account_id = %account_id, "DEK resolution failed — skipping Slack ingest");
            return Ok(());
        }
    };

    // ── Per-destination ingest ─────────────────────────────────────────────
    // The grant's oceans are a single destination for the overwhelming majority of
    // messages today (personal accounts, and org accounts with an explicit
    // `slack_watched_channels` row) — this loop only ever iterates more than
    // once once org fan-out has mappable members (capability C5 wired; see
    // `connectors::slack::resolve_org_fanout_destinations`). Each iteration
    // is independently best-effort: a failure landing this message in one
    // destination must not prevent it landing in the others.
    //
    // That promise used to be false: three `?` operators inside this loop's
    // body (the per-chunk scoped-tx open, the atom INSERT, and the commit)
    // propagated out of the WHOLE function, so a transient error on
    // destination 2 of N silently skipped destinations 3..N without even a
    // log line — unreachable while `resolve_channel_destinations`'s fan-out
    // returns at most one graph (capability C5 unwired), which is exactly
    // why no test caught it, but the comment above is what the C5
    // implementer would have trusted. Each iteration's body is now its own
    // fallible unit (an `async` block returning the atom count for that
    // destination) so a `?` inside it can only end THAT iteration —
    // caught below and turned into a log-and-continue, with a
    // success/failure count in the final summary line.
    let total_destinations = grant.oceans().len();
    let mut destinations_ok: usize = 0;
    let mut destinations_failed: usize = 0;

    for &graph_id in grant.oceans() {
        let destination_result: Result<usize, Box<dyn std::error::Error + Send + Sync>> = async {
        // Recheck at the write boundary, including jobs queued before a pause
        // and channel backfills. One member's gate affects only their ocean.
        if !crate::connectors::catalog::scope::slack_destination_admitted(
            &control_pool, &pool, account_id, graph_id, &team_id, &channel,
        ).await? {
            return Ok(0);
        }
        // Identity persist + backfill schedule — see the "Author
        // canonicalization" comment above for why the DECISION is made once
        // but the write happens per destination graph.
        if should_persist_identity {
            // ── Natural repair trigger (slack-name-backfill), made durable ──
            // The directory is about to gain a real name for this Slack
            // user — every atom this person authored BEFORE this
            // resolution still has the raw id frozen into
            // `author_name`/`participants` (PR #176: the frontend now
            // masks those as "Unknown author", but the stored value never
            // repairs itself). This is the ONLY moment that schedules the
            // repair: once `connector_identity_name` hits above, later
            // ingests for this same user never re-enter this branch, so
            // an enqueue that failed here with the identity write already
            // committed would silently drop the repair forever.
            //
            // `persist_identity_and_enqueue_backfill` commits the
            // `connector_identities` upsert and the `SlackNameBackfill`
            // enqueue as ONE Postgres transaction ON THE CONSUMER PLANE, so
            // that can't happen there: if the enqueue fails, the identity
            // write rolls back too, and the very next message from this
            // user re-enters this branch and retries both together. On an
            // org TENANT plane the two writes cross physical databases and
            // cannot share a transaction — see that function's own doc
            // comment for the accepted trade-off, and for why the enqueue
            // always targets `control_pool` regardless of `tenant`
            // (`workers::slack_name_backfill` stays consumer-only in this
            // step). Still best-effort at the outer level either way — a
            // rolled-back/failed attempt never blocks ingest of the current
            // message.
            if let Err(e) = persist_identity_and_enqueue_backfill(
                &pool,
                &control_pool,
                &tenant,
                account_id,
                graph_id,
                &team_id,
                &user,
                &sender_name_base,
            )
            .await
            {
                tracing::warn!(
                    error = %e,
                    slack_user = %user,
                    graph_id = %graph_id,
                    "failed to durably persist connector identity + schedule backfill (non-fatal — retried on the next message from this user)"
                );
            }
        }

        let sender_name = if is_owner_authored {
            // Owner-authored: bind to the self reef. Its title becomes the
            // canonical author label so "my X" edges anchor to ONE owner
            // reef. `owner_user_id` was resolved once, above the loop over
            // every destination graph — see that hoisted read's own
            // comment for why (CONTROL-only `accounts` table, and why this
            // must not re-query per destination). `Uuid` is `Copy`, so
            // re-reading the outer `owner_user_id` here on every iteration
            // costs nothing.
            match owner_user_id {
                // `ensure_self_node` reads `graphs` and writes `nodes`, both
                // 0083-covered, on `self.pool.begin()` — an UNSCOPED
                // transaction. In production that raised `invalid input
                // syntax for type uuid: ""` on every owner-authored Slack
                // message, and the `warn!` below swallowed it into the
                // sender-name fallback, so the self reef was never bound and
                // nothing looked broken. Observed live on 2026-07-27 in
                // #connector-verify-v224, one message after the sibling
                // picker fix landed.
                //
                // `ensure_self_node_scoped` already existed for exactly this;
                // the call site simply used the wrong one. COMMIT, not
                // rollback — unlike the read above, this one writes.
                Some(owner_user_id) => {
                    let scoped: Result<String, String> = async {
                        let mut tx = crate::tenancy::scope::begin_tenant_system_scoped(&pool, account_id)
                            .await
                            .map_err(|e| format!("self-node scoped tx: {e}"))?;
                        let (self_node_id, self_title) = store
                            .ensure_self_node_scoped(
                                &mut tx,
                                graph_id,
                                owner_user_id,
                                &sender_name_base,
                            )
                            .await
                            .map_err(|e| format!("ensure_self_node_scoped: {e}"))?;

                        // Identity-resolution-at-the-door §10.3: bind the
                        // owner's identifiers at the self reef too, so the
                        // door's step 2 (and ER's identifier auto-merge) is
                        // decisive for every future mention. `clerk_user_id`/
                        // `email` come from `control_pool` — NOT the `pool`/
                        // `tx` this self-node call runs on — because on a
                        // genuine org TENANT `pool` carries no `users` table
                        // at all (see `crate::store::
                        // bind_principal_identifiers_values`'s doc for the
                        // same cross-plane shape `tenancy::member_ocean::
                        // provision_one` already needs). `slack_user` is
                        // bound from the sender id in hand on this same
                        // message, matching a connector-verified sender.
                        let owner_identity: Option<(Option<String>, String)> = sqlx::query_as(
                            "SELECT clerk_user_id, email FROM users WHERE id = $1",
                        )
                        .bind(owner_user_id)
                        .fetch_optional(&control_pool)
                        .await
                        .unwrap_or(None);
                        if let Some((clerk_user_id, email)) = owner_identity {
                            crate::store::bind_principal_identifiers_values(
                                &mut tx,
                                graph_id,
                                self_node_id,
                                clerk_user_id.as_deref(),
                                Some(&email),
                            )
                            .await
                            .map_err(|e| format!("bind_principal_identifiers_values: {e}"))?;
                        }
                        crate::store::bind_slack_identifier_scoped(&mut tx, graph_id, self_node_id, &user)
                            .await
                            .map_err(|e| format!("bind_slack_identifier_scoped: {e}"))?;

                        tx.commit().await.map_err(|e| format!("self-node commit: {e}"))?;
                        Ok(self_title)
                    }
                    .await;

                    match scoped {
                        Ok(self_title) => self_title,
                        Err(e) => {
                            tracing::warn!(
                                error = %e,
                                "ensure_self_node failed (non-fatal) — using sender name"
                            );
                            sender_name_base.clone()
                        }
                    }
                }
                None => sender_name_base.clone(),
            }
        } else {
            sender_name_base.clone()
        };

        // "(app)" is part of the header, not the author name: the name goes
        // into `author_name`/`participants` (where ER treats it as an
        // entity), so decorating it there would fork "GitHub" from
        // "GitHub (app)" across atoms. In the body header it is a reader's
        // cue that a machine wrote this, which is exactly where it belongs.
        let context_header = if is_bot_authored {
            format!("{sender_name} (app) in #{channel_name} · {when}")
        } else {
            format!("{sender_name} in #{channel_name} · {when}")
        };

        // Snapshot, readable chunks, associations and both jobs commit as one
        // unit through the ingest door, under this channel's routing grant. A
        // failed capture/queue write leaves no deduplication tombstone, and a
        // failed body upload fails this destination (Slack does not redeliver
        // the event, so the job retry is the only second chance). The header
        // stays inline with no `header_len` (the historical Slack shape, so
        // existing chunk hashes still match); the fallback title and journal
        // excerpt come from the message body.
        let namespace=grant.namespace();
        let metadata=serde_json::json!({"channel_id":channel,"channel_name":channel_name,
            "author_id":author_id,"author_name":sender_name,"thread_ts":thread_ts,"url":permalink});
        use crate::source_origin::{SourceOrigin, SourceContainer, CaptureTransport, ServiceId,
            ContentKind, CaptureSurfaceId, ContainerKind, ContainerId, ProviderNamespace};
        let origin = SourceOrigin {
            service_id: ServiceId::new("slack")?, transport: CaptureTransport::Native,
            content_kind: ContentKind::new("message")?,
            capture_surface: Some(CaptureSurfaceId::new("slack_message_events")?),
            connection: None,
            container: Some(SourceContainer { kind: ContainerKind::new("channel")?,
                id: ContainerId::new(channel.clone())?,
                provider_namespace: ProviderNamespace::new(namespace.to_string())?,
                label: Some(channel_name.clone()),
            }),
        };
        let participants=[sender_name.clone()];
        let prepared=crate::ingest::capture::Prepared::new(&source_id,"slack",&text,&chunks,
            &crate::ingest::capture::Header::Inline(context_header),None);
        let ingested=crate::ingest::submit(crate::ingest::IngestRequest {
            origin:Some(&origin),
            ..crate::ingest::IngestRequest::new(crate::ingest::Destination::OrgShared(crate::ingest::Routed {
                ocean:graph_id, grant:&grant,
                writer:crate::ingest::SystemWriter {pool:&pool,tenant:&tenant,dek:&dek,r2:state.r2.as_deref()},
                source:crate::source_evidence::CaptureSource {
                    source_type:"slack",provider_namespace:namespace,source_id:&source_id,
                    normalized_text:&text,representation:"normalized_text",metadata:&metadata,
                    occurred_at:Some(timestamp),participants:&participants,
                    ingested_by,scope:"account",duration_ms:None,segments:None,
                },
                chunks:&prepared,
                atoms:crate::ingest::capture::AtomFields {
                    scope:"account",
                    channel_id:Some(&channel),channel_name:Some(&channel_name),
                    author_id:Some(&author_id),author_name:Some(&sender_name),workspace_id:Some(&team_id),
                    ..crate::ingest::capture::AtomFields::new("slack",&participants,timestamp,ingested_by)
                },
                content_hash:&content_hash,
                sync_ops:None,
            }))
        }).await;
        let ingested=match ingested {
            Ok(ingested)=>ingested,
            Err(refused)=>return Err(refused.into()),
        };

        for new_id in &ingested.body.atom_ids {
            tracing::info!(
                atom_id = %new_id,
                graph_id = %graph_id,
                "Slack message atom ingested"
            );
        }

        Ok(ingested.body.atom_ids.len())
        }
        .await;

        match destination_result {
            Ok(atom_count) => {
                destinations_ok += 1;
                tracing::debug!(
                    graph_id = %graph_id,
                    atom_count,
                    "Slack ingest: destination succeeded"
                );
            }
            Err(e) => {
                destinations_failed += 1;
                tracing::warn!(
                    error = %e,
                    graph_id = %graph_id,
                    account_id = %account_id,
                    "Slack ingest: destination failed — continuing with the remaining \
                     destinations (each destination is independently best-effort; a failure \
                     here must not skip the rest of the fan-out)"
                );
            }
        }
    }

    if total_destinations > 1 {
        tracing::info!(
            total_destinations,
            destinations_ok,
            destinations_failed,
            "Slack ingest: fan-out complete"
        );
    }

    if destinations_failed > 0 {
        return Err(format!("Slack ingest failed for {destinations_failed} of {total_destinations} destinations; retry remaining destinations").into());
    }

    Ok(())
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Request as HttpRequest, StatusCode},
        routing::post,
        Router,
    };
    use tower::ServiceExt;

    fn test_state() -> Arc<AppState> {
        crate::test_app_state()
    }

    fn events_router(state: Arc<AppState>) -> Router {
        Router::new()
            .route("/v1/slack/events", post(slack_events))
            .with_state(state)
    }

    // ── Helper: build a valid HMAC signature ──────────────────────────────────
    //
    // M1: The helper mirrors the updated incremental HMAC construction so the
    // known-vector test stays aligned with the implementation.

    fn make_signature(secret: &str, timestamp: &str, body: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(b"v0:");
        mac.update(timestamp.as_bytes());
        mac.update(b":");
        mac.update(body.as_bytes());
        format!("v0={}", hex::encode(mac.finalize().into_bytes()))
    }

    fn now_ts() -> String {
        chrono::Utc::now().timestamp().to_string()
    }

    /// Process-wide guard for every test in this module that mutates the
    /// global `SLACK_SIGNING_SECRET` env var — same pattern as
    /// `llm::retry`'s env-lock tests. Without it, two tests running in parallel
    /// (the default for `cargo test`) that both set/remove this var race each
    /// other: one test's `set_var` can land between another's `remove_var`
    /// and its own read, flipping `verify_slack_signature`'s outcome
    /// underneath it. Every test below that touches the env var (not just the
    /// new HTTP-round-trip ones) takes this lock, including the two
    /// pre-existing ones that used to rely on "only removes, then restores"
    /// being disruptive-but-safe.
    fn slack_signing_secret_lock() -> &'static std::sync::Mutex<()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
    }

    // ── Signature verification: valid HMAC passes ─────────────────────────────
    // These tests use `verify_slack_signature_with_secret` directly to avoid
    // mutating process-global env-var state under parallel test execution.

    #[test]
    fn valid_signature_verifies() {
        let secret = "test_secret_abc123";
        let ts = now_ts();
        let body = b"hello=world";
        let sig = make_signature(secret, &ts, "hello=world");

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        let result = verify_slack_signature_with_secret(secret, &headers, body);
        assert_eq!(result, VerifyResult::Ok);
    }

    // ── Signature verification: known-vector test ─────────────────────────────
    //
    // M1: The HMAC base string construction is identical for ASCII bodies
    // (incremental updates produce the same digest as format!("v0:{ts}:{body}")).
    // This test pins the computed value for a fixed ASCII input to catch
    // accidental regressions.

    #[test]
    fn known_vector_ascii_body() {
        let secret = "example_5";
        // Timestamp chosen so it won't expire — use a future value far enough out.
        // We test verify_slack_signature_with_secret which skips the replay window
        // only when we want; here we pass a live timestamp.
        let ts = now_ts();
        let body = b"token=xyzz0WbapA4vBCDEFasx0q6G&user_id=U0KRQLJ9H&";
        let sig = make_signature(secret, &ts, std::str::from_utf8(body).unwrap());

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        assert_eq!(
            verify_slack_signature_with_secret(secret, &headers, body),
            VerifyResult::Ok
        );
    }

    // ── Signature verification: wrong signature → mismatch ───────────────────

    #[test]
    fn wrong_signature_rejected() {
        let secret = "test_secret_abc123";
        let ts = now_ts();
        let body = b"hello=world";

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        // A signature that's not 64 hex chars after "v0=" → decode fails → mismatch.
        headers.insert("x-slack-signature", "v0=deadbeef".parse().unwrap());

        let result = verify_slack_signature_with_secret(secret, &headers, body);
        assert_eq!(result, VerifyResult::SignatureMismatch);
    }

    // ── Signature verification: wrong signature (correct length) ─────────────

    #[test]
    fn wrong_signature_correct_length_rejected() {
        let secret = "test_secret_abc123";
        let ts = now_ts();
        let body = b"hello=world";

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        // All-zero hex digest — same length as a real sig but wrong value.
        headers.insert(
            "x-slack-signature",
            "v0=0000000000000000000000000000000000000000000000000000000000000000"
                .parse()
                .unwrap(),
        );

        let result = verify_slack_signature_with_secret(secret, &headers, body);
        assert_eq!(result, VerifyResult::SignatureMismatch);
    }

    // ── Signature verification: stale timestamp → invalid ────────────────────

    #[test]
    fn stale_timestamp_rejected() {
        let secret = "test_secret_abc123";
        // 10 minutes ago — well outside the 300-second window.
        let old_ts = (chrono::Utc::now().timestamp() - 600).to_string();
        let body = b"hello=world";
        let sig = make_signature(secret, &old_ts, "hello=world");

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", old_ts.parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        let result = verify_slack_signature_with_secret(secret, &headers, body);
        assert_eq!(result, VerifyResult::TimestampInvalid);
    }

    // ── Signature verification: secret not set → SecretNotSet ────────────────
    // This test does mutate the env var, but it only *removes* the var and
    // then restores it, so it is the least disruptive to parallel neighbours.

    #[test]
    fn missing_secret_returns_secret_not_set() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        // Only remove if it was set — avoids a no-op unsafe call on CI.
        let needs_restore = old_val.is_some();
        unsafe { std::env::remove_var("SLACK_SIGNING_SECRET"); }

        let headers = HeaderMap::new();
        let result = verify_slack_signature(&headers, b"body");
        assert_eq!(result, VerifyResult::SecretNotSet);

        if needs_restore {
            unsafe { std::env::set_var("SLACK_SIGNING_SECRET", old_val.unwrap()); }
        }
    }

    // ── url_verification challenge echo ───────────────────────────────────────

    #[tokio::test]
    async fn url_verification_echoes_challenge() {
        let router = events_router(test_state());
        let body = serde_json::to_string(&json!({
            "type": "url_verification",
            "challenge": "abc123xyz"
        }))
        .unwrap();

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let bytes = axum::body::to_bytes(resp.into_body(), usize::MAX).await.unwrap();
        let json: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["challenge"], "abc123xyz");
    }

    // ── Handler returns 503 when env var absent (unit-path test) ─────────────
    // Uses verify_slack_signature directly — no HTTP round-trip, no env races.

    #[test]
    fn missing_secret_env_returns_secret_not_set_via_outer_fn() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        // When the env var is absent, the public wrapper returns SecretNotSet.
        // We verify that the handler logic correctly maps this to 503.
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        let needs_restore = old_val.is_some();
        unsafe { std::env::remove_var("SLACK_SIGNING_SECRET"); }

        let result = verify_slack_signature(&HeaderMap::new(), b"body");
        assert_eq!(result, VerifyResult::SecretNotSet);

        if needs_restore {
            unsafe { std::env::set_var("SLACK_SIGNING_SECRET", old_val.unwrap()); }
        }
    }

    // ── Handler: event_callback with bad signature → 401 (real handler) ──────
    //
    // The three tests this section replaced (`handler_maps_signature_mismatch_
    // to_401_branch`, `handler_maps_secret_not_set_to_503_branch`,
    // `bot_message_event_dispatch_filter_unit`, and the near-identical
    // `message_event_no_db_handler_branch_returns_200`) each re-implemented a
    // COPY of the handler's `match`/filter logic inside the test body and
    // asserted against that copy — every one of them would stay green even if
    // `slack_events` stopped calling `verify_slack_signature`/
    // `huddle_capture_trigger` entirely, because the test never calls the
    // real handler. These drive the real handler through
    // `events_router(test_state())` + `oneshot`, the same harness
    // `url_verification_echoes_challenge` already proves works with no DB.
    //
    // `bot_message_event_dispatch_filter_unit` has no replacement here: with
    // `state.db` absent (as every test in this module runs), a bot-filtered
    // message and a message that reached the ingest spawn and no-op'd on
    // `db: None` are BOTH observable only as "200, no visible side effect" —
    // there is no way to prove the filter fired rather than the spawn no-op,
    // so a same-shaped replacement would be exactly as unfalsifiable as the
    // original. Deleted rather than faked.

    #[tokio::test]
    async fn handler_signature_mismatch_returns_401() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        unsafe { std::env::set_var("SLACK_SIGNING_SECRET", "handler_401_test_secret"); }

        let router = events_router(test_state());
        let body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T1",
            "event": {"type": "message", "channel": "C1", "user": "U1", "ts": "1.1", "text": "hi"}
        }))
        .unwrap();
        let ts = now_ts();
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            // 64 hex chars after "v0=" (correct length), but not the real HMAC.
            .header(
                "x-slack-signature",
                "v0=0000000000000000000000000000000000000000000000000000000000000000",
            )
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);

        match old_val {
            Some(v) => unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) },
            None => unsafe { std::env::remove_var("SLACK_SIGNING_SECRET") },
        }
    }

    /// A body that does NOT match the signature (signed for a different
    /// payload) must also be rejected — distinct from
    /// `handler_signature_mismatch_returns_401` above (a garbage/all-zero
    /// signature) in that this one proves the handler actually hashes the
    /// REQUEST body it received, not just that a malformed signature header
    /// is rejected.
    #[tokio::test]
    async fn handler_signature_valid_for_a_different_body_returns_401() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        let secret = "handler_body_mismatch_test_secret";
        unsafe { std::env::set_var("SLACK_SIGNING_SECRET", secret); }

        let signed_body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T1",
            "event": {"type": "message", "channel": "C1", "user": "U1", "ts": "1.1", "text": "original"}
        }))
        .unwrap();
        let sent_body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T1",
            "event": {"type": "message", "channel": "C1", "user": "U1", "ts": "1.1", "text": "tampered"}
        }))
        .unwrap();
        let ts = now_ts();
        // Signature is valid for `signed_body`, but the request carries `sent_body`.
        let sig = make_signature(secret, &ts, &signed_body);

        let router = events_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            .header("x-slack-signature", &sig)
            .body(Body::from(sent_body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "a signature computed over a DIFFERENT body must not verify the body actually sent"
        );

        match old_val {
            Some(v) => unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) },
            None => unsafe { std::env::remove_var("SLACK_SIGNING_SECRET") },
        }
    }

    #[tokio::test]
    async fn handler_secret_not_set_returns_503() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        unsafe { std::env::remove_var("SLACK_SIGNING_SECRET"); }

        let router = events_router(test_state());
        let body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T1",
            "event": {"type": "message", "channel": "C1", "user": "U1", "ts": "1.1", "text": "hi"}
        }))
        .unwrap();
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        if let Some(v) = old_val {
            unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) };
        }
    }

    // ── M1: Non-ASCII body bytes do not alter HMAC vs string-concat approach ──
    //
    // For ASCII body bytes, incremental mac.update() over [b"v0:", ts, b":", body]
    // produces the same digest as mac.update(format!("v0:{ts}:{body}").as_bytes())
    // because no lossy UTF-8 conversion is involved.  This test pins that
    // equivalence for a pure-ASCII body.

    #[test]
    fn incremental_hmac_matches_concat_for_ascii_body() {
        let secret = "some_secret_key";
        let ts = "1531420618";
        let body = b"token=xyzz0WbapA4&user_id=U0KRQLJ9H";

        // Incremental (new implementation).
        let mut mac1 = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac1.update(b"v0:");
        mac1.update(ts.as_bytes());
        mac1.update(b":");
        mac1.update(body);
        let digest1 = mac1.finalize().into_bytes();

        // String-concat (old implementation reference).
        let sig_base = format!("v0:{ts}:{}", std::str::from_utf8(body).unwrap());
        let mut mac2 = HmacSha256::new_from_slice(secret.as_bytes()).unwrap();
        mac2.update(sig_base.as_bytes());
        let digest2 = mac2.finalize().into_bytes();

        assert_eq!(
            digest1, digest2,
            "incremental and concat HMAC must produce identical digests for ASCII bodies"
        );
    }

    // ── Unified dispatch: valid-signature message event with db=None → 200 ────
    //
    // Verifies the message-event dispatch path returns 200 without panicking,
    // even when db=None.  The ingest spawned task notices db=None and no-ops.
    //
    // We use verify_slack_signature_with_secret (env-var-free) to assert the
    // signature construction is correct, then assert the handler branch
    // logic is correct via the same pattern as
    // handler_maps_signature_mismatch_to_401_branch.  This avoids racing on the
    // process-global SLACK_SIGNING_SECRET with the existing
    // valid_signature_no_db_returns_200 test.

    #[test]
    fn message_event_signature_constructs_correctly() {
        // Verify that make_signature and verify_slack_signature_with_secret agree
        // for a message event payload.  This proves the handler's signature path
        // would pass for correctly-signed message events.
        let secret = "test_secret_msg_event_cs127_unit";
        let ts = now_ts();
        let body_str = serde_json::to_string(&serde_json::json!({
            "type": "event_callback",
            "team_id": "T_MSG_NODB",
            "event": {
                "type": "message",
                "channel": "C999",
                "user": "U888",
                "ts": "1234567890.000200",
                "text": "hello from the unified endpoint"
            }
        }))
        .unwrap();

        let sig = make_signature(secret, &ts, &body_str);

        let mut headers = HeaderMap::new();
        headers.insert("x-slack-request-timestamp", ts.parse().unwrap());
        headers.insert("x-slack-signature", sig.parse().unwrap());

        assert_eq!(
            verify_slack_signature_with_secret(secret, &headers, body_str.as_bytes()),
            VerifyResult::Ok,
            "message event payload must produce a valid HMAC signature"
        );
    }

    /// Real handler-driven replacement for the deleted
    /// `message_event_no_db_handler_branch_returns_200`: a correctly-signed
    /// `message` event, with `state.db` absent, must return 200 without
    /// panicking (the ingest spawn notices `db: None` and no-ops).
    #[tokio::test]
    async fn handler_message_event_valid_signature_no_db_returns_200() {
        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_val = std::env::var("SLACK_SIGNING_SECRET").ok();
        let secret = "handler_message_no_db_test_secret";
        unsafe { std::env::set_var("SLACK_SIGNING_SECRET", secret); }

        let body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T_MSG_NODB",
            "event": {
                "type": "message",
                "channel": "C999",
                "user": "U888",
                "ts": "1234567890.000200",
                "text": "hello from the unified endpoint"
            }
        }))
        .unwrap();
        let ts = now_ts();
        let sig = make_signature(secret, &ts, &body);

        let router = events_router(test_state());
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            .header("x-slack-signature", &sig)
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        match old_val {
            Some(v) => unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) },
            None => unsafe { std::env::remove_var("SLACK_SIGNING_SECRET") },
        }
    }

    // ── Unified dispatch: a non-triggering `message_changed` edit is filtered
    // before DB access ─────────────────────────────────────────────────────
    //
    // Rewritten for the huddle-capture trigger (which now sits ABOVE this
    // filter — see `huddle_capture_trigger`'s doc comment): a bare edited
    // message with `subtype: "message_changed"` but no `message.room`
    // (ordinary text-message edits are the overwhelming majority of
    // `message_changed` traffic) is NOT the huddle trigger, so it falls
    // through to this filter exactly as before and is acked without
    // ingestion.

    #[test]
    fn subtype_message_event_dispatch_filter_unit() {
        let event = serde_json::json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C999",
            "ts": "1234567890.000400",
            "text": "edited message"
        });

        // The huddle trigger must NOT fire for an ordinary edited message.
        assert!(
            huddle_capture_trigger(&event).is_none(),
            "an ordinary message_changed edit with no room must not trigger huddle capture"
        );

        let inner_type = event.get("type").and_then(|v| v.as_str()).unwrap_or("");
        assert_eq!(inner_type, "message", "inner_type must be 'message'");
        // …so it falls through to the subtype allowlist, which refuses it.
        // Asserted against the PREDICATE the handler actually calls — the
        // older form of this test asserted only that the event carried a
        // `subtype` key it had itself just written, which is true no matter
        // what the handler decides.
        assert!(
            !crate::connectors::slack_message_text::ingestible_subtype(event.get("subtype").and_then(|v| v.as_str())),
            "an edit must not be ingestible"
        );
        // The allowlist admits exactly these three, and nothing else.
        assert!(crate::connectors::slack_message_text::ingestible_subtype(None), "an ordinary message");
        assert!(crate::connectors::slack_message_text::ingestible_subtype(Some("bot_message")), "a legacy app post");
        assert!(
            crate::connectors::slack_message_text::ingestible_subtype(Some("thread_broadcast")),
            "a threaded reply also broadcast to the channel"
        );
        for refused in [
            "message_changed",
            "message_deleted",
            "channel_join",
            "channel_leave",
            "file_share",
            "message_replied",
            "thread_broadcast_but_not_really",
        ] {
            assert!(!crate::connectors::slack_message_text::ingestible_subtype(Some(refused)), "{refused} must stay dropped");
        }
    }

    // ── CS-ER Part B: author canonicalization (moved here with
    // `canonical_author_name` from `routes::slack`, org-tenancy PR 1.6) ──────

    #[test]
    fn two_raw_identifiers_for_same_person_collapse_to_one_name() {
        // The bug: the SAME human surfaces as the Slack display name in one atom
        // and the raw user id in another, scattering their relationships across
        // two author reefs. Part B fixes this: once the (provider, workspace,
        // user-id) → canonical-name mapping is in the directory, EVERY atom for
        // that user resolves to the same canonical name regardless of what the
        // per-atom name resolution produced.
        let raw_id = "U57C3A9EF09";

        // Atom 1 — first sight, no directory entry yet. Slack resolves the
        // display name; it is canonical and gets persisted.
        let (name1, persist1) = canonical_author_name(None, "Alex Example", raw_id);
        assert_eq!(name1, "Alex Example");
        assert!(persist1, "a real display name is persisted to the directory");

        // Atom 2 — the per-process cache has been evicted (process restart), so
        // `resolve_user_name` would FALL BACK to the raw id. But the directory
        // now holds the canonical name, so the hit wins → SAME author_name.
        let (name2, persist2) = canonical_author_name(Some("Alex Example"), raw_id, raw_id);
        assert_eq!(
            name2, name1,
            "directory hit yields the same canonical name as atom 1"
        );
        assert!(!persist2, "an existing directory entry is not rewritten");
    }

    #[test]
    fn raw_id_fallback_is_not_persisted() {
        // When `users.info` fails, `resolve_user_name` returns the raw id. We
        // must NOT persist that as the canonical name — otherwise a transient
        // error would freeze the wrong value forever. A later success then
        // writes the real display name.
        let raw_id = "U57C3A9EF09";
        let (name, persist) = canonical_author_name(None, raw_id, raw_id);
        assert_eq!(name, raw_id, "the raw id is used for this atom (best effort)");
        assert!(!persist, "the raw-id fallback is never persisted as canonical");
    }

    // ── Actor-id shape filter ─────────────────────────────────────────────────
    //
    // `is_slack_actor_id` is the shape filter `huddle_capture_trigger` reuses
    // to validate `room.participant_history` entries before they reach the
    // durable job payload.

    #[test]
    fn actor_id_detection_spares_routing_ids() {
        for id in ["U012ABCDEF", "W012ABCDEF", "B012ABCDEF", "U0123456789ABCDEFGHIJ"] {
            assert!(is_slack_actor_id(id), "{id} is an actor id");
        }
        // Channel, team and file ids are routing values we need to read verbatim.
        for id in ["C012ABCDEF", "T012ABCDEF", "F012ABCDEF", "R012ABCDEF"] {
            assert!(!is_slack_actor_id(id), "{id} is a routing id and must stay readable");
        }
        // Shape guards: too short, lowercase, and over-long are all rejected.
        for id in ["U01", "Uabcdefg", "U0123456789ABCDEFGHIJK", ""] {
            assert!(!is_slack_actor_id(id), "{id:?} is not actor-id shaped");
        }
    }

    // ── huddle_capture_trigger: MEASURED ground truth fixtures ───────────────
    //
    // The complete set of four events Slack delivered for one real huddle
    // (`backend/tests/fixtures/slack_huddle/README.md`). Exactly ONE must
    // fire; the other three are real huddle traffic that must NOT.

    const FIXTURE_01_HUDDLE_THREAD_START: &str = include_str!(
        "../../tests/fixtures/slack_huddle/event_01_huddle_thread_start.json"
    );
    const FIXTURE_02_SUMMARY_PENDING: &str = include_str!(
        "../../tests/fixtures/slack_huddle/event_02_message_changed_summary_pending.json"
    );
    const FIXTURE_03_SUMMARY_ABSENT: &str = include_str!(
        "../../tests/fixtures/slack_huddle/event_03_message_changed_summary_absent.json"
    );
    const FIXTURE_04_TRIGGER: &str = include_str!(
        "../../tests/fixtures/slack_huddle/event_04_trigger_message_changed_complete.json"
    );

    /// Extract the `event_callback.event` object a fixture's `event` key
    /// holds — matching the object [`huddle_capture_trigger`] is called with
    /// from the handler.
    fn fixture_event(raw: &str) -> Value {
        let payload: Value = serde_json::from_str(raw).expect("fixture is valid JSON");
        payload["event"].clone()
    }

    #[test]
    fn huddle_capture_trigger_fires_on_exactly_one_of_four_real_events() {
        let outcomes: Vec<(&str, Option<HuddleTrigger>)> = vec![
            (
                "event_01_huddle_thread_start (huddle_thread, no transcript_file_id)",
                huddle_capture_trigger(&fixture_event(FIXTURE_01_HUDDLE_THREAD_START)),
            ),
            (
                "event_02_message_changed_summary_pending (summary_status=pending, \
                 no transcript_file_id)",
                huddle_capture_trigger(&fixture_event(FIXTURE_02_SUMMARY_PENDING)),
            ),
            (
                "event_03_message_changed_summary_absent (no summary_status, \
                 no transcript_file_id)",
                huddle_capture_trigger(&fixture_event(FIXTURE_03_SUMMARY_ABSENT)),
            ),
            (
                "event_04_trigger_message_changed_complete (summary_status=complete, \
                 transcript_file_id PRESENT)",
                huddle_capture_trigger(&fixture_event(FIXTURE_04_TRIGGER)),
            ),
        ];

        let fired: Vec<&str> =
            outcomes.iter().filter(|(_, t)| t.is_some()).map(|(name, _)| *name).collect();
        assert_eq!(
            fired,
            vec![
                "event_04_trigger_message_changed_complete (summary_status=complete, \
                 transcript_file_id PRESENT)"
            ],
            "exactly one of the four real events must trigger huddle capture, got: {fired:?}"
        );
    }

    #[test]
    fn huddle_capture_trigger_extracts_measured_fields() {
        let trigger = huddle_capture_trigger(&fixture_event(FIXTURE_04_TRIGGER))
            .expect("event_04 is the trigger");

        // NAMING TRAP: `room.transcript_file_id` is the CANVAS id, not the
        // (unfetchable) transcript.
        assert_eq!(trigger.canvas_file_id, "FB96A629E52");
        assert_eq!(trigger.channel_id, "CE1C7114B00");
        // MEASURED (this fixture): `event.ts` (the OUTER `message_changed`
        // edit's own ts) is "1700000000.001000" — distinct from both fields
        // below. `event.message.ts` and `room.thread_root_ts` happen to be
        // IDENTICAL in this fixture, both "1700000000.747249" (the huddle
        // started and was edited at the same instant), so this assertion
        // alone does NOT prove `thread_root_ts` is preferred over the
        // `message.ts` fallback — see
        // `huddle_capture_trigger_prefers_thread_root_ts_over_message_ts_when_they_differ`
        // below for the synthetic case that actually distinguishes them.
        // `huddle_thread_ts` is the third column of `slack_huddle_captures`'s
        // idempotency key, so getting the wrong one silently is exactly the
        // kind of bug that must be caught here, not just here.
        assert_eq!(trigger.huddle_thread_ts, "1700000000.747249");
        assert_eq!(trigger.participant_history, vec!["U57C3A9EF09".to_string()]);
        assert_eq!(trigger.date_start, Some(1700000000));
        assert_eq!(trigger.date_end, Some(1700000000));
    }

    /// `event_04`'s fixture cannot prove `room.thread_root_ts` wins over the
    /// `event.message.ts` fallback (see the comment on
    /// `huddle_capture_trigger_extracts_measured_fields` above): the two
    /// happen to be equal in that real fixture. This synthetic case makes
    /// them deliberately different so the preference is actually exercised.
    #[test]
    fn huddle_capture_trigger_prefers_thread_root_ts_over_message_ts_when_they_differ() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000099.000900",
                "room": {
                    "transcript_file_id": "F123",
                    "thread_root_ts": "1717000000.000100"
                }
            }
        });
        let trigger = huddle_capture_trigger(&event).expect("transcript_file_id present");
        assert_eq!(
            trigger.huddle_thread_ts, "1717000000.000100",
            "room.thread_root_ts must win over event.message.ts when they differ"
        );
        assert_ne!(
            trigger.huddle_thread_ts, "1717000099.000900",
            "must not have fallen through to event.message.ts while thread_root_ts was present"
        );
    }

    #[test]
    fn huddle_capture_trigger_falls_back_to_message_ts_when_thread_root_ts_absent() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "F123",
                    // no thread_root_ts
                }
            }
        });
        let trigger = huddle_capture_trigger(&event).expect("transcript_file_id present");
        assert_eq!(trigger.huddle_thread_ts, "1717000000.000100");
        assert_eq!(trigger.participant_history, Vec::<String>::new());
        assert_eq!(trigger.date_start, None);
        assert_eq!(trigger.date_end, None);
    }

    // ── D: empty-string fields must be treated as absent ─────────────────────
    //
    // Slack sends empty strings for unset `room` fields (fixture 02's
    // `"media_server": ""`/`"name": ""` — measured, not hypothetical). A bare
    // `Value::as_str` treats `Some("")` as present, which is wrong for every
    // field below.

    #[test]
    fn huddle_capture_trigger_treats_empty_transcript_file_id_as_absent() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "",
                    "thread_root_ts": "1717000000.000000"
                }
            }
        });
        assert!(
            huddle_capture_trigger(&event).is_none(),
            "an empty transcript_file_id must not mint a doomed job for a canvas that was \
             never actually produced"
        );
    }

    #[test]
    fn huddle_capture_trigger_treats_empty_thread_root_ts_as_absent_and_falls_back() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "F123",
                    "thread_root_ts": ""
                }
            }
        });
        let trigger = huddle_capture_trigger(&event).expect("transcript_file_id present");
        assert_eq!(
            trigger.huddle_thread_ts, "1717000000.000100",
            "an empty thread_root_ts must fall through to event.message.ts, not produce an \
             EMPTY huddle_thread_ts — an empty huddle_thread_ts collides across every huddle \
             in the same channel in both the job idempotency key and the \
             slack_huddle_captures unique key"
        );
    }

    #[test]
    fn huddle_capture_trigger_treats_empty_channel_as_absent() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "F123",
                    "thread_root_ts": "1717000000.000000"
                }
            }
        });
        assert!(
            huddle_capture_trigger(&event).is_none(),
            "an empty channel id must not be treated as present"
        );
    }

    // ── D: participant_history is shape-validated and capped ─────────────────

    #[test]
    fn huddle_capture_trigger_refuses_partial_participant_history() {
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "F123",
                    "thread_root_ts": "1717000000.000000",
                    "participant_history": [
                        "U012ABCDEF",
                        "not-actor-shaped",
                        "<script>alert(1)</script>",
                        "W012ABCDEF"
                    ]
                }
            }
        });
        let trigger = huddle_capture_trigger(&event).expect("transcript_file_id present");
        assert_eq!(
            trigger.participant_history,
            Vec::<String>::new(),
            "malformed attendance must remain unknown, never a partial audience"
        );
    }

    #[test]
    fn huddle_capture_trigger_refuses_oversized_participant_history() {
        let ids: Vec<String> = (0..(MAX_PARTICIPANT_HISTORY + 50))
            .map(|i| format!("U{i:010}"))
            .collect();
        let event = json!({
            "type": "message",
            "subtype": "message_changed",
            "channel": "C1",
            "message": {
                "ts": "1717000000.000100",
                "room": {
                    "transcript_file_id": "F123",
                    "thread_root_ts": "1717000000.000000",
                    "participant_history": ids
                }
            }
        });
        let trigger = huddle_capture_trigger(&event).expect("transcript_file_id present");
        assert_eq!(
            trigger.participant_history.len(),
            0,
            "oversized attendance must remain unknown, never silently truncated"
        );
    }

    // ── F: ordering invariant — trigger check must run BEFORE the ────────────
    // bot_id/subtype filter (real handler, DB-backed) ─────────────────────────
    //
    // `message_changed` always carries a `subtype`, so a regression that moved
    // `huddle_capture_trigger`'s check below the bot_id/subtype filter would
    // silently and PERMANENTLY kill all huddle capture — exactly the bug that
    // made the predecessor reaction-based opt-in dead for its entire life (see
    // the module doc comment). Nothing else in this file catches that
    // regression: every pure `huddle_capture_trigger` unit test above calls the
    // function directly, so a mis-ordering of the two `if` blocks INSIDE the
    // handler is invisible to them — the bug is in the handler's imperative
    // dispatch order, not in the pure function.
    //
    // An HTTP-status assertion alone cannot catch it either: with `state.db`
    // absent (every other test in this module), BOTH "reached the trigger,
    // found no DB, acked" and "swallowed by the subtype filter before ever
    // reaching the trigger" return a bare 200 with no observable difference.
    // This test instead keys on a real DB side effect — seeds an
    // `oauth_tokens` row so a real account resolves, drives the FULL handler
    // (signed HTTP request, `events_router` + `oneshot`, not the pure
    // function) with a `message_changed` huddle-trigger event, and asserts a
    // `SlackHuddleCapture` job was actually enqueued. If the ordering
    // regressed, the subtype filter would swallow the event first and this
    // job would never appear — while the pure-function tests above stay fully
    // green.
    #[tokio::test]
    async fn huddle_trigger_message_changed_reaches_trigger_ahead_of_subtype_filter_pg() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping huddle_trigger_message_changed_reaches_trigger_ahead_of_subtype_filter_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_huddle_trigger_ordering";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        let db_url = format!("{prefix}/{db}{params}");

        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();

        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&db_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Seed an account + an `oauth_tokens` row so the trigger, if reached,
        // resolves a real account_id and actually enqueues a job — the
        // observable side effect this test keys on.
        let owner: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("owner-{}@test.local", uuid::Uuid::new_v4()))
        .fetch_one(&pool)
        .await
        .unwrap();
        let account_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(owner)
                .fetch_one(&pool)
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, \
             provider_workspace_id) VALUES ($1, 'slack', 'test', $2, $3)",
        )
        .bind(account_id)
        .bind(b"stub-encrypted-token".to_vec())
        .bind("T-ORDERING-TEST")
        .execute(&pool)
        .await
        .unwrap();
        // Deliberately NOT also seeding `connector_installs` (migration
        // 0121_connector_installs) — `resolve_slack_account_id` self-heals
        // on a resolve-miss by falling back to `oauth_tokens`/
        // `slack_integrations` directly (see that function's own doc
        // comment), so this bare `oauth_tokens` row is exactly what a real
        // (if unusual) direct-DB-write install looks like, and the trigger
        // must still be reachable through it. Seeding `connector_installs`
        // here too would make this test pass via the fast path instead and
        // never actually exercise the fallback this fixture is now proving.

        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_secret = std::env::var("SLACK_SIGNING_SECRET").ok();
        let secret = "huddle_ordering_test_secret";
        unsafe { std::env::set_var("SLACK_SIGNING_SECRET", secret); }

        let mut state = (*test_state()).clone();
        state.db = Some(pool.clone());
        let router = events_router(std::sync::Arc::new(state));

        // A real `message_changed` huddle-capture-trigger shape (subtype
        // present, room.transcript_file_id present) — if the handler's
        // ordering regressed, the subtype filter below the trigger check
        // would swallow this before it ever reached `huddle_capture_trigger`.
        let body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T-ORDERING-TEST",
            "event": {
                "type": "message",
                "subtype": "message_changed",
                "channel": "C-ORDERING-TEST",
                "message": {
                    "ts": "1700000000.000100",
                    "room": {
                        "transcript_file_id": "F-ORDERING-TEST",
                        "thread_root_ts": "1700000000.000000"
                    }
                }
            }
        }))
        .unwrap();
        let ts = now_ts();
        let sig = make_signature(secret, &ts, &body);

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            .header("x-slack-signature", &sig)
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        match old_secret {
            Some(v) => unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) },
            None => unsafe { std::env::remove_var("SLACK_SIGNING_SECRET") },
        }

        let job_count: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE kind = 'SlackHuddleCapture' AND account_id = $1",
        )
        .bind(account_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            job_count, 1,
            "a message_changed event with room.transcript_file_id present must reach \
             huddle_capture_trigger and enqueue a SlackHuddleCapture job BEFORE the \
             bot_id/subtype filter below it has a chance to swallow it (message_changed \
             always carries a subtype) — got {job_count} jobs"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── Review finding 2: `None == None` must not read as a self-match ───────
    //
    // `BotIdentity.bot_id` is `None` for a token that is not a bot token, and
    // the admission rule deliberately admits legacy `bot_message` posts that
    // carry no `bot_id` either. The first version of the comparison was
    // `own.bot_id.as_deref() == event_bot_id`, so on such an install every
    // one of those messages was dropped as "our own bot" — with a `debug!`
    // naming no cause.
    #[test]
    fn two_absent_bot_ids_are_not_a_self_match() {
        let no_bot_id = BotIdentity { user_id: "UBOT".into(), bot_id: None };
        assert!(
            !is_our_own_bot_message(&no_bot_id, None, Some("U-SOMEONE-ELSE")),
            "None == None is not evidence of anything, and must never drop a message"
        );
        assert!(
            !is_our_own_bot_message(&no_bot_id, None, None),
            "nothing to compare on either side ⇒ not ours"
        );
        assert!(
            is_our_own_bot_message(&no_bot_id, None, Some("UBOT")),
            "…but the user id still decides when it IS present"
        );
    }

    #[test]
    fn a_self_match_needs_both_sides_present_and_equal() {
        let own = BotIdentity { user_id: "UBOT".into(), bot_id: Some("BOURS".into()) };
        assert!(is_our_own_bot_message(&own, Some("BOURS"), Some("U-OTHER")));
        assert!(is_our_own_bot_message(&own, Some("BTHEIRS"), Some("UBOT")));
        assert!(!is_our_own_bot_message(&own, Some("BTHEIRS"), Some("U-OTHER")));
        assert!(!is_our_own_bot_message(&own, None, Some("U-OTHER")));

        let blank = BotIdentity { user_id: String::new(), bot_id: Some(String::new()) };
        assert!(
            !is_our_own_bot_message(&blank, Some(""), Some("")),
            "an empty-string identity must never match anything"
        );
    }

    // ── Review finding 1: a bot identity is PER WORKSPACE ────────────────────
    //
    // `slack_integrations` is UNIQUE (account_id, workspace_id), so one
    // account can install into T1 and T2 and get a different bot_id/user_id
    // in each. With the account-only cache key, any T1 event warmed the cache
    // and the T2 self-bot check then compared against T1's identity, never
    // matched, and ingested our OWN posts in T2.
    #[test]
    fn the_bot_identity_cache_is_keyed_per_workspace_not_per_account() {
        let account_id = uuid::Uuid::new_v4();
        seed_bot_identity_for_test(account_id, "T-ONE", "U-BOT-ONE", Some("B-OURS-ONE"));
        seed_bot_identity_for_test(account_id, "T-TWO", "U-BOT-TWO", Some("B-OURS-TWO"));

        let one = bot_user_id_cache()
            .lock()
            .unwrap()
            .get(&(account_id, "T-ONE".to_string()))
            .cloned()
            .expect("T-ONE cached");
        let two = bot_user_id_cache()
            .lock()
            .unwrap()
            .get(&(account_id, "T-TWO".to_string()))
            .cloned()
            .expect("T-TWO cached");

        assert_eq!(one.bot_id.as_deref(), Some("B-OURS-ONE"));
        assert_eq!(
            two.bot_id.as_deref(),
            Some("B-OURS-TWO"),
            "seeding T-TWO must not be answered by T-ONE's entry — an account-keyed cache \
             returned the first-warmed identity for every workspace"
        );

        // The consequence the key exists to prevent: our own T-TWO post,
        // checked against T-TWO's identity, IS recognised as ours.
        assert!(
            is_our_own_bot_message(&two, Some("B-OURS-TWO"), Some("U-BOT-TWO")),
            "our own post in the second workspace must be recognised"
        );
        assert!(
            !is_our_own_bot_message(&one, Some("B-OURS-TWO"), Some("U-BOT-TWO")),
            "…and checking it against the WRONG workspace's identity would not — which is \
             exactly what the account-only key did, silently ingesting our own \
             \"Ingested N past messages\" confirmations in the second workspace"
        );
    }

    // ── Review finding 13: a failed resolve must not re-resolve forever ──────
    #[test]
    fn a_failed_own_identity_resolution_is_negatively_cached_with_a_ttl() {
        let key = (uuid::Uuid::new_v4(), "T-NEG".to_string());
        assert!(!bot_identity_negative_hit(&key), "clean slate");
        cache_bot_identity_failure(key.clone());
        assert!(
            bot_identity_negative_hit(&key),
            "a revoked/undecryptable token must not turn every bot message and every Slack \
             retry into a fresh oauth_tokens read, DEK derive and live auth.test"
        );
        let other = (key.0, "T-OTHER".to_string());
        assert!(
            !bot_identity_negative_hit(&other),
            "the suppression is per install, not per account"
        );
        assert!(
            BOT_IDENTITY_NEGATIVE_TTL <= std::time::Duration::from_secs(600),
            "a stale negative entry drops real bot messages, so the window stays short"
        );
    }

    // ── Bot-authored ingest (2026-09) ────────────────────────────────────────
    //
    // The whole point of the change: a GitHub notification carries an EMPTY
    // `text` and 8 KiB of content in `attachments[0]`. Before it, the handler
    // dropped the message twice over (the `bot_id` filter, then the
    // empty-`text` guard). These tests drive the FULL handler — signed HTTP
    // request through `events_router` — and key on the durable job row,
    // because that is the only observable that distinguishes "ingested" from
    // "acked and thrown away": every arm of this dispatch returns a bare 200.
    //
    // Fixtures reproduce the structure of `conversations.history` objects
    // (see that directory's README — they are SYNTHETIC); `slack_event_envelope`
    // wraps one the way Slack's Event API delivers it (see the fixture
    // directory's README §A for the history-object-vs-event-object table).

    #[cfg(test)]
    fn slack_fixture(name: &str) -> serde_json::Value {
        let path = format!(
            "{}/tests/fixtures/slack_bot_messages/{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        serde_json::from_str(&std::fs::read_to_string(&path).expect("read fixture"))
            .expect("parse fixture")
    }

    /// Wrap a captured `conversations.history` message as the Event API
    /// delivers it: `channel`/`event_ts`/`channel_type` are added to the
    /// event, and `team_id` sits on the ENVELOPE, not inside `event`.
    #[cfg(test)]
    fn slack_event_envelope(msg: &serde_json::Value, team_id: &str, channel: &str) -> String {
        let mut event = msg.clone();
        let obj = event.as_object_mut().expect("message is an object");
        obj.insert("type".into(), json!("message"));
        obj.insert("channel".into(), json!(channel));
        obj.insert("channel_type".into(), json!("channel"));
        let ts = msg.get("ts").cloned().unwrap_or(json!("0"));
        obj.insert("event_ts".into(), ts);
        serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": team_id,
            "event": event,
        }))
        .unwrap()
    }

    /// Drive one signed event through the real router, then drain the
    /// handler's tracked spawn so the enqueue it performs is complete before
    /// the caller reads the `jobs` table. A fresh `AppState` per call (and so
    /// a fresh `TaskTracker`) because `TaskTracker::wait` requires `close`,
    /// which would poison a shared tracker for the next request.
    #[cfg(test)]
    async fn post_slack_event(pool: &sqlx::PgPool, secret: &str, body: &str) -> StatusCode {
        let mut state = (*test_state()).clone();
        state.db = Some(pool.clone());
        state.store = Some(std::sync::Arc::new(crate::store::PostgresStore::new(pool.clone())));
        let tracker = state.task_tracker.clone();
        let router = events_router(std::sync::Arc::new(state));

        let ts = now_ts();
        let sig = make_signature(secret, &ts, body);
        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            .header("x-slack-signature", &sig)
            .body(Body::from(body.to_owned()))
            .unwrap();
        let status = router.oneshot(req).await.unwrap().status();
        tracker.close();
        tracker.wait().await;
        status
    }

    #[cfg(test)]
    async fn slack_ingest_jobs(pool: &sqlx::PgPool) -> Vec<serde_json::Value> {
        sqlx::query_scalar::<_, serde_json::Value>(
            "SELECT payload FROM jobs WHERE kind = 'SlackMessageIngest' ORDER BY created_at, id",
        )
        .fetch_all(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn bot_authored_messages_are_ingested_with_attachments_and_blocks_flattened_pg() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping bot_authored_messages_are_ingested_with_attachments_and_blocks_flattened_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_bot_ingest_admits";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        let db_url = format!("{prefix}/{db}{params}");

        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&db_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let team_id = "T0TESTWS0";
        let channel = "C-BOT-INGEST";
        let account_id = seed_slack_account_for_workspace(&pool, team_id).await;
        // Our OWN app, as `auth.test` would report it — seeded so the
        // self-bot check never makes a live HTTP call from a unit test.
        seed_bot_identity_for_test(account_id, team_id, "U0TESTBOT0", Some("B0TESTBOT0"));

        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_secret = std::env::var("SLACK_SIGNING_SECRET").ok();
        let old_flag = std::env::var("SLACK_INGEST_BOT_MESSAGES").ok();
        let secret = "bot_ingest_test_secret";
        unsafe {
            std::env::set_var("SLACK_SIGNING_SECRET", secret);
            std::env::remove_var("SLACK_INGEST_BOT_MESSAGES");
        }

        // ── 1. The GitHub notification: empty `text`, 8 KiB of attachments ──
        let github = slack_fixture("github_pr_opened_attachments");
        assert_eq!(
            github["text"].as_str(),
            Some(""),
            "fixture premise: this is one of the 235/253 bot messages with an EMPTY text"
        );
        let body = slack_event_envelope(&github, team_id, channel);
        assert_eq!(post_slack_event(&pool, secret, &body).await, StatusCode::OK);

        let jobs = slack_ingest_jobs(&pool).await;
        assert_eq!(
            jobs.len(),
            1,
            "a bot-authored message whose content lives entirely in `attachments` must now \
             enqueue exactly one ingest job — got {jobs:?}"
        );
        let payload = &jobs[0];
        let text = payload["text"].as_str().expect("payload carries text");
        assert!(
            text.contains("#101") && text.contains("Pull request opened by"),
            "the job's `text` must be the FLATTENED attachment content — got: {text}"
        );
        assert!(
            text.len() > 1024,
            "{} bytes means the flattener fell back to the 54-char `fallback`",
            text.len()
        );
        assert_eq!(
            payload["bot_id"].as_str(),
            Some("B0TESTGIT0"),
            "the app's bot id must reach the worker — it becomes the atom's author_id"
        );
        assert_eq!(
            payload["bot_name"].as_str(),
            Some("GitHub"),
            "bot_profile.name must reach the worker — it becomes the atom's author_name"
        );

        // ── 2. A human message is untouched by any of this ──────────────────
        let human = slack_fixture("human_plain");
        let body = slack_event_envelope(&human, team_id, channel);
        assert_eq!(post_slack_event(&pool, secret, &body).await, StatusCode::OK);
        let jobs = slack_ingest_jobs(&pool).await;
        assert_eq!(jobs.len(), 2, "the human message must still enqueue — got {jobs:?}");
        assert_eq!(
            jobs[1]["text"].as_str(),
            human["text"].as_str(),
            "a human message's stored text must be byte-identical to today's"
        );
        assert!(jobs[1]["bot_id"].is_null() && jobs[1]["bot_name"].is_null());

        unsafe {
            match old_secret {
                Some(v) => std::env::set_var("SLACK_SIGNING_SECRET", v),
                None => std::env::remove_var("SLACK_SIGNING_SECRET"),
            }
            match old_flag {
                Some(v) => std::env::set_var("SLACK_INGEST_BOT_MESSAGES", v),
                None => std::env::remove_var("SLACK_INGEST_BOT_MESSAGES"),
            }
        }

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    #[tokio::test]
    async fn our_own_bot_the_kill_switch_and_edits_each_still_drop_pg() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!(
                "skipping our_own_bot_the_kill_switch_and_edits_each_still_drop_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_bot_ingest_drops";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        let db_url = format!("{prefix}/{db}{params}");

        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&db_url).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let team_id = "T0TESTWS0";
        let channel = "C-BOT-DROPS";
        let account_id = seed_slack_account_for_workspace(&pool, team_id).await;
        seed_bot_identity_for_test(account_id, team_id, "U0TESTBOT0", Some("B0TESTBOT0"));

        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_secret = std::env::var("SLACK_SIGNING_SECRET").ok();
        let old_flag = std::env::var("SLACK_INGEST_BOT_MESSAGES").ok();
        let secret = "bot_drop_test_secret";
        unsafe {
            std::env::set_var("SLACK_SIGNING_SECRET", secret);
            std::env::remove_var("SLACK_INGEST_BOT_MESSAGES");
        }

        // ── 1. OUR OWN bot's "✅ Ingested N past messages" confirmation ─────
        // The original filter's whole reason: this connector posts into the
        // very channels it watches.
        let own = slack_fixture("own_bot_confirmation");
        assert_eq!(own["bot_id"].as_str(), Some("B0TESTBOT0"));
        let body = slack_event_envelope(&own, team_id, channel);
        assert_eq!(post_slack_event(&pool, secret, &body).await, StatusCode::OK);
        assert!(
            slack_ingest_jobs(&pool).await.is_empty(),
            "our own bot's post must never be ingested — that is what the bot_id filter was \
             originally for, and lifting the filter must not lift THIS"
        );

        // ── 2. Kill switch off ⇒ pre-change behaviour, byte for byte ────────
        unsafe { std::env::set_var("SLACK_INGEST_BOT_MESSAGES", "0") };
        let github = slack_fixture("github_pr_opened_attachments");
        let body = slack_event_envelope(&github, team_id, channel);
        assert_eq!(post_slack_event(&pool, secret, &body).await, StatusCode::OK);
        assert!(
            slack_ingest_jobs(&pool).await.is_empty(),
            "SLACK_INGEST_BOT_MESSAGES=0 must drop the very message the default admits"
        );
        unsafe { std::env::remove_var("SLACK_INGEST_BOT_MESSAGES") };

        // ── 3. A `message_changed` edit is STILL dropped ────────────────────
        // The subtype allowlist admits exactly `bot_message` and
        // `thread_broadcast`; everything else keeps falling through. (This
        // one carries no `room.transcript_file_id`, so the huddle trigger
        // above it does not fire either.)
        let edit = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": team_id,
            "event": {
                "type": "message",
                "subtype": "message_changed",
                "channel": channel,
                "ts": "1788996300.000100",
                "message": {"ts": "1788996300.000100", "text": "edited text", "user": "U57C3A9EF09"},
            }
        }))
        .unwrap();
        assert_eq!(post_slack_event(&pool, secret, &edit).await, StatusCode::OK);
        assert!(
            slack_ingest_jobs(&pool).await.is_empty(),
            "an edit must still be dropped — widening the filter to bot messages must not \
             widen it to every subtype"
        );

        // ── 4. …and a `channel_join`, the biggest of the other-subtype drops
        let join = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": team_id,
            "event": {
                "type": "message",
                "subtype": "channel_join",
                "channel": channel,
                "ts": "1788996400.000100",
                "user": "U57C3A9EF09",
                "text": "<@U57C3A9EF09> has joined the channel",
            }
        }))
        .unwrap();
        assert_eq!(post_slack_event(&pool, secret, &join).await, StatusCode::OK);
        assert!(
            slack_ingest_jobs(&pool).await.is_empty(),
            "40 of the 42 other-subtype drops measured were channel_join — still dropped"
        );

        // ── 5. Control: with the flag back on, a third-party bot DOES land ──
        let alerting_app = slack_fixture("app_rich_text_blocks");
        let body = slack_event_envelope(&alerting_app, team_id, channel);
        assert_eq!(post_slack_event(&pool, secret, &body).await, StatusCode::OK);
        let jobs = slack_ingest_jobs(&pool).await;
        assert_eq!(
            jobs.len(),
            1,
            "negative control: the four drops above must be the FILTERS deciding, not the \
             fixture pipeline silently failing to enqueue anything at all"
        );
        assert_eq!(jobs[0]["bot_name"].as_str(), Some("Sentinel"));

        unsafe {
            match old_secret {
                Some(v) => std::env::set_var("SLACK_SIGNING_SECRET", v),
                None => std::env::remove_var("SLACK_SIGNING_SECRET"),
            }
            match old_flag {
                Some(v) => std::env::set_var("SLACK_INGEST_BOT_MESSAGES", v),
                None => std::env::remove_var("SLACK_INGEST_BOT_MESSAGES"),
            }
        }

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Seed the minimum an install needs for `resolve_slack_account_planes`
    /// to match this workspace: an account and its account-wide
    /// `oauth_tokens` row. Same shape the huddle-ordering test above seeds,
    /// and deliberately WITHOUT a `connector_installs` row, so the
    /// `resolve_slack_account_ids` self-heal path is the one under test.
    #[cfg(test)]
    async fn seed_slack_account_for_workspace(pool: &sqlx::PgPool, team_id: &str) -> uuid::Uuid {
        let owner: uuid::Uuid = sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("owner-{}@test.local", uuid::Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap();
        let account_id: uuid::Uuid =
            sqlx::query_scalar("INSERT INTO accounts(owner_user_id) VALUES ($1) RETURNING id")
                .bind(owner)
                .fetch_one(pool)
                .await
                .unwrap();
        sqlx::query(
            "INSERT INTO oauth_tokens (account_id, provider, scope, access_token, \
             provider_workspace_id) VALUES ($1, 'slack', 'test', $2, $3)",
        )
        .bind(account_id)
        .bind(b"stub-encrypted-token".to_vec())
        .bind(team_id)
        .execute(pool)
        .await
        .unwrap();
        account_id
    }
    // ── A: a genuine DB error on the durable-enqueue path must 503 ────────────
    //
    // Before this fix, the huddle-capture enqueue was a detached
    // `tokio::spawn` whose every failure path only logged — the handler
    // always acked 200 regardless. Proves the opposite is now true: a real DB
    // error while resolving the account (here: an UNMIGRATED database, so
    // `resolve_oauth_account_id` does not exist) must map to 503 so Slack
    // redelivers, exactly like the deleted `reaction_added` branch this
    // mirrors.
    #[tokio::test]
    async fn huddle_trigger_db_error_during_enqueue_returns_503_pg() {
        use sqlx::postgres::PgPoolOptions;
        use sqlx::{Connection, Executor, PgConnection};

        let Some(base) = std::env::var("ORGT_TEST_BASE_URL").ok() else {
            eprintln!("skipping huddle_trigger_db_error_during_enqueue_returns_503_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_huddle_trigger_db_error";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        let db_url = format!("{prefix}/{db}{params}");

        let mut admin_conn = PgConnection::connect(&base).await.expect("connect admin");
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        admin_conn
            .execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();

        // Deliberately NOT migrated: `resolve_oauth_account_id` (and every
        // other table this path touches) does not exist, so the account
        // resolution step inside `enqueue_huddle_capture` fails with a real
        // `sqlx::Error` — the transient-DB-error case this fix exists for.
        let pool: sqlx::PgPool = PgPoolOptions::new().connect(&db_url).await.unwrap();

        let _guard = slack_signing_secret_lock().lock().unwrap();
        let old_secret = std::env::var("SLACK_SIGNING_SECRET").ok();
        let secret = "huddle_db_error_test_secret";
        unsafe { std::env::set_var("SLACK_SIGNING_SECRET", secret); }

        let mut state = (*test_state()).clone();
        state.db = Some(pool.clone());
        let router = events_router(std::sync::Arc::new(state));

        let body = serde_json::to_string(&json!({
            "type": "event_callback",
            "team_id": "T-DB-ERROR-TEST",
            "event": {
                "type": "message",
                "subtype": "message_changed",
                "channel": "C-DB-ERROR-TEST",
                "message": {
                    "ts": "1700000000.000100",
                    "room": {
                        "transcript_file_id": "F-DB-ERROR-TEST",
                        "thread_root_ts": "1700000000.000000"
                    }
                }
            }
        }))
        .unwrap();
        let ts = now_ts();
        let sig = make_signature(secret, &ts, &body);

        let req = HttpRequest::builder()
            .method("POST")
            .uri("/v1/slack/events")
            .header("content-type", "application/json")
            .header("x-slack-request-timestamp", &ts)
            .header("x-slack-signature", &sig)
            .body(Body::from(body))
            .unwrap();

        let resp = router.oneshot(req).await.unwrap();

        match old_secret {
            Some(v) => unsafe { std::env::set_var("SLACK_SIGNING_SECRET", v) },
            None => unsafe { std::env::remove_var("SLACK_SIGNING_SECRET") },
        }

        assert_eq!(
            resp.status(),
            StatusCode::SERVICE_UNAVAILABLE,
            "a genuine DB error while enqueuing the huddle capture job must 503 so Slack \
             redelivers — silently acking 200 here loses the huddle's content forever (the \
             trigger fires exactly once per huddle)"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }
}

#[cfg(test)]
mod slack_watched_channel_destination_tests {
    //! docs/connectors/slack.md §2/§4 + docs/connectors/CONNECTOR_CONTRACT.md
    //! §4 — the `slack_watched_channels` per-channel destination table and
    //! its combined read path
    //! (`connectors::slack::resolve_channel_destinations`).
    //!
    //! In-crate (not `backend/tests/*_pg.rs`) for the same reason as
    //! `routes::slack::resolve_primary_graph_org_gate_tests`: every function
    //! under test here is `pub(crate)` or private, so an external test crate
    //! has no way to drive them at all.
    //!
    //! Gated on `ORGT_TEST_BASE_URL`, migrating the CONTROL tree only — this
    //! file's own module header documents Slack message ingest as CONSUMER-
    //! plane webhook machinery that resolves `slack_integrations`/`graphs`/
    //! `slack_watched_channels` on the shared consumer/control pool for
    //! PERSONAL AND ORG accounts alike (there is no per-org tenant-DB routing
    //! in the Slack ingest funnel today), so the control tree is the schema
    //! every real caller of these functions runs against.
    //!
    //! Not wired into a CI job by itself picking it up: like
    //! `resolve_primary_graph_org_gate_tests`, an in-crate `#[cfg(test)]`
    //! module needs an EXPLICIT `cargo test --lib <module>` step (added to the
    //! `cross-tenant-probe` job in this PR) — the workspace `cargo test` job
    //! has no Postgres service and silently skips every test here.
    //!
    //!   ORGT_TEST_BASE_URL=postgres://alex@localhost:5432/postgres \
    //!     cargo test -p coralswarm-backend --lib slack_watched_channel_destination_tests

    use sqlx::postgres::PgPoolOptions;
    use sqlx::{Connection, Executor, PgConnection, PgPool};
    use uuid::Uuid;

    use super::{enqueue_huddle_capture, resolve_email_unmatched_target_pool, HuddleTrigger};
    use crate::connectors::slack::{
        ensure_slack_watched_channel, resolve_channel_destinations, slack_watched_destination,
        ChannelDestination, PickedDestination, SlackChannelPick,
    };
    use crate::routes::slack::{resolve_primary_graph, AccountPlane, IngestCaller};
    use crate::routes::slack_events::{enqueue_slack_message_ingest, ingest_slack_message};
    use crate::store::PostgresStore;
    use crate::tenancy::registry::TenantRegistry;
    use crate::tenancy::routing::TenantStatus;
    use crate::tenancy::runtime::TenantRuntime;
    use crate::tenancy::secrets::StaticMapResolver;
    use std::sync::Arc;

    fn base_url() -> Option<String> {
        std::env::var("ORGT_TEST_BASE_URL").ok()
    }
    fn with_db(base: &str, db: &str) -> String {
        let (prefix, rest) = base.split_at(base.rfind('/').expect("url has a path /"));
        let params = rest.find('?').map(|i| &rest[i..]).unwrap_or("");
        format!("{prefix}/{db}{params}")
    }
    async fn recreate(base: &str, db: &str) -> PgConnection {
        let mut conn = PgConnection::connect(base).await.expect("connect admin");
        conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .unwrap();
        conn.execute(format!("CREATE DATABASE {db}").as_str())
            .await
            .unwrap();
        conn
    }

    async fn seed_user(pool: &PgPool) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO users(id, email) VALUES (gen_random_uuid(), $1) RETURNING id",
        )
        .bind(format!("user-{}@test.local", Uuid::new_v4()))
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// Seeds an account (personal when `clerk_org_id` is `None`) AND its
    /// `account_members` row for `owner` — `resolve_channel_destinations`'s
    /// org fan-out reads `account_members` directly (via
    /// `resolve_org_fanout_destinations`), so a seed with no member row at
    /// all would make every test here exercise the "zero members" edge case
    /// instead of the real path.
    async fn seed_account(pool: &PgPool, owner: Uuid, clerk_org_id: Option<&str>) -> Uuid {
        let account_id: Uuid = sqlx::query_scalar(
            "INSERT INTO accounts(owner_user_id, clerk_org_id) VALUES ($1, $2) RETURNING id",
        )
        .bind(owner)
        .bind(clerk_org_id)
        .fetch_one(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO account_members(account_id, user_id, role, seat_tier_id) \
             VALUES ($1, $2, 'owner', \
                     CASE WHEN $3 IS NOT NULL \
                          THEN (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true) \
                          ELSE NULL END)",
        )
        .bind(account_id)
        .bind(owner)
        .bind(clerk_org_id)
        .execute(pool)
        .await
        .unwrap();
        account_id
    }

    // ── Durable tenant hand-off helpers (org-slack step 2) ────────────────────

    /// Seed a `connector_installs` row directly — this migration's Rust write
    /// path is `pub(crate)` behind `record_connector_install`, so (same
    /// constraint every other `*_pg`-style suite in this crate documents for
    /// its own private write paths) this drives the underlying SQL.
    async fn seed_connector_install(
        pool: &PgPool,
        workspace_id: &str,
        clerk_org_id: Option<&str>,
        account_id: Uuid,
    ) {
        sqlx::query(
            "INSERT INTO connector_installs \
                (provider, external_workspace_id, clerk_org_id, account_id) \
             VALUES ('slack', $1, $2, $3)",
        )
        .bind(workspace_id)
        .bind(clerk_org_id)
        .bind(account_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Mint an org's `tenant_databases` routing row — mirrors
    /// `member_ocean_provisioning_pg.rs`'s `mint_org`, parameterized on
    /// `account_id` (this suite already has one from [`seed_account`], so
    /// there is no separate owner/account creation step to duplicate here).
    async fn mint_tenant_databases_row(
        control_pool: &PgPool,
        org_id: &str,
        slug: &str,
        account_id: Uuid,
        secret_ref: &str,
    ) {
        // Tenant-binding identity is a financial current-generation write.
        // Use the real scoped entry/rotation lock; do not bypass the guard or
        // mint any credits, grants, reservations or execution authorization.
        let mut tx = crate::tenancy::scope::begin_account_scoped(control_pool, account_id)
            .await.unwrap();
        crate::billing::renewal::lock_period_rotation(&mut tx, account_id).await.unwrap();
        sqlx::query(
            "INSERT INTO tenant_databases \
                (clerk_org_id, account_id, slug, db_secret_ref, substrate, schema_version, status) \
             VALUES ($1, $2, $3, $4, 'neon', 1, $5)",
        )
        .bind(org_id)
        .bind(account_id)
        .bind(slug)
        .bind(secret_ref)
        .bind(TenantStatus::Active.as_str())
        .execute(&mut *tx)
        .await
        .unwrap();
        tx.commit().await.unwrap();
    }

    /// Build (connect + migrate the TENANT tree + stamp `tenant_meta`) a
    /// second scratch database standing in for one org's real tenant DB —
    /// same shape as `member_ocean_provisioning_pg.rs`'s own `tenant_pool`
    /// helper.
    async fn build_tenant_pool(base: &str, db: &str, org_id: &str) -> PgPool {
        let pool = PgPoolOptions::new().connect(&with_db(base, db)).await.unwrap();
        sqlx::migrate!("./migrations_tenant").run(&pool).await.unwrap();
        sqlx::query("INSERT INTO tenant_meta (id, org_id) VALUES (true, $1)")
            .bind(org_id)
            .execute(&pool)
            .await
            .unwrap();
        pool
    }

    /// Count `SlackMessageIngest` jobs in `pool`'s `jobs` table for
    /// `account_id` — the routing assertion every test below reduces to:
    /// "did the job land in THIS database, under THIS account, or not".
    async fn slack_message_ingest_job_count(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE kind = 'SlackMessageIngest' AND account_id = $1",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn huddle_capture_job_count(pool: &PgPool, account_id: Uuid) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM jobs WHERE kind = 'SlackHuddleCapture' AND account_id = $1",
        )
        .bind(account_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// The sibling of `enqueue_slack_message_ingest_fans_out_…` below, for the
    /// huddle path — and the test whose ABSENCE let a real bug ship.
    ///
    /// Step 4 moved `workers::huddle` onto the tenant loop and left this
    /// enqueue writing to the control pool. `tests/slack_worker_tenant_loop_pg
    /// .rs` was written as the regression guard for exactly that pairing, but
    /// it enqueues jobs DIRECTLY onto a tenant's `jobs` table and then proves
    /// another tenant's worker cannot see them — it tests the WORKER half and
    /// ASSUMES the enqueue half. So the guard passed while every huddle in an
    /// org workspace was silently dropped. Half a pairing tested is not a
    /// pairing tested; this closes the other half.
    #[tokio::test]
    async fn enqueue_huddle_capture_fans_out_to_the_correct_plane_per_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enqueue_huddle_capture_fans_out_to_the_correct_plane_per_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt1_db, tnt2_db) = (
            "orgt_huddle_handoff_ctl",
            "orgt_huddle_handoff_tnt1",
            "orgt_huddle_handoff_tnt2",
        );
        let _database_cleanup_8 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let mut _database_cleanup_9s = Vec::new();
        for db in [tnt1_db, tnt2_db] {
            _database_cleanup_9s.push(crate::test_db::TestDatabase::adopt_from_url(&base, db));
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
        }

        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let team_id = format!("T-huddle-{}", Uuid::new_v4());

        // Personal account: no tenant row ⇒ control plane.
        let personal_owner = seed_user(&control).await;
        let personal_account = seed_account(&control, personal_owner, None).await;
        seed_connector_install(&control, &team_id, None, personal_account).await;

        // Two independent orgs sharing the SAME workspace, each with its own
        // active tenant DB. The old singular `resolve_slack_account_id` would
        // have served only whichever sorted first.
        let org1_id = format!("org_huddle1_{}", Uuid::new_v4());
        let org1_owner = seed_user(&control).await;
        let org1_account = seed_account(&control, org1_owner, Some(&org1_id)).await;
        seed_connector_install(&control, &team_id, Some(&org1_id), org1_account).await;
        mint_tenant_databases_row(&control, &org1_id, "huddle-org1", org1_account, "SECRET_HUDDLE1")
            .await;
        let tenant1 = build_tenant_pool(&base, tnt1_db, &org1_id).await;

        let org2_id = format!("org_huddle2_{}", Uuid::new_v4());
        let org2_owner = seed_user(&control).await;
        let org2_account = seed_account(&control, org2_owner, Some(&org2_id)).await;
        seed_connector_install(&control, &team_id, Some(&org2_id), org2_account).await;
        mint_tenant_databases_row(&control, &org2_id, "huddle-org2", org2_account, "SECRET_HUDDLE2")
            .await;
        let tenant2 = build_tenant_pool(&base, tnt2_db, &org2_id).await;

        let resolver = StaticMapResolver::from_pairs([
            ("SECRET_HUDDLE1", with_db(&base, tnt1_db)),
            ("SECRET_HUDDLE2", with_db(&base, tnt2_db)),
        ]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let mut state = crate::test_app_state_owned(Some(TenantRuntime::new(
            true,
            "coralswarm.test",
            registry,
        )));
        state.db = Some(control.clone());
        state.store = Some(Arc::new(PostgresStore::new(control.clone())));
        let state = Arc::new(state);

        let trigger = HuddleTrigger {
            channel_id: "C-huddle".to_string(),
            huddle_thread_ts: "1700000000.000200".to_string(),
            canvas_file_id: "F-CANVAS".to_string(),
            participant_history: vec!["U-ONE".to_string()],
            date_start: None,
            date_end: None,
        };
        enqueue_huddle_capture(&state, &control, &team_id, &trigger)
            .await
            .expect("huddle enqueue must succeed for three servable accounts");

        assert_eq!(
            huddle_capture_job_count(&control, personal_account).await,
            1,
            "the personal account's huddle job must land on the control/consumer plane"
        );

        for (label, account, own, other) in [
            ("org 1", org1_account, &tenant1, &tenant2),
            ("org 2", org2_account, &tenant2, &tenant1),
        ] {
            assert_eq!(
                huddle_capture_job_count(own, account).await,
                1,
                "{label}'s huddle job must land on its OWN tenant plane"
            );
            assert_eq!(
                huddle_capture_job_count(&control, account).await,
                0,
                "{label}'s huddle job must NEVER land on the control/consumer plane — this is \
                 the exact defect: the worker dequeues from the tenant, so a job left on \
                 control is claimed by the consumer entry and captured with the wrong plane"
            );
            assert_eq!(
                huddle_capture_job_count(other, account).await,
                0,
                "{label}'s huddle job must NEVER land on the other org's tenant plane"
            );
        }

        control.close().await;
        tenant1.close().await;
        tenant2.close().await;
        for db in [ctl_db, tnt1_db, tnt2_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// CodeRabbit round 1 (#1005, Data Integrity): FAILS WITHOUT THE FIX —
    /// the exact "job left on control, worker dequeues from tenant" defect
    /// [`enqueue_huddle_capture_fans_out_to_the_correct_plane_per_account_pg`]
    /// above already pins for the huddle path, reproduced here for
    /// `resolve_email_unmatched_target_pool`
    /// (`handle_member_joined_for_account`'s `EmailUnmatchedSlackChannelMembers`
    /// enqueue). A personal account resolves to the control pool; an active
    /// org resolves to its OWN tenant pool, never control and never another
    /// org's tenant.
    #[tokio::test]
    async fn resolve_email_unmatched_target_pool_resolves_to_the_correct_plane_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping resolve_email_unmatched_target_pool_resolves_to_the_correct_plane_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_unmatched_email_ctl", "orgt_unmatched_email_tnt");
        let _database_cleanup_10 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_11 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        {
            let conn = recreate(&base, tnt_db).await;
            conn.close().await.ok();
        }

        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        // Personal account: no tenant row ⇒ control plane.
        let personal_owner = seed_user(&control).await;
        let personal_account = seed_account(&control, personal_owner, None).await;

        // An ACTIVE org with its own tenant database.
        let org_id = format!("org_unmatched_{}", Uuid::new_v4());
        let org_owner = seed_user(&control).await;
        let org_account = seed_account(&control, org_owner, Some(&org_id)).await;
        mint_tenant_databases_row(&control, &org_id, "unmatched-org", org_account, "SECRET_UNMATCHED")
            .await;
        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let resolver = StaticMapResolver::from_pairs([("SECRET_UNMATCHED", with_db(&base, tnt_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let mut state = crate::test_app_state_owned(Some(TenantRuntime::new(
            true,
            "coralswarm.test",
            registry,
        )));
        state.db = Some(control.clone());
        state.store = Some(Arc::new(PostgresStore::new(control.clone())));
        let state = Arc::new(state);

        let personal_pool =
            resolve_email_unmatched_target_pool(&state, &control, personal_account, None)
                .await
                .expect("a personal account must always resolve to a pool");
        assert_eq!(
            personal_pool.connect_options().get_database(),
            control.connect_options().get_database(),
            "a personal account must resolve to the control/consumer plane"
        );

        let org_pool =
            resolve_email_unmatched_target_pool(&state, &control, org_account, Some(&org_id))
                .await
                .expect("an active org must resolve to a pool");
        assert_eq!(
            org_pool.connect_options().get_database(),
            tenant.connect_options().get_database(),
            "an active org must resolve to its OWN tenant plane, never control — this is the \
             exact defect: the worker dequeues from the tenant, so a job left on control is \
             claimed by the consumer entry and run against the wrong identity tables entirely"
        );

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// The durable tenant hand-off's core routing proof (org-slack step 2):
    /// one Slack workspace, matched by THREE different `connector_installs`
    /// rows — a personal account, and two DIFFERENT org accounts each with
    /// their OWN active tenant database — and `enqueue_slack_message_ingest`
    /// must fan out to all three, each `SlackMessageIngest` job landing on
    /// exactly the ONE plane its account actually belongs to:
    ///
    ///   * the personal account's job lands on the CONTROL/consumer `jobs`
    ///     table (consumer-plane behaviour, unchanged from before this PR);
    ///   * each org's job lands on THAT org's own tenant `jobs` table, and
    ///     NEVER on the consumer plane or the OTHER org's tenant plane —
    ///     fan-out to two (here, three) distinct destinations from one
    ///     workspace is correct, not duplication.
    #[tokio::test]
    async fn enqueue_slack_message_ingest_fans_out_to_the_correct_plane_per_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enqueue_slack_message_ingest_fans_out_to_the_correct_plane_per_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt1_db, tnt2_db) = (
            "orgt_slack_handoff_ctl",
            "orgt_slack_handoff_tnt1",
            "orgt_slack_handoff_tnt2",
        );
        let _database_cleanup_12 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let mut _database_cleanup_13s = Vec::new();
        for db in [tnt1_db, tnt2_db] {
            _database_cleanup_13s.push(crate::test_db::TestDatabase::adopt_from_url(&base, db));
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
        }

        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let team_id = format!("T-handoff-{}", Uuid::new_v4());

        // ── Personal account: no tenant_databases row at all — its job must
        // land on the control/consumer plane.
        let personal_owner = seed_user(&control).await;
        let personal_account = seed_account(&control, personal_owner, None).await;
        seed_connector_install(&control, &team_id, None, personal_account).await;

        // ── Org 1: an ACTIVE tenant database of its own.
        let org1_id = format!("org_handoff1_{}", Uuid::new_v4());
        let org1_owner = seed_user(&control).await;
        let org1_account = seed_account(&control, org1_owner, Some(&org1_id)).await;
        seed_connector_install(&control, &team_id, Some(&org1_id), org1_account).await;
        mint_tenant_databases_row(&control, &org1_id, "handoff-org1", org1_account, "SECRET_HANDOFF1")
            .await;
        let tenant1 = build_tenant_pool(&base, tnt1_db, &org1_id).await;

        // ── Org 2: a SECOND, independent org with its own ACTIVE tenant
        // database, sharing the SAME Slack workspace.
        let org2_id = format!("org_handoff2_{}", Uuid::new_v4());
        let org2_owner = seed_user(&control).await;
        let org2_account = seed_account(&control, org2_owner, Some(&org2_id)).await;
        seed_connector_install(&control, &team_id, Some(&org2_id), org2_account).await;
        mint_tenant_databases_row(&control, &org2_id, "handoff-org2", org2_account, "SECRET_HANDOFF2")
            .await;
        let tenant2 = build_tenant_pool(&base, tnt2_db, &org2_id).await;

        let resolver = StaticMapResolver::from_pairs([
            ("SECRET_HANDOFF1", with_db(&base, tnt1_db)),
            ("SECRET_HANDOFF2", with_db(&base, tnt2_db)),
        ]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let mut state = crate::test_app_state_owned(Some(TenantRuntime::new(
            true,
            "coralswarm.test",
            registry,
        )));
        state.db = Some(control.clone());
        state.store = Some(Arc::new(PostgresStore::new(control.clone())));
        let state = Arc::new(state);

        enqueue_slack_message_ingest(
            state,
            team_id.clone(),
            "C-handoff".to_string(),
            "U-AUTHOR".to_string(),
            "1700000000.000100".to_string(),
            None,
            "hello from the shared workspace".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await;

        // ── The personal account's job: control plane only.
        assert_eq!(
            slack_message_ingest_job_count(&control, personal_account).await,
            1,
            "the personal account's job must land on the control/consumer plane"
        );

        // ── Org 1's job: its OWN tenant plane only — never control, never
        // org 2's tenant.
        assert_eq!(
            slack_message_ingest_job_count(&tenant1, org1_account).await,
            1,
            "org 1's job must land on org 1's own tenant plane"
        );
        assert_eq!(
            slack_message_ingest_job_count(&control, org1_account).await,
            0,
            "org 1's job must NEVER land on the control/consumer plane"
        );
        assert_eq!(
            slack_message_ingest_job_count(&tenant2, org1_account).await,
            0,
            "org 1's job must NEVER land on org 2's tenant plane"
        );

        // ── Org 2's job: its OWN tenant plane only — never control, never
        // org 1's tenant. Proves the fan-out is genuinely per-account, not a
        // single resolved plane reused for every match.
        assert_eq!(
            slack_message_ingest_job_count(&tenant2, org2_account).await,
            1,
            "org 2's job must land on org 2's own tenant plane"
        );
        assert_eq!(
            slack_message_ingest_job_count(&control, org2_account).await,
            0,
            "org 2's job must NEVER land on the control/consumer plane"
        );
        assert_eq!(
            slack_message_ingest_job_count(&tenant1, org2_account).await,
            0,
            "org 2's job must NEVER land on org 1's tenant plane"
        );

        control.close().await;
        tenant1.close().await;
        tenant2.close().await;
        // `admin_conn` (from the FIRST `recreate` call) is a plain connection
        // to `base`, not pinned to `ctl_db` — reused here to drop all three
        // scratch databases, same "one admin connection, several DROPs"
        // shape `member_ocean_provisioning_pg.rs`'s own `recreate` helper
        // uses.
        for db in [ctl_db, tnt1_db, tnt2_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// An org account whose Clerk org has NO `tenant_databases` row at all
    /// (never onboarded) must still get its message ingested — on the
    /// CONSUMER plane, since that is genuinely where its content already
    /// lives (see `resolve_org_tenant_pool_for_ingest`'s doc comment: this is
    /// the ONE org case that is not a guess). Proves the fallback is
    /// deliberate, not an accidental "everything lands on control" bug that
    /// would also mask the routing test above.
    #[tokio::test]
    async fn enqueue_slack_message_ingest_falls_back_to_consumer_for_a_never_onboarded_org_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping enqueue_slack_message_ingest_falls_back_to_consumer_for_a_never_onboarded_org_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_handoff_no_tenant_row";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let team_id = format!("T-no-tenant-{}", Uuid::new_v4());
        let org_id = format!("org_no_tenant_{}", Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;
        seed_connector_install(&control, &team_id, Some(&org_id), account).await;
        // Deliberately NO `tenant_databases` row for `org_id`.

        let mut state = crate::test_app_state_owned(None);
        state.db = Some(control.clone());
        state.store = Some(Arc::new(PostgresStore::new(control.clone())));
        let state = Arc::new(state);

        enqueue_slack_message_ingest(
            state,
            team_id.clone(),
            "C-no-tenant".to_string(),
            "U-AUTHOR".to_string(),
            "1700000000.000100".to_string(),
            None,
            "an org that has never been onboarded".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await;

        assert_eq!(
            slack_message_ingest_job_count(&control, account).await,
            1,
            "a never-onboarded org's job must fall back to the control/consumer plane, not be \
             dropped"
        );

        control.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Build a `SlackChannelPick` for a `Single` destination — named fields
    /// throughout, so a test call site can't transpose two `Uuid`s or two
    /// `&str`s the way the old positional `ensure_slack_watched_channel`
    /// signature could (this struct is the fix for exactly that hazard).
    fn single_pick(
        account_id: Uuid,
        user_id: Uuid,
        workspace_id: &str,
        channel_id: &str,
        display_name: &str,
        graph_id: Uuid,
    ) -> SlackChannelPick {
        SlackChannelPick {
            account_id,
            user_id,
            workspace_id: workspace_id.to_string(),
            channel_id: channel_id.to_string(),
            display_name: display_name.to_string(),
            destination: PickedDestination::Single(graph_id),
        }
    }

    async fn seed_graph(
        pool: &PgPool,
        account_id: Uuid,
        kind: &str,
        owner_user_id: Option<Uuid>,
        is_primary: bool,
    ) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO graphs(account_id, name, kind, owner_user_id, is_primary) \
             VALUES ($1, 'test ocean', $2, $3, $4) RETURNING id",
        )
        .bind(account_id)
        .bind(kind)
        .bind(owner_user_id)
        .bind(is_primary)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// A watched channel's explicit destination row wins over the account's
    /// default resolution — the whole point of reading
    /// `slack_watched_channels` before falling back to
    /// `routes::slack::resolve_primary_graph`/org fan-out.
    #[tokio::test]
    async fn destination_row_preferred_over_default_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping destination_row_preferred_over_default_pg: set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_dest_preferred";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;

        // The account's default (auto-created primary) ocean — what ingest
        // would resolve to for a channel with no row.
        let default_graph = match resolve_channel_destinations(
            &pool, &pool, account, AccountPlane::Personal, "T1", "C-other",
        )
            .await
            .unwrap()
        {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single default for a personal account, got {other:?}"),
        };

        // A DIFFERENT ocean, explicitly recorded as this channel's
        // destination.
        let channel_graph = seed_graph(&pool, account, "personal", None, false).await;
        assert_ne!(default_graph, channel_graph);

        let outcome = ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account, owner, "T1", "C1", "general", channel_graph),
        )
        .await
        .unwrap();
        assert!(outcome.newly_inserted, "first pick for this channel must be newly inserted");
        assert_eq!(outcome.destination, ChannelDestination::Single(channel_graph));

        let resolved = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap();
        assert_eq!(
            resolved,
            ChannelDestination::Single(channel_graph),
            "ingest must prefer the channel's explicit destination row over the account's \
             default"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// A channel with NO destination row falls back to the account-level
    /// default — for a PERSONAL account, the account's own primary ocean
    /// (byte-identical to before this table existed); for an ORG account,
    /// this is the actual regression test for the bug this table's read path
    /// exists to fix: the OLD path (`resolve_primary_graph(..,
    /// IngestCaller::NonAttributable)`) refuses under an org account, but
    /// `resolve_channel_destinations` must NOT refuse — it must degrade to
    /// an honest fan-out instead (empty today, since capability C5 identity
    /// mapping is not wired — see `resolve_org_fanout_destinations`'s doc
    /// comment).
    #[tokio::test]
    async fn no_row_falls_back_to_account_default_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping no_row_falls_back_to_account_default_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_dest_fallback";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // ── Personal account: identical to today's behavior ────────────────
        let personal_owner = seed_user(&pool).await;
        let personal_account = seed_account(&pool, personal_owner, None).await;

        let no_row = slack_watched_destination(&pool, &pool, personal_account, "T-personal", "C-none")
            .await
            .unwrap();
        assert_eq!(no_row, None, "no destination row exists yet for this channel");

        let fallback_graph =
            match resolve_channel_destinations(
                &pool, &pool, personal_account, AccountPlane::Personal, "T-personal", "C-none",
            )
                .await
                .unwrap()
            {
                ChannelDestination::Single(g) => g,
                other => panic!("expected a Single default for a personal account, got {other:?}"),
            };
        let (kind, is_primary): (String, bool) =
            sqlx::query_as("SELECT kind, is_primary FROM graphs WHERE id = $1")
                .bind(fallback_graph)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            (kind.as_str(), is_primary),
            ("personal", true),
            "personal accounts with no destination rows must keep landing in the same \
             primary ocean as before this table existed"
        );

        // ── Org account: the actual bug fix ─────────────────────────────────
        // Before this table's read path existed, `resolve_primary_graph(..,
        // IngestCaller::NonAttributable)` REFUSES here (the org branch's
        // unconditional hard-fail) — this is precisely why Slack ingest has
        // been fail-closed dead for every org account.
        // `resolve_channel_destinations` must NOT refuse; it must return an
        // honest (today: empty) fan-out instead.
        let org_owner = seed_user(&pool).await;
        let org_account = seed_account(&pool, org_owner, Some("org_slack_dest_test")).await;
        let _member_ocean = seed_graph(&pool, org_account, "member", Some(org_owner), true).await;

        // Sanity: the OLD path genuinely refuses under this exact account.
        resolve_primary_graph(
            &pool,
            org_account,
            AccountPlane::Org,
            IngestCaller::NonAttributable,
        )
        .await
        .expect_err(
                "sanity check: the pre-existing non-attributable path must still refuse under \
                 an org account — otherwise this test is not exercising the bug it claims to \
                 guard against",
            );

        let destination = resolve_channel_destinations(
            &pool, &pool, org_account, AccountPlane::Org, "T-org", "C-none",
        )
            .await
            .expect(
                "resolve_channel_destinations must NOT refuse under an org account — this is \
                 the fix for Slack ingest being fail-closed dead",
            );
        assert!(
            matches!(&destination, ChannelDestination::FanOut { graphs, unmapped_members: 1, .. } if graphs.is_empty()),
            "{destination:?}: with no identity mapping wired (capability C5), the account's sole member must be \
             counted as unmapped and NEVER guessed a destination — degrade honestly, don't \
             fabricate a seat"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Cross-account isolation (contract I7's spirit, applied to a
    /// destination row rather than a token): two accounts each watch a
    /// channel under the SAME literal `workspace_id`/`channel_id` strings
    /// (the composite key is `(account_id, workspace_id, channel_id)`, so
    /// this is the adversarial case, not an unrealistic one). Reading
    /// account A's destination must never return account B's graph, and vice
    /// versa — there is no `account_id IS NULL OR account_id = $me` fallback
    /// anywhere in `slack_watched_destination`.
    #[tokio::test]
    async fn cross_account_isolation_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping cross_account_isolation_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_dest_cross_account";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a, None).await;
        let graph_a = seed_graph(&pool, account_a, "personal", None, false).await;

        let owner_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, owner_b, None).await;
        let graph_b = seed_graph(&pool, account_b, "personal", None, false).await;

        // Deliberately the SAME workspace_id/channel_id for both accounts.
        ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account_a, owner_a, "T-shared", "C-shared", "general", graph_a),
        )
        .await
        .unwrap();
        ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account_b, owner_b, "T-shared", "C-shared", "general", graph_b),
        )
        .await
        .unwrap();

        let resolved_a =
            slack_watched_destination(&pool, &pool, account_a, "T-shared", "C-shared").await.unwrap();
        assert_eq!(
            resolved_a,
            Some(ChannelDestination::Single(graph_a)),
            "account A must resolve its OWN destination"
        );
        assert_ne!(
            resolved_a,
            Some(ChannelDestination::Single(graph_b)),
            "account A must NEVER receive account B's destination graph"
        );

        let resolved_b =
            slack_watched_destination(&pool, &pool, account_b, "T-shared", "C-shared").await.unwrap();
        assert_eq!(
            resolved_b,
            Some(ChannelDestination::Single(graph_b)),
            "account B must resolve its OWN destination"
        );
        assert_ne!(
            resolved_b,
            Some(ChannelDestination::Single(graph_a)),
            "account B must NEVER receive account A's destination graph"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// Contract I2 — one container, one destination, fixed at authorisation
    /// time. A second `ensure_slack_watched_channel` call for the SAME
    /// channel must never silently re-point an already-authorised channel's
    /// destination.
    #[tokio::test]
    async fn destination_fixed_at_authorisation_time_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping destination_fixed_at_authorisation_time_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_dest_fixed";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        let first_graph = seed_graph(&pool, account, "personal", None, false).await;
        let second_graph = seed_graph(&pool, account, "personal", None, false).await;
        assert_ne!(first_graph, second_graph);

        let first_outcome = ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account, owner, "T1", "C1", "general", first_graph),
        )
        .await
        .unwrap();
        assert!(first_outcome.newly_inserted);
        assert_eq!(first_outcome.destination, ChannelDestination::Single(first_graph));

        // A second pick — must NOT re-point the destination.
        let second_outcome = ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account, owner, "T1", "C1", "general-renamed", second_graph),
        )
        .await
        .unwrap();
        assert!(
            !second_outcome.newly_inserted,
            "a second pick for an already-authorised channel must report newly_inserted = false"
        );
        assert_eq!(
            second_outcome.destination,
            ChannelDestination::Single(first_graph),
            "EnsureOutcome must report the PRE-EXISTING destination, not the refused second pick"
        );

        let resolved = slack_watched_destination(&pool, &pool, account, "T1", "C1").await.unwrap();
        assert_eq!(
            resolved,
            Some(ChannelDestination::Single(first_graph)),
            "a second pick must never silently re-point an already-authorised channel's \
             destination"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// BLOCKER 1 (application-level half): `ensure_slack_watched_channel` used
    /// to accept `graph_id` from the caller with NO verification that it
    /// belongs to `account_id` at all. Proves the explicit ownership refusal
    /// added to close that gap: account A cannot persist a
    /// `slack_watched_channels` row whose `graph_id` belongs to account B, and
    /// no row is written when it refuses. Distinct from `cross_account_
    /// isolation_pg` above (which proves the READ side never crosses
    /// accounts) — this is the WRITE side, and distinct from the RLS-level
    /// proof in `backend/tests/consumer_rls_pg.rs` (which proves the same
    /// thing at the database level under the `coralswarm_app` role) — this
    /// proves the application-level check refuses BEFORE any RLS backstop
    /// would even be consulted (this test runs against the migration owner
    /// connection, which is BYPASSRLS).
    #[tokio::test]
    async fn ensure_slack_watched_channel_refuses_a_foreign_account_graph_id_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping ensure_slack_watched_channel_refuses_a_foreign_account_graph_id_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_dest_foreign_graph";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner_a = seed_user(&pool).await;
        let account_a = seed_account(&pool, owner_a, None).await;

        let owner_b = seed_user(&pool).await;
        let account_b = seed_account(&pool, owner_b, None).await;
        let graph_b = seed_graph(&pool, account_b, "personal", None, false).await;

        // Account A attempts to authorise a channel against account B's
        // graph. Must be refused outright.
        let result = ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            single_pick(account_a, owner_a, "T-foreign", "C-foreign", "general", graph_b),
        )
        .await;
        assert!(
            result.is_err(),
            "ensure_slack_watched_channel must refuse a graph_id that does not belong to the \
             calling account — got {:?}",
            result.err().map(|e| e.to_string())
        );

        // No row must have been written for the refused attempt.
        let resolved = slack_watched_destination(&pool, &pool, account_a, "T-foreign", "C-foreign")
            .await
            .unwrap();
        assert_eq!(
            resolved, None,
            "a refused cross-account write must leave no row behind"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// An explicit `mode='fanout'` row must SHORT-CIRCUIT `resolve_channel_
    /// destinations` straight to the `FanOut` branch — it must never fall
    /// through to the account-level default (PERSONAL: primary ocean; ORG:
    /// the no-row fan-out). Before `mode` existed, `slack_watched_destination`
    /// returned a bare `Option<Uuid>`, which had no way to represent an
    /// explicit fan-out pick at all — the row would have been unwriteable in
    /// the first place. This seeds the row directly (bypassing
    /// `ensure_slack_watched_channel`'s plane guard, which refuses a `FanOut`
    /// pick for a PERSONAL account) specifically so the PERSONAL-account
    /// default (always `Single`) can never be confused with "the explicit
    /// row was honoured": if the read path fell through instead of
    /// short-circuiting, this test's account would resolve to `Single`
    /// (its primary ocean), not `FanOut` — an unambiguous signal either way.
    #[tokio::test]
    async fn explicit_fanout_row_short_circuits_over_personal_default_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping explicit_fanout_row_short_circuits_over_personal_default_pg: set \
                 ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_dest_fanout_short_circuit";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;

        // Confirm the PERSONAL default really is `Single` before any row
        // exists — otherwise a `FanOut` result below would not be a
        // meaningful signal.
        match resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap()
        {
            ChannelDestination::Single(_) => {}
            other => panic!(
                "sanity check: a personal account with no row must default to Single, got \
                 {other:?}"
            ),
        }

        // Seed the row directly — see this test's doc comment for why this
        // bypasses `ensure_slack_watched_channel`'s plane guard rather than
        // calling it.
        sqlx::query(
            "INSERT INTO slack_watched_channels \
                (account_id, user_id, mode, graph_id, workspace_id, channel_id) \
             VALUES ($1, $2, 'fanout', NULL, 'T1', 'C1')",
        )
        .bind(account)
        .bind(owner)
        .execute(&pool)
        .await
        .unwrap();

        let destination = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap();
        assert!(
            matches!(&destination, ChannelDestination::FanOut { graphs, unmapped_members: 1, .. } if graphs.is_empty()),
            "{destination:?}: an explicit 'fanout' row must short-circuit resolution — never fall through to \
             this (personal) account's Single default"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    /// `ensure_slack_watched_channel`'s plane guard: docs/connectors/slack.md
    /// §2's picker only ever offers "everyone's individual oceans" on the
    /// ORG plane — a personal account has no "everyone" to fan out to — so a
    /// `PickedDestination::FanOut` reaching this function for a PERSONAL
    /// account can only be a caller bug and must be refused outright, with
    /// no row written.
    #[tokio::test]
    async fn ensure_slack_watched_channel_refuses_fanout_for_a_personal_account_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping ensure_slack_watched_channel_refuses_fanout_for_a_personal_account_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_dest_fanout_personal_refused";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;

        let result = ensure_slack_watched_channel(
            &pool,
            &pool,
            AccountPlane::Personal,
            SlackChannelPick {
                account_id: account,
                user_id: owner,
                workspace_id: "T-personal-fanout".to_string(),
                channel_id: "C-personal-fanout".to_string(),
                display_name: String::new(),
                destination: PickedDestination::FanOut,
            },
        )
        .await;
        assert!(
            result.is_err(),
            "ensure_slack_watched_channel must refuse a FanOut pick for a PERSONAL account — \
             got {:?}",
            result.err().map(|e| e.to_string())
        );

        let resolved =
            slack_watched_destination(&pool, &pool, account, "T-personal-fanout", "C-personal-fanout")
                .await
                .unwrap();
        assert_eq!(
            resolved, None,
            "a refused FanOut-under-personal-account write must leave no row behind"
        );

        pool.close().await;
        admin_conn
            .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
            .await
            .ok();
    }

    // ── C5 phase two: N>1 fan-out, exercised for the first time ──────────────
    //
    // Until this phase, `resolve_org_fanout_destinations` always returned an
    // empty `graphs` vec (capability C5 unwired), so `ingest_slack_message`'s
    // per-destination loop had NEVER run with more than one destination in
    // any test or in production. This is that test.

    async fn add_member(pool: &PgPool, account_id: Uuid, user_id: Uuid) {
        sqlx::query("INSERT INTO account_members(account_id, user_id, role, seat_tier_id) VALUES ($1, $2, 'member', (SELECT id FROM seat_tiers WHERE code = 'standard' AND is_active = true))")
            .bind(account_id)
            .bind(user_id)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn seed_confirmed_identity(
        pool: &PgPool,
        workspace_id: &str,
        external_user_id: &str,
        user_id: Uuid,
    ) {
        sqlx::query(
            "INSERT INTO connector_identities \
                (provider, workspace_id, external_user_id, canonical_name, user_id, \
                 link_method, link_verified_at) \
             VALUES ('slack', $1, $2, 'Test Person', $3, 'verified_email', now())",
        )
        .bind(workspace_id)
        .bind(external_user_id)
        .bind(user_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// A legacy-shaped `slack_integrations` row.
    ///
    /// **Not sufficient on its own for anything that INGESTS.** Since #671 the
    /// bot token is read from the encrypted `oauth_tokens` copy, so an ingest
    /// test must pair this with [`seed_slack_bot_token`] — see that helper's
    /// doc for what went wrong when they were not paired (issue #741).
    async fn seed_slack_integration(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        sqlx::query(
            "INSERT INTO slack_integrations(account_id, workspace_id, workspace_name, bot_token, created_at) \
             VALUES ($1, $2, 'Test Workspace', 'xoxb-test-garbage-token', now())",
        )
        .bind(account_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
        // Deliberately NOT also seeding `connector_installs` (migration
        // 0121_connector_installs) — `resolve_slack_account_id`, which every
        // `ingest_slack_message` call in this module's tests goes through,
        // self-heals on a resolve-miss by falling back to `oauth_tokens`/
        // `slack_integrations` directly (see that function's own doc
        // comment). This bare `slack_integrations` row — shared by every
        // test in this module — is exactly what a real legacy install
        // written by a route that bypasses `record_connector_install` would
        // look like, and every one of these tests must still resolve
        // through it via the fallback. Seeding `connector_installs` here too
        // would make every test in this module pass via the fast path
        // instead and never exercise the fallback at all.
    }

    async fn atom_count(pool: &PgPool, graph_id: Uuid) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM atoms WHERE graph_id = $1")
            .bind(graph_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// Provision `accounts.dek_wrapped` exactly like
    /// `workers::huddle::seed_dek` does, so `crypto::resolve_dek_cached`
    /// (used by `ingest_slack_message` for blob encryption) succeeds.
    /// Requires `MASTER_KEK_HEX` to already be set in the process
    /// environment — see this test's own `MASTER_KEK_HEX_ENV_LOCK` guard.
    async fn seed_dek(pool: &PgPool, account_id: Uuid) {
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        let kek = load_kek().expect("MASTER_KEK_HEX must be set before calling seed_dek");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap_dek must succeed with a valid KEK");
        sqlx::query("UPDATE accounts SET dek_wrapped = $1 WHERE id = $2")
            .bind(&wrapped)
            .bind(account_id)
            .execute(pool)
            .await
            .expect("seed dek_wrapped");
    }

    /// Seed the ENCRYPTED, account-wide Slack bot token in `oauth_tokens`,
    /// keyed on `provider_workspace_id` — the shape
    /// `workers::huddle::get_slack_bot_token_for_workspace` reads.
    ///
    /// # Why this exists (issue #741)
    ///
    /// Until #671, `ingest_slack_message` read the bot token straight off
    /// `slack_integrations.bot_token` in plain text, so a bare
    /// [`seed_slack_integration`] row was a complete fixture. #671 moved that
    /// read onto the encrypted `oauth_tokens` copy, which additionally needs a
    /// resolvable account DEK. The fixtures were not moved with it, so every
    /// ingest test in this module started taking `slack_events.rs`'s
    /// `Ok(None)` arm — *"no oauth_tokens bot token for Slack workspace (not
    /// yet backfilled …) — skipping message ingest"* — which returns
    /// `Ok(())` having written nothing. `main` was red for eight days.
    ///
    /// These tests caught it only because they assert on ATOM COUNTS rather
    /// than on `ingest_slack_message`'s return value, which is `Ok(())` in
    /// exactly that case. Keep it that way.
    ///
    /// Production always has both rows: `routes::slack::oauth_callback`
    /// dual-writes them (#671), and every Slack workspace in production was
    /// verified to carry its `oauth_tokens` row before this fixture was
    /// written. So pairing this with [`seed_slack_integration`] is what makes
    /// the fixture match production, not an extra courtesy.
    ///
    /// Uses the account's EXISTING DEK when one is already provisioned —
    /// deliberately, so a test that called [`seed_dek`] first keeps the same
    /// key its other encrypted state was written under. Provisions one only
    /// when the account has none, which is what lets this be called from
    /// tests that never call [`seed_dek`] themselves.
    async fn seed_slack_bot_token(pool: &PgPool, account_id: Uuid, workspace_id: &str) {
        // Consumer plane: `pool` holds both the row and the key. Provisions a
        // control DEK if the account has none, which is what lets callers that
        // never call `seed_dek` use this directly.
        if crate::crypto::resolve_dek(pool, account_id)
            .await
            .expect("resolve_dek must not error while seeding a Slack bot token")
            .is_none()
        {
            seed_dek(pool, account_id).await;
        }
        seed_slack_bot_token_on_plane(
            pool,
            &crate::crypto::DekPlane::consumer(pool.clone()),
            account_id,
            workspace_id,
        )
        .await;
    }

    /// [`seed_slack_bot_token`] with the DEK plane named explicitly — the
    /// ORG shape (org-plane DEK fix). The `oauth_tokens` ROW always goes to
    /// `control`; `dek_plane` is where the KEY comes from, and for an org
    /// account that is its tenant DB's `account_keys`, which is exactly what
    /// `routes::oauth::callback` now writes under.
    ///
    /// Before this existed, the two genuine two-plane org fixtures below
    /// encrypted the bot token with the CONTROL DEK while the account's real
    /// key sat in the tenant DB — the pre-fix shape. They passed only because
    /// `ingest_slack_message` read the DEK on the same wrong plane. Seeding
    /// on the right plane is what makes them proofs of the fixed behaviour
    /// rather than of the bug.
    async fn seed_slack_bot_token_on_plane(
        control: &PgPool,
        dek_plane: &crate::crypto::DekPlane,
        account_id: Uuid,
        workspace_id: &str,
    ) {
        use coralswarm_core::oauth_token::encrypt_token;
        let dek = dek_plane
            .resolve(account_id)
            .await
            .expect("resolve must not error while seeding a Slack bot token")
            .expect("the seeding plane must already carry a DEK for this account");
        let pool = control;
        let encrypted = encrypt_token(&dek, "xoxb-test-garbage-token").expect("encrypt_token must succeed");
        // UPSERT, not INSERT. One test in this module already seeds an
        // account-wide `oauth_tokens` row to establish the workspace-
        // CONNECTING identity, with a literal `'unused-ciphertext'`
        // placeholder for `access_token` — which was fine while nothing
        // decrypted it and is not fine now that `ingest_slack_message`
        // authenticates with this row (#671). Replacing the ciphertext in
        // place both fixes that test and keeps this helper idempotent; the
        // conflict target is `uq_oauth_tokens_account_wide`, whose predicate
        // (`owner_user_id IS NULL`, control `0175`) must be restated for the
        // partial index to be matched.
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, provider_workspace_id) \
             VALUES ($1, 'slack', 'channels:history', $2, $3) \
             ON CONFLICT (account_id, provider, provider_workspace_id) WHERE owner_user_id IS NULL \
             DO UPDATE SET access_token = EXCLUDED.access_token",
        )
        .bind(account_id)
        .bind(encrypted)
        .bind(workspace_id)
        .execute(pool)
        .await
        .expect("seed slack bot token");
    }

    /// Build an `AppState` wired to a real DB/store, exactly the shape
    /// `routes::slack_interactivity::backfill_channel` builds for the same
    /// reason: `ingest_slack_message` bails out silently the moment
    /// `state.store` is absent. `http_client` carries a SHORT timeout — this
    /// test's bot token is garbage, so `resolve_channel_name`/
    /// `resolve_user_name`'s best-effort Slack calls must fail fast rather
    /// than hang, exactly like `verified_identity_matcher_pg_tests`'
    /// `confirmed_match_persists_and_is_read_back_on_second_call_pg` already
    /// relies on for the same reason.
    fn ingest_test_state(pool: &PgPool) -> Arc<crate::AppState> {
        let mut state = crate::test_app_state_owned(None);
        state.db = Some(pool.clone());
        state.store = Some(Arc::new(PostgresStore::new(pool.clone())) as Arc<dyn crate::store::Store>);
        state.http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        Arc::new(state)
    }

    /// Routing pin (ADR-0016 layer 2): an org channel with no
    /// `slack_watched_channels` row and no member mapped to a Slack identity
    /// has no destination, so the message lands in no ocean at all.
    #[tokio::test]
    async fn unmapped_org_channel_stores_nothing_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping unmapped_org_channel_stores_nothing_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_unmapped_nothing";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` serializes every `MASTER_KEK_HEX` mutation.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x33u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, Some("org_unmapped_nothing_test")).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        seed_slack_bot_token(&pool, account, "T1").await;
        let member = seed_user(&pool).await;
        add_member(&pool, account, member).await;
        seed_graph(&pool, account, "member", Some(member), true).await;

        ingest_slack_message(
            ingest_test_state(&pool), pool.clone(), pool.clone(),
            Arc::new(PostgresStore::new(pool.clone())), crate::tenancy::TenantId::Consumer,
            account, AccountPlane::Org, "T1".to_string(), "C1".to_string(), "U-AUTHOR".to_string(),
            "1700000000.000100".to_string(), None, "nowhere to land".to_string(), None, None,
        )
        .await
        .expect("an unroutable message is dropped, not an error");

        let (atoms, sources): (i64, i64) = sqlx::query_as(
            "SELECT (SELECT count(*) FROM atoms), (SELECT count(*) FROM source_items)",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!((atoms, sources), (0, 0), "no routing, nothing stored");

        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Routing pin (ADR-0016 layer 2): a `single` channel row lands the
    /// message in the ocean it names and nowhere else, not in the account's
    /// default ocean.
    #[tokio::test]
    async fn single_row_lands_only_in_its_ocean_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping single_row_lands_only_in_its_ocean_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_single_row_only";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` serializes every `MASTER_KEK_HEX` mutation.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x33u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        seed_slack_bot_token(&pool, account, "T1").await;
        let default_graph = match resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C-other")
            .await
            .unwrap()
        {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single default for a personal account, got {other:?}"),
        };
        let channel_graph = seed_graph(&pool, account, "personal", None, false).await;
        ensure_slack_watched_channel(
            &pool, &pool, AccountPlane::Personal,
            single_pick(account, owner, "T1", "C1", "general", channel_graph),
        )
        .await
        .unwrap();

        ingest_slack_message(
            ingest_test_state(&pool), pool.clone(), pool.clone(),
            Arc::new(PostgresStore::new(pool.clone())), crate::tenancy::TenantId::Consumer,
            account, AccountPlane::Personal, "T1".to_string(), "C1".to_string(), "U-AUTHOR".to_string(),
            "1700000000.000100".to_string(), None, "routed to its own ocean".to_string(), None, None,
        )
        .await
        .unwrap();

        assert_eq!(atom_count(&pool, channel_graph).await, 1, "the mapped ocean receives the message");
        assert_eq!(atom_count(&pool, default_graph).await, 0, "the account default receives nothing");

        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// The core untested claim: two org-fan-out destinations, both mapped via
    /// the C5 matcher's confirmed directory, both receive the SAME message as
    /// independent atoms. Then: a genuine per-destination SQL failure on ONE
    /// graph (a `CHECK` constraint standing in for "this destination's insert
    /// fails") must not prevent the OTHER destination from ingesting the next
    /// message — the loop body's restructuring into a fallible per-destination
    /// unit, exercised for the first time with N>1.
    ///
    /// FAILS WITHOUT THE FIX (reintroduce the old placeholder — `Vec::new()`
    /// for `graphs`, `total_members` for `unmapped` — and this test's very
    /// first assertion fails: `resolve_channel_destinations` would return
    /// `FanOut { graphs: vec![], .. }`, `ingest_slack_message` would see an
    /// empty destination set, post the "unmatched" notice, and return early
    /// with ZERO atoms in either graph).
    #[tokio::test]
    async fn org_fanout_ingest_two_destinations_and_survives_a_per_destination_failure_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_fanout_ingest_two_destinations_and_survives_a_per_destination_failure_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_fanout_two_dest";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        // Serialized against every other test in the crate that touches
        // `MASTER_KEK_HEX` (shared with `workers::huddle`/`workers::reembed`'s
        // own tests, all linked into the same `--lib` test binary) — held
        // across every `.await` below, not just around `set_var`.
        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the crate.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x33u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, Some("org_fanout_two_dest_test")).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        seed_slack_bot_token(&pool, account, "T1").await;

        let member_a = seed_user(&pool).await;
        add_member(&pool, account, member_a).await;
        let graph_a = seed_graph(&pool, account, "member", Some(member_a), true).await;
        seed_confirmed_identity(&pool, "T1", "U-A", member_a).await;

        let member_b = seed_user(&pool).await;
        add_member(&pool, account, member_b).await;
        let graph_b = seed_graph(&pool, account, "member", Some(member_b), true).await;
        seed_confirmed_identity(&pool, "T1", "U-B", member_b).await;

        // No explicit `slack_watched_channels` row for "C1" — this exercises
        // the account-wide FanOut default, not an explicit fan-out pick.
        let destination = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Org, "T1", "C1")
            .await
            .unwrap();
        match &destination {
            ChannelDestination::FanOut { graphs, unmapped_members, .. } => {
                let mut sorted = graphs.clone();
                sorted.sort();
                let mut expected = vec![graph_a, graph_b];
                expected.sort();
                assert_eq!(sorted, expected, "both confirmed members must be mapped destinations");
                assert_eq!(*unmapped_members, 1, "only the never-confirmed owner is unmapped");
            }
            other => panic!("expected FanOut with two mapped members, got {other:?}"),
        }

        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");

        // ── Happy path: one message, two destinations, one atom lands in each.
        ingest_slack_message(
            state.clone(),
            pool.clone(),
            pool.clone(),
            store.clone(),
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Org,
            "T1".to_string(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000000.000100".to_string(),
            None,
            "hello from the shared channel".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect("ingest_slack_message must not error even though this account is org-fanout");

        assert_eq!(atom_count(&pool, graph_a).await, 1, "graph A must receive its own atom");
        assert_eq!(atom_count(&pool, graph_b).await, 1, "graph B must receive its own atom");

        // ── Fault isolation: force a genuine per-destination SQL failure for
        // graph A specifically (a CHECK constraint standing in for "this
        // destination's insert fails at ingest time") and confirm graph B
        // still ingests the NEXT message. `resolve_org_fanout_destinations`
        // is, by design, too defensive to ever hand the loop a nonexistent
        // graph id on its own (see that function's doc comment) — this
        // constraint is what stands in for the TOCTOU-style failure the
        // restructured loop exists to survive.
        sqlx::query(&format!(
            "ALTER TABLE atoms ADD CONSTRAINT test_reject_graph_a CHECK (graph_id <> '{graph_a}') \
             NOT VALID"
        ))
        .execute(&pool)
        .await
        .unwrap();

        ingest_slack_message(
            state.clone(),
            pool.clone(),
            pool.clone(),
            store.clone(),
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Org,
            "T1".to_string(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000100.000200".to_string(),
            None,
            "second message, after graph A is poisoned".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect_err("retry the failed destination after completing all healthy destinations");

        assert_eq!(
            atom_count(&pool, graph_a).await,
            1,
            "graph A's poisoned insert must have failed and rolled back — still just the FIRST \
             message's atom, never a second"
        );
        assert_eq!(
            atom_count(&pool, graph_b).await,
            2,
            "graph B must ingest the second message even though graph A's insert failed — the \
             per-destination fault-isolation the loop was restructured to guarantee"
        );

        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A Slack message reaches every funnel stage through the shared core:
    /// a source version, a chunk linked to it, a fallback title and journal
    /// excerpt derived from the message BODY (never the "who in #where"
    /// header), and an embed job. Before the core, this writer inserted its
    /// own rows with no title or excerpt (2,203 NULL-titled Slack atoms in
    /// prod over 14 days).
    #[tokio::test]
    async fn slack_message_reaches_every_funnel_stage_with_a_body_title_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping slack_message_reaches_every_funnel_stage_with_a_body_title_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_funnel_stages";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` serializes every `MASTER_KEK_HEX` mutation.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x34u8; 32]));
        }
        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, Some("org_slack_funnel_stages")).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        seed_slack_bot_token(&pool, account, "T1").await;
        let member = seed_user(&pool).await;
        add_member(&pool, account, member).await;
        let graph = seed_graph(&pool, account, "member", Some(member), true).await;
        seed_confirmed_identity(&pool, "T1", "U-A", member).await;
        let state = ingest_test_state(&pool);
        let store = state.store.clone().unwrap();
        ingest_slack_message(
            state.clone(), pool.clone(), pool.clone(), store, crate::tenancy::TenantId::Consumer,
            account, AccountPlane::Org, "T1".into(), "C1".into(), "U-A".into(),
            "1700000000.000100".into(), None,
            "We decided to ship the search pilot on Friday. Morgan owns the rollout.".into(),
            None, None,
        )
        .await
        .unwrap();
        let (atom_id, title, title_kind, excerpt, text): (Uuid, Option<String>, Option<String>, Option<String>, Option<String>) =
            sqlx::query_as("SELECT atom_id,title,title_kind,excerpt,content_inline FROM atoms WHERE graph_id=$1")
                .bind(graph).fetch_one(&pool).await.unwrap();
        assert!(text.as_deref().is_some_and(|t| t.contains(" in #") && t.ends_with("Morgan owns the rollout.")),
            "the stored chunk keeps its historical inline header: {text:?}");
        assert_eq!(title.as_deref(), Some("We decided to ship the search pilot on Friday"), "title from the body");
        assert_eq!(title_kind.as_deref(), Some("fallback"));
        assert!(excerpt.as_deref().is_some_and(|e| e.starts_with("We decided")), "excerpt from the body: {excerpt:?}");
        let (linked, admitted): (bool, bool) = sqlx::query_as(
            "SELECT EXISTS(SELECT 1 FROM source_version_chunks WHERE graph_id=$1 AND atom_id=$2),
                    EXISTS(SELECT 1 FROM corpus_capture_admissions a JOIN source_versions v ON v.version_id=a.source_version_id WHERE v.graph_id=$1)")
            .bind(graph).bind(atom_id).fetch_one(&pool).await.unwrap();
        assert!(linked && admitted, "linked to its version, and the version is standing-admissible");
        let embed: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND payload->'atom_ids' ? $1")
            .bind(atom_id.to_string()).fetch_one(&pool).await.unwrap();
        assert_eq!(embed, 1, "embed job queued with the capture");
        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Journal completeness gap 1: `ingested_by` attribution ─────────────────
    //
    // The pre-existing pg fixtures throughout this file (and the fixtures in
    // `backend/tests/journal_rollups_pg.rs`) insert Slack atoms WITH
    // `ingested_by` set by hand, which is exactly why this bug — the real
    // WRITER (`ingest_slack_message`) always binding `NULL` — survived: those
    // fixtures prove the journal's filter, never the writer. The tests below
    // drive the real writer end to end and read the column back.

    async fn ingested_by_of(pool: &PgPool, graph_id: Uuid) -> Option<Uuid> {
        sqlx::query_scalar("SELECT ingested_by FROM atoms WHERE graph_id = $1 LIMIT 1")
            .bind(graph_id)
            .fetch_one(pool)
            .await
            .expect("exactly one atom expected")
    }

    /// A Slack author with a CONFIRMED `connector_identities` link gets their
    /// message's atom stamped `ingested_by` at ingest time — the fix this
    /// whole file's audit (gap 1) exists for: before it, this column was a
    /// literal `NULL` in the INSERT regardless of what the directory knew.
    #[tokio::test]
    async fn mapped_author_stamps_ingested_by_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping mapped_author_stamps_ingested_by_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_ingested_by_mapped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x44u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        seed_slack_bot_token(&pool, account, "T1").await;

        // The author is a DIFFERENT CoralSwarm user than the ocean owner —
        // proves attribution follows the message's AUTHOR, not the
        // destination graph's owner/recipient.
        let author = seed_user(&pool).await;
        seed_confirmed_identity(&pool, "T1", "U-AUTHOR", author).await;

        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");

        // No explicit `slack_watched_channels` row + personal account ⇒
        // `ChannelDestination::Single` (the account's own primary ocean).
        let destination = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap();
        let graph_id = match destination {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single destination for a personal account, got {other:?}"),
        };

        // Fail an actual capture write, then the embedding enqueue after
        // chunk/mapping writes. ExtractSourceAtoms is intentionally not queued
        // by an unowned capture; do not activate a managed purpose to inject it.
        // Both real failures must propagate and leave no partial destination.
        for (table, predicate) in [
            ("source_versions", "true"),
            ("jobs", "NEW.kind='EmbedAtoms'"),
        ] {
            sqlx::query(&format!("CREATE OR REPLACE FUNCTION fail_source_job() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF {predicate} THEN RAISE EXCEPTION 'injected source write failure'; END IF; RETURN NEW; END $$"))
                .execute(&pool).await.unwrap();
            sqlx::query(&format!("CREATE TRIGGER fail_source_job BEFORE INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION fail_source_job()"))
                .execute(&pool).await.unwrap();
        ingest_slack_message(
            state.clone(),
            pool.clone(),
            pool.clone(),
            store.clone(),
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Personal,
            "T1".to_string(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000200.000100".to_string(),
            None,
            "message from a mapped author".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect_err("fault injection must propagate failure");

            assert_eq!(atom_count(&pool,graph_id).await,0);
            let sources:i64=sqlx::query_scalar("SELECT count(*) FROM source_items WHERE graph_id=$1")
                .bind(graph_id).fetch_one(&pool).await.unwrap();
            assert_eq!(sources,0,"failed capture/chunk/job transaction must roll back");
            let partials: (i64, i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM source_versions WHERE graph_id=$1), \
                 (SELECT count(*) FROM source_version_chunks WHERE graph_id=$1), \
                 (SELECT count(*) FROM corpus_capture_admissions WHERE graph_id=$1), \
                 (SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND account_id=$2)",
            ).bind(graph_id).bind(account).fetch_one(&pool).await.unwrap();
            assert_eq!(partials, (0, 0, 0, 0), "failed capture or embedding enqueue must leave no version, mapping, admission or embedding job");
            sqlx::query(&format!("DROP TRIGGER fail_source_job ON {table}")).execute(&pool).await.unwrap();
        }
        ingest_slack_message(
            state,
            pool.clone(),
            pool.clone(),
            store,
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Personal,
            "T1".to_string(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000200.000100".to_string(),
            None,
            "message from a mapped author".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect("ingest_slack_message must succeed");

        assert_eq!(atom_count(&pool, graph_id).await, 1, "one atom must land");
        assert_eq!(
            ingested_by_of(&pool, graph_id).await,
            Some(author),
            "a mapped author's message must stamp ingested_by with the confirmed CoralSwarm user, \
             not leave it NULL"
        );

        let source:(String,String,Option<Uuid>,String)=sqlx::query_as(
            "SELECT s.provider_namespace,v.raw_inline,v.ingested_by,v.representation FROM source_items s \
             JOIN source_versions v ON v.version_id=s.current_version_id AND v.graph_id=s.graph_id WHERE s.graph_id=$1",
        ).bind(graph_id).fetch_one(&pool).await.unwrap();
        assert_eq!(source.0,"slack:workspace:T1");
        assert_eq!(source.1,"message from a mapped author");
        assert_eq!(source.2,Some(author));
        assert_eq!(source.3,"normalized_text");
        let mapped:i64=sqlx::query_scalar("SELECT count(*) FROM source_version_chunks WHERE graph_id=$1")
            .bind(graph_id).fetch_one(&pool).await.unwrap();
        assert_eq!(mapped,1,"the successful retry must link its readable chunk");
        let admission: (String, String, Option<String>, i64) = sqlx::query_as(
            "SELECT v.extraction_status,a.state,a.reason, \
             (SELECT count(*) FROM jobs WHERE kind='ExtractSourceAtoms' AND account_id=$2) \
             FROM source_versions v JOIN corpus_capture_admissions a \
             ON a.source_version_id=v.version_id AND a.graph_id=v.graph_id WHERE v.graph_id=$1",
        ).bind(graph_id).bind(account).fetch_one(&pool).await.unwrap();
        assert_eq!(admission, ("blocked".to_string(), "blocked".to_string(),
            Some("managed_admission_required".to_string()), 0),
            "retaining the mapped author's message must not authorize managed extraction");
        let embed:i64=sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND account_id=$1")
            .bind(account).fetch_one(&pool).await.unwrap();
        assert_eq!(embed,1);


        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// An author with NO confirmed `connector_identities` link leaves
    /// `ingested_by = NULL` — the accepted, documented degradation (never
    /// guessed from a display name) — but ingest must still succeed and the
    /// atom must still land.
    #[tokio::test]
    async fn unmapped_author_leaves_ingested_by_null_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping unmapped_author_leaves_ingested_by_null_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_ingested_by_unmapped";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x55u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        seed_slack_bot_token(&pool, account, "T1").await;
        // Deliberately no `seed_confirmed_identity` call for "U-STRANGER".

        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");
        let destination = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap();
        let graph_id = match destination {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single destination for a personal account, got {other:?}"),
        };

        ingest_slack_message(
            state,
            pool.clone(),
            pool.clone(),
            store,
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Personal,
            "T1".to_string(),
            "C1".to_string(),
            "U-STRANGER".to_string(),
            "1700000300.000100".to_string(),
            None,
            "message from an unmapped author".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect("ingest_slack_message must succeed even for an unmapped author");

        assert_eq!(
            atom_count(&pool, graph_id).await,
            1,
            "the atom must still land — an unmapped author is never a reason to drop content"
        );
        assert_eq!(
            ingested_by_of(&pool, graph_id).await,
            None,
            "an unmapped author must leave ingested_by NULL, never guessed"
        );

        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Re-capturing the SAME message (same channel + ts): unchanged text is
    /// adopted by hash (no new atom, no content write) and linked to the
    /// current version; edited text replaces the one chunk in place and the
    /// new version links it. Before content-verified ingest the edit kept the
    /// old text and the new version had no chunk row at all.
    #[tokio::test]
    async fn same_message_recapture_adopts_or_replaces_its_chunk_pg() {
        let Some(base) = base_url() else {
            eprintln!("skipping same_message_recapture_adopts_or_replaces_its_chunk_pg: set ORGT_TEST_BASE_URL");
            return;
        };
        let db = "orgt_slack_recapture_adopt";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();
        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x56u8; 32]));
        }
        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        seed_slack_bot_token(&pool, account, "T1").await;
        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");
        let graph_id = match resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap()
        {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single destination, got {other:?}"),
        };
        let capture = |text: &str| {
            ingest_slack_message(
                state.clone(), pool.clone(), pool.clone(), store.clone(),
                crate::tenancy::TenantId::Consumer, account, AccountPlane::Personal,
                "T1".to_string(), "C1".to_string(), "U-ANY".to_string(),
                "1700000400.000100".to_string(), None, text.to_string(), None, None,
            )
        };
        type Row = (Uuid, Option<String>, Vec<String>);
        let chunk_row = || async {
            sqlx::query_as::<_, Row>("SELECT atom_id,content_inline,superseded_blob_keys FROM atoms WHERE graph_id=$1")
                .bind(graph_id).fetch_one(&pool).await.unwrap()
        };
        let current_links = || async {
            sqlx::query_scalar::<_, Uuid>(
                "SELECT c.atom_id FROM source_items s JOIN source_version_chunks c \
                   ON c.graph_id=s.graph_id AND c.source_version_id=s.current_version_id WHERE s.graph_id=$1",
            ).bind(graph_id).fetch_all(&pool).await.unwrap()
        };
        let versions = || async {
            sqlx::query_scalar::<_, i64>("SELECT count(*) FROM source_versions WHERE graph_id=$1")
                .bind(graph_id).fetch_one(&pool).await.unwrap()
        };

        capture("ship it on friday").await.expect("first capture");
        let first = chunk_row().await;
        assert_eq!(current_links().await, vec![first.0]);

        capture("ship it on friday").await.expect("unchanged re-capture");
        assert_eq!(atom_count(&pool, graph_id).await, 1);
        assert_eq!(chunk_row().await, first, "unchanged text is adopted with no write");
        assert_eq!(versions().await, 1);

        capture("ship it on monday instead").await.expect("edited re-capture");
        assert_eq!(atom_count(&pool, graph_id).await, 1, "no second atom row for the same message");
        let edited = chunk_row().await;
        assert_eq!(edited.0, first.0, "the chunk keeps its identity");
        assert!(edited.1.as_deref().unwrap_or("").ends_with("\n\nship it on monday instead"), "{edited:?}");
        assert_eq!(edited.2.len(), 1, "the old locator is parked for cleanup");
        assert_eq!(versions().await, 2);
        assert_eq!(current_links().await, vec![first.0], "the new version links the replaced chunk");
        let embeds: i64 = sqlx::query_scalar("SELECT count(*) FROM jobs WHERE kind='EmbedAtoms' AND account_id=$1")
            .bind(account).fetch_one(&pool).await.unwrap();
        assert_eq!(embeds, 2, "first capture and the edit embed; the unchanged re-capture does not");

        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);
        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// Org fan-out: the SAME mapped author's message lands in BOTH
    /// destination graphs (one per mapped account member), and BOTH copies
    /// carry the AUTHOR's `ingested_by` — proving attribution is keyed off
    /// the message's author (resolved once, graph-independent), not off
    /// either destination graph's own owner/recipient.
    #[tokio::test]
    async fn org_fanout_stamps_the_same_author_on_every_destination_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping org_fanout_stamps_the_same_author_on_every_destination_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_ingested_by_fanout";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x66u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, Some("org_fanout_ingested_by_test")).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        seed_slack_bot_token(&pool, account, "T1").await;

        let member_a = seed_user(&pool).await;
        add_member(&pool, account, member_a).await;
        let graph_a = seed_graph(&pool, account, "member", Some(member_a), true).await;
        seed_confirmed_identity(&pool, "T1", "U-A", member_a).await;

        let member_b = seed_user(&pool).await;
        add_member(&pool, account, member_b).await;
        let graph_b = seed_graph(&pool, account, "member", Some(member_b), true).await;
        seed_confirmed_identity(&pool, "T1", "U-B", member_b).await;

        // The AUTHOR is member_a — deliberately reusing an already-mapped
        // member's Slack id rather than inventing a third, so the assertion
        // below can only pass if attribution follows the author and not
        // "whichever graph this copy happens to be landing in."
        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");
        ingest_slack_message(
            state,
            pool.clone(),
            pool.clone(),
            store,
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Org,
            "T1".to_string(),
            "C1".to_string(),
            "U-A".to_string(),
            "1700000400.000100".to_string(),
            None,
            "hello from member A, seen by both oceans".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect("ingest_slack_message must succeed across a fan-out");

        assert_eq!(
            ingested_by_of(&pool, graph_a).await,
            Some(member_a),
            "graph A's copy must be attributed to the author (member_a)"
        );
        assert_eq!(
            ingested_by_of(&pool, graph_b).await,
            Some(member_a),
            "graph B's copy — a DIFFERENT ocean, owned by member_b — must ALSO be attributed to \
             the author (member_a), not to member_b just because it's member_b's own ocean"
        );

        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    /// A TRANSIENT failure in the attribution lookup (stood in for here by
    /// dropping `connector_identities` out from under the query — any error
    /// `resolve_linked_user_directory_only` can return degrades identically)
    /// must never drop the whole message. This is the CodeRabbit-flagged
    /// regression this test guards: attribution used `?` and propagated the
    /// lookup's `Err` out of `ingest_slack_message` entirely, so a blip on
    /// this one query lost the MESSAGE, not just its attribution.
    #[tokio::test]
    async fn attribution_lookup_failure_is_best_effort_and_still_ingests_the_message_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping attribution_lookup_failure_is_best_effort_and_still_ingests_the_message_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let db = "orgt_slack_ingested_by_lookup_err";
        let _cleanup_db = crate::test_db::TestDatabase::adopt_from_url(&base, db);
        let mut admin_conn = recreate(&base, db).await;
        let pool: PgPool = PgPoolOptions::new().connect(&with_db(&base, db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&pool).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x88u8; 32]));
        }

        let owner = seed_user(&pool).await;
        let account = seed_account(&pool, owner, None).await;
        seed_dek(&pool, account).await;
        seed_slack_integration(&pool, account, "T1").await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        seed_slack_bot_token(&pool, account, "T1").await;

        let state = ingest_test_state(&pool);
        let store = state.store.clone().expect("ingest_test_state always sets store");
        let destination = resolve_channel_destinations(&pool, &pool, account, AccountPlane::Personal, "T1", "C1")
            .await
            .unwrap();
        let graph_id = match destination {
            ChannelDestination::Single(g) => g,
            other => panic!("expected a Single destination for a personal account, got {other:?}"),
        };

        // Force the attribution lookup's query to fail with a genuine
        // Postgres error — cheapest available stand-in for "any transient DB
        // error mid-query": the table it selects from no longer exists.
        sqlx::query("DROP TABLE connector_identities")
            .execute(&pool)
            .await
            .unwrap();

        ingest_slack_message(
            state,
            pool.clone(),
            pool.clone(),
            store,
            crate::tenancy::TenantId::Consumer,
            account,
            AccountPlane::Personal,
            "T1".to_string(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000500.000100".to_string(),
            None,
            "message ingested despite a broken attribution lookup".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect(
            "a failed attribution lookup must never propagate out of ingest_slack_message — the \
             message must still land",
        );

        assert_eq!(
            atom_count(&pool, graph_id).await,
            1,
            "the message must still be ingested even though attribution lookup errored"
        );
        assert_eq!(
            ingested_by_of(&pool, graph_id).await,
            None,
            "a failed attribution lookup must degrade to ingested_by = NULL, not panic/propagate"
        );

        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        pool.close().await;
        admin_conn.execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str()).await.ok();
    }

    // ── Org-slack step 5a: control-only reads, genuinely two-pool ────────────
    //
    // Every `ingest_slack_message` test above this point — including the
    // org-fanout ones — migrates ONE database from `migrations_control` and
    // passes that SAME pool as both `control_pool` and `pool`. That proves
    // the fan-out/attribution LOGIC, but none of them would have caught (or
    // would catch a regression of) a control-vs-content plane-mixing bug,
    // because none of them ever runs `ingest_slack_message` against two
    // genuinely separate databases. The three tests below close that gap.

    /// Seed an `oauth_tokens` row shaped like the control-plane-only OAuth
    /// callback (`routes::oauth::callback`) actually writes one — used here
    /// only for `provider_user_id` (the workspace-connecting Slack user,
    /// read by `ingest_slack_message`'s owner-authored self-reef check).
    async fn seed_oauth_token(pool: &PgPool, account_id: Uuid, workspace_id: &str, provider_user_id: &str) {
        sqlx::query(
            "INSERT INTO oauth_tokens(account_id, provider, scope, access_token, \
                 provider_user_id, provider_workspace_id) \
             VALUES ($1, 'slack', 'chat:write', 'unused-ciphertext', $2, $3)",
        )
        .bind(account_id)
        .bind(provider_user_id)
        .bind(workspace_id)
        .execute(pool)
        .await
        .unwrap();
    }

    /// Seed the tenant plane's own DEK (`account_keys.dek_wrapped`) — the
    /// `TenantId::Org` half of `crypto::resolve_dek_for_tenant`, which never
    /// reads the control plane's `accounts.dek_wrapped` (see that function's
    /// own doc comment). Mirrors `tests/dek_relocation_pg.rs`'s own seeding.
    async fn seed_tenant_dek(tenant_pool: &PgPool, account_id: Uuid) {
        use coralswarm_core::kek::{generate_dek, load_kek, wrap_dek};
        let kek = load_kek().expect("MASTER_KEK_HEX must be set before calling seed_tenant_dek");
        let wrapped = wrap_dek(&kek, &generate_dek()).expect("wrap_dek must succeed with a valid KEK");
        sqlx::query("INSERT INTO account_keys (account_id, dek_wrapped) VALUES ($1, $2)")
            .bind(account_id)
            .bind(&wrapped)
            .execute(tenant_pool)
            .await
            .expect("seed tenant account_keys");
    }

    /// The decisive proof for org-slack step 5a: `slack_integrations` is
    /// seeded EXACTLY where production writes it — on the CONTROL database,
    /// via the control-plane-only OAuth callback (never on the tenant plane)
    /// — and `ingest_slack_message` still completes end to end against a
    /// genuine, SEPARATE org TENANT database for the message's actual
    /// content, landing the atom there and nowhere else.
    ///
    /// The message's author is ALSO seeded as the workspace-connecting
    /// owner (`oauth_tokens.provider_user_id` on CONTROL), so this single
    /// run exercises every control-only read this function makes:
    /// `slack_integrations` (bot token / workspace name), `oauth_tokens`
    /// (owner detection), and `accounts.owner_user_id` (the self-reef bind)
    /// — all three now correctly on `control_pool`, not `pool`.
    ///
    /// FAILS WITHOUT THE FIX: before `slack_integrations` was read from
    /// `control_pool`, this read ran on `pool` (the tenant/content pool),
    /// which never carries this row for a genuine org tenant (it is written
    /// ONLY on control) — `ingest_slack_message` took the "no integration
    /// found for Slack workspace" arm and returned `Ok(())` with ZERO atoms
    /// written, exactly as it silently did in production for every org
    /// account. Restoring only that one read (without also moving the
    /// `oauth_tokens`/`accounts` reads) would still land the message but
    /// leave `graphs.self_node_id` NULL — this test's last assertion pins
    /// that too.
    #[tokio::test]
    async fn ingest_slack_message_across_two_genuinely_separate_planes_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping ingest_slack_message_across_two_genuinely_separate_planes_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = (
            "orgt_slack_ingest_two_plane_ctl",
            "orgt_slack_ingest_two_plane_tnt",
        );
        let _database_cleanup_14 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_15 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        let tnt_admin = recreate(&base, tnt_db).await;
        tnt_admin.close().await.ok();

        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the crate.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x55u8; 32]));
        }

        let org_id = format!("org_ingest_two_plane_{}", Uuid::new_v4());
        let team_id = format!("T-two-plane-{}", Uuid::new_v4());

        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;
        // CONTROL only — the exact shape production writes (the OAuth
        // callback and this test's own author, "U-AUTHOR", are the SAME
        // Slack user: the workspace-connecting owner).
        seed_slack_integration(&control, account, &team_id).await;
        // ORDER MATTERS. `seed_oauth_token` writes the workspace-CONNECTING
        // identity into the same account-wide row, with a placeholder
        // ciphertext; `seed_slack_bot_token` then upserts real ciphertext over
        // it. Reversed, the identity insert collides on
        // `uq_oauth_tokens_account_wide` instead.
        seed_oauth_token(&control, account, &team_id, "U-AUTHOR").await;

        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;
        seed_tenant_dek(&tenant, account).await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Without this the ingest below takes
        // the no-token arm and writes nothing while still returning `Ok(())`.
        //
        // Seeded on the ORG's own plane (org-plane DEK fix) — the row on
        // CONTROL, the key from the TENANT `account_keys` seeded just above,
        // exactly as `routes::oauth::callback` writes it for an org account.
        // `seed_tenant_dek` mints a key unrelated to any control DEK, so a
        // regression that reads the bot token on the consumer plane fails
        // here rather than silently ingesting nothing.
        seed_slack_bot_token_on_plane(
            &control,
            &crate::crypto::DekPlane::org(org_id.clone(), tenant.clone(), account),
            account,
            &team_id,
        )
        .await;

        // TENANT: the destination graph and its explicit `slack_watched_
        // channels` row — a `Single` pick sidesteps org fan-out's
        // `account_members`/`connector_identities` machinery entirely, which
        // is not what this test exists to prove (`org_fanout_ingest_two_
        // destinations_and_survives_a_per_destination_failure_pg` above
        // already covers that, single-pool).
        let graph_id = seed_graph(&tenant, account, "member", Some(owner), true).await;
        ensure_slack_watched_channel(
            &control,
            &tenant,
            AccountPlane::Org,
            single_pick(account, owner, &team_id, "C1", "general", graph_id),
        )
        .await
        .unwrap();

        let state = ingest_test_state(&control);
        let tenant_store: Arc<dyn crate::store::Store> =
            Arc::new(PostgresStore::new(tenant.clone()));

        ingest_slack_message(
            state,
            control.clone(),
            tenant.clone(),
            tenant_store,
            crate::tenancy::TenantId::Org(org_id.clone()),
            account,
            AccountPlane::Org,
            team_id.clone(),
            "C1".to_string(),
            "U-AUTHOR".to_string(),
            "1700000900.000100".to_string(),
            None,
            "hello from a genuine two-plane org tenant".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect(
            "ingest_slack_message must succeed reading slack_integrations/oauth_tokens/accounts \
             from CONTROL against a genuine org tenant content pool",
        );

        assert_eq!(
            atom_count(&tenant, graph_id).await,
            1,
            "the atom must land on the TENANT plane — this is the exact write that silently \
             never happened before the control-pool fix"
        );
        assert_eq!(
            atom_count(&control, graph_id).await,
            0,
            "the atom must never ALSO land on control — `pool`/`store` are the tenant pool only"
        );

        let self_node_id: Option<Uuid> =
            sqlx::query_scalar("SELECT self_node_id FROM graphs WHERE id = $1")
                .bind(graph_id)
                .fetch_one(&tenant)
                .await
                .unwrap();
        assert!(
            self_node_id.is_some(),
            "the owner-authored self-reef must be bound on the TENANT graph — this is the \
             `accounts.owner_user_id` control-only read's own regression guard"
        );

        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// The `slack_integrations` half of the regression in isolation, with no
    /// owner/self-reef machinery involved: an org account whose author is
    /// NOT the workspace owner still gets its message ingested onto the
    /// tenant plane. Narrower than the test above — if a future change
    /// re-breaks JUST the `slack_integrations` read (e.g. a careless
    /// find-replace back to `pool`), this fails without needing the
    /// owner/DEK/self-reef setup to explain why.
    ///
    /// FAILS WITHOUT THE FIX: same "no integration found" early return as
    /// the test above.
    #[tokio::test]
    async fn ingest_slack_message_finds_slack_integrations_on_control_not_content_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping ingest_slack_message_finds_slack_integrations_on_control_not_content_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = (
            "orgt_slack_ingest_ctrl_only_ctl",
            "orgt_slack_ingest_ctrl_only_tnt",
        );
        let _database_cleanup_16 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_17 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        let tnt_admin = recreate(&base, tnt_db).await;
        tnt_admin.close().await.ok();

        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let _kek_guard = crate::workers::reembed::MASTER_KEK_HEX_ENV_LOCK.lock().await;
        let prior_kek = std::env::var("MASTER_KEK_HEX").ok();
        // SAFETY: `_kek_guard` (held for this test's entire body) serializes
        // every `MASTER_KEK_HEX` mutation against any other test in the crate.
        unsafe {
            std::env::set_var("MASTER_KEK_HEX", hex::encode([0x66u8; 32]));
        }

        let org_id = format!("org_ingest_ctrl_only_{}", Uuid::new_v4());
        let team_id = format!("T-ctrl-only-{}", Uuid::new_v4());

        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;
        seed_slack_integration(&control, account, &team_id).await;
        // Deliberately no oauth_tokens row FOR THE AUTHOR — they are not the
        // owner, so the self-reef path never engages. (The account-wide BOT
        // token seeded below is a different row entirely: `owner_user_id IS
        // NULL`, keyed on `provider_workspace_id`, and it is what
        // `ingest_slack_message` authenticates with.)

        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;
        seed_tenant_dek(&tenant, account).await;
        // #741: the bot token now lives ENCRYPTED in `oauth_tokens`, not in
        // `slack_integrations.bot_token`. Seeded on the ORG's own plane —
        // see `seed_slack_bot_token_on_plane`'s own doc comment.
        seed_slack_bot_token_on_plane(
            &control,
            &crate::crypto::DekPlane::org(org_id.clone(), tenant.clone(), account),
            account,
            &team_id,
        )
        .await;

        let graph_id = seed_graph(&tenant, account, "member", Some(owner), true).await;
        ensure_slack_watched_channel(
            &control,
            &tenant,
            AccountPlane::Org,
            single_pick(account, owner, &team_id, "C1", "general", graph_id),
        )
        .await
        .unwrap();

        let state = ingest_test_state(&control);
        let tenant_store: Arc<dyn crate::store::Store> =
            Arc::new(PostgresStore::new(tenant.clone()));

        ingest_slack_message(
            state,
            control.clone(),
            tenant.clone(),
            tenant_store,
            crate::tenancy::TenantId::Org(org_id.clone()),
            account,
            AccountPlane::Org,
            team_id.clone(),
            "C1".to_string(),
            "U-NOT-THE-OWNER".to_string(),
            "1700001000.000100".to_string(),
            None,
            "a non-owner author's message must still land on the tenant plane".to_string(),
            // Not a bot message.
            None,
            None,
        )
        .await
        .expect("ingest_slack_message must find slack_integrations on the control pool");

        assert_eq!(
            atom_count(&tenant, graph_id).await,
            1,
            "the message must be ingested — a missing slack_integrations lookup previously \
             discarded it silently"
        );

        // SAFETY: still under `_kek_guard`.
        unsafe {
            match &prior_kek {
                Some(v) => std::env::set_var("MASTER_KEK_HEX", v),
                None => std::env::remove_var("MASTER_KEK_HEX"),
            }
        }
        drop(_kek_guard);

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    // ── Org governance decision (2026-08-04), piece 3: shared disconnect purge
    // ── and app_uninstalled/tokens_revoked full cleanup ─────────────────────
    //
    // Reuses this module's existing two-plane harness (`seed_user`/
    // `seed_account`/`mint_tenant_databases_row`/`build_tenant_pool`) rather
    // than inventing a third — same reason every other org-plane test in this
    // file does.

    async fn seed_team(pool: &PgPool, account_id: Uuid, graph_id: Uuid, created_by: Uuid) -> Uuid {
        sqlx::query_scalar(
            "INSERT INTO teams(account_id, graph_id, name, created_by) \
             VALUES ($1, $2, 'Test Team', $3) RETURNING id",
        )
        .bind(account_id)
        .bind(graph_id)
        .bind(created_by)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn seed_team_member(pool: &PgPool, team_id: Uuid, user_id: Uuid, role: &str) {
        sqlx::query("INSERT INTO team_members(team_id, user_id, role) VALUES ($1, $2, $3)")
            .bind(team_id)
            .bind(user_id)
            .bind(role)
            .execute(pool)
            .await
            .unwrap();
    }

    async fn seed_seat_grant(pool: &PgPool, account_id: Uuid, team_id: Uuid, user_id: Uuid, source: &str) {
        sqlx::query(
            "INSERT INTO team_seat_grants(account_id, team_id, user_id, source) VALUES ($1, $2, $3, $4)",
        )
        .bind(account_id)
        .bind(team_id)
        .bind(user_id)
        .bind(source)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn seat_grant_exists(pool: &PgPool, team_id: Uuid, user_id: Uuid, source: &str) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM team_seat_grants \
             WHERE team_id = $1 AND user_id = $2 AND source = $3)",
        )
        .bind(team_id)
        .bind(user_id)
        .bind(source)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn team_member_departing_at(
        pool: &PgPool,
        team_id: Uuid,
        user_id: Uuid,
    ) -> Option<chrono::DateTime<chrono::Utc>> {
        sqlx::query_scalar("SELECT departing_at FROM team_members WHERE team_id = $1 AND user_id = $2")
            .bind(team_id)
            .bind(user_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn watched_channel_count(pool: &PgPool, account_id: Uuid, workspace_id: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM slack_watched_channels WHERE account_id = $1 AND workspace_id = $2",
        )
        .bind(account_id)
        .bind(workspace_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    async fn connector_identity_exists(pool: &PgPool, workspace_id: &str, external_user_id: &str) -> bool {
        sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM connector_identities \
             WHERE workspace_id = $1 AND external_user_id = $2)",
        )
        .bind(workspace_id)
        .bind(external_user_id)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    /// THE seat-revocation safety property, proved directly against
    /// [`crate::connectors::slack_disconnect::purge_slack_workspace`]:
    ///
    /// * a seat held ONLY via a `slack:{channel}`-sourced grant for a
    ///   disconnected channel is REVOKED (`team_members.departing_at` set);
    /// * a HAND-GRANTED (`'preexisting'`-sourced) seat, on an account that
    ///   ALSO happens to hold a `slack:{channel}` grant on the SAME channel
    ///   being purged, is NEVER examined at all — proving the isolation is at
    ///   the SQL `source = ANY(...)` level, not merely `revoke_seat_grant`'s
    ///   own DELETE-row-count gate (already proved elsewhere; this test is
    ///   about THIS module's own source-selection, not re-proving that one);
    /// * an ADMIN's seat, granted via the SAME `slack:{channel}` source as
    ///   the member who gets revoked, is NEVER revoked — the grant row is
    ///   still deleted (provenance is gone either way), but
    ///   `team_members.departing_at` for the admin stays `NULL`.
    ///
    /// `bot_token: None` — no live Slack call is exercised here; the
    /// `conversations.leave` best-effort step is covered separately by the
    /// production code path's own error handling (never a DB assertion).
    #[tokio::test]
    async fn purge_slack_workspace_revokes_only_channel_sourced_grants_and_preserves_hand_granted_and_admin_seats_pg(
    ) {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping purge_slack_workspace_revokes_only_channel_sourced_grants_and_preserves_hand_granted_and_admin_seats_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_slack_purge_seats_ctl", "orgt_slack_purge_seats_tnt");
        let _database_cleanup_18 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_19 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        {
            let conn = recreate(&base, tnt_db).await;
            conn.close().await.ok();
        }
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_purge_seats_{}", Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;

        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let team_graph = seed_graph(&tenant, account, "team", None, false).await;
        let team_id = seed_team(&tenant, account, team_graph, owner).await;

        let workspace_id = format!("T-purge-{}", Uuid::new_v4());
        let channel_id = "C-PURGE-1";
        let source = crate::seats::slack_channel_source(channel_id);

        // The channel being purged, routed into the team ocean — this is
        // what makes `slack:C-PURGE-1` a source this module's own purge is
        // responsible for.
        ensure_slack_watched_channel(
            &control,
            &tenant,
            AccountPlane::Org,
            single_pick(account, owner, &workspace_id, channel_id, "purged-channel", team_graph),
        )
        .await
        .unwrap();

        // Member A: seated ONLY via the channel being purged — must be revoked.
        let member_a = Uuid::new_v4();
        seed_team_member(&tenant, team_id, member_a, "member").await;
        seed_seat_grant(&tenant, account, team_id, member_a, &source).await;

        // Member B: seated via the SAME channel source AND a hand-granted
        // ('preexisting') reason — the grant this purge touches is deleted,
        // but the OTHER grant (never a source this purge computes) must
        // survive untouched, and the seat itself must stand (matches
        // `crate::seats::RevokeOutcome::GrantDropped`).
        let member_b = Uuid::new_v4();
        seed_team_member(&tenant, team_id, member_b, "member").await;
        seed_seat_grant(&tenant, account, team_id, member_b, &source).await;
        seed_seat_grant(&tenant, account, team_id, member_b, crate::seats::SOURCE_PREEXISTING).await;

        // The admin: seated via the SAME channel source as member A — the
        // grant row is deleted (provenance is genuinely gone), but
        // auto-seating never grants 'admin', so it must never TAKE one away.
        let admin_user = Uuid::new_v4();
        seed_team_member(&tenant, team_id, admin_user, "admin").await;
        seed_seat_grant(&tenant, account, team_id, admin_user, &source).await;

        let outcome = crate::connectors::slack_disconnect::purge_slack_workspace(
            &reqwest::Client::new(),
            &tenant,
            &tenant,
            account,
            &workspace_id,
            None,
        )
        .await;

        assert_eq!(outcome.watched_channels_deleted, 1);
        assert_eq!(
            outcome.seats_revoked, 1,
            "exactly one seat (member A's) must have been genuinely revoked"
        );

        assert_eq!(
            watched_channel_count(&tenant, account, &workspace_id).await,
            0,
            "the watched-channel row must be gone"
        );

        // Member A: grant gone, seat revoked.
        assert!(
            !seat_grant_exists(&tenant, team_id, member_a, &source).await,
            "member A's channel-sourced grant must be deleted"
        );
        assert!(
            team_member_departing_at(&tenant, team_id, member_a).await.is_some(),
            "member A's seat must be revoked — it had no other reason to hold it"
        );

        // Member B: channel-sourced grant gone, hand-granted one UNTOUCHED,
        // seat stands.
        assert!(
            !seat_grant_exists(&tenant, team_id, member_b, &source).await,
            "member B's channel-sourced grant must be deleted"
        );
        assert!(
            seat_grant_exists(&tenant, team_id, member_b, crate::seats::SOURCE_PREEXISTING).await,
            "member B's HAND-GRANTED seat must survive untouched — this purge must never delete \
             a grant it did not itself compute a source for"
        );
        assert!(
            team_member_departing_at(&tenant, team_id, member_b).await.is_none(),
            "member B's seat must still stand — a grant survives"
        );

        // Admin: grant gone (provenance genuinely gone), seat NEVER revoked.
        assert!(
            !seat_grant_exists(&tenant, team_id, admin_user, &source).await,
            "the admin's channel-sourced grant row is still deleted — losing provenance is fine"
        );
        assert!(
            team_member_departing_at(&tenant, team_id, admin_user).await.is_none(),
            "an admin's seat must NEVER be revoked by auto-seating cleanup, regardless of grants"
        );

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// Disconnect withdraws consent to keep CAPTURING — it is not an erasure
    /// request. Proves `purge_slack_workspace` leaves `connector_identities`
    /// and every already-ingested atom exactly as they were, even though the
    /// channel that produced them is being purged.
    #[tokio::test]
    async fn purge_slack_workspace_leaves_connector_identities_and_atoms_untouched_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping purge_slack_workspace_leaves_connector_identities_and_atoms_untouched_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_slack_purge_identity_ctl", "orgt_slack_purge_identity_tnt");
        let _database_cleanup_20 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_21 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        {
            let conn = recreate(&base, tnt_db).await;
            conn.close().await.ok();
        }
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_purge_identity_{}", Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;

        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let dest_graph = seed_graph(&tenant, account, "member", Some(owner), true).await;
        let workspace_id = format!("T-purge-id-{}", Uuid::new_v4());
        let channel_id = "C-PURGE-ID-1";

        ensure_slack_watched_channel(
            &control,
            &tenant,
            AccountPlane::Org,
            single_pick(account, owner, &workspace_id, channel_id, "purged-channel", dest_graph),
        )
        .await
        .unwrap();

        // Content this purge must never touch: a confirmed identity link and
        // an already-ingested atom in the destination ocean.
        seed_confirmed_identity(&tenant, &workspace_id, "U-AUTHOR", owner).await;
        sqlx::query(
            "INSERT INTO atoms(atom_id, graph_id, source_type, source_id, content_blob_key, \"timestamp\") \
             VALUES (gen_random_uuid(), $1, 'slack_message', 'purge-test-msg-1', 'blob-key', now())",
        )
        .bind(dest_graph)
        .execute(&tenant)
        .await
        .unwrap();

        assert_eq!(atom_count(&tenant, dest_graph).await, 1, "sanity: the atom seeded");
        assert!(
            connector_identity_exists(&tenant, &workspace_id, "U-AUTHOR").await,
            "sanity: the identity link seeded"
        );

        let outcome = crate::connectors::slack_disconnect::purge_slack_workspace(
            &reqwest::Client::new(),
            &tenant,
            &tenant,
            account,
            &workspace_id,
            None,
        )
        .await;
        assert_eq!(outcome.watched_channels_deleted, 1);

        assert_eq!(
            atom_count(&tenant, dest_graph).await,
            1,
            "disconnect must never delete already-captured atoms"
        );
        assert!(
            connector_identity_exists(&tenant, &workspace_id, "U-AUTHOR").await,
            "disconnect must never delete connector_identities — it withdraws consent to \
             capture, it is not an erasure request"
        );

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// End-to-end `app_uninstalled` (org governance decision 2026-08-04,
    /// piece 3): `handle_slack_full_disconnect` must (a) purge the
    /// watched-channel/seat state on the TENANT plane exactly like a manual
    /// disconnect, AND (b) additionally remove the CONTROL-plane
    /// `oauth_tokens`/`slack_integrations`/`connector_installs` rows — the
    /// "full disconnect equivalent" a Slack-initiated uninstall needs that a
    /// manual disconnect's own caller already owns doing for itself.
    #[tokio::test]
    async fn handle_slack_full_disconnect_removes_token_rows_and_purges_tenant_state_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping handle_slack_full_disconnect_removes_token_rows_and_purges_tenant_state_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt_db) = ("orgt_slack_full_disconnect_ctl", "orgt_slack_full_disconnect_tnt");
        let _database_cleanup_22 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let _database_cleanup_23 = crate::test_db::TestDatabase::adopt_from_url(&base, tnt_db);
        {
            let conn = recreate(&base, tnt_db).await;
            conn.close().await.ok();
        }
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        let org_id = format!("org_full_disconnect_{}", Uuid::new_v4());
        let team_id = format!("T-full-disconnect-{}", Uuid::new_v4());
        let owner = seed_user(&control).await;
        let account = seed_account(&control, owner, Some(&org_id)).await;

        // CONTROL: the full set of rows a real install leaves behind —
        // `connector_installs` (the fast-path resolver), `oauth_tokens`
        // (the modern path), and `slack_integrations` (both paths' mirror /
        // the legacy path's own row, and where this purge's best-effort
        // `conversations.leave` step gets its token from).
        seed_connector_install(&control, &team_id, Some(&org_id), account).await;
        seed_slack_integration(&control, account, &team_id).await;
        sqlx::query(
            "INSERT INTO oauth_tokens \
                (account_id, provider, scope, access_token, provider_workspace_id) \
             VALUES ($1, 'slack', 'channels:read', '\\x00'::bytea, $2)",
        )
        .bind(account)
        .bind(&team_id)
        .execute(&control)
        .await
        .unwrap();

        mint_tenant_databases_row(&control, &org_id, "full-disconnect-org", account, "SECRET_FULL_DISCONNECT")
            .await;
        let tenant = build_tenant_pool(&base, tnt_db, &org_id).await;

        let team_graph = seed_graph(&tenant, account, "team", None, false).await;
        let team = seed_team(&tenant, account, team_graph, owner).await;
        let channel_id = "C-FULL-DISCONNECT-1";
        ensure_slack_watched_channel(
            &control,
            &tenant,
            AccountPlane::Org,
            single_pick(account, owner, &team_id, channel_id, "chan", team_graph),
        )
        .await
        .unwrap();
        let member = Uuid::new_v4();
        seed_team_member(&tenant, team, member, "member").await;
        seed_seat_grant(&tenant, account, team, member, &crate::seats::slack_channel_source(channel_id)).await;

        let resolver = StaticMapResolver::from_pairs([("SECRET_FULL_DISCONNECT", with_db(&base, tnt_db))]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let mut state = crate::test_app_state_owned(Some(runtime));
        state.db = Some(control.clone());
        // Short timeout — this test's bot token is garbage, so the purge's
        // best-effort `conversations.leave` call must fail fast rather than
        // hang, same reasoning as `ingest_test_state`'s own doc comment.
        state.http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let state = Arc::new(state);

        super::handle_slack_full_disconnect(state, team_id.clone(), "app_uninstalled").await;

        // CONTROL: every row that made this workspace resolvable is gone.
        let oauth_tokens_left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(oauth_tokens_left, 0, "oauth_tokens must be deleted");

        let slack_integrations_left: i64 =
            sqlx::query_scalar("SELECT count(*) FROM slack_integrations WHERE account_id = $1")
                .bind(account)
                .fetch_one(&control)
                .await
                .unwrap();
        assert_eq!(slack_integrations_left, 0, "slack_integrations must be deleted");

        let connector_installs_left: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack'",
        )
        .bind(account)
        .fetch_one(&control)
        .await
        .unwrap();
        assert_eq!(connector_installs_left, 0, "connector_installs must be deleted");

        // TENANT: the same watched-channel/seat purge a manual disconnect runs.
        assert_eq!(
            watched_channel_count(&tenant, account, &team_id).await,
            0,
            "slack_watched_channels must be purged"
        );
        assert!(
            !seat_grant_exists(&tenant, team, member, &crate::seats::slack_channel_source(channel_id)).await,
            "the channel-sourced grant must be revoked"
        );
        assert!(
            team_member_departing_at(&tenant, team, member).await.is_some(),
            "the member's seat must be revoked — it had no other reason to hold it"
        );

        control.close().await;
        tenant.close().await;
        for db in [ctl_db, tnt_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }

    /// `app_uninstalled` MULTI-account fan-out (independent verification
    /// finding, MINOR): the stated reason `handle_slack_full_disconnect`
    /// uses `resolve_slack_account_planes` (the SAME multi-account resolver
    /// `handle_member_joined`/`enqueue_slack_message_ingest` already rely
    /// on) rather than a single-account shortcut is that two DIFFERENT
    /// CoralSwarm accounts can legitimately install the SAME Slack
    /// workspace — a customer org plus someone's personal account, or here,
    /// two separate orgs. That fan-out was entirely unexercised by the
    /// single-account test above. This seeds TWO org accounts (their own
    /// separate tenant databases) both connected to ONE Slack workspace, and
    /// proves `app_uninstalled` purges and fully disconnects BOTH — not just
    /// the first one `resolve_slack_account_planes` happens to return.
    #[tokio::test]
    async fn handle_slack_full_disconnect_fans_out_to_every_account_on_the_workspace_pg() {
        let Some(base) = base_url() else {
            eprintln!(
                "skipping handle_slack_full_disconnect_fans_out_to_every_account_on_the_workspace_pg: \
                 set ORGT_TEST_BASE_URL"
            );
            return;
        };
        let (ctl_db, tnt1_db, tnt2_db) = (
            "orgt_slack_full_disc_fanout_ctl",
            "orgt_slack_full_disc_fanout_tnt1",
            "orgt_slack_full_disc_fanout_tnt2",
        );
        let _database_cleanup_24 = crate::test_db::TestDatabase::adopt_from_url(&base, ctl_db);
        let mut admin_conn = recreate(&base, ctl_db).await;
        let mut _database_cleanup_25s = Vec::new();
        for db in [tnt1_db, tnt2_db] {
            _database_cleanup_25s.push(crate::test_db::TestDatabase::adopt_from_url(&base, db));
            let conn = recreate(&base, db).await;
            conn.close().await.ok();
        }
        let control: PgPool = PgPoolOptions::new().connect(&with_db(&base, ctl_db)).await.unwrap();
        sqlx::migrate!("./migrations_control").run(&control).await.unwrap();

        // ONE Slack workspace, TWO independent CoralSwarm org accounts.
        let team_id = format!("T-fanout-{}", Uuid::new_v4());
        let org1_id = format!("org_fanout_1_{}", Uuid::new_v4());
        let org2_id = format!("org_fanout_2_{}", Uuid::new_v4());
        let owner1 = seed_user(&control).await;
        let owner2 = seed_user(&control).await;
        let account1 = seed_account(&control, owner1, Some(&org1_id)).await;
        let account2 = seed_account(&control, owner2, Some(&org2_id)).await;

        for (account, org_id) in [(account1, &org1_id), (account2, &org2_id)] {
            seed_connector_install(&control, &team_id, Some(org_id), account).await;
            seed_slack_integration(&control, account, &team_id).await;
            sqlx::query(
                "INSERT INTO oauth_tokens \
                    (account_id, provider, scope, access_token, provider_workspace_id) \
                 VALUES ($1, 'slack', 'channels:read', '\\x00'::bytea, $2)",
            )
            .bind(account)
            .bind(&team_id)
            .execute(&control)
            .await
            .unwrap();
        }

        mint_tenant_databases_row(&control, &org1_id, "fanout-org-1", account1, "SECRET_FANOUT_1").await;
        mint_tenant_databases_row(&control, &org2_id, "fanout-org-2", account2, "SECRET_FANOUT_2").await;
        let tenant1 = build_tenant_pool(&base, tnt1_db, &org1_id).await;
        let tenant2 = build_tenant_pool(&base, tnt2_db, &org2_id).await;

        // Independent team/channel/seat state on EACH tenant, so a bug that
        // only purges the FIRST-resolved account's tenant plane would leave
        // the second one's seat standing.
        let channel_id = "C-FANOUT-1";
        let team1_graph = seed_graph(&tenant1, account1, "team", None, false).await;
        let team1 = seed_team(&tenant1, account1, team1_graph, owner1).await;
        ensure_slack_watched_channel(
            &control,
            &tenant1,
            AccountPlane::Org,
            single_pick(account1, owner1, &team_id, channel_id, "chan", team1_graph),
        )
        .await
        .unwrap();
        let member1 = Uuid::new_v4();
        seed_team_member(&tenant1, team1, member1, "member").await;
        seed_seat_grant(&tenant1, account1, team1, member1, &crate::seats::slack_channel_source(channel_id)).await;

        let team2_graph = seed_graph(&tenant2, account2, "team", None, false).await;
        let team2 = seed_team(&tenant2, account2, team2_graph, owner2).await;
        ensure_slack_watched_channel(
            &control,
            &tenant2,
            AccountPlane::Org,
            single_pick(account2, owner2, &team_id, channel_id, "chan", team2_graph),
        )
        .await
        .unwrap();
        let member2 = Uuid::new_v4();
        seed_team_member(&tenant2, team2, member2, "member").await;
        seed_seat_grant(&tenant2, account2, team2, member2, &crate::seats::slack_channel_source(channel_id)).await;

        let resolver = StaticMapResolver::from_pairs([
            ("SECRET_FANOUT_1", with_db(&base, tnt1_db)),
            ("SECRET_FANOUT_2", with_db(&base, tnt2_db)),
        ]);
        let registry = Arc::new(TenantRegistry::new(Arc::new(resolver)));
        let runtime = TenantRuntime::new(true, "coralswarm.test", registry);
        let mut state = crate::test_app_state_owned(Some(runtime));
        state.db = Some(control.clone());
        state.http_client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .unwrap();
        let state = Arc::new(state);

        super::handle_slack_full_disconnect(state, team_id.clone(), "app_uninstalled").await;

        for account in [account1, account2] {
            let oauth_tokens_left: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM oauth_tokens WHERE account_id = $1 AND provider = 'slack'",
            )
            .bind(account)
            .fetch_one(&control)
            .await
            .unwrap();
            assert_eq!(oauth_tokens_left, 0, "oauth_tokens must be deleted for account {account}");

            let connector_installs_left: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM connector_installs WHERE account_id = $1 AND provider = 'slack'",
            )
            .bind(account)
            .fetch_one(&control)
            .await
            .unwrap();
            assert_eq!(
                connector_installs_left, 0,
                "connector_installs must be deleted for account {account}"
            );
        }

        assert_eq!(
            watched_channel_count(&tenant1, account1, &team_id).await,
            0,
            "account 1's slack_watched_channels must be purged"
        );
        assert!(
            team_member_departing_at(&tenant1, team1, member1).await.is_some(),
            "account 1's member seat must be revoked"
        );

        assert_eq!(
            watched_channel_count(&tenant2, account2, &team_id).await,
            0,
            "account 2's slack_watched_channels must ALSO be purged — not just the first \
             account resolve_slack_account_planes happened to return"
        );
        assert!(
            team_member_departing_at(&tenant2, team2, member2).await.is_some(),
            "account 2's member seat must ALSO be revoked"
        );

        control.close().await;
        tenant1.close().await;
        tenant2.close().await;
        for db in [ctl_db, tnt1_db, tnt2_db] {
            admin_conn
                .execute(format!("DROP DATABASE IF EXISTS {db} WITH (FORCE)").as_str())
                .await
                .ok();
        }
    }
}
